package store

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/spec-studio-app/plugin-sdk/go/protocol"

	"github.com/spec-studio-app/plugin-temporal/internal/activities"
	"github.com/spec-studio-app/plugin-temporal/internal/workflows"
)

// The workers of the host kind cli.
//
// The studio CLI starts a plugin process for each command. A worker in that
// process joins the task queue of the app and the relay for a moment, so it
// can take a workflow task or an activity of any spec. So the CLI's plugin
// starts a worker on a task queue only when:
//
//   - no other worker polled the task queue lately (the app and the relay
//     are closed): then nobody else runs the workflows, and a call needs
//     this worker; or
//   - a call got no answer in workerWait (for example, the app closed a
//     moment ago and Temporal still lists its poller). Then it starts both
//     workers.
//
// The pollers of CLI workers have an identity that ends in cliIdentity. They
// do not count as other workers: a CLI process lives for one command and
// stops its workers at its end, so the next command must not wait for it.
//
// A CLI worker stops gracefully at the plugin's shutdown (worker.Stop), and
// a workflow that it ran is sticky to it for at most cliStickyTimeout.
var (
	// pollerFresh: a poller that Temporal saw this recently is a live
	// worker. An idle worker polls again at least each minute (long poll).
	pollerFresh = 90 * time.Second
	// workerWait is the time that a call of the CLI waits for another
	// worker before the plugin starts its own.
	workerWait = time.Second
)

// cliIdentity ends the worker identity of the host kind cli.
const cliIdentity = "@studio-cli"

// cliStickyTimeout: after an abrupt end of a CLI process, its workflows go
// back to the normal task queue after this time (Temporal's default is 5 s).
const cliStickyTimeout = time.Second

// openWorkers starts the workers of Open: both workers, or for the host kind
// cli only those whose task queue has no other worker.
func (s *Store) openWorkers(ctx context.Context) error {
	queues := []string{s.set.TaskQueue, s.set.ConfigTaskQueue}
	if !s.lazy {
		for _, tq := range queues {
			if err := s.startWorker(tq); err != nil {
				return err
			}
		}
		return nil
	}
	var wg sync.WaitGroup
	errs := make([]error, len(queues))
	for i, tq := range queues {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if !s.otherWorker(ctx, tq) {
				errs[i] = s.startWorker(tq)
			}
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// otherWorker is true when Temporal saw a workflow poller on the task queue
// in the last pollerFresh. When Temporal does not answer, it is false: the
// plugin then runs its worker, as before.
func (s *Store) otherWorker(ctx context.Context, tq string) bool {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := s.data.DescribeTaskQueue(ctx, tq, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	if err != nil {
		return false
	}
	for _, p := range resp.GetPollers() {
		if strings.HasSuffix(p.GetIdentity(), cliIdentity) {
			continue
		}
		if t := p.GetLastAccessTime(); t != nil && time.Since(t.AsTime()) < pollerFresh {
			return true
		}
	}
	return false
}

// startWorker starts the worker of a task queue, once. It does nothing after
// the store closed.
func (s *Store) startWorker(tq string) error {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	if s.closed || s.workers == nil {
		return nil
	}
	if _, ok := s.workers[tq]; ok {
		return nil
	}
	c, dc := s.workerClient(tq)
	opts := worker.Options{}
	if s.lazy {
		opts.StickyScheduleToStartTimeout = cliStickyTimeout
		host, _ := os.Hostname()
		opts.Identity = fmt.Sprintf("%d@%s%s", os.Getpid(), host, cliIdentity)
	}
	w := worker.New(c, tq, opts)
	w.RegisterWorkflowWithOptions(workflows.Entity, workflow.RegisterOptions{Name: workflows.EntityWorkflow})
	w.RegisterWorkflowWithOptions(workflows.Catalog, workflow.RegisterOptions{Name: workflows.CatalogWorkflow})
	a := &activities.Activities{Client: c, DC: dc, Blobs: s.blobs, Router: s.router, CatalogRunLimit: s.opts.CatalogRunLimit, InlineBytes: s.opts.InlineBytes, RunLimit: s.opts.RunLimit, IdleMs: s.opts.Idle.Milliseconds()}
	a.Register(w)
	if err := w.Start(); err != nil {
		return protocol.NewError(protocol.Unavailable, "The plugin cannot start its Temporal worker on the task queue %s.", tq)
	}
	s.workers[tq] = w
	if s.lazy && s.log != nil {
		s.log.Info("worker started", "taskQueue", tq)
	}
	return nil
}

func (s *Store) workerClient(tq string) (client.Client, converter.DataConverter) {
	if tq == s.set.ConfigTaskQueue {
		return s.config, s.configDC
	}
	return s.data, s.dataDC
}

// hasWorker is true when this process runs the worker of the task queue,
// or can start no worker any more (the store closed).
func (s *Store) hasWorker(tq string) bool {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	_, ok := s.workers[tq]
	return ok || s.closed
}

// workerQueues lists the task queues of the running workers, for the log.
func (s *Store) workerQueues() []string {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	var out []string
	for tq := range s.workers {
		out = append(out, tq)
	}
	return out
}

// needsWorker is true when a call to the task queue may need this process's
// worker: the host kind cli, with no worker of its own on it yet.
func (s *Store) needsWorker(tq string) bool {
	return s.lazy && !s.hasWorker(tq)
}

// fallbackWorker starts the workers when the call to the task queue is not
// done after workerWait. Call the returned function when the call is done.
func (s *Store) fallbackWorker(ctx context.Context, tq string) func() {
	if !s.needsWorker(tq) {
		return func() {}
	}
	done := make(chan struct{})
	go func() {
		t := time.NewTimer(workerWait)
		defer t.Stop()
		select {
		case <-done:
		case <-ctx.Done():
		case <-t.C:
			for _, q := range []string{tq, s.set.TaskQueue, s.set.ConfigTaskQueue} {
				if err := s.startWorker(q); err != nil && s.log != nil {
					s.log.Warn("cannot start the worker", "taskQueue", q, "error", err.Error())
				}
			}
		}
	}()
	var once sync.Once
	return func() { once.Do(func() { close(done) }) }
}

// noPoller is Temporal's answer to a query when no worker polled the task
// queue lately.
func noPoller(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "no poller seen")
}
