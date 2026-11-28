package store

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
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
//   - a call waits for a worker for workerWait, and Temporal shows that no
//     other worker serves the task queue (no other poller polled since the
//     call began, and the call's task waits in the task queue): for example,
//     the app closed a moment ago and Temporal still lists its poller. Then
//     it starts the worker of that task queue only. A slow app (a far
//     region, a long replay) takes the task, so it does not count.
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

// cliStopTimeout is the WorkerStopTimeout of a CLI worker: at the shutdown,
// an activity in progress (a catalog write, a delivery, a run export) gets
// this long to end and report, so Temporal does not wait for its timeout
// (1 to 2 minutes) before it gives the activity to another worker. The
// plugin's shutdown has 5 s for the store's Close (plugin.shutdown's
// default, the end of stdin and SIGINT/SIGTERM): 3 s for the stop, then at
// most activityReport for a stopped activity to report, leaves time to close
// the clients. A CLI process with no activity in progress stops at once.
const cliStopTimeout = 3 * time.Second

// activityReport is the longest wait, after the stop, for the activities
// that the stop cancelled to report their end to Temporal.
const activityReport = time.Second

// noPollerRetry: after the plugin starts a worker, Temporal may still answer
// "no poller seen" for a short time (the new pollers reach the server, and
// each task queue partition, a moment later). A query then tries again for
// this long.
const noPollerRetry = 2 * time.Second

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
		opts.WorkerStopTimeout = cliStopTimeout
		opts.Interceptors = []interceptor.WorkerInterceptor{&countActivities{n: &s.activities}}
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
	s.started[tq] = time.Now()
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

// fallbackWorker checks each workerWait, while the call to the task queue
// is not done, whether another worker serves the task queue; when none
// does, it starts this process's worker of that task queue. Call the
// returned function when the call is done.
//
// A query is never in the backlog (Temporal gives it only to a poller that
// waits), so for a query only a poller that polled again counts.
func (s *Store) fallbackWorker(ctx context.Context, tq string, query bool) func() {
	if !s.needsWorker(tq) {
		return func() {}
	}
	since := time.Now()
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(workerWait)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-t.C:
			}
			if !s.needsWorker(tq) || s.otherWorkerServes(ctx, tq, since, query) {
				continue
			}
			if err := s.startWorker(tq); err != nil && s.log != nil {
				s.log.Warn("cannot start the worker", "taskQueue", tq, "error", err.Error())
			}
			return
		}
	}()
	var once sync.Once
	return func() { once.Do(func() { close(done) }) }
}

// otherWorkerServes is true when another worker (not a CLI worker) serves
// the task queue of a call that began at since: one of its pollers polled
// again after since (it took a task, maybe this call's), or it polled
// lately and no workflow task waits in the task queue for more than
// workerWait (so the call's task went to a worker; not for a query). When
// Temporal does not answer, it is false.
func (s *Store) otherWorkerServes(ctx context.Context, tq string, since time.Time, query bool) bool {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := s.data.WorkflowService().DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
		Namespace:     s.set.Namespace,
		TaskQueue:     &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		ReportStats:   true,
	})
	if err != nil {
		return false
	}
	fresh := false
	for _, p := range resp.GetPollers() {
		at := p.GetLastAccessTime()
		if at == nil || strings.HasSuffix(p.GetIdentity(), cliIdentity) {
			continue
		}
		if at.AsTime().After(since) {
			return true
		}
		if time.Since(at.AsTime()) < pollerFresh {
			fresh = true
		}
	}
	if !fresh || query {
		return false
	}
	st := resp.GetStats()
	if st == nil {
		// No backlog figure: count the fresh poller.
		return true
	}
	return st.GetApproximateBacklogCount() == 0 || st.GetApproximateBacklogAge().AsDuration() < workerWait
}

// justStarted is true when this process started the worker of the task
// queue less than noPollerRetry ago.
func (s *Store) justStarted(tq string) bool {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	at, ok := s.started[tq]
	return ok && time.Since(at) < noPollerRetry
}

// awaitActivities waits, at most activityReport, until no activity of this
// process runs: the stop cancelled them, and each reports its end to
// Temporal (so Temporal retries it at once) before the clients close.
func (s *Store) awaitActivities() {
	deadline := time.Now().Add(activityReport)
	for s.activities.Load() > 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	// The report follows the activity's end in the same goroutine.
	time.Sleep(100 * time.Millisecond)
}

// countActivities counts the activities in progress.
type countActivities struct {
	interceptor.WorkerInterceptorBase
	n *atomic.Int64
}

func (c *countActivities) InterceptActivity(ctx context.Context, next interceptor.ActivityInboundInterceptor) interceptor.ActivityInboundInterceptor {
	return &countActivity{ActivityInboundInterceptorBase: interceptor.ActivityInboundInterceptorBase{Next: next}, n: c.n}
}

type countActivity struct {
	interceptor.ActivityInboundInterceptorBase
	n *atomic.Int64
}

func (a *countActivity) ExecuteActivity(ctx context.Context, in *interceptor.ExecuteActivityInput) (any, error) {
	a.n.Add(1)
	defer a.n.Add(-1)
	return a.Next.ExecuteActivity(ctx, in)
}

// noPoller is Temporal's answer to a query when no worker polled the task
// queue lately.
func noPoller(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "no poller seen")
}
