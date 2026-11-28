//go:build integration

package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/spec-studio-app/plugin-sdk/go/plugin"
	"github.com/spec-studio-app/plugin-sdk/go/protocol"
)

// openKind opens a store for a host kind.
func (e *env) openKind(kind string) *Store {
	e.t.Helper()
	s := New(func() Options { return Options{} })
	c, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := s.Open(c, plugin.OpenOptions{Settings: e.settings, Host: protocol.Host{Kind: kind}, Log: discard()}); err != nil {
		e.t.Fatalf("open %s: %v", kind, err)
	}
	e.t.Cleanup(func() { closeStore(s) })
	return s
}

func closeStore(s *Store) {
	c, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_ = s.Close(c)
}

func workerCount(s *Store) int {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	return len(s.workers)
}

func getText(t *testing.T, s *Store, coll, key string) string {
	t.Helper()
	res, err := s.Get(ctx(t), protocol.StateGetParams{Collection: coll, Key: key})
	if err != nil {
		t.Fatalf("get %s/%s: %v", coll, key, err)
	}
	var v map[string]string
	_ = json.Unmarshal(res.Value, &v)
	return v["text"]
}

// With the app and the relay closed, the CLI's plugin runs the workers
// itself: its writes and reads work, also in a new process for a workflow
// that an earlier CLI process ran.
func TestCLIAloneRunsWorkers(t *testing.T) {
	e := newEnv(t)
	cli := e.openKind(protocol.HostCLI)
	if n := workerCount(cli); n != 2 {
		t.Fatalf("the CLI alone runs %d workers, want 2", n)
	}
	v1 := put(t, cli, "specs", "1a2b3c4d5e6f:fix-login", map[string]string{"text": "one"}, nil, nil).Version
	put(t, cli, "relay", "config", map[string]string{"text": "cfg"}, nil, nil)
	closeStore(cli)

	// The next command: a new process, the entity workflow still runs but
	// nobody polls. The stop was graceful, so nothing waits for the sticky
	// timeout.
	start := time.Now()
	cli2 := e.openKind(protocol.HostCLI)
	if got := getText(t, cli2, "specs", "1a2b3c4d5e6f:fix-login"); got != "one" {
		t.Fatalf("read %q", got)
	}
	put(t, cli2, "specs", "1a2b3c4d5e6f:fix-login", map[string]string{"text": "two"}, nil, &v1)
	if got := getText(t, cli2, "relay", "config"); got != "cfg" {
		t.Fatalf("read config %q", got)
	}
	if n := workerCount(cli2); n != 2 {
		t.Fatalf("the next CLI process runs %d workers, want 2: the poller of the last CLI process is no other worker", n)
	}
	if d := time.Since(start); d > workerWait {
		t.Fatalf("the second CLI process took %v", d)
	}
}

// With the app open, the CLI's plugin starts no worker: the app's worker
// runs the CLI's calls, so the CLI takes none of the app's work.
func TestCLIWithAppRunsNoWorker(t *testing.T) {
	e := newEnv(t)
	app := e.openKind(protocol.HostApp)
	v1 := put(t, app, "specs", "1a2b3c4d5e6f:fix-login", map[string]string{"text": "app"}, nil, nil).Version
	put(t, app, "relay", "config", map[string]string{"text": "cfg"}, nil, nil)

	cli := e.openKind(protocol.HostCLI)
	if n := workerCount(cli); n != 0 {
		t.Fatalf("the CLI runs %d workers next to the app, want 0", n)
	}
	if got := getText(t, cli, "specs", "1a2b3c4d5e6f:fix-login"); got != "app" {
		t.Fatalf("read %q", got)
	}
	cliV := put(t, cli, "specs", "1a2b3c4d5e6f:fix-login", map[string]string{"text": "cli"}, nil, &v1).Version
	// A new workflow: update-with-start, run by the app's worker.
	put(t, cli, "specs", "0f0f0f0f0f0f:other", map[string]string{"text": "new"}, nil, nil)
	appendEvents(t, cli, "spec:1a2b3c4d5e6f:fix-login", "message")
	if got := getText(t, cli, "relay", "config"); got != "cfg" {
		t.Fatalf("read config %q", got)
	}
	// A long wait (longer than workerWait) is accepted at once: it starts
	// no worker either.
	wctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	defer cancel()
	if res, err := cli.Wait(wctx, protocol.WatchWaitParams{Collection: "specs", Key: "1a2b3c4d5e6f:fix-login", Version: &cliV, TimeoutMs: 2500}); err != nil || res.Changed {
		t.Fatalf("wait: %+v %v", res, err)
	}
	if n := workerCount(cli); n != 0 {
		t.Fatalf("the CLI started %d workers next to the app, want 0", n)
	}
	if got := getText(t, app, "specs", "1a2b3c4d5e6f:fix-login"); got != "cli" {
		t.Fatalf("the app reads %q", got)
	}
}

// The app closed a moment ago: Temporal still lists its poller. The CLI's
// call gets no answer, so after workerWait the CLI starts its worker and
// the call completes.
func TestCLIAfterAppClosedStartsWorker(t *testing.T) {
	e := newEnv(t)
	app := e.openKind(protocol.HostApp)
	v1 := put(t, app, "specs", "1a2b3c4d5e6f:fix-login", map[string]string{"text": "app"}, nil, nil).Version
	closeStore(app)

	cli := e.openKind(protocol.HostCLI)
	if n := workerCount(cli); n != 0 {
		t.Fatalf("the CLI runs %d workers while the app's poller is fresh, want 0", n)
	}
	start := time.Now()
	put(t, cli, "specs", "1a2b3c4d5e6f:fix-login", map[string]string{"text": "cli"}, nil, &v1)
	if got := getText(t, cli, "specs", "1a2b3c4d5e6f:fix-login"); got != "cli" {
		t.Fatalf("read %q", got)
	}
	// The point is that the call completes; CI machines are slower than a
	// laptop, so the bound is wide.
	if d := time.Since(start); d > workerWait+15*time.Second {
		t.Fatalf("the CLI took %v", d)
	}
	// The safety net starts only the worker of the call's task queue.
	if !cli.hasWorker(cli.set.TaskQueue) || cli.hasWorker(cli.set.ConfigTaskQueue) {
		t.Fatalf("the CLI runs the workers %v, want only %s", cli.workerQueues(), cli.set.TaskQueue)
	}
}

// A slow call next to a live app: the safety net checks again and starts
// no worker, because the app's worker serves the task queue.
func TestCLISlowCallNextToAppStartsNoWorker(t *testing.T) {
	e := newEnv(t)
	app := e.openKind(protocol.HostApp)
	put(t, app, "specs", "1a2b3c4d5e6f:fix-login", map[string]string{"text": "app"}, nil, nil)
	cli := e.openKind(protocol.HostCLI)
	tq := cli.set.TaskQueue
	if !cli.otherWorkerServes(ctx(t), tq, time.Now(), false) {
		t.Fatal("the app's worker does not count as serving the task queue")
	}
	// A call that takes 3 workerWaits.
	answered := cli.fallbackWorker(ctx(t), tq, false)
	time.Sleep(3*workerWait + 200*time.Millisecond)
	answered()
	if n := workerCount(cli); n != 0 {
		t.Fatalf("a slow call next to the app started %d workers, want 0", n)
	}
	if got := getText(t, cli, "specs", "1a2b3c4d5e6f:fix-login"); got != "app" {
		t.Fatalf("read %q", got)
	}
}
