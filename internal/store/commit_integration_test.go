//go:build integration

package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"

	"github.com/spec-studio-app/plugin-sdk/go/protocol"

	"github.com/spec-studio-app/plugin-temporal/internal/workflows"
)

func activity(types ...string) []protocol.NewEvent {
	out := make([]protocol.NewEvent, len(types))
	for i, ty := range types {
		out[i] = protocol.NewEvent{Type: "activity", Data: json.RawMessage(`{"kind":"` + ty + `"}`)}
	}
	return out
}

func commit(t *testing.T, s *Store, coll, key, stream string, value any, expected *string, events []protocol.NewEvent) (protocol.StateCommitResult, error) {
	t.Helper()
	b, _ := json.Marshal(value)
	return s.Commit(ctx(t), protocol.StateCommitParams{Collection: coll, Key: key, Value: b, Attributes: protocol.Attributes{"stage": fmt.Sprint(value)}, ExpectedVersion: expected, Stream: stream, Events: events})
}

func kinds(t *testing.T, s *Store, stream string) []string {
	t.Helper()
	res, err := s.Read(ctx(t), protocol.EventsReadParams{Stream: stream, Limit: 1000})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var out []string
	for _, e := range res.Events {
		var d struct{ Kind string }
		_ = json.Unmarshal(e.Data, &d)
		out = append(out, d.Kind)
	}
	return out
}

// R2.S1, R2.S2: a spec's state and its activity are one commit update of
// the spec's workflow. A stale version gets conflict with the stored
// version, and the history has no trace of it.
func TestCommitSpec(t *testing.T) {
	e := newEnv(t)
	s := e.open(Options{})
	const key = "1a2b3c4d5e6f:fix-login"
	const stream = "spec:" + key

	r1, err := commit(t, s, "app-specs", key, stream, "spec", nil, activity("a", "b"))
	if err != nil {
		t.Fatal(err)
	}
	r2, err := commit(t, s, "app-specs", key, stream, "plan", &r1.Version, activity("c"))
	if err != nil {
		t.Fatal(err)
	}
	if len(r1.Cursors) != 2 || len(r2.Cursors) != 1 {
		t.Fatalf("cursors %v %v", r1.Cursors, r2.Cursors)
	}
	_, err = commit(t, s, "app-specs", key, stream, "build", &r1.Version, activity("d", "e"))
	var perr *protocol.Error
	if !protocol.Is(err, protocol.Conflict) || !asError(err, &perr) || perr.Data.Version == nil || *perr.Data.Version != r2.Version {
		t.Fatalf("a stale commit: %v", err)
	}
	if got := kinds(t, s, stream); !eq(got, []string{"a", "b", "c"}) {
		t.Fatalf("the stream has %v", got)
	}
	g, err := s.Get(ctx(t), protocol.StateGetParams{Collection: "app-specs", Key: key})
	if err != nil || g.Version != r2.Version || string(g.Value) != `"plan"` {
		t.Fatalf("get: %+v %v", g, err)
	}
	// The cursors of the commit are the cursors of the read.
	res, _ := s.Read(ctx(t), protocol.EventsReadParams{Stream: stream, After: &r1.Cursors[1]})
	if len(res.Events) != 1 || res.Events[0].Cursor != r2.Cursors[0] {
		t.Fatalf("read after the first commit: %+v", res)
	}

	// One workflow: the history has two commit updates (the stale one was
	// refused by the validator) and no append update.
	wf := s.router.Doc("app-specs", key)
	if wf.WorkflowID != s.router.Stream(stream).WorkflowID {
		t.Fatal("the document and the stream have two workflows")
	}
	names := map[string]int{}
	for _, ev := range historyOf(t, raw(t), wf.WorkflowID, "") {
		if ev.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED {
			names[ev.GetWorkflowExecutionUpdateAcceptedEventAttributes().GetAcceptedRequest().GetInput().GetName()]++
		}
	}
	if names[workflows.UpdateCommit] != 2 || names[workflows.UpdateAppend] != 0 {
		t.Fatalf("the accepted updates are %v", names)
	}
}

func asError(err error, out **protocol.Error) bool { return errors.As(err, out) }

// Commits and appends of one spec across run boundaries: the stream reads
// in order, and no event is lost or comes twice.
func TestCommitAcrossRuns(t *testing.T) {
	e := newEnv(t)
	s := e.open(Options{RunLimit: 3})
	const key = "1a2b3c4d5e6f:many"
	const stream = "spec:" + key
	var version *string
	var want []string
	for i := 0; i < 8; i++ {
		k := fmt.Sprintf("c%d", i)
		r, err := commit(t, s, "app-specs", key, stream, i, version, activity(k))
		if err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
		version = &r.Version
		want = append(want, k)
		if i%3 == 1 {
			// The relay's own events in the same stream.
			appendEvents(t, s, stream, "sync")
			want = append(want, "")
		}
	}
	waitFor(t, "the reads", time.Minute, func() bool { return eq(kinds(t, s, stream), want) })
	g, err := s.Get(ctx(t), protocol.StateGetParams{Collection: "app-specs", Key: key})
	if err != nil || g.Version != *version || string(g.Value) != "7" {
		t.Fatalf("get: %+v %v", g, err)
	}
}

// R2 (two workflows): a document and a stream with two workflows. The
// commit writes the document and delivers the events before it returns; a
// stale commit writes neither.
func TestCommitTwoWorkflows(t *testing.T) {
	e := newEnv(t)
	s := e.open(Options{})
	if s.router.Doc("notes", "a").WorkflowID == s.router.Stream("notes-log").WorkflowID {
		t.Fatal("the test needs two workflows")
	}
	r1, err := commit(t, s, "notes", "a", "notes-log", 1, nil, activity("a", "b"))
	if err != nil {
		t.Fatal(err)
	}
	if got := kinds(t, s, "notes-log"); !eq(got, []string{"a", "b"}) {
		t.Fatalf("the stream has %v", got)
	}
	res, _ := s.Read(ctx(t), protocol.EventsReadParams{Stream: "notes-log"})
	if len(res.Events) != 2 || res.Events[0].Cursor != r1.Cursors[0] || res.Events[1].Cursor != r1.Cursors[1] {
		t.Fatalf("the commit gave %v; the read gives %+v", r1.Cursors, res.Events)
	}
	stale := "nope"
	if _, err := commit(t, s, "notes", "a", "notes-log", 2, &stale, activity("c")); !protocol.Is(err, protocol.Conflict) {
		t.Fatalf("a stale commit: %v", err)
	}
	r2, err := commit(t, s, "notes", "a", "notes-log", 3, &r1.Version, activity("d"))
	if err != nil {
		t.Fatal(err)
	}
	if got := kinds(t, s, "notes-log"); !eq(got, []string{"a", "b", "d"}) {
		t.Fatalf("the stream has %v", got)
	}
	g, _ := s.Get(ctx(t), protocol.StateGetParams{Collection: "notes", Key: "a"})
	if g.Version != r2.Version || string(g.Value) != "3" {
		t.Fatalf("get: %+v", g)
	}
	// A relay document with events in a data stream: refused.
	if _, err := commit(t, s, "relay", "x", "notes-log", 1, nil, activity("z")); !protocol.Is(err, protocol.Refused) {
		t.Fatalf("a relay document with a data stream: %v", err)
	}
}

// R2.S2 across processes: commits with the same expected version from two
// plugin processes; exactly one wins, and the stream has only its events.
func TestCommitRaceTwoProcesses(t *testing.T) {
	e := newEnv(t)
	a, b := e.open(Options{}), e.open(Options{})
	const key = "1a2b3c4d5e6f:race"
	const stream = "spec:" + key
	r0, err := commit(t, a, "app-specs", key, stream, "start", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	wins, conflicts := []string{}, 0
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s := a
			if i%2 == 1 {
				s = b
			}
			k := fmt.Sprintf("w%d", i)
			_, err := commit(t, s, "app-specs", key, stream, k, &r0.Version, activity(k+"-1", k+"-2"))
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				wins = append(wins, k)
			case protocol.Is(err, protocol.Conflict):
				conflicts++
			default:
				t.Errorf("commit: %v", err)
			}
		}(i)
	}
	wg.Wait()
	if len(wins) != 1 || conflicts != 9 {
		t.Fatalf("wins %v conflicts %d", wins, conflicts)
	}
	if got := kinds(t, b, stream); strings.Join(got, ",") != wins[0]+"-1,"+wins[0]+"-2" {
		t.Fatalf("the stream has %v", got)
	}
}

// R10: the storeId names the namespace (its name and its ID) and the
// deploy ID, not the address: two processes that reach the same deploy by
// two addresses give the same value.
func TestStoreID(t *testing.T) {
	e := newEnv(t)
	s := e.open(Options{})
	if s.namespaceID == "" {
		t.Fatal("the store has no namespace ID")
	}
	want := "temporal:default/" + s.namespaceID + "/" + e.deploy
	if got := s.StoreID(); got != want {
		t.Fatalf("storeId %q; want %q", got, want)
	}
	if got := e.open(Options{}).StoreID(); got != want {
		t.Fatalf("a second process gives %q", got)
	}
	// The same server by another address.
	host, port, err := net.SplitHostPort(e.settings["address"])
	if err != nil {
		t.Fatal(err)
	}
	other := "localhost"
	if host == "localhost" {
		other = "127.0.0.1"
	}
	e.settings["address"] = strings.ToUpper(other) + ":" + port
	if got := e.open(Options{}).StoreID(); got != want {
		t.Fatalf("a process with the address %s gives %q; want %q", e.settings["address"], got, want)
	}
	// Another deploy on the same namespace gives another value.
	e.settings["deploy_id"] = e.deploy + "x"
	if got := e.open(Options{}).StoreID(); got == want {
		t.Fatalf("another deploy gives the same storeId %q", got)
	}
}
