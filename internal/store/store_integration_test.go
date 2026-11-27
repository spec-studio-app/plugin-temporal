//go:build integration

package store

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"

	"github.com/spec-studio-app/plugin-sdk/go/plugin"
	"github.com/spec-studio-app/plugin-sdk/go/protocol"

	"github.com/spec-studio-app/plugin-temporal/internal/blobstore"
	"github.com/spec-studio-app/plugin-temporal/internal/codec"
	"github.com/spec-studio-app/plugin-temporal/internal/workflows"
)

// R1.S3
func TestUnreachable(t *testing.T) {
	e := newEnv(t)
	e.settings["address"] = "127.0.0.1:1"
	s := New(func() Options { return Options{} })
	err := s.Open(context.Background(), plugin.OpenOptions{Settings: e.settings, Log: discard()})
	if !protocol.Is(err, protocol.Unavailable) || !strings.Contains(err.Error(), "address") {
		t.Fatalf("got %v", err)
	}
}

// R3.S1: the relay and the app write to one workflow.
func TestTwoProcessesOneWorkflow(t *testing.T) {
	e := newEnv(t)
	relay, app := e.open(Options{}), e.open(Options{})
	stream := "spec:1a2b3c4d5e6f:fix-login"
	appendEvents(t, relay, stream, "message")
	appendEvents(t, app, stream, "activity")
	for _, s := range []*Store{relay, app} {
		if got := readTypes(t, s, protocol.EventsReadParams{Stream: stream}); !eq(got, []string{"message", "activity"}) {
			t.Fatalf("read %v", got)
		}
	}
	wf := "spec-" + e.deploy + "-1a2b3c4d5e6f-fix-login"
	if _, err := raw(t).DescribeWorkflowExecution(ctx(t), wf, ""); err != nil {
		t.Fatalf("describe %s: %v", wf, err)
	}
}

// R3.S2: the relay's IDs and task queues. R3.S4: the payloads are encrypted.
func TestIDsQueuesAndEncryption(t *testing.T) {
	e := newEnv(t)
	s := e.open(Options{})
	put(t, s, "specs", "1a2b3c4d5e6f:fix-login", map[string]string{"text": "PLAIN-SPEC"}, nil, nil)
	put(t, s, "relay", "config", map[string]string{"text": "PLAIN-CONFIG"}, nil, nil)
	rc := raw(t)
	cases := []struct{ wf, tq, keyPrefix string }{
		{"spec-" + e.deploy + "-1a2b3c4d5e6f-fix-login", "relay-" + e.deploy, "d"},
		{"relay-config-" + e.deploy, "relay-" + e.deploy + "-config", "c"},
	}
	for _, c := range cases {
		d, err := rc.DescribeWorkflowExecution(ctx(t), c.wf, "")
		if err != nil {
			t.Fatalf("describe %s: %v", c.wf, err)
		}
		if got := d.GetWorkflowExecutionInfo().GetTaskQueue(); got != c.tq {
			t.Fatalf("%s runs on %s, want %s", c.wf, got, c.tq)
		}
		n := 0
		for _, ev := range historyOf(t, rc, c.wf, "") {
			for _, p := range payloadsOf(ev) {
				n++
				if string(p.GetMetadata()["encoding"]) != codec.Encoding {
					t.Fatalf("%s: a payload with encoding %q", c.wf, p.GetMetadata()["encoding"])
				}
				if id := string(p.GetMetadata()[codec.KeyIDHeader]); !strings.HasPrefix(id, c.keyPrefix) {
					t.Fatalf("%s: a payload with the key %q", c.wf, id)
				}
				if strings.Contains(string(p.GetData()), "PLAIN-") {
					t.Fatalf("%s: a plain payload", c.wf)
				}
			}
		}
		if n == 0 {
			t.Fatalf("%s: no payload found", c.wf)
		}
	}
}

// payloadsOf finds every payload in a history event.
func payloadsOf(ev *historypb.HistoryEvent) []*commonpb.Payload {
	var out []*commonpb.Payload
	var walk func(m protoreflect.Message)
	walk = func(m protoreflect.Message) {
		if p, ok := m.Interface().(*commonpb.Payload); ok {
			out = append(out, p)
			return
		}
		m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
			switch {
			case fd.IsList() && fd.Kind() == protoreflect.MessageKind:
				for i := 0; i < v.List().Len(); i++ {
					walk(v.List().Get(i).Message())
				}
			case fd.IsMap() && fd.MapValue().Kind() == protoreflect.MessageKind:
				v.Map().Range(func(_ protoreflect.MapKey, mv protoreflect.Value) bool {
					walk(mv.Message())
					return true
				})
			case fd.Kind() == protoreflect.MessageKind && !fd.IsList() && !fd.IsMap():
				walk(v.Message())
			}
			return true
		})
	}
	walk(proto.Clone(ev).ProtoReflect())
	return out
}

// Search attributes and the header are not payloads of the plugin.
var _ = commonpb.SearchAttributes{}

// R3.S3
func TestOtherName(t *testing.T) {
	s := newEnv(t).open(Options{})
	put(t, s, "notes", "a1", "hello", nil, nil)
	got, err := s.Get(ctx(t), protocol.StateGetParams{Collection: "notes", Key: "a1"})
	if err != nil || !got.Exists || string(got.Value) != `"hello"` {
		t.Fatalf("get: %+v %v", got, err)
	}
}

// R4.S1: two processes write with the same expected version at once.
func TestRaceTwoProcesses(t *testing.T) {
	e := newEnv(t)
	a, b := e.open(Options{}), e.open(Options{})
	v := put(t, a, "specs", "1a2b3c4d5e6f:race", 0, nil, nil).Version
	for i := 1; i <= 20; i++ {
		var wg sync.WaitGroup
		errs := make([]error, 2)
		vers := make([]string, 2)
		for j, s := range []*Store{a, b} {
			wg.Add(1)
			go func(j int, s *Store) {
				defer wg.Done()
				exp := v
				res, err := s.Put(ctx(t), protocol.StatePutParams{Collection: "specs", Key: "1a2b3c4d5e6f:race", Value: []byte("1"), ExpectedVersion: &exp})
				errs[j], vers[j] = err, res.Version
			}(j, s)
		}
		wg.Wait()
		ok := 0
		for j := range errs {
			if errs[j] == nil {
				ok++
				v = vers[j]
			} else if !protocol.Is(errs[j], protocol.Conflict) {
				t.Fatalf("round %d: %v", i, errs[j])
			}
		}
		if ok != 1 {
			t.Fatalf("round %d: %d writes succeeded", i, ok)
		}
	}
}

// R4.S2: a conflict adds no event.
func TestConflictLeavesNoTrace(t *testing.T) {
	e := newEnv(t)
	s := e.open(Options{})
	v1 := put(t, s, "specs", "1a2b3c4d5e6f:x", 1, nil, nil).Version
	v2 := put(t, s, "specs", "1a2b3c4d5e6f:x", 2, nil, &v1).Version
	wf := "spec-" + e.deploy + "-1a2b3c4d5e6f-x"
	before := len(historyOf(t, raw(t), wf, ""))
	_, err := s.Put(ctx(t), protocol.StatePutParams{Collection: "specs", Key: "1a2b3c4d5e6f:x", Value: []byte("3"), ExpectedVersion: &v1})
	if !protocol.Is(err, protocol.Conflict) {
		t.Fatalf("got %v", err)
	}
	var pe *protocol.Error
	if !errors.As(err, &pe) || pe.Data == nil || pe.Data.Version == nil || *pe.Data.Version != v2 {
		t.Fatalf("the conflict does not give the stored version: %+v", pe)
	}
	if after := len(historyOf(t, raw(t), wf, "")); after != before {
		t.Fatalf("the history grew from %d to %d events", before, after)
	}
	got, _ := s.Get(ctx(t), protocol.StateGetParams{Collection: "specs", Key: "1a2b3c4d5e6f:x"})
	if got.Version != v2 {
		t.Fatalf("the version is %s, want %s", got.Version, v2)
	}
}

// R4.S3 and R9.S4: a workflow that ends, a read after the end, a new run that
// links to the chain, and a version that is never used again.
func TestEndAndStartAgain(t *testing.T) {
	e := newEnv(t)
	s := e.open(Options{Idle: 2 * time.Second})
	stream := "spec:1a2b3c4d5e6f:ends"
	seen := map[string]bool{}
	v := put(t, s, "specs", "1a2b3c4d5e6f:ends", 1, nil, nil).Version
	seen[v] = true
	appendEvents(t, s, stream, "A", "B")
	if _, err := s.Delete(ctx(t), protocol.StateDeleteParams{Collection: "specs", Key: "1a2b3c4d5e6f:ends", ExpectedVersion: v}); err != nil {
		t.Fatal(err)
	}
	wf := "spec-" + e.deploy + "-1a2b3c4d5e6f-ends"
	var firstRun string
	waitFor(t, "the workflow ends", time.Minute, func() bool {
		run, running, err := s.running(ctx(t), s.router.Stream(stream))
		firstRun = run
		return err == nil && !running
	})
	if _, err := os.Stat(filepath.Join(e.blobDir, filepath.FromSlash(blobstore.ExportName(wf, firstRun)))); err != nil {
		t.Fatalf("no export of the last run: %v", err)
	}
	if got := readTypes(t, s, protocol.EventsReadParams{Stream: stream}); !eq(got, []string{"A", "B"}) {
		t.Fatalf("read after the end: %v", got)
	}
	v2 := put(t, s, "specs", "1a2b3c4d5e6f:ends", 2, nil, nil).Version
	if seen[v2] {
		t.Fatalf("the version %s is used again", v2)
	}
	var info workflows.Info
	if err := s.query(ctx(t), s.router.Stream(stream), workflows.QueryInfo, &info); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, r := range info.Chain.Runs {
		found = found || r.RunID == firstRun
	}
	if !found || info.RunID == firstRun {
		t.Fatalf("the new run does not link to %s: %+v", firstRun, info.Chain.Runs)
	}
	appendEvents(t, s, stream, "C")
	if got := readTypes(t, s, protocol.EventsReadParams{Stream: stream}); !eq(got, []string{"A", "B", "C"}) {
		t.Fatalf("read across the end: %v", got)
	}
}

// R5.S1 and R5.S2
func TestLists(t *testing.T) {
	s := newEnv(t).open(Options{})
	for i, k := range []string{"a", "b", "c"} {
		put(t, s, "specs", "1a2b3c4d5e6f:"+k, i, map[string]any{"pendingForMac": k != "b"}, nil)
	}
	put(t, s, "specs", "1a2b3c4d5e6f:d", 3, map[string]any{"pendingForMac": true}, nil)
	res, err := s.List(ctx(t), protocol.StateListParams{Collection: "specs", Filter: protocol.Attributes{"pendingForMac": true}})
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	for _, it := range res.Items {
		keys = append(keys, it.Key)
	}
	if !eq(keys, []string{"1a2b3c4d5e6f:a", "1a2b3c4d5e6f:c", "1a2b3c4d5e6f:d"}) {
		t.Fatalf("list: %v", keys)
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, 16)
	for i := range 250 {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			put(t, s, "notes", "k"+string(rune('a'+i/26))+string(rune('a'+i%26))+"-"+time.Now().Format("150405.000000"), i, nil, nil)
		}(i)
	}
	wg.Wait()
	got := map[string]bool{}
	var cursor *string
	pages := 0
	var last string
	for {
		res, err := s.List(ctx(t), protocol.StateListParams{Collection: "notes", PageSize: 100, Cursor: cursor})
		if err != nil {
			t.Fatal(err)
		}
		pages++
		for _, it := range res.Items {
			if got[it.Key] || it.Key <= last {
				t.Fatalf("key %s twice or out of order", it.Key)
			}
			got[it.Key], last = true, it.Key
		}
		if res.Cursor == nil {
			break
		}
		cursor = res.Cursor
	}
	if len(got) != 250 || pages != 3 {
		t.Fatalf("%d keys in %d pages", len(got), pages)
	}
}

// R6.S2: a late event with its own time.
func TestLateEvent(t *testing.T) {
	s := newEnv(t).open(Options{})
	at := time.Date(2026, 11, 27, 10, 0, 0, 0, time.UTC)
	if _, err := s.Append(ctx(t), protocol.EventsAppendParams{Stream: "spec:1a2b3c4d5e6f:late", Events: []protocol.NewEvent{{Type: "offline", Data: []byte("{}"), At: &at}}}); err != nil {
		t.Fatal(err)
	}
	appendEvents(t, s, "spec:1a2b3c4d5e6f:late", "now")
	from, to := at.Add(-time.Minute), at.Add(time.Minute)
	res, err := s.Read(ctx(t), protocol.EventsReadParams{Stream: "spec:1a2b3c4d5e6f:late", From: &from, To: &to})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Events) != 1 || res.Events[0].Type != "offline" || !res.Events[0].At.Equal(at) {
		t.Fatalf("range read: %+v", res.Events)
	}
}

// R7.S1: a wait ends at a write from another process.
func TestWaitAcrossProcesses(t *testing.T) {
	e := newEnv(t)
	a, b := e.open(Options{}), e.open(Options{})
	v := put(t, a, "specs", "1a2b3c4d5e6f:w", 1, nil, nil).Version
	done := make(chan protocol.WatchWaitResult, 1)
	start := time.Now()
	go func() {
		c, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		res, err := a.Wait(c, protocol.WatchWaitParams{Collection: "specs", Key: "1a2b3c4d5e6f:w", Version: &v, TimeoutMs: 30000})
		if err != nil {
			t.Error(err)
		}
		done <- res
	}()
	time.Sleep(500 * time.Millisecond)
	v2 := put(t, b, "specs", "1a2b3c4d5e6f:w", 2, nil, &v).Version
	res := <-done
	if !res.Changed || res.Version == nil || *res.Version != v2 || time.Since(start) > 5*time.Second {
		t.Fatalf("wait: %+v after %v", res, time.Since(start))
	}
}

// R9.S1 and R9.S2: reads across a forced continue-as-new, and after the
// old run is gone from Temporal (as the namespace's retention does).
func TestReadAfterRetention(t *testing.T) {
	e := newEnv(t)
	s := e.open(Options{})
	stream := "spec:1a2b3c4d5e6f:ret"
	cur := appendEvents(t, s, stream, "A", "B")
	wf := "spec-" + e.deploy + "-1a2b3c4d5e6f-ret"
	firstRun, _, _ := s.running(ctx(t), s.router.Stream(stream))
	if _, err := s.ContinueStream(ctx(t), protocol.ContinueStreamParams{Stream: stream}); err != nil {
		t.Fatal(err)
	}
	appendEvents(t, s, stream, "C")
	if got := readTypes(t, s, protocol.EventsReadParams{Stream: stream, After: &cur[1]}); !eq(got, []string{"C"}) {
		t.Fatalf("read after B: %v", got)
	}
	_, err := raw(t).WorkflowService().DeleteWorkflowExecution(ctx(t), &workflowservice.DeleteWorkflowExecutionRequest{
		Namespace: "default", WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: wf, RunId: firstRun},
	})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "Temporal removes the first run", time.Minute, func() bool {
		_, err := raw(t).DescribeWorkflowExecution(ctx(t), wf, firstRun)
		return isNotFound(err)
	})
	if got := readTypes(t, s, protocol.EventsReadParams{Stream: stream}); !eq(got, []string{"A", "B", "C"}) {
		t.Fatalf("read from the start: %v", got)
	}
	if got := readTypes(t, s, protocol.EventsReadParams{Stream: stream, After: &cur[0]}); !eq(got, []string{"B", "C"}) {
		t.Fatalf("read after A: %v", got)
	}
}

// R9.S3: a continue-as-new from the run limit.
func TestRunLimit(t *testing.T) {
	s := newEnv(t).open(Options{RunLimit: 5})
	stream := "spec:1a2b3c4d5e6f:many"
	var want []string
	for i := range 12 {
		ty := "e" + string(rune('a'+i))
		appendEvents(t, s, stream, ty)
		want = append(want, ty)
	}
	var info workflows.Info
	if err := s.query(ctx(t), s.router.Stream(stream), workflows.QueryInfo, &info); err != nil {
		t.Fatal(err)
	}
	if len(info.Chain.Runs) < 2 {
		t.Fatalf("%d ended runs, want 2 or more", len(info.Chain.Runs))
	}
	if got := readTypes(t, s, protocol.EventsReadParams{Stream: stream}); !eq(got, want) {
		t.Fatalf("read: %v", got)
	}
	// A small limit pages across the runs.
	var all []string
	var after *string
	for {
		res, err := s.Read(ctx(t), protocol.EventsReadParams{Stream: stream, After: after, Limit: 5})
		if err != nil {
			t.Fatal(err)
		}
		for _, ev := range res.Events {
			all = append(all, ev.Type)
		}
		if !res.More {
			break
		}
		after = res.Cursor
	}
	if !eq(all, want) {
		t.Fatalf("paged read: %v", all)
	}
}

// R9.S5: delete a stream with an exported run.
func TestDeleteStream(t *testing.T) {
	e := newEnv(t)
	s := e.open(Options{})
	stream := "spec:1a2b3c4d5e6f:del"
	appendEvents(t, s, stream, "A")
	wf := "spec-" + e.deploy + "-1a2b3c4d5e6f-del"
	firstRun, _, _ := s.running(ctx(t), s.router.Stream(stream))
	if _, err := s.ContinueStream(ctx(t), protocol.ContinueStreamParams{Stream: stream}); err != nil {
		t.Fatal(err)
	}
	appendEvents(t, s, stream, "B")
	export := filepath.Join(e.blobDir, filepath.FromSlash(blobstore.ExportName(wf, firstRun)))
	if _, err := os.Stat(export); err != nil {
		t.Fatalf("no export: %v", err)
	}
	res, err := s.DeleteStream(ctx(t), protocol.EventsDeleteParams{Stream: stream})
	if err != nil || !res.Deleted {
		t.Fatalf("delete: %+v %v", res, err)
	}
	if got := readTypes(t, s, protocol.EventsReadParams{Stream: stream}); len(got) != 0 {
		t.Fatalf("read after delete: %v", got)
	}
	if _, err := os.Stat(export); !os.IsNotExist(err) {
		t.Fatalf("the export is still there: %v", err)
	}
	cur := appendEvents(t, s, stream, "C")
	if got := readTypes(t, s, protocol.EventsReadParams{Stream: stream}); !eq(got, []string{"C"}) || cur[0] == "" {
		t.Fatalf("read after a new append: %v", got)
	}
}

var _ = enumspb.EVENT_TYPE_UNSPECIFIED
