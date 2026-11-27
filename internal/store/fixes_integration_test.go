//go:build integration

package store

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"

	"github.com/spec-studio-app/plugin-sdk/go/protocol"

	"github.com/spec-studio-app/plugin-temporal/internal/workflows"
)

// Large state goes to the blob store encrypted, in parts, and the next run
// has every document; reads never see a missing document while it loads.
func TestLargeStateSpill(t *testing.T) {
	e := newEnv(t)
	s := e.open(Options{RunLimit: 4, InlineBytes: 1})
	key := "1a2b3c4d5e6f:big"
	marker := "PLAIN-STATE-" + randomHex(6)
	big := strings.Repeat(marker, 40000) // about 700 KB
	v := put(t, s, "specs", key, big, nil, nil).Version
	put(t, s, "app-specs", key, big, nil, nil)
	put(t, s, "closed-specs", key, big, nil, nil)
	// Updates past the run limit: the run continues as new with a state object.
	for i := range 3 {
		got, err := s.Get(ctx(t), protocol.StateGetParams{Collection: "specs", Key: key})
		if err != nil || !got.Exists || got.Version != v {
			t.Fatalf("get %d: exists %v version %s err %v", i, got.Exists, got.Version, err)
		}
		appendEvents(t, s, "spec:"+key, "e")
	}
	var info workflows.Info
	if err := s.query(ctx(t), s.router.Doc("specs", key), workflows.QueryInfo, &info); err != nil {
		t.Fatal(err)
	}
	if len(info.Chain.Runs) == 0 || info.Docs != 3 {
		t.Fatalf("runs %d docs %d", len(info.Chain.Runs), info.Docs)
	}
	for _, c := range []string{"specs", "app-specs", "closed-specs"} {
		got, err := s.Get(ctx(t), protocol.StateGetParams{Collection: c, Key: key})
		var text string
		_ = json.Unmarshal(got.Value, &text)
		if err != nil || text != big {
			t.Fatalf("%s after the spill: %v", c, err)
		}
	}
	files, _ := filepath.Glob(filepath.Join(e.blobDir, "state", "*", "*.bin"))
	if len(files) == 0 {
		t.Fatal("no state object")
	}
	for _, f := range files {
		b, _ := os.ReadFile(f)
		if bytes.Contains(b, []byte(marker)) {
			t.Fatalf("the state object %s is not encrypted", f)
		}
	}
}

// A catalog whose entries are too large for its input spills them and keeps every key.
func TestCatalogSpill(t *testing.T) {
	e := newEnv(t)
	s := e.open(Options{CatalogRunLimit: 3, InlineBytes: 1})
	for i := range 8 {
		put(t, s, "notes", "n"+string(rune('a'+i)), i, map[string]any{"even": i%2 == 0}, nil)
	}
	res, err := s.List(ctx(t), protocol.StateListParams{Collection: "notes", Filter: protocol.Attributes{"even": true}})
	if err != nil || len(res.Items) != 4 {
		t.Fatalf("list after the spill: %d items, %v", len(res.Items), err)
	}
	files, _ := filepath.Glob(filepath.Join(e.blobDir, "catalog", "*", "*.bin"))
	if len(files) == 0 {
		t.Fatal("no catalog object")
	}
}

// A spec whose relay workflow closed before the handoff has no document.
func TestClosedRelayRun(t *testing.T) {
	e := newEnv(t)
	s := e.open(Options{})
	wf := "spec-" + e.deploy + "-1a2b3c4d5e6f-old"
	run, err := raw(t).ExecuteWorkflow(ctx(t), client.StartWorkflowOptions{ID: wf, TaskQueue: "relay-" + e.deploy}, "specWorkflow")
	if err != nil {
		t.Fatal(err)
	}
	if err := raw(t).TerminateWorkflow(ctx(t), wf, run.GetRunID(), "closed relay run"); err != nil {
		t.Fatal(err)
	}
	d, _ := raw(t).DescribeWorkflowExecution(ctx(t), wf, "")
	if d.GetWorkflowExecutionInfo().GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		t.Fatal("the relay run still runs")
	}
	got, err := s.Get(ctx(t), protocol.StateGetParams{Collection: "closed-specs", Key: "1a2b3c4d5e6f:old"})
	if err != nil || got.Exists {
		t.Fatalf("get: %+v %v", got, err)
	}
	put(t, s, "closed-specs", "1a2b3c4d5e6f:old", "memo", nil, nil)
}

// A wait with no time left still sees a change that happened already.
func TestWaitZeroTimeout(t *testing.T) {
	s := newEnv(t).open(Options{})
	v := put(t, s, "notes", "w0", 1, nil, nil).Version
	v2 := put(t, s, "notes", "w0", 2, nil, &v).Version
	c, cancel := ctxWithTimeout(0)
	defer cancel()
	res, err := s.Wait(c, protocol.WatchWaitParams{Collection: "notes", Key: "w0", Version: &v, TimeoutMs: 0})
	if err != nil || !res.Changed || res.Version == nil || *res.Version != v2 {
		t.Fatalf("document wait: %+v %v", res, err)
	}
	cur := appendEvents(t, s, "other-stream", "a", "b")
	c2, cancel2 := ctxWithTimeout(0)
	defer cancel2()
	res, err = s.Wait(c2, protocol.WatchWaitParams{Stream: "other-stream", After: &cur[0], TimeoutMs: 0})
	if err != nil || !res.Changed || res.Cursor == nil || *res.Cursor != cur[1] {
		t.Fatalf("stream wait: %+v %v", res, err)
	}
	time.Sleep(0)
}
