//go:build integration

package store

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/temporalproto"

	"github.com/spec-studio-app/plugin-sdk/go/protocol"
)

// TestCaptureHistories writes the histories of the replay tests to
// internal/workflows/testdata/histories. It runs only with
// CAPTURE_HISTORIES=1 (scripts/capture-histories.sh): a new history is a
// decision, made on purpose, when a workflow changes its shape with a patch.
func TestCaptureHistories(t *testing.T) {
	if os.Getenv("CAPTURE_HISTORIES") != "1" {
		t.Skip("CAPTURE_HISTORIES is not 1")
	}
	// The name of the workflow shape, for example v2. The files of earlier
	// shapes stay: they must still replay.
	prefix := os.Getenv("CAPTURE_NAME")
	if prefix == "" {
		t.Fatal("CAPTURE_NAME is not set (for example v2)")
	}
	for _, n := range []string{"entity-run1", "entity-run2", "catalog-specs", "entity-commit-remote"} {
		if _, err := os.Stat(filepath.Join("../workflows/testdata/histories", prefix+"-"+n+".json")); err == nil {
			t.Fatalf("the histories %s-* exist already: choose a new CAPTURE_NAME", prefix)
		}
	}
	keys, err := os.ReadFile("../workflows/testdata/replay-keys.txt")
	if err != nil {
		t.Fatal(err)
	}
	e := newEnv(t)
	e.settings["payload_keys"] = strings.TrimSpace(string(keys))
	// A deploy ID of its own: workflows of an earlier capture must not take part.
	deploy := "replay" + randomHex(4)
	e.settings["deploy_id"] = deploy
	s := e.open(Options{})
	key, stream := "1a2b3c4d5e6f:replay", ""
	stream = "spec:" + key

	v1 := put(t, s, "specs", key, map[string]any{"n": 1}, map[string]any{"open": true}, nil).Version
	stale := "stale"
	if _, err := s.Put(ctx(t), protocol.StatePutParams{Collection: "specs", Key: key, Value: []byte("2"), ExpectedVersion: &stale}); !protocol.Is(err, protocol.Conflict) {
		t.Fatalf("stale put: %v", err)
	}
	v2 := put(t, s, "specs", key, map[string]any{"n": 2}, map[string]any{"open": false}, &v1).Version
	// state.commit: a spec's document and its activity in one update; a
	// stale commit; a commit with no events.
	c1, err := s.Commit(ctx(t), protocol.StateCommitParams{Collection: "app-specs", Key: key, Value: []byte(`{"stage":"plan"}`), Stream: stream, Events: []protocol.NewEvent{{Type: "activity", Data: []byte(`{"kind":"stage"}`)}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Commit(ctx(t), protocol.StateCommitParams{Collection: "app-specs", Key: key, Value: []byte(`{}`), ExpectedVersion: &stale, Stream: stream, Events: []protocol.NewEvent{{Type: "activity", Data: []byte(`{}`)}}}); !protocol.Is(err, protocol.Conflict) {
		t.Fatalf("stale commit: %v", err)
	}
	if _, err := s.Commit(ctx(t), protocol.StateCommitParams{Collection: "app-specs", Key: key, Value: []byte(`{"stage":"build"}`), ExpectedVersion: &c1.Version, Stream: stream, Events: []protocol.NewEvent{}}); err != nil {
		t.Fatal(err)
	}
	// A commit of a document and a stream with two workflows (Deliver).
	if _, err := s.Commit(ctx(t), protocol.StateCommitParams{Collection: "notes", Key: "a", Value: []byte(`1`), Stream: "notes-log", Events: []protocol.NewEvent{{Type: "note", Data: []byte(`{}`)}}}); err != nil {
		t.Fatal(err)
	}
	cur := appendEvents(t, s, stream, "sync", "message")
	c, cancel := ctxWithTimeout(700 * time.Millisecond)
	_, _ = s.Wait(c, protocol.WatchWaitParams{Stream: stream, After: &cur[1], TimeoutMs: 700})
	cancel()
	if _, err := s.Wait(ctx(t), protocol.WatchWaitParams{Collection: "specs", Key: key, Version: &v1, TimeoutMs: 1000}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Delete(ctx(t), protocol.StateDeleteParams{Collection: "specs", Key: key, ExpectedVersion: v2}); err != nil {
		t.Fatal(err)
	}
	firstRun, _, _ := s.running(ctx(t), s.router.Stream(stream))
	if _, err := s.ContinueStream(ctx(t), protocol.ContinueStreamParams{Stream: stream}); err != nil {
		t.Fatal(err)
	}
	appendEvents(t, s, stream, "after")
	if _, err := s.DeleteStream(ctx(t), protocol.EventsDeleteParams{Stream: stream}); err != nil {
		t.Fatal(err)
	}

	wf := "spec-" + deploy + "-" + strings.ReplaceAll(key, ":", "-")
	rc := raw(t)
	save(t, prefix+"-entity-run1.json", wf, historyOf(t, rc, wf, firstRun))
	save(t, prefix+"-entity-run2.json", wf, historyOf(t, rc, wf, ""))
	save(t, prefix+"-catalog-specs.json", "catalog/"+deploy+"/specs", historyOf(t, rc, "catalog/"+deploy+"/specs", ""))
	notes := s.router.Doc("notes", "a").WorkflowID
	save(t, prefix+"-entity-commit-remote.json", notes, historyOf(t, rc, notes, ""))
}

// save writes a history, and its workflow ID next to it (<name>.id): the
// workflows check their input against their workflow ID, so the replay needs it.
func save(t *testing.T, name, workflowID string, events []*historypb.HistoryEvent) {
	t.Helper()
	b, err := temporalproto.CustomJSONMarshalOptions{Indent: " "}.Marshal(&historypb.History{Events: events})
	if err != nil {
		t.Fatal(err)
	}
	var check map[string]any
	if err := json.Unmarshal(b, &check); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join("../workflows/testdata/histories", name), b, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join("../workflows/testdata/histories", strings.TrimSuffix(name, ".json")+".id"), []byte(workflowID+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}
