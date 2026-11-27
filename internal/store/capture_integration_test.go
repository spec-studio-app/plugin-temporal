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
	keys, err := os.ReadFile("../workflows/testdata/replay-keys.txt")
	if err != nil {
		t.Fatal(err)
	}
	e := newEnv(t)
	e.settings["payload_keys"] = strings.TrimSpace(string(keys))
	e.settings["deploy_id"] = "replay"
	s := e.open(Options{})
	key, stream := "1a2b3c4d5e6f:replay-"+randomHex(3), ""
	stream = "spec:" + key

	v1 := put(t, s, "specs", key, map[string]any{"n": 1}, map[string]any{"open": true}, nil).Version
	stale := "stale"
	if _, err := s.Put(ctx(t), protocol.StatePutParams{Collection: "specs", Key: key, Value: []byte("2"), ExpectedVersion: &stale}); !protocol.Is(err, protocol.Conflict) {
		t.Fatalf("stale put: %v", err)
	}
	v2 := put(t, s, "specs", key, map[string]any{"n": 2}, map[string]any{"open": false}, &v1).Version
	put(t, s, "app-specs", key, map[string]any{"stage": "build"}, nil, nil)
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

	wf := "spec-replay-" + strings.ReplaceAll(key, ":", "-")
	rc := raw(t)
	save(t, "entity-run1.json", historyOf(t, rc, wf, firstRun))
	save(t, "entity-run2.json", historyOf(t, rc, wf, ""))
	save(t, "catalog-specs.json", historyOf(t, rc, "catalog/replay/specs", ""))
}

func save(t *testing.T, name string, events []*historypb.HistoryEvent) {
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
}
