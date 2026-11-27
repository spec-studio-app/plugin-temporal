//go:build integration

package store

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"

	"github.com/spec-studio-app/plugin-sdk/go/plugin"
	"github.com/spec-studio-app/plugin-sdk/go/protocol"
)

// The integration tests need a Temporal dev server:
//
//	temporal server start-dev --headless
//	TEST_TEMPORAL_ADDRESS=127.0.0.1:7233 go test -tags integration ./...
func address(t *testing.T) string {
	t.Helper()
	a := os.Getenv("TEST_TEMPORAL_ADDRESS")
	if a == "" {
		t.Skip("TEST_TEMPORAL_ADDRESS is not set")
	}
	return a
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func randomKey() string {
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	return base64.StdEncoding.EncodeToString(b)
}

// env is one test's settings: a deploy ID of its own, so the tests never share a workflow.
type env struct {
	t        *testing.T
	settings map[string]string
	deploy   string
	blobDir  string
}

func newEnv(t *testing.T) *env {
	t.Helper()
	deploy := "t" + randomHex(8)
	dir := t.TempDir()
	return &env{t: t, deploy: deploy, blobDir: dir, settings: map[string]string{
		"address":      address(t),
		"payload_keys": "data:d" + randomHex(3) + ":" + randomKey() + ",config:c" + randomHex(3) + ":" + randomKey(),
		"deploy_id":    deploy,
		"blob_dir":     dir,
	}}
}

// open opens a store with the env's settings.
func (e *env) open(o Options) *Store {
	e.t.Helper()
	s := New(func() Options { return o })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := s.Open(ctx, plugin.OpenOptions{Settings: e.settings, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}); err != nil {
		e.t.Fatalf("open: %v", err)
	}
	e.t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		_ = s.Close(ctx)
	})
	return s
}

// raw is a Temporal client with no codec: it sees the payloads as Temporal keeps them.
func raw(t *testing.T) client.Client {
	t.Helper()
	c, err := client.Dial(client.Options{HostPort: address(t), Namespace: "default"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

func historyOf(t *testing.T, c client.Client, wfID, runID string) []*historypb.HistoryEvent {
	t.Helper()
	it := c.GetWorkflowHistory(context.Background(), wfID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	var out []*historypb.HistoryEvent
	for it.HasNext() {
		ev, err := it.Next()
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, ev)
	}
	return out
}

func ctxWithTimeout(d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), d)
}

func discard() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	return c
}

func put(t *testing.T, s *Store, coll, key string, value any, attrs map[string]any, expected *string) protocol.StatePutResult {
	t.Helper()
	b, _ := json.Marshal(value)
	res, err := s.Put(ctx(t), protocol.StatePutParams{Collection: coll, Key: key, Value: b, Attributes: attrs, ExpectedVersion: expected})
	if err != nil {
		t.Fatalf("put %s/%s: %v", coll, key, err)
	}
	return res
}

func appendEvents(t *testing.T, s *Store, stream string, types ...string) []string {
	t.Helper()
	evs := make([]protocol.NewEvent, len(types))
	for i, ty := range types {
		evs[i] = protocol.NewEvent{Type: ty, Data: json.RawMessage(`{"n":"` + ty + `"}`)}
	}
	res, err := s.Append(ctx(t), protocol.EventsAppendParams{Stream: stream, Events: evs})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	return res.Cursors
}

func readTypes(t *testing.T, s *Store, p protocol.EventsReadParams) []string {
	t.Helper()
	res, err := s.Read(ctx(t), p)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var out []string
	for _, e := range res.Events {
		out = append(out, e.Type)
	}
	return out
}

func eq(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// waitFor polls until cond is true.
func waitFor(t *testing.T, what string, d time.Duration, cond func() bool) {
	t.Helper()
	end := time.Now().Add(d)
	for time.Now().Before(end) {
		if cond() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out: %s", what)
}
