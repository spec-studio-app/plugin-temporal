package workflows

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/spec-studio-app/plugin-temporal/internal/codec"
	"github.com/spec-studio-app/plugin-temporal/internal/settings"
)

func replayer(t *testing.T, entity any) worker.WorkflowReplayer {
	t.Helper()
	raw, err := os.ReadFile("testdata/replay-keys.txt")
	if err != nil {
		t.Fatal(err)
	}
	keys, err := settings.ParsePayloadKeys(strings.TrimSpace(string(raw)))
	if err != nil {
		t.Fatal(err)
	}
	c, err := codec.Data(keys)
	if err != nil {
		t.Fatal(err)
	}
	r, err := worker.NewWorkflowReplayerWithOptions(worker.WorkflowReplayerOptions{DataConverter: codec.Converter(c)})
	if err != nil {
		t.Fatal(err)
	}
	r.RegisterWorkflowWithOptions(entity, workflow.RegisterOptions{Name: EntityWorkflow})
	r.RegisterWorkflowWithOptions(Catalog, workflow.RegisterOptions{Name: CatalogWorkflow})
	return r
}

func histories(t *testing.T) []string {
	files, err := filepath.Glob("testdata/histories/*.json")
	if err != nil || len(files) == 0 {
		t.Fatalf("no histories: %v", err)
	}
	return files
}

// replay replays one history file with its workflow ID (<file>.id).
func replay(t *testing.T, r worker.WorkflowReplayer, file string) error {
	t.Helper()
	raw, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	h, err := client.HistoryFromJSON(bytes.NewReader(raw), client.HistoryJSONOptions{})
	if err != nil {
		t.Fatal(err)
	}
	id, err := os.ReadFile(strings.TrimSuffix(file, ".json") + ".id")
	if err != nil {
		t.Fatalf("%s has no workflow ID file: %v", file, err)
	}
	return r.ReplayWorkflowHistoryWithOptions(nil, h, worker.ReplayWorkflowHistoryOptions{
		OriginalExecution: workflow.Execution{ID: strings.TrimSpace(string(id)), RunID: "replay-run"},
	})
}

// R11.S1: every stored history replays with the current code. A change that
// gives other commands for a stored history fails here and names the file.
// See scripts/capture-histories.sh.
func TestReplay(t *testing.T) {
	r := replayer(t, Entity)
	for _, f := range histories(t) {
		if err := replay(t, r, f); err != nil {
			t.Errorf("%s does not replay: %v", f, err)
		}
	}
}

// brokenEntity gives other commands: a timer before anything else.
func brokenEntity(ctx workflow.Context, in EntityInput) error {
	if err := workflow.Sleep(ctx, 1); err != nil {
		return err
	}
	return Entity(ctx, in)
}

// The replay test catches a change that breaks determinism.
func TestReplayCatchesBreak(t *testing.T) {
	r := replayer(t, brokenEntity)
	files, _ := filepath.Glob("testdata/histories/*-entity-run1.json")
	if len(files) == 0 {
		t.Fatal("no entity history")
	}
	for _, f := range files {
		if err := replay(t, r, f); err == nil {
			t.Fatalf("a non-deterministic change replays %s", f)
		}
	}
}

// The stored histories are in a public repo: they hold no computer name.
// Each one is as ScrubHistory leaves it.
func TestHistoriesAreScrubbed(t *testing.T) {
	for _, f := range histories(t) {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(ScrubHistory(raw), raw) {
			t.Errorf("%s has a worker identity or a sticky task queue that is not %q: run it through ScrubHistory", f, HistoryWorker)
		}
	}
}
