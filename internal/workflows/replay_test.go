package workflows

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

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

// R11.S1: every stored history replays with the current code. A change that
// gives other commands for a stored history fails here and names the file.
// See scripts/capture-histories.sh.
func TestReplay(t *testing.T) {
	r := replayer(t, Entity)
	for _, f := range histories(t) {
		if err := r.ReplayWorkflowHistoryFromJSONFile(nil, f); err != nil {
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
	if err := r.ReplayWorkflowHistoryFromJSONFile(nil, "testdata/histories/entity-run1.json"); err == nil {
		t.Fatal("a non-deterministic change replays")
	}
}
