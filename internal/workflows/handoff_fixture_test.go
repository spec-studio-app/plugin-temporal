package workflows

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/spec-studio-app/plugin-temporal/internal/codec"
	"github.com/spec-studio-app/plugin-temporal/internal/settings"
)

// Fixture is testdata/relay/fixtures/expected.json, written by
// scripts/handoff-fixtures.sh from the relay's own Temporal store.
type Fixture struct {
	Keys      string `json:"keys"`
	DeployID  string `json:"deployId"`
	Workflows []struct {
		Name       string       `json:"name"`
		WorkflowID string       `json:"workflowId"`
		TaskQueue  string       `json:"taskQueue"`
		Scope      string       `json:"scope"`
		RelayRunID string       `json:"relayRunId"`
		Documents  []HandoffDoc `json:"documents"`
	} `json:"workflows"`
}

func jsonEqual(t *testing.T, a, b any) bool {
	t.Helper()
	norm := func(v any) any {
		var raw []byte
		switch x := v.(type) {
		case json.RawMessage:
			raw = x
		default:
			raw, _ = json.Marshal(x)
		}
		var out any
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	return reflect.DeepEqual(norm(a), norm(b))
}

// R10.S1 and R10.S2 (offline): the relay's run continued as new into the
// plugin's workflow type; the plugin decrypts its input with the relay's
// keys, and its documents equal the state that the relay's query gave.
func TestHandoffFixtures(t *testing.T) {
	dir := "../../testdata/relay/fixtures"
	raw, err := os.ReadFile(filepath.Join(dir, "expected.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fx Fixture
	if err := json.Unmarshal(raw, &fx); err != nil {
		t.Fatal(err)
	}
	keys, err := settings.ParsePayloadKeys(fx.Keys)
	if err != nil {
		t.Fatal(err)
	}
	if len(fx.Workflows) != 3 {
		t.Fatalf("%d workflows in the fixture", len(fx.Workflows))
	}
	for _, w := range fx.Workflows {
		t.Run(w.Name, func(t *testing.T) {
			c, _ := codec.Data(keys)
			if w.Scope == "config" {
				c, _ = codec.Config(keys)
			}
			dc := codec.Converter(c)
			f, err := os.ReadFile(filepath.Join(dir, w.Name+"-relay-run.json"))
			if err != nil {
				t.Fatal(err)
			}
			h, err := client.HistoryFromJSON(bytes.NewReader(f), client.HistoryJSONOptions{})
			if err != nil {
				t.Fatal(err)
			}
			last := h.Events[len(h.Events)-1]
			if last.GetEventType() != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW {
				t.Fatalf("the relay run ends with %s", last.GetEventType())
			}
			can := last.GetWorkflowExecutionContinuedAsNewEventAttributes()
			if can.GetWorkflowType().GetName() != EntityWorkflow || can.GetTaskQueue().GetName() != w.TaskQueue {
				t.Fatalf("the relay run continues as %s on %s", can.GetWorkflowType().GetName(), can.GetTaskQueue().GetName())
			}
			var in EntityInput
			if err := dc.FromPayloads(can.GetInput(), &in); err != nil {
				t.Fatalf("the plugin cannot read the handoff input: %v", err)
			}
			if in.Handoff == nil || in.Handoff.Version != 1 || in.Handoff.WorkflowID != w.WorkflowID {
				t.Fatalf("handoff: %+v", in.Handoff)
			}

			var s testsuite.WorkflowTestSuite
			env := s.NewTestWorkflowEnvironment()
			env.RegisterWorkflowWithOptions(Entity, workflow.RegisterOptions{Name: EntityWorkflow})
			env.RegisterActivityWithOptions(func(ctx context.Context, in CatalogPutInput) error { return nil }, activity.RegisterOptions{Name: ActivityCatalogPut})
			env.RegisterActivityWithOptions(func(ctx context.Context, in ExportInput) (string, error) { return "", nil }, activity.RegisterOptions{Name: ActivityExportRun})
			env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: w.WorkflowID})
			got := map[string]*Doc{}
			env.RegisterDelayedCallback(func() {
				for _, d := range w.Documents {
					v, err := env.QueryWorkflow(QueryDoc, d.Collection, d.Key)
					if err != nil {
						t.Error(err)
						continue
					}
					var doc *Doc
					_ = v.Get(&doc)
					got[d.Collection+"/"+d.Key] = doc
				}
				env.CancelWorkflow()
			}, time.Second)
			env.ExecuteWorkflow(EntityWorkflow, in)
			for _, d := range w.Documents {
				doc := got[d.Collection+"/"+d.Key]
				if doc == nil {
					t.Fatalf("no document %s/%s", d.Collection, d.Key)
				}
				if !jsonEqual(t, doc.Value, d.Value) {
					t.Fatalf("%s/%s: the value differs from the relay's state", d.Collection, d.Key)
				}
				if !jsonEqual(t, doc.Attributes, d.Attributes) {
					t.Fatalf("%s/%s: attributes %v, want %v", d.Collection, d.Key, doc.Attributes, d.Attributes)
				}
			}
		})
	}
}
