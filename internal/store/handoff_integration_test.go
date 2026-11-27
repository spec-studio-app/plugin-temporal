//go:build integration

package store

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"

	"github.com/spec-studio-app/plugin-sdk/go/protocol"

	"github.com/spec-studio-app/plugin-temporal/internal/blobstore"
	"github.com/spec-studio-app/plugin-temporal/internal/route"
	"github.com/spec-studio-app/plugin-temporal/internal/workflows"
)

type handoffFixture struct {
	Keys      string `json:"keys"`
	DeployID  string `json:"deployId"`
	Workflows []struct {
		Name       string                 `json:"name"`
		WorkflowID string                 `json:"workflowId"`
		Scope      string                 `json:"scope"`
		RelayRunID string                 `json:"relayRunId"`
		Documents  []workflows.HandoffDoc `json:"documents"`
	} `json:"workflows"`
}

func sameJSON(a, b any) bool {
	n := func(v any) any {
		raw, ok := v.(json.RawMessage)
		if !ok {
			raw, _ = json.Marshal(v)
		}
		var out any
		_ = json.Unmarshal(raw, &out)
		return out
	}
	return reflect.DeepEqual(n(a), n(b))
}

// R10.S1, R10.S2 and R10.S3 end to end: scripts/handoff-fixtures.sh ran the
// relay's Temporal store and handed its workflows off into the plugin's type;
// this plugin's worker takes the new runs. HANDOFF_E2E_DIR is the script's
// out dir.
func TestHandoffEndToEnd(t *testing.T) {
	dir := os.Getenv("HANDOFF_E2E_DIR")
	if dir == "" {
		t.Skip("HANDOFF_E2E_DIR is not set")
	}
	fxRaw, err := os.ReadFile(filepath.Join(dir, "expected.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fx handoffFixture
	if err := json.Unmarshal(fxRaw, &fx); err != nil {
		t.Fatal(err)
	}
	e := newEnv(t)
	e.settings["payload_keys"] = fx.Keys
	e.settings["deploy_id"] = fx.DeployID
	s := e.open(Options{})

	for _, w := range fx.Workflows {
		for _, d := range w.Documents {
			var got protocol.StateGetResult
			waitFor(t, "the plugin's run of "+w.WorkflowID+" has "+d.Collection+"/"+d.Key, time.Minute, func() bool {
				got, err = s.Get(ctx(t), protocol.StateGetParams{Collection: d.Collection, Key: d.Key})
				return err == nil && got.Exists
			})
			if !sameJSON(got.Value, d.Value) {
				t.Fatalf("%s/%s: the value differs from the relay's state", d.Collection, d.Key)
			}
			if !sameJSON(got.Attributes, d.Attributes) {
				t.Fatalf("%s/%s: attributes %v, want %v", d.Collection, d.Key, got.Attributes, d.Attributes)
			}
		}

		// R10.S3: the relay's run goes, as the retention removes it; its export stays.
		target := route.Target{WorkflowID: w.WorkflowID, Scope: route.Scope(w.Scope)}
		var info workflows.Info
		if err := s.query(ctx(t), target, workflows.QueryInfo, &info); err != nil {
			t.Fatal(err)
		}
		if len(info.Chain.Runs) == 0 || info.Chain.Runs[0].RunID != w.RelayRunID || !info.Chain.Runs[0].Legacy {
			t.Fatalf("%s: the chain does not start with the relay run: %+v", w.WorkflowID, info.Chain.Runs)
		}
		if _, err := raw(t).WorkflowService().DeleteWorkflowExecution(ctx(t), &workflowservice.DeleteWorkflowExecutionRequest{
			Namespace: "default", WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: w.WorkflowID, RunId: w.RelayRunID},
		}); err != nil && !isNotFound(err) {
			t.Fatal(err)
		}
		export := filepath.Join(e.blobDir, filepath.FromSlash(blobstore.ExportName(w.WorkflowID, w.RelayRunID)))
		if st, err := os.Stat(export); err != nil || st.Size() == 0 {
			t.Fatalf("%s: no export of the relay run: %v", w.WorkflowID, err)
		}
	}
	// The spec keeps working through the plugin.
	spec := fx.Workflows[0].Documents[0]
	got, _ := s.Get(ctx(t), protocol.StateGetParams{Collection: spec.Collection, Key: spec.Key})
	if _, err := s.Put(ctx(t), protocol.StatePutParams{Collection: spec.Collection, Key: spec.Key, Value: got.Value, Attributes: got.Attributes, ExpectedVersion: &got.Version}); err != nil {
		t.Fatalf("a write after the handoff: %v", err)
	}
}
