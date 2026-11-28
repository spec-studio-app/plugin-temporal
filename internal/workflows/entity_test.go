package workflows

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

const wfID = "spec-test-1a2b3c4d5e6f-fix-login"

type harness struct {
	t   *testing.T
	env *testsuite.TestWorkflowEnvironment
	// exports counts the ExportRun calls.
	exports     []ExportInput
	catalogPuts int
	// catalogErr, when set, gives the error of a catalog put.
	catalogErr func(CatalogPutInput) error
}

func newHarness(t *testing.T) *harness {
	var s testsuite.WorkflowTestSuite
	h := &harness{t: t, env: s.NewTestWorkflowEnvironment()}
	h.env.RegisterWorkflowWithOptions(Entity, workflow.RegisterOptions{Name: EntityWorkflow})
	h.env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: wfID})
	h.env.RegisterActivityWithOptions(func(ctx context.Context, in CatalogPutInput) error {
		if h.catalogErr != nil {
			if err := h.catalogErr(in); err != nil {
				return err
			}
		}
		h.catalogPuts++
		return nil
	}, activity.RegisterOptions{Name: ActivityCatalogPut})
	h.env.RegisterActivityWithOptions(func(ctx context.Context, in ExportInput) (string, error) {
		h.exports = append(h.exports, in)
		return in.WorkflowID, nil
	}, activity.RegisterOptions{Name: ActivityExportRun})
	h.env.RegisterActivityWithOptions(func(ctx context.Context, in DeleteExportsInput) error { return nil }, activity.RegisterOptions{Name: ActivityDeleteExports})
	return h
}

func (h *harness) bound() Bound {
	return Bound{Target: wfID, AtMs: h.env.Now().UnixMilli()}
}

// update sends an update at a time and records its outcome.
type outcome struct {
	rejected error
	result   any
	err      error
	done     bool
}

func (h *harness) update(at time.Duration, name string, decode func(any) any, args ...any) *outcome {
	o := &outcome{}
	h.env.RegisterDelayedCallback(func() {
		h.env.UpdateWorkflow(name, "", &testsuite.TestUpdateCallback{
			OnReject: func(err error) { o.rejected, o.done = err, true },
			OnAccept: func() {},
			OnComplete: func(r any, err error) {
				o.result, o.err, o.done = r, err, true
			},
		}, args...)
	}, at)
	return o
}

func appType(err error) string {
	var app *temporal.ApplicationError
	if errors.As(err, &app) {
		return app.Type()
	}
	return ""
}

// R4.S2 (workflow side): a stale version gets conflict with the stored
// version. R6.S1: append gives sequence numbers in order.
func TestPutConflictAppend(t *testing.T) {
	h := newHarness(t)
	var first PutResult
	h.env.RegisterDelayedCallback(func() {
		h.env.UpdateWorkflow(UpdatePut, "p1", &testsuite.TestUpdateCallback{
			OnReject: func(err error) { t.Errorf("put rejected: %v", err) },
			OnAccept: func() {},
			OnComplete: func(r any, err error) {
				if err != nil {
					t.Errorf("put: %v", err)
					return
				}
				first = r.(PutResult)
			},
		}, PutInput{Bound: h.bound(), Collection: "specs", Key: "1a2b3c4d5e6f:fix-login", Value: json.RawMessage(`1`)})
	}, time.Second)
	stale := "nope"
	conflict := h.update(2*time.Second, UpdatePut, nil, PutInput{Bound: Bound{Target: wfID, AtMs: 0}, Collection: "specs", Key: "1a2b3c4d5e6f:fix-login", Value: json.RawMessage(`2`), Expected: &stale})
	var appended AppendResult
	h.env.RegisterDelayedCallback(func() {
		h.env.UpdateWorkflow(UpdateAppend, "a1", &testsuite.TestUpdateCallback{
			OnReject: func(err error) { t.Errorf("append rejected: %v", err) },
			OnAccept: func() {},
			OnComplete: func(r any, err error) {
				if err == nil {
					appended = r.(AppendResult)
				}
			},
		}, AppendInput{Bound: h.bound(), Stream: "spec:1a2b3c4d5e6f:fix-login", Events: []NewEvent{{Type: "a"}, {Type: "b"}}})
	}, 3*time.Second)
	cont := h.update(4*time.Second, UpdateContinue, nil, ContinueInput{Bound: h.bound()})
	h.env.RegisterDelayedCallback(func() {}, 5*time.Second)
	h.env.ExecuteWorkflow(EntityWorkflow, EntityInput{})

	if first.Version == "" {
		t.Fatal("the first put has no version")
	}
	if conflict.rejected == nil {
		t.Fatalf("the stale put was not rejected: %+v", conflict)
	}
	// The stale put had an old bound too: the bound check comes first.
	if appType(conflict.rejected) != ErrBound {
		t.Fatalf("rejection %v", conflict.rejected)
	}
	if len(appended.Seqs) != 2 || appended.Seqs[0] != 1 || appended.Seqs[1] != 2 {
		t.Fatalf("seqs %v", appended.Seqs)
	}
	if cont.err != nil || cont.rejected != nil {
		t.Fatalf("continue: %+v", cont)
	}
	var can *workflow.ContinueAsNewError
	if !errors.As(h.env.GetWorkflowError(), &can) {
		t.Fatalf("the run did not continue as new: %v", h.env.GetWorkflowError())
	}
	if len(h.exports) != 1 || len(h.exports[0].Chain.Runs) != 1 || h.exports[0].Chain.Runs[0].LastSeq != 2 || h.exports[0].Chain.NextSeq != 3 {
		t.Fatalf("export: %+v", h.exports)
	}
}

func TestStaleVersionInValidator(t *testing.T) {
	h := newHarness(t)
	h.update(time.Second, UpdatePut, nil, PutInput{Bound: h.bound(), Collection: "notes", Key: "a", Value: json.RawMessage(`1`)})
	stale := "nope"
	var o *outcome
	h.env.RegisterDelayedCallback(func() {
		o = h.update(0, UpdatePut, nil, PutInput{Bound: h.bound(), Collection: "notes", Key: "a", Value: json.RawMessage(`2`), Expected: &stale})
	}, 2*time.Second)
	h.env.RegisterDelayedCallback(func() { h.env.CancelWorkflow() }, 3*time.Second)
	h.env.ExecuteWorkflow(EntityWorkflow, EntityInput{})
	if o == nil || appType(o.rejected) != ErrConflict {
		t.Fatalf("got %+v", o)
	}
}

// R7.S2 (workflow side): a wait ends with changed false at its timeout.
func TestWaitTimeout(t *testing.T) {
	h := newHarness(t)
	var res WaitResult
	var o *outcome
	h.env.RegisterDelayedCallback(func() {
		o = &outcome{}
		h.env.UpdateWorkflow(UpdateWait, "w1", &testsuite.TestUpdateCallback{
			OnReject: func(err error) { o.rejected = err },
			OnAccept: func() {},
			OnComplete: func(r any, err error) {
				o.err = err
				if err == nil {
					res = r.(WaitResult)
				}
			},
		}, WaitInput{Bound: h.bound(), Stream: "s", AfterSeq: 0, TimeoutMs: 1000})
	}, time.Second)
	h.env.RegisterDelayedCallback(func() { h.env.CancelWorkflow() }, 5*time.Second)
	h.env.ExecuteWorkflow(EntityWorkflow, EntityInput{})
	if o == nil || o.rejected != nil || o.err != nil || res.Changed {
		t.Fatalf("wait: %+v %+v", o, res)
	}
}

// R10.S4: a handoff input copied from another workflow ends the run with no data.
func TestHandoffForAnotherWorkflow(t *testing.T) {
	h := newHarness(t)
	h.env.ExecuteWorkflow(EntityWorkflow, EntityInput{Handoff: &HandoffInput{Version: 1, WorkflowID: "spec-test-other-slug", Documents: []HandoffDoc{{Collection: "specs", Key: "other:slug", Value: json.RawMessage(`{}`)}}}})
	if !h.env.IsWorkflowCompleted() || h.env.GetWorkflowError() != nil {
		t.Fatalf("the run did not end: %v", h.env.GetWorkflowError())
	}
	if h.catalogPuts != 0 {
		t.Fatal("the run wrote to a catalog")
	}
	if len(h.exports) != 0 {
		t.Fatal("the run exported")
	}
}

// A handoff for this workflow writes the documents.
func TestHandoff(t *testing.T) {
	h := newHarness(t)
	var doc *Doc
	h.env.RegisterDelayedCallback(func() {
		v, err := h.env.QueryWorkflow(QueryDoc, "specs", "1a2b3c4d5e6f:fix-login")
		if err != nil {
			t.Error(err)
			return
		}
		_ = v.Get(&doc)
		h.env.CancelWorkflow()
	}, time.Second)
	h.env.ExecuteWorkflow(EntityWorkflow, EntityInput{Handoff: &HandoffInput{Version: 1, WorkflowID: wfID, Documents: []HandoffDoc{{Collection: "specs", Key: "1a2b3c4d5e6f:fix-login", Value: json.RawMessage(`{"line":7}`), Attributes: map[string]any{"open": true}}}}})
	if doc == nil || string(doc.Value) != `{"line":7}` || doc.Attributes["open"] != true {
		t.Fatalf("doc: %+v", doc)
	}
}

// A continue-as-new input copied from another workflow ends the run with no data.
func TestCopiedChain(t *testing.T) {
	h := newHarness(t)
	h.env.ExecuteWorkflow(EntityWorkflow, EntityInput{Chain: &Chain{WorkflowID: "spec-test-other-x", Nonce: "abc", NextSeq: 4}, Docs: []Doc{{Collection: "specs", Key: "other:x", Value: json.RawMessage(`1`)}}})
	if !h.env.IsWorkflowCompleted() || h.env.GetWorkflowError() != nil || len(h.exports) != 0 {
		t.Fatalf("the run took a copied chain: %v", h.env.GetWorkflowError())
	}
}
