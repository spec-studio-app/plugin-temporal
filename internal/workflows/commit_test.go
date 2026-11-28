package workflows

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

const (
	specKey    = "1a2b3c4d5e6f:fix-login"
	specStream = "spec:1a2b3c4d5e6f:fix-login"
)

func (h *harness) info() Info {
	h.t.Helper()
	v, err := h.env.QueryWorkflow(QueryInfo)
	if err != nil {
		h.t.Fatal(err)
	}
	var info Info
	if err := v.Get(&info); err != nil {
		h.t.Fatal(err)
	}
	return info
}

func (h *harness) doc(collection, key string) *Doc {
	h.t.Helper()
	v, err := h.env.QueryWorkflow(QueryDoc, collection, key)
	if err != nil {
		h.t.Fatal(err)
	}
	var d *Doc
	if err := v.Get(&d); err != nil {
		h.t.Fatal(err)
	}
	return d
}

// commit sends a commit update at a time. It makes the input then.
func (h *harness) commit(at time.Duration, in func() CommitInput) *outcome {
	o := &outcome{}
	h.env.RegisterDelayedCallback(func() {
		h.env.UpdateWorkflow(UpdateCommit, "", &testsuite.TestUpdateCallback{
			OnReject:   func(err error) { o.rejected, o.done = err, true },
			OnAccept:   func() {},
			OnComplete: func(r any, err error) { o.result, o.err, o.done = r, err, true },
		}, in())
	}, at)
	return o
}

// R2.S1 and R2.S2 (workflow side): a spec's document and stream are in one
// workflow. A commit writes the document and appends the events in one
// update. A stale commit is refused by the validator: no document, no
// event, no update in the history.
func TestCommitLocal(t *testing.T) {
	h := newHarness(t)
	ok := h.commit(time.Second, func() CommitInput {
		return CommitInput{Bound: h.bound(), Collection: "app-specs", Key: specKey, Value: json.RawMessage(`{"stage":"plan"}`), Attributes: map[string]any{"stage": "plan"},
			Stream: specStream, Events: []NewEvent{{Type: "activity", Data: json.RawMessage(`1`)}, {Type: "activity", Data: json.RawMessage(`2`)}}, StreamTarget: wfID}
	})
	stale := "nope"
	conflict := h.commit(2*time.Second, func() CommitInput {
		return CommitInput{Bound: h.bound(), Collection: "app-specs", Key: specKey, Value: json.RawMessage(`{"stage":"build"}`), Expected: &stale,
			Stream: specStream, Events: []NewEvent{{Type: "activity"}}, StreamTarget: wfID}
	})
	var afterConflict Info
	var afterDoc *Doc
	h.env.RegisterDelayedCallback(func() {
		afterConflict = h.info()
		afterDoc = h.doc("app-specs", specKey)
	}, 3*time.Second)
	none := h.commit(4*time.Second, func() CommitInput {
		v := afterDoc.Version
		return CommitInput{Bound: h.bound(), Collection: "app-specs", Key: specKey, Value: json.RawMessage(`{"stage":"build"}`), Expected: &v,
			Stream: specStream, Events: []NewEvent{}, StreamTarget: wfID}
	})
	cont := h.update(5*time.Second, UpdateContinue, nil, ContinueInput{Bound: h.bound()})
	h.env.RegisterDelayedCallback(func() {}, 6*time.Second)
	h.env.ExecuteWorkflow(EntityWorkflow, EntityInput{})

	if ok.err != nil || ok.rejected != nil {
		t.Fatalf("commit: %+v", ok)
	}
	first := ok.result.(CommitResult)
	if first.Version == "" || first.Remote || len(first.Seqs) != 2 || first.Seqs[0] != 1 || first.Seqs[1] != 2 || len(first.Ats) != 2 || first.Nonce == "" {
		t.Fatalf("commit result: %+v", first)
	}
	if appType(conflict.rejected) != ErrConflict {
		t.Fatalf("the stale commit: %+v", conflict)
	}
	var app *temporal.ApplicationError
	var stored *string
	if !errors.As(conflict.rejected, &app) || app.Details(&stored) != nil || stored == nil || *stored != first.Version {
		t.Fatalf("the conflict does not give the stored version %s: %v", first.Version, conflict.rejected)
	}
	if afterConflict.Chain.NextSeq != 3 || afterConflict.Chain.Stream != specStream {
		t.Fatalf("after the conflict the chain is %+v", afterConflict.Chain)
	}
	if afterDoc == nil || afterDoc.Version != first.Version || string(afterDoc.Value) != `{"stage":"plan"}` || afterDoc.Attributes["stage"] != "plan" {
		t.Fatalf("after the conflict the document is %+v", afterDoc)
	}
	if none.err != nil || none.rejected != nil {
		t.Fatalf("commit with no events: %+v", none)
	}
	if r := none.result.(CommitResult); len(r.Seqs) != 0 || r.Version == first.Version {
		t.Fatalf("commit with no events: %+v", r)
	}
	if cont.err != nil || cont.rejected != nil {
		t.Fatalf("continue: %+v", cont)
	}
	if len(h.exports) != 1 || h.exports[0].Chain.NextSeq != 3 || h.exports[0].Chain.Runs[0].LastSeq != 2 {
		t.Fatalf("export: %+v", h.exports)
	}
	if h.catalogPuts != 2 {
		t.Fatalf("%d catalog puts; want 2", h.catalogPuts)
	}
}

// A commit for this workflow's document with another stream than the
// workflow's stream is refused, and writes no document.
func TestCommitOtherStream(t *testing.T) {
	h := newHarness(t)
	h.update(time.Second, UpdateAppend, nil, AppendInput{Bound: h.bound(), Stream: specStream, Events: []NewEvent{{Type: "a"}}})
	bad := h.commit(2*time.Second, func() CommitInput {
		return CommitInput{Bound: h.bound(), Collection: "app-specs", Key: specKey, Value: json.RawMessage(`1`), Stream: "spec:1a2b3c4d5e6f:other", Events: []NewEvent{{Type: "a"}}, StreamTarget: wfID}
	})
	var d *Doc
	h.env.RegisterDelayedCallback(func() { d = h.doc("app-specs", specKey); h.env.CancelWorkflow() }, 3*time.Second)
	h.env.ExecuteWorkflow(EntityWorkflow, EntityInput{})
	if appType(bad.rejected) != ErrBadInput || d != nil {
		t.Fatalf("commit with another stream: %+v, document %+v", bad, d)
	}
}

// A document and a stream with two workflows: the commit delivers the
// events to the stream's workflow, then writes the document, and completes
// with the stream's sequence numbers.
func TestCommitRemote(t *testing.T) {
	h := newHarness(t)
	var delivered []DeliverInput
	var order []string
	h.catalogErr = func(in CatalogPutInput) error { order = append(order, "catalog "+in.Entry.Version); return nil }
	h.env.RegisterActivityWithOptions(func(ctx context.Context, in DeliverInput) (AppendResult, error) {
		order = append(order, "deliver "+in.ID)
		delivered = append(delivered, in)
		base := int64(len(delivered) * 10)
		res := AppendResult{Nonce: "abc"}
		for i := range in.Events {
			res.Seqs = append(res.Seqs, base+int64(i))
		}
		return res, nil
	}, activity.RegisterOptions{Name: ActivityDeliver})
	in := func(value string, expected *string) CommitInput {
		return CommitInput{Bound: h.bound(), Collection: "notes", Key: "a", Value: json.RawMessage(value), Expected: expected,
			Stream: "notes-stream", Events: []NewEvent{{Type: "e" + value}}, StreamTarget: "stream/test/notes-stream", StreamQueue: "q"}
	}
	one := h.commit(time.Second, func() CommitInput { return in("1", nil) })
	var two *outcome
	h.env.RegisterDelayedCallback(func() {
		r := one.result.(CommitResult)
		two = h.commit(0, func() CommitInput { return in("2", &r.Version) })
	}, 2*time.Second)
	var info Info
	h.env.RegisterDelayedCallback(func() { info = h.info(); h.env.CancelWorkflow() }, 3*time.Second)
	h.env.ExecuteWorkflow(EntityWorkflow, EntityInput{})

	if one.err != nil || one.rejected != nil || two == nil || two.err != nil || two.rejected != nil {
		t.Fatalf("commits: %+v %+v", one, two)
	}
	r1, r2 := one.result.(CommitResult), two.result.(CommitResult)
	if !r1.Remote || r1.Nonce != "abc" || len(r1.Seqs) != 1 || r1.Seqs[0] != 10 || !r2.Remote || r2.Seqs[0] != 20 {
		t.Fatalf("results: %+v %+v", r1, r2)
	}
	if len(delivered) != 2 || delivered[0].Events[0].Type != "e1" || delivered[1].Events[0].Type != "e2" ||
		delivered[0].WorkflowID != "stream/test/notes-stream" || delivered[0].TaskQueue != "q" || delivered[0].ID != wfID+"/"+r1.Version {
		t.Fatalf("deliveries: %+v", delivered)
	}
	// This workflow holds no stream: the events are in the stream's workflow.
	if info.Chain.Stream != "" || info.Chain.NextSeq != 1 {
		t.Fatalf("the document's chain: %+v", info.Chain)
	}
	want := []string{"deliver " + wfID + "/" + r1.Version, "catalog " + r1.Version, "deliver " + wfID + "/" + r2.Version, "catalog " + r2.Version}
	if fmt.Sprint(order) != fmt.Sprint(want) {
		t.Fatalf("the order is %v; want %v", order, want)
	}
}

// A remote commit whose delivery fails writes nothing: no document, no
// catalog entry. While the delivery runs, the document is not visible, and a
// put of the document waits for the commit's end. A later commit of the
// document works.
func TestCommitRemoteDeliveryFails(t *testing.T) {
	h := newHarness(t)
	h.env.RegisterActivityWithOptions(func(ctx context.Context, in DeliverInput) (AppendResult, error) { return AppendResult{}, nil },
		activity.RegisterOptions{Name: ActivityDeliver})
	h.env.OnActivity(ActivityDeliver, mock.Anything, mock.Anything).After(5*time.Second).Return(AppendResult{},
		temporal.NewNonRetryableApplicationError("The update input is too old.", ErrBound, nil)).Once()
	h.env.OnActivity(ActivityDeliver, mock.Anything, mock.Anything).Return(AppendResult{Nonce: "abc", Seqs: []int64{7}}, nil)
	in := func(expected *string) CommitInput {
		return CommitInput{Bound: h.bound(), Collection: "notes", Key: "a", Value: json.RawMessage(`1`), Expected: expected,
			Stream: "notes-stream", Events: []NewEvent{{Type: "e"}}, StreamTarget: "stream/test/notes-stream", StreamQueue: "q"}
	}
	failed := h.commit(time.Second, func() CommitInput { return in(nil) })
	var during *Doc
	h.env.RegisterDelayedCallback(func() { during = h.doc("notes", "a") }, 3*time.Second)
	put := h.update(4*time.Second, UpdatePut, nil, PutInput{Bound: Bound{Target: wfID, AtMs: h.env.Now().Add(4 * time.Second).UnixMilli()}, Collection: "notes", Key: "a", Value: json.RawMessage(`9`)})
	var putDoneEarly bool
	h.env.RegisterDelayedCallback(func() { putDoneEarly = put.done }, 5*time.Second)
	var after *Doc
	var puts int
	h.env.RegisterDelayedCallback(func() { after = h.doc("notes", "a"); puts = h.catalogPuts }, 7*time.Second)
	again := h.commit(8*time.Second, func() CommitInput {
		v := after.Version
		return in(&v)
	})
	cont := h.update(9*time.Second, UpdateContinue, nil, ContinueInput{Bound: h.bound()})
	h.env.RegisterDelayedCallback(func() {}, 10*time.Second)
	h.env.ExecuteWorkflow(EntityWorkflow, EntityInput{})

	if appType(failed.err) != ErrBound {
		t.Fatalf("the failed commit: %+v", failed)
	}
	if during != nil {
		t.Fatalf("the document is visible during the delivery: %+v", during)
	}
	// The put waited for the commit, then wrote: the failed commit left
	// the document missing, so the put of a new document works.
	if putDoneEarly || put.err != nil || put.rejected != nil {
		t.Fatalf("the put during the commit: early %v, %+v", putDoneEarly, put)
	}
	if after == nil || string(after.Value) != `9` || puts != 1 {
		t.Fatalf("after the failed commit: document %+v, %d catalog puts; want the put only", after, puts)
	}
	if again.err != nil || again.rejected != nil {
		t.Fatalf("the commit after the failure: %+v", again)
	}
	if r := again.result.(CommitResult); !r.Remote || len(r.Seqs) != 1 || r.Seqs[0] != 7 {
		t.Fatalf("the commit after the failure: %+v", r)
	}
	if cont.err != nil || cont.rejected != nil || len(h.exports) != 1 {
		t.Fatalf("the run did not continue as new: %+v, %d exports", cont, len(h.exports))
	}
}

// A commit whose catalog put fails writes nothing and holds nothing: no
// document, no sequence number, and a later commit and the run's
// continue-as-new work.
func TestCommitWriteFailsReleases(t *testing.T) {
	h := newHarness(t)
	fail := true
	h.catalogErr = func(CatalogPutInput) error {
		if fail {
			fail = false
			return temporal.NewNonRetryableApplicationError("The update input is too old.", ErrBound, nil)
		}
		return nil
	}
	in := func(expected *string) CommitInput {
		return CommitInput{Bound: h.bound(), Collection: "app-specs", Key: specKey, Value: json.RawMessage(`1`), Expected: expected,
			Stream: specStream, Events: []NewEvent{{Type: "activity"}}, StreamTarget: wfID}
	}
	failed := h.commit(time.Second, func() CommitInput { return in(nil) })
	var info Info
	var d *Doc
	h.env.RegisterDelayedCallback(func() { info = h.info(); d = h.doc("app-specs", specKey) }, 2*time.Second)
	again := h.commit(3*time.Second, func() CommitInput { return in(nil) })
	cont := h.update(4*time.Second, UpdateContinue, nil, ContinueInput{Bound: h.bound()})
	h.env.RegisterDelayedCallback(func() {}, 5*time.Second)
	h.env.ExecuteWorkflow(EntityWorkflow, EntityInput{})

	if appType(failed.err) != ErrBound {
		t.Fatalf("the failed commit: %+v", failed)
	}
	if d != nil || info.Chain.NextSeq != 1 {
		t.Fatalf("the failed commit wrote: document %+v, chain %+v", d, info.Chain)
	}
	if again.err != nil || again.rejected != nil {
		t.Fatalf("the commit after the failure: %+v", again)
	}
	if r := again.result.(CommitResult); len(r.Seqs) != 1 || r.Seqs[0] != 1 {
		t.Fatalf("the commit after the failure: %+v", r)
	}
	var can *workflow.ContinueAsNewError
	if cont.err != nil || cont.rejected != nil || !errors.As(h.env.GetWorkflowError(), &can) {
		t.Fatalf("the run did not continue as new: %+v, %v", cont, h.env.GetWorkflowError())
	}
}

// A local commit gives its events their sequence numbers only in the task
// that completes it. While its catalog put runs, the chain does not move, a
// stream wait does not end on its events, and an append gets the next
// number: the visible order is the order of the numbers.
func TestCommitLocalSeqsAtCompletion(t *testing.T) {
	h := newHarness(t)
	h.env.OnActivity(ActivityCatalogPut, mock.Anything, mock.Anything).After(5 * time.Second).Return(nil)
	commit := h.commit(time.Second, func() CommitInput {
		return CommitInput{Bound: h.bound(), Collection: "app-specs", Key: specKey, Value: json.RawMessage(`1`),
			Stream: specStream, Events: []NewEvent{{Type: "activity"}, {Type: "activity"}}, StreamTarget: wfID}
	})
	wait := h.update(1500*time.Millisecond, UpdateWait, nil, WaitInput{Bound: Bound{Target: wfID, AtMs: h.env.Now().Add(1500 * time.Millisecond).UnixMilli()}, Stream: specStream, AfterSeq: 0, TimeoutMs: 60_000})
	var during Info
	var doc *Doc
	var waitEarly bool
	h.env.RegisterDelayedCallback(func() { during = h.info(); doc = h.doc("app-specs", specKey); waitEarly = wait.done }, 2*time.Second)
	appended := h.update(3*time.Second, UpdateAppend, nil, AppendInput{Bound: Bound{Target: wfID, AtMs: h.env.Now().Add(3 * time.Second).UnixMilli()}, Stream: specStream, Events: []NewEvent{{Type: "sync"}}})
	var waitAfterAppend bool
	h.env.RegisterDelayedCallback(func() { waitAfterAppend = wait.done }, 4*time.Second)
	h.env.RegisterDelayedCallback(func() { h.env.CancelWorkflow() }, 10*time.Second)
	h.env.ExecuteWorkflow(EntityWorkflow, EntityInput{})

	if during.Chain.NextSeq != 1 || doc != nil || waitEarly {
		t.Fatalf("during the catalog put: chain %+v, document %+v, wait ended %v", during.Chain, doc, waitEarly)
	}
	if appended.err != nil || appended.rejected != nil || appended.result.(AppendResult).Seqs[0] != 1 {
		t.Fatalf("the append: %+v", appended)
	}
	if !waitAfterAppend || wait.result.(WaitResult).Seq != 1 {
		t.Fatalf("the wait: done %v, %+v", waitAfterAppend, wait)
	}
	if commit.err != nil || commit.rejected != nil {
		t.Fatalf("the commit: %+v", commit)
	}
	if r := commit.result.(CommitResult); len(r.Seqs) != 2 || r.Seqs[0] != 2 || r.Seqs[1] != 3 {
		t.Fatalf("the commit got %+v; want the sequence numbers after the append", r)
	}
}

// A delivery that comes again (the Deliver activity tried again) adds no
// event and gets the first delivery's sequence numbers, also after a
// continue-as-new.
func TestDeliveryOnce(t *testing.T) {
	h := newHarness(t)
	in := func() AppendInput {
		return AppendInput{Bound: h.bound(), Stream: "s", Events: []NewEvent{{Type: "a"}, {Type: "b"}}, Delivery: "doc/x/7"}
	}
	first := h.update(time.Second, UpdateAppend, nil, in())
	var again *outcome
	h.env.RegisterDelayedCallback(func() { again = h.update(0, UpdateAppend, nil, in()) }, 2*time.Second)
	h.update(3*time.Second, UpdateContinue, nil, ContinueInput{Bound: h.bound()})
	h.env.RegisterDelayedCallback(func() {}, 4*time.Second)
	h.env.ExecuteWorkflow(EntityWorkflow, EntityInput{})

	if first.err != nil || first.rejected != nil {
		t.Fatalf("first: %+v", first)
	}
	if again == nil || appType(again.rejected) != ErrDelivered {
		t.Fatalf("again: %+v", again)
	}
	var app *temporal.ApplicationError
	var res AppendResult
	if !errors.As(again.rejected, &app) || app.Details(&res) != nil || len(res.Seqs) != 2 || res.Seqs[0] != 1 || res.Seqs[1] != 2 || res.Nonce != first.result.(AppendResult).Nonce {
		t.Fatalf("the second delivery got %+v", res)
	}
	var can *workflow.ContinueAsNewError
	if !errors.As(h.env.GetWorkflowError(), &can) {
		t.Fatalf("no continue-as-new: %v", h.env.GetWorkflowError())
	}
	c := h.exports[0].Chain
	if c.NextSeq != 3 || len(c.Deliveries) != 1 || c.Deliveries[0] != (Delivery{ID: "doc/x/7", FirstSeq: 1, Count: 2}) {
		t.Fatalf("chain: %+v", c)
	}
}

// A remote commit whose catalog put fails after the delivery stands: the
// events are delivered and the document is written, so the caller does not
// send it again. Nothing stays held: the next commit and the continue-as-new
// work.
func TestCommitRemoteCatalogFails(t *testing.T) {
	h := newHarness(t)
	fail := true
	h.catalogErr = func(CatalogPutInput) error {
		if fail {
			fail = false
			return temporal.NewNonRetryableApplicationError("The update input is too old.", ErrBound, nil)
		}
		return nil
	}
	h.env.RegisterActivityWithOptions(func(ctx context.Context, in DeliverInput) (AppendResult, error) {
		return AppendResult{Nonce: "abc", Seqs: []int64{4}}, nil
	}, activity.RegisterOptions{Name: ActivityDeliver})
	in := func(expected *string) CommitInput {
		return CommitInput{Bound: h.bound(), Collection: "notes", Key: "a", Value: json.RawMessage(`1`), Expected: expected,
			Stream: "notes-stream", Events: []NewEvent{{Type: "e"}}, StreamTarget: "stream/test/notes-stream", StreamQueue: "q"}
	}
	first := h.commit(time.Second, func() CommitInput { return in(nil) })
	var d *Doc
	h.env.RegisterDelayedCallback(func() { d = h.doc("notes", "a") }, 2*time.Second)
	second := h.commit(3*time.Second, func() CommitInput { v := d.Version; return in(&v) })
	cont := h.update(4*time.Second, UpdateContinue, nil, ContinueInput{Bound: h.bound()})
	h.env.RegisterDelayedCallback(func() {}, 5*time.Second)
	h.env.ExecuteWorkflow(EntityWorkflow, EntityInput{})

	if first.err != nil || first.rejected != nil || d == nil || d.Version != first.result.(CommitResult).Version {
		t.Fatalf("the first commit: %+v, document %+v", first, d)
	}
	if second.err != nil || second.rejected != nil || h.catalogPuts != 1 {
		t.Fatalf("the second commit: %+v, %d catalog puts", second, h.catalogPuts)
	}
	var can *workflow.ContinueAsNewError
	if cont.err != nil || !errors.As(h.env.GetWorkflowError(), &can) {
		t.Fatalf("the run did not continue as new: %+v, %v", cont, h.env.GetWorkflowError())
	}
}
