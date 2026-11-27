package workflows

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/spec-studio-app/plugin-temporal/internal/blobstore"
)

// entity is the state of one run of the entity workflow.
type entity struct {
	wfID  string
	runID string
	chain Chain
	docs  map[string]*Doc
	// runFirstSeq is the first sequence number of this run.
	runFirstSeq  int64
	minAt, maxAt *time.Time
	// updates counts the accepted updates of this run (for the run limit).
	updates int
	// activity counts every accepted update, for the idle time.
	activity    int
	runLimit    int
	idle        time.Duration
	inlineBytes int
	// ready is false while the run starts (handoff, state load).
	ready bool
	// closing is true from the moment the run starts to end: every
	// validator then refuses with ErrRetry, so the export holds every event.
	closing bool
	// force is set by the continue update (the test hook).
	force bool
	// nextTurn and turn put the deliveries of the commits in the order of
	// their versions: a commit takes the ticket nextTurn and delivers when
	// turn is its ticket.
	nextTurn, turn int
}

func docKey(collection, key string) string { return collection + "\x00" + key }

func retryErr(msg string) error {
	return temporal.NewApplicationError(msg, ErrRetry)
}

func conflictErr(stored *string) error {
	return temporal.NewNonRetryableApplicationError("The expected version is not the stored version.", ErrConflict, nil, stored)
}

func (e *entity) version(n int64) string {
	return e.chain.Nonce + "-" + strconv.FormatInt(n, 36)
}

// open checks what every validator checks: the run takes updates, and the
// input is bound to this workflow and is recent.
func (e *entity) open(ctx workflow.Context, b Bound) error {
	if !e.ready {
		return retryErr("The run is starting.")
	}
	if e.closing {
		return retryErr("The run is closing. Send the update again.")
	}
	return checkBound(ctx, e.wfID, b)
}

func checkBound(ctx workflow.Context, wfID string, b Bound) error {
	if b.Target != wfID {
		return temporal.NewNonRetryableApplicationError("The update input is not for this workflow.", ErrBound, nil)
	}
	d := workflow.Now(ctx).Sub(time.UnixMilli(b.AtMs))
	if d > BoundMaxAge || d < -BoundMaxAge {
		return temporal.NewNonRetryableApplicationError("The update input is too old.", ErrBound, nil)
	}
	return nil
}

func (e *entity) storedVersion(collection, key string) *string {
	if d, ok := e.docs[docKey(collection, key)]; ok {
		v := d.Version
		return &v
	}
	return nil
}

func sameVersion(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func (e *entity) touch() {
	e.updates++
	e.activity++
}

// hasData is true when the chain ever held a document or an event.
func (e *entity) hasData() bool {
	return e.chain.N > 0 || e.chain.NextSeq > 1 || len(e.chain.Runs) > 0
}

func (e *entity) info() Info {
	return Info{RunID: e.runID, Chain: e.chain, RunFirstSeq: e.runFirstSeq, Closing: e.closing, Docs: len(e.docs)}
}

func (e *entity) sortedDocs() []Doc {
	out := make([]Doc, 0, len(e.docs))
	for _, d := range e.docs {
		out = append(out, *d)
	}
	sort.Slice(out, func(i, j int) bool {
		return docKey(out[i].Collection, out[i].Key) < docKey(out[j].Collection, out[j].Key)
	})
	return out
}

func catalogCtx(ctx workflow.Context) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		RetryPolicy:         &temporal.RetryPolicy{InitialInterval: 200 * time.Millisecond, BackoffCoefficient: 2, MaximumInterval: 30 * time.Second},
	})
}

func exportCtx(ctx workflow.Context) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Minute,
		HeartbeatTimeout:    2 * time.Minute,
		RetryPolicy:         &temporal.RetryPolicy{InitialInterval: time.Second, BackoffCoefficient: 2, MaximumInterval: time.Minute},
	})
}

// write stores a document with the next write number and puts it in the
// collection's catalog.
func (e *entity) write(ctx workflow.Context, collection, key string, value json.RawMessage, attrs map[string]any) (*Doc, error) {
	if attrs == nil {
		attrs = map[string]any{}
	}
	e.chain.N++
	d := &Doc{Collection: collection, Key: key, Value: value, Attributes: attrs, Version: e.version(e.chain.N), N: e.chain.N, UpdatedAt: workflow.Now(ctx).UTC()}
	e.docs[docKey(collection, key)] = d
	entry := CatalogEntry{Attributes: attrs, Version: d.Version, N: d.N, UpdatedAt: d.UpdatedAt}
	err := workflow.ExecuteActivity(catalogCtx(ctx), ActivityCatalogPut, CatalogPutInput{Collection: collection, Key: key, Entry: entry}).Get(ctx, nil)
	return d, err
}

// appendEvents gives the next sequence numbers to the events of the stream.
// It does not wait, so the caller's other changes go into the same
// workflow task.
func (e *entity) appendEvents(ctx workflow.Context, stream string, events []NewEvent) (seqs []int64, ats []time.Time) {
	now := workflow.Now(ctx).UTC()
	for _, ev := range events {
		at := now
		if ev.At != nil {
			at = ev.At.UTC()
		}
		seqs = append(seqs, e.chain.NextSeq)
		ats = append(ats, at)
		e.chain.NextSeq++
		if e.minAt == nil || at.Before(*e.minAt) {
			t := at
			e.minAt = &t
		}
		if e.maxAt == nil || at.After(*e.maxAt) {
			t := at
			e.maxAt = &t
		}
	}
	e.chain.Stream = stream
	return seqs, ats
}

// delivered finds a delivery that the chain has already.
func (e *entity) delivered(id string) *Delivery {
	if id == "" {
		return nil
	}
	for i := range e.chain.Deliveries {
		if e.chain.Deliveries[i].ID == id {
			d := e.chain.Deliveries[i]
			return &d
		}
	}
	return nil
}

func (e *entity) checkStream(stream string) error {
	if e.chain.Stream != "" && e.chain.Stream != stream {
		return temporal.NewNonRetryableApplicationError("The stream is not the stream of this workflow.", ErrBadInput, nil)
	}
	return nil
}

func deliverCtx(ctx workflow.Context) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 2 * time.Minute,
		RetryPolicy:         &temporal.RetryPolicy{InitialInterval: 200 * time.Millisecond, BackoffCoefficient: 2, MaximumInterval: 30 * time.Second},
	})
}

// Entity is the entity workflow.
func Entity(ctx workflow.Context, in EntityInput) error {
	info := workflow.GetInfo(ctx)
	log := workflow.GetLogger(ctx)
	e := &entity{wfID: info.WorkflowExecution.ID, runID: info.WorkflowExecution.RunID, docs: map[string]*Doc{}}

	// A handoff input copied from another workflow: end at once, as the
	// relay's workflows do.
	if in.Handoff != nil && in.Handoff.WorkflowID != e.wfID {
		log.Warn("The handoff input is for another workflow. The run ends with no data.")
		return nil
	}
	// The same for a continue-as-new input: a run gives its chain and its
	// documents only to a run of its own workflow.
	if in.Chain != nil && in.Chain.WorkflowID != "" && in.Chain.WorkflowID != e.wfID {
		log.Warn("The start input is for another workflow. The run ends with no data.")
		return nil
	}
	if (len(in.Docs) > 0 || in.StateObject != "") && in.Chain == nil {
		log.Warn("The start input has documents but no chain. The run ends with no data.")
		return nil
	}
	if in.StateObject != "" && !strings.HasPrefix(in.StateObject, blobstore.StatePrefix(e.wfID)) {
		log.Warn("The state object of the start input is for another workflow. The run ends with no data.")
		return nil
	}

	e.inlineBytes = in.InlineBytes
	e.runLimit = in.RunLimit
	if e.runLimit <= 0 {
		e.runLimit = DefaultRunLimit
	}
	e.idle = time.Duration(in.IdleMs) * time.Millisecond
	if e.idle <= 0 {
		e.idle = DefaultIdle
	}
	if in.Chain != nil {
		e.chain = *in.Chain
	}
	e.chain.WorkflowID = e.wfID
	if e.chain.NextSeq < 1 {
		e.chain.NextSeq = 1
	}
	if e.chain.KeptFrom < 1 {
		e.chain.KeptFrom = 1
	}
	if e.chain.Nonce == "" {
		var nonce string
		if err := workflow.SideEffect(ctx, func(workflow.Context) any {
			b := make([]byte, 6)
			_, _ = rand.Read(b)
			return hex.EncodeToString(b)
		}).Get(&nonce); err != nil {
			return err
		}
		e.chain.Nonce = nonce
	}
	e.runFirstSeq = e.chain.NextSeq
	for i := range in.Docs {
		d := in.Docs[i]
		e.docs[docKey(d.Collection, d.Key)] = &d
	}

	if err := e.handlers(ctx); err != nil {
		return err
	}

	// The start: the documents of the last run from the blob store, then
	// the handoff from a relay run.
	// One document per activity, so no payload holds the whole state.
	if in.StateObject != "" {
		for i, count := 0, 1; i < count; i++ {
			var res LoadStateResult
			if err := workflow.ExecuteActivity(exportCtx(ctx), ActivityLoadState, LoadStateInput{Name: in.StateObject, Index: i}).Get(ctx, &res); err != nil {
				return err
			}
			count = res.Count
			if res.Doc != nil {
				d := *res.Doc
				e.docs[docKey(d.Collection, d.Key)] = &d
			}
		}
		// The run has the documents: the object of the last run goes.
		if err := workflow.ExecuteActivity(exportCtx(ctx), ActivityDeleteObject, in.StateObject).Get(ctx, nil); err != nil {
			return err
		}
	}
	if in.Handoff != nil {
		if err := e.handoff(ctx, in.Handoff, info.ContinuedExecutionRunID); err != nil {
			return err
		}
	}
	e.ready = true

	end, err := e.loop(ctx)
	if err != nil {
		return err
	}
	return e.close(ctx, end)
}

// handoff takes over a relay run: it exports the relay's last run (closed,
// so its history is whole) as the first run of the chain, and writes the
// documents that the relay gave.
func (e *entity) handoff(ctx workflow.Context, h *HandoffInput, prevRunID string) error {
	if prevRunID != "" {
		rec := RunRecord{RunID: prevRunID, Export: blobstore.ExportName(e.wfID, prevRunID), Legacy: true}
		chain := e.chain
		chain.Runs = append(append([]RunRecord{}, chain.Runs...), rec)
		var name string
		if err := workflow.ExecuteActivity(exportCtx(ctx), ActivityExportRun, ExportInput{WorkflowID: e.wfID, RunID: prevRunID, Chain: chain}).Get(ctx, &name); err != nil {
			return err
		}
		e.chain = chain
	}
	for _, d := range h.Documents {
		if _, err := e.write(ctx, d.Collection, d.Key, d.Value, d.Attributes); err != nil {
			return err
		}
	}
	return nil
}

func (e *entity) handlers(ctx workflow.Context) error {
	// Until the run has its documents, a query fails with QueryStarting,
	// and the store asks again: an empty answer would say that a document
	// does not exist.
	starting := errors.New(QueryStarting)
	if err := workflow.SetQueryHandler(ctx, QueryDoc, func(collection, key string) (*Doc, error) {
		if !e.ready {
			return nil, starting
		}
		if d, ok := e.docs[docKey(collection, key)]; ok {
			c := *d
			return &c, nil
		}
		return nil, nil
	}); err != nil {
		return err
	}
	if err := workflow.SetQueryHandler(ctx, QueryInfo, func() (Info, error) {
		if !e.ready {
			return Info{}, starting
		}
		return e.info(), nil
	}); err != nil {
		return err
	}
	if err := workflow.SetQueryHandler(ctx, QueryDocKeys, func() ([]DocRef, error) {
		if !e.ready {
			return nil, starting
		}
		refs := []DocRef{}
		for _, d := range e.sortedDocs() {
			refs = append(refs, DocRef{Collection: d.Collection, Key: d.Key})
		}
		return refs, nil
	}); err != nil {
		return err
	}

	// put: the validator checks the expected version, so a conflict adds
	// no event. The handler checks it again: two updates of one workflow
	// task can both pass their validators before either handler runs.
	putCheck := func(in PutInput) error {
		if !sameVersion(e.storedVersion(in.Collection, in.Key), in.Expected) {
			return conflictErr(e.storedVersion(in.Collection, in.Key))
		}
		return nil
	}
	if err := workflow.SetUpdateHandlerWithOptions(ctx, UpdatePut,
		func(ctx workflow.Context, in PutInput) (PutResult, error) {
			if err := putCheck(in); err != nil {
				return PutResult{}, err
			}
			e.touch()
			d, err := e.write(ctx, in.Collection, in.Key, in.Value, in.Attributes)
			if err != nil {
				return PutResult{}, err
			}
			return PutResult{Version: d.Version, UpdatedAt: d.UpdatedAt}, nil
		},
		workflow.UpdateHandlerOptions{Validator: func(ctx workflow.Context, in PutInput) error {
			if err := e.open(ctx, in.Bound); err != nil {
				return err
			}
			return putCheck(in)
		}},
	); err != nil {
		return err
	}

	delCheck := func(in DeleteInput) error {
		stored := e.storedVersion(in.Collection, in.Key)
		if stored == nil {
			return temporal.NewNonRetryableApplicationError("The document does not exist.", ErrNotFound, nil)
		}
		if *stored != in.Expected {
			return conflictErr(stored)
		}
		return nil
	}
	if err := workflow.SetUpdateHandlerWithOptions(ctx, UpdateDelete,
		func(ctx workflow.Context, in DeleteInput) (bool, error) {
			if err := delCheck(in); err != nil {
				return false, err
			}
			e.touch()
			delete(e.docs, docKey(in.Collection, in.Key))
			e.chain.N++
			entry := CatalogEntry{Version: e.version(e.chain.N), N: e.chain.N, UpdatedAt: workflow.Now(ctx).UTC(), Deleted: true}
			if err := workflow.ExecuteActivity(catalogCtx(ctx), ActivityCatalogPut, CatalogPutInput{Collection: in.Collection, Key: in.Key, Entry: entry}).Get(ctx, nil); err != nil {
				return false, err
			}
			return true, nil
		},
		workflow.UpdateHandlerOptions{Validator: func(ctx workflow.Context, in DeleteInput) error {
			if err := e.open(ctx, in.Bound); err != nil {
				return err
			}
			return delCheck(in)
		}},
	); err != nil {
		return err
	}

	// append: the events of the stream. The input and the result stay in
	// the history: they are the event log.
	if err := workflow.SetUpdateHandlerWithOptions(ctx, UpdateAppend,
		func(ctx workflow.Context, in AppendInput) (AppendResult, error) {
			if d := e.delivered(in.Delivery); d != nil {
				return AppendResult{}, e.deliveredErr(*d)
			}
			e.touch()
			res := AppendResult{Nonce: e.chain.Nonce}
			res.Seqs, res.Ats = e.appendEvents(ctx, in.Stream, in.Events)
			if in.Delivery != "" {
				e.chain.Deliveries = append(e.chain.Deliveries, Delivery{ID: in.Delivery, FirstSeq: res.Seqs[0], Count: len(res.Seqs)})
				if n := len(e.chain.Deliveries); n > MaxDeliveries {
					e.chain.Deliveries = append([]Delivery(nil), e.chain.Deliveries[n-MaxDeliveries:]...)
				}
			}
			return res, nil
		},
		workflow.UpdateHandlerOptions{Validator: func(ctx workflow.Context, in AppendInput) error {
			if err := e.open(ctx, in.Bound); err != nil {
				return err
			}
			if len(in.Events) == 0 {
				return temporal.NewNonRetryableApplicationError("The event list is empty.", ErrBadInput, nil)
			}
			if err := e.checkStream(in.Stream); err != nil {
				return err
			}
			if d := e.delivered(in.Delivery); d != nil {
				return e.deliveredErr(*d)
			}
			return nil
		}},
	); err != nil {
		return err
	}

	// commit: state.commit. The update checks the version, writes the
	// document and appends the events. When the stream is in this
	// workflow (a spec: its document and its stream have one workflow),
	// the document and the events change in the same workflow task, and the
	// update's input and result in the history hold both, or the history
	// holds neither. Else the events go to the stream's workflow with the
	// Deliver activity, in the order of the versions, before the update
	// completes; the activity tries again until the stream takes them.
	commitCheck := func(in CommitInput) error {
		if !sameVersion(e.storedVersion(in.Collection, in.Key), in.Expected) {
			return conflictErr(e.storedVersion(in.Collection, in.Key))
		}
		return nil
	}
	if err := workflow.SetUpdateHandlerWithOptions(ctx, UpdateCommit,
		func(ctx workflow.Context, in CommitInput) (CommitResult, error) {
			if err := commitCheck(in); err != nil {
				return CommitResult{}, err
			}
			local := in.StreamTarget == e.wfID
			if local && len(in.Events) > 0 {
				if err := e.checkStream(in.Stream); err != nil {
					return CommitResult{}, err
				}
			}
			e.touch()
			var res CommitResult
			ticket := -1
			if len(in.Events) > 0 {
				if local {
					res.Nonce = e.chain.Nonce
					res.Seqs, res.Ats = e.appendEvents(ctx, in.Stream, in.Events)
				} else {
					ticket = e.nextTurn
					e.nextTurn++
				}
			}
			// write sets the document before it waits for the catalog.
			d, err := e.write(ctx, in.Collection, in.Key, in.Value, in.Attributes)
			if err == nil && ticket >= 0 {
				err = e.deliver(ctx, ticket, in, d.Version, &res)
			}
			if err != nil {
				return CommitResult{}, err
			}
			res.Version, res.UpdatedAt = d.Version, d.UpdatedAt
			return res, nil
		},
		workflow.UpdateHandlerOptions{Validator: func(ctx workflow.Context, in CommitInput) error {
			if err := e.open(ctx, in.Bound); err != nil {
				return err
			}
			if in.StreamTarget == e.wfID && len(in.Events) > 0 {
				if err := e.checkStream(in.Stream); err != nil {
					return err
				}
			}
			return commitCheck(in)
		}},
	); err != nil {
		return err
	}

	if err := workflow.SetUpdateHandlerWithOptions(ctx, UpdateDeleteStream,
		func(ctx workflow.Context, in DeleteStreamInput) (bool, error) {
			e.touch()
			deleted := e.chain.NextSeq > e.chain.KeptFrom
			e.chain.KeptFrom = e.chain.NextSeq
			e.minAt, e.maxAt = nil, nil
			var exports []string
			for _, r := range e.chain.Runs {
				if r.Export != "" {
					exports = append(exports, r.Export)
				}
			}
			e.chain.Runs = nil
			if err := workflow.ExecuteActivity(exportCtx(ctx), ActivityDeleteExports, DeleteExportsInput{Exports: exports, Chain: e.chain}).Get(ctx, nil); err != nil {
				return false, err
			}
			return deleted, nil
		},
		workflow.UpdateHandlerOptions{Validator: func(ctx workflow.Context, in DeleteStreamInput) error {
			return e.open(ctx, in.Bound)
		}},
	); err != nil {
		return err
	}

	// wait: the validator refuses with ErrReady when the condition is true
	// already, so a wait that ends at once adds no event.
	cond := func(in WaitInput) bool {
		if in.Stream != "" {
			last := e.chain.NextSeq - 1
			return last >= e.chain.KeptFrom && last > in.AfterSeq
		}
		return !sameVersion(e.storedVersion(in.Collection, in.Key), in.Version)
	}
	if err := workflow.SetUpdateHandlerWithOptions(ctx, UpdateWait,
		func(ctx workflow.Context, in WaitInput) (WaitResult, error) {
			e.touch()
			timeout := time.Duration(in.TimeoutMs) * time.Millisecond
			if _, err := workflow.AwaitWithTimeout(ctx, timeout, func() bool { return cond(in) || e.closing }); err != nil {
				return WaitResult{}, err
			}
			if !cond(in) {
				return WaitResult{Changed: false}, nil
			}
			if in.Stream != "" {
				return WaitResult{Changed: true, Seq: e.chain.NextSeq - 1, Nonce: e.chain.Nonce}, nil
			}
			return WaitResult{Changed: true, Version: e.storedVersion(in.Collection, in.Key)}, nil
		},
		workflow.UpdateHandlerOptions{Validator: func(ctx workflow.Context, in WaitInput) error {
			if err := e.open(ctx, in.Bound); err != nil {
				return err
			}
			if cond(in) {
				return temporal.NewNonRetryableApplicationError("The condition is true already.", ErrReady, nil)
			}
			return nil
		}},
	); err != nil {
		return err
	}

	return workflow.SetUpdateHandlerWithOptions(ctx, UpdateContinue,
		func(ctx workflow.Context, in ContinueInput) (string, error) {
			e.touch()
			e.force = true
			return e.runID, nil
		},
		workflow.UpdateHandlerOptions{Validator: func(ctx workflow.Context, in ContinueInput) error {
			return e.open(ctx, in.Bound)
		}},
	)
}

// deliver waits for the commit's turn, then sends its events to the
// stream's workflow.
func (e *entity) deliver(ctx workflow.Context, ticket int, in CommitInput, version string, res *CommitResult) error {
	defer func() { e.turn++ }()
	if err := workflow.Await(ctx, func() bool { return e.turn == ticket }); err != nil {
		return err
	}
	var ar AppendResult
	err := workflow.ExecuteActivity(deliverCtx(ctx), ActivityDeliver, DeliverInput{
		WorkflowID: in.StreamTarget, TaskQueue: in.StreamQueue, Stream: in.Stream, Events: in.Events,
		ID: e.wfID + "/" + version,
	}).Get(ctx, &ar)
	if err != nil {
		return err
	}
	res.Remote, res.Nonce, res.Seqs = true, ar.Nonce, ar.Seqs
	return nil
}

// deliveredErr refuses a delivery that the stream has already. Its detail
// is the AppendResult of the first delivery (with no times).
func (e *entity) deliveredErr(d Delivery) error {
	res := AppendResult{Nonce: e.chain.Nonce}
	for i := 0; i < d.Count; i++ {
		res.Seqs = append(res.Seqs, d.FirstSeq+int64(i))
	}
	return temporal.NewNonRetryableApplicationError("The stream has the events of this delivery already.", ErrDelivered, nil, res)
}

// loop waits until the run must continue as new (end false) or end (end true).
func (e *entity) loop(ctx workflow.Context) (bool, error) {
	mustContinue := func() bool {
		return e.force || e.updates >= e.runLimit || (e.updates >= MinUpdatesPerRun && workflow.GetInfo(ctx).GetContinueAsNewSuggested())
	}
	for {
		seen := e.activity
		idle := e.idle
		if !e.hasData() {
			idle = EmptyIdle
		}
		ok, err := workflow.AwaitWithTimeout(ctx, idle, func() bool { return mustContinue() || e.activity != seen })
		if err != nil {
			return false, err
		}
		if mustContinue() {
			return false, nil
		}
		if ok {
			continue
		}
		// The idle time passed with no update. A run with documents stays.
		if len(e.docs) == 0 {
			return true, nil
		}
	}
}

// close exports the run and continues as new, or ends.
func (e *entity) close(ctx workflow.Context, end bool) error {
	// From here every validator refuses with ErrRetry, so no event comes
	// after the export. The store sends a refused update again; the next
	// run takes it.
	e.closing = true
	if err := workflow.Await(ctx, func() bool { return workflow.AllHandlersFinished(ctx) }); err != nil {
		return err
	}

	in := ExportInput{WorkflowID: e.wfID, RunID: e.runID}
	if e.hasData() {
		rec := RunRecord{RunID: e.runID, Export: blobstore.ExportName(e.wfID, e.runID), MinAt: e.minAt, MaxAt: e.maxAt}
		if e.chain.NextSeq > e.runFirstSeq && e.chain.NextSeq-1 >= e.chain.KeptFrom {
			rec.FirstSeq = max(e.runFirstSeq, e.chain.KeptFrom)
			rec.LastSeq = e.chain.NextSeq - 1
		}
		e.chain.Runs = append(e.chain.Runs, rec)
	} else {
		in.SkipHistory = true
	}
	next := EntityInput{Chain: nil, RunLimit: e.runLimit, IdleMs: e.idle.Milliseconds(), InlineBytes: e.inlineBytes}
	docs := e.sortedDocs()
	if !end {
		size := 0
		for _, d := range docs {
			size += JSONSize(d)
		}
		limit := InlineStateBytes
		if e.inlineBytes > 0 {
			limit = e.inlineBytes
		}
		if size > limit {
			// The activity reads the documents from this run with queries.
			next.StateObject = blobstore.StateName(e.wfID, e.runID)
			in.StateObject = next.StateObject
		} else {
			next.Docs = docs
		}
	}
	in.Chain = e.chain
	if e.hasData() || in.StateObject != "" {
		if err := workflow.ExecuteActivity(exportCtx(ctx), ActivityExportRun, in).Get(ctx, nil); err != nil {
			return err
		}
	}
	if end {
		return nil
	}
	chain := e.chain
	next.Chain = &chain
	return workflow.NewContinueAsNewError(ctx, EntityWorkflow, next)
}
