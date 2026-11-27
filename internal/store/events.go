package store

import (
	"context"
	"errors"
	"time"

	"github.com/spec-studio-app/plugin-sdk/go/protocol"

	"github.com/spec-studio-app/plugin-temporal/internal/activities"
	"github.com/spec-studio-app/plugin-temporal/internal/blobstore"
	"github.com/spec-studio-app/plugin-temporal/internal/history"
	"github.com/spec-studio-app/plugin-temporal/internal/route"
	"github.com/spec-studio-app/plugin-temporal/internal/workflows"
)

// Append is one update: the events go into the workflow history.
func (s *Store) Append(ctx context.Context, p protocol.EventsAppendParams) (protocol.EventsAppendResult, error) {
	t := s.router.Stream(p.Stream)
	evs := make([]workflows.NewEvent, len(p.Events))
	for i, e := range p.Events {
		evs[i] = workflows.NewEvent{Type: e.Type, Data: e.Data, At: e.At}
	}
	var res workflows.AppendResult
	arg := func() any { return workflows.AppendInput{Bound: bound(t), Stream: p.Stream, Events: evs} }
	if err := s.update(ctx, t, workflows.UpdateAppend, arg, &res, true); err != nil {
		return protocol.EventsAppendResult{}, err
	}
	out := protocol.EventsAppendResult{Cursors: make([]string, len(res.Seqs))}
	for i, seq := range res.Seqs {
		out.Cursors[i] = history.Cursor(res.Nonce, seq)
	}
	return out, nil
}

// streamState is what a read needs: the chain, and the live run when one runs.
type streamState struct {
	chain   workflows.Chain
	liveRun string
}

// state reads the chain from the running workflow, or from the manifest when
// no run runs. It returns nil when the stream never had a workflow.
func (s *Store) streamState(ctx context.Context, t route.Target) (*streamState, error) {
	_, running, err := s.running(ctx, t)
	if err != nil {
		return nil, err
	}
	if running {
		var info workflows.Info
		err := s.query(ctx, t, workflows.QueryInfo, &info)
		if err == nil {
			st := &streamState{chain: info.Chain, liveRun: info.RunID}
			for _, r := range info.Chain.Runs {
				if r.RunID == info.RunID {
					// The run is closing and is in its chain already.
					st.liveRun = ""
				}
			}
			return st, nil
		}
		if !errors.Is(err, errNotRunning) {
			return nil, err
		}
	}
	chain, err := activities.ReadChain(ctx, s.blobs, t.WorkflowID)
	if err != nil {
		return nil, protocol.NewError(protocol.Unavailable, "The plugin cannot read the chain of %s from the blob store.", t.WorkflowID)
	}
	if chain == nil {
		return nil, nil
	}
	return &streamState{chain: *chain}, nil
}

// runEvents reads the events of one run: from Temporal while Temporal has
// it, else from its export.
func (s *Store) runEvents(ctx context.Context, t route.Target, runID, export string) ([]history.Event, error) {
	events, err := history.FromTemporal(ctx, s.clientFor(t), t.WorkflowID, runID)
	if err != nil {
		if !isNotFound(err) {
			return nil, unavailable(err)
		}
		if export == "" {
			return nil, protocol.NewError(protocol.Unavailable, "The run %s of %s is gone from Temporal and has no export.", runID, t.WorkflowID)
		}
		events, err = history.FromExport(ctx, s.blobs, export)
		if errors.Is(err, blobstore.ErrNotFound) {
			return nil, protocol.NewError(protocol.Unavailable, "The run %s of %s is gone from Temporal, and its export is not in the blob store.", runID, t.WorkflowID)
		}
		if err != nil {
			return nil, protocol.NewError(protocol.Unavailable, "The plugin cannot read the export of run %s: %v", runID, err)
		}
	}
	out, err := history.FromHistory(events, s.dcFor(t))
	if err != nil {
		return nil, protocol.NewError(protocol.Unavailable, "The plugin cannot read the history of run %s: %v", runID, err)
	}
	return out, nil
}

func overlaps(r workflows.RunRecord, from, to *time.Time) bool {
	if r.MinAt == nil || r.MaxAt == nil {
		return true
	}
	if from != nil && r.MaxAt.Before(*from) {
		return false
	}
	if to != nil && !r.MinAt.Before(*to) {
		return false
	}
	return true
}

// Read follows the chain of runs. The host never sees a run boundary.
func (s *Store) Read(ctx context.Context, p protocol.EventsReadParams) (protocol.EventsReadResult, error) {
	limit := p.Limit
	if limit <= 0 {
		limit = 100
	}
	empty := protocol.EventsReadResult{Events: []protocol.Event{}, Cursor: p.After}
	t := s.router.Stream(p.Stream)
	st, err := s.streamState(ctx, t)
	if err != nil {
		return protocol.EventsReadResult{}, err
	}
	start := int64(1)
	if st != nil {
		start = st.chain.KeptFrom
	}
	if p.After != nil {
		nonce, seq, err := history.ParseCursor(*p.After)
		if err != nil || st == nil || nonce != st.chain.Nonce {
			return protocol.EventsReadResult{}, protocol.NewError(protocol.Refused, "The cursor in after is not a cursor of the stream %s.", p.Stream)
		}
		start = max(start, seq+1)
	}
	if st == nil {
		return empty, nil
	}

	match := func(e history.Event) bool {
		if e.Seq < start {
			return false
		}
		if p.From != nil && e.At.Before(*p.From) {
			return false
		}
		if p.To != nil && !e.At.Before(*p.To) {
			return false
		}
		return true
	}
	var found []history.Event
	enough := func() bool { return len(found) > limit }
	for _, r := range st.chain.Runs {
		if enough() {
			break
		}
		if r.Legacy || r.LastSeq == 0 || r.LastSeq < start || !overlaps(r, p.From, p.To) {
			continue
		}
		evs, err := s.runEvents(ctx, t, r.RunID, r.Export)
		if err != nil {
			return protocol.EventsReadResult{}, err
		}
		for _, e := range evs {
			if match(e) {
				found = append(found, e)
			}
		}
	}
	if !enough() && st.liveRun != "" {
		evs, err := s.runEvents(ctx, t, st.liveRun, "")
		if err != nil {
			return protocol.EventsReadResult{}, err
		}
		for _, e := range evs {
			if match(e) {
				found = append(found, e)
			}
		}
	}

	if len(found) == 0 {
		return empty, nil
	}
	more := len(found) > limit
	if more {
		found = found[:limit]
	}
	out := protocol.EventsReadResult{Events: make([]protocol.Event, len(found)), More: more}
	for i, e := range found {
		out.Events[i] = protocol.Event{Cursor: history.Cursor(st.chain.Nonce, e.Seq), Type: e.Type, At: e.At, Data: e.Data}
	}
	last := out.Events[len(out.Events)-1].Cursor
	out.Cursor = &last
	return out, nil
}

// DeleteStream removes every event of the stream and its exports.
func (s *Store) DeleteStream(ctx context.Context, p protocol.EventsDeleteParams) (protocol.EventsDeleteResult, error) {
	t := s.router.Stream(p.Stream)
	_, running, err := s.running(ctx, t)
	if err != nil {
		return protocol.EventsDeleteResult{}, err
	}
	if !running {
		chain, err := activities.ReadChain(ctx, s.blobs, t.WorkflowID)
		if err != nil {
			return protocol.EventsDeleteResult{}, protocol.NewError(protocol.Unavailable, "The plugin cannot read the chain of %s from the blob store.", t.WorkflowID)
		}
		if chain == nil || (chain.NextSeq <= chain.KeptFrom && len(chain.Runs) == 0) {
			return protocol.EventsDeleteResult{Deleted: false}, nil
		}
	}
	var deleted bool
	arg := func() any { return workflows.DeleteStreamInput{Bound: bound(t), Stream: p.Stream} }
	if err := s.update(ctx, t, workflows.UpdateDeleteStream, arg, &deleted, true); err != nil {
		return protocol.EventsDeleteResult{}, err
	}
	return protocol.EventsDeleteResult{Deleted: deleted}, nil
}

// ContinueStream makes the stream's run continue as new now (the test hook),
// and returns the new run's ID.
func (s *Store) ContinueStream(ctx context.Context, p protocol.ContinueStreamParams) (protocol.ContinueStreamResult, error) {
	t := s.router.Stream(p.Stream)
	var old string
	arg := func() any { return workflows.ContinueInput{Bound: bound(t)} }
	if err := s.update(ctx, t, workflows.UpdateContinue, arg, &old, true); err != nil {
		return protocol.ContinueStreamResult{}, err
	}
	deadline := time.Now().Add(time.Minute)
	for time.Now().Before(deadline) {
		runID, running, err := s.running(ctx, t)
		if err != nil {
			return protocol.ContinueStreamResult{}, err
		}
		if running && runID != old {
			return protocol.ContinueStreamResult{Run: runID}, nil
		}
		select {
		case <-ctx.Done():
			return protocol.ContinueStreamResult{}, unavailable(ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	return protocol.ContinueStreamResult{}, notRunningErr(t)
}
