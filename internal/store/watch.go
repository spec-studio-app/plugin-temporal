package store

import (
	"context"
	"errors"
	"time"

	"github.com/spec-studio-app/plugin-sdk/go/protocol"

	"github.com/spec-studio-app/plugin-temporal/internal/history"
	"github.com/spec-studio-app/plugin-temporal/internal/workflows"
)

// Wait is a wait update on the document's or the stream's workflow. A run
// that ends or continues as new ends its waits; the store then waits again
// on the next run, until the timeout.
func (s *Store) Wait(ctx context.Context, p protocol.WatchWaitParams) (protocol.WatchWaitResult, error) {
	if p.IsStream() {
		return s.waitStream(ctx, p)
	}
	return s.waitDoc(ctx, p)
}

func remaining(ctx context.Context) time.Duration {
	if d, ok := ctx.Deadline(); ok {
		return time.Until(d)
	}
	return time.Minute
}

// checkCtx gives a short context for the one check of a wait whose own
// timeout is too short for an update: the protocol says that a wait whose
// condition is true already ends at once with changed true.
func checkCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 5*time.Second)
}

func (s *Store) waitDoc(ctx context.Context, p protocol.WatchWaitParams) (protocol.WatchWaitResult, error) {
	t := s.router.Doc(p.Collection, p.Key)
	changedTo := func(v *string) protocol.WatchWaitResult { return protocol.WatchWaitResult{Changed: true, Version: v} }
	for {
		left := remaining(ctx)
		if left <= 50*time.Millisecond || ctx.Err() != nil {
			c, cancel := checkCtx()
			defer cancel()
			d, err := s.getDoc(c, p.Collection, p.Key)
			if err != nil {
				return protocol.WatchWaitResult{Changed: false}, nil
			}
			var v *string
			if d != nil {
				v = &d.Version
			}
			if (v == nil) != (p.Version == nil) || (v != nil && *v != *p.Version) {
				return changedTo(v), nil
			}
			return protocol.WatchWaitResult{Changed: false}, nil
		}
		var res workflows.WaitResult
		arg := func() any {
			return workflows.WaitInput{Bound: bound(t), Collection: p.Collection, Key: p.Key, Version: p.Version, TimeoutMs: remaining(ctx).Milliseconds()}
		}
		// A wait for a document that does not exist yet starts its workflow.
		err := s.update(ctx, t, workflows.UpdateWait, arg, &res, p.Version == nil)
		switch {
		case errors.Is(err, errNotRunning):
			// No run: the document does not exist, and the host has a version.
			return changedTo(nil), nil
		case errors.Is(err, errReady):
			d, err := s.getDoc(ctx, p.Collection, p.Key)
			if err != nil {
				return protocol.WatchWaitResult{}, err
			}
			if d == nil {
				return changedTo(nil), nil
			}
			v := d.Version
			return changedTo(&v), nil
		case err != nil:
			if ctx.Err() != nil {
				return protocol.WatchWaitResult{Changed: false}, nil
			}
			return protocol.WatchWaitResult{}, err
		case res.Changed:
			return changedTo(res.Version), nil
		}
	}
}

func (s *Store) waitStream(ctx context.Context, p protocol.WatchWaitParams) (protocol.WatchWaitResult, error) {
	t := s.router.Stream(p.Stream)
	var after int64
	if p.After != nil {
		c := ctx
		if remaining(ctx) <= 50*time.Millisecond {
			var cancel context.CancelFunc
			c, cancel = checkCtx()
			defer cancel()
		}
		st, err := s.streamState(c, t)
		if err != nil {
			return protocol.WatchWaitResult{}, err
		}
		nonce, seq, perr := history.ParseCursor(*p.After)
		if perr != nil || st == nil || nonce != st.chain.Nonce {
			return protocol.WatchWaitResult{}, protocol.NewError(protocol.Refused, "The cursor in after is not a cursor of the stream %s.", p.Stream)
		}
		after = seq
	}
	for {
		left := remaining(ctx)
		if left <= 50*time.Millisecond || ctx.Err() != nil {
			c, cancel := checkCtx()
			defer cancel()
			st, err := s.streamState(c, t)
			if err != nil || st == nil || (st.chain.Stream != "" && st.chain.Stream != p.Stream) {
				return protocol.WatchWaitResult{Changed: false}, nil
			}
			last := st.chain.NextSeq - 1
			if last >= st.chain.KeptFrom && last > after {
				cur := history.Cursor(st.chain.Nonce, last)
				return protocol.WatchWaitResult{Changed: true, Cursor: &cur}, nil
			}
			return protocol.WatchWaitResult{Changed: false}, nil
		}
		var res workflows.WaitResult
		arg := func() any {
			return workflows.WaitInput{Bound: bound(t), Stream: p.Stream, AfterSeq: after, TimeoutMs: remaining(ctx).Milliseconds()}
		}
		err := s.update(ctx, t, workflows.UpdateWait, arg, &res, true)
		switch {
		case errors.Is(err, errReady):
			st, err := s.streamState(ctx, t)
			if err != nil {
				return protocol.WatchWaitResult{}, err
			}
			if st == nil {
				continue
			}
			c := history.Cursor(st.chain.Nonce, st.chain.NextSeq-1)
			return protocol.WatchWaitResult{Changed: true, Cursor: &c}, nil
		case err != nil:
			if ctx.Err() != nil {
				return protocol.WatchWaitResult{Changed: false}, nil
			}
			return protocol.WatchWaitResult{}, err
		case res.Changed:
			c := history.Cursor(res.Nonce, res.Seq)
			return protocol.WatchWaitResult{Changed: true, Cursor: &c}, nil
		}
	}
}
