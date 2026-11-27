package store

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/spec-studio-app/plugin-sdk/go/protocol"

	"github.com/spec-studio-app/plugin-temporal/internal/history"
	"github.com/spec-studio-app/plugin-temporal/internal/route"
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
	return context.WithTimeout(context.Background(), time.Second)
}

// docNow checks a document wait once, with its own short context: the
// protocol says that a wait whose condition is true already ends with
// changed true, also when the host's time is up.
func (s *Store) docNow(p protocol.WatchWaitParams, atTimeout bool) (protocol.WatchWaitResult, error) {
	c, cancel := checkCtx()
	defer cancel()
	d, err := s.getDoc(c, p.Collection, p.Key)
	if err != nil {
		if atTimeout {
			// The host's time is up and the check did not answer: the
			// wait ends as a timeout, as the protocol says.
			return protocol.WatchWaitResult{Changed: false}, nil
		}
		return protocol.WatchWaitResult{}, err
	}
	var v *string
	if d != nil {
		v = &d.Version
	}
	if (v == nil) != (p.Version == nil) || (v != nil && *v != *p.Version) {
		return protocol.WatchWaitResult{Changed: true, Version: v}, nil
	}
	return protocol.WatchWaitResult{Changed: false}, nil
}

// streamNow checks a stream wait once, with its own short context.
func (s *Store) streamNow(t route.Target, stream string, after int64, atTimeout bool) (protocol.WatchWaitResult, error) {
	c, cancel := checkCtx()
	defer cancel()
	st, err := s.streamState(c, t)
	if err != nil {
		if atTimeout {
			return protocol.WatchWaitResult{Changed: false}, nil
		}
		return protocol.WatchWaitResult{}, err
	}
	if st == nil || (st.chain.Stream != "" && st.chain.Stream != stream) {
		return protocol.WatchWaitResult{Changed: false}, nil
	}
	last := st.chain.NextSeq - 1
	if last >= st.chain.KeptFrom && last > after {
		cur := history.Cursor(st.chain.Nonce, last)
		return protocol.WatchWaitResult{Changed: true, Cursor: &cur}, nil
	}
	return protocol.WatchWaitResult{Changed: false}, nil
}

// waitTimedOut is true when a wait update ended because time ran out: the
// host's deadline, or Temporal's own limit of a long update call. The loop
// then checks again (once, at the host's deadline), and never gives an error.
// pause waits a short time before a wait is sent again, or until ctx ends.
func pause(ctx context.Context) {
	select {
	case <-ctx.Done():
	case <-time.After(50 * time.Millisecond):
	}
}

func waitTimedOut(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return true
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "Timeout or cancellation waiting for update") || strings.Contains(msg, "did not answer in time") || strings.Contains(msg, "deadline exceeded")
}

func (s *Store) waitDoc(ctx context.Context, p protocol.WatchWaitParams) (protocol.WatchWaitResult, error) {
	t := s.router.Doc(p.Collection, p.Key)
	for {
		if remaining(ctx) <= 50*time.Millisecond || ctx.Err() != nil {
			return s.docNow(p, true)
		}
		var res workflows.WaitResult
		arg := func() any {
			return workflows.WaitInput{Bound: bound(t), Collection: p.Collection, Key: p.Key, Version: p.Version, TimeoutMs: remaining(ctx).Milliseconds()}
		}
		// A wait for a document that does not exist yet starts its workflow.
		err := s.update(ctx, t, workflows.UpdateWait, arg, &res, p.Version == nil)
		switch {
		case errors.Is(err, errNotRunning):
			// No run: the document does not exist, and the host has a
			// version. A failed or stopped run is not "no document".
			if nerr := s.noDocument(ctx, t); nerr != nil {
				return protocol.WatchWaitResult{}, nerr
			}
			return protocol.WatchWaitResult{Changed: true, Version: nil}, nil
		case errors.Is(err, errReady):
			return s.docNow(p, false)
		case err != nil:
			if waitTimedOut(ctx, err) {
				pause(ctx)
				continue
			}
			return protocol.WatchWaitResult{}, err
		case res.Changed:
			return protocol.WatchWaitResult{Changed: true, Version: res.Version}, nil
		}
	}
}

func (s *Store) waitStream(ctx context.Context, p protocol.WatchWaitParams) (protocol.WatchWaitResult, error) {
	t := s.router.Stream(p.Stream)
	var after int64
	if p.After != nil {
		c, cancel := checkCtx()
		st, err := s.streamState(c, t)
		cancel()
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
		if remaining(ctx) <= 50*time.Millisecond || ctx.Err() != nil {
			return s.streamNow(t, p.Stream, after, true)
		}
		var res workflows.WaitResult
		arg := func() any {
			return workflows.WaitInput{Bound: bound(t), Stream: p.Stream, AfterSeq: after, TimeoutMs: remaining(ctx).Milliseconds()}
		}
		err := s.update(ctx, t, workflows.UpdateWait, arg, &res, true)
		switch {
		case errors.Is(err, errReady):
			return s.streamNow(t, p.Stream, after, false)
		case err != nil:
			if waitTimedOut(ctx, err) {
				pause(ctx)
				continue
			}
			return protocol.WatchWaitResult{}, err
		case res.Changed:
			c := history.Cursor(res.Nonce, res.Seq)
			return protocol.WatchWaitResult{Changed: true, Cursor: &c}, nil
		}
	}
}
