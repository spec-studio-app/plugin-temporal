package store

import (
	"context"
	"errors"
	"strings"

	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"

	"github.com/spec-studio-app/plugin-sdk/go/protocol"

	"github.com/spec-studio-app/plugin-temporal/internal/activities"
	"github.com/spec-studio-app/plugin-temporal/internal/route"
	"github.com/spec-studio-app/plugin-temporal/internal/workflows"
)

// errReady is a wait whose condition was true already.
var errReady = errors.New("ready")

// errNotRunning is an update to a workflow with no running run.
var errNotRunning = errors.New("not running")

func (s *Store) clientFor(t route.Target) client.Client {
	if t.Scope == route.ScopeConfig {
		return s.config
	}
	return s.data
}

func (s *Store) dcFor(t route.Target) converter.DataConverter {
	if t.Scope == route.ScopeConfig {
		return s.configDC
	}
	return s.dataDC
}

func bound(t route.Target) workflows.Bound {
	return workflows.Bound{Target: t.WorkflowID, AtMs: time.Now().UnixMilli()}
}

func isNotFound(err error) bool {
	var nf *serviceerror.NotFound
	return errors.As(err, &nf)
}

func unavailable(err error) error {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return protocol.NewError(protocol.Unavailable, "Temporal did not answer in time. Try again.")
	}
	return protocol.NewError(protocol.Unavailable, "Temporal did not do the call: %s", errorText(err))
}

// errorText gives a short error text. Temporal's errors hold no payload.
func errorText(err error) string {
	var app *temporal.ApplicationError
	if errors.As(err, &app) {
		return app.Message()
	}
	return err.Error()
}

// classify maps a workflow error to a protocol error, errRetry or errReady.
func classify(err error) error {
	if err == nil {
		return nil
	}
	if isNotFound(err) {
		return errNotRunning
	}
	var app *temporal.ApplicationError
	if errors.As(err, &app) {
		// A worker of an older plugin-temporal took the workflow task and
		// rejected an update that it does not know. A rejected update writes
		// nothing.
		if name, ok := strings.CutPrefix(app.Message(), "unknown update "); ok {
			name, _, _ = strings.Cut(name, ".")
			return protocol.NewError(protocol.Unavailable, "A Temporal worker of this deploy runs an older plugin-temporal that does not know the update %s, so it wrote nothing. Update every plugin-temporal of the deploy (the app's and the relay's) to the same version.", name)
		}
		switch app.Type() {
		case workflows.ErrConflict:
			var stored *string
			if app.HasDetails() {
				_ = app.Details(&stored)
			}
			return protocol.ConflictError(stored)
		case workflows.ErrNotFound:
			return protocol.NewError(protocol.NotFound, "The document does not exist.")
		case workflows.ErrRetry:
			return errRetry
		case workflows.ErrReady:
			return errReady
		case workflows.ErrBadInput:
			return protocol.NewError(protocol.Refused, "%s", app.Message())
		case workflows.ErrBound:
			return protocol.NewError(protocol.Unavailable, "The workflow refused the update: %s Check the clock of this computer.", app.Message())
		}
	}
	return unavailable(err)
}

var errRetry = errors.New("retry")

// callTimeout is the longest time of one store call that has no deadline.
const callTimeout = 2 * time.Minute

// update sends an update, and starts the workflow when no run runs and
// start is true. It sends the update again while the run starts or closes.
func (s *Store) update(ctx context.Context, t route.Target, name string, arg func() any, out any, start bool) error {
	// A workflow that never answers (for example, its payloads use a key
	// that this plugin does not have) must not hold a call for ever.
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, callTimeout)
		defer cancel()
	}
	delay := 50 * time.Millisecond
	deadline := time.Now().Add(2 * time.Minute)
	for {
		err := classify(s.plainUpdate(ctx, t, name, arg(), out))
		if errors.Is(err, errNotRunning) {
			if !start {
				return errNotRunning
			}
			err = classify(s.startUpdate(ctx, t, name, arg(), out))
		}
		if !errors.Is(err, errRetry) && !errors.Is(err, errNotRunning) {
			return err
		}
		if time.Now().After(deadline) {
			return protocol.NewError(protocol.Unavailable, "The workflow %s did not take the update in time. Try again.", t.WorkflowID)
		}
		select {
		case <-ctx.Done():
			return unavailable(ctx.Err())
		case <-time.After(delay):
		}
		delay = min(delay*2, time.Second)
	}
}

func (s *Store) plainUpdate(ctx context.Context, t route.Target, name string, arg any, out any) error {
	h, err := s.clientFor(t).UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID:   t.WorkflowID,
		UpdateName:   name,
		Args:         []any{arg},
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		return err
	}
	return h.Get(ctx, out)
}

// startUpdate starts a run with the chain from the manifest (a new chain
// when there is none) and sends the update in the same call.
func (s *Store) startUpdate(ctx context.Context, t route.Target, name string, arg any, out any) error {
	chain, err := activities.ReadChain(ctx, s.blobs, t.WorkflowID)
	if err != nil {
		return protocol.NewError(protocol.Unavailable, "The plugin cannot read the chain of %s from the blob store.", t.WorkflowID)
	}
	c := s.clientFor(t)
	in := workflows.EntityInput{Chain: chain, RunLimit: s.opts.RunLimit, IdleMs: s.opts.Idle.Milliseconds(), InlineBytes: s.opts.InlineBytes}
	op := c.NewWithStartWorkflowOperation(client.StartWorkflowOptions{
		ID:                       t.WorkflowID,
		TaskQueue:                t.TaskQueue,
		WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		WorkflowIDReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	}, workflows.EntityWorkflow, in)
	h, err := c.UpdateWithStartWorkflow(ctx, client.UpdateWithStartWorkflowOptions{
		StartWorkflowOperation: op,
		UpdateOptions: client.UpdateWorkflowOptions{
			WorkflowID:   t.WorkflowID,
			UpdateName:   name,
			Args:         []any{arg},
			WaitForStage: client.WorkflowUpdateStageCompleted,
		},
	})
	if err != nil {
		return err
	}
	return h.Get(ctx, out)
}

// query runs a query on the latest run. It returns errNotRunning when the
// workflow does not exist, or when its latest run is a closed run of another
// workflow type (a relay workflow that ended before the handoff: no worker
// of this plugin can answer for it, and it holds no plugin document). It
// asks again while the run loads its state.
func (s *Store) query(ctx context.Context, t route.Target, name string, out any, args ...any) error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, callTimeout)
		defer cancel()
	}
	delay := 50 * time.Millisecond
	for {
		v, err := s.clientFor(t).QueryWorkflow(ctx, t.WorkflowID, "", name, args...)
		if err == nil {
			if err := v.Get(out); err != nil {
				return unavailable(err)
			}
			return nil
		}
		if isNotFound(err) {
			return errNotRunning
		}
		// A closed latest run holds no document: a relay run that ended
		// before the handoff, a run that refused its input, or a run that
		// ended with no data.
		state, derr := s.runState(ctx, t)
		if derr == nil && state == runEmpty {
			return errNotRunning
		}
		if !strings.Contains(err.Error(), workflows.QueryStarting) {
			return unavailable(err)
		}
		if derr == nil && state == runStopped {
			// The run stopped while it loaded its state: it never becomes ready.
			return protocol.NewError(protocol.Unavailable, "The workflow %s stopped while it loaded its state. Look at the workflow in Temporal.", t.WorkflowID)
		}
		select {
		case <-ctx.Done():
			return unavailable(ctx.Err())
		case <-time.After(delay):
		}
		delay = min(delay*2, time.Second)
	}
}

// The states of the latest run of a workflow, for a reader.
const (
	// runLive: the run runs.
	runLive = iota
	// runEmpty: no run, or a run that holds no document for sure: a plugin
	// run that completed (it ends only with no document, or when it refused
	// its input), or a closed relay run from before the handoff.
	runEmpty
	// runStopped: a plugin run that failed or was stopped. It can still
	// have documents (in its memory or in a state object), so a reader
	// answers unavailable, never "no document".
	runStopped
)

func (s *Store) runState(ctx context.Context, t route.Target) (int, error) {
	d, err := s.clientFor(t).DescribeWorkflowExecution(ctx, t.WorkflowID, "")
	if err != nil {
		if isNotFound(err) {
			return runEmpty, nil
		}
		return runLive, err
	}
	info := d.GetWorkflowExecutionInfo()
	status := info.GetStatus()
	if status == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		return runLive, nil
	}
	ty := info.GetType().GetName()
	if ty != workflows.EntityWorkflow && ty != workflows.CatalogWorkflow {
		return runEmpty, nil
	}
	if status == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED {
		return runEmpty, nil
	}
	return runStopped, nil
}

// noDocument is called when an update found no running run. It gives nil
// when the workflow holds no document for sure, else an unavailable error.
func (s *Store) noDocument(ctx context.Context, t route.Target) error {
	c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if ctx.Err() == nil {
		c = ctx
	}
	state, err := s.runState(c, t)
	if err != nil {
		return unavailable(err)
	}
	if state == runStopped {
		return protocol.NewError(protocol.Unavailable, "The workflow %s failed or was stopped. Look at the workflow in Temporal.", t.WorkflowID)
	}
	return nil
}

// running is true when the workflow has a running run.
func (s *Store) running(ctx context.Context, t route.Target) (string, bool, error) {
	d, err := s.clientFor(t).DescribeWorkflowExecution(ctx, t.WorkflowID, "")
	if err != nil {
		if isNotFound(err) {
			return "", false, nil
		}
		return "", false, unavailable(err)
	}
	info := d.GetWorkflowExecutionInfo()
	return info.GetExecution().GetRunId(), info.GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, nil
}

func notRunningErr(t route.Target) error {
	return protocol.NewError(protocol.Unavailable, "The workflow %s does not run.", t.WorkflowID)
}
