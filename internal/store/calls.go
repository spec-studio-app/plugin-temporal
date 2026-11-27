package store

import (
	"context"
	"errors"

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

// update sends an update, and starts the workflow when no run runs and
// start is true. It sends the update again while the run starts or closes.
func (s *Store) update(ctx context.Context, t route.Target, name string, arg func() any, out any, start bool) error {
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
	in := workflows.EntityInput{Chain: chain, RunLimit: s.opts.RunLimit, IdleMs: s.opts.Idle.Milliseconds()}
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
// workflow does not exist.
func (s *Store) query(ctx context.Context, t route.Target, name string, out any, args ...any) error {
	v, err := s.clientFor(t).QueryWorkflow(ctx, t.WorkflowID, "", name, args...)
	if err != nil {
		if isNotFound(err) {
			return errNotRunning
		}
		return unavailable(err)
	}
	if err := v.Get(out); err != nil {
		return unavailable(err)
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
