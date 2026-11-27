// Package activities holds the activities of the entity workflow: the
// catalog write, the history export and the chain manifest. Each worker has
// its own Activities, with the client of its payload key scope.
package activities

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/temporalproto"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"

	"github.com/spec-studio-app/plugin-temporal/internal/blobstore"
	"github.com/spec-studio-app/plugin-temporal/internal/route"
	"github.com/spec-studio-app/plugin-temporal/internal/workflows"
)

// Activities are the activities of one worker.
type Activities struct {
	Client client.Client
	Blobs  blobstore.Store
	Router route.Router
	// CatalogRunLimit is the run limit of new catalogs (0: the default).
	CatalogRunLimit int
}

// Register adds the activities to a worker.
func (a *Activities) Register(w worker.Worker) {
	w.RegisterActivityWithOptions(a.CatalogPut, activity.RegisterOptions{Name: workflows.ActivityCatalogPut})
	w.RegisterActivityWithOptions(a.ExportRun, activity.RegisterOptions{Name: workflows.ActivityExportRun})
	w.RegisterActivityWithOptions(a.DeleteExports, activity.RegisterOptions{Name: workflows.ActivityDeleteExports})
	w.RegisterActivityWithOptions(a.LoadState, activity.RegisterOptions{Name: workflows.ActivityLoadState})
}

// CatalogPut puts an entry in the catalog of its collection. It starts the
// catalog when it does not run.
func (a *Activities) CatalogPut(ctx context.Context, in workflows.CatalogPutInput) error {
	t := a.Router.Catalog(in.Collection)
	start := a.Client.NewWithStartWorkflowOperation(client.StartWorkflowOptions{
		ID:                       t.WorkflowID,
		TaskQueue:                t.TaskQueue,
		WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
	}, workflows.CatalogWorkflow, workflows.CatalogInput{Collection: in.Collection, RunLimit: a.CatalogRunLimit})
	h, err := a.Client.UpdateWithStartWorkflow(ctx, client.UpdateWithStartWorkflowOptions{
		StartWorkflowOperation: start,
		UpdateOptions: client.UpdateWorkflowOptions{
			WorkflowID:   t.WorkflowID,
			UpdateName:   workflows.UpdateCatalogPut,
			Args:         []any{in},
			WaitForStage: client.WorkflowUpdateStageCompleted,
		},
	})
	if err != nil {
		return err
	}
	return h.Get(ctx, nil)
}

// ExportRun writes the history of one run to the blob store, as gzipped
// Temporal JSON (the format of `temporal workflow show --output json`), then
// the documents (when the input has them) and the chain manifest. The
// payloads stay encrypted, as Temporal keeps them.
func (a *Activities) ExportRun(ctx context.Context, in workflows.ExportInput) (string, error) {
	name := blobstore.ExportName(in.WorkflowID, in.RunID)
	if !in.SkipHistory {
		if err := a.exportHistory(ctx, in.WorkflowID, in.RunID, name); err != nil {
			var nf *serviceerror.NotFound
			if !errors.As(err, &nf) {
				return "", err
			}
			// The run is gone already: nothing to export. This happens only
			// for a relay run whose history the namespace removed.
			activity.GetLogger(ctx).Warn("The run to export is not in Temporal.", "workflowId", in.WorkflowID, "runId", in.RunID)
			name = ""
		}
	}
	if in.StateObject != "" {
		if err := a.putJSON(ctx, in.StateObject, in.Docs); err != nil {
			return "", err
		}
	}
	if err := a.putJSON(ctx, blobstore.ChainName(in.WorkflowID), in.Chain); err != nil {
		return "", err
	}
	return name, nil
}

func (a *Activities) exportHistory(ctx context.Context, workflowID, runID, name string) error {
	tmp, err := os.CreateTemp("", "plugin-temporal-export-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	defer tmp.Close()

	gz := gzip.NewWriter(tmp)
	w := bufio.NewWriter(gz)
	if _, err := io.WriteString(w, `{"events":[`); err != nil {
		return err
	}
	it := a.Client.GetWorkflowHistory(ctx, workflowID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	opts := temporalproto.CustomJSONMarshalOptions{}
	n := 0
	for it.HasNext() {
		ev, err := it.Next()
		if err != nil {
			return err
		}
		b, err := opts.Marshal(ev)
		if err != nil {
			return err
		}
		if n > 0 {
			if err := w.WriteByte(','); err != nil {
				return err
			}
		}
		if _, err := w.Write(b); err != nil {
			return err
		}
		n++
		if n%200 == 0 {
			activity.RecordHeartbeat(ctx, n)
		}
	}
	if n == 0 {
		return serviceerror.NewNotFound("the run has no history")
	}
	if _, err := io.WriteString(w, "]}"); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}
	if err := gz.Close(); err != nil {
		return err
	}
	size, err := tmp.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if err := a.Blobs.PutObject(ctx, name, tmp, size); err != nil {
		return fmt.Errorf("cannot write the export: %w", err)
	}
	return nil
}

func (a *Activities) putJSON(ctx context.Context, name string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return a.Blobs.PutObject(ctx, name, bytesReader(b), int64(len(b)))
}

// LoadState reads the documents that the last run wrote to the blob store.
func (a *Activities) LoadState(ctx context.Context, name string) ([]workflows.Doc, error) {
	r, err := a.Blobs.GetObject(ctx, name)
	if errors.Is(err, blobstore.ErrNotFound) {
		return nil, temporal.NewNonRetryableApplicationError("The state of the last run is not in the blob store.", "state_missing", nil)
	}
	if err != nil {
		return nil, err
	}
	defer r.Close()
	var docs []workflows.Doc
	if err := json.NewDecoder(r).Decode(&docs); err != nil {
		return nil, err
	}
	return docs, nil
}

// DeleteExports removes exports and writes the manifest.
func (a *Activities) DeleteExports(ctx context.Context, in workflows.DeleteExportsInput) error {
	for _, name := range in.Exports {
		if err := a.Blobs.DeleteObject(ctx, name); err != nil {
			return err
		}
	}
	return a.putJSON(ctx, blobstore.ChainName(in.Chain.WorkflowID), in.Chain)
}

// ReadChain reads a workflow's chain manifest. It returns nil when there is none.
func ReadChain(ctx context.Context, blobs blobstore.Store, workflowID string) (*workflows.Chain, error) {
	r, err := blobs.GetObject(ctx, blobstore.ChainName(workflowID))
	if errors.Is(err, blobstore.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer r.Close()
	var c workflows.Chain
	if err := json.NewDecoder(r).Decode(&c); err != nil {
		return nil, fmt.Errorf("the chain manifest of %s is not valid JSON", workflowID)
	}
	return &c, nil
}
