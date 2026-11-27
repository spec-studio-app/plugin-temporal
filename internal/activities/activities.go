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
	"strings"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/temporalproto"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/proto"

	"github.com/spec-studio-app/plugin-temporal/internal/blobstore"
	"github.com/spec-studio-app/plugin-temporal/internal/route"
	"github.com/spec-studio-app/plugin-temporal/internal/workflows"
)

// Activities are the activities of one worker.
type Activities struct {
	Client client.Client
	// DC is the data converter of the worker's scope (with the codec). The
	// state and catalog objects are encrypted with it.
	DC     converter.DataConverter
	Blobs  blobstore.Store
	Router route.Router
	// CatalogRunLimit is the run limit of new catalogs (0: the default).
	CatalogRunLimit int
	// InlineBytes is the inline state limit of new catalogs (tests only).
	InlineBytes int
}

// Register adds the activities to a worker.
func (a *Activities) Register(w worker.Worker) {
	w.RegisterActivityWithOptions(a.CatalogPut, activity.RegisterOptions{Name: workflows.ActivityCatalogPut})
	w.RegisterActivityWithOptions(a.ExportRun, activity.RegisterOptions{Name: workflows.ActivityExportRun})
	w.RegisterActivityWithOptions(a.DeleteExports, activity.RegisterOptions{Name: workflows.ActivityDeleteExports})
	w.RegisterActivityWithOptions(a.LoadState, activity.RegisterOptions{Name: workflows.ActivityLoadState})
	w.RegisterActivityWithOptions(a.SaveCatalog, activity.RegisterOptions{Name: workflows.ActivitySaveCatalog})
	w.RegisterActivityWithOptions(a.LoadCatalog, activity.RegisterOptions{Name: workflows.ActivityLoadCatalog})
	w.RegisterActivityWithOptions(a.DeleteObject, activity.RegisterOptions{Name: workflows.ActivityDeleteObject})
}

// CatalogPut puts an entry in the catalog of its collection. It starts the
// catalog when it does not run.
func (a *Activities) CatalogPut(ctx context.Context, in workflows.CatalogPutInput) error {
	t := a.Router.Catalog(in.Collection)
	start := a.Client.NewWithStartWorkflowOperation(client.StartWorkflowOptions{
		ID:                       t.WorkflowID,
		TaskQueue:                t.TaskQueue,
		WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
	}, workflows.CatalogWorkflow, workflows.CatalogInput{WorkflowID: t.WorkflowID, Collection: in.Collection, RunLimit: a.CatalogRunLimit, InlineBytes: a.InlineBytes})
	in.Bound = workflows.Bound{Target: t.WorkflowID, AtMs: time.Now().UnixMilli()}
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
		if err := a.saveState(ctx, in); err != nil {
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

// queryRun runs a query on one run, and asks again while the run starts.
func (a *Activities) queryRun(ctx context.Context, workflowID, runID, name string, out any, args ...any) error {
	for {
		v, err := a.Client.QueryWorkflow(ctx, workflowID, runID, name, args...)
		if err == nil {
			return v.Get(out)
		}
		if !strings.Contains(err.Error(), workflows.QueryStarting) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// writeSealed writes items as Temporal payloads, encrypted by the scope's
// codec, so a state or catalog object in the blob store is never plain.
func (a *Activities) writeSealed(ctx context.Context, name string, items []any) error {
	ps := &commonpb.Payloads{}
	for _, it := range items {
		p, err := a.DC.ToPayload(it)
		if err != nil {
			return fmt.Errorf("cannot encrypt an item of %s", name)
		}
		ps.Payloads = append(ps.Payloads, p)
	}
	b, err := proto.Marshal(ps)
	if err != nil {
		return err
	}
	return a.Blobs.PutObject(ctx, name, bytesReader(b), int64(len(b)))
}

// readSealed reads an object that writeSealed wrote.
func (a *Activities) readSealed(ctx context.Context, name string) ([]*commonpb.Payload, error) {
	r, err := a.Blobs.GetObject(ctx, name)
	if errors.Is(err, blobstore.ErrNotFound) {
		return nil, temporal.NewNonRetryableApplicationError("The object "+name+" is not in the blob store.", "object_missing", nil)
	}
	if err != nil {
		return nil, err
	}
	defer r.Close()
	b, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	ps := &commonpb.Payloads{}
	if err := proto.Unmarshal(b, ps); err != nil {
		return nil, temporal.NewNonRetryableApplicationError("The object "+name+" is not valid.", "object_bad", nil)
	}
	return ps.Payloads, nil
}

// saveState reads the documents of the closing run one at a time with
// queries, and writes them encrypted. No payload holds the whole state.
func (a *Activities) saveState(ctx context.Context, in workflows.ExportInput) error {
	var refs []workflows.DocRef
	if err := a.queryRun(ctx, in.WorkflowID, in.RunID, workflows.QueryDocKeys, &refs); err != nil {
		return err
	}
	items := make([]any, 0, len(refs))
	for _, ref := range refs {
		var d *workflows.Doc
		if err := a.queryRun(ctx, in.WorkflowID, in.RunID, workflows.QueryDoc, &d, ref.Collection, ref.Key); err != nil {
			return err
		}
		if d != nil {
			items = append(items, *d)
		}
		activity.RecordHeartbeat(ctx, len(items))
	}
	return a.writeSealed(ctx, in.StateObject, items)
}

// LoadState gives one document of a state object, and the count.
func (a *Activities) LoadState(ctx context.Context, in workflows.LoadStateInput) (workflows.LoadStateResult, error) {
	ps, err := a.readSealed(ctx, in.Name)
	if err != nil {
		return workflows.LoadStateResult{}, err
	}
	res := workflows.LoadStateResult{Count: len(ps)}
	if in.Index < len(ps) {
		var d workflows.Doc
		if err := a.DC.FromPayload(ps[in.Index], &d); err != nil {
			return res, temporal.NewNonRetryableApplicationError("A document of the state object does not decrypt.", "object_bad", nil)
		}
		res.Doc = &d
	}
	return res, nil
}

// SaveCatalog reads every entry of a closing catalog run with queries, and
// writes them encrypted.
func (a *Activities) SaveCatalog(ctx context.Context, in workflows.SaveCatalogInput) error {
	var items []any
	after := ""
	for {
		var page []workflows.PageItem
		if err := a.queryRun(ctx, in.WorkflowID, in.RunID, workflows.QueryCatalogEntries, &page, workflows.EntriesQuery{After: after, Limit: 1000}); err != nil {
			return err
		}
		for _, it := range page {
			items = append(items, it)
			after = it.Key
		}
		activity.RecordHeartbeat(ctx, len(items))
		if len(page) < 1000 {
			break
		}
	}
	return a.writeSealed(ctx, in.Name, items)
}

// LoadCatalog gives one page of a saved catalog, in key order after After,
// of at most MaxBytes of JSON.
func (a *Activities) LoadCatalog(ctx context.Context, in workflows.LoadCatalogInput) (workflows.LoadCatalogResult, error) {
	ps, err := a.readSealed(ctx, in.Name)
	if err != nil {
		return workflows.LoadCatalogResult{}, err
	}
	out := workflows.LoadCatalogResult{Items: []workflows.PageItem{}}
	size := 0
	for _, p := range ps {
		var it workflows.PageItem
		if err := a.DC.FromPayload(p, &it); err != nil {
			return out, temporal.NewNonRetryableApplicationError("An entry of the catalog object does not decrypt.", "object_bad", nil)
		}
		if it.Key <= in.After {
			continue
		}
		n := workflows.JSONSize(it)
		if len(out.Items) > 0 && size+n > in.MaxBytes {
			out.More = true
			break
		}
		out.Items = append(out.Items, it)
		size += n
	}
	return out, nil
}

// DeleteObject removes a state or catalog object that a run loaded.
func (a *Activities) DeleteObject(ctx context.Context, name string) error {
	return a.Blobs.DeleteObject(ctx, name)
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
