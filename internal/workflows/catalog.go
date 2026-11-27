package workflows

import (
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/spec-studio-app/plugin-temporal/internal/blobstore"
)

// tombstoneAge is how long a deleted key stays as a tombstone, so that a
// late entry for it cannot bring it back.
const tombstoneAge = time.Hour

// newer is true when the entry a is newer than b.
func newer(a, b CatalogEntry) bool {
	if !a.UpdatedAt.Equal(b.UpdatedAt) {
		return a.UpdatedAt.After(b.UpdatedAt)
	}
	return a.N > b.N
}

// attrEqual compares two attribute values as JSON values. A missing
// attribute is null.
func attrEqual(a, b any) bool {
	ja, _ := json.Marshal(a)
	jb, _ := json.Marshal(b)
	return string(ja) == string(jb)
}

// Matches is true when the entry has each attribute of the filter.
func Matches(attrs, filter map[string]any) bool {
	for k, want := range filter {
		if !attrEqual(attrs[k], want) {
			return false
		}
	}
	return true
}

// Catalog is the catalog workflow: the list index of one collection.
func Catalog(ctx workflow.Context, in CatalogInput) error {
	info := workflow.GetInfo(ctx)
	wfID := info.WorkflowExecution.ID
	// A start input copied from another catalog (another collection or
	// another deploy): end at once.
	if in.WorkflowID != wfID || !strings.HasSuffix(wfID, "/"+in.Collection) {
		workflow.GetLogger(ctx).Warn("The start input is for another catalog. The run ends with no data.")
		return nil
	}
	if in.EntriesObject != "" && !strings.HasPrefix(in.EntriesObject, blobstore.CatalogPrefix(wfID)) {
		workflow.GetLogger(ctx).Warn("The entries object of the start input is for another catalog. The run ends with no data.")
		return nil
	}
	entries := in.Entries
	if entries == nil {
		entries = map[string]CatalogEntry{}
	}
	runLimit := in.RunLimit
	if runLimit <= 0 {
		runLimit = DefaultRunLimit
	}
	updates := 0
	closing := false
	ready := in.EntriesObject == ""
	starting := errors.New(QueryStarting)

	if err := workflow.SetQueryHandler(ctx, QueryCatalogPage, func(q PageQuery) (Page, error) {
		if !ready {
			return Page{}, starting
		}
		keys := make([]string, 0, len(entries))
		for k, e := range entries {
			if !e.Deleted && k > q.After && Matches(e.Attributes, q.Filter) {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		limit := q.Limit
		if limit <= 0 {
			limit = 100
		}
		page := Page{Items: []PageItem{}}
		for i, k := range keys {
			if i == limit {
				page.Next = keys[i-1]
				break
			}
			page.Items = append(page.Items, PageItem{Key: k, Entry: entries[k]})
		}
		return page, nil
	}); err != nil {
		return err
	}
	if err := workflow.SetQueryHandler(ctx, QueryCatalogEntries, func(q EntriesQuery) ([]PageItem, error) {
		if !ready {
			return nil, starting
		}
		keys := make([]string, 0, len(entries))
		for k := range entries {
			if k > q.After {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		out := []PageItem{}
		for _, k := range keys {
			if len(out) == q.Limit {
				break
			}
			out = append(out, PageItem{Key: k, Entry: entries[k]})
		}
		return out, nil
	}); err != nil {
		return err
	}

	if err := workflow.SetUpdateHandlerWithOptions(ctx, UpdateCatalogPut,
		func(ctx workflow.Context, p CatalogPutInput) (bool, error) {
			updates++
			old, ok := entries[p.Key]
			if ok && !newer(p.Entry, old) {
				return false, nil
			}
			entries[p.Key] = p.Entry
			return true, nil
		},
		workflow.UpdateHandlerOptions{Validator: func(ctx workflow.Context, p CatalogPutInput) error {
			if !ready {
				return retryErr("The catalog is starting. Send the update again.")
			}
			if closing {
				return retryErr("The catalog is closing. Send the update again.")
			}
			if err := checkBound(ctx, wfID, p.Bound); err != nil {
				return err
			}
			if p.Collection != in.Collection {
				return temporal.NewNonRetryableApplicationError("The entry is for another collection.", ErrBadInput, nil)
			}
			return nil
		}},
	); err != nil {
		return err
	}

	// The entries of the last run from the blob store, one page per activity.
	if in.EntriesObject != "" {
		after := ""
		for {
			var page LoadCatalogResult
			if err := workflow.ExecuteActivity(exportCtx(ctx), ActivityLoadCatalog, LoadCatalogInput{Name: in.EntriesObject, After: after, MaxBytes: LoadPageBytes}).Get(ctx, &page); err != nil {
				return err
			}
			for _, it := range page.Items {
				entries[it.Key] = it.Entry
				after = it.Key
			}
			if !page.More {
				break
			}
		}
		// The run has the entries: the object of the last run goes.
		if err := workflow.ExecuteActivity(exportCtx(ctx), ActivityDeleteObject, in.EntriesObject).Get(ctx, nil); err != nil {
			return err
		}
		ready = true
	}

	if err := workflow.Await(ctx, func() bool {
		return updates >= runLimit || (updates >= MinUpdatesPerRun && workflow.GetInfo(ctx).GetContinueAsNewSuggested())
	}); err != nil {
		return err
	}
	closing = true
	if err := workflow.Await(ctx, func() bool { return workflow.AllHandlersFinished(ctx) }); err != nil {
		return err
	}
	now := workflow.Now(ctx)
	size := 0
	for k, e := range entries {
		if e.Deleted && now.Sub(e.UpdatedAt) > tombstoneAge {
			delete(entries, k)
			continue
		}
		size += len(k) + JSONSize(e) + 8
	}
	next := CatalogInput{WorkflowID: wfID, Collection: in.Collection, RunLimit: in.RunLimit, InlineBytes: in.InlineBytes}
	limit := InlineStateBytes
	if in.InlineBytes > 0 {
		limit = in.InlineBytes
	}
	if size > limit {
		// Too large for the continue-as-new input: the activity reads the
		// entries with queries and writes them encrypted to the blob store.
		next.EntriesObject = blobstore.CatalogName(wfID, info.WorkflowExecution.RunID)
		if err := workflow.ExecuteActivity(exportCtx(ctx), ActivitySaveCatalog, SaveCatalogInput{WorkflowID: wfID, RunID: info.WorkflowExecution.RunID, Name: next.EntriesObject}).Get(ctx, nil); err != nil {
			return err
		}
	} else {
		next.Entries = entries
	}
	return workflow.NewContinueAsNewError(ctx, CatalogWorkflow, next)
}
