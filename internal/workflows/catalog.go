package workflows

import (
	"encoding/json"
	"sort"
	"time"

	"go.temporal.io/sdk/workflow"
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

	if err := workflow.SetQueryHandler(ctx, QueryCatalogPage, func(q PageQuery) (Page, error) {
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
			if closing {
				return retryErr("The catalog is closing. Send the update again.")
			}
			return nil
		}},
	); err != nil {
		return err
	}

	if err := workflow.Await(ctx, func() bool {
		return updates >= runLimit || workflow.GetInfo(ctx).GetContinueAsNewSuggested()
	}); err != nil {
		return err
	}
	closing = true
	if err := workflow.Await(ctx, func() bool { return workflow.AllHandlersFinished(ctx) }); err != nil {
		return err
	}
	now := workflow.Now(ctx)
	for k, e := range entries {
		if e.Deleted && now.Sub(e.UpdatedAt) > tombstoneAge {
			delete(entries, k)
		}
	}
	return workflow.NewContinueAsNewError(ctx, CatalogWorkflow, CatalogInput{Collection: in.Collection, Entries: entries, RunLimit: in.RunLimit})
}
