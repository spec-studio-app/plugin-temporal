package store

import (
	"context"
	"errors"
	"strings"
	"sync"

	"github.com/spec-studio-app/plugin-sdk/go/protocol"

	"github.com/spec-studio-app/plugin-temporal/internal/history"
	"github.com/spec-studio-app/plugin-temporal/internal/workflows"
)

// Get queries the document from its workflow.
func (s *Store) Get(ctx context.Context, p protocol.StateGetParams) (protocol.StateGetResult, error) {
	d, err := s.getDoc(ctx, p.Collection, p.Key)
	if err != nil || d == nil {
		return protocol.StateGetResult{Exists: false}, err
	}
	t := d.UpdatedAt
	return protocol.StateGetResult{Exists: true, Value: d.Value, Attributes: protocol.Attributes(d.Attributes), Version: d.Version, UpdatedAt: &t}, nil
}

func (s *Store) getDoc(ctx context.Context, collection, key string) (*workflows.Doc, error) {
	var d *workflows.Doc
	err := s.query(ctx, s.router.Doc(collection, key), workflows.QueryDoc, &d, collection, key)
	if errors.Is(err, errNotRunning) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if d != nil && d.Attributes == nil {
		d.Attributes = map[string]any{}
	}
	return d, nil
}

// Put is one update: the workflow checks the expected version and writes in
// one step.
func (s *Store) Put(ctx context.Context, p protocol.StatePutParams) (protocol.StatePutResult, error) {
	t := s.router.Doc(p.Collection, p.Key)
	var res workflows.PutResult
	arg := func() any {
		return workflows.PutInput{Bound: bound(t), Collection: p.Collection, Key: p.Key, Value: p.Value, Attributes: p.Attributes, Expected: p.ExpectedVersion}
	}
	if err := s.update(ctx, t, workflows.UpdatePut, arg, &res, true); err != nil {
		return protocol.StatePutResult{}, err
	}
	return protocol.StatePutResult{Version: res.Version, UpdatedAt: res.UpdatedAt}, nil
}

// Delete is one update.
func (s *Store) Delete(ctx context.Context, p protocol.StateDeleteParams) (protocol.StateDeleteResult, error) {
	t := s.router.Doc(p.Collection, p.Key)
	var deleted bool
	arg := func() any {
		return workflows.DeleteInput{Bound: bound(t), Collection: p.Collection, Key: p.Key, Expected: p.ExpectedVersion}
	}
	err := s.update(ctx, t, workflows.UpdateDelete, arg, &deleted, false)
	if errors.Is(err, errNotRunning) {
		if nerr := s.noDocument(ctx, t); nerr != nil {
			return protocol.StateDeleteResult{}, nerr
		}
		return protocol.StateDeleteResult{}, protocol.NewError(protocol.NotFound, "The document does not exist.")
	}
	if err != nil {
		return protocol.StateDeleteResult{}, err
	}
	return protocol.StateDeleteResult{Deleted: true}, nil
}

// listConcurrency is the most document queries of one list page at a time.
const listConcurrency = 16

// List queries the collection's catalog for a page of keys. With values, it
// queries each document's workflow too.
func (s *Store) List(ctx context.Context, p protocol.StateListParams) (protocol.StateListResult, error) {
	pageSize := p.PageSize
	if pageSize <= 0 {
		pageSize = 100
	}
	q := workflows.PageQuery{Filter: p.Filter, Limit: pageSize}
	if p.Cursor != nil {
		q.After = *p.Cursor
	}
	var page workflows.Page
	err := s.query(ctx, s.router.Catalog(p.Collection), workflows.QueryCatalogPage, &page, q)
	if errors.Is(err, errNotRunning) {
		return protocol.StateListResult{Items: []protocol.Document{}}, nil
	}
	if err != nil {
		return protocol.StateListResult{}, err
	}

	items := make([]*protocol.Document, len(page.Items))
	for i, it := range page.Items {
		attrs := it.Entry.Attributes
		if attrs == nil {
			attrs = map[string]any{}
		}
		items[i] = &protocol.Document{Key: it.Key, Attributes: attrs, Version: it.Entry.Version, UpdatedAt: it.Entry.UpdatedAt}
	}
	if p.WithValues {
		var wg sync.WaitGroup
		var mu sync.Mutex
		var firstErr error
		sem := make(chan struct{}, listConcurrency)
		for i := range items {
			wg.Add(1)
			sem <- struct{}{}
			go func(i int) {
				defer wg.Done()
				defer func() { <-sem }()
				d, err := s.getDoc(ctx, p.Collection, items[i].Key)
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					if firstErr == nil {
						firstErr = err
					}
					return
				}
				if d == nil {
					// Deleted after the catalog page: leave it out.
					items[i] = nil
					return
				}
				// The document can be newer than its catalog entry: show one
				// version, the document's.
				if !workflows.Matches(d.Attributes, p.Filter) {
					items[i] = nil
					return
				}
				items[i] = &protocol.Document{Key: d.Key, Attributes: d.Attributes, Version: d.Version, UpdatedAt: d.UpdatedAt, Value: d.Value}
			}(i)
		}
		wg.Wait()
		if firstErr != nil {
			return protocol.StateListResult{}, firstErr
		}
	}
	out := protocol.StateListResult{Items: []protocol.Document{}}
	for _, it := range items {
		if it != nil {
			out.Items = append(out.Items, *it)
		}
	}
	if page.Next != "" {
		next := page.Next
		out.Cursor = &next
	}
	return out, nil
}

// Commit is state.commit: one commit update on the document's workflow. The
// update checks the expected version, writes the document and appends the
// events. A stale version gets conflict, and the update changes nothing.
//
// A spec's document (app-specs, specs or closed-specs, key
// "<repoKey>:<slug>") and its stream "spec:<repoKey>:<slug>" have the same
// workflow (see the route package). Then the document and the events change
// in one workflow task, and the history holds both or neither. For a
// document and a stream with two workflows, the update sends the events to
// the stream's workflow first, and writes the document only after the
// delivery, so a failed commit writes nothing (see the commit update in the
// workflows package).
func (s *Store) Commit(ctx context.Context, p protocol.StateCommitParams) (protocol.StateCommitResult, error) {
	t := s.router.Doc(p.Collection, p.Key)
	st := s.router.Stream(p.Stream)
	if len(p.Events) > 0 && st.WorkflowID != t.WorkflowID && st.Scope != t.Scope {
		// The two workflows have different payload keys: the document's
		// worker cannot write to the stream's workflow.
		return protocol.StateCommitResult{}, protocol.NewError(protocol.Refused, "The plugin cannot commit a document of the collection %s with events: the document and the stream use different payload keys.", p.Collection)
	}
	evs := make([]workflows.NewEvent, len(p.Events))
	for i, e := range p.Events {
		evs[i] = workflows.NewEvent{Type: e.Type, Data: e.Data, At: e.At}
	}
	var res workflows.CommitResult
	arg := func() any {
		return workflows.CommitInput{
			Bound: bound(t), Collection: p.Collection, Key: p.Key, Value: p.Value, Attributes: p.Attributes, Expected: p.ExpectedVersion,
			Stream: p.Stream, Events: evs, StreamTarget: st.WorkflowID, StreamQueue: st.TaskQueue,
		}
	}
	if err := s.update(ctx, t, workflows.UpdateCommit, arg, &res, true); err != nil {
		return protocol.StateCommitResult{}, err
	}
	out := protocol.StateCommitResult{Version: res.Version, UpdatedAt: res.UpdatedAt, Cursors: make([]string, len(res.Seqs))}
	for i, seq := range res.Seqs {
		out.Cursors[i] = history.Cursor(res.Nonce, seq)
	}
	return out, nil
}

// StoreID is the storeId of the handshake: "temporal:<address>/<namespace>/<deploy ID>".
// Two processes with the same address, namespace and deploy ID use the same
// workflows, so they give the same value. The address is in lowercase.
func (s *Store) StoreID() string {
	if s.set == nil {
		return ""
	}
	return "temporal:" + strings.ToLower(strings.TrimSpace(s.set.Address)) + "/" + s.set.Namespace + "/" + s.set.DeployID
}
