// Package workflows holds the two workflow types of plugin-temporal:
//
//   - SpecStudioEntity keeps the documents and the stream of one target (one
//     spec, one desk, the relay config, or one other name). Each write is an
//     update, so the workflow history is the event log.
//   - SpecStudioCatalog keeps the list index of one collection, for state.list.
//
// The workflows know no Spec Studio rule. They store what the host gives.
package workflows

import (
	"encoding/json"
	"time"
)

// The workflow type names. A relay workflow continues as new into
// EntityWorkflow with a HandoffInput (see HANDOFF.md).
const (
	EntityWorkflow  = "SpecStudioEntity"
	CatalogWorkflow = "SpecStudioCatalog"
)

// The update names of the entity workflow. The history reader finds the
// events by UpdateAppend.
const (
	UpdatePut          = "put"
	UpdateDelete       = "delete"
	UpdateAppend       = "append"
	UpdateDeleteStream = "deleteStream"
	UpdateWait         = "wait"
	UpdateContinue     = "continue"
)

// The query names of the entity workflow.
const (
	QueryDoc  = "doc"
	QueryInfo = "info"
)

// The update and query names of the catalog workflow.
const (
	UpdateCatalogPut = "catalogPut"
	QueryCatalogPage = "page"
)

// The activity names.
const (
	ActivityCatalogPut    = "CatalogPut"
	ActivityExportRun     = "ExportRun"
	ActivityDeleteExports = "DeleteExports"
	ActivityLoadState     = "LoadState"
)

// The types of the application errors that a validator or a handler gives.
// The store maps them to protocol errors.
const (
	// ErrConflict: the expected version is not the stored version. The
	// detail is the stored version (a *string, nil when no document exists).
	ErrConflict = "conflict"
	// ErrNotFound: the document does not exist.
	ErrNotFound = "not_found"
	// ErrRetry: the run is starting or closing. The store sends the update
	// again after a short time; the next run takes it.
	ErrRetry = "retry"
	// ErrReady: the condition of a wait is true already. The store reads
	// the state with a query and adds no event to the history.
	ErrReady = "ready"
	// ErrBound: the input is for another workflow, or it is too old.
	ErrBound = "bound"
	// ErrBadInput: the input cannot be applied.
	ErrBadInput = "bad_input"
)

// BoundMaxAge is the oldest input that a validator takes, as in the relay's
// workflows/bound.ts. A person with write access to the namespace can copy
// an encrypted payload from one history to another; the bound refuses a copy
// for another workflow, or an old copy.
const BoundMaxAge = 5 * time.Minute

// Bound binds an update input to its workflow and its time.
type Bound struct {
	Target string `json:"target"`
	// AtMs is the plugin's time when it made the input (ms since the epoch).
	AtMs int64 `json:"at"`
}

// Defaults of the start input.
const (
	DefaultRunLimit = 2000
	DefaultIdle     = 7 * 24 * time.Hour
	// EmptyIdle is the idle time of a run that never held data (a wait on a
	// document that does not exist starts one).
	EmptyIdle = 10 * time.Minute
	// InlineStateBytes is the largest size of the documents in a
	// continue-as-new input. Larger state goes to the blob store.
	InlineStateBytes = 512 * 1024
)

// Doc is one document.
type Doc struct {
	Collection string          `json:"collection"`
	Key        string          `json:"key"`
	Value      json.RawMessage `json:"value"`
	Attributes map[string]any  `json:"attributes"`
	Version    string          `json:"version"`
	// N is the write number of the version. It grows at each write of the
	// target, so the catalog keeps the newest entry.
	N         int64     `json:"n"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// RunRecord is one ended run of a chain.
type RunRecord struct {
	RunID string `json:"runId"`
	// Export is the object name of the run's history export.
	Export string `json:"export"`
	// Legacy is true for a relay run from before the handoff. It has no
	// protocol events.
	Legacy bool `json:"legacy,omitempty"`
	// FirstSeq and LastSeq are the sequence numbers of the run's events
	// (both 0 when the run has no event).
	FirstSeq int64 `json:"firstSeq"`
	LastSeq  int64 `json:"lastSeq"`
	// MinAt and MaxAt are the earliest and latest event times of the run.
	MinAt *time.Time `json:"minAt,omitempty"`
	MaxAt *time.Time `json:"maxAt,omitempty"`
}

// Chain is what a run passes to the next run, and what the manifest keeps
// after the last run ends.
type Chain struct {
	WorkflowID string `json:"workflowId"`
	// Nonce is part of each version and cursor. A new chain gets a new nonce,
	// so no version or cursor is used again.
	Nonce string `json:"nonce"`
	// N is the last write number.
	N int64 `json:"n"`
	// NextSeq is the sequence number of the next event (it starts at 1).
	NextSeq int64 `json:"nextSeq"`
	// KeptFrom is the first sequence number that a read returns. An
	// events.delete moves it to NextSeq.
	KeptFrom int64 `json:"keptFrom"`
	// Stream is the stream name, when the target has events.
	Stream string `json:"stream,omitempty"`
	// Runs are the ended runs, oldest first.
	Runs []RunRecord `json:"runs"`
}

// HandoffInput is the input with which a relay workflow continues as new into
// EntityWorkflow. The relay makes the documents with its protocol adapter.
type HandoffInput struct {
	Version    int          `json:"version"`
	WorkflowID string       `json:"workflowId"`
	Documents  []HandoffDoc `json:"documents"`
}

// HandoffDoc is one document of a handoff.
type HandoffDoc struct {
	Collection string          `json:"collection"`
	Key        string          `json:"key"`
	Value      json.RawMessage `json:"value"`
	Attributes map[string]any  `json:"attributes"`
}

// EntityInput is the start input of the entity workflow.
type EntityInput struct {
	// Scope is "data" or "config": the activities use the client of the scope.
	Scope string `json:"scope,omitempty"`
	Chain *Chain `json:"chain,omitempty"`
	Docs  []Doc  `json:"docs,omitempty"`
	// StateObject names the documents in the blob store, when they were too
	// large for this input.
	StateObject string `json:"stateObject,omitempty"`
	RunLimit    int    `json:"runLimit,omitempty"`
	// IdleMs is the time with no update after which a run with no document ends.
	IdleMs int64 `json:"idleMs,omitempty"`
	// Handoff is set by a relay workflow that continues as new into this one.
	Handoff *HandoffInput `json:"handoff,omitempty"`
}

// PutInput is the input of the put update.
type PutInput struct {
	Bound      Bound           `json:"bound"`
	Collection string          `json:"collection"`
	Key        string          `json:"key"`
	Value      json.RawMessage `json:"value"`
	Attributes map[string]any  `json:"attributes"`
	// Expected is nil for "the document must not exist".
	Expected *string `json:"expected"`
}

// PutResult is the result of the put update.
type PutResult struct {
	Version   string    `json:"version"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// DeleteInput is the input of the delete update.
type DeleteInput struct {
	Bound      Bound  `json:"bound"`
	Collection string `json:"collection"`
	Key        string `json:"key"`
	Expected   string `json:"expected"`
}

// NewEvent is one event of an append.
type NewEvent struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
	At   *time.Time      `json:"at,omitempty"`
}

// AppendInput is the input of the append update. Its events are the events
// of the stream: the history reader reads them back from the history.
type AppendInput struct {
	Bound  Bound      `json:"bound"`
	Stream string     `json:"stream"`
	Events []NewEvent `json:"events"`
}

// AppendResult gives each event its sequence number and its time.
type AppendResult struct {
	Nonce string      `json:"nonce"`
	Seqs  []int64     `json:"seqs"`
	Ats   []time.Time `json:"ats"`
}

// DeleteStreamInput is the input of the deleteStream update.
type DeleteStreamInput struct {
	Bound  Bound  `json:"bound"`
	Stream string `json:"stream"`
}

// WaitInput is the input of the wait update: a document (Collection, Key
// and Version) or the stream (Stream and AfterSeq).
type WaitInput struct {
	Bound      Bound   `json:"bound"`
	Collection string  `json:"collection,omitempty"`
	Key        string  `json:"key,omitempty"`
	Version    *string `json:"version"`
	Stream     string  `json:"stream,omitempty"`
	AfterSeq   int64   `json:"afterSeq"`
	TimeoutMs  int64   `json:"timeoutMs"`
}

// WaitResult is the result of the wait update.
type WaitResult struct {
	Changed bool    `json:"changed"`
	Version *string `json:"version,omitempty"`
	Seq     int64   `json:"seq,omitempty"`
	Nonce   string  `json:"nonce,omitempty"`
}

// ContinueInput is the input of the continue update (the test hook).
type ContinueInput struct {
	Bound Bound `json:"bound"`
}

// Info is the result of the info query: what a reader needs.
type Info struct {
	RunID string `json:"runId"`
	Chain Chain  `json:"chain"`
	// RunFirstSeq is the first sequence number of this run.
	RunFirstSeq int64 `json:"runFirstSeq"`
	Closing     bool  `json:"closing"`
	Docs        int   `json:"docs"`
}

// CatalogEntry is one key of a catalog.
type CatalogEntry struct {
	Attributes map[string]any `json:"attributes"`
	Version    string         `json:"version"`
	N          int64          `json:"n"`
	UpdatedAt  time.Time      `json:"updatedAt"`
	// Deleted marks a removed key, so a late entry cannot bring it back.
	Deleted bool `json:"deleted,omitempty"`
}

// CatalogInput is the start input of the catalog workflow.
type CatalogInput struct {
	Collection string                  `json:"collection"`
	Entries    map[string]CatalogEntry `json:"entries,omitempty"`
	RunLimit   int                     `json:"runLimit,omitempty"`
}

// CatalogPutInput is the input of the catalog's update and of the
// CatalogPut activity.
type CatalogPutInput struct {
	Collection string       `json:"collection"`
	Key        string       `json:"key"`
	Entry      CatalogEntry `json:"entry"`
}

// PageQuery is the input of the catalog's page query.
type PageQuery struct {
	Filter map[string]any `json:"filter"`
	After  string         `json:"after"`
	Limit  int            `json:"limit"`
}

// PageItem is one key of a page.
type PageItem struct {
	Key   string       `json:"key"`
	Entry CatalogEntry `json:"entry"`
}

// Page is the result of the page query.
type Page struct {
	Items []PageItem `json:"items"`
	// Next is the last key of the page when more keys match, else "".
	Next string `json:"next"`
}

// ExportInput is the input of the ExportRun activity.
type ExportInput struct {
	WorkflowID string `json:"workflowId"`
	RunID      string `json:"runId"`
	// Chain is the chain with the run in it. The activity writes it as the
	// manifest after the export.
	Chain Chain `json:"chain"`
	// Docs are written to the blob store too when StateObject is set.
	Docs        []Doc  `json:"docs,omitempty"`
	StateObject string `json:"stateObject,omitempty"`
	// SkipHistory is true when the run has nothing to export (it never held
	// data); only the manifest is written.
	SkipHistory bool `json:"skipHistory,omitempty"`
}

// DeleteExportsInput is the input of the DeleteExports activity.
type DeleteExportsInput struct {
	Exports []string `json:"exports"`
	Chain   Chain    `json:"chain"`
}
