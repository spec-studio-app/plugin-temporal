// Package history reads the protocol events of a run from its workflow
// history: from Temporal, or from the run's export in the blob store.
//
// An event is an accepted append or commit update whose completion
// succeeded. The update's input holds the events; its result holds their
// sequence numbers and times. A commit whose events went to the workflow of
// another stream (its result is Remote) has no event in this history.
package history

import (
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"

	"github.com/spec-studio-app/plugin-temporal/internal/blobstore"
	"github.com/spec-studio-app/plugin-temporal/internal/workflows"
)

// Event is one protocol event.
type Event struct {
	Seq  int64
	Type string
	At   time.Time
	Data json.RawMessage
}

// FromHistory finds the events of one stream in a run's history events.
func FromHistory(events []*historypb.HistoryEvent, dc converter.DataConverter, stream string) ([]Event, error) {
	inputs := map[string]pending{}
	var out []Event
	for _, ev := range events {
		switch ev.GetEventType() {
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED:
			a := ev.GetWorkflowExecutionUpdateAcceptedEventAttributes()
			req := a.GetAcceptedRequest()
			var p pending
			switch name := req.GetInput().GetName(); name {
			case workflows.UpdateAppend:
				var in workflows.AppendInput
				if err := dc.FromPayloads(req.GetInput().GetArgs(), &in); err != nil {
					return nil, fmt.Errorf("cannot read the append update of event %d: %w", ev.GetEventId(), err)
				}
				p = pending{stream: in.Stream, events: in.Events}
			case workflows.UpdateCommit:
				var in workflows.CommitInput
				if err := dc.FromPayloads(req.GetInput().GetArgs(), &in); err != nil {
					return nil, fmt.Errorf("cannot read the commit update of event %d: %w", ev.GetEventId(), err)
				}
				p = pending{stream: in.Stream, events: in.Events, commit: true}
			default:
				continue
			}
			if p.stream != stream || len(p.events) == 0 {
				continue
			}
			inputs[req.GetMeta().GetUpdateId()] = p
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_COMPLETED:
			a := ev.GetWorkflowExecutionUpdateCompletedEventAttributes()
			in, ok := inputs[a.GetMeta().GetUpdateId()]
			if !ok {
				continue
			}
			delete(inputs, a.GetMeta().GetUpdateId())
			success := a.GetOutcome().GetSuccess()
			if success == nil {
				continue
			}
			var seqs []int64
			var ats []time.Time
			if in.commit {
				var res workflows.CommitResult
				if err := dc.FromPayloads(success, &res); err != nil {
					return nil, fmt.Errorf("cannot read the commit result of event %d: %w", ev.GetEventId(), err)
				}
				if res.Remote {
					continue
				}
				seqs, ats = res.Seqs, res.Ats
			} else {
				var res workflows.AppendResult
				if err := dc.FromPayloads(success, &res); err != nil {
					return nil, fmt.Errorf("cannot read the append result of event %d: %w", ev.GetEventId(), err)
				}
				seqs, ats = res.Seqs, res.Ats
			}
			if len(seqs) != len(in.events) || len(ats) != len(in.events) {
				return nil, fmt.Errorf("the update of event %d has %d events and %d sequence numbers", ev.GetEventId(), len(in.events), len(seqs))
			}
			for i, e := range in.events {
				data := e.Data
				if len(data) == 0 {
					data = json.RawMessage("null")
				}
				out = append(out, Event{Seq: seqs[i], Type: e.Type, At: ats[i].UTC(), Data: data})
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out, nil
}

// pending is an accepted update with events, until its completion.
type pending struct {
	stream string
	events []workflows.NewEvent
	commit bool
}

// FromTemporal reads the history of a run from Temporal. It returns the
// client's NotFound error when Temporal does not have the run.
func FromTemporal(ctx context.Context, c client.Client, workflowID, runID string) ([]*historypb.HistoryEvent, error) {
	it := c.GetWorkflowHistory(ctx, workflowID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	var events []*historypb.HistoryEvent
	for it.HasNext() {
		ev, err := it.Next()
		if err != nil {
			return nil, err
		}
		events = append(events, ev)
	}
	return events, nil
}

// FromExport reads the history of a run from its export.
func FromExport(ctx context.Context, blobs blobstore.Store, name string) ([]*historypb.HistoryEvent, error) {
	r, err := blobs.GetObject(ctx, name)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	gz, err := gzip.NewReader(r)
	if err != nil {
		return nil, fmt.Errorf("the export %s is not gzip: %w", name, err)
	}
	h, err := client.HistoryFromJSON(gz, client.HistoryJSONOptions{})
	if err != nil {
		return nil, fmt.Errorf("the export %s is not a Temporal history: %w", name, err)
	}
	return h.GetEvents(), nil
}

// ErrBadCursor is the error for a cursor that the plugin did not make for
// this stream.
var ErrBadCursor = errors.New("the cursor is not a cursor of this stream")

var nonceRE = regexp.MustCompile(`^[0-9a-f]{1,32}$`)

// Cursor makes the opaque cursor of an event.
func Cursor(nonce string, seq int64) string {
	return base64.RawURLEncoding.EncodeToString([]byte("c1:" + nonce + ":" + strconv.FormatInt(seq, 36)))
}

// ParseCursor reads a cursor. It returns its nonce and its sequence number.
func ParseCursor(cursor string) (string, int64, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return "", 0, ErrBadCursor
	}
	parts := strings.Split(string(raw), ":")
	if len(parts) != 3 || parts[0] != "c1" || !nonceRE.MatchString(parts[1]) {
		return "", 0, ErrBadCursor
	}
	seq, err := strconv.ParseInt(parts[2], 36, 64)
	if err != nil || seq < 1 {
		return "", 0, ErrBadCursor
	}
	return parts[1], seq, nil
}
