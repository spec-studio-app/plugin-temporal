package workflows

import "regexp"

// HistoryWorker is the worker name in the stored replay histories.
const HistoryWorker = "test-worker"

var (
	identityRE = regexp.MustCompile(`"identity":\s*"[^"]*"`)
	// A sticky task queue is "<host name>:<UUID>".
	stickyRE = regexp.MustCompile(`"[^"]+:([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"`)
)

// ScrubHistory replaces the worker identities ("<pid>@<host name>@") and the
// sticky task queue names of a history in Temporal JSON with HistoryWorker,
// so a stored history holds no computer name. The replay does not read them.
func ScrubHistory(b []byte) []byte {
	b = identityRE.ReplaceAll(b, []byte(`"identity": "`+HistoryWorker+`"`))
	return stickyRE.ReplaceAll(b, []byte(`"`+HistoryWorker+`:$1"`))
}
