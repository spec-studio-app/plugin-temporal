package store

import (
	"strings"
	"testing"

	"go.temporal.io/sdk/temporal"

	"github.com/spec-studio-app/plugin-sdk/go/protocol"
)

// A worker of an older plugin-temporal rejects the commit update as unknown.
// The caller gets unavailable with a text that names the update and the
// deploy rule, not a bare Temporal error.
func TestClassifyUnknownUpdate(t *testing.T) {
	err := classify(temporal.NewApplicationError("unknown update commit. KnownUpdates=[append put]", ""))
	if !protocol.Is(err, protocol.Unavailable) {
		t.Fatalf("got %v; want unavailable", err)
	}
	if msg := err.Error(); !strings.Contains(msg, "update commit,") || !strings.Contains(msg, "Update every plugin-temporal") {
		t.Fatalf("the text is %q", msg)
	}
}
