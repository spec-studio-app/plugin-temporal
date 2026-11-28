package store

import (
	"testing"
	"time"
)

// The stop of the CLI workers fits the plugin's 5 s shutdown, with time to
// close the clients.
func TestCLIStopFitsTheShutdown(t *testing.T) {
	if cliStopTimeout > 4*time.Second {
		t.Fatalf("the stop timeout %v leaves no time to close the clients in 5 s", cliStopTimeout)
	}
}
