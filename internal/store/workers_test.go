package store

import (
	"testing"
	"time"
)

// The wait for the stopped activities is bounded.
func TestAwaitActivitiesIsBounded(t *testing.T) {
	s := &Store{}
	s.activities.Add(1)
	start := time.Now()
	s.awaitActivities()
	if d := time.Since(start); d < activityReport || d > activityReport+500*time.Millisecond {
		t.Fatalf("the wait took %v", d)
	}
	s.activities.Add(-1)
	start = time.Now()
	s.awaitActivities()
	if d := time.Since(start); d > 300*time.Millisecond {
		t.Fatalf("the wait with no activity took %v", d)
	}
}

// The stop budget fits the plugin's 5 s shutdown.
func TestCLIStopFitsTheShutdown(t *testing.T) {
	if cliStopTimeout+activityReport+200*time.Millisecond >= 5*time.Second {
		t.Fatalf("stop %v + report %v does not fit 5 s", cliStopTimeout, activityReport)
	}
}
