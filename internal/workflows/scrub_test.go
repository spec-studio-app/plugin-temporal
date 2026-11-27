package workflows

import "testing"

func TestScrubHistory(t *testing.T) {
	in := `{"identity": "66306@Some-Mac.local@", "taskQueue": {"name": "Some-Mac.local:50f529f5-4bcc-4683-a64f-f1a7e3bedbee", "kind": "TASK_QUEUE_KIND_STICKY"}, "workflowId": "spec-x"}`
	want := `{"identity": "test-worker", "taskQueue": {"name": "test-worker:50f529f5-4bcc-4683-a64f-f1a7e3bedbee", "kind": "TASK_QUEUE_KIND_STICKY"}, "workflowId": "spec-x"}`
	if got := string(ScrubHistory([]byte(in))); got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}
