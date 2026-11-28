package route

import "testing"

var r = Router{DeployID: "prod", TaskQueue: "relay-prod", ConfigTaskQueue: "relay-prod-config"}

// R3.S2 (offline): the relay's IDs and task queues.
func TestRelayIDs(t *testing.T) {
	cases := []struct {
		got  Target
		id   string
		tq   string
		sc   Scope
		kind string
	}{
		{r.Doc("specs", "1a2b3c4d5e6f:fix-login"), "spec-prod-1a2b3c4d5e6f-fix-login", "relay-prod", ScopeData, KindSpec},
		{r.Doc("app-specs", "1a2b3c4d5e6f:fix-login"), "spec-prod-1a2b3c4d5e6f-fix-login", "relay-prod", ScopeData, KindSpec},
		{r.Doc("closed-specs", "1a2b3c4d5e6f:fix-login"), "spec-prod-1a2b3c4d5e6f-fix-login", "relay-prod", ScopeData, KindSpec},
		{r.Stream("spec:1a2b3c4d5e6f:fix-login"), "spec-prod-1a2b3c4d5e6f-fix-login", "relay-prod", ScopeData, KindSpec},
		{r.Doc("specs", "1a2b3c4d5e6f:fix..login:x"), "spec-prod-1a2b3c4d5e6f-fix..login:x", "relay-prod", ScopeData, KindSpec},
		{r.Doc("desks", "1a2b3c4d5e6f"), "desk-prod-1a2b3c4d5e6f", "relay-prod", ScopeData, KindDesk},
		{r.Stream("desk:1a2b3c4d5e6f"), "desk-prod-1a2b3c4d5e6f", "relay-prod", ScopeData, KindDesk},
		{r.Doc("relay", "config"), "relay-config-prod", "relay-prod-config", ScopeConfig, KindConfig},
		{r.Doc("notes", "a1"), "doc/prod/notes/a1", "relay-prod", ScopeData, KindDoc},
		{r.Doc("specs", "nocolon"), "doc/prod/specs/nocolon", "relay-prod", ScopeData, KindDoc},
		{r.Stream("other"), "stream/prod/other", "relay-prod", ScopeData, KindStream},
		{r.Stream("spec:nocolon"), "stream/prod/spec:nocolon", "relay-prod", ScopeData, KindStream},
		{r.Catalog("specs"), "catalog/prod/specs", "relay-prod", ScopeData, KindCatalog},
		{r.Catalog("relay"), "catalog/prod/relay", "relay-prod-config", ScopeConfig, KindCatalog},
	}
	for _, c := range cases {
		if c.got.WorkflowID != c.id || c.got.TaskQueue != c.tq || c.got.Scope != c.sc || c.got.Kind != c.kind {
			t.Errorf("got %+v, want %s %s %s %s", c.got, c.id, c.tq, c.sc, c.kind)
		}
	}
}

func TestNoCollisions(t *testing.T) {
	pairs := [][2]Target{
		{r.Doc("a-b", "c"), r.Doc("a", "b-c")},
		{r.Stream("spec:a-b:c"), r.Stream("spec:a:b-c")},
		{r.Doc("specs", "a-b:c"), r.Doc("specs", "a:b-c")},
		{r.Doc("desks", "a-b"), r.Doc("desks", "a")},
	}
	for _, p := range pairs {
		if p[0].WorkflowID == p[1].WorkflowID {
			t.Fatalf("two names share the workflow %s", p[0].WorkflowID)
		}
	}
	// Only a 12-hex repo key gets the relay's IDs.
	if r.Doc("specs", "a-b:c").Kind != KindDoc || r.Stream("desk:a-b").Kind != KindStream {
		t.Fatal("a key that is not a relay repo key gets a relay ID")
	}
}

// R2 (state.commit): the app's spec document app-specs/<repoKey>:<slug> and
// its stream spec:<repoKey>:<slug> have one workflow, so a commit is one
// update of one workflow. A slug with ":" stays in the same workflow too.
func TestSpecDocAndStreamShareAWorkflow(t *testing.T) {
	for _, key := range []string{"1a2b3c4d5e6f:fix-login", "1a2b3c4d5e6f:a:b", "1a2b3c4d5e6f:x..y"} {
		doc, stream := r.Doc("app-specs", key), r.Stream("spec:"+key)
		if doc != stream {
			t.Fatalf("%s: the document has %+v and the stream has %+v", key, doc, stream)
		}
	}
	// A key that is not the relay's form gets two workflows.
	if r.Doc("app-specs", "notarepokey:x").WorkflowID == r.Stream("spec:notarepokey:x").WorkflowID {
		t.Fatal("a generic key shares a workflow")
	}
}
