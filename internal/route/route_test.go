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
	a := r.Doc("a-b", "c")
	b := r.Doc("a", "b-c")
	if a.WorkflowID == b.WorkflowID {
		t.Fatalf("two documents share the workflow %s", a.WorkflowID)
	}
}
