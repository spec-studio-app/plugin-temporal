// Package route maps a document or a stream to the workflow that holds it.
//
// The spec, desk and relay-config workflows have the relay's workflow IDs and
// task queues, so the relay and the app write to the same workflow, and a
// relay that used its built-in Temporal store keeps its IDs. Every other name
// gets a workflow of its own. The plugin knows the relay's names only to
// choose a workflow: it applies no Spec Studio rule.
package route

import (
	"regexp"
	"strings"
)

// repoKeyRE is the shape of the relay's repo keys: 12 lowercase hex digits.
// Only a key of this shape gets the relay's spec and desk workflow IDs; any
// other key gets a generic ID, so two names never share a workflow (the
// relay's IDs join their parts with "-", which a name can hold).
var repoKeyRE = regexp.MustCompile(`^[0-9a-f]{12}$`)

// Scope is the payload key scope of a workflow.
type Scope string

// The scopes.
const (
	ScopeData   Scope = "data"
	ScopeConfig Scope = "config"
)

// The kinds of target.
const (
	KindSpec    = "spec"
	KindDesk    = "desk"
	KindConfig  = "relay-config"
	KindDoc     = "doc"
	KindStream  = "stream"
	KindCatalog = "catalog"
)

// Target is one workflow.
type Target struct {
	WorkflowID string
	TaskQueue  string
	Scope      Scope
	Kind       string
}

// Router makes targets from the settings.
type Router struct {
	DeployID        string
	TaskQueue       string
	ConfigTaskQueue string
}

func (r Router) data(id, kind string) Target {
	return Target{WorkflowID: id, TaskQueue: r.TaskQueue, Scope: ScopeData, Kind: kind}
}

func (r Router) config(id, kind string) Target {
	return Target{WorkflowID: id, TaskQueue: r.ConfigTaskQueue, Scope: ScopeConfig, Kind: kind}
}

// spec gives the spec workflow of "<repoKey>:<slug>". The key splits at its
// first ":" only, so a slug can hold ":" or "..".
func (r Router) spec(key string) (Target, bool) {
	repoKey, slug, ok := strings.Cut(key, ":")
	if !ok || !repoKeyRE.MatchString(repoKey) || slug == "" {
		return Target{}, false
	}
	return r.data("spec-"+r.DeployID+"-"+repoKey+"-"+slug, KindSpec), true
}

func (r Router) desk(repoKey string) Target {
	return r.data("desk-"+r.DeployID+"-"+repoKey, KindDesk)
}

// Doc gives the workflow of a document.
func (r Router) Doc(collection, key string) Target {
	switch collection {
	case "specs", "app-specs", "closed-specs":
		if t, ok := r.spec(key); ok {
			return t
		}
	case "desks":
		if repoKeyRE.MatchString(key) {
			return r.desk(key)
		}
	case "relay":
		if key == "config" {
			return r.config("relay-config-"+r.DeployID, KindConfig)
		}
		return r.config("doc/"+r.DeployID+"/"+collection+"/"+key, KindDoc)
	}
	// "/" is not in a name, so the parts of the ID never run together.
	return r.data("doc/"+r.DeployID+"/"+collection+"/"+key, KindDoc)
}

// Stream gives the workflow of a stream.
func (r Router) Stream(stream string) Target {
	if rest, ok := strings.CutPrefix(stream, "spec:"); ok {
		if t, ok := r.spec(rest); ok {
			return t
		}
	}
	if rest, ok := strings.CutPrefix(stream, "desk:"); ok && repoKeyRE.MatchString(rest) {
		return r.desk(rest)
	}
	return r.data("stream/"+r.DeployID+"/"+stream, KindStream)
}

// Catalog gives the list index of a collection. It has the scope of the
// collection's documents.
func (r Router) Catalog(collection string) Target {
	id := "catalog/" + r.DeployID + "/" + collection
	if collection == "relay" {
		return r.config(id, KindCatalog)
	}
	return r.data(id, KindCatalog)
}
