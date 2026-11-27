package workflows

import (
	"testing"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func catalogEnv(id string) *testsuite.TestWorkflowEnvironment {
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(Catalog, workflow.RegisterOptions{Name: CatalogWorkflow})
	env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: id})
	return env
}

// A catalog refuses an entry of another collection, and an entry with no bound.
func TestCatalogRefusesCopies(t *testing.T) {
	env := catalogEnv("catalog/test/specs")
	var rejected []string
	send := func(at time.Duration, in CatalogPutInput) {
		env.RegisterDelayedCallback(func() {
			env.UpdateWorkflow(UpdateCatalogPut, "", &testsuite.TestUpdateCallback{
				OnReject:   func(err error) { rejected = append(rejected, appType(err)) },
				OnAccept:   func() {},
				OnComplete: func(any, error) {},
			}, in)
		}, at)
	}
	b := func() Bound { return Bound{Target: "catalog/test/specs", AtMs: env.Now().UnixMilli()} }
	send(time.Second, CatalogPutInput{Bound: b(), Collection: "app-specs", Key: "k"})
	send(2*time.Second, CatalogPutInput{Bound: Bound{Target: "catalog/test/app-specs", AtMs: env.Now().UnixMilli()}, Collection: "specs", Key: "k"})
	send(3*time.Second, CatalogPutInput{Bound: b(), Collection: "specs", Key: "k"})
	env.RegisterDelayedCallback(func() { env.CancelWorkflow() }, 4*time.Second)
	env.ExecuteWorkflow(CatalogWorkflow, CatalogInput{Collection: "specs"})
	if len(rejected) != 2 || rejected[0] != ErrBadInput || rejected[1] != ErrBound {
		t.Fatalf("rejections %v", rejected)
	}
}

// A start input of another collection ends the catalog run with no data.
func TestCatalogCopiedInput(t *testing.T) {
	env := catalogEnv("catalog/test/specs")
	env.ExecuteWorkflow(CatalogWorkflow, CatalogInput{Collection: "app-specs", Entries: map[string]CatalogEntry{"x": {}}})
	if !env.IsWorkflowCompleted() || env.GetWorkflowError() != nil {
		t.Fatalf("the catalog took a copied input: %v", env.GetWorkflowError())
	}
}
