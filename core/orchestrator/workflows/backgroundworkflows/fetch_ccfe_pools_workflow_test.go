package backgroundworkflows

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/resourcescope"
	"go.temporal.io/sdk/testsuite"
)

// TestFetchCCFEPoolsWorkflow_Success exercises the happy path: every
// location's activity returns a non-empty slice and the workflow surfaces
// the per-location result in the response map.
func TestFetchCCFEPoolsWorkflow_Success(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	wantA := []resourcescope.CachedPool{{UUID: "uuid-a", Name: "pool-a"}}
	wantB := []resourcescope.CachedPool{{UUID: "uuid-b", Name: "pool-b"}}
	wantRegion := []resourcescope.CachedPool{{UUID: "uuid-r", Name: "pool-r"}}

	act := &backgroundactivities.FetchStoragePoolsActivity{}
	env.RegisterActivity(act.FetchStoragePools)
	env.OnActivity(act.FetchStoragePools, mock.Anything, "proj-a", "us-central1").Return(wantRegion, nil)
	env.OnActivity(act.FetchStoragePools, mock.Anything, "proj-a", "us-central1-a").Return(wantA, nil)
	env.OnActivity(act.FetchStoragePools, mock.Anything, "proj-a", "us-central1-b").Return(wantB, nil)

	env.ExecuteWorkflow(FetchCCFEPoolsWorkflow, "proj-a", []string{"us-central1", "us-central1-a", "us-central1-b"})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var got map[string][]resourcescope.CachedPool
	require.NoError(t, env.GetWorkflowResult(&got))
	assert.Equal(t, map[string][]resourcescope.CachedPool{
		"us-central1":   wantRegion,
		"us-central1-a": wantA,
		"us-central1-b": wantB,
	}, got)
}

// TestFetchCCFEPoolsWorkflow_EmptyLocationsShortCircuits ensures the
// workflow doesn't fire any activity when the caller passes a zero-length
// location slice. This is the "no enumerated pairs for this project"
// edge case.
func TestFetchCCFEPoolsWorkflow_EmptyLocationsShortCircuits(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	act := &backgroundactivities.FetchStoragePoolsActivity{}
	env.RegisterActivity(act.FetchStoragePools)

	env.ExecuteWorkflow(FetchCCFEPoolsWorkflow, "proj-a", []string{})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var got map[string][]resourcescope.CachedPool
	require.NoError(t, env.GetWorkflowResult(&got))
	assert.Empty(t, got)
}

// TestFetchCCFEPoolsWorkflow_PartialFailure_DropsFailedLocations is the
// resilience guarantee. One location's CCFE activity exhausts retries, the
// other two succeed, and the workflow returns the two successes (the
// failed location is omitted from the map) without surfacing an overall
// error. The detector treats absence as "skip the diff for this pair".
func TestFetchCCFEPoolsWorkflow_PartialFailure_DropsFailedLocations(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	wantA := []resourcescope.CachedPool{{UUID: "uuid-a", Name: "pool-a"}}
	wantC := []resourcescope.CachedPool{{UUID: "uuid-c", Name: "pool-c"}}

	act := &backgroundactivities.FetchStoragePoolsActivity{}
	env.RegisterActivity(act.FetchStoragePools)
	env.OnActivity(act.FetchStoragePools, mock.Anything, "proj-a", "us-central1-a").Return(wantA, nil)
	env.OnActivity(act.FetchStoragePools, mock.Anything, "proj-a", "us-central1-b").Return(nil, errors.New("ccfe boom"))
	env.OnActivity(act.FetchStoragePools, mock.Anything, "proj-a", "us-central1-c").Return(wantC, nil)

	env.ExecuteWorkflow(FetchCCFEPoolsWorkflow, "proj-a", []string{"us-central1-a", "us-central1-b", "us-central1-c"})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var got map[string][]resourcescope.CachedPool
	require.NoError(t, env.GetWorkflowResult(&got))
	assert.Equal(t, map[string][]resourcescope.CachedPool{
		"us-central1-a": wantA,
		"us-central1-c": wantC,
	}, got)
	assert.NotContains(t, got, "us-central1-b")
}

// TestFetchCCFEPoolsWorkflow_CCFEDisabledNilSlicePropagated covers the
// "CCFE disabled" signal: the activity returns (nil, nil) and the workflow
// must keep the location in the map with a nil value. The detector
// distinguishes this case (skip diff because we have no authoritative
// answer) from "key absent" (workflow couldn't fetch at all).
func TestFetchCCFEPoolsWorkflow_CCFEDisabledNilSlicePropagated(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	act := &backgroundactivities.FetchStoragePoolsActivity{}
	env.RegisterActivity(act.FetchStoragePools)
	env.OnActivity(act.FetchStoragePools, mock.Anything, "proj-a", "us-central1-a").Return(nil, nil)

	env.ExecuteWorkflow(FetchCCFEPoolsWorkflow, "proj-a", []string{"us-central1-a"})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var got map[string][]resourcescope.CachedPool
	require.NoError(t, env.GetWorkflowResult(&got))
	require.Contains(t, got, "us-central1-a")
	assert.Nil(t, got["us-central1-a"])
}
