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

func TestFetchCCFEBackupVaultsWorkflow_Success(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	wantA := []resourcescope.CachedBackupVault{
		{UUID: "uuid-a1", Name: "vault-a1"},
		{UUID: "uuid-a2", Name: "vault-a2"},
	}
	wantB := []resourcescope.CachedBackupVault{
		{UUID: "uuid-b1", Name: "vault-b1"},
	}

	act := &backgroundactivities.FetchBackupVaultsActivity{}
	env.RegisterActivity(act.FetchBackupVaults)
	env.OnActivity(act.FetchBackupVaults, mock.Anything, "proj-a", "us-central1").Return(wantA, nil)
	env.OnActivity(act.FetchBackupVaults, mock.Anything, "proj-a", "us-east1").Return(wantB, nil)

	env.ExecuteWorkflow(FetchCCFEBackupVaultsWorkflow, "proj-a", []string{"us-central1", "us-east1"})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var got map[string][]resourcescope.CachedBackupVault
	require.NoError(t, env.GetWorkflowResult(&got))
	assert.Equal(t, map[string][]resourcescope.CachedBackupVault{
		"us-central1": wantA,
		"us-east1":    wantB,
	}, got)
}

func TestFetchCCFEBackupVaultsWorkflow_EmptyLocationsShortCircuits(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	act := &backgroundactivities.FetchBackupVaultsActivity{}
	env.RegisterActivity(act.FetchBackupVaults)

	env.ExecuteWorkflow(FetchCCFEBackupVaultsWorkflow, "proj-a", []string{})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var got map[string][]resourcescope.CachedBackupVault
	require.NoError(t, env.GetWorkflowResult(&got))
	assert.Empty(t, got)
}

func TestFetchCCFEBackupVaultsWorkflow_PartialFailure_DropsFailedLocations(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	wantA := []resourcescope.CachedBackupVault{{UUID: "uuid-a1", Name: "vault-a1"}}
	wantC := []resourcescope.CachedBackupVault{{UUID: "uuid-c1", Name: "vault-c1"}}

	act := &backgroundactivities.FetchBackupVaultsActivity{}
	env.RegisterActivity(act.FetchBackupVaults)
	env.OnActivity(act.FetchBackupVaults, mock.Anything, "proj-a", "us-central1").Return(wantA, nil)
	env.OnActivity(act.FetchBackupVaults, mock.Anything, "proj-a", "us-east1").Return(nil, errors.New("ccfe boom"))
	env.OnActivity(act.FetchBackupVaults, mock.Anything, "proj-a", "eu-west1").Return(wantC, nil)

	env.ExecuteWorkflow(FetchCCFEBackupVaultsWorkflow, "proj-a", []string{"us-central1", "us-east1", "eu-west1"})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var got map[string][]resourcescope.CachedBackupVault
	require.NoError(t, env.GetWorkflowResult(&got))
	assert.Equal(t, map[string][]resourcescope.CachedBackupVault{
		"us-central1": wantA,
		"eu-west1":    wantC,
	}, got)
	assert.NotContains(t, got, "us-east1")
}

func TestFetchCCFEBackupVaultsWorkflow_CCFEDisabledNilSlicePropagated(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	act := &backgroundactivities.FetchBackupVaultsActivity{}
	env.RegisterActivity(act.FetchBackupVaults)
	env.OnActivity(act.FetchBackupVaults, mock.Anything, "proj-a", "us-central1").Return(nil, nil)

	env.ExecuteWorkflow(FetchCCFEBackupVaultsWorkflow, "proj-a", []string{"us-central1"})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var got map[string][]resourcescope.CachedBackupVault
	require.NoError(t, env.GetWorkflowResult(&got))
	require.Contains(t, got, "us-central1")
	assert.Nil(t, got["us-central1"])
}
