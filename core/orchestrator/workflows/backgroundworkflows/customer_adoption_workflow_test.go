package backgroundworkflows

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/worker/metrics"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// setupTestEnvironment sets up package variables and returns a test workflow environment
// This ensures PopulateRetryPolicyParams() works correctly in tests
// Note: We modify package variables directly because PopulateRetryPolicyParams() reads from
// package variables (initialized at package load time), not environment variables.
func setupTestEnvironment(t *testing.T) *testsuite.TestWorkflowEnvironment {
	t.Helper()

	// Save original values
	origStartToClose := workflows.StartToCloseTimeout
	origRetryInterval := workflows.RetryInterval
	origRetryMaxAttempts := workflows.RetryMaxAttempts
	origRetryMaxInterval := workflows.RetryMaxInterval
	origRetryBackoff := workflows.RetryBackoff
	origHeartbeat := workflows.ActivityHeartBeatTimeout

	// Set test values
	workflows.StartToCloseTimeout = "10s"
	workflows.RetryInterval = "1s"
	workflows.RetryMaxAttempts = 1
	workflows.RetryMaxInterval = "2s"
	workflows.RetryBackoff = "1.0"
	workflows.ActivityHeartBeatTimeout = "5s"

	// Restore original values after test
	t.Cleanup(func() {
		workflows.StartToCloseTimeout = origStartToClose
		workflows.RetryInterval = origRetryInterval
		workflows.RetryMaxAttempts = origRetryMaxAttempts
		workflows.RetryMaxInterval = origRetryMaxInterval
		workflows.RetryBackoff = origRetryBackoff
		workflows.ActivityHeartBeatTimeout = origHeartbeat
	})

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})

	return env
}

func TestVolumeDetailsWorkflow(t *testing.T) {
	env := setupTestEnvironment(t)

	// Create and register activity (must use same instance for registration and mocking)
	customerAdoptionActivity := &backgroundactivities.CustomerAdoptionActivity{}
	env.RegisterActivity(customerAdoptionActivity.GetActiveVolumesActivity)

	// Mock the activity AFTER registration
	env.OnActivity(customerAdoptionActivity.GetActiveVolumesActivity, mock.Anything).Return([]*datamodel.Volume{
		{Name: "Volume1", State: "active", AccountID: 123},
		{Name: "Volume2", State: "active", AccountID: 456},
	}, nil).Once()

	// Execute the workflow
	env.ExecuteWorkflow(VolumeDetailsWorkflow)

	// Verify workflow completed successfully
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	// Verify workflow results
	var result []metrics.VolumeDetails
	err := env.GetWorkflowResult(&result)
	require.NoError(t, err)
	require.Len(t, result, 2, "Should return 2 volume details")
	assert.Equal(t, "Volume1", result[0].Name)
	assert.Equal(t, "active", result[0].State)
	assert.Equal(t, int64(123), result[0].AccountID)
	assert.Equal(t, "Volume2", result[1].Name)
	assert.Equal(t, "active", result[1].State)
	assert.Equal(t, int64(456), result[1].AccountID)

	env.AssertExpectations(t)
}

func TestBackupSizeDetailsWorkflow(t *testing.T) {
	env := setupTestEnvironment(t)

	// Create and register activity (must use same instance for registration and mocking)
	customerAdoptionActivity := &backgroundactivities.CustomerAdoptionActivity{}
	env.RegisterActivity(customerAdoptionActivity.GetBackupDetailsActivity)

	// Mock the activity AFTER registration
	env.OnActivity(customerAdoptionActivity.GetBackupDetailsActivity, mock.Anything, mock.Anything).Return(&backgroundactivities.BackupDetailsResult{
		Details: []backgroundactivities.BackupDetail{
			{
				VolName:     "test-backup",
				Size:        12345,
				AccountName: "test-account",
			},
		},
	}, nil).Once()

	// Execute the workflow
	env.ExecuteWorkflow(BackupSizeDetailsWorkflow)

	// Verify workflow completed successfully
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	// Verify workflow results
	var details []metrics.BackupDetailForMetric
	err := env.GetWorkflowResult(&details)
	require.NoError(t, err)
	require.Len(t, details, 1, "Should return 1 backup detail")
	assert.Equal(t, "test-backup", details[0].VolName)
	assert.Equal(t, float64(12345), float64(details[0].Size)) // Convert int64 to float64 for comparison
	assert.Equal(t, "test-account", details[0].AccountName)

	env.AssertExpectations(t)
}
