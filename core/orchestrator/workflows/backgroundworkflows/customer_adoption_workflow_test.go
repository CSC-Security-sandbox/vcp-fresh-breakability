package backgroundworkflows

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/worker/metrics"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	"go.temporal.io/sdk/testsuite"
)

func TestVolumeDetailsWorkflow(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	// Mock the activity
	customerAdoptionActivity := &backgroundactivities.CustomerAdoptionActivity{}
	env.OnActivity(customerAdoptionActivity.GetActiveVolumesActivity, mock.Anything).Return([]*datamodel.Volume{
		{Name: "Volume1", State: "active", AccountID: 123},
		{Name: "Volume2", State: "active", AccountID: 456},
	}, nil).Once()

	// Execute the workflow
	env.ExecuteWorkflow(VolumeDetailsWorkflow)

	// Verify
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestBackupSizeDetailsWorkflow(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	// Mock the activity
	customerAdoptionActivity := &backgroundactivities.CustomerAdoptionActivity{}
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

	// Verify
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())

	var details []metrics.BackupDetailForMetric
	err := env.GetWorkflowResult(&details)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(details))
	assert.Equal(t, "test-backup", details[0].VolName)
	assert.Equal(t, float64(12345), float64(details[0].Size)) // Convert int64 to float64 for comparison
	assert.Equal(t, "test-account", details[0].AccountName)

	env.AssertExpectations(t)
}
