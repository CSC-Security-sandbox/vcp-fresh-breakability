package backgroundworkflows

import (
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
