package backgroundworkflows

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	"go.temporal.io/sdk/testsuite"
)

func TestEligibilityStringWorkflow_Success(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	// Mock the activity - activity now returns only error (emits metrics internally)
	eligibilityStringActivity := &backgroundactivities.EligibilityStringActivity{}
	env.OnActivity(eligibilityStringActivity.GetEligibilityString, mock.Anything).Return(nil).Once()

	// Execute the workflow
	env.ExecuteWorkflow(EligibilityStringWorkflow)

	// Verify
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestEligibilityStringWorkflow_ActivityFails(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	// Mock the activity to fail - activity now returns only error
	eligibilityStringActivity := &backgroundactivities.EligibilityStringActivity{}
	env.OnActivity(eligibilityStringActivity.GetEligibilityString, mock.Anything).Return(assert.AnError).Once()

	// Execute the workflow
	env.ExecuteWorkflow(EligibilityStringWorkflow)

	// Verify
	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}
