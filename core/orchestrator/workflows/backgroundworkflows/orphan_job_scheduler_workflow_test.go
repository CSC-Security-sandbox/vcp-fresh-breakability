package backgroundworkflows

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"go.temporal.io/sdk/testsuite"
)

func TestOrphanJobSchedulerWorkflow_Success(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	backgroundActivities := &backgroundactivities.OrphanJobActivity{}

	// Register activities
	env.RegisterActivity(backgroundActivities)

	// Mock ListPools activity to return test pools
	env.OnActivity(backgroundActivities.OrphanJobsActivity, mock.Anything).Return(nil).Once()

	// Execute workflow
	env.ExecuteWorkflow(OrphanJobSchedulerWorkflow)

	// Assert workflow completion
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
}

func TestOrphanJobSchedulerWorkflow_Fails(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	backgroundActivities := &backgroundactivities.OrphanJobActivity{}

	// Register activities
	env.RegisterActivity(backgroundActivities)

	// Mock ListPools activity to return test pools
	env.OnActivity(backgroundActivities.OrphanJobsActivity, mock.Anything).Return(errors.New("some error")).Once()

	// Execute workflow
	env.ExecuteWorkflow(OrphanJobSchedulerWorkflow)

	// Assert workflow completion
	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
}
