package backgroundworkflows

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	"go.temporal.io/sdk/testsuite"
)

// Trial sync scheduling is gated by TRIAL_ACCOUNT_SYNC_ENABLED in LoadJobSpecs (google-proxy), not in the workflow.

func TestTrialAccountSyncWorkflow_Success(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	activity := &backgroundactivities.TrialAccountSyncActivity{}
	env.OnActivity(activity.SyncTrialAccounts, mock.Anything).Return(nil).Once()

	env.ExecuteWorkflow(TrialAccountSyncWorkflow)

	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestTrialAccountSyncWorkflow_ActivityFails(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	activity := &backgroundactivities.TrialAccountSyncActivity{}
	env.OnActivity(activity.SyncTrialAccounts, mock.Anything).Return(assert.AnError).Once()

	env.ExecuteWorkflow(TrialAccountSyncWorkflow)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}
