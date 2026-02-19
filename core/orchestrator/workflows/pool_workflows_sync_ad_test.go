package workflows

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	cvpmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/active_directory_activities"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestSyncActiveDirectoryInVcp(t *testing.T) {
	input := adSyncInput{
		ActiveDirectoryID: "ad-id",
		AccountName:       "acct",
		Region:            "loc",
		XCorrelationID:    "corr",
		ActiveDirectory:   &models.ActiveDirectory{AdName: "ad-name"},
		LargeCapacity:     false,
	}
	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
	}

	t.Run("ReturnsErrorWhenActiveDirectoryIsNil", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.RegisterWorkflow(testSyncADWorkflow)

		invalid := input
		invalid.ActiveDirectory = nil

		env.ExecuteWorkflow(testSyncADWorkflow, invalid, pool)
		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("ReturnsErrorWhenCreatedADIsNil", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		mockStorage := database.NewMockStorage(tt)
		env.RegisterActivity(&active_directory_activities.ActiveDirectorySyncActivity{})
		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.RegisterActivity(commonActivity.GetAuthJWTToken)
		env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, input.AccountName).Return("test-jwt-token", nil)
		env.OnActivity("PushActiveDirectoryPasswordActivity", mock.Anything, mock.Anything).Return(&active_directory_activities.PushActiveDirectoryPasswordResult{
			Operation:  &cvpmodels.OperationV1beta{Name: "op"},
			SecretName: "secret-path",
		}, nil)
		env.OnActivity("PollPushPasswordOperationActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateActiveDirectoryInVCPActivity", mock.Anything, mock.Anything, "secret-path").Return((*datamodel.ActiveDirectory)(nil), nil)
		env.OnActivity("UpdatePoolActiveDirectoryIDActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.RegisterWorkflow(testSyncADWorkflow)

		env.ExecuteWorkflow(testSyncADWorkflow, input, pool)
		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("ReturnsErrorWhenPushPasswordResultIsNil", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		mockStorage := database.NewMockStorage(tt)
		env.RegisterActivity(&active_directory_activities.ActiveDirectorySyncActivity{})
		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.RegisterActivity(commonActivity.GetAuthJWTToken)
		env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, input.AccountName).Return("test-jwt-token", nil)
		env.OnActivity("PushActiveDirectoryPasswordActivity", mock.Anything, mock.Anything).Return((*active_directory_activities.PushActiveDirectoryPasswordResult)(nil), nil)
		env.RegisterWorkflow(testSyncADWorkflow)

		env.ExecuteWorkflow(testSyncADWorkflow, input, pool)
		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("ReturnsSuccess", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		mockStorage := database.NewMockStorage(tt)
		env.RegisterActivity(&active_directory_activities.ActiveDirectorySyncActivity{})
		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.RegisterActivity(commonActivity.GetAuthJWTToken)
		env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, input.AccountName).Return("test-jwt-token", nil)
		env.OnActivity("PushActiveDirectoryPasswordActivity", mock.Anything, mock.Anything).Return(&active_directory_activities.PushActiveDirectoryPasswordResult{
			Operation:  &cvpmodels.OperationV1beta{Name: "op"},
			SecretName: "secret-path",
		}, nil)
		env.OnActivity("PollPushPasswordOperationActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateActiveDirectoryInVCPActivity", mock.Anything, mock.Anything, "secret-path").Return(&datamodel.ActiveDirectory{BaseModel: datamodel.BaseModel{ID: 10}}, nil)
		env.OnActivity("UpdatePoolActiveDirectoryIDActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.RegisterWorkflow(testSyncADWorkflow)

		env.ExecuteWorkflow(testSyncADWorkflow, input, pool)
		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})
}

func testSyncADWorkflow(ctx workflow.Context, input adSyncInput, pool *datamodel.Pool) error {
	return syncActiveDirectoryInVcp(ctx, input, pool)
}
