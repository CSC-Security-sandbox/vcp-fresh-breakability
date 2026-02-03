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
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestSyncActiveDirectoryInVcp(t *testing.T) {
	params := &common.CreatePoolParams{
		ActiveDirectoryId: "ad-id",
		AccountName:       "acct",
		Region:            "loc",
		XCorrelationID:    "corr",
		ActiveDirectory:   &models.ActiveDirectory{AdName: "ad-name"},
		ADExistsInVCP:     false,
	}
	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
	}

	t.Run("ReturnsErrorWhenCreatedADIsNil", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		mockStorage := database.NewMockStorage(tt)
		env.RegisterActivity(&active_directory_activities.ActiveDirectorySyncActivity{})
		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.RegisterActivity(commonActivity.GetAuthJWTToken)
		env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, params.AccountName).Return("test-jwt-token", nil)
		env.OnActivity("PushActiveDirectoryPasswordActivity", mock.Anything, mock.Anything).Return(&cvpmodels.OperationV1beta{Name: "op"}, nil)
		env.OnActivity("PollPushPasswordOperationActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateActiveDirectoryInVCPActivity", mock.Anything, mock.Anything).Return((*datamodel.ActiveDirectory)(nil), nil)
		env.OnActivity("UpdatePoolActiveDirectoryIDActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.RegisterWorkflow(testSyncADWorkflow)

		env.ExecuteWorkflow(testSyncADWorkflow, params, pool)
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
		env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, params.AccountName).Return("test-jwt-token", nil)
		env.OnActivity("PushActiveDirectoryPasswordActivity", mock.Anything, mock.Anything).Return(&cvpmodels.OperationV1beta{Name: "op"}, nil)
		env.OnActivity("PollPushPasswordOperationActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateActiveDirectoryInVCPActivity", mock.Anything, mock.Anything).Return(&datamodel.ActiveDirectory{BaseModel: datamodel.BaseModel{ID: 10}}, nil)
		env.OnActivity("UpdatePoolActiveDirectoryIDActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.RegisterWorkflow(testSyncADWorkflow)

		env.ExecuteWorkflow(testSyncADWorkflow, params, pool)
		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})
}

func testSyncADWorkflow(ctx workflow.Context, params *common.CreatePoolParams, pool *datamodel.Pool) error {
	return syncActiveDirectoryInVcp(ctx, params, pool)
}
