package workflows

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestUpdateVolumeV2Workflow(t *testing.T) {
	t.Run("TestUpdateVolumeV2Workflow", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		mockStorage := database.NewMockStorage(tt)
		updateActivity := &activities.UpdateVolumeInReplicationActivity{}
		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(updateActivity.GetReplicationFromDBVolume)
		env.RegisterActivity(updateActivity.GetLocalBasePathVolume)
		env.RegisterActivity(updateActivity.GetRemoteBasePathVolume)
		env.RegisterActivity(updateActivity.GetSignedLocalTokenVolume)
		env.RegisterActivity(updateActivity.GetSignedRemoteTokenVolume)
		env.RegisterActivity(updateActivity.GetReplicationMirrorState)
		env.RegisterActivity(updateActivity.GetRemotePoolDetailsVolume)
		env.RegisterActivity(updateActivity.ValidateRemoteVolumeUpdate)
		env.RegisterActivity(updateActivity.UpdateRemoteVolume)
		env.RegisterActivity(updateActivity.DescribeRemoteJobVolumeUpdate)
		env.RegisterActivity(updateActivity.CreateJobForChildWorkflow)
		env.RegisterWorkflow(UpdateVolumeWorkflow)

		params := &common.UpdateVolumeParams{}
		volume := &datamodel.Volume{}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetReplicationFromDBVolume", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetLocalBasePathVolume", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetRemoteBasePathVolume", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetSignedLocalTokenVolume", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetSignedRemoteTokenVolume", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetReplicationMirrorState", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetRemotePoolDetailsVolume", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("ValidateRemoteVolumeUpdate", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateRemoteVolume", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DescribeRemoteJobVolumeUpdate", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateJobForChildWorkflow", mock.Anything, mock.Anything).Return(nil)
		env.OnWorkflow("UpdateVolumeWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(UpdateVolumeInReplicationWorkflow, params, volume)
		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		assert.Nil(tt, err)
		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
		mockStorage.AssertExpectations(tt)
	})
}
