package replicationWorkflows

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/replicationActivities"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/replication"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestUpdateReplicationWorkflow(t *testing.T) {
	t.Run("TestUpdateReplicationWorkflow", func(tt *testing.T) {
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
		commonActivity := activities.CommonActivities{SE: mockStorage}
		updateReplicationActivity := replicationActivities.VolumeReplicationUpdateActivity{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(updateReplicationActivity.GetSrcBasePathUpdate)
		env.RegisterActivity(updateReplicationActivity.GetDstBasePathUpdate)
		env.RegisterActivity(updateReplicationActivity.GetSignedSrcTokenUpdate)
		env.RegisterActivity(updateReplicationActivity.GetSignedDstTokenUpdate)
		env.RegisterActivity(updateReplicationActivity.UpdateReplicationOnDestination)
		env.RegisterActivity(updateReplicationActivity.DescribeRemoteUpdateJob)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.UpdateReplicationParams{}

		event := &replication.UpdateReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{},
			},
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSrcBasePathUpdate", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetDstBasePathUpdate", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetSignedSrcTokenUpdate", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetSignedDstTokenUpdate", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("UpdateReplicationOnDestination", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("DescribeRemoteUpdateJob", mock.Anything, mock.Anything).Return(nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.ExecuteWorkflow(UpdateVolumeReplicationWorkflow, params, event)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		assert.Nil(tt, err)
		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})
}
