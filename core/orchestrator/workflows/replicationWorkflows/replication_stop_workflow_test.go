package replicationWorkflows

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	coreModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
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

func TestStopReplicationWorkflow(t *testing.T) {
	t.Run("TestStopReplicationWorkflow", func(tt *testing.T) {
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
		stopReplicationActivity := replicationActivities.StopVolumeReplicationActivity{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(stopReplicationActivity.GetSrcBasePathStop)
		env.RegisterActivity(stopReplicationActivity.GetDstBasePathStop)
		env.RegisterActivity(stopReplicationActivity.GetSignedSrcTokenStop)
		env.RegisterActivity(stopReplicationActivity.GetSignedDstTokenStop)
		env.RegisterActivity(stopReplicationActivity.StopReplicationOnDestination)
		env.RegisterActivity(stopReplicationActivity.DescribeDestJobStop)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.StopReplicationParams{}

		event := &replication.StopReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{},
			},
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSrcBasePathStop", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetDstBasePathStop", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetSignedSrcTokenStop", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetSignedDstTokenStop", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("StopReplicationOnDestination", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("DescribeDestJobStop", mock.Anything, mock.Anything).Return(nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.ExecuteWorkflow(StopReplicationWorkflow, params, event)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		assert.Nil(tt, err)
		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})
	t.Run("TestStopReplicationWorkflow_WhenIsSrcForHybridReplicationIsTrue", func(tt *testing.T) {
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
		stopReplicationActivity := replicationActivities.StopVolumeReplicationActivity{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(stopReplicationActivity.SetHybridReplicationVariablesStop)
		env.RegisterActivity(stopReplicationActivity.GetSrcBasePathStop)
		env.RegisterActivity(stopReplicationActivity.GetDstBasePathStop)
		env.RegisterActivity(stopReplicationActivity.GetSignedSrcTokenStop)
		env.RegisterActivity(stopReplicationActivity.GetSignedDstTokenStop)
		env.RegisterActivity(stopReplicationActivity.HandleHybridReplicationStopWhenGcnvIsSrc)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.StopReplicationParams{}

		reverseType := string(coreModels.HybridReplicationParametersReplicationTypeREVERSE)
		replicationModel := &datamodel.VolumeReplication{
			HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
				HybridReplicationType: &reverseType,
			},
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationLocation: "",
			},
		}

		event := &replication.StopReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: replicationModel,
			},
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SetHybridReplicationVariablesStop", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.StopReplicationResult) (*replication.StopReplicationResult, error) {
			if result != nil {
				result.IsSrcForHybridReplication = true
			}
			return result, nil
		})
		env.OnActivity("GetSrcBasePathStop", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetDstBasePathStop", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetSignedSrcTokenStop", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetSignedDstTokenStop", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("HandleHybridReplicationStopWhenGcnvIsSrc", mock.Anything, mock.Anything).Return(nil, nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.ExecuteWorkflow(StopReplicationWorkflow, params, event)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		assert.Nil(tt, err)
		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
		env.AssertExpectations(tt)
	})
}
