package replicationWorkflows

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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

func TestUpdateVolumeReplicationAttributesWorkflow(t *testing.T) {
	t.Run("TestUpdateVolumeReplicationAttributesWorkflow", func(tt *testing.T) {
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
		updateAttrActivity := replicationActivities.UpdateVolumeReplicationAttributesActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(updateAttrActivity.GetSnapmirrorDetailsFromOntap)
		env.RegisterActivity(updateAttrActivity.UpdateReplicationAttributes)
		env.RegisterActivity(updateAttrActivity.UpdateVolumeTypeOnNewDestination)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.UpdateVolumeReplicationAttributesParams{
			AccountName: "test-account",
		}

		event := &replication.UpdateVolumeReplicationAttributesEvent{
			UpdateVolumeReplicationAttributesParams: nil,
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSnapmirrorDetailsFromOntap", mock.Anything, mock.Anything).Return(&replication.UpdateVolumeReplicationAttributesResult{Event: event}, nil)
		env.OnActivity("UpdateReplicationAttributes", mock.Anything, mock.Anything).Return(&replication.UpdateVolumeReplicationAttributesResult{Event: event}, nil)
		env.OnActivity("UpdateVolumeTypeOnNewDestination", mock.Anything, mock.Anything).Return(&replication.UpdateVolumeReplicationAttributesResult{Event: event}, nil)

		env.ExecuteWorkflow(UpdateVolumeReplicationAttributesWorkflow, params, event)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		assert.Nil(tt, err)
		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestUpdateVolumeReplicationAttributesWorkflow_GetSnapmirrorDetailsError", func(tt *testing.T) {
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
		updateAttrActivity := replicationActivities.UpdateVolumeReplicationAttributesActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(updateAttrActivity.GetSnapmirrorDetailsFromOntap)
		env.RegisterActivity(updateAttrActivity.UpdateReplicationAttributes)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.UpdateVolumeReplicationAttributesParams{
			AccountName: "test-account",
		}

		event := &replication.UpdateVolumeReplicationAttributesEvent{
			UpdateVolumeReplicationAttributesParams: nil,
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSnapmirrorDetailsFromOntap", mock.Anything, mock.Anything).Return(nil, assert.AnError)

		env.ExecuteWorkflow(UpdateVolumeReplicationAttributesWorkflow, params, event)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestUpdateVolumeReplicationAttributesWorkflow_UpdateReplicationTableEntriesError", func(tt *testing.T) {
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
		updateAttrActivity := replicationActivities.UpdateVolumeReplicationAttributesActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(updateAttrActivity.GetSnapmirrorDetailsFromOntap)
		env.RegisterActivity(updateAttrActivity.UpdateReplicationAttributes)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.UpdateVolumeReplicationAttributesParams{
			AccountName: "test-account",
		}

		event := &replication.UpdateVolumeReplicationAttributesEvent{
			UpdateVolumeReplicationAttributesParams: nil,
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSnapmirrorDetailsFromOntap", mock.Anything, mock.Anything).Return(&replication.UpdateVolumeReplicationAttributesResult{Event: event}, nil)
		env.OnActivity("UpdateReplicationAttributes", mock.Anything, mock.Anything).Return(nil, assert.AnError)

		env.ExecuteWorkflow(UpdateVolumeReplicationAttributesWorkflow, params, event)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})
}
