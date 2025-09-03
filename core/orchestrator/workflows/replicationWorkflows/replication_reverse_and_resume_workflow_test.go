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

func TestReverseAndResumeVolumeReplicationWorkflow(t *testing.T) {
	t.Run("TestReverseAndResumeVolumeReplicationWorkflow_Success", func(tt *testing.T) {
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
		replicationActivity := replicationActivities.ReverseVolumeReplicationActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(replicationActivity.GetSrcBasePathReverse)
		env.RegisterActivity(replicationActivity.GetDstBasePathReverse)
		env.RegisterActivity(replicationActivity.GetSignedSrcTokenReverse)
		env.RegisterActivity(replicationActivity.GetSignedDstTokenReverse)
		env.RegisterActivity(replicationActivity.VerifyNewDstVolume)
		env.RegisterActivity(replicationActivity.ResizeNewDstVolumeIfNeeded)
		env.RegisterActivity(replicationActivity.ReverseAndResumeReplication)
		env.RegisterActivity(replicationActivity.DescribeRemoteJobOnDst)
		env.RegisterActivity(replicationActivity.UpdateVolumeReplicationAttributes)
		env.RegisterActivity(replicationActivity.DescribeRemoteJobOnSrc)
		env.RegisterActivity(replicationActivity.MountReplicationAfterReverse)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.ReverseAndResumeReplicationParams{
			AccountName: "test-account",
		}

		event := &replication.ReverseReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{
					BaseModel: datamodel.BaseModel{ID: 1},
					Name:      "test-replication",
				},
				SourceProjectNumber:      "123456789",
				DestinationProjectNumber: "987654321",
			},
		}

		reverseResult := &replication.ReverseReplicationResult{
			Event:            event,
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
			DbVolReplication: event.ReplicationModel,
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSrcBasePathReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetDstBasePathReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetSignedSrcTokenReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetSignedDstTokenReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("VerifyNewDstVolume", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("ResizeNewDstVolumeIfNeeded", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ReverseAndResumeReplication", mock.Anything, mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("DescribeRemoteJobOnDst", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVolumeReplicationAttributes", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("MountReplicationAfterReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("DescribeRemoteJobOnSrc", mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(ReverseAndResumeVolumeReplicationWorkflow, params, event)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		assert.Nil(tt, err)
		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestReverseAndResumeVolumeReplicationWorkflow_GetSrcBasePathError", func(tt *testing.T) {
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
		replicationActivity := replicationActivities.ReverseVolumeReplicationActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(replicationActivity.GetSrcBasePathReverse)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.ReverseAndResumeReplicationParams{
			AccountName: "test-account",
		}

		event := &replication.ReverseReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{
					BaseModel: datamodel.BaseModel{ID: 1},
					Name:      "test-replication",
				},
				SourceProjectNumber:      "123456789",
				DestinationProjectNumber: "987654321",
			},
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSrcBasePathReverse", mock.Anything, mock.Anything).Return(nil, assert.AnError)

		env.ExecuteWorkflow(ReverseAndResumeVolumeReplicationWorkflow, params, event)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestReverseAndResumeVolumeReplicationWorkflow_ReverseAndResumeError", func(tt *testing.T) {
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
		replicationActivity := replicationActivities.ReverseVolumeReplicationActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(replicationActivity.GetSrcBasePathReverse)
		env.RegisterActivity(replicationActivity.GetDstBasePathReverse)
		env.RegisterActivity(replicationActivity.GetSignedSrcTokenReverse)
		env.RegisterActivity(replicationActivity.GetSignedDstTokenReverse)
		env.RegisterActivity(replicationActivity.VerifyNewDstVolume)
		env.RegisterActivity(replicationActivity.ResizeNewDstVolumeIfNeeded)
		env.RegisterActivity(replicationActivity.ReverseAndResumeReplication)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.ReverseAndResumeReplicationParams{
			AccountName: "test-account",
		}

		event := &replication.ReverseReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{
					BaseModel: datamodel.BaseModel{ID: 1},
					Name:      "test-replication",
				},
				SourceProjectNumber:      "123456789",
				DestinationProjectNumber: "987654321",
			},
		}

		reverseResult := &replication.ReverseReplicationResult{
			Event:            event,
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
			DbVolReplication: event.ReplicationModel,
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSrcBasePathReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetDstBasePathReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetSignedSrcTokenReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetSignedDstTokenReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("VerifyNewDstVolume", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("ResizeNewDstVolumeIfNeeded", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ReverseAndResumeReplication", mock.Anything, mock.Anything, mock.Anything).Return(nil, assert.AnError)

		env.ExecuteWorkflow(ReverseAndResumeVolumeReplicationWorkflow, params, event)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestReverseAndResumeVolumeReplicationWorkflow_GetDstBasePathError", func(tt *testing.T) {
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
		replicationActivity := replicationActivities.ReverseVolumeReplicationActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(replicationActivity.GetSrcBasePathReverse)
		env.RegisterActivity(replicationActivity.GetDstBasePathReverse)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.ReverseAndResumeReplicationParams{
			AccountName: "test-account",
		}

		event := &replication.ReverseReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{
					BaseModel: datamodel.BaseModel{ID: 1},
					Name:      "test-replication",
				},
				SourceProjectNumber:      "123456789",
				DestinationProjectNumber: "987654321",
			},
		}

		reverseResult := &replication.ReverseReplicationResult{
			Event:            event,
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
			DbVolReplication: event.ReplicationModel,
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSrcBasePathReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetDstBasePathReverse", mock.Anything, mock.Anything).Return(nil, assert.AnError)

		env.ExecuteWorkflow(ReverseAndResumeVolumeReplicationWorkflow, params, event)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestReverseAndResumeVolumeReplicationWorkflow_GetSignedSrcTokenError", func(tt *testing.T) {
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
		replicationActivity := replicationActivities.ReverseVolumeReplicationActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(replicationActivity.GetSrcBasePathReverse)
		env.RegisterActivity(replicationActivity.GetDstBasePathReverse)
		env.RegisterActivity(replicationActivity.GetSignedSrcTokenReverse)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.ReverseAndResumeReplicationParams{
			AccountName: "test-account",
		}

		event := &replication.ReverseReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{
					BaseModel: datamodel.BaseModel{ID: 1},
					Name:      "test-replication",
				},
				SourceProjectNumber:      "123456789",
				DestinationProjectNumber: "987654321",
			},
		}

		reverseResult := &replication.ReverseReplicationResult{
			Event:            event,
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
			DbVolReplication: event.ReplicationModel,
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSrcBasePathReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetDstBasePathReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetSignedSrcTokenReverse", mock.Anything, mock.Anything).Return(nil, assert.AnError)

		env.ExecuteWorkflow(ReverseAndResumeVolumeReplicationWorkflow, params, event)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestReverseAndResumeVolumeReplicationWorkflow_GetSignedDstTokenError", func(tt *testing.T) {
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
		replicationActivity := replicationActivities.ReverseVolumeReplicationActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(replicationActivity.GetSrcBasePathReverse)
		env.RegisterActivity(replicationActivity.GetDstBasePathReverse)
		env.RegisterActivity(replicationActivity.GetSignedSrcTokenReverse)
		env.RegisterActivity(replicationActivity.GetSignedDstTokenReverse)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.ReverseAndResumeReplicationParams{
			AccountName: "test-account",
		}

		event := &replication.ReverseReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{
					BaseModel: datamodel.BaseModel{ID: 1},
					Name:      "test-replication",
				},
				SourceProjectNumber:      "123456789",
				DestinationProjectNumber: "987654321",
			},
		}

		reverseResult := &replication.ReverseReplicationResult{
			Event:            event,
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
			DbVolReplication: event.ReplicationModel,
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSrcBasePathReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetDstBasePathReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetSignedSrcTokenReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetSignedDstTokenReverse", mock.Anything, mock.Anything).Return(nil, assert.AnError)

		env.ExecuteWorkflow(ReverseAndResumeVolumeReplicationWorkflow, params, event)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestReverseAndResumeVolumeReplicationWorkflow_VerifyNewDstVolumeError", func(tt *testing.T) {
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
		replicationActivity := replicationActivities.ReverseVolumeReplicationActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(replicationActivity.GetSrcBasePathReverse)
		env.RegisterActivity(replicationActivity.GetDstBasePathReverse)
		env.RegisterActivity(replicationActivity.GetSignedSrcTokenReverse)
		env.RegisterActivity(replicationActivity.GetSignedDstTokenReverse)
		env.RegisterActivity(replicationActivity.VerifyNewDstVolume)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.ReverseAndResumeReplicationParams{
			AccountName: "test-account",
		}

		event := &replication.ReverseReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{
					BaseModel: datamodel.BaseModel{ID: 1},
					Name:      "test-replication",
				},
				SourceProjectNumber:      "123456789",
				DestinationProjectNumber: "987654321",
			},
		}

		reverseResult := &replication.ReverseReplicationResult{
			Event:            event,
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
			DbVolReplication: event.ReplicationModel,
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSrcBasePathReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetDstBasePathReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetSignedSrcTokenReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetSignedDstTokenReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("VerifyNewDstVolume", mock.Anything, mock.Anything).Return(nil, assert.AnError)

		env.ExecuteWorkflow(ReverseAndResumeVolumeReplicationWorkflow, params, event)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestReverseAndResumeVolumeReplicationWorkflow_ResizeNewDstVolumeError", func(tt *testing.T) {
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
		replicationActivity := replicationActivities.ReverseVolumeReplicationActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(replicationActivity.GetSrcBasePathReverse)
		env.RegisterActivity(replicationActivity.GetDstBasePathReverse)
		env.RegisterActivity(replicationActivity.GetSignedSrcTokenReverse)
		env.RegisterActivity(replicationActivity.GetSignedDstTokenReverse)
		env.RegisterActivity(replicationActivity.VerifyNewDstVolume)
		env.RegisterActivity(replicationActivity.ResizeNewDstVolumeIfNeeded)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.ReverseAndResumeReplicationParams{
			AccountName: "test-account",
		}

		event := &replication.ReverseReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{
					BaseModel: datamodel.BaseModel{ID: 1},
					Name:      "test-replication",
				},
				SourceProjectNumber:      "123456789",
				DestinationProjectNumber: "987654321",
			},
		}

		reverseResult := &replication.ReverseReplicationResult{
			Event:            event,
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
			DbVolReplication: event.ReplicationModel,
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSrcBasePathReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetDstBasePathReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetSignedSrcTokenReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetSignedDstTokenReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("VerifyNewDstVolume", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("ResizeNewDstVolumeIfNeeded", mock.Anything, mock.Anything).Return(assert.AnError)

		env.ExecuteWorkflow(ReverseAndResumeVolumeReplicationWorkflow, params, event)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestReverseAndResumeVolumeReplicationWorkflow_DescribeRemoteJobOnDstError", func(tt *testing.T) {
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
		replicationActivity := replicationActivities.ReverseVolumeReplicationActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(replicationActivity.GetSrcBasePathReverse)
		env.RegisterActivity(replicationActivity.GetDstBasePathReverse)
		env.RegisterActivity(replicationActivity.GetSignedSrcTokenReverse)
		env.RegisterActivity(replicationActivity.GetSignedDstTokenReverse)
		env.RegisterActivity(replicationActivity.VerifyNewDstVolume)
		env.RegisterActivity(replicationActivity.ResizeNewDstVolumeIfNeeded)
		env.RegisterActivity(replicationActivity.ReverseAndResumeReplication)
		env.RegisterActivity(replicationActivity.DescribeRemoteJobOnDst)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.ReverseAndResumeReplicationParams{
			AccountName: "test-account",
		}

		event := &replication.ReverseReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{
					BaseModel: datamodel.BaseModel{ID: 1},
					Name:      "test-replication",
				},
				SourceProjectNumber:      "123456789",
				DestinationProjectNumber: "987654321",
			},
		}

		reverseResult := &replication.ReverseReplicationResult{
			Event:            event,
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
			DbVolReplication: event.ReplicationModel,
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSrcBasePathReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetDstBasePathReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetSignedSrcTokenReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetSignedDstTokenReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("VerifyNewDstVolume", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("ResizeNewDstVolumeIfNeeded", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ReverseAndResumeReplication", mock.Anything, mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("DescribeRemoteJobOnDst", mock.Anything, mock.Anything).Return(nil, assert.AnError)

		env.ExecuteWorkflow(ReverseAndResumeVolumeReplicationWorkflow, params, event)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestReverseAndResumeVolumeReplicationWorkflow_UpdateVolumeReplicationAttributesError", func(tt *testing.T) {
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
		replicationActivity := replicationActivities.ReverseVolumeReplicationActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(replicationActivity.GetSrcBasePathReverse)
		env.RegisterActivity(replicationActivity.GetDstBasePathReverse)
		env.RegisterActivity(replicationActivity.GetSignedSrcTokenReverse)
		env.RegisterActivity(replicationActivity.GetSignedDstTokenReverse)
		env.RegisterActivity(replicationActivity.VerifyNewDstVolume)
		env.RegisterActivity(replicationActivity.ResizeNewDstVolumeIfNeeded)
		env.RegisterActivity(replicationActivity.ReverseAndResumeReplication)
		env.RegisterActivity(replicationActivity.DescribeRemoteJobOnDst)
		env.RegisterActivity(replicationActivity.UpdateVolumeReplicationAttributes)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.ReverseAndResumeReplicationParams{
			AccountName: "test-account",
		}

		event := &replication.ReverseReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{
					BaseModel: datamodel.BaseModel{ID: 1},
					Name:      "test-replication",
				},
				SourceProjectNumber:      "123456789",
				DestinationProjectNumber: "987654321",
			},
		}

		reverseResult := &replication.ReverseReplicationResult{
			Event:            event,
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
			DbVolReplication: event.ReplicationModel,
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSrcBasePathReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetDstBasePathReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetSignedSrcTokenReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetSignedDstTokenReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("VerifyNewDstVolume", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("ResizeNewDstVolumeIfNeeded", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ReverseAndResumeReplication", mock.Anything, mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("DescribeRemoteJobOnDst", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVolumeReplicationAttributes", mock.Anything, mock.Anything).Return(nil, assert.AnError)

		env.ExecuteWorkflow(ReverseAndResumeVolumeReplicationWorkflow, params, event)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestReverseAndResumeVolumeReplicationWorkflow_DescribeRemoteJobOnSrcError", func(tt *testing.T) {
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
		replicationActivity := replicationActivities.ReverseVolumeReplicationActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(replicationActivity.GetSrcBasePathReverse)
		env.RegisterActivity(replicationActivity.GetDstBasePathReverse)
		env.RegisterActivity(replicationActivity.GetSignedSrcTokenReverse)
		env.RegisterActivity(replicationActivity.GetSignedDstTokenReverse)
		env.RegisterActivity(replicationActivity.VerifyNewDstVolume)
		env.RegisterActivity(replicationActivity.ResizeNewDstVolumeIfNeeded)
		env.RegisterActivity(replicationActivity.ReverseAndResumeReplication)
		env.RegisterActivity(replicationActivity.DescribeRemoteJobOnDst)
		env.RegisterActivity(replicationActivity.UpdateVolumeReplicationAttributes)
		env.RegisterActivity(replicationActivity.DescribeRemoteJobOnSrc)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.ReverseAndResumeReplicationParams{
			AccountName: "test-account",
		}

		event := &replication.ReverseReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{
					BaseModel: datamodel.BaseModel{ID: 1},
					Name:      "test-replication",
				},
				SourceProjectNumber:      "123456789",
				DestinationProjectNumber: "987654321",
			},
		}

		reverseResult := &replication.ReverseReplicationResult{
			Event:            event,
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
			DbVolReplication: event.ReplicationModel,
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSrcBasePathReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetDstBasePathReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetSignedSrcTokenReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetSignedDstTokenReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("VerifyNewDstVolume", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("ResizeNewDstVolumeIfNeeded", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ReverseAndResumeReplication", mock.Anything, mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("DescribeRemoteJobOnDst", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVolumeReplicationAttributes", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("DescribeRemoteJobOnSrc", mock.Anything, mock.Anything).Return(nil, assert.AnError)

		env.ExecuteWorkflow(ReverseAndResumeVolumeReplicationWorkflow, params, event)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestReverseAndResumeVolumeReplicationWorkflow_MountReplicationAfterReverseError", func(tt *testing.T) {
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
		replicationActivity := replicationActivities.ReverseVolumeReplicationActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(replicationActivity.GetSrcBasePathReverse)
		env.RegisterActivity(replicationActivity.GetDstBasePathReverse)
		env.RegisterActivity(replicationActivity.GetSignedSrcTokenReverse)
		env.RegisterActivity(replicationActivity.GetSignedDstTokenReverse)
		env.RegisterActivity(replicationActivity.VerifyNewDstVolume)
		env.RegisterActivity(replicationActivity.ResizeNewDstVolumeIfNeeded)
		env.RegisterActivity(replicationActivity.ReverseAndResumeReplication)
		env.RegisterActivity(replicationActivity.DescribeRemoteJobOnDst)
		env.RegisterActivity(replicationActivity.UpdateVolumeReplicationAttributes)
		env.RegisterActivity(replicationActivity.DescribeRemoteJobOnSrc)
		env.RegisterActivity(replicationActivity.MountReplicationAfterReverse)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.ReverseAndResumeReplicationParams{
			AccountName: "test-account",
		}

		event := &replication.ReverseReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{
					BaseModel: datamodel.BaseModel{ID: 1},
					Name:      "test-replication",
				},
				SourceProjectNumber:      "123456789",
				DestinationProjectNumber: "987654321",
			},
		}

		reverseResult := &replication.ReverseReplicationResult{
			Event:            event,
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
			DbVolReplication: event.ReplicationModel,
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSrcBasePathReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetDstBasePathReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetSignedSrcTokenReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetSignedDstTokenReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("VerifyNewDstVolume", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("ResizeNewDstVolumeIfNeeded", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ReverseAndResumeReplication", mock.Anything, mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("DescribeRemoteJobOnDst", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVolumeReplicationAttributes", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("DescribeRemoteJobOnSrc", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("MountReplicationAfterReverse", mock.Anything, mock.Anything).Return(nil, assert.AnError)

		env.ExecuteWorkflow(ReverseAndResumeVolumeReplicationWorkflow, params, event)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestReverseAndResumeVolumeReplicationWorkflow_SetupError", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})

		// Test with nil params to trigger setup error
		env.ExecuteWorkflow(ReverseAndResumeVolumeReplicationWorkflow, nil, nil)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})
}
