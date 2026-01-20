package replicationWorkflows

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/replicationActivities"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/replication"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
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
		env.RegisterActivity(replicationActivity.DescribeRemoteJobOnSrc)
		env.RegisterActivity(replicationActivity.ReverseAndResumeReplication)
		env.RegisterActivity(replicationActivity.DescribeRemoteJobOnSrc)
		env.RegisterActivity(replicationActivity.UpdateVolumeReplicationAttributesSrc)
		env.RegisterActivity(replicationActivity.DescribeRemoteJobOnSrc)
		env.RegisterActivity(replicationActivity.UpdateVolumeReplicationAttributesDst)
		env.RegisterActivity(replicationActivity.DescribeRemoteJobOnDst)
		env.RegisterActivity(replicationActivity.MountReplicationAfterReverse)
		env.RegisterActivity(replicationActivity.CleanupOldReplication)
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
		env.OnActivity("ResizeNewDstVolumeIfNeeded", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("ReverseAndResumeReplication", mock.Anything, mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("DescribeRemoteJobOnSrc", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVolumeReplicationAttributesSrc", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("DescribeRemoteJobOnSrc", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVolumeReplicationAttributesDst", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("DescribeRemoteJobOnDst", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("MountReplicationAfterReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("CleanupOldReplication", mock.Anything, mock.Anything).Return(reverseResult, nil)

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
		env.RegisterActivity(replicationActivity.DescribeRemoteJobOnSrc)
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
		env.OnActivity("ResizeNewDstVolumeIfNeeded", mock.Anything, mock.Anything).Return(reverseResult, nil)
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
		env.RegisterActivity(replicationActivity.DescribeRemoteJobOnSrc)
		env.RegisterActivity(replicationActivity.ReverseAndResumeReplication)
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
		env.OnActivity("ResizeNewDstVolumeIfNeeded", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("ReverseAndResumeReplication", mock.Anything, mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("DescribeRemoteJobOnSrc", mock.Anything, mock.Anything).Return(nil, assert.AnError)

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
		env.RegisterActivity(replicationActivity.DescribeRemoteJobOnSrc)
		env.RegisterActivity(replicationActivity.ReverseAndResumeReplication)
		env.RegisterActivity(replicationActivity.DescribeRemoteJobOnSrc)
		env.RegisterActivity(replicationActivity.UpdateVolumeReplicationAttributesSrc)
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
		env.OnActivity("ResizeNewDstVolumeIfNeeded", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("ReverseAndResumeReplication", mock.Anything, mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("DescribeRemoteJobOnSrc", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVolumeReplicationAttributesSrc", mock.Anything, mock.Anything).Return(nil, assert.AnError)

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
		env.RegisterActivity(replicationActivity.DescribeRemoteJobOnSrc)
		env.RegisterActivity(replicationActivity.ReverseAndResumeReplication)
		env.RegisterActivity(replicationActivity.DescribeRemoteJobOnSrc)
		env.RegisterActivity(replicationActivity.UpdateVolumeReplicationAttributesSrc)
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
		env.OnActivity("ResizeNewDstVolumeIfNeeded", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("ReverseAndResumeReplication", mock.Anything, mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("DescribeRemoteJobOnSrc", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVolumeReplicationAttributesSrc", mock.Anything, mock.Anything).Return(reverseResult, nil)
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
		env.RegisterActivity(replicationActivity.DescribeRemoteJobOnSrc)
		env.RegisterActivity(replicationActivity.ReverseAndResumeReplication)
		env.RegisterActivity(replicationActivity.DescribeRemoteJobOnSrc)
		env.RegisterActivity(replicationActivity.UpdateVolumeReplicationAttributesSrc)
		env.RegisterActivity(replicationActivity.DescribeRemoteJobOnSrc)
		env.RegisterActivity(replicationActivity.UpdateVolumeReplicationAttributesDst)
		env.RegisterActivity(replicationActivity.DescribeRemoteJobOnDst)
		env.RegisterActivity(replicationActivity.MountReplicationAfterReverse)
		env.RegisterActivity(replicationActivity.CleanupOldReplication)
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
		env.OnActivity("ResizeNewDstVolumeIfNeeded", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("ReverseAndResumeReplication", mock.Anything, mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("DescribeRemoteJobOnSrc", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVolumeReplicationAttributesSrc", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("DescribeRemoteJobOnSrc", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("UpdateVolumeReplicationAttributesDst", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("DescribeRemoteJobOnDst", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CleanupOldReplication", mock.Anything, mock.Anything).Return(nil, assert.AnError)
		env.OnActivity("MountReplicationAfterReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)

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

// Test_isReverseResumeQuotaRuleFailure tests the helper function (lines 73-74, 76)
func Test_isReverseResumeQuotaRuleFailure(t *testing.T) {
	t.Run("WhenErrorIsNil", func(tt *testing.T) {
		result := isReverseResumeQuotaRuleFailure(nil)
		assert.False(tt, result, "Should return false for nil error")
	})

	t.Run("WhenErrorIsReverseResumeQuotaRuleFailure", func(tt *testing.T) {
		err := &vsaerrors.CustomError{
			TrackingID: vsaerrors.ErrReverseResumeReplicationQuotaRuleFailure,
		}
		result := isReverseResumeQuotaRuleFailure(err)
		assert.True(tt, result, "Should return true for reverse resume quota rule failure")
	})

	t.Run("WhenErrorIsOtherFailure", func(tt *testing.T) {
		err := &vsaerrors.CustomError{
			TrackingID: vsaerrors.ErrInternalServerError,
		}
		result := isReverseResumeQuotaRuleFailure(err)
		assert.False(tt, result, "Should return false for other errors")
	})
}

// TestReverseResumeWorkflow_PartialSuccessHandling tests lines 56-58, 60 and quota rule sync lines
func TestReverseResumeWorkflow_PartialSuccessHandling(t *testing.T) {
	// Enable quota rule sync for this test
	originalQuotaRuleSync := quotaRuleSync
	originalHydrationEnabled := hydrationEnabled
	quotaRuleSync = true
	hydrationEnabled = true
	defer func() {
		quotaRuleSync = originalQuotaRuleSync
		hydrationEnabled = originalHydrationEnabled
	}()

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
	mockHeader := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{
			"logParam": encodedValue,
		},
	}
	env.SetHeader(mockHeader)

	// Setup mock storage and activities
	mockStorage := database.NewMockStorage(t)
	commonActivity := activities.CommonActivities{SE: mockStorage}
	reverseReplicationActivity := replicationActivities.ReverseVolumeReplicationActivity{SE: mockStorage}

	// Register all activities
	env.RegisterActivity(reverseReplicationActivity.GetSrcBasePathReverse)
	env.RegisterActivity(reverseReplicationActivity.GetDstBasePathReverse)
	env.RegisterActivity(reverseReplicationActivity.GetSignedSrcTokenReverse)
	env.RegisterActivity(reverseReplicationActivity.GetSignedDstTokenReverse)
	env.RegisterActivity(reverseReplicationActivity.VerifyNewDstVolume)
	env.RegisterActivity(reverseReplicationActivity.ResizeNewDstVolumeIfNeeded)
	env.RegisterActivity(reverseReplicationActivity.ReverseAndResumeReplication)
	env.RegisterActivity(reverseReplicationActivity.DescribeRemoteJobOnSrc)
	env.RegisterActivity(reverseReplicationActivity.DescribeRemoteJobOnDst)
	env.RegisterActivity(reverseReplicationActivity.UpdateVolumeReplicationAttributesSrc)
	env.RegisterActivity(reverseReplicationActivity.UpdateVolumeReplicationAttributesDst)
	env.RegisterActivity(reverseReplicationActivity.MountReplicationAfterReverse)
	env.RegisterActivity(reverseReplicationActivity.ListQuotaRulesOnNewSourceReverse)
	env.RegisterActivity(reverseReplicationActivity.ListQuotaRulesOnNewDestinationReverse)
	env.RegisterActivity(reverseReplicationActivity.CleanupOldReplication)
	env.RegisterActivity(commonActivity.UpdateJobStatus)

	params := &commonparams.ReverseAndResumeReplicationParams{
		AccountName:   "test-account",
		CorrelationId: "test-correlation-id",
	}

	event := &replication.ReverseReplicationEvent{
		CommonReplicationEventParams: replication.CommonReplicationEventParams{
			SourceProjectNumber:      "123456789",
			DestinationProjectNumber: "987654321",
			ReplicationModel: &datamodel.VolumeReplication{
				BaseModel: datamodel.BaseModel{UUID: "replication-uuid"},
				ReplicationAttributes: &datamodel.ReplicationDetails{
					SourceLocation:      "us-east1",
					DestinationLocation: "us-west1",
				},
				HybridReplicationAttributes: nil, // Not hybrid
			},
		},
	}

	// Mock all activities up to quota rule sync
	env.OnActivity("GetSrcBasePathReverse", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ReverseReplicationResult) (*replication.ReverseReplicationResult, error) {
		result.SrcBasePath = stringPtr("https://src-path")
		return result, nil
	})
	env.OnActivity("GetDstBasePathReverse", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ReverseReplicationResult) (*replication.ReverseReplicationResult, error) {
		result.DstBasePath = stringPtr("https://dst-path")
		return result, nil
	})
	env.OnActivity("GetSignedSrcTokenReverse", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ReverseReplicationResult) (*replication.ReverseReplicationResult, error) {
		result.SrcJwtToken = stringPtr("src-token")
		return result, nil
	})
	env.OnActivity("GetSignedDstTokenReverse", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ReverseReplicationResult) (*replication.ReverseReplicationResult, error) {
		result.DstJwtToken = stringPtr("dst-token")
		return result, nil
	})
	env.OnActivity("VerifyNewDstVolume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ReverseReplicationResult) (*replication.ReverseReplicationResult, error) {
		return result, nil
	})
	env.OnActivity("ResizeNewDstVolumeIfNeeded", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ReverseReplicationResult) (*replication.ReverseReplicationResult, error) {
		return result, nil
	})
	env.OnActivity("DescribeRemoteJobOnSrc", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("ReverseAndResumeReplication", mock.Anything, mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ReverseReplicationResult, params *commonparams.ReverseAndResumeReplicationParams) (*replication.ReverseReplicationResult, error) {
		return result, nil
	})
	env.OnActivity("UpdateVolumeReplicationAttributesSrc", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ReverseReplicationResult) (*replication.ReverseReplicationResult, error) {
		return result, nil
	})
	env.OnActivity("UpdateVolumeReplicationAttributesDst", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ReverseReplicationResult) (*replication.ReverseReplicationResult, error) {
		return result, nil
	})
	env.OnActivity("DescribeRemoteJobOnDst", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("MountReplicationAfterReverse", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ReverseReplicationResult) (*replication.ReverseReplicationResult, error) {
		result.NewDstVolume = &googleproxyclient.VolumeV1beta{
			ResourceId: "dst-volume-id",
		}
		return result, nil
	})

	// Mock quota rule activities to trigger partial success (lines 206-210 failure)
	env.OnActivity("ListQuotaRulesOnNewSourceReverse", mock.Anything, mock.Anything).Return(nil, assert.AnError)

	// Mock UpdateJobStatus to capture the partial success call (lines 58-60)
	var capturedStatus string
	var capturedErrorDetails string
	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, job *datamodel.Job) error {
			capturedStatus = job.State
			capturedErrorDetails = job.ErrorDetails
			return nil
		})

	// Execute workflow
	env.ExecuteWorkflow(ReverseAndResumeVolumeReplicationWorkflow, params, event)

	// Verify workflow completed
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())

	// Verify lines 56-58, 60: partial success handling
	assert.Equal(t, string(models.JobsStateDONE), capturedStatus, "Should set status to DONE for partial success")
	assert.Contains(t, capturedErrorDetails, reverseResumeQuotaRuleError, "Should have reverseResumeQuotaRuleError message for partial success")
}

// TestReverseResumeWorkflow_QuotaRuleSync_Lines covers quota rule sync flow lines
func TestReverseResumeWorkflow_QuotaRuleSync_Lines(t *testing.T) {
	t.Run("Lines202_203_HybridReplication_SkipsQuotaRuleSync", func(tt *testing.T) {
		// Enable quota rule sync
		originalQuotaRuleSync := quotaRuleSync
		quotaRuleSync = true
		defer func() { quotaRuleSync = originalQuotaRuleSync }()

		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)

		mockStorage := database.NewMockStorage(tt)
		commonActivity := activities.CommonActivities{SE: mockStorage}
		reverseReplicationActivity := replicationActivities.ReverseVolumeReplicationActivity{SE: mockStorage}

		// Register activities
		env.RegisterActivity(reverseReplicationActivity.GetSrcBasePathReverse)
		env.RegisterActivity(reverseReplicationActivity.GetDstBasePathReverse)
		env.RegisterActivity(reverseReplicationActivity.GetSignedSrcTokenReverse)
		env.RegisterActivity(reverseReplicationActivity.GetSignedDstTokenReverse)
		env.RegisterActivity(reverseReplicationActivity.VerifyNewDstVolume)
		env.RegisterActivity(reverseReplicationActivity.ResizeNewDstVolumeIfNeeded)
		env.RegisterActivity(reverseReplicationActivity.ReverseAndResumeReplication)
		env.RegisterActivity(reverseReplicationActivity.DescribeRemoteJobOnSrc)
		env.RegisterActivity(reverseReplicationActivity.DescribeRemoteJobOnDst)
		env.RegisterActivity(reverseReplicationActivity.UpdateVolumeReplicationAttributesSrc)
		env.RegisterActivity(reverseReplicationActivity.UpdateVolumeReplicationAttributesDst)
		env.RegisterActivity(reverseReplicationActivity.MountReplicationAfterReverse)
		env.RegisterActivity(reverseReplicationActivity.CleanupOldReplication)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.ReverseAndResumeReplicationParams{
			AccountName: "test-account",
		}

		hybridReplicationType := string(models.HybridReplicationParametersReplicationTypeONPREM)
		event := &replication.ReverseReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				SourceProjectNumber:      "123456789",
				DestinationProjectNumber: "987654321",
				ReplicationModel: &datamodel.VolumeReplication{
					BaseModel: datamodel.BaseModel{UUID: "replication-uuid"},
					ReplicationAttributes: &datamodel.ReplicationDetails{
						SourceLocation:      "us-east1",
						DestinationLocation: "us-west1",
					},
					HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
						HybridReplicationType: &hybridReplicationType,
					},
				},
			},
		}

		// Mock all activities
		reverseResult := &replication.ReverseReplicationResult{
			Event:            event,
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
			DbVolReplication: event.ReplicationModel,
			SrcBasePath:      stringPtr("https://src-path"),
			DstBasePath:      stringPtr("https://dst-path"),
			SrcJwtToken:      stringPtr("src-token"),
			DstJwtToken:      stringPtr("dst-token"),
		}

		env.OnActivity("GetSrcBasePathReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetDstBasePathReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetSignedSrcTokenReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetSignedDstTokenReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("VerifyNewDstVolume", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("ResizeNewDstVolumeIfNeeded", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("ReverseAndResumeReplication", mock.Anything, mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("DescribeRemoteJobOnSrc", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVolumeReplicationAttributesSrc", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("UpdateVolumeReplicationAttributesDst", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("DescribeRemoteJobOnDst", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("MountReplicationAfterReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("CleanupOldReplication", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(ReverseAndResumeVolumeReplicationWorkflow, params, event)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("Lines218_222_227_ListDestinationFailure", func(tt *testing.T) {
		// Enable quota rule sync
		originalQuotaRuleSync := quotaRuleSync
		quotaRuleSync = true
		defer func() { quotaRuleSync = originalQuotaRuleSync }()

		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)

		mockStorage := database.NewMockStorage(tt)
		commonActivity := activities.CommonActivities{SE: mockStorage}
		reverseReplicationActivity := replicationActivities.ReverseVolumeReplicationActivity{SE: mockStorage}

		// Register activities
		env.RegisterActivity(reverseReplicationActivity.GetSrcBasePathReverse)
		env.RegisterActivity(reverseReplicationActivity.GetDstBasePathReverse)
		env.RegisterActivity(reverseReplicationActivity.GetSignedSrcTokenReverse)
		env.RegisterActivity(reverseReplicationActivity.GetSignedDstTokenReverse)
		env.RegisterActivity(reverseReplicationActivity.VerifyNewDstVolume)
		env.RegisterActivity(reverseReplicationActivity.ResizeNewDstVolumeIfNeeded)
		env.RegisterActivity(reverseReplicationActivity.ReverseAndResumeReplication)
		env.RegisterActivity(reverseReplicationActivity.DescribeRemoteJobOnSrc)
		env.RegisterActivity(reverseReplicationActivity.DescribeRemoteJobOnDst)
		env.RegisterActivity(reverseReplicationActivity.UpdateVolumeReplicationAttributesSrc)
		env.RegisterActivity(reverseReplicationActivity.UpdateVolumeReplicationAttributesDst)
		env.RegisterActivity(reverseReplicationActivity.MountReplicationAfterReverse)
		env.RegisterActivity(reverseReplicationActivity.ListQuotaRulesOnNewSourceReverse)
		env.RegisterActivity(reverseReplicationActivity.ListQuotaRulesOnNewDestinationReverse)
		env.RegisterActivity(reverseReplicationActivity.CleanupOldReplication)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.ReverseAndResumeReplicationParams{
			AccountName: "test-account",
		}

		event := &replication.ReverseReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				SourceProjectNumber:      "123456789",
				DestinationProjectNumber: "987654321",
				ReplicationModel: &datamodel.VolumeReplication{
					BaseModel: datamodel.BaseModel{UUID: "replication-uuid"},
					ReplicationAttributes: &datamodel.ReplicationDetails{
						SourceLocation:      "us-east1",
						DestinationLocation: "us-west1",
					},
					HybridReplicationAttributes: nil,
				},
			},
		}

		reverseResult := &replication.ReverseReplicationResult{
			Event:            event,
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
			DbVolReplication: event.ReplicationModel,
			SrcBasePath:      stringPtr("https://src-path"),
			DstBasePath:      stringPtr("https://dst-path"),
			SrcJwtToken:      stringPtr("src-token"),
			DstJwtToken:      stringPtr("dst-token"),
		}

		env.OnActivity("GetSrcBasePathReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetDstBasePathReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetSignedSrcTokenReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetSignedDstTokenReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("VerifyNewDstVolume", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("ResizeNewDstVolumeIfNeeded", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("ReverseAndResumeReplication", mock.Anything, mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("DescribeRemoteJobOnSrc", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVolumeReplicationAttributesSrc", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("UpdateVolumeReplicationAttributesDst", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("DescribeRemoteJobOnDst", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("MountReplicationAfterReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)

		// Mock ListQuotaRulesOnNewSourceReverse success
		env.OnActivity("ListQuotaRulesOnNewSourceReverse", mock.Anything, mock.Anything).Return([]*datamodel.QuotaRule{
			{BaseModel: datamodel.BaseModel{UUID: "quota-1"}, Name: "rule-1"},
		}, nil)

		// Mock ListQuotaRulesOnNewDestinationReverse failure (lines 218-222, 227)
		env.OnActivity("ListQuotaRulesOnNewDestinationReverse", mock.Anything, mock.Anything).Return(nil, assert.AnError)

		var capturedStatus string
		var capturedErrorDetails string
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything, mock.Anything).
			Return(func(ctx context.Context, job *datamodel.Job) error {
				capturedStatus = job.State
				capturedErrorDetails = job.ErrorDetails
				return nil
			})

		env.ExecuteWorkflow(ReverseAndResumeVolumeReplicationWorkflow, params, event)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
		assert.Equal(tt, string(models.JobsStateDONE), capturedStatus)
		assert.Contains(tt, capturedErrorDetails, reverseResumeQuotaRuleError)
	})

	t.Run("Lines230_232_238_240_DehydrateFailure", func(tt *testing.T) {
		// Enable quota rule sync and hydration
		originalQuotaRuleSync := quotaRuleSync
		originalHydrationEnabled := hydrationEnabled
		quotaRuleSync = true
		hydrationEnabled = true
		defer func() {
			quotaRuleSync = originalQuotaRuleSync
			hydrationEnabled = originalHydrationEnabled
		}()

		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)

		mockStorage := database.NewMockStorage(tt)
		commonActivity := activities.CommonActivities{SE: mockStorage}
		reverseReplicationActivity := replicationActivities.ReverseVolumeReplicationActivity{SE: mockStorage}

		// Register activities
		env.RegisterActivity(reverseReplicationActivity.GetSrcBasePathReverse)
		env.RegisterActivity(reverseReplicationActivity.GetDstBasePathReverse)
		env.RegisterActivity(reverseReplicationActivity.GetSignedSrcTokenReverse)
		env.RegisterActivity(reverseReplicationActivity.GetSignedDstTokenReverse)
		env.RegisterActivity(reverseReplicationActivity.VerifyNewDstVolume)
		env.RegisterActivity(reverseReplicationActivity.ResizeNewDstVolumeIfNeeded)
		env.RegisterActivity(reverseReplicationActivity.ReverseAndResumeReplication)
		env.RegisterActivity(reverseReplicationActivity.DescribeRemoteJobOnSrc)
		env.RegisterActivity(reverseReplicationActivity.DescribeRemoteJobOnDst)
		env.RegisterActivity(reverseReplicationActivity.UpdateVolumeReplicationAttributesSrc)
		env.RegisterActivity(reverseReplicationActivity.UpdateVolumeReplicationAttributesDst)
		env.RegisterActivity(reverseReplicationActivity.MountReplicationAfterReverse)
		env.RegisterActivity(reverseReplicationActivity.ListQuotaRulesOnNewSourceReverse)
		env.RegisterActivity(reverseReplicationActivity.ListQuotaRulesOnNewDestinationReverse)
		env.RegisterActivity(reverseReplicationActivity.DehydrateQuotaRulesReverse)
		env.RegisterActivity(reverseReplicationActivity.CleanupOldReplication)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.ReverseAndResumeReplicationParams{
			AccountName: "test-account",
		}

		event := &replication.ReverseReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				SourceProjectNumber:      "123456789",
				DestinationProjectNumber: "987654321",
				ReplicationModel: &datamodel.VolumeReplication{
					BaseModel: datamodel.BaseModel{UUID: "replication-uuid"},
					ReplicationAttributes: &datamodel.ReplicationDetails{
						SourceLocation:      "us-east1",
						DestinationLocation: "us-west1",
					},
					HybridReplicationAttributes: nil,
				},
			},
		}

		reverseResult := &replication.ReverseReplicationResult{
			Event:            event,
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
			DbVolReplication: event.ReplicationModel,
			SrcBasePath:      stringPtr("https://src-path"),
			DstBasePath:      stringPtr("https://dst-path"),
			SrcJwtToken:      stringPtr("src-token"),
			DstJwtToken:      stringPtr("dst-token"),
			NewDstVolume: &googleproxyclient.VolumeV1beta{
				ResourceId: "dst-volume-id",
			},
		}

		env.OnActivity("GetSrcBasePathReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetDstBasePathReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetSignedSrcTokenReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetSignedDstTokenReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("VerifyNewDstVolume", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("ResizeNewDstVolumeIfNeeded", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("ReverseAndResumeReplication", mock.Anything, mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("DescribeRemoteJobOnSrc", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVolumeReplicationAttributesSrc", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("UpdateVolumeReplicationAttributesDst", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("DescribeRemoteJobOnDst", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("MountReplicationAfterReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)

		// Mock quota rule activities
		env.OnActivity("ListQuotaRulesOnNewSourceReverse", mock.Anything, mock.Anything).Return([]*datamodel.QuotaRule{
			{BaseModel: datamodel.BaseModel{UUID: "quota-1"}, Name: "rule-1"},
		}, nil)
		env.OnActivity("ListQuotaRulesOnNewDestinationReverse", mock.Anything, mock.Anything).Return([]*datamodel.QuotaRule{
			{BaseModel: datamodel.BaseModel{UUID: "quota-2"}, Name: "rule-2"},
		}, nil)

		// Mock DehydrateQuotaRulesReverse failure (lines 238-240)
		env.OnActivity("DehydrateQuotaRulesReverse", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, assert.AnError)

		var capturedStatus string
		var capturedErrorDetails string
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything, mock.Anything).
			Return(func(ctx context.Context, job *datamodel.Job) error {
				capturedStatus = job.State
				capturedErrorDetails = job.ErrorDetails
				return nil
			})

		env.ExecuteWorkflow(ReverseAndResumeVolumeReplicationWorkflow, params, event)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
		assert.Equal(tt, string(models.JobsStateDONE), capturedStatus)
		assert.Contains(tt, capturedErrorDetails, reverseResumeQuotaRuleError)
	})

	t.Run("Lines247_249_PartialDehydration", func(tt *testing.T) {
		// Enable quota rule sync and hydration
		originalQuotaRuleSync := quotaRuleSync
		originalHydrationEnabled := hydrationEnabled
		quotaRuleSync = true
		hydrationEnabled = true
		defer func() {
			quotaRuleSync = originalQuotaRuleSync
			hydrationEnabled = originalHydrationEnabled
		}()

		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)

		mockStorage := database.NewMockStorage(tt)
		commonActivity := activities.CommonActivities{SE: mockStorage}
		reverseReplicationActivity := replicationActivities.ReverseVolumeReplicationActivity{SE: mockStorage}

		// Register activities
		env.RegisterActivity(reverseReplicationActivity.GetSrcBasePathReverse)
		env.RegisterActivity(reverseReplicationActivity.GetDstBasePathReverse)
		env.RegisterActivity(reverseReplicationActivity.GetSignedSrcTokenReverse)
		env.RegisterActivity(reverseReplicationActivity.GetSignedDstTokenReverse)
		env.RegisterActivity(reverseReplicationActivity.VerifyNewDstVolume)
		env.RegisterActivity(reverseReplicationActivity.ResizeNewDstVolumeIfNeeded)
		env.RegisterActivity(reverseReplicationActivity.ReverseAndResumeReplication)
		env.RegisterActivity(reverseReplicationActivity.DescribeRemoteJobOnSrc)
		env.RegisterActivity(reverseReplicationActivity.DescribeRemoteJobOnDst)
		env.RegisterActivity(reverseReplicationActivity.UpdateVolumeReplicationAttributesSrc)
		env.RegisterActivity(reverseReplicationActivity.UpdateVolumeReplicationAttributesDst)
		env.RegisterActivity(reverseReplicationActivity.MountReplicationAfterReverse)
		env.RegisterActivity(reverseReplicationActivity.ListQuotaRulesOnNewSourceReverse)
		env.RegisterActivity(reverseReplicationActivity.ListQuotaRulesOnNewDestinationReverse)
		env.RegisterActivity(reverseReplicationActivity.DehydrateQuotaRulesReverse)
		env.RegisterActivity(reverseReplicationActivity.AddNewSrcQuotaRulesToNewDstDBReverse)
		env.RegisterActivity(reverseReplicationActivity.HydrateQuotaRulesReverse)
		env.RegisterActivity(reverseReplicationActivity.CleanupOldReplication)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.ReverseAndResumeReplicationParams{
			AccountName: "test-account",
		}

		event := &replication.ReverseReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				SourceProjectNumber:      "123456789",
				DestinationProjectNumber: "987654321",
				ReplicationModel: &datamodel.VolumeReplication{
					BaseModel: datamodel.BaseModel{UUID: "replication-uuid"},
					ReplicationAttributes: &datamodel.ReplicationDetails{
						SourceLocation:      "us-east1",
						DestinationLocation: "us-west1",
					},
					HybridReplicationAttributes: nil,
				},
			},
		}

		reverseResult := &replication.ReverseReplicationResult{
			Event:            event,
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
			DbVolReplication: event.ReplicationModel,
			SrcBasePath:      stringPtr("https://src-path"),
			DstBasePath:      stringPtr("https://dst-path"),
			SrcJwtToken:      stringPtr("src-token"),
			DstJwtToken:      stringPtr("dst-token"),
			NewDstVolume: &googleproxyclient.VolumeV1beta{
				ResourceId: "dst-volume-id",
			},
		}

		env.OnActivity("GetSrcBasePathReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetDstBasePathReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetSignedSrcTokenReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetSignedDstTokenReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("VerifyNewDstVolume", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("ResizeNewDstVolumeIfNeeded", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("ReverseAndResumeReplication", mock.Anything, mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("DescribeRemoteJobOnSrc", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVolumeReplicationAttributesSrc", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("UpdateVolumeReplicationAttributesDst", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("DescribeRemoteJobOnDst", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("MountReplicationAfterReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)

		// Mock quota rule activities
		env.OnActivity("ListQuotaRulesOnNewSourceReverse", mock.Anything, mock.Anything).Return([]*datamodel.QuotaRule{
			{BaseModel: datamodel.BaseModel{UUID: "quota-1"}, Name: "rule-1"},
		}, nil)
		env.OnActivity("ListQuotaRulesOnNewDestinationReverse", mock.Anything, mock.Anything).Return([]*datamodel.QuotaRule{
			{BaseModel: datamodel.BaseModel{UUID: "quota-2"}, Name: "rule-2"},
			{BaseModel: datamodel.BaseModel{UUID: "quota-3"}, Name: "rule-3"},
		}, nil)

		// Mock DehydrateQuotaRulesReverse with partial success (lines 247-249)
		partiallyDehydrated := []*datamodel.QuotaRule{
			{BaseModel: datamodel.BaseModel{UUID: "quota-2"}, Name: "rule-2"},
		}
		env.OnActivity("DehydrateQuotaRulesReverse", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(partiallyDehydrated, nil)

		// Mock AddNewSrcQuotaRulesToNewDstDBReverse
		env.OnActivity("AddNewSrcQuotaRulesToNewDstDBReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)

		// Mock HydrateQuotaRulesReverse
		env.OnActivity("HydrateQuotaRulesReverse", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.OnActivity("CleanupOldReplication", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(ReverseAndResumeVolumeReplicationWorkflow, params, event)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("Lines254_257_AddNewSrcQuotaRulesToNewDstDBReverseFailure", func(tt *testing.T) {
		// Enable quota rule sync
		originalQuotaRuleSync := quotaRuleSync
		quotaRuleSync = true
		defer func() { quotaRuleSync = originalQuotaRuleSync }()

		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)

		mockStorage := database.NewMockStorage(tt)
		commonActivity := activities.CommonActivities{SE: mockStorage}
		reverseReplicationActivity := replicationActivities.ReverseVolumeReplicationActivity{SE: mockStorage}

		// Register activities
		env.RegisterActivity(reverseReplicationActivity.GetSrcBasePathReverse)
		env.RegisterActivity(reverseReplicationActivity.GetDstBasePathReverse)
		env.RegisterActivity(reverseReplicationActivity.GetSignedSrcTokenReverse)
		env.RegisterActivity(reverseReplicationActivity.GetSignedDstTokenReverse)
		env.RegisterActivity(reverseReplicationActivity.VerifyNewDstVolume)
		env.RegisterActivity(reverseReplicationActivity.ResizeNewDstVolumeIfNeeded)
		env.RegisterActivity(reverseReplicationActivity.ReverseAndResumeReplication)
		env.RegisterActivity(reverseReplicationActivity.DescribeRemoteJobOnSrc)
		env.RegisterActivity(reverseReplicationActivity.DescribeRemoteJobOnDst)
		env.RegisterActivity(reverseReplicationActivity.UpdateVolumeReplicationAttributesSrc)
		env.RegisterActivity(reverseReplicationActivity.UpdateVolumeReplicationAttributesDst)
		env.RegisterActivity(reverseReplicationActivity.MountReplicationAfterReverse)
		env.RegisterActivity(reverseReplicationActivity.ListQuotaRulesOnNewSourceReverse)
		env.RegisterActivity(reverseReplicationActivity.ListQuotaRulesOnNewDestinationReverse)
		env.RegisterActivity(reverseReplicationActivity.AddNewSrcQuotaRulesToNewDstDBReverse)
		env.RegisterActivity(reverseReplicationActivity.CleanupOldReplication)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.ReverseAndResumeReplicationParams{
			AccountName: "test-account",
		}

		event := &replication.ReverseReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				SourceProjectNumber:      "123456789",
				DestinationProjectNumber: "987654321",
				ReplicationModel: &datamodel.VolumeReplication{
					BaseModel: datamodel.BaseModel{UUID: "replication-uuid"},
					ReplicationAttributes: &datamodel.ReplicationDetails{
						SourceLocation:      "us-east1",
						DestinationLocation: "us-west1",
					},
					HybridReplicationAttributes: nil,
				},
			},
		}

		reverseResult := &replication.ReverseReplicationResult{
			Event:            event,
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
			DbVolReplication: event.ReplicationModel,
			SrcBasePath:      stringPtr("https://src-path"),
			DstBasePath:      stringPtr("https://dst-path"),
			SrcJwtToken:      stringPtr("src-token"),
			DstJwtToken:      stringPtr("dst-token"),
		}

		env.OnActivity("GetSrcBasePathReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetDstBasePathReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetSignedSrcTokenReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetSignedDstTokenReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("VerifyNewDstVolume", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("ResizeNewDstVolumeIfNeeded", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("ReverseAndResumeReplication", mock.Anything, mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("DescribeRemoteJobOnSrc", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVolumeReplicationAttributesSrc", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("UpdateVolumeReplicationAttributesDst", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("DescribeRemoteJobOnDst", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("MountReplicationAfterReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)

		// Mock quota rule activities
		env.OnActivity("ListQuotaRulesOnNewSourceReverse", mock.Anything, mock.Anything).Return([]*datamodel.QuotaRule{
			{BaseModel: datamodel.BaseModel{UUID: "quota-1"}, Name: "rule-1"},
		}, nil)
		env.OnActivity("ListQuotaRulesOnNewDestinationReverse", mock.Anything, mock.Anything).Return([]*datamodel.QuotaRule{}, nil)

		// Mock AddNewSrcQuotaRulesToNewDstDBReverse failure (lines 254-257)
		env.OnActivity("AddNewSrcQuotaRulesToNewDstDBReverse", mock.Anything, mock.Anything).Return(nil, assert.AnError)

		var capturedStatus string
		var capturedErrorDetails string
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything, mock.Anything).
			Return(func(ctx context.Context, job *datamodel.Job) error {
				capturedStatus = job.State
				capturedErrorDetails = job.ErrorDetails
				return nil
			})

		env.ExecuteWorkflow(ReverseAndResumeVolumeReplicationWorkflow, params, event)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
		assert.Equal(tt, string(models.JobsStateDONE), capturedStatus)
		assert.Contains(tt, capturedErrorDetails, reverseResumeQuotaRuleError)
	})

	t.Run("Lines264_265_271_273_HydrateFailure", func(tt *testing.T) {
		// Enable quota rule sync and hydration
		originalQuotaRuleSync := quotaRuleSync
		originalHydrationEnabled := hydrationEnabled
		quotaRuleSync = true
		hydrationEnabled = true
		defer func() {
			quotaRuleSync = originalQuotaRuleSync
			hydrationEnabled = originalHydrationEnabled
		}()

		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)

		mockStorage := database.NewMockStorage(tt)
		commonActivity := activities.CommonActivities{SE: mockStorage}
		reverseReplicationActivity := replicationActivities.ReverseVolumeReplicationActivity{SE: mockStorage}

		// Register activities
		env.RegisterActivity(reverseReplicationActivity.GetSrcBasePathReverse)
		env.RegisterActivity(reverseReplicationActivity.GetDstBasePathReverse)
		env.RegisterActivity(reverseReplicationActivity.GetSignedSrcTokenReverse)
		env.RegisterActivity(reverseReplicationActivity.GetSignedDstTokenReverse)
		env.RegisterActivity(reverseReplicationActivity.VerifyNewDstVolume)
		env.RegisterActivity(reverseReplicationActivity.ResizeNewDstVolumeIfNeeded)
		env.RegisterActivity(reverseReplicationActivity.ReverseAndResumeReplication)
		env.RegisterActivity(reverseReplicationActivity.DescribeRemoteJobOnSrc)
		env.RegisterActivity(reverseReplicationActivity.DescribeRemoteJobOnDst)
		env.RegisterActivity(reverseReplicationActivity.UpdateVolumeReplicationAttributesSrc)
		env.RegisterActivity(reverseReplicationActivity.UpdateVolumeReplicationAttributesDst)
		env.RegisterActivity(reverseReplicationActivity.MountReplicationAfterReverse)
		env.RegisterActivity(reverseReplicationActivity.ListQuotaRulesOnNewSourceReverse)
		env.RegisterActivity(reverseReplicationActivity.ListQuotaRulesOnNewDestinationReverse)
		env.RegisterActivity(reverseReplicationActivity.AddNewSrcQuotaRulesToNewDstDBReverse)
		env.RegisterActivity(reverseReplicationActivity.HydrateQuotaRulesReverse)
		env.RegisterActivity(reverseReplicationActivity.CleanupOldReplication)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.ReverseAndResumeReplicationParams{
			AccountName: "test-account",
		}

		event := &replication.ReverseReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				SourceProjectNumber:      "123456789",
				DestinationProjectNumber: "987654321",
				ReplicationModel: &datamodel.VolumeReplication{
					BaseModel: datamodel.BaseModel{UUID: "replication-uuid"},
					ReplicationAttributes: &datamodel.ReplicationDetails{
						SourceLocation:      "us-east1",
						DestinationLocation: "us-west1",
					},
					HybridReplicationAttributes: nil,
				},
			},
		}

		reverseResult := &replication.ReverseReplicationResult{
			Event:            event,
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
			DbVolReplication: event.ReplicationModel,
			SrcBasePath:      stringPtr("https://src-path"),
			DstBasePath:      stringPtr("https://dst-path"),
			SrcJwtToken:      stringPtr("src-token"),
			DstJwtToken:      stringPtr("dst-token"),
			NewDstVolume: &googleproxyclient.VolumeV1beta{
				ResourceId: "dst-volume-id",
			},
		}

		env.OnActivity("GetSrcBasePathReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetDstBasePathReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetSignedSrcTokenReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetSignedDstTokenReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("VerifyNewDstVolume", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("ResizeNewDstVolumeIfNeeded", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("ReverseAndResumeReplication", mock.Anything, mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("DescribeRemoteJobOnSrc", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVolumeReplicationAttributesSrc", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("UpdateVolumeReplicationAttributesDst", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("DescribeRemoteJobOnDst", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("MountReplicationAfterReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)

		// Mock quota rule activities
		env.OnActivity("ListQuotaRulesOnNewSourceReverse", mock.Anything, mock.Anything).Return([]*datamodel.QuotaRule{
			{BaseModel: datamodel.BaseModel{UUID: "quota-1"}, Name: "rule-1"},
		}, nil)
		env.OnActivity("ListQuotaRulesOnNewDestinationReverse", mock.Anything, mock.Anything).Return([]*datamodel.QuotaRule{}, nil)

		// Mock AddNewSrcQuotaRulesToNewDstDBReverse success with destination quota rules
		env.OnActivity("AddNewSrcQuotaRulesToNewDstDBReverse", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ReverseReplicationResult) (*replication.ReverseReplicationResult, error) {
			result.DestinationQuotaRules = []*datamodel.QuotaRule{
				{BaseModel: datamodel.BaseModel{UUID: "quota-1"}, Name: "rule-1"},
			}
			return result, nil
		}, nil)

		// Mock HydrateQuotaRulesReverse failure (lines 271-273)
		env.OnActivity("HydrateQuotaRulesReverse", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(assert.AnError)

		var capturedStatus string
		var capturedErrorDetails string
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything, mock.Anything).
			Return(func(ctx context.Context, job *datamodel.Job) error {
				capturedStatus = job.State
				capturedErrorDetails = job.ErrorDetails
				return nil
			})

		env.ExecuteWorkflow(ReverseAndResumeVolumeReplicationWorkflow, params, event)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
		assert.Equal(tt, string(models.JobsStateDONE), capturedStatus)
		assert.Contains(tt, capturedErrorDetails, reverseResumeQuotaRuleError)
	})
}
