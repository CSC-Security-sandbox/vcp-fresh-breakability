package replicationWorkflows

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/replicationActivities"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/replication"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestReverseHybridFallbackReplicationWorkflow(t *testing.T) {
	t.Run("TestReverseHybridFallbackReplicationWorkflow_Success", func(tt *testing.T) {
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
		updateAttrActivity := replicationActivities.UpdateVolumeReplicationAttributesActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(replicationActivity.GetSrcBasePathReverse)
		env.RegisterActivity(replicationActivity.GetSignedSrcTokenReverse)
		env.RegisterActivity(replicationActivity.DescribeRemoteJobOnSrc)
		env.RegisterActivity(replicationActivity.HydrateReplicationSateAndTypeForReverseFallbackHybridReplication)
		env.RegisterActivity(replicationActivity.ReverseAndResumeReplication)
		env.RegisterActivity(updateAttrActivity.GetSnapmirrorDetailsFromOntap)
		env.RegisterActivity(updateAttrActivity.UpdateDstVolumeReplication)
		env.RegisterActivity(replicationActivity.SetVolumeReplicationStatusToOnpremReplication)
		env.RegisterActivity(replicationActivity.ReleaseReplicationOnOldSrc)
		env.RegisterActivity(replicationActivity.MountReplicationAfterReverse)
		env.RegisterActivity(commonActivity.GetJob)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.ReverseAndResumeReplicationParams{
			AccountName:    "test-account",
			CorrelationId: "test-correlation-id",
		}

		event := &replication.ReverseReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{
					BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-replication-uuid"},
					Name:      "test-replication",
					Volume: &datamodel.Volume{
						BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-volume-uuid"},
						VolumeAttributes: &datamodel.VolumeAttributes{
							ExternalUUID: "volume-external-uuid",
						},
					},
					ReplicationAttributes: &datamodel.ReplicationDetails{
						SourceLocation:           "us-central1",
						SourceSvmName:           "source-svm",
						SourceVolumeName:        "source-volume",
						SourceReplicationUUID:    "source-replication-uuid",
						SourceHostName:           "source-host",
						SourceVolumeUUID:        "source-volume-uuid",
						SourcePoolUUID:          "source-pool-uuid",
						DestinationSvmName:      "dest-svm",
						DestinationVolumeName:   "dest-volume",
						DestinationReplicationUUID: "dest-replication-uuid",
						DestinationHostName:      "dest-host",
						DestinationVolumeUUID:    "dest-volume-uuid",
						DestinationPoolUUID:     "dest-pool-uuid",
						ReplicationSchedule:     vsa.VolumeReplicationScheduleHourly,
						EndpointType:            "src",
					},
				},
				SourceProjectNumber:      "123456789",
				DestinationProjectNumber: "987654321",
				XCorrelationID:           func() *string { s := "test-correlation-id"; return &s }(),
			},
		}

		nodeProvider := &models.Node{
			Name: "test-node",
		}

		hybridReverseResult := &replication.ReverseHybridReplicationResult{
			Event:            event,
			DbVolReplication: event.ReplicationModel,
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
			NodeProvider:    nodeProvider,
		}

		reverseResult := &replication.ReverseReplicationResult{
			Event:            event,
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
			DbVolReplication: event.ReplicationModel,
			NodeProvider:    nodeProvider,
		}

		srcBasePath := "https://test-src-base-path"
		srcJwtToken := "test-src-jwt-token"
		jobId := "test-job-id"

		reverseResultWithBasePath := *reverseResult
		reverseResultWithBasePath.SrcBasePath = &srcBasePath

		reverseResultWithToken := reverseResultWithBasePath
		reverseResultWithToken.SrcJwtToken = &srcJwtToken

		reverseResultWithJobId := reverseResultWithToken
		reverseResultWithJobId.JobId = &jobId

		updateAttrResult := &replication.UpdateVolumeReplicationAttributesResult{
			Event: &replication.UpdateVolumeReplicationAttributesEvent{
				UpdateVolumeReplicationAttributesParams: &models.UpdateVolumeReplicationAttributesParams{
					VolumeReplicationId:       "dest-replication-uuid",
					VolumeReplicationInternal: nil,
				},
			},
		}

		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     "NEW",
		}, nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSrcBasePathReverse", mock.Anything, mock.Anything).Return(&reverseResultWithBasePath, nil)
		env.OnActivity("GetSignedSrcTokenReverse", mock.Anything, mock.Anything).Return(&reverseResultWithToken, nil)
		env.OnActivity("DescribeRemoteJobOnSrc", mock.Anything, mock.Anything).Return(nil).Times(3)
		env.OnActivity("HydrateReplicationSateAndTypeForReverseFallbackHybridReplication", mock.Anything, mock.Anything).Return(&reverseResultWithJobId, nil)
		env.OnActivity("ReverseAndResumeReplication", mock.Anything, mock.Anything, mock.Anything).Return(&reverseResultWithJobId, nil)
		env.OnActivity("GetSnapmirrorDetailsFromOntap", mock.Anything, mock.Anything).Return(updateAttrResult, nil)
		env.OnActivity("UpdateDstVolumeReplication", mock.Anything, mock.Anything).Return(updateAttrResult, nil)
		env.OnActivity("SetVolumeReplicationStatusToOnpremReplication", mock.Anything, mock.Anything).Return(&reverseResultWithJobId, nil)
		env.OnActivity("ReleaseReplicationOnOldSrc", mock.Anything, mock.Anything).Return(&reverseResultWithJobId, nil)
		env.OnActivity("MountReplicationAfterReverse", mock.Anything, mock.Anything).Return(&reverseResultWithJobId, nil)

		env.ExecuteWorkflow(ReverseHybridFallbackReplicationWorkflow, params, hybridReverseResult)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		assert.Nil(tt, err)
		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestReverseHybridFallbackReplicationWorkflow_GetSrcBasePathReverseError", func(tt *testing.T) {
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
		reverseHybridActivity := replicationActivities.ReverseHybridReplicationActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(replicationActivity.GetSrcBasePathReverse)
		env.RegisterActivity(reverseHybridActivity.SetReplicationToErrorForReverseHybrid)
		env.RegisterActivity(commonActivity.GetJob)
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

		hybridReverseResult := &replication.ReverseHybridReplicationResult{
			Event:            event,
			DbVolReplication: event.ReplicationModel,
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
		}

		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     "NEW",
		}, nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSrcBasePathReverse", mock.Anything, mock.Anything).Return(nil, assert.AnError)
		env.OnActivity("SetReplicationToErrorForReverseHybrid", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(ReverseHybridFallbackReplicationWorkflow, params, hybridReverseResult)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("TestReverseHybridFallbackReplicationWorkflow_GetSignedSrcTokenReverseError", func(tt *testing.T) {
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
		reverseHybridActivity := replicationActivities.ReverseHybridReplicationActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(replicationActivity.GetSrcBasePathReverse)
		env.RegisterActivity(replicationActivity.GetSignedSrcTokenReverse)
		env.RegisterActivity(reverseHybridActivity.SetReplicationToErrorForReverseHybrid)
		env.RegisterActivity(commonActivity.GetJob)
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

		hybridReverseResult := &replication.ReverseHybridReplicationResult{
			Event:            event,
			DbVolReplication: event.ReplicationModel,
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
		}

		reverseResult := &replication.ReverseReplicationResult{
			Event:            event,
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
			DbVolReplication: event.ReplicationModel,
		}

		srcBasePath := "https://test-src-base-path"
		reverseResultWithBasePath := *reverseResult
		reverseResultWithBasePath.SrcBasePath = &srcBasePath

		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     "NEW",
		}, nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSrcBasePathReverse", mock.Anything, mock.Anything).Return(&reverseResultWithBasePath, nil)
		env.OnActivity("GetSignedSrcTokenReverse", mock.Anything, mock.Anything).Return(nil, assert.AnError)
		env.OnActivity("SetReplicationToErrorForReverseHybrid", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(ReverseHybridFallbackReplicationWorkflow, params, hybridReverseResult)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("TestReverseHybridFallbackReplicationWorkflow_DescribeRemoteJobOnSrcBeforeReverseError", func(tt *testing.T) {
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
		reverseHybridActivity := replicationActivities.ReverseHybridReplicationActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(replicationActivity.GetSrcBasePathReverse)
		env.RegisterActivity(replicationActivity.GetSignedSrcTokenReverse)
		env.RegisterActivity(replicationActivity.DescribeRemoteJobOnSrc)
		env.RegisterActivity(replicationActivity.HydrateReplicationSateAndTypeForReverseFallbackHybridReplication)
		env.RegisterActivity(reverseHybridActivity.SetReplicationToErrorForReverseHybrid)
		env.RegisterActivity(commonActivity.GetJob)
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

		hybridReverseResult := &replication.ReverseHybridReplicationResult{
			Event:            event,
			DbVolReplication: event.ReplicationModel,
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
		}

		reverseResult := &replication.ReverseReplicationResult{
			Event:            event,
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
			DbVolReplication: event.ReplicationModel,
		}

		srcBasePath := "https://test-src-base-path"
		srcJwtToken := "test-src-jwt-token"
		reverseResultWithBasePath := *reverseResult
		reverseResultWithBasePath.SrcBasePath = &srcBasePath
		reverseResultWithToken := reverseResultWithBasePath
		reverseResultWithToken.SrcJwtToken = &srcJwtToken

		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     "NEW",
		}, nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSrcBasePathReverse", mock.Anything, mock.Anything).Return(&reverseResultWithBasePath, nil)
		env.OnActivity("GetSignedSrcTokenReverse", mock.Anything, mock.Anything).Return(&reverseResultWithToken, nil)
		env.OnActivity("DescribeRemoteJobOnSrc", mock.Anything, mock.Anything).Return(assert.AnError)
		env.OnActivity("SetReplicationToErrorForReverseHybrid", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(ReverseHybridFallbackReplicationWorkflow, params, hybridReverseResult)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("TestReverseHybridFallbackReplicationWorkflow_ReverseAndResumeReplicationError", func(tt *testing.T) {
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
		reverseHybridActivity := replicationActivities.ReverseHybridReplicationActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(replicationActivity.GetSrcBasePathReverse)
		env.RegisterActivity(replicationActivity.GetSignedSrcTokenReverse)
		env.RegisterActivity(replicationActivity.DescribeRemoteJobOnSrc)
		env.RegisterActivity(replicationActivity.HydrateReplicationSateAndTypeForReverseFallbackHybridReplication)
		env.RegisterActivity(replicationActivity.ReverseAndResumeReplication)
		env.RegisterActivity(reverseHybridActivity.SetReplicationToErrorForReverseHybrid)
		env.RegisterActivity(commonActivity.GetJob)
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

		hybridReverseResult := &replication.ReverseHybridReplicationResult{
			Event:            event,
			DbVolReplication: event.ReplicationModel,
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
		}

		reverseResult := &replication.ReverseReplicationResult{
			Event:            event,
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
			DbVolReplication: event.ReplicationModel,
		}

		srcBasePath := "https://test-src-base-path"
		srcJwtToken := "test-src-jwt-token"
		reverseResultWithBasePath := *reverseResult
		reverseResultWithBasePath.SrcBasePath = &srcBasePath
		reverseResultWithToken := reverseResultWithBasePath
		reverseResultWithToken.SrcJwtToken = &srcJwtToken

		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     "NEW",
		}, nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSrcBasePathReverse", mock.Anything, mock.Anything).Return(&reverseResultWithBasePath, nil)
		env.OnActivity("GetSignedSrcTokenReverse", mock.Anything, mock.Anything).Return(&reverseResultWithToken, nil)
		env.OnActivity("DescribeRemoteJobOnSrc", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("HydrateReplicationSateAndTypeForReverseFallbackHybridReplication", mock.Anything, mock.Anything).Return(&reverseResultWithToken, nil)
		env.OnActivity("ReverseAndResumeReplication", mock.Anything, mock.Anything, mock.Anything).Return(nil, assert.AnError)
		env.OnActivity("SetReplicationToErrorForReverseHybrid", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(ReverseHybridFallbackReplicationWorkflow, params, hybridReverseResult)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("TestReverseHybridFallbackReplicationWorkflow_DescribeRemoteJobOnSrcAfterReverseError", func(tt *testing.T) {
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
		reverseHybridActivity := replicationActivities.ReverseHybridReplicationActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(replicationActivity.GetSrcBasePathReverse)
		env.RegisterActivity(replicationActivity.GetSignedSrcTokenReverse)
		env.RegisterActivity(replicationActivity.DescribeRemoteJobOnSrc)
		env.RegisterActivity(replicationActivity.HydrateReplicationSateAndTypeForReverseFallbackHybridReplication)
		env.RegisterActivity(replicationActivity.ReverseAndResumeReplication)
		env.RegisterActivity(replicationActivity.DeleteNewReplicationOnSrc)
		env.RegisterActivity(reverseHybridActivity.SetReplicationToErrorForReverseHybrid)
		env.RegisterActivity(commonActivity.GetJob)
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

		hybridReverseResult := &replication.ReverseHybridReplicationResult{
			Event:            event,
			DbVolReplication: event.ReplicationModel,
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
		}

		reverseResult := &replication.ReverseReplicationResult{
			Event:            event,
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
			DbVolReplication: event.ReplicationModel,
		}

		srcBasePath := "https://test-src-base-path"
		srcJwtToken := "test-src-jwt-token"
		jobId := "test-job-id"
		reverseResultWithBasePath := *reverseResult
		reverseResultWithBasePath.SrcBasePath = &srcBasePath
		reverseResultWithToken := reverseResultWithBasePath
		reverseResultWithToken.SrcJwtToken = &srcJwtToken
		reverseResultWithJobId := reverseResultWithToken
		reverseResultWithJobId.JobId = &jobId

		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     "NEW",
		}, nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSrcBasePathReverse", mock.Anything, mock.Anything).Return(&reverseResultWithBasePath, nil)
		env.OnActivity("GetSignedSrcTokenReverse", mock.Anything, mock.Anything).Return(&reverseResultWithToken, nil)
		env.OnActivity("DescribeRemoteJobOnSrc", mock.Anything, mock.Anything).Return(nil).Once()
		env.OnActivity("HydrateReplicationSateAndTypeForReverseFallbackHybridReplication", mock.Anything, mock.Anything).Return(&reverseResultWithJobId, nil)
		env.OnActivity("ReverseAndResumeReplication", mock.Anything, mock.Anything, mock.Anything).Return(&reverseResultWithJobId, nil)
		env.OnActivity("DescribeRemoteJobOnSrc", mock.Anything, mock.Anything).Return(assert.AnError).Once()
		env.OnActivity("DeleteNewReplicationOnSrc", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SetReplicationToErrorForReverseHybrid", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(ReverseHybridFallbackReplicationWorkflow, params, hybridReverseResult)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("TestReverseHybridFallbackReplicationWorkflow_UpdateVolumeReplicationAttributesSrcError", func(tt *testing.T) {
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
		updateAttrActivity := replicationActivities.UpdateVolumeReplicationAttributesActivity{SE: mockStorage}
		reverseHybridActivity := replicationActivities.ReverseHybridReplicationActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(replicationActivity.GetSrcBasePathReverse)
		env.RegisterActivity(replicationActivity.GetSignedSrcTokenReverse)
		env.RegisterActivity(replicationActivity.DescribeRemoteJobOnSrc)
		env.RegisterActivity(replicationActivity.HydrateReplicationSateAndTypeForReverseFallbackHybridReplication)
		env.RegisterActivity(replicationActivity.ReverseAndResumeReplication)
		env.RegisterActivity(replicationActivity.DeleteNewReplicationOnSrc)
		env.RegisterActivity(updateAttrActivity.GetSnapmirrorDetailsFromOntap)
		env.RegisterActivity(updateAttrActivity.UpdateDstVolumeReplication)
		env.RegisterActivity(reverseHybridActivity.SetReplicationToErrorForReverseHybrid)
		env.RegisterActivity(commonActivity.GetJob)
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

		hybridReverseResult := &replication.ReverseHybridReplicationResult{
			Event:            event,
			DbVolReplication: event.ReplicationModel,
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
		}

		reverseResult := &replication.ReverseReplicationResult{
			Event:            event,
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
			DbVolReplication: event.ReplicationModel,
		}

		srcBasePath := "https://test-src-base-path"
		srcJwtToken := "test-src-jwt-token"
		jobId := "test-job-id"
		reverseResultWithBasePath := *reverseResult
		reverseResultWithBasePath.SrcBasePath = &srcBasePath
		reverseResultWithToken := reverseResultWithBasePath
		reverseResultWithToken.SrcJwtToken = &srcJwtToken
		reverseResultWithJobId := reverseResultWithToken
		reverseResultWithJobId.JobId = &jobId

		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     "NEW",
		}, nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSrcBasePathReverse", mock.Anything, mock.Anything).Return(&reverseResultWithBasePath, nil)
		env.OnActivity("GetSignedSrcTokenReverse", mock.Anything, mock.Anything).Return(&reverseResultWithToken, nil)
		env.OnActivity("DescribeRemoteJobOnSrc", mock.Anything, mock.Anything).Return(nil).Times(2)
		env.OnActivity("HydrateReplicationSateAndTypeForReverseFallbackHybridReplication", mock.Anything, mock.Anything).Return(&reverseResultWithJobId, nil)
		env.OnActivity("ReverseAndResumeReplication", mock.Anything, mock.Anything, mock.Anything).Return(&reverseResultWithJobId, nil)
		updateAttrResult := &replication.UpdateVolumeReplicationAttributesResult{
			Event: &replication.UpdateVolumeReplicationAttributesEvent{
				UpdateVolumeReplicationAttributesParams: &models.UpdateVolumeReplicationAttributesParams{
					VolumeReplicationId:       "dest-replication-uuid",
					VolumeReplicationInternal: nil,
				},
			},
		}
		env.OnActivity("GetSnapmirrorDetailsFromOntap", mock.Anything, mock.Anything).Return(updateAttrResult, assert.AnError)
		env.OnActivity("DeleteNewReplicationOnSrc", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SetReplicationToErrorForReverseHybrid", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(ReverseHybridFallbackReplicationWorkflow, params, hybridReverseResult)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("TestReverseHybridFallbackReplicationWorkflow_DescribeRemoteJobOnSrcAfterUpdateError", func(tt *testing.T) {
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
		updateAttrActivity := replicationActivities.UpdateVolumeReplicationAttributesActivity{SE: mockStorage}
		reverseHybridActivity := replicationActivities.ReverseHybridReplicationActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(replicationActivity.GetSrcBasePathReverse)
		env.RegisterActivity(replicationActivity.GetSignedSrcTokenReverse)
		env.RegisterActivity(replicationActivity.DescribeRemoteJobOnSrc)
		env.RegisterActivity(replicationActivity.HydrateReplicationSateAndTypeForReverseFallbackHybridReplication)
		env.RegisterActivity(replicationActivity.ReverseAndResumeReplication)
		env.RegisterActivity(replicationActivity.DeleteNewReplicationOnSrc)
		env.RegisterActivity(updateAttrActivity.GetSnapmirrorDetailsFromOntap)
		env.RegisterActivity(updateAttrActivity.UpdateDstVolumeReplication)
		env.RegisterActivity(reverseHybridActivity.SetReplicationToErrorForReverseHybrid)
		env.RegisterActivity(commonActivity.GetJob)
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

		hybridReverseResult := &replication.ReverseHybridReplicationResult{
			Event:            event,
			DbVolReplication: event.ReplicationModel,
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
		}

		reverseResult := &replication.ReverseReplicationResult{
			Event:            event,
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
			DbVolReplication: event.ReplicationModel,
		}

		srcBasePath := "https://test-src-base-path"
		srcJwtToken := "test-src-jwt-token"
		jobId := "test-job-id"
		reverseResultWithBasePath := *reverseResult
		reverseResultWithBasePath.SrcBasePath = &srcBasePath
		reverseResultWithToken := reverseResultWithBasePath
		reverseResultWithToken.SrcJwtToken = &srcJwtToken
		reverseResultWithJobId := reverseResultWithToken
		reverseResultWithJobId.JobId = &jobId

		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     "NEW",
		}, nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSrcBasePathReverse", mock.Anything, mock.Anything).Return(&reverseResultWithBasePath, nil)
		env.OnActivity("GetSignedSrcTokenReverse", mock.Anything, mock.Anything).Return(&reverseResultWithToken, nil)
		env.OnActivity("DescribeRemoteJobOnSrc", mock.Anything, mock.Anything).Return(nil).Times(2)
		env.OnActivity("HydrateReplicationSateAndTypeForReverseFallbackHybridReplication", mock.Anything, mock.Anything).Return(&reverseResultWithJobId, nil)
		env.OnActivity("ReverseAndResumeReplication", mock.Anything, mock.Anything, mock.Anything).Return(&reverseResultWithJobId, nil)
		updateAttrResult := &replication.UpdateVolumeReplicationAttributesResult{
			Event: &replication.UpdateVolumeReplicationAttributesEvent{
				UpdateVolumeReplicationAttributesParams: &models.UpdateVolumeReplicationAttributesParams{
					VolumeReplicationId:       "dest-replication-uuid",
					VolumeReplicationInternal: nil,
				},
			},
		}
		env.OnActivity("GetSnapmirrorDetailsFromOntap", mock.Anything, mock.Anything).Return(updateAttrResult, nil)
		env.OnActivity("UpdateDstVolumeReplication", mock.Anything, mock.Anything).Return(updateAttrResult, nil)
		env.OnActivity("DescribeRemoteJobOnSrc", mock.Anything, mock.Anything).Return(assert.AnError).Once()
		env.OnActivity("DeleteNewReplicationOnSrc", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SetReplicationToErrorForReverseHybrid", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(ReverseHybridFallbackReplicationWorkflow, params, hybridReverseResult)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("TestReverseHybridFallbackReplicationWorkflow_SetVolumeReplicationStatusToOnpremReplicationError", func(tt *testing.T) {
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
		updateAttrActivity := replicationActivities.UpdateVolumeReplicationAttributesActivity{SE: mockStorage}
		reverseHybridActivity := replicationActivities.ReverseHybridReplicationActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(replicationActivity.GetSrcBasePathReverse)
		env.RegisterActivity(replicationActivity.GetSignedSrcTokenReverse)
		env.RegisterActivity(replicationActivity.DescribeRemoteJobOnSrc)
		env.RegisterActivity(replicationActivity.HydrateReplicationSateAndTypeForReverseFallbackHybridReplication)
		env.RegisterActivity(replicationActivity.ReverseAndResumeReplication)
		env.RegisterActivity(replicationActivity.DeleteNewReplicationOnSrc)
		env.RegisterActivity(updateAttrActivity.GetSnapmirrorDetailsFromOntap)
		env.RegisterActivity(updateAttrActivity.UpdateDstVolumeReplication)
		env.RegisterActivity(replicationActivity.SetVolumeReplicationStatusToOnpremReplication)
		env.RegisterActivity(reverseHybridActivity.SetReplicationToErrorForReverseHybrid)
		env.RegisterActivity(commonActivity.GetJob)
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

		hybridReverseResult := &replication.ReverseHybridReplicationResult{
			Event:            event,
			DbVolReplication: event.ReplicationModel,
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
		}

		reverseResult := &replication.ReverseReplicationResult{
			Event:            event,
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
			DbVolReplication: event.ReplicationModel,
		}

		srcBasePath := "https://test-src-base-path"
		srcJwtToken := "test-src-jwt-token"
		jobId := "test-job-id"
		reverseResultWithBasePath := *reverseResult
		reverseResultWithBasePath.SrcBasePath = &srcBasePath
		reverseResultWithToken := reverseResultWithBasePath
		reverseResultWithToken.SrcJwtToken = &srcJwtToken
		reverseResultWithJobId := reverseResultWithToken
		reverseResultWithJobId.JobId = &jobId

		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     "NEW",
		}, nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSrcBasePathReverse", mock.Anything, mock.Anything).Return(&reverseResultWithBasePath, nil)
		env.OnActivity("GetSignedSrcTokenReverse", mock.Anything, mock.Anything).Return(&reverseResultWithToken, nil)
		env.OnActivity("DescribeRemoteJobOnSrc", mock.Anything, mock.Anything).Return(nil).Times(3)
		env.OnActivity("HydrateReplicationSateAndTypeForReverseFallbackHybridReplication", mock.Anything, mock.Anything).Return(&reverseResultWithJobId, nil)
		env.OnActivity("ReverseAndResumeReplication", mock.Anything, mock.Anything, mock.Anything).Return(&reverseResultWithJobId, nil)
		updateAttrResult := &replication.UpdateVolumeReplicationAttributesResult{
			Event: &replication.UpdateVolumeReplicationAttributesEvent{
				UpdateVolumeReplicationAttributesParams: &models.UpdateVolumeReplicationAttributesParams{
					VolumeReplicationId:       "dest-replication-uuid",
					VolumeReplicationInternal: nil,
				},
			},
		}
		env.OnActivity("GetSnapmirrorDetailsFromOntap", mock.Anything, mock.Anything).Return(updateAttrResult, nil)
		env.OnActivity("UpdateDstVolumeReplication", mock.Anything, mock.Anything).Return(updateAttrResult, nil)
		env.OnActivity("SetVolumeReplicationStatusToOnpremReplication", mock.Anything, mock.Anything).Return(nil, assert.AnError)
		env.OnActivity("DeleteNewReplicationOnSrc", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SetReplicationToErrorForReverseHybrid", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(ReverseHybridFallbackReplicationWorkflow, params, hybridReverseResult)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("TestReverseHybridFallbackReplicationWorkflow_ReleaseReplicationOnOldSrcError", func(tt *testing.T) {
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
		updateAttrActivity := replicationActivities.UpdateVolumeReplicationAttributesActivity{SE: mockStorage}
		reverseHybridActivity := replicationActivities.ReverseHybridReplicationActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(replicationActivity.GetSrcBasePathReverse)
		env.RegisterActivity(replicationActivity.GetSignedSrcTokenReverse)
		env.RegisterActivity(replicationActivity.DescribeRemoteJobOnSrc)
		env.RegisterActivity(replicationActivity.HydrateReplicationSateAndTypeForReverseFallbackHybridReplication)
		env.RegisterActivity(replicationActivity.ReverseAndResumeReplication)
		env.RegisterActivity(replicationActivity.DeleteNewReplicationOnSrc)
		env.RegisterActivity(updateAttrActivity.GetSnapmirrorDetailsFromOntap)
		env.RegisterActivity(updateAttrActivity.UpdateDstVolumeReplication)
		env.RegisterActivity(replicationActivity.SetVolumeReplicationStatusToOnpremReplication)
		env.RegisterActivity(replicationActivity.ReleaseReplicationOnOldSrc)
		env.RegisterActivity(reverseHybridActivity.SetReplicationToErrorForReverseHybrid)
		env.RegisterActivity(commonActivity.GetJob)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.ReverseAndResumeReplicationParams{
			AccountName: "test-account",
		}

		event := &replication.ReverseReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{
					BaseModel: datamodel.BaseModel{ID: 1},
					Name:      "test-replication",
					Volume: &datamodel.Volume{
						VolumeAttributes: &datamodel.VolumeAttributes{
							ExternalUUID: "volume-external-uuid",
						},
					},
					ReplicationAttributes: &datamodel.ReplicationDetails{
						SourceLocation:           "us-central1",
						SourceSvmName:           "source-svm",
						SourceVolumeName:        "source-volume",
						SourceReplicationUUID:    "source-replication-uuid",
						SourceHostName:           "source-host",
						SourceVolumeUUID:        "source-volume-uuid",
						SourcePoolUUID:          "source-pool-uuid",
						DestinationSvmName:      "dest-svm",
						DestinationVolumeName:   "dest-volume",
						DestinationReplicationUUID: "dest-replication-uuid",
						DestinationHostName:      "dest-host",
						DestinationVolumeUUID:    "dest-volume-uuid",
						DestinationPoolUUID:     "dest-pool-uuid",
						EndpointType:            "src",
					},
				},
				SourceProjectNumber:      "123456789",
				DestinationProjectNumber: "987654321",
			},
		}

		nodeProvider := &models.Node{
			Name: "test-node",
		}

		hybridReverseResult := &replication.ReverseHybridReplicationResult{
			Event:            event,
			DbVolReplication: event.ReplicationModel,
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
			NodeProvider:    nodeProvider,
		}

		reverseResult := &replication.ReverseReplicationResult{
			Event:            event,
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
			DbVolReplication: event.ReplicationModel,
			NodeProvider:    nodeProvider,
		}

		srcBasePath := "https://test-src-base-path"
		srcJwtToken := "test-src-jwt-token"
		jobId := "test-job-id"
		reverseResultWithBasePath := *reverseResult
		reverseResultWithBasePath.SrcBasePath = &srcBasePath
		reverseResultWithToken := reverseResultWithBasePath
		reverseResultWithToken.SrcJwtToken = &srcJwtToken
		reverseResultWithJobId := reverseResultWithToken
		reverseResultWithJobId.JobId = &jobId

		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     "NEW",
		}, nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSrcBasePathReverse", mock.Anything, mock.Anything).Return(&reverseResultWithBasePath, nil)
		env.OnActivity("GetSignedSrcTokenReverse", mock.Anything, mock.Anything).Return(&reverseResultWithToken, nil)
		env.OnActivity("DescribeRemoteJobOnSrc", mock.Anything, mock.Anything).Return(nil).Times(3)
		env.OnActivity("HydrateReplicationSateAndTypeForReverseFallbackHybridReplication", mock.Anything, mock.Anything).Return(&reverseResultWithJobId, nil)
		env.OnActivity("ReverseAndResumeReplication", mock.Anything, mock.Anything, mock.Anything).Return(&reverseResultWithJobId, nil)
		updateAttrResult := &replication.UpdateVolumeReplicationAttributesResult{
			Event: &replication.UpdateVolumeReplicationAttributesEvent{
				UpdateVolumeReplicationAttributesParams: &models.UpdateVolumeReplicationAttributesParams{
					VolumeReplicationId:       "dest-replication-uuid",
					VolumeReplicationInternal: nil,
				},
			},
		}
		env.OnActivity("GetSnapmirrorDetailsFromOntap", mock.Anything, mock.Anything).Return(updateAttrResult, nil)
		env.OnActivity("UpdateDstVolumeReplication", mock.Anything, mock.Anything).Return(updateAttrResult, nil)
		env.OnActivity("SetVolumeReplicationStatusToOnpremReplication", mock.Anything, mock.Anything).Return(&reverseResultWithJobId, nil)
		env.OnActivity("ReleaseReplicationOnOldSrc", mock.Anything, mock.Anything).Return(nil, assert.AnError)
		env.OnActivity("DeleteNewReplicationOnSrc", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SetReplicationToErrorForReverseHybrid", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(ReverseHybridFallbackReplicationWorkflow, params, hybridReverseResult)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("TestReverseHybridFallbackReplicationWorkflow_MountReplicationAfterReverseError", func(tt *testing.T) {
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
		updateAttrActivity := replicationActivities.UpdateVolumeReplicationAttributesActivity{SE: mockStorage}
		reverseHybridActivity := replicationActivities.ReverseHybridReplicationActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(replicationActivity.GetSrcBasePathReverse)
		env.RegisterActivity(replicationActivity.GetSignedSrcTokenReverse)
		env.RegisterActivity(replicationActivity.DescribeRemoteJobOnSrc)
		env.RegisterActivity(replicationActivity.HydrateReplicationSateAndTypeForReverseFallbackHybridReplication)
		env.RegisterActivity(replicationActivity.ReverseAndResumeReplication)
		env.RegisterActivity(replicationActivity.DeleteNewReplicationOnSrc)
		env.RegisterActivity(updateAttrActivity.GetSnapmirrorDetailsFromOntap)
		env.RegisterActivity(updateAttrActivity.UpdateDstVolumeReplication)
		env.RegisterActivity(replicationActivity.SetVolumeReplicationStatusToOnpremReplication)
		env.RegisterActivity(replicationActivity.ReleaseReplicationOnOldSrc)
		env.RegisterActivity(replicationActivity.MountReplicationAfterReverse)
		env.RegisterActivity(reverseHybridActivity.SetReplicationToErrorForReverseHybrid)
		env.RegisterActivity(commonActivity.GetJob)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.ReverseAndResumeReplicationParams{
			AccountName: "test-account",
		}

		event := &replication.ReverseReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{
					BaseModel: datamodel.BaseModel{ID: 1},
					Name:      "test-replication",
					Volume: &datamodel.Volume{
						VolumeAttributes: &datamodel.VolumeAttributes{
							ExternalUUID: "volume-external-uuid",
						},
					},
					ReplicationAttributes: &datamodel.ReplicationDetails{
						SourceLocation:           "us-central1",
						SourceSvmName:           "source-svm",
						SourceVolumeName:        "source-volume",
						SourceReplicationUUID:    "source-replication-uuid",
						SourceHostName:           "source-host",
						SourceVolumeUUID:        "source-volume-uuid",
						SourcePoolUUID:          "source-pool-uuid",
						DestinationSvmName:      "dest-svm",
						DestinationVolumeName:   "dest-volume",
						DestinationReplicationUUID: "dest-replication-uuid",
						DestinationHostName:      "dest-host",
						DestinationVolumeUUID:    "dest-volume-uuid",
						DestinationPoolUUID:     "dest-pool-uuid",
						EndpointType:            "src",
					},
				},
				SourceProjectNumber:      "123456789",
				DestinationProjectNumber: "987654321",
				XCorrelationID:           func() *string { s := "test-correlation-id"; return &s }(),
			},
		}

		nodeProvider := &models.Node{
			Name: "test-node",
		}

		hybridReverseResult := &replication.ReverseHybridReplicationResult{
			Event:            event,
			DbVolReplication: event.ReplicationModel,
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
			NodeProvider:    nodeProvider,
		}

		reverseResult := &replication.ReverseReplicationResult{
			Event:            event,
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
			DbVolReplication: event.ReplicationModel,
			NodeProvider:    nodeProvider,
		}

		srcBasePath := "https://test-src-base-path"
		srcJwtToken := "test-src-jwt-token"
		jobId := "test-job-id"
		reverseResultWithBasePath := *reverseResult
		reverseResultWithBasePath.SrcBasePath = &srcBasePath
		reverseResultWithToken := reverseResultWithBasePath
		reverseResultWithToken.SrcJwtToken = &srcJwtToken
		reverseResultWithJobId := reverseResultWithToken
		reverseResultWithJobId.JobId = &jobId

		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     "NEW",
		}, nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSrcBasePathReverse", mock.Anything, mock.Anything).Return(&reverseResultWithBasePath, nil)
		env.OnActivity("GetSignedSrcTokenReverse", mock.Anything, mock.Anything).Return(&reverseResultWithToken, nil)
		env.OnActivity("DescribeRemoteJobOnSrc", mock.Anything, mock.Anything).Return(nil).Times(3)
		env.OnActivity("HydrateReplicationSateAndTypeForReverseFallbackHybridReplication", mock.Anything, mock.Anything).Return(&reverseResultWithJobId, nil)
		env.OnActivity("ReverseAndResumeReplication", mock.Anything, mock.Anything, mock.Anything).Return(&reverseResultWithJobId, nil)
		updateAttrResult := &replication.UpdateVolumeReplicationAttributesResult{
			Event: &replication.UpdateVolumeReplicationAttributesEvent{
				UpdateVolumeReplicationAttributesParams: &models.UpdateVolumeReplicationAttributesParams{
					VolumeReplicationId:       "dest-replication-uuid",
					VolumeReplicationInternal: nil,
				},
			},
		}
		env.OnActivity("GetSnapmirrorDetailsFromOntap", mock.Anything, mock.Anything).Return(updateAttrResult, nil)
		env.OnActivity("UpdateDstVolumeReplication", mock.Anything, mock.Anything).Return(updateAttrResult, nil)
		env.OnActivity("SetVolumeReplicationStatusToOnpremReplication", mock.Anything, mock.Anything).Return(&reverseResultWithJobId, nil)
		env.OnActivity("ReleaseReplicationOnOldSrc", mock.Anything, mock.Anything).Return(&reverseResultWithJobId, nil)
		env.OnActivity("MountReplicationAfterReverse", mock.Anything, mock.Anything).Return(nil, assert.AnError)
		env.OnActivity("DeleteNewReplicationOnSrc", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SetReplicationToErrorForReverseHybrid", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(ReverseHybridFallbackReplicationWorkflow, params, hybridReverseResult)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("TestReverseHybridFallbackReplicationWorkflow_SetupError", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})

		// Test with nil params to trigger setup error
		env.ExecuteWorkflow(ReverseHybridFallbackReplicationWorkflow, nil, nil)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("TestReverseHybridFallbackReplicationWorkflow_UpdateJobStatusError", func(tt *testing.T) {
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
		env.SetHeader(mockHeader)
		env.RegisterActivity(commonActivity.GetJob)
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

		hybridReverseResult := &replication.ReverseHybridReplicationResult{
			Event:            event,
			DbVolReplication: event.ReplicationModel,
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
		}

		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     "NEW",
		}, nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(assert.AnError)

		env.ExecuteWorkflow(ReverseHybridFallbackReplicationWorkflow, params, hybridReverseResult)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})
}

