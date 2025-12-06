package replicationWorkflows

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	coreModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
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

func TestReverseHybridReplicationWorkflow(t *testing.T) {
	t.Run("TestReverseHybridReplicationWorkflow_Success", func(tt *testing.T) {
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
		replicationActivity := replicationActivities.ReverseHybridReplicationActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(commonActivity.GetJob)
		env.RegisterActivity(replicationActivity.SetHybridReplicationVariablesReverse)
		env.RegisterActivity(replicationActivity.GetNodeProviderForHybridReverse)
		env.RegisterActivity(replicationActivity.CheckClusterPeerHealthForHybridReverse)
		env.RegisterActivity(replicationActivity.UpdateRbacRoleForHybridReverse)
		env.RegisterActivity(replicationActivity.GenerateReverseCommandsForHybridReverse)
		env.RegisterActivity(replicationActivity.UpdateReplicationWithReverseCommandsForHybridReverse)
		env.RegisterActivity(replicationActivity.CreateJobForHybridReverse)
		env.RegisterWorkflow(ReverseHybridReplicationPollWorkflow)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.ReverseAndResumeReplicationParams{
			AccountName: "test-account",
		}

		event := &replication.ReverseReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{
					BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-replication-uuid"},
					Name:      "test-replication",
					Volume: &datamodel.Volume{
						BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-volume-uuid"},
						PoolID:    1,
						Pool: &datamodel.Pool{
							BaseModel: datamodel.BaseModel{ID: 1},
							DeploymentName: "test-deployment",
							PoolCredentials: &datamodel.PoolCredentials{
								Password:      "test-password",
								SecretID:      "test-secret-id",
								CertificateID: "test-cert-id",
							},
						},
					},
					ReplicationAttributes: &datamodel.ReplicationDetails{
						SourceSvmName:      "source-svm",
						SourceVolumeName:   "source-volume",
						DestinationSvmName: "dest-svm",
						DestinationVolumeName: "dest-volume",
						ReplicationSchedule: vsa.VolumeReplicationScheduleHourly,
					},
					HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{},
					ClusterPeer: &datamodel.ClusterPeerings{
						BaseModel: datamodel.BaseModel{UUID: "test-cluster-peer-uuid"},
						OntapPeerUUID: "test-ontap-peer-uuid",
					},
				},
				SourceProjectNumber:      "123456789",
				DestinationProjectNumber: "987654321",
				XCorrelationID:          func() *string { s := "test-correlation-id"; return &s }(),
			},
		}

		reverseResult := &replication.ReverseHybridReplicationResult{
			Event:            event,
			DbVolReplication: event.ReplicationModel,
			DstProjectNumber: &event.DestinationProjectNumber,
			SrcProjectNumber: &event.SourceProjectNumber,
			IsSrcForHybridReplication: false, // Default to false for poll workflow
		}

		pollJob := &datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "test-job-uuid"},
			WorkflowID: "test-poll-workflow-id",
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     "NEW",
		}, nil)
		env.OnActivity("SetHybridReplicationVariablesReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetNodeProviderForHybridReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("CheckClusterPeerHealthForHybridReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("UpdateRbacRoleForHybridReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GenerateReverseCommandsForHybridReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("UpdateReplicationWithReverseCommandsForHybridReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("CreateJobForHybridReverse", mock.Anything, mock.Anything, mock.Anything).Return(pollJob, nil)
		env.OnWorkflow("ReverseHybridReplicationPollWorkflow", mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(ReverseHybridReplicationWorkflow, params, event)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		assert.Nil(tt, err)
		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestReverseHybridReplicationWorkflow_Success_WithIsSrcForHybridReplication", func(tt *testing.T) {
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
		replicationActivity := replicationActivities.ReverseHybridReplicationActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(commonActivity.GetJob)
		env.RegisterActivity(replicationActivity.SetHybridReplicationVariablesReverse)
		env.RegisterActivity(replicationActivity.GetNodeProviderForHybridReverse)
		env.RegisterActivity(replicationActivity.CheckClusterPeerHealthForHybridReverse)
		env.RegisterActivity(replicationActivity.UpdateRbacRoleForHybridReverse)
		env.RegisterActivity(replicationActivity.GenerateReverseCommandsForHybridReverse)
		env.RegisterActivity(replicationActivity.UpdateReplicationWithReverseCommandsForHybridReverse)
		env.RegisterActivity(replicationActivity.CreateJobForHybridReverse)
		env.RegisterWorkflow(ReverseHybridFallbackReplicationWorkflow)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.ReverseAndResumeReplicationParams{
			AccountName: "test-account",
		}

		reverseType := string(coreModels.HybridReplicationParametersReplicationTypeREVERSE)
		event := &replication.ReverseReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{
					BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-replication-uuid"},
					Name:      "test-replication",
					Volume: &datamodel.Volume{
						BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-volume-uuid"},
						PoolID:    1,
						Pool: &datamodel.Pool{
							BaseModel: datamodel.BaseModel{ID: 1},
							DeploymentName: "test-deployment",
							PoolCredentials: &datamodel.PoolCredentials{
								Password:      "test-password",
								SecretID:      "test-secret-id",
								CertificateID: "test-cert-id",
							},
						},
					},
					ReplicationAttributes: &datamodel.ReplicationDetails{
						SourceSvmName:      "source-svm",
						SourceVolumeName:   "source-volume",
						DestinationSvmName: "dest-svm",
						DestinationVolumeName: "dest-volume",
						ReplicationSchedule: vsa.VolumeReplicationScheduleHourly,
						DestinationLocation: "customer", // This makes IsSrcForHybridReplication return true
					},
					HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
						HybridReplicationType: &reverseType,
					},
					ClusterPeer: &datamodel.ClusterPeerings{
						BaseModel: datamodel.BaseModel{UUID: "test-cluster-peer-uuid"},
						OntapPeerUUID: "test-ontap-peer-uuid",
					},
				},
				SourceProjectNumber:      "123456789",
				DestinationProjectNumber: "987654321",
				XCorrelationID:          func() *string { s := "test-correlation-id"; return &s }(),
			},
		}

		reverseResult := &replication.ReverseHybridReplicationResult{
			Event:            event,
			DbVolReplication: event.ReplicationModel,
			DstProjectNumber: &event.DestinationProjectNumber,
			SrcProjectNumber: &event.SourceProjectNumber,
			IsSrcForHybridReplication: true, // This triggers fallback workflow
		}

		fallbackJob := &datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "test-fallback-job-uuid"},
			WorkflowID: "test-fallback-workflow-id",
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     "NEW",
		}, nil)
		env.OnActivity("SetHybridReplicationVariablesReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetNodeProviderForHybridReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("CheckClusterPeerHealthForHybridReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("UpdateRbacRoleForHybridReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GenerateReverseCommandsForHybridReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("UpdateReplicationWithReverseCommandsForHybridReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("CreateJobForHybridReverse", mock.Anything, mock.Anything, mock.Anything).Return(fallbackJob, nil)
		env.OnWorkflow("ReverseHybridFallbackReplicationWorkflow", mock.Anything, mock.Anything, mock.Anything).Return((*vsa.VolumeReplication)(nil), nil)

		env.ExecuteWorkflow(ReverseHybridReplicationWorkflow, params, event)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		assert.Nil(tt, err)
		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestReverseHybridReplicationWorkflow_GetNodeProviderError", func(tt *testing.T) {
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
		replicationActivity := replicationActivities.ReverseHybridReplicationActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(commonActivity.GetJob)
		env.RegisterActivity(replicationActivity.SetHybridReplicationVariablesReverse)
		env.RegisterActivity(replicationActivity.GetNodeProviderForHybridReverse)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.ReverseAndResumeReplicationParams{
			AccountName: "test-account",
		}

		event := &replication.ReverseReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{
					BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-replication-uuid"},
					Name:      "test-replication",
					Volume: &datamodel.Volume{
						BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-volume-uuid"},
						PoolID:    1,
					},
					HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{},
				},
				SourceProjectNumber:      "123456789",
				DestinationProjectNumber: "987654321",
			},
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     "NEW",
		}, nil)
		env.OnActivity("SetHybridReplicationVariablesReverse", mock.Anything, mock.Anything).Return(nil, assert.AnError)

		env.ExecuteWorkflow(ReverseHybridReplicationWorkflow, params, event)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("TestReverseHybridReplicationWorkflow_CheckClusterPeerHealthError", func(tt *testing.T) {
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
		replicationActivity := replicationActivities.ReverseHybridReplicationActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(commonActivity.GetJob)
		env.RegisterActivity(replicationActivity.SetHybridReplicationVariablesReverse)
		env.RegisterActivity(replicationActivity.GetNodeProviderForHybridReverse)
		env.RegisterActivity(replicationActivity.CheckClusterPeerHealthForHybridReverse)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.ReverseAndResumeReplicationParams{
			AccountName: "test-account",
		}

		event := &replication.ReverseReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{
					BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-replication-uuid"},
					Name:      "test-replication",
					Volume: &datamodel.Volume{
						BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-volume-uuid"},
						PoolID:    1,
						Pool: &datamodel.Pool{
							BaseModel: datamodel.BaseModel{ID: 1},
							DeploymentName: "test-deployment",
							PoolCredentials: &datamodel.PoolCredentials{
								Password:      "test-password",
								SecretID:      "test-secret-id",
								CertificateID: "test-cert-id",
							},
						},
					},
					HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{},
					ClusterPeer: &datamodel.ClusterPeerings{
						BaseModel: datamodel.BaseModel{UUID: "test-cluster-peer-uuid"},
						OntapPeerUUID: "test-ontap-peer-uuid",
					},
				},
				SourceProjectNumber:      "123456789",
				DestinationProjectNumber: "987654321",
			},
		}

		reverseResult := &replication.ReverseHybridReplicationResult{
			Event:            event,
			DbVolReplication: event.ReplicationModel,
			DstProjectNumber: &event.DestinationProjectNumber,
			SrcProjectNumber: &event.SourceProjectNumber,
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     "NEW",
		}, nil)
		env.OnActivity("SetHybridReplicationVariablesReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetNodeProviderForHybridReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("CheckClusterPeerHealthForHybridReverse", mock.Anything, mock.Anything).Return(nil, assert.AnError)

		env.ExecuteWorkflow(ReverseHybridReplicationWorkflow, params, event)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("TestReverseHybridReplicationWorkflow_UpdateRbacRoleError", func(tt *testing.T) {
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
		replicationActivity := replicationActivities.ReverseHybridReplicationActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(commonActivity.GetJob)
		env.RegisterActivity(replicationActivity.SetHybridReplicationVariablesReverse)
		env.RegisterActivity(replicationActivity.GetNodeProviderForHybridReverse)
		env.RegisterActivity(replicationActivity.CheckClusterPeerHealthForHybridReverse)
		env.RegisterActivity(replicationActivity.UpdateRbacRoleForHybridReverse)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.ReverseAndResumeReplicationParams{
			AccountName: "test-account",
		}

		event := &replication.ReverseReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{
					BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-replication-uuid"},
					Name:      "test-replication",
					Volume: &datamodel.Volume{
						BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-volume-uuid"},
						PoolID:    1,
						Pool: &datamodel.Pool{
							BaseModel: datamodel.BaseModel{ID: 1},
							DeploymentName: "test-deployment",
							PoolCredentials: &datamodel.PoolCredentials{
								Password:      "test-password",
								SecretID:      "test-secret-id",
								CertificateID: "test-cert-id",
							},
						},
					},
					ReplicationAttributes: &datamodel.ReplicationDetails{
						SourceSvmName:      "source-svm",
						SourceVolumeName:   "source-volume",
					},
					HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{},
					ClusterPeer: &datamodel.ClusterPeerings{
						BaseModel: datamodel.BaseModel{UUID: "test-cluster-peer-uuid"},
						OntapPeerUUID: "test-ontap-peer-uuid",
					},
				},
				SourceProjectNumber:      "123456789",
				DestinationProjectNumber: "987654321",
			},
		}

		reverseResult := &replication.ReverseHybridReplicationResult{
			Event:            event,
			DbVolReplication: event.ReplicationModel,
			DstProjectNumber: &event.DestinationProjectNumber,
			SrcProjectNumber: &event.SourceProjectNumber,
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     "NEW",
		}, nil)
		env.OnActivity("SetHybridReplicationVariablesReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetNodeProviderForHybridReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("CheckClusterPeerHealthForHybridReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("UpdateRbacRoleForHybridReverse", mock.Anything, mock.Anything).Return(nil, assert.AnError)

		env.ExecuteWorkflow(ReverseHybridReplicationWorkflow, params, event)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("TestReverseHybridReplicationWorkflow_GenerateReverseCommandsError", func(tt *testing.T) {
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
		replicationActivity := replicationActivities.ReverseHybridReplicationActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(commonActivity.GetJob)
		env.RegisterActivity(replicationActivity.SetHybridReplicationVariablesReverse)
		env.RegisterActivity(replicationActivity.GetNodeProviderForHybridReverse)
		env.RegisterActivity(replicationActivity.CheckClusterPeerHealthForHybridReverse)
		env.RegisterActivity(replicationActivity.UpdateRbacRoleForHybridReverse)
		env.RegisterActivity(replicationActivity.GenerateReverseCommandsForHybridReverse)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.ReverseAndResumeReplicationParams{
			AccountName: "test-account",
		}

		event := &replication.ReverseReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{
					BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-replication-uuid"},
					Name:      "test-replication",
					Volume: &datamodel.Volume{
						BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-volume-uuid"},
						PoolID:    1,
						Pool: &datamodel.Pool{
							BaseModel: datamodel.BaseModel{ID: 1},
							DeploymentName: "test-deployment",
							PoolCredentials: &datamodel.PoolCredentials{
								Password:      "test-password",
								SecretID:      "test-secret-id",
								CertificateID: "test-cert-id",
							},
						},
					},
					ReplicationAttributes: &datamodel.ReplicationDetails{
						SourceSvmName:      "source-svm",
						SourceVolumeName:   "source-volume",
						DestinationSvmName: "dest-svm",
						DestinationVolumeName: "dest-volume",
						ReplicationSchedule: vsa.VolumeReplicationScheduleHourly,
					},
					HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{},
					ClusterPeer: &datamodel.ClusterPeerings{
						BaseModel: datamodel.BaseModel{UUID: "test-cluster-peer-uuid"},
						OntapPeerUUID: "test-ontap-peer-uuid",
					},
				},
				SourceProjectNumber:      "123456789",
				DestinationProjectNumber: "987654321",
			},
		}

		reverseResult := &replication.ReverseHybridReplicationResult{
			Event:            event,
			DbVolReplication: event.ReplicationModel,
			DstProjectNumber: &event.DestinationProjectNumber,
			SrcProjectNumber: &event.SourceProjectNumber,
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     "NEW",
		}, nil)
		env.OnActivity("SetHybridReplicationVariablesReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetNodeProviderForHybridReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("CheckClusterPeerHealthForHybridReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("UpdateRbacRoleForHybridReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GenerateReverseCommandsForHybridReverse", mock.Anything, mock.Anything).Return(nil, assert.AnError)

		env.ExecuteWorkflow(ReverseHybridReplicationWorkflow, params, event)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("TestReverseHybridReplicationWorkflow_UpdateReplicationWithReverseCommandsError", func(tt *testing.T) {
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
		replicationActivity := replicationActivities.ReverseHybridReplicationActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(commonActivity.GetJob)
		env.RegisterActivity(replicationActivity.SetHybridReplicationVariablesReverse)
		env.RegisterActivity(replicationActivity.GetNodeProviderForHybridReverse)
		env.RegisterActivity(replicationActivity.CheckClusterPeerHealthForHybridReverse)
		env.RegisterActivity(replicationActivity.UpdateRbacRoleForHybridReverse)
		env.RegisterActivity(replicationActivity.GenerateReverseCommandsForHybridReverse)
		env.RegisterActivity(replicationActivity.UpdateReplicationWithReverseCommandsForHybridReverse)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.ReverseAndResumeReplicationParams{
			AccountName: "test-account",
		}

		event := &replication.ReverseReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{
					BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-replication-uuid"},
					Name:      "test-replication",
					Volume: &datamodel.Volume{
						BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-volume-uuid"},
						PoolID:    1,
						Pool: &datamodel.Pool{
							BaseModel: datamodel.BaseModel{ID: 1},
							DeploymentName: "test-deployment",
							PoolCredentials: &datamodel.PoolCredentials{
								Password:      "test-password",
								SecretID:      "test-secret-id",
								CertificateID: "test-cert-id",
							},
						},
					},
					ReplicationAttributes: &datamodel.ReplicationDetails{
						SourceSvmName:      "source-svm",
						SourceVolumeName:   "source-volume",
						DestinationSvmName: "dest-svm",
						DestinationVolumeName: "dest-volume",
						ReplicationSchedule: vsa.VolumeReplicationScheduleHourly,
					},
					HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{},
					ClusterPeer: &datamodel.ClusterPeerings{
						BaseModel: datamodel.BaseModel{UUID: "test-cluster-peer-uuid"},
						OntapPeerUUID: "test-ontap-peer-uuid",
					},
				},
				SourceProjectNumber:      "123456789",
				DestinationProjectNumber: "987654321",
			},
		}

		reverseResult := &replication.ReverseHybridReplicationResult{
			Event:            event,
			DbVolReplication: event.ReplicationModel,
			DstProjectNumber: &event.DestinationProjectNumber,
			SrcProjectNumber: &event.SourceProjectNumber,
			HybridReplicationUserCommands: []string{"command1", "command2"},
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     "NEW",
		}, nil)
		env.OnActivity("SetHybridReplicationVariablesReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetNodeProviderForHybridReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("CheckClusterPeerHealthForHybridReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("UpdateRbacRoleForHybridReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GenerateReverseCommandsForHybridReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("UpdateReplicationWithReverseCommandsForHybridReverse", mock.Anything, mock.Anything).Return(nil, assert.AnError)

		env.ExecuteWorkflow(ReverseHybridReplicationWorkflow, params, event)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("TestReverseHybridReplicationWorkflow_CreateJobError", func(tt *testing.T) {
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
		replicationActivity := replicationActivities.ReverseHybridReplicationActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(commonActivity.GetJob)
		env.RegisterActivity(replicationActivity.SetHybridReplicationVariablesReverse)
		env.RegisterActivity(replicationActivity.GetNodeProviderForHybridReverse)
		env.RegisterActivity(replicationActivity.CheckClusterPeerHealthForHybridReverse)
		env.RegisterActivity(replicationActivity.UpdateRbacRoleForHybridReverse)
		env.RegisterActivity(replicationActivity.GenerateReverseCommandsForHybridReverse)
		env.RegisterActivity(replicationActivity.UpdateReplicationWithReverseCommandsForHybridReverse)
		env.RegisterActivity(replicationActivity.CreateJobForHybridReverse)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.ReverseAndResumeReplicationParams{
			AccountName: "test-account",
		}

		event := &replication.ReverseReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{
					BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-replication-uuid"},
					Name:      "test-replication",
					Volume: &datamodel.Volume{
						BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-volume-uuid"},
						PoolID:    1,
						Pool: &datamodel.Pool{
							BaseModel: datamodel.BaseModel{ID: 1},
							DeploymentName: "test-deployment",
							PoolCredentials: &datamodel.PoolCredentials{
								Password:      "test-password",
								SecretID:      "test-secret-id",
								CertificateID: "test-cert-id",
							},
						},
					},
					ReplicationAttributes: &datamodel.ReplicationDetails{
						SourceSvmName:      "source-svm",
						SourceVolumeName:   "source-volume",
						DestinationSvmName: "dest-svm",
						DestinationVolumeName: "dest-volume",
						ReplicationSchedule: vsa.VolumeReplicationScheduleHourly,
					},
					HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{},
					ClusterPeer: &datamodel.ClusterPeerings{
						BaseModel: datamodel.BaseModel{UUID: "test-cluster-peer-uuid"},
						OntapPeerUUID: "test-ontap-peer-uuid",
					},
				},
				SourceProjectNumber:      "123456789",
				DestinationProjectNumber: "987654321",
			},
		}

		reverseResult := &replication.ReverseHybridReplicationResult{
			Event:            event,
			DbVolReplication: event.ReplicationModel,
			DstProjectNumber: &event.DestinationProjectNumber,
			SrcProjectNumber: &event.SourceProjectNumber,
			HybridReplicationUserCommands: []string{"command1", "command2"},
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     "NEW",
		}, nil)
		env.OnActivity("SetHybridReplicationVariablesReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetNodeProviderForHybridReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("CheckClusterPeerHealthForHybridReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("UpdateRbacRoleForHybridReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GenerateReverseCommandsForHybridReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("UpdateReplicationWithReverseCommandsForHybridReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("CreateJobForHybridReverse", mock.Anything, mock.Anything, mock.Anything).Return(nil, assert.AnError)

		env.ExecuteWorkflow(ReverseHybridReplicationWorkflow, params, event)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("TestReverseHybridReplicationWorkflow_ChildWorkflowStartError", func(tt *testing.T) {
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
		replicationActivity := replicationActivities.ReverseHybridReplicationActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(commonActivity.GetJob)
		env.RegisterActivity(replicationActivity.SetHybridReplicationVariablesReverse)
		env.RegisterActivity(replicationActivity.GetNodeProviderForHybridReverse)
		env.RegisterActivity(replicationActivity.CheckClusterPeerHealthForHybridReverse)
		env.RegisterActivity(replicationActivity.UpdateRbacRoleForHybridReverse)
		env.RegisterActivity(replicationActivity.GenerateReverseCommandsForHybridReverse)
		env.RegisterActivity(replicationActivity.UpdateReplicationWithReverseCommandsForHybridReverse)
		env.RegisterActivity(replicationActivity.CreateJobForHybridReverse)
		env.RegisterWorkflow(ReverseHybridReplicationPollWorkflow)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.ReverseAndResumeReplicationParams{
			AccountName: "test-account",
		}

		event := &replication.ReverseReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{
					BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-replication-uuid"},
					Name:      "test-replication",
					Volume: &datamodel.Volume{
						BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-volume-uuid"},
						PoolID:    1,
						Pool: &datamodel.Pool{
							BaseModel: datamodel.BaseModel{ID: 1},
							DeploymentName: "test-deployment",
							PoolCredentials: &datamodel.PoolCredentials{
								Password:      "test-password",
								SecretID:      "test-secret-id",
								CertificateID: "test-cert-id",
							},
						},
					},
					ReplicationAttributes: &datamodel.ReplicationDetails{
						SourceSvmName:      "source-svm",
						SourceVolumeName:   "source-volume",
						DestinationSvmName: "dest-svm",
						DestinationVolumeName: "dest-volume",
						ReplicationSchedule: vsa.VolumeReplicationScheduleHourly,
					},
					HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{},
					ClusterPeer: &datamodel.ClusterPeerings{
						BaseModel: datamodel.BaseModel{UUID: "test-cluster-peer-uuid"},
						OntapPeerUUID: "test-ontap-peer-uuid",
					},
				},
				SourceProjectNumber:      "123456789",
				DestinationProjectNumber: "987654321",
			},
		}

		reverseResult := &replication.ReverseHybridReplicationResult{
			Event:            event,
			DbVolReplication: event.ReplicationModel,
			DstProjectNumber: &event.DestinationProjectNumber,
			SrcProjectNumber: &event.SourceProjectNumber,
		}

		pollJob := &datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "test-job-uuid"},
			WorkflowID: "test-poll-workflow-id",
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     "NEW",
		}, nil)
		env.OnActivity("SetHybridReplicationVariablesReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GetNodeProviderForHybridReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("CheckClusterPeerHealthForHybridReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("UpdateRbacRoleForHybridReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("GenerateReverseCommandsForHybridReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("UpdateReplicationWithReverseCommandsForHybridReverse", mock.Anything, mock.Anything).Return(reverseResult, nil)
		env.OnActivity("CreateJobForHybridReverse", mock.Anything, mock.Anything, mock.Anything).Return(pollJob, nil)
		env.OnWorkflow("ReverseHybridReplicationPollWorkflow", mock.Anything, mock.Anything).Return(assert.AnError)

		env.ExecuteWorkflow(ReverseHybridReplicationWorkflow, params, event)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})
}
