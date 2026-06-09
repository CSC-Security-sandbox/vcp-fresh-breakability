package replicationWorkflows

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/replicationActivities"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/replication"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestHybridReplicationDeleteWorkflow(t *testing.T) {
	t.Run("TestHybridReplicationDeleteWorkflow_Success_GCNVIsDestination", func(tt *testing.T) {
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
		deleteReplicationActivity := replicationActivities.DeleteVolumeReplicationActivity{SE: mockStorage}
		hybridDeleteActivity := replicationActivities.HybridDeleteVolumeReplicationActivity{SE: mockStorage}
		env.SetHeader(mockHeader)

		// Register activities
		env.RegisterActivity(deleteReplicationActivity.SetHybridReplicationVariablesDelete)
		env.RegisterActivity(deleteReplicationActivity.UpdateReplicationInDBToErrorState)
		env.RegisterActivity(deleteReplicationActivity.UpdateRbacRole)
		env.RegisterActivity(deleteReplicationActivity.DeleteClusterPeeringInOntap)
		env.RegisterActivity(deleteReplicationActivity.DeleteClusterPeeringDB)
		env.RegisterActivity(deleteReplicationActivity.DeleteRoleInOntap)
		env.RegisterActivity(hybridDeleteActivity.CreateJobForHybridDeleteVolume)
		env.RegisterActivity(deleteReplicationActivity.GetSrcBasePathDelete)
		env.RegisterActivity(deleteReplicationActivity.GetDstBasePathDelete)
		env.RegisterActivity(deleteReplicationActivity.GetSignedSrcTokenDelete)
		env.RegisterActivity(deleteReplicationActivity.GetSignedDstTokenDelete)
		env.RegisterActivity(deleteReplicationActivity.GetReplicationOnDestinationForDelete)
		env.RegisterActivity(deleteReplicationActivity.DeleteReplicationOnDestination)
		env.RegisterActivity(deleteReplicationActivity.DescribeRemoteJobForDelete)
		env.RegisterActivity(deleteReplicationActivity.UpdateReplicationRecordOnDestination)
		env.RegisterActivity(deleteReplicationActivity.DeleteSnapmirrorSnapshotsOnDestination)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.DeleteReplicationParams{
			AccountName:           "test-account",
			Region:                "us-central1",
			CorrelationId:         "test-correlation-id",
			VolumeResourceId:      "test-volume-id",
			ReplicationResourceId: "test-replication-id",
			Zone:                  "us-central1-a",
		}

		event := &replication.DeleteReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{
					HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{},
					ReplicationAttributes: &datamodel.ReplicationDetails{
						SourceLocation:      "customer",
						DestinationLocation: "us-central1",
					},
				},
				SourceProjectNumber:      "123456789",
				DestinationProjectNumber: "987654321",
				VolumeResourceID:         "test-volume-id",
				ReplicationResourceID:    "test-replication-id",
				Location:                 "us-central1",
				Zone:                     "us-central1-a",
				AccountName:              "test-account",
			},
		}

		replicationResult := &replication.DeleteReplicationResult{
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
			Event:            event,
			DstReplication: &googleproxyclient.VolumeReplicationInternalV1beta{
				Name:             googleproxyclient.NewOptString("repl-123"),
				LastTransferSize: googleproxyclient.NewOptInt64(100),
				MirrorState:      googleproxyclient.NewOptVolumeReplicationInternalV1betaMirrorState(googleproxyclient.VolumeReplicationInternalV1betaMirrorStateSTOPPED),
			},
			IsHybridReplicationVolume: true,
			IsSrcForHybridReplication: false,
			CleanupClusterPeering:     false,
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SetHybridReplicationVariablesDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetSrcBasePathDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetDstBasePathDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetSignedSrcTokenDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetSignedDstTokenDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetReplicationOnDestinationForDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("DeleteReplicationOnDestination", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("DescribeRemoteJobForDelete", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteSnapmirrorSnapshotsOnDestination", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("UpdateReplicationRecordOnDestination", mock.Anything, mock.Anything).Return(replicationResult, nil)

		env.ExecuteWorkflow(HybridReplicationDeleteWorkflow, params, event)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		assert.Nil(tt, err)
		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestHybridReplicationDeleteWorkflow_Success_GCNVIsSource", func(tt *testing.T) {
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
		deleteReplicationActivity := replicationActivities.DeleteVolumeReplicationActivity{SE: mockStorage}
		env.SetHeader(mockHeader)

		// Register activities
		env.RegisterActivity(deleteReplicationActivity.SetHybridReplicationVariablesDelete)
		env.RegisterActivity(deleteReplicationActivity.UpdateReplicationInDBToErrorState)
		env.RegisterActivity(deleteReplicationActivity.UpdateRbacRole)
		env.RegisterActivity(deleteReplicationActivity.DeleteClusterPeeringInOntap)
		env.RegisterActivity(deleteReplicationActivity.DeleteClusterPeeringDB)
		env.RegisterActivity(deleteReplicationActivity.DeleteRoleInOntap)
		env.RegisterActivity(deleteReplicationActivity.GetSrcBasePathDelete)
		env.RegisterActivity(deleteReplicationActivity.GetDstBasePathDelete)
		env.RegisterActivity(deleteReplicationActivity.GetSignedSrcTokenDelete)
		env.RegisterActivity(deleteReplicationActivity.GetSignedDstTokenDelete)
		env.RegisterActivity(deleteReplicationActivity.ReleaseReplicationOnSrc)
		env.RegisterActivity(deleteReplicationActivity.DeleteSnapmirrorSnapshotsOnSource)
		env.RegisterActivity(deleteReplicationActivity.DescribeSourceJobForDelete)
		env.RegisterActivity(deleteReplicationActivity.DeleteReplicationRecordOnSource)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.DeleteReplicationParams{
			AccountName:           "test-account",
			Region:                "us-central1",
			CorrelationId:         "test-correlation-id",
			VolumeResourceId:      "test-volume-id",
			ReplicationResourceId: "test-replication-id",
			Zone:                  "us-central1-a",
		}

		migrationType := string(datamodel.HybridReplicationParametersReplicationTypeREVERSE)
		event := &replication.DeleteReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{
					HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
						HybridReplicationType: &migrationType,
					},
					ReplicationAttributes: &datamodel.ReplicationDetails{
						SourceLocation:      "us-central1",
						DestinationLocation: "customer",
					},
					Volume: &datamodel.Volume{
						Pool: &datamodel.Pool{
							BaseModel: datamodel.BaseModel{
								ID: 1,
							},
							DeploymentName: "test-deployment",
							PoolCredentials: &datamodel.PoolCredentials{
								SecretID: "test-secret-id",
							},
						},
					},
				},
				SourceProjectNumber:      "123456789",
				DestinationProjectNumber: "987654321",
				VolumeResourceID:         "test-volume-id",
				ReplicationResourceID:    "test-replication-id",
				Location:                 "us-central1",
				Zone:                     "us-central1-a",
				AccountName:              "test-account",
			},
		}

		replicationResult := &replication.DeleteReplicationResult{
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
			Event:            event,
			DstReplication: &googleproxyclient.VolumeReplicationInternalV1beta{
				Name:             googleproxyclient.NewOptString("repl-123"),
				LastTransferSize: googleproxyclient.NewOptInt64(100),
				MirrorState:      googleproxyclient.NewOptVolumeReplicationInternalV1betaMirrorState(googleproxyclient.VolumeReplicationInternalV1betaMirrorStateSTOPPED),
			},
			IsHybridReplicationVolume: true,
			IsSrcForHybridReplication: true,
			CleanupClusterPeering:     false,
		}

		dbNodes := []*datamodel.Node{
			{
				Name: "test-node",
			},
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SetHybridReplicationVariablesDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetSrcBasePathDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetDstBasePathDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetSignedSrcTokenDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetSignedDstTokenDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return(dbNodes, nil)
		env.OnActivity("ReleaseReplicationOnSrc", mock.Anything, mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("DeleteSnapmirrorSnapshotsOnSource", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("DescribeSourceJobForDelete", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteReplicationRecordOnSource", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateRbacRole", mock.Anything, mock.Anything, mock.Anything).Return(replicationResult, nil)

		env.ExecuteWorkflow(HybridReplicationDeleteWorkflow, params, event)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		assert.Nil(tt, err)
		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestHybridReplicationDeleteWorkflow_Success_GCNVIsSource_ReleaseActivityUsesExtendedTimeout", func(tt *testing.T) {
		originalStartToClose := workflows.StartToCloseTimeoutForReplicationActivities
		workflows.StartToCloseTimeoutForReplicationActivities = 1
		defer func() {
			workflows.StartToCloseTimeoutForReplicationActivities = originalStartToClose
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
		mockStorage := database.NewMockStorage(tt)
		commonActivity := activities.CommonActivities{SE: mockStorage}
		deleteReplicationActivity := replicationActivities.DeleteVolumeReplicationActivity{SE: mockStorage}
		env.SetHeader(mockHeader)

		env.RegisterActivity(deleteReplicationActivity.SetHybridReplicationVariablesDelete)
		env.RegisterActivity(deleteReplicationActivity.UpdateReplicationInDBToErrorState)
		env.RegisterActivity(deleteReplicationActivity.UpdateRbacRole)
		env.RegisterActivity(deleteReplicationActivity.DeleteClusterPeeringInOntap)
		env.RegisterActivity(deleteReplicationActivity.DeleteClusterPeeringDB)
		env.RegisterActivity(deleteReplicationActivity.DeleteRoleInOntap)
		env.RegisterActivity(deleteReplicationActivity.GetSrcBasePathDelete)
		env.RegisterActivity(deleteReplicationActivity.GetDstBasePathDelete)
		env.RegisterActivity(deleteReplicationActivity.GetSignedSrcTokenDelete)
		env.RegisterActivity(deleteReplicationActivity.GetSignedDstTokenDelete)
		env.RegisterActivity(deleteReplicationActivity.ReleaseReplicationOnSrc)
		env.RegisterActivity(deleteReplicationActivity.DeleteSnapmirrorSnapshotsOnSource)
		env.RegisterActivity(deleteReplicationActivity.DescribeSourceJobForDelete)
		env.RegisterActivity(deleteReplicationActivity.DeleteReplicationRecordOnSource)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.DeleteReplicationParams{
			AccountName:           "test-account",
			Region:                "us-central1",
			CorrelationId:         "test-correlation-id",
			VolumeResourceId:      "test-volume-id",
			ReplicationResourceId: "test-replication-id",
			Zone:                  "us-central1-a",
		}

		migrationType := string(datamodel.HybridReplicationParametersReplicationTypeREVERSE)
		event := &replication.DeleteReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{
					HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
						HybridReplicationType: &migrationType,
					},
					ReplicationAttributes: &datamodel.ReplicationDetails{
						SourceLocation:      "us-central1",
						DestinationLocation: "customer",
					},
					Volume: &datamodel.Volume{
						Pool: &datamodel.Pool{
							BaseModel:      datamodel.BaseModel{ID: 1},
							DeploymentName: "test-deployment",
							PoolCredentials: &datamodel.PoolCredentials{
								SecretID: "test-secret-id",
							},
						},
					},
				},
				SourceProjectNumber:      "123456789",
				DestinationProjectNumber: "987654321",
				VolumeResourceID:         "test-volume-id",
				ReplicationResourceID:    "test-replication-id",
				Location:                 "us-central1",
				Zone:                     "us-central1-a",
				AccountName:              "test-account",
			},
		}

		replicationResult := &replication.DeleteReplicationResult{
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
			Event:            event,
			DstReplication: &googleproxyclient.VolumeReplicationInternalV1beta{
				Name:             googleproxyclient.NewOptString("repl-123"),
				LastTransferSize: googleproxyclient.NewOptInt64(100),
				MirrorState:      googleproxyclient.NewOptVolumeReplicationInternalV1betaMirrorState(googleproxyclient.VolumeReplicationInternalV1betaMirrorStateSTOPPED),
			},
			IsHybridReplicationVolume: true,
			IsSrcForHybridReplication: true,
			CleanupClusterPeering:     false,
		}
		dbNodes := []*datamodel.Node{{Name: "test-node"}}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SetHybridReplicationVariablesDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetSrcBasePathDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetDstBasePathDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetSignedSrcTokenDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetSignedDstTokenDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return(dbNodes, nil)
		env.OnActivity("ReleaseReplicationOnSrc", mock.Anything, mock.Anything, mock.Anything).After(2*time.Second).Return(replicationResult, nil)
		env.OnActivity("DeleteSnapmirrorSnapshotsOnSource", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("DescribeSourceJobForDelete", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteReplicationRecordOnSource", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateRbacRole", mock.Anything, mock.Anything, mock.Anything).Return(replicationResult, nil)

		env.ExecuteWorkflow(HybridReplicationDeleteWorkflow, params, event)
		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestHybridReplicationDeleteWorkflow_Success_GCNVIsSource_WithClusterPeeringCleanup", func(tt *testing.T) {
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
		deleteReplicationActivity := replicationActivities.DeleteVolumeReplicationActivity{SE: mockStorage}
		env.SetHeader(mockHeader)

		// Register activities
		env.RegisterActivity(deleteReplicationActivity.SetHybridReplicationVariablesDelete)
		env.RegisterActivity(deleteReplicationActivity.UpdateReplicationInDBToErrorState)
		env.RegisterActivity(deleteReplicationActivity.UpdateRbacRole)
		env.RegisterActivity(deleteReplicationActivity.DeleteClusterPeeringInOntap)
		env.RegisterActivity(deleteReplicationActivity.DeleteClusterPeeringDB)
		env.RegisterActivity(deleteReplicationActivity.DeleteRoleInOntap)
		env.RegisterActivity(deleteReplicationActivity.GetSrcBasePathDelete)
		env.RegisterActivity(deleteReplicationActivity.GetDstBasePathDelete)
		env.RegisterActivity(deleteReplicationActivity.GetSignedSrcTokenDelete)
		env.RegisterActivity(deleteReplicationActivity.GetSignedDstTokenDelete)
		env.RegisterActivity(deleteReplicationActivity.ReleaseReplicationOnSrc)
		env.RegisterActivity(deleteReplicationActivity.DeleteSnapmirrorSnapshotsOnSource)
		env.RegisterActivity(deleteReplicationActivity.DescribeSourceJobForDelete)
		env.RegisterActivity(deleteReplicationActivity.DeleteReplicationRecordOnSource)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.DeleteReplicationParams{
			AccountName:           "test-account",
			Region:                "us-central1",
			CorrelationId:         "test-correlation-id",
			VolumeResourceId:      "test-volume-id",
			ReplicationResourceId: "test-replication-id",
			Zone:                  "us-central1-a",
		}

		migrationType := string(datamodel.HybridReplicationParametersReplicationTypeREVERSE)
		event := &replication.DeleteReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{
					HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
						HybridReplicationType: &migrationType,
					},
					ReplicationAttributes: &datamodel.ReplicationDetails{
						SourceLocation:      "us-central1",
						DestinationLocation: "customer",
					},
					Volume: &datamodel.Volume{
						Pool: &datamodel.Pool{
							BaseModel: datamodel.BaseModel{
								ID: 1,
							},
							DeploymentName: "test-deployment",
							PoolCredentials: &datamodel.PoolCredentials{
								SecretID: "test-secret-id",
							},
						},
					},
				},
				SourceProjectNumber:      "123456789",
				DestinationProjectNumber: "987654321",
				VolumeResourceID:         "test-volume-id",
				ReplicationResourceID:    "test-replication-id",
				Location:                 "us-central1",
				Zone:                     "us-central1-a",
				AccountName:              "test-account",
			},
		}

		replicationResult := &replication.DeleteReplicationResult{
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
			Event:            event,
			DstReplication: &googleproxyclient.VolumeReplicationInternalV1beta{
				Name:             googleproxyclient.NewOptString("repl-123"),
				LastTransferSize: googleproxyclient.NewOptInt64(100),
				MirrorState:      googleproxyclient.NewOptVolumeReplicationInternalV1betaMirrorState(googleproxyclient.VolumeReplicationInternalV1betaMirrorStateSTOPPED),
			},
			IsHybridReplicationVolume: true,
			IsSrcForHybridReplication: true,
			CleanupClusterPeering:     true,
		}

		dbNodes := []*datamodel.Node{
			{
				Name: "test-node",
			},
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SetHybridReplicationVariablesDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetSrcBasePathDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetDstBasePathDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetSignedSrcTokenDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetSignedDstTokenDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return(dbNodes, nil)
		env.OnActivity("ReleaseReplicationOnSrc", mock.Anything, mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("DeleteSnapmirrorSnapshotsOnSource", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("DescribeSourceJobForDelete", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteReplicationRecordOnSource", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateRbacRole", mock.Anything, mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("DeleteClusterPeeringInOntap", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteClusterPeeringDB", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteRoleInOntap", mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(HybridReplicationDeleteWorkflow, params, event)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		assert.Nil(tt, err)
		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestHybridReplicationDeleteWorkflow_Success_GCNVIsDestination_WithClusterPeeringCleanup", func(tt *testing.T) {
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
		deleteReplicationActivity := replicationActivities.DeleteVolumeReplicationActivity{SE: mockStorage}
		hybridDeleteActivity := replicationActivities.HybridDeleteVolumeReplicationActivity{SE: mockStorage}
		env.SetHeader(mockHeader)

		// Register activities
		env.RegisterActivity(deleteReplicationActivity.SetHybridReplicationVariablesDelete)
		env.RegisterActivity(deleteReplicationActivity.UpdateReplicationInDBToErrorState)
		env.RegisterActivity(deleteReplicationActivity.UpdateRbacRole)
		env.RegisterActivity(deleteReplicationActivity.DeleteClusterPeeringInOntap)
		env.RegisterActivity(deleteReplicationActivity.DeleteClusterPeeringDB)
		env.RegisterActivity(deleteReplicationActivity.DeleteRoleInOntap)
		env.RegisterActivity(hybridDeleteActivity.CreateJobForHybridDeleteVolume)
		env.RegisterActivity(deleteReplicationActivity.GetSrcBasePathDelete)
		env.RegisterActivity(deleteReplicationActivity.GetDstBasePathDelete)
		env.RegisterActivity(deleteReplicationActivity.GetSignedSrcTokenDelete)
		env.RegisterActivity(deleteReplicationActivity.GetSignedDstTokenDelete)
		env.RegisterActivity(deleteReplicationActivity.GetReplicationOnDestinationForDelete)
		env.RegisterActivity(deleteReplicationActivity.DeleteReplicationOnDestination)
		env.RegisterActivity(deleteReplicationActivity.DescribeRemoteJobForDelete)
		env.RegisterActivity(deleteReplicationActivity.UpdateReplicationRecordOnDestination)
		env.RegisterActivity(deleteReplicationActivity.DeleteSnapmirrorSnapshotsOnDestination)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.DeleteReplicationParams{
			AccountName:           "test-account",
			Region:                "us-central1",
			CorrelationId:         "test-correlation-id",
			VolumeResourceId:      "test-volume-id",
			ReplicationResourceId: "test-replication-id",
			Zone:                  "us-central1-a",
		}

		event := &replication.DeleteReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{
					HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{},
					ReplicationAttributes: &datamodel.ReplicationDetails{
						SourceLocation:      "customer",
						DestinationLocation: "us-central1",
					},
					Volume: &datamodel.Volume{
						Pool: &datamodel.Pool{
							BaseModel: datamodel.BaseModel{
								ID: 1,
							},
							DeploymentName: "test-deployment",
							PoolCredentials: &datamodel.PoolCredentials{
								SecretID: "test-secret-id",
							},
						},
					},
				},
				SourceProjectNumber:      "123456789",
				DestinationProjectNumber: "987654321",
				VolumeResourceID:         "test-volume-id",
				ReplicationResourceID:    "test-replication-id",
				Location:                 "us-central1",
				Zone:                     "us-central1-a",
				AccountName:              "test-account",
			},
		}

		replicationResult := &replication.DeleteReplicationResult{
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
			Event:            event,
			DstReplication: &googleproxyclient.VolumeReplicationInternalV1beta{
				Name:             googleproxyclient.NewOptString("repl-123"),
				LastTransferSize: googleproxyclient.NewOptInt64(100),
				MirrorState:      googleproxyclient.NewOptVolumeReplicationInternalV1betaMirrorState(googleproxyclient.VolumeReplicationInternalV1betaMirrorStateSTOPPED),
			},
			IsHybridReplicationVolume: true,
			IsSrcForHybridReplication: false,
			CleanupClusterPeering:     true,
		}

		dbNodes := []*datamodel.Node{
			{
				Name: "test-node",
			},
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SetHybridReplicationVariablesDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetSrcBasePathDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetDstBasePathDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetSignedSrcTokenDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetSignedDstTokenDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetReplicationOnDestinationForDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("DeleteReplicationOnDestination", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("DescribeRemoteJobForDelete", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteSnapmirrorSnapshotsOnDestination", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("UpdateReplicationRecordOnDestination", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return(dbNodes, nil)
		env.OnActivity("DeleteClusterPeeringInOntap", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteClusterPeeringDB", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteRoleInOntap", mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(HybridReplicationDeleteWorkflow, params, event)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		assert.Nil(tt, err)
		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestHybridReplicationDeleteWorkflow_Success_PendingClusterPeer", func(tt *testing.T) {
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
		deleteReplicationActivity := replicationActivities.DeleteVolumeReplicationActivity{SE: mockStorage}
		hybridDeleteActivity := replicationActivities.HybridDeleteVolumeReplicationActivity{SE: mockStorage}
		env.SetHeader(mockHeader)

		// Register activities
		env.RegisterActivity(deleteReplicationActivity.SetHybridReplicationVariablesDelete)
		env.RegisterActivity(deleteReplicationActivity.UpdateReplicationInDBToErrorState)
		env.RegisterActivity(hybridDeleteActivity.CreateJobForHybridDeleteVolume)
		env.RegisterActivity(deleteReplicationActivity.GetSrcBasePathDelete)
		env.RegisterActivity(deleteReplicationActivity.GetDstBasePathDelete)
		env.RegisterActivity(deleteReplicationActivity.GetSignedSrcTokenDelete)
		env.RegisterActivity(deleteReplicationActivity.GetSignedDstTokenDelete)
		env.RegisterActivity(deleteReplicationActivity.GetReplicationOnDestinationForDelete)
		env.RegisterActivity(deleteReplicationActivity.DescribeRemoteJobForDelete)
		env.RegisterActivity(deleteReplicationActivity.UpdateReplicationRecordOnDestination)
		env.RegisterActivity(deleteReplicationActivity.DeleteVolumeOnDestination)
		env.RegisterActivity(deleteReplicationActivity.DeHydrateDestinationVolume)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.DeleteReplicationParams{
			AccountName:           "test-account",
			Region:                "us-central1",
			CorrelationId:         "test-correlation-id",
			VolumeResourceId:      "test-volume-id",
			ReplicationResourceId: "test-replication-id",
			Zone:                  "us-central1-a",
		}

		event := &replication.DeleteReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{
					HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
						Status: datamodel.HybridReplicationStatusPendingClusterPeer,
					},
					ReplicationAttributes: &datamodel.ReplicationDetails{
						SourceLocation:        "customer",
						DestinationLocation:   "us-central1",
						DestinationVolumeUUID: "dest-vol-uuid",
					},
				},
				SourceProjectNumber:      "123456789",
				DestinationProjectNumber: "987654321",
				VolumeResourceID:         "test-volume-id",
				ReplicationResourceID:    "test-replication-id",
				Location:                 "us-central1",
				Zone:                     "us-central1-a",
				AccountName:              "test-account",
			},
		}

		replicationResult := &replication.DeleteReplicationResult{
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
			Event:            event,
			DstReplication: &googleproxyclient.VolumeReplicationInternalV1beta{
				Name:             googleproxyclient.NewOptString("repl-123"),
				LastTransferSize: googleproxyclient.NewOptInt64(100),
				MirrorState:      googleproxyclient.NewOptVolumeReplicationInternalV1betaMirrorState(googleproxyclient.VolumeReplicationInternalV1betaMirrorStateUNINITIALIZED),
			},
			IsHybridReplicationVolume: true,
			IsSrcForHybridReplication: false,
			CleanupClusterPeering:     false,
		}

		deleteVolumeJob := &datamodel.Job{
			BaseModel: datamodel.BaseModel{
				UUID: "job-uuid-123",
			},
			Type:       string(datamodel.JobTypeHybridReplicationDeleteVolume),
			WorkflowID: "child-workflow-id",
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SetHybridReplicationVariablesDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetSrcBasePathDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetDstBasePathDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetSignedSrcTokenDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetSignedDstTokenDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetReplicationOnDestinationForDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("UpdateReplicationRecordOnDestination", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("DescribeRemoteJobForDelete", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateJobForHybridDeleteVolume", mock.Anything, mock.Anything, mock.Anything).Return(deleteVolumeJob, nil)

		// Register child workflow
		env.RegisterWorkflow(HybridDeleteDestinationVolumeWorkflow)
		env.OnWorkflow(HybridDeleteDestinationVolumeWorkflow, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(HybridReplicationDeleteWorkflow, params, event)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		assert.Nil(tt, err)
		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestHybridReplicationDeleteWorkflow_Success_PendingSVMPeer", func(tt *testing.T) {
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
		deleteReplicationActivity := replicationActivities.DeleteVolumeReplicationActivity{SE: mockStorage}
		hybridDeleteActivity := replicationActivities.HybridDeleteVolumeReplicationActivity{SE: mockStorage}
		env.SetHeader(mockHeader)

		// Register activities
		env.RegisterActivity(deleteReplicationActivity.SetHybridReplicationVariablesDelete)
		env.RegisterActivity(deleteReplicationActivity.UpdateReplicationInDBToErrorState)
		env.RegisterActivity(hybridDeleteActivity.CreateJobForHybridDeleteVolume)
		env.RegisterActivity(deleteReplicationActivity.GetSrcBasePathDelete)
		env.RegisterActivity(deleteReplicationActivity.GetDstBasePathDelete)
		env.RegisterActivity(deleteReplicationActivity.GetSignedSrcTokenDelete)
		env.RegisterActivity(deleteReplicationActivity.GetSignedDstTokenDelete)
		env.RegisterActivity(deleteReplicationActivity.GetReplicationOnDestinationForDelete)
		env.RegisterActivity(deleteReplicationActivity.DescribeRemoteJobForDelete)
		env.RegisterActivity(deleteReplicationActivity.UpdateReplicationRecordOnDestination)
		env.RegisterActivity(deleteReplicationActivity.DeleteVolumeOnDestination)
		env.RegisterActivity(deleteReplicationActivity.DeHydrateDestinationVolume)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.DeleteReplicationParams{
			AccountName:           "test-account",
			Region:                "us-central1",
			CorrelationId:         "test-correlation-id",
			VolumeResourceId:      "test-volume-id",
			ReplicationResourceId: "test-replication-id",
			Zone:                  "us-central1-a",
		}

		event := &replication.DeleteReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{
					HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
						Status: datamodel.HybridReplicationStatusPendingSVMPeer,
					},
					ReplicationAttributes: &datamodel.ReplicationDetails{
						SourceLocation:        "customer",
						DestinationLocation:   "us-central1",
						DestinationVolumeUUID: "dest-vol-uuid",
					},
				},
				SourceProjectNumber:      "123456789",
				DestinationProjectNumber: "987654321",
				VolumeResourceID:         "test-volume-id",
				ReplicationResourceID:    "test-replication-id",
				Location:                 "us-central1",
				Zone:                     "us-central1-a",
				AccountName:              "test-account",
			},
		}

		replicationResult := &replication.DeleteReplicationResult{
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
			Event:            event,
			DstReplication: &googleproxyclient.VolumeReplicationInternalV1beta{
				Name:             googleproxyclient.NewOptString("repl-123"),
				LastTransferSize: googleproxyclient.NewOptInt64(100),
				MirrorState:      googleproxyclient.NewOptVolumeReplicationInternalV1betaMirrorState(googleproxyclient.VolumeReplicationInternalV1betaMirrorStateUNINITIALIZED),
			},
			IsHybridReplicationVolume: true,
			IsSrcForHybridReplication: false,
			CleanupClusterPeering:     false,
		}

		deleteVolumeJob := &datamodel.Job{
			BaseModel: datamodel.BaseModel{
				UUID: "job-uuid-123",
			},
			Type:       string(datamodel.JobTypeHybridReplicationDeleteVolume),
			WorkflowID: "child-workflow-id",
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SetHybridReplicationVariablesDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetSrcBasePathDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetDstBasePathDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetSignedSrcTokenDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetSignedDstTokenDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetReplicationOnDestinationForDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("UpdateReplicationRecordOnDestination", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("DescribeRemoteJobForDelete", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateJobForHybridDeleteVolume", mock.Anything, mock.Anything, mock.Anything).Return(deleteVolumeJob, nil)

		// Register child workflow
		env.RegisterWorkflow(HybridDeleteDestinationVolumeWorkflow)
		env.OnWorkflow(HybridDeleteDestinationVolumeWorkflow, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(HybridReplicationDeleteWorkflow, params, event)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		assert.Nil(tt, err)
		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestHybridReplicationDeleteWorkflow_Success_UninitializedMirrorState", func(tt *testing.T) {
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
		deleteReplicationActivity := replicationActivities.DeleteVolumeReplicationActivity{SE: mockStorage}
		hybridDeleteActivity := replicationActivities.HybridDeleteVolumeReplicationActivity{SE: mockStorage}
		env.SetHeader(mockHeader)

		// Register activities
		env.RegisterActivity(deleteReplicationActivity.SetHybridReplicationVariablesDelete)
		env.RegisterActivity(deleteReplicationActivity.UpdateReplicationInDBToErrorState)
		env.RegisterActivity(hybridDeleteActivity.CreateJobForHybridDeleteVolume)
		env.RegisterActivity(deleteReplicationActivity.GetSrcBasePathDelete)
		env.RegisterActivity(deleteReplicationActivity.GetDstBasePathDelete)
		env.RegisterActivity(deleteReplicationActivity.GetSignedSrcTokenDelete)
		env.RegisterActivity(deleteReplicationActivity.GetSignedDstTokenDelete)
		env.RegisterActivity(deleteReplicationActivity.GetReplicationOnDestinationForDelete)
		env.RegisterActivity(deleteReplicationActivity.DeleteReplicationOnDestination)
		env.RegisterActivity(deleteReplicationActivity.DescribeRemoteJobForDelete)
		env.RegisterActivity(deleteReplicationActivity.UpdateReplicationRecordOnDestination)
		env.RegisterActivity(deleteReplicationActivity.DeleteSnapmirrorSnapshotsOnDestination)
		env.RegisterActivity(deleteReplicationActivity.DeleteVolumeOnDestination)
		env.RegisterActivity(deleteReplicationActivity.DeHydrateDestinationVolume)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.DeleteReplicationParams{
			AccountName:           "test-account",
			Region:                "us-central1",
			CorrelationId:         "test-correlation-id",
			VolumeResourceId:      "test-volume-id",
			ReplicationResourceId: "test-replication-id",
			Zone:                  "us-central1-a",
		}

		event := &replication.DeleteReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{
					HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
						Status: datamodel.HybridReplicationStatusPeered,
					},
					ReplicationAttributes: &datamodel.ReplicationDetails{
						SourceLocation:        "customer",
						DestinationLocation:   "us-central1",
						DestinationVolumeUUID: "dest-vol-uuid",
					},
				},
				SourceProjectNumber:      "123456789",
				DestinationProjectNumber: "987654321",
				VolumeResourceID:         "test-volume-id",
				ReplicationResourceID:    "test-replication-id",
				Location:                 "us-central1",
				Zone:                     "us-central1-a",
				AccountName:              "test-account",
			},
		}

		replicationResult := &replication.DeleteReplicationResult{
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
			Event:            event,
			DstReplication: &googleproxyclient.VolumeReplicationInternalV1beta{
				Name:             googleproxyclient.NewOptString("repl-123"),
				LastTransferSize: googleproxyclient.NewOptInt64(100),
				MirrorState:      googleproxyclient.NewOptVolumeReplicationInternalV1betaMirrorState(googleproxyclient.VolumeReplicationInternalV1betaMirrorStateUNINITIALIZED),
			},
			IsHybridReplicationVolume: true,
			IsSrcForHybridReplication: false,
			CleanupClusterPeering:     false,
		}

		deleteVolumeJob := &datamodel.Job{
			BaseModel: datamodel.BaseModel{
				UUID: "job-uuid-123",
			},
			Type:       string(datamodel.JobTypeHybridReplicationDeleteVolume),
			WorkflowID: "child-workflow-id",
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SetHybridReplicationVariablesDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetSrcBasePathDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetDstBasePathDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetSignedSrcTokenDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetSignedDstTokenDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetReplicationOnDestinationForDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("DeleteReplicationOnDestination", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("DescribeRemoteJobForDelete", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteSnapmirrorSnapshotsOnDestination", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("UpdateReplicationRecordOnDestination", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("CreateJobForHybridDeleteVolume", mock.Anything, mock.Anything, mock.Anything).Return(deleteVolumeJob, nil)

		// Register child workflow
		env.RegisterWorkflow(HybridDeleteDestinationVolumeWorkflow)
		env.OnWorkflow(HybridDeleteDestinationVolumeWorkflow, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(HybridReplicationDeleteWorkflow, params, event)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		assert.Nil(tt, err)
		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestHybridReplicationDeleteWorkflow_Failure_UpdateJobStatus", func(tt *testing.T) {
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

		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.DeleteReplicationParams{
			AccountName:           "test-account",
			Region:                "us-central1",
			CorrelationId:         "test-correlation-id",
			VolumeResourceId:      "test-volume-id",
			ReplicationResourceId: "test-replication-id",
			Zone:                  "us-central1-a",
		}

		event := &replication.DeleteReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel:         &datamodel.VolumeReplication{},
				SourceProjectNumber:      "123456789",
				DestinationProjectNumber: "987654321",
				VolumeResourceID:         "test-volume-id",
				ReplicationResourceID:    "test-replication-id",
				Location:                 "us-central1",
				Zone:                     "us-central1-a",
				AccountName:              "test-account",
			},
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(assert.AnError)

		env.ExecuteWorkflow(HybridReplicationDeleteWorkflow, params, event)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("TestHybridReplicationDeleteWorkflow_Error_SetHybridReplicationVariablesDelete", func(tt *testing.T) {
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
		deleteReplicationActivity := replicationActivities.DeleteVolumeReplicationActivity{SE: mockStorage}
		env.SetHeader(mockHeader)

		env.RegisterActivity(deleteReplicationActivity.SetHybridReplicationVariablesDelete)
		env.RegisterActivity(deleteReplicationActivity.UpdateReplicationInDBToErrorState)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.DeleteReplicationParams{
			AccountName:           "test-account",
			Region:                "us-central1",
			CorrelationId:         "test-correlation-id",
			VolumeResourceId:      "test-volume-id",
			ReplicationResourceId: "test-replication-id",
			Zone:                  "us-central1-a",
		}

		event := &replication.DeleteReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel:         &datamodel.VolumeReplication{},
				SourceProjectNumber:      "123456789",
				DestinationProjectNumber: "987654321",
				VolumeResourceID:         "test-volume-id",
				ReplicationResourceID:    "test-replication-id",
				Location:                 "us-central1",
				Zone:                     "us-central1-a",
				AccountName:              "test-account",
			},
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SetHybridReplicationVariablesDelete", mock.Anything, mock.Anything).Return(nil, assert.AnError)
		env.OnActivity("UpdateReplicationInDBToErrorState", mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(HybridReplicationDeleteWorkflow, params, event)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestHybridReplicationDeleteWorkflow_Error_GetSrcBasePathDelete", func(tt *testing.T) {
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
		deleteReplicationActivity := replicationActivities.DeleteVolumeReplicationActivity{SE: mockStorage}
		env.SetHeader(mockHeader)

		env.RegisterActivity(deleteReplicationActivity.SetHybridReplicationVariablesDelete)
		env.RegisterActivity(deleteReplicationActivity.UpdateReplicationInDBToErrorState)
		env.RegisterActivity(deleteReplicationActivity.GetSrcBasePathDelete)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.DeleteReplicationParams{
			AccountName:           "test-account",
			Region:                "us-central1",
			CorrelationId:         "test-correlation-id",
			VolumeResourceId:      "test-volume-id",
			ReplicationResourceId: "test-replication-id",
			Zone:                  "us-central1-a",
		}

		event := &replication.DeleteReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel:         &datamodel.VolumeReplication{},
				SourceProjectNumber:      "123456789",
				DestinationProjectNumber: "987654321",
				VolumeResourceID:         "test-volume-id",
				ReplicationResourceID:    "test-replication-id",
				Location:                 "us-central1",
				Zone:                     "us-central1-a",
				AccountName:              "test-account",
			},
		}

		replicationResult := &replication.DeleteReplicationResult{
			SrcProjectNumber:          &event.SourceProjectNumber,
			DstProjectNumber:          &event.DestinationProjectNumber,
			Event:                     event,
			IsHybridReplicationVolume: true,
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SetHybridReplicationVariablesDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetSrcBasePathDelete", mock.Anything, mock.Anything).Return(nil, assert.AnError)
		env.OnActivity("UpdateReplicationInDBToErrorState", mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(HybridReplicationDeleteWorkflow, params, event)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestHybridReplicationDeleteWorkflow_Error_DeleteReplicationOnDestination", func(tt *testing.T) {
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
		deleteReplicationActivity := replicationActivities.DeleteVolumeReplicationActivity{SE: mockStorage}
		env.SetHeader(mockHeader)

		env.RegisterActivity(deleteReplicationActivity.SetHybridReplicationVariablesDelete)
		env.RegisterActivity(deleteReplicationActivity.UpdateReplicationInDBToErrorState)
		env.RegisterActivity(deleteReplicationActivity.GetSrcBasePathDelete)
		env.RegisterActivity(deleteReplicationActivity.GetDstBasePathDelete)
		env.RegisterActivity(deleteReplicationActivity.GetSignedSrcTokenDelete)
		env.RegisterActivity(deleteReplicationActivity.GetSignedDstTokenDelete)
		env.RegisterActivity(deleteReplicationActivity.GetReplicationOnDestinationForDelete)
		env.RegisterActivity(deleteReplicationActivity.DeleteReplicationOnDestination)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.DeleteReplicationParams{
			AccountName:           "test-account",
			Region:                "us-central1",
			CorrelationId:         "test-correlation-id",
			VolumeResourceId:      "test-volume-id",
			ReplicationResourceId: "test-replication-id",
			Zone:                  "us-central1-a",
		}

		event := &replication.DeleteReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{
					HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{},
					ReplicationAttributes: &datamodel.ReplicationDetails{
						SourceLocation:      "customer",
						DestinationLocation: "us-central1",
					},
				},
				SourceProjectNumber:      "123456789",
				DestinationProjectNumber: "987654321",
				VolumeResourceID:         "test-volume-id",
				ReplicationResourceID:    "test-replication-id",
				Location:                 "us-central1",
				Zone:                     "us-central1-a",
				AccountName:              "test-account",
			},
		}

		replicationResult := &replication.DeleteReplicationResult{
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
			Event:            event,
			DstReplication: &googleproxyclient.VolumeReplicationInternalV1beta{
				Name:             googleproxyclient.NewOptString("repl-123"),
				LastTransferSize: googleproxyclient.NewOptInt64(100),
				MirrorState:      googleproxyclient.NewOptVolumeReplicationInternalV1betaMirrorState(googleproxyclient.VolumeReplicationInternalV1betaMirrorStateSTOPPED),
			},
			IsHybridReplicationVolume: true,
			IsSrcForHybridReplication: false,
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SetHybridReplicationVariablesDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetSrcBasePathDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetDstBasePathDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetSignedSrcTokenDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetSignedDstTokenDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetReplicationOnDestinationForDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("DeleteReplicationOnDestination", mock.Anything, mock.Anything).Return(nil, assert.AnError)
		env.OnActivity("UpdateReplicationInDBToErrorState", mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(HybridReplicationDeleteWorkflow, params, event)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestHybridReplicationDeleteWorkflow_Error_ReleaseReplicationOnSrc", func(tt *testing.T) {
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
		deleteReplicationActivity := replicationActivities.DeleteVolumeReplicationActivity{SE: mockStorage}
		env.SetHeader(mockHeader)

		env.RegisterActivity(deleteReplicationActivity.SetHybridReplicationVariablesDelete)
		env.RegisterActivity(deleteReplicationActivity.UpdateReplicationInDBToErrorState)
		env.RegisterActivity(deleteReplicationActivity.GetSrcBasePathDelete)
		env.RegisterActivity(deleteReplicationActivity.GetDstBasePathDelete)
		env.RegisterActivity(deleteReplicationActivity.GetSignedSrcTokenDelete)
		env.RegisterActivity(deleteReplicationActivity.GetSignedDstTokenDelete)
		env.RegisterActivity(deleteReplicationActivity.ReleaseReplicationOnSrc)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.DeleteReplicationParams{
			AccountName:           "test-account",
			Region:                "us-central1",
			CorrelationId:         "test-correlation-id",
			VolumeResourceId:      "test-volume-id",
			ReplicationResourceId: "test-replication-id",
			Zone:                  "us-central1-a",
		}

		migrationType := string(datamodel.HybridReplicationParametersReplicationTypeREVERSE)
		event := &replication.DeleteReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{
					HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
						HybridReplicationType: &migrationType,
					},
					ReplicationAttributes: &datamodel.ReplicationDetails{
						SourceLocation:      "us-central1",
						DestinationLocation: "customer",
					},
					Volume: &datamodel.Volume{
						Pool: &datamodel.Pool{
							BaseModel: datamodel.BaseModel{
								ID: 1,
							},
							DeploymentName: "test-deployment",
							PoolCredentials: &datamodel.PoolCredentials{
								SecretID: "test-secret-id",
							},
						},
					},
				},
				SourceProjectNumber:      "123456789",
				DestinationProjectNumber: "987654321",
				VolumeResourceID:         "test-volume-id",
				ReplicationResourceID:    "test-replication-id",
				Location:                 "us-central1",
				Zone:                     "us-central1-a",
				AccountName:              "test-account",
			},
		}

		replicationResult := &replication.DeleteReplicationResult{
			SrcProjectNumber:          &event.SourceProjectNumber,
			DstProjectNumber:          &event.DestinationProjectNumber,
			Event:                     event,
			IsHybridReplicationVolume: true,
			IsSrcForHybridReplication: true,
		}

		dbNodes := []*datamodel.Node{
			{
				Name: "test-node",
			},
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SetHybridReplicationVariablesDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetSrcBasePathDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetDstBasePathDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetSignedSrcTokenDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetSignedDstTokenDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return(dbNodes, nil)
		env.OnActivity("ReleaseReplicationOnSrc", mock.Anything, mock.Anything, mock.Anything).Return(nil, assert.AnError)
		env.OnActivity("UpdateReplicationInDBToErrorState", mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(HybridReplicationDeleteWorkflow, params, event)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})
}

func TestHybridDeleteDestinationVolumeWorkflow(t *testing.T) {
	t.Run("TestHybridDeleteDestinationVolumeWorkflow_Success", func(tt *testing.T) {
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
		deleteReplicationActivity := replicationActivities.DeleteVolumeReplicationActivity{SE: mockStorage}
		env.SetHeader(mockHeader)

		env.RegisterActivity(deleteReplicationActivity.DeleteVolumeOnDestination)
		env.RegisterActivity(deleteReplicationActivity.DescribeRemoteJobForDelete)
		env.RegisterActivity(deleteReplicationActivity.DeHydrateDestinationVolume)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		srcProjectNumber := "123456789"
		dstProjectNumber := "987654321"

		replicationResult := replication.DeleteReplicationResult{
			SrcProjectNumber: &srcProjectNumber,
			DstProjectNumber: &dstProjectNumber,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:   "us-central1",
							DestinationVolumeUUID: "dest-vol-uuid",
						},
					},
				},
			},
			DstReplication: &googleproxyclient.VolumeReplicationInternalV1beta{
				Name:             googleproxyclient.NewOptString("repl-123"),
				LastTransferSize: googleproxyclient.NewOptInt64(100),
				MirrorState:      googleproxyclient.NewOptVolumeReplicationInternalV1betaMirrorState(googleproxyclient.VolumeReplicationInternalV1betaMirrorStateUNINITIALIZED),
			},
			IsHybridReplicationVolume: true,
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteVolumeOnDestination", mock.Anything, mock.Anything).Return(&replicationResult, nil)
		env.OnActivity("DescribeRemoteJobForDelete", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeHydrateDestinationVolume", mock.Anything, mock.Anything).Return(&replicationResult, nil)

		env.ExecuteWorkflow(HybridDeleteDestinationVolumeWorkflow, replicationResult)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		assert.Nil(tt, err)
		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestHybridDeleteDestinationVolumeWorkflow_Failure_UpdateJobStatus", func(tt *testing.T) {
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

		env.RegisterActivity(commonActivity.UpdateJobStatus)

		srcProjectNumber := "123456789"
		dstProjectNumber := "987654321"

		replicationResult := replication.DeleteReplicationResult{
			SrcProjectNumber: &srcProjectNumber,
			DstProjectNumber: &dstProjectNumber,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{},
				},
			},
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(assert.AnError)

		env.ExecuteWorkflow(HybridDeleteDestinationVolumeWorkflow, replicationResult)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("TestHybridDeleteDestinationVolumeWorkflow_Error_DeleteVolumeOnDestination", func(tt *testing.T) {
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
		deleteReplicationActivity := replicationActivities.DeleteVolumeReplicationActivity{SE: mockStorage}
		env.SetHeader(mockHeader)

		env.RegisterActivity(deleteReplicationActivity.DeleteVolumeOnDestination)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		srcProjectNumber := "123456789"
		dstProjectNumber := "987654321"

		replicationResult := replication.DeleteReplicationResult{
			SrcProjectNumber: &srcProjectNumber,
			DstProjectNumber: &dstProjectNumber,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{},
				},
			},
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteVolumeOnDestination", mock.Anything, mock.Anything).Return(nil, assert.AnError)

		env.ExecuteWorkflow(HybridDeleteDestinationVolumeWorkflow, replicationResult)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("TestHybridDeleteDestinationVolumeWorkflow_Error_DescribeRemoteJobForDelete", func(tt *testing.T) {
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
		deleteReplicationActivity := replicationActivities.DeleteVolumeReplicationActivity{SE: mockStorage}
		env.SetHeader(mockHeader)

		env.RegisterActivity(deleteReplicationActivity.DeleteVolumeOnDestination)
		env.RegisterActivity(deleteReplicationActivity.DescribeRemoteJobForDelete)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		srcProjectNumber := "123456789"
		dstProjectNumber := "987654321"

		replicationResult := replication.DeleteReplicationResult{
			SrcProjectNumber: &srcProjectNumber,
			DstProjectNumber: &dstProjectNumber,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{},
				},
			},
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteVolumeOnDestination", mock.Anything, mock.Anything).Return(&replicationResult, nil)
		env.OnActivity("DescribeRemoteJobForDelete", mock.Anything, mock.Anything).Return(assert.AnError)

		env.ExecuteWorkflow(HybridDeleteDestinationVolumeWorkflow, replicationResult)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("TestHybridDeleteDestinationVolumeWorkflow_Error_DeHydrateDestinationVolume", func(tt *testing.T) {
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
		deleteReplicationActivity := replicationActivities.DeleteVolumeReplicationActivity{SE: mockStorage}
		env.SetHeader(mockHeader)

		env.RegisterActivity(deleteReplicationActivity.DeleteVolumeOnDestination)
		env.RegisterActivity(deleteReplicationActivity.DescribeRemoteJobForDelete)
		env.RegisterActivity(deleteReplicationActivity.DeHydrateDestinationVolume)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		srcProjectNumber := "123456789"
		dstProjectNumber := "987654321"

		replicationResult := replication.DeleteReplicationResult{
			SrcProjectNumber: &srcProjectNumber,
			DstProjectNumber: &dstProjectNumber,
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: &datamodel.VolumeReplication{},
				},
			},
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteVolumeOnDestination", mock.Anything, mock.Anything).Return(&replicationResult, nil)
		env.OnActivity("DescribeRemoteJobForDelete", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeHydrateDestinationVolume", mock.Anything, mock.Anything).Return(nil, assert.AnError)

		env.ExecuteWorkflow(HybridDeleteDestinationVolumeWorkflow, replicationResult)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})
}
