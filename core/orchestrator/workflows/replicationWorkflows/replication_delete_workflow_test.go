package replicationWorkflows

import (
	"testing"

	"github.com/go-openapi/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
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

func TestReplicationDeleteWorkflow(t *testing.T) {
	t.Run("TestReplicationDeleteWorkflow_Success", func(tt *testing.T) {
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
		env.RegisterActivity(deleteReplicationActivity.GetSrcBasePathDelete)
		env.RegisterActivity(deleteReplicationActivity.GetDstBasePathDelete)
		env.RegisterActivity(deleteReplicationActivity.GetSignedSrcTokenDelete)
		env.RegisterActivity(deleteReplicationActivity.GetSignedDstTokenDelete)
		env.RegisterActivity(deleteReplicationActivity.GetReplicationOnDestinationForDelete)
		env.RegisterActivity(deleteReplicationActivity.DeleteReplicationOnDestination)
		env.RegisterActivity(deleteReplicationActivity.DescribeRemoteJobForDelete)
		env.RegisterActivity(deleteReplicationActivity.DescribeSourceJobForDelete)
		env.RegisterActivity(deleteReplicationActivity.UpdateReplicationRecordOnSource)
		env.RegisterActivity(deleteReplicationActivity.UpdateReplicationRecordOnDestination)
		env.RegisterActivity(deleteReplicationActivity.DeleteSnapmirrorSnapshotsOnDestination)
		env.RegisterActivity(deleteReplicationActivity.DeleteSnapmirrorSnapshotsOnSource)
		env.RegisterActivity(deleteReplicationActivity.DeHydrateDestinationVolumeReplication)
		env.RegisterActivity(deleteReplicationActivity.DeleteVolumeOnDestination)
		env.RegisterActivity(deleteReplicationActivity.DeHydrateDestinationVolume)
		env.RegisterActivity(deleteReplicationActivity.UpdateReplicationOnDestinationToErrorState)

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
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
			Event:            event,
			DstReplication: &googleproxyclient.VolumeReplicationInternalV1beta{
				Name:             googleproxyclient.NewOptString("repl-123"),
				LastTransferSize: googleproxyclient.NewOptInt64(100),
				MirrorState:      googleproxyclient.NewOptVolumeReplicationInternalV1betaMirrorState(googleproxyclient.VolumeReplicationInternalV1betaMirrorStateUNINITIALIZED),
			},
			IsHybridReplicationVolume: false,
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
		env.OnActivity("DescribeRemoteJobForDelete", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteSnapmirrorSnapshotsOnSource", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("DescribeSourceJobForDelete", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeHydrateDestinationVolumeReplication", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("UpdateReplicationRecordOnSource", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("DescribeSourceJobForDelete", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateReplicationRecordOnDestination", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("DescribeRemoteJobForDelete", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteVolumeOnDestination", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("DescribeRemoteJobForDelete", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeHydrateDestinationVolume", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.ExecuteWorkflow(ReplicationDeleteWorkflow, params, event)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		assert.Nil(tt, err)
		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestReplicationDeleteWorkflow_Failure", func(tt *testing.T) {
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

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(assert.AnError)

		env.ExecuteWorkflow(ReplicationDeleteWorkflow, params, event)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("TestReplicationDeleteWorkflow_SuccessWhenHybridReplication", func(tt *testing.T) {
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
		env.RegisterActivity(deleteReplicationActivity.GetSrcBasePathDelete)
		env.RegisterActivity(deleteReplicationActivity.GetDstBasePathDelete)
		env.RegisterActivity(deleteReplicationActivity.GetSignedSrcTokenDelete)
		env.RegisterActivity(deleteReplicationActivity.GetSignedDstTokenDelete)
		env.RegisterActivity(deleteReplicationActivity.GetReplicationOnDestinationForDelete)
		env.RegisterActivity(deleteReplicationActivity.DeleteReplicationOnDestination)
		env.RegisterActivity(deleteReplicationActivity.DescribeRemoteJobForDelete)
		env.RegisterActivity(deleteReplicationActivity.DescribeSourceJobForDelete)
		env.RegisterActivity(deleteReplicationActivity.UpdateReplicationRecordOnSource)
		env.RegisterActivity(deleteReplicationActivity.UpdateReplicationRecordOnDestination)
		env.RegisterActivity(deleteReplicationActivity.DeleteSnapmirrorSnapshotsOnDestination)
		env.RegisterActivity(deleteReplicationActivity.DeleteSnapmirrorSnapshotsOnSource)
		env.RegisterActivity(deleteReplicationActivity.DeHydrateDestinationVolumeReplication)
		env.RegisterActivity(deleteReplicationActivity.DeleteVolumeOnDestination)
		env.RegisterActivity(deleteReplicationActivity.DeHydrateDestinationVolume)
		env.RegisterActivity(deleteReplicationActivity.UpdateReplicationOnDestinationToErrorState)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(deleteReplicationActivity.DeleteClusterPeeringInOntap)
		env.RegisterActivity(deleteReplicationActivity.DeleteClusterPeeringDB)
		env.RegisterActivity(deleteReplicationActivity.DeleteRoleInOntap)

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
					ReplicationAttributes: &datamodel.ReplicationDetails{
						SourceLocation: "customer",
					},
					HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{},
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
				MirrorState:      googleproxyclient.NewOptVolumeReplicationInternalV1betaMirrorState(googleproxyclient.VolumeReplicationInternalV1betaMirrorStateUNINITIALIZED),
			},
			IsHybridReplicationVolume: true,
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
		env.OnActivity("DescribeRemoteJobForDelete", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeHydrateDestinationVolumeReplication", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("UpdateReplicationRecordOnDestination", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("DescribeRemoteJobForDelete", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteVolumeOnDestination", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("DescribeRemoteJobForDelete", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeHydrateDestinationVolume", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return(dbNodes, nil)
		env.OnActivity("DeleteClusterPeeringInOntap", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteClusterPeeringDB", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteRoleInOntap", mock.Anything, mock.Anything).Return(nil)
		env.ExecuteWorkflow(ReplicationDeleteWorkflow, params, event)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		assert.Nil(tt, err)
		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestReplicationDeleteWorkflow_ErrorSetHybridReplicationVariablesDelete", func(tt *testing.T) {
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
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
			Event:            event,
			DstReplication: &googleproxyclient.VolumeReplicationInternalV1beta{
				Name:             googleproxyclient.NewOptString("repl-123"),
				LastTransferSize: googleproxyclient.NewOptInt64(100),
				MirrorState:      googleproxyclient.NewOptVolumeReplicationInternalV1betaMirrorState(googleproxyclient.VolumeReplicationInternalV1betaMirrorStateUNINITIALIZED),
			},
			IsHybridReplicationVolume: false,
		}
		mockError := errors.New(500, "Internal Server Error")
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SetHybridReplicationVariablesDelete", mock.Anything, mock.Anything).Return(replicationResult, mockError)
		env.ExecuteWorkflow(ReplicationDeleteWorkflow, params, event)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestReplicationDeleteWorkflow_ErrorDeleteSnapmirrorSnapshotsOnSource", func(tt *testing.T) {
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
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(deleteReplicationActivity.GetSrcBasePathDelete)
		env.RegisterActivity(deleteReplicationActivity.GetDstBasePathDelete)
		env.RegisterActivity(deleteReplicationActivity.GetSignedSrcTokenDelete)
		env.RegisterActivity(deleteReplicationActivity.GetSignedDstTokenDelete)
		env.RegisterActivity(deleteReplicationActivity.GetReplicationOnDestinationForDelete)
		env.RegisterActivity(deleteReplicationActivity.DeleteReplicationOnDestination)
		env.RegisterActivity(deleteReplicationActivity.DescribeRemoteJobForDelete)
		env.RegisterActivity(deleteReplicationActivity.DescribeSourceJobForDelete)
		env.RegisterActivity(deleteReplicationActivity.UpdateReplicationRecordOnSource)
		env.RegisterActivity(deleteReplicationActivity.UpdateReplicationRecordOnDestination)
		env.RegisterActivity(deleteReplicationActivity.DeleteSnapmirrorSnapshotsOnDestination)
		env.RegisterActivity(deleteReplicationActivity.DeleteSnapmirrorSnapshotsOnSource)
		env.RegisterActivity(deleteReplicationActivity.DeHydrateDestinationVolumeReplication)
		env.RegisterActivity(deleteReplicationActivity.DeleteVolumeOnDestination)
		env.RegisterActivity(deleteReplicationActivity.DeHydrateDestinationVolume)
		env.RegisterActivity(deleteReplicationActivity.UpdateReplicationOnDestinationToErrorState)
		env.RegisterActivity(deleteReplicationActivity.UpdateReplicationOnSourceToErrorState)

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
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
			Event:            event,
			DstReplication: &googleproxyclient.VolumeReplicationInternalV1beta{
				Name:             googleproxyclient.NewOptString("repl-123"),
				LastTransferSize: googleproxyclient.NewOptInt64(100),
				MirrorState:      googleproxyclient.NewOptVolumeReplicationInternalV1betaMirrorState(googleproxyclient.VolumeReplicationInternalV1betaMirrorStateUNINITIALIZED),
			},
			IsHybridReplicationVolume: false,
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
		env.OnActivity("DescribeRemoteJobForDelete", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteSnapmirrorSnapshotsOnSource", mock.Anything, mock.Anything).Return(nil, assert.AnError)
		env.OnActivity("UpdateReplicationOnDestinationToErrorState", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("UpdateReplicationOnSourceToErrorState", mock.Anything, mock.Anything).Return(replicationResult, nil)

		env.ExecuteWorkflow(ReplicationDeleteWorkflow, params, event)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestReplicationDeleteWorkflow_ErrorDescribeSourceJobForDelete", func(tt *testing.T) {
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
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(deleteReplicationActivity.GetSrcBasePathDelete)
		env.RegisterActivity(deleteReplicationActivity.GetDstBasePathDelete)
		env.RegisterActivity(deleteReplicationActivity.GetSignedSrcTokenDelete)
		env.RegisterActivity(deleteReplicationActivity.GetSignedDstTokenDelete)
		env.RegisterActivity(deleteReplicationActivity.GetReplicationOnDestinationForDelete)
		env.RegisterActivity(deleteReplicationActivity.DeleteReplicationOnDestination)
		env.RegisterActivity(deleteReplicationActivity.DescribeRemoteJobForDelete)
		env.RegisterActivity(deleteReplicationActivity.DescribeSourceJobForDelete)
		env.RegisterActivity(deleteReplicationActivity.UpdateReplicationRecordOnSource)
		env.RegisterActivity(deleteReplicationActivity.UpdateReplicationRecordOnDestination)
		env.RegisterActivity(deleteReplicationActivity.DeleteSnapmirrorSnapshotsOnDestination)
		env.RegisterActivity(deleteReplicationActivity.DeleteSnapmirrorSnapshotsOnSource)
		env.RegisterActivity(deleteReplicationActivity.DeHydrateDestinationVolumeReplication)
		env.RegisterActivity(deleteReplicationActivity.DeleteVolumeOnDestination)
		env.RegisterActivity(deleteReplicationActivity.DeHydrateDestinationVolume)
		env.RegisterActivity(deleteReplicationActivity.UpdateReplicationOnDestinationToErrorState)
		env.RegisterActivity(deleteReplicationActivity.UpdateReplicationOnSourceToErrorState)

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
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
			Event:            event,
			DstReplication: &googleproxyclient.VolumeReplicationInternalV1beta{
				Name:             googleproxyclient.NewOptString("repl-123"),
				LastTransferSize: googleproxyclient.NewOptInt64(100),
				MirrorState:      googleproxyclient.NewOptVolumeReplicationInternalV1betaMirrorState(googleproxyclient.VolumeReplicationInternalV1betaMirrorStateUNINITIALIZED),
			},
			IsHybridReplicationVolume: false,
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
		env.OnActivity("DescribeRemoteJobForDelete", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteSnapmirrorSnapshotsOnSource", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("DescribeSourceJobForDelete", mock.Anything, mock.Anything).Return(assert.AnError)
		env.OnActivity("UpdateReplicationOnDestinationToErrorState", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("UpdateReplicationOnSourceToErrorState", mock.Anything, mock.Anything).Return(replicationResult, nil)

		env.ExecuteWorkflow(ReplicationDeleteWorkflow, params, event)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestReplicationDeleteWorkflow_SuccessWhenIsSrcForHybridReplication", func(tt *testing.T) {
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
		env.RegisterActivity(deleteReplicationActivity.GetSrcBasePathDelete)
		env.RegisterActivity(deleteReplicationActivity.GetDstBasePathDelete)
		env.RegisterActivity(deleteReplicationActivity.GetSignedSrcTokenDelete)
		env.RegisterActivity(deleteReplicationActivity.GetSignedDstTokenDelete)
		env.RegisterActivity(deleteReplicationActivity.ReleaseReplicationOnSrc)
		env.RegisterActivity(deleteReplicationActivity.DeleteSnapmirrorSnapshotsOnSource)
		env.RegisterActivity(deleteReplicationActivity.DescribeSourceJobForDelete)
		env.RegisterActivity(deleteReplicationActivity.DeleteReplicationRecordOnSource)
		env.RegisterActivity(deleteReplicationActivity.UpdateRbacRole)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(deleteReplicationActivity.DeleteClusterPeeringInOntap)
		env.RegisterActivity(deleteReplicationActivity.DeleteClusterPeeringDB)
		env.RegisterActivity(deleteReplicationActivity.DeleteRoleInOntap)
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
				ReplicationModel: &datamodel.VolumeReplication{
					ReplicationAttributes: &datamodel.ReplicationDetails{
						SourceLocation: "customer",
					},
					HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{},
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
				MirrorState:      googleproxyclient.NewOptVolumeReplicationInternalV1betaMirrorState(googleproxyclient.VolumeReplicationInternalV1betaMirrorStateUNINITIALIZED),
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
		env.OnActivity("DescribeSourceJobForDelete", mock.Anything, mock.Anything).Return(nil).Times(2)
		env.OnActivity("DeleteReplicationRecordOnSource", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateRbacRole", mock.Anything, mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("DeleteClusterPeeringInOntap", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteClusterPeeringDB", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteRoleInOntap", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateReplicationInDBToErrorState", mock.Anything, mock.Anything).Return(nil)
		env.ExecuteWorkflow(ReplicationDeleteWorkflow, params, event)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		assert.Nil(tt, err)
		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestReplicationDeleteWorkflow_ErrorUpdateReplicationRecordOnSource", func(tt *testing.T) {
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
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(deleteReplicationActivity.GetSrcBasePathDelete)
		env.RegisterActivity(deleteReplicationActivity.GetDstBasePathDelete)
		env.RegisterActivity(deleteReplicationActivity.GetSignedSrcTokenDelete)
		env.RegisterActivity(deleteReplicationActivity.GetSignedDstTokenDelete)
		env.RegisterActivity(deleteReplicationActivity.GetReplicationOnDestinationForDelete)
		env.RegisterActivity(deleteReplicationActivity.DeleteReplicationOnDestination)
		env.RegisterActivity(deleteReplicationActivity.DescribeRemoteJobForDelete)
		env.RegisterActivity(deleteReplicationActivity.DescribeSourceJobForDelete)
		env.RegisterActivity(deleteReplicationActivity.UpdateReplicationRecordOnSource)
		env.RegisterActivity(deleteReplicationActivity.UpdateReplicationRecordOnDestination)
		env.RegisterActivity(deleteReplicationActivity.DeleteSnapmirrorSnapshotsOnDestination)
		env.RegisterActivity(deleteReplicationActivity.DeleteSnapmirrorSnapshotsOnSource)
		env.RegisterActivity(deleteReplicationActivity.DeHydrateDestinationVolumeReplication)
		env.RegisterActivity(deleteReplicationActivity.DeleteVolumeOnDestination)
		env.RegisterActivity(deleteReplicationActivity.DeHydrateDestinationVolume)
		env.RegisterActivity(deleteReplicationActivity.UpdateReplicationOnDestinationToErrorState)
		env.RegisterActivity(deleteReplicationActivity.UpdateReplicationOnSourceToErrorState)

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
					Name: "test-replication",
					ReplicationAttributes: &datamodel.ReplicationDetails{
						SourceLocation:        "us-central1",
						DestinationLocation:   "us-east1",
						SourceVolumeName:      "source-volume",
						DestinationVolumeName: "destination-volume",
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
			IsHybridReplicationVolume: false,
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
		env.OnActivity("DescribeRemoteJobForDelete", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteSnapmirrorSnapshotsOnSource", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("DescribeSourceJobForDelete", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeHydrateDestinationVolumeReplication", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("UpdateReplicationRecordOnSource", mock.Anything, mock.Anything).Return(nil, assert.AnError)
		env.OnActivity("UpdateReplicationOnDestinationToErrorState", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("UpdateReplicationOnSourceToErrorState", mock.Anything, mock.Anything).Return(replicationResult, nil)

		env.ExecuteWorkflow(ReplicationDeleteWorkflow, params, event)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestReplicationDeleteWorkflow_SuccessWhenHybridReplicationPendingClusterPeering", func(tt *testing.T) {
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
		env.RegisterActivity(deleteReplicationActivity.GetSrcBasePathDelete)
		env.RegisterActivity(deleteReplicationActivity.GetDstBasePathDelete)
		env.RegisterActivity(deleteReplicationActivity.GetSignedSrcTokenDelete)
		env.RegisterActivity(deleteReplicationActivity.GetSignedDstTokenDelete)
		env.RegisterActivity(deleteReplicationActivity.GetReplicationOnDestinationForDelete)
		env.RegisterActivity(deleteReplicationActivity.UpdateReplicationRecordOnDestination)
		env.RegisterActivity(deleteReplicationActivity.DescribeRemoteJobForDelete)
		env.RegisterActivity(deleteReplicationActivity.DeleteVolumeOnDestination)
		env.RegisterActivity(deleteReplicationActivity.DeHydrateDestinationVolume)
		env.RegisterActivity(deleteReplicationActivity.UpdateReplicationOnDestinationToErrorState)
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
					ReplicationAttributes: &datamodel.ReplicationDetails{
						SourceLocation: "us-central1",
					},
					HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
						Status: models.HybridReplicationStatusPendingClusterPeer,
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
		env.OnActivity("DeleteVolumeOnDestination", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("DescribeRemoteJobForDelete", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeHydrateDestinationVolume", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.ExecuteWorkflow(ReplicationDeleteWorkflow, params, event)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		assert.Nil(tt, err)
		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestReplicationDeleteWorkflow_ErrorDeleteReplicationOnDestination", func(tt *testing.T) {
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
		env.RegisterActivity(deleteReplicationActivity.GetSrcBasePathDelete)
		env.RegisterActivity(deleteReplicationActivity.GetDstBasePathDelete)
		env.RegisterActivity(deleteReplicationActivity.GetSignedSrcTokenDelete)
		env.RegisterActivity(deleteReplicationActivity.GetSignedDstTokenDelete)
		env.RegisterActivity(deleteReplicationActivity.GetReplicationOnDestinationForDelete)
		env.RegisterActivity(deleteReplicationActivity.DeleteReplicationOnDestination)
		env.RegisterActivity(deleteReplicationActivity.UpdateReplicationOnDestinationToErrorState)
		env.RegisterActivity(deleteReplicationActivity.UpdateReplicationOnSourceToErrorState)
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
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
			Event:            event,
			DstReplication: &googleproxyclient.VolumeReplicationInternalV1beta{
				Name:             googleproxyclient.NewOptString("repl-123"),
				LastTransferSize: googleproxyclient.NewOptInt64(100),
				MirrorState:      googleproxyclient.NewOptVolumeReplicationInternalV1betaMirrorState(googleproxyclient.VolumeReplicationInternalV1betaMirrorStateSTOPPED),
			},
			IsHybridReplicationVolume: false,
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SetHybridReplicationVariablesDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetSrcBasePathDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetDstBasePathDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetSignedSrcTokenDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetSignedDstTokenDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetReplicationOnDestinationForDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("DeleteReplicationOnDestination", mock.Anything, mock.Anything).Return(nil, assert.AnError)
		env.OnActivity("UpdateReplicationOnDestinationToErrorState", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("UpdateReplicationOnSourceToErrorState", mock.Anything, mock.Anything).Return(replicationResult, nil)

		env.ExecuteWorkflow(ReplicationDeleteWorkflow, params, event)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestReplicationDeleteWorkflow_ErrorDescribeRemoteJobForDeleteAfterDeleteReplication", func(tt *testing.T) {
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
		env.RegisterActivity(deleteReplicationActivity.GetSrcBasePathDelete)
		env.RegisterActivity(deleteReplicationActivity.GetDstBasePathDelete)
		env.RegisterActivity(deleteReplicationActivity.GetSignedSrcTokenDelete)
		env.RegisterActivity(deleteReplicationActivity.GetSignedDstTokenDelete)
		env.RegisterActivity(deleteReplicationActivity.GetReplicationOnDestinationForDelete)
		env.RegisterActivity(deleteReplicationActivity.DeleteReplicationOnDestination)
		env.RegisterActivity(deleteReplicationActivity.DescribeRemoteJobForDelete)
		env.RegisterActivity(deleteReplicationActivity.UpdateReplicationOnDestinationToErrorState)
		env.RegisterActivity(deleteReplicationActivity.UpdateReplicationOnSourceToErrorState)
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
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
			Event:            event,
			DstReplication: &googleproxyclient.VolumeReplicationInternalV1beta{
				Name:             googleproxyclient.NewOptString("repl-123"),
				LastTransferSize: googleproxyclient.NewOptInt64(100),
				MirrorState:      googleproxyclient.NewOptVolumeReplicationInternalV1betaMirrorState(googleproxyclient.VolumeReplicationInternalV1betaMirrorStateSTOPPED),
			},
			IsHybridReplicationVolume: false,
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SetHybridReplicationVariablesDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetSrcBasePathDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetDstBasePathDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetSignedSrcTokenDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetSignedDstTokenDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetReplicationOnDestinationForDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("DeleteReplicationOnDestination", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("DescribeRemoteJobForDelete", mock.Anything, mock.Anything).Return(assert.AnError)
		env.OnActivity("UpdateReplicationOnDestinationToErrorState", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("UpdateReplicationOnSourceToErrorState", mock.Anything, mock.Anything).Return(replicationResult, nil)

		env.ExecuteWorkflow(ReplicationDeleteWorkflow, params, event)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestReplicationDeleteWorkflow_ErrorDeleteSnapmirrorSnapshotsOnDestination", func(tt *testing.T) {
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
		env.RegisterActivity(deleteReplicationActivity.GetSrcBasePathDelete)
		env.RegisterActivity(deleteReplicationActivity.GetDstBasePathDelete)
		env.RegisterActivity(deleteReplicationActivity.GetSignedSrcTokenDelete)
		env.RegisterActivity(deleteReplicationActivity.GetSignedDstTokenDelete)
		env.RegisterActivity(deleteReplicationActivity.GetReplicationOnDestinationForDelete)
		env.RegisterActivity(deleteReplicationActivity.DeleteReplicationOnDestination)
		env.RegisterActivity(deleteReplicationActivity.DescribeRemoteJobForDelete)
		env.RegisterActivity(deleteReplicationActivity.DeleteSnapmirrorSnapshotsOnDestination)
		env.RegisterActivity(deleteReplicationActivity.UpdateReplicationOnDestinationToErrorState)
		env.RegisterActivity(deleteReplicationActivity.UpdateReplicationOnSourceToErrorState)
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
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
			Event:            event,
			DstReplication: &googleproxyclient.VolumeReplicationInternalV1beta{
				Name:             googleproxyclient.NewOptString("repl-123"),
				LastTransferSize: googleproxyclient.NewOptInt64(100),
				MirrorState:      googleproxyclient.NewOptVolumeReplicationInternalV1betaMirrorState(googleproxyclient.VolumeReplicationInternalV1betaMirrorStateSTOPPED),
			},
			IsHybridReplicationVolume: false,
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
		env.OnActivity("DeleteSnapmirrorSnapshotsOnDestination", mock.Anything, mock.Anything).Return(nil, assert.AnError)
		env.OnActivity("UpdateReplicationOnDestinationToErrorState", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("UpdateReplicationOnSourceToErrorState", mock.Anything, mock.Anything).Return(replicationResult, nil)

		env.ExecuteWorkflow(ReplicationDeleteWorkflow, params, event)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestReplicationDeleteWorkflow_ErrorDescribeRemoteJobForDeleteAfterDeleteSnapshots", func(tt *testing.T) {
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
		env.RegisterActivity(deleteReplicationActivity.GetSrcBasePathDelete)
		env.RegisterActivity(deleteReplicationActivity.GetDstBasePathDelete)
		env.RegisterActivity(deleteReplicationActivity.GetSignedSrcTokenDelete)
		env.RegisterActivity(deleteReplicationActivity.GetSignedDstTokenDelete)
		env.RegisterActivity(deleteReplicationActivity.GetReplicationOnDestinationForDelete)
		env.RegisterActivity(deleteReplicationActivity.DeleteReplicationOnDestination)
		env.RegisterActivity(deleteReplicationActivity.DescribeRemoteJobForDelete)
		env.RegisterActivity(deleteReplicationActivity.DeleteSnapmirrorSnapshotsOnDestination)
		env.RegisterActivity(deleteReplicationActivity.UpdateReplicationOnDestinationToErrorState)
		env.RegisterActivity(deleteReplicationActivity.UpdateReplicationOnSourceToErrorState)
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
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
			Event:            event,
			DstReplication: &googleproxyclient.VolumeReplicationInternalV1beta{
				Name:             googleproxyclient.NewOptString("repl-123"),
				LastTransferSize: googleproxyclient.NewOptInt64(100),
				MirrorState:      googleproxyclient.NewOptVolumeReplicationInternalV1betaMirrorState(googleproxyclient.VolumeReplicationInternalV1betaMirrorStateSTOPPED),
			},
			IsHybridReplicationVolume: false,
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SetHybridReplicationVariablesDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetSrcBasePathDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetDstBasePathDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetSignedSrcTokenDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetSignedDstTokenDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetReplicationOnDestinationForDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("DeleteReplicationOnDestination", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("DescribeRemoteJobForDelete", mock.Anything, mock.Anything).Return(nil).Once()
		env.OnActivity("DeleteSnapmirrorSnapshotsOnDestination", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("DescribeRemoteJobForDelete", mock.Anything, mock.Anything).Return(assert.AnError).Once()
		env.OnActivity("UpdateReplicationOnDestinationToErrorState", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("UpdateReplicationOnSourceToErrorState", mock.Anything, mock.Anything).Return(replicationResult, nil)

		env.ExecuteWorkflow(ReplicationDeleteWorkflow, params, event)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})
}
