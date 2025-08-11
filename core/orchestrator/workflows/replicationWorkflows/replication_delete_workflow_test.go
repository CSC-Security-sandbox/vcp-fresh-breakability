package replicationWorkflows

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
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
		resumeReplicationActivity := replicationActivities.DeleteVolumeReplicationActivity{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(resumeReplicationActivity.GetSrcBasePathDelete)
		env.RegisterActivity(resumeReplicationActivity.GetDstBasePathDelete)
		env.RegisterActivity(resumeReplicationActivity.GetSignedSrcTokenDelete)
		env.RegisterActivity(resumeReplicationActivity.GetSignedDstTokenDelete)
		env.RegisterActivity(resumeReplicationActivity.GetReplicationOnDestinationForDelete)
		env.RegisterActivity(resumeReplicationActivity.DeleteReplicationOnDestination)
		env.RegisterActivity(resumeReplicationActivity.DescribeRemoteJobForDelete)
		env.RegisterActivity(resumeReplicationActivity.DescribeSourceJobForDelete)
		env.RegisterActivity(resumeReplicationActivity.ReleaseReplicationOnSource)
		env.RegisterActivity(resumeReplicationActivity.DeleteSnapmirrorSnapshotsOnDestination)
		env.RegisterActivity(resumeReplicationActivity.DeleteSnapmirrorSnapshotsOnSource)
		env.RegisterActivity(resumeReplicationActivity.DeHydrateDestinationVolumeReplication)
		env.RegisterActivity(resumeReplicationActivity.DeleteVolumeOnDestination)
		env.RegisterActivity(resumeReplicationActivity.DeHydrateDestinationVolume)

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
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSrcBasePathDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetDstBasePathDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetSignedSrcTokenDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetSignedDstTokenDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetReplicationOnDestinationForDelete", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("DeleteReplicationOnDestination", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("DescribeRemoteJobForDelete", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ReleaseReplicationOnSource", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("DescribeSourceJobForDelete", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteSnapmirrorSnapshotsOnDestination", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("DeleteSnapmirrorSnapshotsOnSource", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("DeHydrateDestinationVolumeReplication", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("DeleteVolumeOnDestination", mock.Anything, mock.Anything).Return(replicationResult, nil)
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
		resumeReplicationActivity := replicationActivities.DeleteVolumeReplicationActivity{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(resumeReplicationActivity.GetSrcBasePathDelete)
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
}
