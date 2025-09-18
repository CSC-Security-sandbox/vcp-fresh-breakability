package replicationWorkflows

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
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

func TestReplicationCleanupWorkflow(t *testing.T) {
	t.Run("TestReplicationCleanupWorkflow", func(tt *testing.T) {
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
		resumeReplicationActivity := replicationActivities.CleanupVolumeReplicationActivity{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(resumeReplicationActivity.GetSrcBasePathCleanup)
		env.RegisterActivity(resumeReplicationActivity.GetDstBasePathCleanup)
		env.RegisterActivity(resumeReplicationActivity.GetSignedSrcTokenCleanup)
		env.RegisterActivity(resumeReplicationActivity.GetSignedDstTokenCleanup)
		env.RegisterActivity(resumeReplicationActivity.GetReplicationOnDestinationForCleanup)
		env.RegisterActivity(resumeReplicationActivity.StopReplicationOnDestinationForCleanup)
		env.RegisterActivity(resumeReplicationActivity.DescribeRemoteJobForCleanup)
		env.RegisterActivity(resumeReplicationActivity.DeleteReplicationOnDestinationForCleanup)
		env.RegisterActivity(resumeReplicationActivity.ReleaseReplicationOnSourceForCleanup)
		env.RegisterActivity(resumeReplicationActivity.DescribeSourceJobForCleanup)
		env.RegisterActivity(resumeReplicationActivity.DeHydrateDestinationVolumeReplicationForCleanup)
		env.RegisterActivity(resumeReplicationActivity.GetDestinationVolumeForCleanup)
		env.RegisterActivity(resumeReplicationActivity.DeleteVolumeOnDestinationForCleanup)
		env.RegisterActivity(resumeReplicationActivity.DeHydrateDestinationVolumeForCleanup)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.DeleteReplicationParams{}

		event := &replication.DeleteReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{},
			},
		}

		replicationResult := &replication.DeleteReplicationResult{
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
			Event:            event,
			DstReplication: &googleproxyclient.VolumeReplicationInternalV1beta{
				Name:             googleproxyclient.NewOptString("repl-123"),
				LastTransferSize: googleproxyclient.NewOptInt64(100),
				MirrorState:      googleproxyclient.NewOptVolumeReplicationInternalV1betaMirrorState(googleproxyclient.VolumeReplicationInternalV1betaMirrorStatePREPARING),
			},
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSrcBasePathCleanup", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetDstBasePathCleanup", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetSignedSrcTokenCleanup", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetSignedDstTokenCleanup", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetReplicationOnDestinationForCleanup", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("DescribeRemoteJobForCleanup", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteReplicationOnDestinationForCleanup", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("ReleaseReplicationOnSourceForCleanup", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("DescribeSourceJobForCleanup", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeHydrateDestinationVolumeReplicationForCleanup", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetDestinationVolumeForCleanup", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("DeleteVolumeOnDestinationForCleanup", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("DeHydrateDestinationVolumeForCleanup", mock.Anything, mock.Anything).Return(replicationResult, nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.ExecuteWorkflow(ReplicationCleanupWorkflow, params, event)
		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		assert.Nil(tt, err)
		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})
}

func TestReplicationCleanupWorkflowWhenError(t *testing.T) {
	t.Run("TestReplicationCleanupWorkflow", func(tt *testing.T) {
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
		resumeReplicationActivity := replicationActivities.CleanupVolumeReplicationActivity{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(resumeReplicationActivity.GetSrcBasePathCleanup)
		env.RegisterActivity(resumeReplicationActivity.GetDstBasePathCleanup)
		env.RegisterActivity(resumeReplicationActivity.GetSignedSrcTokenCleanup)
		env.RegisterActivity(resumeReplicationActivity.GetSignedDstTokenCleanup)
		env.RegisterActivity(resumeReplicationActivity.GetReplicationOnDestinationForCleanup)
		env.RegisterActivity(resumeReplicationActivity.StopReplicationOnDestinationForCleanup)
		env.RegisterActivity(resumeReplicationActivity.DescribeRemoteJobForCleanup)
		env.RegisterActivity(resumeReplicationActivity.DeleteReplicationOnDestinationForCleanup)
		env.RegisterActivity(resumeReplicationActivity.ReleaseReplicationOnSourceForCleanup)
		env.RegisterActivity(resumeReplicationActivity.DescribeSourceJobForCleanup)
		env.RegisterActivity(resumeReplicationActivity.DeHydrateDestinationVolumeReplicationForCleanup)
		env.RegisterActivity(resumeReplicationActivity.GetDestinationVolumeForCleanup)
		env.RegisterActivity(resumeReplicationActivity.DeleteVolumeOnDestinationForCleanup)
		env.RegisterActivity(resumeReplicationActivity.DeHydrateDestinationVolumeForCleanup)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.DeleteReplicationParams{}

		event := &replication.DeleteReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{},
			},
		}

		replicationResult := &replication.DeleteReplicationResult{
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
			Event:            event,
			DstReplication: &googleproxyclient.VolumeReplicationInternalV1beta{
				Name:             googleproxyclient.NewOptString("repl-123"),
				LastTransferSize: googleproxyclient.NewOptInt64(100),
				MirrorState:      googleproxyclient.NewOptVolumeReplicationInternalV1betaMirrorState(googleproxyclient.VolumeReplicationInternalV1betaMirrorStateMIRRORED),
			},
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSrcBasePathCleanup", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetDstBasePathCleanup", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetSignedSrcTokenCleanup", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetSignedDstTokenCleanup", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetReplicationOnDestinationForCleanup", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity(resumeReplicationActivity.StopReplicationOnDestinationForCleanup, mock.Anything, mock.Anything).Return(nil, errors.New("failed to update volume details"))
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.ExecuteWorkflow(ReplicationCleanupWorkflow, params, event)
		// Assert that the workflow was executed and handled the error
		assert.True(t, env.IsWorkflowCompleted())
		env.AssertExpectations(t)
	})
}
