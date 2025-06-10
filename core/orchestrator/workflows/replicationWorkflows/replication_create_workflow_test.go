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
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestRun(t *testing.T) {
	t.Run("TestCreateInternalVolumeReplicationWorkflow", func(tt *testing.T) {
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
		volumeCreateReplicationActivity := replicationActivities.VolumeReplicationCreateActivity{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(volumeCreateReplicationActivity.GetSrcBasePath)
		env.RegisterActivity(volumeCreateReplicationActivity.GetDstBasePath)
		env.RegisterActivity(volumeCreateReplicationActivity.GetSignedSrcToken)
		env.RegisterActivity(volumeCreateReplicationActivity.GetSignedDstToken)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(volumeCreateReplicationActivity.GetSourceInterclusterLifs)
		env.RegisterActivity(volumeCreateReplicationActivity.GetDestinationPoolDetails)
		env.RegisterActivity(volumeCreateReplicationActivity.CreateClusterPeering)
		env.RegisterActivity(volumeCreateReplicationActivity.AcceptClusterPeering)
		env.RegisterActivity(volumeCreateReplicationActivity.DescribeRemoteJob)
		env.RegisterActivity(volumeCreateReplicationActivity.UpdateReplicationState)
		env.RegisterActivity(volumeCreateReplicationActivity.CreateDestinationVolume)
		env.RegisterActivity(volumeCreateReplicationActivity.HydrateDestinationVolume)
		env.RegisterActivity(volumeCreateReplicationActivity.UpdateReplicationState)
		env.RegisterActivity(volumeCreateReplicationActivity.GetVolumeSVMNames)
		env.RegisterActivity(volumeCreateReplicationActivity.CreateReplicationOnDestination)
		env.RegisterActivity(volumeCreateReplicationActivity.AcceptSvmPeer)
		env.RegisterActivity(volumeCreateReplicationActivity.UpdateReplicationDetails)
		env.RegisterActivity(volumeCreateReplicationActivity.MountReplication)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID: 1,
			},
			Name: "test-account",
		}
		volume := &datamodel.Volume{
			Pool:             &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, Username: "username", Password: "password"},
			Svm:              &datamodel.Svm{Name: "svm_test"},
			VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}},
		}
		params := &commonparams.CreateVolumeReplicationParams{
			AccountName: "test-account",
			Name:        "test-replication",
		}
		replicationDb := &datamodel.VolumeReplication{
			Name:      params.Name,
			AccountID: account.ID,
			Account:   account,
			VolumeID:  int64(1),
			Volume:    volume,
		}
		event := &replication.CreateReplicationEvent{
			SourceVolume: datamodel.Volume{
				Name: "source-volume",
			},
		}
		replicationResult := &replication.CreateReplicationResult{
			SrcProjectNumber: &event.SourceProjectNumber,
			DstProjectNumber: &event.DestinationProjectNumber,
			Event:            event,
		}
		// Mocking the required activities
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSrcBasePath", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetDstBasePath", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetSignedSrcToken", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetSignedDstToken", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return(&datamodel.Node{EndpointAddress: "127.0.0.1"}, nil)
		env.OnActivity("GetSourceInterclusterLifs", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetDestinationPoolDetails", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("CreateClusterPeering", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("AcceptClusterPeering", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("DescribeRemoteJob", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateReplicationState", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateDestinationVolume", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("HydrateDestinationVolume", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("GetVolumeSVMNames", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("CreateReplicationOnDestination", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("AcceptSvmPeer", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.OnActivity("UpdateReplicationDetails", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("MountReplication", mock.Anything, mock.Anything).Return(replicationResult, nil)
		env.ExecuteWorkflow(CreateVolumeReplicationWorkflow, params, replicationDb, event)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		assert.Nil(tt, err)
		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})
}
