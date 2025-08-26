package replicationWorkflows

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/replicationActivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestPerformMountCheckWorkFlow(t *testing.T) {
	t.Run("TestPerformMountCheckWorkFlow", func(t *testing.T) {
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
		mockStorage := database.NewMockStorage(t)
		commonActivity := activities.CommonActivities{SE: mockStorage}
		mountJobActivity := replicationActivities.MountJobActivity{SE: mockStorage}
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(mountJobActivity.GetReplication)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(mountJobActivity.CheckMountJob)
		env.RegisterActivity(mountJobActivity.GetReplicationFromOntap)
		env.RegisterActivity(mountJobActivity.UpdateReplicationInDB)
		env.RegisterActivity(mountJobActivity.GetLunDetailsFromOntap)
		env.RegisterActivity(mountJobActivity.UpdateVolumeLunDetailsInDB)

		replicationUUID := "replication-uuid"
		accountName := "testAccount"
		volume := &datamodel.Volume{
			Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			}},
			Svm:              &datamodel.Svm{Name: "svm_test"},
			VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}},
			PoolID:           1,
		}
		dbreplication := &datamodel.VolumeReplication{ReplicationAttributes: &datamodel.ReplicationDetails{
			ExternalUUID: "external",
		},
			Volume: volume}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.OnActivity(mountJobActivity.GetReplication, mock.Anything, replicationUUID).Return(dbreplication, nil)
		env.OnActivity(commonActivity.GetNode, mock.Anything, dbreplication.Volume.PoolID).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
		env.OnActivity(mountJobActivity.CheckMountJob, mock.Anything, dbreplication, mock.Anything, accountName).Return(nil)
		env.OnActivity(mountJobActivity.GetReplicationFromOntap, mock.Anything, dbreplication, mock.Anything, accountName).Return(dbreplication, nil)
		env.OnActivity(mountJobActivity.UpdateReplicationInDB, mock.Anything, dbreplication).Return(nil)
		env.OnActivity(mountJobActivity.GetLunDetailsFromOntap, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity(mountJobActivity.UpdateVolumeLunDetailsInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		// Execute workflow
		env.ExecuteWorkflow(PerformMountCheckWorkflow, replicationUUID, accountName)

		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.Nil(t, env.GetWorkflowError())
		mockStorage.AssertExpectations(t)
	})
}
