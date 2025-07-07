package replicationWorkflows

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/replicationActivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestDeleteInternalVolumeReplicationWorkflow(t *testing.T) {
	t.Run("TestDeleteInternalVolumeReplicationWorkflow", func(tt *testing.T) {
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
		internalVolumeCreateReplicationActivity := replicationActivities.InternalVolumeReplicationDeleteActivity{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(internalVolumeCreateReplicationActivity.DeleteVolumeReplication)
		env.RegisterActivity(internalVolumeCreateReplicationActivity.UpdateVolumeReplicationDetailsForDelete)
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

		params := &models.VolumeReplication{
			Account: &models.Account{
				BaseModel: models.BaseModel{
					ID: 1,
				},
				Name: "test-account",
			},
			Name: "test-replication",
			ReplicationAttributes: &models.ReplicationDetails{
				DestinationVolumeUUID: "test-volume-uuid",
			},
			Volume: &models.Volume{
				BaseModel: models.BaseModel{
					ID: 1,
				},
			},
		}
		replicationDb := &datamodel.VolumeReplication{
			Name: params.Name,
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationVolumeUUID: params.ReplicationAttributes.DestinationVolumeUUID,
			},
			AccountID: account.ID,
			Account:   account,
			VolumeID:  params.VolumeID,
			Volume:    volume,
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
		env.OnActivity("DeleteVolumeReplication", mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeReplication{}, nil)
		env.OnActivity("UpdateVolumeReplicationDetailsForDelete", mock.Anything, mock.Anything).Return(nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.ExecuteWorkflow(DeleteInternalVolumeReplicationWorkflow, replicationDb)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		assert.Nil(tt, err)
		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})
}

func TestDeleteInternalVolumeReplicationWorkflowFailure(t *testing.T) {
	t.Run("TestDeleteInternalVolumeReplicationWorkflow", func(tt *testing.T) {
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
		internalVolumeCreateReplicationActivity := replicationActivities.InternalVolumeReplicationDeleteActivity{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(internalVolumeCreateReplicationActivity.DeleteVolumeReplication)
		env.RegisterActivity(internalVolumeCreateReplicationActivity.UpdateVolumeReplicationDetailsForDelete)
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

		params := &models.VolumeReplication{
			Account: &models.Account{
				BaseModel: models.BaseModel{
					ID: 1,
				},
				Name: "test-account",
			},
			Name: "test-replication",
			ReplicationAttributes: &models.ReplicationDetails{
				DestinationVolumeUUID: "test-volume-uuid",
			},
			Volume: &models.Volume{
				BaseModel: models.BaseModel{
					ID: 1,
				},
			},
		}
		replicationDb := &datamodel.VolumeReplication{
			Name: params.Name,
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationVolumeUUID: params.ReplicationAttributes.DestinationVolumeUUID,
			},
			AccountID: account.ID,
			Account:   account,
			VolumeID:  params.VolumeID,
			Volume:    volume,
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
		env.OnActivity("DeleteVolumeReplication", mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeReplication{}, nil)
		env.OnActivity("UpdateVolumeReplicationDetailsForDelete", mock.Anything, mock.Anything).Return(errors.New("error"))
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("error"))
		env.ExecuteWorkflow(DeleteInternalVolumeReplicationWorkflow, replicationDb)
		assert.NotNil(tt, env.GetWorkflowError())
	})
}
