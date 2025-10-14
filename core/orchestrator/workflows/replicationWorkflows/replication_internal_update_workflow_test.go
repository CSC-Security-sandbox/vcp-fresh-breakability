package replicationWorkflows

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/replicationActivities"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestUpdateInternalVolumeReplicationWorkflow(t *testing.T) {
	t.Run("TestUpdateInternalVolumeReplicationWorkflowSuccess", func(tt *testing.T) {
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
		internalVolumeReplicationActivity := replicationActivities.InternalVolumeReplicationActivity{SE: mockStorage}
		internalVolumeUpdateReplicationActivity := replicationActivities.InternalVolumeReplicationUpdateActivity{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(internalVolumeUpdateReplicationActivity.UpdateVolumeReplicationOntap)
		env.RegisterActivity(internalVolumeReplicationActivity.UpdateVolumeReplicationDetails)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID: 1,
			},
			Name: "test-account",
		}
		volume := &datamodel.Volume{
			Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
				PoolCredentials: &datamodel.PoolCredentials{
					Password:      "password",
					SecretID:      "",
					CertificateID: "",
				}},
			Svm:              &datamodel.Svm{Name: "svm_test"},
			VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}},
		}
		params := &commonparams.UpdateVolumeReplicationInternalParams{
			VolumeReplicationUuid: "test-replication-uuid",
			Labels: &datamodel.JSONB{
				"key": "value",
			},
		}

		volumeReplication := &models.VolumeReplication{
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
			Name: volumeReplication.Name,
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationVolumeUUID: volumeReplication.ReplicationAttributes.DestinationVolumeUUID,
			},
			AccountID: account.ID,
			Account:   account,
			VolumeID:  volumeReplication.VolumeID,
			Volume:    volume,
		}
		replicationUpdateResponseONTAP := &vsa.VolumeReplication{
			RelationshipID:        "test-relationship-id-123",
			ReplicationSchedule:   "hourly",
			MirrorState:           "snapmirrored",
			RelationshipStatus:    "idle",
			TotalTransferBytes:    1024000,
			TotalTransferTimeSecs: 3600,
			LastTransferSize:      512000,
			LastTransferError:     "",
			LastTransferDuration:  1800,
			LastTransferEndTime:   &time.Time{},
			LagTime:               300,
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
		env.OnActivity("UpdateVolumeReplicationOntap", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeReplication{}, nil)
		env.OnActivity("UpdateVolumeReplicationDetails", mock.Anything, replicationDb, replicationUpdateResponseONTAP, params).Return(nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.ExecuteWorkflow(UpdateInternalVolumeReplicationWorkflow, params, replicationDb)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		assert.Nil(tt, err)
		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})
}
