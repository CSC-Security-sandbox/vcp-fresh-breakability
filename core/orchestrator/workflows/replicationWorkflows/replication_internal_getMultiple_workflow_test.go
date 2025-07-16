package replicationWorkflows

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/replicationActivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestGetMultipleReplicationsInternalWorkflow(t *testing.T) {
	t.Run("WhenSuccess", func(tt *testing.T) {
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
		env.SetHeader(mockHeader)
		replicationInternalGetMultipleActivity := replicationActivities.ReplicationInternalGetMultipleActivity{SE: mockStorage}
		env.RegisterActivity(replicationInternalGetMultipleActivity.GetReplicationsFromDB)
		env.RegisterActivity(replicationInternalGetMultipleActivity.GetNodesForPools)
		env.RegisterActivity(replicationInternalGetMultipleActivity.GetReplicationsFromOntap)
		env.RegisterActivity(replicationInternalGetMultipleActivity.UpdateReplicationsInDB)
		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		expectedNode1 := &datamodel.Node{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "node-uuid-1",
			},
			Name:            "node-name-1",
			EndpointAddress: "10.0.0.0",
		}
		replication1 := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "replication-uuid-1",
			},
			AccountID: 1,
			Volume: &datamodel.Volume{
				BaseModel: datamodel.BaseModel{
					ID:   1,
					UUID: "volume-uuid-1",
				},
				PoolID: 1,
				Pool: &datamodel.Pool{
					PoolCredentials: &datamodel.PoolCredentials{
						Password:      "password",
						SecretID:      "",
						CertificateID: "",
					},
				},
			},
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationVolumeName: "destination-volume-name-1",
				DestinationHostName:   "destination-host-name-1",
				DestinationSvmName:    "destination-svm-name-1",
				ExternalUUID:          "external-uuid-1",
				ReplicationSchedule:   "daily",
			},
		}
		params := &common.ReplicationInternalGetMultipleParams{
			ReplicationsFromDB:  []*datamodel.VolumeReplication{replication1},
			ReplicationUUIDs:    []string{"replication-uuid-1", "replication-uuid-2"},
			AccountName:         "deathstar",
			PoolUUIDs:           nil,
			PoolNodeMap:         map[int64]*datamodel.Node{1: expectedNode1},
			PoolReplicationsMap: nil,
			UpdatedReplications: []*datamodel.VolumeReplication{replication1},
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetReplicationsFromDB", mock.Anything, params).Return(params, nil)
		env.OnActivity("GetNodesForPools", mock.Anything, params).Return(params, nil)
		env.OnActivity("GetReplicationsFromOntap", mock.Anything, params).Return(params, nil)
		env.OnActivity("UpdateReplicationsInDB", mock.Anything, params).Return(nil)
		env.ExecuteWorkflow(GetMultipleReplicationsInternalWorkflow, params)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		assert.Nil(tt, err)
		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})
	t.Run("WhenError", func(tt *testing.T) {
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
		env.SetHeader(mockHeader)
		replicationInternalGetMultipleActivity := replicationActivities.ReplicationInternalGetMultipleActivity{SE: mockStorage}
		env.RegisterActivity(replicationInternalGetMultipleActivity.GetReplicationsFromDB)
		env.RegisterActivity(replicationInternalGetMultipleActivity.GetNodesForPools)
		env.RegisterActivity(replicationInternalGetMultipleActivity.GetReplicationsFromOntap)
		env.RegisterActivity(replicationInternalGetMultipleActivity.UpdateReplicationsInDB)
		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		expectedNode1 := &datamodel.Node{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "node-uuid-1",
			},
			Name:            "node-name-1",
			EndpointAddress: "10.0.0.0",
		}
		replication1 := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "replication-uuid-1",
			},
			AccountID: 1,
			Volume: &datamodel.Volume{
				BaseModel: datamodel.BaseModel{
					ID:   1,
					UUID: "volume-uuid-1",
				},
				PoolID: 1,
				Pool: &datamodel.Pool{
					PoolCredentials: &datamodel.PoolCredentials{
						Password:      "password",
						SecretID:      "",
						CertificateID: "",
					},
				},
			},
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationVolumeName: "destination-volume-name-1",
				DestinationHostName:   "destination-host-name-1",
				DestinationSvmName:    "destination-svm-name-1",
				ExternalUUID:          "external-uuid-1",
				ReplicationSchedule:   "daily",
			},
		}
		params := &common.ReplicationInternalGetMultipleParams{
			ReplicationsFromDB:  []*datamodel.VolumeReplication{replication1},
			ReplicationUUIDs:    []string{"replication-uuid-1", "replication-uuid-2"},
			AccountName:         "deathstar",
			PoolUUIDs:           nil,
			PoolNodeMap:         map[int64]*datamodel.Node{1: expectedNode1},
			PoolReplicationsMap: nil,
			UpdatedReplications: []*datamodel.VolumeReplication{replication1},
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetReplicationsFromDB", mock.Anything, params).Return(params, nil)
		env.OnActivity("GetNodesForPools", mock.Anything, params).Return(params, nil)
		env.OnActivity("GetReplicationsFromOntap", mock.Anything, params).Return(nil, errors.New("failed to get replications from Ontap"))
		env.OnActivity("UpdateReplicationsInDB", mock.Anything, params).Return(nil)
		env.ExecuteWorkflow(GetMultipleReplicationsInternalWorkflow, params)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		assert.Nil(tt, err)
		assert.True(tt, env.IsWorkflowCompleted())
		assert.ErrorContains(tt, env.GetWorkflowError(), "failed to get replications from Ontap")
	})
}
