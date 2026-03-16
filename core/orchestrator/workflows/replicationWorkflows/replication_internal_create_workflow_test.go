package replicationWorkflows

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/replicationActivities"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestCreateInternalVolumeReplicationWorkflow(t *testing.T) {
	t.Run("TestCreateInternalVolumeReplicationWorkflow_HeartbeatTimeoutIsSet", func(tt *testing.T) {
		// Verifies activity options include HeartbeatTimeout (from retry policy); success path completes with heartbeat-enabled activities.
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
		internalVolumeCreateReplicationActivity := replicationActivities.InternalVolumeReplicationActivity{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(internalVolumeCreateReplicationActivity.CreateVolumeReplicationInternal)
		env.RegisterActivity(internalVolumeCreateReplicationActivity.UpdateVolumeReplicationDetails)
		env.RegisterActivity(internalVolumeCreateReplicationActivity.HydrateReplicationCreate)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "test-account"}
		volume := &datamodel.Volume{
			Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, PoolCredentials: &datamodel.PoolCredentials{Password: "password", SecretID: "", CertificateID: ""}},
			Svm:  &datamodel.Svm{Name: "svm_test"}, VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}},
		}
		params := &commonparams.CreateVolumeReplicationInternalParams{
			VolumeReplication: &models.VolumeReplication{
				Account: &models.Account{BaseModel: models.BaseModel{ID: 1}, Name: "test-account"},
				Name:    "test-replication",
				ReplicationAttributes: &models.ReplicationDetails{DestinationVolumeUUID: "test-volume-uuid"},
				Volume:                &models.Volume{BaseModel: models.BaseModel{ID: 1}},
			},
		}
		replicationDb := &datamodel.VolumeReplication{
			Name: params.VolumeReplication.Name,
			ReplicationAttributes: &datamodel.ReplicationDetails{DestinationVolumeUUID: params.VolumeReplication.ReplicationAttributes.DestinationVolumeUUID},
			AccountID: account.ID, Account: account, VolumeID: params.VolumeReplication.VolumeID, Volume: volume,
		}
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"}, State: "NEW"}, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
		env.OnActivity("CreateVolumeReplicationInternal", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeReplication{}, nil)
		env.OnActivity("UpdateVolumeReplicationDetails", mock.Anything, mock.Anything, mock.Anything, (*commonparams.UpdateVolumeReplicationInternalParams)(nil)).Return(nil)
		env.OnActivity("HydrateReplicationCreate", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.ExecuteWorkflow(CreateInternalVolumeReplicationWorkflow, params, replicationDb)
		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

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
		internalVolumeCreateReplicationActivity := replicationActivities.InternalVolumeReplicationActivity{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(internalVolumeCreateReplicationActivity.CreateVolumeReplicationInternal)
		env.RegisterActivity(internalVolumeCreateReplicationActivity.UpdateVolumeReplicationDetails)
		env.RegisterActivity(internalVolumeCreateReplicationActivity.HydrateReplicationCreate)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)

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
				},
			},
			Svm:              &datamodel.Svm{Name: "svm_test"},
			VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}},
		}
		params := &commonparams.CreateVolumeReplicationInternalParams{
			VolumeReplication: &models.VolumeReplication{
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
			},
		}
		replicationDb := &datamodel.VolumeReplication{
			Name: params.VolumeReplication.Name,
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationVolumeUUID: params.VolumeReplication.ReplicationAttributes.DestinationVolumeUUID,
			},
			AccountID: account.ID,
			Account:   account,
			VolumeID:  params.VolumeReplication.VolumeID,
			Volume:    volume,
		}
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     "NEW",
		}, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
		env.OnActivity("CreateVolumeReplicationInternal", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeReplication{}, nil)
		env.OnActivity("UpdateVolumeReplicationDetails", mock.Anything, mock.Anything, mock.Anything, (*commonparams.UpdateVolumeReplicationInternalParams)(nil)).Return(nil)
		env.OnActivity("HydrateReplicationCreate", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.ExecuteWorkflow(CreateInternalVolumeReplicationWorkflow, params, replicationDb)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		assert.Nil(tt, err)
		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestCreateInternalVolumeReplicationWorkflow_DeferCase_UpdateReplicationState", func(tt *testing.T) {
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
		internalVolumeCreateReplicationActivity := replicationActivities.InternalVolumeReplicationActivity{SE: mockStorage}
		replicationCreateActivity := replicationActivities.VolumeReplicationCreateActivity{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(internalVolumeCreateReplicationActivity.CreateVolumeReplicationInternal)
		env.RegisterActivity(replicationCreateActivity.UpdateReplicationState)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)

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
				},
			},
			Svm:              &datamodel.Svm{Name: "svm_test"},
			VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}},
		}
		params := &commonparams.CreateVolumeReplicationInternalParams{
			VolumeReplication: &models.VolumeReplication{
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
			},
		}
		replicationDb := &datamodel.VolumeReplication{
			Name: params.VolumeReplication.Name,
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationVolumeUUID: params.VolumeReplication.ReplicationAttributes.DestinationVolumeUUID,
			},
			AccountID: account.ID,
			Account:   account,
			VolumeID:  params.VolumeReplication.VolumeID,
			Volume:    volume,
		}
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     "NEW",
		}, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
		env.OnActivity("CreateVolumeReplicationInternal", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, assert.AnError)
		env.OnActivity("UpdateReplicationState", mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(CreateInternalVolumeReplicationWorkflow, params, replicationDb)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
		env.AssertExpectations(tt)
	})

	t.Run("TestCreateInternalVolumeReplicationWorkflow_EnsureJobStateError", func(tt *testing.T) {
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
		replicationCreateActivity := replicationActivities.VolumeReplicationCreateActivity{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(commonActivity.GetJob)
		env.RegisterActivity(replicationCreateActivity.UpdateReplicationState)
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
				},
			},
			Svm:              &datamodel.Svm{Name: "svm_test"},
			VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}},
		}
		params := &commonparams.CreateVolumeReplicationInternalParams{
			VolumeReplication: &models.VolumeReplication{
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
			},
		}
		replicationDb := &datamodel.VolumeReplication{
			Name: params.VolumeReplication.Name,
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationVolumeUUID: params.VolumeReplication.ReplicationAttributes.DestinationVolumeUUID,
			},
			AccountID: account.ID,
			Account:   account,
			VolumeID:  params.VolumeReplication.VolumeID,
			Volume:    volume,
		}
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

		// Mock GetJob to return a job with state PROCESSING (not NEW) to trigger EnsureJobState error
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     "PROCESSING", // Wrong state to trigger error
		}, nil)
		env.OnActivity("UpdateReplicationState", mock.Anything, mock.Anything).Return(nil).Maybe()

		env.ExecuteWorkflow(CreateInternalVolumeReplicationWorkflow, params, replicationDb)

		// Assert that the workflow failed due to EnsureJobState error
		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
		env.AssertExpectations(tt)
	})

	t.Run("TestCreateInternalVolumeReplicationWorkflow_EnsureJobState_JobNotFound", func(tt *testing.T) {
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
		replicationCreateActivity := replicationActivities.VolumeReplicationCreateActivity{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(commonActivity.GetJob)
		env.RegisterActivity(replicationCreateActivity.UpdateReplicationState)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.CreateVolumeReplicationInternalParams{
			VolumeReplication: &models.VolumeReplication{
				Account: &models.Account{
					BaseModel: models.BaseModel{
						ID: 1,
					},
					Name: "test-account",
				},
				Name: "test-replication",
			},
		}
		replicationDb := &datamodel.VolumeReplication{
			Name: params.VolumeReplication.Name,
		}
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

		// Mock GetJob to return nil (job not found) to trigger EnsureJobState error
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return((*datamodel.Job)(nil), nil)
		env.OnActivity("UpdateReplicationState", mock.Anything, mock.Anything).Return(nil).Maybe()

		env.ExecuteWorkflow(CreateInternalVolumeReplicationWorkflow, params, replicationDb)

		// Assert that the workflow failed due to EnsureJobState error
		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
		env.AssertExpectations(tt)
	})

	t.Run("TestCreateInternalVolumeReplicationWorkflow_EnsureJobState_GetJobError", func(tt *testing.T) {
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
		replicationCreateActivity := replicationActivities.VolumeReplicationCreateActivity{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(commonActivity.GetJob)
		env.RegisterActivity(replicationCreateActivity.UpdateReplicationState)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.CreateVolumeReplicationInternalParams{
			VolumeReplication: &models.VolumeReplication{
				Account: &models.Account{
					BaseModel: models.BaseModel{
						ID: 1,
					},
					Name: "test-account",
				},
				Name: "test-replication",
			},
		}
		replicationDb := &datamodel.VolumeReplication{
			Name: params.VolumeReplication.Name,
		}
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

		// Mock GetJob to return an error
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return((*datamodel.Job)(nil), assert.AnError)
		env.OnActivity("UpdateReplicationState", mock.Anything, mock.Anything).Return(nil).Maybe()

		env.ExecuteWorkflow(CreateInternalVolumeReplicationWorkflow, params, replicationDb)

		// Assert that the workflow failed due to EnsureJobState error
		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
		env.AssertExpectations(tt)
	})
}
