package replicationWorkflows

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/replicationActivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestReverseInternalVolumeReplicationWorkflow(t *testing.T) {
	t.Run("TestReverseInternalVolumeReplicationWorkflow_Success", func(tt *testing.T) {
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
		replicationActivity := replicationActivities.InternalVolumeReplicationReverseActivity{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(replicationActivity.ReverseVolumeReplication)
		env.RegisterActivity(replicationActivity.UpdateVolumeTypeForNewDestination)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      "test-account",
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
				AuthType:      1,
			},
			DeploymentName: "test-deployment",
		}

		volume := &datamodel.Volume{
			BaseModel:        datamodel.BaseModel{ID: 1},
			Pool:             pool,
			PoolID:           1,
			Svm:              &datamodel.Svm{Name: "svm_test"},
			VolumeAttributes: &datamodel.VolumeAttributes{},
		}

		replicationDb := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      "test-replication",
			AccountID: 1,
			Account:   account,
			VolumeID:  1,
			Volume:    volume,
		}

		dbNodes := []*datamodel.Node{
			{
				BaseModel:       datamodel.BaseModel{ID: 1},
				EndpointAddress: "127.0.0.1",
				PoolID:          1,
			},
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return(dbNodes, nil)
		env.OnActivity("ReverseVolumeReplication", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeReplication{}, nil)
		env.OnActivity("UpdateVolumeTypeForNewDestination", mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(ReverseInternalVolumeReplicationWorkflow, replicationDb)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		assert.Nil(tt, err)
		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestReverseInternalVolumeReplicationWorkflow_GetNodeError", func(tt *testing.T) {
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
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      "test-account",
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
				AuthType:      1,
			},
			DeploymentName: "test-deployment",
		}

		volume := &datamodel.Volume{
			BaseModel:        datamodel.BaseModel{ID: 1},
			Pool:             pool,
			PoolID:           1,
			Svm:              &datamodel.Svm{Name: "svm_test"},
			VolumeAttributes: &datamodel.VolumeAttributes{},
		}

		replicationDb := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      "test-replication",
			AccountID: 1,
			Account:   account,
			VolumeID:  1,
			Volume:    volume,
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return(nil, assert.AnError)

		env.ExecuteWorkflow(ReverseInternalVolumeReplicationWorkflow, replicationDb)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestReverseInternalVolumeReplicationWorkflow_ReverseVolumeReplicationError", func(tt *testing.T) {
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
		replicationActivity := replicationActivities.InternalVolumeReplicationReverseActivity{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(replicationActivity.ReverseVolumeReplication)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      "test-account",
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
				AuthType:      1,
			},
			DeploymentName: "test-deployment",
		}

		volume := &datamodel.Volume{
			BaseModel:        datamodel.BaseModel{ID: 1},
			Pool:             pool,
			PoolID:           1,
			Svm:              &datamodel.Svm{Name: "svm_test"},
			VolumeAttributes: &datamodel.VolumeAttributes{},
		}

		replicationDb := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      "test-replication",
			AccountID: 1,
			Account:   account,
			VolumeID:  1,
			Volume:    volume,
		}

		dbNodes := []*datamodel.Node{
			{
				BaseModel:       datamodel.BaseModel{ID: 1},
				EndpointAddress: "127.0.0.1",
				PoolID:          1,
			},
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return(dbNodes, nil)
		env.OnActivity("ReverseVolumeReplication", mock.Anything, mock.Anything, mock.Anything).Return(nil, assert.AnError)

		env.ExecuteWorkflow(ReverseInternalVolumeReplicationWorkflow, replicationDb)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestReverseInternalVolumeReplicationWorkflow_UpdateVolumeReplicationReverseDetailsError", func(tt *testing.T) {
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
		replicationActivity := replicationActivities.InternalVolumeReplicationReverseActivity{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(replicationActivity.ReverseVolumeReplication)
		env.RegisterActivity(replicationActivity.UpdateVolumeTypeForNewDestination)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      "test-account",
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
				AuthType:      1,
			},
			DeploymentName: "test-deployment",
		}

		volume := &datamodel.Volume{
			BaseModel:        datamodel.BaseModel{ID: 1},
			Pool:             pool,
			PoolID:           1,
			Svm:              &datamodel.Svm{Name: "svm_test"},
			VolumeAttributes: &datamodel.VolumeAttributes{},
		}

		replicationDb := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      "test-replication",
			AccountID: 1,
			Account:   account,
			VolumeID:  1,
			Volume:    volume,
		}

		dbNodes := []*datamodel.Node{
			{
				BaseModel:       datamodel.BaseModel{ID: 1},
				EndpointAddress: "127.0.0.1",
				PoolID:          1,
			},
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return(dbNodes, nil)
		env.OnActivity("ReverseVolumeReplication", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeReplication{}, nil)
		env.OnActivity("UpdateVolumeTypeForNewDestination", mock.Anything, mock.Anything).Return(assert.AnError)

		env.ExecuteWorkflow(ReverseInternalVolumeReplicationWorkflow, replicationDb)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestReverseInternalVolumeReplicationWorkflow_DeferCase_UpdateReplicationState", func(tt *testing.T) {
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
		replicationActivity := replicationActivities.InternalVolumeReplicationReverseActivity{SE: mockStorage}
		replicationCommonActivity := replicationActivities.VolumeReplicationCreateActivity{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(replicationActivity.ReverseVolumeReplication)
		env.RegisterActivity(replicationCommonActivity.UpdateReplicationState)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      "test-account",
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
				AuthType:      1,
			},
			DeploymentName: "test-deployment",
		}

		volume := &datamodel.Volume{
			BaseModel:        datamodel.BaseModel{ID: 1},
			Pool:             pool,
			PoolID:           1,
			Svm:              &datamodel.Svm{Name: "svm_test"},
			VolumeAttributes: &datamodel.VolumeAttributes{},
		}

		replicationDb := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      "test-replication",
			AccountID: 1,
			Account:   account,
			VolumeID:  1,
			Volume:    volume,
		}

		dbNodes := []*datamodel.Node{
			{
				BaseModel:       datamodel.BaseModel{ID: 1},
				EndpointAddress: "127.0.0.1",
				PoolID:          1,
			},
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return(dbNodes, nil)
		env.OnActivity("ReverseVolumeReplication", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, assert.AnError)
		env.OnActivity("UpdateReplicationState", mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(ReverseInternalVolumeReplicationWorkflow, replicationDb)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
		env.AssertExpectations(tt)
	})
}
