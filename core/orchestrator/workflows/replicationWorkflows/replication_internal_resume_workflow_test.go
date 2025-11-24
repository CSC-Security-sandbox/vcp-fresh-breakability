package replicationWorkflows

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/replicationActivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestResumeInternalVolumeReplicationWorkflow(t *testing.T) {
	t.Run("TestResumeReplicationWorkflow", func(tt *testing.T) {
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
		internalResumeReplicationActivity := replicationActivities.InternalVolumeReplicationResumeActivity{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(internalResumeReplicationActivity.ResumeVolumeReplication)
		env.RegisterActivity(internalResumeReplicationActivity.GetSnapmirrorDetails)
		env.RegisterActivity(internalResumeReplicationActivity.UpdateVolumeReplicationResumeDetails)
		env.RegisterActivity(internalResumeReplicationActivity.UpdateVolumeType)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

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
		replicationDb := &datamodel.VolumeReplication{
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				Name: "test-account",
			},
			Volume: volume,
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
		env.OnActivity("ResumeVolumeReplication", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeReplication{}, nil)
		env.OnActivity("GetSnapmirrorDetails", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("UpdateVolumeReplicationResumeDetails", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVolumeType", mock.Anything, mock.Anything).Return(nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.ExecuteWorkflow(ResumeInternalVolumeReplicationWorkflow, replicationDb, false)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		assert.Nil(tt, err)
		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestResumeInternalVolumeReplicationWorkflow_DeferCase_UpdateReplicationState", func(tt *testing.T) {
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
		internalResumeReplicationActivity := replicationActivities.InternalVolumeReplicationResumeActivity{SE: mockStorage}
		replicationCommonActivity := replicationActivities.VolumeReplicationCreateActivity{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(internalResumeReplicationActivity.ResumeVolumeReplication)
		env.RegisterActivity(replicationCommonActivity.UpdateReplicationState)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

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
		replicationDb := &datamodel.VolumeReplication{
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				Name: "test-account",
			},
			Volume: volume,
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
		env.OnActivity("ResumeVolumeReplication", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, assert.AnError)
		env.OnActivity("UpdateReplicationState", mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(ResumeInternalVolumeReplicationWorkflow, replicationDb, false)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
		env.AssertExpectations(tt)
	})
}
