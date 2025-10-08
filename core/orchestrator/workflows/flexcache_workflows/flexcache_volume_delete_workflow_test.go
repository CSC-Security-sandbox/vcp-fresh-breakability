package flexcache_workflows

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/flexcache_activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/flexcache"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

type FlexCacheDeleteUnitTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env *testsuite.TestWorkflowEnvironment

	// store activities to use in tests so we don't need to mock them every time
	commonActivity                *activities.CommonActivities
	volumeCreateActivity          *activities.VolumeCreateActivity
	volumeDeleteActivity          *activities.VolumeDeleteActivity
	flexCacheVolumeDeleteActivity *flexcache_activities.FlexCacheVolumeDeleteActivity
}

func (s *FlexCacheDeleteUnitTestSuite) SetupTest() {
	s.env = s.NewTestWorkflowEnvironment()
	s.env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
	mockHeader := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{
			"logParam": encodedValue,
		},
	}
	s.env.SetHeader(mockHeader)

	// Register workflow
	s.env.RegisterWorkflow(DeleteFlexCacheVolumeWorkflow)

	// Register activities
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	volumeDeleteActivity := activities.VolumeDeleteActivity{SE: mockStorage}
	flexCacheVolumeDeleteActivity := flexcache_activities.FlexCacheVolumeDeleteActivity{SE: mockStorage}

	s.commonActivity = &commonActivity
	s.volumeCreateActivity = &volumeCreateActivity
	s.volumeDeleteActivity = &volumeDeleteActivity
	s.flexCacheVolumeDeleteActivity = &flexCacheVolumeDeleteActivity

	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(commonActivity.GetNode)
	s.env.RegisterActivity(commonActivity.GetOntapJob)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeStateInDB)
	s.env.RegisterActivity(volumeDeleteActivity.DeleteVolume)
	s.env.RegisterActivity(flexCacheVolumeDeleteActivity.UnmountVolumeInOntapActivity)
	s.env.RegisterActivity(flexCacheVolumeDeleteActivity.DeleteFlexCacheVolumeInOntapActivity)
}

func CreateTestVolumeForDelete() *datamodel.Volume {
	return &datamodel.Volume{
		Account:   &datamodel.Account{Name: "account-1"},
		BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
		Name:      "test-flexcache-volume",
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
			DeploymentName: "test-deployment",
		},
	}
}

func (s *FlexCacheDeleteUnitTestSuite) AfterTest() {
	s.env.AssertExpectations(s.T())
}

func (s *FlexCacheDeleteUnitTestSuite) Test_DeleteFlexCacheVolumeWorkflow_Success() {
	volume := CreateTestVolumeForDelete()
	unmountJobUuid := "unmount-job-uuid"
	deleteJobUuid := "delete-job-uuid"

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(s.flexCacheVolumeDeleteActivity.UnmountVolumeInOntapActivity, mock.Anything, mock.Anything).Return(&flexcache.DeleteFlexCacheResult{
		UnmountJobResponse: &vsa.OntapAsyncResponse{JobUUID: unmountJobUuid},
	}, nil)
	s.env.OnActivity(s.commonActivity.GetOntapJob, mock.Anything, unmountJobUuid, mock.Anything).Return(&vsa.OntapJob{State: "success"}, nil)
	s.env.OnActivity(s.flexCacheVolumeDeleteActivity.DeleteFlexCacheVolumeInOntapActivity, mock.Anything, mock.Anything).Return(&flexcache.DeleteFlexCacheResult{
		DeleteJobResponse: &vsa.OntapAsyncResponse{JobUUID: deleteJobUuid},
	}, nil)
	s.env.OnActivity(s.commonActivity.GetOntapJob, mock.Anything, deleteJobUuid, mock.Anything).Return(&vsa.OntapJob{State: "success"}, nil)
	s.env.OnActivity(s.flexCacheVolumeDeleteActivity.DeleteSVMPeeringInOntapActivity, mock.Anything, mock.Anything).Return(&flexcache.DeleteFlexCacheResult{}, nil)
	s.env.OnActivity(s.flexCacheVolumeDeleteActivity.DeleteClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(&flexcache.DeleteFlexCacheResult{}, nil)
	s.env.OnActivity(s.volumeDeleteActivity.DeleteVolume, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(DeleteFlexCacheVolumeWorkflow, volume)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}

func (s *FlexCacheDeleteUnitTestSuite) Test_DeleteFlexCacheVolumeWorkflow_GetNodeFailure() {
	volume := CreateTestVolumeForDelete()

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return(nil, errors.New("some_error"))
	s.env.OnActivity(s.volumeCreateActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(DeleteFlexCacheVolumeWorkflow, volume)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

func (s *FlexCacheDeleteUnitTestSuite) Test_DeleteFlexCacheVolumeWorkflow_UnmountVolumeFailure() {
	volume := CreateTestVolumeForDelete()

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{
		BaseModel:       datamodel.BaseModel{ID: 1},
		EndpointAddress: "127.0.0.1",
	}}, nil)
	s.env.OnActivity(s.flexCacheVolumeDeleteActivity.UnmountVolumeInOntapActivity, mock.Anything, mock.Anything).Return(errors.New("some_error"))
	s.env.OnActivity(s.volumeCreateActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(DeleteFlexCacheVolumeWorkflow, volume)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

func (s *FlexCacheDeleteUnitTestSuite) Test_DeleteFlexCacheVolumeWorkflow_UnmountVolumeWaitFailure() {
	volume := CreateTestVolumeForDelete()

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{
		BaseModel:       datamodel.BaseModel{ID: 1},
		EndpointAddress: "127.0.0.1",
	}}, nil)
	s.env.OnActivity(s.flexCacheVolumeDeleteActivity.UnmountVolumeInOntapActivity, mock.Anything, mock.Anything).Return(&flexcache.DeleteFlexCacheResult{
		UnmountJobResponse: &vsa.OntapAsyncResponse{JobUUID: "unmount-job-uuid"},
	}, nil)
	s.env.OnActivity(s.commonActivity.GetOntapJob, mock.Anything, "unmount-job-uuid", mock.Anything).Return(nil, errors.New("some_error"))
	s.env.OnActivity(s.volumeCreateActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(DeleteFlexCacheVolumeWorkflow, volume)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

func (s *FlexCacheDeleteUnitTestSuite) Test_DeleteFlexCacheVolumeWorkflow_DeleteFlexCacheVolumeFailure() {
	volume := CreateTestVolumeForDelete()

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{
		BaseModel:       datamodel.BaseModel{ID: 1},
		EndpointAddress: "127.0.0.1",
	}}, nil)
	s.env.OnActivity(s.flexCacheVolumeDeleteActivity.UnmountVolumeInOntapActivity, mock.Anything, mock.Anything).Return(&flexcache.DeleteFlexCacheResult{}, nil)
	s.env.OnActivity(s.flexCacheVolumeDeleteActivity.DeleteFlexCacheVolumeInOntapActivity, mock.Anything, mock.Anything).Return(nil, errors.New("some_error"))
	s.env.OnActivity(s.volumeCreateActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(DeleteFlexCacheVolumeWorkflow, volume)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

func (s *FlexCacheDeleteUnitTestSuite) Test_DeleteFlexCacheVolumeWorkflow_DeleteFlexCacheVolumeWaitFailure() {
	volume := CreateTestVolumeForDelete()

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{
		BaseModel:       datamodel.BaseModel{ID: 1},
		EndpointAddress: "127.0.0.1",
	}}, nil)
	s.env.OnActivity(s.flexCacheVolumeDeleteActivity.UnmountVolumeInOntapActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeDeleteActivity.DeleteFlexCacheVolumeInOntapActivity, mock.Anything, mock.Anything).Return(&flexcache.DeleteFlexCacheResult{
		DeleteJobResponse: &vsa.OntapAsyncResponse{JobUUID: "delete-job-uuid"},
	}, nil)
	s.env.OnActivity(s.commonActivity.GetOntapJob, mock.Anything, "delete-job-uuid", mock.Anything).Return(nil, errors.New("some_error"))
	s.env.OnActivity(s.volumeCreateActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(DeleteFlexCacheVolumeWorkflow, volume)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

func (s *FlexCacheDeleteUnitTestSuite) Test_DeleteFlexCacheVolumeWorkflow_DeleteSVMPeeringFailure() {
	volume := CreateTestVolumeForDelete()

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1", BaseModel: datamodel.BaseModel{ID: 1}}}, nil)
	s.env.OnActivity(s.flexCacheVolumeDeleteActivity.UnmountVolumeInOntapActivity, mock.Anything, mock.Anything).Return(&flexcache.DeleteFlexCacheResult{
		UnmountJobResponse: &vsa.OntapAsyncResponse{JobUUID: ""},
	}, nil)
	s.env.OnActivity(s.commonActivity.GetOntapJob, mock.Anything, "", mock.Anything).Return(&vsa.OntapJob{State: "success"}, nil)
	s.env.OnActivity(s.flexCacheVolumeDeleteActivity.DeleteFlexCacheVolumeInOntapActivity, mock.Anything, mock.Anything).Return(&flexcache.DeleteFlexCacheResult{
		DeleteJobResponse: &vsa.OntapAsyncResponse{JobUUID: ""},
	}, nil)
	s.env.OnActivity(s.commonActivity.GetOntapJob, mock.Anything, "", mock.Anything).Return(&vsa.OntapJob{State: "success"}, nil)
	s.env.OnActivity(s.flexCacheVolumeDeleteActivity.DeleteSVMPeeringInOntapActivity, mock.Anything, mock.Anything).Return(errors.New("some_error"))
	s.env.OnActivity(s.volumeCreateActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(DeleteFlexCacheVolumeWorkflow, volume)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

func (s *FlexCacheDeleteUnitTestSuite) Test_DeleteFlexCacheVolumeWorkflow_DeleteClusterPeerFailure() {
	volume := CreateTestVolumeForDelete()

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1", BaseModel: datamodel.BaseModel{ID: 1}}}, nil)
	s.env.OnActivity(s.flexCacheVolumeDeleteActivity.UnmountVolumeInOntapActivity, mock.Anything, mock.Anything).Return(&flexcache.DeleteFlexCacheResult{
		UnmountJobResponse: &vsa.OntapAsyncResponse{JobUUID: ""},
	}, nil)
	s.env.OnActivity(s.commonActivity.GetOntapJob, mock.Anything, "", mock.Anything).Return(&vsa.OntapJob{State: "success"}, nil)
	s.env.OnActivity(s.flexCacheVolumeDeleteActivity.DeleteFlexCacheVolumeInOntapActivity, mock.Anything, mock.Anything).Return(&flexcache.DeleteFlexCacheResult{
		DeleteJobResponse: &vsa.OntapAsyncResponse{JobUUID: ""},
	}, nil)
	s.env.OnActivity(s.commonActivity.GetOntapJob, mock.Anything, "", mock.Anything).Return(&vsa.OntapJob{State: "success"}, nil)
	s.env.OnActivity(s.flexCacheVolumeDeleteActivity.DeleteSVMPeeringInOntapActivity, mock.Anything, mock.Anything).Return(&flexcache.DeleteFlexCacheResult{}, nil)
	s.env.OnActivity(s.flexCacheVolumeDeleteActivity.DeleteClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(errors.New("some_error"))
	s.env.OnActivity(s.volumeCreateActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(DeleteFlexCacheVolumeWorkflow, volume)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
}

func (s *FlexCacheDeleteUnitTestSuite) Test_DeleteFlexCacheVolumeWorkflow_DeleteVolumeFailure() {
	volume := CreateTestVolumeForDelete()

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(s.flexCacheVolumeDeleteActivity.UnmountVolumeInOntapActivity, mock.Anything, mock.Anything).Return(&flexcache.DeleteFlexCacheResult{
		UnmountJobResponse: &vsa.OntapAsyncResponse{JobUUID: ""},
	}, nil)
	s.env.OnActivity(s.commonActivity.GetOntapJob, mock.Anything, "", mock.Anything).Return(&vsa.OntapJob{State: "success"}, nil)
	s.env.OnActivity(s.flexCacheVolumeDeleteActivity.DeleteFlexCacheVolumeInOntapActivity, mock.Anything, mock.Anything).Return(&flexcache.DeleteFlexCacheResult{
		DeleteJobResponse: &vsa.OntapAsyncResponse{JobUUID: ""},
	}, nil)
	s.env.OnActivity(s.commonActivity.GetOntapJob, mock.Anything, "", mock.Anything).Return(&vsa.OntapJob{State: "success"}, nil)
	s.env.OnActivity(s.volumeDeleteActivity.DeleteVolume, mock.Anything, mock.Anything).Return(errors.New("some_error"))
	s.env.OnActivity(s.volumeCreateActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(DeleteFlexCacheVolumeWorkflow, volume)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

func (s *FlexCacheDeleteUnitTestSuite) Test_DeleteFlexCacheVolumeWorkflow_UpdateVolumeStateInDBFailure() {
	volume := CreateTestVolumeForDelete()

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(s.flexCacheVolumeDeleteActivity.UnmountVolumeInOntapActivity, mock.Anything, mock.Anything).Return(&flexcache.DeleteFlexCacheResult{
		UnmountJobResponse: &vsa.OntapAsyncResponse{JobUUID: ""},
	}, nil)
	s.env.OnActivity(s.commonActivity.GetOntapJob, mock.Anything, "", mock.Anything).Return(&vsa.OntapJob{State: "success"}, nil)
	s.env.OnActivity(s.flexCacheVolumeDeleteActivity.DeleteFlexCacheVolumeInOntapActivity, mock.Anything, mock.Anything).Return(&flexcache.DeleteFlexCacheResult{
		DeleteJobResponse: &vsa.OntapAsyncResponse{JobUUID: ""},
	}, nil)
	s.env.OnActivity(s.commonActivity.GetOntapJob, mock.Anything, "", mock.Anything).Return(&vsa.OntapJob{State: "success"}, nil)
	s.env.OnActivity(s.volumeDeleteActivity.DeleteVolume, mock.Anything, mock.Anything).Return(errors.New("some_error"))
	s.env.OnActivity(s.volumeCreateActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("some_error"))

	s.env.ExecuteWorkflow(DeleteFlexCacheVolumeWorkflow, volume)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

func (s *FlexCacheDeleteUnitTestSuite) Test_DeleteFlexCacheVolumeWorkflow_UpdateJobStatusFailureBeforeRun() {
	volume := CreateTestVolumeForDelete()

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("some_error"))

	s.env.ExecuteWorkflow(DeleteFlexCacheVolumeWorkflow, volume)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

func (s *FlexCacheDeleteUnitTestSuite) Test_DeleteFlexCacheVolumeWorkflow_UpdateJobStatusFailureAfterFailedRun() {
	volume := CreateTestVolumeForDelete()

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(func(ctx context.Context, job *datamodel.Job) error {
		if job.State == string(models.JobsStatePROCESSING) {
			return nil
		}

		return errors.New("some_error")
	})
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{}}, errors.New("some_error"))
	s.env.OnActivity(s.volumeCreateActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(DeleteFlexCacheVolumeWorkflow, volume)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

func (s *FlexCacheDeleteUnitTestSuite) Test_DeleteFlexCacheVolumeWorkflow_UpdateJobStatusFailureAfterCompletedRun() {
	volume := CreateTestVolumeForDelete()

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(func(ctx context.Context, job *datamodel.Job) error {
		if job.State == string(models.JobsStatePROCESSING) {
			return nil
		}

		return errors.New("some_error")
	})
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(s.flexCacheVolumeDeleteActivity.UnmountVolumeInOntapActivity, mock.Anything, mock.Anything).Return(&flexcache.DeleteFlexCacheResult{
		UnmountJobResponse: &vsa.OntapAsyncResponse{JobUUID: ""},
	}, nil)
	s.env.OnActivity(s.commonActivity.GetOntapJob, mock.Anything, "", mock.Anything).Return(&vsa.OntapJob{State: "success"}, nil)
	s.env.OnActivity(s.flexCacheVolumeDeleteActivity.DeleteFlexCacheVolumeInOntapActivity, mock.Anything, mock.Anything).Return(&flexcache.DeleteFlexCacheResult{
		DeleteJobResponse: &vsa.OntapAsyncResponse{JobUUID: ""},
	}, nil)
	s.env.OnActivity(s.commonActivity.GetOntapJob, mock.Anything, "", mock.Anything).Return(&vsa.OntapJob{State: "success"}, nil)
	s.env.OnActivity(s.flexCacheVolumeDeleteActivity.DeleteSVMPeeringInOntapActivity, mock.Anything, mock.Anything).Return(&flexcache.DeleteFlexCacheResult{}, nil)
	s.env.OnActivity(s.flexCacheVolumeDeleteActivity.DeleteClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(&flexcache.DeleteFlexCacheResult{}, nil)
	s.env.OnActivity(s.volumeDeleteActivity.DeleteVolume, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(DeleteFlexCacheVolumeWorkflow, volume)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

func TestFlexCacheDeleteUnitTestSuite(t *testing.T) {
	suite.Run(t, new(FlexCacheDeleteUnitTestSuite))
}
