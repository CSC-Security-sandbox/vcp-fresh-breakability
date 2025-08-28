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
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

type FlexCacheUnitTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env *testsuite.TestWorkflowEnvironment

	// store activities to use in tests so we don't need to mock them every time
	commonActivity                *activities.CommonActivities
	volumeCreateActivity          *activities.VolumeCreateActivity
	flexCacheVolumeCreateActivity *flexcache_activities.FlexCacheVolumeCreateActivity
}

func (s *FlexCacheUnitTestSuite) SetupTest() {
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
	s.env.RegisterWorkflow(CreateFlexCacheWorkflow)

	// Register activities
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	flexCacheVolumeCreateActivity := flexcache_activities.FlexCacheVolumeCreateActivity{SE: mockStorage}

	s.commonActivity = &commonActivity
	s.volumeCreateActivity = &volumeCreateActivity
	s.flexCacheVolumeCreateActivity = &flexCacheVolumeCreateActivity

	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(commonActivity.GetNode)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeStateInDB)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)
	s.env.RegisterActivity(flexCacheVolumeCreateActivity.CreateFlexCacheVolumeInOntapActivity)
}

func CreateTestVolume() *datamodel.Volume {
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

func (s *FlexCacheUnitTestSuite) AfterTest() {
	s.env.AssertExpectations(s.T())
}

func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_Success() {
	volume := CreateTestVolume()

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateFlexCacheVolumeInOntapActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{}, volume)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}

func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_GetNodeFailure() {
	volume := CreateTestVolume()

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return(nil, errors.New("some_error"))
	s.env.OnActivity(s.volumeCreateActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{}, volume)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_CreateFlexCacheVolumeFailure() {
	volume := CreateTestVolume()

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{
		BaseModel:       datamodel.BaseModel{ID: 1},
		EndpointAddress: "127.0.0.1",
	}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateFlexCacheVolumeInOntapActivity, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("some_error"))
	s.env.OnActivity(s.volumeCreateActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{AccountName: "test-account"}, volume)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_UpdateVolumeDetailsFailure() {
	volume := CreateTestVolume()

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: ""}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateFlexCacheVolumeInOntapActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("some_error"))
	s.env.OnActivity(s.volumeCreateActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{}, volume)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_UpdateVolumeStateInDBFailure() {
	volume := CreateTestVolume()

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: ""}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateFlexCacheVolumeInOntapActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("some_error"))
	s.env.OnActivity(s.volumeCreateActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("some_error"))

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{}, volume)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_UpdateJobStatusFailureBeforeRun() {
	volume := CreateTestVolume()

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("some_error"))

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{}, volume)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_UpdateJobStatusFailureAfterFailedRun() {
	volume := CreateTestVolume()

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(func(ctx context.Context, job *datamodel.Job) error {
		if job.State == string(models.JobsStatePROCESSING) {
			return nil
		}

		return errors.New("some_error")
	})
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{}}, errors.New("some_error"))
	s.env.OnActivity(s.volumeCreateActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{}, volume)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_UpdateJobStatusFailureAfterCompletedRun() {
	volume := CreateTestVolume()

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(func(ctx context.Context, job *datamodel.Job) error {
		if job.State == string(models.JobsStatePROCESSING) {
			return nil
		}

		return errors.New("some_error")
	})
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateFlexCacheVolumeInOntapActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{}, volume)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

func TestFlexCacheUnitTestSuite(t *testing.T) {
	suite.Run(t, new(FlexCacheUnitTestSuite))
}
