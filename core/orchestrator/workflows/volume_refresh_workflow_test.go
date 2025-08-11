package workflows

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

type VolumeGetWorkflowTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env *testsuite.TestWorkflowEnvironment
}

func (s *VolumeGetWorkflowTestSuite) SetupTest() {
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
	s.env.RegisterWorkflow(VolumeRefreshWorkflow)
}

func (s *VolumeGetWorkflowTestSuite) AfterTest() {
	s.env.AssertExpectations(s.T())
}

func (s *VolumeGetWorkflowTestSuite) Test_GetVolumeWorkflow_Success() {
	// Test successful volume get workflow execution
	mockStorage := database.NewMockStorage(s.T())

	// Create test volume
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "test-volume-uuid",
		},
		Name: "test-volume",
		Account: &datamodel.Account{
			Name: "test-account",
		},
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
		},
		Svm: &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "external-uuid",
		},
	}

	// Mock activities
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeUpdateActivity := activities.VolumeUpdateActivity{SE: mockStorage}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(commonActivity.GetNode)
	s.env.RegisterActivity(volumeUpdateActivity.GetVolumeFromONTAP)
	s.env.RegisterActivity(volumeUpdateActivity.RefreshVolumeFieldsInDB)

	// Mock expectations
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Mock GetNode activity
	dbNodes := []*datamodel.Node{
		{
			BaseModel:       datamodel.BaseModel{ID: int64(1)},
			EndpointAddress: "192.168.1.1",
		},
	}
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return(dbNodes, nil)

	// Mock GetVolumeFromONTAPAgain activity
	volumeResponse := &vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			ExternalUUID: "external-uuid",
		},
		UsedBytes: 1024,
	}
	s.env.OnActivity(volumeUpdateActivity.GetVolumeFromONTAP, mock.Anything, mock.Anything, mock.Anything).Return(volumeResponse, nil)

	// Mock RefreshVolumeFieldsInDB activity
	s.env.OnActivity(volumeUpdateActivity.RefreshVolumeFieldsInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(VolumeRefreshWorkflow, volume)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NoError(s.T(), s.env.GetWorkflowError())
}

func (s *VolumeGetWorkflowTestSuite) Test_GetVolumeWorkflow_SetupError() {
	// Test workflow setup error
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "test-volume-uuid",
		},
		Name:    "test-volume",
		Account: nil, // This will cause setup to fail
	}

	// Execute workflow
	s.env.ExecuteWorkflow(VolumeRefreshWorkflow, volume)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
}

func (s *VolumeGetWorkflowTestSuite) Test_GetVolumeWorkflow_UpdateJobStatusProcessingError() {
	// Test error during job status update to PROCESSING
	mockStorage := database.NewMockStorage(s.T())

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "test-volume-uuid",
		},
		Name: "test-volume",
		Account: &datamodel.Account{
			Name: "test-account",
		},
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
		},
		Svm: &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "external-uuid",
		},
	}

	// Mock activities
	commonActivity := activities.CommonActivities{SE: mockStorage}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)

	// Mock UpdateJobStatus to return error for PROCESSING state
	expectedError := errors.New("failed to update job status to PROCESSING")
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.MatchedBy(func(job *datamodel.Job) bool {
		return job.State == string(models.JobsStatePROCESSING)
	})).Return(expectedError)

	// Execute workflow
	s.env.ExecuteWorkflow(VolumeRefreshWorkflow, volume)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), expectedError.Error())
}

func (s *VolumeGetWorkflowTestSuite) Test_GetVolumeWorkflow_GetNodeError() {
	// Test error during GetNode activity
	mockStorage := database.NewMockStorage(s.T())

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "test-volume-uuid",
		},
		Name: "test-volume",
		Account: &datamodel.Account{
			Name: "test-account",
		},
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
		},
		Svm: &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "external-uuid",
		},
	}

	// Mock activities
	commonActivity := activities.CommonActivities{SE: mockStorage}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(commonActivity.GetNode)

	// Mock UpdateJobStatus to succeed for PROCESSING state
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.MatchedBy(func(job *datamodel.Job) bool {
		return job.State == string(models.JobsStatePROCESSING)
	})).Return(nil)

	// Mock UpdateJobStatus to succeed for DONE state with error
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.MatchedBy(func(job *datamodel.Job) bool {
		return job.State == string(models.JobsStateDONE) && job.ErrorDetails != ""
	})).Return(nil)

	// Mock GetNode to return error
	expectedError := errors.New("failed to get node")
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return(nil, expectedError)

	// Execute workflow
	s.env.ExecuteWorkflow(VolumeRefreshWorkflow, volume)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), expectedError.Error())
}

func (s *VolumeGetWorkflowTestSuite) Test_GetVolumeWorkflow_GetVolumeFromONTAPAgainError() {
	// Test error during GetVolumeFromONTAPAgain activity
	mockStorage := database.NewMockStorage(s.T())

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "test-volume-uuid",
		},
		Name: "test-volume",
		Account: &datamodel.Account{
			Name: "test-account",
		},
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
		},
		Svm: &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "external-uuid",
		},
	}

	// Mock activities
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeUpdateActivity := activities.VolumeUpdateActivity{SE: mockStorage}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(commonActivity.GetNode)
	s.env.RegisterActivity(volumeUpdateActivity.GetVolumeFromONTAP)

	// Mock UpdateJobStatus to succeed for PROCESSING state
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.MatchedBy(func(job *datamodel.Job) bool {
		return job.State == string(models.JobsStatePROCESSING)
	})).Return(nil)

	// Mock UpdateJobStatus to succeed for DONE state with error
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.MatchedBy(func(job *datamodel.Job) bool {
		return job.State == string(models.JobsStateDONE) && job.ErrorDetails != ""
	})).Return(nil)

	// Mock GetNode to succeed
	dbNodes := []*datamodel.Node{
		{
			BaseModel:       datamodel.BaseModel{ID: int64(1)},
			EndpointAddress: "192.168.1.1",
		},
	}
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return(dbNodes, nil)

	// Mock GetVolumeFromONTAPAgain to return error
	expectedError := errors.New("failed to get volume from ONTAP")
	s.env.OnActivity(volumeUpdateActivity.GetVolumeFromONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil, expectedError)

	// Execute workflow
	s.env.ExecuteWorkflow(VolumeRefreshWorkflow, volume)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), expectedError.Error())
}

func (s *VolumeGetWorkflowTestSuite) Test_GetVolumeWorkflow_UpdateVolumeUsedBytesError() {
	// Test error during RefreshVolumeFieldsInDB activity
	mockStorage := database.NewMockStorage(s.T())

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "test-volume-uuid",
		},
		Name: "test-volume",
		Account: &datamodel.Account{
			Name: "test-account",
		},
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
		},
		Svm: &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "external-uuid",
		},
	}

	// Mock activities
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeUpdateActivity := activities.VolumeUpdateActivity{SE: mockStorage}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(commonActivity.GetNode)
	s.env.RegisterActivity(volumeUpdateActivity.GetVolumeFromONTAP)
	s.env.RegisterActivity(volumeUpdateActivity.RefreshVolumeFieldsInDB)

	// Mock UpdateJobStatus to succeed for PROCESSING state
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.MatchedBy(func(job *datamodel.Job) bool {
		return job.State == string(models.JobsStatePROCESSING)
	})).Return(nil)

	// Mock UpdateJobStatus to succeed for DONE state with error
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.MatchedBy(func(job *datamodel.Job) bool {
		return job.State == string(models.JobsStateDONE) && job.ErrorDetails != ""
	})).Return(nil)

	// Mock GetNode to succeed
	dbNodes := []*datamodel.Node{
		{
			BaseModel:       datamodel.BaseModel{ID: int64(1)},
			EndpointAddress: "192.168.1.1",
		},
	}
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return(dbNodes, nil)

	// Mock GetVolumeFromONTAPAgain to succeed
	volumeResponse := &vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			ExternalUUID: "external-uuid",
		},
		UsedBytes: 1024,
	}
	s.env.OnActivity(volumeUpdateActivity.GetVolumeFromONTAP, mock.Anything, mock.Anything, mock.Anything).Return(volumeResponse, nil)

	// Mock RefreshVolumeFieldsInDB to return error
	expectedError := errors.New("failed to update volume used bytes")
	s.env.OnActivity(volumeUpdateActivity.RefreshVolumeFieldsInDB, mock.Anything, mock.Anything, mock.Anything).Return(expectedError)

	// Execute workflow
	s.env.ExecuteWorkflow(VolumeRefreshWorkflow, volume)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), expectedError.Error())
}

func (s *VolumeGetWorkflowTestSuite) Test_GetVolumeWorkflow_UpdateJobStatusDoneError() {
	// Test error during final job status update to DONE
	mockStorage := database.NewMockStorage(s.T())

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "test-volume-uuid",
		},
		Name: "test-volume",
		Account: &datamodel.Account{
			Name: "test-account",
		},
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
		},
		Svm: &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "external-uuid",
		},
	}

	// Mock activities
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeUpdateActivity := activities.VolumeUpdateActivity{SE: mockStorage}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(commonActivity.GetNode)
	s.env.RegisterActivity(volumeUpdateActivity.GetVolumeFromONTAP)
	s.env.RegisterActivity(volumeUpdateActivity.RefreshVolumeFieldsInDB)

	// Mock UpdateJobStatus to succeed for PROCESSING state
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.MatchedBy(func(job *datamodel.Job) bool {
		return job.State == string(models.JobsStatePROCESSING)
	})).Return(nil)

	// Mock UpdateJobStatus to fail for DONE state (successful completion)
	expectedError := errors.New("failed to update job status to DONE")
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.MatchedBy(func(job *datamodel.Job) bool {
		return job.State == string(models.JobsStateDONE) && job.ErrorDetails == ""
	})).Return(expectedError)

	// Mock GetNode to succeed
	dbNodes := []*datamodel.Node{
		{
			BaseModel:       datamodel.BaseModel{ID: int64(1)},
			EndpointAddress: "192.168.1.1",
		},
	}
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return(dbNodes, nil)

	// Mock GetVolumeFromONTAPAgain to succeed
	volumeResponse := &vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			ExternalUUID: "external-uuid",
		},
		UsedBytes: 1024,
	}
	s.env.OnActivity(volumeUpdateActivity.GetVolumeFromONTAP, mock.Anything, mock.Anything, mock.Anything).Return(volumeResponse, nil)

	// Mock RefreshVolumeFieldsInDB to succeed
	s.env.OnActivity(volumeUpdateActivity.RefreshVolumeFieldsInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(VolumeRefreshWorkflow, volume)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), expectedError.Error())
}

func (s *VolumeGetWorkflowTestSuite) Test_GetVolumeWorkflow_UpdateJobStatusErrorDetailsError() {
	// Test error during job status update with error details
	mockStorage := database.NewMockStorage(s.T())

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "test-volume-uuid",
		},
		Name: "test-volume",
		Account: &datamodel.Account{
			Name: "test-account",
		},
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
		},
		Svm: &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "external-uuid",
		},
	}

	// Mock activities
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeUpdateActivity := activities.VolumeUpdateActivity{SE: mockStorage}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(commonActivity.GetNode)
	s.env.RegisterActivity(volumeUpdateActivity.GetVolumeFromONTAP)

	// Mock UpdateJobStatus to succeed for PROCESSING state
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.MatchedBy(func(job *datamodel.Job) bool {
		return job.State == string(models.JobsStatePROCESSING)
	})).Return(nil)

	// Mock UpdateJobStatus to fail for DONE state with error details
	errorDetailsUpdateError := errors.New("failed to update job status with error details")
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.MatchedBy(func(job *datamodel.Job) bool {
		return job.State == string(models.JobsStateERROR) && job.ErrorDetails != ""
	})).Return(errorDetailsUpdateError)

	// Mock GetNode to succeed
	dbNodes := []*datamodel.Node{
		{
			BaseModel:       datamodel.BaseModel{ID: int64(1)},
			EndpointAddress: "192.168.1.1",
		},
	}
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return(dbNodes, nil)

	// Mock GetVolumeFromONTAPAgain to return error
	ontapError := errors.New("failed to get volume from ONTAP")
	s.env.OnActivity(volumeUpdateActivity.GetVolumeFromONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil, ontapError)

	// Execute workflow
	s.env.ExecuteWorkflow(VolumeRefreshWorkflow, volume)

	// Assert workflow failed with the error details update error
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), errorDetailsUpdateError.Error())
}

func TestVolumeGetWorkflowTestSuite(t *testing.T) {
	suite.Run(t, new(VolumeGetWorkflowTestSuite))
}
