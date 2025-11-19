package workflows

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

type VolumeSplitUnitTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env *testsuite.TestWorkflowEnvironment
}

func (s *VolumeSplitUnitTestSuite) SetupTest() {
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
	s.env.RegisterWorkflow(SplitVolumeWorkflow)
}

func (s *VolumeSplitUnitTestSuite) AfterTest() {
	s.env.AssertExpectations(s.T())
}

func (s *VolumeSplitUnitTestSuite) Test_SplitVolumeWorkflow_Success() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	volumeSplitActivity := activities.VolumeSplitActivity{SE: mockStorage}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			ID:   1,
			UUID: "volume-uuid",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "external-volume-uuid",
		},
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				ID: 1,
			},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
				AuthType:      env.USERNAME_PWD,
			},
			DeploymentName: "test-deployment",
		},
		Account: &datamodel.Account{
			Name: "test-account",
		},
	}

	// Register activities
	s.env.RegisterActivity(&commonActivity)
	s.env.RegisterActivity(&volumeCreateActivity)
	s.env.RegisterActivity(&volumeSplitActivity)

	// Mock storage method for UpdateVolumeStateInDB activity (using Maybe since activity is mocked)
	mockStorage.On("UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	// Set up mock expectations
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{
		{
			BaseModel: datamodel.BaseModel{
				ID: 1,
			},
			Name:            "test-node",
			EndpointAddress: "127.0.0.1",
		},
	}, nil)
	s.env.OnActivity(volumeCreateActivity.InitiateSplitForVolume, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeSplitActivity.UpdateCloneSharedBytesInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(SplitVolumeWorkflow, volume)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NoError(s.T(), s.env.GetWorkflowError())
}

func (s *VolumeSplitUnitTestSuite) Test_SplitVolumeWorkflow_GetNodeError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			ID:   1,
			UUID: "volume-uuid",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "external-volume-uuid",
		},
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				ID: 1,
			},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
				AuthType:      env.USERNAME_PWD,
			},
			DeploymentName: "test-deployment",
		},
		Account: &datamodel.Account{
			Name: "test-account",
		},
	}

	// Register activities
	s.env.RegisterActivity(&commonActivity)
	s.env.RegisterActivity(&volumeCreateActivity)

	// Mock storage method for UpdateVolumeStateInDB activity (using Maybe since activity is mocked)
	mockStorage.On("UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	// Set up mock expectations
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return(nil, errors.New("failed to get node"))
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(SplitVolumeWorkflow, volume)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
}

func (s *VolumeSplitUnitTestSuite) Test_SplitVolumeWorkflow_InitiateSplitForVolumeError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	volumeSplitActivity := activities.VolumeSplitActivity{SE: mockStorage}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			ID:   1,
			UUID: "volume-uuid",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "external-volume-uuid",
		},
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				ID: 1,
			},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
				AuthType:      env.USERNAME_PWD,
			},
			DeploymentName: "test-deployment",
		},
		Account: &datamodel.Account{
			Name: "test-account",
		},
	}

	// Register activities
	s.env.RegisterActivity(&commonActivity)
	s.env.RegisterActivity(&volumeCreateActivity)
	s.env.RegisterActivity(&volumeSplitActivity)

	// Mock storage method for UpdateVolumeStateInDB activity (using Maybe since activity is mocked)
	mockStorage.On("UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	// Set up mock expectations
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{
		{
			BaseModel: datamodel.BaseModel{
				ID: 1,
			},
			Name:            "test-node",
			EndpointAddress: "127.0.0.1",
		},
	}, nil)
	s.env.OnActivity(volumeCreateActivity.InitiateSplitForVolume, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to initiate split"))
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(SplitVolumeWorkflow, volume)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
}

func (s *VolumeSplitUnitTestSuite) Test_SplitVolumeWorkflow_UpdateCloneSharedBytesError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	volumeSplitActivity := activities.VolumeSplitActivity{SE: mockStorage}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			ID:   1,
			UUID: "volume-uuid",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "external-volume-uuid",
		},
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				ID: 1,
			},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
				AuthType:      env.USERNAME_PWD,
			},
			DeploymentName: "test-deployment",
		},
		Account: &datamodel.Account{
			Name: "test-account",
		},
	}

	// Register activities
	s.env.RegisterActivity(&commonActivity)
	s.env.RegisterActivity(&volumeCreateActivity)
	s.env.RegisterActivity(&volumeSplitActivity)

	// Mock storage method for UpdateVolumeStateInDB activity (using Maybe since activity is mocked)
	mockStorage.On("UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	// Set up mock expectations
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{
		{
			BaseModel: datamodel.BaseModel{
				ID: 1,
			},
			Name:            "test-node",
			EndpointAddress: "127.0.0.1",
		},
	}, nil)
	s.env.OnActivity(volumeCreateActivity.InitiateSplitForVolume, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeSplitActivity.UpdateCloneSharedBytesInDB, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to update clone shared bytes"))
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(SplitVolumeWorkflow, volume)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
}

func (s *VolumeSplitUnitTestSuite) Test_SplitVolumeWorkflow_UpdateJobStatusProcessingError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			ID:   1,
			UUID: "volume-uuid",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "external-volume-uuid",
		},
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				ID: 1,
			},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
				AuthType:      env.USERNAME_PWD,
			},
			DeploymentName: "test-deployment",
		},
		Account: &datamodel.Account{
			Name: "test-account",
		},
	}

	// Register activities
	s.env.RegisterActivity(&commonActivity)

	// Set up mock expectations - fail on UpdateJobStatus
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to update job status"))

	s.env.ExecuteWorkflow(SplitVolumeWorkflow, volume)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
}

func (s *VolumeSplitUnitTestSuite) Test_SplitVolumeWorkflow_UpdateJobStatusDoneError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	volumeSplitActivity := activities.VolumeSplitActivity{SE: mockStorage}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			ID:   1,
			UUID: "volume-uuid",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "external-volume-uuid",
		},
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				ID: 1,
			},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
				AuthType:      env.USERNAME_PWD,
			},
			DeploymentName: "test-deployment",
		},
		Account: &datamodel.Account{
			Name: "test-account",
		},
	}

	// Register activities
	s.env.RegisterActivity(&commonActivity)
	s.env.RegisterActivity(&volumeCreateActivity)
	s.env.RegisterActivity(&volumeSplitActivity)

	// Mock storage method for UpdateVolumeStateInDB activity (using Maybe since activity is mocked)
	mockStorage.On("UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	// Set up mock expectations
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once() // PROCESSING
	// DONE update fails - allow retries (Temporal will retry failed activities)
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to update job status"))
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{
		{
			BaseModel: datamodel.BaseModel{
				ID: 1,
			},
			Name:            "test-node",
			EndpointAddress: "127.0.0.1",
		},
	}, nil)
	s.env.OnActivity(volumeCreateActivity.InitiateSplitForVolume, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeSplitActivity.UpdateCloneSharedBytesInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(SplitVolumeWorkflow, volume)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	// When UpdateJobStatus for DONE fails, the workflow logs the error but still completes successfully (returns nil)
	assert.NoError(s.T(), s.env.GetWorkflowError())
}

func (s *VolumeSplitUnitTestSuite) Test_SplitVolumeWorkflow_UpdateJobStatusErrorDetailsError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	volumeSplitActivity := activities.VolumeSplitActivity{SE: mockStorage}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			ID:   1,
			UUID: "volume-uuid",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "external-volume-uuid",
		},
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				ID: 1,
			},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
				AuthType:      env.USERNAME_PWD,
			},
			DeploymentName: "test-deployment",
		},
		Account: &datamodel.Account{
			Name: "test-account",
		},
	}

	// Register activities
	s.env.RegisterActivity(&commonActivity)
	s.env.RegisterActivity(&volumeCreateActivity)
	s.env.RegisterActivity(&volumeSplitActivity)

	// Mock storage method for UpdateVolumeStateInDB activity (using Maybe since activity is mocked)
	mockStorage.On("UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	// Set up mock expectations
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once() // PROCESSING
	// ERROR update fails - allow retries (Temporal will retry failed activities)
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to update job status"))
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{
		{
			BaseModel: datamodel.BaseModel{
				ID: 1,
			},
			Name:            "test-node",
			EndpointAddress: "127.0.0.1",
		},
	}, nil)
	s.env.OnActivity(volumeCreateActivity.InitiateSplitForVolume, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("split volume failed"))
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(SplitVolumeWorkflow, volume)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
}

func (s *VolumeSplitUnitTestSuite) Test_SplitVolumeWorkflow_SetupError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}

	// Create a volume with nil Account to cause setup error
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			ID:   1,
			UUID: "volume-uuid",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "external-volume-uuid",
		},
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				ID: 1,
			},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
				AuthType:      env.USERNAME_PWD,
			},
			DeploymentName: "test-deployment",
		},
		// Account is nil to cause setup error
	}

	// Register activities
	s.env.RegisterActivity(&commonActivity)

	s.env.ExecuteWorkflow(SplitVolumeWorkflow, volume)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
}

func (s *VolumeSplitUnitTestSuite) Test_SplitVolumeWorkflow_WithCertificateAuth() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	volumeSplitActivity := activities.VolumeSplitActivity{SE: mockStorage}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			ID:   1,
			UUID: "volume-uuid",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "external-volume-uuid",
		},
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				ID: 1,
			},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "",
				SecretID:      "",
				CertificateID: "cert-id",
				AuthType:      env.USER_CERTIFICATE,
			},
			DeploymentName: "test-deployment",
		},
		Account: &datamodel.Account{
			Name: "test-account",
		},
	}

	// Register activities
	s.env.RegisterActivity(&commonActivity)
	s.env.RegisterActivity(&volumeCreateActivity)
	s.env.RegisterActivity(&volumeSplitActivity)

	// Mock storage method for UpdateVolumeStateInDB activity (using Maybe since activity is mocked)
	mockStorage.On("UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	// Set up mock expectations
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{
		{
			BaseModel: datamodel.BaseModel{
				ID: 1,
			},
			Name:            "test-node",
			EndpointAddress: "127.0.0.1",
		},
	}, nil)
	s.env.OnActivity(volumeCreateActivity.InitiateSplitForVolume, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeSplitActivity.UpdateCloneSharedBytesInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(SplitVolumeWorkflow, volume)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NoError(s.T(), s.env.GetWorkflowError())
}

func (s *VolumeSplitUnitTestSuite) Test_SplitVolumeWorkflow_WithSecretManagerAuth() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	volumeSplitActivity := activities.VolumeSplitActivity{SE: mockStorage}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			ID:   1,
			UUID: "volume-uuid",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "external-volume-uuid",
		},
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				ID: 1,
			},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "",
				SecretID:      "secret-id",
				CertificateID: "",
				AuthType:      env.USERNAME_PWD_SEC_MGR,
			},
			DeploymentName: "test-deployment",
		},
		Account: &datamodel.Account{
			Name: "test-account",
		},
	}

	// Register activities
	s.env.RegisterActivity(&commonActivity)
	s.env.RegisterActivity(&volumeCreateActivity)
	s.env.RegisterActivity(&volumeSplitActivity)

	// Mock storage method for UpdateVolumeStateInDB activity (using Maybe since activity is mocked)
	mockStorage.On("UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	// Set up mock expectations
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{
		{
			BaseModel: datamodel.BaseModel{
				ID: 1,
			},
			Name:            "test-node",
			EndpointAddress: "127.0.0.1",
		},
	}, nil)
	s.env.OnActivity(volumeCreateActivity.InitiateSplitForVolume, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeSplitActivity.UpdateCloneSharedBytesInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(SplitVolumeWorkflow, volume)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NoError(s.T(), s.env.GetWorkflowError())
}

func (s *VolumeSplitUnitTestSuite) Test_SplitVolumeWorkflow_UpdateVolumeStateInDBError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	volumeSplitActivity := activities.VolumeSplitActivity{SE: mockStorage}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			ID:   1,
			UUID: "volume-uuid",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "external-volume-uuid",
		},
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				ID: 1,
			},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
				AuthType:      env.USERNAME_PWD,
			},
			DeploymentName: "test-deployment",
		},
		Account: &datamodel.Account{
			Name: "test-account",
		},
	}

	// Register activities
	s.env.RegisterActivity(&commonActivity)
	s.env.RegisterActivity(&volumeCreateActivity)
	s.env.RegisterActivity(&volumeSplitActivity)

	// Mock storage method for UpdateVolumeStateInDB activity (using Maybe since activity is mocked)
	mockStorage.On("UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	// Set up mock expectations
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{
		{
			BaseModel: datamodel.BaseModel{
				ID: 1,
			},
			Name:            "test-node",
			EndpointAddress: "127.0.0.1",
		},
	}, nil)
	s.env.OnActivity(volumeCreateActivity.InitiateSplitForVolume, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("split volume failed"))
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to update volume state"))

	s.env.ExecuteWorkflow(SplitVolumeWorkflow, volume)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
}

func TestVolumeSplitUnitTestSuite(t *testing.T) {
	suite.Run(t, new(VolumeSplitUnitTestSuite))
}
