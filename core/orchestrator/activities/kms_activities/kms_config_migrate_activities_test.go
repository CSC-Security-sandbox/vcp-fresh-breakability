package kms_activities

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/async"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/kms_configurations"
	cvpModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"go.temporal.io/sdk/testsuite"
)

func TestMigrateSdeKmsConfigActivity(t *testing.T) {
	t.Run("MigrateSdeKmsConfigActivityReturnsOperationStatus", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &KmsConfigActivity{SE: mockSE}
		params := common.MigrateKmsConfigParams{}
		mockClient := kms_configurations.NewMockClientService(t)
		mockResponse := &kms_configurations.V1betaEncryptVolumesAccepted{
			Payload: &cvpModels.OperationV1beta{
				Name: "operation-path",
				Done: nillable.GetBoolPtr(false),
				Response: cvpModels.EncryptVolumeStatusV1beta{
					UUID:   "kmsconfig-uuid",
					Status: "UPDATING",
				}},
		}

		mockClient.EXPECT().
			V1betaEncryptVolumes(mock.Anything).
			Return(mockResponse, nil)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.MigrateSdeKmsConfigActivity)
		result, err := env.ExecuteActivity(activity.MigrateSdeKmsConfigActivity, &params)

		assert.NoError(tt, err)
		var cvpResponse *kms_configurations.V1betaEncryptVolumesAccepted
		err = result.Get(&cvpResponse)
		if err != nil {
			tt.Fatalf("failed to get result: %v", err)
		}
		assert.NotNil(tt, cvpResponse)
		assert.NotNil(tt, cvpResponse.Payload)
		// When Temporal deserializes, Response (interface{}) becomes map[string]interface{}
		// We need to handle the type conversion properly
		responseMap, ok := cvpResponse.Payload.Response.(map[string]interface{})
		if !ok {
			// Try direct type assertion first
			response, ok := cvpResponse.Payload.Response.(cvpModels.EncryptVolumeStatusV1beta)
			if ok {
				assert.Equal(tt, "kmsconfig-uuid", response.UUID)
				assert.Equal(tt, "UPDATING", response.Status)
			} else {
				tt.Fatalf("unexpected response type: %T", cvpResponse.Payload.Response)
			}
		} else {
			// Handle map[string]interface{} case - JSON tags are "UUID" and "status"
			uuidVal, uuidOk := responseMap["UUID"].(string)
			if !uuidOk {
				// Try lowercase as fallback
				uuidVal, uuidOk = responseMap["uuid"].(string)
			}
			assert.True(tt, uuidOk, "UUID should be present in response")
			assert.Equal(tt, "kmsconfig-uuid", uuidVal)

			statusVal, statusOk := responseMap["status"].(string)
			assert.True(tt, statusOk, "status should be present in response")
			assert.Equal(tt, "UPDATING", statusVal)
		}
	})
	t.Run("MigrateSdeKmsConfigActivityReturnsNilPayload", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &KmsConfigActivity{SE: mockSE}
		params := common.MigrateKmsConfigParams{}
		mockClient := kms_configurations.NewMockClientService(t)
		mockResponse := &kms_configurations.V1betaEncryptVolumesAccepted{Payload: nil}

		mockClient.EXPECT().
			V1betaEncryptVolumes(mock.Anything).
			Return(mockResponse, nil)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.MigrateSdeKmsConfigActivity)
		_, err := env.ExecuteActivity(activity.MigrateSdeKmsConfigActivity, &params)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Error encountered during SDE CMEK migration: CVP response is empty")
	})
	t.Run("MigrateSdeKmsConfigActivityReturnsNil", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &KmsConfigActivity{SE: mockSE}
		params := common.MigrateKmsConfigParams{}
		mockClient := kms_configurations.NewMockClientService(t)

		mockClient.EXPECT().
			V1betaEncryptVolumes(mock.Anything).
			Return(nil, nil)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.MigrateSdeKmsConfigActivity)
		_, err := env.ExecuteActivity(activity.MigrateSdeKmsConfigActivity, &params)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Error encountered during SDE CMEK migration: CVP response is empty")
	})
	t.Run("MigrateSdeKmsConfigActivityReturnsError", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &KmsConfigActivity{SE: mockSE}
		params := common.MigrateKmsConfigParams{}
		mockClient := kms_configurations.NewMockClientService(t)

		mockClient.EXPECT().
			V1betaEncryptVolumes(mock.Anything).
			Return(nil, errors.New("migration ran into an error"))
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.MigrateSdeKmsConfigActivity)
		_, err := env.ExecuteActivity(activity.MigrateSdeKmsConfigActivity, &params)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Error migrating SDE KMS Configuration (type: DescribeOperationError, retryable: false): migration ran into an error")
	})
}

func TestPollMigrateSdeKmsConfigActivity(t *testing.T) {
	t.Run("PollMigrateSdeKmsConfigActivityGetsNilResponse", func(tt *testing.T) {
		activity := &KmsConfigActivity{}
		params := common.MigrateKmsConfigParams{}
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.PollMigrateSdeKmsConfigActivity)

		_, errPoll := env.ExecuteActivity(activity.PollMigrateSdeKmsConfigActivity, &params, nil)

		assert.Error(tt, errPoll)
		assert.Contains(tt, errPoll.Error(), "Error migrating SDE KMS Configuration")
		assert.Contains(tt, errPoll.Error(), "SDE CMEK migration error")
	})
	t.Run("PollMigrateSdeKmsConfigActivityGetsNilPayload", func(tt *testing.T) {
		activity := &KmsConfigActivity{}
		params := common.MigrateKmsConfigParams{}
		response := kms_configurations.V1betaEncryptVolumesAccepted{Payload: nil}
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.PollMigrateSdeKmsConfigActivity)

		_, errPoll := env.ExecuteActivity(activity.PollMigrateSdeKmsConfigActivity, &params, &response)

		assert.Error(tt, errPoll)
		assert.Contains(tt, errPoll.Error(), "Error migrating SDE KMS Configuration")
		assert.Contains(tt, errPoll.Error(), "SDE CMEK migration error")
	})
	t.Run("PollMigrateSdeKmsConfigActivityReturnsDoneInPayload", func(tt *testing.T) {
		activity := &KmsConfigActivity{}
		params := common.MigrateKmsConfigParams{
			ProjectNumber: "projectNumber",
			LocationID:    "locationID",
		}
		doneStatus := true
		response := kms_configurations.V1betaEncryptVolumesAccepted{Payload: &cvpModels.OperationV1beta{Done: &doneStatus, Name: "operationName"}}
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.PollMigrateSdeKmsConfigActivity)

		_, errPoll := env.ExecuteActivity(activity.PollMigrateSdeKmsConfigActivity, &params, &response)

		assert.NoError(tt, errPoll)
	})
	t.Run("WhenPollCvpOperationForWorkflowReturnsError", func(tt *testing.T) {
		activity := &KmsConfigActivity{}
		params := common.MigrateKmsConfigParams{
			ProjectNumber: "projectNumber",
			LocationID:    "locationID",
		}
		doneStatus := false
		response := kms_configurations.V1betaEncryptVolumesAccepted{Payload: &cvpModels.OperationV1beta{Done: &doneStatus}}

		originalGetSignedJwtToken := getSignedJwtToken
		defer func() {
			pollCvpOperationForWorkflow = _pollCvpOperationForWorkflow
			getSignedJwtToken = originalGetSignedJwtToken
		}()
		getSignedJwtToken = func(projectNumber string) (string, error) {
			return "mock-jwt-token", nil
		}
		pollCvpOperationForWorkflow = func(ctx context.Context, cvpClient cvpapi.Cvp, operationParams *async.V1betaDescribeOperationParams) (*cvpModels.OperationV1beta, error) {
			return nil, errors.New("CVP Polling is returning error to flag that polling needs to continue")
		}
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.PollMigrateSdeKmsConfigActivity)
		_, errPoll := env.ExecuteActivity(activity.PollMigrateSdeKmsConfigActivity, &params, &response)

		assert.Error(tt, errPoll)
		assert.Contains(tt, errPoll.Error(), "CVP Polling is returning error to flag that polling needs to continue")
	})
	t.Run("WhenPollCvpOperationForWorkflowReturnsWithoutError", func(tt *testing.T) {
		activity := &KmsConfigActivity{}
		params := common.MigrateKmsConfigParams{
			ProjectNumber: "projectNumber",
			LocationID:    "locationID",
		}
		doneStatus := false
		response := kms_configurations.V1betaEncryptVolumesAccepted{Payload: &cvpModels.OperationV1beta{Done: &doneStatus}}

		originalGetSignedJwtToken := getSignedJwtToken
		defer func() {
			pollCvpOperationForWorkflow = _pollCvpOperationForWorkflow
			getSignedJwtToken = originalGetSignedJwtToken
		}()
		getSignedJwtToken = func(projectNumber string) (string, error) {
			return "mock-jwt-token", nil
		}
		pollCvpOperationForWorkflow = func(ctx context.Context, cvpClient cvpapi.Cvp, operationParams *async.V1betaDescribeOperationParams) (*cvpModels.OperationV1beta, error) {
			return nil, nil
		}
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.PollMigrateSdeKmsConfigActivity)
		_, errPoll := env.ExecuteActivity(activity.PollMigrateSdeKmsConfigActivity, &params, &response)

		assert.NoError(tt, errPoll)
	})
}

func TestMigrateVsaPoolActivity(t *testing.T) {
	var volumes []*datamodel.Volume
	vol1 := datamodel.Volume{
		BaseModel:        datamodel.BaseModel{UUID: "uuid1"},
		Name:             "vol1",
		VolumeAttributes: &datamodel.VolumeAttributes{ExternalUUID: "externalUUID"},
		Svm:              &datamodel.Svm{Name: "svmName"},
		State:            models.LifeCycleStateREADY,
		StateDetails:     models.LifeCycleStateReadyDetails,
	}
	volumes = append(volumes, &vol1)
	node := &models.Node{Name: "nodeName"}

	t.Run("WhenVolumeStateIsNotValidForMigration", func(tt *testing.T) {
		activity := &KmsConfigActivity{}
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		tests := []struct {
			name         string
			state        string
			stateDetails string
		}{
			{
				name:         "NotInReadyState",
				state:        models.LifeCycleStateError,
				stateDetails: models.LifeCycleStateUpdateErrorDetails,
			},
			{
				name:         "InUpdatingStateButNotMigrating",
				state:        models.LifeCycleStateUpdating,
				stateDetails: models.LifeCycleStateUpdatingDetails,
			},
		}

		for _, test := range tests {
			tt.Run(test.name, func(t *testing.T) {
				vol := datamodel.Volume{
					Name:         "volumeName",
					State:        test.state,
					StateDetails: test.stateDetails,
				}
				var volumesErr []*datamodel.Volume
				volumesErr = append(volumesErr, &vol)

				testSuite := &testsuite.WorkflowTestSuite{}
				env := testSuite.NewTestActivityEnvironment()
				env.RegisterActivity(activity.MigrateVsaPoolActivity)
				_, errGetVolume := env.ExecuteActivity(activity.MigrateVsaPoolActivity, volumesErr, node)
				assert.Error(t, errGetVolume)
				assert.Contains(t, errGetVolume.Error(), "Encryption failed for one/some of the volumes (type: CmekVolumeMigrationError, retryable: false): Volume encryption failure")
			})
		}
	})
	t.Run("WhenVolumeAttributesIsNil", func(tt *testing.T) {
		activity := &KmsConfigActivity{}
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		vol2 := datamodel.Volume{Name: "vol2",
			VolumeAttributes: nil,
			Svm:              &datamodel.Svm{Name: "svmName"},
			State:            models.LifeCycleStateUpdating,
			StateDetails:     models.LifeCycleStateVolMigratingDetails,
		}
		var volumesErr []*datamodel.Volume
		volumesErr = append(volumesErr, &vol2)

		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.MigrateVsaPoolActivity)
		_, errGetVolume := env.ExecuteActivity(activity.MigrateVsaPoolActivity, volumesErr, node)
		assert.Error(tt, errGetVolume)
		assert.Contains(tt, errGetVolume.Error(), "Encryption failed for one/some of the volumes (type: CmekVolumeMigrationError, retryable: false): Volume encryption failure")
	})
	t.Run("WhenVolumeSvmIsNil", func(tt *testing.T) {
		activity := &KmsConfigActivity{}
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		var volumesSvmNil []*datamodel.Volume
		vol3 := datamodel.Volume{Name: "vol3",
			VolumeAttributes: &datamodel.VolumeAttributes{ExternalUUID: "externalUUID"},
			Svm:              nil,
			State:            models.LifeCycleStateREADY,
			StateDetails:     models.LifeCycleStateReadyDetails,
		}
		volumesSvmNil = append(volumesSvmNil, &vol3)

		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.MigrateVsaPoolActivity)
		_, errGetVolume := env.ExecuteActivity(activity.MigrateVsaPoolActivity, volumesSvmNil, node)
		assert.Error(tt, errGetVolume)
		assert.Contains(tt, errGetVolume.Error(), "Encryption failed for one/some of the volumes (type: CmekVolumeMigrationError, retryable: false): Volume encryption failure")
	})
	t.Run("WhenGetVolumeEncryptionReturnsStateAsEncryptedWithGetVolumeErr", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockSE := database.NewMockStorage(tt)
		activity := &KmsConfigActivity{SE: mockSE}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		encrypted := "encrypted"
		getEncryptionStatus := vsa.VolumeResponse{
			ProviderResponse: vsa.ProviderResponse{},
			Encryption:       vsa.Encryption{State: &encrypted},
		}
		mockProvider.On("GetVolumeEncryptionStatus", mock.Anything).Return(&getEncryptionStatus, nil)
		mockSE.On("GetVolume", mock.Anything, mock.Anything).Return(nil, errors.New("GetVolume"))
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.MigrateVsaPoolActivity)
		_, errMigrate := env.ExecuteActivity(activity.MigrateVsaPoolActivity, volumes, node)

		assert.NoError(tt, errMigrate)
	})
	t.Run("WhenGetVolumeEncryptionReturnsStateAsEncryptedWithUpdateVolumeErr", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockSE := database.NewMockStorage(tt)
		activity := &KmsConfigActivity{SE: mockSE}
		volDataModel := datamodel.Volume{Name: "volume",
			State:        models.LifeCycleStateUpdating,
			StateDetails: models.LifeCycleStateVolMigratingDetails,
		}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		encrypted := "encrypted"
		getEncryptionStatus := vsa.VolumeResponse{
			ProviderResponse: vsa.ProviderResponse{},
			Encryption:       vsa.Encryption{State: &encrypted},
		}
		mockProvider.On("GetVolumeEncryptionStatus", mock.Anything).Return(&getEncryptionStatus, nil)
		mockSE.On("GetVolume", mock.Anything, mock.Anything).Return(&volDataModel, nil)
		mockSE.On("UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("UpdateVolume error"))
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.MigrateVsaPoolActivity)
		_, errMigrate := env.ExecuteActivity(activity.MigrateVsaPoolActivity, volumes, node)

		assert.NoError(tt, errMigrate)
	})
	t.Run("WhenGetVolumeEncryptionReturnsStateAsEncrypted", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockSE := database.NewMockStorage(tt)
		activity := &KmsConfigActivity{SE: mockSE}
		volDataModel := datamodel.Volume{Name: "volume",
			State:        models.LifeCycleStateREADY,
			StateDetails: models.LifeCycleStateReadyDetails,
		}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		encrypted := "encrypted"
		getEncryptionStatus := vsa.VolumeResponse{
			ProviderResponse: vsa.ProviderResponse{},
			Encryption:       vsa.Encryption{State: &encrypted},
		}
		mockProvider.On("GetVolumeEncryptionStatus", mock.Anything).Return(&getEncryptionStatus, nil)
		mockSE.On("GetVolume", mock.Anything, mock.Anything).Return(&volDataModel, nil)
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.MigrateVsaPoolActivity)
		_, errMigrate := env.ExecuteActivity(activity.MigrateVsaPoolActivity, volumes, node)

		assert.NoError(tt, errMigrate)
	})
	t.Run("WhenGetVolumeEncryptionReturnsStateAsEncryptedForFunctionReEntry", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockSE := database.NewMockStorage(tt)
		activity := &KmsConfigActivity{SE: mockSE}
		volDataModel := datamodel.Volume{Name: "volume",
			State:        models.LifeCycleStateUpdating,
			StateDetails: models.LifeCycleStateVolMigratingDetails,
		}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		encrypted := "encrypted"
		getEncryptionStatus := vsa.VolumeResponse{
			ProviderResponse: vsa.ProviderResponse{},
			Encryption:       vsa.Encryption{State: &encrypted},
		}
		mockProvider.On("GetVolumeEncryptionStatus", mock.Anything).Return(&getEncryptionStatus, nil)
		mockSE.On("GetVolume", mock.Anything, mock.Anything).Return(&volDataModel, nil)
		mockSE.On("UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.MigrateVsaPoolActivity)
		_, errMigrate := env.ExecuteActivity(activity.MigrateVsaPoolActivity, volumes, node)

		assert.NoError(tt, errMigrate)
	})
	t.Run("WhenGetVolumeEncryptionReturnsStateAsEncrypting", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockSE := database.NewMockStorage(tt)
		activity := &KmsConfigActivity{SE: mockSE}
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.MigrateVsaPoolActivity)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		encryptionState := "encrypting"
		response := vsa.VolumeResponse{
			AvailableSpace: 1000,
			Size:           1024,
			Encryption:     vsa.Encryption{State: &encryptionState},
		}

		mockProvider.On("GetVolumeEncryptionStatus", mock.Anything).Return(&response, nil).Once()
		mockSE.On("UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockProvider.On("UpdateVolumeEnableEncryption", mock.Anything).Return(nil)
		mockProvider.On("GetVolumeEncryptionStatus", mock.Anything).Return(&response, nil)

		var err error
		done := make(chan struct{})
		go func() {
			defer close(done)
			_, err = env.ExecuteActivity(activity.MigrateVsaPoolActivity, volumes, node)
			if err != nil {
				t.Errorf("Function failed: %v", err)
			}
		}()

		select {
		case <-done:
			assert.Fail(tt, "Migrate function exited loop even though status was encrypting")
		case <-time.After(5 * time.Second):
			assert.Nil(tt, err)
		}
	})
	t.Run("WhenUpdateVolumeEnableEncryptionReturnsVolumeAlreadyMigratedError", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockSE := database.NewMockStorage(tt)
		activity := &KmsConfigActivity{SE: mockSE}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		encrypted := "unencrypted"
		getEncryptionStatus := vsa.VolumeResponse{
			ProviderResponse: vsa.ProviderResponse{},
			Encryption:       vsa.Encryption{State: &encrypted},
		}
		mockProvider.On("GetVolumeEncryptionStatus", mock.Anything).Return(&getEncryptionStatus, nil)
		mockProvider.On("UpdateVolumeEnableEncryption", mock.Anything).Return(errors.New("reason: Volume is encrypted"))

		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.MigrateVsaPoolActivity)
		_, errMigrate := env.ExecuteActivity(activity.MigrateVsaPoolActivity, volumes, node)
		assert.NoError(tt, errMigrate)
	})
	t.Run("WhenUpdateVolumeEnableEncryptionReturnsError", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockSE := database.NewMockStorage(tt)
		activity := &KmsConfigActivity{SE: mockSE}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		encrypted := "unencrypted"
		getEncryptionStatus := vsa.VolumeResponse{
			ProviderResponse: vsa.ProviderResponse{},
			Encryption:       vsa.Encryption{State: &encrypted},
		}
		mockProvider.On("GetVolumeEncryptionStatus", mock.Anything).Return(&getEncryptionStatus, nil)
		mockProvider.On("UpdateVolumeEnableEncryption", mock.Anything).Return(errors.New("volume encryption error"))

		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.MigrateVsaPoolActivity)
		_, errMigrate := env.ExecuteActivity(activity.MigrateVsaPoolActivity, volumes, node)

		assert.Error(tt, errMigrate)
		assert.Contains(tt, errMigrate.Error(), "Encryption failed for one/some of the volumes (type: CmekVolumeMigrationError, retryable: false): Volume encryption failure")
	})
	t.Run("WhenGetVolumeEncryptionStatusReturnsError", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockSE := database.NewMockStorage(tt)
		activity := &KmsConfigActivity{SE: mockSE}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		encrypted := "encrypting"
		getEncryptionStatus := vsa.VolumeResponse{
			ProviderResponse: vsa.ProviderResponse{},
			Encryption:       vsa.Encryption{State: &encrypted},
		}
		mockProvider.On("GetVolumeEncryptionStatus", mock.Anything).Return(&getEncryptionStatus, nil).Once()
		mockSE.On("UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockProvider.On("UpdateVolumeEnableEncryption", mock.Anything).Return(errors.New("volume encryption error"))
		mockProvider.On("GetVolumeEncryptionStatus", mock.Anything).Return(nil, errors.New("get volume encryption error"))

		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.MigrateVsaPoolActivity)
		_, errMigrate := env.ExecuteActivity(activity.MigrateVsaPoolActivity, volumes, node)

		assert.Error(tt, errMigrate)
		assert.Contains(tt, errMigrate.Error(), "Encryption failed for one/some of the volumes (type: CmekVolumeMigrationError, retryable: false): Volume encryption failure")
	})
	t.Run("WhenGetEncryptionResponseIsNil", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockSE := database.NewMockStorage(tt)
		activity := &KmsConfigActivity{SE: mockSE}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		encrypted := "encrypting"
		getEncryptionStatus := vsa.VolumeResponse{
			ProviderResponse: vsa.ProviderResponse{},
			Encryption:       vsa.Encryption{State: &encrypted},
		}
		mockProvider.On("GetVolumeEncryptionStatus", mock.Anything).Return(&getEncryptionStatus, nil).Once()
		mockSE.On("UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockProvider.On("UpdateVolumeEnableEncryption", mock.Anything).Return(errors.New("volume encryption error"))
		mockProvider.On("GetVolumeEncryptionStatus", mock.Anything).Return(nil, nil)

		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.MigrateVsaPoolActivity)
		_, errMigrate := env.ExecuteActivity(activity.MigrateVsaPoolActivity, volumes, node)

		assert.Error(tt, errMigrate)
		assert.Contains(tt, errMigrate.Error(), "Encryption failed for one/some of the volumes (type: CmekVolumeMigrationError, retryable: false): Volume encryption failure")
	})
	t.Run("WhenGetEncryptionResponseIsEncrypted", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockSE := database.NewMockStorage(tt)
		activity := &KmsConfigActivity{SE: mockSE}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		encrypting := "encrypting"
		encrypted := "encrypted"

		getEncryptionStatus := vsa.VolumeResponse{
			ProviderResponse: vsa.ProviderResponse{},
			Encryption:       vsa.Encryption{State: &encrypting},
		}
		getEncryptionStatusEncrypted := vsa.VolumeResponse{
			ProviderResponse: vsa.ProviderResponse{},
			Encryption:       vsa.Encryption{State: &encrypted},
		}
		mockProvider.On("GetVolumeEncryptionStatus", mock.Anything).Return(&getEncryptionStatus, nil).Once()
		mockSE.On("UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockProvider.On("UpdateVolumeEnableEncryption", mock.Anything).Return(nil)
		mockProvider.On("GetVolumeEncryptionStatus", mock.Anything).Return(&getEncryptionStatusEncrypted, nil)

		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.MigrateVsaPoolActivity)
		_, errMigrate := env.ExecuteActivity(activity.MigrateVsaPoolActivity, volumes, node)

		assert.NoError(tt, errMigrate)
	})
	t.Run("WhenGetEncryptionResponseStateIsUnknown", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockSE := database.NewMockStorage(tt)
		activity := &KmsConfigActivity{SE: mockSE}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		encrypting := "encrypting"
		unknown := "unknown"

		getEncryptionStatus := vsa.VolumeResponse{
			ProviderResponse: vsa.ProviderResponse{},
			Encryption:       vsa.Encryption{State: &encrypting},
		}
		getEncryptionStatusEncrypted := vsa.VolumeResponse{
			ProviderResponse: vsa.ProviderResponse{},
			Encryption:       vsa.Encryption{State: &unknown},
		}
		mockProvider.On("GetVolumeEncryptionStatus", mock.Anything).Return(&getEncryptionStatus, nil).Once()
		mockSE.On("UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockProvider.On("UpdateVolumeEnableEncryption", mock.Anything).Return(nil)
		mockProvider.On("GetVolumeEncryptionStatus", mock.Anything).Return(&getEncryptionStatusEncrypted, nil)

		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.MigrateVsaPoolActivity)
		_, errMigrate := env.ExecuteActivity(activity.MigrateVsaPoolActivity, volumes, node)

		assert.Error(tt, errMigrate)
		assert.Contains(tt, errMigrate.Error(), "Encryption failed for one/some of the volumes (type: CmekVolumeMigrationError, retryable: false): Volume encryption failure")
	})
	t.Run("WhenUpdateVolumeToReadyReturnsError", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockSE := database.NewMockStorage(tt)
		activity := &KmsConfigActivity{SE: mockSE}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		encrypting := "encrypting"
		encrypted := "encrypted"

		getEncryptionStatus := vsa.VolumeResponse{
			ProviderResponse: vsa.ProviderResponse{},
			Encryption:       vsa.Encryption{State: &encrypting},
		}
		getEncryptionStatusEncrypted := vsa.VolumeResponse{
			ProviderResponse: vsa.ProviderResponse{},
			Encryption:       vsa.Encryption{State: &encrypted},
		}
		mockProvider.On("GetVolumeEncryptionStatus", mock.Anything).Return(&getEncryptionStatus, nil).Once()
		mockSE.On("UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		mockSE.On("UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to update volume state in DB"))
		mockProvider.On("UpdateVolumeEnableEncryption", mock.Anything).Return(nil)
		mockProvider.On("GetVolumeEncryptionStatus", mock.Anything).Return(&getEncryptionStatusEncrypted, nil)

		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.MigrateVsaPoolActivity)
		_, errMigrate := env.ExecuteActivity(activity.MigrateVsaPoolActivity, volumes, node)

		assert.NoError(tt, errMigrate)
	})
}
