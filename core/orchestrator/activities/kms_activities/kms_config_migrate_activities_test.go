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
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestMigrateSdeKmsConfigActivity(t *testing.T) {
	t.Run("MigrateSdeKmsConfigActivityReturnsOperationStatus", func(tt *testing.T) {
		mockLogger := log.NewLogger()
		ctx := context.WithValue(context.Background(), middleware.ContextSLoggerKey, mockLogger)
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
		cvpResponse, err := activity.MigrateSdeKmsConfigActivity(ctx, &params)

		assert.NoError(tt, err)
		assert.NotNil(tt, cvpResponse)
		assert.NotNil(tt, cvpResponse.Payload)
		assert.Equal(tt, "kmsconfig-uuid", cvpResponse.Payload.Response.(cvpModels.EncryptVolumeStatusV1beta).UUID)
		assert.Equal(tt, "UPDATING", cvpResponse.Payload.Response.(cvpModels.EncryptVolumeStatusV1beta).Status)
	})
	t.Run("MigrateSdeKmsConfigActivityReturnsNilPayload", func(tt *testing.T) {
		mockLogger := log.NewLogger()
		ctx := context.WithValue(context.Background(), middleware.ContextSLoggerKey, mockLogger)
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
		cvpResponse, err := activity.MigrateSdeKmsConfigActivity(ctx, &params)

		assert.Error(tt, err)
		assert.EqualError(tt, err, "Error encountered during SDE CMEK migration: CVP response is empty")
		assert.Nil(tt, cvpResponse)
	})
	t.Run("MigrateSdeKmsConfigActivityReturnsNil", func(tt *testing.T) {
		mockLogger := log.NewLogger()
		ctx := context.WithValue(context.Background(), middleware.ContextSLoggerKey, mockLogger)
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
		cvpResponse, err := activity.MigrateSdeKmsConfigActivity(ctx, &params)

		assert.Error(tt, err)
		assert.EqualError(tt, err, "Error encountered during SDE CMEK migration: CVP response is empty")
		assert.Nil(tt, cvpResponse)
	})
	t.Run("MigrateSdeKmsConfigActivityReturnsError", func(tt *testing.T) {
		mockLogger := log.NewLogger()
		ctx := context.WithValue(context.Background(), middleware.ContextSLoggerKey, mockLogger)
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
		cvpResponse, err := activity.MigrateSdeKmsConfigActivity(ctx, &params)

		assert.Error(tt, err)
		assert.EqualError(tt, err, "Error migrating SDE KMS Configuration (type: DescribeOperationError, retryable: false): migration ran into an error")
		assert.Nil(tt, cvpResponse)
	})
}

func TestPollMigrateSdeKmsConfigActivity(t *testing.T) {
	t.Run("PollMigrateSdeKmsConfigActivityGetsNilResponse", func(tt *testing.T) {
		activity := &KmsConfigActivity{}
		params := common.MigrateKmsConfigParams{}

		errPoll := activity.PollMigrateSdeKmsConfigActivity(context.Background(), &params, nil)

		assert.Error(tt, errPoll)
		assert.Error(tt, errPoll, "unknown error encountered during Migrate KMS configuration")
	})
	t.Run("PollMigrateSdeKmsConfigActivityGetsNilPayload", func(tt *testing.T) {
		activity := &KmsConfigActivity{}
		params := common.MigrateKmsConfigParams{}
		response := kms_configurations.V1betaEncryptVolumesAccepted{Payload: nil}

		errPoll := activity.PollMigrateSdeKmsConfigActivity(context.Background(), &params, &response)

		assert.Error(tt, errPoll)
		assert.Error(tt, errPoll, "unknown error encountered during Migrate KMS configuration")
	})
	t.Run("PollMigrateSdeKmsConfigActivityReturnsDoneInPayload", func(tt *testing.T) {
		activity := &KmsConfigActivity{}
		params := common.MigrateKmsConfigParams{
			ProjectNumber: "projectNumber",
			LocationID:    "locationID",
		}
		doneStatus := true
		response := kms_configurations.V1betaEncryptVolumesAccepted{Payload: &cvpModels.OperationV1beta{Done: &doneStatus, Name: "operationName"}}

		errPoll := activity.PollMigrateSdeKmsConfigActivity(context.Background(), &params, &response)

		assert.NoError(tt, errPoll)
		assert.Nil(tt, errPoll)
	})
	t.Run("WhenPollCvpOperationForWorkflowReturnsError", func(tt *testing.T) {
		activity := &KmsConfigActivity{}
		params := common.MigrateKmsConfigParams{
			ProjectNumber: "projectNumber",
			LocationID:    "locationID",
		}
		doneStatus := false
		response := kms_configurations.V1betaEncryptVolumesAccepted{Payload: &cvpModels.OperationV1beta{Done: &doneStatus}}

		defer func() {
			pollCvpOperationForWorkflow = _pollCvpOperationForWorkflow
		}()
		pollCvpOperationForWorkflow = func(ctx context.Context, cvpClient cvpapi.Cvp, operationParams *async.V1betaDescribeOperationParams) (*cvpModels.OperationV1beta, error) {
			return nil, errors.New("CVP Polling is returning error to flag that polling needs to continue")
		}
		errPoll := activity.PollMigrateSdeKmsConfigActivity(context.Background(), &params, &response)

		assert.Error(tt, errPoll)
		assert.EqualError(tt, errPoll, "CVP Polling is returning error to flag that polling needs to continue")
	})
	t.Run("WhenPollCvpOperationForWorkflowReturnsWithoutError", func(tt *testing.T) {
		activity := &KmsConfigActivity{}
		params := common.MigrateKmsConfigParams{
			ProjectNumber: "projectNumber",
			LocationID:    "locationID",
		}
		doneStatus := false
		response := kms_configurations.V1betaEncryptVolumesAccepted{Payload: &cvpModels.OperationV1beta{Done: &doneStatus}}

		defer func() {
			pollCvpOperationForWorkflow = _pollCvpOperationForWorkflow
		}()
		pollCvpOperationForWorkflow = func(ctx context.Context, cvpClient cvpapi.Cvp, operationParams *async.V1betaDescribeOperationParams) (*cvpModels.OperationV1beta, error) {
			return nil, nil
		}
		errPoll := activity.PollMigrateSdeKmsConfigActivity(context.Background(), &params, &response)

		assert.NoError(tt, errPoll)
		assert.Nil(tt, errPoll)
	})
}

func TestMigrateVsaPoolActivity(t *testing.T) {
	ctx := context.Background()
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

	t.Run("WhenVolumeAttributesIsNil", func(tt *testing.T) {
		activity := &KmsConfigActivity{}
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() { activities.GetProviderByNode = originalGetProviderByNode }()

		activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		vol2 := datamodel.Volume{Name: "vol2",
			VolumeAttributes: nil,
			Svm:              &datamodel.Svm{Name: "svmName"},
		}
		var volumesErr []*datamodel.Volume
		volumesErr = append(volumesErr, &vol2)

		errGetVolume := activity.MigrateVsaPoolActivity(ctx, volumesErr, node)
		assert.Error(tt, errGetVolume)
		assert.EqualError(tt, errGetVolume, "Encryption failed for one/some of the volumes (type: CmekVolumeMigrationError, retryable: false): Volume encryption failure")
	})
	t.Run("WhenVolumeSvmIsNil", func(tt *testing.T) {
		activity := &KmsConfigActivity{}
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() { activities.GetProviderByNode = originalGetProviderByNode }()

		activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		var volumesSvmNil []*datamodel.Volume
		vol3 := datamodel.Volume{Name: "vol3",
			VolumeAttributes: &datamodel.VolumeAttributes{ExternalUUID: "externalUUID"},
			Svm:              nil,
		}
		volumesSvmNil = append(volumesSvmNil, &vol3)

		errGetVolume := activity.MigrateVsaPoolActivity(ctx, volumesSvmNil, node)
		assert.Error(tt, errGetVolume)
		assert.EqualError(tt, errGetVolume, "Encryption failed for one/some of the volumes (type: CmekVolumeMigrationError, retryable: false): Volume encryption failure")
	})
	t.Run("WhenGetVolumeEncryptionReturnsStateAsEncrypted", func(tt *testing.T) {
		activity := &KmsConfigActivity{}
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() { activities.GetProviderByNode = originalGetProviderByNode }()

		activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		encrypted := "encrypted"
		getEncryptionStatus := vsa.VolumeResponse{
			ProviderResponse: vsa.ProviderResponse{},
			Encryption:       vsa.Encryption{State: &encrypted},
		}
		mockProvider.On("GetVolumeEncryptionStatus", mock.Anything).Return(&getEncryptionStatus, nil)
		errMigrate := activity.MigrateVsaPoolActivity(ctx, volumes, node)

		assert.NoError(tt, errMigrate)
		assert.Nil(tt, errMigrate)
	})
	t.Run("WhenGetVolumeEncryptionReturnsStateAsEncrypting", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockSE := database.NewMockStorage(tt)
		activity := &KmsConfigActivity{SE: mockSE}
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() { activities.GetProviderByNode = originalGetProviderByNode }()

		activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
			err = activity.MigrateVsaPoolActivity(ctx, volumes, node)
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
	t.Run("WhenUpdateVolumeEnableEncryptionReturnsError", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockSE := database.NewMockStorage(tt)
		activity := &KmsConfigActivity{SE: mockSE}
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() { activities.GetProviderByNode = originalGetProviderByNode }()

		activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		encrypted := "unencrypted"
		getEncryptionStatus := vsa.VolumeResponse{
			ProviderResponse: vsa.ProviderResponse{},
			Encryption:       vsa.Encryption{State: &encrypted},
		}
		mockProvider.On("GetVolumeEncryptionStatus", mock.Anything).Return(&getEncryptionStatus, nil)
		mockProvider.On("UpdateVolumeEnableEncryption", mock.Anything).Return(errors.New("volume encryption error"))

		errMigrate := activity.MigrateVsaPoolActivity(ctx, volumes, node)

		assert.Error(tt, errMigrate)
		assert.EqualError(tt, errMigrate, "Encryption failed for one/some of the volumes (type: CmekVolumeMigrationError, retryable: false): Volume encryption failure")
	})
	t.Run("WhenGetVolumeEncryptionStatusReturnsError", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockSE := database.NewMockStorage(tt)
		activity := &KmsConfigActivity{SE: mockSE}
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() { activities.GetProviderByNode = originalGetProviderByNode }()

		activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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

		errMigrate := activity.MigrateVsaPoolActivity(ctx, volumes, node)

		assert.Error(tt, errMigrate)
		assert.EqualError(tt, errMigrate, "Encryption failed for one/some of the volumes (type: CmekVolumeMigrationError, retryable: false): Volume encryption failure")
	})
	t.Run("WhenGetEncryptionResponseIsNil", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockSE := database.NewMockStorage(tt)
		activity := &KmsConfigActivity{SE: mockSE}
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() { activities.GetProviderByNode = originalGetProviderByNode }()

		activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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

		errMigrate := activity.MigrateVsaPoolActivity(ctx, volumes, node)

		assert.Error(tt, errMigrate)
		assert.EqualError(tt, errMigrate, "Encryption failed for one/some of the volumes (type: CmekVolumeMigrationError, retryable: false): Volume encryption failure")
	})
	t.Run("WhenGetEncryptionResponseIsEncrypted", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockSE := database.NewMockStorage(tt)
		activity := &KmsConfigActivity{SE: mockSE}
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() { activities.GetProviderByNode = originalGetProviderByNode }()

		activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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

		errMigrate := activity.MigrateVsaPoolActivity(ctx, volumes, node)

		assert.NoError(tt, errMigrate)
		assert.Nil(tt, errMigrate)
	})
	t.Run("WhenGetEncryptionResponseStateIsUnknown", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockSE := database.NewMockStorage(tt)
		activity := &KmsConfigActivity{SE: mockSE}
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() { activities.GetProviderByNode = originalGetProviderByNode }()

		activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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

		errMigrate := activity.MigrateVsaPoolActivity(ctx, volumes, node)

		assert.Error(tt, errMigrate)
		assert.EqualError(tt, errMigrate, "Encryption failed for one/some of the volumes (type: CmekVolumeMigrationError, retryable: false): Volume encryption failure")
	})
}

func TestCompleteKmsMigrationActivity(t *testing.T) {
	ctx := context.Background()
	t.Run("WhenGetKmsConfigReturnsError", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		kmsConfigActivity := KmsConfigActivity{SE: mockSE}

		mockSE.On("GetKmsConfig", mock.Anything, mock.Anything).Return(nil, errors.New("GetKmsConfig error"))
		err := kmsConfigActivity.CompleteKmsMigrationActivity(ctx, "uuid1")

		assert.Error(tt, err)
		assert.EqualError(tt, err, "GetKmsConfig error")
	})
	t.Run("WhenIsKmsConfigInUseReturnsError", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		kmsConfigActivity := KmsConfigActivity{SE: mockSE}
		dbKmsConfig := &datamodel.KmsConfig{
			BaseModel:      datamodel.BaseModel{UUID: "test-kms-config-id", ID: int64(1)},
			State:          models.LifeCycleStateREADY,
			ServiceAccount: &datamodel.ServiceAccount{BaseModel: datamodel.BaseModel{UUID: "test-sa-id"}},
		}
		origAccessCryptoKey := AccessCryptoKey
		defer func() { AccessCryptoKey = origAccessCryptoKey }()
		AccessCryptoKey = func(ctx context.Context, se database.Storage, dbKmsConfig *datamodel.KmsConfig) error {
			return errors.New("AccessCryptoKey error")
		}
		mockSE.On("GetKmsConfig", mock.Anything, mock.Anything).Return(dbKmsConfig, nil)
		mockSE.On("GetSvmsByKmsConfigID", mock.Anything, mock.Anything).Return(nil, errors.New("KmsConfigInUse error"))
		err := kmsConfigActivity.CompleteKmsMigrationActivity(ctx, "uuid1")

		assert.Error(tt, err)
		assert.EqualError(tt, err, "KmsConfigInUse error")
	})
	t.Run("WhenUpdateKmsConfigHealthReturnsError", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		kmsConfigActivity := KmsConfigActivity{SE: mockSE}
		dbKmsConfig := &datamodel.KmsConfig{
			BaseModel:      datamodel.BaseModel{UUID: "test-kms-config-id", ID: int64(1)},
			State:          models.LifeCycleStateInUse,
			ServiceAccount: &datamodel.ServiceAccount{BaseModel: datamodel.BaseModel{UUID: "test-sa-id"}},
		}
		origAccessCryptoKey := AccessCryptoKey
		defer func() { AccessCryptoKey = origAccessCryptoKey }()
		AccessCryptoKey = func(ctx context.Context, se database.Storage, dbKmsConfig *datamodel.KmsConfig) error {
			return errors.New("AccessCryptoKey error")
		}
		mockSE.On("GetKmsConfig", mock.Anything, mock.Anything).Return(dbKmsConfig, nil)
		mockSE.On("UpdateKmsConfigState", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(dbKmsConfig, errors.New("UpdateKmsConfigHealth error"))
		err := kmsConfigActivity.CompleteKmsMigrationActivity(ctx, "uuid1")

		assert.Error(tt, err)
		assert.EqualError(tt, err, "UpdateKmsConfigHealth error")
	})
	t.Run("WhenCompleteKmsMigrationActivityIsSuccessful", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		kmsConfigActivity := KmsConfigActivity{SE: mockSE}
		dbKmsConfig := &datamodel.KmsConfig{
			BaseModel:      datamodel.BaseModel{UUID: "test-kms-config-id", ID: int64(1)},
			State:          models.LifeCycleStateInUse,
			ServiceAccount: &datamodel.ServiceAccount{BaseModel: datamodel.BaseModel{UUID: "test-sa-id"}},
			KmsAttributes:  &datamodel.KmsAttributes{},
		}
		origAccessCryptoKey := AccessCryptoKey
		defer func() { AccessCryptoKey = origAccessCryptoKey }()
		AccessCryptoKey = func(ctx context.Context, se database.Storage, dbKmsConfig *datamodel.KmsConfig) error {
			return errors.New("AccessCryptoKey error")
		}
		mockSE.On("GetKmsConfig", mock.Anything, mock.Anything).Return(dbKmsConfig, nil)
		mockSE.On("UpdateKmsConfigState", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(dbKmsConfig, nil)
		mockSE.On("UpdateKmsConfigAttributes", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(dbKmsConfig, nil)

		err := kmsConfigActivity.CompleteKmsMigrationActivity(ctx, "uuid1")

		assert.NoError(tt, err)
		assert.Nil(tt, err)
	})
}
