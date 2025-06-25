package orchestrator

import (
	"context"
	"gorm.io/gorm"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/retry"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine"
	"go.temporal.io/sdk/client"
)

func TestGetMultipleKmsConfigs(t *testing.T) {
	mockLogger := log.NewLogger()
	store, err := database.SetupStorageForTest(mockLogger)
	if err != nil {
		t.Fatalf("Failed to create test storage: %v", err)
	}

	err = database.ClearInMemoryDB(store.DB())
	if err != nil {
		t.Fatalf("Failed to clean up test storage: %v", err)
	}

	orchInstance := Orchestrator{
		storage: store,
	}
	serviceAccounts := []*datamodel.ServiceAccount{
		{BaseModel: datamodel.BaseModel{ID: int64(111), UUID: "uuid10"}, Name: "ServiceAccount1"},
		{BaseModel: datamodel.BaseModel{ID: int64(222), UUID: "uuid20"}, Name: "ServiceAccount2"},
	}
	err = store.DB().Create(serviceAccounts).Error
	if err != nil {
		t.Fatalf("Failed to create Service-Accounts table: %v", err)
	}

	kmsConfigs := []*datamodel.KmsConfig{
		{BaseModel: datamodel.BaseModel{UUID: "uuid1", DeletedAt: nil}, Name: "kmsConfig1", ServiceAccountID: serviceAccounts[0].ID,
			KmsAttributes: &datamodel.KmsAttributes{SdeServiceAccountEmail: "sdeServiceAccount1@account.com"}},
		{BaseModel: datamodel.BaseModel{UUID: "uuid2", DeletedAt: nil}, Name: "kmsConfig2", ServiceAccountID: serviceAccounts[1].ID,
			KmsAttributes: &datamodel.KmsAttributes{SdeServiceAccountEmail: "sdeServiceAccount2@account.com"}},
	}
	err = store.DB().Create(kmsConfigs).Error
	if err != nil {
		t.Fatalf("Failed to create KMS Configs table: %v", err)
	}

	t.Run("WhenListedKMSConfigsAreFound", func(tt *testing.T) {
		kmsConfigUUIDList := []string{"uuid1", "uuid2"}
		result, err := orchInstance.GetMultipleKMSConfigs(context.Background(), kmsConfigUUIDList)

		assert.NoError(tt, err)
		assert.Equal(tt, "kmsConfig1", result[0].Name)
		assert.Equal(tt, "kmsConfig2", result[1].Name)
		assert.Equal(tt, "sdeServiceAccount1@account.com", result[0].KmsAttributes.SdeServiceAccountEmail)
		assert.Equal(tt, "sdeServiceAccount2@account.com", result[1].KmsAttributes.SdeServiceAccountEmail)
	})
	t.Run("ReturnsEmptyListWhenNoUUIDsAreProvided", func(tt *testing.T) {
		kmsConfigUUIDList := []string{}
		result, err := orchInstance.GetMultipleKMSConfigs(context.Background(), kmsConfigUUIDList)

		assert.NoError(tt, err)
		assert.Empty(tt, result)
	})
	t.Run("ReturnsNilWhenKMSConfigsAreNotFound", func(tt *testing.T) {
		kmsConfigUUIDList := []string{"nonexistent-uuid"}
		result, err := orchInstance.GetMultipleKMSConfigs(context.Background(), kmsConfigUUIDList)

		assert.NoError(tt, err)
		assert.Empty(tt, result)
	})
	t.Run("WhenStorageLayerReturnsError", func(tt *testing.T) {
		kmsConfigUUIDList := []string{"some-uuid"}
		mockStorage := database.NewMockStorage(tt)
		mockStorage.EXPECT().GetMultipleKmsConfigs(mock.Anything, mock.Anything).Return(nil, errors.New("internal error"))
		orchInstanceNew := Orchestrator{storage: mockStorage}

		result, err := orchInstanceNew.GetMultipleKMSConfigs(context.Background(), kmsConfigUUIDList)

		assert.Error(tt, err)
		assert.Empty(tt, result)
	})
}

func TestConvertDataStoreKmsConfigToModel(t *testing.T) {
	t.Run("ReturnsValidKmsConfigWhenAllFieldsArePopulated", func(t *testing.T) {
		kmsConfig := &datamodel.KmsConfig{
			Name:              "test-name",
			Description:       "test-description",
			State:             "ACTIVE",
			StateDetails:      "test-state-details",
			KeyRing:           "test-key-ring",
			KeyRingLocation:   "test-location",
			KeyName:           "test-key-name",
			AccountID:         int64(1234),
			CustomerProjectID: "test-customer-project-id",
			KeyProjectID:      "test-key-project-id",
			ServiceAccountID:  int64(1234),
			ResourceID:        "test-resource-id",
			KmsAttributes: &datamodel.KmsAttributes{
				SdeKmsConfigUUID:       "test-external-uuid",
				SdeServiceAccountEmail: "test-service-account@test.com",
			},
		}
		expectedDate := time.Date(2022, time.February, 2, 2, 2, 2, 2, time.UTC)
		kmsConfig.BaseModel = datamodel.BaseModel{
			UUID:      "test-uuid",
			CreatedAt: expectedDate,
			UpdatedAt: expectedDate,
			DeletedAt: &gorm.DeletedAt{Time: expectedDate, Valid: true},
		}

		result := convertDataStoreKmsConfigToModel(kmsConfig)

		assert.NotNil(t, result)
		assert.Equal(t, kmsConfig.UUID, result.UUID)
		assert.Equal(t, expectedDate, result.CreatedAt)
		assert.Equal(t, expectedDate, result.UpdatedAt)
		assert.Equal(t, expectedDate, *result.DeletedAt)
		assert.Equal(t, kmsConfig.Name, result.Name)
		assert.Equal(t, kmsConfig.Description, result.Description)
		assert.Equal(t, kmsConfig.State, result.State)
		assert.Equal(t, kmsConfig.StateDetails, result.StateDetails)
		assert.Equal(t, kmsConfig.KeyRing, result.KeyRing)
		assert.Equal(t, kmsConfig.KeyRingLocation, result.KeyRingLocation)
		assert.Equal(t, kmsConfig.KeyName, result.KeyName)
		assert.Equal(t, kmsConfig.AccountID, result.AccountID)
		assert.Equal(t, kmsConfig.CustomerProjectID, result.CustomerProjectID)
		assert.Equal(t, kmsConfig.KeyProjectID, result.KeyProjectID)
		assert.Equal(t, kmsConfig.ServiceAccountID, result.ServiceAccountID)
		assert.Equal(t, kmsConfig.ResourceID, result.ResourceID)
		assert.NotNil(t, result.KmsAttributes)
		assert.Equal(t, kmsConfig.KmsAttributes.SdeKmsConfigUUID, result.KmsAttributes.SdeKmsConfigUUID)
		assert.Equal(t, kmsConfig.KmsAttributes.SdeServiceAccountEmail, result.KmsAttributes.SdeServiceAccountEmail)
	})
	t.Run("HandlesNilKmsAttributesGracefully", func(t *testing.T) {
		kmsConfig := &datamodel.KmsConfig{
			Name:              "test-name",
			Description:       "test-description",
			State:             "ACTIVE",
			StateDetails:      "test-state-details",
			KeyRing:           "test-key-ring",
			KeyRingLocation:   "test-location",
			KeyName:           "test-key-name",
			AccountID:         int64(1234),
			CustomerProjectID: "test-customer-project-id",
			KeyProjectID:      "test-key-project-id",
			ServiceAccountID:  int64(1234),
			ResourceID:        "test-resource-id",
			KmsAttributes:     nil,
		}
		kmsConfig.BaseModel = datamodel.BaseModel{
			UUID:      "test-uuid",
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
			DeletedAt: nil,
		}

		result := convertDataStoreKmsConfigToModel(kmsConfig)

		assert.NotNil(t, result)
		assert.Nil(t, result.KmsAttributes)
		assert.Nil(t, result.DeletedAt)
	})
}

func TestUpdateKmsConfig(t *testing.T) {
	ctx := context.Background()

	t.Run("SuccessfulUpdate", func(tt *testing.T) {
		mockStorage := new(database.MockStorage)
		mockTemporal := new(workflow_engine.MockTemporalTestClient)

		params := &common.UpdateKmsConfigParams{
			KmsConfigID: "test-kms-config-id",
			AccountName: "test-account",
			Name:        "updated-kms-config",
		}

		orchestrator := Orchestrator{
			storage:  mockStorage,
			temporal: mockTemporal,
		}

		account := &datamodel.Account{Name: "test-account"}
		dbKmsConfig := &datamodel.KmsConfig{
			BaseModel:      datamodel.BaseModel{UUID: "test-kms-config-id"},
			State:          models.LifeCycleStateAvailable,
			ServiceAccount: &datamodel.ServiceAccount{BaseModel: datamodel.BaseModel{UUID: "test-sa-id"}},
		}
		// Mock storage behavior
		mockStorage.On("GetAccount", ctx, "test-account").Return(account, nil)
		mockStorage.On("GetKmsConfig", ctx, "test-kms-config-id").Return(dbKmsConfig, nil)
		mockStorage.On("GetSvmsByKmsConfigID", ctx, dbKmsConfig.ID).Return(nil, nil)
		mockStorage.On("UpdateKmsConfigState", ctx, dbKmsConfig.UUID, models.LifeCycleStateUpdating, models.LifeCycleStateUpdatingDetails).Return(dbKmsConfig, nil)
		mockStorage.On("CreateJob", ctx, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "test-job-uuid"},
		}, nil)

		// Mock Temporal client behavior
		mockTemporal.On("ExecuteWorkflow", ctx, mock.Anything, mock.Anything, dbKmsConfig, params).Return(nil, nil)

		kmsConfig, jobUUID, err := orchestrator.UpdateKmsConfig(ctx, params)

		assert.NoError(tt, err)
		assert.Equal(tt, "test-job-uuid", jobUUID)
		assert.NotNil(tt, kmsConfig)
		assert.Equal(tt, "test-kms-config-id", kmsConfig.UUID)
	})

	t.Run("SuccessfulUpdateWhenVcpKmsConfigNotFound", func(tt *testing.T) {
		mockStorage := new(database.MockStorage)
		mockTemporal := new(workflow_engine.MockTemporalTestClient)

		params := &common.UpdateKmsConfigParams{
			KmsConfigID: "test-kms-config-id",
			AccountName: "test-account",
			Name:        "updated-kms-config",
		}

		orchestrator := Orchestrator{
			storage:  mockStorage,
			temporal: mockTemporal,
		}

		account := &datamodel.Account{Name: "test-account"}
		dbKmsConfig := &datamodel.KmsConfig{
			BaseModel:      datamodel.BaseModel{UUID: "test-kms-config-id"},
			State:          models.LifeCycleStateAvailable,
			ServiceAccount: &datamodel.ServiceAccount{BaseModel: datamodel.BaseModel{UUID: "test-sa-id"}},
		}
		// Mock storage behavior
		mockStorage.On("GetAccount", ctx, "test-account").Return(account, nil)
		mockStorage.On("GetKmsConfig", ctx, "test-kms-config-id").Return(nil, errors.NewNotFoundErr("kms config", nil))
		mockStorage.On("CreateJob", ctx, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "test-job-uuid"},
		}, nil)

		// Mock Temporal client behavior
		mockTemporal.On("ExecuteWorkflow", ctx, mock.Anything, mock.Anything, &datamodel.KmsConfig{KmsAttributes: &datamodel.KmsAttributes{SdeKmsConfigUUID: dbKmsConfig.UUID}}, params).Return(nil, nil)

		kmsConfig, jobUUID, err := orchestrator.UpdateKmsConfig(ctx, params)

		assert.NoError(tt, err)
		assert.Equal(tt, "test-job-uuid", jobUUID)
		assert.Nil(tt, kmsConfig)
	})

	t.Run("ValidationError", func(tt *testing.T) {
		mockStorage := new(database.MockStorage)
		mockTemporal := new(workflow_engine.MockTemporalTestClient)

		params := &common.UpdateKmsConfigParams{
			KmsConfigID: "test-kms-config-id",
			AccountName: "test-account",
			Name:        "updated-kms-config",
			KeyName:     "key1",
		}

		orchestrator := Orchestrator{
			storage:  mockStorage,
			temporal: mockTemporal,
		}

		dbKmsConfig := &datamodel.KmsConfig{
			BaseModel:      datamodel.BaseModel{UUID: "test-kms-config-id"},
			State:          models.LifeCycleStateAvailable,
			ServiceAccount: &datamodel.ServiceAccount{BaseModel: datamodel.BaseModel{UUID: "test-sa-id"}},
		}
		account := &datamodel.Account{Name: "test-account"}

		// Mock storage behavior
		mockStorage.On("GetAccount", ctx, "test-account").Return(account, nil)
		mockStorage.On("GetKmsConfig", ctx, "test-kms-config-id").Return(dbKmsConfig, nil)
		mockStorage.On("GetSvmsByKmsConfigID", ctx, dbKmsConfig.ID).Return([]*datamodel.Svm{&datamodel.Svm{Name: "svm"}}, nil)

		// Mock Temporal client behavior
		mockTemporal.On("ExecuteWorkflow", ctx, mock.Anything, mock.Anything, dbKmsConfig, params).Return(nil, nil)

		kmsConfig, jobUUID, err := orchestrator.UpdateKmsConfig(ctx, params)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "can not update key details while kms config is in use")
		assert.Nil(tt, kmsConfig)
		assert.Empty(tt, jobUUID)
	})

	t.Run("WhenSvmNotFound", func(tt *testing.T) {
		mockStorage := new(database.MockStorage)
		mockTemporal := new(workflow_engine.MockTemporalTestClient)

		params := &common.UpdateKmsConfigParams{
			KmsConfigID: "test-kms-config-id",
			AccountName: "test-account",
			Name:        "updated-kms-config",
			KeyName:     "key1",
		}

		orchestrator := Orchestrator{
			storage:  mockStorage,
			temporal: mockTemporal,
		}

		dbKmsConfig := &datamodel.KmsConfig{
			BaseModel:      datamodel.BaseModel{UUID: "test-kms-config-id"},
			State:          models.LifeCycleStateAvailable,
			ServiceAccount: &datamodel.ServiceAccount{BaseModel: datamodel.BaseModel{UUID: "test-sa-id"}},
		}
		account := &datamodel.Account{Name: "test-account"}

		// Mock storage behavior
		mockStorage.On("GetAccount", ctx, "test-account").Return(account, nil)
		mockStorage.On("GetKmsConfig", ctx, "test-kms-config-id").Return(dbKmsConfig, nil)
		mockStorage.On("GetSvmsByKmsConfigID", ctx, dbKmsConfig.ID).Return(nil, errors.New("error"))

		// Mock Temporal client behavior
		mockTemporal.On("ExecuteWorkflow", ctx, mock.Anything, mock.Anything, dbKmsConfig, params).Return(nil, nil)

		kmsConfig, jobUUID, err := orchestrator.UpdateKmsConfig(ctx, params)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "error")
		assert.Nil(tt, kmsConfig)
		assert.Empty(tt, jobUUID)
	})

	t.Run("WhenKmsConfigInCreatingState", func(tt *testing.T) {
		mockStorage := new(database.MockStorage)
		mockTemporal := new(workflow_engine.MockTemporalTestClient)

		params := &common.UpdateKmsConfigParams{
			KmsConfigID: "test-kms-config-id",
			AccountName: "test-account",
			Name:        "updated-kms-config",
			KeyName:     "key1",
		}

		orchestrator := Orchestrator{
			storage:  mockStorage,
			temporal: mockTemporal,
		}

		dbKmsConfig := &datamodel.KmsConfig{
			BaseModel:      datamodel.BaseModel{UUID: "test-kms-config-id"},
			State:          models.LifeCycleStateCreating,
			ServiceAccount: &datamodel.ServiceAccount{BaseModel: datamodel.BaseModel{UUID: "test-sa-id"}},
		}
		account := &datamodel.Account{Name: "test-account"}

		// Mock storage behavior
		mockStorage.On("GetAccount", ctx, "test-account").Return(account, nil)
		mockStorage.On("GetKmsConfig", ctx, "test-kms-config-id").Return(dbKmsConfig, nil)

		// Mock Temporal client behavior
		mockTemporal.On("ExecuteWorkflow", ctx, mock.Anything, mock.Anything, dbKmsConfig, params).Return(nil, nil)

		kmsConfig, jobUUID, err := orchestrator.UpdateKmsConfig(ctx, params)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "can not update a gcpKmsConfig which is in creating or error state.")
		assert.Nil(tt, kmsConfig)
		assert.Empty(tt, jobUUID)
	})

	t.Run("WorkflowExecutionFailure", func(tt *testing.T) {
		mockStorage := new(database.MockStorage)
		mockTemporal := new(workflow_engine.MockTemporalTestClient)

		params := &common.UpdateKmsConfigParams{
			KmsConfigID: "test-kms-config-id",
			AccountName: "test-account",
			Name:        "updated-kms-config",
		}

		orchestrator := Orchestrator{
			storage:  mockStorage,
			temporal: mockTemporal,
		}

		account := &datamodel.Account{Name: "test-account"}
		dbKmsConfig := &datamodel.KmsConfig{
			BaseModel:      datamodel.BaseModel{UUID: "test-kms-config-id"},
			State:          models.LifeCycleStateAvailable,
			ServiceAccount: &datamodel.ServiceAccount{BaseModel: datamodel.BaseModel{UUID: "test-sa-id"}},
		}
		// Mock storage behavior
		mockStorage.On("GetAccount", ctx, "test-account").Return(account, nil)
		mockStorage.On("GetKmsConfig", ctx, "test-kms-config-id").Return(dbKmsConfig, nil)
		mockStorage.On("GetSvmsByKmsConfigID", ctx, dbKmsConfig.ID).Return(nil, nil)
		mockStorage.On("UpdateKmsConfigState", ctx, dbKmsConfig.UUID, models.LifeCycleStateUpdating, models.LifeCycleStateUpdatingDetails).Return(dbKmsConfig, nil)
		mockStorage.On("CreateJob", ctx, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "test-job-uuid"},
		}, nil)

		// Mock Temporal client behavior
		mockTemporal.On("ExecuteWorkflow", ctx, mock.Anything, mock.Anything, dbKmsConfig, params).Return(nil, errors.New("workflow execution failed"))

		kmsConfig, jobUUID, err := orchestrator.UpdateKmsConfig(ctx, params)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "workflow execution failed")
		assert.Nil(tt, kmsConfig)
		assert.Empty(tt, jobUUID)
	})
}

func TestIsKmsConfigInUse(t *testing.T) {
	ctx := context.Background()
	t.Run("WhenSvmFound", func(tt *testing.T) {
		mockStorage := new(database.MockStorage)
		dbKmsConfig := &datamodel.KmsConfig{
			BaseModel:      datamodel.BaseModel{UUID: "test-kms-config-id"},
			State:          models.LifeCycleStateAvailable,
			ServiceAccount: &datamodel.ServiceAccount{BaseModel: datamodel.BaseModel{UUID: "test-sa-id"}},
		}
		mockStorage.On("GetSvmsByKmsConfigID", ctx, dbKmsConfig.ID).Return([]*datamodel.Svm{&datamodel.Svm{Name: "svm"}}, nil)

		inuse, err := isKmsConfigInUse(ctx, mockStorage, dbKmsConfig)
		assert.NoError(tt, err)
		assert.Equal(tt, inuse, true)
	})
	t.Run("WhenSvmNotFoundError", func(tt *testing.T) {
		mockStorage := new(database.MockStorage)
		dbKmsConfig := &datamodel.KmsConfig{
			BaseModel:      datamodel.BaseModel{UUID: "test-kms-config-id"},
			State:          models.LifeCycleStateAvailable,
			ServiceAccount: &datamodel.ServiceAccount{BaseModel: datamodel.BaseModel{UUID: "test-sa-id"}},
		}
		mockStorage.On("GetSvmsByKmsConfigID", ctx, dbKmsConfig.ID).Return(nil, errors.NewNotFoundErr("svm", nil))

		inuse, err := isKmsConfigInUse(ctx, mockStorage, dbKmsConfig)
		assert.NoError(tt, err)
		assert.Equal(tt, inuse, false)
	})
	t.Run("WhenSvmOtherError", func(tt *testing.T) {
		mockStorage := new(database.MockStorage)
		dbKmsConfig := &datamodel.KmsConfig{
			BaseModel:      datamodel.BaseModel{UUID: "test-kms-config-id"},
			State:          models.LifeCycleStateAvailable,
			ServiceAccount: &datamodel.ServiceAccount{BaseModel: datamodel.BaseModel{UUID: "test-sa-id"}},
		}
		mockStorage.On("GetSvmsByKmsConfigID", ctx, dbKmsConfig.ID).Return(nil, errors.New("some error"))

		inuse, err := isKmsConfigInUse(ctx, mockStorage, dbKmsConfig)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "some error")
		assert.Equal(tt, inuse, false)
	})
	t.Run("WhenKmsStateInUse", func(tt *testing.T) {
		mockStorage := new(database.MockStorage)
		dbKmsConfig := &datamodel.KmsConfig{
			BaseModel:      datamodel.BaseModel{UUID: "test-kms-config-id"},
			State:          models.LifeCycleStateInUse,
			ServiceAccount: &datamodel.ServiceAccount{BaseModel: datamodel.BaseModel{UUID: "test-sa-id"}},
		}

		inuse, err := isKmsConfigInUse(ctx, mockStorage, dbKmsConfig)
		assert.NoError(tt, err)
		assert.Equal(tt, inuse, true)
	})
}

func TestCreateKmsConfig(t *testing.T) {
	temporal := workflow_engine.NewMockTemporalTestClient(t)
	t.Run("CreateKmsConfigReturnsErrorWhenAccountCreationFails", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		params := &common.CreateKmsConfigParams{AccountName: "fail_account"}
		se := database.Storage(nil)
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.New("account error")
		}
		defer func() {
			getOrCreateAccount = _getOrCreateAccount
		}()
		_, _, err := _createKmsConfig(ctx, se, temporal, params)
		if err == nil || err.Error() != "account error" {
			t.Errorf("Expected account error, got %v", err)
		}
	})

	t.Run("CreateKmsConfigParseKeyFullPathResourceFails", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		params := &common.CreateKmsConfigParams{AccountName: "test_account", KeyFullPath: "projects/p/locations/l/keyRings/r/cryptoKeys/k"}
		mockStorage := new(database.MockStorage)
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{}, nil
		}
		parseKeyFullPathResource = func(s string) (*utils.ParsedKeyFullPathResource, error) {
			return nil, errors.New("resource error")
		}
		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			parseKeyFullPathResource = utils.ParseKeyFullPathResource
		}()
		_, _, err := _createKmsConfig(ctx, mockStorage, temporal, params)
		assert.Error(tt, err)
	})

	t.Run("CreateKmsConfigReturnsErrorWhenJobCreationFails", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		params := &common.CreateKmsConfigParams{AccountName: "test_account", KeyFullPath: "projects/p/locations/l/keyRings/r/cryptoKeys/k"}
		mockStorage := new(database.MockStorage)
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{}, nil
		}
		parseKeyFullPathResource = func(s string) (*utils.ParsedKeyFullPathResource, error) {
			return &utils.ParsedKeyFullPathResource{CryptoKey: "k", ProjectID: "p", Location: "l", KeyRing: "r"}, nil
		}
		mockStorage.On("CreateKmsConfig", ctx, mock.Anything).Return(&datamodel.KmsConfig{AccountID: 1}, nil)
		mockStorage.On("CreateJob", ctx, mock.Anything).Return(nil, errors.New("job error"))

		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			parseKeyFullPathResource = utils.ParseKeyFullPathResource
		}()
		_, _, err := _createKmsConfig(ctx, mockStorage, temporal, params)
		if err == nil || err.Error() != "job error" {
			t.Errorf("Expected job error, got %v", err)
		}
	})

	t.Run("CreateKmsConfigReturnsErrorWhenStorageFails", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		params := &common.CreateKmsConfigParams{AccountName: "test_account", KeyFullPath: "projects/p/locations/l/keyRings/r/cryptoKeys/k"}
		mockStorage := new(database.MockStorage)
		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			parseKeyFullPathResource = utils.ParseKeyFullPathResource
		}()
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{}, nil
		}
		parseKeyFullPathResource = func(s string) (*utils.ParsedKeyFullPathResource, error) {
			return &utils.ParsedKeyFullPathResource{CryptoKey: "k", ProjectID: "p", Location: "l", KeyRing: "r"}, nil
		}
		mockStorage.On("CreateKmsConfig", ctx, mock.Anything).Return(nil, errors.New("db error"))

		_, _, err := _createKmsConfig(ctx, mockStorage, temporal, params)
		if err == nil || err.Error() != "db error" {
			t.Errorf("Expected db error, got %v", err)
		}
	})
	t.Run("CreateKmsConfigReturnsErrorWhenWorkflowFails", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		params := &common.CreateKmsConfigParams{AccountName: "test_account", KeyFullPath: "projects/p/locations/l/keyRings/r/cryptoKeys/k"}
		mockStorage := new(database.MockStorage)
		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			parseKeyFullPathResource = utils.ParseKeyFullPathResource
		}()
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{}, nil
		}
		parseKeyFullPathResource = func(s string) (*utils.ParsedKeyFullPathResource, error) {
			return &utils.ParsedKeyFullPathResource{CryptoKey: "k", ProjectID: "p", Location: "l", KeyRing: "r"}, nil
		}
		mockStorage.On("CreateKmsConfig", ctx, mock.Anything).Return(&datamodel.KmsConfig{BaseModel: datamodel.BaseModel{
			UUID: "uuid"}, AccountID: 1}, nil)
		mockStorage.On("CreateJob", ctx, mock.Anything).Return(&datamodel.Job{BaseModel: datamodel.BaseModel{
			UUID: "job-uuid"}, WorkflowID: "wf-id"}, nil)
		temporal.On("ExecuteWorkflow", ctx, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("workflow error"))

		_, _, err := _createKmsConfig(ctx, mockStorage, temporal, params)
		if err == nil || err.Error() != "workflow error" {
			t.Errorf("Expected workflow error, got %v", err)
		}
	})
	t.Run("CreateKmsConfigReturnsKmsConfigAndJobUUIDOnSuccess", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		params := &common.CreateKmsConfigParams{AccountName: "test_account", KeyFullPath: "projects/p/locations/l/keyRings/r/cryptoKeys/k", ResourceID: "res-id", Name: "kms-name"}
		mockStorage := new(database.MockStorage)
		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			parseKeyFullPathResource = utils.ParseKeyFullPathResource
		}()
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{}, nil
		}
		parseKeyFullPathResource = func(s string) (*utils.ParsedKeyFullPathResource, error) {
			return &utils.ParsedKeyFullPathResource{CryptoKey: "k", ProjectID: "p", Location: "l", KeyRing: "r"}, nil
		}
		mockStorage.On("CreateKmsConfig", ctx, mock.Anything).Return(&datamodel.KmsConfig{BaseModel: datamodel.BaseModel{
			UUID: "uuid"}, AccountID: 1, KeyName: "k", CustomerProjectID: "p", KeyRingLocation: "l", KeyRing: "r", ResourceID: "res-id",
			KmsAttributes: &datamodel.KmsAttributes{}, ServiceAccount: &datamodel.ServiceAccount{}}, nil)
		mockStorage.On("CreateJob", ctx, mock.Anything).Return(&datamodel.Job{BaseModel: datamodel.BaseModel{
			UUID: "job-uuid"}, WorkflowID: "wf-id"}, nil)
		temporal.On("ExecuteWorkflow", ctx, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		kmsConfig, jobUUID, err := _createKmsConfig(ctx, mockStorage, temporal, params)
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
		if kmsConfig == nil || jobUUID != "job-uuid" {
			t.Errorf("Expected valid kmsConfig and jobUUID, got %v, %v", kmsConfig, jobUUID)
		}
	})
}

func TestCreateKmsConfigFails(t *testing.T) {
	ctx := context.Background()
	mockLogger := log.NewLogger()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
	params := &common.CreateKmsConfigParams{AccountName: "fail_account"}
	mockStorage := new(database.MockStorage)
	temporal := workflow_engine.NewMockTemporalTestClient(t)
	createKmsConfig = func(ctx context.Context, se database.Storage, temporal client.Client, params *common.CreateKmsConfigParams) (*models.KmsConfig, string, error) {
		return nil, "", errors.New("some error")
	}
	defer func() {
		createKmsConfig = _createKmsConfig
	}()
	orch := Orchestrator{storage: mockStorage, temporal: temporal}
	_, _, err := orch.CreateKmsConfig(ctx, params)
	assert.Error(t, err)
	assert.Equal(t, "some error", err.Error())
}

func TestGetKmsConfigFails(t *testing.T) {
	ctx := context.Background()
	mockLogger := log.NewLogger()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
	params := &common.GetKmsConfigParams{AccountName: "fail_account"}
	mockStorage := new(database.MockStorage)
	temporal := workflow_engine.NewMockTemporalTestClient(t)
	getKmsConfig = func(ctx context.Context, se database.Storage, temporal client.Client, params *common.GetKmsConfigParams) (*models.KmsConfig, error) {
		return nil, errors.New("some error")
	}
	defer func() {
		getKmsConfig = _getKmsConfig
	}()
	orch := Orchestrator{storage: mockStorage, temporal: temporal}
	_, err := orch.GetKmsConfig(ctx, params)
	assert.Error(t, err)
	assert.Equal(t, "some error", err.Error())
}

func TestUpdateKmsConfigHealth(t *testing.T) {
	t.Run("UpdateKmsConfigHealthUpdatesStateToReadyWhenInErrorStateAndDoNotUsedByAnySvms", func(tt *testing.T) {
		mockStorage := new(database.MockStorage)
		orch := Orchestrator{storage: mockStorage}
		ctx := context.Background()
		kmsConfig := &datamodel.KmsConfig{
			BaseModel:     datamodel.BaseModel{UUID: "test-uuid"},
			State:         models.LifeCycleStateError,
			KeyName:       "key1",
			KeyRing:       "ring1",
			KmsAttributes: &datamodel.KmsAttributes{},
		}
		defer func() {
			isKmsConfigInUse = _isKmsConfigInUse
		}()
		isKmsConfigInUse = func(ctx context.Context, se database.Storage, kmsConfig *datamodel.KmsConfig) (bool, error) {
			return false, nil
		}
		mockStorage.On("GetKmsConfig", ctx, "test-uuid").Return(kmsConfig, nil)
		mockStorage.On("UpdateKmsConfigState", ctx, "test-uuid", models.LifeCycleStateREADY, models.LifeCycleStateReadyDetails).Return(kmsConfig, nil)
		mockStorage.On("UpdateKmsConfigAttributes", ctx, "test-uuid", kmsConfig.KmsAttributes).Return(kmsConfig, nil)
		response := &models.KmsConfigCheck{
			KmsConfig:   &models.KmsConfig{BaseModel: models.BaseModel{UUID: "test-uuid"}},
			IsHealthy:   true,
			HealthError: "",
		}
		result, err := orch.CheckAndUpdateKmsConfigHealth(ctx, response)
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})
	t.Run("UpdateKmsConfigHealthUpdatesStateToInUseWhenInErrorStateAndUsedBySvms", func(tt *testing.T) {
		mockStorage := new(database.MockStorage)
		orch := Orchestrator{storage: mockStorage}
		ctx := context.Background()
		kmsConfig := &datamodel.KmsConfig{
			BaseModel:     datamodel.BaseModel{UUID: "test-uuid"},
			State:         models.LifeCycleStateError,
			KeyName:       "key1",
			KeyRing:       "ring1",
			KmsAttributes: &datamodel.KmsAttributes{},
		}
		defer func() {
			isKmsConfigInUse = _isKmsConfigInUse
		}()
		isKmsConfigInUse = func(ctx context.Context, se database.Storage, kmsConfig *datamodel.KmsConfig) (bool, error) {
			return true, nil
		}
		mockStorage.On("GetKmsConfig", ctx, "test-uuid").Return(kmsConfig, nil)
		mockStorage.On("UpdateKmsConfigState", ctx, "test-uuid", models.LifeCycleStateInUse, models.LifeCycleStateAvailableDetails).Return(kmsConfig, nil)
		mockStorage.On("UpdateKmsConfigAttributes", ctx, "test-uuid", kmsConfig.KmsAttributes).Return(kmsConfig, nil)
		response := &models.KmsConfigCheck{
			KmsConfig:   &models.KmsConfig{BaseModel: models.BaseModel{UUID: "test-uuid"}},
			IsHealthy:   true,
			HealthError: "",
		}
		result, err := orch.CheckAndUpdateKmsConfigHealth(ctx, response)
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})
	t.Run("UpdateKmsConfigHealthKeepsStateInUseWhenHealthyAndInUse", func(tt *testing.T) {
		mockStorage := new(database.MockStorage)
		orch := Orchestrator{storage: mockStorage}
		ctx := context.Background()
		kmsConfig := &datamodel.KmsConfig{
			BaseModel:     datamodel.BaseModel{UUID: "test-uuid"},
			State:         models.LifeCycleStateInUse,
			KeyName:       "key1",
			KeyRing:       "ring1",
			KmsAttributes: &datamodel.KmsAttributes{},
		}
		defer func() {
			isKmsConfigInUse = _isKmsConfigInUse
		}()
		isKmsConfigInUse = func(ctx context.Context, se database.Storage, kmsConfig *datamodel.KmsConfig) (bool, error) {
			return false, nil
		}
		mockStorage.On("GetKmsConfig", ctx, "test-uuid").Return(kmsConfig, nil)
		mockStorage.On("UpdateKmsConfigState", ctx, "test-uuid", models.LifeCycleStateREADY, models.LifeCycleStateReadyDetails).Return(kmsConfig, nil)
		mockStorage.On("UpdateKmsConfigAttributes", ctx, "test-uuid", kmsConfig.KmsAttributes).Return(kmsConfig, nil)

		response := &models.KmsConfigCheck{
			KmsConfig:   &models.KmsConfig{BaseModel: models.BaseModel{UUID: "test-uuid"}},
			IsHealthy:   true,
			HealthError: "",
		}
		result, err := orch.CheckAndUpdateKmsConfigHealth(ctx, response)
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})
	t.Run("UpdateKmsConfigHealthSetsStateToErrorWhenUnhealthy", func(tt *testing.T) {
		mockStorage := new(database.MockStorage)
		orch := Orchestrator{storage: mockStorage}
		ctx := context.Background()
		kmsConfig := &datamodel.KmsConfig{
			BaseModel:     datamodel.BaseModel{UUID: "test-uuid"},
			State:         models.LifeCycleStateAvailable,
			KeyName:       "key1",
			KeyRing:       "ring1",
			KmsAttributes: &datamodel.KmsAttributes{},
		}
		isKmsConfigInUse = func(ctx context.Context, se database.Storage, kmsConfig *datamodel.KmsConfig) (bool, error) {
			return true, nil
		}
		mockStorage.On("GetKmsConfig", ctx, "test-uuid").Return(kmsConfig, nil)
		mockStorage.On("UpdateKmsConfigState", ctx, "test-uuid", models.LifeCycleStateError, "some error").Return(kmsConfig, nil)
		mockStorage.On("UpdateKmsConfigAttributes", ctx, "test-uuid", kmsConfig.KmsAttributes).Return(kmsConfig, nil)

		response := &models.KmsConfigCheck{
			KmsConfig:   &models.KmsConfig{BaseModel: models.BaseModel{UUID: "test-uuid"}},
			IsHealthy:   false,
			HealthError: "some error",
		}
		result, err := orch.CheckAndUpdateKmsConfigHealth(ctx, response)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		isKmsConfigInUse = _isKmsConfigInUse
	})
	t.Run("UpdateKmsConfigHealthSetsStateToCreatedWhenHealthErrorMatchesKeyNotFound", func(tt *testing.T) {
		mockStorage := new(database.MockStorage)
		orch := Orchestrator{storage: mockStorage}
		ctx := context.Background()
		kmsConfig := &datamodel.KmsConfig{
			BaseModel:     datamodel.BaseModel{UUID: "test-uuid"},
			State:         models.LifeCycleStateCreated,
			KeyName:       "key1",
			KeyRing:       "ring1",
			KmsAttributes: &datamodel.KmsAttributes{},
		}
		defer func() {
			isKmsConfigInUse = _isKmsConfigInUse
		}()
		healthError := strings.Replace(strings.Replace(GcpKmsConfigHealthError, "<key_name>", "key1", 1), "<key_ring>", "ring1", 1)
		isKmsConfigInUse = func(ctx context.Context, se database.Storage, kmsConfig *datamodel.KmsConfig) (bool, error) {
			return true, nil
		}
		mockStorage.On("GetKmsConfig", ctx, "test-uuid").Return(kmsConfig, nil)
		mockStorage.On("UpdateKmsConfigState", ctx, "test-uuid", models.LifeCycleStateCreated, healthError).Return(kmsConfig, nil)
		mockStorage.On("UpdateKmsConfigAttributes", ctx, "test-uuid", kmsConfig.KmsAttributes).Return(kmsConfig, nil)

		response := &models.KmsConfigCheck{
			KmsConfig:   &models.KmsConfig{BaseModel: models.BaseModel{UUID: "test-uuid"}},
			IsHealthy:   false,
			HealthError: healthError,
		}
		result, err := orch.CheckAndUpdateKmsConfigHealth(ctx, response)
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})
	t.Run("UpdateKmsConfigHealthReturnsErrorWhenGetKmsConfigFails", func(tt *testing.T) {
		mockStorage := new(database.MockStorage)
		orch := Orchestrator{storage: mockStorage}
		ctx := context.Background()
		mockStorage.On("GetKmsConfig", ctx, "test-uuid").Return(nil, errors.New("some error"))
		response := &models.KmsConfigCheck{
			KmsConfig:   &models.KmsConfig{BaseModel: models.BaseModel{UUID: "test-uuid"}},
			IsHealthy:   true,
			HealthError: "",
		}
		result, err := orch.CheckAndUpdateKmsConfigHealth(ctx, response)
		assert.Error(t, err)
		assert.Nil(t, result)
	})
	t.Run("UpdateKmsConfigHealthReturnsErrorWhenIsKmsConfigInUseFails", func(tt *testing.T) {
		mockStorage := new(database.MockStorage)
		orch := Orchestrator{storage: mockStorage}
		ctx := context.Background()
		kmsConfig := &datamodel.KmsConfig{
			BaseModel:     datamodel.BaseModel{UUID: "test-uuid"},
			State:         models.LifeCycleStateAvailable,
			KeyName:       "key1",
			KeyRing:       "ring1",
			KmsAttributes: &datamodel.KmsAttributes{},
		}
		mockStorage.On("GetKmsConfig", ctx, "test-uuid").Return(kmsConfig, nil)
		defer func() {
			isKmsConfigInUse = _isKmsConfigInUse
		}()
		isKmsConfigInUse = func(ctx context.Context, se database.Storage, kmsConfig *datamodel.KmsConfig) (bool, error) {
			return false, errors.New("some error")
		}

		response := &models.KmsConfigCheck{
			KmsConfig:   &models.KmsConfig{BaseModel: models.BaseModel{UUID: "test-uuid"}},
			IsHealthy:   true,
			HealthError: "",
		}
		result, err := orch.CheckAndUpdateKmsConfigHealth(ctx, response)
		assert.Error(t, err)
		assert.Nil(t, result)
	})
	t.Run("UpdateKmsConfigHealthReturnsErrorWhenUpdateKmsConfigStateFails", func(tt *testing.T) {
		mockStorage := new(database.MockStorage)
		orch := Orchestrator{storage: mockStorage}
		ctx := context.Background()
		kmsConfig := &datamodel.KmsConfig{
			BaseModel: datamodel.BaseModel{UUID: "test-uuid"},
			State:     models.LifeCycleStateError,
			KeyName:   "key1",
			KeyRing:   "ring1",
		}
		isKmsConfigInUse = func(ctx context.Context, se database.Storage, kmsConfig *datamodel.KmsConfig) (bool, error) {
			return false, nil
		}
		mockStorage.On("GetKmsConfig", ctx, "test-uuid").Return(kmsConfig, nil)
		mockStorage.On("UpdateKmsConfigState", ctx, "test-uuid", models.LifeCycleStateREADY, models.LifeCycleStateReadyDetails).Return(nil, errors.New("update error"))

		response := &models.KmsConfigCheck{
			KmsConfig:   &models.KmsConfig{BaseModel: models.BaseModel{UUID: "test-uuid"}},
			IsHealthy:   true,
			HealthError: "",
		}
		result, err := orch.CheckAndUpdateKmsConfigHealth(ctx, response)
		assert.Error(t, err)
		assert.Nil(t, result)
		isKmsConfigInUse = _isKmsConfigInUse
	})
	t.Run("UpdateKmsConfigHealthReturnsErrorUpdateKmsConfigAttributesFails", func(tt *testing.T) {
		mockStorage := new(database.MockStorage)
		orch := Orchestrator{storage: mockStorage}
		ctx := context.Background()
		kmsConfig := &datamodel.KmsConfig{
			BaseModel:     datamodel.BaseModel{UUID: "test-uuid"},
			State:         models.LifeCycleStateError,
			KeyName:       "key1",
			KeyRing:       "ring1",
			KmsAttributes: &datamodel.KmsAttributes{},
		}
		isKmsConfigInUse = func(ctx context.Context, se database.Storage, kmsConfig *datamodel.KmsConfig) (bool, error) {
			return false, nil
		}
		mockStorage.On("GetKmsConfig", ctx, "test-uuid").Return(kmsConfig, nil)
		mockStorage.On("UpdateKmsConfigState", ctx, "test-uuid", models.LifeCycleStateREADY, models.LifeCycleStateReadyDetails).Return(kmsConfig, nil)
		mockStorage.On("UpdateKmsConfigAttributes", ctx, "test-uuid", kmsConfig.KmsAttributes).Return(nil, errors.New("some thing went wrong"))
		response := &models.KmsConfigCheck{
			KmsConfig:   &models.KmsConfig{BaseModel: models.BaseModel{UUID: "test-uuid"}},
			IsHealthy:   true,
			HealthError: "",
		}
		result, err := orch.CheckAndUpdateKmsConfigHealth(ctx, response)
		assert.Error(t, err)
		assert.Nil(t, result)
		isKmsConfigInUse = _isKmsConfigInUse
	})
}

func TestAccessKmsCryptoKey(t *testing.T) {
	t.Run("TestAccessKmsCryptoKeyReturnsErrorWhenDecryptPasswordFails", func(tt *testing.T) {
		mockStorage := new(database.MockStorage)
		orch := Orchestrator{storage: mockStorage}
		mockLogger := log.NewLogger()
		ctx := context.WithValue(context.Background(), middleware.ContextSLoggerKey, mockLogger)
		kmsConfig := &models.KmsConfig{
			ServiceAccount: &models.ServiceAccount{ServiceAccountPasswordLocation: "mock-location"},
			KmsAttributes:  &models.KmsAttributes{SdeServiceAccountEmail: "sde@project.iam.gserviceaccount.com"},
		}
		origDecryptPassword := utils.DecryptPassword
		defer func() { utils.DecryptPassword = origDecryptPassword }()
		utils.DecryptPassword = func(_ log.Secret) (*string, error) {
			return nil, errors.New("decrypt error")
		}
		mockStorage.On("UpdateKmsConfigState", ctx, kmsConfig.UUID, models.LifeCycleStateError, mock.Anything).Return(&datamodel.KmsConfig{}, nil)
		err := orch.AccessKmsCryptoKey(ctx, kmsConfig)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "decrypt error")
	})
	t.Run("TestAccessKmsCryptoKeyReturnsErrorWhenBase64DecodeFails", func(tt *testing.T) {
		mockLogger := log.NewLogger()
		ctx := context.WithValue(context.Background(), middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)
		orch := Orchestrator{storage: mockStorage}
		kmsConfig := &models.KmsConfig{
			ServiceAccount: &models.ServiceAccount{ServiceAccountPasswordLocation: "mock-location"},
			KmsAttributes:  &models.KmsAttributes{SdeServiceAccountEmail: "sde@project.iam.gserviceaccount.com"},
		}
		origDecryptPassword := utils.DecryptPassword
		defer func() { utils.DecryptPassword = origDecryptPassword }()
		utils.DecryptPassword = func(_ log.Secret) (*string, error) {
			s := "not-base64"
			return &s, nil
		}
		mockStorage.On("UpdateKmsConfigState", ctx, kmsConfig.UUID, models.LifeCycleStateError, mock.Anything).Return(&datamodel.KmsConfig{}, nil)
		err := orch.AccessKmsCryptoKey(ctx, kmsConfig)
		assert.Error(t, err)
	})
	t.Run("TestAccessKmsCryptoKeyRetryDoFail", func(tt *testing.T) {
		mockLogger := log.NewLogger()
		ctx := context.WithValue(context.Background(), middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)
		orch := Orchestrator{storage: mockStorage}
		kmsConfig := &models.KmsConfig{
			ServiceAccount: &models.ServiceAccount{ServiceAccountPasswordLocation: "mock-location"},
			KmsAttributes:  &models.KmsAttributes{SdeServiceAccountEmail: "sde@project.iam.gserviceaccount.com"},
		}
		origDecryptPassword := utils.DecryptPassword

		defer func() {
			utils.DecryptPassword = origDecryptPassword
			retryDo = retry.RetryDoWithTimeout
		}()
		utils.DecryptPassword = func(_ log.Secret) (*string, error) {
			s := "ewogICJ0eXBlIjogInNlcnZpY2VfYWNjb3VudCIsCiAgInByb2plY3RfaWQiOiAicGhhc2UzLWhvc3QiLAogICJlbXVsYXRpb24iOiAibG9jYWxob3N0OjkwMTEiCn0K"
			return &s, nil
		}

		retryDo = func(ctx context.Context, timeout, wait time.Duration, caller string, fn retry.Retriable) error {
			return errors.New("retry error")
		}
		mockStorage.On("UpdateKmsConfigState", ctx, kmsConfig.UUID, models.LifeCycleStateError, mock.Anything).Return(&datamodel.KmsConfig{}, nil)
		err := orch.AccessKmsCryptoKey(ctx, kmsConfig)
		assert.Error(t, err)
	})
	t.Run("TestAccessKmsCryptoKeyRetryCreateCryptoKey", func(tt *testing.T) {
		mockLogger := log.NewLogger()
		ctx := context.WithValue(context.Background(), middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)
		orch := Orchestrator{storage: mockStorage}
		kmsConfig := &models.KmsConfig{
			ServiceAccount: &models.ServiceAccount{ServiceAccountPasswordLocation: "mock-location"},
			KmsAttributes:  &models.KmsAttributes{SdeServiceAccountEmail: "sde@project.iam.gserviceaccount.com"},
		}
		origDecryptPassword := utils.DecryptPassword

		defer func() {
			utils.DecryptPassword = origDecryptPassword
		}()
		utils.DecryptPassword = func(_ log.Secret) (*string, error) {
			s := "ewogICJ0eXBlIjogInNlcnZpY2VfYWNjb3VudCIsCiAgInByb2plY3RfaWQiOiAicGhhc2UzLWhvc3QiLAogICJlbXVsYXRpb24iOiAibG9jYWxob3N0OjkwMTEiCn0K"
			return &s, nil
		}

		mockStorage.On("UpdateKmsConfigState", ctx, kmsConfig.UUID, models.LifeCycleStateError, mock.Anything).Return(&datamodel.KmsConfig{}, nil)
		err := orch.AccessKmsCryptoKey(ctx, kmsConfig)
		assert.Error(t, err)
	})
}
