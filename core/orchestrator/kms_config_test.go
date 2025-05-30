package orchestrator

import (
	"context"
	"gorm.io/gorm"
	"testing"
	"time"

	"github.com/go-openapi/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	errors2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine"
)

func TestGetMultipleKmsConfigs(t *testing.T) {
	mockLogger := log.NewLogger()
	store, err := database.NewTestStorage(mockLogger)
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
		mockStorage.EXPECT().GetMultipleKmsConfigs(mock.Anything, mock.Anything).Return(nil, errors.New(500, "internal error"))
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
		mockStorage.On("GetKmsConfig", ctx, "test-kms-config-id").Return(nil, errors2.NewNotFoundErr("kms config", nil))
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
		mockStorage.On("GetSvmsByKmsConfigID", ctx, dbKmsConfig.ID).Return(nil, errors2.New("error"))

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
		mockTemporal.On("ExecuteWorkflow", ctx, mock.Anything, mock.Anything, dbKmsConfig, params).Return(nil, errors2.New("workflow execution failed"))

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
		mockStorage.On("GetSvmsByKmsConfigID", ctx, dbKmsConfig.ID).Return(nil, errors2.NewNotFoundErr("svm", nil))

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
		mockStorage.On("GetSvmsByKmsConfigID", ctx, dbKmsConfig.ID).Return(nil, errors2.New("some error"))

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
