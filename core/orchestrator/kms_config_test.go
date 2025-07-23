package orchestrator

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	cvpModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/kms_activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine"
	"go.temporal.io/sdk/client"
	"gorm.io/gorm"
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
		{BaseModel: datamodel.BaseModel{UUID: "uuid1", DeletedAt: nil}, Name: "kmsConfig1", ServiceAccountID: &serviceAccounts[0].ID,
			KmsAttributes: &datamodel.KmsAttributes{SdeServiceAccountEmail: "sdeServiceAccount1@account.com"}},
		{BaseModel: datamodel.BaseModel{UUID: "uuid2", DeletedAt: nil}, Name: "kmsConfig2", ServiceAccountID: &serviceAccounts[1].ID,
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

func TestMigrateKmsConfig(t *testing.T) {
	ctx := context.Background()
	mockLogger := log.NewLogger()
	store, err := database.SetupStorageForTest(mockLogger)
	if err != nil {
		t.Fatalf("Failed to create test storage: %v", err)
	}

	err = database.ClearInMemoryDB(store.DB())
	if err != nil {
		t.Fatalf("Failed to clean up test storage: %v", err)
	}

	accounts := []*datamodel.Account{{BaseModel: datamodel.BaseModel{UUID: "uuid1", ID: int64(1)}, Name: "account1"}}
	err = store.DB().Create(accounts).Error
	if err != nil {
		t.Fatalf("Failed to create Accounts table: %v", err)
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
		{BaseModel: datamodel.BaseModel{UUID: "uuid1", DeletedAt: nil}, Name: "kmsConfig1", ServiceAccountID: &serviceAccounts[0].ID, State: models.LifeCycleStateCreated,
			KmsAttributes: &datamodel.KmsAttributes{SdeServiceAccountEmail: "sdeServiceAccount1@account.com", SdeKmsConfigUUID: "sdeUuid1"}},
		{BaseModel: datamodel.BaseModel{UUID: "uuid2", DeletedAt: nil}, Name: "kmsConfig2", ServiceAccountID: &serviceAccounts[1].ID,
			KmsAttributes: &datamodel.KmsAttributes{SdeServiceAccountEmail: "sdeServiceAccount2@account.com", SdeKmsConfigUUID: ""}},
		{BaseModel: datamodel.BaseModel{UUID: "uuid3", DeletedAt: nil}, Name: "kmsConfig3", ServiceAccountID: &serviceAccounts[1].ID, State: models.LifeCycleStateREADY,
			KmsAttributes: &datamodel.KmsAttributes{SdeServiceAccountEmail: "sdeServiceAccount1@account.com", SdeKmsConfigUUID: "sdeUuid1"}},
		{BaseModel: datamodel.BaseModel{UUID: "uuid4", DeletedAt: nil}, Name: "kmsConfig4", ServiceAccountID: &serviceAccounts[1].ID},
	}
	err = store.DB().Create(kmsConfigs).Error
	if err != nil {
		t.Fatalf("Failed to create KMS Configs table: %v", err)
	}

	mockTemporal := new(workflow_engine.MockTemporalTestClient)

	orchInstance := Orchestrator{
		storage:  store,
		temporal: mockTemporal,
	}

	t.Run("WhenGetKmsConfigByUUIDReturnsRecordWithoutKmsAttributes", func(tt *testing.T) {
		params := common.MigrateKmsConfigParams{
			LocationID:     "home-location",
			ProjectNumber:  "my-project",
			UUID:           "uuid4",
			AccountName:    "account1",
			XCorrelationID: "",
		}
		result, errMigrate := orchInstance.MigrateKmsConfig(context.Background(), &params)
		assert.Error(tt, errMigrate)
		assert.Equal(tt, "KmsAttributes property not present within KmsConfig DB entry in VCP", errMigrate.Error())
		assert.Equal(tt, "", result)
	})
	t.Run("WhenGetKmsConfigByUUIDReturnsRecordWithEmptySdeUUID", func(tt *testing.T) {
		params := common.MigrateKmsConfigParams{
			LocationID:     "home-location",
			ProjectNumber:  "my-project",
			UUID:           "uuid2",
			AccountName:    "account1",
			XCorrelationID: "",
		}
		result, errMigrate := orchInstance.MigrateKmsConfig(context.Background(), &params)
		assert.Error(tt, errMigrate)
		assert.Equal(tt, "KmsAttributes property not present within KmsConfig DB entry in VCP", errMigrate.Error())
		assert.Equal(tt, "", result)
	})
	t.Run("WhenValidateKmsConfigForMigrationFails", func(tt *testing.T) {
		params := common.MigrateKmsConfigParams{
			LocationID:     "home-location",
			ProjectNumber:  "my-project",
			UUID:           "uuid1",
			AccountName:    "account1",
			XCorrelationID: "",
		}

		result, errMigrate := orchInstance.MigrateKmsConfig(ctx, &params)
		assert.Error(tt, errMigrate)
		assert.Equal(tt, "CMEK Configuration needs to be in either Ready or In_Use state for migration", errMigrate.Error())
		assert.Equal(tt, "", result)
	})
	t.Run("WhenMigrateKmsSuccessfulWithKmsConfigRecordNotFoundInVcpDB", func(tt *testing.T) {
		params := common.MigrateKmsConfigParams{
			LocationID:     "home-location",
			ProjectNumber:  "my-project",
			UUID:           "uuid99",
			AccountName:    "account1",
			XCorrelationID: "",
			State:          models.LifeCycleStateREADY,
		}
		mockTemporal.On("ExecuteWorkflow", ctx, mock.Anything, mock.Anything, &params).Return(nil, nil)

		result, errMigrate := orchInstance.MigrateKmsConfig(ctx, &params)
		assert.NoError(tt, errMigrate)
		assert.NotEmpty(tt, result)
		assert.Equal(tt, "uuid99", params.SdeUUID)
	})
	t.Run("WhenTemporalWorkflowReturnsError", func(tt *testing.T) {
		mockTemporall := new(workflow_engine.MockTemporalTestClient)
		orchInstancee := Orchestrator{
			storage:  store,
			temporal: mockTemporall,
		}

		params := common.MigrateKmsConfigParams{
			LocationID:     "home-location",
			ProjectNumber:  "my-project",
			UUID:           "uuid3",
			AccountName:    "account1",
			XCorrelationID: "",
			State:          models.LifeCycleStateREADY,
		}
		mockTemporall.On("ExecuteWorkflow", ctx, mock.Anything, mock.Anything, &params).Return(nil, errors.New("This is a Temporal error"))

		result, errMigrate := orchInstancee.MigrateKmsConfig(ctx, &params)
		assert.Error(tt, errMigrate)
		assert.Equal(tt, "This is a Temporal error", errMigrate.Error())
		assert.Equal(tt, "", result)
	})
	t.Run("WhenCreateJobReturnsError", func(tt *testing.T) {
		params := common.MigrateKmsConfigParams{
			LocationID:     "home-location",
			ProjectNumber:  "my-project",
			UUID:           "uuid3",
			AccountName:    "account1",
			XCorrelationID: "",
			State:          models.LifeCycleStateREADY,
		}
		err = store.DB().Migrator().DropTable(&datamodel.Job{})
		assert.NoError(tt, err)

		result, errMigrate := orchInstance.MigrateKmsConfig(context.Background(), &params)
		assert.Error(tt, errMigrate)
		assert.Equal(tt, "no such table: jobs", errMigrate.Error())
		assert.Equal(tt, "", result)
	})
	t.Run("WhenGetKmsConfigByUUIDReturnsError", func(tt *testing.T) {
		params := common.MigrateKmsConfigParams{
			LocationID:     "home-location",
			ProjectNumber:  "my-project",
			UUID:           "uuid1",
			AccountName:    "account1",
			XCorrelationID: "",
		}
		err = store.DB().Migrator().DropTable(&datamodel.KmsConfig{})
		assert.NoError(tt, err)

		result, errMigrate := orchInstance.MigrateKmsConfig(context.Background(), &params)
		assert.Error(tt, errMigrate)
		assert.Equal(tt, "no such table: kms_configs", errMigrate.Error())
		assert.Equal(tt, "", result)
	})
}

func TestConvertDataStoreKmsConfigToModel(t *testing.T) {
	t.Run("ReturnsValidKmsConfigWhenAllFieldsArePopulated", func(t *testing.T) {
		saId := int64(1234)
		kmsConfig := &datamodel.KmsConfig{
			Name:              "test-name",
			Description:       "test-description",
			State:             "ACTIVE",
			StateDetails:      "",
			KeyRing:           "test-key-ring",
			KeyRingLocation:   "test-location",
			KeyName:           "test-key-name",
			AccountID:         int64(1234),
			CustomerProjectID: "test-customer-project-id",
			KeyProjectID:      "test-key-project-id",
			ServiceAccountID:  &saId,
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
		saId := int64(1234)
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
			ServiceAccountID:  &saId,
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
			ResourceID:  "updated-kms-config",
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
		mockStorage.On("IsKmsConfigInUse", ctx, dbKmsConfig.UUID).Return(false, nil)
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
			ResourceID:  "updated-kms-config",
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
			ResourceID:  "updated-kms-config",
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
		mockStorage.On("IsKmsConfigInUse", ctx, dbKmsConfig.UUID).Return(true, nil)
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
			ResourceID:  "updated-kms-config",
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
		mockStorage.On("IsKmsConfigInUse", ctx, dbKmsConfig.UUID).Return(false, errors.New("error not found"))
		mockStorage.On("GetKmsConfig", ctx, "test-kms-config-id").Return(dbKmsConfig, nil)

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
			ResourceID:  "updated-kms-config",
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
		mockStorage.On("IsKmsConfigInUse", ctx, dbKmsConfig.UUID).Return(false, nil)
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
			ResourceID:  "updated-kms-config",
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
		mockStorage.On("IsKmsConfigInUse", ctx, dbKmsConfig.UUID).Return(false, nil)
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

func TestCheckAndUpdateKmsConfigHealth(t *testing.T) {
	t.Run("WhenSuccess", func(tt *testing.T) {
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
		orig := kms_activities.UpdateKmsConfigHealth
		defer func() {
			kms_activities.UpdateKmsConfigHealth = orig
		}()
		kms_activities.UpdateKmsConfigHealth = func(ctx context.Context, se database.Storage, configCheck *models.KmsConfigCheck) (*datamodel.KmsConfig, error) {
			return kmsConfig, nil
		}
		response := &models.KmsConfigCheck{
			KmsConfig:   &models.KmsConfig{BaseModel: models.BaseModel{UUID: "test-uuid"}},
			IsHealthy:   true,
			HealthError: "",
		}
		result, err := orch.CheckAndUpdateKmsConfigHealth(ctx, response)
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})
	t.Run("WhenFailure", func(tt *testing.T) {
		mockStorage := new(database.MockStorage)
		orch := Orchestrator{storage: mockStorage}
		ctx := context.Background()
		orig := kms_activities.UpdateKmsConfigHealth
		defer func() {
			kms_activities.UpdateKmsConfigHealth = orig
		}()
		kms_activities.UpdateKmsConfigHealth = func(ctx context.Context, se database.Storage, configCheck *models.KmsConfigCheck) (*datamodel.KmsConfig, error) {
			return nil, errors.New("some error")
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
}

func TestAccessKmsCryptoKey(t *testing.T) {
	t.Run("AccessKmsCryptoKeyReturnsNoErrorWhenStorageSucceeds", func(tt *testing.T) {
		mockStorage := new(database.MockStorage)
		orch := Orchestrator{storage: mockStorage}
		ctx := context.Background()
		kmsConfig := &models.KmsConfig{BaseModel: models.BaseModel{UUID: "test-uuid"}}
		dbKmsConfig := &datamodel.KmsConfig{BaseModel: datamodel.BaseModel{UUID: "test-uuid"}}

		mockStorage.On("GetKmsConfig", ctx, "test-uuid").Return(dbKmsConfig, nil)
		origAccessCryptoKey := kms_activities.AccessCryptoKey
		defer func() { kms_activities.AccessCryptoKey = origAccessCryptoKey }()
		kms_activities.AccessCryptoKey = func(ctx context.Context, dbKmsConfig *datamodel.KmsConfig) error {
			return nil
		}

		err := orch.AccessCryptoKeyWithImpersonation(ctx, kmsConfig)
		assert.NoError(tt, err)
	})

	t.Run("AccessKmsCryptoKeyReturnsErrorWhenGetKmsConfigFails", func(tt *testing.T) {
		mockStorage := new(database.MockStorage)
		orch := Orchestrator{storage: mockStorage}
		ctx := context.Background()
		kmsConfig := &models.KmsConfig{BaseModel: models.BaseModel{UUID: "test-uuid"}}

		mockStorage.On("GetKmsConfig", ctx, "test-uuid").Return(nil, errors.New("get error"))

		err := orch.AccessCryptoKeyWithImpersonation(ctx, kmsConfig)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "get error")
	})

	t.Run("AccessKmsCryptoKeyReturnsErrorWhenAccessCryptoKeyFails", func(tt *testing.T) {
		mockStorage := new(database.MockStorage)
		orch := Orchestrator{storage: mockStorage}
		ctx := context.Background()
		kmsConfig := &models.KmsConfig{BaseModel: models.BaseModel{UUID: "test-uuid"}}
		dbKmsConfig := &datamodel.KmsConfig{BaseModel: datamodel.BaseModel{UUID: "test-uuid"}}

		mockStorage.On("GetKmsConfig", ctx, "test-uuid").Return(dbKmsConfig, nil)
		origAccessCryptoKey := kms_activities.AccessCryptoKey
		defer func() { kms_activities.AccessCryptoKey = origAccessCryptoKey }()
		kms_activities.AccessCryptoKey = func(ctx context.Context, dbKmsConfig *datamodel.KmsConfig) error {
			return errors.New("access error")
		}

		err := orch.AccessCryptoKeyWithImpersonation(ctx, kmsConfig)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "access error")
	})
}

func TestGetKmsConfigByKeyFullPath(t *testing.T) {
	t.Run("GetKmsConfigByKeyFullPathReturnsKmsConfigOnSuccess", func(t *testing.T) {
		ctx := context.Background()
		mockStorage := new(database.MockStorage)
		params := &common.GetKmsConfigParams{AccountName: "test-account", KeyFullPath: "projects/p/locations/l/keyRings/r/cryptoKeys/k"}
		expectedAccount := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "uuid"}, Name: "test-account"}
		expectedKmsConfig := &datamodel.KmsConfig{BaseModel: datamodel.BaseModel{UUID: "uuid"}, KeyName: "k", KmsAttributes: &datamodel.KmsAttributes{},
			ServiceAccount: &datamodel.ServiceAccount{}}
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return expectedAccount, nil
		}
		defer func() { getOrCreateAccount = _getOrCreateAccount }()
		mockStorage.On("GetKmsConfigByKeyFullPath", ctx, params.KeyFullPath).Return(expectedKmsConfig, nil)

		result, err := _getKmsConfigByKeyFullPath(ctx, mockStorage, params)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, "uuid", result.UUID)
	})
	t.Run("GetKmsConfigByKeyFullPathReturnsErrorWhenAccountFails", func(t *testing.T) {
		ctx := context.Background()
		mockStorage := new(database.MockStorage)
		params := &common.GetKmsConfigParams{AccountName: "fail-account", KeyFullPath: "projects/p/locations/l/keyRings/r/cryptoKeys/k"}
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.New("account error")
		}
		defer func() { getOrCreateAccount = _getOrCreateAccount }()

		result, err := _getKmsConfigByKeyFullPath(ctx, mockStorage, params)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "account error")
	})
	t.Run("GetKmsConfigByKeyFullPathReturnsErrorWhenStorageFails", func(t *testing.T) {
		ctx := context.Background()
		mockStorage := new(database.MockStorage)
		params := &common.GetKmsConfigParams{AccountName: "test-account", KeyFullPath: "projects/p/locations/l/keyRings/r/cryptoKeys/k"}
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}}, nil
		}
		defer func() { getOrCreateAccount = _getOrCreateAccount }()
		mockStorage.On("GetKmsConfigByKeyFullPath", ctx, params.KeyFullPath).Return(nil, errors.New("db error"))

		result, err := _getKmsConfigByKeyFullPath(ctx, mockStorage, params)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "db error")
	})
}

func TestOrchestratorGetKmsConfigByKeyFullPath(t *testing.T) {
	t.Run("OrchestratorGetKmsConfigByKeyFullPathReturnsKmsConfigOnSuccess", func(t *testing.T) {
		ctx := context.Background()
		mockStorage := new(database.MockStorage)
		orch := Orchestrator{storage: mockStorage}
		params := &common.GetKmsConfigParams{AccountName: "test-account", KeyFullPath: "projects/p/locations/l/keyRings/r/cryptoKeys/k"}
		expectedKmsConfig := &models.KmsConfig{BaseModel: models.BaseModel{UUID: "uuid"}}

		getKmsConfigByKeyFullPath = func(ctx context.Context, se database.Storage, params *common.GetKmsConfigParams) (*models.KmsConfig, error) {
			return expectedKmsConfig, nil
		}
		defer func() { getKmsConfigByKeyFullPath = _getKmsConfigByKeyFullPath }()

		result, err := orch.GetKmsConfigByKeyFullPath(ctx, params)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, "uuid", result.UUID)
	})
	t.Run("OrchestratorGetKmsConfigByKeyFullPathReturnsErrorOnFailure", func(t *testing.T) {
		ctx := context.Background()
		mockStorage := new(database.MockStorage)
		orch := Orchestrator{storage: mockStorage}
		params := &common.GetKmsConfigParams{AccountName: "fail-account", KeyFullPath: "projects/p/locations/l/keyRings/r/cryptoKeys/k"}

		getKmsConfigByKeyFullPath = func(ctx context.Context, se database.Storage, params *common.GetKmsConfigParams) (*models.KmsConfig, error) {
			return nil, errors.New("some error")
		}
		defer func() { getKmsConfigByKeyFullPath = _getKmsConfigByKeyFullPath }()

		result, err := orch.GetKmsConfigByKeyFullPath(ctx, params)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "some error")
	})
}

func TestDeleteKmsConfig(t *testing.T) {
	ctx := context.Background()

	t.Run("SuccessfulDelete", func(tt *testing.T) {
		mockStorage := new(database.MockStorage)
		mockTemporal := new(workflow_engine.MockTemporalTestClient)

		params := &common.DeleteKmsConfigParams{
			KmsConfigID: "test-kms-config-id",
			AccountName: "test-account",
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
		mockStorage.On("IsKmsConfigInUse", ctx, dbKmsConfig.UUID).Return(false, nil)
		mockStorage.On("GetKmsConfig", ctx, "test-kms-config-id").Return(dbKmsConfig, nil)
		mockStorage.On("GetSvmsByKmsConfigID", ctx, dbKmsConfig.ID).Return(nil, nil)
		mockStorage.On("ListOngoingPoolJobsWithKmsConfigId", ctx, dbKmsConfig.ID, dbKmsConfig.AccountID).Return(make([]*datamodel.Job, 0), nil)
		mockStorage.On("UpdateKmsConfigState", ctx, dbKmsConfig.UUID, models.LifeCycleStateDeleting, models.LifeCycleStateDeletingDetails).Return(dbKmsConfig, nil)
		mockStorage.On("CreateJob", ctx, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "test-job-uuid"},
		}, nil)

		// Mock Temporal client behavior
		mockTemporal.On("ExecuteWorkflow", ctx, mock.Anything, mock.Anything, dbKmsConfig, params).Return(nil, nil)

		kmsConfig, jobUUID, err := orchestrator.DeleteKmsConfig(ctx, params)

		assert.NoError(tt, err)
		assert.Equal(tt, "test-job-uuid", jobUUID)
		assert.NotNil(tt, kmsConfig)
		assert.Equal(tt, "test-kms-config-id", kmsConfig.UUID)
	})

	t.Run("SuccessfulUpdateWhenVcpKmsConfigNotFound", func(tt *testing.T) {
		mockStorage := new(database.MockStorage)
		mockTemporal := new(workflow_engine.MockTemporalTestClient)

		params := &common.DeleteKmsConfigParams{
			KmsConfigID: "test-kms-config-id",
			AccountName: "test-account",
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
		mockStorage.On("IsKmsConfigInUse", ctx, dbKmsConfig.UUID).Return(false, nil)
		mockStorage.On("GetKmsConfig", ctx, "test-kms-config-id").Return(nil, errors.NewNotFoundErr("kms config", nil))
		mockStorage.On("CreateJob", ctx, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "test-job-uuid"},
		}, nil)

		// Mock Temporal client behavior
		mockTemporal.On("ExecuteWorkflow", ctx, mock.Anything, mock.Anything, &datamodel.KmsConfig{KmsAttributes: &datamodel.KmsAttributes{SdeKmsConfigUUID: dbKmsConfig.UUID}}, params).Return(nil, nil)

		kmsConfig, jobUUID, err := orchestrator.DeleteKmsConfig(ctx, params)

		assert.NoError(tt, err)
		assert.Equal(tt, "test-job-uuid", jobUUID)
		assert.Nil(tt, kmsConfig)
	})

	t.Run("ValidationError", func(tt *testing.T) {
		mockStorage := new(database.MockStorage)
		mockTemporal := new(workflow_engine.MockTemporalTestClient)

		params := &common.DeleteKmsConfigParams{
			KmsConfigID: "test-kms-config-id",
			AccountName: "test-account",
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
		mockStorage.On("IsKmsConfigInUse", ctx, dbKmsConfig.UUID).Return(true, nil)
		mockStorage.On("GetKmsConfig", ctx, "test-kms-config-id").Return(dbKmsConfig, nil)
		mockStorage.On("GetSvmsByKmsConfigID", ctx, dbKmsConfig.ID).Return([]*datamodel.Svm{&datamodel.Svm{Name: "svm"}}, nil)

		// Mock Temporal client behavior
		mockTemporal.On("ExecuteWorkflow", ctx, mock.Anything, mock.Anything, dbKmsConfig, params).Return(nil, nil)

		kmsConfig, jobUUID, err := orchestrator.DeleteKmsConfig(ctx, params)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "can not delete this policy as it is still in use")
		assert.Nil(tt, kmsConfig)
		assert.Empty(tt, jobUUID)
	})

	t.Run("WhenSvmNotFound", func(tt *testing.T) {
		mockStorage := new(database.MockStorage)
		mockTemporal := new(workflow_engine.MockTemporalTestClient)

		params := &common.DeleteKmsConfigParams{
			KmsConfigID: "test-kms-config-id",
			AccountName: "test-account",
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
		mockStorage.On("IsKmsConfigInUse", ctx, dbKmsConfig.UUID).Return(false, errors.New("error"))
		mockStorage.On("GetSvmsByKmsConfigID", ctx, dbKmsConfig.ID).Return(nil, errors.New("error"))

		// Mock Temporal client behavior
		mockTemporal.On("ExecuteWorkflow", ctx, mock.Anything, mock.Anything, dbKmsConfig, params).Return(nil, nil)

		kmsConfig, jobUUID, err := orchestrator.DeleteKmsConfig(ctx, params)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "error")
		assert.Nil(tt, kmsConfig)
		assert.Empty(tt, jobUUID)
	})

	t.Run("WhenKmsConfigInCreatingState", func(tt *testing.T) {
		mockStorage := new(database.MockStorage)
		mockTemporal := new(workflow_engine.MockTemporalTestClient)

		params := &common.DeleteKmsConfigParams{
			KmsConfigID: "test-kms-config-id",
			AccountName: "test-account",
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
		mockStorage.On("IsKmsConfigInUse", ctx, dbKmsConfig.UUID).Return(false, nil)
		mockStorage.On("GetKmsConfig", ctx, "test-kms-config-id").Return(dbKmsConfig, nil)

		// Mock Temporal client behavior
		mockTemporal.On("ExecuteWorkflow", ctx, mock.Anything, mock.Anything, dbKmsConfig, params).Return(nil, nil)

		kmsConfig, jobUUID, err := orchestrator.DeleteKmsConfig(ctx, params)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "can not delete a gcpKmsConfig which is in creating state.")
		assert.Nil(tt, kmsConfig)
		assert.Empty(tt, jobUUID)
	})

	t.Run("WorkflowExecutionFailure", func(tt *testing.T) {
		mockStorage := new(database.MockStorage)
		mockTemporal := new(workflow_engine.MockTemporalTestClient)

		params := &common.DeleteKmsConfigParams{
			KmsConfigID: "test-kms-config-id",
			AccountName: "test-account",
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
		mockStorage.On("IsKmsConfigInUse", ctx, dbKmsConfig.UUID).Return(false, nil)
		mockStorage.On("GetKmsConfig", ctx, "test-kms-config-id").Return(dbKmsConfig, nil)
		mockStorage.On("GetSvmsByKmsConfigID", ctx, dbKmsConfig.ID).Return(nil, nil)
		mockStorage.On("ListOngoingPoolJobsWithKmsConfigId", ctx, dbKmsConfig.ID, dbKmsConfig.AccountID).Return(make([]*datamodel.Job, 0), nil)
		mockStorage.On("UpdateKmsConfigState", ctx, dbKmsConfig.UUID, models.LifeCycleStateDeleting, models.LifeCycleStateDeletingDetails).Return(dbKmsConfig, nil)
		mockStorage.On("CreateJob", ctx, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "test-job-uuid"},
		}, nil)

		// Mock Temporal client behavior
		mockTemporal.On("ExecuteWorkflow", ctx, mock.Anything, mock.Anything, dbKmsConfig, params).Return(nil, errors.New("workflow execution failed"))

		kmsConfig, jobUUID, err := orchestrator.DeleteKmsConfig(ctx, params)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "workflow execution failed")
		assert.Nil(tt, kmsConfig)
		assert.Empty(tt, jobUUID)
	})
}

func TestValidateKmsConfigState(t *testing.T) {
	ctx := context.Background()
	mockLogger := log.NewLogger()
	store, err := database.SetupStorageForTest(mockLogger)
	if err != nil {
		t.Fatalf("Failed to create test storage: %v", err)
	}

	err = database.ClearInMemoryDB(store.DB())
	if err != nil {
		t.Fatalf("Failed to clean up test storage: %v", err)
	}

	jobs := []*datamodel.Job{{BaseModel: datamodel.BaseModel{UUID: "uuid1"}, Type: "MIGRATE_KMS_CONFIG", State: "NEW",
		AccountID: sql.NullInt64{Int64: 1, Valid: true}}}

	err = store.DB().Create(jobs).Error
	if err != nil {
		t.Fatalf("Failed to create Jobs table: %v", err)
	}
	t.Run("WhenKmsConfigStateIsNotReadyOrInUse", func(tt *testing.T) {
		jobId, errValidate := validateKmsConfigState(ctx, store, models.LifeCycleStateCreated, int64(1), true)
		assert.Equal(tt, jobId, "")
		assert.Error(tt, errValidate)
		assert.EqualError(tt, errValidate, "CMEK Configuration needs to be in either Ready or In_Use state for migration")
	})
	t.Run("WhenKmsConfigStateIsInReadyState", func(tt *testing.T) {
		jobId, errValidate := validateKmsConfigState(ctx, store, models.LifeCycleStateREADY, int64(1), true)

		assert.NoError(tt, errValidate)
		assert.Equal(tt, jobId, "")
	})
	t.Run("WhenKmsConfigStateIsMigratingAndDBEntryIsNotInVCP", func(tt *testing.T) {
		jobId, errValidate := validateKmsConfigState(ctx, store, models.LifeCycleStateMigrating, int64(1), false)

		assert.NoError(tt, errValidate)
		assert.Equal(tt, "uuid1", jobId)
	})
	t.Run("WhenKmsConfigStateIsUpdatingAndDBEntryIsInVCP", func(tt *testing.T) {
		jobId, errValidate := validateKmsConfigState(ctx, store, models.LifeCycleStateUpdating, int64(1), true)

		assert.NoError(tt, errValidate)
		assert.Equal(tt, "uuid1", jobId)
	})
	t.Run("WhenKmsConfigStateIsMigratingAndDBEntryIsInVCP", func(tt *testing.T) {
		jobId, errValidate := validateKmsConfigState(ctx, store, models.LifeCycleStateMigrating, int64(1), true)

		assert.NoError(tt, errValidate)
		assert.Equal(tt, "uuid1", jobId)
	})
}

func TestConvertKmsConfigStateV1beta(t *testing.T) {
	t.Run("ReturnsKeyCheckPendingForCreatedState", func(t *testing.T) {
		state, details := convertKmsConfigStateV1beta(models.LifeCycleStateCreated, "ignored")
		assert.Equal(t, cvpModels.KmsConfigV1betaKmsStateKEYCHECKPENDING, state)
		assert.Equal(t, "Credentials created and key check pending", details)
	})

	t.Run("ReturnsInUseForInUseState", func(t *testing.T) {
		state, details := convertKmsConfigStateV1beta(models.LifeCycleStateInUse, "ignored")
		assert.Equal(t, cvpModels.KmsConfigV1betaKmsStateINUSE, state)
		assert.Equal(t, "Kms config in use", details)
	})

	t.Run("ReturnsReadyForREADYState", func(t *testing.T) {
		state, details := convertKmsConfigStateV1beta(models.LifeCycleStateREADY, "ignored")
		assert.Equal(t, cvpModels.KmsConfigV1betaKmsStateREADY, state)
		assert.Equal(t, "Kms config is ready for use", details)
	})

	t.Run("ReturnsMigratingForMigratingState", func(t *testing.T) {
		state, details := convertKmsConfigStateV1beta(models.LifeCycleStateMigrating, "ignored")
		assert.Equal(t, cvpModels.KmsConfigV1betaKmsStateMIGRATING, state)
		assert.Equal(t, "Kms config is in migrating state", details)
	})

	t.Run("ReturnsEmptyForUnknownState", func(t *testing.T) {
		state, details := convertKmsConfigStateV1beta("unknown_state", "ignored")
		assert.Equal(t, "unknown_state", state)
		assert.Equal(t, "", details)
	})
}
