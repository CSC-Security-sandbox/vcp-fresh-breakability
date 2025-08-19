package database

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils/gorm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"gorm.io/gorm"
)

func TestGetKmsConfig(t *testing.T) {
	t.Run("WhenKmsConfigExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			tt.Fatalf("Failed to clean up test database: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test_account",
		}

		kmsConfig := &datamodel.KmsConfig{
			BaseModel: datamodel.BaseModel{UUID: "test-uuid"},
			Name:      "test",
			AccountID: account.ID,
		}
		err = store.db.Create(account).Error()
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}
		err = store.db.Create(kmsConfig).Error()
		if err != nil {
			tt.Fatalf("Failed to create kms config: %v", err)
		}

		result, err := store.GetKmsConfig(context.Background(), "test-uuid")
		if err != nil {
			tt.Errorf("Expected no error, got %v", err)
		}
		if result.Name != kmsConfig.Name {
			tt.Errorf("Expected kms config name %v, got %v", kmsConfig.Name, result.Name)
		}
		if result.AccountID != account.ID {
			tt.Errorf("Expected account name %v, got %v", account.ID, result.AccountID)
		}
	})

	t.Run("WhenKmsConfigDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			tt.Fatalf("Failed to clean up test database: %v", err)
		}

		_, err = store.GetKmsConfig(context.Background(), "test-uuid")
		if err == nil {
			tt.Errorf("Expected error, got nil")
		}
		if !customerrors.IsNotFoundErr(err) {
			tt.Errorf("Expected error %v, got %v", gorm.ErrRecordNotFound, err)
		}
	})
}

func TestUpdateUpdateKmsConfigState(t *testing.T) {
	t.Run("WhenUpdateKmsConfigStateIsUpdatedSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test_account",
		}
		err = store.db.Create(account).Error()
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		kmsConfig := &datamodel.KmsConfig{
			Name:      "test_kms_config",
			AccountID: account.ID,
			BaseModel: datamodel.BaseModel{UUID: "test-uuid"},
		}

		err = store.db.Create(kmsConfig).Error()
		if err != nil {
			tt.Fatalf("Failed to create kms config: %v", err)
		}

		_, err = store.UpdateKmsConfigState(context.Background(), "test-uuid", models.LifeCycleStateUpdating, models.LifeCycleStateUpdatingDetails)
		assert.NoError(tt, err, "Expected no error, got %v", err)

		updatedkms, err1 := store.GetKmsConfig(context.Background(), "test-uuid")
		assert.NoError(tt, err1, "Expected no error, got %v", err1)
		assert.Equal(tt, models.LifeCycleStateUpdating, updatedkms.State, "Expected volume state %v, got %v", models.LifeCycleStateUpdating, updatedkms.State)
	})
	t.Run("WhenUpdateKmsConfigIsNotFound", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		kms := &datamodel.KmsConfig{
			BaseModel: datamodel.BaseModel{UUID: "dummy"},
			Name:      "test_volume_rep",
			State:     models.LifeCycleStateUpdating,
		}
		_, err = store.UpdateKmsConfigState(context.Background(), kms.UUID, models.LifeCycleStateUpdating, models.LifeCycleStateUpdatingDetails)
		assert.EqualError(tt, err, "KMS Configuration not found", "Expected no error, got %v", err)
	})
}

func TestGetMultipleKMSConfigs(t *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(t, err, "Failed to set up test database")
	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	assert.NoError(t, err, "Failed to clean up test database")

	serviceAccounts := []*datamodel.ServiceAccount{
		{BaseModel: datamodel.BaseModel{ID: int64(111), UUID: "uuid10"}, Name: "ServiceAccount1"},
		{BaseModel: datamodel.BaseModel{ID: int64(222), UUID: "uuid20"}, Name: "ServiceAccount2"},
	}
	kmsConfigs := []*datamodel.KmsConfig{
		{BaseModel: datamodel.BaseModel{UUID: "uuid1", DeletedAt: nil}, Name: "kmsConfig1", ServiceAccountID: &serviceAccounts[0].ID},
		{BaseModel: datamodel.BaseModel{UUID: "uuid2", DeletedAt: nil}, Name: "kmsConfig2", ServiceAccountID: &serviceAccounts[1].ID},
	}

	err = store.db.Create(serviceAccounts).Error()
	assert.NoError(t, err, "Failed to create Service account table")
	err = store.db.Create(kmsConfigs).Error()
	assert.NoError(t, err, "Failed to create KMS config table")

	t.Run("RetrievesKMSConfigsSuccessfully", func(tt *testing.T) {
		kmsConfigUUIDList := []string{"uuid1", "uuid2"}
		conditions := [][]interface{}{{"uuid in ?", kmsConfigUUIDList}}
		result, err := store.GetMultipleKmsConfigs(context.Background(), conditions)

		assert.NoError(tt, err)
		assert.Equal(tt, "kmsConfig1", result[0].Name)
		assert.Equal(tt, "kmsConfig2", result[1].Name)
		assert.Equal(tt, "ServiceAccount1", result[0].ServiceAccount.Name)
		assert.Equal(tt, "ServiceAccount2", result[1].ServiceAccount.Name)
	})
	t.Run("ReturnsEmptyWhenRecordsAreNotFound", func(tt *testing.T) {
		kmsConfigUUIDList := []string{"nonexistent-uuid"}
		conditions := [][]interface{}{{"uuid in ?", kmsConfigUUIDList}}
		result, err := store.GetMultipleKmsConfigs(context.Background(), conditions)

		assert.NoError(tt, err)
		assert.Empty(tt, result)
	})
	t.Run("HandlesEmptyUUIDListGracefully", func(tt *testing.T) {
		kmsConfigUUIDList := []string{}
		conditions := [][]interface{}{{"uuid in ?", kmsConfigUUIDList}}
		result, err := store.GetMultipleKmsConfigs(context.Background(), conditions)

		assert.NoError(tt, err)
		assert.Empty(tt, result)
	})
}

func TestGetMultipleKMSConfigsDBErrorCondition(t *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(t, err, "Failed to set up test database")
	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	assert.NoError(t, err, "Failed to clean up test database")

	t.Run("WhenRetrieveKMSConfigsRunsIntoDBError", func(tt *testing.T) {
		dbErr := db.Migrator().DropTable(&datamodel.KmsConfig{})
		if dbErr != nil {
			assert.Fail(tt, "Dropping table KmsConfig from in-memory DB failed; aborting test")
		}
		kmsConfigUUIDList := []string{"uuid1", "uuid2"}
		conditions := [][]interface{}{{"uuid in ?", kmsConfigUUIDList}}
		result, kmsErr := store.GetMultipleKmsConfigs(context.Background(), conditions)

		assert.Error(tt, kmsErr)
		assert.Nil(tt, result)
	})
}

func TestCreateGetUpdateListKmsConfigAndGetJob(t *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(t, err, "Failed to set up test database")
	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	assert.NoError(t, err, "Failed to clean up test database")

	serviceAccounts := []*datamodel.ServiceAccount{
		{BaseModel: datamodel.BaseModel{ID: int64(111), UUID: "uuid10", DeletedAt: nil}, Name: "ServiceAccount1", AccountID: 1111, State: KmsSaStateEnable},
		{BaseModel: datamodel.BaseModel{ID: int64(222), UUID: "uuid20"}, Name: "ServiceAccount2", AccountID: 2222},
	}
	accounts := []*datamodel.Account{
		{BaseModel: datamodel.BaseModel{ID: int64(1111), UUID: "uuid100"}, Name: "Account1"},
		{BaseModel: datamodel.BaseModel{ID: int64(2222), UUID: "uuid200"}, Name: "Account2"},
		{BaseModel: datamodel.BaseModel{ID: int64(3333), UUID: "uuid300"}, Name: "Account3"},
	}
	kmsConfigs := []*datamodel.KmsConfig{
		{BaseModel: datamodel.BaseModel{UUID: "uuid1", DeletedAt: nil}, Name: "kmsConfig1", ServiceAccountID: &serviceAccounts[0].ID, AccountID: 1111, State: "Ready", StateDetails: "Key is in Ready state"},
		{BaseModel: datamodel.BaseModel{UUID: "uuid2", DeletedAt: nil}, Name: "kmsConfig2", ServiceAccountID: &serviceAccounts[1].ID, AccountID: 2222, State: models.LifeCycleStateAvailable},
		{BaseModel: datamodel.BaseModel{UUID: "uuid3", DeletedAt: nil}, Name: "kmsConfig3", ServiceAccountID: &serviceAccounts[0].ID, AccountID: 2222},
		{BaseModel: datamodel.BaseModel{UUID: "uuid4", DeletedAt: nil}, Name: "kmsConfig4", ServiceAccountID: &serviceAccounts[1].ID, AccountID: 1111},
		{BaseModel: datamodel.BaseModel{UUID: "uuid5", DeletedAt: nil}, Name: "kmsConfig5", AccountID: 3333, State: "Ready", StateDetails: "Key is in Ready state", Description: "kms description"},
		{BaseModel: datamodel.BaseModel{UUID: "uuid6", DeletedAt: nil}, Name: "kmsConfig6", AccountID: 4444, State: models.LifeCycleStateCreating},
		{BaseModel: datamodel.BaseModel{UUID: "uuid7", DeletedAt: nil}, Name: "kmsConfig7", AccountID: 5555, State: models.LifeCycleStateDeleting},
		{BaseModel: datamodel.BaseModel{UUID: "uuid8", DeletedAt: nil}, Name: "kmsConfig8", ServiceAccountID: &serviceAccounts[1].ID, AccountID: 6666, State: models.LifeCycleStateAvailable},
		{BaseModel: datamodel.BaseModel{UUID: "uuid9", DeletedAt: nil}, Name: "kmsConfig9", ServiceAccountID: &serviceAccounts[1].ID, AccountID: 6666, State: "Ready", ResourceID: "kmsConfig9"},
	}
	jobs := []*datamodel.Job{
		{BaseModel: datamodel.BaseModel{UUID: "job-uuid1", DeletedAt: nil}, JobAttributes: &datamodel.JobAttributes{ResourceUUID: "uuid1"}},
		{BaseModel: datamodel.BaseModel{UUID: "job-uuid2", DeletedAt: nil}, JobAttributes: &datamodel.JobAttributes{ResourceUUID: "uuid2"}, Type: "create_kms_config"},
	}
	err = store.db.Create(accounts).Error()
	assert.NoError(t, err, "Failed to create Service account table")
	err = store.db.Create(kmsConfigs).Error()
	assert.NoError(t, err, "Failed to create KMS config table")
	err = store.db.Create(jobs).Error()
	assert.NoError(t, err, "Failed to create Job table")

	t.Run("GetKmsConfigRetrievesKMSConfigSuccessfully", func(tt *testing.T) {
		kmsConfigUUID := "uuid1"
		result, err := store.GetKmsConfigByUUID(context.Background(), kmsConfigUUID)

		assert.NoError(tt, err)
		assert.Equal(tt, "kmsConfig1", result.Name)
	})
	t.Run("GetKmsConfigReturnsErrorWhenRecordIsNotFound", func(tt *testing.T) {
		kmsConfigUUID := "nonexistent-uuid"
		result, err := store.GetKmsConfigByUUID(context.Background(), kmsConfigUUID)

		assert.ErrorContains(tt, err, "KMS Configuration not found")
		assert.Empty(tt, result)
	})
	t.Run("GetKmsConfigVariationRetrievesKMSConfigSuccessfully", func(tt *testing.T) {
		kmsConfig := new(datamodel.KmsConfig)
		kmsConfig.UUID = "uuid1"
		result, err := getKmsConfig(db, kmsConfig)

		assert.NoError(tt, err)
		assert.Equal(tt, "kmsConfig1", result.Name)
	})
	t.Run("GetKmsConfigVariationReturnsErrorWhenRecordIsNotFound", func(tt *testing.T) {
		kmsConfig := new(datamodel.KmsConfig)
		kmsConfig.UUID = "nonexistent-uuid"

		result, err := getKmsConfig(db, kmsConfig)

		assert.ErrorContains(tt, err, "KMS Configuration not found")
		assert.Nil(tt, result)
	})

	t.Run("ListKmsByAccountIDRetrievesKMSConfigsSuccessfully", func(tt *testing.T) {
		accountId := int64(1111)
		result, err := store.ListKmsConfigByAccountID(context.Background(), accountId)

		assert.NoError(tt, err)
		assert.Equal(tt, "kmsConfig1", result[0].Name)
		assert.Equal(tt, "kmsConfig4", result[1].Name)
	})
	t.Run("ListKmsByAccountIDReturnsEmptyWhenRecordsAreNotFound", func(tt *testing.T) {
		accountId := int64(9999)
		result, err := store.ListKmsConfigByAccountID(context.Background(), accountId)

		assert.NoError(tt, err)
		assert.Empty(tt, result)
	})

	t.Run("GetJobByKmsConfigIDRetrievesJobSuccessfully", func(tt *testing.T) {
		kmsConfigUUID := "uuid1"
		result, err := store.GetJobByResourceUUID(context.Background(), kmsConfigUUID, "")

		assert.NoError(tt, err)
		assert.Equal(tt, "job-uuid1", result.UUID)
	})
	t.Run("GetJobByKmsConfigIDWithJobTypeFilter", func(tt *testing.T) {
		kmsConfigUUID := "uuid2"
		result, err := store.GetJobByResourceUUID(context.Background(), kmsConfigUUID, "create_kms_config")

		assert.NoError(tt, err)
		assert.Equal(tt, "job-uuid2", result.UUID)
	})
	t.Run("GetJobByKmsConfigIDReturnsErrorWhenRecordIsNotFound", func(tt *testing.T) {
		kmsConfigUUID := "nonexistent-uuid"
		result, err := store.GetJobByResourceUUID(context.Background(), kmsConfigUUID, "")

		assert.ErrorContains(tt, err, "record not found")
		assert.Nil(tt, result)
	})

	t.Run("UpdateKmsConfigStateUpdatesKMSConfigSuccessfully", func(tt *testing.T) {
		kmsConfig := new(datamodel.KmsConfig)
		kmsConfig.UUID = "uuid1"
		kmsConfig.State = "In_Use"
		kmsConfig.StateDetails = "Key in use"

		_, err = store.UpdateKmsConfigState(context.Background(), kmsConfig.UUID, kmsConfig.State, kmsConfig.StateDetails)
		assert.NoError(tt, err)

		result, err := store.GetKmsConfigByUUID(context.Background(), "uuid1")
		assert.NoError(tt, err)
		assert.Equal(tt, "In_Use", result.State)
		assert.Equal(tt, "Key in use", result.StateDetails)
	})
	t.Run("UpdateKmsConfigStateReturnsErrorWhenRecordIsNotFound", func(tt *testing.T) {
		kmsConfig := new(datamodel.KmsConfig)
		kmsConfig.UUID = "nonexistent-uuid"

		_, err = store.UpdateKmsConfigState(context.Background(), kmsConfig.UUID, kmsConfig.State, kmsConfig.StateDetails)
		assert.ErrorContains(tt, err, "KMS Configuration not found")
	})
	t.Run("UpdateKmsConfigAttributesSuccessfully", func(tt *testing.T) {
		kmsConfigAttributes := datamodel.KmsAttributes{
			SdeKmsConfigUUID:       "uuid-sde",
			SdeServiceAccountEmail: "sa-sde@sde.com",
			Instructions:           "Instructions",
		}
		kmsConfigUpdated, err := store.UpdateKmsConfigAttributes(context.Background(), kmsConfigs[8].UUID, &kmsConfigAttributes)

		assert.NoError(tt, err)
		assert.NotNil(tt, kmsConfigUpdated)
		assert.Equal(tt, kmsConfigAttributes.SdeKmsConfigUUID, kmsConfigUpdated.KmsAttributes.SdeKmsConfigUUID)
		assert.Equal(tt, kmsConfigAttributes.SdeServiceAccountEmail, kmsConfigUpdated.KmsAttributes.SdeServiceAccountEmail)
		assert.Equal(tt, kmsConfigAttributes.Instructions, kmsConfigUpdated.KmsAttributes.Instructions)
	})
	t.Run("UpdateKmsConfigAttributesWhenKMSConfigRecordIsNotPresent", func(tt *testing.T) {
		kmsConfigAttributes := datamodel.KmsAttributes{
			SdeKmsConfigUUID:       "uuid-sde",
			SdeServiceAccountEmail: "sa-sde@sde.com",
			Instructions:           "Instructions",
		}
		kmsConfigUpdated, err := store.UpdateKmsConfigAttributes(context.Background(), "non-existent-uuid", &kmsConfigAttributes)

		assert.Error(tt, err)
		assert.Nil(tt, kmsConfigUpdated)
	})

	t.Run("UpdateKmsConfigDetailsInternalWhenFullPathIsInvalid", func(tt *testing.T) {
		keyFullPathInvalid := "projects/projectId/locations/australia-southeast1/keyRings/KeyRingName/cryptoKeysKeyName"
		resultUpdate, err := _updateKmsConfigDetails(db, kmsConfigs[8].UUID, keyFullPathInvalid, kmsConfigs[8].ResourceID)

		assert.Error(tt, err)
		assert.Nil(tt, resultUpdate)
	})
	t.Run("UpdateKmsConfigDetailsInternalWhenRecordIsNotFound", func(tt *testing.T) {
		uuidNonExistent := "uuid-non-existent"
		keyFullPath := "projects/projectId/locations/australia-southeast1/keyRings/KeyRingName/cryptoKeys/KeyName"
		resultUpdate, err := _updateKmsConfigDetails(db, uuidNonExistent, keyFullPath, kmsConfigs[8].ResourceID)

		assert.Error(tt, err)
		assert.Nil(tt, resultUpdate)
	})
	t.Run("WhenUpdateKmsConfigDetailsInternalIsSuccessful", func(tt *testing.T) {
		resourceID := "resourceIdUpdated"
		keyFullPathInvalid := "projects/projectId/locations/australia-southeast1/keyRings/KeyRingName/cryptoKeys/KeyName"
		resultUpdate, err := _updateKmsConfigDetails(db, kmsConfigs[8].UUID, keyFullPathInvalid, resourceID)

		assert.NoError(tt, err)
		assert.NotNil(tt, resultUpdate)
		assert.Equal(tt, resourceID, resultUpdate.ResourceID)
		assert.Equal(tt, "KeyName", resultUpdate.KeyName)
		assert.Equal(tt, "KeyRingName", resultUpdate.KeyRing)
		assert.Equal(tt, "australia-southeast1", resultUpdate.KeyRingLocation)
		assert.Equal(tt, "projectId", resultUpdate.KeyProjectID)
	})
	t.Run("WhenUpdateKmsConfigDetailsExternalIsSuccessful", func(tt *testing.T) {
		resourceID := "resourceIdUpdated"
		keyFullPathInvalid := "projects/projectId/locations/australia-southeast1/keyRings/KeyRingName/cryptoKeys/KeyName"
		resultUpdate, err := store.UpdateKmsConfigDetails(context.Background(), kmsConfigs[8].UUID, keyFullPathInvalid, resourceID)

		assert.NoError(tt, err)
		assert.NotNil(tt, resultUpdate)
		assert.Equal(tt, resourceID, resultUpdate.ResourceID)
		assert.Equal(tt, "KeyName", resultUpdate.KeyName)
		assert.Equal(tt, "KeyRingName", resultUpdate.KeyRing)
		assert.Equal(tt, "australia-southeast1", resultUpdate.KeyRingLocation)
		assert.Equal(tt, "projectId", resultUpdate.KeyProjectID)
	})
	t.Run("WhenUpdateKmsConfigDetailsExternalRunsIntoError", func(tt *testing.T) {
		keyFullPathInvalid := "projects/projectId/locations/australia-southeast1/keyRings/KeyRingName/cryptoKeysKeyName"
		resultUpdate, err := store.UpdateKmsConfigDetails(context.Background(), kmsConfigs[8].UUID, keyFullPathInvalid, kmsConfigs[8].ResourceID)

		assert.Error(tt, err)
		assert.Nil(tt, resultUpdate)
	})

	t.Run("CreatesKmsServiceAccountReturnsExistingServiceAccountWhenFound", func(tt *testing.T) {
		serviceAccount, err := store.CreateKmsServiceAccount(context.Background(), serviceAccounts[0])

		assert.NoError(t, err)
		assert.NotNil(t, serviceAccount)
		assert.Equal(t, serviceAccounts[0].Name, serviceAccount.Name)
		assert.Equal(t, serviceAccounts[0].UUID, serviceAccount.UUID)
		assert.Equal(t, serviceAccounts[0].State, serviceAccount.State)
		assert.Equal(t, serviceAccounts[0].AccountID, serviceAccount.AccountID)
	})
	t.Run("CreatesKmsServiceAccountCreatesWhenNoExistingAccountIsFound", func(t *testing.T) {
		serviceAccount, err := store.CreateKmsServiceAccount(context.Background(), serviceAccounts[1])

		assert.NoError(t, err)
		assert.NotNil(t, serviceAccount)
		assert.Equal(t, "ServiceAccount2", serviceAccount.Name)
	})

	t.Run("CreatesKmsConfigFailsWhenAnotherIsPresentInCreatingState", func(tt *testing.T) {
		kmsConfigInCreatingState := &datamodel.KmsConfig{
			AccountID: 4444,
		}
		result, err := store.CreateKmsConfig(context.Background(), kmsConfigInCreatingState)
		assert.Error(tt, err)
		assert.Equal(tt, "another config create operation is in progress for this region and project", err.Error())
		assert.Nil(tt, result)
	})
	t.Run("CreatesKmsConfigFailsWhenAnotherIsPresentInDeletingState", func(tt *testing.T) {
		kmsConfigInDeletingState := &datamodel.KmsConfig{
			AccountID: 5555,
		}
		result, err := store.CreateKmsConfig(context.Background(), kmsConfigInDeletingState)
		assert.Error(tt, err)
		assert.Equal(tt, "another config delete operation is in progress for this region and project", err.Error())
		assert.Nil(tt, result)
	})
	t.Run("CreatesKmsConfigWhenFailsWhenState", func(tt *testing.T) {
		kmsConfigTest := &datamodel.KmsConfig{
			AccountID: 6666,
		}
		result, err := store.CreateKmsConfig(context.Background(), kmsConfigTest)
		assert.NoError(tt, err)
		assert.Equal(tt, kmsConfigs[7].UUID, result.UUID)
		assert.Equal(tt, kmsConfigs[7].Name, result.Name)
	})
	t.Run("CreatesKmsConfigSuccessfully", func(tt *testing.T) {
		ctx := context.Background()
		kmsConfigCreate := &datamodel.KmsConfig{
			BaseModel: datamodel.BaseModel{
				UUID: utils.RandomUUID(),
			},
			AccountID:       1111,
			Name:            "kmsConfigCreate",
			State:           "Ready",
			StateDetails:    "Read for use",
			Description:     "KMS config for creation",
			KeyRingLocation: "global",
			KeyRing:         "key-ring",
			KeyName:         "key-name",
		}

		resultCreate, err := store.CreateKmsConfig(ctx, kmsConfigCreate)
		assert.NoError(tt, err)
		assert.NotNil(tt, resultCreate)
		assert.Equal(tt, kmsConfigCreate.Name, resultCreate.Name)

		resultGet, err := store.GetKmsConfigByUUID(ctx, resultCreate.UUID)
		assert.NoError(tt, err)

		assert.Equal(tt, kmsConfigCreate.Name, resultGet.Name)
		assert.Equal(tt, kmsConfigCreate.State, resultGet.State)
		assert.Equal(tt, kmsConfigCreate.StateDetails, resultGet.StateDetails)
		assert.Equal(tt, kmsConfigCreate.Description, resultGet.Description)
		assert.Equal(tt, kmsConfigCreate.KeyRingLocation, resultGet.KeyRingLocation)
		assert.Equal(tt, kmsConfigCreate.KeyRing, resultGet.KeyRing)
		assert.Equal(tt, kmsConfigCreate.KeyName, resultGet.KeyName)
		assert.Equal(tt, accounts[0].Name, resultGet.Account.Name)
	})
}

func TestDeleteKmsConfig(t *testing.T) {
	t.Run("WhenKmsConfigIsDeletedSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test_account",
		}
		err = store.db.Create(account).Error()
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			Name:    "test_pool",
			Account: account,
		}

		err = store.db.Create(pool).Error()
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		kmsConfig := &datamodel.KmsConfig{
			Name:      "test_kms_config",
			AccountID: account.ID,
			BaseModel: datamodel.BaseModel{UUID: "test-uuid"},
		}

		err = store.db.Create(kmsConfig).Error()
		if err != nil {
			tt.Fatalf("Failed to create kms config: %v", err)
		}

		deletedKmsConfig, err := store.DeleteKmsConfig(context.Background(), kmsConfig.UUID, models.LifeCycleStateDeleted, "")
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.NotNil(tt, deletedKmsConfig.DeletedAt, "Expected kms config to be deleted, got %v", deletedKmsConfig.DeletedAt)
		assert.Equal(tt, models.LifeCycleStateDeleted, deletedKmsConfig.State, "Expected kms config state %v, got %v", models.LifeCycleStateDeleted, deletedKmsConfig.State)
		assert.Equal(tt, "", deletedKmsConfig.StateDetails, "Expected kms config details %v, got %v", "", deletedKmsConfig.StateDetails)

		_, err = store.GetKmsConfigByUUID(context.Background(), kmsConfig.UUID)
		assert.EqualError(tt, err, "KMS Configuration not found", "Expected no error, got %v", err)
	})
	t.Run("WhenKmsConfigIsNotFound", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		deletedKmsConfig, err := store.DeleteKmsConfig(context.Background(), "dummy", models.LifeCycleStateDeleted, "")
		assert.Nil(tt, deletedKmsConfig, "Expected nil volume replication, got %v", deletedKmsConfig)
		assert.EqualError(tt, err, "KMS Configuration not found", "Expected no error, got %v", err)
	})
}

func TestGetKmsConfigByKeyFullPath(t *testing.T) {
	t.Run("GetKmsConfigByKeyFullPathReturnsKmsConfigSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(t, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(t, err)

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"}, Name: "account"}
		serviceAccount := &datamodel.ServiceAccount{BaseModel: datamodel.BaseModel{ID: 2, UUID: "sa-uuid"}, Name: "sa", AccountID: 1}
		kmsConfig := &datamodel.KmsConfig{
			BaseModel:         datamodel.BaseModel{UUID: "kms-uuid"},
			Name:              "kms",
			AccountID:         1,
			ServiceAccountID:  &serviceAccount.ID,
			KeyRingLocation:   "us-central1",
			KeyRing:           "ring1",
			KeyName:           "key1",
			CustomerProjectID: "projectNumber",
			KeyProjectID:      "project1",
		}
		err = store.db.Create(account).Error()
		assert.NoError(t, err)
		err = store.db.Create(serviceAccount).Error()
		assert.NoError(t, err)
		err = store.db.Create(kmsConfig).Error()
		assert.NoError(t, err)

		keyFullPath := "projects/project1/locations/us-central1/keyRings/ring1/cryptoKeys/key1"
		result, err := store.GetKmsConfigByKeyFullPath(context.Background(), keyFullPath, account.ID)
		assert.NoError(t, err)
		assert.Equal(t, kmsConfig.UUID, result.UUID)
		assert.Equal(t, kmsConfig.Name, result.Name)
		assert.Equal(t, serviceAccount.Name, result.ServiceAccount.Name)
		assert.Equal(t, account.Name, result.Account.Name)
	})
	t.Run("TestGetKmsConfigByKeyFullPathReturnsErrorWhenKeyFullPathIsInvalid", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(t, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(t, err)

		invalidKeyFullPath := "invalid/key/full/path"
		result, err := store.GetKmsConfigByKeyFullPath(context.Background(), invalidKeyFullPath, 1)
		assert.Error(t, err)
		assert.Nil(t, result)
	})
	t.Run("TestGetKmsConfigByKeyFullPathReturnsNotFoundWhenNoRecordExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(t, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(t, err)

		keyFullPath := "projects/project1/locations/us-central1/keyRings/ring1/cryptoKeys/key1"
		result, err := store.GetKmsConfigByKeyFullPath(context.Background(), keyFullPath, 1)
		assert.Error(t, err)
		assert.Nil(t, result)
	})
}

func TestIsKmsConfigInUse(t *testing.T) {
	t.Run("ReturnsTrueWhenKmsConfigIsInUseByAtLeastOneVM", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		kmsConfig := &datamodel.KmsConfig{
			BaseModel: datamodel.BaseModel{UUID: "test-kms-uuid"},
			Name:      "test-kms",
		}
		err = store.db.Create(kmsConfig).Error()
		assert.NoError(tt, err)

		originalIsKmsConfigInUse := isKmsConfigInUse
		defer func() { isKmsConfigInUse = originalIsKmsConfigInUse }()

		isKmsConfigInUse = func(db *gorm.DB, kmsConfig *datamodel.KmsConfig) (bool, error) {
			return true, nil
		}

		inUse, err := store.IsKmsConfigInUse(context.Background(), "test-kms-uuid")
		assert.NoError(tt, err)
		assert.True(tt, inUse)
	})

	t.Run("ReturnsFalseWhenKmsConfigIsNotInUseByAnyVM", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		kmsConfig := &datamodel.KmsConfig{
			BaseModel: datamodel.BaseModel{UUID: "test-kms-uuid"},
			Name:      "test-kms",
		}
		err = store.db.Create(kmsConfig).Error()
		assert.NoError(tt, err)

		originalIsKmsConfigInUse := isKmsConfigInUse
		defer func() { isKmsConfigInUse = originalIsKmsConfigInUse }()

		isKmsConfigInUse = func(db *gorm.DB, kmsConfig *datamodel.KmsConfig) (bool, error) {
			return false, nil
		}

		inUse, err := store.IsKmsConfigInUse(context.Background(), "test-kms-uuid")
		assert.NoError(tt, err)
		assert.False(tt, inUse)
	})

	t.Run("ReturnsErrorWhenKmsConfigDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		inUse, err := store.IsKmsConfigInUse(context.Background(), "non-existent-uuid")
		assert.Error(tt, err)
		assert.False(tt, inUse)
		assert.True(tt, customerrors.IsNotFoundErr(err))
	})

	t.Run("ReturnsErrorWhenDatabaseOperationFails", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		kmsConfig := &datamodel.KmsConfig{
			BaseModel: datamodel.BaseModel{UUID: "test-kms-uuid"},
			Name:      "test-kms",
		}
		err = store.db.Create(kmsConfig).Error()
		assert.NoError(tt, err)

		originalIsKmsConfigInUse := isKmsConfigInUse
		defer func() { isKmsConfigInUse = originalIsKmsConfigInUse }()

		isKmsConfigInUse = func(db *gorm.DB, kmsConfig *datamodel.KmsConfig) (bool, error) {
			return false, fmt.Errorf("database error")
		}

		inUse, err := store.IsKmsConfigInUse(context.Background(), "test-kms-uuid")
		assert.Error(tt, err)
		assert.False(tt, inUse)
		assert.Contains(tt, err.Error(), "database error")
	})
}

func Test_isKmsConfigInUse(t *testing.T) {
	t.Run("ReturnsTrueWhenSVMsAreUsingKmsConfig", func(t *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(t, err)

		kmsConfig := &datamodel.KmsConfig{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "kms-uuid"},
			Name:      "test-kms",
		}

		originalGetSvmsByKmsConfigID := getSvmsByKmsConfigID
		defer func() { getSvmsByKmsConfigID = originalGetSvmsByKmsConfigID }()

		getSvmsByKmsConfigID = func(db *gorm.DB, kmsConfigID int64) ([]*datamodel.Svm, error) {
			return []*datamodel.Svm{
				{BaseModel: datamodel.BaseModel{UUID: "svm-uuid"}, Name: "test-svm"},
			}, nil
		}

		inUse, err := _isKmsConfigInUse(db, kmsConfig)
		assert.NoError(t, err)
		assert.True(t, inUse)
	})

	t.Run("ReturnsFalseWhenNoSVMsAreUsingKmsConfig", func(t *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(t, err)

		kmsConfig := &datamodel.KmsConfig{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "kms-uuid"},
			Name:      "test-kms",
		}

		originalGetSvmsByKmsConfigID := getSvmsByKmsConfigID
		defer func() { getSvmsByKmsConfigID = originalGetSvmsByKmsConfigID }()

		getSvmsByKmsConfigID = func(db *gorm.DB, kmsConfigID int64) ([]*datamodel.Svm, error) {
			return []*datamodel.Svm{}, nil
		}

		inUse, err := _isKmsConfigInUse(db, kmsConfig)
		assert.NoError(t, err)
		assert.False(t, inUse)
	})

	t.Run("ReturnsFalseWhenNotFoundErrorOccurs", func(t *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(t, err)

		kmsConfig := &datamodel.KmsConfig{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "kms-uuid"},
			Name:      "test-kms",
		}

		originalGetSvmsByKmsConfigID := getSvmsByKmsConfigID
		defer func() { getSvmsByKmsConfigID = originalGetSvmsByKmsConfigID }()

		getSvmsByKmsConfigID = func(db *gorm.DB, kmsConfigID int64) ([]*datamodel.Svm, error) {
			return nil, errors.New("some error")
		}

		inUse, err := _isKmsConfigInUse(db, kmsConfig)
		assert.Error(t, err)
		assert.False(t, inUse)
	})

	t.Run("ReturnsErrorWhenDatabaseErrorOccurs", func(t *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(t, err)

		kmsConfig := &datamodel.KmsConfig{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "kms-uuid"},
			Name:      "test-kms",
		}

		originalGetSvmsByKmsConfigID := getSvmsByKmsConfigID
		defer func() { getSvmsByKmsConfigID = originalGetSvmsByKmsConfigID }()

		getSvmsByKmsConfigID = func(db *gorm.DB, kmsConfigID int64) ([]*datamodel.Svm, error) {
			return nil, fmt.Errorf("database connection error")
		}

		inUse, err := _isKmsConfigInUse(db, kmsConfig)
		assert.Error(t, err)
		assert.False(t, inUse)
		assert.Contains(t, err.Error(), "database connection error")
	})
}
