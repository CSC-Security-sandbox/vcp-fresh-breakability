package repository

import (
	"context"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"gorm.io/gorm"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/gorm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
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
		{BaseModel: datamodel.BaseModel{UUID: "uuid1", DeletedAt: nil}, Name: "kmsConfig1", ServiceAccountID: serviceAccounts[0].ID},
		{BaseModel: datamodel.BaseModel{UUID: "uuid2", DeletedAt: nil}, Name: "kmsConfig2", ServiceAccountID: serviceAccounts[1].ID},
	}

	err = store.db.Create(serviceAccounts).Error()
	assert.NoError(t, err, "Failed to create Service account table")
	err = store.db.Create(kmsConfigs).Error()
	assert.NoError(t, err, "Failed to create KMS Config table")

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

	t.Run("RetrievesKMSConfigsSuccessfully", func(tt *testing.T) {
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
		{BaseModel: datamodel.BaseModel{UUID: "uuid1", DeletedAt: nil}, Name: "kmsConfig1", ServiceAccountID: serviceAccounts[0].ID, AccountID: 1111, State: "Ready", StateDetails: "Key is in Ready state"},
		{BaseModel: datamodel.BaseModel{UUID: "uuid2", DeletedAt: nil}, Name: "kmsConfig2", ServiceAccountID: serviceAccounts[1].ID, AccountID: 2222, State: models.LifeCycleStateAvailable},
		{BaseModel: datamodel.BaseModel{UUID: "uuid3", DeletedAt: nil}, Name: "kmsConfig3", ServiceAccountID: serviceAccounts[0].ID, AccountID: 2222},
		{BaseModel: datamodel.BaseModel{UUID: "uuid4", DeletedAt: nil}, Name: "kmsConfig4", ServiceAccountID: serviceAccounts[1].ID, AccountID: 1111},
		{BaseModel: datamodel.BaseModel{UUID: "uuid5", DeletedAt: nil}, Name: "kmsConfig5", AccountID: 3333, State: "Ready", StateDetails: "Key is in Ready state", Description: "kms description"},
		{BaseModel: datamodel.BaseModel{UUID: "uuid6", DeletedAt: nil}, Name: "kmsConfig6", AccountID: 4444, State: models.LifeCycleStateCreating},
		{BaseModel: datamodel.BaseModel{UUID: "uuid7", DeletedAt: nil}, Name: "kmsConfig7", AccountID: 5555, State: models.LifeCycleStateDeleting},
		{BaseModel: datamodel.BaseModel{UUID: "uuid8", DeletedAt: nil}, Name: "kmsConfig8", ServiceAccountID: serviceAccounts[1].ID, AccountID: 6666, State: models.LifeCycleStateAvailable},
		{BaseModel: datamodel.BaseModel{UUID: "uuid9", DeletedAt: nil}, Name: "kmsConfig9", ServiceAccountID: serviceAccounts[1].ID, AccountID: 6666, State: "Ready", ResourceID: "kmsConfig9"},
	}
	jobs := []*datamodel.Job{
		{BaseModel: datamodel.BaseModel{UUID: "job-uuid1", DeletedAt: nil}, JobAttributes: &datamodel.JobAttributes{ResourceUUID: "uuid1"}},
	}
	err = store.db.Create(serviceAccounts).Error()
	assert.NoError(t, err, "Failed to create Service account table")
	err = store.db.Create(accounts).Error()
	assert.NoError(t, err, "Failed to create Service account table")
	err = store.db.Create(kmsConfigs).Error()
	assert.NoError(t, err, "Failed to create KMS Config table")
	err = store.db.Create(jobs).Error()
	assert.NoError(t, err, "Failed to create Job table")

	t.Run("GetKmsConfigRetrievesKMSConfigSuccessfully", func(tt *testing.T) {
		kmsConfigUUID := "uuid1"
		result, err := store.GetKmsConfigByUUID(context.Background(), kmsConfigUUID)

		assert.NoError(tt, err)
		assert.Equal(tt, "kmsConfig1", result.Name)
		assert.Equal(tt, "ServiceAccount1", result.ServiceAccount.Name)
	})
	t.Run("GetKmsConfigReturnsErrorWhenRecordIsNotFound", func(tt *testing.T) {
		kmsConfigUUID := "nonexistent-uuid"
		result, err := store.GetKmsConfigByUUID(context.Background(), kmsConfigUUID)

		assert.ErrorContains(tt, err, "record not found")
		assert.Empty(tt, result)
	})
	t.Run("GetKmsConfigVariationRetrievesKMSConfigSuccessfully", func(tt *testing.T) {
		kmsConfig := new(datamodel.KmsConfig)
		kmsConfig.UUID = "uuid1"
		result, err := getKmsConfig(db, kmsConfig)

		assert.NoError(tt, err)
		assert.Equal(tt, "kmsConfig1", result.Name)
		assert.Equal(tt, "ServiceAccount1", result.ServiceAccount.Name)
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
		assert.Equal(tt, "ServiceAccount1", result[0].ServiceAccount.Name)
		assert.Equal(tt, "ServiceAccount2", result[1].ServiceAccount.Name)
	})
	t.Run("ListKmsByAccountIDReturnsEmptyWhenRecordsAreNotFound", func(tt *testing.T) {
		accountId := int64(9999)
		result, err := store.ListKmsConfigByAccountID(context.Background(), accountId)

		assert.NoError(tt, err)
		assert.Empty(tt, result)
	})

	t.Run("GetJobByKmsConfigIDRetrievesJobSuccessfully", func(tt *testing.T) {
		kmsConfigUUID := "uuid1"
		result, err := store.GetJobByResourceUUID(context.Background(), kmsConfigUUID)

		assert.NoError(tt, err)
		assert.Equal(tt, "job-uuid1", result.UUID)
	})
	t.Run("GetJobByKmsConfigIDReturnsErrorWhenRecordIsNotFound", func(tt *testing.T) {
		kmsConfigUUID := "nonexistent-uuid"
		result, err := store.GetJobByResourceUUID(context.Background(), kmsConfigUUID)

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
		assert.Equal(tt, "projectId", resultUpdate.CustomerProjectID)
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
		assert.Equal(tt, "projectId", resultUpdate.CustomerProjectID)
	})
	t.Run("WhenUpdateKmsConfigDetailsExternalRunsIntoError", func(tt *testing.T) {
		keyFullPathInvalid := "projects/projectId/locations/australia-southeast1/keyRings/KeyRingName/cryptoKeysKeyName"
		resultUpdate, err := store.UpdateKmsConfigDetails(context.Background(), kmsConfigs[8].UUID, keyFullPathInvalid, kmsConfigs[8].ResourceID)

		assert.Error(tt, err)
		assert.Nil(tt, resultUpdate)
	})

	t.Run("CreatesKmsServiceAccountReturnsExistingServiceAccountWhenFound", func(tt *testing.T) {
		serviceAccount, err := _createKmsServiceAccount(db, kmsConfigs[0])

		assert.NoError(t, err)
		assert.NotNil(t, serviceAccount)
		assert.Equal(t, serviceAccounts[0].Name, serviceAccount.Name)
		assert.Equal(t, serviceAccounts[0].UUID, serviceAccount.UUID)
		assert.Equal(t, serviceAccounts[0].State, serviceAccount.State)
		assert.Equal(t, serviceAccounts[0].AccountID, serviceAccount.AccountID)
	})
	t.Run("CreatesKmsServiceAccountCreatesWhenNoExistingAccountIsFound", func(t *testing.T) {
		serviceAccount, err := _createKmsServiceAccount(db, kmsConfigs[4])

		assert.NoError(t, err)
		assert.NotNil(t, serviceAccount)
		assert.Equal(t, kmsConfigs[4].Name, serviceAccount.Name)
		assert.Equal(t, kmsConfigs[4].State, serviceAccount.State)
		assert.Equal(t, kmsConfigs[4].StateDetails, serviceAccount.StateDetails)
		assert.Equal(t, kmsConfigs[4].Description, serviceAccount.Description)
		assert.Equal(t, kmsConfigs[4].AccountID, serviceAccount.AccountID)
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
			Description:     "KMS Config for creation",
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
		assert.Equal(tt, serviceAccounts[0].Name, resultGet.ServiceAccount.Name)
	})
}
