package database

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils/gorm"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"gorm.io/gorm"
)

func TestGetBackupPolicyByNameAndOwnerID(t *testing.T) {
	t.Run("WhenGetBackupPolicyByNameAndOwnerIDReturnsBackupPolicy", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 10, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err, "Expected no error when creating account")

		backupPolicy := &datamodel.BackupPolicy{
			BaseModel: datamodel.BaseModel{UUID: "test-backup-policy-uuid"},
			Name:      "backup-policy-name",
			AccountID: account.ID,
			Account:   account,
		}
		err = store.db.Create(backupPolicy).Error()
		assert.NoError(tt, err, "Expected no error when creating backup policy")

		result, err := store.GetBackupPolicyByNameAndOwnerID(context.Background(), backupPolicy.Name, account.ID)
		assert.NoError(tt, err, "Expected no error")
		assert.Equal(tt, backupPolicy.UUID, result.UUID, "Expected backup policy UUID to match")
		assert.Equal(tt, account.Name, result.Account.Name, "Expected account name to match")
	})
	t.Run("WhenGetBackupPolicyByNameAndOwnerIDReturnsErrorWhenDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")
		backupPolicyName := "backupPolicy"
		_, err = store.GetBackupPolicyByNameAndOwnerID(context.Background(), backupPolicyName, 9999)
		assert.Error(tt, err)
		assert.Equal(tt, customerrors.NewNotFoundErr("backup policy", &backupPolicyName).Error(), err.Error(), "Expected error to be a not found error")
	})
}

func TestGetBackupPolicyByUUIDAndOwnerID(t *testing.T) {
	t.Run("WhenGetBackupPolicyByUUIDAndOwnerIDReturnsBackupPolicy", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 20, UUID: "test-account-uuid-2"},
			Name:      "test_account_2",
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err, "Expected no error when creating account")

		backupPolicy := &datamodel.BackupPolicy{
			BaseModel: datamodel.BaseModel{UUID: "test-backup-policy-uuid-2"},
			Name:      "backup-policy-name-2",
			AccountID: account.ID,
			Account:   account,
		}
		err = store.db.Create(backupPolicy).Error()
		assert.NoError(tt, err, "Expected no error when creating backup policy")

		result, err := store.GetBackupPolicyByUUIDAndOwnerID(context.Background(), backupPolicy.UUID, account.ID)
		assert.NoError(tt, err, "Expected no error")
		assert.Equal(tt, backupPolicy.UUID, result.UUID, "Expected backup policy UUID to match")
		assert.Equal(tt, account.Name, result.Account.Name, "Expected account name to match")
	})

	t.Run("WhenGetBackupPolicyByUUIDAndOwnerIDReturnsBackupPolicyNotFound", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")
		backupPolicyUUID := "non-existent-uuid"
		_, err = store.GetBackupPolicyByUUIDAndOwnerID(context.Background(), backupPolicyUUID, 9999)
		assert.Error(tt, err)
		assert.Equal(tt, customerrors.NewNotFoundErr("backup policy", &backupPolicyUUID).Error(), err.Error(), "Expected error to be a not found error")
	})
}

func TestGetVolumeCountByBackupPolicyID(t *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(t, err, "Failed to set up test database")
	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	assert.NoError(t, err, "Failed to clean up test database")

	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: 100, UUID: "test-account-uuid-100"},
		Name:      "test_account_100",
	}
	err = store.db.Create(account).Error()
	assert.NoError(t, err, "Expected no error when creating account")

	backupPolicyUUID := "test-backup-policy-uuid-100"
	dataProtection := datamodel.DataProtection{
		BackupPolicyID: backupPolicyUUID,
	}
	volume := datamodel.Volume{
		BaseModel:      datamodel.BaseModel{ID: 1, UUID: "test-volume-uuid-100"},
		Name:           "test_volume_100",
		AccountID:      account.ID,
		Account:        account,
		DataProtection: &dataProtection,
	}
	err = store.db.Create(&volume).Error()
	assert.NoError(t, err, "Expected no error when creating volume")

	t.Run("WhenGetVolumeCountByBackupPolicyIDReturnsVolumeCount", func(tt *testing.T) {
		count, err := store.GetVolumeCountByBackupPolicyID(context.Background(), backupPolicyUUID)
		assert.NoError(tt, err, "Expected no error when getting volume count")
		assert.Equal(tt, int64(1), count, "Expected volume count to be 1")
	})
	t.Run("WhenGetVolumeCountByBackupPolicyIDReturnsZeroForInvalidBackupPolicyID", func(tt *testing.T) {
		count, err := store.GetVolumeCountByBackupPolicyID(context.Background(), "non-existent-uuid")
		assert.NoError(tt, err, "Expected no error when getting volume count for non-existent backup policy")
		assert.Equal(tt, int64(0), count, "Expected volume count to be 0")
	})
}

func TestListBackupPolicyVolumeCount(t *testing.T) {
	t.Run("WhenListBackupPolicyVolumeCountReturnsValidBackupPolicies", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		account := datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 60, UUID: "test-account-uuid-6"},
			Name:      "test_account_6",
		}

		// Create another accounts with different UUID
		account2 := account
		account2.UUID = "test-account-uuid-7"
		account2.ID = 61

		err = store.db.Create(&account).Error()
		assert.NoError(tt, err, "Expected no error when creating account")
		err = store.db.Create(&account2).Error()
		assert.NoError(tt, err, "Expected no error when creating account2")

		dataProtection := datamodel.DataProtection{
			BackupPolicyID: "test-backup-policy-uuid-1",
		}
		volume := datamodel.Volume{
			BaseModel:      datamodel.BaseModel{ID: 1, UUID: "test-volume-uuid"},
			Name:           "test_volume",
			AccountID:      account.ID,
			Account:        &account,
			DataProtection: &dataProtection,
		}
		err = store.db.Create(&volume).Error()
		assert.NoError(tt, err, "Expected no error when creating volume")

		// Volume with same AccountID and no BackupPolicyID
		volume2 := volume
		volume2.ID = 2
		volume2.BaseModel = datamodel.BaseModel{UUID: "test-volume-uuid-2"}
		volume2.DataProtection = nil
		err = store.db.Create(&volume2).Error()
		assert.NoError(tt, err, "Expected no error when creating volume 2")

		// Volume with different AccountID and BackupPolicyID
		volume3 := volume
		volume3.ID = 3
		volume3.BaseModel = datamodel.BaseModel{UUID: "test-volume-uuid-3"}
		volume3.AccountID = account2.ID
		volume3.Account = &account2
		dataProtection.BackupPolicyID = "test-backup-policy-uuid-2"
		volume3.DataProtection = &dataProtection
		err = store.db.Create(&volume3).Error()
		assert.NoError(tt, err, "Expected no error when creating volume 3")

		conditions := [][]interface{}{{"account_id = ?", account.ID}}
		result, err := store.ListBackupPolicyVolumeCount(context.Background(), conditions)
		assert.NoError(tt, err)
		assert.Len(tt, result, 1)
		assert.Equal(tt, int64(1), result["test-backup-policy-uuid-1"], "Expected backup policy volume count to match")
	})

	t.Run("WhenListBackupPolicyVolumeCountWithUUIDsReturnsValidBackupPolicies", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		account := datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 60, UUID: "test-account-uuid-6"},
			Name:      "test_account_6",
		}

		// Create another accounts with different UUID
		account2 := account
		account2.UUID = "test-account-uuid-7"
		account2.ID = 61

		err = store.db.Create(&account).Error()
		assert.NoError(tt, err, "Expected no error when creating account")
		err = store.db.Create(&account2).Error()
		assert.NoError(tt, err, "Expected no error when creating account2")

		backupPolicyUUIDs := []string{"test-backup-policy-uuid-1", "test-backup-policy-uuid-2"}

		dataProtection := datamodel.DataProtection{
			BackupPolicyID: backupPolicyUUIDs[0],
		}
		volume := datamodel.Volume{
			BaseModel:      datamodel.BaseModel{ID: 1, UUID: "test-volume-uuid"},
			Name:           "test_volume",
			AccountID:      account.ID,
			Account:        &account,
			DataProtection: &dataProtection,
		}
		err = store.db.Create(&volume).Error()
		assert.NoError(tt, err, "Expected no error when creating volume")

		// Volume with same AccountID and different BackupPolicyID
		volume2 := volume
		volume2.ID = 3
		volume2.BaseModel = datamodel.BaseModel{UUID: "test-volume-uuid-2"}
		dataProtection.BackupPolicyID = backupPolicyUUIDs[1]
		volume2.DataProtection = &dataProtection
		err = store.db.Create(&volume2).Error()
		assert.NoError(tt, err, "Expected no error when creating volume 2")

		conditions := [][]interface{}{{"account_id = ?", account.ID}, {"data_protection->>'backup_policy_id' IN ?", backupPolicyUUIDs}}
		result, err := store.ListBackupPolicyVolumeCount(context.Background(), conditions)
		assert.NoError(tt, err)
		assert.Len(tt, result, 2)
		assert.Equal(tt, int64(1), result["test-backup-policy-uuid-1"], "Expected backup policy volume count to match")
		assert.Equal(tt, int64(1), result["test-backup-policy-uuid-2"], "Expected backup policy volume count to match")
	})

	t.Run("ReturnsEmptyBackupPoliciesWhenNoBackupPoliciesExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		account := datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 60, UUID: "test-account-uuid-6"},
			Name:      "test_account_6",
		}
		err = store.db.Create(&account).Error()
		assert.NoError(tt, err, "Expected no error when creating account")

		conditions := [][]interface{}{{"account_id = ?", account.ID}}
		result, err := store.ListBackupPolicyVolumeCount(context.Background(), conditions)
		assert.NoError(tt, err)
		assert.Empty(tt, result)
	})
}

func TestListBackupPolicies(t *testing.T) {
	t.Run("WhenListBackupPoliciesReturnsBackupPolicies", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 30, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err, "Expected no error when creating account")

		backupPolicy := &datamodel.BackupPolicy{
			BaseModel: datamodel.BaseModel{UUID: "test-backup-policy-uuid-3"},
			Name:      "backup-policy-name-3",
			AccountID: account.ID,
			Account:   account,
		}
		err = store.db.Create(backupPolicy).Error()
		assert.NoError(tt, err, "Expected no error when creating backup policy")

		result, err := store.ListBackupPolicies(context.Background(), [][]interface{}{{"account_id = ?", account.ID}})
		assert.NoError(tt, err)
		assert.Len(tt, result, 1)
		assert.Equal(tt, backupPolicy.UUID, result[0].UUID)
	})
	t.Run("WhenListBackupPoliciesWithUUIDsReturnsBackupPolicies", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 70, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err, "Expected no error when creating account")

		backupPolicyUUIDs := []string{"test-backup-policy-uuid-4", "test-backup-policy-uuid-5"}
		for _, uuid := range backupPolicyUUIDs {
			backupPolicy := &datamodel.BackupPolicy{
				BaseModel: datamodel.BaseModel{UUID: uuid},
				Name:      "backup-policy-name-" + uuid,
				AccountID: account.ID,
				Account:   account,
			}
			err = store.db.Create(backupPolicy).Error()
			assert.NoError(tt, err, "Expected no error when creating backup policy")
		}

		result, err := store.ListBackupPolicies(context.Background(), [][]interface{}{{"account_id = ?", account.ID}, {"uuid IN ?", backupPolicyUUIDs}})
		assert.NoError(tt, err)
		assert.Len(tt, result, 2)
		for _, backupPolicy := range result {
			assert.Contains(tt, backupPolicyUUIDs, backupPolicy.UUID)
		}
	})
	t.Run("WhenListBackupPoliciesReturnsEmptySlice", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 70, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err, "Expected no error when creating account")

		result, err := store.ListBackupPolicies(context.Background(), [][]interface{}{{"account_id = ?", 9999}})
		assert.NoError(tt, err)
		assert.Empty(tt, result)
	})
	t.Run("WhenListBackupPoliciesReturnsError", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		_, err = store.ListBackupPolicies(context.Background(), [][]interface{}{{"invalid_column = ?", "value"}})
		assert.Error(tt, err, "Expected error when listing backup policies with invalid conditions")
	})
}

func TestCreateBackupPolicyEntryInVCP(t *testing.T) {
	t.Run("WhenCreateBackupPolicyEntryInVCPSucceeds", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 40, UUID: "test-account-uuid-4"},
			Name:      "test_account_4",
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err, "Expected no error when creating account")

		backupPolicy := &datamodel.BackupPolicy{
			BaseModel: datamodel.BaseModel{UUID: "test-backup-policy-uuid-4"},
			Name:      "backup-policy-name-4",
			AccountID: account.ID,
			Account:   account,
		}

		result, err := store.CreateBackupPolicyEntryInVCP(context.Background(), backupPolicy)
		assert.NoError(tt, err, "Expected no error when creating backup policy entry in VCP")
		assert.NotNil(tt, result, "Expected result to not be nil")
		assert.Equal(tt, backupPolicy.UUID, result.UUID, "Expected backup policy UUID to match")
		assert.Equal(tt, account.Name, result.Account.Name, "Expected account name to match")
		assert.Equal(tt, result.CreatedAt, result.UpdatedAt, "Expected CreatedAt and UpdatedAt to be equal on creation")
	})

	t.Run("ReturnsExistingBackupPolicyIfAlreadyExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 101, UUID: "test-account-uuid-101"},
			Name:      "test_account_101",
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err, "Expected no error when creating account")

		backupPolicy := &datamodel.BackupPolicy{
			BaseModel: datamodel.BaseModel{UUID: "test-backup-policy-uuid-101"},
			Name:      "backup-policy-name-101",
			AccountID: account.ID,
			Account:   account,
		}
		err = store.db.Create(backupPolicy).Error()
		assert.NoError(tt, err)

		defer func() {
			getBackupPolicyWithDetails = _getBackupPolicyWithDetails
		}()
		getBackupPolicyWithDetails = func(db *gorm.DB, query *datamodel.BackupPolicy) (*datamodel.BackupPolicy, error) {
			return backupPolicy, nil
		}

		result, err := store.CreateBackupPolicyEntryInVCP(context.Background(), backupPolicy)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, backupPolicy.UUID, result.UUID)
		assert.Equal(tt, account.Name, result.Account.Name)
	})

	t.Run("WhenCreateBackupPolicyEntryInVCPFailsWhenAccountIDDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		db.Exec("PRAGMA foreign_keys = ON;")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 40, UUID: "test-account-uuid-4"},
			Name:      "test_account_4",
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err, "Expected no error when creating account")

		backupPolicy := &datamodel.BackupPolicy{
			BaseModel: datamodel.BaseModel{UUID: "test-backup-policy-uuid-5"},
			Name:      "backup-policy-name-5",
		}

		result, err := store.CreateBackupPolicyEntryInVCP(context.Background(), backupPolicy)
		assert.Error(tt, err, "Expected error when creating backup policy entry in VCP with missing AccountID")
		assert.Nil(tt, result, "Expected result to be nil")
	})
}

func TestDeleteBackupPolicy(t *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(t, err, "Failed to set up test database")
	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	assert.NoError(t, err, "Failed to clean up test database")

	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: 80, UUID: "test-account-uuid-8"},
		Name:      "test_account_8",
	}
	err = store.db.Create(account).Error()
	assert.NoError(t, err, "Expected no error when creating account")

	backupPolicy := &datamodel.BackupPolicy{
		BaseModel: datamodel.BaseModel{UUID: "test-backup-policy-uuid-8"},
		Name:      "backup-policy-name-8",
		AccountID: account.ID,
		Account:   account,
	}
	err = store.db.Create(backupPolicy).Error()
	assert.NoError(t, err, "Expected no error when creating backup policy")

	t.Run("WhenDeleteBackupPolicySucceeds", func(tt *testing.T) {
		result, err := store.DeleteBackupPolicy(context.Background(), backupPolicy.UUID)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, backupPolicy.UUID, result.UUID)
		assert.NotNil(tt, result.DeletedAt)
		assert.True(tt, result.DeletedAt.Valid)
		assert.Equal(tt, models.LifeCycleStateDeleted, result.LifeCycleState)
		assert.Equal(tt, models.LifeCycleStateDeletedDetails, result.LifeCycleStateDetails)
	})

	t.Run("WhenDeleteBackupPolicyFailsWithNonExistentBackupPolicyID", func(tt *testing.T) {
		result, err := store.DeleteBackupPolicy(context.Background(), "non-existent-uuid")
		assert.Error(tt, err)
		assert.Nil(tt, result)
	})
}

func TestGetBackupPolicyWithDetails(t *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(t, err, "Failed to set up test database")
	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	assert.NoError(t, err, "Failed to clean up test database")

	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: 50, UUID: "test-account-uuid-5"},
		Name:      "test-account-5",
	}
	err = store.db.Create(account).Error()
	assert.NoError(t, err, "Expected no error when creating account")

	backupPolicy := &datamodel.BackupPolicy{
		BaseModel: datamodel.BaseModel{UUID: "test-backup-policy-uuid-6"},
		Name:      "backup-policy-name-6",
		AccountID: account.ID,
		Account:   account,
	}
	err = store.db.Create(backupPolicy).Error()
	assert.NoError(t, err, "Expected no error when creating backup policy")

	t.Run("Returns backup policy with account details", func(tt *testing.T) {
		result, err := _getBackupPolicyWithDetails(store.db.GORM(), &datamodel.BackupPolicy{BaseModel: datamodel.BaseModel{UUID: backupPolicy.UUID}, AccountID: account.ID})
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, backupPolicy.UUID, result.UUID)
		assert.Equal(tt, account.Name, result.Account.Name)
	})

	t.Run("Returns error when backup policy not found", func(tt *testing.T) {
		result, err := _getBackupPolicyWithDetails(store.db.GORM(), &datamodel.BackupPolicy{BaseModel: datamodel.BaseModel{UUID: "non-existent-uuid"}, AccountID: 9999})
		assert.Error(tt, err)
		assert.Nil(tt, result)
	})
}

func TestUpdateBackupPolicy(t *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(t, err, "Failed to set up test database")
	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	assert.NoError(t, err, "Failed to clean up test database")

	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
		Name:      "test-account",
	}
	err = store.db.Create(account).Error()
	assert.NoError(t, err, "Expected no error when creating account")

	backupPolicy := &datamodel.BackupPolicy{
		BaseModel:             datamodel.BaseModel{UUID: "test-backup-policy-uuid"},
		Name:                  "test-backup-policy",
		Description:           nillable.ToPointer("This is a test backup policy"),
		PolicyEnabled:         false,
		DailyBackupsToKeep:    5,
		WeeklyBackupsToKeep:   2,
		MonthlyBackupsToKeep:  1,
		AccountID:             account.ID,
		Account:               account,
		LifeCycleState:        models.LifeCycleStateREADY,
		LifeCycleStateDetails: models.LifeCycleStateReadyDetails,
	}
	err = store.db.Create(backupPolicy).Error()
	assert.NoError(t, err, "Expected no error when creating backup policy")

	t.Run("UpdateBackupPolicySucceeds", func(tt *testing.T) {
		updates := map[string]interface{}{
			"description":             "Updated description",
			"policy_enabled":          true,
			"daily_backups_to_keep":   10,
			"weekly_backups_to_keep":  5,
			"monthly_backups_to_keep": 3,
		}
		result, err := store.UpdateBackupPolicy(context.Background(), backupPolicy.UUID, updates)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "Updated description", *result.Description)
		assert.True(tt, result.PolicyEnabled)
		assert.Equal(tt, int64(10), result.DailyBackupsToKeep)
		assert.Equal(tt, int64(5), result.WeeklyBackupsToKeep)
		assert.Equal(tt, int64(3), result.MonthlyBackupsToKeep)
	})

	t.Run("UpdateBackupPolicyFails", func(tt *testing.T) {
		updates := map[string]interface{}{
			"non_existent_column": "This will fail",
		}
		result, err := store.UpdateBackupPolicy(context.Background(), backupPolicy.UUID, updates)
		assert.Error(tt, err)
		assert.Nil(tt, result)
	})
}
