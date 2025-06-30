package repository

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/gorm"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
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
