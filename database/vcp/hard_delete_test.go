package database

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils/gorm"
	"gorm.io/gorm"
)

func TestAccountSoftDelete(t *testing.T) {
	t.Run("WhenAccountExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create test account
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name:  "test_account",
			State: datamodel.AccountStateEnabled,
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err, "Failed to create account")

		// Perform soft delete
		err = store.DeleteAccount(context.Background(), account.ID)
		assert.NoError(tt, err, "Expected no error during soft delete")

		// Verify account is soft deleted
		var deletedAccount datamodel.Account
		err = store.db.GORM().Unscoped().First(&deletedAccount, account.ID).Error
		assert.NoError(tt, err, "Failed to retrieve soft deleted account")
		assert.NotNil(tt, deletedAccount.DeletedAt, "DeletedAt should not be nil")
		assert.True(tt, deletedAccount.DeletedAt.Valid, "DeletedAt should be valid")
		assert.Equal(tt, datamodel.AccountStateDeleted, deletedAccount.State, "Account state should be deleted")
		assert.Equal(tt, "Deleted", deletedAccount.StateDetails, "State details should match")
	})

	t.Run("WhenAccountDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Try to soft delete non-existent account
		err = store.DeleteAccount(context.Background(), 9999)
		assert.Error(tt, err, "Expected error when account does not exist")
		assert.Equal(tt, gorm.ErrRecordNotFound, err, "Expected record not found error")
	})
}

func TestHardDeleteResourceByTable(t *testing.T) {
	t.Run("WhenResourcesExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create test account
		accountID := int64(1)
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   accountID,
				UUID: "test-account-uuid",
			},
			Name: "test_account",
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err, "Failed to create account")

		// Create multiple volumes for the account
		for i := 0; i < 10; i++ {
			volume := &datamodel.Volume{
				BaseModel: datamodel.BaseModel{
					UUID: fmt.Sprintf("test-volume-uuid-%d", i),
				},
				Name:      fmt.Sprintf("test_volume_%d", i),
				AccountID: accountID,
			}
			err = store.db.Create(volume).Error()
			assert.NoError(tt, err, "Failed to create volume")
		}

		// Hard delete volumes by account ID
		err = store.HardDeleteResourceByTable(context.Background(), "volumes", "account_id = ?", accountID)
		assert.NoError(tt, err, "Expected no error during hard delete")

		// Verify all volumes are deleted
		var count int64
		err = store.db.GORM().Unscoped().Table("volumes").Where("account_id = ?", accountID).Count(&count).Error
		assert.NoError(tt, err, "Failed to count volumes")
		assert.Equal(tt, int64(0), count, "All volumes should be deleted")
	})

	t.Run("WhenNoResourcesExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Try to delete non-existent resources
		err = store.HardDeleteResourceByTable(context.Background(), "volumes", "account_id = ?", 9999)
		assert.NoError(tt, err, "Should not error when no resources exist")
	})

	t.Run("WhenLargeBatchDelete", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create test account
		accountID := int64(1)
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   accountID,
				UUID: "test-account-uuid",
			},
			Name: "test_account",
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err, "Failed to create account")

		// Create more volumes than batch size
		volumeCount := 100
		for i := 0; i < volumeCount; i++ {
			volume := &datamodel.Volume{
				BaseModel: datamodel.BaseModel{
					UUID: fmt.Sprintf("test-volume-uuid-%d", i),
				},
				Name:      fmt.Sprintf("test_volume_%d", i),
				AccountID: accountID,
			}
			err = store.db.Create(volume).Error()
			assert.NoError(tt, err, "Failed to create volume")
		}

		// Hard delete volumes by account ID
		err = store.HardDeleteResourceByTable(context.Background(), "volumes", "account_id = ?", accountID)
		assert.NoError(tt, err, "Expected no error during batch hard delete")

		// Verify all volumes are deleted
		var count int64
		err = store.db.GORM().Unscoped().Table("volumes").Where("account_id = ?", accountID).Count(&count).Error
		assert.NoError(tt, err, "Failed to count volumes")
		assert.Equal(tt, int64(0), count, "All volumes should be deleted in batches")
	})
}

func TestRollBackDeletedAccount(t *testing.T) {
	t.Run("WhenAccountIsSoftDeleted", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create and soft-delete an account
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name:  "test_account",
			State: datamodel.AccountStateDeleted,
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err, "Failed to create account")

		// Soft delete
		err = store.DeleteAccount(context.Background(), account.ID)
		assert.NoError(tt, err, "Expected no error during soft delete")

		// Rollback soft delete
		err = store.RollBackDeletedAccount(context.Background(), account.ID)
		assert.NoError(tt, err, "Expected no error during rollback")

		// Verify account is restored
		var restoredAccount datamodel.Account
		err = store.db.GORM().First(&restoredAccount, account.ID).Error
		assert.NoError(tt, err, "Failed to retrieve restored account")
		assert.Nil(tt, restoredAccount.DeletedAt, "DeletedAt should be nil after rollback")
		assert.Equal(tt, datamodel.AccountStateHyperscalerDisabled, restoredAccount.State, "Account state should be hyperscalerDisabled")
		assert.Equal(tt, datamodel.LifeCycleStateHyperscalerDisabledDetails, restoredAccount.StateDetails, "State details should match")
	})

	t.Run("WhenAccountDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Try to rollback non-existent account
		err = store.RollBackDeletedAccount(context.Background(), 9999)
		assert.Error(tt, err, "Expected error when account does not exist")
		assert.Equal(tt, gorm.ErrRecordNotFound, err, "Expected record not found error")
	})
}
