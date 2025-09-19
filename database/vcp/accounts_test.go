package database

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils/gorm"
	"gorm.io/gorm"
)

func TestGetAccount(t *testing.T) {
	t.Run("WhenAccountExists", func(tt *testing.T) {
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

		result, err := store.GetAccount(context.Background(), account.Name)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, account.Name, result.Name, "Expected account name %v, got %v", account.Name, result.Name)
	})
	t.Run("WhenAccountDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		_, err = store.GetAccount(context.Background(), "non-existent-account")
		var customErr *vsaerrors.CustomError
		if vsaerrors.As(err, &customErr) {
			assert.EqualError(tt, err, "Account not found")
		}
	})
}

func TestCreateAccount(t *testing.T) {
	t.Run("WhenAccountIsCreatedSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-account-uuid",
			},
			Name: "test_account",
		}

		createdAccount, err := store.CreateAccount(context.Background(), account)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, account.Name, createdAccount.Name, "Expected account name %v, got %v", account.Name, createdAccount.Name)
	})
	t.Run("WhenAccountAlreadyExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-account-uuid",
			},
			Name: "test_account",
		}
		err = store.db.Create(account).Error()
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		_, err = store.CreateAccount(context.Background(), account)
		assert.EqualError(tt, err, "account already exists")
	})
}

func TestGetSoftDeleteAccount(t *testing.T) {
	t.Run("WhenSoftDeletedAccountExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create an account
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-uuid",
			},
			Name: "soft-deleted-account",
		}
		createdAccount, err := store.CreateAccount(context.Background(), account)
		assert.NoError(tt, err)
		assert.NotNil(tt, createdAccount)

		// Soft delete the account
		err = store.db.GORM().Delete(&datamodel.Account{}, "uuid = ?", account.UUID).Error
		assert.NoError(tt, err)

		// Retrieve the soft-deleted account
		retrievedAccount, err := store.GetSoftDeleteAccount(context.Background(), "soft-deleted-account")
		assert.NoError(tt, err)
		assert.NotNil(tt, retrievedAccount)
		assert.Equal(tt, "soft-deleted-account", retrievedAccount.Name)
		assert.Equal(tt, "test-uuid", retrievedAccount.UUID)
		assert.NotNil(tt, retrievedAccount.DeletedAt)
	})

	t.Run("WhenActiveAccountExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create an active account (not soft-deleted)
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "active-uuid",
			},
			Name: "active-account",
		}
		createdAccount, err := store.CreateAccount(context.Background(), account)
		assert.NoError(tt, err)
		assert.NotNil(tt, createdAccount)

		// Retrieve the active account using GetSoftDeleteAccount
		retrievedAccount, err := store.GetSoftDeleteAccount(context.Background(), "active-account")
		assert.NoError(tt, err)
		assert.NotNil(tt, retrievedAccount)
		assert.Equal(tt, "active-account", retrievedAccount.Name)
		assert.Equal(tt, "active-uuid", retrievedAccount.UUID)
		assert.Nil(tt, retrievedAccount.DeletedAt)
	})

	t.Run("WhenAccountDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Try to retrieve a non-existent account
		retrievedAccount, err := store.GetSoftDeleteAccount(context.Background(), "non-existent-account")
		assert.Error(tt, err)
		assert.Nil(tt, retrievedAccount)

		// Verify the error is the expected type
		var customErr *vsaerrors.CustomError
		assert.True(tt, vsaerrors.As(err, &customErr))
		assert.Equal(tt, 404, *customErr.HttpCode)
	})

	t.Run("WhenMultipleAccountsExistWithSameName", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create first account
		account1 := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "uuid-1",
			},
			Name: "duplicate-name",
		}
		_, err = store.CreateAccount(context.Background(), account1)
		assert.NoError(tt, err)

		// Soft delete the first account
		err = store.db.GORM().Delete(&datamodel.Account{}, "uuid = ?", account1.UUID).Error
		assert.NoError(tt, err)

		// Create second account with same name (this would normally fail with CreateAccount,
		// but we're creating it directly to test the scenario)
		account2 := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "uuid-2",
			},
			Name: "duplicate-name",
		}
		err = store.db.GORM().Create(account2).Error
		assert.NoError(tt, err)

		// GetSoftDeleteAccount should return one of them (typically the first one found)
		retrievedAccount, err := store.GetSoftDeleteAccount(context.Background(), "duplicate-name")
		assert.NoError(tt, err)
		assert.NotNil(tt, retrievedAccount)
		assert.Equal(tt, "duplicate-name", retrievedAccount.Name)
		assert.True(tt, retrievedAccount.UUID == "uuid-1" || retrievedAccount.UUID == "uuid-2")
	})
}

func TestGetAccounts(t *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(t, err, "Failed to set up test database")
	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	assert.NoError(t, err, "Failed to clean up test database")

	accounts := []*datamodel.Account{
		{
			BaseModel: datamodel.BaseModel{UUID: "uuid-1"},
			Name:      "account1",
		},
		{
			BaseModel: datamodel.BaseModel{UUID: "uuid-2"},
			Name:      "account2",
		},
		{
			BaseModel: datamodel.BaseModel{UUID: "uuid-3", DeletedAt: &gorm.DeletedAt{Time: time.Now(), Valid: true}},
			Name:      "account3",
		},
	}
	for _, acc := range accounts {
		err := store.db.Create(acc).Error()
		assert.NoError(t, err, "Failed to create account")
	}

	t.Run("Get all accounts without filter/pagination", func(tt *testing.T) {
		result, err := store.GetAccounts(context.Background(), false, nil)
		assert.NoError(tt, err)
		assert.Len(tt, result, 2)
	})

	t.Run("Pagination limit 1", func(tt *testing.T) {
		pagination := &dbutils.Pagination{Limit: 1, Offset: 0}
		result, err := store.GetAccounts(context.Background(), false, pagination)
		assert.NoError(tt, err)
		assert.Len(tt, result, 1)
	})

	t.Run("Include deleted records", func(tt *testing.T) {
		result, err := store.GetAccounts(context.Background(), true, nil)
		assert.NoError(tt, err)
		assert.Len(tt, result, 3)
	})

	t.Run("Include deleted records with pagination", func(tt *testing.T) {
		pagination := &dbutils.Pagination{Limit: 1, Offset: 2}
		result, err := store.GetAccounts(context.Background(), true, pagination)
		assert.NoError(tt, err)
		assert.Len(tt, result, 1)
		assert.Equal(tt, "uuid-3", result[0].UUID)
	})

	t.Run("Empty Records", func(tt *testing.T) {
		pagination := &dbutils.Pagination{Limit: 1, Offset: 3}
		result, err := store.GetAccounts(context.Background(), true, pagination)
		assert.NoError(tt, err)
		assert.Len(tt, result, 0)
	})
}
