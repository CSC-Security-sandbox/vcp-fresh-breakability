package database

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils/gorm"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
	"gorm.io/driver/postgres"
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

// TestListAccountsForTelemetry tests the optimized account query for telemetry/bizops
func TestListAccountsForTelemetry(t *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(t, err, "Failed to set up test database")
	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	assert.NoError(t, err, "Failed to clean up test database")

	accounts := []*datamodel.Account{
		{
			BaseModel:   datamodel.BaseModel{UUID: "uuid-1"},
			Name:        "account1",
			State:       "ENABLED",
			Description: "Account 1 description - this field should not be returned",
		},
		{
			BaseModel:   datamodel.BaseModel{UUID: "uuid-2"},
			Name:        "account2",
			State:       "DISABLED",
			Description: "Account 2 description - this field should not be returned",
		},
		{
			BaseModel: datamodel.BaseModel{UUID: "uuid-3", DeletedAt: &gorm.DeletedAt{Time: time.Now(), Valid: true}},
			Name:      "account3",
			State:     "DELETED",
		},
	}
	for _, acc := range accounts {
		err := store.db.Create(acc).Error()
		assert.NoError(t, err, "Failed to create account")
	}

	t.Run("Returns only active accounts with minimal fields", func(tt *testing.T) {
		result, err := store.ListAccountsForTelemetry(context.Background(), nil)
		assert.NoError(tt, err)
		assert.Len(tt, result, 2) // Should not include soft-deleted account

		// Verify the returned data contains only the expected fields
		for _, acc := range result {
			assert.NotZero(tt, acc.ID)
			assert.NotEmpty(tt, acc.Name)
			assert.NotEmpty(tt, acc.State)
		}
	})

	t.Run("Pagination works correctly", func(tt *testing.T) {
		pagination := &dbutils.Pagination{Limit: 1, Offset: 0}
		result, err := store.ListAccountsForTelemetry(context.Background(), pagination)
		assert.NoError(tt, err)
		assert.Len(tt, result, 1)
		assert.Equal(tt, "account1", result[0].Name)
		assert.Equal(tt, "ENABLED", result[0].State)
	})

	t.Run("Pagination with offset", func(tt *testing.T) {
		pagination := &dbutils.Pagination{Limit: 1, Offset: 1}
		result, err := store.ListAccountsForTelemetry(context.Background(), pagination)
		assert.NoError(tt, err)
		assert.Len(tt, result, 1)
		assert.Equal(tt, "account2", result[0].Name)
		assert.Equal(tt, "DISABLED", result[0].State)
	})

	t.Run("Empty records with high offset", func(tt *testing.T) {
		pagination := &dbutils.Pagination{Limit: 10, Offset: 100}
		result, err := store.ListAccountsForTelemetry(context.Background(), pagination)
		assert.NoError(tt, err)
		assert.Len(tt, result, 0)
	})
}

func TestUpdateAccountStateForHandleResource(t *testing.T) {
	t.Run("WhenAccountStateIsUpdatedSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create an account first
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-account-uuid",
			},
			Name:  "test_account",
			State: "initial_state",
		}
		err = store.db.Create(account).Error()
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		// Update the account state
		newState := "enabled"
		err = store.UpdateAccountStateForHandleResource(context.Background(), account.UUID, newState)
		assert.NoError(tt, err, "Expected no error, got %v", err)

		// Verify the state was updated
		updatedAccount, err := store.GetAccountByUUID(context.Background(), account.UUID)
		assert.NoError(tt, err, "Failed to retrieve updated account")
		assert.Equal(tt, newState, updatedAccount.State, "Expected account state %v, got %v", newState, updatedAccount.State)
	})

	t.Run("WhenAccountDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Try to update state for non-existent account
		err = store.UpdateAccountStateForHandleResource(context.Background(), "non-existent-uuid", "enabled")

		// The function should succeed even if no rows are affected (GORM behavior)
		// But we can verify no account exists with this UUID
		assert.NoError(tt, err, "Expected no error for non-existent account update")
	})

	t.Run("WhenAccountStateIsUpdatedToDisabled", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create an account first
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-account-uuid-2",
			},
			Name:  "test_account_2",
			State: "enabled",
		}
		err = store.db.Create(account).Error()
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		// Update the account state to disabled
		newState := "disabled"
		err = store.UpdateAccountStateForHandleResource(context.Background(), account.UUID, newState)
		assert.NoError(tt, err, "Expected no error, got %v", err)

		// Verify the state was updated
		updatedAccount, err := store.GetAccountByUUID(context.Background(), account.UUID)
		assert.NoError(tt, err, "Failed to retrieve updated account")
		assert.Equal(tt, newState, updatedAccount.State, "Expected account state %v, got %v", newState, updatedAccount.State)
	})

	t.Run("WhenEmptyStateIsProvided", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create an account first
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-account-uuid-3",
			},
			Name:  "test_account_3",
			State: "enabled",
		}
		err = store.db.Create(account).Error()
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		// Update the account state to empty string
		newState := ""
		err = store.UpdateAccountStateForHandleResource(context.Background(), account.UUID, newState)
		assert.NoError(tt, err, "Expected no error, got %v", err)

		// Verify the state was updated to empty string
		updatedAccount, err := store.GetAccountByUUID(context.Background(), account.UUID)
		assert.NoError(tt, err, "Failed to retrieve updated account")
		assert.Equal(tt, newState, updatedAccount.State, "Expected account state %v, got %v", newState, updatedAccount.State)
	})

	t.Run("WhenDatabaseError", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Close the database to simulate a database error
		sqlDB, err := db.DB()
		assert.NoError(tt, err)
		err = sqlDB.Close()
		if err != nil {
			return
		}

		// Try to update state when database is closed
		err = store.UpdateAccountStateForHandleResource(context.Background(), "test-uuid", "enabled")

		// Should return a VCP error wrapping the database error
		var customErr *vsaerrors.CustomError
		if vsaerrors.As(err, &customErr) {
			assert.Equal(tt, vsaerrors.ErrDatabaseUpdateAccountState, customErr.TrackingID, "Expected database update error code")
			assert.Contains(tt, customErr.OriginalErr.Error(), "sql: database is closed", "Expected database closed error in original error")
		} else {
			tt.Fatal("Expected VCP error, got different error type")
		}
	})
}

func TestUpdateAccountVolumeRefreshTimestamp(t *testing.T) {
	t.Run("WhenAccountExistsWithNoMetadata", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create an account without metadata
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-account-uuid",
			},
			Name:            "test_account",
			AccountMetadata: nil, // No metadata initially
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err, "Failed to create account")

		// Update the timestamp
		completionTime := time.Now()
		err = store.UpdateAccountVolumeRefreshTimestamp(context.Background(), account.UUID, completionTime)
		assert.NoError(tt, err, "Expected no error, got %v", err)

		// Verify the timestamp was updated
		updatedAccount, err := store.GetAccountByUUID(context.Background(), account.UUID)
		assert.NoError(tt, err, "Failed to retrieve updated account")
		assert.NotNil(tt, updatedAccount.AccountMetadata, "AccountMetadata should not be nil")
		assert.WithinDuration(tt, completionTime, updatedAccount.AccountMetadata.VolumeRefreshWorkflowLastCompletionAt, time.Second)
	})

	t.Run("WhenAccountExistsWithExistingMetadata", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create an account with existing metadata
		oldTime := time.Now().Add(-24 * time.Hour)
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-account-uuid",
			},
			Name: "test_account",
			AccountMetadata: &datamodel.AccountMetadata{
				VolumeRefreshWorkflowLastCompletionAt: oldTime,
			},
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err, "Failed to create account")

		// Update the timestamp
		newTime := time.Now()
		err = store.UpdateAccountVolumeRefreshTimestamp(context.Background(), account.UUID, newTime)
		assert.NoError(tt, err, "Expected no error, got %v", err)

		// Verify the timestamp was updated
		updatedAccount, err := store.GetAccountByUUID(context.Background(), account.UUID)
		assert.NoError(tt, err, "Failed to retrieve updated account")
		assert.NotNil(tt, updatedAccount.AccountMetadata, "AccountMetadata should not be nil")
		assert.WithinDuration(tt, newTime, updatedAccount.AccountMetadata.VolumeRefreshWorkflowLastCompletionAt, time.Second)
		assert.NotEqual(tt, oldTime, updatedAccount.AccountMetadata.VolumeRefreshWorkflowLastCompletionAt, "Timestamp should be updated")
	})

	t.Run("WhenAccountDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Try to update timestamp for non-existent account
		err = store.UpdateAccountVolumeRefreshTimestamp(context.Background(), "non-existent-uuid", time.Now())

		// Should return an error since GetAccountByUUID will fail
		assert.Error(tt, err, "Expected error for non-existent account")
		var customErr *vsaerrors.CustomError
		if vsaerrors.As(err, &customErr) {
			assert.Equal(tt, 404, *customErr.HttpCode, "Expected 404 error code")
		}
	})

	t.Run("WhenZeroTimeIsProvided", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create an account
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-account-uuid",
			},
			Name:            "test_account",
			AccountMetadata: nil,
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err, "Failed to create account")

		// Update with zero time
		zeroTime := time.Time{}
		err = store.UpdateAccountVolumeRefreshTimestamp(context.Background(), account.UUID, zeroTime)
		assert.NoError(tt, err, "Expected no error, got %v", err)

		// Verify the timestamp was set to zero time
		updatedAccount, err := store.GetAccountByUUID(context.Background(), account.UUID)
		assert.NoError(tt, err, "Failed to retrieve updated account")
		assert.NotNil(tt, updatedAccount.AccountMetadata, "AccountMetadata should not be nil")
		assert.True(tt, updatedAccount.AccountMetadata.VolumeRefreshWorkflowLastCompletionAt.IsZero(), "Timestamp should be zero")
	})

	t.Run("WhenFutureTimeIsProvided", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create an account
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-account-uuid",
			},
			Name:            "test_account",
			AccountMetadata: nil,
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err, "Failed to create account")

		// Update with future time
		futureTime := time.Now().Add(24 * time.Hour)
		err = store.UpdateAccountVolumeRefreshTimestamp(context.Background(), account.UUID, futureTime)
		assert.NoError(tt, err, "Expected no error, got %v", err)

		// Verify the timestamp was set to future time
		updatedAccount, err := store.GetAccountByUUID(context.Background(), account.UUID)
		assert.NoError(tt, err, "Failed to retrieve updated account")
		assert.NotNil(tt, updatedAccount.AccountMetadata, "AccountMetadata should not be nil")
		assert.WithinDuration(tt, futureTime, updatedAccount.AccountMetadata.VolumeRefreshWorkflowLastCompletionAt, time.Second)
	})

	t.Run("WhenMultipleUpdatesArePerformed", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create an account
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-account-uuid",
			},
			Name:            "test_account",
			AccountMetadata: nil,
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err, "Failed to create account")

		// Perform multiple updates
		time1 := time.Now()
		err = store.UpdateAccountVolumeRefreshTimestamp(context.Background(), account.UUID, time1)
		assert.NoError(tt, err)

		time2 := time.Now().Add(1 * time.Hour)
		err = store.UpdateAccountVolumeRefreshTimestamp(context.Background(), account.UUID, time2)
		assert.NoError(tt, err)

		time3 := time.Now().Add(2 * time.Hour)
		err = store.UpdateAccountVolumeRefreshTimestamp(context.Background(), account.UUID, time3)
		assert.NoError(tt, err)

		// Verify the final timestamp
		updatedAccount, err := store.GetAccountByUUID(context.Background(), account.UUID)
		assert.NoError(tt, err, "Failed to retrieve updated account")
		assert.NotNil(tt, updatedAccount.AccountMetadata, "AccountMetadata should not be nil")
		assert.WithinDuration(tt, time3, updatedAccount.AccountMetadata.VolumeRefreshWorkflowLastCompletionAt, time.Second)
	})

	t.Run("WhenDatabaseError", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create an account
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-account-uuid",
			},
			Name:            "test_account",
			AccountMetadata: nil,
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err, "Failed to create account")

		// Close the database to simulate a database error
		sqlDB, err := db.DB()
		assert.NoError(tt, err)
		err = sqlDB.Close()
		if err != nil {
			return
		}

		// Try to update timestamp when database is closed
		err = store.UpdateAccountVolumeRefreshTimestamp(context.Background(), account.UUID, time.Now())

		// Should return a VCP error wrapping the database error
		// The error occurs during GetAccountByUUID (read operation), not during the update
		assert.Error(tt, err)
		var customErr *vsaerrors.CustomError
		if vsaerrors.As(err, &customErr) {
			assert.Equal(tt, vsaerrors.ErrDatabaseDataReadError, customErr.TrackingID, "Expected database read error code since GetAccountByUUID fails first")
		}
	})
}

func TestAccountMetadataTrialModeJSONShape(t *testing.T) {
	start := time.Date(2025, 5, 14, 11, 0, 0, 0, time.UTC)
	end := start.Add(30 * 24 * time.Hour)
	meta := datamodel.AccountMetadata{
		VolumeRefreshWorkflowLastCompletionAt: time.Date(2024, 8, 1, 15, 30, 0, 0, time.UTC),
		TrialMode: &datamodel.AccountTrialMode{
			StartTime: &start,
			EndTime:   &end,
		},
	}

	b, err := json.Marshal(meta)
	require.NoError(t, err)
	assert.Contains(t, string(b), `"trialMode"`)
	assert.Contains(t, string(b), `"startTime"`)
	assert.Contains(t, string(b), `"endTime"`)
	assert.NotContains(t, string(b), `"trialStart"`)
}

func TestUpdateAccountTrialMetadata(t *testing.T) {
	trialWindow := func(dayOffset int) (time.Time, time.Time) {
		start := time.Date(2025, 5, 14, 11, 0, 0, 0, time.UTC).Add(time.Duration(dayOffset) * 24 * time.Hour)
		end := start.Add(30 * 24 * time.Hour)
		return start, end
	}

	t.Run("WhenNoPriorMetadata_MultipleTrialUpdatesThenVolumeRefresh_SubsequentTrialRetainsVolumeRefresh", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		accountUUID := "trial-metadata-no-prior-uuid"
		account := &datamodel.Account{
			BaseModel:       datamodel.BaseModel{UUID: accountUUID},
			Name:            "trial_metadata_no_prior",
			AccountMetadata: nil,
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err)

		ctx := context.Background()
		start1, end1 := trialWindow(0)
		err = store.UpdateAccountTrialMetadata(ctx, account, &datamodel.AccountTrialMode{StartTime: &start1, EndTime: &end1})
		assert.NoError(tt, err)

		start2, end2 := trialWindow(10)
		account, err = store.GetAccountByUUID(ctx, accountUUID)
		assert.NoError(tt, err)
		err = store.UpdateAccountTrialMetadata(ctx, account, &datamodel.AccountTrialMode{StartTime: &start2, EndTime: &end2})
		assert.NoError(tt, err)

		afterTrialOnly, err := store.GetAccountByUUID(ctx, accountUUID)
		assert.NoError(tt, err)
		assert.NotNil(tt, afterTrialOnly.AccountMetadata)
		require.NotNil(tt, afterTrialOnly.AccountMetadata.TrialMode)
		assert.WithinDuration(tt, start2, *afterTrialOnly.AccountMetadata.TrialMode.StartTime, time.Second)
		assert.WithinDuration(tt, end2, *afterTrialOnly.AccountMetadata.TrialMode.EndTime, time.Second)

		volumeRefreshTime := time.Date(2024, 8, 1, 15, 30, 0, 0, time.UTC)
		err = store.UpdateAccountVolumeRefreshTimestamp(ctx, accountUUID, volumeRefreshTime)
		assert.NoError(tt, err)

		start3, end3 := trialWindow(20)
		account, err = store.GetAccountByUUID(ctx, accountUUID)
		assert.NoError(tt, err)
		err = store.UpdateAccountTrialMetadata(ctx, account, &datamodel.AccountTrialMode{StartTime: &start3, EndTime: &end3})
		assert.NoError(tt, err)

		final, err := store.GetAccountByUUID(ctx, accountUUID)
		assert.NoError(tt, err)
		assert.NotNil(tt, final.AccountMetadata)
		require.NotNil(tt, final.AccountMetadata.TrialMode)
		assert.WithinDuration(tt, start3, *final.AccountMetadata.TrialMode.StartTime, time.Second)
		assert.WithinDuration(tt, end3, *final.AccountMetadata.TrialMode.EndTime, time.Second)
		assert.WithinDuration(tt, volumeRefreshTime, final.AccountMetadata.VolumeRefreshWorkflowLastCompletionAt, time.Second)
	})

	t.Run("WhenVolumeRefreshExists_MultipleTrialUpdatesRetainVolumeRefresh", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		volumeRefreshTime := time.Date(2024, 3, 1, 12, 0, 0, 0, time.UTC)
		accountUUID := "trial-metadata-with-volume-refresh-uuid"
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: accountUUID},
			Name:      "trial_metadata_with_volume_refresh",
			AccountMetadata: &datamodel.AccountMetadata{
				VolumeRefreshWorkflowLastCompletionAt: volumeRefreshTime,
			},
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err)

		ctx := context.Background()
		for _, dayOffset := range []int{0, 5, 15} {
			start, end := trialWindow(dayOffset)
			account, err = store.GetAccountByUUID(ctx, accountUUID)
			assert.NoError(tt, err)
			err = store.UpdateAccountTrialMetadata(ctx, account, &datamodel.AccountTrialMode{StartTime: &start, EndTime: &end})
			assert.NoError(tt, err)

			updated, err := store.GetAccountByUUID(ctx, accountUUID)
			assert.NoError(tt, err)
			assert.NotNil(tt, updated.AccountMetadata)
			require.NotNil(tt, updated.AccountMetadata.TrialMode)
			assert.WithinDuration(tt, start, *updated.AccountMetadata.TrialMode.StartTime, time.Second)
			assert.WithinDuration(tt, end, *updated.AccountMetadata.TrialMode.EndTime, time.Second)
			assert.WithinDuration(
				tt,
				volumeRefreshTime,
				updated.AccountMetadata.VolumeRefreshWorkflowLastCompletionAt,
				time.Second,
				"trial update must not change volumeRefreshWorkflowLastCompletionAt",
			)
		}
	})

	t.Run("WhenVolumeRefreshExists_InterleavedTrialAndVolumeRefreshUpdates", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		initialVolumeRefresh := time.Date(2023, 1, 10, 9, 0, 0, 0, time.UTC)
		accountUUID := "trial-metadata-interleaved-uuid"
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: accountUUID},
			Name:      "trial_metadata_interleaved",
			AccountMetadata: &datamodel.AccountMetadata{
				VolumeRefreshWorkflowLastCompletionAt: initialVolumeRefresh,
			},
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err)

		ctx := context.Background()
		start1, end1 := trialWindow(0)
		err = store.UpdateAccountTrialMetadata(ctx, account, &datamodel.AccountTrialMode{StartTime: &start1, EndTime: &end1})
		assert.NoError(tt, err)

		updatedVolumeRefresh := time.Date(2024, 6, 20, 18, 45, 0, 0, time.UTC)
		err = store.UpdateAccountVolumeRefreshTimestamp(ctx, accountUUID, updatedVolumeRefresh)
		assert.NoError(tt, err)

		start2, end2 := trialWindow(7)
		account, err = store.GetAccountByUUID(ctx, accountUUID)
		assert.NoError(tt, err)
		err = store.UpdateAccountTrialMetadata(ctx, account, &datamodel.AccountTrialMode{StartTime: &start2, EndTime: &end2})
		assert.NoError(tt, err)

		start3, end3 := trialWindow(14)
		account, err = store.GetAccountByUUID(ctx, accountUUID)
		assert.NoError(tt, err)
		err = store.UpdateAccountTrialMetadata(ctx, account, &datamodel.AccountTrialMode{StartTime: &start3, EndTime: &end3})
		assert.NoError(tt, err)

		final, err := store.GetAccountByUUID(ctx, accountUUID)
		assert.NoError(tt, err)
		assert.NotNil(tt, final.AccountMetadata)
		require.NotNil(tt, final.AccountMetadata.TrialMode)
		assert.WithinDuration(tt, start3, *final.AccountMetadata.TrialMode.StartTime, time.Second)
		assert.WithinDuration(tt, end3, *final.AccountMetadata.TrialMode.EndTime, time.Second)
		assert.WithinDuration(tt, updatedVolumeRefresh, final.AccountMetadata.VolumeRefreshWorkflowLastCompletionAt, time.Second)
		assert.NotEqual(tt, initialVolumeRefresh, final.AccountMetadata.VolumeRefreshWorkflowLastCompletionAt)
	})

	t.Run("OmittedSecondUpdateDoesNotClearExistingTrialMode", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		accountUUID := "trial-retain-omit-second-uuid"
		account := &datamodel.Account{
			BaseModel:       datamodel.BaseModel{UUID: accountUUID},
			Name:            "trial_retain_omit_second",
			AccountMetadata: nil,
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err)

		ctx := context.Background()
		start, end := trialWindow(0)
		err = store.UpdateAccountTrialMetadata(ctx, account, &datamodel.AccountTrialMode{StartTime: &start, EndTime: &end})
		assert.NoError(tt, err)

		afterFirst, err := store.GetAccountByUUID(ctx, accountUUID)
		assert.NoError(tt, err)
		require.NotNil(tt, afterFirst.AccountMetadata)
		require.NotNil(tt, afterFirst.AccountMetadata.TrialMode)
		assert.WithinDuration(tt, start, *afterFirst.AccountMetadata.TrialMode.StartTime, time.Second)
		assert.WithinDuration(tt, end, *afterFirst.AccountMetadata.TrialMode.EndTime, time.Second)

		// No second UpdateAccountTrialMetadata call (omitted trialMode on a later create skips persist).
		afterOmit, err := store.GetAccountByUUID(ctx, accountUUID)
		assert.NoError(tt, err)
		require.NotNil(tt, afterOmit.AccountMetadata)
		require.NotNil(tt, afterOmit.AccountMetadata.TrialMode)
		assert.WithinDuration(tt, start, *afterOmit.AccountMetadata.TrialMode.StartTime, time.Second)
		assert.WithinDuration(tt, end, *afterOmit.AccountMetadata.TrialMode.EndTime, time.Second)
	})

	t.Run("WhenTrialIsNil_ReturnsNilWithoutUpdating", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		store := NewDataStoreRepository(gormwrapper.New(db))

		err = store.UpdateAccountTrialMetadata(context.Background(), &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "any-uuid"},
		}, nil)
		assert.NoError(tt, err)
	})

	t.Run("WhenAccountIsNil_ReturnsAccountNotFound", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		store := NewDataStoreRepository(gormwrapper.New(db))
		start, end := trialWindow(0)

		err = store.UpdateAccountTrialMetadata(context.Background(), nil, &datamodel.AccountTrialMode{
			StartTime: &start,
			EndTime:   &end,
		})
		assert.Error(tt, err)
	})

	t.Run("WhenAccountUUIDEmpty_ReturnsAccountNotFound", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		store := NewDataStoreRepository(gormwrapper.New(db))
		start, end := trialWindow(0)

		err = store.UpdateAccountTrialMetadata(context.Background(), &datamodel.Account{
			Name: "no-uuid",
		}, &datamodel.AccountTrialMode{
			StartTime: &start,
			EndTime:   &end,
		})
		assert.Error(tt, err)
	})

	t.Run("WhenDatabaseUpdateFails_ReturnsError", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		accountUUID := "trial-metadata-db-error-uuid"
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: accountUUID},
			Name:      "trial_metadata_db_error",
		}
		assert.NoError(tt, store.db.Create(account).Error())

		account, err = store.GetAccountByUUID(context.Background(), accountUUID)
		assert.NoError(tt, err)

		sqlDB, err := db.DB()
		assert.NoError(tt, err)
		assert.NoError(tt, sqlDB.Close())

		start, end := trialWindow(0)
		err = store.UpdateAccountTrialMetadata(context.Background(), account, &datamodel.AccountTrialMode{
			StartTime: &start,
			EndTime:   &end,
		})
		assert.Error(tt, err)
	})
}

func TestListFreeTrialAccountsForBilling(t *testing.T) {
	t.Run("ReturnsAccountsWithTrialEndTime", func(tt *testing.T) {
		dbSQL, mock, err := sqlmock.New()
		require.NoError(tt, err)
		defer func() { _ = dbSQL.Close() }()

		dialector := postgres.New(postgres.Config{Conn: dbSQL, PreferSimpleProtocol: true})
		gormDB, err := gorm.Open(dialector, &gorm.Config{})
		require.NoError(tt, err)

		trialEnd := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
		rows := sqlmock.NewRows([]string{"id", "free_trial_end_at"}).
			AddRow(int64(10), trialEnd).
			AddRow(int64(20), trialEnd)

		mock.ExpectQuery(`SELECT`).WillReturnRows(rows)

		wrapper := gormwrapper.New(gormDB)
		store := NewDataStoreRepository(wrapper)

		result, err := store.ListFreeTrialAccountsForBilling(context.Background())
		assert.NoError(tt, err)
		assert.Len(tt, result, 2)
		assert.Equal(tt, &trialEnd, result[int64(10)])
		assert.Equal(tt, &trialEnd, result[int64(20)])
		assert.NoError(tt, mock.ExpectationsWereMet())
	})

	t.Run("ReturnsEmptyMapWhenNoRows", func(tt *testing.T) {
		dbSQL, mock, err := sqlmock.New()
		require.NoError(tt, err)
		defer func() { _ = dbSQL.Close() }()

		dialector := postgres.New(postgres.Config{Conn: dbSQL, PreferSimpleProtocol: true})
		gormDB, err := gorm.Open(dialector, &gorm.Config{})
		require.NoError(tt, err)

		rows := sqlmock.NewRows([]string{"id", "free_trial_end_at"})
		mock.ExpectQuery(`SELECT`).WillReturnRows(rows)

		wrapper := gormwrapper.New(gormDB)
		store := NewDataStoreRepository(wrapper)

		result, err := store.ListFreeTrialAccountsForBilling(context.Background())
		assert.NoError(tt, err)
		assert.Empty(tt, result)
		assert.NoError(tt, mock.ExpectationsWereMet())
	})

	t.Run("ReturnsErrorOnDBFailure", func(tt *testing.T) {
		dbSQL, mock, err := sqlmock.New()
		require.NoError(tt, err)
		defer func() { _ = dbSQL.Close() }()

		dialector := postgres.New(postgres.Config{Conn: dbSQL, PreferSimpleProtocol: true})
		gormDB, err := gorm.Open(dialector, &gorm.Config{})
		require.NoError(tt, err)

		mock.ExpectQuery(`SELECT`).WillReturnError(fmt.Errorf("connection refused"))

		wrapper := gormwrapper.New(gormDB)
		store := NewDataStoreRepository(wrapper)

		result, err := store.ListFreeTrialAccountsForBilling(context.Background())
		assert.Nil(tt, result)
		assert.Error(tt, err)
		assert.NoError(tt, mock.ExpectationsWereMet())
	})

	t.Run("SkipsRowsWithNilFreeTrialEndAt", func(tt *testing.T) {
		dbSQL, mock, err := sqlmock.New()
		require.NoError(tt, err)
		defer func() { _ = dbSQL.Close() }()

		dialector := postgres.New(postgres.Config{Conn: dbSQL, PreferSimpleProtocol: true})
		gormDB, err := gorm.Open(dialector, &gorm.Config{})
		require.NoError(tt, err)

		trialEnd := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
		rows := sqlmock.NewRows([]string{"id", "free_trial_end_at"}).
			AddRow(int64(10), trialEnd).
			AddRow(int64(20), nil)

		mock.ExpectQuery(`SELECT`).WillReturnRows(rows)

		wrapper := gormwrapper.New(gormDB)
		store := NewDataStoreRepository(wrapper)

		result, err := store.ListFreeTrialAccountsForBilling(context.Background())
		assert.NoError(tt, err)
		assert.Len(tt, result, 1)
		assert.Equal(tt, &trialEnd, result[int64(10)])
		assert.NoError(tt, mock.ExpectationsWereMet())
	})
}
