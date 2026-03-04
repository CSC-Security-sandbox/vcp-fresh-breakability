package gcp

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestGetOrCreateAccount(t *testing.T) {
	t.Run("WhenGetAccountWithNameFails", func(tt *testing.T) {
		ctx := context.Background()
		se := database.Storage(nil)

		dbAccount := &datamodel.Account{
			Name: "test_account",
		}

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.New("account not found")
		}
		createAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return dbAccount, nil
		}

		account, err := getOrCreateAccount(ctx, se, "test_account")
		if err != nil {
			t.Errorf("Expected nil, got Error %v", err)
		}
		if account.Name != "test_account" {
			t.Errorf("Expected account name 'test_account', got %v", account.Name)
		}
	})

	t.Run("WhenAccountIsDisabled", func(tt *testing.T) {
		ctx := context.Background()
		se := database.Storage(nil)

		disabledAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-uuid",
			},
			Name:  "test_account",
			State: models.AccountStateDisabled,
		}
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return disabledAccount, nil
		}

		_, err := getOrCreateAccount(ctx, se, "test_account")
		if err == nil {
			t.Errorf("Expected error, got nil")
		}
		if err.Error() != "account is disabled" {
			t.Errorf("Expected error 'account is disabled', got %v", err)
		}
	})

	t.Run("WhenCreateAccountFails", func(tt *testing.T) {
		ctx := context.Background()
		se := database.Storage(nil)

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.New("account not found")
		}
		createAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.New("failed to create account")
		}
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.New("account not found")
		}

		_, err := getOrCreateAccount(ctx, se, "test_account")
		if err == nil {
			t.Errorf("Expected error, got nil")
		}
		if err.Error() != "account not found" {
			t.Errorf("Expected error 'account not found', got %v", err)
		}
	})

	t.Run("WhenAccountIsCreatedSuccessfully", func(tt *testing.T) {
		ctx := context.Background()
		se := database.Storage(nil)

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.New("account not found")
		}
		createdAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-uuid",
			},
			Name:  "test_account",
			State: models.AccountStateEnabled,
		}
		createAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return createdAccount, nil
		}

		account, err := getOrCreateAccount(ctx, se, "test_account")
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
		if account.Name != "test_account" {
			t.Errorf("Expected account name 'test_account', got %v", account.Name)
		}
	})
}

func TestCreateAccount(t *testing.T) {
	testAccount := "test_account"
	t.Run("WhenAccountIsCreatedSuccessfully", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
		}

		account, err := _createAccount(ctx, store, testAccount)

		if err != nil {
			tt.Errorf("Expected no error, got %v", err)
		}
		if account.Name != testAccount {
			tt.Errorf("Expected account name 'test_account', got %v", account.Name)
		}
	})
	t.Run("WhenCreateAccountFails", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
		}

		account, err := _createAccount(ctx, store, testAccount)
		if err != nil {
			tt.Errorf("Expected nil, got error")
		}
		if account.Name != testAccount {
			tt.Errorf("Expected account name 'test_account', got %v", account.Name)
		}

		_, err = _createAccount(ctx, store, testAccount)
		if err == nil {
			tt.Errorf("Expected error, got nil")
		}
		if err.Error() != "account already exists" {
			tt.Errorf("Expected error 'account already exists', got %v", err)
		}
	})
}

func TestGetAccountName(t *testing.T) {
	t.Run("WhenAccountIsNil", func(tt *testing.T) {
		result := getAccountName(nil)
		assert.Equal(tt, "", result, "Expected empty string when account is nil")
	})

	t.Run("WhenAccountIsNotNil", func(tt *testing.T) {
		account := &datamodel.Account{
			Name: "test_account",
		}
		result := getAccountName(account)
		assert.Equal(tt, "test_account", result, "Expected account name 'test_account'")
	})

	t.Run("WhenAccountHasEmptyName", func(tt *testing.T) {
		account := &datamodel.Account{
			Name: "",
		}
		result := getAccountName(account)
		assert.Equal(tt, "", result, "Expected empty string when account name is empty")
	})
}

func TestGetAccountWithName(t *testing.T) {
	testAccount := "test_account"
	t.Run("WhenAccountDoesNotExist", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
		}

		account, mainErr := _getAccountWithName(ctx, store, testAccount)
		assert.NotNil(tt, mainErr, "Expected an error when account does not exist")
		assert.EqualError(tt, mainErr, "Account not found")
		var customErr *vsaerrors.CustomError
		if vsaerrors.As(mainErr, &customErr) {
			assert.Equal(tt, customErr.OriginalErr.Error(), "account not found")
			assert.Equal(tt, customErr.HttpCode, nillable.ToPointer(404), "Expected HTTP code 404 for not found error but got %v", customErr.HttpCode)
			assert.Equal(tt, customErr.TrackingID, 2101)
			assert.Equal(tt, customErr.Message, "Account not found")
			assert.Equal(tt, customErr.Retriable, false)
		} else {
			tt.Fatalf("Expected a CustomError, got %T", err)
		}
		if account != nil {
			tt.Errorf("Expected nil account, got %v", account)
		}
	})
	t.Run("WhenAccountExists", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
		}

		err = store.DB().Create(&datamodel.Account{Name: testAccount}).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		account, err := _getAccountWithName(ctx, store, testAccount)
		if err != nil {
			tt.Errorf("Expected no error, got %v", err)
		}
		if account.Name != testAccount {
			tt.Errorf("Expected account name '%s', got %v", testAccount, account.Name)
		}
	})
}
