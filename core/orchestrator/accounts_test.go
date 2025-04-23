package orchestrator

import (
	"context"
	"fmt"
	"gorm.io/gorm"
	"testing"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
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
		store, err := database.NewTestStorage(mockLogger)
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
		store, err := database.NewTestStorage(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
		}

		account, err := _createAccount(ctx, store, testAccount)
		fmt.Println(account)
		if err != nil {
			tt.Errorf("Expected nil, got error")
		}
		if account.Name != testAccount {
			tt.Errorf("Expected account name 'test_account', got %v", account.Name)
		}

		account1, err := _createAccount(ctx, store, testAccount)
		fmt.Println(account1)
		if err == nil {
			tt.Errorf("Expected error, got nil")
		}
		if err.Error() != "account already exists" {
			tt.Errorf("Expected error 'account already exists', got %v", err)
		}
	})
}

func TestGetAccountWithName(t *testing.T) {
	testAccount := "test_account"
	t.Run("WhenAccountDoesNotExist", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		store, err := database.NewTestStorage(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
		}

		account, err := _getAccountWithName(ctx, store, testAccount)
		if err == nil {
			tt.Errorf("Expected error, got nil")
		}
		if err.Error() != "record not found" {
			tt.Errorf("Expected error '%v', got %v", gorm.ErrRecordNotFound, err)
		}
		if account != nil {
			tt.Errorf("Expected nil account, got %v", account)
		}
	})
	t.Run("WhenAccountExists", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		store, err := database.NewTestStorage(mockLogger)
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
