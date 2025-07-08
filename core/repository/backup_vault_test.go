package repository

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/gorm"
	"gorm.io/gorm"
)

func TestGetBackupVaultWithDetails(t *testing.T) {
	t.Run("WhenBackupVaultExists", func(tt *testing.T) {
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

		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "test-backup-vault-uuid"},
			Name:      "test_backup_vault",
			AccountID: account.ID,
			Account:   account,
		}
		err = store.db.Create(account).Error()
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}
		err = store.db.Create(backupVault).Error()
		if err != nil {
			tt.Fatalf("Failed to create backup vault: %v", err)
		}

		result, err := _getBackupVaultWithDetails(store.db.GORM(), &datamodel.BackupVault{BaseModel: datamodel.BaseModel{UUID: backupVault.UUID}})
		if err != nil {
			tt.Errorf("Expected no error, got %v", err)
		}
		if result.UUID != backupVault.UUID {
			tt.Errorf("Expected backup vault UUID %v, got %v", backupVault.UUID, result.UUID)
		}
		if result.Account.Name != account.Name {
			tt.Errorf("Expected account name %v, got %v", account.Name, result.Account.Name)
		}
	})

	t.Run("WhenBackupVaultDoesNotExist", func(tt *testing.T) {
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

		_, err = _getBackupVaultWithDetails(store.db.GORM(), &datamodel.BackupVault{BaseModel: datamodel.BaseModel{UUID: "non-existent-uuid"}})
		if err == nil {
			tt.Errorf("Expected error, got nil")
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			tt.Errorf("Expected error %v, got %v", gorm.ErrRecordNotFound, err)
		}
	})
}

func TestGetBackupVaultByUUID(t *testing.T) {
	t.Run("WhenGetBackupVaultByUUIDReturnsBackupVaultWhenExists", func(tt *testing.T) {
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
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.db.Create(account).Error()
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "test-backup-vault-uuid"},
			Name:      "test_backup_vault",
			AccountID: account.ID,
			Account:   account,
		}
		err = store.db.Create(backupVault).Error()
		if err != nil {
			tt.Fatalf("Failed to create backup vault: %v", err)
		}
		originalGetBackupVaultWithDetails := getBackupVaultWithDetails
		defer func() { getBackupVaultWithDetails = originalGetBackupVaultWithDetails }()
		getBackupVaultWithDetails = func(db *gorm.DB, bv *datamodel.BackupVault) (*datamodel.BackupVault, error) {
			return backupVault, nil
		}

		result, err := store.GetBackupVaultByUUIDndOwnerID(context.Background(), backupVault.UUID, int64(123))
		if err != nil {
			tt.Errorf("Expected no error, got %v", err)
		}
		if result.UUID != backupVault.UUID {
			tt.Errorf("Expected backup vault UUID %v, got %v", backupVault.UUID, result.UUID)
		}
	})
	t.Run("WhenGetBackupVaultByUUIDReturnsErrorWhenBackupVaultDoesNotExist", func(tt *testing.T) {
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
		originalGetBackupVaultWithDetails := getBackupVaultWithDetails
		defer func() { getBackupVaultWithDetails = originalGetBackupVaultWithDetails }()
		getBackupVaultWithDetails = func(db *gorm.DB, bv *datamodel.BackupVault) (*datamodel.BackupVault, error) {
			return nil, gorm.ErrRecordNotFound
		}

		_, err = store.GetBackupVaultByUUIDndOwnerID(context.Background(), "non-existent-uuid", int64(123))
		if err == nil {
			tt.Errorf("Expected error, got nil")
		}
		if err.Error() != "backup vault 'non-existent-uuid' not found" {
			tt.Errorf("Expected error 'backup vault 'non-existent-uuid' not found', got %v", err)
		}
	})
}

func TestGetBackupVaultByNameAndOwnerID(t *testing.T) {
	t.Run("WhenGetBackupVaultByNameAndOwnerIDReturnsBackupVaultWhenExists", func(tt *testing.T) {
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
			BaseModel: datamodel.BaseModel{ID: 10, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.db.Create(account).Error()
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "test-backup-vault-uuid"},
			Name:      "test_backup_vault",
			AccountID: account.ID,
			Account:   account,
		}
		err = store.db.Create(backupVault).Error()
		if err != nil {
			tt.Fatalf("Failed to create backup vault: %v", err)
		}

		result, err := store.GetBackupVaultByNameAndOwnerID(context.Background(), backupVault.Name, strconv.FormatInt(account.ID, 10))
		if err != nil {
			tt.Errorf("Expected no error, got %v", err)
		}
		if result.UUID != backupVault.UUID {
			tt.Errorf("Expected backup vault UUID %v, got %v", backupVault.UUID, result.UUID)
		}
		if result.Account.Name != account.Name {
			tt.Errorf("Expected account name %v, got %v", account.Name, result.Account.Name)
		}
	})
	t.Run("WhenGetBackupVaultByNameAndOwnerIDReturnsErrorWhenBackupVaultDoesNotExist", func(tt *testing.T) {
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

		_, err = store.GetBackupVaultByNameAndOwnerID(context.Background(), "non-existent-name", "non-existent-owner-id")
		if err == nil {
			tt.Errorf("Expected error, got nil")
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			tt.Errorf("Expected error %v, got %v", gorm.ErrRecordNotFound, err)
		}
	})
}

func TestGetBackupVault(t *testing.T) {
	t.Run("WhenGetBackupVaultReturnsErrorWhenBackupVaultDoesNotExist", func(tt *testing.T) {
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

		_, err = store.GetBackupVault(context.Background(), "non-existent-uuid")
		if err == nil {
			tt.Errorf("Expected error, got nil")
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			tt.Errorf("Expected error %v, got %v", gorm.ErrRecordNotFound, err)
		}
	})
	t.Run("GetBackupVaultReturnsBackupVaultWhenExists", func(tt *testing.T) {
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
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.db.Create(account).Error()
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "test-backup-vault-uuid"},
			Name:      "test_backup_vault",
			AccountID: account.ID,
			Account:   account,
		}
		err = store.db.Create(backupVault).Error()
		if err != nil {
			tt.Fatalf("Failed to create backup vault: %v", err)
		}

		result, err := store.GetBackupVault(context.Background(), backupVault.UUID)
		if err != nil {
			tt.Errorf("Expected no error, got %v", err)
		}
		if result.UUID != backupVault.UUID {
			tt.Errorf("Expected backup vault UUID %v, got %v", backupVault.UUID, result.UUID)
		}
		if result.Account.Name != account.Name {
			tt.Errorf("Expected account name %v, got %v", account.Name, result.Account.Name)
		}
	})
}

func TestUpdateBackupVaultUpdatesBucketDetailsSuccessfully(tt *testing.T) {
	tt.Run("TestUpdateBackupVaultUpdatesBucketDetailsSuccessfully", func(t *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			t.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			t.Fatalf("Failed to clean up test database: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.db.Create(account).Error()
		if err != nil {
			t.Fatalf("Failed to create account: %v", err)
		}

		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "test-backup-vault-uuid"},
			Name:      "test_backup_vault",
			AccountID: account.ID,
			Account:   account,
			BucketDetails: datamodel.BucketDetailsArray{
				{
					BucketName: "old_bucket",
				},
			},
		}
		err = store.db.Create(backupVault).Error()
		if err != nil {
			t.Fatalf("Failed to create backup vault: %v", err)
		}

		updatedBackupVault := &datamodel.BackupVault{
			BaseModel:     datamodel.BaseModel{UUID: backupVault.UUID},
			BucketDetails: datamodel.BucketDetailsArray{}, // Clear bucket details
		}
		originalGetBackupVaultWithDetails := getBackupVaultWithDetails
		defer func() { getBackupVaultWithDetails = originalGetBackupVaultWithDetails }()

		getBackupVaultWithDetails = func(db *gorm.DB, bv *datamodel.BackupVault) (*datamodel.BackupVault, error) {
			return backupVault, nil
		}

		err = store.UpdateBackupVault(context.Background(), updatedBackupVault)
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		result, err := store.GetBackupVault(context.Background(), backupVault.UUID)
		if err != nil {
			t.Fatalf("Failed to retrieve updated backup vault: %v", err)
		}
		if len(result.BucketDetails) != 0 {
			t.Errorf("Expected empty bucket details, got %v", result.BucketDetails)
		}
	})
	tt.Run("TestUpdateBackupVaultUpdatesBucketDetailsFails", func(t *testing.T) {
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
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.db.Create(account).Error()
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "test-backup-vault-uuid"},
			Name:      "test_backup_vault",
			AccountID: account.ID,
			Account:   account,
			BucketDetails: datamodel.BucketDetailsArray{
				{
					BucketName: "old_bucket",
				},
			},
		}
		err = store.db.Create(backupVault).Error()
		if err != nil {
			tt.Fatalf("Failed to create backup vault: %v", err)
		}

		updatedBackupVault := &datamodel.BackupVault{
			BaseModel:     datamodel.BaseModel{UUID: backupVault.UUID},
			BucketDetails: datamodel.BucketDetailsArray{},
		}
		originalGetBackupVaultWithDetails := getBackupVaultWithDetails
		defer func() { getBackupVaultWithDetails = originalGetBackupVaultWithDetails }()

		getBackupVaultWithDetails = func(db *gorm.DB, bv *datamodel.BackupVault) (*datamodel.BackupVault, error) {
			return nil, gorm.ErrRecordNotFound
		}

		err = store.UpdateBackupVault(context.Background(), updatedBackupVault)
		if err == nil {
			t.Errorf("Expected no error, got %v", err)
		}
	})
}

func TestListBackupVaultsReturnsBackupVaultsWhenAccountHasVaults(tt *testing.T) {
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
		BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
		Name:      "test_account",
	}
	err = store.db.Create(account).Error()
	if err != nil {
		tt.Fatalf("Failed to create account: %v", err)
	}

	backupVault := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{UUID: "test-backup-vault-uuid"},
		Name:      "test_backup_vault",
		AccountID: account.ID,
		Account:   account,
	}
	err = store.db.Create(backupVault).Error()
	if err != nil {
		tt.Fatalf("Failed to create backup vault: %v", err)
	}

	result, err := store.ListBackupVaults(context.Background(), account.ID)
	if err != nil {
		tt.Errorf("Expected no error, got %v", err)
	}
	if len(result) != 1 {
		tt.Errorf("Expected 1 backup vault, got %v", len(result))
	}
	if result[0].UUID != backupVault.UUID {
		tt.Errorf("Expected backup vault UUID %v, got %v", backupVault.UUID, result[0].UUID)
	}
}

func TestListBackupVaultsReturnsEmptyListWhenNoVaultsExist(tt *testing.T) {
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
		BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
		Name:      "test_account",
	}
	err = store.db.Create(account).Error()
	if err != nil {
		tt.Fatalf("Failed to create account: %v", err)
	}

	result, err := store.ListBackupVaults(context.Background(), account.ID)
	if err != nil {
		tt.Errorf("Expected no error, got %v", err)
	}
	if len(result) != 0 {
		tt.Errorf("Expected 0 backup vaults, got %v", len(result))
	}
}

func TestListBackupVaultsReturnsErrorWhenDatabaseFails(tt *testing.T) {
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

	// Simulate database failure by closing the connection
	sqlDB, _ := db.DB()
	err = sqlDB.Close()
	if err != nil {
		return
	}

	_, err = store.ListBackupVaults(context.Background(), 123)
	if err == nil {
		tt.Errorf("Expected error, got nil")
	}
}

func TestCreateBackupVaultEntryInVCPCreatesBackupVaultWhenDoesNotExist(tt *testing.T) {
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
		BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: 1},
		Name:      "test_account",
	}
	err = store.db.Create(account).Error()
	if err != nil {
		tt.Fatalf("Failed to create account: %v", err)
	}

	backupVault := &datamodel.BackupVault{
		Name:      "test_backup_vault",
		AccountID: account.ID,
		Account:   account,
		BaseModel: datamodel.BaseModel{UUID: "test-backup-vault-uuid"},
	}

	result, err := store.CreateBackupVaultEntryInVCP(context.Background(), backupVault)
	if err != nil {
		tt.Errorf("Expected no error, got %v", err)
	}
	if result.Name != backupVault.Name {
		tt.Errorf("Expected backup vault name %v, got %v", backupVault.Name, result.Name)
	}
}

func TestCreateBackupVaultEntryInVCPReturnsErrorWhenBackupVaultExists(tt *testing.T) {
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
		BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: 1},
		Name:      "test_account",
	}
	err = store.db.Create(account).Error()
	if err != nil {
		tt.Fatalf("Failed to create account: %v", err)
	}

	backupVault := &datamodel.BackupVault{
		Name:      "test_backup_vault",
		AccountID: account.ID,
		Account:   account,
		BaseModel: datamodel.BaseModel{UUID: "test-backup-vault-uuid"},
	}
	err = store.db.Create(backupVault).Error()
	if err != nil {
		tt.Fatalf("Failed to create backup vault: %v", err)
	}

	defer func() {
		getBackupVaultWithDetails = _getBackupVaultWithDetails
	}()
	getBackupVaultWithDetails = func(db *gorm.DB, query *datamodel.BackupVault) (*datamodel.BackupVault, error) {
		return backupVault, nil
	}

	result, err := store.CreateBackupVaultEntryInVCP(context.Background(), backupVault)
	if err != nil {
		tt.Errorf("Expected no error, got %v", err)
	}
	assert.NotNil(tt, result, "Expected result to be non-nil")
}

func TestCreateBackupVaultEntryInVCPReturnsGetBackupVaultError(tt *testing.T) {
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
		BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: 1},
		Name:      "test_account",
	}
	err = store.db.Create(account).Error()
	if err != nil {
		tt.Fatalf("Failed to create account: %v", err)
	}

	backupVault := &datamodel.BackupVault{
		Name:      "test_backup_vault",
		AccountID: account.ID,
		Account:   account,
		BaseModel: datamodel.BaseModel{UUID: "test-backup-vault-uuid"},
	}
	err = store.db.Create(backupVault).Error()
	if err != nil {
		tt.Fatalf("Failed to create backup vault: %v", err)
	}

	defer func() {
		getBackupVaultWithDetails = _getBackupVaultWithDetails
	}()
	getBackupVaultWithDetails = func(db *gorm.DB, query *datamodel.BackupVault) (*datamodel.BackupVault, error) {
		return nil, gorm.ErrRecordNotFound
	}

	result, err := store.CreateBackupVaultEntryInVCP(context.Background(), backupVault)
	assert.NotNil(tt, err, "Expected error when backup vault does not exist")
	assert.Nil(tt, result, "Expected result to be non-nil")
}

func TestCreateBackupVaultEntryInVCPReturnsErrorWhenDatabaseFails(tt *testing.T) {
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

	backupVault := &datamodel.BackupVault{
		Name:      "test_backup_vault",
		AccountID: 999, // Invalid account ID to simulate database failure
	}

	_, err = store.CreateBackupVaultEntryInVCP(context.Background(), backupVault)
	assert.Nil(tt, err)
}

func TestCreateBackupVaultEntryInVCPReturnsStartTransactionsFails(tt *testing.T) {
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

	backupVault := &datamodel.BackupVault{
		Name:      "test_backup_vault",
		AccountID: 999, // Invalid account ID to simulate database failure
	}

	defer func() {
		startTransaction = _startTransaction
	}()
	startTransaction = func(db *gorm.DB) (*gorm.DB, error) {
		return nil, errors.New("failed to start transaction")
	}

	_, err = store.CreateBackupVaultEntryInVCP(context.Background(), backupVault)
	assert.NotNil(tt, err)
}

func TestCreateBackupVaultEntryInVCPError(tt *testing.T) {
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
		BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: 1},
		Name:      "test_account",
	}
	err = store.db.Create(account).Error()
	if err != nil {
		tt.Fatalf("Failed to create account: %v", err)
	}

	backupVault := &datamodel.BackupVault{
		Name:      "test_backup_vault",
		AccountID: account.ID,
		Account:   account,
		BaseModel: datamodel.BaseModel{UUID: "test-backup-vault-uuid"},
	}
	err = store.db.Create(backupVault).Error()
	if err != nil {
		tt.Fatalf("Failed to create backup vault: %v", err)
	}

	defer func() {
		getBackupVaultWithDetails = _getBackupVaultWithDetails
		checkBVExists = _checkBVExists
	}()
	getBackupVaultWithDetails = func(db *gorm.DB, query *datamodel.BackupVault) (*datamodel.BackupVault, error) {
		return backupVault, nil
	}
	checkBVExists = func(tx *gorm.DB, bv *datamodel.BackupVault) (*datamodel.BackupVault, error) {
		return nil, errors.New("mock checkBVExists error")
	}

	result, err := store.CreateBackupVaultEntryInVCP(context.Background(), backupVault)
	assert.Nil(tt, result)
	assert.Error(tt, err, "Expected error when creating backup vault entry in VCP")
}

func TestGetBackupVaultById(t *testing.T) {
	t.Run("TestGetBackupVaultByIdReturnsBackupVaultWhenExists", func(tt *testing.T) {
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
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.db.Create(account).Error()
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{ID: 123},
			Name:      "test_backup_vault",
			AccountID: account.ID,
			Account:   account,
		}
		err = store.db.Create(backupVault).Error()
		if err != nil {
			tt.Fatalf("Failed to create backup vault: %v", err)
		}

		result, err := store.GetBackupVaultById(context.Background(), backupVault.ID)
		if err != nil {
			tt.Errorf("Expected no error, got %v", err)
		}
		if result.ID != backupVault.ID {
			tt.Errorf("Expected backup vault ID %v, got %v", backupVault.ID, result.ID)
		}
		if result.Account.Name != account.Name {
			tt.Errorf("Expected account name %v, got %v", account.Name, result.Account.Name)
		}
	})
	t.Run("TestGetBackupVaultByIdReturnsErrorWhenBackupVaultDoesNotExist", func(tt *testing.T) {
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

		_, err = store.GetBackupVaultById(context.Background(), 999) // Non-existent ID
		if err == nil {
			tt.Errorf("Expected error, got nil")
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			tt.Errorf("Expected error %v, got %v", gorm.ErrRecordNotFound, err)
		}
	})
}

func TestUpdateBackupVaultInVCP(tt *testing.T) {
	tt.Run("TestUpdateBackupVaultInVCPUpdatesBackupVaultSuccessfully", func(t *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			t.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			t.Fatalf("Failed to clean up test database: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.db.Create(account).Error()
		if err != nil {
			t.Fatalf("Failed to create account: %v", err)
		}
		initDesc := "Initial description"
		backupVault := &datamodel.BackupVault{
			BaseModel:   datamodel.BaseModel{UUID: "test-backup-vault-uuid"},
			Name:        "test_backup_vault",
			AccountID:   account.ID,
			Account:     account,
			Description: &initDesc,
		}
		err = store.db.Create(backupVault).Error()
		if err != nil {
			t.Fatalf("Failed to create backup vault: %v", err)
		}
		desc := "Updated description"
		updateParams := &datamodel.BackupVault{
			BaseModel:     datamodel.BaseModel{UUID: backupVault.UUID},
			Description:   &desc,
			AccountID:     account.ID,
			Name:          "Updated Backup Vault Name",
			BucketDetails: backupVault.BucketDetails,
		}
		updatedParams := &datamodel.BackupVault{
			BaseModel:     datamodel.BaseModel{UUID: backupVault.UUID},
			Description:   &desc,
			AccountID:     account.ID,
			Name:          "Updated Backup Vault Name",
			BucketDetails: backupVault.BucketDetails,
		}

		result, err := store.UpdateBackupVaultInVCP(context.Background(), updateParams, updatedParams)
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
		assert.NotNil(t, result)
		assert.NoError(t, err)
	})
}

func TestUpdatingBackupVaultState(tt *testing.T) {
	tt.Run("TestUpdatingBackupVaultStateUpdatesStateSuccessfully", func(t *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			t.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			t.Fatalf("Failed to clean up test database: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.db.Create(account).Error()
		if err != nil {
			t.Fatalf("Failed to create account: %v", err)
		}

		backupVault := &datamodel.BackupVault{
			BaseModel:             datamodel.BaseModel{UUID: "test-backup-vault-uuid"},
			Name:                  "test_backup_vault",
			AccountID:             account.ID,
			Account:               account,
			LifeCycleState:        models.LifeCycleStateAvailable,
			LifeCycleStateDetails: models.LifeCycleStateAvailableDetails,
		}
		err = store.db.Create(backupVault).Error()
		if err != nil {
			t.Fatalf("Failed to create backup vault: %v", err)
		}

		bv, err := store.UpdateBackupVaultState(context.Background(), backupVault, models.LifeCycleStateUpdating, models.LifeCycleStateUpdatingDetails)
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		result, err := store.GetBackupVault(context.Background(), backupVault.UUID)
		if err != nil {
			t.Fatalf("Failed to retrieve updated backup vault: %v", err)
		}
		assert.Equal(t, models.LifeCycleStateUpdating, result.LifeCycleState)
		assert.Equal(t, models.LifeCycleStateUpdatingDetails, result.LifeCycleStateDetails)
		assert.NotNil(t, bv)
	})
}
