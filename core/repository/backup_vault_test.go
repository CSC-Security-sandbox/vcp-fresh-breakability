package repository

import (
	"context"
	"errors"
	"strconv"
	"testing"

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

		result, err := store.GetBackupVaultByUUID(context.Background(), backupVault.UUID, int64(123))
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

		_, err = store.GetBackupVaultByUUID(context.Background(), "non-existent-uuid", int64(123))
		if err == nil {
			tt.Errorf("Expected error, got nil")
		}
		if err.Error() != "backup vault 'non-existent-uuid' not found" {
			tt.Errorf("Expected error 'backup vault 'non-existent-uuid' not found', got %v", err)
		}
	})
}

func TestCreatingBackupVault(t *testing.T) {
	t.Run("TestCreatingBackupVaultCreatesNewBackupVaultWhenDoesNotExist", func(tt *testing.T) {
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
			Name:      "test_backup_vault",
			AccountID: account.ID,
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: account.ID, UUID: account.UUID},
			},
		}
		originalGetBackupVaultWithDetails := getBackupVaultWithDetails
		defer func() { getBackupVaultWithDetails = originalGetBackupVaultWithDetails }()

		getBackupVaultWithDetails = func(db *gorm.DB, bv *datamodel.BackupVault) (*datamodel.BackupVault, error) {
			return backupVault, nil
		}

		result, err := store.CreatingBackupVault(context.Background(), backupVault)
		if err != nil {
			tt.Errorf("Expected no error, got %v", err)
		}
		if result.Name != backupVault.Name {
			tt.Errorf("Expected backup vault name %v, got %v", backupVault.Name, result.Name)
		}
		if result.Account.ID != account.ID {
			tt.Errorf("Expected account ID %v, got %v", account.ID, result.Account.ID)
		}
	})
	t.Run("WhenCreatingBackupVaultReturnsConflictErrorWhenBackupVaultExists", func(tt *testing.T) {
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
			Name:      "test_backup_vault",
			AccountID: account.ID,
		}

		err = store.db.Create(backupVault).Error()
		if err != nil {
			tt.Fatalf("Failed to create backup vault: %v", err)
		}

		_, err = store.CreatingBackupVault(context.Background(), backupVault)
		if err == nil {
			tt.Errorf("Expected error, got nil")
		}
	})
	t.Run("WhenCreatingBackupVaultReturnsErrorWhenDatabaseFails", func(tt *testing.T) {
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

		backupVault := &datamodel.BackupVault{
			Name:      "test_backup_vault",
			AccountID: 999, // Invalid account ID to simulate database failure
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 999, UUID: "invalid-account-uuid"},
			},
		}
		_, err = store.CreatingBackupVault(context.Background(), backupVault)
		if err != nil {
			t.Errorf("Expected error, got nil")
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

func TestCreateBackupVaultCreatesAndUpdatesBackupVaultSuccessfully(t *testing.T) {
	t.Run("CreatesBackupVaultWhenEntryDoesNotExist", func(tt *testing.T) {
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
			Name:           "test_backup_vault",
			AccountID:      account.ID,
			LifeCycleState: "READY",
		}
		vcpBvParams := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "non-existent-uuid"},
		}
		originalGetBackupVaultWithDetails := getBackupVaultWithDetails
		defer func() { getBackupVaultWithDetails = originalGetBackupVaultWithDetails }()

		getBackupVaultWithDetails = func(db *gorm.DB, bv *datamodel.BackupVault) (*datamodel.BackupVault, error) {
			return vcpBvParams, nil
		}

		result, err := store.CreateBackupVault(context.Background(), backupVault, vcpBvParams)
		if err != nil {
			tt.Errorf("Expected no error, got %v", err)
		}
		if result.Name != backupVault.Name {
			tt.Errorf("Expected backup vault name %v, got %v", backupVault.Name, result.Name)
		}
		if result.LifeCycleState != models.LifeCycleStateAvailable {
			tt.Errorf("Expected life cycle state %v, got %v", models.LifeCycleStateAvailable, result.LifeCycleState)
		}
	})

	t.Run("ReturnsErrorWhenEntryDoesNotExist", func(tt *testing.T) {
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
			AccountID: 999, // Invalid account ID
		}
		vcpBvParams := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "non-existent-uuid"},
		}

		originalGetBackupVaultWithDetails := getBackupVaultWithDetails
		defer func() { getBackupVaultWithDetails = originalGetBackupVaultWithDetails }()

		getBackupVaultWithDetails = func(db *gorm.DB, bv *datamodel.BackupVault) (*datamodel.BackupVault, error) {
			return nil, gorm.ErrRecordNotFound
		}

		_, err = store.CreateBackupVault(context.Background(), backupVault, vcpBvParams)
		if err == nil {
			tt.Errorf("Expected error, got nil")
		}
		if err.Error() != "entry not found" {
			tt.Errorf("Expected error 'entry not found', got %v", err)
		}
	})

	t.Run("UpdatesBackupVaultWhenEntryExists", func(tt *testing.T) {
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

		existingBackupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "existing-uuid"},
			Name:      "existing_backup_vault",
			AccountID: account.ID,
		}
		originalGetBackupVaultWithDetails := getBackupVaultWithDetails
		defer func() { getBackupVaultWithDetails = originalGetBackupVaultWithDetails }()

		getBackupVaultWithDetails = func(db *gorm.DB, bv *datamodel.BackupVault) (*datamodel.BackupVault, error) {
			if bv.UUID == existingBackupVault.UUID {
				return existingBackupVault, nil
			}
			return nil, gorm.ErrRecordNotFound
		}
		err = store.db.Create(existingBackupVault).Error()
		if err != nil {
			tt.Fatalf("Failed to create backup vault: %v", err)
		}

		updatedBackupVault := &datamodel.BackupVault{
			Name:           "updated_backup_vault",
			AccountID:      account.ID,
			LifeCycleState: "READY",
		}

		result, err := store.CreateBackupVault(context.Background(), updatedBackupVault, existingBackupVault)
		if err != nil {
			tt.Errorf("Expected no error, got %v", err)
		}
		if result.Name != updatedBackupVault.Name {
			tt.Errorf("Expected backup vault name %v, got %v", updatedBackupVault.Name, result.Name)
		}
		if result.LifeCycleState != models.LifeCycleStateAvailable {
			tt.Errorf("Expected life cycle state %v, got %v", models.LifeCycleStateAvailable, result.LifeCycleState)
		}
	})
}

func TestCreateBackupVaultEntryInVCP(tt *testing.T) {
	tt.Run("TestCreateBackupVaultEntryInVCPCreatesBackupVaultSuccessfully", func(t *testing.T) {
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
			Name:      "test_backup_vault",
			AccountID: account.ID,
			Account:   account,
		}

		originalGetBackupVaultWithDetails := getBackupVaultWithDetails
		defer func() { getBackupVaultWithDetails = originalGetBackupVaultWithDetails }()

		getBackupVaultWithDetails = func(db *gorm.DB, bv *datamodel.BackupVault) (*datamodel.BackupVault, error) {
			return backupVault, nil
		}

		result, err := store.CreateBackupVaultEntryInVCP(context.Background(), backupVault)
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
		if result.Name != backupVault.Name {
			t.Errorf("Expected backup vault name %v, got %v", backupVault.Name, result.Name)
		}
		if result.Account.ID != account.ID {
			t.Errorf("Expected account ID %v, got %v", account.ID, result.Account.ID)
		}
	})
	tt.Run("TestCreateBackupVaultEntryInVCPReturnsErrorWhenDatabaseFails", func(t *testing.T) {
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
		originalGetBackupVaultWithDetails := getBackupVaultWithDetails
		defer func() { getBackupVaultWithDetails = originalGetBackupVaultWithDetails }()

		getBackupVaultWithDetails = func(db *gorm.DB, bv *datamodel.BackupVault) (*datamodel.BackupVault, error) {
			return nil, gorm.ErrRecordNotFound
		}

		_, err = store.CreateBackupVaultEntryInVCP(context.Background(), backupVault)
		if err == nil {
			t.Errorf("Expected error, got nil")
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
