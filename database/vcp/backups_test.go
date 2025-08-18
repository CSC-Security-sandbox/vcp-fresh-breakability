package database

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils/gorm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"gorm.io/gorm"
)

func Test_getBackupWithName(t *testing.T) {
	t.Run("ReturnsBackupWhenNameExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "test-backup-vault-uuid"},
			Name:      "test-backup-vault",
		}
		err = store.db.Create(backupVault).Error()
		assert.NoError(tt, err)

		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "test-backup-uuid"},
			Name:          "test_backup",
			BackupVaultID: backupVault.ID,
		}
		err = store.db.Create(backup).Error()
		assert.NoError(tt, err)

		result, err := _getBackupVaultByNameAndBackupVaultID(store.db.GORM(), &datamodel.Backup{Name: backup.Name, BackupVaultID: backupVault.ID})
		assert.NoError(tt, err)
		assert.Equal(tt, backup.Name, result.Name)
		assert.Equal(tt, backupVault.ID, result.BackupVaultID)
	})
	t.Run("ReturnsErrorWhenNameDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		_, err = _getBackupVaultByNameAndBackupVaultID(store.db.GORM(), &datamodel.Backup{Name: "non-existent-name", BackupVaultID: 1})
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "not found")
	})
	t.Run("ReturnsErrorWhenDBFails", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Simulate DB failure by closing the connection
		sqlDB, err := store.db.GORM().DB()
		assert.NoError(tt, err)
		_ = sqlDB.Close()

		_, err = _getBackupVaultByNameAndBackupVaultID(store.db.GORM(), &datamodel.Backup{Name: "any-name", BackupVaultID: 1})
		assert.Error(tt, err)
	})
}

func TestCreateBackup(t *testing.T) {
	t.Run("CreatesBackupSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		backup := &datamodel.Backup{
			Name:          "test_backup",
			BackupVaultID: 1,
		}

		result, err := store.CreateBackup(context.Background(), backup)
		assert.NoError(tt, err)
		assert.Equal(tt, backup.Name, result.Name)
		assert.Equal(tt, models.LifeCycleStateCreating, result.State)
	})

	t.Run("ReturnsErrorWhenBackupAlreadyExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		existingBackup := &datamodel.Backup{
			Name:          "test_backup",
			BackupVaultID: 1,
		}
		err = store.db.Create(existingBackup).Error()
		assert.NoError(tt, err)

		backup := &datamodel.Backup{
			Name:          "test_backup",
			BackupVaultID: 1,
		}

		_, err = store.CreateBackup(context.Background(), backup)
		assert.Error(tt, err)
		assert.Equal(tt, "backup already exists", err.Error())
	})
}

func TestGetBackup(t *testing.T) {
	t.Run("ReturnsBackupWhenExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Create account
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test-account",
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err)

		// Create backup vault
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "test-backup-vault-uuid"},
			Name:      "test-backup-vault",
		}
		backupVault.AccountID = account.ID
		err = store.db.Create(backupVault).Error()
		assert.NoError(tt, err)

		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "test-backup-uuid"},
			Name:          "test_backup",
			BackupVaultID: backupVault.ID,
		}
		err = store.db.Create(backup).Error()
		assert.NoError(tt, err)

		result, err := store.GetBackup(context.Background(), backupVault.UUID, backup.UUID, account.Name)
		assert.NoError(tt, err)
		assert.Equal(tt, backup.UUID, result.UUID)
	})

	t.Run("ReturnsErrorWhenBackupDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Create account
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test-account",
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err)

		// Create backup vault
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "test-backup-vault-uuid"},
			Name:      "test-backup-vault",
		}
		backupVault.AccountID = account.ID
		err = store.db.Create(backupVault).Error()
		assert.NoError(tt, err)

		_, err = store.GetBackup(context.Background(), backupVault.UUID, "non-existent-backup-uuid", account.Name)
		assert.Error(tt, err)
	})
}

func TestDeleteBackup(t *testing.T) {
	t.Run("DeletesBackupSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{UUID: "test-backup-uuid"},
		}
		err = store.db.Create(backup).Error()
		assert.NoError(tt, err)

		result, err := store.DeleteBackup(context.Background(), backup.UUID)
		assert.NoError(tt, err)
		assert.Equal(tt, backup.UUID, result.UUID)
	})

	t.Run("ReturnsErrorWhenBackupDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		_, err = store.DeleteBackup(context.Background(), "non-existent-uuid")
		assert.Error(tt, err)
	})
}

func TestCreateBackup_Errors(t *testing.T) {
	t.Run("ReturnsErrorWhenDBFails", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		backup := &datamodel.Backup{
			Name:          "test_backup",
			BackupVaultID: 1,
		}

		// Simulate DB failure by closing the connection
		sqlDB, err := store.db.GORM().DB()
		assert.NoError(tt, err)
		_ = sqlDB.Close() // Close the database connection to simulate failure

		_, err = store.CreateBackup(context.Background(), backup)
		assert.Error(tt, err) // Expect an error due to DB failure
	})
}

func TestGetBackup_Errors(t *testing.T) {
	t.Run("ReturnsErrorWhenDBFails", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)
		_, err = store.GetBackup(context.Background(), "test-backup-vault-uuid", "test-backup-uuid", "test-account")
		assert.Error(tt, err)
	})
}

func TestDeleteBackup_Errors(t *testing.T) {
	t.Run("ReturnsErrorWhenDBFails", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		_, err = store.DeleteBackup(context.Background(), "test-uuid")
		assert.Error(tt, err)
	})
}

func TestUpdateBackupState_Errors(t *testing.T) {
	t.Run("ReturnsErrorWhenDBFails", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{UUID: "test-uuid"},
			State:     models.LifeCycleStateAvailable,
		}
		_, err = store.UpdateBackupState(context.Background(), backup)
		assert.Error(tt, err)
	})
}

func TestCreateBackup_EdgeCases(t *testing.T) {
	t.Run("ReturnsErrorWhenTransactionFails", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		backup := &datamodel.Backup{
			Name:          "test_backup",
			BackupVaultID: 1,
		}
		defer func() {
			startTransaction = _startTransaction
		}()
		// Simulate transaction failure
		startTransaction = func(db *gorm.DB) (*gorm.DB, error) {
			return nil, errors.New("transaction failed")
		}

		_, err = store.CreateBackup(context.Background(), backup)
		assert.Error(tt, err)
		assert.Equal(tt, "transaction failed", err.Error())
	})
}

func TestDeleteBackup_EdgeCases(t *testing.T) {
	t.Run("ReturnsErrorWhenTransactionFails", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{UUID: "test-backup-uuid"},
		}
		err = store.db.Create(backup).Error()
		assert.NoError(tt, err)
		defer func() {
			startTransaction = _startTransaction
		}()
		// Simulate transaction failure
		startTransaction = func(db *gorm.DB) (*gorm.DB, error) {
			return nil, errors.New("transaction failed")
		}
		_, err = store.DeleteBackup(context.Background(), backup.UUID)
		assert.Error(tt, err)
		assert.Equal(tt, "transaction failed", err.Error())
	})
}

func TestFinishBackup_EdgeCases(t *testing.T) {
	t.Run("ReturnsErrorWhenBackupNotFound", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{UUID: "non-existent-uuid"},
		}

		_, err = store.FinishBackup(context.Background(), backup)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "not found")
	})
}

func TestUpdateBackup_EdgeCases(t *testing.T) {
	t.Run("ReturnsErrorWhenBackupNotFound", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{UUID: "non-existent-uuid"},
		}
		_, err = store.UpdateBackup(context.Background(), backup)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "not found")
	})
}

func TestUpdateBackupState_EdgeCases(t *testing.T) {
	t.Run("ReturnsErrorWhenBackupNotFound", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{UUID: "non-existent-uuid"},
			State:     models.LifeCycleStateAvailable,
		}

		_, err = store.UpdateBackupState(context.Background(), backup)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "not found")
	})
}

func TestUpdateBackupDescriptionSetsStateToAvailable(t *testing.T) {
	t.Run("UpdatesDescriptionAndSetsStateToAvailable", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Create account and backup vault first
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      "test-account",
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err)

		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "test-vault-uuid", ID: 1},
			AccountID: 1,
			Name:      "test-vault",
		}
		err = store.db.Create(backupVault).Error()
		assert.NoError(tt, err)

		// Create backup with UPLOADING state (simulating an update in progress)
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "test-backup-uuid"},
			Name:          "test-backup",
			Description:   "Original description",
			BackupVaultID: 1,
			State:         models.LifeCycleStateUpdating, // Using the correct internal updating state
			StateDetails:  "Updating backup",
		}
		err = store.db.Create(backup).Error()
		assert.NoError(tt, err)

		// Update the backup description
		backup.Description = "Updated backup description"
		updatedBackup, err := store.UpdateBackup(context.Background(), backup)

		// Assert successful update
		assert.NoError(tt, err)
		assert.NotNil(tt, updatedBackup)
		assert.Equal(tt, "Updated backup description", updatedBackup.Description)
		assert.Equal(tt, models.LifeCycleStateAvailable, updatedBackup.State)
		assert.Equal(tt, models.LifeCycleStateAvailableDetails, updatedBackup.StateDetails)

		// Verify the backup state is persisted in database
		var dbBackup datamodel.Backup
		err = store.db.GORM().Where("uuid = ?", "test-backup-uuid").First(&dbBackup).Error
		assert.NoError(tt, err)
		assert.Equal(tt, "Updated backup description", dbBackup.Description)
		assert.Equal(tt, models.LifeCycleStateAvailable, dbBackup.State)
		assert.Equal(tt, models.LifeCycleStateAvailableDetails, dbBackup.StateDetails)
	})
}

func TestUpdateBackup(t *testing.T) {
	t.Run("ReturnsErrorWhenDBFailsInIsBackupInCreatingStateByVolume", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Simulate DB failure by closing the connection
		sqlDB, err := store.db.GORM().DB()
		assert.NoError(tt, err)
		_ = sqlDB.Close()

		_, err = store.IsBackupInCreatingorDeletingStateByVolume(context.Background(), "any-volume")
		assert.Error(tt, err)
	})

	t.Run("ReturnsBackupsByBackupVaultSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		bv := &datamodel.BackupVault{AccountID: 1, BaseModel: datamodel.BaseModel{UUID: "123", ID: 1}}
		backup := &datamodel.Backup{
			Name:          "backup-vault",
			BackupVaultID: 1,
			BackupVault:   bv,
			VolumeUUID:    "any-volume",
		}
		err = store.db.Create(bv).Error()
		assert.NoError(tt, err)
		err = store.db.Create(backup).Error()
		assert.NoError(tt, err)

		backups, err := store.GetBackupsByBackupVaultOwnerIDAndFilter(context.Background(), "123", 1, nil)
		assert.NoError(tt, err)
		assert.Len(tt, backups, 1)
		assert.Equal(tt, "backup-vault", backups[0].Name)
	})

	t.Run("ReturnsEmptySliceWhenNoBackupsForBackupVault", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		backups, err := store.GetBackupsByBackupVaultOwnerIDAndFilter(context.Background(), "non-existent-vault", 1, nil)
		assert.Error(tt, err)
		assert.Equal(tt, "backup vault not found", err.Error())
		assert.Empty(tt, backups)
	})
}
func TestUpdateBackupState(t *testing.T) {
	t.Run("UpdatesBackupStateSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		backup := &datamodel.Backup{
			BaseModel:    datamodel.BaseModel{UUID: "test-backup-uuid"},
			State:        models.LifeCycleStateCreating,
			StateDetails: "Creating backup",
		}
		err = store.db.Create(backup).Error()
		assert.NoError(tt, err)

		backup.State = models.LifeCycleStateAvailable
		backup.StateDetails = "Backup available"

		updatedBackup, err := store.UpdateBackupState(context.Background(), backup)
		assert.NoError(tt, err)
		assert.Equal(tt, models.LifeCycleStateAvailable, updatedBackup.State)
		assert.Equal(tt, "Backup available", updatedBackup.StateDetails)
	})

	t.Run("ReturnsErrorWhenBackupNotFound", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		backup := &datamodel.Backup{
			BaseModel:    datamodel.BaseModel{UUID: "non-existent-uuid"},
			State:        models.LifeCycleStateAvailable,
			StateDetails: "Backup available",
		}

		_, err = store.UpdateBackupState(context.Background(), backup)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "not found")
	})

	t.Run("ReturnsErrorWhenDBFailsDuringUpdate", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		backup := &datamodel.Backup{
			BaseModel:    datamodel.BaseModel{UUID: "test-backup-uuid"},
			State:        models.LifeCycleStateCreating,
			StateDetails: "Creating backup",
		}
		err = store.db.Create(backup).Error()
		assert.NoError(tt, err)

		backup.State = models.LifeCycleStateAvailable
		backup.StateDetails = "Backup available"

		// Simulate DB failure by closing the connection
		sqlDB, err := store.db.GORM().DB()
		assert.NoError(tt, err)
		_ = sqlDB.Close()

		_, err = store.UpdateBackupState(context.Background(), backup)
		assert.Error(tt, err)
	})
}
func TestFinishBackup(t *testing.T) {
	t.Run("FinishesBackupSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		backup := &datamodel.Backup{
			BaseModel:    datamodel.BaseModel{UUID: "test-backup-uuid"},
			Description:  "Initial description",
			Attributes:   &datamodel.BackupAttributes{},
			State:        models.LifeCycleStateCreating,
			StateDetails: models.LifeCycleStateCreatingDetails,
			SizeInBytes:  1024,
		}
		err = store.db.Create(backup).Error()
		assert.NoError(tt, err)

		backup.Description = "Updated description"
		backup.Attributes = &datamodel.BackupAttributes{SnapshotID: "test-snapshot-id"}
		finishedBackup, err := store.FinishBackup(context.Background(), backup)
		assert.NoError(tt, err)
		assert.Equal(tt, models.LifeCycleStateAvailable, finishedBackup.State)
		assert.Equal(tt, models.LifeCycleStateAvailableDetails, finishedBackup.StateDetails)
		assert.Equal(tt, "Updated description", finishedBackup.Description)
		assert.Equal(tt, backup.Attributes, finishedBackup.Attributes)
		assert.Equal(tt, int64(1024), finishedBackup.SizeInBytes)
	})

	t.Run("ReturnsErrorWhenBackupNotFound", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		backup := &datamodel.Backup{
			BaseModel:   datamodel.BaseModel{UUID: "non-existent-uuid"},
			Description: "Updated description",
			Attributes:  &datamodel.BackupAttributes{},
		}

		_, err = store.FinishBackup(context.Background(), backup)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "not found")
	})

	t.Run("ReturnsErrorWhenDBFailsDuringFinishBackup", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		backup := &datamodel.Backup{
			BaseModel:    datamodel.BaseModel{UUID: "test-backup-uuid"},
			Description:  "Initial description",
			Attributes:   &datamodel.BackupAttributes{},
			State:        models.LifeCycleStateCreating,
			StateDetails: models.LifeCycleStateCreatingDetails,
		}
		err = store.db.Create(backup).Error()
		assert.NoError(tt, err)

		backup.Description = "Updated description"
		backup.Attributes = &datamodel.BackupAttributes{}

		// Simulate DB failure by closing the connection
		sqlDB, err := store.db.GORM().DB()
		assert.NoError(tt, err)
		_ = sqlDB.Close()

		_, err = store.FinishBackup(context.Background(), backup)
		assert.Error(tt, err)
	})
}

func TestIsLatestBackup(t *testing.T) {
	t.Run("OnSuccessWithLatestBackup", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		backup1 := &datamodel.Backup{
			BaseModel:    datamodel.BaseModel{UUID: "test-backup-uuid1"},
			Name:         "test_backup",
			Description:  "Test backup",
			State:        models.LifeCycleStateAvailable,
			StateDetails: models.LifeCycleStateAvailableDetails,
			VolumeUUID:   "volume1",
		}
		err = store.db.Create(backup1).Error()
		assert.NoError(tt, err)

		backup2 := &datamodel.Backup{
			BaseModel:    datamodel.BaseModel{UUID: "test-backup-uuid2"},
			Name:         "test_backup",
			Description:  "Test backup",
			State:        models.LifeCycleStateAvailable,
			StateDetails: models.LifeCycleStateAvailableDetails,
			VolumeUUID:   "volume1",
		}
		err = store.db.Create(backup2).Error()
		assert.NoError(tt, err)

		isLatest, err := store.IsLatestBackup(context.Background(), backup2.UUID, "volume1")
		assert.NoError(tt, err)
		assert.True(tt, isLatest)
	})
	t.Run("OnSuccessWithNotLatestBackup", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		backup1 := &datamodel.Backup{
			BaseModel:    datamodel.BaseModel{UUID: "test-backup-uuid1"},
			Name:         "test_backup",
			Description:  "Test backup",
			State:        models.LifeCycleStateAvailable,
			StateDetails: models.LifeCycleStateAvailableDetails,
			VolumeUUID:   "volume1",
		}
		err = store.db.Create(backup1).Error()
		assert.NoError(tt, err)

		backup2 := &datamodel.Backup{
			BaseModel:    datamodel.BaseModel{UUID: "test-backup-uuid2"},
			Name:         "test_backup",
			Description:  "Test backup",
			State:        models.LifeCycleStateAvailable,
			StateDetails: models.LifeCycleStateAvailableDetails,
			VolumeUUID:   "volume1",
		}
		err = store.db.Create(backup2).Error()
		assert.NoError(tt, err)

		isLatest, err := store.IsLatestBackup(context.Background(), backup1.UUID, "volume1")
		assert.NoError(tt, err)
		assert.False(tt, isLatest)
	})
	t.Run("OnError", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Simulate DB failure by closing the connection
		sqlDB, err := store.db.GORM().DB()
		assert.NoError(tt, err)
		_ = sqlDB.Close()

		isLatest, err := store.IsLatestBackup(context.Background(), "test-backup-uuid", "volume1")
		assert.Error(tt, err)
		assert.False(tt, isLatest)
	})
}

func TestBackupCountByVolumeID(t *testing.T) {
	t.Run("OnSuccessWithBackupCount", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		backup1 := &datamodel.Backup{
			BaseModel:    datamodel.BaseModel{UUID: "test-backup-uuid1"},
			Name:         "test_backup",
			Description:  "Test backup",
			State:        models.LifeCycleStateAvailable,
			StateDetails: models.LifeCycleStateAvailableDetails,
			VolumeUUID:   "volume1",
		}
		err = store.db.Create(backup1).Error()
		assert.NoError(tt, err)

		backup2 := &datamodel.Backup{
			BaseModel:    datamodel.BaseModel{UUID: "test-backup-uuid2"},
			Name:         "test_backup",
			Description:  "Test backup",
			State:        models.LifeCycleStateError,
			StateDetails: models.LifeCycleStateCreationErrorDetails,
			VolumeUUID:   "volume1",
		}
		err = store.db.Create(backup2).Error()
		assert.NoError(tt, err)

		count, err := store.BackupCountByVolumeID(context.Background(), "volume1")
		assert.NoError(tt, err)
		assert.Equal(tt, int64(1), count)
	})
	t.Run("onDBFailure", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Simulate DB failure by closing the connection
		sqlDB, err := store.db.GORM().DB()
		assert.NoError(tt, err)
		_ = sqlDB.Close()

		count, err := store.BackupCountByVolumeID(context.Background(), "volume1")
		assert.Error(tt, err)
		assert.Equal(tt, int64(0), count)
	})
}

func TestFetchScheduledBackupsForDeletion(t *testing.T) {
	t.Run("onSuccess", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		DailyBackup1 := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{
				UUID:      "test-backup-uuid1",
				CreatedAt: getTimeNow().Add(-2 * time.Second), // 2 days ago
			},
			Attributes: &datamodel.BackupAttributes{
				SnapshotID: "test-snapshot-id-1",
			},
			Name:        "Daily-backup1",
			ScheduleTag: nillable.ToPointer(Daily),
			Type:        BackupTypeScheduled,
			VolumeUUID:  "volume-uuid-1",
		}
		DailyBackup2 := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{
				UUID:      "test-backup-uuid2",
				CreatedAt: getTimeNow(),
			},
			Attributes: &datamodel.BackupAttributes{
				SnapshotID: "test-snapshot-id-2",
			},
			Name:        "Daily-backup2",
			ScheduleTag: nillable.ToPointer(Daily),
			Type:        BackupTypeScheduled,
			VolumeUUID:  "volume-uuid-1",
		}
		err = store.db.Create(DailyBackup1).Error()
		assert.NoError(tt, err)
		err = store.db.Create(DailyBackup2).Error()
		assert.NoError(tt, err)
		WeeklyBackup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{UUID: "test-backup-uuid3"},
			Attributes: &datamodel.BackupAttributes{
				SnapshotID: "test-snapshot-id-2",
			},
			Name:        "Weekly-backup1",
			ScheduleTag: nillable.ToPointer(Weekly),
			Type:        BackupTypeScheduled,
			VolumeUUID:  "volume-uuid-1",
		}
		err = store.db.Create(WeeklyBackup).Error()
		assert.NoError(tt, err)
		MonthlyBackup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{UUID: "test-backup-uuid4"},
			Attributes: &datamodel.BackupAttributes{
				SnapshotID: "test-snapshot-id-3",
			},
			Name:        "Monthly-backup1",
			ScheduleTag: nillable.ToPointer(Monthly),
			Type:        BackupTypeScheduled,
			VolumeUUID:  "volume-uuid-1",
		}
		err = store.db.Create(MonthlyBackup).Error()
		assert.NoError(tt, err)

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid-1",
			},
			Name: "test-volume-1",
			Svm: &datamodel.Svm{
				Name: "test-svm-1",
			},
			Pool: &datamodel.Pool{
				PoolCredentials: &datamodel.PoolCredentials{
					Password: "pool-password",
					SecretID: "pool-credential-secret-id",
				},
				DeploymentName: "test-pool-deployment",
			},
			DataProtection: &datamodel.DataProtection{
				BackupVaultID:  "backup-vault-uuid-1",
				BackupPolicyID: "backup-policy-uuid-1",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				ExternalUUID:   "external-uuid-1",
				VendorSubnetID: "test-vendor-subnet-id",
			},
		}
		backupPolicy := &datamodel.BackupPolicy{
			BaseModel: datamodel.BaseModel{
				UUID: "backup-policy-uuid-1",
			},
			DailyBackupsToKeep:   1,
			WeeklyBackupsToKeep:  1,
			MonthlyBackupsToKeep: 1,
		}

		resultBackups, err := store.FetchScheduledBackupsForDeletion(context.Background(), volume, backupPolicy)
		assert.NoError(tt, err)
		assert.Len(tt, resultBackups, 1)
		assert.Equal(tt, DailyBackup1.UUID, resultBackups[0].UUID)
	})
	t.Run("whenBackupPolicyIDIsEmpty", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		DailyBackup1 := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{
				UUID:      "test-backup-uuid1",
				CreatedAt: getTimeNow().Add(-2 * time.Second),
			},
			Attributes: &datamodel.BackupAttributes{
				SnapshotID: "test-snapshot-id-1",
			},
			Name:        "Daily-backup1",
			ScheduleTag: nillable.ToPointer(Daily),
			Type:        BackupTypeScheduled,
			VolumeUUID:  "volume-uuid-1",
		}
		err = store.db.Create(DailyBackup1).Error()
		assert.NoError(tt, err)

		resultBackups, err := store.FetchScheduledBackupsForDeletion(context.Background(), &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: "volume-uuid-1"}}, nil)
		assert.Nil(tt, resultBackups)
		assert.NotNil(tt, err)
		assert.EqualError(tt, err, "volume does not have a backup policy associated with it")
	})
}

func TestIsBackupShared(t *testing.T) {
	t.Run("onSuccess", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		DailyBackup1 := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{
				UUID:      "test-backup-uuid1",
				CreatedAt: getTimeNow().Add(-2 * time.Second), // 2 days ago
			},
			Attributes: &datamodel.BackupAttributes{
				SnapshotID: "test-snapshot-id-1",
			},
			Name:        "Daily-backup1",
			ScheduleTag: nillable.ToPointer(Daily),
			Type:        BackupTypeScheduled,
			VolumeUUID:  "volume-uuid-1",
		}
		DailyBackup2 := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{
				UUID:      "test-backup-uuid2",
				CreatedAt: getTimeNow(),
			},
			Attributes: &datamodel.BackupAttributes{
				SnapshotID: "test-snapshot-id-1",
			},
			Name:        "Daily-backup2",
			ScheduleTag: nillable.ToPointer(Daily),
			Type:        BackupTypeScheduled,
			VolumeUUID:  "volume-uuid-1",
		}
		err = store.db.Create(DailyBackup1).Error()
		assert.NoError(tt, err)
		err = store.db.Create(DailyBackup2).Error()
		assert.NoError(tt, err)

		shared, err := store.IsBackupShared(context.Background(), DailyBackup1)
		assert.Nil(tt, err)
		assert.True(tt, shared)
	})
}

func TestBackupCountByBackupVaultIDReturnsCorrectCount(tt *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(tt, err)

	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	assert.NoError(tt, err)

	backup1 := &datamodel.Backup{
		BaseModel:     datamodel.BaseModel{UUID: "test-backup-uuid1"},
		BackupVaultID: 1,
	}
	backup2 := &datamodel.Backup{
		BaseModel:     datamodel.BaseModel{UUID: "test-backup-uuid2"},
		BackupVaultID: 1,
	}
	err = store.db.Create(backup1).Error()
	assert.NoError(tt, err)
	err = store.db.Create(backup2).Error()
	assert.NoError(tt, err)

	count, err := store.GetBackupCountByBackupVaultID(context.Background(), 1)
	assert.NoError(tt, err)
	assert.Equal(tt, int64(2), count)
}

func TestBackupCountByBackupVaultIDReturnsZeroWhenNoBackups(tt *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(tt, err)

	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	assert.NoError(tt, err)

	count, err := store.GetBackupCountByBackupVaultID(context.Background(), 1)
	assert.NoError(tt, err)
	assert.Equal(tt, int64(0), count)
}

func TestBackupCountByBackupVaultIDReturnsErrorOnDBFailure(tt *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(tt, err)

	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	assert.NoError(tt, err)

	// Simulate DB failure by closing the connection
	sqlDB, err := store.db.GORM().DB()
	assert.NoError(tt, err)
	_ = sqlDB.Close()

	count, err := store.GetBackupCountByBackupVaultID(context.Background(), 1)
	assert.Error(tt, err)
	assert.Equal(tt, int64(0), count)
}

func TestReturnsVolumeCountSuccessfully(tt *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(tt, err)

	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	assert.NoError(tt, err)

	volume := &datamodel.Volume{
		BaseModel:      datamodel.BaseModel{UUID: "test-volume-uuid"},
		DataProtection: &datamodel.DataProtection{BackupVaultID: "test-backup-vault-uuid"},
	}
	err = store.db.Create(volume).Error()
	assert.NoError(tt, err)

	count, err := store.GetVolumeCountByBackupVaultID(context.Background(), "test-backup-vault-uuid")
	assert.NoError(tt, err)
	assert.Equal(tt, int64(1), count)
}

func TestReturnsZeroWhenNoVolumesAssociated(tt *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(tt, err)

	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	assert.NoError(tt, err)

	count, err := store.GetVolumeCountByBackupVaultID(context.Background(), "non-existent-backup-vault-uuid")
	assert.NoError(tt, err)
	assert.Equal(tt, int64(0), count)
}

func TestReturnsErrorOnDBFailure(tt *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(tt, err)

	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	assert.NoError(tt, err)

	sqlDB, err := store.db.GORM().DB()
	assert.NoError(tt, err)
	_ = sqlDB.Close()

	count, err := store.GetVolumeCountByBackupVaultID(context.Background(), "test-backup-vault-uuid")
	assert.Error(tt, err)
	assert.Equal(tt, int64(0), count)
}

func TestGetBackupCountByVolumeUUIDs(t *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(t, err)

	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	assert.NoError(t, err)

	// Create backups for two volumes
	backup1 := &datamodel.Backup{
		BaseModel:  datamodel.BaseModel{UUID: "backup-uuid-1"},
		VolumeUUID: "volume-uuid-1",
		State:      models.LifeCycleStateAvailable,
	}
	backup2 := &datamodel.Backup{
		BaseModel:  datamodel.BaseModel{UUID: "backup-uuid-2"},
		VolumeUUID: "volume-uuid-1",
		State:      models.LifeCycleStateAvailable,
	}
	backup3 := &datamodel.Backup{
		BaseModel:  datamodel.BaseModel{UUID: "backup-uuid-3"},
		VolumeUUID: "volume-uuid-2",
		State:      models.LifeCycleStateAvailable,
	}
	err = store.db.Create(backup1).Error()
	assert.NoError(t, err)
	err = store.db.Create(backup2).Error()
	assert.NoError(t, err)
	err = store.db.Create(backup3).Error()
	assert.NoError(t, err)

	// Test: returns correct counts
	volumeUUIDs := []string{"volume-uuid-1", "volume-uuid-2"}
	counts, err := store.GetBackupCountByVolumeUUIDs(context.Background(), volumeUUIDs, nil)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), counts["volume-uuid-1"])
	assert.Equal(t, int64(1), counts["volume-uuid-2"])

	// Test: returns zero for volume with no backups
	volumeUUIDs = []string{"volume-uuid-3"}
	counts, err = store.GetBackupCountByVolumeUUIDs(context.Background(), volumeUUIDs, nil)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), counts["volume-uuid-3"])

	// Test: returns error on DB failure
	sqlDB, err := store.db.GORM().DB()
	assert.NoError(t, err)
	_ = sqlDB.Close()
	_, err = store.GetBackupCountByVolumeUUIDs(context.Background(), []string{"volume-uuid-1"}, nil)
	assert.Error(t, err)
}
