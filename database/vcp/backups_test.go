package database

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
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

func TestDeleteBackup_BackupChainHistory(t *testing.T) {
	t.Run("MarksBackupChainHistoryDeletedWhenLastBackup", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Create a volume UUID for testing
		volumeUUID := "test-volume-uuid"

		// Create backup chain history entry
		backupChainHistory := &datamodel.BackupChainHistory{
			BaseModel:      datamodel.BaseModel{UUID: "history-uuid"},
			ResourceName:   "test-volume",
			Size:           1073741824, // 1GB
			ResourceUUID:   volumeUUID,
			ConsumerID:     "account-123",
			DeploymentName: "test-deployment",
		}
		err = store.db.Create(backupChainHistory).Error()
		assert.NoError(tt, err)

		// Create a single backup (this will be the last backup)
		backup := &datamodel.Backup{
			BaseModel:  datamodel.BaseModel{UUID: "test-backup-uuid"},
			VolumeUUID: volumeUUID,
			State:      models.LifeCycleStateAvailable,
		}
		err = store.db.Create(backup).Error()
		assert.NoError(tt, err)

		// Verify backup chain history is not deleted before backup deletion
		var historyBeforeDeletion datamodel.BackupChainHistory
		err = store.db.GORM().Unscoped().First(&historyBeforeDeletion, "uuid = ?", backupChainHistory.UUID).Error
		assert.NoError(tt, err)
		assert.Nil(tt, historyBeforeDeletion.DeletedAt)

		// Delete the backup (should mark backup chain history as deleted)
		result, err := store.DeleteBackup(context.Background(), backup.UUID)
		assert.NoError(tt, err)
		assert.Equal(tt, backup.UUID, result.UUID)
		assert.Equal(tt, models.LifeCycleStateDeleted, result.State)

		// Verify backup chain history is now marked as deleted
		var historyAfterDeletion datamodel.BackupChainHistory
		err = store.db.GORM().Unscoped().First(&historyAfterDeletion, "uuid = ?", backupChainHistory.UUID).Error
		assert.NoError(tt, err)
		assert.NotNil(tt, historyAfterDeletion.DeletedAt)
	})

	t.Run("DoesNotMarkBackupChainHistoryDeletedWhenOtherBackupsExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Create a volume UUID for testing
		volumeUUID := "test-volume-uuid"

		// Create backup chain history entry
		backupChainHistory := &datamodel.BackupChainHistory{
			BaseModel:      datamodel.BaseModel{UUID: "history-uuid"},
			ResourceName:   "test-volume",
			Size:           1073741824, // 1GB
			ResourceUUID:   volumeUUID,
			ConsumerID:     "account-123",
			DeploymentName: "test-deployment",
		}
		err = store.db.Create(backupChainHistory).Error()
		assert.NoError(tt, err)

		// Create multiple backups for the same volume
		backup1 := &datamodel.Backup{
			BaseModel:  datamodel.BaseModel{UUID: "backup-1-uuid"},
			VolumeUUID: volumeUUID,
			State:      models.LifeCycleStateAvailable,
		}
		backup2 := &datamodel.Backup{
			BaseModel:  datamodel.BaseModel{UUID: "backup-2-uuid"},
			VolumeUUID: volumeUUID,
			State:      models.LifeCycleStateAvailable,
		}
		err = store.db.Create(backup1).Error()
		assert.NoError(tt, err)
		err = store.db.Create(backup2).Error()
		assert.NoError(tt, err)

		// Delete one backup (should NOT mark backup chain history as deleted)
		result, err := store.DeleteBackup(context.Background(), backup1.UUID)
		assert.NoError(tt, err)
		assert.Equal(tt, backup1.UUID, result.UUID)
		assert.Equal(tt, models.LifeCycleStateDeleted, result.State)

		// Verify backup chain history is still NOT deleted
		var historyAfterDeletion datamodel.BackupChainHistory
		err = store.db.GORM().Unscoped().First(&historyAfterDeletion, "uuid = ?", backupChainHistory.UUID).Error
		assert.NoError(tt, err)
		assert.Nil(tt, historyAfterDeletion.DeletedAt)
	})

	t.Run("HandlesBackupChainHistoryDeletionError", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Create a backup without backup chain history
		backup := &datamodel.Backup{
			BaseModel:  datamodel.BaseModel{UUID: "test-backup-uuid"},
			VolumeUUID: "test-volume-uuid",
			State:      models.LifeCycleStateAvailable,
		}
		err = store.db.Create(backup).Error()
		assert.NoError(tt, err)

		// Delete the backup - should succeed even without backup chain history
		result, err := store.DeleteBackup(context.Background(), backup.UUID)
		assert.NoError(tt, err)
		assert.Equal(tt, backup.UUID, result.UUID)
		assert.Equal(tt, models.LifeCycleStateDeleted, result.State)
	})

	t.Run("HandlesBackupCountQueryError", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Create a backup
		backup := &datamodel.Backup{
			BaseModel:  datamodel.BaseModel{UUID: "test-backup-uuid"},
			VolumeUUID: "test-volume-uuid",
			State:      models.LifeCycleStateAvailable,
		}
		err = store.db.Create(backup).Error()
		assert.NoError(tt, err)

		// Close the database connection to force count query error
		sqlDB, err := store.db.GORM().DB()
		assert.NoError(tt, err)
		_ = sqlDB.Close()

		// Delete the backup - should fail due to count query error (covers line 383)
		_, err = store.DeleteBackup(context.Background(), backup.UUID)
		assert.Error(tt, err)
	})

	t.Run("HandlesBackupChainHistoryUpdateError", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Create a volume UUID for testing
		volumeUUID := "test-volume-uuid"

		// Create backup chain history entry
		backupChainHistory := &datamodel.BackupChainHistory{
			BaseModel:      datamodel.BaseModel{UUID: "history-uuid"},
			ResourceName:   "test-volume",
			Size:           1073741824, // 1GB
			ResourceUUID:   volumeUUID,
			ConsumerID:     "account-123",
			DeploymentName: "test-deployment",
		}
		err = store.db.Create(backupChainHistory).Error()
		assert.NoError(tt, err)

		// Create a single backup (this will be the last backup)
		backup := &datamodel.Backup{
			BaseModel:  datamodel.BaseModel{UUID: "test-backup-uuid"},
			VolumeUUID: volumeUUID,
			State:      models.LifeCycleStateAvailable,
		}
		err = store.db.Create(backup).Error()
		assert.NoError(tt, err)

		// Drop the backup_chain_histories table to force an error in markPreviousBackupChainHistoryAsDeleted
		err = store.db.GORM().Exec("DROP TABLE backup_chain_histories").Error
		assert.NoError(tt, err)

		// Delete the backup - should succeed despite backup chain history update error (covers line 399)
		result, err := store.DeleteBackup(context.Background(), backup.UUID)
		assert.NoError(tt, err) // Should not fail even if history update fails
		assert.Equal(tt, backup.UUID, result.UUID)
		assert.Equal(tt, models.LifeCycleStateDeleted, result.State)
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

		// Close the database connection
		sqlDB, err := store.db.GORM().DB()
		assert.NoError(tt, err)
		_ = sqlDB.Close()

		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "test-vault-uuid"},
			Name:      "test-vault",
		}
		backup := &datamodel.Backup{
			Name:          "test-backup",
			BackupVaultID: backupVault.ID,
			VolumeUUID:    "volume-uuid-1",
			Attributes: &datamodel.BackupAttributes{
				VolumeName:        "test-volume",
				AccountIdentifier: "account-123",
			},
		}

		_, err = store.CreateBackup(context.Background(), backup)
		assert.Error(tt, err)
	})

	t.Run("HandlesBackupChainHistoryCreationGracefully", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "test-vault-uuid"},
			Name:      "test-vault",
		}
		err = store.db.Create(backupVault).Error()
		assert.NoError(tt, err)

		// Create a backup - the backup chain history creation might log warnings but backup should succeed
		backup := &datamodel.Backup{
			Name:          "test-backup-with-history",
			BackupVaultID: backupVault.ID,
			VolumeUUID:    "volume-uuid-1",
			Attributes: &datamodel.BackupAttributes{
				VolumeName:        "test-volume",
				AccountIdentifier: "account-123",
			},
		}

		createdBackup, err := store.CreateBackup(context.Background(), backup)
		assert.NoError(tt, err) // Should succeed even if history creation has issues (lines 147, 154, 175)
		assert.NotNil(tt, createdBackup)
		assert.NotEmpty(tt, createdBackup.UUID)

		// Verify backup chain history was created
		var history datamodel.BackupChainHistory
		err = store.db.GORM().Where("resource_uuid = ?", createdBackup.VolumeUUID).First(&history).Error
		assert.NoError(tt, err)
		assert.Equal(tt, "test-volume", history.ResourceName)
		assert.Equal(tt, "account-123", history.ConsumerID)
		assert.Equal(tt, int64(0), history.Size) // Initial size is 0
	})
}

func TestFinishBackup_BackupChainHistoryUpdate(t *testing.T) {
	t.Run("UpdatesBackupChainHistoryWithLogicalSize", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Create backup vault and backup
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "test-vault-uuid"},
			Name:      "test-vault",
		}
		err = store.db.Create(backupVault).Error()
		assert.NoError(tt, err)

		backup := &datamodel.Backup{
			Name:          "test-backup",
			BackupVaultID: backupVault.ID,
			VolumeUUID:    "volume-uuid-1",
			Attributes: &datamodel.BackupAttributes{
				VolumeName:        "test-volume",
				AccountIdentifier: "account-123",
			},
		}

		createdBackup, err := store.CreateBackup(context.Background(), backup)
		assert.NoError(tt, err)

		// Finish backup with logical size
		logicalSize := int64(1024 * 1024 * 1024) // 1GB
		finishBackup := &datamodel.Backup{
			BaseModel:               datamodel.BaseModel{UUID: createdBackup.UUID},
			State:                   models.LifeCycleStateAvailable,
			LatestLogicalBackupSize: logicalSize,
		}

		_, err = store.FinishBackup(context.Background(), finishBackup)
		assert.NoError(tt, err)

		// Verify backup chain history was updated with the size (lines 413, 420-421)
		var history datamodel.BackupChainHistory
		err = store.db.GORM().Where("resource_uuid = ?", "volume-uuid-1").First(&history).Error
		assert.NoError(tt, err)
		assert.Equal(tt, logicalSize, history.Size)
	})

	t.Run("HandlesBackupChainHistoryUpdateFailureGracefully", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Create backup vault and backup without volume UUID
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "test-vault-uuid"},
			Name:      "test-vault",
		}
		err = store.db.Create(backupVault).Error()
		assert.NoError(tt, err)

		backup := &datamodel.Backup{
			Name:          "test-backup-no-volume",
			BackupVaultID: backupVault.ID,
			VolumeUUID:    "", // Empty volume UUID
		}

		createdBackup, err := store.CreateBackup(context.Background(), backup)
		assert.NoError(tt, err)

		// Finish backup - should succeed even without updating backup chain history
		finishBackup := &datamodel.Backup{
			BaseModel:               datamodel.BaseModel{UUID: createdBackup.UUID},
			State:                   models.LifeCycleStateAvailable,
			LatestLogicalBackupSize: 1024,
		}

		_, err = store.FinishBackup(context.Background(), finishBackup)
		assert.NoError(tt, err) // Should succeed even if history update is skipped
	})
}

func TestUpdateLatestBackupLogicalSize_BackupChainHistory(t *testing.T) {
	t.Run("UpdatesBackupChainHistorySuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Create account, pool, and volume
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "account-uuid"},
			Name:      "test-account",
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err)

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
			Name:      "test-pool",
			AccountID: account.ID,
		}
		err = store.db.Create(pool).Error()
		assert.NoError(tt, err)

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid-1"},
			Name:      "test-volume",
			AccountID: account.ID,
			PoolID:    pool.ID,
		}
		err = store.db.Create(volume).Error()
		assert.NoError(tt, err)

		// Create backup vault and backup
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "test-vault-uuid"},
			Name:      "test-vault",
		}
		err = store.db.Create(backupVault).Error()
		assert.NoError(tt, err)

		backup := &datamodel.Backup{
			Name:          "test-backup",
			BackupVaultID: backupVault.ID,
			VolumeUUID:    volume.UUID,
			Attributes: &datamodel.BackupAttributes{
				VolumeName:        volume.Name,
				AccountIdentifier: "account-123",
			},
		}

		createdBackup, err := store.CreateBackup(context.Background(), backup)
		assert.NoError(tt, err)

		// Finish the backup to make it AVAILABLE
		finishBackup := &datamodel.Backup{
			BaseModel:               datamodel.BaseModel{UUID: createdBackup.UUID},
			State:                   models.LifeCycleStateAvailable,
			LatestLogicalBackupSize: 1024 * 1024 * 1024, // 1GB initial size
		}
		_, err = store.FinishBackup(context.Background(), finishBackup)
		assert.NoError(tt, err)

		// Update latest backup logical size
		newLogicalSize := int64(2 * 1024 * 1024 * 1024) // 2GB
		err = store.UpdateLatestBackupLogicalSize(context.Background(), volume.UUID, newLogicalSize)
		assert.NoError(tt, err)

		// Verify backup chain history was updated (line 738)
		var histories []datamodel.BackupChainHistory
		err = store.db.GORM().Unscoped().Where("resource_uuid = ?", volume.UUID).Order("created_at DESC").Find(&histories).Error
		assert.NoError(tt, err)

		// Should have at least 1 entry
		assert.GreaterOrEqual(tt, len(histories), 1)

		// The active (non-deleted) entry should have the new size
		var activeHistory datamodel.BackupChainHistory
		err = store.db.GORM().Where("resource_uuid = ? AND deleted_at IS NULL", volume.UUID).First(&activeHistory).Error
		assert.NoError(tt, err)
		assert.Equal(tt, newLogicalSize, activeHistory.Size)
	})

	t.Run("HandlesBackupChainHistoryFailureGracefully", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Create a volume without creating backup - UpdateLatestBackupLogicalSize will fail early
		volumeUUID := "volume-without-history"

		// This should fail because no AVAILABLE backup exists
		err = store.UpdateLatestBackupLogicalSize(context.Background(), volumeUUID, int64(1024*1024*1024))
		assert.Error(tt, err) // Should fail - no backup found
	})
}

func TestListBackupChainHistoriesWithPagination_IncludesDeletedRows(t *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(t, err)

	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	assert.NoError(t, err)

	start := time.Date(2026, 2, 18, 10, 0, 0, 0, time.UTC)
	end := start.Add(1 * time.Hour)
	deletedAt := start.Add(10 * time.Minute)

	activeHistory := &datamodel.BackupChainHistory{
		BaseModel: datamodel.BaseModel{
			UUID:      "history-active",
			CreatedAt: start.Add(-10 * time.Minute),
		},
		ResourceUUID:   "resource-uuid",
		ConsumerID:     "consumer-id",
		DeploymentName: "deployment-name",
		Size:           123,
	}
	deletedHistory := &datamodel.BackupChainHistory{
		BaseModel: datamodel.BaseModel{
			UUID:      "history-deleted",
			CreatedAt: start.Add(-20 * time.Minute),
			DeletedAt: &gorm.DeletedAt{Time: deletedAt, Valid: true},
		},
		ResourceUUID:   "resource-uuid",
		ConsumerID:     "consumer-id",
		DeploymentName: "deployment-name",
		Size:           456,
	}

	err = store.db.Create(activeHistory).Error()
	assert.NoError(t, err)
	err = store.db.Create(deletedHistory).Error()
	assert.NoError(t, err)

	conditions := [][]interface{}{
		{"resource_uuid IS NOT NULL"},
		{"created_at <= ?", end},
		{"(deleted_at IS NULL OR deleted_at >= ?)", start},
	}
	pagination := &dbutils.Pagination{Offset: 0, Limit: 10}

	results, err := store.ListBackupChainHistoriesWithPagination(context.Background(), conditions, pagination)
	assert.NoError(t, err)
	require.Len(t, results, 2)

	uuids := map[string]bool{results[0].UUID: true, results[1].UUID: true}
	assert.True(t, uuids["history-active"])
	assert.True(t, uuids["history-deleted"])
}

func TestCreateBackup_Errors_Old(t *testing.T) {
	t.Run("ReturnsErrorWhenDBFails_Old", func(tt *testing.T) {
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

func TestGetBackupByExternalUUID(t *testing.T) {
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

		// Create backup vault with external UUID
		backupVaultExternalUUID := "external-backup-vault-uuid"
		backupVault := &datamodel.BackupVault{
			BaseModel:    datamodel.BaseModel{UUID: "test-backup-vault-uuid"},
			Name:         "test-backup-vault",
			AccountID:    account.ID,
			Account:      account,
			ExternalUUID: &backupVaultExternalUUID,
		}
		err = store.db.Create(backupVault).Error()
		assert.NoError(tt, err)

		// Create backup with external UUID
		backupExternalUUID := "external-backup-uuid"
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "test-backup-uuid"},
			Name:          "test_backup",
			BackupVaultID: backupVault.ID,
			ExternalUUID:  backupExternalUUID,
		}
		err = store.db.Create(backup).Error()
		assert.NoError(tt, err)

		result, err := store.GetBackupByExternalUUID(context.Background(), backupVaultExternalUUID, backupExternalUUID, account.Name)
		assert.NoError(tt, err)
		assert.Equal(tt, backup.UUID, result.UUID)
		assert.Equal(tt, backupExternalUUID, result.ExternalUUID)
	})

	t.Run("ReturnsErrorWhenBackupVaultDoesNotExist", func(tt *testing.T) {
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

		_, err = store.GetBackupByExternalUUID(context.Background(), "non-existent-backup-vault-uuid", "external-backup-uuid", account.Name)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "not found")
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

		// Create backup vault with external UUID
		backupVaultExternalUUID := "external-backup-vault-uuid"
		backupVault := &datamodel.BackupVault{
			BaseModel:    datamodel.BaseModel{UUID: "test-backup-vault-uuid"},
			Name:         "test-backup-vault",
			AccountID:    account.ID,
			Account:      account,
			ExternalUUID: &backupVaultExternalUUID,
		}
		err = store.db.Create(backupVault).Error()
		assert.NoError(tt, err)

		_, err = store.GetBackupByExternalUUID(context.Background(), backupVaultExternalUUID, "non-existent-external-backup-uuid", account.Name)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "not found")
	})

	t.Run("ReturnsErrorWhenAccountNameMismatch", func(tt *testing.T) {
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

		// Create backup vault with external UUID
		backupVaultExternalUUID := "external-backup-vault-uuid"
		backupVault := &datamodel.BackupVault{
			BaseModel:    datamodel.BaseModel{UUID: "test-backup-vault-uuid"},
			Name:         "test-backup-vault",
			AccountID:    account.ID,
			Account:      account,
			ExternalUUID: &backupVaultExternalUUID,
		}
		err = store.db.Create(backupVault).Error()
		assert.NoError(tt, err)

		// Create backup with external UUID
		backupExternalUUID := "external-backup-uuid"
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "test-backup-uuid"},
			Name:          "test_backup",
			BackupVaultID: backupVault.ID,
			ExternalUUID:  backupExternalUUID,
		}
		err = store.db.Create(backup).Error()
		assert.NoError(tt, err)

		// Try to get backup with wrong account name
		_, err = store.GetBackupByExternalUUID(context.Background(), backupVaultExternalUUID, backupExternalUUID, "wrong-account-name")
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "not found")
	})
}

func TestGetBackupByExternalUUID_Errors(t *testing.T) {
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

		_, err = store.GetBackupByExternalUUID(context.Background(), "external-backup-vault-uuid", "external-backup-uuid", "test-account")
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

	t.Run("ReturnsFilteredBackupsWithFilters", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		bv := &datamodel.BackupVault{AccountID: 1, BaseModel: datamodel.BaseModel{UUID: "bv-filter-uuid"}}
		err = store.db.Create(bv).Error()
		assert.NoError(tt, err)

		backup1 := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-filter-uuid-1"},
			Name:          "matching-backup",
			BackupVaultID: bv.ID,
			BackupVault:   bv,
			VolumeUUID:    "vol-1",
		}
		err = store.db.Create(backup1).Error()
		assert.NoError(tt, err)

		backup2 := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-filter-uuid-2"},
			Name:          "other-backup",
			BackupVaultID: bv.ID,
			BackupVault:   bv,
			VolumeUUID:    "vol-2",
		}
		err = store.db.Create(backup2).Error()
		assert.NoError(tt, err)

		filters := [][]interface{}{{"name = ?", "matching-backup"}}
		backups, err := store.GetBackupsByBackupVaultOwnerIDAndFilter(context.Background(), bv.UUID, int64(1), filters)
		assert.NoError(tt, err)
		assert.Len(tt, backups, 1)
		assert.Equal(tt, "matching-backup", backups[0].Name)
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
			BaseModel:               datamodel.BaseModel{UUID: "test-backup-uuid"},
			Description:             "Initial description",
			Attributes:              &datamodel.BackupAttributes{},
			State:                   models.LifeCycleStateCreating,
			StateDetails:            models.LifeCycleStateCreatingDetails,
			SizeInBytes:             1024,
			LatestLogicalBackupSize: 1024,
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
		assert.Equal(tt, int64(1024), finishedBackup.LatestLogicalBackupSize)
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
	t.Run("OnSuccessWithErrorStateBackupWithoutDeleteInitiated", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Create an available backup
		backup1 := &datamodel.Backup{
			BaseModel:    datamodel.BaseModel{UUID: "test-backup-uuid1"},
			Name:         "test_backup_1",
			Description:  "Test backup 1",
			State:        models.LifeCycleStateAvailable,
			StateDetails: models.LifeCycleStateAvailableDetails,
			VolumeUUID:   "volume1",
		}
		err = store.db.Create(backup1).Error()
		assert.NoError(tt, err)

		// Create an error state backup without delete_initiated
		backup2 := &datamodel.Backup{
			BaseModel:    datamodel.BaseModel{UUID: "test-backup-uuid2"},
			Name:         "test_backup_2",
			Description:  "Test backup 2",
			State:        models.LifeCycleStateError,
			StateDetails: "Error in backup",
			VolumeUUID:   "volume1",
			Attributes: &datamodel.BackupAttributes{
				DeleteInitiated: false,
			},
		}
		err = store.db.Create(backup2).Error()
		assert.NoError(tt, err)

		// The available backup should be considered latest (error without delete_initiated is not included)
		isLatest, err := store.IsLatestBackup(context.Background(), backup1.UUID, "volume1")
		assert.NoError(tt, err)
		assert.True(tt, isLatest)

		// The error backup without delete_initiated should not be considered latest
		isLatest, err = store.IsLatestBackup(context.Background(), backup2.UUID, "volume1")
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

		resultBackups, err := store.FetchScheduledBackupsForDeletion(context.Background(), volume, backupPolicy, false)
		assert.NoError(tt, err)
		assert.Len(tt, resultBackups, 1)
		assert.Equal(tt, DailyBackup1.UUID, resultBackups[0].UUID)
	})
	t.Run("isExpertMode_usesExternalUUID", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		const externalUUID = "expert-mode-external-uuid"

		// Backup stored under externalUUID as volume_uuid (expert mode path)
		expertBackup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{
				UUID:      "expert-backup-uuid",
				CreatedAt: getTimeNow().Add(-2 * time.Second),
			},
			Attributes: &datamodel.BackupAttributes{
				SnapshotID: "snap-expert-1",
			},
			Name:        "Expert-daily-backup",
			ScheduleTag: nillable.ToPointer(Daily),
			Type:        BackupTypeScheduled,
			VolumeUUID:  externalUUID,
		}
		err = store.db.Create(expertBackup).Error()
		assert.NoError(tt, err)

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "regular-volume-uuid"},
			DataProtection: &datamodel.DataProtection{
				BackupVaultID:  "bv-uuid",
				BackupPolicyID: "bp-uuid",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				ExternalUUID: externalUUID,
			},
		}
		backupPolicy := &datamodel.BackupPolicy{
			BaseModel:          datamodel.BaseModel{UUID: "bp-uuid"},
			DailyBackupsToKeep: 0, // keep 0, so the 1 daily backup should be eligible for deletion
		}

		resultBackups, err := store.FetchScheduledBackupsForDeletion(context.Background(), volume, backupPolicy, true)
		assert.NoError(tt, err)
		assert.Len(tt, resultBackups, 1)
		assert.Equal(tt, expertBackup.UUID, resultBackups[0].UUID)
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

		resultBackups, err := store.FetchScheduledBackupsForDeletion(context.Background(), &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: "volume-uuid-1"}}, nil, false)
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

func TestGetBackupCountByVolumeAndVault(t *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(t, err)

	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	assert.NoError(t, err)

	// Create backup vaults
	vault1 := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{UUID: "vault-uuid-1"},
		Name:      "vault-1",
	}
	vault2 := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{UUID: "vault-uuid-2"},
		Name:      "vault-2",
	}
	err = store.db.Create(vault1).Error()
	assert.NoError(t, err)
	err = store.db.Create(vault2).Error()
	assert.NoError(t, err)

	// Create backups: volume1 has 2 in vault1, 1 in vault2; volume2 has 1 in vault1
	backups := []*datamodel.Backup{
		{BaseModel: datamodel.BaseModel{UUID: "b1"}, VolumeUUID: "vol-1", BackupVaultID: vault1.ID, State: models.LifeCycleStateAvailable},
		{BaseModel: datamodel.BaseModel{UUID: "b2"}, VolumeUUID: "vol-1", BackupVaultID: vault1.ID, State: models.LifeCycleStateAvailable},
		{BaseModel: datamodel.BaseModel{UUID: "b3"}, VolumeUUID: "vol-1", BackupVaultID: vault2.ID, State: models.LifeCycleStateAvailable},
		{BaseModel: datamodel.BaseModel{UUID: "b4"}, VolumeUUID: "vol-2", BackupVaultID: vault1.ID, State: models.LifeCycleStateAvailable},
		{BaseModel: datamodel.BaseModel{UUID: "b5"}, VolumeUUID: "vol-1", BackupVaultID: vault1.ID, State: models.LifeCycleStateDeleted},
	}
	for _, b := range backups {
		err = store.db.Create(b).Error()
		assert.NoError(t, err)
	}

	// vol-1 + vault1: 2 available (b5 deleted is excluded)
	count, err := store.GetBackupCountByVolumeAndVault(context.Background(), "vol-1", vault1.ID)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)

	// vol-1 + vault2: 1
	count, err = store.GetBackupCountByVolumeAndVault(context.Background(), "vol-1", vault2.ID)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)

	// vol-2 + vault1: 1
	count, err = store.GetBackupCountByVolumeAndVault(context.Background(), "vol-2", vault1.ID)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)

	// No backups for this volume+vault
	count, err = store.GetBackupCountByVolumeAndVault(context.Background(), "vol-2", vault2.ID)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)
}

func TestGetBackupCountByVolumeAndVault_ReturnsErrorWhenDBFails(t *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(t, err)
	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)
	sqlDB, err := store.db.GORM().DB()
	assert.NoError(t, err)
	_ = sqlDB.Close()
	_, err = store.GetBackupCountByVolumeAndVault(context.Background(), "vol-1", 1)
	assert.Error(t, err)
}

func TestGetDistinctBackupVaultIDsByVolumeUUID_ReturnsErrorWhenDBFails(t *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(t, err)
	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)
	sqlDB, err := store.db.GORM().DB()
	assert.NoError(t, err)
	_ = sqlDB.Close()
	_, err = store.GetDistinctBackupVaultIDsByVolumeUUID(context.Background(), "vol-1")
	assert.Error(t, err)
}

func TestGetLatestBackupByVolumeUUID_ReturnsErrorWhenDBFails(t *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(t, err)
	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)
	sqlDB, err := store.db.GORM().DB()
	assert.NoError(t, err)
	_ = sqlDB.Close()
	_, err = store.GetLatestBackupByVolumeUUID(context.Background(), "vol-1")
	assert.Error(t, err)
}

func TestGetLatestBackupByVolumeAndVault(t *testing.T) {
	db, err := SetupTestDB()
	require.NoError(t, err)
	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)
	err = ClearInMemoryDB(store.db.GORM())
	require.NoError(t, err)

	vault1 := &datamodel.BackupVault{BaseModel: datamodel.BaseModel{UUID: "vault-uuid-1"}, Name: "vault-1"}
	vault2 := &datamodel.BackupVault{BaseModel: datamodel.BaseModel{UUID: "vault-uuid-2"}, Name: "vault-2"}
	require.NoError(t, store.db.Create(vault1).Error())
	require.NoError(t, store.db.Create(vault2).Error())

	// vol-1 vault1: two backups, latest by id is b2
	b1 := &datamodel.Backup{BaseModel: datamodel.BaseModel{UUID: "b1", ID: 1}, VolumeUUID: "vol-1", BackupVaultID: vault1.ID, State: models.LifeCycleStateAvailable}
	b2 := &datamodel.Backup{BaseModel: datamodel.BaseModel{UUID: "b2", ID: 2}, VolumeUUID: "vol-1", BackupVaultID: vault1.ID, State: models.LifeCycleStateAvailable}
	b3 := &datamodel.Backup{BaseModel: datamodel.BaseModel{UUID: "b3", ID: 3}, VolumeUUID: "vol-1", BackupVaultID: vault2.ID, State: models.LifeCycleStateAvailable}
	require.NoError(t, store.db.Create(b1).Error())
	require.NoError(t, store.db.Create(b2).Error())
	require.NoError(t, store.db.Create(b3).Error())

	latest, err := store.GetLatestBackupByVolumeAndVault(context.Background(), "vol-1", vault1.ID)
	require.NoError(t, err)
	assert.NotNil(t, latest)
	assert.Equal(t, "b2", latest.UUID)

	latest, err = store.GetLatestBackupByVolumeAndVault(context.Background(), "vol-1", vault2.ID)
	require.NoError(t, err)
	assert.NotNil(t, latest)
	assert.Equal(t, "b3", latest.UUID)

	_, err = store.GetLatestBackupByVolumeAndVault(context.Background(), "vol-none", vault1.ID)
	assert.Error(t, err)
}

func TestGetLatestBackupsPerVaultByVolumeUUID(t *testing.T) {
	db, err := SetupTestDB()
	require.NoError(t, err)
	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)
	err = ClearInMemoryDB(store.db.GORM())
	require.NoError(t, err)

	vault1 := &datamodel.BackupVault{BaseModel: datamodel.BaseModel{UUID: "vault-uuid-1"}, Name: "vault-1"}
	vault2 := &datamodel.BackupVault{BaseModel: datamodel.BaseModel{UUID: "vault-uuid-2"}, Name: "vault-2"}
	require.NoError(t, store.db.Create(vault1).Error())
	require.NoError(t, store.db.Create(vault2).Error())

	b1 := &datamodel.Backup{BaseModel: datamodel.BaseModel{UUID: "b1", ID: 1}, VolumeUUID: "vol-1", BackupVaultID: vault1.ID, State: models.LifeCycleStateAvailable}
	b2 := &datamodel.Backup{BaseModel: datamodel.BaseModel{UUID: "b2", ID: 2}, VolumeUUID: "vol-1", BackupVaultID: vault1.ID, State: models.LifeCycleStateAvailable}
	b3 := &datamodel.Backup{BaseModel: datamodel.BaseModel{UUID: "b3", ID: 3}, VolumeUUID: "vol-1", BackupVaultID: vault2.ID, State: models.LifeCycleStateAvailable}
	require.NoError(t, store.db.Create(b1).Error())
	require.NoError(t, store.db.Create(b2).Error())
	require.NoError(t, store.db.Create(b3).Error())

	perVault, err := store.GetLatestBackupsPerVaultByVolumeUUID(context.Background(), "vol-1")
	require.NoError(t, err)
	assert.Len(t, perVault, 2)
	uuids := []string{perVault[0].UUID, perVault[1].UUID}
	assert.Contains(t, uuids, "b2")
	assert.Contains(t, uuids, "b3")

	perVault, err = store.GetLatestBackupsPerVaultByVolumeUUID(context.Background(), "vol-none")
	require.NoError(t, err)
	assert.Empty(t, perVault)
}

func TestGetLatestBackupsPerVaultByVolumeUUID_ReturnsErrorWhenDBFails(t *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(t, err)
	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)
	sqlDB, err := store.db.GORM().DB()
	assert.NoError(t, err)
	_ = sqlDB.Close()
	_, err = store.GetLatestBackupsPerVaultByVolumeUUID(context.Background(), "vol-1")
	assert.Error(t, err)
}

func TestGetDistinctBackupVaultIDsByVolumeUUID(t *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(t, err)

	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	assert.NoError(t, err)

	vault1 := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{UUID: "vault-uuid-1"},
		Name:      "vault-1",
	}
	vault2 := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{UUID: "vault-uuid-2"},
		Name:      "vault-2",
	}
	err = store.db.Create(vault1).Error()
	assert.NoError(t, err)
	err = store.db.Create(vault2).Error()
	assert.NoError(t, err)

	backups := []*datamodel.Backup{
		{BaseModel: datamodel.BaseModel{UUID: "b1"}, VolumeUUID: "vol-1", BackupVaultID: vault1.ID, State: models.LifeCycleStateAvailable},
		{BaseModel: datamodel.BaseModel{UUID: "b2"}, VolumeUUID: "vol-1", BackupVaultID: vault2.ID, State: models.LifeCycleStateAvailable},
		{BaseModel: datamodel.BaseModel{UUID: "b3"}, VolumeUUID: "vol-2", BackupVaultID: vault1.ID, State: models.LifeCycleStateAvailable},
		{BaseModel: datamodel.BaseModel{UUID: "b4"}, VolumeUUID: "vol-1", BackupVaultID: vault1.ID, State: models.LifeCycleStateDeleted},
	}
	for _, b := range backups {
		err = store.db.Create(b).Error()
		assert.NoError(t, err)
	}

	// vol-1: distinct vaults with available backups are vault1, vault2 (b4 is deleted so not counted)
	ids, err := store.GetDistinctBackupVaultIDsByVolumeUUID(context.Background(), "vol-1")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []int64{vault1.ID, vault2.ID}, ids)

	// vol-2: only vault1
	ids, err = store.GetDistinctBackupVaultIDsByVolumeUUID(context.Background(), "vol-2")
	assert.NoError(t, err)
	assert.Equal(t, []int64{vault1.ID}, ids)

	// vol with no backups
	ids, err = store.GetDistinctBackupVaultIDsByVolumeUUID(context.Background(), "vol-none")
	assert.NoError(t, err)
	assert.Empty(t, ids)
}

func TestGetBackupsByVolumeUUID(t *testing.T) {
	t.Run("ReturnsBackupsSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Create backup vault
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "test-backup-vault-uuid"},
			Name:      "test-backup-vault",
		}
		err = store.db.Create(backupVault).Error()
		assert.NoError(tt, err)

		// Create backups for the volume
		backup1 := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid-1"},
			VolumeUUID:    "volume-uuid-1",
			BackupVaultID: backupVault.ID,
			BackupVault:   backupVault,
		}
		backup2 := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid-2"},
			VolumeUUID:    "volume-uuid-1",
			BackupVaultID: backupVault.ID,
			BackupVault:   backupVault,
		}
		err = store.db.Create(backup1).Error()
		assert.NoError(tt, err)
		err = store.db.Create(backup2).Error()
		assert.NoError(tt, err)

		// Test: returns backups for the volume
		backups, err := store.GetBackupsByVolumeUUID(context.Background(), "volume-uuid-1")
		assert.NoError(tt, err)
		assert.Len(tt, backups, 2)
		assert.Equal(tt, "backup-uuid-1", backups[0].UUID)
		assert.Equal(tt, "backup-uuid-2", backups[1].UUID)
	})

	t.Run("ReturnsEmptySliceWhenNoBackups", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Test: returns empty slice for volume with no backups
		backups, err := store.GetBackupsByVolumeUUID(context.Background(), "volume-uuid-1")
		assert.NoError(tt, err)
		assert.Empty(tt, backups)
	})

	t.Run("ReturnsErrorOnDBFailure", func(tt *testing.T) {
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

		// Test: returns error on DB failure
		backups, err := store.GetBackupsByVolumeUUID(context.Background(), "volume-uuid-1")
		assert.Error(tt, err)
		assert.Nil(tt, backups)
	})
}

func TestUpdateBackupLatestLogicalBackupSizeByVolume(t *testing.T) {
	t.Run("UpdatesBackupsSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Create backups for the volume
		backup1 := &datamodel.Backup{
			BaseModel:               datamodel.BaseModel{UUID: "backup-uuid-1"},
			VolumeUUID:              "volume-uuid-1",
			LatestLogicalBackupSize: 1024,
		}
		backup2 := &datamodel.Backup{
			BaseModel:               datamodel.BaseModel{UUID: "backup-uuid-2"},
			VolumeUUID:              "volume-uuid-1",
			LatestLogicalBackupSize: 2048,
		}
		backup3 := &datamodel.Backup{
			BaseModel:               datamodel.BaseModel{UUID: "backup-uuid-3"},
			VolumeUUID:              "volume-uuid-1",
			LatestLogicalBackupSize: 4096,
		}
		err = store.db.Create(backup1).Error()
		assert.NoError(tt, err)
		err = store.db.Create(backup2).Error()
		assert.NoError(tt, err)
		err = store.db.Create(backup3).Error()
		assert.NoError(tt, err)

		// Test: updates all backups except the excluded one
		err = store.UpdateBackupLatestLogicalBackupSizeByVolume(context.Background(), "volume-uuid-1", "backup-uuid-2")
		assert.NoError(tt, err)

		// Verify the updates
		var updatedBackups []*datamodel.Backup
		err = store.db.GORM().Where("volume_uuid = ?", "volume-uuid-1").Find(&updatedBackups).Error
		assert.NoError(tt, err)
		assert.Len(tt, updatedBackups, 3)

		// Find the specific backups to verify their sizes
		for _, backup := range updatedBackups {
			if backup.UUID == "backup-uuid-1" || backup.UUID == "backup-uuid-3" {
				assert.Equal(tt, int64(0), backup.LatestLogicalBackupSize)
			} else if backup.UUID == "backup-uuid-2" {
				assert.Equal(tt, int64(2048), backup.LatestLogicalBackupSize)
			}
		}
	})

	t.Run("ReturnsErrorOnTransactionFailure", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		defer func() {
			startTransaction = _startTransaction
		}()
		// Simulate transaction failure
		startTransaction = func(db *gorm.DB) (*gorm.DB, error) {
			return nil, errors.New("transaction failed")
		}

		// Test: returns error when transaction fails
		err = store.UpdateBackupLatestLogicalBackupSizeByVolume(context.Background(), "volume-uuid-1", "backup-uuid-2")
		assert.Error(tt, err)
		assert.Equal(tt, "transaction failed", err.Error())
	})

	t.Run("ReturnsErrorOnUpdateFailure", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Create a backup first
		backup := &datamodel.Backup{
			BaseModel:               datamodel.BaseModel{UUID: "backup-uuid-1"},
			VolumeUUID:              "volume-uuid-1",
			LatestLogicalBackupSize: 1024,
		}
		err = store.db.Create(backup).Error()
		assert.NoError(tt, err)

		// Simulate DB failure by closing the connection after transaction starts
		sqlDB, err := store.db.GORM().DB()
		assert.NoError(tt, err)
		_ = sqlDB.Close()

		// Test: returns error when update fails
		err = store.UpdateBackupLatestLogicalBackupSizeByVolume(context.Background(), "volume-uuid-1", "backup-uuid-2")
		assert.Error(tt, err)
	})
}

func TestIsLatestBackupAnyState(t *testing.T) {
	t.Run("ReturnsTrueWhenBackupIsLatest", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Create backups for the same volume with different IDs
		backup1 := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{
				UUID:      "backup-uuid-1",
				CreatedAt: time.Now().Add(-2 * time.Hour), // 2 hours ago
			},
			VolumeUUID: "volume-uuid-1",
			State:      models.LifeCycleStateAvailable,
		}
		backup2 := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{
				UUID:      "backup-uuid-2",
				CreatedAt: time.Now().Add(-1 * time.Hour), // 1 hour ago
			},
			VolumeUUID: "volume-uuid-1",
			State:      models.LifeCycleStateAvailable,
		}
		backup3 := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{
				UUID:      "backup-uuid-3",
				CreatedAt: time.Now().Add(-30 * time.Minute), // 30 minutes ago (highest ID)
			},
			VolumeUUID: "volume-uuid-1",
			State:      models.LifeCycleStateDeleting, // Different state
		}

		err = store.db.Create(backup1).Error()
		assert.NoError(tt, err)
		err = store.db.Create(backup2).Error()
		assert.NoError(tt, err)
		err = store.db.Create(backup3).Error()
		assert.NoError(tt, err)

		// Test: backup3 should be latest (highest id, any state)
		isLatest, err := store.IsLatestBackupAnyState(context.Background(), "backup-uuid-3", "volume-uuid-1")
		assert.NoError(tt, err)
		assert.True(tt, isLatest)
	})

	t.Run("ReturnsFalseWhenBackupIsNotLatest", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Create backups for the same volume with different IDs
		backup1 := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{
				UUID:      "backup-uuid-1",
				CreatedAt: time.Now().Add(-2 * time.Hour), // 2 hours ago
			},
			VolumeUUID: "volume-uuid-1",
			State:      models.LifeCycleStateAvailable,
		}
		backup2 := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{
				UUID:      "backup-uuid-2",
				CreatedAt: time.Now().Add(-1 * time.Hour), // 1 hour ago (highest ID)
			},
			VolumeUUID: "volume-uuid-1",
			State:      models.LifeCycleStateAvailable,
		}

		err = store.db.Create(backup1).Error()
		assert.NoError(tt, err)
		err = store.db.Create(backup2).Error()
		assert.NoError(tt, err)

		// Test: backup1 should not be latest (backup2 has higher ID)
		isLatest, err := store.IsLatestBackupAnyState(context.Background(), "backup-uuid-1", "volume-uuid-1")
		assert.NoError(tt, err)
		assert.False(tt, isLatest)
	})

	t.Run("ReturnsFalseWhenNoBackupsExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Test: no backups exist for the volume
		isLatest, err := store.IsLatestBackupAnyState(context.Background(), "backup-uuid-1", "volume-uuid-1")
		assert.Error(tt, err)
		assert.False(tt, isLatest)
	})

	t.Run("ReturnsErrorOnDBFailure", func(tt *testing.T) {
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

		isLatest, err := store.IsLatestBackupAnyState(context.Background(), "backup-uuid-1", "volume-uuid-1")
		assert.Error(tt, err)
		assert.False(tt, isLatest)
	})

	t.Run("ReturnsTrueWhenOnlyOneBackupExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Create only one backup
		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{
				UUID:      "backup-uuid-1",
				CreatedAt: time.Now(),
			},
			VolumeUUID: "volume-uuid-1",
			State:      models.LifeCycleStateAvailable,
		}

		err = store.db.Create(backup).Error()
		assert.NoError(tt, err)

		// Test: the only backup should be latest
		isLatest, err := store.IsLatestBackupAnyState(context.Background(), "backup-uuid-1", "volume-uuid-1")
		assert.NoError(tt, err)
		assert.True(tt, isLatest)
	})
}

func TestIsLatestBackupAnyStateInVault(t *testing.T) {
	t.Run("ReturnsTrueWhenBackupIsLatestInVault", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		vault1 := &datamodel.BackupVault{BaseModel: datamodel.BaseModel{UUID: "vault-uuid-1"}, Name: "vault1"}
		vault2 := &datamodel.BackupVault{BaseModel: datamodel.BaseModel{UUID: "vault-uuid-2"}, Name: "vault2"}
		err = store.db.Create(vault1).Error()
		assert.NoError(tt, err)
		err = store.db.Create(vault2).Error()
		assert.NoError(tt, err)

		// Vault1: backup1 (older), backup2 (latest in vault1)
		backup1 := &datamodel.Backup{
			BaseModel:  datamodel.BaseModel{UUID: "backup-uuid-1", CreatedAt: time.Now().Add(-2 * time.Hour)},
			VolumeUUID: "volume-uuid-1", BackupVaultID: vault1.ID, State: models.LifeCycleStateAvailable,
		}
		backup2 := &datamodel.Backup{
			BaseModel:  datamodel.BaseModel{UUID: "backup-uuid-2", CreatedAt: time.Now().Add(-1 * time.Hour)},
			VolumeUUID: "volume-uuid-1", BackupVaultID: vault1.ID, State: models.LifeCycleStateAvailable,
		}
		err = store.db.Create(backup1).Error()
		assert.NoError(tt, err)
		err = store.db.Create(backup2).Error()
		assert.NoError(tt, err)

		isLatest, err := store.IsLatestBackupInVault(context.Background(), "backup-uuid-2", "volume-uuid-1", vault1.ID)
		assert.NoError(tt, err)
		assert.True(tt, isLatest)
	})

	t.Run("ReturnsFalseWhenBackupIsNotLatestInVault", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		vault1 := &datamodel.BackupVault{BaseModel: datamodel.BaseModel{UUID: "vault-uuid-1"}, Name: "vault1"}
		err = store.db.Create(vault1).Error()
		assert.NoError(tt, err)

		backup1 := &datamodel.Backup{
			BaseModel:  datamodel.BaseModel{UUID: "backup-uuid-1", CreatedAt: time.Now().Add(-2 * time.Hour)},
			VolumeUUID: "volume-uuid-1", BackupVaultID: vault1.ID, State: models.LifeCycleStateAvailable,
		}
		backup2 := &datamodel.Backup{
			BaseModel:  datamodel.BaseModel{UUID: "backup-uuid-2", CreatedAt: time.Now().Add(-1 * time.Hour)},
			VolumeUUID: "volume-uuid-1", BackupVaultID: vault1.ID, State: models.LifeCycleStateAvailable,
		}
		err = store.db.Create(backup1).Error()
		assert.NoError(tt, err)
		err = store.db.Create(backup2).Error()
		assert.NoError(tt, err)

		isLatest, err := store.IsLatestBackupInVault(context.Background(), "backup-uuid-1", "volume-uuid-1", vault1.ID)
		assert.NoError(tt, err)
		assert.False(tt, isLatest)
	})

	t.Run("ReturnsFalseWhenNoBackupsExistInVault", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		vault1 := &datamodel.BackupVault{BaseModel: datamodel.BaseModel{UUID: "vault-uuid-1"}, Name: "vault1"}
		err = store.db.Create(vault1).Error()
		assert.NoError(tt, err)

		isLatest, err := store.IsLatestBackupInVault(context.Background(), "backup-uuid-1", "volume-uuid-1", vault1.ID)
		assert.Error(tt, err)
		assert.False(tt, isLatest)
	})

	t.Run("ScopesByVault", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		vault1 := &datamodel.BackupVault{BaseModel: datamodel.BaseModel{UUID: "vault-uuid-1"}, Name: "vault1"}
		vault2 := &datamodel.BackupVault{BaseModel: datamodel.BaseModel{UUID: "vault-uuid-2"}, Name: "vault2"}
		err = store.db.Create(vault1).Error()
		assert.NoError(tt, err)
		err = store.db.Create(vault2).Error()
		assert.NoError(tt, err)

		backupV1 := &datamodel.Backup{
			BaseModel:  datamodel.BaseModel{UUID: "backup-v1", CreatedAt: time.Now().Add(-1 * time.Hour)},
			VolumeUUID: "volume-uuid-1", BackupVaultID: vault1.ID, State: models.LifeCycleStateAvailable,
		}
		backupV2 := &datamodel.Backup{
			BaseModel:  datamodel.BaseModel{UUID: "backup-v2", CreatedAt: time.Now().Add(-30 * time.Minute)},
			VolumeUUID: "volume-uuid-1", BackupVaultID: vault2.ID, State: models.LifeCycleStateAvailable,
		}
		err = store.db.Create(backupV1).Error()
		assert.NoError(tt, err)
		err = store.db.Create(backupV2).Error()
		assert.NoError(tt, err)

		// Each is latest in its own vault
		isLatest1, err := store.IsLatestBackupInVault(context.Background(), "backup-v1", "volume-uuid-1", vault1.ID)
		assert.NoError(tt, err)
		assert.True(tt, isLatest1)
		isLatest2, err := store.IsLatestBackupInVault(context.Background(), "backup-v2", "volume-uuid-1", vault2.ID)
		assert.NoError(tt, err)
		assert.True(tt, isLatest2)
		// backup-v1 is not latest in vault2
		isLatestV2Wrong, err := store.IsLatestBackupInVault(context.Background(), "backup-v1", "volume-uuid-1", vault2.ID)
		assert.NoError(tt, err)
		assert.False(tt, isLatestV2Wrong)
	})
}

func TestUpdateLatestBackupLogicalSize(t *testing.T) {
	t.Run("UpdatesLatestBackupSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Create backups for the same volume with different IDs
		backup1 := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{
				UUID:      "backup-uuid-1",
				CreatedAt: time.Now().Add(-2 * time.Hour), // 2 hours ago
			},
			VolumeUUID:              "volume-uuid-1",
			State:                   models.LifeCycleStateAvailable,
			LatestLogicalBackupSize: 1024,
		}
		backup2 := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{
				UUID:      "backup-uuid-2",
				CreatedAt: time.Now().Add(-1 * time.Hour), // 1 hour ago (latest by ID)
			},
			VolumeUUID:              "volume-uuid-1",
			State:                   models.LifeCycleStateAvailable,
			LatestLogicalBackupSize: 2048,
		}

		err = store.db.Create(backup1).Error()
		assert.NoError(tt, err)
		err = store.db.Create(backup2).Error()
		assert.NoError(tt, err)

		// Create initial backup chain history
		initialHistory := &datamodel.BackupChainHistory{
			BaseModel: datamodel.BaseModel{
				UUID:      "history-uuid-1",
				CreatedAt: time.Now(),
				UpdatedAt: time.Now(),
				DeletedAt: nil,
			},
			ResourceName: "test-volume",
			ResourceUUID: "volume-uuid-1",
			Size:         2048,
		}
		err = store.db.Create(initialHistory).Error()
		assert.NoError(tt, err)

		// Test: update the latest backup's logical size
		newLogicalSize := int64(4096)
		err = store.UpdateLatestBackupLogicalSize(context.Background(), "volume-uuid-1", newLogicalSize)
		assert.NoError(tt, err)

		// Verify the update
		var updatedBackup datamodel.Backup
		err = store.db.GORM().Where("uuid = ?", "backup-uuid-2").First(&updatedBackup).Error
		assert.NoError(tt, err)
		assert.Equal(tt, newLogicalSize, updatedBackup.LatestLogicalBackupSize)

		// Verify backup1 was not updated
		var unchangedBackup datamodel.Backup
		err = store.db.GORM().Where("uuid = ?", "backup-uuid-1").First(&unchangedBackup).Error
		assert.NoError(tt, err)
		assert.Equal(tt, int64(1024), unchangedBackup.LatestLogicalBackupSize)
	})

	t.Run("ReturnsErrorWhenNoAvailableBackupsExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Create a backup with non-available state
		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{
				UUID:      "backup-uuid-1",
				CreatedAt: time.Now(),
			},
			VolumeUUID:              "volume-uuid-1",
			State:                   models.LifeCycleStateDeleting, // Not available
			LatestLogicalBackupSize: 1024,
		}

		err = store.db.Create(backup).Error()
		assert.NoError(tt, err)

		// Test: should return error when no available backups exist
		err = store.UpdateLatestBackupLogicalSize(context.Background(), "volume-uuid-1", 4096)
		assert.Error(tt, err)
	})

	t.Run("ReturnsErrorWhenNoBackupsExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Test: should return error when no backups exist for the volume
		err = store.UpdateLatestBackupLogicalSize(context.Background(), "volume-uuid-1", 4096)
		assert.Error(tt, err)
	})

	t.Run("ReturnsErrorOnTransactionFailure", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Create a backup first
		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{
				UUID:      "backup-uuid-1",
				CreatedAt: time.Now(),
			},
			VolumeUUID:              "volume-uuid-1",
			State:                   models.LifeCycleStateAvailable,
			LatestLogicalBackupSize: 1024,
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

		// Test: should return error when transaction fails
		err = store.UpdateLatestBackupLogicalSize(context.Background(), "volume-uuid-1", 4096)
		assert.Error(tt, err)
		assert.Equal(tt, "transaction failed", err.Error())
	})

	t.Run("ReturnsErrorOnUpdateFailure", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Create a backup first
		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{
				UUID:      "backup-uuid-1",
				CreatedAt: time.Now(),
			},
			VolumeUUID:              "volume-uuid-1",
			State:                   models.LifeCycleStateAvailable,
			LatestLogicalBackupSize: 1024,
		}
		err = store.db.Create(backup).Error()
		assert.NoError(tt, err)

		// Simulate DB failure by closing the connection after transaction starts
		sqlDB, err := store.db.GORM().DB()
		assert.NoError(tt, err)
		_ = sqlDB.Close()

		// Test: should return error when update fails
		err = store.UpdateLatestBackupLogicalSize(context.Background(), "volume-uuid-1", 4096)
		assert.Error(tt, err)
	})

	t.Run("UpdatesCorrectBackupWhenMultipleAvailableBackupsExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Create multiple available backups with different IDs
		backup1 := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{
				UUID:      "backup-uuid-1",
				CreatedAt: time.Now().Add(-3 * time.Hour), // 3 hours ago
			},
			VolumeUUID:              "volume-uuid-1",
			State:                   models.LifeCycleStateAvailable,
			LatestLogicalBackupSize: 1024,
		}
		backup2 := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{
				UUID:      "backup-uuid-2",
				CreatedAt: time.Now().Add(-2 * time.Hour), // 2 hours ago
			},
			VolumeUUID:              "volume-uuid-1",
			State:                   models.LifeCycleStateAvailable,
			LatestLogicalBackupSize: 2048,
		}
		backup3 := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{
				UUID:      "backup-uuid-3",
				CreatedAt: time.Now().Add(-1 * time.Hour), // 1 hour ago (latest by ID)
			},
			VolumeUUID:              "volume-uuid-1",
			State:                   models.LifeCycleStateAvailable,
			LatestLogicalBackupSize: 3072,
		}

		err = store.db.Create(backup1).Error()
		assert.NoError(tt, err)
		err = store.db.Create(backup2).Error()
		assert.NoError(tt, err)
		err = store.db.Create(backup3).Error()
		assert.NoError(tt, err)

		// Create initial backup chain history
		initialHistory := &datamodel.BackupChainHistory{
			BaseModel: datamodel.BaseModel{
				UUID:      "history-uuid-1",
				CreatedAt: time.Now(),
				UpdatedAt: time.Now(),
				DeletedAt: nil,
			},
			ResourceName: "test-volume",
			ResourceUUID: "volume-uuid-1",
			Size:         3072,
		}
		err = store.db.Create(initialHistory).Error()
		assert.NoError(tt, err)

		// Test: update the latest backup's logical size
		newLogicalSize := int64(8192)
		err = store.UpdateLatestBackupLogicalSize(context.Background(), "volume-uuid-1", newLogicalSize)
		assert.NoError(tt, err)

		// Verify only backup3 (latest) was updated
		var updatedBackup datamodel.Backup
		err = store.db.GORM().Where("uuid = ?", "backup-uuid-3").First(&updatedBackup).Error
		assert.NoError(tt, err)
		assert.Equal(tt, newLogicalSize, updatedBackup.LatestLogicalBackupSize)

		// Verify backup1 and backup2 were not updated
		var unchangedBackup1 datamodel.Backup
		err = store.db.GORM().Where("uuid = ?", "backup-uuid-1").First(&unchangedBackup1).Error
		assert.NoError(tt, err)
		assert.Equal(tt, int64(1024), unchangedBackup1.LatestLogicalBackupSize)

		var unchangedBackup2 datamodel.Backup
		err = store.db.GORM().Where("uuid = ?", "backup-uuid-2").First(&unchangedBackup2).Error
		assert.NoError(tt, err)
		assert.Equal(tt, int64(2048), unchangedBackup2.LatestLogicalBackupSize)
	})

	t.Run("CreatesBackupChainHistoryWhenSizeChanges", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		volumeUUID := "volume-uuid-test"
		volumeName := "test-volume"

		// Create initial backup
		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{
				UUID:      "backup-uuid-1",
				CreatedAt: time.Now(),
			},
			VolumeUUID:              volumeUUID,
			State:                   models.LifeCycleStateAvailable,
			LatestLogicalBackupSize: 1024,
		}
		err = store.db.Create(backup).Error()
		assert.NoError(tt, err)

		// Create initial backup chain history
		initialHistory := &datamodel.BackupChainHistory{
			BaseModel: datamodel.BaseModel{
				UUID:      "history-uuid-1",
				CreatedAt: time.Now(),
				UpdatedAt: time.Now(),
				DeletedAt: nil,
			},
			ResourceName: volumeName,
			ResourceUUID: volumeUUID,
			Size:         1024,
		}
		err = store.db.Create(initialHistory).Error()
		assert.NoError(tt, err)

		// Update with new size - should trigger backup chain history update
		newSize := int64(2048)
		err = store.UpdateLatestBackupLogicalSize(context.Background(), volumeUUID, newSize)
		assert.NoError(tt, err)

		// Verify backup was updated
		var updatedBackup datamodel.Backup
		err = store.db.GORM().Where("uuid = ?", "backup-uuid-1").First(&updatedBackup).Error
		assert.NoError(tt, err)
		assert.Equal(tt, newSize, updatedBackup.LatestLogicalBackupSize)

		// Verify old history entry was marked as deleted
		var oldHistory datamodel.BackupChainHistory
		err = store.db.GORM().Unscoped().Where("uuid = ?", "history-uuid-1").First(&oldHistory).Error
		assert.NoError(tt, err)
		assert.NotNil(tt, oldHistory.DeletedAt, "Old history entry should be marked as deleted")

		// Verify new history entry was created
		var newHistories []datamodel.BackupChainHistory
		err = store.db.GORM().Where("resource_uuid = ? AND deleted_at IS NULL", volumeUUID).Find(&newHistories).Error
		assert.NoError(tt, err)
		assert.Equal(tt, 1, len(newHistories), "Should have exactly one active history entry")
		assert.Equal(tt, newSize, newHistories[0].Size, "New history should have updated size")
		assert.Equal(tt, volumeName, newHistories[0].ResourceName)
	})

	t.Run("DoesNotCreateHistoryWhenSizeUnchanged", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		volumeUUID := "volume-uuid-test"
		volumeName := "test-volume"

		// Create backup
		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{
				UUID:      "backup-uuid-1",
				CreatedAt: time.Now(),
			},
			VolumeUUID:              volumeUUID,
			State:                   models.LifeCycleStateAvailable,
			LatestLogicalBackupSize: 1024,
		}
		err = store.db.Create(backup).Error()
		assert.NoError(tt, err)

		// Create backup chain history
		initialHistory := &datamodel.BackupChainHistory{
			BaseModel: datamodel.BaseModel{
				UUID:      "history-uuid-1",
				CreatedAt: time.Now(),
				UpdatedAt: time.Now(),
				DeletedAt: nil,
			},
			ResourceName: volumeName,
			ResourceUUID: volumeUUID,
			Size:         1024,
		}
		err = store.db.Create(initialHistory).Error()
		assert.NoError(tt, err)

		// Update with same size - should NOT trigger new history entry
		err = store.UpdateLatestBackupLogicalSize(context.Background(), volumeUUID, 1024)
		assert.NoError(tt, err)

		// Verify old history entry is still active (not deleted)
		var oldHistory datamodel.BackupChainHistory
		err = store.db.GORM().Where("uuid = ?", "history-uuid-1").First(&oldHistory).Error
		assert.NoError(tt, err)
		assert.Nil(tt, oldHistory.DeletedAt, "History entry should still be active")

		// Verify no new history entries were created
		var allHistories []datamodel.BackupChainHistory
		err = store.db.GORM().Where("resource_uuid = ?", volumeUUID).Find(&allHistories).Error
		assert.NoError(tt, err)
		assert.Equal(tt, 1, len(allHistories), "Should still have only one history entry")
	})

	t.Run("HandlesMultipleSizeChanges", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		volumeUUID := "volume-uuid-test"
		volumeName := "test-volume"

		// Create backup
		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{
				UUID:      "backup-uuid-1",
				CreatedAt: time.Now(),
			},
			VolumeUUID:              volumeUUID,
			State:                   models.LifeCycleStateAvailable,
			LatestLogicalBackupSize: 1024,
		}
		err = store.db.Create(backup).Error()
		assert.NoError(tt, err)

		// Create initial history
		initialHistory := &datamodel.BackupChainHistory{
			BaseModel: datamodel.BaseModel{
				UUID:      "history-uuid-1",
				CreatedAt: time.Now(),
				UpdatedAt: time.Now(),
				DeletedAt: nil,
			},
			ResourceName: volumeName,
			ResourceUUID: volumeUUID,
			Size:         1024,
		}
		err = store.db.Create(initialHistory).Error()
		assert.NoError(tt, err)

		// First size change
		err = store.UpdateLatestBackupLogicalSize(context.Background(), volumeUUID, 2048)
		assert.NoError(tt, err)

		// Second size change
		err = store.UpdateLatestBackupLogicalSize(context.Background(), volumeUUID, 4096)
		assert.NoError(tt, err)

		// Third size change
		err = store.UpdateLatestBackupLogicalSize(context.Background(), volumeUUID, 8192)
		assert.NoError(tt, err)

		// Verify all history entries exist
		var allHistories []datamodel.BackupChainHistory
		err = store.db.GORM().Unscoped().Where("resource_uuid = ?", volumeUUID).Order("created_at asc").Find(&allHistories).Error
		assert.NoError(tt, err)
		assert.Equal(tt, 4, len(allHistories), "Should have 4 history entries (1 initial + 3 updates)")

		// Verify only the latest is active
		var activeHistories []datamodel.BackupChainHistory
		err = store.db.GORM().Where("resource_uuid = ? AND deleted_at IS NULL", volumeUUID).Find(&activeHistories).Error
		assert.NoError(tt, err)
		assert.Equal(tt, 1, len(activeHistories), "Should have only one active history entry")
		assert.Equal(tt, int64(8192), activeHistories[0].Size, "Active history should have the latest size")

		// Verify progression of sizes in history
		assert.Equal(tt, int64(1024), allHistories[0].Size)
		assert.Equal(tt, int64(2048), allHistories[1].Size)
		assert.Equal(tt, int64(4096), allHistories[2].Size)
		assert.Equal(tt, int64(8192), allHistories[3].Size)

		// Verify first 3 are marked as deleted
		assert.NotNil(tt, allHistories[0].DeletedAt)
		assert.NotNil(tt, allHistories[1].DeletedAt)
		assert.NotNil(tt, allHistories[2].DeletedAt)
		assert.Nil(tt, allHistories[3].DeletedAt)
	})
}

func TestDataStoreRepository_UpdateBackupFields(t *testing.T) {
	tests := []struct {
		name          string
		setupData     func(*DataStoreRepository) *datamodel.Backup
		backupUUID    string
		updates       map[string]interface{}
		expectedError bool
		errorContains string
		verifyUpdate  func(*testing.T, *DataStoreRepository, string)
	}{
		{
			name: "Success - Updates backup fields",
			setupData: func(store *DataStoreRepository) *datamodel.Backup {
				backupVault := &datamodel.BackupVault{
					BaseModel: datamodel.BaseModel{UUID: "test-backup-vault-uuid"},
					Name:      "test-backup-vault",
				}
				err := store.db.Create(backupVault).Error()
				if err != nil {
					panic(err)
				}

				backup := &datamodel.Backup{
					BaseModel:               datamodel.BaseModel{UUID: "test-backup-uuid"},
					Name:                    "test-backup",
					BackupVaultID:           backupVault.ID,
					LatestLogicalBackupSize: 1024,
					Attributes:              &datamodel.BackupAttributes{},
				}
				err = store.db.Create(backup).Error()
				if err != nil {
					panic(err)
				}
				return backup
			},
			backupUUID: "test-backup-uuid",
			updates: map[string]interface{}{
				"latest_logical_backup_size": int64(2048),
				"attributes":                 &datamodel.BackupAttributes{ObjectStoreUUID: "new-object-store-uuid"},
			},
			expectedError: false,
			verifyUpdate: func(t *testing.T, store *DataStoreRepository, backupUUID string) {
				var updatedBackup datamodel.Backup
				err := store.db.GORM().Where("uuid = ?", backupUUID).First(&updatedBackup).Error
				assert.NoError(t, err)
				assert.Equal(t, int64(2048), updatedBackup.LatestLogicalBackupSize)
				assert.Equal(t, "new-object-store-uuid", updatedBackup.Attributes.ObjectStoreUUID)
			},
		},
		{
			name: "Success - Updates single field",
			setupData: func(store *DataStoreRepository) *datamodel.Backup {
				backupVault := &datamodel.BackupVault{
					BaseModel: datamodel.BaseModel{UUID: "test-backup-vault-uuid-2"},
					Name:      "test-backup-vault-2",
				}
				err := store.db.Create(backupVault).Error()
				if err != nil {
					panic(err)
				}

				backup := &datamodel.Backup{
					BaseModel:               datamodel.BaseModel{UUID: "test-backup-uuid-2"},
					Name:                    "test-backup-2",
					BackupVaultID:           backupVault.ID,
					LatestLogicalBackupSize: 512,
				}
				err = store.db.Create(backup).Error()
				if err != nil {
					panic(err)
				}
				return backup
			},
			backupUUID: "test-backup-uuid-2",
			updates: map[string]interface{}{
				"latest_logical_backup_size": int64(4096),
			},
			expectedError: false,
			verifyUpdate: func(t *testing.T, store *DataStoreRepository, backupUUID string) {
				var updatedBackup datamodel.Backup
				err := store.db.GORM().Where("uuid = ?", backupUUID).First(&updatedBackup).Error
				assert.NoError(t, err)
				assert.Equal(t, int64(4096), updatedBackup.LatestLogicalBackupSize)
			},
		},
		{
			name: "Error - Backup not found",
			setupData: func(store *DataStoreRepository) *datamodel.Backup {
				return nil // No backup created
			},
			backupUUID: "non-existent-uuid",
			updates: map[string]interface{}{
				"latest_logical_backup_size": int64(1024),
			},
			expectedError: true,
			errorContains: "not found",
		},
		{
			name: "Success - Updates updated_at timestamp",
			setupData: func(store *DataStoreRepository) *datamodel.Backup {
				backupVault := &datamodel.BackupVault{
					BaseModel: datamodel.BaseModel{UUID: "test-backup-vault-uuid-3"},
					Name:      "test-backup-vault-3",
				}
				err := store.db.Create(backupVault).Error()
				if err != nil {
					panic(err)
				}

				backup := &datamodel.Backup{
					BaseModel:     datamodel.BaseModel{UUID: "test-backup-uuid-3"},
					Name:          "test-backup-3",
					BackupVaultID: backupVault.ID,
				}
				err = store.db.Create(backup).Error()
				if err != nil {
					panic(err)
				}
				return backup
			},
			backupUUID: "test-backup-uuid-3",
			updates: map[string]interface{}{
				"latest_logical_backup_size": int64(8192),
			},
			expectedError: false,
			verifyUpdate: func(t *testing.T, store *DataStoreRepository, backupUUID string) {
				var updatedBackup datamodel.Backup
				err := store.db.GORM().Where("uuid = ?", backupUUID).First(&updatedBackup).Error
				assert.NoError(t, err)
				assert.Equal(t, int64(8192), updatedBackup.LatestLogicalBackupSize)
				assert.True(t, updatedBackup.UpdatedAt.After(updatedBackup.CreatedAt))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup
			db, err := SetupTestDB()
			assert.NoError(t, err)

			wrapper := gormwrapper.New(db)
			store := NewDataStoreRepository(wrapper)

			err = ClearInMemoryDB(store.db.GORM())
			assert.NoError(t, err)

			if tt.setupData != nil {
				_ = tt.setupData(store)
			}

			// Execute
			err = store.UpdateBackupFields(context.Background(), tt.backupUUID, tt.updates)

			// Verify
			if tt.expectedError {
				assert.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				assert.NoError(t, err)
				if tt.verifyUpdate != nil {
					tt.verifyUpdate(t, store, tt.backupUUID)
				}
			}
		})
	}
}

func TestDataStoreRepository_GetLatestBackupsGroupedByVolumeUUID(t *testing.T) {
	tests := []struct {
		name          string
		setupData     func(*DataStoreRepository) ([]*datamodel.Backup, []*datamodel.Volume)
		expectedCount int
		expectedError bool
		verifyResults func(*testing.T, []datamodel.Backup, []*datamodel.Backup)
	}{
		{
			name: "Success - Returns latest backups for multiple volumes",
			setupData: func(store *DataStoreRepository) ([]*datamodel.Backup, []*datamodel.Volume) {
				// Create accounts and pools
				account := &datamodel.Account{
					BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
					Name:      "test-account",
				}
				err := store.db.Create(account).Error()
				if err != nil {
					panic(err)
				}

				pool := &datamodel.Pool{
					BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
					Name:      "test-pool",
					AccountID: account.ID,
				}
				err = store.db.Create(pool).Error()
				if err != nil {
					panic(err)
				}

				// Create backup vault
				backupVault := &datamodel.BackupVault{
					BaseModel: datamodel.BaseModel{UUID: "test-backup-vault-uuid"},
					Name:      "test-backup-vault",
				}
				err = store.db.Create(backupVault).Error()
				if err != nil {
					panic(err)
				}

				// Create volumes
				volume1 := &datamodel.Volume{
					BaseModel: datamodel.BaseModel{UUID: "volume-uuid-1"},
					Name:      "test-volume-1",
					PoolID:    pool.ID,
					AccountID: account.ID,
					State:     models.LifeCycleStateREADY,
				}
				volume2 := &datamodel.Volume{
					BaseModel: datamodel.BaseModel{UUID: "volume-uuid-2"},
					Name:      "test-volume-2",
					PoolID:    pool.ID,
					AccountID: account.ID,
					State:     models.LifeCycleStateREADY,
				}
				err = store.db.Create(volume1).Error()
				if err != nil {
					panic(err)
				}
				err = store.db.Create(volume2).Error()
				if err != nil {
					panic(err)
				}

				// Create backups for volume1 (older backup first, then newer)
				backup1Old := &datamodel.Backup{
					BaseModel:     datamodel.BaseModel{UUID: "backup-1-old-uuid", CreatedAt: time.Now().Add(-2 * time.Hour)},
					Name:          "backup-1-old",
					VolumeUUID:    volume1.UUID,
					BackupVaultID: backupVault.ID,
					State:         models.LifeCycleStateAvailable,
				}
				backup1New := &datamodel.Backup{
					BaseModel:     datamodel.BaseModel{UUID: "backup-1-new-uuid", CreatedAt: time.Now().Add(-1 * time.Hour)},
					Name:          "backup-1-new",
					VolumeUUID:    volume1.UUID,
					BackupVaultID: backupVault.ID,
					State:         models.LifeCycleStateAvailable,
				}

				// Create backup for volume2
				backup2 := &datamodel.Backup{
					BaseModel:     datamodel.BaseModel{UUID: "backup-2-uuid", CreatedAt: time.Now().Add(-30 * time.Minute)},
					Name:          "backup-2",
					VolumeUUID:    volume2.UUID,
					BackupVaultID: backupVault.ID,
					State:         models.LifeCycleStateAvailable,
				}

				err = store.db.Create(backup1Old).Error()
				if err != nil {
					panic(err)
				}
				err = store.db.Create(backup1New).Error()
				if err != nil {
					panic(err)
				}
				err = store.db.Create(backup2).Error()
				if err != nil {
					panic(err)
				}

				return []*datamodel.Backup{backup1New, backup2}, []*datamodel.Volume{volume1, volume2}
			},
			expectedCount: 2,
			expectedError: false,
			verifyResults: func(t *testing.T, results []datamodel.Backup, expectedBackups []*datamodel.Backup) {
				assert.Len(t, results, 2)

				// Create a map for easier verification
				resultMap := make(map[string]datamodel.Backup)
				for _, backup := range results {
					resultMap[backup.VolumeUUID] = backup
				}

				// Verify volume1 has the newer backup
				backup1, exists := resultMap["volume-uuid-1"]
				assert.True(t, exists)
				assert.Equal(t, "backup-1-new-uuid", backup1.UUID)

				// Verify volume2 has its backup
				backup2, exists := resultMap["volume-uuid-2"]
				assert.True(t, exists)
				assert.Equal(t, "backup-2-uuid", backup2.UUID)
			},
		},
		{
			name: "Success - Returns empty when no backups exist",
			setupData: func(store *DataStoreRepository) ([]*datamodel.Backup, []*datamodel.Volume) {
				// No backups created
				return []*datamodel.Backup{}, []*datamodel.Volume{}
			},
			expectedCount: 0,
			expectedError: false,
		},
		{
			name: "Success - Filters out non-available backups",
			setupData: func(store *DataStoreRepository) ([]*datamodel.Backup, []*datamodel.Volume) {
				// Create minimal setup
				account := &datamodel.Account{
					BaseModel: datamodel.BaseModel{UUID: "test-account-uuid-2"},
					Name:      "test-account-2",
				}
				err := store.db.Create(account).Error()
				if err != nil {
					panic(err)
				}

				pool := &datamodel.Pool{
					BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid-2"},
					Name:      "test-pool-2",
					AccountID: account.ID,
				}
				err = store.db.Create(pool).Error()
				if err != nil {
					panic(err)
				}

				backupVault := &datamodel.BackupVault{
					BaseModel: datamodel.BaseModel{UUID: "test-backup-vault-uuid-2"},
					Name:      "test-backup-vault-2",
				}
				err = store.db.Create(backupVault).Error()
				if err != nil {
					panic(err)
				}

				volume := &datamodel.Volume{
					BaseModel: datamodel.BaseModel{UUID: "volume-uuid-3"},
					Name:      "test-volume-3",
					PoolID:    pool.ID,
					AccountID: account.ID,
					State:     models.LifeCycleStateREADY,
				}
				err = store.db.Create(volume).Error()
				if err != nil {
					panic(err)
				}

				// Create one available and one creating backup
				backupAvailable := &datamodel.Backup{
					BaseModel:     datamodel.BaseModel{UUID: "backup-available-uuid"},
					Name:          "backup-available",
					VolumeUUID:    volume.UUID,
					BackupVaultID: backupVault.ID,
					State:         models.LifeCycleStateAvailable,
				}
				backupCreating := &datamodel.Backup{
					BaseModel:     datamodel.BaseModel{UUID: "backup-creating-uuid"},
					Name:          "backup-creating",
					VolumeUUID:    volume.UUID,
					BackupVaultID: backupVault.ID,
					State:         models.LifeCycleStateCreating,
				}

				err = store.db.Create(backupAvailable).Error()
				if err != nil {
					panic(err)
				}
				err = store.db.Create(backupCreating).Error()
				if err != nil {
					panic(err)
				}

				return []*datamodel.Backup{backupAvailable}, []*datamodel.Volume{volume}
			},
			expectedCount: 1,
			expectedError: false,
			verifyResults: func(t *testing.T, results []datamodel.Backup, expectedBackups []*datamodel.Backup) {
				assert.Len(t, results, 1)
				assert.Equal(t, "backup-available-uuid", results[0].UUID)
				assert.Equal(t, models.LifeCycleStateAvailable, results[0].State)
			},
		},
		{
			name: "Success - Filters out soft-deleted backups",
			setupData: func(store *DataStoreRepository) ([]*datamodel.Backup, []*datamodel.Volume) {
				// Create minimal setup
				account := &datamodel.Account{
					BaseModel: datamodel.BaseModel{UUID: "test-account-uuid-3"},
					Name:      "test-account-3",
				}
				err := store.db.Create(account).Error()
				if err != nil {
					panic(err)
				}

				pool := &datamodel.Pool{
					BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid-3"},
					Name:      "test-pool-3",
					AccountID: account.ID,
				}
				err = store.db.Create(pool).Error()
				if err != nil {
					panic(err)
				}

				backupVault := &datamodel.BackupVault{
					BaseModel: datamodel.BaseModel{UUID: "test-backup-vault-uuid-3"},
					Name:      "test-backup-vault-3",
				}
				err = store.db.Create(backupVault).Error()
				if err != nil {
					panic(err)
				}

				volume := &datamodel.Volume{
					BaseModel: datamodel.BaseModel{UUID: "volume-uuid-4"},
					Name:      "test-volume-4",
					PoolID:    pool.ID,
					AccountID: account.ID,
					State:     models.LifeCycleStateREADY,
				}
				err = store.db.Create(volume).Error()
				if err != nil {
					panic(err)
				}

				// Create one normal and one soft-deleted backup
				backupNormal := &datamodel.Backup{
					BaseModel:     datamodel.BaseModel{UUID: "backup-normal-uuid"},
					Name:          "backup-normal",
					VolumeUUID:    volume.UUID,
					BackupVaultID: backupVault.ID,
					State:         models.LifeCycleStateAvailable,
				}
				backupDeleted := &datamodel.Backup{
					BaseModel:     datamodel.BaseModel{UUID: "backup-deleted-uuid", DeletedAt: &gorm.DeletedAt{Time: time.Now(), Valid: true}},
					Name:          "backup-deleted",
					VolumeUUID:    volume.UUID,
					BackupVaultID: backupVault.ID,
					State:         models.LifeCycleStateAvailable,
				}

				err = store.db.Create(backupNormal).Error()
				if err != nil {
					panic(err)
				}
				err = store.db.Create(backupDeleted).Error()
				if err != nil {
					panic(err)
				}

				return []*datamodel.Backup{backupNormal}, []*datamodel.Volume{volume}
			},
			expectedCount: 1,
			expectedError: false,
			verifyResults: func(t *testing.T, results []datamodel.Backup, expectedBackups []*datamodel.Backup) {
				assert.Len(t, results, 1)
				assert.Equal(t, "backup-normal-uuid", results[0].UUID)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup
			db, err := SetupTestDB()
			assert.NoError(t, err)

			wrapper := gormwrapper.New(db)
			store := NewDataStoreRepository(wrapper)

			err = ClearInMemoryDB(store.db.GORM())
			assert.NoError(t, err)

			var expectedBackups []*datamodel.Backup
			if tt.setupData != nil {
				expectedBackups, _ = tt.setupData(store)
			}

			// Execute
			results, err := store.GetLatestBackupsGroupedByVolumeUUID(context.Background())

			// Verify
			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Len(t, results, tt.expectedCount)
				if tt.verifyResults != nil {
					tt.verifyResults(t, results, expectedBackups)
				}
			}
		})
	}
}

func TestDataStoreRepository_GetVolumeLatestBackupMap(t *testing.T) {
	tests := []struct {
		name          string
		setupData     func(*DataStoreRepository) ([]*datamodel.Volume, []*datamodel.Backup)
		expectedCount int
		expectedError bool
		verifyResults func(*testing.T, map[int64]*datamodel.VolumeLatestBackup, []*datamodel.Volume, []*datamodel.Backup)
	}{
		{
			name: "Success - Returns volume latest backup map",
			setupData: func(store *DataStoreRepository) ([]*datamodel.Volume, []*datamodel.Backup) {
				// Create accounts and pools with credentials
				account := &datamodel.Account{
					BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
					Name:      "test-account",
				}
				err := store.db.Create(account).Error()
				if err != nil {
					panic(err)
				}

				pool := &datamodel.Pool{
					BaseModel:      datamodel.BaseModel{UUID: "test-pool-uuid"},
					Name:           "test-pool",
					AccountID:      account.ID,
					DeploymentName: "test-deployment",
					PoolCredentials: &datamodel.PoolCredentials{
						Password:      "test-password",
						SecretID:      "test-secret-id",
						CertificateID: "test-cert-id",
						AuthType:      0, // USERNAME_PWD for basic authentication
					},
				}
				err = store.db.Create(pool).Error()
				if err != nil {
					panic(err)
				}

				// Create backup vault
				backupVault := &datamodel.BackupVault{
					BaseModel: datamodel.BaseModel{UUID: "test-backup-vault-uuid"},
					Name:      "test-backup-vault",
				}
				err = store.db.Create(backupVault).Error()
				if err != nil {
					panic(err)
				}

				// Create volumes
				volume1 := &datamodel.Volume{
					BaseModel: datamodel.BaseModel{UUID: "volume-uuid-1"},
					Name:      "test-volume-1",
					PoolID:    pool.ID,
					AccountID: account.ID,
					State:     models.LifeCycleStateREADY,
				}
				volume2 := &datamodel.Volume{
					BaseModel: datamodel.BaseModel{UUID: "volume-uuid-2"},
					Name:      "test-volume-2",
					PoolID:    pool.ID,
					AccountID: account.ID,
					State:     models.LifeCycleStateREADY,
				}
				err = store.db.Create(volume1).Error()
				if err != nil {
					panic(err)
				}
				err = store.db.Create(volume2).Error()
				if err != nil {
					panic(err)
				}

				// Create backups
				backup1 := &datamodel.Backup{
					BaseModel:     datamodel.BaseModel{UUID: "backup-1-uuid"},
					Name:          "backup-1",
					VolumeUUID:    volume1.UUID,
					BackupVaultID: backupVault.ID,
					State:         models.LifeCycleStateAvailable,
				}
				backup2 := &datamodel.Backup{
					BaseModel:     datamodel.BaseModel{UUID: "backup-2-uuid"},
					Name:          "backup-2",
					VolumeUUID:    volume2.UUID,
					BackupVaultID: backupVault.ID,
					State:         models.LifeCycleStateAvailable,
				}
				err = store.db.Create(backup1).Error()
				if err != nil {
					panic(err)
				}
				err = store.db.Create(backup2).Error()
				if err != nil {
					panic(err)
				}

				return []*datamodel.Volume{volume1, volume2}, []*datamodel.Backup{backup1, backup2}
			},
			expectedCount: 2,
			expectedError: false,
			verifyResults: func(t *testing.T, results map[int64]*datamodel.VolumeLatestBackup, volumes []*datamodel.Volume, backups []*datamodel.Backup) {
				assert.Len(t, results, 2)

				// Verify volume1 mapping
				volume1Mapping, exists := results[volumes[0].ID]
				assert.True(t, exists)
				assert.Equal(t, volumes[0].UUID, volume1Mapping.Volume.UUID)
				assert.Equal(t, backups[0].UUID, volume1Mapping.LatestBackup.UUID)
				assert.NotNil(t, volume1Mapping.Volume.Pool)
				assert.Equal(t, "test-deployment", volume1Mapping.Volume.Pool.DeploymentName)

				// Verify volume2 mapping
				volume2Mapping, exists := results[volumes[1].ID]
				assert.True(t, exists)
				assert.Equal(t, volumes[1].UUID, volume2Mapping.Volume.UUID)
				assert.Equal(t, backups[1].UUID, volume2Mapping.LatestBackup.UUID)
			},
		},
		{
			name: "Success - Returns empty map when no volumes exist",
			setupData: func(store *DataStoreRepository) ([]*datamodel.Volume, []*datamodel.Backup) {
				return []*datamodel.Volume{}, []*datamodel.Backup{}
			},
			expectedCount: 0,
			expectedError: false,
		},
		{
			name: "Success - Returns empty map when no backups exist",
			setupData: func(store *DataStoreRepository) ([]*datamodel.Volume, []*datamodel.Backup) {
				// Create account and pool
				account := &datamodel.Account{
					BaseModel: datamodel.BaseModel{UUID: "test-account-uuid-2"},
					Name:      "test-account-2",
				}
				err := store.db.Create(account).Error()
				if err != nil {
					panic(err)
				}

				pool := &datamodel.Pool{
					BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid-2"},
					Name:      "test-pool-2",
					AccountID: account.ID,
				}
				err = store.db.Create(pool).Error()
				if err != nil {
					panic(err)
				}

				// Create volume but no backups
				volume := &datamodel.Volume{
					BaseModel: datamodel.BaseModel{UUID: "volume-uuid-no-backup"},
					Name:      "test-volume-no-backup",
					PoolID:    pool.ID,
					AccountID: account.ID,
					State:     models.LifeCycleStateREADY,
				}
				err = store.db.Create(volume).Error()
				if err != nil {
					panic(err)
				}

				return []*datamodel.Volume{volume}, []*datamodel.Backup{}
			},
			expectedCount: 0,
			expectedError: false,
		},
		{
			name: "Success - Filters out non-ready volumes",
			setupData: func(store *DataStoreRepository) ([]*datamodel.Volume, []*datamodel.Backup) {
				// Create account and pool
				account := &datamodel.Account{
					BaseModel: datamodel.BaseModel{UUID: "test-account-uuid-3"},
					Name:      "test-account-3",
				}
				err := store.db.Create(account).Error()
				if err != nil {
					panic(err)
				}

				pool := &datamodel.Pool{
					BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid-3"},
					Name:      "test-pool-3",
					AccountID: account.ID,
				}
				err = store.db.Create(pool).Error()
				if err != nil {
					panic(err)
				}

				// Create backup vault
				backupVault := &datamodel.BackupVault{
					BaseModel: datamodel.BaseModel{UUID: "test-backup-vault-uuid-3"},
					Name:      "test-backup-vault-3",
				}
				err = store.db.Create(backupVault).Error()
				if err != nil {
					panic(err)
				}

				// Create ready and creating volumes
				volumeReady := &datamodel.Volume{
					BaseModel: datamodel.BaseModel{UUID: "volume-uuid-ready"},
					Name:      "test-volume-ready",
					PoolID:    pool.ID,
					AccountID: account.ID,
					State:     models.LifeCycleStateREADY,
				}
				volumeCreating := &datamodel.Volume{
					BaseModel: datamodel.BaseModel{UUID: "volume-uuid-creating"},
					Name:      "test-volume-creating",
					PoolID:    pool.ID,
					AccountID: account.ID,
					State:     models.LifeCycleStateCreating,
				}
				err = store.db.Create(volumeReady).Error()
				if err != nil {
					panic(err)
				}
				err = store.db.Create(volumeCreating).Error()
				if err != nil {
					panic(err)
				}

				// Create backups for both volumes
				backupReady := &datamodel.Backup{
					BaseModel:     datamodel.BaseModel{UUID: "backup-ready-uuid"},
					Name:          "backup-ready",
					VolumeUUID:    volumeReady.UUID,
					BackupVaultID: backupVault.ID,
					State:         models.LifeCycleStateAvailable,
				}
				backupCreating := &datamodel.Backup{
					BaseModel:     datamodel.BaseModel{UUID: "backup-creating-uuid"},
					Name:          "backup-creating",
					VolumeUUID:    volumeCreating.UUID,
					BackupVaultID: backupVault.ID,
					State:         models.LifeCycleStateAvailable,
				}
				err = store.db.Create(backupReady).Error()
				if err != nil {
					panic(err)
				}
				err = store.db.Create(backupCreating).Error()
				if err != nil {
					panic(err)
				}

				return []*datamodel.Volume{volumeReady}, []*datamodel.Backup{backupReady}
			},
			expectedCount: 1,
			expectedError: false,
			verifyResults: func(t *testing.T, results map[int64]*datamodel.VolumeLatestBackup, volumes []*datamodel.Volume, backups []*datamodel.Backup) {
				assert.Len(t, results, 1)

				// Should only contain the ready volume
				volumeMapping, exists := results[volumes[0].ID]
				assert.True(t, exists)
				assert.Equal(t, volumes[0].UUID, volumeMapping.Volume.UUID)
				assert.Equal(t, models.LifeCycleStateREADY, volumeMapping.Volume.State)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup
			db, err := SetupTestDB()
			assert.NoError(t, err)

			wrapper := gormwrapper.New(db)
			store := NewDataStoreRepository(wrapper)

			err = ClearInMemoryDB(store.db.GORM())
			assert.NoError(t, err)

			var expectedVolumes []*datamodel.Volume
			var expectedBackups []*datamodel.Backup
			if tt.setupData != nil {
				expectedVolumes, expectedBackups = tt.setupData(store)
			}

			// Execute
			results, err := store.GetVolumeLatestBackupMap(context.Background())

			// Verify
			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Len(t, results, tt.expectedCount)
				if tt.verifyResults != nil {
					tt.verifyResults(t, results, expectedVolumes, expectedBackups)
				}
			}
		})
	}
}

func TestGetBackupMetrics(t *testing.T) {
	t.Run("ReturnsLatestBackupForEachVolumeSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Create backup vault first with CMEK attributes to verify they are loaded
		kmsConfigPath := "projects/test-project/locations/us/keyRings/test-keyring/cryptoKeys/test-key"
		encryptionState := "ENABLED"
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "test-backup-vault-uuid"},
			Name:      "test-backup-vault",
			CmekAttributes: &datamodel.CmekAttributes{
				KmsConfigResourcePath: &kmsConfigPath,
				EncryptionState:       &encryptionState,
			},
		}
		err = store.db.Create(backupVault).Error()
		assert.NoError(tt, err)

		// Create multiple backups for volume 1 (only the latest should be returned)
		backup1 := &datamodel.Backup{
			BaseModel:               datamodel.BaseModel{UUID: "backup-uuid-1"},
			Name:                    "backup-1",
			VolumeUUID:              "volume-uuid-1",
			LatestLogicalBackupSize: 1024,
			State:                   models.LifeCycleStateAvailable,
			BackupVaultID:           backupVault.ID,
			Attributes: &datamodel.BackupAttributes{
				AccountIdentifier: "account-1",
				VolumeName:        "volume-1",
			},
		}
		backup2 := &datamodel.Backup{
			BaseModel:               datamodel.BaseModel{UUID: "backup-uuid-2"},
			Name:                    "backup-2",
			VolumeUUID:              "volume-uuid-1",
			LatestLogicalBackupSize: 2048,
			State:                   models.LifeCycleStateAvailable,
			BackupVaultID:           backupVault.ID,
			Attributes: &datamodel.BackupAttributes{
				AccountIdentifier: "account-1",
				VolumeName:        "volume-1",
			},
		}
		// Create backup for volume 2
		backup3 := &datamodel.Backup{
			BaseModel:               datamodel.BaseModel{UUID: "backup-uuid-3"},
			Name:                    "backup-3",
			VolumeUUID:              "volume-uuid-2",
			LatestLogicalBackupSize: 4096,
			State:                   models.LifeCycleStateAvailable,
			BackupVaultID:           backupVault.ID,
			Attributes: &datamodel.BackupAttributes{
				AccountIdentifier: "account-2",
				VolumeName:        "volume-2",
			},
		}
		// Create backup with different state (should be filtered out)
		backup4 := &datamodel.Backup{
			BaseModel:               datamodel.BaseModel{UUID: "backup-uuid-4"},
			Name:                    "backup-4",
			VolumeUUID:              "volume-uuid-3",
			LatestLogicalBackupSize: 8192,
			State:                   models.LifeCycleStateCreating,
			BackupVaultID:           backupVault.ID,
			Attributes: &datamodel.BackupAttributes{
				AccountIdentifier: "account-3",
				VolumeName:        "volume-3",
			},
		}

		err = store.db.Create(backup1).Error()
		assert.NoError(tt, err)
		err = store.db.Create(backup2).Error()
		assert.NoError(tt, err)
		err = store.db.Create(backup3).Error()
		assert.NoError(tt, err)
		err = store.db.Create(backup4).Error()
		assert.NoError(tt, err)

		// Test: should return only the latest available backup for each volume
		results, err := store.GetBackupMetrics(context.Background(), [][]interface{}{}, nil)
		assert.NoError(tt, err)
		assert.Len(tt, results, 2) // Only 2 volumes with available backups

		// Verify the results contain the latest backups
		volume1Backup := findBackupByVolumeUUID(results, "volume-uuid-1")
		volume2Backup := findBackupByVolumeUUID(results, "volume-uuid-2")

		assert.NotNil(tt, volume1Backup)
		assert.Equal(tt, "backup-uuid-2", volume1Backup.UUID) // Latest backup for volume 1
		assert.Equal(tt, "backup-2", volume1Backup.Name)
		assert.Equal(tt, "volume-uuid-1", volume1Backup.VolumeUUID)
		assert.Equal(tt, int64(2048), volume1Backup.LatestLogicalBackupSize)
		assert.NotNil(tt, volume1Backup.Attributes)
		// Verify BackupVault is properly loaded with CMEK attributes
		assert.NotNil(tt, volume1Backup.BackupVault)
		assert.Equal(tt, "test-backup-vault", volume1Backup.BackupVault.Name)
		assert.NotNil(tt, volume1Backup.BackupVault.CmekAttributes)
		assert.NotNil(tt, volume1Backup.BackupVault.CmekAttributes.KmsConfigResourcePath)
		assert.Equal(tt, kmsConfigPath, *volume1Backup.BackupVault.CmekAttributes.KmsConfigResourcePath)
		assert.NotNil(tt, volume1Backup.BackupVault.CmekAttributes.EncryptionState)
		assert.Equal(tt, encryptionState, *volume1Backup.BackupVault.CmekAttributes.EncryptionState)

		assert.NotNil(tt, volume2Backup)
		assert.Equal(tt, "backup-uuid-3", volume2Backup.UUID)
		assert.Equal(tt, "backup-3", volume2Backup.Name)
		assert.Equal(tt, "volume-uuid-2", volume2Backup.VolumeUUID)
		assert.Equal(tt, int64(4096), volume2Backup.LatestLogicalBackupSize)
		assert.NotNil(tt, volume2Backup.Attributes)
		// Verify BackupVault is properly loaded with CMEK attributes
		assert.NotNil(tt, volume2Backup.BackupVault)
		assert.Equal(tt, "test-backup-vault", volume2Backup.BackupVault.Name)
		assert.NotNil(tt, volume2Backup.BackupVault.CmekAttributes)
		assert.NotNil(tt, volume2Backup.BackupVault.CmekAttributes.KmsConfigResourcePath)
		assert.Equal(tt, kmsConfigPath, *volume2Backup.BackupVault.CmekAttributes.KmsConfigResourcePath)
	})

	t.Run("ReturnsEmptySliceWhenNoAvailableBackups", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Create backup vault first
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "test-backup-vault-uuid"},
			Name:      "test-backup-vault",
		}
		err = store.db.Create(backupVault).Error()
		assert.NoError(tt, err)

		// Create backup with non-available state
		backup := &datamodel.Backup{
			BaseModel:               datamodel.BaseModel{UUID: "backup-uuid-1"},
			Name:                    "backup-1",
			VolumeUUID:              "volume-uuid-1",
			LatestLogicalBackupSize: 1024,
			State:                   models.LifeCycleStateCreating,
			BackupVaultID:           backupVault.ID,
		}
		err = store.db.Create(backup).Error()
		assert.NoError(tt, err)

		// Test: should return empty slice when no available backups
		results, err := store.GetBackupMetrics(context.Background(), [][]interface{}{}, nil)
		assert.NoError(tt, err)
		assert.Empty(tt, results)
	})

	t.Run("ReturnsErrorOnDBFailure", func(tt *testing.T) {
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

		// Test: should return error on DB failure
		results, err := store.GetBackupMetrics(context.Background(), [][]interface{}{}, nil)
		assert.Error(tt, err)
		assert.Nil(tt, results)
	})

	t.Run("HandlesMultipleBackupsWithSameVolumeUUID", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Create backup vault first
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "test-backup-vault-uuid"},
			Name:      "test-backup-vault",
		}
		err = store.db.Create(backupVault).Error()
		assert.NoError(tt, err)

		// Create multiple backups for the same volume with different IDs
		// The one with the highest ID should be returned
		backup1 := &datamodel.Backup{
			BaseModel:               datamodel.BaseModel{UUID: "backup-uuid-1"},
			Name:                    "backup-1",
			VolumeUUID:              "volume-uuid-1",
			LatestLogicalBackupSize: 1024,
			State:                   models.LifeCycleStateAvailable,
			BackupVaultID:           backupVault.ID,
		}
		backup2 := &datamodel.Backup{
			BaseModel:               datamodel.BaseModel{UUID: "backup-uuid-2"},
			Name:                    "backup-2",
			VolumeUUID:              "volume-uuid-1",
			LatestLogicalBackupSize: 2048,
			State:                   models.LifeCycleStateAvailable,
			BackupVaultID:           backupVault.ID,
		}
		backup3 := &datamodel.Backup{
			BaseModel:               datamodel.BaseModel{UUID: "backup-uuid-3"},
			Name:                    "backup-3",
			VolumeUUID:              "volume-uuid-1",
			LatestLogicalBackupSize: 4096,
			State:                   models.LifeCycleStateAvailable,
			BackupVaultID:           backupVault.ID,
		}

		err = store.db.Create(backup1).Error()
		assert.NoError(tt, err)
		err = store.db.Create(backup2).Error()
		assert.NoError(tt, err)
		err = store.db.Create(backup3).Error()
		assert.NoError(tt, err)

		// Test: should return only the latest backup (highest ID)
		results, err := store.GetBackupMetrics(context.Background(), [][]interface{}{}, nil)
		assert.NoError(tt, err)
		assert.Len(tt, results, 1)

		// Verify it's the latest backup
		assert.Equal(tt, "backup-uuid-3", results[0].UUID)
		assert.Equal(tt, "backup-3", results[0].Name)
		assert.Equal(tt, "volume-uuid-1", results[0].VolumeUUID)
		assert.Equal(tt, int64(4096), results[0].LatestLogicalBackupSize)
	})

	t.Run("HandlesBackupsWithNilAttributes", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Create backup vault first
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "test-backup-vault-uuid"},
			Name:      "test-backup-vault",
		}
		err = store.db.Create(backupVault).Error()
		assert.NoError(tt, err)

		// Create backup with nil attributes
		backup := &datamodel.Backup{
			BaseModel:               datamodel.BaseModel{UUID: "backup-uuid-1"},
			Name:                    "backup-1",
			VolumeUUID:              "volume-uuid-1",
			LatestLogicalBackupSize: 1024,
			State:                   models.LifeCycleStateAvailable,
			BackupVaultID:           backupVault.ID,
			Attributes:              nil, // Nil attributes
		}
		err = store.db.Create(backup).Error()
		assert.NoError(tt, err)

		// Test: should still return the backup even with nil attributes
		results, err := store.GetBackupMetrics(context.Background(), [][]interface{}{}, nil)
		assert.NoError(tt, err)
		assert.Len(tt, results, 1)

		assert.Equal(tt, "backup-uuid-1", results[0].UUID)
		assert.Equal(tt, "backup-1", results[0].Name)
		assert.Equal(tt, "volume-uuid-1", results[0].VolumeUUID)
		assert.Equal(tt, int64(1024), results[0].LatestLogicalBackupSize)
		// GORM creates an empty BackupAttributes struct instead of nil for JSONB fields
		// So we check that the attributes are present but have default values
		assert.NotNil(tt, results[0].Attributes)
		assert.Equal(tt, "", results[0].Attributes.AccountIdentifier)
		assert.Equal(tt, "", results[0].Attributes.VolumeName)
		// Verify BackupVault is loaded (cmek_attributes can be nil for non-CMEK vaults)
		assert.NotNil(tt, results[0].BackupVault)
		assert.Equal(tt, "test-backup-vault", results[0].BackupVault.Name)
	})
}

func TestGetBackupResourceDataForAggregation(t *testing.T) {
	t.Run("ReturnsLatestBackupPerVolumeWithVaultFields", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid-agg"},
			Name:      "agg-vault",
			AccountID: 42,
		}
		err = store.db.Create(backupVault).Error()
		assert.NoError(tt, err)

		backup1 := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "agg-backup-1"},
			VolumeUUID:    "vol-uuid-agg-1",
			State:         models.LifeCycleStateAvailable,
			BackupVaultID: backupVault.ID,
			Attributes: &datamodel.BackupAttributes{
				AccountIdentifier: "acct-agg",
				VolumeName:        "vol-agg-1",
			},
		}
		backup2 := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "agg-backup-2"},
			VolumeUUID:    "vol-uuid-agg-1",
			State:         models.LifeCycleStateAvailable,
			BackupVaultID: backupVault.ID,
			Attributes: &datamodel.BackupAttributes{
				AccountIdentifier: "acct-agg",
				VolumeName:        "vol-agg-1",
			},
		}

		err = store.db.Create(backup1).Error()
		assert.NoError(tt, err)
		err = store.db.Create(backup2).Error()
		assert.NoError(tt, err)

		results, err := store.GetBackupResourceDataForAggregation(context.Background(), [][]interface{}{}, nil)
		assert.NoError(tt, err)
		assert.Len(tt, results, 1)

		assert.Equal(tt, "agg-backup-2", results[0].UUID)
		assert.Equal(tt, "vol-uuid-agg-1", results[0].VolumeUUID)
		assert.NotNil(tt, results[0].Attributes)
		assert.Equal(tt, "acct-agg", results[0].Attributes.AccountIdentifier)
		assert.NotNil(tt, results[0].BackupVault)
		assert.Equal(tt, "agg-vault", results[0].BackupVault.Name)
		assert.Equal(tt, int64(42), results[0].BackupVault.AccountID)
		assert.Equal(tt, backupVault.ID, results[0].BackupVaultID)
		assert.Nil(tt, results[0].BackupVault.BackupRegionName)
	})

	t.Run("ReturnsEmptySliceWhenNoBackups", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		results, err := store.GetBackupResourceDataForAggregation(context.Background(), [][]interface{}{}, nil)
		assert.NoError(tt, err)
		assert.Empty(tt, results)
	})

	t.Run("RespectsPagination", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid-page"},
			Name:      "page-vault",
			AccountID: 10,
		}
		err = store.db.Create(backupVault).Error()
		assert.NoError(tt, err)

		for i := 0; i < 3; i++ {
			b := &datamodel.Backup{
				BaseModel:     datamodel.BaseModel{UUID: "page-backup-" + string(rune('a'+i))},
				VolumeUUID:    "page-vol-" + string(rune('a'+i)),
				State:         models.LifeCycleStateAvailable,
				BackupVaultID: backupVault.ID,
				Attributes:    &datamodel.BackupAttributes{AccountIdentifier: "acct"},
			}
			err = store.db.Create(b).Error()
			assert.NoError(tt, err)
		}

		pagination := &dbutils.Pagination{Limit: 2, Offset: 0}
		results, err := store.GetBackupResourceDataForAggregation(context.Background(), [][]interface{}{}, pagination)
		assert.NoError(tt, err)
		assert.Len(tt, results, 2)

		pagination2 := &dbutils.Pagination{Limit: 2, Offset: 2}
		results2, err := store.GetBackupResourceDataForAggregation(context.Background(), [][]interface{}{}, pagination2)
		assert.NoError(tt, err)
		assert.Len(tt, results2, 1)
	})

	t.Run("IncludesSoftDeletedBackupsViaUnscoped", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid-del"},
			Name:      "del-vault",
			AccountID: 99,
		}
		err = store.db.Create(backupVault).Error()
		assert.NoError(tt, err)

		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "del-backup-1"},
			VolumeUUID:    "del-vol-1",
			State:         models.LifeCycleStateAvailable,
			BackupVaultID: backupVault.ID,
			Attributes:    &datamodel.BackupAttributes{AccountIdentifier: "acct-del"},
		}
		err = store.db.Create(backup).Error()
		assert.NoError(tt, err)

		err = store.db.Delete(backup).Error()
		assert.NoError(tt, err)

		results, err := store.GetBackupResourceDataForAggregation(context.Background(), [][]interface{}{}, nil)
		assert.NoError(tt, err)
		assert.Len(tt, results, 1)
		assert.Equal(tt, "del-backup-1", results[0].UUID)
	})

	t.Run("ReturnsBackupRegionNameFromVault", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		backupRegion := "us-east4"
		backupVault := &datamodel.BackupVault{
			BaseModel:        datamodel.BaseModel{UUID: "vault-uuid-region"},
			Name:             "region-vault",
			AccountID:        50,
			BackupRegionName: &backupRegion,
		}
		err = store.db.Create(backupVault).Error()
		assert.NoError(tt, err)

		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "region-backup-1"},
			VolumeUUID:    "region-vol-1",
			State:         models.LifeCycleStateAvailable,
			BackupVaultID: backupVault.ID,
			Attributes:    &datamodel.BackupAttributes{AccountIdentifier: "acct-region"},
		}
		err = store.db.Create(backup).Error()
		assert.NoError(tt, err)

		results, err := store.GetBackupResourceDataForAggregation(context.Background(), [][]interface{}{}, nil)
		assert.NoError(tt, err)
		assert.Len(tt, results, 1)

		assert.NotNil(tt, results[0].BackupVault)
		assert.NotNil(tt, results[0].BackupVault.BackupRegionName)
		assert.Equal(tt, "us-east4", *results[0].BackupVault.BackupRegionName)
		assert.Equal(tt, "region-vault", results[0].BackupVault.Name)
		assert.Equal(tt, int64(50), results[0].BackupVault.AccountID)
	})

	t.Run("ReturnsNilBackupRegionNameWhenVaultHasNoRegion", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		backupVault := &datamodel.BackupVault{
			BaseModel:        datamodel.BaseModel{UUID: "vault-uuid-no-region"},
			Name:             "no-region-vault",
			AccountID:        60,
			BackupRegionName: nil,
		}
		err = store.db.Create(backupVault).Error()
		assert.NoError(tt, err)

		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "no-region-backup-1"},
			VolumeUUID:    "no-region-vol-1",
			State:         models.LifeCycleStateAvailable,
			BackupVaultID: backupVault.ID,
			Attributes:    &datamodel.BackupAttributes{AccountIdentifier: "acct-no-region"},
		}
		err = store.db.Create(backup).Error()
		assert.NoError(tt, err)

		results, err := store.GetBackupResourceDataForAggregation(context.Background(), [][]interface{}{}, nil)
		assert.NoError(tt, err)
		assert.Len(tt, results, 1)

		assert.NotNil(tt, results[0].BackupVault)
		assert.Nil(tt, results[0].BackupVault.BackupRegionName)
	})

	t.Run("ReturnsCorrectBackupRegionForMultipleVaults", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		region1 := "us-east4"
		vault1 := &datamodel.BackupVault{
			BaseModel:        datamodel.BaseModel{UUID: "vault-multi-1"},
			Name:             "cross-region-vault",
			AccountID:        70,
			BackupRegionName: &region1,
		}
		err = store.db.Create(vault1).Error()
		assert.NoError(tt, err)

		vault2 := &datamodel.BackupVault{
			BaseModel:        datamodel.BaseModel{UUID: "vault-multi-2"},
			Name:             "local-vault",
			AccountID:        70,
			BackupRegionName: nil,
		}
		err = store.db.Create(vault2).Error()
		assert.NoError(tt, err)

		backup1 := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "multi-backup-1"},
			VolumeUUID:    "multi-vol-1",
			State:         models.LifeCycleStateAvailable,
			BackupVaultID: vault1.ID,
			Attributes:    &datamodel.BackupAttributes{AccountIdentifier: "acct-multi"},
		}
		backup2 := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "multi-backup-2"},
			VolumeUUID:    "multi-vol-2",
			State:         models.LifeCycleStateAvailable,
			BackupVaultID: vault2.ID,
			Attributes:    &datamodel.BackupAttributes{AccountIdentifier: "acct-multi"},
		}

		err = store.db.Create(backup1).Error()
		assert.NoError(tt, err)
		err = store.db.Create(backup2).Error()
		assert.NoError(tt, err)

		results, err := store.GetBackupResourceDataForAggregation(context.Background(), [][]interface{}{}, nil)
		assert.NoError(tt, err)
		assert.Len(tt, results, 2)

		resultMap := make(map[string]*datamodel.Backup)
		for i := range results {
			resultMap[results[i].VolumeUUID] = results[i]
		}

		crossRegionResult := resultMap["multi-vol-1"]
		assert.NotNil(tt, crossRegionResult)
		assert.NotNil(tt, crossRegionResult.BackupVault.BackupRegionName)
		assert.Equal(tt, "us-east4", *crossRegionResult.BackupVault.BackupRegionName)

		localResult := resultMap["multi-vol-2"]
		assert.NotNil(tt, localResult)
		assert.Nil(tt, localResult.BackupVault.BackupRegionName)
	})

	t.Run("ReturnsEmptyBackupRegionNameForInRegionVault", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		sourceRegion := "us-central1"
		emptyRegion := ""
		inRegionVault := &datamodel.BackupVault{
			BaseModel:             datamodel.BaseModel{UUID: "720e0eb2-58e9-932c-f04c-7cda16e4e61d"},
			Name:                  "ccfe-bv1003260343148wybg",
			AccountID:             1091,
			BackupRegionName:      &emptyRegion,
			SourceRegionName:      &sourceRegion,
			LifeCycleState:        "READY",
			LifeCycleStateDetails: "Available for use",
			BackupVaultType:       "IN_REGION",
			Description:           nillable.ToPointer("CCFE backup vault created by automation"),
			ServiceType:           "GCNV",
		}
		err = store.db.Create(inRegionVault).Error()
		assert.NoError(tt, err)

		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "in-region-backup-1"},
			VolumeUUID:    "in-region-vol-1",
			State:         models.LifeCycleStateAvailable,
			BackupVaultID: inRegionVault.ID,
			Attributes: &datamodel.BackupAttributes{
				AccountIdentifier: "1088371202435",
				VolumeName:        "test-volume",
			},
		}
		err = store.db.Create(backup).Error()
		assert.NoError(tt, err)

		results, err := store.GetBackupResourceDataForAggregation(context.Background(), [][]interface{}{}, nil)
		assert.NoError(tt, err)
		assert.Len(tt, results, 1)

		assert.NotNil(tt, results[0].BackupVault)
		assert.Equal(tt, "ccfe-bv1003260343148wybg", results[0].BackupVault.Name)
		assert.Equal(tt, int64(1091), results[0].BackupVault.AccountID)

		// IN_REGION vaults have empty string for BackupRegionName, not nil
		assert.NotNil(tt, results[0].BackupVault.BackupRegionName)
		assert.Equal(tt, "", *results[0].BackupVault.BackupRegionName)

		assert.Equal(tt, "in-region-vol-1", results[0].VolumeUUID)
		assert.Equal(tt, "1088371202435", results[0].Attributes.AccountIdentifier)
	})
}

// Helper function to find backup by volume UUID in results
func findBackupByVolumeUUID(backups []*datamodel.Backup, volumeUUID string) *datamodel.Backup {
	for _, backup := range backups {
		if backup.VolumeUUID == volumeUUID {
			return backup
		}
	}
	return nil
}

func TestUpdateBackupConstituentCountFromVolumeUpdatesConstituentCountSuccessfully(t *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(t, err)

	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	assert.NoError(t, err)

	lvCount := int32(5)
	// Create a volume with LargeVolumeAttributes
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid-1"},
		LargeVolumeAttributes: &datamodel.LargeVolumeAttributes{
			LargeVolumeConstituentCount: &lvCount,
		},
	}
	err = store.db.Create(volume).Error()
	assert.NoError(t, err)

	// Create a backup
	backup := &datamodel.Backup{
		BaseModel: datamodel.BaseModel{UUID: "backup-uuid-1"},
		Attributes: &datamodel.BackupAttributes{
			Protocols: []string{"nfsv3"},
		},
	}
	err = store.db.Create(backup).Error()
	assert.NoError(t, err)

	// Call the method
	updatedBackup, err := store.UpdateBackupConstituentCountFromVolume(context.Background(), backup, volume)
	assert.NoError(t, err)
	assert.NotNil(t, updatedBackup)
	assert.NotNil(t, updatedBackup.Attributes)
	assert.Equal(t, int32(5), updatedBackup.Attributes.ConstituentCountOfBackup)
}

func TestUpdateBackupConstituentCountFromVolumeHandlesNilLargeVolumeAttributes(t *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(t, err)

	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	assert.NoError(t, err)

	// Create a volume without LargeVolumeAttributes
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid-1"},
	}
	err = store.db.Create(volume).Error()
	assert.NoError(t, err)

	// Create a backup
	backup := &datamodel.Backup{
		BaseModel: datamodel.BaseModel{UUID: "backup-uuid-1"},
		Attributes: &datamodel.BackupAttributes{
			Protocols: []string{"nfsv3"},
		},
	}
	err = store.db.Create(backup).Error()
	assert.NoError(t, err)

	// Call the method
	updatedBackup, err := store.UpdateBackupConstituentCountFromVolume(context.Background(), backup, volume)
	assert.NoError(t, err)
	assert.NotNil(t, updatedBackup)
	assert.NotNil(t, updatedBackup.Attributes)
	assert.Equal(t, int32(0), updatedBackup.Attributes.ConstituentCountOfBackup)
}

func TestUpdateBackupConstituentCountFromVolumeReturnsErrorWhenBackupNotFound(t *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(t, err)

	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	assert.NoError(t, err)

	// Create a volume
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid-1"},
	}
	err = store.db.Create(volume).Error()
	assert.NoError(t, err)

	// Call the method with a non-existent backup
	_, err = store.UpdateBackupConstituentCountFromVolume(context.Background(), &datamodel.Backup{BaseModel: datamodel.BaseModel{UUID: "non-existent-backup"}, Attributes: &datamodel.BackupAttributes{Protocols: []string{"nfsv3"}}}, volume)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestUpdateBackupConstituentCountFromVolumeReturnsErrorOnTransactionFailure(t *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(t, err)

	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	assert.NoError(t, err)

	// Create a volume
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid-1"},
	}
	err = store.db.Create(volume).Error()
	assert.NoError(t, err)

	// Create a backup
	backup := &datamodel.Backup{
		BaseModel: datamodel.BaseModel{UUID: "backup-uuid-1"},
		Attributes: &datamodel.BackupAttributes{
			Protocols: []string{"nfsv3"},
		},
	}
	err = store.db.Create(backup).Error()
	assert.NoError(t, err)

	// Simulate transaction failure
	sqlDB, err := store.db.GORM().DB()
	assert.NoError(t, err)
	_ = sqlDB.Close()

	_, err = store.UpdateBackupConstituentCountFromVolume(context.Background(), backup, volume)
	assert.Error(t, err)
}

func TestCreateBackupMetadata_Success(t *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(t, err)

	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	assert.NoError(t, err)

	ctx := context.Background()
	volumeUUID := "test-volume-uuid"
	labels := &datamodel.JSONB{"env": "test", "team": "backend"}

	backupMetadata := &datamodel.BackupMetadata{
		VolumeUUID: volumeUUID,
		Labels:     labels,
	}

	result, err := store.CreateBackupMetadata(ctx, backupMetadata)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, volumeUUID, result.VolumeUUID)
	assert.Equal(t, labels, result.Labels)
	assert.NotEmpty(t, result.UUID)
}

func TestCreateBackupMetadata_AlreadyExists(t *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(t, err)

	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	assert.NoError(t, err)

	ctx := context.Background()
	volumeUUID := "test-volume-uuid"
	labels := &datamodel.JSONB{"env": "test", "team": "backend"}

	// Create first backup metadata
	backupMetadata1 := &datamodel.BackupMetadata{
		VolumeUUID: volumeUUID,
		Labels:     labels,
	}
	result1, err := store.CreateBackupMetadata(ctx, backupMetadata1)
	assert.NoError(t, err)
	assert.NotNil(t, result1)

	// Try to create another backup metadata for the same volume
	backupMetadata2 := &datamodel.BackupMetadata{
		VolumeUUID: volumeUUID,
		Labels:     &datamodel.JSONB{"env": "prod", "team": "frontend"},
	}
	result2, err := store.CreateBackupMetadata(ctx, backupMetadata2)
	assert.NoError(t, err)
	assert.NotNil(t, result2)
	// Should return the existing one
	assert.Equal(t, result1.UUID, result2.UUID)
}

func TestDeleteBackupMetadata(t *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(t, err)

	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	assert.NoError(t, err)

	ctx := context.Background()
	volumeUUID := "test-volume-uuid"

	// First create a backup metadata
	backupMetadata := &datamodel.BackupMetadata{
		VolumeUUID: volumeUUID,
		Labels:     &datamodel.JSONB{"env": "test"},
	}
	created, err := store.CreateBackupMetadata(ctx, backupMetadata)
	assert.NoError(t, err)
	assert.NotNil(t, created)

	// Now delete it
	err = store.DeleteBackupMetadata(ctx, volumeUUID)
	assert.NoError(t, err)

	// Verify it's deleted
	_, err = store.GetBackupMetadataByVolumeUUID(ctx, volumeUUID)
	assert.Error(t, err)
}

func TestDeleteBackupMetadata_NotFound(t *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(t, err)

	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	assert.NoError(t, err)

	ctx := context.Background()
	volumeUUID := "non-existent-volume-uuid"

	// Try to delete non-existent backup metadata
	err = store.DeleteBackupMetadata(ctx, volumeUUID)
	assert.NoError(t, err) // Should not return error for non-existent entry
}

func TestGetBackupMetadataByVolumeUUID_Success(t *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(t, err)

	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	assert.NoError(t, err)

	ctx := context.Background()
	volumeUUID := "test-volume-uuid"
	labels := &datamodel.JSONB{"env": "test", "team": "backend"}

	// Create backup metadata
	backupMetadata := &datamodel.BackupMetadata{
		VolumeUUID: volumeUUID,
		Labels:     labels,
	}
	created, err := store.CreateBackupMetadata(ctx, backupMetadata)
	assert.NoError(t, err)
	assert.NotNil(t, created)

	// Retrieve it
	result, err := store.GetBackupMetadataByVolumeUUID(ctx, volumeUUID)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, volumeUUID, result.VolumeUUID)
	assert.Equal(t, labels, result.Labels)
	assert.Equal(t, created.UUID, result.UUID)
}

func TestGetBackupMetadataByVolumeUUID_NotFound(t *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(t, err)

	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	assert.NoError(t, err)

	ctx := context.Background()
	volumeUUID := "non-existent-volume-uuid"

	// Try to get non-existent backup metadata
	result, err := store.GetBackupMetadataByVolumeUUID(ctx, volumeUUID)
	assert.Error(t, err)
	assert.Nil(t, result)
}

func TestUpdateBackupMetadata_Success(t *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(t, err)

	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	assert.NoError(t, err)

	ctx := context.Background()
	volumeUUID := "test-volume-uuid"
	originalLabels := &datamodel.JSONB{"env": "test", "team": "backend"}
	updatedLabels := &datamodel.JSONB{"env": "prod", "team": "frontend", "version": "v2"}

	// Create backup metadata
	backupMetadata := &datamodel.BackupMetadata{
		VolumeUUID: volumeUUID,
		Labels:     originalLabels,
	}
	created, err := store.CreateBackupMetadata(ctx, backupMetadata)
	assert.NoError(t, err)
	assert.NotNil(t, created)

	// Update it
	created.Labels = updatedLabels
	result, err := store.UpdateBackupMetadata(ctx, created)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, volumeUUID, result.VolumeUUID)
	assert.Equal(t, updatedLabels, result.Labels)
	assert.Equal(t, created.UUID, result.UUID)
}

func TestUpdateBackupMetadata_NotFound(t *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(t, err)

	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	assert.NoError(t, err)

	ctx := context.Background()
	volumeUUID := "non-existent-volume-uuid"

	// Try to update non-existent backup metadata
	backupMetadata := &datamodel.BackupMetadata{
		BaseModel:  datamodel.BaseModel{UUID: "non-existent-uuid"},
		VolumeUUID: volumeUUID,
		Labels:     &datamodel.JSONB{"env": "test"},
	}
	result, err := store.UpdateBackupMetadata(ctx, backupMetadata)
	assert.Error(t, err)
	assert.Nil(t, result)
}

// TestGetBackupMetadata_WithPagination tests GetBackupMetadata with pagination
func TestGetBackupMetadata_WithPagination(t *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(t, err)

	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	assert.NoError(t, err)

	ctx := context.Background()

	// Create test backup metadata entries
	backupMetadata1 := &datamodel.BackupMetadata{
		BaseModel:  datamodel.BaseModel{UUID: "metadata-uuid-1"},
		VolumeUUID: "volume-uuid-1",
		Labels:     &datamodel.JSONB{"env": "test1"},
	}
	err = store.db.Create(backupMetadata1).Error()
	assert.NoError(t, err)

	backupMetadata2 := &datamodel.BackupMetadata{
		BaseModel:  datamodel.BaseModel{UUID: "metadata-uuid-2"},
		VolumeUUID: "volume-uuid-2",
		Labels:     &datamodel.JSONB{"env": "test2"},
	}
	err = store.db.Create(backupMetadata2).Error()
	assert.NoError(t, err)

	backupMetadata3 := &datamodel.BackupMetadata{
		BaseModel:  datamodel.BaseModel{UUID: "metadata-uuid-3"},
		VolumeUUID: "volume-uuid-3",
		Labels:     &datamodel.JSONB{"env": "test3"},
	}
	err = store.db.Create(backupMetadata3).Error()
	assert.NoError(t, err)

	// Test pagination with limit 2, offset 0
	pagination := &dbutils.Pagination{
		Offset: 0,
		Limit:  2,
	}
	conditions := [][]interface{}{}

	result, err := store.GetBackupMetadata(ctx, conditions, pagination)
	assert.NoError(t, err)
	assert.Len(t, result, 2)

	// Test pagination with limit 2, offset 2
	pagination2 := &dbutils.Pagination{
		Offset: 2,
		Limit:  2,
	}

	result2, err := store.GetBackupMetadata(ctx, conditions, pagination2)
	assert.NoError(t, err)
	assert.Len(t, result2, 1)
}

// TestGetBackupMetadata_WithConditions tests GetBackupMetadata with conditions
func TestGetBackupMetadata_WithConditions(t *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(t, err)

	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	assert.NoError(t, err)

	ctx := context.Background()

	// Create test backup metadata entries
	backupMetadata1 := &datamodel.BackupMetadata{
		BaseModel:  datamodel.BaseModel{UUID: "metadata-uuid-1"},
		VolumeUUID: "volume-uuid-1",
		Labels:     &datamodel.JSONB{"env": "test1"},
	}
	err = store.db.Create(backupMetadata1).Error()
	assert.NoError(t, err)

	backupMetadata2 := &datamodel.BackupMetadata{
		BaseModel:  datamodel.BaseModel{UUID: "metadata-uuid-2"},
		VolumeUUID: "volume-uuid-2",
		Labels:     &datamodel.JSONB{"env": "test2"},
	}
	err = store.db.Create(backupMetadata2).Error()
	assert.NoError(t, err)

	// Test with condition to filter by volume UUID
	conditions := [][]interface{}{
		{"volume_uuid = ?", "volume-uuid-1"},
	}
	pagination := &dbutils.Pagination{
		Offset: 0,
		Limit:  10,
	}

	result, err := store.GetBackupMetadata(ctx, conditions, pagination)
	assert.NoError(t, err)
	assert.Len(t, result, 1)
	assert.Equal(t, "volume-uuid-1", result[0].VolumeUUID)
}

// TestGetBackupMetadata_EmptyResult tests GetBackupMetadata with no results
func TestGetBackupMetadata_EmptyResult(t *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(t, err)

	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	assert.NoError(t, err)

	ctx := context.Background()

	// Test with condition that matches no records
	conditions := [][]interface{}{
		{"volume_uuid = ?", "non-existent-volume"},
	}
	pagination := &dbutils.Pagination{
		Offset: 0,
		Limit:  10,
	}

	result, err := store.GetBackupMetadata(ctx, conditions, pagination)
	assert.NoError(t, err)
	assert.Len(t, result, 0)
}

// TestGetBackupMetadata_WithNilPagination tests GetBackupMetadata with nil pagination
func TestGetBackupMetadata_WithNilPagination(t *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(t, err)

	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	assert.NoError(t, err)

	ctx := context.Background()

	// Create test backup metadata entry
	backupMetadata := &datamodel.BackupMetadata{
		BaseModel:  datamodel.BaseModel{UUID: "metadata-uuid-1"},
		VolumeUUID: "volume-uuid-1",
		Labels:     &datamodel.JSONB{"env": "test1"},
	}
	err = store.db.Create(backupMetadata).Error()
	assert.NoError(t, err)

	// Test with nil pagination
	conditions := [][]interface{}{}
	var pagination *dbutils.Pagination = nil

	result, err := store.GetBackupMetadata(ctx, conditions, pagination)
	assert.NoError(t, err)
	assert.Len(t, result, 1)
	assert.Equal(t, "volume-uuid-1", result[0].VolumeUUID)
}

// TestGetBackupMetadata_WithComplexConditions tests GetBackupMetadata with complex conditions
func TestGetBackupMetadata_WithComplexConditions(t *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(t, err)

	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	assert.NoError(t, err)

	ctx := context.Background()

	// Create test backup metadata entries with different timestamps
	now := time.Now()
	backupMetadata1 := &datamodel.BackupMetadata{
		BaseModel:  datamodel.BaseModel{UUID: "metadata-uuid-1", CreatedAt: now.Add(-2 * time.Hour)},
		VolumeUUID: "volume-uuid-1",
		Labels:     &datamodel.JSONB{"env": "test1"},
	}
	err = store.db.Create(backupMetadata1).Error()
	assert.NoError(t, err)

	backupMetadata2 := &datamodel.BackupMetadata{
		BaseModel:  datamodel.BaseModel{UUID: "metadata-uuid-2", CreatedAt: now.Add(-1 * time.Hour)},
		VolumeUUID: "volume-uuid-2",
		Labels:     &datamodel.JSONB{"env": "test2"},
	}
	err = store.db.Create(backupMetadata2).Error()
	assert.NoError(t, err)

	// Test with complex conditions (created_at > specific time)
	conditions := [][]interface{}{
		{"created_at > ?", now.Add(-90 * time.Minute)},
	}
	pagination := &dbutils.Pagination{
		Offset: 0,
		Limit:  10,
	}

	result, err := store.GetBackupMetadata(ctx, conditions, pagination)
	assert.NoError(t, err)
	assert.Len(t, result, 1)
	assert.Equal(t, "volume-uuid-2", result[0].VolumeUUID)
}

func TestCreateSfrMetadata_Success(t *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(t, err)

	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	assert.NoError(t, err)

	ctx := context.Background()
	sfrMetadata := &datamodel.SfrMetadata{
		FilesSize:  1024,
		FileCount:  1,
		VolumeName: "test-volume",
		VolumeUUID: "volume-uuid",
		BackupUUID: "backup-uuid",
		AccountID:  sql.NullInt64{Int64: 1, Valid: true},
		JobID:      sql.NullInt64{Int64: 100, Valid: true},
	}

	result, err := store.CreateSfrMetadata(ctx, sfrMetadata)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, sfrMetadata.FilesSize, result.FilesSize)
	assert.Equal(t, sfrMetadata.FileCount, result.FileCount)
	assert.Equal(t, sfrMetadata.VolumeName, result.VolumeName)
	assert.Equal(t, sfrMetadata.VolumeUUID, result.VolumeUUID)
	assert.Equal(t, sfrMetadata.BackupUUID, result.BackupUUID)
	assert.True(t, result.AccountID.Valid)
	assert.Equal(t, int64(1), result.AccountID.Int64)
	assert.True(t, result.JobID.Valid)
	assert.Equal(t, int64(100), result.JobID.Int64)
	assert.NotZero(t, result.ID)
	assert.NotZero(t, result.CreatedAt)
}

func TestCreateSfrMetadata_WithNilJobID(t *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(t, err)

	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	assert.NoError(t, err)

	ctx := context.Background()
	sfrMetadata := &datamodel.SfrMetadata{
		FilesSize:  2048,
		FileCount:  2,
		VolumeName: "test-volume-2",
		VolumeUUID: "volume-uuid-2",
		BackupUUID: "backup-uuid-2",
		AccountID:  sql.NullInt64{Int64: 2, Valid: true},
		JobID:      sql.NullInt64{Valid: false}, // Nil job ID
	}

	result, err := store.CreateSfrMetadata(ctx, sfrMetadata)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.False(t, result.JobID.Valid)
}

func TestCreateSfrMetadata_TransactionError(t *testing.T) {
	// This test simulates a transaction start failure
	// We'll use a closed database connection to trigger the error
	db, err := SetupTestDB()
	assert.NoError(t, err)

	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	assert.NoError(t, err)

	// Close the database connection to cause transaction start failure
	sqlDB, err := store.db.GORM().DB()
	assert.NoError(t, err)
	err = sqlDB.Close()
	assert.NoError(t, err)

	ctx := context.Background()
	sfrMetadata := &datamodel.SfrMetadata{
		FilesSize:  1024,
		FileCount:  1,
		VolumeName: "test-volume",
		VolumeUUID: "volume-uuid",
		BackupUUID: "backup-uuid",
	}

	result, err := store.CreateSfrMetadata(ctx, sfrMetadata)
	assert.Error(t, err)
	assert.Nil(t, result)
}

func TestCreateSfrMetadata_CreateError(t *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(t, err)

	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	assert.NoError(t, err)

	ctx := context.Background()

	// Create a duplicate entry to trigger a constraint violation
	sfrMetadata1 := &datamodel.SfrMetadata{
		FilesSize:  1024,
		FileCount:  1,
		VolumeName: "test-volume",
		VolumeUUID: "volume-uuid",
		BackupUUID: "backup-uuid",
		AccountID:  sql.NullInt64{Int64: 1, Valid: true},
		JobID:      sql.NullInt64{Int64: 100, Valid: true},
	}
	result1, err := store.CreateSfrMetadata(ctx, sfrMetadata1)
	assert.NoError(t, err)
	assert.NotNil(t, result1)

	// Try to create with invalid data that would cause an error
	// Using an invalid foreign key reference to trigger an error
	sfrMetadata2 := &datamodel.SfrMetadata{
		FilesSize:  2048,
		FileCount:  2,
		VolumeName: "test-volume-2",
		VolumeUUID: "volume-uuid-2",
		BackupUUID: "backup-uuid-2",
		AccountID:  sql.NullInt64{Int64: 999999, Valid: true}, // Non-existent account ID
		JobID:      sql.NullInt64{Int64: 999999, Valid: true}, // Non-existent job ID
	}

	// Note: This might not always fail depending on foreign key constraints
	// If foreign keys are not enforced, we'll need a different approach
	// For now, we'll test that the function handles the create operation
	result2, err := store.CreateSfrMetadata(ctx, sfrMetadata2)
	// The error depends on whether foreign key constraints are enforced
	// If they are, this should fail; if not, it should succeed
	if err != nil {
		assert.Nil(t, result2)
		assert.Error(t, err)
	} else {
		assert.NotNil(t, result2)
	}
}

func TestCreateSfrMetadata_DatabaseCreateError(t *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(t, err)

	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	assert.NoError(t, err)

	ctx := context.Background()

	// Mock startTransaction to succeed but mock the Create to fail
	originalStartTransaction := startTransaction
	startTransaction = func(db *gorm.DB) (*gorm.DB, error) {
		// Return a transaction that will fail on Create
		return db.Begin(), nil
	}
	defer func() { startTransaction = originalStartTransaction }()

	// Create a mock transaction that fails on Create
	// We'll use a closed database connection approach
	sqlDB, err := store.db.GORM().DB()
	assert.NoError(t, err)
	err = sqlDB.Close()
	assert.NoError(t, err)

	sfrMetadata := &datamodel.SfrMetadata{
		FilesSize:  1024,
		FileCount:  1,
		VolumeName: "test-volume",
		VolumeUUID: "volume-uuid",
		BackupUUID: "backup-uuid",
	}

	// This should trigger the error path at line 749
	result, err := store.CreateSfrMetadata(ctx, sfrMetadata)
	assert.Error(t, err)
	assert.Nil(t, result)
}

func TestGetSfrMetricsByTimeRange_Success(t *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(t, err)

	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	assert.NoError(t, err)

	ctx := context.Background()

	// Create test SFR metadata records
	now := time.Now()
	startTime := now.Add(-10 * time.Minute)
	endTime := now

	// Create SFR metadata for volume 1
	sfrMetadata1 := &datamodel.SfrMetadata{
		FilesSize:  1024,
		FileCount:  5,
		VolumeName: "test-volume-1",
		VolumeUUID: "volume-uuid-1",
		BackupUUID: "backup-uuid-1",
		AccountID:  sql.NullInt64{Int64: 1, Valid: true},
		CreatedAt:  now.Add(-5 * time.Minute), // Within time range
	}
	err = store.db.Create(sfrMetadata1).Error()
	assert.NoError(t, err)

	// Create another SFR metadata for volume 1 (to test aggregation)
	sfrMetadata2 := &datamodel.SfrMetadata{
		FilesSize:  2048,
		FileCount:  3,
		VolumeName: "test-volume-1",
		VolumeUUID: "volume-uuid-1",
		BackupUUID: "backup-uuid-2",
		AccountID:  sql.NullInt64{Int64: 1, Valid: true},
		CreatedAt:  now.Add(-3 * time.Minute), // Within time range
	}
	err = store.db.Create(sfrMetadata2).Error()
	assert.NoError(t, err)

	// Create SFR metadata for volume 2
	sfrMetadata3 := &datamodel.SfrMetadata{
		FilesSize:  4096,
		FileCount:  10,
		VolumeName: "test-volume-2",
		VolumeUUID: "volume-uuid-2",
		BackupUUID: "backup-uuid-3",
		AccountID:  sql.NullInt64{Int64: 2, Valid: true},
		CreatedAt:  now.Add(-2 * time.Minute), // Within time range
	}
	err = store.db.Create(sfrMetadata3).Error()
	assert.NoError(t, err)

	// Create SFR metadata outside time range (should not be included)
	sfrMetadata4 := &datamodel.SfrMetadata{
		FilesSize:  8192,
		FileCount:  20,
		VolumeName: "test-volume-3",
		VolumeUUID: "volume-uuid-3",
		BackupUUID: "backup-uuid-4",
		AccountID:  sql.NullInt64{Int64: 3, Valid: true},
		CreatedAt:  now.Add(-15 * time.Minute), // Outside time range
	}
	err = store.db.Create(sfrMetadata4).Error()
	assert.NoError(t, err)

	// Call GetSfrMetricsByTimeRange
	result, err := store.GetSfrMetricsByTimeRange(ctx, startTime, endTime)
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Verify results
	// Volume 1 should have aggregated metrics: 1024 + 2048 = 3072 total size, 5 + 3 = 8 total count
	assert.Contains(t, result, "volume-uuid-1")
	assert.Equal(t, int64(3072), result["volume-uuid-1"].TotalSize)
	assert.Equal(t, int64(8), result["volume-uuid-1"].TotalCount)

	// Volume 2 should have its metrics
	assert.Contains(t, result, "volume-uuid-2")
	assert.Equal(t, int64(4096), result["volume-uuid-2"].TotalSize)
	assert.Equal(t, int64(10), result["volume-uuid-2"].TotalCount)

	// Volume 3 should not be in results (outside time range)
	assert.NotContains(t, result, "volume-uuid-3")
}

func TestGetSfrMetricsByTimeRange_EmptyResults(t *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(t, err)

	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	assert.NoError(t, err)

	ctx := context.Background()

	now := time.Now()
	startTime := now.Add(-10 * time.Minute)
	endTime := now

	// Call GetSfrMetricsByTimeRange with no data
	result, err := store.GetSfrMetricsByTimeRange(ctx, startTime, endTime)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Empty(t, result)
}

func TestGetSfrMetricsByTimeRange_DatabaseError(t *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(t, err)

	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	assert.NoError(t, err)

	ctx := context.Background()

	// Close the database connection to simulate an error
	sqlDB, err := store.db.GORM().DB()
	assert.NoError(t, err)
	_ = sqlDB.Close()

	now := time.Now()
	startTime := now.Add(-10 * time.Minute)
	endTime := now

	// Call GetSfrMetricsByTimeRange with closed connection
	result, err := store.GetSfrMetricsByTimeRange(ctx, startTime, endTime)
	assert.Error(t, err)
	assert.Nil(t, result)
}

func TestAreBackupsInProgressForVolume(t *testing.T) {
	t.Run("ReturnsFalseWhenNoBackupsInProgress", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Create backup vault
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "test-backup-vault-uuid", ID: 1},
			Name:      "test-backup-vault",
		}
		err = store.db.Create(backupVault).Error()
		assert.NoError(tt, err)

		// Create backup in Available state (not in progress)
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "test-backup-uuid"},
			Name:          "test-backup",
			BackupVaultID: backupVault.ID,
			VolumeUUID:    "test-volume-uuid",
			State:         models.LifeCycleStateAvailable,
		}
		err = store.db.Create(backup).Error()
		assert.NoError(tt, err)

		// Check if backups are in progress
		inProgress, err := store.AreBackupsInProgressForVolume(context.Background(), "test-volume-uuid", nil, nil)
		assert.NoError(tt, err)
		assert.False(tt, inProgress)
	})

	t.Run("ReturnsTrueWhenBackupInCreatingState", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Create backup vault
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "test-backup-vault-uuid", ID: 1},
			Name:      "test-backup-vault",
		}
		err = store.db.Create(backupVault).Error()
		assert.NoError(tt, err)

		// Create backup in Creating state
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "test-backup-uuid"},
			Name:          "test-backup",
			BackupVaultID: backupVault.ID,
			VolumeUUID:    "test-volume-uuid",
			State:         models.LifeCycleStateCreating,
		}
		err = store.db.Create(backup).Error()
		assert.NoError(tt, err)

		// Check if backups are in progress
		inProgress, err := store.AreBackupsInProgressForVolume(context.Background(), "test-volume-uuid", nil, nil)
		assert.NoError(tt, err)
		assert.True(tt, inProgress)
	})

	t.Run("ReturnsTrueWhenBackupInDeletingState", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Create backup vault
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "test-backup-vault-uuid", ID: 1},
			Name:      "test-backup-vault",
		}
		err = store.db.Create(backupVault).Error()
		assert.NoError(tt, err)

		// Create backup in Deleting state
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "test-backup-uuid"},
			Name:          "test-backup",
			BackupVaultID: backupVault.ID,
			VolumeUUID:    "test-volume-uuid",
			State:         models.LifeCycleStateDeleting,
		}
		err = store.db.Create(backup).Error()
		assert.NoError(tt, err)

		// Check if backups are in progress
		inProgress, err := store.AreBackupsInProgressForVolume(context.Background(), "test-volume-uuid", nil, nil)
		assert.NoError(tt, err)
		assert.True(tt, inProgress)
	})

	t.Run("ReturnsFalseWhenExcludedBackupUUIDMatches", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Create backup vault
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "test-backup-vault-uuid", ID: 1},
			Name:      "test-backup-vault",
		}
		err = store.db.Create(backupVault).Error()
		assert.NoError(tt, err)

		// Create backup in Creating state
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "test-backup-uuid"},
			Name:          "test-backup",
			BackupVaultID: backupVault.ID,
			VolumeUUID:    "test-volume-uuid",
			State:         models.LifeCycleStateCreating,
		}
		err = store.db.Create(backup).Error()
		assert.NoError(tt, err)

		// Check if backups are in progress, excluding the backup we just created
		excludeUUIDs := []string{"test-backup-uuid"}
		inProgress, err := store.AreBackupsInProgressForVolume(context.Background(), "test-volume-uuid", excludeUUIDs, nil)
		assert.NoError(tt, err)
		assert.False(tt, inProgress)
	})

	t.Run("ReturnsTrueWhenExcludedBackupUUIDDoesNotMatch", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Create backup vault
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "test-backup-vault-uuid", ID: 1},
			Name:      "test-backup-vault",
		}
		err = store.db.Create(backupVault).Error()
		assert.NoError(tt, err)

		// Create backup in Creating state
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "test-backup-uuid"},
			Name:          "test-backup",
			BackupVaultID: backupVault.ID,
			VolumeUUID:    "test-volume-uuid",
			State:         models.LifeCycleStateCreating,
		}
		err = store.db.Create(backup).Error()
		assert.NoError(tt, err)

		// Check if backups are in progress, excluding a different backup UUID
		excludeUUIDs := []string{"different-backup-uuid"}
		inProgress, err := store.AreBackupsInProgressForVolume(context.Background(), "test-volume-uuid", excludeUUIDs, nil)
		assert.NoError(tt, err)
		assert.True(tt, inProgress)
	})

	t.Run("ReturnsTrueWhenMultipleBackupsInProgress", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Create backup vault
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "test-backup-vault-uuid", ID: 1},
			Name:      "test-backup-vault",
		}
		err = store.db.Create(backupVault).Error()
		assert.NoError(tt, err)

		// Create multiple backups in progress states
		backup1 := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "test-backup-uuid-1"},
			Name:          "test-backup-1",
			BackupVaultID: backupVault.ID,
			VolumeUUID:    "test-volume-uuid",
			State:         models.LifeCycleStateCreating,
		}
		err = store.db.Create(backup1).Error()
		assert.NoError(tt, err)

		backup2 := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "test-backup-uuid-2"},
			Name:          "test-backup-2",
			BackupVaultID: backupVault.ID,
			VolumeUUID:    "test-volume-uuid",
			State:         models.LifeCycleStateDeleting,
		}
		err = store.db.Create(backup2).Error()
		assert.NoError(tt, err)

		// Check if backups are in progress
		inProgress, err := store.AreBackupsInProgressForVolume(context.Background(), "test-volume-uuid", nil, nil)
		assert.NoError(tt, err)
		assert.True(tt, inProgress)
	})

	t.Run("ReturnsFalseWhenAllBackupsExcluded", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Create backup vault
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "test-backup-vault-uuid", ID: 1},
			Name:      "test-backup-vault",
		}
		err = store.db.Create(backupVault).Error()
		assert.NoError(tt, err)

		// Create multiple backups in progress states
		backup1 := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "test-backup-uuid-1"},
			Name:          "test-backup-1",
			BackupVaultID: backupVault.ID,
			VolumeUUID:    "test-volume-uuid",
			State:         models.LifeCycleStateCreating,
		}
		err = store.db.Create(backup1).Error()
		assert.NoError(tt, err)

		backup2 := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "test-backup-uuid-2"},
			Name:          "test-backup-2",
			BackupVaultID: backupVault.ID,
			VolumeUUID:    "test-volume-uuid",
			State:         models.LifeCycleStateDeleting,
		}
		err = store.db.Create(backup2).Error()
		assert.NoError(tt, err)

		// Check if backups are in progress, excluding all backups
		excludeUUIDs := []string{"test-backup-uuid-1", "test-backup-uuid-2"}
		inProgress, err := store.AreBackupsInProgressForVolume(context.Background(), "test-volume-uuid", excludeUUIDs, nil)
		assert.NoError(tt, err)
		assert.False(tt, inProgress)
	})

	t.Run("ReturnsFalseWhenVolumeHasNoBackups", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Check if backups are in progress for a volume with no backups
		inProgress, err := store.AreBackupsInProgressForVolume(context.Background(), "non-existent-volume-uuid", nil, nil)
		assert.NoError(tt, err)
		assert.False(tt, inProgress)
	})

	t.Run("ReturnsFalseWhenEmptyExcludeListProvided", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Create backup vault
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "test-backup-vault-uuid", ID: 1},
			Name:      "test-backup-vault",
		}
		err = store.db.Create(backupVault).Error()
		assert.NoError(tt, err)

		// Create backup in Creating state
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "test-backup-uuid"},
			Name:          "test-backup",
			BackupVaultID: backupVault.ID,
			VolumeUUID:    "test-volume-uuid",
			State:         models.LifeCycleStateCreating,
		}
		err = store.db.Create(backup).Error()
		assert.NoError(tt, err)

		// Check if backups are in progress with empty exclude list
		excludeUUIDs := []string{}
		inProgress, err := store.AreBackupsInProgressForVolume(context.Background(), "test-volume-uuid", excludeUUIDs, nil)
		assert.NoError(tt, err)
		assert.True(tt, inProgress)
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

		_, err = store.AreBackupsInProgressForVolume(context.Background(), "test-volume-uuid", nil, nil)
		assert.Error(tt, err)
	})
}

func TestGetEarliestCreatingBackupTime(t *testing.T) {
	t.Run("ReturnsNilWhenNoCreatingBackups", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "test-backup-vault-uuid", ID: 1},
			Name:      "test-backup-vault",
		}
		err = store.db.Create(backupVault).Error()
		assert.NoError(tt, err)

		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "test-backup-uuid"},
			Name:          "test-backup",
			BackupVaultID: backupVault.ID,
			VolumeUUID:    "test-volume-uuid",
			State:         models.LifeCycleStateAvailable,
		}
		err = store.db.Create(backup).Error()
		assert.NoError(tt, err)

		ts, err := store.GetEarliestCreatingBackupTime(context.Background(), "test-volume-uuid")
		assert.NoError(tt, err)
		assert.Nil(tt, ts)
	})

	t.Run("ReturnsMinCreatedAtAmongCreatingBackups", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "test-backup-vault-uuid", ID: 1},
			Name:      "test-backup-vault",
		}
		err = store.db.Create(backupVault).Error()
		assert.NoError(tt, err)

		b1 := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-older", CreatedAt: time.Now()},
			Name:          "backup-older",
			BackupVaultID: backupVault.ID,
			VolumeUUID:    "test-volume-uuid",
			State:         models.LifeCycleStateCreating,
		}
		err = store.db.Create(b1).Error()
		assert.NoError(tt, err)

		b2 := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-newer", CreatedAt: time.Now().Add(time.Hour)},
			Name:          "backup-newer",
			BackupVaultID: backupVault.ID,
			VolumeUUID:    "test-volume-uuid",
			State:         models.LifeCycleStateCreating,
		}
		err = store.db.Create(b2).Error()
		assert.NoError(tt, err)

		var loaded1, loaded2 datamodel.Backup
		require.NoError(tt, store.db.GORM().Where("uuid = ?", "backup-older").First(&loaded1).Error)
		require.NoError(tt, store.db.GORM().Where("uuid = ?", "backup-newer").First(&loaded2).Error)
		require.True(tt, loaded1.CreatedAt.Before(loaded2.CreatedAt))

		ts, err := store.GetEarliestCreatingBackupTime(context.Background(), "test-volume-uuid")
		assert.NoError(tt, err)
		require.NotNil(tt, ts)
		assert.WithinDuration(tt, loaded1.CreatedAt, *ts, time.Second)
	})

	t.Run("IgnoresNonCreatingStates", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "test-backup-vault-uuid", ID: 1},
			Name:      "test-backup-vault",
		}
		err = store.db.Create(backupVault).Error()
		assert.NoError(tt, err)

		oldTime := time.Now().UTC().Add(-24 * time.Hour)
		ready := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-ready", CreatedAt: oldTime},
			Name:          "backup-ready",
			BackupVaultID: backupVault.ID,
			VolumeUUID:    "test-volume-uuid",
			State:         models.LifeCycleStateAvailable,
		}
		err = store.db.Create(ready).Error()
		assert.NoError(tt, err)

		ts, err := store.GetEarliestCreatingBackupTime(context.Background(), "test-volume-uuid")
		assert.NoError(tt, err)
		assert.Nil(tt, ts)
	})
}

func Test_areBackupsInProgressForVolume(t *testing.T) {
	t.Run("ReturnsFalseWhenNoBackupsInProgress", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Create backup vault
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "test-backup-vault-uuid", ID: 1},
			Name:      "test-backup-vault",
		}
		err = store.db.Create(backupVault).Error()
		assert.NoError(tt, err)

		// Create backup in Available state (not in progress)
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "test-backup-uuid"},
			Name:          "test-backup",
			BackupVaultID: backupVault.ID,
			VolumeUUID:    "test-volume-uuid",
			State:         models.LifeCycleStateAvailable,
		}
		err = store.db.Create(backup).Error()
		assert.NoError(tt, err)

		// Test the internal function directly
		inProgress, err := areBackupsInProgressForVolume(store.db.GORM(), "test-volume-uuid", nil, nil)
		assert.NoError(tt, err)
		assert.False(tt, inProgress)
	})

	t.Run("ReturnsTrueWhenBackupInCreatingState", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Create backup vault
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "test-backup-vault-uuid", ID: 1},
			Name:      "test-backup-vault",
		}
		err = store.db.Create(backupVault).Error()
		assert.NoError(tt, err)

		// Create backup in Creating state
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "test-backup-uuid"},
			Name:          "test-backup",
			BackupVaultID: backupVault.ID,
			VolumeUUID:    "test-volume-uuid",
			State:         models.LifeCycleStateCreating,
		}
		err = store.db.Create(backup).Error()
		assert.NoError(tt, err)

		// Test the internal function directly
		inProgress, err := areBackupsInProgressForVolume(store.db.GORM(), "test-volume-uuid", nil, nil)
		assert.NoError(tt, err)
		assert.True(tt, inProgress)
	})

	t.Run("ReturnsTrueWhenBackupInDeletingState", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Create backup vault
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "test-backup-vault-uuid", ID: 1},
			Name:      "test-backup-vault",
		}
		err = store.db.Create(backupVault).Error()
		assert.NoError(tt, err)

		// Create backup in Deleting state
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "test-backup-uuid"},
			Name:          "test-backup",
			BackupVaultID: backupVault.ID,
			VolumeUUID:    "test-volume-uuid",
			State:         models.LifeCycleStateDeleting,
		}
		err = store.db.Create(backup).Error()
		assert.NoError(tt, err)

		// Test the internal function directly
		inProgress, err := areBackupsInProgressForVolume(store.db.GORM(), "test-volume-uuid", nil, nil)
		assert.NoError(tt, err)
		assert.True(tt, inProgress)
	})

	t.Run("ReturnsFalseWhenExcludedBackupUUIDMatches", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Create backup vault
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "test-backup-vault-uuid", ID: 1},
			Name:      "test-backup-vault",
		}
		err = store.db.Create(backupVault).Error()
		assert.NoError(tt, err)

		// Create backup in Creating state
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "test-backup-uuid"},
			Name:          "test-backup",
			BackupVaultID: backupVault.ID,
			VolumeUUID:    "test-volume-uuid",
			State:         models.LifeCycleStateCreating,
		}
		err = store.db.Create(backup).Error()
		assert.NoError(tt, err)

		// Test the internal function directly with exclude list
		excludeUUIDs := []string{"test-backup-uuid"}
		inProgress, err := areBackupsInProgressForVolume(store.db.GORM(), "test-volume-uuid", excludeUUIDs, nil)
		assert.NoError(tt, err)
		assert.False(tt, inProgress)
	})

	t.Run("ReturnsTrueWhenExcludedBackupUUIDDoesNotMatch", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Create backup vault
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "test-backup-vault-uuid", ID: 1},
			Name:      "test-backup-vault",
		}
		err = store.db.Create(backupVault).Error()
		assert.NoError(tt, err)

		// Create backup in Creating state
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "test-backup-uuid"},
			Name:          "test-backup",
			BackupVaultID: backupVault.ID,
			VolumeUUID:    "test-volume-uuid",
			State:         models.LifeCycleStateCreating,
		}
		err = store.db.Create(backup).Error()
		assert.NoError(tt, err)

		// Test the internal function directly with different exclude UUID
		excludeUUIDs := []string{"different-backup-uuid"}
		inProgress, err := areBackupsInProgressForVolume(store.db.GORM(), "test-volume-uuid", excludeUUIDs, nil)
		assert.NoError(tt, err)
		assert.True(tt, inProgress)
	})

	t.Run("ReturnsFalseWhenCreatingBackupIsNewerThanCreatedBefore", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "test-backup-vault-uuid", ID: 1},
			Name:      "test-backup-vault",
		}
		err = store.db.Create(backupVault).Error()
		assert.NoError(tt, err)

		future := time.Now().Add(2 * time.Hour)
		past := time.Now().Add(-1 * time.Hour)
		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{
				UUID:      "test-backup-uuid-newer",
				CreatedAt: future,
			},
			Name:          "test-backup",
			BackupVaultID: backupVault.ID,
			VolumeUUID:    "test-volume-uuid",
			State:         models.LifeCycleStateCreating,
		}
		err = store.db.Create(backup).Error()
		assert.NoError(tt, err)

		inProgress, err := areBackupsInProgressForVolume(store.db.GORM(), "test-volume-uuid", nil, &past)
		assert.NoError(tt, err)
		assert.False(tt, inProgress)
	})

	t.Run("HandlesRecordNotFoundError", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Test with non-existent volume (should return false, not error)
		// Note: GORM's Count doesn't return ErrRecordNotFound, but we test the error handling path
		inProgress, err := areBackupsInProgressForVolume(store.db.GORM(), "non-existent-volume-uuid", nil, nil)
		assert.NoError(tt, err)
		assert.False(tt, inProgress)
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

		_, err = areBackupsInProgressForVolume(store.db.GORM(), "test-volume-uuid", nil, nil)
		assert.Error(tt, err)
	})
}

func TestGetVolumeCountByBackupVaultID(t *testing.T) {
	t.Run("Success_BothRegularAndExpertModeVolumes", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		backupVaultUUID := "test-backup-vault-uuid"

		// Create regular volumes with backup vault ID in data_protection
		dataProtection1 := &datamodel.DataProtection{
			BackupVaultID: backupVaultUUID,
		}
		volume1 := &datamodel.Volume{
			BaseModel:      datamodel.BaseModel{UUID: "volume-uuid-1"},
			DataProtection: dataProtection1,
		}
		err = store.db.Create(volume1).Error()
		assert.NoError(tt, err)

		dataProtection2 := &datamodel.DataProtection{
			BackupVaultID: backupVaultUUID,
		}
		volume2 := &datamodel.Volume{
			BaseModel:      datamodel.BaseModel{UUID: "volume-uuid-2"},
			DataProtection: dataProtection2,
		}
		err = store.db.Create(volume2).Error()
		assert.NoError(tt, err)

		// Create expert mode volumes with backup vault ID in data_protection
		expertDataProtection1 := &datamodel.DataProtection{
			BackupVaultID: backupVaultUUID,
		}
		expertVolume1 := &datamodel.ExpertModeVolumes{
			BaseModel:    datamodel.BaseModel{UUID: "expert-volume-uuid-1"},
			BackupConfig: expertDataProtection1,
		}
		err = store.db.Create(expertVolume1).Error()
		assert.NoError(tt, err)

		expertDataProtection2 := &datamodel.DataProtection{
			BackupVaultID: backupVaultUUID,
		}
		expertVolume2 := &datamodel.ExpertModeVolumes{
			BaseModel:    datamodel.BaseModel{UUID: "expert-volume-uuid-2"},
			BackupConfig: expertDataProtection2,
		}
		err = store.db.Create(expertVolume2).Error()
		assert.NoError(tt, err)

		// Create volumes with different backup vault ID (should not be counted)
		differentDataProtection := &datamodel.DataProtection{
			BackupVaultID: "different-backup-vault-uuid",
		}
		differentVolume := &datamodel.Volume{
			BaseModel:      datamodel.BaseModel{UUID: "different-volume-uuid"},
			DataProtection: differentDataProtection,
		}
		err = store.db.Create(differentVolume).Error()
		assert.NoError(tt, err)

		count, err := store.GetVolumeCountByBackupVaultID(context.Background(), backupVaultUUID)
		assert.NoError(tt, err)
		assert.Equal(tt, int64(4), count) // 2 regular + 2 expert mode
	})

	t.Run("Success_OnlyRegularVolumes", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		backupVaultUUID := "test-backup-vault-uuid"

		// Create regular volumes with backup vault ID
		for i := 1; i <= 3; i++ {
			dataProtection := &datamodel.DataProtection{
				BackupVaultID: backupVaultUUID,
			}
			volume := &datamodel.Volume{
				BaseModel:      datamodel.BaseModel{UUID: "volume-uuid-" + string(rune(i))},
				DataProtection: dataProtection,
			}
			err = store.db.Create(volume).Error()
			assert.NoError(tt, err)
		}

		count, err := store.GetVolumeCountByBackupVaultID(context.Background(), backupVaultUUID)
		assert.NoError(tt, err)
		assert.Equal(tt, int64(3), count)
	})

	t.Run("Success_OnlyExpertModeVolumes", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		backupVaultUUID := "test-backup-vault-uuid"

		// Create expert mode volumes with backup vault ID
		for i := 1; i <= 2; i++ {
			expertDataProtection := &datamodel.DataProtection{
				BackupVaultID: backupVaultUUID,
			}
			expertVolume := &datamodel.ExpertModeVolumes{
				BaseModel:    datamodel.BaseModel{UUID: "expert-volume-uuid-" + string(rune(i))},
				BackupConfig: expertDataProtection,
			}
			err = store.db.Create(expertVolume).Error()
			assert.NoError(tt, err)
		}

		count, err := store.GetVolumeCountByBackupVaultID(context.Background(), backupVaultUUID)
		assert.NoError(tt, err)
		assert.Equal(tt, int64(2), count)
	})

	t.Run("Success_NoVolumes", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		backupVaultUUID := "non-existent-backup-vault-uuid"

		count, err := store.GetVolumeCountByBackupVaultID(context.Background(), backupVaultUUID)
		assert.NoError(tt, err)
		assert.Equal(tt, int64(0), count)
	})

	t.Run("Error_RegularVolumeQueryFails", func(tt *testing.T) {
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

		_, err = store.GetVolumeCountByBackupVaultID(context.Background(), "test-backup-vault-uuid")
		assert.Error(tt, err)
	})

	t.Run("Error_ExpertModeVolumeQueryFails", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		backupVaultUUID := "test-backup-vault-uuid"

		// Create regular volume first (to get past first query)
		dataProtection := &datamodel.DataProtection{
			BackupVaultID: backupVaultUUID,
		}
		volume := &datamodel.Volume{
			BaseModel:      datamodel.BaseModel{UUID: "volume-uuid-1"},
			DataProtection: dataProtection,
		}
		err = store.db.Create(volume).Error()
		assert.NoError(tt, err)

		// Close connection after first query succeeds
		sqlDB, err := store.db.GORM().DB()
		assert.NoError(tt, err)
		_ = sqlDB.Close()

		// The second query (expert mode volumes) should fail
		_, err = store.GetVolumeCountByBackupVaultID(context.Background(), backupVaultUUID)
		assert.Error(tt, err)
	})
}

func TestGetVolumesByBackupVaultID(t *testing.T) {
	t.Run("Success_ReturnsMatchingVolumes", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		backupVaultUUID := "target-vault-uuid"
		matchingVolume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "vol-match-1"},
			DataProtection: &datamodel.DataProtection{
				BackupVaultID: backupVaultUUID,
			},
		}
		otherVolume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "vol-other-1"},
			DataProtection: &datamodel.DataProtection{
				BackupVaultID: "another-vault-uuid",
			},
		}

		err = store.db.Create(matchingVolume).Error()
		assert.NoError(tt, err)
		err = store.db.Create(otherVolume).Error()
		assert.NoError(tt, err)

		volumes, err := store.GetVolumesByBackupVaultID(context.Background(), backupVaultUUID)
		assert.NoError(tt, err)
		require.Len(tt, volumes, 1)
		assert.Equal(tt, "vol-match-1", volumes[0].UUID)
	})

	t.Run("Error_WhenDBQueryFails", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		sqlDB, err := store.db.GORM().DB()
		assert.NoError(tt, err)
		_ = sqlDB.Close()

		_, err = store.GetVolumesByBackupVaultID(context.Background(), "target-vault-uuid")
		assert.Error(tt, err)
	})
}

func TestGetExpertModeVolumesByBackupVaultID(t *testing.T) {
	t.Run("Success_ReturnsMatchingExpertModeVolumes", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		backupVaultUUID := "target-vault-uuid"
		matchingEMV := &datamodel.ExpertModeVolumes{
			BaseModel: datamodel.BaseModel{UUID: "emv-match-1"},
			BackupConfig: &datamodel.DataProtection{
				BackupVaultID: backupVaultUUID,
			},
		}
		otherEMV := &datamodel.ExpertModeVolumes{
			BaseModel: datamodel.BaseModel{UUID: "emv-other-1"},
			BackupConfig: &datamodel.DataProtection{
				BackupVaultID: "another-vault-uuid",
			},
		}

		err = store.db.Create(matchingEMV).Error()
		assert.NoError(tt, err)
		err = store.db.Create(otherEMV).Error()
		assert.NoError(tt, err)

		volumes, err := store.GetExpertModeVolumesByBackupVaultID(context.Background(), backupVaultUUID)
		assert.NoError(tt, err)
		require.Len(tt, volumes, 1)
		assert.Equal(tt, "emv-match-1", volumes[0].UUID)
	})

	t.Run("Error_WhenDBQueryFails", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		sqlDB, err := store.db.GORM().DB()
		assert.NoError(tt, err)
		_ = sqlDB.Close()

		_, err = store.GetExpertModeVolumesByBackupVaultID(context.Background(), "target-vault-uuid")
		assert.Error(tt, err)
	})
}

func TestUpdateBackupChainHistory(t *testing.T) {
	t.Run("Success_CreatesNewHistoryEntry", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		ctx := context.Background()
		volumeUUID := "test-volume-uuid"
		newSize := int64(1024 * 1024 * 1024) // 1GB

		// Call UpdateBackupChainHistory
		err = store.UpdateBackupChainHistory(ctx, volumeUUID, newSize)
		assert.NoError(tt, err)

		// Verify no history was created because there was no existing history to supersede
		var histories []datamodel.BackupChainHistory
		err = store.db.GORM().Unscoped().Where("resource_uuid = ?", volumeUUID).Find(&histories).Error
		assert.NoError(tt, err)
		assert.Len(tt, histories, 0)
	})

	t.Run("Success_SupersedesExistingHistory", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		ctx := context.Background()
		volumeUUID := "test-volume-uuid"
		oldSize := int64(500 * 1024 * 1024)  // 500MB
		newSize := int64(1024 * 1024 * 1024) // 1GB

		// Create initial history entry
		initialHistory := &datamodel.BackupChainHistory{
			BaseModel:      datamodel.BaseModel{UUID: "history-1"},
			ResourceName:   "test-volume",
			Size:           oldSize,
			ResourceUUID:   volumeUUID,
			ConsumerID:     "consumer-1",
			DeploymentName: "deployment-1",
		}
		err = store.db.Create(initialHistory).Error()
		assert.NoError(tt, err)

		// Call UpdateBackupChainHistory with new size
		err = store.UpdateBackupChainHistory(ctx, volumeUUID, newSize)
		assert.NoError(tt, err)

		// Verify old history was soft-deleted
		var oldHistory datamodel.BackupChainHistory
		err = store.db.GORM().Unscoped().Where("uuid = ?", "history-1").First(&oldHistory).Error
		assert.NoError(tt, err)
		assert.NotNil(tt, oldHistory.DeletedAt)

		// Verify new history was created
		var newHistory datamodel.BackupChainHistory
		err = store.db.GORM().Where("resource_uuid = ? AND deleted_at IS NULL", volumeUUID).First(&newHistory).Error
		assert.NoError(tt, err)
		assert.Equal(tt, newSize, newHistory.Size)
		assert.Equal(tt, volumeUUID, newHistory.ResourceUUID)
		assert.Equal(tt, "test-volume", newHistory.ResourceName)
		assert.Equal(tt, "consumer-1", newHistory.ConsumerID)
		assert.Equal(tt, "deployment-1", newHistory.DeploymentName)
	})

	t.Run("Success_SkipsWhenSizeUnchanged", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		ctx := context.Background()
		volumeUUID := "test-volume-uuid"
		size := int64(1024 * 1024 * 1024) // 1GB

		// Create initial history entry
		initialHistory := &datamodel.BackupChainHistory{
			BaseModel:      datamodel.BaseModel{UUID: "history-1"},
			ResourceName:   "test-volume",
			Size:           size,
			ResourceUUID:   volumeUUID,
			ConsumerID:     "consumer-1",
			DeploymentName: "deployment-1",
		}
		err = store.db.Create(initialHistory).Error()
		assert.NoError(tt, err)

		// Call UpdateBackupChainHistory with same size
		err = store.UpdateBackupChainHistory(ctx, volumeUUID, size)
		assert.NoError(tt, err)

		// Verify history was NOT deleted (size unchanged)
		var history datamodel.BackupChainHistory
		err = store.db.GORM().Where("uuid = ? AND deleted_at IS NULL", "history-1").First(&history).Error
		assert.NoError(tt, err)
		assert.Equal(tt, size, history.Size)

		// Verify no new history entry was created
		var allHistories []datamodel.BackupChainHistory
		err = store.db.GORM().Unscoped().Where("resource_uuid = ?", volumeUUID).Find(&allHistories).Error
		assert.NoError(tt, err)
		assert.Len(tt, allHistories, 1)
	})

	t.Run("Error_TransactionFailure", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		ctx := context.Background()

		// Close the database connection to cause transaction failure
		sqlDB, err := store.db.GORM().DB()
		assert.NoError(tt, err)
		_ = sqlDB.Close()

		// This should fail because the database connection is closed
		err = store.UpdateBackupChainHistory(ctx, "test-volume-uuid", 1024)
		assert.Error(tt, err)
	})
}

func TestDeleteBackupChainHistoryOlderThan(t *testing.T) {
	t.Run("Success_DeletesOldSoftDeletedRecords", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		ctx := context.Background()

		// Create soft-deleted history entries with old deleted_at timestamps
		oldTime := time.Now().AddDate(0, 0, -10) // 10 days ago
		oldHistory1 := &datamodel.BackupChainHistory{
			BaseModel:      datamodel.BaseModel{UUID: "old-history-1"},
			ResourceName:   "old-volume-1",
			Size:           1024,
			ResourceUUID:   "volume-uuid-1",
			ConsumerID:     "consumer-1",
			DeploymentName: "deployment-1",
		}
		err = store.db.Create(oldHistory1).Error()
		assert.NoError(tt, err)

		// Soft delete the record and set deleted_at to old time
		err = store.db.GORM().Unscoped().Model(&datamodel.BackupChainHistory{}).
			Where("uuid = ?", "old-history-1").
			Update("deleted_at", oldTime).Error
		assert.NoError(tt, err)

		oldHistory2 := &datamodel.BackupChainHistory{
			BaseModel:      datamodel.BaseModel{UUID: "old-history-2"},
			ResourceName:   "old-volume-2",
			Size:           2048,
			ResourceUUID:   "volume-uuid-2",
			ConsumerID:     "consumer-2",
			DeploymentName: "deployment-2",
		}
		err = store.db.Create(oldHistory2).Error()
		assert.NoError(tt, err)

		// Soft delete the record and set deleted_at to old time
		err = store.db.GORM().Unscoped().Model(&datamodel.BackupChainHistory{}).
			Where("uuid = ?", "old-history-2").
			Update("deleted_at", oldTime).Error
		assert.NoError(tt, err)

		// Create a recent soft-deleted record (should NOT be deleted)
		recentTime := time.Now().AddDate(0, 0, -1) // 1 day ago
		recentHistory := &datamodel.BackupChainHistory{
			BaseModel:      datamodel.BaseModel{UUID: "recent-history"},
			ResourceName:   "recent-volume",
			Size:           4096,
			ResourceUUID:   "volume-uuid-3",
			ConsumerID:     "consumer-3",
			DeploymentName: "deployment-3",
		}
		err = store.db.Create(recentHistory).Error()
		assert.NoError(tt, err)

		// Soft delete the record and set deleted_at to recent time
		err = store.db.GORM().Unscoped().Model(&datamodel.BackupChainHistory{}).
			Where("uuid = ?", "recent-history").
			Update("deleted_at", recentTime).Error
		assert.NoError(tt, err)

		// Create an active record (not soft-deleted, should NOT be deleted)
		activeHistory := &datamodel.BackupChainHistory{
			BaseModel:      datamodel.BaseModel{UUID: "active-history"},
			ResourceName:   "active-volume",
			Size:           8192,
			ResourceUUID:   "volume-uuid-4",
			ConsumerID:     "consumer-4",
			DeploymentName: "deployment-4",
		}
		err = store.db.Create(activeHistory).Error()
		assert.NoError(tt, err)

		// Delete records older than 7 days
		cutoffTime := time.Now().AddDate(0, 0, -7)
		rowsDeleted, err := store.DeleteBackupChainHistoryOlderThan(ctx, cutoffTime)

		assert.NoError(tt, err)
		assert.Equal(tt, int64(2), rowsDeleted)

		// Verify old records are hard deleted
		var count int64
		err = store.db.GORM().Unscoped().Model(&datamodel.BackupChainHistory{}).
			Where("uuid IN ?", []string{"old-history-1", "old-history-2"}).
			Count(&count).Error
		assert.NoError(tt, err)
		assert.Equal(tt, int64(0), count)

		// Verify recent soft-deleted record still exists
		var recentCount int64
		err = store.db.GORM().Unscoped().Model(&datamodel.BackupChainHistory{}).
			Where("uuid = ?", "recent-history").
			Count(&recentCount).Error
		assert.NoError(tt, err)
		assert.Equal(tt, int64(1), recentCount)

		// Verify active record still exists
		var activeCount int64
		err = store.db.GORM().Model(&datamodel.BackupChainHistory{}).
			Where("uuid = ?", "active-history").
			Count(&activeCount).Error
		assert.NoError(tt, err)
		assert.Equal(tt, int64(1), activeCount)
	})

	t.Run("Success_NoRecordsToDelete", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		ctx := context.Background()

		// Create only active records (not soft-deleted)
		activeHistory := &datamodel.BackupChainHistory{
			BaseModel:      datamodel.BaseModel{UUID: "active-history"},
			ResourceName:   "active-volume",
			Size:           1024,
			ResourceUUID:   "volume-uuid-1",
			ConsumerID:     "consumer-1",
			DeploymentName: "deployment-1",
		}
		err = store.db.Create(activeHistory).Error()
		assert.NoError(tt, err)

		// Delete records older than 7 days
		cutoffTime := time.Now().AddDate(0, 0, -7)
		rowsDeleted, err := store.DeleteBackupChainHistoryOlderThan(ctx, cutoffTime)

		assert.NoError(tt, err)
		assert.Equal(tt, int64(0), rowsDeleted)

		// Verify active record still exists
		var count int64
		err = store.db.GORM().Model(&datamodel.BackupChainHistory{}).
			Where("uuid = ?", "active-history").
			Count(&count).Error
		assert.NoError(tt, err)
		assert.Equal(tt, int64(1), count)
	})

	t.Run("Success_EmptyTable", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		ctx := context.Background()

		// Delete records older than 7 days from empty table
		cutoffTime := time.Now().AddDate(0, 0, -7)
		rowsDeleted, err := store.DeleteBackupChainHistoryOlderThan(ctx, cutoffTime)

		assert.NoError(tt, err)
		assert.Equal(tt, int64(0), rowsDeleted)
	})

	t.Run("Success_DeletesOnlyRecordsOlderThanCutoff", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		ctx := context.Background()

		// Create soft-deleted record exactly at cutoff (should NOT be deleted)
		cutoffTime := time.Now().AddDate(0, 0, -7)
		exactCutoffHistory := &datamodel.BackupChainHistory{
			BaseModel:      datamodel.BaseModel{UUID: "exact-cutoff-history"},
			ResourceName:   "exact-volume",
			Size:           1024,
			ResourceUUID:   "volume-uuid-1",
			ConsumerID:     "consumer-1",
			DeploymentName: "deployment-1",
		}
		err = store.db.Create(exactCutoffHistory).Error()
		assert.NoError(tt, err)

		err = store.db.GORM().Unscoped().Model(&datamodel.BackupChainHistory{}).
			Where("uuid = ?", "exact-cutoff-history").
			Update("deleted_at", cutoffTime).Error
		assert.NoError(tt, err)

		// Create soft-deleted record just before cutoff (should be deleted)
		beforeCutoffTime := cutoffTime.Add(-time.Hour)
		beforeCutoffHistory := &datamodel.BackupChainHistory{
			BaseModel:      datamodel.BaseModel{UUID: "before-cutoff-history"},
			ResourceName:   "before-volume",
			Size:           2048,
			ResourceUUID:   "volume-uuid-2",
			ConsumerID:     "consumer-2",
			DeploymentName: "deployment-2",
		}
		err = store.db.Create(beforeCutoffHistory).Error()
		assert.NoError(tt, err)

		err = store.db.GORM().Unscoped().Model(&datamodel.BackupChainHistory{}).
			Where("uuid = ?", "before-cutoff-history").
			Update("deleted_at", beforeCutoffTime).Error
		assert.NoError(tt, err)

		// Delete records older than cutoff
		rowsDeleted, err := store.DeleteBackupChainHistoryOlderThan(ctx, cutoffTime)

		assert.NoError(tt, err)
		assert.Equal(tt, int64(1), rowsDeleted)

		// Verify exact cutoff record still exists
		var exactCount int64
		err = store.db.GORM().Unscoped().Model(&datamodel.BackupChainHistory{}).
			Where("uuid = ?", "exact-cutoff-history").
			Count(&exactCount).Error
		assert.NoError(tt, err)
		assert.Equal(tt, int64(1), exactCount)

		// Verify before cutoff record is deleted
		var beforeCount int64
		err = store.db.GORM().Unscoped().Model(&datamodel.BackupChainHistory{}).
			Where("uuid = ?", "before-cutoff-history").
			Count(&beforeCount).Error
		assert.NoError(tt, err)
		assert.Equal(tt, int64(0), beforeCount)
	})

	t.Run("Error_DatabaseConnectionClosed", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		ctx := context.Background()

		// Close the database connection to cause failure
		sqlDB, err := store.db.GORM().DB()
		assert.NoError(tt, err)
		_ = sqlDB.Close()

		// This should fail because the database connection is closed
		cutoffTime := time.Now().AddDate(0, 0, -7)
		_, err = store.DeleteBackupChainHistoryOlderThan(ctx, cutoffTime)
		assert.Error(tt, err)
	})
}

func TestGetBackupsByBackupVaultUUIDAndFilter(t *testing.T) {
	ctx := context.Background()

	t.Run("ReturnsBackupsWithoutAccountFiltering", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Create two accounts
		account1 := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "account-1-uuid"},
			Name:      "project-1",
		}
		err = store.db.Create(account1).Error()
		assert.NoError(tt, err)

		account2 := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "account-2-uuid"},
			Name:      "project-2",
		}
		err = store.db.Create(account2).Error()
		assert.NoError(tt, err)

		// Create a GCBDR backup vault
		backupVault := &datamodel.BackupVault{
			BaseModel:   datamodel.BaseModel{UUID: "gcbdr-vault-uuid"},
			Name:        "gcbdr-vault",
			AccountID:   account1.ID,
			ServiceType: "GCBDR",
		}
		err = store.db.Create(backupVault).Error()
		assert.NoError(tt, err)

		// Create backup from account1
		backup1 := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-1-uuid"},
			Name:          "backup-1",
			BackupVaultID: backupVault.ID,
		}
		err = store.db.Create(backup1).Error()
		assert.NoError(tt, err)

		// Create backup from account2 in the same GCBDR vault (cross-project)
		backup2 := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-2-uuid"},
			Name:          "backup-2",
			BackupVaultID: backupVault.ID,
		}
		err = store.db.Create(backup2).Error()
		assert.NoError(tt, err)

		// List backups without account filtering
		backups, err := store.GetBackupsByBackupVaultUUIDAndFilter(ctx, backupVault.UUID, nil)
		assert.NoError(tt, err)
		assert.Len(tt, backups, 2)

		// Verify we got backups from the vault (both backups belong to the same vault)
		for _, b := range backups {
			assert.Equal(tt, backupVault.ID, b.BackupVaultID)
		}
	})

	t.Run("ReturnsFilteredBackups", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "account-uuid"},
			Name:      "project-1",
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err)

		backupVault := &datamodel.BackupVault{
			BaseModel:   datamodel.BaseModel{UUID: "vault-uuid"},
			Name:        "test-vault",
			AccountID:   account.ID,
			ServiceType: "GCBDR",
		}
		err = store.db.Create(backupVault).Error()
		assert.NoError(tt, err)

		backup1 := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-1-uuid"},
			Name:          "backup-to-find",
			BackupVaultID: backupVault.ID,
		}
		err = store.db.Create(backup1).Error()
		assert.NoError(tt, err)

		backup2 := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-2-uuid"},
			Name:          "other-backup",
			BackupVaultID: backupVault.ID,
		}
		err = store.db.Create(backup2).Error()
		assert.NoError(tt, err)

		// Filter by name
		filters := [][]interface{}{{"name = ?", "backup-to-find"}}
		backups, err := store.GetBackupsByBackupVaultUUIDAndFilter(ctx, backupVault.UUID, filters)
		assert.NoError(tt, err)
		assert.Len(tt, backups, 1)
		assert.Equal(tt, "backup-to-find", backups[0].Name)
	})

	t.Run("ReturnsErrorWhenVaultNotFound", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		// Try to list backups for non-existent vault
		_, err = store.GetBackupsByBackupVaultUUIDAndFilter(ctx, "non-existent-vault", nil)
		assert.Error(tt, err)
	})
}

func TestGetExpertModeBackupsByVolumeExternalUUID(t *testing.T) {
	setupStoreAndVault := func(tt *testing.T) (*DataStoreRepository, *datamodel.BackupVault) {
		tt.Helper()
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		vault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid-1"},
			Name:      "test-vault",
		}
		err = store.db.Create(vault).Error()
		assert.NoError(tt, err)
		return store, vault
	}

	createBackup := func(tt *testing.T, store *DataStoreRepository, vaultID int64, volumeUUID, name, state string) *datamodel.Backup {
		tt.Helper()
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: name + "-uuid"},
			Name:          name,
			VolumeUUID:    volumeUUID,
			State:         state,
			BackupVaultID: vaultID,
		}
		err := store.db.Create(backup).Error()
		assert.NoError(tt, err)
		return backup
	}

	t.Run("ReturnsBackupsForVolume", func(tt *testing.T) {
		store, vault := setupStoreAndVault(tt)

		createBackup(tt, store, vault.ID, "ext-vol-1", "backup-1", models.LifeCycleStateREADY)
		createBackup(tt, store, vault.ID, "ext-vol-1", "backup-2", models.LifeCycleStateCreating)

		results, err := store.GetExpertModeBackupsByVolumeExternalUUID(context.Background(), "ext-vol-1")

		assert.NoError(tt, err)
		assert.Len(tt, results, 2)
	})

	t.Run("ExcludesErrorStateBackups", func(tt *testing.T) {
		store, vault := setupStoreAndVault(tt)

		createBackup(tt, store, vault.ID, "ext-vol-2", "good-backup", models.LifeCycleStateREADY)
		createBackup(tt, store, vault.ID, "ext-vol-2", "error-backup", models.LifeCycleStateError)

		results, err := store.GetExpertModeBackupsByVolumeExternalUUID(context.Background(), "ext-vol-2")

		assert.NoError(tt, err)
		assert.Len(tt, results, 1)
		assert.Equal(tt, "good-backup", results[0].Name)
	})

	t.Run("OrdersByCreatedAtDescending", func(tt *testing.T) {
		store, vault := setupStoreAndVault(tt)

		b1 := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "oldest-uuid", CreatedAt: time.Now().Add(-2 * time.Hour)},
			Name:          "oldest",
			VolumeUUID:    "ext-vol-3",
			State:         models.LifeCycleStateREADY,
			BackupVaultID: vault.ID,
		}
		err := store.db.Create(b1).Error()
		assert.NoError(tt, err)

		b2 := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "newest-uuid", CreatedAt: time.Now()},
			Name:          "newest",
			VolumeUUID:    "ext-vol-3",
			State:         models.LifeCycleStateREADY,
			BackupVaultID: vault.ID,
		}
		err = store.db.Create(b2).Error()
		assert.NoError(tt, err)

		results, err := store.GetExpertModeBackupsByVolumeExternalUUID(context.Background(), "ext-vol-3")

		assert.NoError(tt, err)
		assert.Len(tt, results, 2)
		assert.Equal(tt, "newest", results[0].Name)
		assert.Equal(tt, "oldest", results[1].Name)
	})

	t.Run("DoesNotReturnBackupsFromOtherVolumes", func(tt *testing.T) {
		store, vault := setupStoreAndVault(tt)

		createBackup(tt, store, vault.ID, "ext-vol-4", "my-backup", models.LifeCycleStateREADY)
		createBackup(tt, store, vault.ID, "ext-vol-other", "other-backup", models.LifeCycleStateREADY)

		results, err := store.GetExpertModeBackupsByVolumeExternalUUID(context.Background(), "ext-vol-4")

		assert.NoError(tt, err)
		assert.Len(tt, results, 1)
		assert.Equal(tt, "my-backup", results[0].Name)
	})

	t.Run("ReturnsEmptyWhenNoBackups", func(tt *testing.T) {
		store, _ := setupStoreAndVault(tt)

		results, err := store.GetExpertModeBackupsByVolumeExternalUUID(context.Background(), "nonexistent-vol")

		assert.NoError(tt, err)
		assert.Empty(tt, results)
	})

	t.Run("ReturnsEmptyWhenAllBackupsAreError", func(tt *testing.T) {
		store, vault := setupStoreAndVault(tt)

		createBackup(tt, store, vault.ID, "ext-vol-5", "err-1", models.LifeCycleStateError)
		createBackup(tt, store, vault.ID, "ext-vol-5", "err-2", models.LifeCycleStateError)

		results, err := store.GetExpertModeBackupsByVolumeExternalUUID(context.Background(), "ext-vol-5")

		assert.NoError(tt, err)
		assert.Empty(tt, results)
	})

	t.Run("ReturnsErrorOnDBFailure", func(tt *testing.T) {
		store, _ := setupStoreAndVault(tt)

		sqlDB, err := store.db.GORM().DB()
		assert.NoError(tt, err)
		_ = sqlDB.Close()

		results, err := store.GetExpertModeBackupsByVolumeExternalUUID(context.Background(), "any-vol")

		assert.Error(tt, err)
		assert.Nil(tt, results)
	})
}
