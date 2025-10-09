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

func TestGetBackupLogicalSizeMetrics(t *testing.T) {
	t.Run("ReturnsLatestBackupForEachVolumeSuccessfully", func(tt *testing.T) {
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
		results, err := store.GetBackupLogicalSizeMetrics(context.Background())
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

		assert.NotNil(tt, volume2Backup)
		assert.Equal(tt, "backup-uuid-3", volume2Backup.UUID)
		assert.Equal(tt, "backup-3", volume2Backup.Name)
		assert.Equal(tt, "volume-uuid-2", volume2Backup.VolumeUUID)
		assert.Equal(tt, int64(4096), volume2Backup.LatestLogicalBackupSize)
		assert.NotNil(tt, volume2Backup.Attributes)
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
		results, err := store.GetBackupLogicalSizeMetrics(context.Background())
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
		results, err := store.GetBackupLogicalSizeMetrics(context.Background())
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
		results, err := store.GetBackupLogicalSizeMetrics(context.Background())
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
		results, err := store.GetBackupLogicalSizeMetrics(context.Background())
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
