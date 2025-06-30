package repository

import (
	"context"
	"errors"
	"gorm.io/gorm"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/gorm"
)

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
		}
		err = store.db.Create(backup).Error()
		assert.NoError(tt, err)

		backup.Description = "Updated description"
		backup.Attributes = &datamodel.BackupAttributes{}

		finishedBackup, err := store.FinishBackup(context.Background(), backup)
		assert.NoError(tt, err)
		assert.Equal(tt, models.LifeCycleStateAvailable, finishedBackup.State)
		assert.Equal(tt, models.LifeCycleStateAvailableDetails, finishedBackup.StateDetails)
		assert.Equal(tt, "Updated description", finishedBackup.Description)
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
			State:        models.LifeCycleStateAvailable,
			StateDetails: models.LifeCycleStateAvailableDetails,
			VolumeUUID:   "volume1",
		}
		err = store.db.Create(backup2).Error()
		assert.NoError(tt, err)

		count, err := store.BackupCountByVolumeID(context.Background(), "volume1")
		assert.NoError(tt, err)
		assert.Equal(tt, int64(2), count)
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
