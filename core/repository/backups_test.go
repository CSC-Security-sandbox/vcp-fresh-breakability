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

		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{UUID: "test-backup-uuid"},
		}
		err = store.db.Create(backup).Error()
		assert.NoError(tt, err)

		result, err := store.GetBackup(context.Background(), backup.UUID)
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

		_, err = store.GetBackup(context.Background(), "non-existent-uuid")
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
		_, err = store.GetBackup(context.Background(), "test-uuid")
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

func TestGetBackup_EdgeCases(t *testing.T) {
	t.Run("ReturnsErrorWhenBackupNotFound", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)

		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		_, err = store.GetBackup(context.Background(), "non-existent-uuid")
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "not found")
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

		backup := &datamodel.Backup{
			Name:          "backup-vault",
			BackupVaultID: 123,
		}
		err = store.db.Create(backup).Error()
		assert.NoError(tt, err)

		backups, err := store.GetBackupsByBackupVault(context.Background(), "123")
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

		backups, err := store.GetBackupsByBackupVault(context.Background(), "non-existent-vault")
		assert.NoError(tt, err)
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
