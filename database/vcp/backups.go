package database

import (
	"context"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"gorm.io/gorm"
)

var (
	deleteBackup                         = _deleteBackup
	getBackupWithDetails                 = _getBackupWithDetails
	getBackupVaultByNameAndBackupVaultID = _getBackupVaultByNameAndBackupVaultID
)

const (
	BackupTypeScheduled = "SCHEDULED"
	Daily               = "daily"
	Weekly              = "weekly"
	Monthly             = "monthly"
)

func (d *DataStoreRepository) GetBackupByNameAndBackupVaultID(ctx context.Context, backupName string, backupVaultID int64) (*datamodel.Backup, error) {
	return getBackupVaultByNameAndBackupVaultID(d.db.GORM().WithContext(ctx), &datamodel.Backup{Name: backupName})
}

func _getBackupVaultByNameAndBackupVaultID(db *gorm.DB, query *datamodel.Backup) (*datamodel.Backup, error) {
	backup := &datamodel.Backup{}
	err := db.Preload("BackupVault").First(&backup, query).Error
	if err != nil {
		return nil, customerrors.ConvertToNotFoundErrIfContainsMessage(err, "record not found", "backup", &backup.UUID)
	}
	return backup, nil
}

func (d *DataStoreRepository) CreateBackup(ctx context.Context, backup *datamodel.Backup) (*datamodel.Backup, error) {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}
	defer commitOrRollbackOnError(util.GetLogger(ctx), tx, &err)

	if tx.Where("name = ?", backup.Name).Where("backup_vault_id = ?", backup.BackupVaultID).First(&backup).Error != nil {
		backup.UUID = utils.RandomUUID()
		backup.State = models.LifeCycleStateCreating
		backup.StateDetails = models.LifeCycleStateCreatingDetails
		backup.CreatedAt = time.Now()
		backup.UpdatedAt = backup.CreatedAt

		err := tx.Create(backup).Error
		if err != nil {
			return nil, err
		}

		dbBackup, err := getBackupWithDetails(tx, &datamodel.Backup{BaseModel: datamodel.BaseModel{UUID: backup.UUID}})
		if err != nil {
			return nil, err
		}
		return dbBackup, nil
	}
	return nil, customerrors.NewUserInputValidationErr("backup already exists")
}

func _getBackupWithDetails(db *gorm.DB, query *datamodel.Backup) (*datamodel.Backup, error) {
	backup := &datamodel.Backup{}
	err := db.Preload("BackupVault").First(&backup, query).Error
	if err != nil {
		return nil, customerrors.ConvertToNotFoundErrIfContainsMessage(err, "record not found", "backup", &backup.UUID)
	}
	return backup, nil
}

func (d *DataStoreRepository) GetBackupCountByBackupVaultID(ctx context.Context, backupVaultID int64) (int64, error) {
	var count int64
	err := d.db.GORM().WithContext(ctx).Model(&datamodel.Backup{}).Where("backup_vault_id = ?", backupVaultID).Count(&count).Error
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (d *DataStoreRepository) GetVolumeCountByBackupVaultID(ctx context.Context, backupVaultUUID string) (int64, error) {
	var volumeCount int64
	err := d.db.GORM().WithContext(ctx).Model(&datamodel.Volume{}).
		Where("data_protection->>'backup_vault_id' = ?", backupVaultUUID).
		Count(&volumeCount).Error
	if err != nil {
		return 0, err
	}
	return volumeCount, nil
}

func (d *DataStoreRepository) GetBackupsByBackupVaultOwnerIDAndFilter(ctx context.Context, backupVaultUUID string, accountID int64, filters [][]interface{}) ([]*datamodel.Backup, error) {
	bv, err := d.GetBackupVaultByUUIDndOwnerID(ctx, backupVaultUUID, accountID)
	if err != nil {
		if customerrors.IsNotFoundErr(err) {
			return nil, customerrors.NewNotFoundErr("backup vault", nil)
		}
		return nil, err
	}
	// If no filters are provided, fetch all backups for the backup vault
	if len(filters) == 0 {
		return getBackupsByBackupVault(d.db.GORM().WithContext(ctx), bv.ID)
	}
	return getBackupsByBackupVault(d.db.ApplyFilter(filters).GORM().WithContext(ctx), bv.ID)
}

func getBackupsByBackupVault(db *gorm.DB, backupVaultUUID int64) ([]*datamodel.Backup, error) {
	var backups []*datamodel.Backup

	err := db.Preload("BackupVault").Where("backup_vault_id = ?", backupVaultUUID).Find(&backups).Error
	if err != nil {
		return nil, err
	}

	return backups, nil
}

func (d *DataStoreRepository) GetBackup(ctx context.Context, backupVaultUUID string, backupUUID string, accountName string) (*datamodel.Backup, error) {
	return getBackup(d.db.GORM().WithContext(ctx), backupVaultUUID, backupUUID, accountName)
}

func getBackup(db *gorm.DB, backupVaultUUID string, backupUUID string, accountName string) (*datamodel.Backup, error) {
	// Retrieve the backup vault details using the backupVaultUUID and account
	backupVault, err := getBackupVaultWithDetails(db, &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{UUID: backupVaultUUID},
		Account: &datamodel.Account{
			Name: accountName,
		},
	})
	if err != nil {
		return nil, err
	}
	if backupVault.Account.Name != accountName {
		return nil, customerrors.NewNotFoundErr("backup vault", &backupVaultUUID)
	}

	// Retrieve the backup using the backupVaultUUID and backupUUID
	var backup *datamodel.Backup
	backup, err = getBackupWithDetails(db, &datamodel.Backup{
		BaseModel:     datamodel.BaseModel{UUID: backupUUID},
		BackupVaultID: backupVault.ID,
	})
	if err != nil {
		return nil, err
	}

	return backup, nil
}

func (d *DataStoreRepository) IsBackupInCreatingorDeletingStateByVolume(ctx context.Context, volumeUUID string) (bool, error) {
	return isBackupInCreatingorDeletingStateByVolume(d.db.GORM().WithContext(ctx), volumeUUID)
}

func isBackupInCreatingorDeletingStateByVolume(db *gorm.DB, volumeUUID string) (bool, error) {
	var backups int64
	err := db.Model(&datamodel.Backup{}).Where("volume_uuid = ?", volumeUUID).Where("state = ? OR state = ?", models.LifeCycleStateCreating, models.LifeCycleStateDeleting).Count(&backups).Error

	if err != nil && err == gorm.ErrRecordNotFound {
		return false, nil
	}
	if backups > 0 {
		return true, nil
	}
	return false, err
}

func (d *DataStoreRepository) DeleteBackup(ctx context.Context, backupUUID string) (*datamodel.Backup, error) {
	return deleteBackup(ctx, d.db.GORM().WithContext(ctx), backupUUID)
}

func _deleteBackup(ctx context.Context, db *gorm.DB, backupUUID string) (*datamodel.Backup, error) {
	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}
	defer commitOrRollbackOnError(util.GetLogger(ctx), tx, &err)
	backup, err := getBackupWithDetails(tx, &datamodel.Backup{BaseModel: datamodel.BaseModel{UUID: backupUUID}})
	if err != nil {
		return nil, err
	}
	backup.DeletedAt = &gorm.DeletedAt{Time: time.Now(), Valid: true}
	backup.State = models.LifeCycleStateDeleted
	backup.StateDetails = ""
	err = tx.Save(backup).Error
	if err != nil {
		return nil, err
	}

	return backup, nil
}

func (d *DataStoreRepository) FinishBackup(ctx context.Context, backup *datamodel.Backup) (*datamodel.Backup, error) {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}
	defer commitOrRollbackOnError(util.GetLogger(ctx), tx, &err)
	dbBackup, err := getBackupWithDetails(tx, &datamodel.Backup{BaseModel: datamodel.BaseModel{UUID: backup.UUID}})
	if err != nil {
		return nil, err
	}

	err = tx.Model(&dbBackup).Updates(datamodel.Backup{
		Description:  backup.Description,
		State:        models.LifeCycleStateAvailable,
		StateDetails: models.LifeCycleStateAvailableDetails,
		Attributes:   backup.Attributes,
	}).Error
	if err != nil {
		return nil, err
	}

	return dbBackup, nil
}

func (d *DataStoreRepository) UpdateBackup(ctx context.Context, backup *datamodel.Backup) (*datamodel.Backup, error) {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}
	defer commitOrRollbackOnError(util.GetLogger(ctx), tx, &err)
	dbBackup, err := getBackupWithDetails(tx, &datamodel.Backup{BaseModel: datamodel.BaseModel{UUID: backup.UUID}})
	if err != nil {
		return nil, err
	}

	// Prepare update fields
	updateFields := datamodel.Backup{
		Description: backup.Description,
	}

	updateFields.State = models.LifeCycleStateAvailable
	updateFields.StateDetails = models.LifeCycleStateAvailableDetails

	err = tx.Model(&dbBackup).Updates(updateFields).Error
	if err != nil {
		return nil, err
	}

	return dbBackup, nil
}
func (d *DataStoreRepository) UpdateBackupState(ctx context.Context, backup *datamodel.Backup) (*datamodel.Backup, error) {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}
	defer commitOrRollbackOnError(util.GetLogger(ctx), tx, &err)
	dbBackup, err := getBackupWithDetails(tx, &datamodel.Backup{BaseModel: datamodel.BaseModel{UUID: backup.UUID}})
	if err != nil {
		return nil, err
	}

	err = tx.Model(&dbBackup).Updates(datamodel.Backup{
		State:        backup.State,
		StateDetails: backup.StateDetails,
	}).Error
	if err != nil {
		return nil, err
	}
	return dbBackup, nil
}

func (d *DataStoreRepository) IsLatestBackup(ctx context.Context, backupUUID, volumeUUID string) (bool, error) {
	db := d.db.GORM().WithContext(ctx)
	backup := &datamodel.Backup{}
	// get backup by created_at timestamp under a volume
	err := db.Where("volume_uuid = ? and state = ?", volumeUUID, models.LifeCycleStateAvailable).Order("created_at desc").First(&backup).Error
	if err != nil {
		return false, err
	}
	// check if the backup is latest
	if backup.UUID == backupUUID {
		return true, nil
	}
	return false, nil
}

func (d *DataStoreRepository) BackupCountByVolumeID(ctx context.Context, volumeUUID string) (int64, error) {
	db := d.db.GORM().WithContext(ctx)
	var count int64
	err := db.Model(&datamodel.Backup{}).Where("volume_uuid = ? and state != ?", volumeUUID, models.LifeCycleStateError).Count(&count).Error
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (d *DataStoreRepository) FetchScheduledBackupsForDeletion(ctx context.Context, volume *datamodel.Volume, backupPolicy *datamodel.BackupPolicy) ([]*datamodel.Backup, error) {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}
	defer commitOrRollbackOnError(util.GetLogger(ctx), tx, &err)

	if volume.DataProtection == nil || volume.DataProtection.BackupPolicyID == "" {
		return nil, errors.New("volume does not have a backup policy associated with it")
	}

	var allBackups []*datamodel.Backup

	var dailyBackups []*datamodel.Backup
	err = tx.Where("volume_uuid = ?", volume.UUID).
		Where("type = ?", BackupTypeScheduled).
		Where("schedule_tag = ?", Daily).
		Order("id desc").
		Offset(int(backupPolicy.DailyBackupsToKeep)).
		Find(&dailyBackups).Error
	if err != nil {
		return nil, err
	}
	allBackups = append(allBackups, dailyBackups...)

	var weeklyBackups []*datamodel.Backup
	err = tx.Where("volume_uuid = ?", volume.UUID).
		Where("type = ?", BackupTypeScheduled).
		Where("schedule_tag = ?", Weekly).
		Order("id desc").
		Offset(int(backupPolicy.WeeklyBackupsToKeep)).
		Find(&weeklyBackups).Error
	if err != nil {
		return nil, err
	}
	allBackups = append(allBackups, weeklyBackups...)

	var monthlyBackups []*datamodel.Backup
	err = tx.Where("volume_uuid = ?", volume.UUID).
		Where("type = ?", BackupTypeScheduled).
		Where("schedule_tag = ?", Monthly).
		Order("id desc").
		Offset(int(backupPolicy.MonthlyBackupsToKeep)).
		Find(&monthlyBackups).Error
	if err != nil {
		return nil, err
	}
	allBackups = append(allBackups, monthlyBackups...)

	return allBackups, nil
}

func (d *DataStoreRepository) IsBackupShared(ctx context.Context, backup *datamodel.Backup) (bool, error) {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return false, err
	}
	defer commitOrRollbackOnError(util.GetLogger(ctx), tx, &err)

	// Check if the backup is shared by looking for any other backup with the same snapshot ID
	var count int64
	err = tx.Model(&datamodel.Backup{}).
		Where("attributes->>'snapshot_id' = ? AND uuid != ?", backup.Attributes.SnapshotID, backup.UUID).
		Count(&count).Error
	if err != nil {
		return false, err
	}

	return count > 0, nil
}
