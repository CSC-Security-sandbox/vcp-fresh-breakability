package repository

import (
	"context"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"gorm.io/gorm"
)

var (
	deleteBackup         = _deleteBackup
	getBackupWithDetails = _getBackupWithDetails
)

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

func (d *DataStoreRepository) GetBackup(ctx context.Context, backupUUID string) (*datamodel.Backup, error) {
	return getBackupWithDetails(d.db.GORM().WithContext(ctx), &datamodel.Backup{BaseModel: datamodel.BaseModel{UUID: backupUUID}})
}

func _getBackupWithDetails(db *gorm.DB, query *datamodel.Backup) (*datamodel.Backup, error) {
	backup := &datamodel.Backup{}
	err := db.Preload("BackupVault").First(&backup, query).Error
	if err != nil {
		return nil, customerrors.ConvertToNotFoundErrIfContainsMessage(err, "record not found", "backup", &backup.UUID)
	}
	return backup, nil
}

func (d *DataStoreRepository) GetBackupsByBackupVault(ctx context.Context, backupVaultUUID string) ([]*datamodel.Backup, error) {
	return getBackupsByBackupVault(d.db.GORM().WithContext(ctx), backupVaultUUID)
}

func getBackupsByBackupVault(db *gorm.DB, backupVaultUUID string) ([]*datamodel.Backup, error) {
	var backups []*datamodel.Backup
	err := db.Preload("BackupVault").Where("backup_vault_id = ?", backupVaultUUID).Find(&backups).Error
	if err != nil {
		return nil, err
	}

	return backups, nil
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

	err = tx.Model(&dbBackup).Updates(datamodel.Backup{
		Description: backup.Description,
	}).Error
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
