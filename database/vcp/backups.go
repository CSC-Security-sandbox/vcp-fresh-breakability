package database

import (
	"context"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/common"
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
	OntapFgVolumeStyle  = "flexgroup"
)

// backupChainHistoryParams holds parameters for creating/updating backup chain history
type backupChainHistoryParams struct {
	ResourceName   string
	VolumeUUID     string
	Size           int64
	ConsumerID     string
	DeploymentName string
	Timestamp      time.Time
}

type BackupMetricsData struct {
	UUID          string                      `gorm:"column:uuid"`
	VolumeUUID    string                      `gorm:"column:volume_uuid"`
	Attributes    *datamodel.BackupAttributes `gorm:"column:attributes;type:jsonb"`
	BackupVaultID int64                       `gorm:"column:backup_vault_id"`
	// BackupVault fields (from JOIN)
	VaultAccountID int64  `gorm:"column:vault_account_id"`
	VaultName      string `gorm:"column:vault_name"`
}

// createBackupChainHistoryEntry creates a new backup chain history entry
func createBackupChainHistoryEntry(tx *gorm.DB, params backupChainHistoryParams) error {
	history := &datamodel.BackupChainHistory{
		BaseModel: datamodel.BaseModel{
			UUID:      utils.RandomUUID(),
			CreatedAt: params.Timestamp,
			UpdatedAt: params.Timestamp,
			DeletedAt: nil,
		},
		ResourceName:   params.ResourceName,
		ResourceUUID:   params.VolumeUUID,
		Size:           params.Size,
		ConsumerID:     params.ConsumerID,
		DeploymentName: params.DeploymentName,
	}
	return tx.Create(history).Error
}

// supersedePreviousBackupChainHistory marks old entries as deleted and creates a new one if size changed
func supersedePreviousBackupChainHistory(ctx context.Context, tx *gorm.DB, volumeUUID string, newSize int64, now time.Time) error {
	var currentHistory datamodel.BackupChainHistory
	err := tx.Where("resource_uuid = ?", volumeUUID).
		Where("deleted_at IS NULL").
		First(&currentHistory).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil // No history to supersede
		}
		return err
	}

	// Skip if size unchanged
	if currentHistory.Size == newSize {
		util.GetLogger(ctx).Debugf("Backup chain history size unchanged for volume %s (size: %d)", volumeUUID, newSize)
		return nil
	}

	// Mark current as deleted
	err = tx.Model(&datamodel.BackupChainHistory{}).
		Where("id = ?", currentHistory.ID).
		Update("deleted_at", now).Error
	if err != nil {
		return err
	}

	// Create new entry with updated size
	err = createBackupChainHistoryEntry(tx, backupChainHistoryParams{
		ResourceName:   currentHistory.ResourceName,
		VolumeUUID:     volumeUUID,
		Size:           newSize,
		ConsumerID:     currentHistory.ConsumerID,
		DeploymentName: currentHistory.DeploymentName,
		Timestamp:      now,
	})
	if err != nil {
		return err
	}

	util.GetLogger(ctx).Infof("Updated backup chain history for volume %s: old size %d -> new size %d",
		volumeUUID, currentHistory.Size, newSize)
	return nil
}

func (d *DataStoreRepository) GetBackupByNameAndBackupVaultID(ctx context.Context, backupName string, backupVaultID int64) (*datamodel.Backup, error) {
	return getBackupVaultByNameAndBackupVaultID(d.db.GORM().WithContext(ctx), &datamodel.Backup{Name: backupName, BackupVaultID: backupVaultID})
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

		// Handle backup chain history: mark previous entries as deleted and create new entry
		// Mark previous backup chain history entries for this volume as deleted
		err = markPreviousBackupChainHistoryAsDeleted(tx, dbBackup.VolumeUUID, dbBackup.CreatedAt)
		if err != nil {
			util.GetLogger(ctx).Warnf("Failed to mark previous backup chain history as deleted for volume %s: %v", dbBackup.VolumeUUID, err)
			// Don't fail the entire backup creation if history update fails
		}

		volumeName := ""
		// Create backup chain history entry for this new backup
		if dbBackup.Attributes != nil && dbBackup.Attributes.VolumeName != "" {
			volumeName = dbBackup.Attributes.VolumeName
		}
		deploymentName := ""
		if dbBackup.BackupVault != nil {
			deploymentName = dbBackup.BackupVault.Name
		}

		consumerID := ""
		if dbBackup.Attributes != nil {
			consumerID = dbBackup.Attributes.AccountIdentifier
		}

		err = createBackupChainHistoryEntry(tx, backupChainHistoryParams{
			ResourceName:   volumeName,
			VolumeUUID:     dbBackup.VolumeUUID,
			Size:           0, // Will be updated later when backup completes
			ConsumerID:     consumerID,
			DeploymentName: deploymentName,
			Timestamp:      dbBackup.CreatedAt,
		})
		if err != nil {
			util.GetLogger(ctx).Warnf("Failed to create backup chain history for backup %s: %v", backup.UUID, err)
			// Don't fail the entire backup creation if history creation fails
		}

		return dbBackup, nil
	}
	return nil, customerrors.NewUserInputValidationErr("backup already exists")
}

// markPreviousBackupChainHistoryAsDeleted marks backup chain history entries as deleted for a volume
// Can be used when creating new backups or when deleting the last backup for a volume
func markPreviousBackupChainHistoryAsDeleted(tx *gorm.DB, volumeUUID string, timeStamp time.Time) error {
	return tx.Model(&datamodel.BackupChainHistory{}).
		Where("resource_uuid = ?", volumeUUID).
		Where("deleted_at IS NULL").
		Update("deleted_at", timeStamp).Error
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
	var expertModeVolumeCount int64
	err := d.db.GORM().WithContext(ctx).Model(&datamodel.Volume{}).
		Where("data_protection->>'backup_vault_id' = ?", backupVaultUUID).
		Count(&volumeCount).Error
	if err != nil {
		return 0, err
	}

	// fetch count from expert mode volumes as well
	err = d.db.GORM().WithContext(ctx).Model(&datamodel.ExpertModeVolumes{}).
		Where("data_protection->>'backup_vault_id' = ?", backupVaultUUID).
		Count(&expertModeVolumeCount).Error
	if err != nil {
		return 0, err
	}
	return volumeCount + expertModeVolumeCount, nil
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

// GetBackupsByBackupVaultUUIDAndFilter retrieves backups by vault UUID without account filtering
// This is used for GCBDR vaults where backups can come from multiple accounts/projects
func (d *DataStoreRepository) GetBackupsByBackupVaultUUIDAndFilter(ctx context.Context, backupVaultUUID string, filters [][]interface{}) ([]*datamodel.Backup, error) {
	bv, err := d.GetBackupVault(ctx, backupVaultUUID)
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

func (d *DataStoreRepository) GetBackupByExternalUUID(ctx context.Context, backupVaultUUID string, externalUUID string, accountName string) (*datamodel.Backup, error) {
	return getBackupByExternalUUID(d.db.GORM().WithContext(ctx), backupVaultUUID, externalUUID, accountName)
}

func getBackupByExternalUUID(db *gorm.DB, backupVaultUUID string, externalUUID string, accountName string) (*datamodel.Backup, error) {
	// Retrieve the backup vault details using the backupVaultUUID and account
	backupVault, err := getBackupVaultWithDetails(db, &datamodel.BackupVault{
		ExternalUUID: &backupVaultUUID,
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

	// Retrieve the backup using the backupVaultID and externalUUID
	var backup *datamodel.Backup
	backup, err = getBackupWithDetails(db, &datamodel.Backup{
		ExternalUUID:  externalUUID,
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

func (d *DataStoreRepository) AreBackupsInProgressForVolume(ctx context.Context, volumeUUID string, excludeBackupUUIDs []string) (bool, error) {
	return areBackupsInProgressForVolume(d.db.GORM().WithContext(ctx), volumeUUID, excludeBackupUUIDs)
}

func areBackupsInProgressForVolume(db *gorm.DB, volumeUUID string, excludeBackupUUIDs []string) (bool, error) {
	var backups int64
	query := db.Model(&datamodel.Backup{}).Where("volume_uuid = ?", volumeUUID).Where("state = ? OR state = ?", models.LifeCycleStateCreating, models.LifeCycleStateDeleting)

	if len(excludeBackupUUIDs) > 0 {
		query = query.Where("uuid NOT IN ?", excludeBackupUUIDs)
	}

	err := query.Count(&backups).Error

	if err != nil && errors.Is(err, gorm.ErrRecordNotFound) {
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

	// Check if this is the last backup for the volume before deleting
	var remainingBackupCount int64
	if backup.VolumeUUID != "" {
		err = tx.Model(&datamodel.Backup{}).
			Where("volume_uuid = ? AND uuid != ? AND deleted_at IS NULL", backup.VolumeUUID, backupUUID).
			Count(&remainingBackupCount).Error
		if err != nil {
			return nil, err
		}
	}

	backup.DeletedAt = &gorm.DeletedAt{Time: time.Now(), Valid: true}
	backup.State = models.LifeCycleStateDeleted
	backup.StateDetails = ""
	err = tx.Save(backup).Error
	if err != nil {
		return nil, err
	}

	// If this was the last backup for the volume, mark backup chain history as deleted
	if remainingBackupCount == 0 {
		err = markPreviousBackupChainHistoryAsDeleted(tx, backup.VolumeUUID, backup.DeletedAt.Time)
		if err != nil {
			util.GetLogger(ctx).Warnf("Failed to mark backup chain history as deleted for volume %s: %v", backup.VolumeUUID, err)
			// Don't fail the entire backup deletion if history update fails
		}
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
		Description:             backup.Description,
		State:                   models.LifeCycleStateAvailable,
		StateDetails:            models.LifeCycleStateAvailableDetails,
		Attributes:              backup.Attributes,
		AssetMetadata:           backup.AssetMetadata,
		SizeInBytes:             backup.SizeInBytes,
		LatestLogicalBackupSize: backup.LatestLogicalBackupSize,
	}).Error
	if err != nil {
		return nil, err
	}

	// Update backup chain history size if backup has a volume and size
	if dbBackup.VolumeUUID != "" && backup.LatestLogicalBackupSize > 0 {
		err = tx.Model(&datamodel.BackupChainHistory{}).
			Where("resource_uuid = ?", dbBackup.VolumeUUID).
			Where("deleted_at IS NULL").
			Updates(map[string]interface{}{
				"size":       backup.LatestLogicalBackupSize,
				"updated_at": time.Now(),
			}).Error
		if err != nil {
			util.GetLogger(ctx).Warnf("Failed to update backup chain history size for backup %s: %v", dbBackup.UUID, err)
			// Don't fail the entire operation if history update fails
		}
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

func (d *DataStoreRepository) UpdateBackupFields(ctx context.Context, backupUUID string, updates map[string]interface{}) error {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return err
	}
	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)

	dbBackup, err := getBackupWithDetails(tx, &datamodel.Backup{BaseModel: datamodel.BaseModel{UUID: backupUUID}})
	if err != nil {
		return err
	}

	updates["updated_at"] = time.Now()

	err = tx.Model(&dbBackup).Updates(updates).Error
	if err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
	}

	return nil
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
		Attributes:   backup.Attributes,
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
	err := db.Where("volume_uuid = ? and (state = ? or (state = ? and attributes->>'delete_initiated' = 'true'))", volumeUUID, models.LifeCycleStateAvailable, models.LifeCycleStateError).Order("created_at desc").First(&backup).Error
	if err != nil {
		return false, err
	}
	// check if the backup is latest
	if backup.UUID == backupUUID {
		return true, nil
	}
	return false, nil
}

// IsLatestBackupAnyState checks if a backup is the latest for its volume regardless of state
func (d *DataStoreRepository) IsLatestBackupAnyState(ctx context.Context, backupUUID, volumeUUID string) (bool, error) {
	db := d.db.GORM().WithContext(ctx)
	backup := &datamodel.Backup{}
	// get backup by id under a volume (any state)
	err := db.Where("volume_uuid = ?", volumeUUID).Order("id desc").First(&backup).Error
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

func (d *DataStoreRepository) GetBackupCountByVolumeUUIDs(ctx context.Context, volumeUUIDs []string, conditions [][]interface{}) (map[string]int64, error) {
	var results []struct {
		VolumeUUID  string `json:"volume_uuid"`
		BackupCount int64  `json:"backup_count"`
	}
	db := d.db.ApplyFilter(conditions).GORM().WithContext(ctx)
	err := db.Model(&datamodel.Backup{}).
		Select("volume_uuid, count(*) as backup_count").
		Where("volume_uuid IN ?", volumeUUIDs).
		Group("volume_uuid").
		Scan(&results).Error
	if err != nil {
		return nil, err
	}

	backupsCountByVolume := make(map[string]int64)
	for _, result := range results {
		backupsCountByVolume[result.VolumeUUID] = result.BackupCount
	}
	return backupsCountByVolume, nil
}

func (d *DataStoreRepository) GetBackupsByVolumeUUID(ctx context.Context, volumeUUID string) ([]*datamodel.Backup, error) {
	db := d.db.GORM().WithContext(ctx)
	var backups []*datamodel.Backup

	err := db.Preload("BackupVault").Where("volume_uuid = ?", volumeUUID).Find(&backups).Error
	if err != nil {
		return nil, err
	}

	return backups, nil
}

func (d *DataStoreRepository) UpdateBackupLatestLogicalBackupSizeByVolume(ctx context.Context, volumeUUID, excludeBackupUUID string) error {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return err
	}
	defer commitOrRollbackOnError(util.GetLogger(ctx), tx, &err)

	// Update all backups for the volume except the specified one, setting latest_logical_backup_size to 0
	err = tx.Model(&datamodel.Backup{}).
		Where("volume_uuid = ? AND uuid != ?", volumeUUID, excludeBackupUUID).
		Update("latest_logical_backup_size", 0).Error
	if err != nil {
		return err
	}

	return nil
}

// GetBackupMetrics retrieves backup logical size metrics grouped by volume UUID with pagination
// Returns the latest backup entry for each volume with state 'available'
func (d *DataStoreRepository) GetBackupMetrics(ctx context.Context, conditions [][]interface{}, pagination *dbutils.Pagination) ([]*datamodel.Backup, error) {
	db := d.db.ApplyFilter(conditions).GORM().WithContext(ctx)
	var results []*datamodel.Backup

	// Query to get the latest backup for each volume with state 'available'
	// Use Find instead of Scan to ensure Preload works correctly
	err := db.Preload("BackupVault", func(db *gorm.DB) *gorm.DB {
		return db.Select("id, name, account_id, backup_vault_type, cmek_attributes")
	}).
		Where("state = ?", models.LifeCycleStateAvailable).
		Where("id IN (?)", db.Table("backups").
			Select("MAX(id)").
			Where("state = ?", models.LifeCycleStateAvailable).
			Group("volume_uuid")).
		Scopes(dbutils.Paginate(pagination)).
		Find(&results).Error

	if err != nil {
		return nil, err
	}

	return results, nil
}

func (d *DataStoreRepository) GetBackupResourceDataForAggregation(ctx context.Context, conditions [][]interface{}, pagination *dbutils.Pagination) ([]*datamodel.Backup, error) {
	db := d.db.Unscoped().ApplyFilter(conditions).GORM().WithContext(ctx)

	var metricsData []*BackupMetricsData

	subquery := db.Table("backups").
		Select("MAX(id)").
		Group("volume_uuid")

	// Query backups table with JOIN to backup_vaults
	query := db.Table("backups").
		Select(`
			backups.uuid,
			backups.volume_uuid,
			backups.attributes,
			backups.backup_vault_id,
			backup_vaults.account_id AS vault_account_id,
			backup_vaults.name AS vault_name
		`).
		Joins("LEFT JOIN backup_vaults ON backups.backup_vault_id = backup_vaults.id").
		Where("backups.id IN (?)", subquery)

	// Apply pagination
	if pagination != nil {
		if pagination.Limit > 0 {
			query = query.Limit(pagination.Limit)
		}
		if pagination.Offset > 0 {
			query = query.Offset(pagination.Offset)
		}
	}

	err := query.Find(&metricsData).Error
	if err != nil {
		return nil, err
	}

	// Convert BackupMetricsData to datamodel.Backup for backward compatibility
	results := make([]*datamodel.Backup, len(metricsData))
	for i, data := range metricsData {
		results[i] = &datamodel.Backup{
			BaseModel: datamodel.BaseModel{
				UUID: data.UUID,
			},
			VolumeUUID:    data.VolumeUUID,
			Attributes:    data.Attributes,
			BackupVaultID: data.BackupVaultID,
			BackupVault: &datamodel.BackupVault{
				BaseModel: datamodel.BaseModel{
					ID: data.BackupVaultID,
				},
				Name:      data.VaultName,
				AccountID: data.VaultAccountID,
			},
		}
	}

	return results, nil
}

// GetBackupMetadata retrieves backup metadata entries with pagination and conditions
func (d *DataStoreRepository) GetBackupMetadata(ctx context.Context, conditions [][]interface{}, pagination *dbutils.Pagination) ([]*datamodel.BackupMetadata, error) {
	db := d.db.ApplyFilter(conditions).GORM().WithContext(ctx)
	var results []*datamodel.BackupMetadata

	err := db.Unscoped().Scopes(dbutils.Paginate(pagination)).Find(&results).Error
	if err != nil {
		return nil, err
	}

	return results, nil
}

// UpdateLatestBackupLogicalSize updates the latest backup's logical size for a given volume
func (d *DataStoreRepository) UpdateLatestBackupLogicalSize(ctx context.Context, volumeUUID string, newLogicalSize int64) error {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return err
	}
	defer commitOrRollbackOnError(util.GetLogger(ctx), tx, &err)

	// Find the latest backup for the volume (by id)
	var latestBackup datamodel.Backup
	err = tx.Where("volume_uuid = ? AND state = ?", volumeUUID, models.LifeCycleStateAvailable).
		Order("id desc").
		First(&latestBackup).Error
	if err != nil {
		return err
	}

	// Update the latest backup's logical size
	err = tx.Model(&latestBackup).
		Update("latest_logical_backup_size", newLogicalSize).Error
	if err != nil {
		return err
	}

	// Update backup chain history
	err = supersedePreviousBackupChainHistory(ctx, tx, volumeUUID, newLogicalSize, time.Now())
	if err != nil {
		util.GetLogger(ctx).Warnf("Failed to update backup chain history for volume %s: %v", volumeUUID, err)
		// Don't fail the entire operation if history update fails
	}

	return nil
}

func (d *DataStoreRepository) GetVolumeLatestBackupMap(ctx context.Context) (map[int64]*datamodel.VolumeLatestBackup, error) {
	logger := util.GetLogger(ctx)
	logger.Infof("Getting volume node latest backup map")

	// Step 1: Get latest backups grouped by volume_uuid
	latestBackups, err := d.GetLatestBackupsGroupedByVolumeUUID(ctx)
	if err != nil {
		logger.Errorf("Failed to get latest backups grouped by volume UUID: %v", err)
		return nil, err
	}
	logger.Infof("Retrieved %d latest backups", len(latestBackups))

	if len(latestBackups) == 0 {
		logger.Info("No latest backups found, returning empty map")
		return make(map[int64]*datamodel.VolumeLatestBackup), nil
	}

	// Extract volume UUIDs from latest backups
	volumeUUIDs := make([]string, 0, len(latestBackups))
	backupMap := make(map[string]*datamodel.Backup)
	for i := range latestBackups {
		volumeUUIDs = append(volumeUUIDs, latestBackups[i].VolumeUUID)
		backupMap[latestBackups[i].VolumeUUID] = &latestBackups[i]
	}
	logger.Infof("Extracted %d volume UUIDs from latest backups", len(volumeUUIDs))

	// Step 2: Get volumes with pool preloaded using the volume UUIDs
	conditions := [][]interface{}{{"uuid in ?", volumeUUIDs}}
	conditions = append(conditions, []interface{}{"state = ?", models.LifeCycleStateREADY})
	logger.Info("Fetching volumes with pool preloaded for extracted volume UUIDs")
	volumes, err2 := d.GetMultipleVolumes(ctx, conditions)
	if err2 != nil {
		logger.Errorf("Failed to get volumes: %v", err2)
		return nil, err2
	}
	logger.Infof("Retrieved %d volumes", len(volumes))

	// Step 3: Create map of volume_uuid -> {volume, latestBackup}
	resultMap := make(map[int64]*datamodel.VolumeLatestBackup)
	for i := range volumes {
		volumeUUID := volumes[i].UUID
		volumeID := volumes[i].ID
		if backup, exists := backupMap[volumeUUID]; exists {
			resultMap[volumeID] = &datamodel.VolumeLatestBackup{
				Volume:       volumes[i],
				LatestBackup: backup,
			}
			logger.Infof("Mapped volume %s (ID: %d) with its latest backup %s", volumeUUID, volumeID, backup.UUID)
		}
	}
	logger.Infof("Created result map with %d volume-backup pairs", len(resultMap))

	return resultMap, nil
}

// GetLatestBackupsGroupedByVolumeUUID gets all latest backups grouped by volume_uuid
func (d *DataStoreRepository) GetLatestBackupsGroupedByVolumeUUID(ctx context.Context) ([]datamodel.Backup, error) {
	db := d.db.GORM().WithContext(ctx)
	var latestBackups []datamodel.Backup

	// Use GORM's Raw method with a window function for better performance
	// Select only the necessary columns instead of * to improve performance
	err := db.Raw(`
		SELECT id, uuid, name, attributes, volume_uuid, state, size_in_bytes, latest_logical_backup_size
		FROM (
			SELECT id, uuid, name, attributes, volume_uuid, state, size_in_bytes, latest_logical_backup_size,
				   ROW_NUMBER() OVER (PARTITION BY volume_uuid ORDER BY created_at DESC) as rn
			FROM backups 
			WHERE deleted_at IS NULL AND state = ?
		) ranked 
		WHERE rn = 1
	`, models.LifeCycleStateAvailable).Scan(&latestBackups).Error
	if err != nil {
		return nil, err
	}
	return latestBackups, nil
}

func (d *DataStoreRepository) UpdateBackupConstituentCountFromVolume(ctx context.Context, backup *datamodel.Backup, volume *datamodel.Volume) (*datamodel.Backup, error) {
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

	lvCount := int32(0)
	if volume.LargeVolumeAttributes != nil && volume.LargeVolumeAttributes.LargeVolumeConstituentCount != nil {
		lvCount = *volume.LargeVolumeAttributes.LargeVolumeConstituentCount
	}

	backup.Attributes.ConstituentCountOfBackup = lvCount
	backup.Attributes.OntapVolumeStyle = OntapFgVolumeStyle

	// Prepare update fields
	updateFields := datamodel.Backup{
		Description: backup.Description,
		Attributes:  backup.Attributes,
	}

	err = tx.Model(&dbBackup).Updates(updateFields).Error
	if err != nil {
		return nil, err
	}

	return dbBackup, nil
}

// CreateBackupMetadata creates a new BackupMetadata entry in the database
func (d *DataStoreRepository) CreateBackupMetadata(ctx context.Context, backupMetadata *datamodel.BackupMetadata) (*datamodel.BackupMetadata, error) {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}
	defer commitOrRollbackOnError(util.GetLogger(ctx), tx, &err)

	// Check if BackupMetadata already exists for this volume
	var existingBackupMetadata datamodel.BackupMetadata
	err = tx.Where("volume_uuid = ?", backupMetadata.VolumeUUID).First(&existingBackupMetadata).Error
	if err == nil {
		// BackupMetadata already exists for this volume
		return &existingBackupMetadata, nil
	}
	if !vsaerrors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}

	// Create new BackupMetadata entry
	backupMetadata.UUID = utils.RandomUUID()

	err = tx.Create(backupMetadata).Error
	if err != nil {
		return nil, err
	}

	return backupMetadata, nil
}

// DeleteBackupMetadata deletes a BackupMetadata entry by volume UUID
func (d *DataStoreRepository) DeleteBackupMetadata(ctx context.Context, volumeUUID string) error {
	db := d.db.GORM().WithContext(ctx)

	// Delete BackupMetadata entry by volume UUID
	err := db.Where("volume_uuid = ?", volumeUUID).Delete(&datamodel.BackupMetadata{}).Error
	if err != nil {
		return err
	}

	return nil
}

// GetBackupMetadataByVolumeUUID gets a BackupMetadata entry by volume UUID
func (d *DataStoreRepository) GetBackupMetadataByVolumeUUID(ctx context.Context, volumeUUID string) (*datamodel.BackupMetadata, error) {
	db := d.db.GORM().WithContext(ctx)
	var backupMetadata datamodel.BackupMetadata

	err := db.Where("volume_uuid = ?", volumeUUID).First(&backupMetadata).Error
	if err != nil {
		return nil, customerrors.ConvertToNotFoundErrIfContainsMessage(err, "record not found", "backup metadata", &volumeUUID)
	}

	return &backupMetadata, nil
}

// UpdateBackupMetadata updates a BackupMetadata entry
func (d *DataStoreRepository) UpdateBackupMetadata(ctx context.Context, backupMetadata *datamodel.BackupMetadata) (*datamodel.BackupMetadata, error) {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}
	defer commitOrRollbackOnError(util.GetLogger(ctx), tx, &err)

	// First check if the record exists
	var existingBackupMetadata datamodel.BackupMetadata
	err = tx.Where("uuid = ?", backupMetadata.UUID).First(&existingBackupMetadata).Error
	if err != nil {
		if vsaerrors.Is(err, gorm.ErrRecordNotFound) {
			return nil, customerrors.NewNotFoundErr("backup metadata", &backupMetadata.UUID)
		}
		return nil, err
	}

	// Update the existing record
	err = tx.Model(&existingBackupMetadata).Updates(backupMetadata).Error
	if err != nil {
		return nil, err
	}

	// Return the updated record
	return &existingBackupMetadata, nil
}

// CreateSfrMetadata creates a new SfrMetadata entry in the database
func (d *DataStoreRepository) CreateSfrMetadata(ctx context.Context, sfrMetadata *datamodel.SfrMetadata) (*datamodel.SfrMetadata, error) {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}
	defer commitOrRollbackOnError(util.GetLogger(ctx), tx, &err)

	err = tx.Create(sfrMetadata).Error
	if err != nil {
		return nil, err
	}

	return sfrMetadata, nil
}

// GetSfrMetricsByTimeRange fetches SFR metadata records between startTime and endTime,
// aggregates them by volume UUID, and returns a map of volume UUID to aggregated metrics
func (d *DataStoreRepository) GetSfrMetricsByTimeRange(ctx context.Context, startTime, endTime time.Time) (map[string]datamodel.SfrMetricsAggregate, error) {
	db := d.db.GORM().WithContext(ctx)

	var results []struct {
		VolumeUUID string `gorm:"column:volume_uuid"`
		TotalSize  int64  `gorm:"column:total_size"`
		TotalCount int64  `gorm:"column:total_count"`
	}

	err := db.Model(&datamodel.SfrMetadata{}).
		Select("volume_uuid, SUM(files_size) as total_size, SUM(file_count) as total_count").
		Where("created_at >= ? AND created_at <= ?", startTime, endTime).
		Group("volume_uuid").
		Scan(&results).Error

	if err != nil {
		return nil, err
	}

	// Convert results to map
	sfrMetricsMap := make(map[string]datamodel.SfrMetricsAggregate)
	for _, result := range results {
		sfrMetricsMap[result.VolumeUUID] = datamodel.SfrMetricsAggregate{
			TotalSize:  result.TotalSize,
			TotalCount: result.TotalCount,
		}
	}

	return sfrMetricsMap, nil
}

func (d *DataStoreRepository) GetSfrMetadataByJobID(ctx context.Context, jobID int64) (*datamodel.SfrMetadata, error) {
	db := d.db.GORM().WithContext(ctx)
	var sfrMetadata datamodel.SfrMetadata
	err := db.Where("job_id = ?", jobID).First(&sfrMetadata).Error
	if err != nil {
		return nil, err
	}
	return &sfrMetadata, nil
}

func (d *DataStoreRepository) GetBackupWithVaultByUUID(ctx context.Context, backupUUID string) (*datamodel.Backup, error) {
	var backup datamodel.Backup
	err := d.db.GORM().WithContext(ctx).Unscoped().Preload("BackupVault").Where("uuid = ?", backupUUID).First(&backup).Error
	if err != nil {
		return nil, err
	}
	return &backup, nil
}

// UpdateBackupChainHistory updates the backup chain history with a new size for the active backup
// It marks the current active entry as deleted and creates a new entry with the updated size
func (d *DataStoreRepository) UpdateBackupChainHistory(ctx context.Context, volumeUUID string, newSize int64) error {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return err
	}
	defer commitOrRollbackOnError(util.GetLogger(ctx), tx, &err)

	err = supersedePreviousBackupChainHistory(ctx, tx, volumeUUID, newSize, time.Now())
	if err != nil {
		return err
	}

	return nil
}

// DeleteBackupChainHistoryOlderThan removes backup chain history records that have been soft deleted and are older than the specified time
// Uses batch deletion to avoid long-running transactions and lock contention
func (d *DataStoreRepository) DeleteBackupChainHistoryOlderThan(ctx context.Context, olderThan time.Time) (int64, error) {
	db := d.db.GORM().WithContext(ctx)

	batchSize := common.LoadConfig().PageSize
	var totalDeleted int64

	// Batch delete in chunks to avoid long-running transactions and lock contention
	for {
		result := db.Unscoped().Where("deleted_at IS NOT NULL AND deleted_at < ?", olderThan).Limit(int(batchSize)).Delete(&datamodel.BackupChainHistory{})
		if result.Error != nil {
			return totalDeleted, result.Error
		}
		if result.RowsAffected == 0 {
			break
		}
		totalDeleted += result.RowsAffected
	}

	return totalDeleted, nil
}

// ListBackupChainHistoriesWithPagination retrieves backup chain history entries with pagination and conditions.
func (d *DataStoreRepository) ListBackupChainHistoriesWithPagination(ctx context.Context, conditions [][]interface{}, pagination *dbutils.Pagination) ([]*datamodel.BackupChainHistory, error) {
	// Use Unscoped to include soft-deleted history rows when deleted_at filters are provided.
	db := d.db.ApplyFilter(conditions).Unscoped().GORM().WithContext(ctx).Order("created_at ASC")
	var results []*datamodel.BackupChainHistory

	err := db.Scopes(dbutils.Paginate(pagination)).Find(&results).Error
	if err != nil {
		return nil, err
	}

	return results, nil
}
