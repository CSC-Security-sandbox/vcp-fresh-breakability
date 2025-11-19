package database

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"gorm.io/gorm"
)

var (
	updateVolumeState            = _updateVolumeState
	deleteVolume                 = _deleteVolume
	getMultipleVolumes           = _getMultipleVolumes
	volumesWithHG                = _volumesWithHG
	listVolumesWithDetails       = _listVolumesWithDetails
	listAllVolumesWithDetails    = _listAllVolumesWithDetails
	eligibleVolDetails           = _eligibleVolDetails
	FindVolumeInRegionalPool     = _findVolumeInRegionalPool
	FindVolumeInZonalPool        = _findVolumeInZonalPool
	UpdateVolumeTieringBatchSize = env.GetInt("UPDATE_VOLUME_TIERING_BATCH_SIZE", 20)
)

func (d *DataStoreRepository) CreateVolume(ctx context.Context, volume *datamodel.Volume) (*datamodel.Volume, error) {
	db := d.db.GORM().WithContext(ctx)
	tx, err1 := startTransaction(db)
	if err1 != nil {
		return nil, err1
	}
	var err error
	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)
	var volErr error
	if volume.Pool.PoolAttributes.IsRegionalHA {
		_, volErr = FindVolumeInRegionalPool(tx, volume.Name, volume.AccountID, false)
	} else {
		_, volErr = FindVolumeInZonalPool(tx, volume.Name, volume.AccountID, volume.Pool.PoolAttributes.PrimaryZone, false)
	}

	if errors.Is(volErr, gorm.ErrRecordNotFound) {
		volume.UUID = utils.RandomUUID()
		if volume.VolumeAttributes != nil && volume.VolumeAttributes.RestoredBackupPath != "" {
			// This is volume restore case
			volume.State = models.LifeCycleStateRestoring
			volume.StateDetails = models.LifeCycleStateRestoringDetails
		} else if volume.State == "" {
			// Normal volume creation case
			volume.State = models.LifeCycleStateCreating
			volume.StateDetails = models.LifeCycleStateCreatingDetails
		}
		volume.CreatedAt = time.Now()
		volume.UpdatedAt = volume.CreatedAt

		err = tx.Create(volume).Error
		if err != nil {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataInsertError, err)
		}
		volume, err = getVolumeWithDetails(tx, &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: volume.UUID}})
		if err != nil {
			return nil, err
		}
		return volume, nil
	} else if volErr != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, volErr)
	}
	// Volume already exists in the same zone
	return nil, vsaerrors.NewVCPError(vsaerrors.ErrInputValidationError, customerrors.NewUserInputValidationErr("volume with this name already exists in the same zone"))
}

// GetVolume retrieves a volume by its UUID and if the deletedAt field is not set, it returns the volume details.
func (d *DataStoreRepository) GetVolume(ctx context.Context, volUUID string) (*datamodel.Volume, error) {
	return getVolumeWithDetails(d.db.GORM().WithContext(ctx), &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: volUUID}})
}

// DescribeVolume retrieves a volume by its UUID and returns the volume details, including deleted volumes.
func (d *DataStoreRepository) DescribeVolume(ctx context.Context, volUUID string) (*datamodel.Volume, error) {
	return getVolumeWithDetails(d.db.Unscoped().GORM().WithContext(ctx), &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: volUUID}})
}

func (d *DataStoreRepository) GetVolumeWithAccountID(ctx context.Context, volUUID string, accountID int64) (*datamodel.Volume, error) {
	return getVolumeWithDetails(d.db.GORM().WithContext(ctx), &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: volUUID}, AccountID: accountID})
}

func (d *DataStoreRepository) GetVolumeByNameAndAccountID(ctx context.Context, name string, accountID int64) (*datamodel.Volume, error) {
	return getVolumeWithDetails(d.db.GORM().WithContext(ctx), &datamodel.Volume{Name: name, AccountID: accountID})
}

func (d *DataStoreRepository) GetVolumeByName(ctx context.Context, volName string) (*datamodel.Volume, error) {
	return getVolumeWithDetails(d.db.GORM().WithContext(ctx), &datamodel.Volume{Name: volName})
}

func (d *DataStoreRepository) UpdateVolume(ctx context.Context, volume *datamodel.Volume) error {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return err
	}
	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)
	dbVolume, err := getVolumeWithDetails(tx, &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: volume.UUID}})
	if err != nil {
		return err
	}

	// Prepare the fields to update
	updateFields := datamodel.Volume{
		VolumeAttributes: volume.VolumeAttributes,
		State:            volume.State,
		StateDetails:     volume.StateDetails,
	}

	// Update LargeVolumeAttributes only if LargeVolume is true
	if volume.LargeVolumeAttributes != nil && volume.LargeVolumeAttributes.LargeCapacity {
		updateFields.LargeVolumeAttributes = volume.LargeVolumeAttributes
	}

	err = tx.Model(&dbVolume).Updates(updateFields).Error
	if err != nil {
		return err
	}

	return nil
}

func (d *DataStoreRepository) RevertedVolume(ctx context.Context, volume *datamodel.Volume, snapshot *datamodel.Snapshot) ([]*datamodel.Snapshot, error) {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}
	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)

	snapshots, err := revertDeleteSnapshots(ctx, tx, volume.ID, snapshot.UUID)
	if err != nil {
		return nil, err
	}

	volume.State = models.LifeCycleStateREADY
	volume.StateDetails = models.LifeCycleStateAvailableDetails
	err = tx.Unscoped().Save(volume).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
	}

	return snapshots, nil
}

func revertDeleteSnapshots(ctx context.Context, db *gorm.DB, volumeID int64, snapshotID string) ([]*datamodel.Snapshot, error) {
	db = db.Preload("Account").Preload("Volume").Preload("Volume.Pool")
	logger := util.GetLogger(ctx)

	var snapshots []*datamodel.Snapshot
	err := db.Where(
		"volume_id = ? and created_at > (select created_at from (select created_at from snapshots where uuid = ?) as ss)",
		volumeID, snapshotID,
	).Find(&snapshots).Error
	if err != nil {
		logger.Warnf("failed to revert delete snapshots: %v", err)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError,
			customerrors.ConvertToNotFoundErrIfContainsMessage(err, "record not found", "snapshot", nil))
	}

	result := db.Exec(
		"UPDATE snapshots SET deleted_at = CURRENT_TIMESTAMP, state = ?, state_details = ? "+
			"WHERE volume_id = ? AND created_at > (SELECT created_at FROM snapshots WHERE uuid = ?)",
		models.LifeCycleStateDeleted, models.LifeCycleStateDeletedDetails,
		volumeID, snapshotID,
	)
	if result.Error != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, result.Error)
	}

	return snapshots, nil
}

func (d *DataStoreRepository) UpdateVolumeFields(ctx context.Context, volumeUUID string, updates map[string]interface{}) error {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return err
	}
	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)

	dbVolume, err := getVolumeWithDetails(tx, &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: volumeUUID}})
	if err != nil {
		return err
	}

	updates["updated_at"] = time.Now()

	err = tx.Model(&dbVolume).Updates(updates).Error
	if err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
	}

	return nil
}

// BatchUpdateVolumeFields efficiently updates specific fields for multiple volumes using PostgreSQL bulk operations
// Currently supports updating: used_bytes
//
// To add new fields in the future:
// 1. Add the field to the buildVolumeUpdateQuery method (placeholders, args, and SET clause)
// 2. Update the paramCounter increment (currently +=2, will be +=3 for 3 fields, etc.)
// 3. Ensure the field exists in VolumeFieldUpdate.Fields map before calling
func (d *DataStoreRepository) BatchUpdateVolumeFields(ctx context.Context, updates []datamodel.VolumeFieldUpdate) error {
	if len(updates) == 0 {
		return nil
	}

	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return err
	}
	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)

	// Build parameterized query to prevent SQL injection
	sql, args := d.buildVolumeUpdateQuery(ctx, updates)

	err = tx.Exec(sql, args...).Error
	if err != nil {
		logger.Errorf("Bulk volume field update failed: %v", err)
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
	}

	logger.Infof("Successfully bulk updated %d volumes", len(updates))
	return nil
}

// buildVolumeUpdateQuery creates a parameterized SQL query for bulk volume updates
// Returns the SQL string and arguments slice to prevent SQL injection
func (d *DataStoreRepository) buildVolumeUpdateQuery(ctx context.Context, updates []datamodel.VolumeFieldUpdate) (string, []interface{}) {
	// Build parameterized query with placeholders
	placeholders := make([]string, len(updates))
	args := make([]interface{}, len(updates)*2) // 2 params per update: UUID + used_bytes

	paramCounter := 1 // PostgreSQL params start at $1
	argIndex := 0

	for i, update := range updates {
		uuidParam := paramCounter
		usedBytesParam := paramCounter + 1

		placeholders[i] = fmt.Sprintf("($%d::uuid, $%d::bigint)", uuidParam, usedBytesParam)

		args[argIndex] = update.UUID

		// Simple existence check to prevent panic
		if usedBytes, exists := update.Fields["used_bytes"]; exists {
			args[argIndex+1] = usedBytes
		} else {
			args[argIndex+1] = 0 // Default value if missing
		}

		paramCounter += 2 // Next update needs next 2 parameter slots (UUID + used_bytes)
		argIndex += 2     // Next 2 positions in args array

		// TO ADD NEW FIELDS:
		// - Add new parameter: newFieldParam := paramCounter + 2
		// - Update placeholder: fmt.Sprintf("($%d::uuid, $%d::bigint, $%d::type)", uuidParam, usedBytesParam, newFieldParam)
		// - Add to args: args[argIndex+2] = update.Fields["new_field"]
		// - Update counters: paramCounter += 3, argIndex += 3
		// - Update args slice size above: len(updates)*3
		// - Update SET clause below to include: new_field = tmp.new_field
	}

	// Use parameterized query to prevent SQL injection
	sql := fmt.Sprintf("UPDATE volumes "+
		"SET used_bytes = tmp.used_bytes, updated_at = NOW() "+
		"FROM (VALUES %s) AS tmp(uuid, used_bytes) "+
		"WHERE volumes.uuid::text = tmp.uuid::text",
		strings.Join(placeholders, ", "))

	return sql, args
}

// BatchUpdateVolumeTieringFields efficiently updates tiering fields for multiple volumes using PostgreSQL bulk operations
// Updates: hot_tier_size_gib, cold_tier_size_gib
// Processes updates in batches to avoid overwhelming the database with large queries
func (d *DataStoreRepository) BatchUpdateVolumeTieringFields(ctx context.Context, updates map[string]datamodel.VolumeTieringUpdate) error {
	if len(updates) == 0 {
		return nil
	}

	db := d.db.GORM().WithContext(ctx)
	logger := util.GetLogger(ctx)

	// Convert map to slice for easier batching
	updateSlice := make([]struct {
		UUID   string
		Update datamodel.VolumeTieringUpdate
	}, 0, len(updates))
	for uuid, update := range updates {
		updateSlice = append(updateSlice, struct {
			UUID   string
			Update datamodel.VolumeTieringUpdate
		}{UUID: uuid, Update: update})
	}

	totalUpdates := len(updateSlice)
	logger.Infof("Starting batch update of tiering fields for %d volumes with batch size %d", totalUpdates, UpdateVolumeTieringBatchSize)

	// Process updates in batches
	for i := 0; i < totalUpdates; i += UpdateVolumeTieringBatchSize {
		end := i + UpdateVolumeTieringBatchSize
		if end > totalUpdates {
			end = totalUpdates
		}

		batch := updateSlice[i:end]

		// Start transaction for this batch
		tx, err := startTransaction(db)
		if err != nil {
			return err
		}
		defer commitOrRollbackOnError(logger, tx, &err)

		// Build VALUES clause and args for this batch
		placeholders := make([]string, 0, len(batch))
		args := make([]interface{}, 0, len(batch)*3)
		paramCounter := 1

		for _, item := range batch {
			placeholders = append(placeholders, fmt.Sprintf("($%d::uuid, $%d::bigint, $%d::bigint)",
				paramCounter, paramCounter+1, paramCounter+2))
			args = append(args, item.UUID, item.Update.HotTierSizeGib, item.Update.ColdTierSizeGib)
			paramCounter += 3
		}

		sql := fmt.Sprintf(`UPDATE volumes 
			SET hot_tier_size_gib = tmp.hot_tier_size_gib, 
			    cold_tier_size_gib = tmp.cold_tier_size_gib, 
			    updated_at = NOW() 
			FROM (VALUES %s) AS tmp(uuid, hot_tier_size_gib, cold_tier_size_gib) 
			WHERE volumes.uuid::text = tmp.uuid::text`,
			strings.Join(placeholders, ", "))

		err = tx.Exec(sql, args...).Error
		if err != nil {
			logger.Errorf("Bulk volume tiering update failed for batch %d-%d: %v", i+1, end, err)
			return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
		}

		logger.Infof("Successfully updated tiering fields for batch %d-%d (%d volumes)", i+1, end, len(batch))
	}

	logger.Infof("Successfully bulk updated tiering fields for all %d volumes", totalUpdates)
	return nil
}

func (d *DataStoreRepository) DeleteVolume(ctx context.Context, volumeUUID string) (*datamodel.Volume, error) {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}
	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)
	return deleteVolume(tx, volumeUUID)
}

func (d *DataStoreRepository) DeleteVolumeAndChildResources(ctx context.Context, volumeUUID string) (*datamodel.Volume, error) {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}
	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)

	volume, err := getVolumeWithDetails(tx, &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: volumeUUID}})
	if err != nil {
		return nil, err
	}

	// Mark associated snapshots as deleted
	err = tx.Model(&datamodel.Snapshot{}).Where("volume_id = ?", volume.ID).Updates(
		datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{
				DeletedAt: &gorm.DeletedAt{Time: time.Now(), Valid: true},
			},
			State:        models.LifeCycleStateDeleted,
			StateDetails: models.LifeCycleStateDeletedDetails,
		}).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
	}

	// Finally, mark the volume as deleted
	deletedVolume, err := deleteVolume(tx, volumeUUID)
	if err != nil {
		return nil, err
	}

	return deletedVolume, nil
}

func (d *DataStoreRepository) UpdateVolumeState(ctx context.Context, volumeUUID string, state string, stateDetails string) (*datamodel.Volume, error) {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}
	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)
	return updateVolumeState(tx, volumeUUID, state, stateDetails)
}

func _deleteVolume(db *gorm.DB, volumeUUID string) (*datamodel.Volume, error) {
	volume, err := getVolumeWithDetails(db, &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: volumeUUID}})
	if err != nil {
		return nil, err
	}
	volume.DeletedAt = &gorm.DeletedAt{Time: time.Now(), Valid: true}
	volume.State = models.LifeCycleStateDeleted
	volume.StateDetails = ""
	err = db.Save(volume).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataDeleteError, fmt.Errorf("failed to delete volume %s: %w", volumeUUID, err))
	}

	return volume, nil
}

func _updateVolumeState(db *gorm.DB, volumeUUID string, state string, stateDetails string) (*datamodel.Volume, error) {
	volume, err := getVolumeWithDetails(db, &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: volumeUUID}})
	if err != nil {
		return nil, err
	}

	volume.State = state
	volume.StateDetails = stateDetails
	err = db.Save(volume).Error
	if err != nil {
		return nil, err
	}

	return volume, nil
}

func (d *DataStoreRepository) ListVolumes(ctx context.Context, conditions [][]interface{}) ([]*datamodel.Volume, error) {
	return listVolumesWithDetails(d.db.ApplyFilter(conditions).GORM().WithContext(ctx))
}

func _listVolumesWithDetails(db *gorm.DB) ([]*datamodel.Volume, error) {
	var volumes []*datamodel.Volume
	err := db.Preload("Account").Preload("Pool").Preload("Svm").Preload("Pool.KmsConfig").Find(&volumes).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	return volumes, nil
}

// ListVolumesWithPagination retrieves volumes with pagination support including deleted volumes
func (d *DataStoreRepository) ListVolumesWithPagination(ctx context.Context, conditions [][]interface{}, pagination *dbutils.Pagination) ([]*datamodel.Volume, error) {
	return _listVolumesWithDetailsPagination(d.db.ApplyFilter(conditions).Unscoped().GORM().WithContext(ctx), pagination)
}

func _listVolumesWithDetailsPagination(db *gorm.DB, pagination *dbutils.Pagination) ([]*datamodel.Volume, error) {
	var volumes []*datamodel.Volume
	err := db.Preload("Account").Preload("Pool").Preload("Svm").Scopes(dbutils.Paginate(pagination)).Find(&volumes).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	return volumes, nil
}

func getVolumeWithDetails(db *gorm.DB, query *datamodel.Volume) (*datamodel.Volume, error) {
	volume := &datamodel.Volume{}
	err := db.Preload("Account").Preload("Pool").Preload("Svm").Preload("Pool.KmsConfig").First(&volume, query).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrVolumeNotFound,
			customerrors.ConvertToNotFoundErrIfContainsMessage(err, "record not found", "volume", nil))
	}
	return volume, nil
}

func (d *DataStoreRepository) GetVolumesByPoolID(ctx context.Context, poolID int64) ([]*datamodel.Volume, error) {
	var volumes []*datamodel.Volume
	err := d.db.GORM().WithContext(ctx).Preload("Account").Preload("Pool").Preload("Svm").Preload("Pool.KmsConfig").Where("pool_id = ?", poolID).Find(&volumes).Error
	if err != nil {
		return nil, err
	}
	return volumes, nil
}

func (d *DataStoreRepository) GetVolumeCountByPoolID(ctx context.Context, poolID int64) (int64, error) {
	var count int64
	err := d.db.GORM().WithContext(ctx).Model(&datamodel.Volume{}).Where("pool_id = ?", poolID).Count(&count).Error
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (d *DataStoreRepository) GetFlexCacheVolumeCountByClusterPeerID(ctx context.Context, clusterPeerID int64) (int64, error) {
	var count int64
	err := d.db.GORM().
		WithContext(ctx).
		Model(&datamodel.Volume{}).
		Joins("JOIN cluster_peerings ON cluster_peerings.id = volumes.cluster_peer_id AND cluster_peerings.deleted_at IS NULL").
		Where("volumes.cluster_peer_id = ?", clusterPeerID).
		Count(&count).Error
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (d *DataStoreRepository) GetMultipleVolumes(ctx context.Context, conditions [][]interface{}) ([]*datamodel.Volume, error) {
	return getMultipleVolumes(d.db.ApplyFilter(conditions).GORM().WithContext(ctx))
}

func _getMultipleVolumes(db *gorm.DB) ([]*datamodel.Volume, error) {
	var volumes []*datamodel.Volume
	err := db.Preload("Account").Preload("Pool").Preload("Svm").Preload("Pool.KmsConfig").Find(&volumes).Error
	if err != nil {
		return nil, err
	}
	return volumes, nil
}

func (d *DataStoreRepository) VerifyVolumeOwnership(ctx context.Context, volumeUUID string, accountName string) (*datamodel.Volume, error) {
	db := d.db.GORM().WithContext(ctx)
	var account *datamodel.Account
	if err := db.Where("name = ?", accountName).First(&account).Error; err != nil {
		return nil, err
	}
	var volume *datamodel.Volume
	if err := db.Preload("Account").Preload("Pool").Preload("Svm").Where("uuid = ?", volumeUUID).Where("account_id= ?", account.ID).First(&volume).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, customerrors.NewNotFoundErr("volume", &volumeUUID)
		}
		return nil, err
	}
	return volume, nil
}

func (d *DataStoreRepository) GetVolumeCount(ctx context.Context, accountName string) (int64, error) {
	var count int64
	account, err := d.GetAccount(ctx, accountName)
	if err != nil {
		return 0, err
	}
	err = d.db.GORM().WithContext(ctx).Model(&datamodel.Volume{}).Where("account_id = ?", account.ID).Count(&count).Error
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (d *DataStoreRepository) GetAllVolumesForHG(ctx context.Context, hostGroupUUID string, accountID int64) ([]*datamodel.Volume, error) {
	return volumesWithHG(d.db.GORM().WithContext(ctx), hostGroupUUID, accountID)
}

func _volumesWithHG(db *gorm.DB, hostGroupUUID string, accountID int64) ([]*datamodel.Volume, error) {
	var volumesWithBD []*datamodel.Volume
	err := db.Model(&datamodel.Volume{}).
		Preload("Account").
		Preload("Pool").
		Preload("Svm").
		Where("account_id = ?", accountID).
		Where("(volume_attributes::jsonb->'block_devices' != 'null'::jsonb) AND EXISTS(SELECT 1 FROM jsonb_array_elements(volume_attributes->'block_devices') AS bd WHERE (bd->'host_group_details' != 'null'::jsonb) AND EXISTS (SELECT 1 FROM jsonb_array_elements(bd->'host_group_details') AS hgd WHERE hgd->>'host_group_uuid' = ?))", hostGroupUUID).
		Find(&volumesWithBD).Error
	if err != nil {
		return nil, err
	}

	var volumesWithBP []*datamodel.Volume
	err = db.Model(&datamodel.Volume{}).
		Preload("Account").
		Preload("Pool").
		Preload("Svm").
		Where("account_id = ?", accountID).
		Where("(volume_attributes::jsonb->'block_properties' IS NOT NULL) AND (volume_attributes::jsonb->'block_properties'->'host_group_details' != 'null'::jsonb) AND EXISTS (SELECT 1 FROM jsonb_array_elements(volume_attributes::jsonb->'block_properties'->'host_group_details') AS elem WHERE elem->>'host_group_uuid' = ?)", hostGroupUUID).
		Find(&volumesWithBP).Error
	if err != nil {
		return nil, err
	}

	volumesWithBD = append(volumesWithBD, volumesWithBP...)
	return volumesWithBD, err
}

// ListAllVolumes retrieves all volumes
func (d *DataStoreRepository) ListAllVolumes(ctx context.Context, conditions [][]interface{}, pagination *dbutils.Pagination) ([]*datamodel.Volume, error) {
	return listAllVolumesWithDetails(d.db.ApplyFilter(conditions).GORM().WithContext(ctx), pagination)
}

func _listAllVolumesWithDetails(db *gorm.DB, pagination *dbutils.Pagination) ([]*datamodel.Volume, error) {
	var volumes []*datamodel.Volume
	err := db.Select("name, state, account_id, auto_tiering_enabled, snapshot_policy, large_volume_attributes, data_protection").Preload("Account").Where("deleted_at IS NULL").Scopes(dbutils.Paginate(pagination)).Find(&volumes).Error
	if err != nil {
		return nil, err
	}
	return volumes, nil
}

// ListVolumesWithAccounts retrieves all volumes with preloaded accounts
func (d *DataStoreRepository) ListVolumesWithAccounts(ctx context.Context) ([]*datamodel.Volume, error) {
	db := d.db.GORM().WithContext(ctx)
	var volumes []*datamodel.Volume

	// Query to get all volumes with preloaded accounts and pools (only deployment_name for pools)
	err := db.Preload("Account", func(db *gorm.DB) *gorm.DB {
		return db.Select("id, name")
	}).
		Preload("Pool", func(db *gorm.DB) *gorm.DB {
			return db.Select("id, deployment_name")
		}).
		Find(&volumes).Error

	if err != nil {
		return nil, err
	}

	return volumes, nil
}

func (d *DataStoreRepository) GetEligibleVolumes(ctx context.Context, conditions [][]interface{}, pagination *dbutils.Pagination) ([]*datamodel.Volume, error) {
	return eligibleVolDetails(d.db.ApplyFilter(conditions).GORM().WithContext(ctx), pagination)
}

func _eligibleVolDetails(db *gorm.DB, pagination *dbutils.Pagination) ([]*datamodel.Volume, error) {
	var volumes []*datamodel.Volume
	err := db.Select("name, state").Preload("Account").Where("deleted_at IS NULL").Scopes(dbutils.Paginate(pagination)).Find(&volumes).Error
	if err != nil {
		return nil, err
	}
	return volumes, nil
}

// _findVolumeInRegionalPool finds a volume by name and account ID in regional pools
// Returns gorm.ErrRecordNotFound if no matching volume exists
func _findVolumeInRegionalPool(db *gorm.DB, volumeName string, accountID int64, preloadAssociations bool) (*datamodel.Volume, error) {
	var volume datamodel.Volume
	query := db.Table("volumes").
		Joins("JOIN pools ON volumes.pool_id = pools.id").
		Where("volumes.name = ? AND volumes.account_id = ?", volumeName, accountID).
		Where("pools.pool_attributes->>'is_regional_ha' = 'true'")

	if preloadAssociations {
		query = query.Preload("Account").Preload("Pool")
	}

	err := query.First(&volume).Error
	if err != nil {
		return nil, err
	}
	return &volume, nil
}

// _findVolumeInZonalPool finds a volume by name and account ID in a specific zone's non-regional i.e zonal pools
// Returns gorm.ErrRecordNotFound if no matching volume exists
func _findVolumeInZonalPool(db *gorm.DB, volumeName string, accountID int64, zone string, preloadAssociations bool) (*datamodel.Volume, error) {
	var volume datamodel.Volume
	query := db.Table("volumes").
		Joins("JOIN pools ON volumes.pool_id = pools.id").
		Where("volumes.name = ? AND volumes.account_id = ?", volumeName, accountID).
		Where("pools.pool_attributes->>'primary_zone' = ?", zone).
		Where("pools.pool_attributes->>'is_regional_ha' = 'false'")

	if preloadAssociations {
		query = query.Preload("Account").Preload("Pool")
	}

	err := query.First(&volume).Error
	if err != nil {
		return nil, err
	}
	return &volume, nil
}

func (d *DataStoreRepository) GetVolumeByNameAccountIDAndZone(ctx context.Context, name string, accountID int64, zone string, isRegionalPool bool) (*datamodel.Volume, error) {
	db := d.db.GORM().WithContext(ctx)

	var volume *datamodel.Volume
	var err error

	if isRegionalPool {
		volume, err = FindVolumeInRegionalPool(db, name, accountID, true)
	} else {
		volume, err = FindVolumeInZonalPool(db, name, accountID, zone, true)
	}

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrVolumeNotFound,
				customerrors.ConvertToNotFoundErrIfContainsMessage(err, "record not found", "volume", nil))
		}
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	return volume, nil
}
