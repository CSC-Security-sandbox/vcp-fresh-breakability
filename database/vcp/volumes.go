package database

import (
	"context"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/hydrationActivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"gorm.io/gorm"
)

var (
	updateVolumeState      = _updateVolumeState
	deleteVolume           = _deleteVolume
	getMultipleVolumes     = _getMultipleVolumes
	volumesWithHG          = _volumesWithHG
	listVolumesWithDetails = _listVolumesWithDetails
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
	err2 := tx.Where("name = ?", volume.Name).Where("account_id = ?", volume.AccountID).First(&volume).Error
	if errors.Is(err2, gorm.ErrRecordNotFound) {
		volume.UUID = utils.RandomUUID()
		if volume.VolumeAttributes != nil && volume.VolumeAttributes.RestoredBackupID != "" && volume.VolumeAttributes.RestoredBackupPath != "" {
			// This is volume restore case
			volume.State = models.LifeCycleStateRestoring
			volume.StateDetails = models.LifeCycleStateRestoringDetails
		} else {
			volume.State = models.LifeCycleStateCreating
			volume.StateDetails = models.LifeCycleStateCreatingDetails
		}
		volume.CreatedAt = time.Now()
		volume.UpdatedAt = volume.CreatedAt

		err = tx.Create(volume).Error
		if err != nil {
			return nil, err
		}
		volume, err = getVolumeWithDetails(tx, &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: volume.UUID}})
		if err != nil {
			return nil, err
		}
		return volume, nil
	} else if err2 != nil {
		return nil, err1
	}
	return nil, customerrors.NewUserInputValidationErr("volume already exists")
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

	err = tx.Model(&dbVolume).Updates(datamodel.Volume{
		VolumeAttributes: volume.VolumeAttributes,
		State:            volume.State,
		StateDetails:     volume.StateDetails,
	}).Error
	if err != nil {
		return err
	}

	return nil
}

func (d *DataStoreRepository) RevertedVolume(ctx context.Context, volume *datamodel.Volume, snapshot *datamodel.Snapshot) error {
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

	snapshots, err := revertDeleteSnapshots(ctx, tx, volume.ID, snapshot.UUID)
	if err != nil {
		return err
	}

	dbVolume.State = models.LifeCycleStateREADY
	dbVolume.StateDetails = models.LifeCycleStateAvailableDetails
	err = tx.Unscoped().Save(dbVolume).Error
	if err != nil {
		return err
	}

	if len(snapshots) > 0 {
		err = hydrationActivities.HydrateBatchSnapshotstoCCFE(ctx, nil, snapshots)
		if err != nil {
			logger.Errorf("Failed to hydrate snapshots to CCFE after volume revert: %v, snapshots: %+v", err, snapshots)
			return err
		}
	}
	return nil
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
		return nil, err
	}

	result := db.Exec(
		"UPDATE snapshots SET deleted_at = CURRENT_TIMESTAMP, state = ?, state_details = ? "+
			"WHERE volume_id = ? AND created_at > (SELECT created_at FROM snapshots WHERE uuid = ?)",
		models.LifeCycleStateDeleted, models.LifeCycleStateDeletedDetails,
		volumeID, snapshotID,
	)
	if result.Error != nil {
		return nil, result.Error
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
		return err
	}

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
		return nil, err
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
		return nil, errors.NewVCPError(errors.ErrDatabaseDataReadError, err)
	}
	return volumes, nil
}

func getVolumeWithDetails(db *gorm.DB, query *datamodel.Volume) (*datamodel.Volume, error) {
	volume := &datamodel.Volume{}
	err := db.Preload("Account").Preload("Pool").Preload("Svm").Preload("Pool.KmsConfig").First(&volume, query).Error
	if err != nil {
		return nil, customerrors.ConvertToNotFoundErrIfContainsMessage(err, "record not found", "volume", nil)
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
	var volumes []*datamodel.Volume
	err := db.Model(&datamodel.Volume{}).
		Preload("Account").
		Preload("Pool").
		Preload("Svm").
		Where("account_id = ?", accountID).
		Where("(volume_attributes::jsonb->'block_properties' IS NOT NULL) AND (volume_attributes::jsonb->'block_properties'->'host_group_details' != 'null'::jsonb) AND EXISTS (SELECT 1 FROM jsonb_array_elements(volume_attributes::jsonb->'block_properties'->'host_group_details') AS elem WHERE elem->>'host_group_uuid' = ?)", hostGroupUUID).
		Find(&volumes).Error

	return volumes, err
}
