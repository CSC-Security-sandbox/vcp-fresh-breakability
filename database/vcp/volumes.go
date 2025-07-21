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
	updateVolumeState      = _updateVolumeState
	deleteVolume           = _deleteVolume
	getMultipleVolumes     = _getMultipleVolumes
	volumesWithHG          = _volumesWithHG
	listVolumesWithDetails = _listVolumesWithDetails
)

func (d *DataStoreRepository) CreateVolume(ctx context.Context, volume *datamodel.Volume, isRestore bool) (*datamodel.Volume, error) {
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
		if isRestore {
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

func (d *DataStoreRepository) GetVolume(ctx context.Context, volUUID string) (*datamodel.Volume, error) {
	return getVolumeWithDetails(d.db.GORM().WithContext(ctx), &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: volUUID}})
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
	err := db.Preload("Account").Preload("Pool").Preload("Svm").Find(&volumes).Error
	if err != nil {
		return nil, errors.NewVCPError(errors.ErrDatabaseDataReadError, err)
	}
	return volumes, nil
}

func getVolumeWithDetails(db *gorm.DB, query *datamodel.Volume) (*datamodel.Volume, error) {
	volume := &datamodel.Volume{}
	err := db.Preload("Account").Preload("Pool").Preload("Svm").First(&volume, query).Error
	if err != nil {
		return nil, customerrors.ConvertToNotFoundErrIfContainsMessage(err, "record not found", "volume", nil)
	}
	return volume, nil
}

func (d *DataStoreRepository) GetVolumesByPoolID(ctx context.Context, poolID int64) ([]*datamodel.Volume, error) {
	var volumes []*datamodel.Volume
	err := d.db.GORM().WithContext(ctx).Preload("Account").Preload("Pool").Preload("Svm").Where("pool_id = ?", poolID).Find(&volumes).Error
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
	err := db.Preload("Account").Preload("Pool").Preload("Svm").Find(&volumes).Error
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
		Where("volume_attributes::jsonb->'block_properties' IS NOT NULL AND EXISTS (SELECT 1 FROM jsonb_array_elements(volume_attributes::jsonb->'block_properties'->'host_group_details') AS elem WHERE elem->>'host_group_uuid' = ?)", hostGroupUUID).
		Find(&volumes).Error

	return volumes, err
}
