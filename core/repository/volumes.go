package repository

import (
	"context"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"gorm.io/gorm"
)

var (
	updateVolumeState = _updateVolumeState
	deleteVolume      = _deleteVolume
)

func (d *DataStoreRepository) CreateVolume(ctx context.Context, volume *datamodel.Volume) (*datamodel.Volume, error) {
	db := d.db.GORM().WithContext(ctx)

	if db.Where("name = ?", volume.Name).Where("account_id = ?", volume.AccountID).First(&volume).Error != nil {
		volume.UUID = utils.RandomUUID()
		volume.State = models.LifeCycleStateCreating
		volume.StateDetails = models.LifeCycleStateCreatingDetails
		volume.CreatedAt = time.Now()
		volume.UpdatedAt = volume.CreatedAt

		err := db.Create(volume).Error
		if err != nil {
			return nil, err
		}

		dbVolume, err := d.GetVolume(ctx, volume.UUID)
		if err != nil {
			return nil, err
		}
		return dbVolume, nil
	}
	return nil, customerrors.NewUserInputValidationErr("volume already exists")
}

func (d *DataStoreRepository) GetVolume(ctx context.Context, volUUID string) (*datamodel.Volume, error) {
	return getVolumeWithDetails(d.db.GORM().WithContext(ctx), &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: volUUID}})
}

func (d *DataStoreRepository) UpdateVolume(ctx context.Context, volume *datamodel.Volume) error {
	db := d.db.GORM().WithContext(ctx)
	dbVolume, err := getVolumeWithDetails(db, &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: volume.UUID}})
	if err != nil {
		return err
	}

	err = db.Model(&dbVolume).Updates(datamodel.Volume{
		VolumeAttributes: volume.VolumeAttributes,
		State:            volume.State,
		StateDetails:     volume.StateDetails,
	}).Error
	if err != nil {
		return err
	}

	return nil
}

func (d *DataStoreRepository) DeleteVolume(ctx context.Context, volumeUUID string) (*datamodel.Volume, error) {
	return deleteVolume(d.db.GORM().WithContext(ctx), volumeUUID)
}

func (d *DataStoreRepository) UpdateVolumeState(ctx context.Context, volumeUUID string, state string, stateDetails string) (*datamodel.Volume, error) {
	return updateVolumeState(d.db.GORM().WithContext(ctx), volumeUUID, state, stateDetails)
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

func (d *DataStoreRepository) ListVolumes(ctx context.Context) ([]*datamodel.Volume, error) {
	// TODO implement me
	panic("implement me")
}

func getVolumeWithDetails(db *gorm.DB, query *datamodel.Volume) (*datamodel.Volume, error) {
	volume := &datamodel.Volume{}
	err := db.Preload("Account").Preload("Pool").Preload("Svm").First(&volume, query).Error
	if err != nil {
		return nil, customerrors.ConvertToNotFoundErrIfContainsMessage(err, "record not found", "host group", &volume.UUID)
	}
	return volume, nil
}
