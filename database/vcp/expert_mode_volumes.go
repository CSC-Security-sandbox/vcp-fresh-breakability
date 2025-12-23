package database

import (
	"context"
	"gorm.io/gorm"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// CreateExpertModeVolume creates a new expert mode volume record
func (d *DataStoreRepository) CreateExpertModeVolume(ctx context.Context, expertModeVolume *datamodel.ExpertModeVolumes) (*datamodel.ExpertModeVolumes, error) {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}
	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)
	if expertModeVolume.UUID == "" {
		expertModeVolume.UUID = utils.RandomUUID()
	}

	if err := tx.Create(expertModeVolume).Error; err != nil {
		return nil, err
	}

	return expertModeVolume, nil
}

// GetExpertModePoolUsedCapacity calculates the total size of all expert mode volumes for a given pool ID
func (d *DataStoreRepository) GetExpertModePoolUsedCapacity(ctx context.Context, poolID int64) (int64, error) {
	var totalSize int64

	err := d.db.GORM().WithContext(ctx).
		Model(&datamodel.ExpertModeVolumes{}).
		Where("pool_id = ?", poolID).
		Select("COALESCE(SUM(size_in_bytes), 0)").
		Scan(&totalSize).Error

	if err != nil {
		return 0, err
	}

	return totalSize, nil
}

// GetExpertModeVolumeByNameAndPoolID retrieves an expert mode volume by its name and pool ID
func (d *DataStoreRepository) GetExpertModeVolumeByNameAndPoolID(ctx context.Context, name string, poolID int64) (*datamodel.ExpertModeVolumes, error) {
	var volume datamodel.ExpertModeVolumes

	// Query the database for the volume with the given name and pool ID
	err := d.db.GORM().WithContext(ctx).
		Where("name = ? AND pool_id = ?", name, poolID).
		First(&volume).Error

	if err != nil {
		return nil, err
	}

	return &volume, nil // Return the found volume
}

// GetExpertModeVolumeByUUID retrieves an expert mode volume by UUID with details
func (d *DataStoreRepository) GetExpertModeVolumeByUUID(ctx context.Context, volumeUUID string) (*datamodel.ExpertModeVolumes, error) {
	return getExpertModeVolumeWithDetails(d.db.GORM().WithContext(ctx), &datamodel.ExpertModeVolumes{BaseModel: datamodel.BaseModel{UUID: volumeUUID}})
}

func getExpertModeVolumeWithDetails(db *gorm.DB, query *datamodel.ExpertModeVolumes) (*datamodel.ExpertModeVolumes, error) {
	volume := &datamodel.ExpertModeVolumes{}
	err := db.Preload("Account").Preload("Pool").Preload("Svm").First(volume, query).Error
	if err != nil {
		return nil, err
	}
	return volume, nil
}

// UpdateExpertModeVolume updates an expert mode volume
func (d *DataStoreRepository) UpdateExpertModeVolume(ctx context.Context, expertModeVolume *datamodel.ExpertModeVolumes) (*datamodel.ExpertModeVolumes, error) {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}
	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)

	dbVolume, err := getExpertModeVolumeWithDetails(tx, &datamodel.ExpertModeVolumes{BaseModel: datamodel.BaseModel{UUID: expertModeVolume.UUID}})
	if err != nil {
		return nil, err
	}

	// Update the fields
	dbVolume.Name = expertModeVolume.Name
	dbVolume.SizeInBytes = expertModeVolume.SizeInBytes
	dbVolume.Style = expertModeVolume.Style
	dbVolume.State = expertModeVolume.State
	dbVolume.ExternalUUID = expertModeVolume.ExternalUUID

	err = tx.Save(dbVolume).Error
	if err != nil {
		return nil, err
	}

	return dbVolume, nil
}

// GetExpertModeVolumeByUUID retrieves an expert mode volume by its UUID
func (d *DataStoreRepository) GetExpertModeVolumeByVolumeUUID(ctx context.Context, volumeUUID string) (*datamodel.ExpertModeVolumes, error) {
	var volume datamodel.ExpertModeVolumes

	// Query the database for the volume with the given UUID
	err := d.db.GORM().WithContext(ctx).
		Where("uuid = ?", volumeUUID).
		Preload("Account").
		Preload("Pool").
		Preload("Svm").
		First(&volume).Error

	if err != nil {
		return nil, err
	}

	return &volume, nil
}

// UpdateExpertModeVolume updates an expert mode volume in the database
func (d *DataStoreRepository) UpdateExpertModeVolumeDataProtection(ctx context.Context, expertModeVolume *datamodel.ExpertModeVolumes) error {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return err
	}
	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)

	// Prepare the fields to update - only update BackupConfig
	updateFields := map[string]interface{}{
		"data_protection": expertModeVolume.BackupConfig,
	}

	err = tx.Model(expertModeVolume).
		Where("uuid = ?", expertModeVolume.UUID).
		Updates(updateFields).Error
	if err != nil {
		return err
	}

	return nil
}
