package database

import (
	"context"

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
