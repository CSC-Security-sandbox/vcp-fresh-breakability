package database

import (
	"context"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"gorm.io/gorm"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// ExpertModePoolCapacity represents the total size and volume count for expert mode volumes in a pool
type ExpertModePoolCapacity struct {
	TotalSize   int64
	VolumeCount int64
}

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

// GetExpertModePoolUsedCapacityAndVolumeCount calculates the total size and count of all expert mode volumes for a given pool ID, returns total size in bytes and volume count
func (d *DataStoreRepository) GetExpertModePoolUsedCapacityAndVolumeCount(ctx context.Context, poolID int64) (*ExpertModePoolCapacity, error) {
	var result struct {
		TotalSize   int64 `gorm:"column:total_size"`
		VolumeCount int64 `gorm:"column:volume_count"`
	}

	err := d.db.GORM().WithContext(ctx).
		Model(&datamodel.ExpertModeVolumes{}).
		Select("COALESCE(SUM(size_in_bytes), 0) as total_size, COUNT(*) as volume_count").
		Where("pool_id = ?", poolID).
		Scan(&result).Error

	if err != nil {
		return nil, err
	}

	return &ExpertModePoolCapacity{
		TotalSize:   result.TotalSize,
		VolumeCount: result.VolumeCount,
	}, nil
}

// GetExpertModeVolumeByNameAndPoolID retrieves an expert mode volume by its name and pool ID
func (d *DataStoreRepository) GetExpertModeVolumeByNameAndPoolID(ctx context.Context, name string, poolID int64) (*datamodel.ExpertModeVolumes, error) {
	return getExpertModeVolumeWithDetails(d.db.GORM().WithContext(ctx), &datamodel.ExpertModeVolumes{
		Name:   name,
		PoolID: poolID,
	})
}

// GetExpertModeVolumeByUUID retrieves an expert mode volume by UUID with details
func (d *DataStoreRepository) GetExpertModeVolumeByUUID(ctx context.Context, volumeUUID string) (*datamodel.ExpertModeVolumes, error) {
	return getExpertModeVolumeWithDetails(d.db.GORM().WithContext(ctx), &datamodel.ExpertModeVolumes{BaseModel: datamodel.BaseModel{UUID: volumeUUID}})
}

func (d *DataStoreRepository) GetExpertModeVolumeByExternalUUID(ctx context.Context, volumeUUID string) (*datamodel.ExpertModeVolumes, error) {
	return getExpertModeVolumeWithDetails(d.db.GORM().WithContext(ctx), &datamodel.ExpertModeVolumes{ExternalUUID: volumeUUID})
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

// DeleteExpertModeVolume soft deletes an expert mode volume by setting DeletedAt and State
func (d *DataStoreRepository) DeleteExpertModeVolume(ctx context.Context, volumeUUID string) error {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return err
	}
	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)

	volume, err := getExpertModeVolumeWithDetails(tx, &datamodel.ExpertModeVolumes{BaseModel: datamodel.BaseModel{UUID: volumeUUID}})
	if err != nil {
		return err
	}

	volume.DeletedAt = &gorm.DeletedAt{Time: time.Now(), Valid: true}
	volume.State = models.LifeCycleStateDeleted

	err = tx.Save(volume).Error
	if err != nil {
		return err
	}

	return nil
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

func (d *DataStoreRepository) UpdateExpertModeVolumeFields(ctx context.Context, volumeUUID string, updates map[string]interface{}) error {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return err
	}
	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)

	dbVolume, err := getExpertModeVolumeWithDetails(tx, &datamodel.ExpertModeVolumes{ExternalUUID: volumeUUID})
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
