package database

import (
	"context"
	"errors"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"gorm.io/gorm"
)

// CreateVolumePerformanceGroup creates a new volume performance group row in the database.
func (d *DataStoreRepository) CreateVolumePerformanceGroup(ctx context.Context, vpg *datamodel.VolumePerformanceGroup) (*datamodel.VolumePerformanceGroup, error) {
	return createVolumePerformanceGroup(d.db.GORM().WithContext(ctx), vpg)
}

// UpdateVolumePerformanceGroup updates an existing volume performance group row in the database.
// Only updatable fields are modified: Name, ThroughputMibps, and Iops.
// IsShared and PoolID cannot be updated.
func (d *DataStoreRepository) UpdateVolumePerformanceGroup(ctx context.Context, vpg *datamodel.VolumePerformanceGroup) error {
	return updateVolumePerformanceGroup(d.db.GORM().WithContext(ctx), ctx, vpg)
}

// DeleteVolumePerformanceGroup soft-deletes a volume performance group row in the database.
func (d *DataStoreRepository) DeleteVolumePerformanceGroup(ctx context.Context, vpg *datamodel.VolumePerformanceGroup) error {
	return deleteVolumePerformanceGroup(d.db.GORM().WithContext(ctx), vpg)
}

// GetVolumePerformanceGroupByUUID retrieves a volume performance group row by its row UUID.
func (d *DataStoreRepository) GetVolumePerformanceGroupByUUID(ctx context.Context, uuid string) (*datamodel.VolumePerformanceGroup, error) {
	return getVolumePerformanceGroupByUUID(d.db.GORM().WithContext(ctx), uuid)
}

// ListVolumePerformanceGroupsByPoolID retrieves all volume performance group rows for a given pool ID.
func (d *DataStoreRepository) ListVolumePerformanceGroupsByPoolID(ctx context.Context, poolID int64) ([]*datamodel.VolumePerformanceGroup, error) {
	return listVolumePerformanceGroupsByPoolID(d.db.GORM().WithContext(ctx), poolID)
}

// Returns the volume performance group. deleted_at field does not need to be considered VPGs are not a soft delete.
func getVolumePerformanceGroupByUUID(db *gorm.DB, uuid string) (*datamodel.VolumePerformanceGroup, error) {
	vpg := &datamodel.VolumePerformanceGroup{}
	err := db.Where("uuid = ?", uuid).First(vpg).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, customerrors.NewNotFoundErr("volume performance group", &uuid)
		}
		return nil, err
	}
	return vpg, nil
}

// Returns the volume performance groups. deleted_at field does not need to be considered VPGs are not a soft delete.
func listVolumePerformanceGroupsByPoolID(db *gorm.DB, poolID int64) ([]*datamodel.VolumePerformanceGroup, error) {
	var vpgs []*datamodel.VolumePerformanceGroup
	if err := db.Where("pool_id = ?", poolID).Find(&vpgs).Error; err != nil {
		return nil, err
	}
	return vpgs, nil
}

func createVolumePerformanceGroup(db *gorm.DB, vpg *datamodel.VolumePerformanceGroup) (*datamodel.VolumePerformanceGroup, error) {
	if err := db.Create(vpg).Error; err != nil {
		return nil, err
	}
	return vpg, nil
}

func updateVolumePerformanceGroup(db *gorm.DB, ctx context.Context, vpg *datamodel.VolumePerformanceGroup) error {
	tx, err := startTransaction(db)
	if err != nil {
		return err
	}
	defer commitOrRollbackOnError(util.GetLogger(ctx), tx, &err)

	// Fetch the existing VPG from the database
	dbVPG, err := getVolumePerformanceGroupByUUID(tx, vpg.UUID)
	if err != nil {
		return err
	}

	// Only update allowed fields: Name, ThroughputMibps, Iops
	// Do NOT update IsShared or PoolID
	dbVPG.UpdatedAt = time.Now()
	dbVPG.Name = vpg.Name
	dbVPG.ThroughputMibps = vpg.ThroughputMibps
	dbVPG.Iops = vpg.Iops

	if err := tx.Updates(dbVPG).Error; err != nil {
		return err
	}

	return nil
}

func deleteVolumePerformanceGroup(db *gorm.DB, vpg *datamodel.VolumePerformanceGroup) error {
	res := db.Delete(vpg)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return customerrors.NewNotFoundErr("volume performance group", nil)
	}
	return nil
}
