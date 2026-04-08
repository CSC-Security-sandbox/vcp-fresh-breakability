package database

import (
	"context"
	"errors"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"gorm.io/gorm"
)

// CreateVolumePerformanceGroup creates a new volume performance group row in the database.
func (d *DataStoreRepository) CreateVolumePerformanceGroup(ctx context.Context, vpg *datamodel.VolumePerformanceGroup) (*datamodel.VolumePerformanceGroup, error) {
	return createVolumePerformanceGroup(d.db.GORM().WithContext(ctx), vpg)
}

// UpdateVolumePerformanceGroup updates an existing volume performance group row in the database.
// Only updatable fields are modified: Name, ThroughputMibps, Iops, OntapQosPolicyID, and IsAutoGen.
// IsShared and PoolID cannot be updated.
func (d *DataStoreRepository) UpdateVolumePerformanceGroup(ctx context.Context, vpg *datamodel.VolumePerformanceGroup) error {
	return updateVolumePerformanceGroup(d.db.GORM().WithContext(ctx), ctx, vpg)
}

// DeleteVolumePerformanceGroup soft-deletes a volume performance group row in the database.
func (d *DataStoreRepository) DeleteVolumePerformanceGroup(ctx context.Context, vpg *datamodel.VolumePerformanceGroup) error {
	return deleteVolumePerformanceGroup(d.db.GORM().WithContext(ctx), vpg)
}

// HardDeleteVolumePerformanceGroup permanently deletes a volume performance group row from the database.
// This should only be used for auto-generated VPGs that need to be completely removed.
func (d *DataStoreRepository) HardDeleteVolumePerformanceGroup(ctx context.Context, vpg *datamodel.VolumePerformanceGroup) error {
	return hardDeleteVolumePerformanceGroup(d.db.GORM().WithContext(ctx), vpg)
}

// GetVolumePerformanceGroupByUUID retrieves a volume performance group row by its row UUID.
func (d *DataStoreRepository) GetVolumePerformanceGroupByUUID(ctx context.Context, uuid string) (*datamodel.VolumePerformanceGroup, error) {
	return getVolumePerformanceGroupByUUID(d.db.GORM().WithContext(ctx), uuid)
}

// GetVolumePerformanceGroupByID retrieves a volume performance group row by its database ID.
func (d *DataStoreRepository) GetVolumePerformanceGroupByID(ctx context.Context, id int64) (*datamodel.VolumePerformanceGroup, error) {
	return getVolumePerformanceGroupByID(d.db.GORM().WithContext(ctx), id)
}

// GetVolumePerformanceGroupByPoolAndName retrieves a volume performance group row by pool ID and name.
func (d *DataStoreRepository) GetVolumePerformanceGroupByPoolAndName(ctx context.Context, poolID int64, name string) (*datamodel.VolumePerformanceGroup, error) {
	return getVolumePerformanceGroupByPoolAndName(d.db.GORM().WithContext(ctx), poolID, name)
}

// ListVolumePerformanceGroupsByPoolID retrieves all volume performance group rows for a given pool ID.
func (d *DataStoreRepository) ListVolumePerformanceGroupsByPoolID(ctx context.Context, poolID int64) ([]*datamodel.VolumePerformanceGroup, error) {
	return listVolumePerformanceGroupsByPoolID(d.db.GORM().WithContext(ctx), poolID)
}

// Returns the volume performance group. Respects soft deletes (deleted_at filter).
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

// Returns the volume performance group by ID. Respects soft deletes (deleted_at filter).
func getVolumePerformanceGroupByID(db *gorm.DB, id int64) (*datamodel.VolumePerformanceGroup, error) {
	vpg := &datamodel.VolumePerformanceGroup{}
	err := db.Where("id = ?", id).First(vpg).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, customerrors.NewNotFoundErr("volume performance group", nil)
		}
		return nil, err
	}
	return vpg, nil
}

// Returns the volume performance group by pool ID and name. Respects soft deletes (deleted_at filter).
func getVolumePerformanceGroupByPoolAndName(db *gorm.DB, poolID int64, name string) (*datamodel.VolumePerformanceGroup, error) {
	vpg := &datamodel.VolumePerformanceGroup{}
	err := db.Where("pool_id = ? AND name = ?", poolID, name).First(vpg).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, customerrors.NewNotFoundErr("volume performance group", &name)
		}
		return nil, err
	}
	return vpg, nil
}

// Returns the volume performance groups. Respects soft deletes (deleted_at filter).
func listVolumePerformanceGroupsByPoolID(db *gorm.DB, poolID int64) ([]*datamodel.VolumePerformanceGroup, error) {
	var vpgs []*datamodel.VolumePerformanceGroup
	if err := db.Where("pool_id = ?", poolID).Find(&vpgs).Error; err != nil {
		return nil, err
	}
	return vpgs, nil
}

func createVolumePerformanceGroup(db *gorm.DB, vpg *datamodel.VolumePerformanceGroup) (*datamodel.VolumePerformanceGroup, error) {
	existing, err := getVolumePerformanceGroupByPoolAndName(db, vpg.PoolID, vpg.Name)
	if err == nil && existing != nil {
		return nil, customerrors.NewConflictErr("volume performance group with this name already exists")
	}
	if err != nil && !customerrors.IsNotFoundErr(err) {
		return nil, err
	}

	// Generate UUID if empty (following the pattern from pools.go)
	if vpg.UUID == "" {
		vpg.UUID = utils.RandomUUID()
	}

	// Set timestamps
	now := time.Now()
	vpg.CreatedAt = now
	vpg.UpdatedAt = now

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

	dbVPG, err := getVolumePerformanceGroupByUUID(tx, vpg.UUID)
	if err != nil {
		return err
	}

	// Map-based update so GORM doesn't skip boolean zero-values. IsShared/PoolID are immutable.
	updates := map[string]interface{}{
		"updated_at":          time.Now(),
		"name":                vpg.Name,
		"throughput_mibps":    vpg.ThroughputMibps,
		"iops":                vpg.Iops,
		"is_auto_gen":         vpg.IsAutoGen,
	}
	if vpg.OntapQosPolicyID != "" {
		updates["ontap_qos_policy_id"] = vpg.OntapQosPolicyID
	}

	target := &datamodel.VolumePerformanceGroup{BaseModel: datamodel.BaseModel{ID: dbVPG.ID}}
	err = tx.Model(target).Updates(updates).Error
	return err
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

func hardDeleteVolumePerformanceGroup(db *gorm.DB, vpg *datamodel.VolumePerformanceGroup) error {
	// Use Unscoped() to perform a hard delete (permanently remove from database)
	res := db.Unscoped().Delete(vpg)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return customerrors.NewNotFoundErr("volume performance group", nil)
	}
	return nil
}
