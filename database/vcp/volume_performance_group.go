package database

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// CreateVolumePerformanceGroup creates a new volume performance group row in the database.
func (d *DataStoreRepository) CreateVolumePerformanceGroup(ctx context.Context, vpg *datamodel.VolumePerformanceGroup) (*datamodel.VolumePerformanceGroup, error) {
	return createVolumePerformanceGroup(d.db.GORM().WithContext(ctx), vpg)
}

// CreateVolumePerformanceGroupWithCap locks the pool row, counts VPGs, enforces maxCount, then inserts. maxCount <= 0 skips the cap.
func (d *DataStoreRepository) CreateVolumePerformanceGroupWithCap(ctx context.Context, vpg *datamodel.VolumePerformanceGroup, maxCount int) (*datamodel.VolumePerformanceGroup, error) {
	if vpg == nil {
		return nil, fmt.Errorf("vpg is required")
	}
	return createVolumePerformanceGroupWithCap(d.db.GORM().WithContext(ctx), ctx, vpg, maxCount)
}

func createVolumePerformanceGroupWithCap(db *gorm.DB, ctx context.Context, vpg *datamodel.VolumePerformanceGroup, maxCount int) (created *datamodel.VolumePerformanceGroup, err error) {
	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}
	defer commitOrRollbackOnError(util.GetLogger(ctx), tx, &err)

	var pool datamodel.Pool
	q := tx.Where("id = ?", vpg.PoolID)
	if isPostgresDialect(tx) {
		q = q.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	if lookupErr := q.First(&pool).Error; lookupErr != nil {
		if errors.Is(lookupErr, gorm.ErrRecordNotFound) {
			err = customerrors.NewNotFoundErr("pool", nil)
			return nil, err
		}
		err = lookupErr
		return nil, err
	}

	if maxCount > 0 {
		var count int64
		if countErr := tx.Model(&datamodel.VolumePerformanceGroup{}).
			Where("pool_id = ?", vpg.PoolID).
			Count(&count).Error; countErr != nil {
			err = countErr
			return nil, err
		}
		if count >= int64(maxCount) {
			err = customerrors.NewUserInputValidationErr(fmt.Sprintf(
				"Pool has reached the maximum number of Volume Performance Groups (%d). "+
					"Delete unused VPGs to proceed.",
				maxCount))
			return nil, err
		}
	}

	created, err = createVolumePerformanceGroup(tx, vpg)
	if err != nil {
		return nil, err
	}
	return created, nil
}

// isPostgresDialect is true for Postgres (FOR UPDATE); false for SQLite tests.
func isPostgresDialect(db *gorm.DB) bool {
	if db == nil || db.Dialector == nil {
		return false
	}
	name := db.Dialector.Name()
	return name == "postgres" || name == "postgresql"
}

// UpdateVolumePerformanceGroup updates an existing volume performance group row in the database.
// Always-written fields: Name, ThroughputMibps, Iops, IsAutoGen.
// Conditionally-written fields: OntapQosPolicyID (when non-empty), State/StateDetails (when State is non-empty),
// Description (when changed from current DB value), Labels (when non-nil).
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

// UpdateVolumePerformanceGroupState updates only the state and state_details columns of a VPG by UUID.
func (d *DataStoreRepository) UpdateVolumePerformanceGroupState(ctx context.Context, uuid, state, stateDetails string) error {
	return updateVolumePerformanceGroupState(d.db.GORM().WithContext(ctx), uuid, state, stateDetails)
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

// CountVolumePerformanceGroupsByPoolID counts VPGs for the pool, including IsAutoGen.
func (d *DataStoreRepository) CountVolumePerformanceGroupsByPoolID(ctx context.Context, poolID int64) (int64, error) {
	var count int64
	err := d.db.GORM().WithContext(ctx).
		Model(&datamodel.VolumePerformanceGroup{}).
		Where("pool_id = ?", poolID).
		Count(&count).Error
	if err != nil {
		return 0, err
	}
	return count, nil
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
		"updated_at":       time.Now(),
		"name":             vpg.Name,
		"throughput_mibps": vpg.ThroughputMibps,
		"iops":             vpg.Iops,
		"is_auto_gen":      vpg.IsAutoGen,
	}
	if vpg.OntapQosPolicyID != "" {
		updates["ontap_qos_policy_id"] = vpg.OntapQosPolicyID
	}
	if vpg.State != "" {
		updates["state"] = vpg.State
		updates["state_details"] = vpg.StateDetails
	}
	if vpg.Description != dbVPG.Description {
		updates["description"] = vpg.Description
	}
	if vpg.Labels != nil {
		updates["labels"] = vpg.Labels
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

func updateVolumePerformanceGroupState(db *gorm.DB, uuid, state, stateDetails string) error {
	updates := map[string]interface{}{
		"updated_at":    time.Now(),
		"state":         state,
		"state_details": stateDetails,
	}
	res := db.Model(&datamodel.VolumePerformanceGroup{}).Where("uuid = ?", uuid).Updates(updates)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return customerrors.NewNotFoundErr("volume performance group", &uuid)
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
