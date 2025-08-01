package database

import (
	"context"
	"errors"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	utils2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	deleteSnapshot = _deleteSnapshot
)

func (d *DataStoreRepository) CreatingSnapshot(ctx context.Context, snapshot *datamodel.Snapshot) (*datamodel.Snapshot, error) {
	db := d.db.GORM().WithContext(ctx)
	logger := util.GetLogger(ctx)
	tx, err1 := startTransaction(db)
	if err1 != nil {
		return nil, err1
	}
	var err error
	defer commitOrRollbackOnError(logger, tx, &err)
	snapshotEntry := &datamodel.Snapshot{}

	dbError := tx.Where("account_id = ?", snapshot.AccountID).Where("volume_id = ?", snapshot.VolumeID).Where("name = ?", snapshot.Name).First(&snapshotEntry).Error

	if snapshotEntry.ID != 0 {
		logger.Warnf("Snapshot with name %s already exists", snapshot.Name)
		return nil, customerrors.NewConflictErr("Snapshot already exists")
	}
	if dbError != nil && !errors.Is(dbError, gorm.ErrRecordNotFound) {
		logger.Errorf("Snapshot create operation failed with error: %v", dbError)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataInsertError, dbError)
	}
	snapshot.UUID = utils.RandomUUID()
	snapshot.State = models.LifeCycleStateCreating
	snapshot.StateDetails = models.LifeCycleStateCreatingDetails
	snapshot.CreatedAt = time.Now()
	snapshot.UpdatedAt = snapshot.CreatedAt
	snapshot.DeletedAt = nil

	err = tx.Create(snapshot).Error
	if err != nil {
		logger.Errorf("Snapshot create operation failed with error: %v", err)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataInsertError, err)
	}
	return snapshot, nil
}

func getSnapshotWithDetails(db *gorm.DB, query *datamodel.Snapshot) (*datamodel.Snapshot, error) {
	snapshot := &datamodel.Snapshot{}
	db = db.Preload("Account").Preload("Volume")
	err := db.First(&snapshot, query).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, customerrors.ConvertToNotFoundErrIfContainsMessage(err, "record not found", "snapshot", &query.UUID)
		}
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	return snapshot, nil
}

func (d *DataStoreRepository) UpdateSnapshot(ctx context.Context, snapshot *datamodel.Snapshot) (*datamodel.Snapshot, error) {
	logger := util.GetLogger(ctx)
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}
	defer commitOrRollbackOnError(logger, tx, &err)
	dbSnapshot, err := getSnapshotWithDetails(tx, &datamodel.Snapshot{BaseModel: datamodel.BaseModel{UUID: snapshot.UUID}})
	if err != nil {
		return nil, err
	}
	err = tx.Model(&dbSnapshot).Updates(datamodel.Snapshot{
		Name:               snapshot.Name,
		Description:        snapshot.Description,
		SnapshotAttributes: snapshot.SnapshotAttributes,
		State:              snapshot.State,
		StateDetails:       snapshot.StateDetails,
	}).Error

	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
	}
	return dbSnapshot, nil
}

func (d *DataStoreRepository) GetAppConsistentSnapshotsForVolume(ctx context.Context, accountID, volumeID int64) ([]*datamodel.Snapshot, error) {
	filter := utils2.CreateFilterWithConditions(
		utils2.NewFilterCondition("account_id", "=", accountID),
		utils2.NewFilterCondition("volume_id", "=", volumeID),
		utils2.NewFilterCondition("is_app_consistent", "=", true))
	return d.GetSnapshotsWithCondition(ctx, *filter)
}

// GetSnapshotByUUID Retrieves a snapshot by UUID, account ID, and volume ID from the database.
func (d *DataStoreRepository) GetSnapshotByUUID(ctx context.Context, uuid string, accountID int64, volumeID int64) (*datamodel.Snapshot, error) {
	return getSnapshotWithDetails(d.db.GORM().WithContext(ctx), &datamodel.Snapshot{AccountID: accountID, VolumeID: volumeID, BaseModel: datamodel.BaseModel{UUID: uuid}})
}

// GetSnapshotByPoolID Retrieves a snapshot by UUID, account ID, and pool ID, validating the pool association and optionally preloading parent snapshot details.
func (d *DataStoreRepository) GetSnapshotByPoolID(ctx context.Context, uuid string, accountID int64, poolID int64, isParentSnapshot bool) (*datamodel.Snapshot, error) {
	db := d.db.GORM().WithContext(ctx).Preload("Volume").Preload("Volume.Pool")
	if isParentSnapshot {
		db = db.Preload("Volume.Svm")
	}

	snapshot := &datamodel.Snapshot{}
	err := db.First(&snapshot, &datamodel.Snapshot{
		AccountID: accountID,
		BaseModel: datamodel.BaseModel{UUID: uuid},
	}).Error

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, customerrors.ConvertToNotFoundErrIfContainsMessage(err, "record not found", "snapshot", &uuid)
		}
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}

	// Check if the PoolID of the associated volume matches the provided PoolID
	if snapshot.Volume != nil && snapshot.Volume.PoolID == poolID {
		return snapshot, nil
	}

	return nil, customerrors.NewBadRequestErr("Restore snapshots across pool is not supported")
}

func (d *DataStoreRepository) GetSnapshotsWithCondition(ctx context.Context, filter utils2.Filter) ([]*datamodel.Snapshot, error) {
	db := d.db.ApplyFilter(filter.Apply()).GORM().WithContext(ctx)
	var snapshots []*datamodel.Snapshot
	err := db.Preload("Volume").Find(&snapshots).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	return snapshots, nil
}

func (d *DataStoreRepository) GetWronglyDeletedSnapshot(ctx context.Context, snapshotExternalUUID string) (*datamodel.Snapshot, error) {
	db := d.db.Unscoped().GORM().WithContext(ctx)
	var snapshot *datamodel.Snapshot
	err := db.Preload("Volume").Find(&snapshot, "snapshot_attributes @> ?", map[string]interface{}{"external_uuid": snapshotExternalUUID}).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	return snapshot, nil
}

func (d *DataStoreRepository) UnDeleteSnapshot(ctx context.Context, snapshot *datamodel.Snapshot) error {
	if snapshot == nil {
		return errors.New("snapshot is nil")
	}

	logger := util.GetLogger(ctx)
	db := d.db.Unscoped().GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return err
	}
	defer commitOrRollbackOnError(logger, tx, &err)

	snapshot.State = models.LifeCycleStateREADY
	snapshot.StateDetails = models.LifeCycleStateReadyDetails
	snapshot.DeletedAt = &gorm.DeletedAt{}

	err = tx.Model(&snapshot).Where("uuid = ?", snapshot.UUID).Updates(snapshot).Error

	if err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
	}
	return nil
}

func (d *DataStoreRepository) DeleteSnapshot(ctx context.Context, snapshotUUID string) (*datamodel.Snapshot, error) {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}
	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)
	return deleteSnapshot(tx, snapshotUUID)
}

func _deleteSnapshot(db *gorm.DB, snapshotUUID string) (*datamodel.Snapshot, error) {
	snapshot, err := getSnapshotWithDetails(db, &datamodel.Snapshot{BaseModel: datamodel.BaseModel{UUID: snapshotUUID}})
	if err != nil {
		return nil, err
	}
	snapshot.DeletedAt = &gorm.DeletedAt{Time: time.Now(), Valid: true}
	snapshot.State = models.LifeCycleStateDeleted
	snapshot.StateDetails = models.LifeCycleStateDeletedDetails
	err = db.Save(snapshot).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataDeleteError, err)
	}

	return snapshot, nil
}

// DeletingSnapshot updates the snapshot entry to deleting state
func (d *DataStoreRepository) DeletingSnapshot(ctx context.Context, snapshot *datamodel.Snapshot) error {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return err
	}
	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)
	snapshot.State = models.LifeCycleStateDeleting
	snapshot.StateDetails = models.LifeCycleStateDeletingDetails
	err = tx.Updates(snapshot).Error
	if err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
	}
	return nil
}

func (d *DataStoreRepository) BatchDeleteSnapshots(ctx context.Context, snapshotIDs []int64) ([]*datamodel.Snapshot, error) {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}
	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)

	var snapshots []*datamodel.Snapshot
	err = tx.Model(&snapshots).Clauses(clause.Returning{}).Where("id IN ?", snapshotIDs).Updates(
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
	return snapshots, nil
}

func (d *DataStoreRepository) GetSnapshotsByVolumeID(ctx context.Context, volumeID int64) ([]*datamodel.Snapshot, error) {
	filter := utils2.CreateFilterWithConditions(
		utils2.NewFilterCondition("volume_id", "=", volumeID),
	)
	return d.GetSnapshotsWithCondition(ctx, *filter)
}

// GetReplicationSnapshotsByVolumeID ge the snapshots with name starting with "snapmirror." for a given volume ID
func (d *DataStoreRepository) GetReplicationSnapshotsByVolumeID(ctx context.Context, volumeID int64) ([]*datamodel.Snapshot, error) {
	filter := utils2.CreateFilterWithConditions(
		utils2.NewFilterCondition("volume_id", "=", volumeID),
		utils2.NewFilterCondition("name", "LIKE", "snapmirror.%"))
	db := d.db.ApplyFilter(filter.Apply()).GORM().WithContext(ctx)

	var snapshots []*datamodel.Snapshot
	err := db.Preload("Volume").Preload("Volume.Pool").Preload("Account").Find(&snapshots).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	return snapshots, nil
}
func (d *DataStoreRepository) GetSnapshotsByVolumeIDs(ctx context.Context, volumeIDs []int64) ([]*datamodel.Snapshot, error) {
	var snapshots []*datamodel.Snapshot
	db := d.db.GORM().WithContext(ctx)
	err := db.Preload("Volume").Where("volume_id IN ? AND state = ?", volumeIDs, models.LifeCycleStateREADY).Find(&snapshots).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	return snapshots, nil
}
