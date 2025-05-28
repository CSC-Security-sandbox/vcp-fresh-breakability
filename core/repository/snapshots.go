package repository

import (
	"context"
	"errors"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"gorm.io/gorm"
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
		return nil, customerrors.NewConflictErr("snapshot already exists")
	}
	if dbError != nil && !errors.Is(dbError, gorm.ErrRecordNotFound) {
		logger.Errorf("Snapshot create operation failed with error: %v", dbError)
		return nil, customerrors.New("snapshot create operation failed")
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
		return nil, customerrors.New("snapshot create operation failed")
	}
	return snapshot, nil
}

func getSnapshotWithDetails(db *gorm.DB, query *datamodel.Snapshot) (*datamodel.Snapshot, error) {
	snapshot := &datamodel.Snapshot{}
	err := db.Preload("Account").Preload("Volume").First(&snapshot, query).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, customerrors.NewNotFoundErr("snapshot", &query.UUID)
		}
		return nil, customerrors.Errorf("failed to retrieve snapshot details: %v", err)
	}
	return snapshot, nil
}

func (d *DataStoreRepository) UpdateSnapshot(ctx context.Context, snapshot *datamodel.Snapshot) error {
	logger := util.GetLogger(ctx)
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return err
	}
	defer commitOrRollbackOnError(logger, tx, &err)
	dbSnapshot, err := getSnapshotWithDetails(tx, &datamodel.Snapshot{BaseModel: datamodel.BaseModel{UUID: snapshot.UUID}})
	if err != nil {
		return err
	}
	err = tx.Model(&dbSnapshot).Updates(datamodel.Snapshot{
		SnapshotAttributes: snapshot.SnapshotAttributes,
		State:              snapshot.State,
		StateDetails:       snapshot.StateDetails,
	}).Error

	if err != nil {
		return err
	}
	return nil
}

func (d *DataStoreRepository) GetAppConsistentSnapshotsForVolume(ctx context.Context, accountID, volumeID int64) ([]*datamodel.Snapshot, error) {
	conditions := [][]interface{}{{"account_id = ?", accountID}, {"volume_id = ?", volumeID}, {"is_app_consistent = ?", true}}
	return getSnapshotsWithCondition(d.db.ApplyFilter(conditions).GORM().WithContext(ctx))
}

func (d *DataStoreRepository) GetSnapshot(ctx context.Context, uuid string) (*datamodel.Snapshot, error) {
	return getSnapshotWithDetails(d.db.GORM().WithContext(ctx), &datamodel.Snapshot{BaseModel: datamodel.BaseModel{UUID: uuid}})
}

func getSnapshotsWithCondition(db *gorm.DB) ([]*datamodel.Snapshot, error) {
	var snapshots []*datamodel.Snapshot
	err := db.Preload("Account").Find(&snapshots).Error
	if err != nil {
		return nil, err
	}
	return snapshots, nil
}
