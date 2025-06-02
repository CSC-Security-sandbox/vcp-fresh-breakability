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

var (
	getVolumeReplicationDetails = _getVolumeReplicationDetails
)

// CreateVolumeReplication creates a new replication in the database
func (d *DataStoreRepository) CreateVolumeReplication(ctx context.Context, replication *datamodel.VolumeReplication) (*datamodel.VolumeReplication, error) {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}
	defer commitOrRollbackOnError(util.GetLogger(ctx), tx, &err)

	// Check if replication already exists for the volume
	dbReplicationEntry := &datamodel.VolumeReplication{}
	dbError := tx.Where("account_id = ?", replication.AccountID).Where("volume_id = ?", replication.VolumeID).First(&dbReplicationEntry).Error

	if dbReplicationEntry.ID != 0 {
		return nil, customerrors.NewUserInputValidationErr("replication already exists for this volume")
	}
	if dbError != nil && !errors.Is(dbError, gorm.ErrRecordNotFound) {
		return nil, dbError
	}

	replication.UUID = utils.RandomUUID()
	replication.State = models.LifeCycleStateCreating
	replication.StateDetails = models.LifeCycleStateCreatingDetails
	replication.CreatedAt = time.Now()
	replication.UpdatedAt = replication.CreatedAt

	err = tx.Create(replication).Error
	if err != nil {
		return nil, err
	}

	dbVolumeRep, err := getVolumeReplicationDetails(tx, &datamodel.VolumeReplication{BaseModel: datamodel.BaseModel{UUID: replication.UUID}})
	if err != nil {
		return nil, err
	}
	return dbVolumeRep, nil
}

// GetVolumeReplication retrieves a replication by its UUID
func (d *DataStoreRepository) GetVolumeReplication(ctx context.Context, volumeReplicationID string) (*datamodel.VolumeReplication, error) {
	db := d.db.GORM().WithContext(ctx)
	return getVolumeReplicationDetails(db, &datamodel.VolumeReplication{BaseModel: datamodel.BaseModel{UUID: volumeReplicationID}})
}

func _getVolumeReplicationDetails(db *gorm.DB, query *datamodel.VolumeReplication) (*datamodel.VolumeReplication, error) {
	vr := &datamodel.VolumeReplication{}
	err := db.Preload("Volume").First(&vr, query).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, customerrors.NewNotFoundErr("volume replication", nil)
		}
		return nil, err
	}
	return vr, nil
}

// UpdateVolumeReplicationStates updates state & state_details for the replication in database
func (d *DataStoreRepository) UpdateVolumeReplicationStates(ctx context.Context, volumeRep *datamodel.VolumeReplication) error {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return err
	}
	defer commitOrRollbackOnError(util.GetLogger(ctx), tx, &err)

	dbReplication, err := getVolumeReplicationDetails(tx, &datamodel.VolumeReplication{BaseModel: datamodel.BaseModel{UUID: volumeRep.UUID}})
	if err != nil {
		return err
	}

	dbReplication.UpdatedAt = time.Now()
	dbReplication.State = volumeRep.State
	dbReplication.StateDetails = volumeRep.StateDetails
	err = tx.Updates(dbReplication).Error
	if err != nil {
		return err
	}

	return nil
}

// UpdateVolumeReplicationTransferStats updates transfer stats for the replication
func (d *DataStoreRepository) UpdateVolumeReplicationTransferStats(ctx context.Context, volumeRep *datamodel.VolumeReplication) error {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return err
	}
	defer commitOrRollbackOnError(util.GetLogger(ctx), tx, &err)

	dbReplication, err := getVolumeReplicationDetails(tx, &datamodel.VolumeReplication{BaseModel: datamodel.BaseModel{UUID: volumeRep.UUID}})
	if err != nil {
		return err
	}
	dbReplication.LastUpdatedFromOntap = time.Now()
	dbReplication.TotalProgress = volumeRep.TotalProgress
	dbReplication.TotalTransferBytes = volumeRep.TotalTransferBytes
	dbReplication.TotalTransferTimeSecs = volumeRep.TotalTransferTimeSecs
	dbReplication.LastTransferSize = volumeRep.LastTransferSize
	dbReplication.LastTransferError = volumeRep.LastTransferError
	dbReplication.LastTransferDuration = volumeRep.LastTransferDuration
	dbReplication.LastTransferEndTime = volumeRep.LastTransferEndTime
	dbReplication.ProgressLastUpdated = volumeRep.ProgressLastUpdated
	if err := tx.Updates(dbReplication).Error; err != nil {
		return err
	}
	return nil
}

// DeleteVolumeReplication deletes replication based on UUID
func (d *DataStoreRepository) DeleteVolumeReplication(ctx context.Context, volumeReplicationID string) (*datamodel.VolumeReplication, error) {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}
	defer commitOrRollbackOnError(util.GetLogger(ctx), tx, &err)

	volumeRep, err := getVolumeReplicationDetails(tx, &datamodel.VolumeReplication{BaseModel: datamodel.BaseModel{UUID: volumeReplicationID}})
	if err != nil {
		return nil, err
	}
	volumeRep.DeletedAt = &gorm.DeletedAt{Time: time.Now(), Valid: true}
	volumeRep.State = models.LifeCycleStateDeleted
	volumeRep.StateDetails = ""
	err = tx.Save(volumeRep).Error
	if err != nil {
		return nil, err
	}

	return volumeRep, nil
}
