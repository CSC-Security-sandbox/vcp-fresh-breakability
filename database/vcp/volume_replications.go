package database

import (
	"context"
	"errors"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	utils2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"gorm.io/gorm"
)

var (
	getVolumeReplicationDetails = _getVolumeReplicationDetails
)

const (
	VolumeReplicationEndpointTypeDestination = "dst"
	VolumeReplicationEndpointTypeSource      = "src"
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
	if replication.ReplicationAttributes.EndpointType == VolumeReplicationEndpointTypeSource {
		replication.ReplicationAttributes.SourceReplicationUUID = replication.UUID
	} else {
		replication.ReplicationAttributes.DestinationReplicationUUID = replication.UUID
	}

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

// GetVolumeReplicationByProjectId retrieves a replication by projectId
func (d *DataStoreRepository) GetVolumeReplicationByProjectId(ctx context.Context, accountId int64) ([]*datamodel.VolumeReplication, error) {
	db := d.db.GORM().WithContext(ctx)
	var replications []*datamodel.VolumeReplication
	err := db.Preload("Volume").Find(&replications, &datamodel.VolumeReplication{AccountID: accountId}).Error
	if err != nil {
		return nil, err
	}
	return replications, nil
}

func _getVolumeReplicationDetails(db *gorm.DB, query *datamodel.VolumeReplication) (*datamodel.VolumeReplication, error) {
	vr := &datamodel.VolumeReplication{}
	err := db.Preload("Volume").Preload("Volume.Pool").Preload("Account").First(&vr, query).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, customerrors.NewNotFoundErr("volume replication", nil)
		}
		return nil, err
	}
	return vr, nil
}

func (d *DataStoreRepository) UpdateVolumeReplication(ctx context.Context, volumeRep *datamodel.VolumeReplication) error {
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
	if dbReplication.ReplicationAttributes.ExternalUUID != volumeRep.ReplicationAttributes.ExternalUUID {
		dbReplication.ReplicationAttributes.ExternalUUID = volumeRep.ReplicationAttributes.ExternalUUID
	}
	if dbReplication.ReplicationAttributes.ReplicationSchedule != volumeRep.ReplicationAttributes.ReplicationSchedule {
		dbReplication.ReplicationAttributes.ReplicationSchedule = volumeRep.ReplicationAttributes.ReplicationSchedule
	}
	dbReplication.ReplicationAttributes = volumeRep.ReplicationAttributes
	dbReplication.MirrorState = volumeRep.MirrorState
	dbReplication.RelationshipStatus = volumeRep.RelationshipStatus
	dbReplication.TotalTransferBytes = volumeRep.TotalTransferBytes
	dbReplication.TotalProgress = volumeRep.TotalProgress
	dbReplication.TotalTransferTimeSecs = volumeRep.TotalTransferTimeSecs
	dbReplication.LastTransferSize = volumeRep.LastTransferSize
	dbReplication.LastTransferError = volumeRep.LastTransferError
	dbReplication.LastTransferDuration = volumeRep.LastTransferDuration
	dbReplication.LastTransferEndTime = volumeRep.LastTransferEndTime
	dbReplication.ProgressLastUpdated = volumeRep.ProgressLastUpdated
	dbReplication.Description = volumeRep.Description

	dbReplication.LagTime = volumeRep.LagTime
	dbReplication.LastUpdatedFromOntap = volumeRep.LastUpdatedFromOntap
	err = tx.Updates(dbReplication).Error
	if err != nil {
		return err
	}

	return nil
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
	dbReplication.LagTime = volumeRep.LagTime
	dbReplication.MirrorState = volumeRep.MirrorState
	dbReplication.RelationshipStatus = volumeRep.RelationshipStatus
	if err := tx.Updates(dbReplication).Error; err != nil {
		return err
	}
	return nil
}

// DeleteVolumeReplication deletes replication based on UUID
func (d *DataStoreRepository) DeleteVolumeReplication(ctx context.Context, replication *datamodel.VolumeReplication) (*datamodel.VolumeReplication, error) {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}
	defer commitOrRollbackOnError(util.GetLogger(ctx), tx, &err)
	mirrorState := models.OntapUninitialized
	replicationStatus := models.SnapmirrorRelationshipIdle
	replication.DeletedAt = &gorm.DeletedAt{Time: time.Now(), Valid: true}
	replication.MirrorState = &mirrorState
	replication.RelationshipStatus = &replicationStatus
	replication.State = models.LifeCycleStateDeleted
	replication.StateDetails = models.LifeCycleStateDeletedDetails
	err = tx.Save(replication).Error
	if err != nil {
		return nil, err
	}

	return replication, nil
}

func (d *DataStoreRepository) GetVolumeReplicationCount(ctx context.Context, accountName string) (int64, error) {
	var count int64
	account, err := d.GetAccount(ctx, accountName)
	if err != nil {
		return 0, err
	}
	err = d.db.GORM().WithContext(ctx).Model(&datamodel.VolumeReplication{}).Where("account_id = ?", account.ID).Count(&count).Error
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (d *DataStoreRepository) ListVolumeReplications(ctx context.Context, filter utils2.Filter) ([]*datamodel.VolumeReplication, error) {
	if len(filter.Conditions) == 0 {
		return nil, customerrors.NewUserInputValidationErr("no filter conditions provided for listing volume replications")
	}

	db := d.db.ApplyFilter(filter.Apply()).GORM().WithContext(ctx)
	var volumeReplications []*datamodel.VolumeReplication
	err := db.Preload("Volume").Preload("Volume.Pool").Find(&volumeReplications).Error
	if err != nil {
		return nil, err
	}
	return volumeReplications, nil
}
