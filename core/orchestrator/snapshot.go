package orchestrator

import (
	"context"
	"database/sql"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

const (
	SNAPSHOT_TYPE_ADHOC = "adhoc"
)

var (
	createSnapshot       = _createSnapshot
	getSnapshot          = _getSnapshot
	VolumeOwnershipCheck = _volumeOwnershipCheck
	listSnapshots        = _listSnapshots
)

// CreateSnapshot creates the snapshot and adds to the specified volume belonging to the specified owner
func (o *Orchestrator) CreateSnapshot(ctx context.Context, params *common.CreateSnapshotParams) (*models.Snapshot, string, error) {
	return createSnapshot(ctx, o.storage, o.temporal, params)
}

func _createSnapshot(ctx context.Context, se database.Storage, temporal client.Client, params *common.CreateSnapshotParams) (*models.Snapshot, string, error) {
	logger := util.GetLogger(ctx)

	account, err := se.GetAccount(ctx, params.AccountName)
	if err != nil {
		logger.Errorf("Failed to get account: %s. Error: %v", params.AccountName, err)
		return nil, "", errors.NewNotFoundErr("account", &params.AccountName)
	}

	volume, err := VolumeOwnershipCheck(ctx, se, params.VolumeID, params.AccountName)
	if err != nil {
		logger.Errorf("Failed to validate volume ownership")
		return nil, "", errors.NewUserInputValidationErr("failed to validate volume ownership")
	}

	if params.IsAppConsistent {
		appConsistentSnaps, err := se.GetAppConsistentSnapshotsForVolume(ctx, account.ID, volume.ID)
		if err != nil {
			return nil, "", err
		} else if len(appConsistentSnaps) == 1 {
			return nil, "", errors.NewConflictErr("Volume already has an app consistent snapshot")
		}
	}

	err = validateCreatSnapshotOperation(volume, params, account)
	if err != nil {
		return nil, "", err
	}

	job := &datamodel.Job{
		Type:         string(models.JobTypeCreateSnapshot),
		State:        string(models.JobsStateNEW),
		ResourceName: params.Name,
		AccountID:    sql.NullInt64{Int64: account.ID, Valid: true},
	}

	job, err = se.CreateJob(ctx, job)
	if err != nil {
		logger.Errorf("Failed to create job in database. Error: %v", err)
		return nil, "", err
	}

	snapshot := &datamodel.Snapshot{
		Name:            params.Name,
		Description:     params.Description,
		VolumeID:        volume.ID,
		AccountID:       account.ID,
		Volume:          volume,
		Account:         account,
		IsAppConsistent: params.IsAppConsistent,
		SnapshotAttributes: &datamodel.SnapshotAttributes{
			Type: SNAPSHOT_TYPE_ADHOC,
		},
	}

	dbSnapshot, err := se.CreatingSnapshot(ctx, snapshot)
	if err != nil {
		logger.Errorf("Failed to create snapshot in database. Error: %v", err)
		return nil, "", err
	}

	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			ID:                    job.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		},
		workflows.CreateSnapshotWorkflow,
		params,
		dbSnapshot,
	)

	if err != nil {
		logger.Errorf("Failed to start create snapshot workflow. Error: %v ", err)
		return nil, "", err
	}

	dataStoreSnap := convertDatastoreSnapshotToModel(dbSnapshot)
	return dataStoreSnap, job.UUID, nil
}

func (o *Orchestrator) GetSnapshot(ctx context.Context, params *common.GetSnapshotParams) (*models.Snapshot, error) {
	return getSnapshot(ctx, o.storage, params)
}

func _getSnapshot(ctx context.Context, se database.Storage, params *common.GetSnapshotParams) (*models.Snapshot, error) {
	logger := util.GetLogger(ctx)

	_, err := VolumeOwnershipCheck(ctx, se, params.VolumeID, params.AccountName)
	if err != nil {
		logger.Errorf("Failed to validate volume ownership")
		return nil, errors.NewUserInputValidationErr("failed to validate volume ownership")
	}

	snapshot, err := se.GetSnapshot(ctx, params.SnapshotUUID)
	if err != nil {
		logger.Errorf("Failed to get snapshot: %s. Error: %v", params.SnapshotUUID, err)
		return nil, err
	}

	dataStoreSnap := convertDatastoreSnapshotToModel(snapshot)
	return dataStoreSnap, nil
}

func (o *Orchestrator) ListSnapshots(ctx context.Context, params *common.ListSnapshotsParams) ([]*models.Snapshot, error) {
	return listSnapshots(ctx, o.storage, params)
}

func _listSnapshots(ctx context.Context, se database.Storage, params *common.ListSnapshotsParams) ([]*models.Snapshot, error) {
	logger := util.GetLogger(ctx)

	volume, err := VolumeOwnershipCheck(ctx, se, params.VolumeID, params.AccountName)
	if err != nil {
		logger.Errorf("Failed to validate volume ownership")
		return nil, errors.NewUserInputValidationErr("failed to validate volume ownership")
	}

	snapshots, err := se.GetSnapshotsByVolumeID(ctx, volume.ID)
	if err != nil {
		logger.Errorf("Failed to get snapshots for volume: %s. Error: %v", params.VolumeID, err)
		return nil, err
	}

	var snapshotsToReturn []*models.Snapshot
	for _, snapshot := range snapshots {
		snapshotsToReturn = append(snapshotsToReturn, convertDatastoreSnapshotToModel(snapshot))
	}
	return snapshotsToReturn, nil
}

func convertDatastoreSnapshotToModel(snapshot *datamodel.Snapshot) *models.Snapshot {
	if snapshot == nil {
		return nil
	}

	res := &models.Snapshot{
		BaseModel: models.BaseModel{
			UUID:      snapshot.UUID,
			CreatedAt: snapshot.CreatedAt,
			UpdatedAt: snapshot.UpdatedAt,
			DeletedAt: DeletedAtOrNil(snapshot.DeletedAt),
		},
		Name:                  snapshot.Name,
		Description:           snapshot.Description,
		LifeCycleState:        snapshot.State,
		LifeCycleStateDetails: snapshot.StateDetails,
		VolumeUUID:            snapshot.Volume.UUID,
		VolumeName:            snapshot.Volume.Name,
	}
	return res
}

func validateCreatSnapshotOperation(volume *datamodel.Volume, params *common.CreateSnapshotParams, account *datamodel.Account) error {
	if params.Name == "" {
		return errors.NewUserInputValidationErr("Snapshot name is empty. Please provide a valid name.")
	}

	if volume.State == models.LifeCycleStateCreating {
		return errors.NewNotReadyErr("Can not create a snapshot when volume is in creating stage.")
	}
	if volume.State == models.LifeCycleStateDeleting {
		return errors.NewConflictErr("Can not create a snapshot when volume is in deleting stage.")
	}

	// @TODO: Include DataProtection check when implemented

	return nil
}

func _volumeOwnershipCheck(ctx context.Context, se database.Storage, volumeUUID string, accountName string) (*datamodel.Volume, error) {
	logger := util.GetLogger(ctx)

	volume, err := se.VerifyVolumeOwnership(ctx, volumeUUID, accountName)
	if err != nil {
		logger.Errorf("Failed to verify volume ownership: %v", err)
		return nil, err
	}

	return volume, nil // If volume is nil, it means ownership verification failed
}
