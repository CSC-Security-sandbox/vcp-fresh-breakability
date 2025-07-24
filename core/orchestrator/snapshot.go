package orchestrator

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows/replicationWorkflows"
	utils2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
)

const (
	SNAPSHOT_TYPE_ADHOC    = "adhoc"
	STORAGE_CLASS_SOFTWARE = "SOFTWARE"
)

var (
	createSnapshot                  = _createSnapshot
	updateSnapshot                  = _updateSnapshot
	getSnapshot                     = _getSnapshot
	VolumeOwnershipCheck            = _volumeOwnershipCheck
	ValidateSnapshotName            = _validateSnapshotName
	deleteSnapshot                  = _deleteSnapshot
	listSnapshots                   = _listSnapshots
	deleteSnapshots                 = _deleteSnapshots
	ConvertDatastoreSnapshotToModel = _convertDatastoreSnapshotToModel
)

const (
	snapshotNamePattern           = `^[\w()+.-]+$`
	snapshotNameErrorEmpty        = "Snapshot name must not be empty."
	snapshotNameError             = "Snapshot name can only include alphanumeric characters and the following special characters: ()-_+."
	snapshotNameErrorDots         = "Snapshot name cannot include consecutive dots: .."
	snapshotNameErrorSingleDot    = "Snapshot name cannot be a single dot."
	snapshotNameErrorIllegalNames = `Snapshot name cannot start with the following: "ref_ss_volmove", "snapmirror", "hourly.", "daily.", "weekly." or "monthly.".`
)

var (
	illegalNamesRegexp = []*regexp.Regexp{
		regexp.MustCompile(`^ref_ss_volmove.*$`),
		regexp.MustCompile(`^snapmirror.*$`),
		regexp.MustCompile(`^(hourly|daily|weekly|monthly)\..*$`),
	}
)

// CreateSnapshot creates the snapshot and adds to the specified volume belonging to the specified owner
func (o *Orchestrator) CreateSnapshot(ctx context.Context, params *common.CreateSnapshotParams) (*models.Snapshot, string, error) {
	return createSnapshot(ctx, o.storage, o.temporal, params)
}

func _createSnapshot(ctx context.Context, se database.Storage, temporal client.Client, params *common.CreateSnapshotParams) (*models.Snapshot, string, error) {
	logger := util.GetLogger(ctx)

	if err := ValidateSnapshotName(nillable.GetString(&params.Name, "")); err != nil {
		logger.Errorf("Error creating snapshot: %v", err)
		return nil, "", err
	}

	account, err := se.GetAccount(ctx, params.AccountName)
	if err != nil {
		logger.Errorf("Failed to get account: %s. Error: %v", params.AccountName, err)
		return nil, "", err
	}

	volume, err := VolumeOwnershipCheck(ctx, se, params.VolumeID, params.AccountName)
	if err != nil {
		logger.Errorf("Failed to validate volume ownership")
		return nil, "", err
	}

	if params.IsAppConsistent {
		appConsistentSnaps, err := se.GetAppConsistentSnapshotsForVolume(ctx, account.ID, volume.ID)
		if err != nil {
			return nil, "", err
		} else if len(appConsistentSnaps) == 1 {
			return nil, "", vsaerrors.NewVCPError(vsaerrors.ErrSnapshotAppConsistencyError, customerrors.NewConflictErr("Volume already has an app consistent snapshot"))
		}
	}

	err = validateCreatSnapshotOperation(volume, params, account)
	if err != nil {
		return nil, "", err
	}

	// Check and return early if a snapshot with the same name is already in creation for this volume and account
	filter := utils2.CreateFilterWithConditions(
		utils2.NewFilterCondition("name", "=", params.Name),
		utils2.NewFilterCondition("account_id", "=", account.ID),
		utils2.NewFilterCondition("volume_id", "=", volume.ID))
	existingSnapshots, err := se.GetSnapshotsWithCondition(ctx, *filter)
	if err != nil {
		logger.Errorf("Failed to get snapshots with conditions: %v. Error: %v", filter, err)
		return nil, "", err
	}

	if len(existingSnapshots) > 0 {
		filter := utils2.CreateFilterWithConditions(
			utils2.NewFilterCondition("resource_name", "=", params.Name),
			utils2.NewFilterCondition("account_id", "=", account.ID),
			utils2.NewFilterCondition("type", "=", string(models.JobTypeCreateSnapshot)),
			utils2.NewFilterCondition("state", "!=", string(models.JobsStateDONE)),
			utils2.NewFilterCondition("state", "!=", string(models.JobsStateERROR)))

		jobs, err := se.GetJobsWithCondition(ctx, *filter)
		if err != nil {
			logger.Errorf("Failed to get jobs with conditions: %v. Error: %v", filter, err)
			return nil, "", err
		}
		if len(jobs) > 0 {
			for _, job := range jobs {
				for _, snapshot := range existingSnapshots {
					if snapshot.Name == job.ResourceName {
						if snapshot.State == models.LifeCycleStateREADY {
							logger.Warnf("Snapshot with name %s already exists", snapshot.Name)
							return nil, "", vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceAlreadyExistsError, customerrors.NewConflictErr("snapshot already exists"))
						} else {
							logger.Infof("Found ongoing snapshot creation job for account %s with name %s. Job UUID: %s", params.AccountName, params.Name, job.UUID)
							dataStoreSnap := ConvertDatastoreSnapshotToModel(snapshot)
							return dataStoreSnap, job.UUID, nil
						}
					}
				}
			}
		}
	}

	job := &datamodel.Job{
		Type:          string(models.JobTypeCreateSnapshot),
		State:         string(models.JobsStateNEW),
		ResourceName:  params.Name,
		AccountID:     sql.NullInt64{Int64: account.ID, Valid: true},
		CorrelationID: utils.GetCoRelationIDFromContext(ctx),
		RequestID:     utils.GetRequestIDFromContext(ctx),
	}

	var dbSnapshot *datamodel.Snapshot
	// Cleanup in case of error
	defer func() {
		if err != nil {
			if job != nil && job.UUID != "" {
				logger.Warnf("Error occurred, marking job entry in DB as deleted. Job UUID: %s", job.UUID)
				if delErr := se.DeleteJob(ctx, job.UUID, err.Error()); delErr != nil {
					logger.Errorf("Failed to delete job: %v", delErr)
				}
			}
			if dbSnapshot != nil && dbSnapshot.UUID != "" {
				logger.Warnf("Error occurred, marking snapshot in DB as deleted. Snapshot UUID: %s", dbSnapshot.UUID)
				if _, delErr := se.DeleteSnapshot(ctx, dbSnapshot.UUID); delErr != nil {
					logger.Errorf("Failed to delete snapshot: %v", delErr)
				}
			}
		}
	}()

	job, err = se.CreateJob(ctx, job)
	if err != nil {
		logger.Errorf("Failed to create job in database. Error: %v", err)
		return nil, "", err
	}

	snapshot := &datamodel.Snapshot{
		Name:               params.Name,
		Description:        params.Description,
		VolumeID:           volume.ID,
		AccountID:          account.ID,
		Volume:             volume,
		Account:            account,
		IsAppConsistent:    params.IsAppConsistent,
		Type:               SNAPSHOT_TYPE_ADHOC,
		SnapshotAttributes: &datamodel.SnapshotAttributes{},
	}

	dbSnapshot, err = se.CreatingSnapshot(ctx, snapshot)
	if err != nil {
		logger.Errorf("Failed to create snapshot in database. Error: %v", err)
		return nil, "", err
	}

	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			ID:                    job.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			WorkflowRunTimeout:    workflowengine.GetWorkflowGlobalTimeout(),
		},
		workflows.CreateSnapshotWorkflow,
		params,
		dbSnapshot,
	)

	if err != nil {
		logger.Errorf("Failed to start create snapshot workflow. Error: %v ", err)
		return nil, "", err
	}

	dataStoreSnap := ConvertDatastoreSnapshotToModel(dbSnapshot)
	return dataStoreSnap, job.UUID, nil
}

func (o *Orchestrator) GetSnapshot(ctx context.Context, params *common.GetSnapshotParams) (*models.Snapshot, error) {
	return getSnapshot(ctx, o.storage, params)
}

func _getSnapshot(ctx context.Context, se database.Storage, params *common.GetSnapshotParams) (*models.Snapshot, error) {
	logger := util.GetLogger(ctx)

	volume, err := VolumeOwnershipCheck(ctx, se, params.VolumeID, params.AccountName)
	if err != nil {
		logger.Errorf("Failed to validate volume ownership")
		return nil, err
	}

	snapshot, err := se.GetSnapshotByUUID(ctx, params.SnapshotUUID, volume.Account.ID, volume.ID)
	if err != nil {
		logger.Errorf("Failed to get snapshot: %s. Error: %v", params.SnapshotUUID, err)
		return nil, err
	}

	dataStoreSnap := ConvertDatastoreSnapshotToModel(snapshot)
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
		return nil, err
	}

	snapshots, err := se.GetSnapshotsByVolumeID(ctx, volume.ID)
	if err != nil {
		logger.Errorf("Failed to get snapshots for volume: %s. Error: %v", params.VolumeID, err)
		return nil, err
	}

	var snapshotsToReturn []*models.Snapshot
	for _, snapshot := range snapshots {
		snapshotsToReturn = append(snapshotsToReturn, ConvertDatastoreSnapshotToModel(snapshot))
	}
	return snapshotsToReturn, nil
}

func (o *Orchestrator) GetMultipleSnapshots(ctx context.Context, volumeUuid string, accountName string, snapshotUUIDs []string) ([]*models.Snapshot, error) {
	se := o.storage

	account, err := getAccountWithName(ctx, se, accountName)
	if err != nil {
		if customerrors.IsNotFoundErr(err) {
			util.GetLogger(ctx).Warnf("Account with name %s not found in VCP, checking in CVP", accountName)
			return []*models.Snapshot{}, nil
		}
		return nil, err
	}

	volume, err := se.GetVolumeWithAccountID(ctx, volumeUuid, account.ID)
	if err != nil {
		if customerrors.IsNotFoundErr(err) {
			util.GetLogger(ctx).Warnf("Volume with uuid %s not found in VCP, checking in CVP", volumeUuid)
			return []*models.Snapshot{}, nil
		}
		return nil, err
	}

	filter := utils2.CreateFilterWithConditions(
		utils2.NewFilterCondition("account_id", "=", account.ID),
		utils2.NewFilterCondition("volume_id", "=", volume.ID),
		utils2.NewFilterCondition("uuid", "in", snapshotUUIDs))

	dbSnapshots, err := se.GetSnapshotsWithCondition(ctx, *filter)
	if err != nil {
		return nil, err
	}

	modelSnapshots := make([]*models.Snapshot, len(dbSnapshots))
	for i, snapshot := range dbSnapshots {
		modelSnapshots[i] = ConvertDatastoreSnapshotToModel(snapshot)
	}
	return modelSnapshots, nil
}

func (o *Orchestrator) UpdateSnapshot(ctx context.Context, params *common.UpdateSnapshotParams) (*models.Snapshot, string, error) {
	return updateSnapshot(ctx, o.storage, o.temporal, params)
}

func _updateSnapshot(ctx context.Context, se database.Storage, temporal client.Client, params *common.UpdateSnapshotParams) (*models.Snapshot, string, error) {
	logger := util.GetLogger(ctx)

	account, err := se.GetAccount(ctx, params.AccountName)
	if err != nil {
		logger.Errorf("Failed to get account: %s. Error: %v", params.AccountName, err)
		return nil, "", err
	}

	volume, err := VolumeOwnershipCheck(ctx, se, params.VolumeID, params.AccountName)
	if err != nil {
		logger.Errorf("Failed to validate volume ownership")
		return nil, "", err
	}

	snapshot, err := se.GetSnapshotByUUID(ctx, params.SnapshotUUID, account.ID, volume.ID)
	if err != nil {
		logger.Errorf("Failed to get snapshot: %s. Error: %v", params.SnapshotUUID, err)
		return nil, "", err
	}

	if snapshot.State == models.LifeCycleStateCreating || snapshot.State == models.LifeCycleStateUpdating || snapshot.State == models.LifeCycleStateDeleting {
		logger.Errorf("Snapshot %s cannot be update, while in transitioning state: %s", params.SnapshotUUID, snapshot.State)
		return nil, "", vsaerrors.NewVCPError(vsaerrors.ErrResourceStateConflictError, customerrors.NewConflictErr("Snapshot is in transition state and cannot be updated, state: "+snapshot.State))
	}

	job := &datamodel.Job{
		Type:          string(models.JobTypeUpdateSnapshot),
		State:         string(models.JobsStateNEW),
		ResourceName:  snapshot.Name,
		AccountID:     sql.NullInt64{Int64: account.ID, Valid: true},
		CorrelationID: utils.GetCoRelationIDFromContext(ctx),
		RequestID:     utils.GetRequestIDFromContext(ctx),
	}

	// Cleanup in case of error
	defer func() {
		if err != nil {
			if job != nil && job.UUID != "" {
				logger.Warnf("Error occurred, marking job entry in DB as deleted. Job UUID: %s", job.UUID)
				if delErr := se.DeleteJob(ctx, job.UUID, err.Error()); delErr != nil {
					logger.Errorf("Failed to delete job: %v", delErr)
				}
			}
		}
	}()

	job, err = se.CreateJob(ctx, job)
	if err != nil {
		logger.Errorf("Failed to create job in database. Error: %v", err)
		return nil, "", err
	}

	snapshot.Description = params.Description // Only snapshot description is allowed to be updated in GCNV

	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			ID:                    job.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			WorkflowRunTimeout:    workflowengine.GetWorkflowGlobalTimeout(),
		},
		workflows.UpdateSnapshotWorkflow,
		snapshot,
	)
	if err != nil {
		logger.Errorf("Failed to start update snapshot workflow. Error: %v ", err)
		return nil, "", err
	}

	dataStoreSnap := ConvertDatastoreSnapshotToModel(snapshot)
	return dataStoreSnap, job.UUID, nil
}

func _convertDatastoreSnapshotToModel(snapshot *datamodel.Snapshot) *models.Snapshot {
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
		SizeInBytes:           uint64(snapshot.SnapshotAttributes.SizeInBytes),
		StorageClass:          STORAGE_CLASS_SOFTWARE,
	}
	return res
}

func validateCreatSnapshotOperation(volume *datamodel.Volume, params *common.CreateSnapshotParams, account *datamodel.Account) error {
	if params.Name == "" {
		return vsaerrors.NewVCPError(vsaerrors.ErrInputValidationError, customerrors.NewUserInputValidationErr("Snapshot name is empty. Please provide a valid name."))
	}

	if volume.State == models.LifeCycleStateCreating {
		return vsaerrors.NewVCPError(vsaerrors.ErrResourceStateConflictError, customerrors.NewConflictErr("Can not create a snapshot when volume is in creating stage."))
	}
	if volume.State == models.LifeCycleStateDeleting {
		return vsaerrors.NewVCPError(vsaerrors.ErrResourceStateConflictError, customerrors.NewConflictErr("Can not create a snapshot when volume is in deleting stage."))
	}

	// @TODO: Include DataProtection check when implemented

	return nil
}

// DeleteSnapshot deletes the specified snapshot
func (o *Orchestrator) DeleteSnapshot(ctx context.Context, params *common.DeleteSnapshotParams) (*models.Snapshot, string, error) {
	return deleteSnapshot(ctx, o.storage, o.temporal, params)
}

// DeleteSnapshot deletes the specified snapshot from the specified volume belonging to the specified owner
func _deleteSnapshot(ctx context.Context, se database.Storage, temporal client.Client, params *common.DeleteSnapshotParams) (*models.Snapshot, string, error) {
	logger := util.GetLogger(ctx)

	volume, err := VolumeOwnershipCheck(ctx, se, params.VolumeID, params.AccountName)
	if err != nil {
		logger.Errorf("Failed to validate volume ownership")
		return nil, "", err
	}

	snapshot, err := se.GetSnapshotByUUID(ctx, params.SnapshotID, volume.Account.ID, volume.ID)
	if err != nil {
		return nil, "", err
	}

	snapshot.Volume = volume
	if snapshot.State == models.LifeCycleStateDeleting ||
		snapshot.State == models.LifeCycleStateCreating ||
		snapshot.State == models.LifeCycleStateUpdating {
		return nil, "", vsaerrors.NewVCPError(vsaerrors.ErrResourceStateConflictError, customerrors.NewConflictErr("Snapshot is in transition state and cannot be deleted, state: "+snapshot.State))
	}

	job := &datamodel.Job{
		Type:          string(models.JobTypeDeleteSnapshot),
		State:         string(models.JobsStateNEW),
		ResourceName:  snapshot.Name,
		AccountID:     sql.NullInt64{Int64: snapshot.Account.ID, Valid: true},
		CorrelationID: utils.GetCoRelationIDFromContext(ctx),
		RequestID:     utils.GetRequestIDFromContext(ctx),
	}

	// Cleanup in case of error
	defer func() {
		if err != nil {
			if job != nil && job.UUID != "" {
				logger.Warnf("Error occurred, marking job entry in DB as deleted. Job UUID: %s", job.UUID)
				if delErr := se.DeleteJob(ctx, job.UUID, err.Error()); delErr != nil {
					logger.Errorf("Failed to delete job: %v", delErr)
				}
			}
		}
	}()

	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Errorf("Failed to create snapshot delete job in database %v", err)
		return nil, "", err
	}

	if err = se.DeletingSnapshot(ctx, snapshot); err != nil {
		return nil, "", err
	}

	location, err := getLocationFromVendorID(volume.Pool.VendorID)
	if err != nil {
		logger.Error("Failed to get location from vendor ID: ", "error", err)
		return nil, "", err
	}

	// controlWorkflowID defines the workflow ID for the control workflow
	controlWorkflowID := fmt.Sprintf(workflows.VolumeCreateDeleteSnapshotDeleteSeq, volume.Account.ID, location, volume.Pool.Name)
	err = workflows.ExecuteWorkflowSequentially(
		temporal,
		ctx,
		client.StartWorkflowOptions{
			TaskQueue: workflowengine.CustomerTaskQueue,
			ID:        controlWorkflowID,
		},
		workflows.DeleteSnapshotWorkflow,
		workflow.ChildWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			WorkflowID:            createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			WorkflowRunTimeout:    workflowengine.GetWorkflowGlobalTimeout(),
		},
		params,
		snapshot,
	)
	if err != nil {
		logger.Error("Failed to start delete snapshot workflow: ", "error", err)
		return nil, "", err
	}

	return ConvertDatastoreSnapshotToModel(snapshot), createdJob.UUID, nil
}

func _volumeOwnershipCheck(ctx context.Context, se database.Storage, volumeUUID string, accountName string) (*datamodel.Volume, error) {
	logger := util.GetLogger(ctx)

	volume, err := se.VerifyVolumeOwnership(ctx, volumeUUID, accountName)
	if err != nil {
		if customerrors.IsNotFoundErr(err) {
			logger.Errorf("Volume %s not found for account %s", volumeUUID, accountName)
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrResourceNotFound, customerrors.NewNotFoundErr("volume", &volumeUUID))
		}
		logger.Errorf("Failed to verify volume ownership: %v", err)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrInputValidationError, customerrors.NewUserInputValidationErr("failed to validate volume ownership"))
	}

	return volume, nil
}

func _validateSnapshotName(name string) error {
	if name == "" {
		return customerrors.NewUserInputValidationErr(snapshotNameErrorEmpty)
	}
	if match, _ := regexp.MatchString(snapshotNamePattern, name); !match {
		return customerrors.NewUserInputValidationErr(snapshotNameError)
	}
	if strings.Contains(name, "..") {
		return customerrors.NewUserInputValidationErr(snapshotNameErrorDots)
	}
	if name == "." {
		return customerrors.NewUserInputValidationErr(snapshotNameErrorSingleDot)
	}
	for _, reg := range illegalNamesRegexp {
		if reg.MatchString(name) {
			return customerrors.NewUserInputValidationErr(snapshotNameErrorIllegalNames)
		}
	}
	return nil
}

// DeleteSnapmirrorSnapshots deletes the snapmirror snapshots for the specified volume belonging to the specified owner
func (o *Orchestrator) DeleteSnapmirrorSnapshots(ctx context.Context, params *common.SnapshotsInternalDeleteParams) (string, error) {
	return deleteSnapshots(ctx, o.storage, o.temporal, params)
}

// DeleteSnapshot deletes the specified snapshot from the specified volume belonging to the specified owner
func _deleteSnapshots(ctx context.Context, se database.Storage, temporal client.Client, params *common.SnapshotsInternalDeleteParams) (string, error) {
	logger := util.GetLogger(ctx)
	dbVolume, err := VolumeOwnershipCheck(ctx, se, params.VolumeID, params.AccountName)
	if err != nil {
		logger.Errorf("Failed to validate volume ownership")
		return "", err
	}
	if dbVolume.State == models.LifeCycleStateRetained {
		return "", customerrors.NewNotFoundErr("Volume", nil)
	}
	if dbVolume.State == models.LifeCycleStateDeleting {
		return "", vsaerrors.NewVCPError(vsaerrors.ErrResourceStateConflictError, customerrors.NewConflictErr("Volume of the snapshot is being deleted."))
	}
	if dbVolume.State == models.VolumeStateOffline {
		return "", vsaerrors.NewVCPError(vsaerrors.ErrResourceStateConflictError, customerrors.NewConflictErr("Volume is offline."))
	}

	job := &datamodel.Job{
		Type:          string(models.JobTypeDeleteSnapmirrorSnapshotsInternal),
		State:         string(models.JobsStateNEW),
		ResourceName:  dbVolume.Name,
		AccountID:     sql.NullInt64{Int64: dbVolume.Account.ID, Valid: true},
		CorrelationID: utils.GetCoRelationIDFromContext(ctx),
		RequestID:     utils.GetRequestIDFromContext(ctx),
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: dbVolume.UUID,
			PoolUUID:     dbVolume.Pool.UUID,
		},
	}

	// Cleanup in case of error
	defer func() {
		if err != nil {
			if job != nil && job.UUID != "" {
				logger.Warnf("Error occurred, marking job entry in DB as deleted. Job UUID: %s", job.UUID)
				if delErr := se.DeleteJob(ctx, job.UUID, err.Error()); delErr != nil {
					logger.Errorf("Failed to delete job: %v", delErr)
				}
			}
		}
	}()

	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Errorf("Failed to create snapshot delete job in database %v", err)
		return "", err
	}

	params.Volume = dbVolume
	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			ID:                    createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			WorkflowRunTimeout:    workflowengine.GetWorkflowGlobalTimeout(),
		},
		replicationWorkflows.DeleteInternalSnapshotWorkflow,
		params,
	)
	if err != nil {
		logger.Error("Failed to start delete snapshot workflow: ", "error", err)
		return "", err
	}

	return createdJob.UUID, nil
}
