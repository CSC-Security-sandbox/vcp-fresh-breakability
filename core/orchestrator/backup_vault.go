package orchestrator

import (
	"context"
	"database/sql"
	"errors"
	"strconv"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"gorm.io/gorm"
)

var (
	convertDatastoreBackupVaultToModel = _convertDatastoreBackupVaultToModel
	getBackupVaultByNameAndOwnerID     = _getBackupVaultByNameAndOwnerID
	updateBackupVault                  = _updateBackupVault
	deleteBackupVault                  = _deleteBackupVault
)

// CreateBackupVaultParams describes parameters supplied to CreateBackupVault
type CreateBackupVaultParams struct {
	BackupVaultID              string
	Name                       string
	Description                *string
	Region                     string
	AccountVendorID            string
	BackupRegion               *string
	BackupVaultType            string
	SourceRegion               *string
	BackupRetentionPolicy      *BackupRetentionPolicyV2params
	ExternalUUID               string
	CrossRegionBackupVaultName *string
	ProjectNumber              string
}

// BackupRetentionPolicyV2params describes request parameters for BackupRetentionPolicyV2
type BackupRetentionPolicyV2params struct {
	BackupMinimumEnforcedRetentionDuration *int64
	IsDailyBackupImmutable                 bool
	IsMonthlyBackupImmutable               bool
	IsWeeklyBackupImmutable                bool
	IsAdhocBackupImmutable                 bool
}

func (o *Orchestrator) ListBackupVaults(ctx context.Context, accountName string) ([]*models.BackupVaultV1beta, error) {
	se := o.storage
	account, err := getOrCreateAccount(ctx, se, accountName)
	if err != nil {
		return nil, err
	}

	return ListBackupVaultsByOwnerID(ctx, se, account.ID)
}

func (o *Orchestrator) DeleteBackupVault(ctx context.Context, params *commonparams.BackupVaultParams) (*models.BackupVaultV1beta, string, error) {
	return deleteBackupVault(ctx, o.storage, o.temporal, params)
}

func _deleteBackupVault(ctx context.Context, se database.Storage, temporal client.Client, params *commonparams.BackupVaultParams) (*models.BackupVaultV1beta, string, error) {
	logger := util.GetLogger(ctx)
	account, err := getOrCreateAccount(ctx, se, params.OwnerID)
	if err != nil {
		return nil, "", err
	}

	dbBv, err := se.GetBackupVaultByUUIDndOwnerID(ctx, params.BackupVaultID, account.ID)
	if err != nil {
		return nil, "", err
	}

	if dbBv.LifeCycleState == models.LifeCycleStateUpdating || dbBv.LifeCycleState == models.LifeCycleStateDeleting {
		return nil, "", customerrors.NewUserInputValidationErr("backup vault is in transition state")
	}

	backups, err := se.GetBackupCountByBackupVaultID(ctx, dbBv.ID)
	if backups > 0 {
		return nil, "", customerrors.NewUserInputValidationErr("backup vault has backups, please delete backups before deleting backup vault")
	}
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, "", customerrors.NewNotFoundErr("backup vault", &params.BackupVaultID)
		}
		return nil, "", err
	}

	volumes, err := se.GetVolumeCountByBackupVaultID(ctx, dbBv.UUID)
	if volumes > 0 {
		return nil, "", customerrors.NewUserInputValidationErr("backup vault has volumes attached, please delete volumes before deleting backup vault")
	}
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, "", customerrors.NewNotFoundErr("backup vault", &params.BackupVaultID)
		}
		return nil, "", err
	}

	job := &datamodel.Job{
		Type:          string(models.JobTypeDeleteBackupVault),
		State:         string(models.JobsStateNEW),
		ResourceName:  params.Name,
		AccountID:     sql.NullInt64{Int64: account.ID, Valid: true},
		CorrelationID: utils.GetCoRelationIDFromContext(ctx),
		RequestID:     utils.GetRequestIDFromContext(ctx),
	}

	// Store original state for rollback
	originalState := dbBv.LifeCycleState
	originalStateDetails := dbBv.LifeCycleStateDetails
	workflowStarted := false
	stateUpdated := false

	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job in database", "error", err)
		return nil, "", err
	}

	// Defer function to handle rollback on workflow startup failure only
	defer func() {
		if err != nil && !workflowStarted {
			// This condition is met: err != nil (workflow start failed)
			// && !workflowStarted (true, since workflow didn't start)
			// && stateUpdated (true, since state was successfully updated)

			// Rollback the backup vault state to original state
			if stateUpdated {
				if _, rollbackErr := se.UpdateBackupVaultState(ctx, dbBv, originalState, originalStateDetails); rollbackErr != nil {
					logger.Error("Failed to rollback backup vault state", "error", rollbackErr, "originalState", originalState)
				}
			}

			// Mark the job as ERROR
			if createdJob != nil {
				if jobErr := se.UpdateJob(ctx, createdJob.UUID, string(models.JobsStateERROR), 0, err.Error()); jobErr != nil {
					logger.Error("Failed to update job state to ERROR", "error", jobErr, "jobUUID", createdJob.UUID)
				}
			}
		}
	}()

	dbBV, err := se.UpdateBackupVaultState(ctx, dbBv, models.LifeCycleStateDeleting, models.LifeCycleStateDeletingDetails)
	if err != nil {
		return nil, "", err
	}
	stateUpdated = true

	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			ID:                    createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		},
		workflows.DeleteBackupVaultWorkflow,
		params,
		dbBV,
	)
	if err != nil {
		logger.Error("Failed to start backup vault delete workflow: ", "error", err)
		return nil, "", err
	}
	workflowStarted = true
	return convertDatastoreBackupVaultToModel(dbBV), createdJob.UUID, nil
}

func (o *Orchestrator) UpdateBackupVault(ctx context.Context, params *commonparams.BackupVaultParams) (*models.BackupVaultV1beta, string, error) {
	return updateBackupVault(ctx, o.storage, o.temporal, params)
}

func _updateBackupVault(ctx context.Context, se database.Storage, temporal client.Client, params *commonparams.BackupVaultParams) (*models.BackupVaultV1beta, string, error) {
	logger := util.GetLogger(ctx)
	account, err := getOrCreateAccount(ctx, se, params.OwnerID)
	if err != nil {
		return nil, "", err
	}

	dbBv, err := se.GetBackupVaultByUUIDndOwnerID(ctx, params.BackupVaultID, account.ID)
	if err != nil {
		return nil, "", err
	}

	if dbBv.LifeCycleState == models.LifeCycleStateUpdating || dbBv.LifeCycleState == models.LifeCycleStateDeleting {
		return nil, "", customerrors.NewUserInputValidationErr("backup vault is in transition state")
	}

	job := &datamodel.Job{
		Type:          string(models.JobTypeUpdateBackupVault),
		State:         string(models.JobsStateNEW),
		ResourceName:  params.Name,
		AccountID:     sql.NullInt64{Int64: account.ID, Valid: true},
		CorrelationID: utils.GetCoRelationIDFromContext(ctx),
		RequestID:     utils.GetRequestIDFromContext(ctx),
	}

	// Store original state for rollback
	originalState := dbBv.LifeCycleState
	originalStateDetails := dbBv.LifeCycleStateDetails
	workflowStarted := false
	stateUpdated := false

	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job in database", "error", err)
		return nil, "", err
	}

	// Defer function to handle rollback on workflow startup failure only
	defer func() {
		if err != nil && !workflowStarted {
			// Only rollback if the state was successfully updated but workflow failed to start
			// The workflow will handle its own error states
			if stateUpdated {
				if _, rollbackErr := se.UpdateBackupVaultState(ctx, dbBv, originalState, originalStateDetails); rollbackErr != nil {
					logger.Error("Failed to rollback backup vault state", "error", rollbackErr, "originalState", originalState)
				}
			}

			// Mark job as error if it was created
			if createdJob != nil {
				if jobErr := se.UpdateJob(ctx, createdJob.UUID, string(models.JobsStateERROR), 0, err.Error()); jobErr != nil {
					logger.Error("Failed to update job state to ERROR", "error", jobErr, "jobUUID", createdJob.UUID)
				}
			}
		}
	}()

	dbBV, err := se.UpdateBackupVaultState(ctx, dbBv, models.LifeCycleStateUpdating, models.LifeCycleStateUpdatingDetails)
	if err != nil {
		return nil, "", err
	}
	stateUpdated = true

	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			ID:                    createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			WorkflowRunTimeout:    workflowengine.GetWorkflowGlobalTimeout(),
		},
		workflows.UpdateBackupVaultWorkflow,
		params,
		dbBV,
	)
	if err != nil {
		logger.Error("Failed to start backup vault update workflow: ", "error", err)
		return nil, "", err
	}
	workflowStarted = true
	return convertDatastoreBackupVaultToModel(dbBV), createdJob.UUID, nil
}

func (o *Orchestrator) GetBackupVaultByUUID(ctx context.Context, bvUUID string, ownerID string) (*models.BackupVaultV1beta, error) {
	se := o.storage
	account, err := getOrCreateAccount(ctx, se, ownerID)
	if err != nil {
		return nil, err
	}
	return GetBackupVaultByUUIDAndOwnerID(ctx, se, bvUUID, account.ID)
}

func GetBackupVaultByUUIDAndOwnerID(ctx context.Context, se database.Storage, bvUUID string, accountID int64) (*models.BackupVaultV1beta, error) {
	bvDetails, err := se.GetBackupVaultByUUIDndOwnerID(ctx, bvUUID, accountID)
	if err != nil {
		return nil, err
	}

	return convertDatastoreBackupVaultToModel(bvDetails), nil
}

func ListBackupVaultsByOwnerID(ctx context.Context, se database.Storage, ownerID int64) ([]*models.BackupVaultV1beta, error) {
	bvDetails, err := se.ListBackupVaults(ctx, ownerID)
	if err != nil {
		return nil, err
	}

	var backupVaults []*models.BackupVaultV1beta
	for _, bv := range bvDetails {
		backupVaults = append(backupVaults, convertDatastoreBackupVaultToModel(bv))
	}
	return backupVaults, nil
}

func (o *Orchestrator) GetBackupVaultByNameAndOwnerID(ctx context.Context, bvName, ownerID string) (*models.BackupVaultV1beta, error) {
	se := o.storage
	bvDetails, err := getBackupVaultByNameAndOwnerID(ctx, se, bvName, ownerID)
	if err != nil {
		return nil, err
	}
	return convertDatastoreBackupVaultToModel(bvDetails), nil
}

func _getBackupVaultByNameAndOwnerID(ctx context.Context, se database.Storage, bvName, ownerID string) (*datamodel.BackupVault, error) {
	account, err := getOrCreateAccount(ctx, se, ownerID)
	if err != nil {
		return nil, err
	}

	bv, err := se.GetBackupVaultByNameAndOwnerID(ctx, bvName, strconv.FormatInt(account.ID, 10))
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, customerrors.NewNotFoundErr("backup vault", &bvName)
		}
		return nil, err
	}

	return bv, nil
}

func _convertDatastoreBackupVaultToModel(bv *datamodel.BackupVault) *models.BackupVaultV1beta {
	res := &models.BackupVaultV1beta{
		ID:                         bv.ID,
		OwnerID:                    bv.Account.UUID,
		BackupVaultID:              bv.UUID,
		Name:                       bv.Name,
		Description:                bv.Description,
		LifeCycleState:             bv.LifeCycleState,
		LifeCycleStateDetails:      bv.LifeCycleStateDetails,
		CreatedAt:                  bv.CreatedAt,
		UpdatedAt:                  bv.UpdatedAt,
		BackupRegion:               bv.BackupRegionName,
		SourceRegion:               bv.SourceRegionName,
		Region:                     bv.RegionName,
		AccountVendorID:            bv.AccountVendorID,
		SourceBackupVault:          &bv.Name,
		DestinationBackupVault:     bv.CrossRegionBackupVaultName,
		BackupVaultType:            &bv.BackupVaultType,
		CrossRegionBackupVaultName: bv.CrossRegionBackupVaultName,
	}
	if bv.ImmutableAttributes != nil {
		res.BackupRetentionPolicy = models.BackupRetentionPolicyparams{
			BackupMinimumEnforcedRetentionDuration: bv.ImmutableAttributes.BackupMinimumEnforcedRetentionDuration,
			IsDailyBackupImmutable:                 bv.ImmutableAttributes.IsDailyBackupImmutable,
			IsMonthlyBackupImmutable:               bv.ImmutableAttributes.IsMonthlyBackupImmutable,
			IsWeeklyBackupImmutable:                bv.ImmutableAttributes.IsWeeklyBackupImmutable,
			IsAdhocBackupImmutable:                 bv.ImmutableAttributes.IsAdhocBackupImmutable,
		}
	}
	return res
}

// GetMultipleBackupVaults gets BackupVault records for the UUIDs provided
func (o *Orchestrator) GetMultipleBackupVaults(ctx context.Context, backupVaultUUIDList []string) ([]*models.BackupVaultV1beta, error) {
	se := o.storage

	conditions := [][]interface{}{{"uuid in ?", backupVaultUUIDList}}
	backupVaultList, err := se.GetMultipleBackupVaults(ctx, conditions)
	if err != nil {
		return nil, err
	}
	var backupVaultModelList []*models.BackupVaultV1beta
	for _, backupVault := range backupVaultList {
		backupVaultModel := convertDatastoreBackupVaultToModel(backupVault)
		backupVaultModelList = append(backupVaultModelList, backupVaultModel)
	}

	return backupVaultModelList, nil
}
