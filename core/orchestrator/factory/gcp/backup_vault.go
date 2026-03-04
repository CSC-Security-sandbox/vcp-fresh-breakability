package gcp

import (
	"context"
	"database/sql"
	"errors"
	"strconv"

	cvpmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/client"
	"gorm.io/gorm"
)

var (
	convertDatastoreBackupVaultToModel = _convertDatastoreBackupVaultToModel
	getBackupVaultByNameAndOwnerID     = _getBackupVaultByNameAndOwnerID
	updateBackupVault                  = _updateBackupVault
	deleteBackupVault                  = _deleteBackupVault
	convertCVPToBackupVaultDataModel   = activities.ConvertToBackupVaultDataModel
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
	TenantProject              string
	ServiceType                string
}

// BackupRetentionPolicyV2params describes request parameters for BackupRetentionPolicyV2
type BackupRetentionPolicyV2params struct {
	BackupMinimumEnforcedRetentionDuration *int64
	IsDailyBackupImmutable                 bool
	IsMonthlyBackupImmutable               bool
	IsWeeklyBackupImmutable                bool
	IsAdhocBackupImmutable                 bool
}

func (o *GCPOrchestrator) ListBackupVaults(ctx context.Context, accountName string) ([]*models.BackupVaultV1beta, error) {
	se := o.storage
	account, err := getOrCreateAccount(ctx, se, accountName)
	if err != nil {
		return nil, err
	}

	return ListBackupVaultsByOwnerID(ctx, se, account.ID)
}

func (o *GCPOrchestrator) DeleteBackupVault(ctx context.Context, params *commonparams.BackupVaultParams) (*models.BackupVaultV1beta, string, error) {
	return deleteBackupVault(ctx, o.storage, o.temporal, params)
}

func (o *GCPOrchestrator) DeleteBackupVaultInternal(ctx context.Context, params *commonparams.BackupVaultParams) (string, error) {
	se := o.storage
	account, err := se.GetAccount(ctx, params.OwnerID)
	if err != nil {
		return "", err
	}
	RemoteBV, err := se.GetBackupVaultByExternalUUIDAndOwnerID(ctx, params.BackupVaultID, account.ID)
	if err != nil {
		return "", err
	}
	params.BackupVaultID = RemoteBV.UUID
	_, err = se.DeleteBackupVaultInVCP(ctx, RemoteBV.UUID)
	if err != nil {
		return "", err
	}
	return "", nil
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

	if dbBv.BackupVaultType == activities.CrossRegionBackupType && params.Region == *dbBv.BackupRegionName {
		return nil, "", customerrors.NewUserInputValidationErr("backup vault cannot be deleted from the destination region")
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
	stateUpdated := false

	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job in database", "error", err)
		return nil, "", err
	}

	// Defer function to handle rollback on workflow startup failure only
	defer func() {
		if err != nil {
			// This condition is met: err != nil (workflow start failed)
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

	workflowExecutor := workflows.NewWorkflowExecutor(temporal, logger)
	err = workflowExecutor.ExecuteWorkflow(
		ctx,
		createdJob.WorkflowID,
		workflowengine.CustomerTaskQueue,
		workflows.DeleteBackupVaultWorkflow,
		nil,
		params,
		dbBV,
	)
	if err != nil {
		logger.Error("Failed to start backup vault delete workflow after retries: ", "error", err)
		return nil, "", err
	}
	return convertDatastoreBackupVaultToModel(dbBV), createdJob.UUID, nil
}

func (o *GCPOrchestrator) UpdateBackupVault(ctx context.Context, params *commonparams.BackupVaultParams) (*models.BackupVaultV1beta, string, error) {
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

	if dbBv.BackupVaultType == activities.CrossRegionBackupType && params.Region == *dbBv.BackupRegionName {
		return nil, "", customerrors.NewUserInputValidationErr("cross-region backup vault cannot be updated from the destination region")
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
	stateUpdated := false

	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job in database", "error", err)
		return nil, "", err
	}

	// Defer function to handle rollback on workflow startup failure only
	defer func() {
		if err != nil {
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

	workflowExecutor := workflows.NewWorkflowExecutor(temporal, logger)
	err = workflowExecutor.ExecuteWorkflow(
		ctx,
		createdJob.WorkflowID,
		workflowengine.CustomerTaskQueue,
		workflows.UpdateBackupVaultWorkflow,
		nil,
		params,
		dbBV,
	)
	if err != nil {
		logger.Error("Failed to start backup vault update workflow after retries: ", "error", err)
		return nil, "", err
	}
	return convertDatastoreBackupVaultToModel(dbBV), createdJob.UUID, nil
}

func (o *GCPOrchestrator) GetBackupVaultByUUID(ctx context.Context, bvUUID string, ownerID string) (*models.BackupVaultV1beta, error) {
	se := o.storage
	account, err := getOrCreateAccount(ctx, se, ownerID)
	if err != nil {
		return nil, err
	}
	return GetBackupVaultByUUIDAndOwnerID(ctx, se, bvUUID, account.ID)
}

// GetBackupVaultByUUIDWithoutAccount gets backup vault by UUID without account filtering (for GCBDR vaults)
func (o *GCPOrchestrator) GetBackupVaultByUUIDWithoutAccount(ctx context.Context, bvUUID string) (*models.BackupVaultV1beta, error) {
	se := o.storage
	bvDetails, err := se.GetBackupVault(ctx, bvUUID)
	if err != nil {
		return nil, err
	}
	return convertDatastoreBackupVaultToModel(bvDetails), nil
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

func (o *GCPOrchestrator) GetBackupVaultByNameAndOwnerID(ctx context.Context, bvName, ownerID string) (*models.BackupVaultV1beta, error) {
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
		ExternalUUID:               bv.ExternalUUID,
		AccountName:                bv.Account.Name,
		ServiceType:                bv.ServiceType,
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
	if bv.CmekAttributes != nil {
		res.KmsConfigResourcePath = bv.CmekAttributes.KmsConfigResourcePath
		res.EncryptionState = bv.CmekAttributes.EncryptionState
		res.BackupsPrimaryKeyVersion = bv.CmekAttributes.BackupsPrimaryKeyVersion
	}
	return res
}

// GetMultipleBackupVaults gets BackupVault records for the UUIDs provided
func (o *GCPOrchestrator) GetMultipleBackupVaults(ctx context.Context, backupVaultUUIDList []string) ([]*models.BackupVaultV1beta, error) {
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

// IsBackupVaultAttachedToVolume checks if a backup vault is attached to any volume in the VCP database
func (o *GCPOrchestrator) IsBackupVaultAttachedToVolume(ctx context.Context, backupVaultUUID string) (bool, error) {
	se := o.storage

	volumeCount, err := se.GetVolumeCountByBackupVaultID(ctx, backupVaultUUID)
	if err != nil {
		return false, err
	}

	return volumeCount > 0, nil
}

// GetBackupVaultUUIDsFromBackupPolicyUUID retrieves all backup vault UUIDs associated with a backup policy
func (o *GCPOrchestrator) GetBackupVaultUUIDsFromBackupPolicyUUID(ctx context.Context, backupPolicyUUID string, accountName string) ([]string, error) {
	se := o.storage
	account, err := se.GetAccount(ctx, accountName)
	if err != nil {
		return nil, err
	}
	backupVaultUUIDs, err := se.GetBackupVaultUUIDsFromBackupPolicyUUID(ctx, backupPolicyUUID, account.ID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, customerrors.NewNotFoundErr("backup vaults for backup policy", &backupPolicyUUID)
		}
		return nil, err
	}
	return backupVaultUUIDs, nil
}

// CreateBackupVaultEntryInVCP creates a BackupVault entry directly in the VCP database for cross-region operations
func (o *GCPOrchestrator) CreateBackupVaultEntryInVCP(ctx context.Context, bv *datamodel.BackupVault, accountName string) (*datamodel.BackupVault, error) {
	se := o.storage
	logger := util.GetLogger(ctx)

	account, err := getOrCreateAccount(ctx, se, accountName)
	if err != nil {
		return nil, err
	}
	bv.AccountID = account.ID
	backupVault, err := se.CreateBackupVaultEntryInVCP(ctx, bv)
	if err != nil {
		logger.Errorf("Failed to create cross-region backup vault entry in VCP: %v", err)
		return nil, err
	}
	return backupVault, nil
}

// CreateBackupVaultEntryInVCPFromCVP converts a CVP backup vault response to the VCP datamodel and creates
// a BackupVault entry in the VCP database. Used when GCBDR_VAULT_ENABLED is set to mirror CVP-created vaults in VCP.
// When tenantProject is non-empty, the vault is tagged as GCBDR with the tenant project stored in bucket_details.
func (o *GCPOrchestrator) CreateBackupVaultEntryInVCPFromCVP(ctx context.Context, cvpBV *cvpmodels.BackupVaultV1beta, region, accountName string, tenantProject string) (*datamodel.BackupVault, error) {
	if cvpBV == nil {
		return nil, errors.New("CVP backup vault is nil")
	}
	bv, err := convertCVPToBackupVaultDataModel(cvpBV, region)
	if err != nil {
		return nil, err
	}

	if tenantProject != "" {
		bv.ServiceType = models.ServiceTypeGCBDR
		bv.BucketDetails = datamodel.BucketDetailsArray{
			&datamodel.BucketDetails{
				TenantProjectNumber: tenantProject,
			},
		}
	} else {
		bv.ServiceType = models.ServiceTypeGCNV
	}

	return o.CreateBackupVaultEntryInVCP(ctx, bv, accountName)
}

// GetBackupVaultByExternalUUIDAndOwnerID gets a BackupVault by external UUID directly from storage for cross-region operations
// The external UUID is used as the identifier for cross-region BackupVault references
func (o *GCPOrchestrator) GetBackupVaultByExternalUUIDAndOwnerID(ctx context.Context, externalUUID string, ownerID string) (*datamodel.BackupVault, error) {
	se := o.storage
	account, err := getOrCreateAccount(ctx, se, ownerID)
	if err != nil {
		return nil, err
	}
	return se.GetBackupVaultByExternalUUIDAndOwnerID(ctx, externalUUID, account.ID)
}

// UpdateBackupVaultInternal handles internal updates to a BackupVault in the VCP database
// This is used for cross-region operations where the remote region's VCP database needs to be updated
func (o *GCPOrchestrator) UpdateBackupVaultInternal(ctx context.Context, params *commonparams.BackupVaultParams, useExternalUUID bool) (*models.BackupVaultV1beta, string, error) {
	logger := util.GetLogger(ctx)
	se := o.storage
	account, err := getOrCreateAccount(ctx, se, params.OwnerID)
	if err != nil {
		return nil, "", err
	}
	var existingBV *datamodel.BackupVault
	if useExternalUUID == true {
		existingBV, err = se.GetBackupVaultByExternalUUIDAndOwnerID(ctx, params.BackupVaultID, account.ID)
		if err != nil {
			return nil, "", err
		}
	} else {
		existingBV, err = se.GetBackupVaultByUUIDndOwnerID(ctx, params.BackupVaultID, account.ID)
		if err != nil {
			return nil, "", err
		}
	}
	updatedBV := &datamodel.BackupVault{
		BaseModel: existingBV.BaseModel,
		AccountID: existingBV.AccountID,
	}

	if params.Description != nil {
		updatedBV.Description = params.Description
	} else {
		updatedBV.Description = existingBV.Description
	}

	brp := params.BackupRetentionPolicy
	if brp.BackupMinimumEnforcedRetentionDuration != nil ||
		brp.IsDailyBackupImmutable != nil ||
		brp.IsWeeklyBackupImmutable != nil ||
		brp.IsMonthlyBackupImmutable != nil ||
		brp.IsAdhocBackupImmutable != nil {
		if existingBV.ImmutableAttributes != nil {
			updatedBV.ImmutableAttributes = &datamodel.ImmutableAttributes{
				BackupMinimumEnforcedRetentionDuration: existingBV.ImmutableAttributes.BackupMinimumEnforcedRetentionDuration,
				IsDailyBackupImmutable:                 existingBV.ImmutableAttributes.IsDailyBackupImmutable,
				IsWeeklyBackupImmutable:                existingBV.ImmutableAttributes.IsWeeklyBackupImmutable,
				IsMonthlyBackupImmutable:               existingBV.ImmutableAttributes.IsMonthlyBackupImmutable,
				IsAdhocBackupImmutable:                 existingBV.ImmutableAttributes.IsAdhocBackupImmutable,
			}
		} else {
			updatedBV.ImmutableAttributes = &datamodel.ImmutableAttributes{}
		}

		if brp.BackupMinimumEnforcedRetentionDuration != nil {
			updatedBV.ImmutableAttributes.BackupMinimumEnforcedRetentionDuration = brp.BackupMinimumEnforcedRetentionDuration
		}
		if brp.IsDailyBackupImmutable != nil {
			updatedBV.ImmutableAttributes.IsDailyBackupImmutable = *brp.IsDailyBackupImmutable
		}
		if brp.IsWeeklyBackupImmutable != nil {
			updatedBV.ImmutableAttributes.IsWeeklyBackupImmutable = *brp.IsWeeklyBackupImmutable
		}
		if brp.IsMonthlyBackupImmutable != nil {
			updatedBV.ImmutableAttributes.IsMonthlyBackupImmutable = *brp.IsMonthlyBackupImmutable
		}
		if brp.IsAdhocBackupImmutable != nil {
			updatedBV.ImmutableAttributes.IsAdhocBackupImmutable = *brp.IsAdhocBackupImmutable
		}
	} else {
		updatedBV.ImmutableAttributes = existingBV.ImmutableAttributes
	}

	// CMEK attributes: start from existing attributes and overlay any CMEK fields
	// provided in the internal request (used by cross-region CMEK hydration).
	if existingBV.CmekAttributes != nil {
		updatedBV.CmekAttributes = &datamodel.CmekAttributes{
			KmsConfigResourcePath:    existingBV.CmekAttributes.KmsConfigResourcePath,
			EncryptionState:          existingBV.CmekAttributes.EncryptionState,
			BackupsPrimaryKeyVersion: existingBV.CmekAttributes.BackupsPrimaryKeyVersion,
		}
	}
	if params.CmekEncryptionState != nil || params.CmekBackupsPrimaryKeyVersion != nil {
		if updatedBV.CmekAttributes == nil {
			updatedBV.CmekAttributes = &datamodel.CmekAttributes{}
		}
		if params.CmekEncryptionState != nil {
			updatedBV.CmekAttributes.EncryptionState = params.CmekEncryptionState
		}
		if params.CmekBackupsPrimaryKeyVersion != nil {
			updatedBV.CmekAttributes.BackupsPrimaryKeyVersion = params.CmekBackupsPrimaryKeyVersion
		}
	}

	if params.BucketDetails != nil && len(params.BucketDetails) > 0 {
		updatedBV.BucketDetails = params.BucketDetails
	} else {
		updatedBV.BucketDetails = existingBV.BucketDetails
	}

	updatedBV.LifeCycleState = existingBV.LifeCycleState
	updatedBV.LifeCycleStateDetails = existingBV.LifeCycleStateDetails
	// Only manipulate ExternalUUID when we are in the "external UUID lookup"
	// mode (typical CRB flows). For CMEK hydration (useExternalUUID == false),
	// we must not overwrite the source vault's ExternalUUID.
	if useExternalUUID {
		updatedBV.ExternalUUID = &params.BackupVaultID
	} else {
		updatedBV.ExternalUUID = existingBV.ExternalUUID
	}

	resultBV, err := se.UpdateBackupVaultInVCP(ctx, updatedBV, existingBV)
	if err != nil {
		logger.Error("Failed to update backup vault in VCP database", "error", err, "backupVaultId", params.BackupVaultID)
		return nil, "", err
	}

	logger.Info("Successfully updated backup vault in VCP database",
		"backupVaultId", params.BackupVaultID,
		"ownerID", params.OwnerID)

	return convertDatastoreBackupVaultToModel(resultBV), "", nil
}

// RotateCmekBackupsForBackupVault creates a job and starts a Temporal workflow to
// rotate CMEK for all backups within a backup vault tracked by VCP.
func (o *GCPOrchestrator) RotateCmekBackupsForBackupVault(
	ctx context.Context,
	params *commonparams.BackupVaultParams,
	primaryKeyVersion string,
) (string, error) {
	logger := util.GetLogger(ctx)
	se := o.storage

	account, err := getOrCreateAccount(ctx, se, params.OwnerID)
	if err != nil {
		return "", err
	}

	dbBv, err := se.GetBackupVaultByUUIDndOwnerID(ctx, params.BackupVaultID, account.ID)
	if err != nil {
		return "", err
	}

	// Reject rotation if backup vault is in a transitional state.
	if dbBv.LifeCycleState == models.LifeCycleStateUpdating || dbBv.LifeCycleState == models.LifeCycleStateDeleting {
		return "", customerrors.NewUserInputValidationErr("backup vault is in transition state")
	}

	// Reject rotation if the backup vault is not CMEK-configured. VCP's CMEK
	if dbBv.CmekAttributes == nil || dbBv.CmekAttributes.KmsConfigResourcePath == nil || *dbBv.CmekAttributes.KmsConfigResourcePath == "" {
		return "", customerrors.NewUserInputValidationErr("cmek backup rotation can not be called for backup vault without CMEK configuration")
	}

	// For cross-region backup vaults, only allow CMEK rotation from the destination
	if dbBv.BackupVaultType == activities.CrossRegionBackupType {
		if dbBv.SourceRegionName != nil && params.Region == *dbBv.SourceRegionName {
			return "", customerrors.NewUserInputValidationErr("cmek backup rotation can not be called for cross region source backup vault")
		}
	}

	job := &datamodel.Job{
		Type:          string(models.JobTypeRotateCmekBackups),
		State:         string(models.JobsStateNEW),
		ResourceName:  dbBv.Name,
		AccountID:     sql.NullInt64{Int64: account.ID, Valid: true},
		CorrelationID: utils.GetCoRelationIDFromContext(ctx),
		RequestID:     utils.GetRequestIDFromContext(ctx),
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: dbBv.UUID,
			Location:     params.Region,
			KmsAttributes: &datamodel.JobKmsAttributes{
				NewKmsKeyURL:      primaryKeyVersion,
				AccountIdentifier: account.Name,
			},
		},
	}

	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create CMEK rotation job in database", "error", err)
		return "", err
	}

	// On workflow start failure, mark job as ERROR.
	defer func() {
		if err != nil && createdJob != nil {
			if jobErr := se.UpdateJob(ctx, createdJob.UUID, string(models.JobsStateERROR), 0, err.Error()); jobErr != nil {
				logger.Error("Failed to update CMEK rotation job state to ERROR", "error", jobErr, "jobUUID", createdJob.UUID)
			}
		}
	}()

	workflowExecutor := workflows.NewWorkflowExecutor(o.temporal, logger)
	err = workflowExecutor.ExecuteWorkflow(
		ctx,
		createdJob.WorkflowID,
		workflowengine.CustomerTaskQueue,
		workflows.RotateCmekBackupsWorkflow,
		workflowengine.GetCreateBackupWorkflowTimeout(),
		params,
		dbBv,
		primaryKeyVersion,
	)
	if err != nil {
		logger.Error("Failed to start CMEK rotation workflow after retries", "error", err)
		return "", err
	}

	return createdJob.UUID, nil
}
