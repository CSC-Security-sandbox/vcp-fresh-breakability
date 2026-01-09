package workflows

import (
	"fmt"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// backupVaultCmekRotationWorkflow orchestrates CMEK rotation for all backups
// under a backup vault by invoking per-bucket activities and finally updating
// CMEK metadata in VCP and SDE.
type backupVaultCmekRotationWorkflow struct {
	BaseWorkflow
}

var (
	_ WorkflowInterface = &backupVaultCmekRotationWorkflow{}
)

// RotateCmekBackupsWorkflow is the Temporal entry point used by the orchestrator.
func RotateCmekBackupsWorkflow(ctx workflow.Context, params *common.BackupVaultParams, backupVault *datamodel.BackupVault, primaryKeyVersion string) error {
	wf := new(backupVaultCmekRotationWorkflow)
	if err := wf.Setup(ctx, params); err != nil {
		return ConvertToVSAError(err)
	}
	if err := wf.EnsureJobState(ctx, models.JobsStateNEW); err != nil {
		return ConvertToVSAError(err)
	}
	wf.Status = WorkflowStatusRunning
	if err := wf.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil); err != nil {
		return ConvertToVSAError(err)
	}
	_, customErr := wf.Run(ctx, backupVault, params, primaryKeyVersion)
	if customErr != nil {
		wf.Status = WorkflowStatusFailed
		if err := wf.UpdateJobStatus(ctx, string(models.JobsStateERROR), customErr); err != nil {
			wf.Logger.Errorf("Error when updating the job status for CMEK rotation workflow: %v", err)
		}
		return ConvertToVSAError(customErr)
	}

	wf.Status = WorkflowStatusCompleted
	if err := wf.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil); err != nil {
		wf.Logger.Errorf("Error when updating the job status for CMEK rotation workflow: %v", err)
		return ConvertToVSAError(err)
	}

	return nil
}

// Setup initialises workflow metadata and status query handler.
func (wf *backupVaultCmekRotationWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	backupVaultParams := input.(*common.BackupVaultParams)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = backupVaultParams.AccountName
	wf.Status = WorkflowStatusCreated
	ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{"workflowID": wf.ID, "customerID": wf.CustomerID})
	logger := util.GetLogger(ctx)
	wf.Logger = logger

	// Set the query handler in a non-blocking way
	err := workflow.SetQueryHandler(ctx, "status", func() (*WorkflowStatus, error) {
		return &WorkflowStatus{
			ID:         wf.ID,
			Status:     wf.Status,
			CustomerID: wf.CustomerID,
		}, nil
	})
	if err != nil {
		return err
	}
	return nil
}

// Run performs the actual CMEK rotation logic by iterating buckets and invoking
// per-bucket rotation activities followed by vault metadata updates.
func (wf *backupVaultCmekRotationWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	backupVault := args[0].(*datamodel.BackupVault)
	bvCommonParams := args[1].(*common.BackupVaultParams)
	primaryKeyVersion := args[2].(string)

	backupVaultActivity := &activities.BackupVaultActivity{}

	// Helper to best-effort hydrate FAILED encryption state back to the
	// source-region VCP backup vault for CRB vaults, keeping the old key
	// version (no BackupsPrimaryKeyVersion change).
	hydrateSourceFailed := func() {
		if backupVault.BackupVaultType != activities.CrossRegionBackupType || backupVault.SourceRegionName == nil {
			return
		}
		sourceParams := *bvCommonParams
		sourceParams.BackupRegion = backupVault.SourceRegionName
		if backupVault.ExternalUUID != nil && *backupVault.ExternalUUID != "" {
			sourceParams.BackupVaultID = *backupVault.ExternalUUID
		}

		failedVault := *backupVault
		if failedVault.CmekAttributes == nil {
			failedVault.CmekAttributes = &datamodel.CmekAttributes{}
		}
		stateFailed := models.EncryptionStateFailed
		failedVault.CmekAttributes.EncryptionState = &stateFailed

		_ = workflow.ExecuteActivity(ctx, backupVaultActivity.UpdateRemoteBackupVaultInVCP, &sourceParams, &failedVault).Get(ctx, nil)
	}

	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	timeout, err := time.ParseDuration(StartToCloseTimeoutCmekBackupRotate)
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrWorkflowConfigurationError, err)
	}

	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        retryPolicy.InitialInterval,
			BackoffCoefficient:     retryPolicy.BackoffCoefficient,
			MaximumInterval:        retryPolicy.MaximumInterval,
			MaximumAttempts:        int32(retryPolicy.MaximumAttempts),
			NonRetryableErrorTypes: []string{"PanicError"},
		},
	}
	rotateAo := workflow.ActivityOptions{
		StartToCloseTimeout: timeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        retryPolicy.InitialInterval,
			BackoffCoefficient:     retryPolicy.BackoffCoefficient,
			MaximumInterval:        retryPolicy.MaximumInterval,
			MaximumAttempts:        int32(retryPolicy.MaximumAttempts),
			NonRetryableErrorTypes: []string{"PanicError"},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	rotateCtx := workflow.WithActivityOptions(ctx, rotateAo)

	// Ensure vault state is restored in case of unexpected errors.
	defer func() {
		if err != nil {
			_ = workflow.ExecuteActivity(ctx, backupVaultActivity.UpdateBackupVaultStateInCaseOfError, backupVault, models.LifeCycleStateREADY, models.LifeCycleStateAvailableDetails).Get(ctx, nil)
		}
	}()

	// Mark encryption state as IN_PROGRESS in VCP before starting bucket rotations.
	inProgressState := models.EncryptionStateInProgress
	err = workflow.ExecuteActivity(ctx, backupVaultActivity.UpdateBackupVaultEncryptionStateInVCPActivity, backupVault, inProgressState).Get(ctx, nil)
	if err != nil {
		wf.Logger.Error("Failed to update backup vault encryption state to IN_PROGRESS in VCP", log.Fields{
			"backupVaultUUID": backupVault.UUID,
			"error":           err,
		})
		return nil, ConvertToVSAError(fmt.Errorf("UpdateBackupVaultEncryptionStateInVCPActivity failed for IN_PROGRESS: %w", err))
	}

	if backupVault.BackupVaultType == activities.CrossRegionBackupType && backupVault.SourceRegionName != nil {
		sourceParams := *bvCommonParams
		sourceParams.BackupRegion = backupVault.SourceRegionName
		if backupVault.ExternalUUID != nil && *backupVault.ExternalUUID != "" {
			sourceParams.BackupVaultID = *backupVault.ExternalUUID
		}

		inProgressVault := *backupVault
		if inProgressVault.CmekAttributes == nil {
			inProgressVault.CmekAttributes = &datamodel.CmekAttributes{}
		}
		inProgState := models.EncryptionStateInProgress
		inProgressVault.CmekAttributes.EncryptionState = &inProgState
		inProgressVault.CmekAttributes.BackupsPrimaryKeyVersion = nil

		_ = workflow.ExecuteActivity(ctx, backupVaultActivity.UpdateRemoteBackupVaultInVCP, &sourceParams, &inProgressVault).Get(ctx, nil)
	}

	// Step 1: Rotate VCP buckets first. These are the buckets tracked in VCP;
	// SDE-managed buckets are rotated separately by SDE/CBS.
	var bucketNames []string
	for _, bd := range backupVault.BucketDetails {
		if bd == nil || bd.BucketName == "" {
			continue
		}
		bucketNames = append(bucketNames, bd.BucketName)
	}

	// Sequential, fail-fast bucket rotation in VCP. If any bucket rotation
	// fails, mark VCP encryption state as FAILED and abort without invoking SDE.
	for _, bucketName := range bucketNames {
		err = workflow.ExecuteActivity(rotateCtx, backupVaultActivity.RotateBucketCmekActivity, bucketName, primaryKeyVersion).Get(rotateCtx, nil)
		if err != nil {
			wf.Logger.Error("Failed to rotate CMEK for bucket", log.Fields{
				"bucketName":        bucketName,
				"primaryKeyVersion": primaryKeyVersion,
				"backupVaultUUID":   backupVault.UUID,
				"backupVaultName":   backupVault.Name,
				"error":             err,
			})
			failedState := models.EncryptionStateFailed
			_ = workflow.ExecuteActivity(ctx, backupVaultActivity.UpdateBackupVaultEncryptionStateInVCPActivity, backupVault, failedState).Get(ctx, nil)
			hydrateSourceFailed()
			return nil, ConvertToVSAError(fmt.Errorf("RotateBucketCmekActivity failed for bucket %s: %w", bucketName, err))
		}
	}

	// Step 2: Only after successful VCP bucket rotation, start SDE rotation.
	var jwtToken string
	err = workflow.ExecuteActivity(ctx, activities.CommonActivities.GetAuthJWTToken, bvCommonParams.AccountName).Get(ctx, &jwtToken)
	if err != nil {
		wf.Logger.Error("Failed to get auth JWT token for SDE CMEK rotation", log.Fields{
			"backupVaultUUID": backupVault.UUID,
			"error":           err,
		})
		failedState := models.EncryptionStateFailed
		_ = workflow.ExecuteActivity(ctx, backupVaultActivity.UpdateBackupVaultEncryptionStateInVCPActivity, backupVault, failedState).Get(ctx, nil)
		// For CRB, best-effort mark source-region vault as FAILED as well.
		hydrateSourceFailed()
		return nil, ConvertToVSAError(fmt.Errorf("GetAuthJWTToken failed: %w", err))
	}
	ctx = workflow.WithValue(ctx, middleware.AuthorizationToken, jwtToken)

	// Start SDE-side CMEK rotation for SDE-managed buckets. This creates an async
	// job in SDE/CBS and returns immediately once the job is accepted.
	err = workflow.ExecuteActivity(ctx, backupVaultActivity.StartSDECmekRotationForBackupVault, bvCommonParams, primaryKeyVersion).Get(ctx, nil)
	if err != nil {
		wf.Logger.Error("Failed to start SDE CMEK rotation for backup vault", log.Fields{
			"backupVaultUUID": backupVault.UUID,
			"error":           err,
		})

		// Mark encryption state as FAILED in VCP since we could not even start SDE rotation.
		failedState := models.EncryptionStateFailed
		_ = workflow.ExecuteActivity(ctx, backupVaultActivity.UpdateBackupVaultEncryptionStateInVCPActivity, backupVault, failedState).Get(ctx, nil)

		// For CRB, best-effort mark source-region vault as FAILED as well.
		hydrateSourceFailed()

		return nil, ConvertToVSAError(fmt.Errorf("StartSDECmekRotationForBackupVault failed: %w", err))
	}

	// Wait for SDE-side CMEK rotation to complete and determine its outcome.
	var sdeSucceeded bool
	err = workflow.ExecuteActivity(rotateCtx, backupVaultActivity.WaitForSDECmekRotationCompletion, bvCommonParams).Get(rotateCtx, &sdeSucceeded)
	if err != nil {
		wf.Logger.Error("Failed while waiting for SDE CMEK rotation completion", log.Fields{
			"backupVaultUUID": backupVault.UUID,
			"error":           err,
		})
		// Treat this as SDE failure.
		sdeSucceeded = false
	}

	if !sdeSucceeded {
		// Mark VCP encryption state as FAILED when SDE rotation does not complete successfully.
		failedState := models.EncryptionStateFailed
		_ = workflow.ExecuteActivity(ctx, backupVaultActivity.UpdateBackupVaultEncryptionStateInVCPActivity, backupVault, failedState).Get(ctx, nil)
		// For CRB, best-effort mark source-region vault as FAILED as well.
		hydrateSourceFailed()
		return nil, ConvertToVSAError(fmt.Errorf("SDE CMEK rotation failed for backup vault %s", backupVault.UUID))
	}

	// Step 3: Both VCP and SDE rotations have succeeded; update VCP CMEK metadata
	// with the new key version and mark encryption state COMPLETED.
	err = workflow.ExecuteActivity(ctx, backupVaultActivity.UpdateBackupVaultCmekInVCPActivity, backupVault, primaryKeyVersion).Get(ctx, nil)
	if err != nil {
		wf.Logger.Error("Failed to update backup vault CMEK metadata in VCP", log.Fields{
			"backupVaultUUID":   backupVault.UUID,
			"primaryKeyVersion": primaryKeyVersion,
			"error":             err,
		})
		failedState := models.EncryptionStateFailed
		_ = workflow.ExecuteActivity(ctx, backupVaultActivity.UpdateBackupVaultEncryptionStateInVCPActivity, backupVault, failedState).Get(ctx, nil)
		// For CRB, best-effort mark source-region vault as FAILED as well.
		hydrateSourceFailed()
		return nil, ConvertToVSAError(fmt.Errorf("UpdateBackupVaultCmekInVCPActivity failed: %w", err))
	}

	// For cross-region backup vaults, mirror SDE's hydration by updating the
	// source-region VCP backup vault once rotation has completed successfully in
	// the destination region and VCP metadata has been updated.
	if backupVault.BackupVaultType == activities.CrossRegionBackupType && backupVault.SourceRegionName != nil {
		sourceParams := *bvCommonParams
		sourceParams.BackupRegion = backupVault.SourceRegionName
		// As with the FAILED hydration helper, use ExternalUUID (source vault
		// UUID) when calling the source-region VCP instance so that we update
		// the correct backup vault record.
		if backupVault.ExternalUUID != nil && *backupVault.ExternalUUID != "" {
			sourceParams.BackupVaultID = *backupVault.ExternalUUID
		}

		// Build a hydrated view of the destination vault with COMPLETED state and
		// the new backups primary key version, and propagate it to the source
		// region.
		completedVault := *backupVault
		if completedVault.CmekAttributes == nil {
			completedVault.CmekAttributes = &datamodel.CmekAttributes{}
		}
		stateCompleted := models.EncryptionStateCompleted
		completedVault.CmekAttributes.EncryptionState = &stateCompleted
		completedVault.CmekAttributes.BackupsPrimaryKeyVersion = &primaryKeyVersion

		_ = workflow.ExecuteActivity(ctx, backupVaultActivity.UpdateRemoteBackupVaultInVCP, &sourceParams, &completedVault).Get(ctx, nil)
	}
	return nil, nil
}
