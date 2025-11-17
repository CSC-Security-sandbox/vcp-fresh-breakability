package workflows

import (
	"fmt"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type BackupCreateWorkflow struct {
	BaseWorkflow
	SE *database.Storage
}

type BackupDeleteWorkflow struct {
	BaseWorkflow
	SE              *database.Storage
	deleteInitiated bool
}

type backupUpdateWorkflow struct {
	BaseWorkflow
	SE database.Storage
}

var (
	_    WorkflowInterface = &backupUpdateWorkflow{}
	_    WorkflowInterface = &BackupCreateWorkflow{}
	_    WorkflowInterface = &BackupDeleteWorkflow{}
	Wait                   = time.Duration(env.GetUint("ONTAP_REST_ASYNC_POLL_WAIT_SECONDS", 3)) * time.Second
)

const (
	BackupComment        = "VCP-Backup"
	BackupMaxWaitTimeCap = 15 * time.Minute   // Maximum wait time cap
	adcWorkflowTimeout   = 7 * 24 * time.Hour // 7 days timeout to accommodate 6-day max sleep + buffer
)

// CreateBackupWorkflow  process backup related requests from a customer.
func CreateBackupWorkflow(ctx workflow.Context, params *commonparams.CreateBackupParams, backup *datamodel.Backup, backupVault *datamodel.BackupVault, volume *datamodel.Volume) (interface{}, error) {
	backupWf := new(BackupCreateWorkflow)
	err := backupWf.Setup(ctx, params)
	if err != nil {
		return nil, err
	}
	backupWf.Status = WorkflowStatusRunning
	err = backupWf.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		return nil, err
	}
	_, customErr := backupWf.Run(ctx, backup, backupVault, volume, params)

	if customErr != nil {
		if workflow.IsContinueAsNewError(customErr.OriginalErr) {
			return nil, customErr
		}
		err2 := backupWf.Revert(ctx, backup, volume, customErr.OriginalErr.Error())
		if err2 != nil {
			backupWf.Logger.Errorf("Failed to execute rollback for workflow %s: %v", backupWf.ID, err2)
		}
		backupWf.Status = WorkflowStatusFailed
		err2 = backupWf.UpdateJobStatus(ctx, string(models.JobsStateERROR), customErr)
		if err2 != nil {
			backupWf.Logger.Errorf("Failed to update job status for workflow %s: %v", backupWf.ID, err2)
		}
		return nil, customErr
	}
	backupWf.Status = WorkflowStatusCompleted
	err2 := backupWf.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	if err2 != nil {
		backupWf.Logger.Errorf("Failed to update job status for workflow %s: %v", backupWf.ID, err2)
		return nil, ConvertToVSAError(err2)
	}
	return nil, nil
}

// CreateBackupWorkflowWithContext process backup with context for continuation
func CreateBackupWorkflowWithContext(ctx workflow.Context, backupActivitiesContext *activities.BackupActivitiesContext, params *commonparams.CreateBackupParams) (interface{}, error) {
	backupWf := new(BackupCreateWorkflow)

	// Setup workflow
	if err := backupWf.Setup(ctx, params); err != nil {
		return nil, err
	}

	backupWf.Status = WorkflowStatusRunning
	err := backupWf.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		return nil, err
	}

	_, customErr := backupWf.RunBackupCreateWithContext(ctx, backupActivitiesContext, params)

	if customErr != nil {
		// Check if the error is a ContinueAsNewError - if so, don't call revert
		if workflow.IsContinueAsNewError(customErr.OriginalErr) {
			return nil, customErr
		}

		err2 := backupWf.Revert(ctx, backupActivitiesContext.BackupWorkflowInit.Backup, backupActivitiesContext.BackupWorkflowInit.Volume, customErr.OriginalErr.Error())
		if err2 != nil {
			backupWf.Logger.Errorf("Failed to execute rollback for workflow %s: %v", backupWf.ID, err2)
		}
		backupWf.Status = WorkflowStatusFailed
		err2 = backupWf.UpdateJobStatus(ctx, string(models.JobsStateERROR), customErr)
		if err2 != nil {
			backupWf.Logger.Errorf("Failed to update job status for workflow %s: %v", backupWf.ID, err2)
		}
		return nil, customErr
	}
	backupWf.Status = WorkflowStatusCompleted
	err2 := backupWf.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	if err2 != nil {
		backupWf.Logger.Errorf("Failed to update job status for workflow %s: %v", backupWf.ID, err2)
		return nil, ConvertToVSAError(err2)
	}
	return nil, nil
}

func (wf *BackupCreateWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	createBackupParams := input.(*commonparams.CreateBackupParams)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = createBackupParams.AccountName
	wf.Status = WorkflowStatusCreated
	ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{"workflowID": wf.ID, "customerID": wf.CustomerID})
	logger := util.GetLogger(ctx)
	wf.Logger = logger

	return workflow.SetQueryHandler(ctx, "status", func() (*WorkflowStatus, error) {
		return &WorkflowStatus{
			ID:         wf.ID,
			Status:     wf.Status,
			CustomerID: wf.CustomerID,
		}, nil
	})
}

func (wf *BackupCreateWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	// Initialize backupActivitiesContext with input arguments
	backupActivitiesContext := &activities.BackupActivitiesContext{
		BackupWorkflowInit: &activities.BackupWorkflowInput{
			Backup:      args[0].(*datamodel.Backup),
			BackupVault: args[1].(*datamodel.BackupVault),
			Volume:      args[2].(*datamodel.Volume),
		},
	}

	params := args[3].(*commonparams.CreateBackupParams)
	return wf.RunBackupCreateWithContext(ctx, backupActivitiesContext, params)
}

func (wf *BackupCreateWorkflow) RunBackupCreateWithContext(ctx workflow.Context, backupActivitiesContext *activities.BackupActivitiesContext, params *commonparams.CreateBackupParams) (interface{}, *vsaerrors.CustomError) {
	backupActivity := &activities.BackupActivity{}
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, ConvertToVSAError(err)
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
	ctx = workflow.WithActivityOptions(ctx, ao)
	// Check if this is a continuation workflow
	info := workflow.GetInfo(ctx)
	isContinuation := info.ContinuedExecutionRunID != ""

	if isContinuation {
		wf.Logger.Info("Resuming backup workflow from continuation",
			"workflowID", wf.ID,
			"continuedFromRunID", info.OriginalRunID,
			"snapshotName", backupActivitiesContext.SnapshotName,
			"transferStatus", backupActivitiesContext.TransferStatus)
	} else {
		var dbNodes []*datamodel.Node
		err = workflow.ExecuteActivity(ctx, activities.CommonActivities.GetNode, backupActivitiesContext.BackupWorkflowInit.Volume.PoolID).Get(ctx, &dbNodes)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}

		node := hyperscaler.CreateNodeForProvider(hyperscaler.NodeProviderInput{Nodes: dbNodes, Password: backupActivitiesContext.BackupWorkflowInit.Volume.Pool.PoolCredentials.Password, SecretID: backupActivitiesContext.BackupWorkflowInit.Volume.Pool.PoolCredentials.SecretID, DeploymentName: backupActivitiesContext.BackupWorkflowInit.Volume.Pool.DeploymentName, CertificateID: backupActivitiesContext.BackupWorkflowInit.Volume.Pool.PoolCredentials.CertificateID, AuthType: backupActivitiesContext.BackupWorkflowInit.Volume.Pool.PoolCredentials.AuthType})
		backupActivitiesContext.Node = node
		backupActivitiesContext.BackupWorkflowInit.Backup.Attributes.SourceVolumeZone = backupActivitiesContext.BackupWorkflowInit.Volume.Pool.PoolAttributes.PrimaryZone
		backupActivitiesContext.BackupWorkflowInit.Backup.Attributes.IsRegionalHA = backupActivitiesContext.BackupWorkflowInit.Volume.Pool.PoolAttributes.IsRegionalHA
		// Prepare object store details
		err = workflow.ExecuteActivity(ctx, backupActivity.PrepareObjectStoreActivity, backupActivitiesContext).Get(ctx, &backupActivitiesContext)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}

		// Get or create object store
		err = workflow.ExecuteActivity(ctx, backupActivity.GetOrCreateObjectStoreActivity, backupActivitiesContext).Get(ctx, &backupActivitiesContext)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}

		// Prepare snapmirror paths
		err = workflow.ExecuteActivity(ctx, backupActivity.PrepareSnapmirrorActivity, backupActivitiesContext).Get(ctx, &backupActivitiesContext)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}

		// Create snapmirror relationship
		err = workflow.ExecuteActivity(ctx, backupActivity.CreateSnapmirrorRelationshipActivity, backupActivitiesContext).Get(ctx, &backupActivitiesContext)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}

		// Creating snapshot in DB
		err = workflow.ExecuteActivity(ctx, backupActivity.CreatingSnapshotActivity, backupActivitiesContext).Get(ctx, &backupActivitiesContext)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}

		defer func() {
			// Update snapshot details in DB
			err = workflow.ExecuteActivity(ctx, backupActivity.UpdateSnapshotActivity, backupActivitiesContext).Get(ctx, &backupActivitiesContext)
			if err != nil {
				util.GetLogger(ctx).Errorf("Failed to Update Snapshot State: %v", err)
			}
		}()

		// Create snapshot
		err = workflow.ExecuteActivity(ctx, backupActivity.CreateSnapshotActivity, backupActivitiesContext).Get(ctx, &backupActivitiesContext)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}

		rollbackManager := commonparams.NewRollbackManager()
		// Only add rollback activity if the required fields are not nil
		if backupActivitiesContext.BackupWorkflowInit.Backup.Attributes != nil &&
			backupActivitiesContext.BackupWorkflowInit.Volume.VolumeAttributes != nil {
			rollbackManager.AddActivity(backupActivity.DeleteBackupSnapshot, node, backupActivitiesContext.BackupWorkflowInit.Backup.Attributes.SnapshotID, backupActivitiesContext.BackupWorkflowInit.Volume.VolumeAttributes.ExternalUUID)
		}

		// Transfer snapshot
		err = workflow.ExecuteActivity(ctx, backupActivity.TransferSnapshotActivity, backupActivitiesContext).Get(ctx, &backupActivitiesContext)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
	}

	// Poll for transfer completion with event history monitoring and ContinueAsNew capability
	err = wf.PollTransferStatusWithContinueAsNew(ctx, backupActivitiesContext, params)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	// Update ConstituentCount for a backup from Volume
	if backupActivitiesContext.BackupWorkflowInit.Volume.LargeVolumeAttributes != nil && backupActivitiesContext.BackupWorkflowInit.Volume.LargeVolumeAttributes.LargeCapacity {
		err = workflow.ExecuteActivity(ctx, backupActivity.UpdateConstituentCountForBackup, backupActivitiesContext).Get(ctx, &backupActivitiesContext)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
	}

	backupActivitiesContext.BackupWorkflowInit.Backup.AssetMetadata = &datamodel.AssetMetadata{
		ChildAssets: []datamodel.ChildAsset{
			{
				AssetType:  commonparams.BackupAssetType,
				AssetNames: []string{fmt.Sprintf("//storage.googleapis.com/%s", backupActivitiesContext.BackupWorkflowInit.Backup.Attributes.BucketName)},
			},
		},
	}
	// Finish backup now as the transfer has completed
	err = workflow.ExecuteActivity(ctx, backupActivity.FinishBackupActivity, backupActivitiesContext).Get(ctx, &backupActivitiesContext)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	// No need to check for error as if there is an error it will be caught in the above step itself
	// After this step the backup is considered successful
	// Get object store endpoint info
	err = workflow.ExecuteActivity(ctx, backupActivity.GetObjectStoreEndpointActivity, backupActivitiesContext).Get(ctx, &backupActivitiesContext)
	if err != nil {
		// Log the error but don't fail the entire backup workflow
		wf.Logger.Errorf("Failed to get object store endpoint info for volume %s: %v", backupActivitiesContext.BackupWorkflowInit.Volume.Name, err)
	}
	// Get snapshot from object store
	err = workflow.ExecuteActivity(ctx, backupActivity.GetObjectStoreSnapshotActivity, backupActivitiesContext).Get(ctx, &backupActivitiesContext)
	if err != nil {
		// Log the error but don't fail the entire backup workflow
		wf.Logger.Errorf("Failed to get snapshot from object store for volume %s: %v", backupActivitiesContext.BackupWorkflowInit.Volume.Name, err)
	}
	// Update backup size fields in both backup and volume tables
	err = workflow.ExecuteActivity(ctx, backupActivity.UpdateBackupSizeActivity, backupActivitiesContext).Get(ctx, &backupActivitiesContext)
	if err != nil {
		// Log the error but don't fail the entire backup workflow
		wf.Logger.Errorf("Failed to update backup size fields for volume %s: %v", backupActivitiesContext.BackupWorkflowInit.Volume.Name, err)
	}

	// Create remote backup from VCP if this is a cross-region backup
	// This is done after UpdateBackupSizeActivity to ensure ExternalUUID is set
	err = workflow.ExecuteActivity(ctx, backupActivity.CreateRemoteBackupFromVCPActivity, backupActivitiesContext).Get(ctx, &backupActivitiesContext)
	if err != nil {
		// Log the error but don't fail the entire backup workflow
		wf.Logger.Errorf("Failed to create remote backup from VCP for backup %s: %v", backupActivitiesContext.BackupWorkflowInit.Backup.UUID, err)
	}

	// Cleanup older adhoc-backup snapshots for this volume
	err = workflow.ExecuteActivity(ctx, backupActivity.CleanupOldAdhocBackupSnapshotsActivity, backupActivitiesContext.BackupWorkflowInit.Volume, backupActivitiesContext.Node).Get(ctx, nil)
	if err != nil {
		// Log the error but don't fail the entire backup workflow
		wf.Logger.Errorf("Failed to cleanup older adhoc-backup snapshots for volume %s: %v", backupActivitiesContext.BackupWorkflowInit.Volume.Name, err)
	}

	// Hydrate snapshot to CCFE
	if backupActivitiesContext.DbSnapshot != nil &&
		backupActivitiesContext.BackupWorkflowInit.Volume != nil &&
		backupActivitiesContext.BackupWorkflowInit.BackupVault != nil &&
		backupActivitiesContext.BackupWorkflowInit.Volume.Account != nil {
		location := utils.GetLocation(*backupActivitiesContext.DbSnapshot)
		err = workflow.ExecuteActivity(ctx, backupActivity.HydrateSnapshotToCCFEActivity,
			backupActivitiesContext.DbSnapshot,
			backupActivitiesContext.BackupWorkflowInit.Volume.Name,
			location,
			backupActivitiesContext.BackupWorkflowInit.Volume.Account.Name).Get(ctx, nil)
		if err != nil {
			// Log the error but don't fail the entire backup workflow
			wf.Logger.Errorf("Failed to hydrate snapshot to CCFE for volume %s: %v", backupActivitiesContext.BackupWorkflowInit.Volume.Name, err)
		}
	}

	// Create BackupMetadata entry if this is the first backup for the volume
	err = workflow.ExecuteActivity(ctx, backupActivity.CreateBackupMetadataIfFirstBackupActivity, backupActivitiesContext.BackupWorkflowInit.Volume).Get(ctx, nil)
	if err != nil {
		// Log the error but don't fail the entire backup workflow
		wf.Logger.Errorf("Failed to create BackupMetadata for volume %s: %v", backupActivitiesContext.BackupWorkflowInit.Volume.UUID, err)
	}

	return backupActivitiesContext, nil
}

func (wf *BackupCreateWorkflow) Revert(ctx workflow.Context, backup *datamodel.Backup, volume *datamodel.Volume, errString string) error {
	// Implement the revert logic for backup workflows
	// This might involve rolling back any changes made during the workflow execution
	backupActivity := &activities.BackupActivity{}
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return err
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
	ctx = workflow.WithActivityOptions(ctx, ao)
	// updating the backup backupActivitiesContext to error
	err = workflow.ExecuteActivity(ctx, backupActivity.UpdateBackupError, &backup, errString).Get(ctx, nil)
	if err != nil {
		return err
	}
	return nil
}

// PollTransferStatusWithContinueAsNew polls transfer status with automatic ContinueAsNew when history limits are reached
func (wf *BackupCreateWorkflow) PollTransferStatusWithContinueAsNew(ctx workflow.Context, backupActivitiesContext *activities.BackupActivitiesContext, params *commonparams.CreateBackupParams) error {
	return PollTransferStatusWithContinueAsNewCommon(ctx, backupActivitiesContext, CreateBackupWorkflowWithContext, backupActivitiesContext, params)
}

func getBucketDetailsForBucket(backupVault *datamodel.BackupVault, bucketName string) (*datamodel.BucketDetails, error) {
	for _, bucketDetail := range backupVault.BucketDetails {
		if bucketDetail.BucketName == bucketName {
			return bucketDetail, nil
		}
	}
	return nil, ConvertToVSAError(fmt.Errorf("no matching bucket details found for bucket %s in backup vault %s", bucketName, backupVault.Name))
}

func DeleteBackupWorkflow(ctx workflow.Context, params *commonparams.DeleteBackupParams) (interface{}, error) {
	backupWf := new(BackupDeleteWorkflow)
	err := backupWf.Setup(ctx, params)
	if err != nil {
		return nil, err
	}
	backupWf.Status = WorkflowStatusRunning
	err = backupWf.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		return nil, err
	}
	_, customErr := backupWf.Run(ctx, params)

	if customErr != nil {
		// backup backupActivitiesContext to error
		err2 := backupWf.HandleError(ctx, params, customErr.OriginalErr.Error())
		if err2 != nil {
			// If revert fails, log the error but do not return it
			backupWf.Logger.Errorf("Failed to revert backup delete workflow: %v", err2)
		}
		backupWf.Status = WorkflowStatusFailed
		err2 = backupWf.UpdateJobStatus(ctx, string(models.JobsStateERROR), customErr)
		if err2 != nil {
			backupWf.Logger.Errorf("Failed to update job status for workflow %s: %v", backupWf.ID, err2)
		}
		return nil, customErr
	}
	backupWf.Status = WorkflowStatusCompleted
	err2 := backupWf.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	if err2 != nil {
		backupWf.Logger.Errorf("Failed to update job status for workflow %s: %v", backupWf.ID, err2)
		return nil, ConvertToVSAError(err2)
	}
	return nil, nil
}

func (wf *BackupDeleteWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	deleteBackupParams := input.(*commonparams.DeleteBackupParams)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = deleteBackupParams.AccountName
	wf.Status = WorkflowStatusCreated
	ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{"workflowID": wf.ID, "customerID": wf.CustomerID})
	logger := util.GetLogger(ctx)
	wf.Logger = logger

	return workflow.SetQueryHandler(ctx, "status", func() (*WorkflowStatus, error) {
		return &WorkflowStatus{
			ID:         wf.ID,
			Status:     wf.Status,
			CustomerID: wf.CustomerID,
		}, nil
	})
}

func (wf *BackupDeleteWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	deleteBackupParams := args[0].(*commonparams.DeleteBackupParams)
	backupActivity := &activities.BackupActivity{}
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, ConvertToVSAError(err)
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
	ctx = workflow.WithActivityOptions(ctx, ao)

	// fetch account by name
	account := &datamodel.Account{}
	err = workflow.ExecuteActivity(ctx, backupActivity.GetAccountByName, deleteBackupParams.AccountName).Get(ctx, account)
	if err != nil {
		return nil, ConvertToVSAError(fmt.Errorf("failed to get account by name %s: %w", deleteBackupParams.AccountName, err))
	}

	// check if backup Vault is present in VSA
	dbBackupVault := &datamodel.BackupVault{}
	err = workflow.ExecuteActivity(ctx, backupActivity.GetBackupVault, deleteBackupParams.BackupVaultUUID).Get(ctx, &dbBackupVault)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	dbBackupVault.Account = account

	dbBackup := &datamodel.Backup{}
	err = workflow.ExecuteActivity(ctx, backupActivity.GetBackup, deleteBackupParams.BackupVaultUUID, deleteBackupParams.BackupUUID, deleteBackupParams.AccountName).Get(ctx, &dbBackup)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	var isVolumeDeleted bool
	err = workflow.ExecuteActivity(ctx, backupActivity.IsVolumeDeleted, dbBackup.VolumeUUID).Get(ctx, &isVolumeDeleted)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	bucketDetails, err := getBucketDetailsForBucket(dbBackupVault, dbBackup.Attributes.BucketName)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	var volume *datamodel.Volume
	var node *models.Node
	var smSourcePath string
	var smDestinationPath string
	var isSnapmirrorDeleted bool
	if !isVolumeDeleted {
		err = workflow.ExecuteActivity(ctx, backupActivity.GetVolume, dbBackup.VolumeUUID).Get(ctx, &volume)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}

		var dbNodes []*datamodel.Node
		err = workflow.ExecuteActivity(ctx, activities.CommonActivities.GetNode, &volume.PoolID).Get(ctx, &dbNodes)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
		node = hyperscaler.CreateNodeForProvider(hyperscaler.NodeProviderInput{Nodes: dbNodes, Password: volume.Pool.PoolCredentials.Password, SecretID: volume.Pool.PoolCredentials.SecretID, DeploymentName: volume.Pool.DeploymentName, CertificateID: volume.Pool.PoolCredentials.CertificateID, AuthType: volume.Pool.PoolCredentials.AuthType})

		err = workflow.ExecuteActivity(ctx, backupActivity.GetSmSourcePathActivity, volume).Get(ctx, &smSourcePath)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}

		err = workflow.ExecuteActivity(ctx, backupActivity.GetSmDestinationPathActivity, dbBackupVault, volume).Get(ctx, &smDestinationPath)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}

		params := &commonparams.SnapmirrorRelationshipParams{
			SourcePath:      smSourcePath,
			DestinationPath: smDestinationPath,
		}

		err = workflow.ExecuteActivity(ctx, backupActivity.IsSnapmirrorDeleted, node, params).Get(ctx, &isSnapmirrorDeleted)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
	}

	if isVolumeDeleted || isSnapmirrorDeleted {
		cloudDeletionIntiated := false
		// Create a new context for ADCWorkflow with extended timeout to accommodate 6-day maximum sleep duration
		adcWorkflowCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			WorkflowExecutionTimeout: adcWorkflowTimeout, // 7 days timeout
		})

		// if volume is deleted then we need to delete the backup with adc
		err = workflow.ExecuteChildWorkflow(adcWorkflowCtx, ADCWorkflow, deleteBackupParams, dbBackupVault, dbBackup, account).Get(adcWorkflowCtx, &cloudDeletionIntiated)
		if err != nil {
			wf.Logger.Errorf("Backup deletion failed with ADC, backupUUID: %s, error: %v", dbBackup.UUID, err)
			wf.deleteInitiated = true
			return nil, ConvertToVSAError(err)
		}
	} else {
		var backupCount int64
		err = workflow.ExecuteActivity(ctx, backupActivity.GetBackupCountByVolumeUUID, dbBackup.VolumeUUID).Get(ctx, &backupCount)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
		var objStore *commonparams.CloudTarget
		err = workflow.ExecuteActivity(ctx, backupActivity.GetObjectStore, node, bucketDetails.BucketName).Get(ctx, &objStore)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}

		var ontapAsyncResponse *vsa.OntapAsyncResponse
		if backupCount == 1 {
			var snapmirrorRelationship *commonparams.SnapmirrorRelationship
			err = workflow.ExecuteActivity(ctx, backupActivity.GetSnapmirror, node, smSourcePath, smDestinationPath).Get(ctx, &snapmirrorRelationship)
			if err != nil {
				return nil, ConvertToVSAError(err)
			}

			err = workflow.ExecuteActivity(ctx, backupActivity.DeleteSnapmirror, node, snapmirrorRelationship.UUID).Get(ctx, &ontapAsyncResponse)
			if err != nil {
				return nil, ConvertToVSAError(err)
			}
			err = WaitForONTAPJob(ctx, ontapAsyncResponse, node, time.Minute*10)
			if err != nil {
				return nil, ConvertToVSAError(fmt.Errorf("failed to delete snapmirror: %w", err))
			}

			wf.deleteInitiated = true
			err = workflow.ExecuteActivity(ctx, backupActivity.DeleteCloudEndpoint, node, objStore.UUID, dbBackup.Attributes.EndpointUUID).Get(ctx, &ontapAsyncResponse)
			if err != nil {
				return nil, ConvertToVSAError(err)
			}

			err = workflow.ExecuteActivity(ctx, backupActivity.DeleteSnapshotForBackup, node, dbBackup.Attributes.SnapshotID, volume.VolumeAttributes.ExternalUUID, dbBackup.Attributes.UseExistingSnapshot).Get(ctx, nil)
			if err != nil {
				return nil, ConvertToVSAError(err)
			}
		} else {
			var isBackupShared bool
			err = workflow.ExecuteActivity(ctx, backupActivity.IsBackupShared, dbBackup).Get(ctx, &isBackupShared)
			if err != nil {
				return nil, ConvertToVSAError(err)
			}
			if !isBackupShared {
				wf.deleteInitiated = true
				// Only proceed if backup attributes are not nil
				if dbBackup.Attributes != nil {
					err = workflow.ExecuteActivity(ctx, backupActivity.DeleteSnapshotFromObjectStore, node, objStore.UUID, dbBackup.Attributes.EndpointUUID, dbBackup.Attributes.SnapshotID).Get(ctx, &ontapAsyncResponse)
					if err != nil {
						return nil, ConvertToVSAError(err)
					}
					err = WaitForONTAPJob(ctx, ontapAsyncResponse, node, time.Minute*120)
					if err != nil {
						return nil, ConvertToVSAError(fmt.Errorf("failed to delete cloud endpoint: %w", err))
					}
				}
			}
		}
	}

	err = workflow.ExecuteActivity(ctx, backupActivity.DeleteBackup, deleteBackupParams.BackupUUID).Get(ctx, &dbBackup)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	// Delete remote backup from VCP if this is a cross-region backup
	if dbBackupVault.BackupVaultType == activities.CrossRegionBackupType && dbBackupVault.BackupRegionName != nil {
		remoteBackupErr := workflow.ExecuteActivity(ctx, backupActivity.DeleteRemoteBackupFromVCPActivity,
			dbBackup.UUID,
			dbBackupVault.UUID,
			deleteBackupParams.AccountName,
			*dbBackupVault.BackupRegionName).Get(ctx, nil)
		if remoteBackupErr != nil {
			wf.Logger.Errorf("Failed to delete remote backup from VCP for backup %s: %v", dbBackup.UUID, remoteBackupErr)
			return nil, ConvertToVSAError(fmt.Errorf("failed to delete remote backup from VCP for backup %s: %w", dbBackup.UUID, remoteBackupErr))
		}
	}

	// Hydrate snapshot deletion to CCFE
	// Only proceed if all required fields are not nil
	if dbBackup.Attributes != nil && volume != nil && account != nil {
		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{
				UUID:      dbBackup.Attributes.SnapshotID,
				CreatedAt: dbBackup.CreatedAt,
			},
			Name:         dbBackup.Name,
			State:        models.LifeCycleStateDeleted,
			StateDetails: models.LifeCycleStateDeletedDetails,
			Description:  dbBackup.Description,
			Volume:       volume,
			Account:      account,
			SnapshotAttributes: &datamodel.SnapshotAttributes{
				SizeInBytes: dbBackup.SizeInBytes,
			},
		}

		location := utils.GetLocation(*snapshot)
		hydrateSnapshoterr := workflow.ExecuteActivity(ctx, backupActivity.HydrateSnapshotDeletionToCCFEActivity,
			snapshot,
			volume.Name,
			location,
			account.Name).Get(ctx, nil)
		if hydrateSnapshoterr != nil {
			// Log the error but don't fail the entire backup deletion workflow
			wf.Logger.Errorf("Failed to hydrate snapshot deletion to CCFE for backup %s: %v", dbBackup.Name, hydrateSnapshoterr)
		}
	}

	// Try to delete snapshot from DB, but don't fail the workflow if this fails
	snapshotErr := workflow.ExecuteActivity(ctx, backupActivity.DeleteBackupSnapshotFromDB, dbBackup).Get(ctx, nil)
	if snapshotErr != nil {
		workflow.GetLogger(ctx).Error("Failed to delete snapshot from database", "error", snapshotErr)
	}

	// Delete BackupMetadata entry if this was the last backup for the volume
	metadataErr := workflow.ExecuteActivity(ctx, backupActivity.DeleteBackupMetadataIfLastBackupActivity, dbBackup.VolumeUUID).Get(ctx, nil)
	if metadataErr != nil {
		// Log the error but don't fail the entire backup deletion workflow
		wf.Logger.Errorf("Failed to delete BackupMetadata for volume %s: %v", dbBackup.VolumeUUID, metadataErr)
	}
	return nil, ConvertToVSAError(err)
}

func (wf *BackupDeleteWorkflow) HandleError(ctx workflow.Context, params *commonparams.DeleteBackupParams, errString string) error {
	// Implement the revert logic for backup delete workflows
	// This might involve rolling back any changes made during the workflow execution
	backupActivity := &activities.BackupActivity{}
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return err
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
	ctx = workflow.WithActivityOptions(ctx, ao)
	// Get backup from DB
	dbBackup := &datamodel.Backup{}
	err = workflow.ExecuteActivity(ctx, backupActivity.GetBackup, params.BackupVaultUUID, params.BackupUUID, params.AccountName).Get(ctx, &dbBackup)
	if err != nil {
		return fmt.Errorf("failed to get backup: %w", err)
	}
	dbBackup.Attributes.DeleteInitiated = wf.deleteInitiated
	if wf.deleteInitiated {
		if wf.Logger != nil {
			wf.Logger.Errorf("Backup to error state as delete has been initiated but failed to complete, backupUUID: %s", dbBackup.UUID)
		}
		err = workflow.ExecuteActivity(ctx, backupActivity.UpdateBackupError, dbBackup, errString).Get(ctx, nil)
		if err != nil {
			return ConvertToVSAError(err)
		}

		dbBackupVault := dbBackup.BackupVault
		// Delete remote backup from VCP if this is a cross-region backup
		if dbBackupVault.BackupVaultType == activities.CrossRegionBackupType && dbBackup.ExternalUUID != "" && dbBackupVault.ExternalUUID != nil && dbBackupVault.BackupRegionName != nil {
			remoteBackupErr := workflow.ExecuteActivity(ctx, backupActivity.DeleteRemoteBackupFromVCPActivity,
				dbBackup.ExternalUUID,
				*dbBackupVault.ExternalUUID,
				params.AccountName,
				*dbBackupVault.BackupRegionName).Get(ctx, nil)
			if remoteBackupErr != nil {
				wf.Logger.Errorf("Failed to delete remote backup from VCP for errored backup %s: %v", dbBackup.UUID, remoteBackupErr)
			}
		}
	} else {
		if wf.Logger != nil {
			wf.Logger.Errorf("Reverting backup state to available as delete was not initiated, backupUUID: %s", dbBackup.UUID)
		}
		// mark the backup back to available state
		err = workflow.ExecuteActivity(ctx, backupActivity.MarkBackupAvailable, dbBackup).Get(ctx, nil)
		if err != nil {
			return ConvertToVSAError(err)
		}
	}

	return nil
}

// UpdateBackupWorkflow Backup Workflow process backup related requests from a customer.
func UpdateBackupWorkflow(ctx workflow.Context, backup *datamodel.Backup) (gcpgenserver.V1betaUpdateBackupRes, error) {
	logger := util.GetLogger(ctx)
	backupWf := new(backupUpdateWorkflow)
	err := backupWf.Setup(ctx, backup)
	if err != nil {
		logger.Infof("Backup update workflow setup executed with error: %v", err)
		return nil, err
	}
	backupWf.Status = WorkflowStatusRunning
	err = backupWf.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		logger.Infof("Update job status for backup executed with error: %v", err)
		return nil, err
	}
	_, customErr := backupWf.Run(ctx, backup)

	if customErr != nil {
		backupWf.Status = WorkflowStatusFailed
		err2 := backupWf.UpdateJobStatus(ctx, string(models.JobsStateERROR), customErr)
		if err2 != nil {
			backupWf.Logger.Errorf("Failed to update job status: %v", err2)
		}
		return nil, customErr
	}
	backupWf.Status = WorkflowStatusCompleted
	err2 := backupWf.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	if err2 != nil {
		backupWf.Logger.Errorf("Failed to update job status: %v", err2)
		return nil, ConvertToVSAError(err2)
	}
	logger.Debug("Backup update workflow completed successfully")
	return nil, nil
}

// Setup initializes the workflow with the necessary parameters and sets up a query handler for status updates.
func (wf *backupUpdateWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	backupParams := input.(*datamodel.Backup)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = backupParams.Name
	wf.Status = WorkflowStatusCreated
	ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{"workflowID": wf.ID, "customerID": wf.CustomerID})
	logger := util.GetLogger(ctx)
	wf.Logger = logger

	return workflow.SetQueryHandler(ctx, "status", func() (*WorkflowStatus, error) {
		return &WorkflowStatus{
			ID:         wf.ID,
			Status:     wf.Status,
			CustomerID: wf.CustomerID,
		}, nil
	})
}

// Run executes the backup creation workflow, including creating the backup and updating its details.
func (wf *backupUpdateWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	backup := args[0].(*datamodel.Backup)
	backupUpdateActivity := &activities.BackupActivity{}
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts:        int32(retryPolicy.MaximumAttempts),
			NonRetryableErrorTypes: []string{"PanicError"},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	defer func() {
		if err != nil {
			// If an error occurs, update the backup backupActivitiesContext to ERROR
			errorActivity := workflow.ExecuteActivity(ctx, backupUpdateActivity.UpdateBackupError, backup, err.Error())
			if errorActivity.Get(ctx, nil) != nil {
				util.GetLogger(ctx).Errorf("Failed to update backup backupActivitiesContext to ERROR: %v", err)
			}
			return
		}
	}()
	err = workflow.ExecuteActivity(ctx, backupUpdateActivity.UpdateBackup, backup).Get(ctx, nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	// Update remote backup from VCP if this is a cross-region backup
	err = workflow.ExecuteActivity(ctx, backupUpdateActivity.UpdateRemoteBackupFromVCPActivity, backup).Get(ctx, nil)
	if err != nil {
		// Log the error but don't fail the entire backup workflow
		wf.Logger.Errorf("Failed to update remote backup from VCP for backup %s: %v", backup.UUID, err)
	}

	return nil, nil
}
