package workflows

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// Volume State constants
const (
	VolumeStateRW = "rw"
	VolumeStateDP = "dp"
	VolumeStateLS = "ls"
)

var (
	WaitForRestore = time.Duration(10) * time.Second
)

type restoreBackupWorkflow struct {
	BaseWorkflow
}

// Enforcing the WorkflowInterface on backupRestoreWorkflow
var _ WorkflowInterface = &restoreBackupWorkflow{}

// RestoreBackupWorkflow Restore Workflow process backup restore related requests from a customer.
func RestoreBackupWorkflow(ctx workflow.Context, params *common.CreateVolumeParams, volume *datamodel.Volume, backupVault *datamodel.BackupVault, backup *datamodel.Backup, hostParams []*common.HostParams, volCreateResponse *vsa.VolumeResponse) (gcpgenserver.V1betaDescribeVolumeRes, error) {
	log := util.GetLogger(ctx)
	restoreWf := new(restoreBackupWorkflow)
	err := restoreWf.Setup(ctx, params)
	if err != nil {
		log.Errorf("Failed to setup RestoreBackupWorkflow: %v", err)
		return nil, err
	}
	restoreWf.Status = WorkflowStatusRunning
	err = restoreWf.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		log.Errorf("Failed to update job status to Processing for RestoreBackupWorkflow: %v", err)
		return nil, err
	}
	var customErr *vsaerrors.CustomError
	_, customErr = restoreWf.Run(ctx, volume, params, backupVault, backup, hostParams, volCreateResponse)
	if customErr != nil {
		// Check if the error is a ContinueAsNewError - if so, don't call revert
		if workflow.IsContinueAsNewError(customErr.OriginalErr) {
			return nil, customErr
		}
		log.Errorf("RestoreBackupWorkflow completed with error: %v", customErr.OriginalErr.Error())
		restoreWf.Status = WorkflowStatusFailed
		err2 := restoreWf.UpdateJobStatus(ctx, string(models.JobsStateERROR), customErr)
		if err2 != nil {
			log.Errorf("Failed to update job status to Done with error for RestoreBackupWorkflow: %v", err2)
			return nil, err2
		}
		return nil, customErr
	}
	restoreWf.Status = WorkflowStatusCompleted
	err = restoreWf.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	if err != nil {
		log.Errorf("Failed to update job status to Done for RestoreBackupWorkflow: %v", err)
	}
	return nil, err
}

func RestoreBackupWorkflowWithContext(ctx workflow.Context, backupActivitiesContext *activities.BackupActivitiesContext, params *common.CreateVolumeParams, hostParams []*common.HostParams, volCreateResponse *vsa.VolumeResponse) (gcpgenserver.V1betaDescribeVolumeRes, error) {
	log := util.GetLogger(ctx)
	restoreWf := new(restoreBackupWorkflow)
	err := restoreWf.Setup(ctx, params)
	if err != nil {
		log.Errorf("Failed to setup RestoreBackupWorkflow: %v", err)
		return nil, err
	}
	restoreWf.Status = WorkflowStatusRunning
	err = restoreWf.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		log.Errorf("Failed to update job status to Processing for RestoreBackupWorkflow: %v", err)
		return nil, err
	}
	var customErr *vsaerrors.CustomError
	_, customErr = restoreWf.RunWithContext(ctx, backupActivitiesContext, params, hostParams, volCreateResponse)
	if customErr != nil {
		// Check if the error is a ContinueAsNewError - if so, don't call revert
		if workflow.IsContinueAsNewError(customErr.OriginalErr) {
			return nil, customErr
		}
		log.Errorf("RestoreBackupWorkflow completed with error: %v", customErr.OriginalErr.Error())
		restoreWf.Status = WorkflowStatusFailed
		err2 := restoreWf.UpdateJobStatus(ctx, string(models.JobsStateERROR), customErr)
		if err2 != nil {
			log.Errorf("Failed to update job status to Done with error for RestoreBackupWorkflow: %v", err2)
			return nil, err2
		}
		return nil, customErr
	}
	restoreWf.Status = WorkflowStatusCompleted
	err = restoreWf.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	if err != nil {
		log.Errorf("Failed to update job status to Done for RestoreBackupWorkflow: %v", err)
	}
	return nil, err
}

func (wf *restoreBackupWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	createVolParams := input.(*common.CreateVolumeParams)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = createVolParams.AccountName
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

func (wf *restoreBackupWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	createVolumeParams := args[1].(*common.CreateVolumeParams)
	hostParams := args[4].([]*common.HostParams)
	volCreateResponse := args[5].(*vsa.VolumeResponse)

	backupActivitiesContext := &activities.BackupActivitiesContext{
		BackupWorkflowInit: &activities.BackupWorkflowInput{
			Backup:      args[3].(*datamodel.Backup),
			BackupVault: args[2].(*datamodel.BackupVault),
			Volume:      args[0].(*datamodel.Volume),
		},
	}

	return wf.RunWithContext(ctx, backupActivitiesContext, createVolumeParams, hostParams, volCreateResponse)
}

func (wf *restoreBackupWorkflow) RunWithContext(ctx workflow.Context, backupActivitiesContext *activities.BackupActivitiesContext, createVolumeParams *common.CreateVolumeParams, hostParams []*common.HostParams, volCreateResponse *vsa.VolumeResponse) (interface{}, *vsaerrors.CustomError) {
	log := util.GetLogger(ctx)
	isRestoreFromBackup := createVolumeParams.BackupPath != ""
	volumeActivity := &activities.VolumeCreateActivity{}
	var volumeUpdateActivity *activities.VolumeUpdateActivity
	volumeUpdateActivity = &activities.VolumeUpdateActivity{}
	backupActivity := &activities.BackupActivity{}
	var err error
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: RestoreStartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    retryPolicy.InitialInterval,
			BackoffCoefficient: retryPolicy.BackoffCoefficient,
			MaximumInterval:    retryPolicy.MaximumInterval,
			MaximumAttempts:    int32(retryPolicy.MaximumAttempts),
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	rollbackManager := common.NewRollbackManager()

	var incrementErr error
	defer func() {
		// Decrement backup restore count after the workflow is complete
		if incrementErr == nil {
			decrementErr := workflow.ExecuteActivity(ctx, backupActivity.UpdateBackupRestoreCount,
				backupActivitiesContext.BackupWorkflowInit.BackupVault.UUID,
				backupActivitiesContext.BackupWorkflowInit.Backup.UUID,
				backupActivitiesContext.BackupWorkflowInit.Volume.Account.Name, activities.BackupRestoreCountDecrement).Get(ctx, nil)
			log.Errorf("Failed to revert backup restore count: %v", decrementErr)
		}

		// just a placeholder for rollback manager cleanup
		if err != nil && workflow.IsContinueAsNewError(err) {
			// Don't execute rollback for ContinueAsNew - let the new execution handle it
			return
		}
		disconnectedCtx, _ := workflow.NewDisconnectedContext(ctx)
		rollbackManager.ExecuteRollback(disconnectedCtx, err)
	}()

	info := workflow.GetInfo(ctx)
	isContinuation := info.ContinuedExecutionRunID != ""

	if isContinuation {
		wf.Logger.Info("Resuming backup workflow from continuation",
			"workflowID", wf.ID,
			"continuedFromRunID", info.OriginalRunID,
			"snapshotName", backupActivitiesContext.SnapshotName,
			"transferStatus", backupActivitiesContext.TransferStatus)
	} else {
		// Increment restore count to indicate that a volume restoration is in-progress for the backup
		incrementErr = workflow.ExecuteActivity(ctx, backupActivity.UpdateBackupRestoreCount,
			backupActivitiesContext.BackupWorkflowInit.BackupVault.UUID,
			backupActivitiesContext.BackupWorkflowInit.Backup.UUID,
			backupActivitiesContext.BackupWorkflowInit.Volume.Account.Name, activities.BackupRestoreCountIncrement).Get(ctx, nil)
		if incrementErr != nil {
			log.Errorf("Failed to update backup restore count: %v", incrementErr)
			return nil, ConvertToVSAError(incrementErr)
		}

		// Execute VPC pool restoration activity to handle cross-project permissions
		err = workflow.ExecuteActivity(ctx, volumeActivity.CrossPoolOrVPCRestorationActivity, backupActivitiesContext.BackupWorkflowInit.Volume.Pool, backupActivitiesContext.BackupWorkflowInit.Backup).Get(ctx, nil)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
		rollbackManager.AddActivity(activities.VolumeCreateActivity.DeleteRolesForServiceAccountInBackupTenantProject, backupActivitiesContext.BackupWorkflowInit.Volume.Pool, backupActivitiesContext.BackupWorkflowInit.Backup)

		var dbNodes []*datamodel.Node
		err = workflow.ExecuteActivity(ctx, activities.CommonActivities.GetNode, &backupActivitiesContext.BackupWorkflowInit.Volume.Pool.ID).Get(ctx, &dbNodes)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}

		node := hyperscaler.CreateNodeForProvider(hyperscaler.NodeProviderInput{
			Nodes:            dbNodes,
			DeploymentName:   backupActivitiesContext.BackupWorkflowInit.Volume.Pool.DeploymentName,
			OntapCredentials: backupActivitiesContext.BackupWorkflowInit.Volume.Pool.PoolCredentials,
		})
		backupActivitiesContext.Node = node

		objStore := &common.CloudTarget{}
		var smDestinationPath string
		err = workflow.ExecuteActivity(ctx, backupActivity.GetSmSourcePathActivity, backupActivitiesContext.BackupWorkflowInit.Volume).Get(ctx, &smDestinationPath)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
		var smSourcePath string
		err = workflow.ExecuteActivity(ctx, backupActivity.GetSmSourcePathForRestoreActivity, backupActivitiesContext.BackupWorkflowInit.BackupVault, backupActivitiesContext.BackupWorkflowInit.Backup).Get(ctx, &smSourcePath)
		log.Debugf("\nsmDestinationPath: %v", smDestinationPath)
		log.Debugf("\nsmSourcePath: %v", smSourcePath)

		if err != nil {
			return nil, ConvertToVSAError(err)
		}

		snapmirrorRelationship := &common.SnapmirrorRelationship{}
		SnapmirrorRelationshipParams := &common.SnapmirrorRelationshipParams{
			SourcePath:      smSourcePath,
			DestinationPath: smDestinationPath,
			SourceUUID:      &backupActivitiesContext.BackupWorkflowInit.Backup.Attributes.EndpointUUID,
			IsRestore:       true,
		}

		objStoreName, err := activities.GetObjStoreNameFromBackup(backupActivitiesContext.BackupWorkflowInit.BackupVault, backupActivitiesContext.BackupWorkflowInit.Backup)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
		backupActivitiesContext.ObjStoreName = objStoreName

		bucketDetails, err := activities.GetBucketDetailsFromBackup(backupActivitiesContext.BackupWorkflowInit.BackupVault, backupActivitiesContext.BackupWorkflowInit.Backup)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
		bucketName := bucketDetails.BucketName

		err = workflow.Sleep(ctx, 60*time.Second)
		if err != nil {
			return nil, ConvertToVSAError(fmt.Errorf("failed to sleep before starting snapmirror restore: %w", err))
		}
		err = workflow.ExecuteActivity(ctx, activities.BackupActivity.GetOrCreateObjectStore, node, objStoreName, bucketName).Get(ctx, &objStore)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
		err = workflow.ExecuteActivity(ctx, activities.BackupActivity.SnapmirrorGetOrCreate, node, &SnapmirrorRelationshipParams).Get(ctx, &snapmirrorRelationship)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
		err = workflow.ExecuteActivity(ctx, activities.BackupActivity.SnapmirrorTransfer, node, snapmirrorRelationship.UUID, "").Get(ctx, nil)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
		backupActivitiesContext.SnapmirrorRelationship = snapmirrorRelationship
	}

	err = wf.PollTransferStatusWithContinueAsNew(ctx, backupActivitiesContext, createVolumeParams, hostParams, volCreateResponse)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	volResponse := &vsa.VolumeResponse{}
	volumeTypeUpdateDone := false // reset for polling volume state change to RW
	for !volumeTypeUpdateDone {
		if errors.Is(ctx.Err(), workflow.ErrCanceled) {
			return nil, ConvertToVSAError(err)
		}
		err = workflow.ExecuteActivity(ctx, volumeUpdateActivity.GetVolumeFromONTAP, backupActivitiesContext.BackupWorkflowInit.Volume, backupActivitiesContext.Node, true).Get(ctx, &volResponse)
		if err != nil {
			log.Debugf("Get Volume from Ontap error : %s", err.Error())
			return nil, ConvertToVSAError(err)
		}
		if volResponse.Type == VolumeStateRW {
			log.Debugf("Volume %s is available as RW in ONTAP", backupActivitiesContext.BackupWorkflowInit.Volume.UUID)
			volumeTypeUpdateDone = true
		} else if volResponse.Type == VolumeStateDP || volResponse.Type == VolumeStateLS {
			log.Debugf("Volume %s is still DP/LS and not available as RW in ONTAP", backupActivitiesContext.BackupWorkflowInit.Volume.UUID)
			err := workflow.Sleep(ctx, WaitForRestore) // Wait before polling again
			if err != nil {
				return nil, ConvertToVSAError(fmt.Errorf("failed to sleep during volume availability polling: %w", err))
			}
		} else {
			log.Debugf("Type of volume %s is not correct. Current state in ONTAP is: %s", backupActivitiesContext.BackupWorkflowInit.Volume.UUID, volResponse.Type)
			return nil, ConvertToVSAError(fmt.Errorf("failed to move the volume type of volume  %s to RW ", backupActivitiesContext.BackupWorkflowInit.Volume.UUID))
		}
	}

	// Post-provisioning child workflow
	postWorkflowFunc, err := selectVolumeChildWorkflow(backupActivitiesContext.BackupWorkflowInit.Volume.VolumeAttributes.Protocols, PhasePost, backupActivitiesContext.BackupWorkflowInit.Volume.Account.Name)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	var updatedVolume *datamodel.Volume
	err = workflow.ExecuteChildWorkflow(ctx, postWorkflowFunc, backupActivitiesContext.BackupWorkflowInit.Volume, backupActivitiesContext.Node, hostParams, volCreateResponse, isRestoreFromBackup, false).Get(ctx, &updatedVolume)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	// Update the dbVolume with the changes from the child workflow
	if updatedVolume != nil {
		backupActivitiesContext.BackupWorkflowInit.Volume = updatedVolume
	}
	backupActivitiesContext.BackupWorkflowInit.Volume.VolumeAttributes.ExternalUUID = volCreateResponse.ExternalUUID

	err = workflow.ExecuteActivity(ctx, volumeUpdateActivity.UpdateVolumeJunctionpath, backupActivitiesContext.BackupWorkflowInit.Volume, backupActivitiesContext.Node).Get(ctx, nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	var ontapAsyncResponse *vsa.OntapAsyncResponse
	err = workflow.ExecuteActivity(ctx, volumeActivity.DeleteObjectStoreForCrossVPC, backupActivitiesContext.BackupWorkflowInit.Volume.Pool, backupActivitiesContext.BackupWorkflowInit.Backup, backupActivitiesContext.Node, backupActivitiesContext.ObjStoreName).Get(ctx, &ontapAsyncResponse)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	err = WaitForONTAPJob(ctx, ontapAsyncResponse, backupActivitiesContext.Node, time.Minute*10)
	if err != nil {
		return nil, ConvertToVSAError(fmt.Errorf("failed to delete cloud endpoint: %w", err))
	}

	err = workflow.ExecuteActivity(ctx, volumeActivity.FinaliseRestoredVolume, &backupActivitiesContext.BackupWorkflowInit.Volume).Get(ctx, nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	// No need to defer rollback manager cleanup here, as it will be handled by the workflow engine
	return nil, ConvertToVSAError(err)
}

// PollTransferStatusWithContinueAsNew polls transfer status with automatic ContinueAsNew when history limits are reached
// This function is specifically designed for backup restore workflows
func (wf *restoreBackupWorkflow) PollTransferStatusWithContinueAsNew(ctx workflow.Context, backupActivitiesContext *activities.BackupActivitiesContext, params *common.CreateVolumeParams, hostParams []*common.HostParams, volCreateResponse *vsa.VolumeResponse) error {
	return PollTransferStatusWithContinueAsNewCommon(ctx, backupActivitiesContext, RestoreBackupWorkflowWithContext, backupActivitiesContext, params, hostParams, volCreateResponse)
}
