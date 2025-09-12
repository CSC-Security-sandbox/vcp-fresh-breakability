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
	log := util.GetLogger(ctx)
	dbVolume := args[0].(*datamodel.Volume)
	createVolumeParams := args[1].(*common.CreateVolumeParams)
	backupVault := args[2].(*datamodel.BackupVault)
	backup := args[3].(*datamodel.Backup)
	hostParams := args[4].([]*common.HostParams)
	volCreateResponse := args[5].(*vsa.VolumeResponse)
	isRestoreFromBackup := createVolumeParams.BackupPath != ""
	volumeActivity := &activities.VolumeCreateActivity{}
	var volumeUpdateActivity *activities.VolumeUpdateActivity
	volumeUpdateActivity = &activities.VolumeUpdateActivity{}
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

	// No need to defer rollback manager cleanup here, as it will be handled by the workflow engine
	defer func() {
		// just a placeholder for rollback manager cleanup
		disconnectedCtx, _ := workflow.NewDisconnectedContext(ctx)
		rollbackManager.ExecuteRollback(disconnectedCtx, err)
	}()

	// Execute VPC pool restoration activity to handle cross-project permissions
	err = workflow.ExecuteActivity(ctx, volumeActivity.CrossPoolOrVPCRestorationActivity, dbVolume.Pool, backup).Get(ctx, nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	rollbackManager.AddActivity(activities.VolumeCreateActivity.DeleteRolesForServiceAccountInBackupTenantProject, dbVolume.Pool, backup)

	var dbNodes []*datamodel.Node
	err = workflow.ExecuteActivity(ctx, activities.CommonActivities.GetNode, &dbVolume.Pool.ID).Get(ctx, &dbNodes)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	node := hyperscaler.CreateNodeForProvider(hyperscaler.NodeProviderInput{Nodes: dbNodes, Password: dbVolume.Pool.PoolCredentials.Password, SecretID: dbVolume.Pool.PoolCredentials.SecretID, DeploymentName: dbVolume.Pool.DeploymentName, CertificateID: dbVolume.Pool.PoolCredentials.CertificateID, AuthType: dbVolume.Pool.PoolCredentials.AuthType})

	// Post-provisioning child workflow
	preWorkflowFunc, err := selectVolumeChildWorkflow(dbVolume.VolumeAttributes.Protocols, PhasePre, dbVolume.Account.Name)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	var preUpdatedVolume *datamodel.Volume
	err = workflow.ExecuteChildWorkflow(ctx, preWorkflowFunc, dbVolume, node).Get(ctx, &preUpdatedVolume)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	// Update the dbVolume with any changes from the pre-workflow
	if preUpdatedVolume != nil {
		dbVolume = preUpdatedVolume
	}

	objStore := &common.CloudTarget{}
	backupActivity := &activities.BackupActivity{}
	var smDestinationPath string
	err = workflow.ExecuteActivity(ctx, backupActivity.GetSmSourcePathActivity, dbVolume).Get(ctx, &smDestinationPath)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	var smSourcePath string
	err = workflow.ExecuteActivity(ctx, backupActivity.GetSmSourcePathForRestoreActivity, backupVault, backup).Get(ctx, &smSourcePath)
	log.Debugf("\nsmDestinationPath: %v", smDestinationPath)
	log.Debugf("\nsmSourcePath: %v", smSourcePath)

	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	snapmirrorRelationship := &common.SnapmirrorRelationship{}
	SnapmirrorRelationshipParams := &common.SnapmirrorRelationshipParams{
		SourcePath:      smSourcePath,
		DestinationPath: smDestinationPath,
		SourceUUID:      &backup.Attributes.EndpointUUID,
		IsRestore:       true,
	}

	objStoreName, err := activities.GetObjStoreNameFromBackup(backupVault, backup)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	bucketDetails, err := activities.GetBucketDetailsFromBackup(backupVault, backup)
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

	done := false
	var status string
	waitTime := Wait
	for !done {
		err = workflow.ExecuteActivity(ctx, activities.BackupActivity.GetSnapmirrorTransferStatus, node, snapmirrorRelationship.UUID, "").Get(ctx, &status)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
		switch status {
		case activities.SmStatusTransferring:
			err := workflow.Sleep(ctx, waitTime) // Wait before polling again with exponential backoff
			if err != nil {
				return nil, ConvertToVSAError(fmt.Errorf("failed to sleep during snapmirror transfer polling: %w", err))
			}
			// Exponential backoff: double the wait time, but cap it at maxWaitTime
			waitTime = time.Duration(float64(waitTime) * 2)
			if waitTime > BackupMaxWaitTimeCap {
				waitTime = BackupMaxWaitTimeCap
			}
		case activities.SmStatusSuccess:
			log.Debugf("Snapmirror transfer completed successfully")
			done = true
		case activities.SmStatusFailed:
			return nil, ConvertToVSAError(fmt.Errorf("snapmirror transfer failed for restore with status: %s", status))
		}
	}

	volResponse := &vsa.VolumeResponse{}
	volumeTypeUpdateDone := false // reset for polling volume state change to RW
	for !volumeTypeUpdateDone {
		if errors.Is(ctx.Err(), workflow.ErrCanceled) {
			return nil, ConvertToVSAError(err)
		}
		err = workflow.ExecuteActivity(ctx, volumeUpdateActivity.GetVolumeFromONTAP, dbVolume, node, true).Get(ctx, &volResponse)
		if err != nil {
			log.Debugf("Get Volume from Ontap error : %s", err.Error())
			return nil, ConvertToVSAError(err)
		}
		if volResponse.Type == VolumeStateRW {
			log.Debugf("Volume %s is available as RW in ONTAP", dbVolume.UUID)
			volumeTypeUpdateDone = true
		} else if volResponse.Type == VolumeStateDP || volResponse.Type == VolumeStateLS {
			log.Debugf("Volume %s is still DP/LS and not available as RW in ONTAP", dbVolume.UUID)
			err := workflow.Sleep(ctx, WaitForRestore) // Wait before polling again
			if err != nil {
				return nil, ConvertToVSAError(fmt.Errorf("failed to sleep during volume availability polling: %w", err))
			}
		} else {
			log.Debugf("Type of volume %s is not correct. Current state in ONTAP is: %s", dbVolume.UUID, volResponse.Type)
			return nil, ConvertToVSAError(fmt.Errorf("failed to move the volume type of volume  %s to RW ", dbVolume.UUID))
		}
	}

	// Post-provisioning child workflow
	postWorkflowFunc, err := selectVolumeChildWorkflow(dbVolume.VolumeAttributes.Protocols, PhasePost, dbVolume.Account.Name)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	var updatedVolume *datamodel.Volume
	err = workflow.ExecuteChildWorkflow(ctx, postWorkflowFunc, dbVolume, node, hostParams, volCreateResponse, isRestoreFromBackup, false).Get(ctx, &updatedVolume)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	// Update the dbVolume with the changes from the child workflow
	if updatedVolume != nil {
		dbVolume = updatedVolume
	}
	dbVolume.VolumeAttributes.ExternalUUID = volCreateResponse.ExternalUUID

	var ontapAsyncResponse *vsa.OntapAsyncResponse
	err = workflow.ExecuteActivity(ctx, volumeActivity.DeleteObjectStoreForCrossVPC, dbVolume.Pool, backup, node, objStoreName).Get(ctx, &ontapAsyncResponse)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	err = WaitForONTAPJob(ctx, ontapAsyncResponse, node, time.Minute*10)
	if err != nil {
		return nil, ConvertToVSAError(fmt.Errorf("failed to delete cloud endpoint: %w", err))
	}

	err = workflow.ExecuteActivity(ctx, volumeActivity.FinaliseRestoredVolume, &dbVolume).Get(ctx, nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	return nil, ConvertToVSAError(err)
}
