package workflows

import (
	"fmt"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
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
	SE *database.Storage
}

var (
	_    WorkflowInterface = &BackupCreateWorkflow{}
	_    WorkflowInterface = &BackupDeleteWorkflow{}
	wait                   = time.Duration(env.GetUint("ONTAP_REST_ASYNC_POLL_WAIT_SECONDS", 3)) * time.Second
)

const (
	ObjStoreProviderType = "GoogleCloud"
	ObjStoreServer       = "storage.googleapis.com"
	backupComment        = "VCP-Backup"
	adcPort              = 443
)

// CreateBackupWorkflow  process backup related requests from a customer.
func CreateBackupWorkflow(ctx workflow.Context, params *commonparams.CreateBackupParams, backup *datamodel.Backup, backupVault *datamodel.BackupVault, volume *datamodel.Volume) (gcpgenserver.V1betaDeletePoolRes, error) {
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
	_, err = backupWf.Run(ctx, backup, backupVault, volume)
	if err != nil {
		err = backupWf.Revert(ctx, backup, volume, err.Error())
		if err != nil {
			return nil, err
		}
		backupWf.Status = WorkflowStatusFailed
		err = backupWf.UpdateJobStatus(ctx, string(models.JobsStateDONE), err)
		if err != nil {
			return nil, err
		}
	}
	backupWf.Status = WorkflowStatusCompleted
	err = backupWf.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	return nil, err
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

func (wf *BackupCreateWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, error) {
	dbBackup := args[0].(*datamodel.Backup)
	backupVault := args[1].(*datamodel.BackupVault)
	volume := args[2].(*datamodel.Volume)

	backupActivity := &activities.BackupActivity{}
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, err
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    retryPolicy.InitialInterval,
			BackoffCoefficient: retryPolicy.BackoffCoefficient,
			MaximumInterval:    retryPolicy.MaximumInterval,
			MaximumAttempts:    int32(retryPolicy.MaximumAttempts),
		},
	}
	// TODO: Once all the operation which are using these workflows are done, Need to investigate
	// That Can we pass the dbBackup object into each activity and let the activity enrich it with information as it progresses.
	// Or just have a common object which contains everything and pass it in and out of the activities

	ctx = workflow.WithActivityOptions(ctx, ao)
	var dbNodes []*datamodel.Node
	err = workflow.ExecuteActivity(ctx, activities.CommonActivities.GetNode, &volume.PoolID).Get(ctx, &dbNodes)
	if err != nil {
		return nil, err
	}
	node := commonparams.CreateNodeForProvider(commonparams.NodeProviderInput{Nodes: dbNodes, Username: volume.Pool.Username, Password: volume.Pool.Password, SecretID: volume.Pool.SecretID})
	objStore := &commonparams.CloudTarget{}
	objStoreName, err := getObjStoreName(backupVault, volume)
	if err != nil {
		return nil, err
	}
	bucketDetails, err := getBucketDetails(backupVault, volume)
	if err != nil {
		return nil, err
	}
	bucketName := bucketDetails.BucketName
	err = workflow.ExecuteActivity(ctx, backupActivity.GetOrCreateObjectStore, node, objStoreName, bucketName).Get(ctx, &objStore)
	if err != nil {
		return nil, err
	}
	dbBackup.Attributes.BucketName = bucketName
	dbBackup.Attributes.ServiceAccountName = bucketDetails.ServiceAccountName
	smDestinationPath, err := getSmDestinationPath(backupVault, volume)
	if err != nil {
		return nil, err
	}
	smSourcePath := getSmSourcePath(volume)
	snapmirrorRelationship := &commonparams.SnapmirrorRelationship{}
	SnapmirrorRelationshipParams := &commonparams.SnapmirrorRelationshipParams{
		SourcePath:      smSourcePath,
		DestinationPath: smDestinationPath,
		SourceUUID:      nil,
		IsRestore:       false,
	}
	err = workflow.ExecuteActivity(ctx, backupActivity.SnapmirrorGetorCreate, node, &SnapmirrorRelationshipParams).Get(ctx, &snapmirrorRelationship)
	if err != nil {
		return nil, err
	}
	if snapmirrorRelationship != nil && snapmirrorRelationship.DestinationUUID != nil {
		dbBackup.Attributes.EndpointUUID = *snapmirrorRelationship.DestinationUUID
	}
	snapshotName := getSnapshotName(dbBackup)
	snapshotResponse := &vsa.SnapshotProviderResponse{}
	err = workflow.ExecuteActivity(ctx, backupActivity.SnapshotCreate, node, volume.VolumeAttributes.ExternalUUID, snapshotName, backupComment).Get(ctx, &snapshotResponse)
	if err != nil {
		return nil, err
	}
	dbBackup.Attributes.SnapshotName = snapshotName
	dbBackup.Attributes.SnapshotID = snapshotResponse.ExternalUUID
	dbBackup.Attributes.SnapshotCreationTime = time.Now().String()

	err = workflow.ExecuteActivity(ctx, backupActivity.SnapmirrorTransfer, node, snapmirrorRelationship.UUID, snapshotName).Get(ctx, nil)
	if err != nil {
		return nil, err
	}
	done := false
	var status string
	for !done {
		err = workflow.ExecuteActivity(ctx, backupActivity.GetSnapmirrorTransferStatus, node, snapmirrorRelationship.UUID, snapshotName).Get(ctx, &status)
		if err != nil {
			return nil, err
		}
		switch status {
		case activities.SmStatusTransferring:
			err := workflow.Sleep(ctx, wait) // Wait before polling again
			if err != nil {
				return nil, fmt.Errorf("failed to sleep during snapmirror transfer polling: %w", err)
			}
		case activities.SmStatusSuccess:
			done = true
		case activities.SmStatusFailed:
			return nil, fmt.Errorf("snapmirror transfer failed for snapshot %s with status: %s", snapshotName, status)
		}
	}
	// TODO:  VSCP-615 - Delete older snapshots after backup is completed
	// err = workflow.ExecuteActivity(ctx, backupActivity.DeleteSnapshot, node, snapshotResponse.ExternalUUID, volume.VolumeAttributes.ExternalUUID).Get(ctx, nil)
	// if err != nil {
	//	return nil, err
	// }
	err = workflow.ExecuteActivity(ctx, backupActivity.FinishBackup, &dbBackup).Get(ctx, nil)
	if err != nil {
		return nil, err
	}
	return nil, err
}

func getObjStoreNameFromBackup(backupVault *datamodel.BackupVault, backup *datamodel.Backup) (string, error) {
	bucketDetails, err := getBucketDetailsFromBackup(backupVault, backup)
	if err != nil {
		return "", err
	}
	return bucketDetails.BucketName, nil
}

func getBucketDetailsFromBackup(backupVault *datamodel.BackupVault, backup *datamodel.Backup) (*datamodel.BucketDetails, error) {
	for _, bucketDetail := range backupVault.BucketDetails {
		if bucketDetail.BucketName != "" && bucketDetail.BucketName == backup.Attributes.BucketName {
			return bucketDetail, nil
		}
	}
	return nil, fmt.Errorf("no matching bucket details found for backup %s", backup.Name)
}

func getSmSourcePathForRestore(backupVault *datamodel.BackupVault, backup *datamodel.Backup) (string, error) {
	objStoreName, err := getObjStoreNameFromBackup(backupVault, backup)
	if err != nil {
		return "", fmt.Errorf("failed to get object store name: %w", err)
	}
	return fmt.Sprintf("%s:/objstore/%s", objStoreName, backup.Attributes.SnapshotID), nil
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
			InitialInterval:    retryPolicy.InitialInterval,
			BackoffCoefficient: retryPolicy.BackoffCoefficient,
			MaximumInterval:    retryPolicy.MaximumInterval,
			MaximumAttempts:    int32(retryPolicy.MaximumAttempts),
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	// updating the backup state to error
	err = workflow.ExecuteActivity(ctx, backupActivity.UpdateBackupError, &backup, errString).Get(ctx, nil)
	if err != nil {
		return err
	}
	// If the backup has a snapshot ID, delete the snapshot
	if backup.Attributes.SnapshotID != "" {
		var dbNodes []*datamodel.Node
		err = workflow.ExecuteActivity(ctx, activities.CommonActivities.GetNode, &volume.PoolID).Get(ctx, &dbNodes)
		if err != nil {
			return err
		}
		node := commonparams.
			CreateNodeForProvider(commonparams.NodeProviderInput{Nodes: dbNodes, Username: volume.Pool.Username, Password: volume.Pool.Password, SecretID: volume.Pool.SecretID})
		err = workflow.ExecuteActivity(ctx, backupActivity.DeleteBackupSnapshot, node, backup.Attributes.SnapshotID, volume.VolumeAttributes.ExternalUUID).Get(ctx, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func getSnapshotName(backup *datamodel.Backup) string {
	return fmt.Sprintf("vcp-ad-%s", backup.Name)
}

func getObjStoreName(backupVault *datamodel.BackupVault, vol *datamodel.Volume) (string, error) {
	bucketDetails, err := getBucketDetails(backupVault, vol)
	if err != nil {
		return "", err
	}
	return bucketDetails.BucketName, nil
}
func getBucketDetails(backupVault *datamodel.BackupVault, vol *datamodel.Volume) (*datamodel.BucketDetails, error) {
	for _, bucketDetail := range backupVault.BucketDetails {
		if bucketDetail.VendorSubnetID == vol.VolumeAttributes.VendorSubnetID && bucketDetail.BucketName != "" {
			return bucketDetail, nil
		}
	}
	return nil, fmt.Errorf("no matching bucket details found for volume %s in backup vault %s", vol.Name, backupVault.Name)
}

func getSmSourcePath(volume *datamodel.Volume) string {
	return fmt.Sprintf("%s:%s", volume.Svm.Name, volume.Name)
}

func getSmDestinationPath(backupVault *datamodel.BackupVault, volume *datamodel.Volume) (string, error) {
	objStoreName, err := getObjStoreName(backupVault, volume)
	if err != nil {
		return "", fmt.Errorf("failed to get object store name: %w", err)
	}
	return fmt.Sprintf("%s:/objstore/%s", objStoreName, volume.UUID), nil
}

func getBucketDetailsForBucket(backupVault *datamodel.BackupVault, bucketName string) (*datamodel.BucketDetails, error) {
	for _, bucketDetail := range backupVault.BucketDetails {
		if bucketDetail.BucketName == bucketName {
			return bucketDetail, nil
		}
	}
	return nil, fmt.Errorf("no matching bucket details found for bucket %s in backup vault %s", bucketName, backupVault.Name)
}

func DeleteBackupWorkflow(ctx workflow.Context, params *commonparams.DeleteBackupParams) (gcpgenserver.V1betaDeletePoolRes, error) {
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
	_, err = backupWf.Run(ctx, params)
	if err != nil {
		// backup state to error
		err = backupWf.HandleError(ctx, params, err.Error())
		if err != nil {
			// If revert fails, log the error but do not return it
			util.GetLogger(ctx).Errorf("Failed to revert backup delete workflow: %v", err)
		}
		backupWf.Status = WorkflowStatusFailed
		err = backupWf.UpdateJobStatus(ctx, string(models.JobsStateDONE), err)
		if err != nil {
			return nil, err
		}
	}
	backupWf.Status = WorkflowStatusCompleted
	err = backupWf.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	return nil, err
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

func (wf *BackupDeleteWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, error) {
	deleteBackupParams := args[0].(*commonparams.DeleteBackupParams)
	backupActivity := &activities.BackupActivity{}
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, err
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    retryPolicy.InitialInterval,
			BackoffCoefficient: retryPolicy.BackoffCoefficient,
			MaximumInterval:    retryPolicy.MaximumInterval,
			MaximumAttempts:    int32(retryPolicy.MaximumAttempts),
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	// check if backup Vault is present in VSA
	dbBackupVault := &datamodel.BackupVault{}
	err = workflow.ExecuteActivity(ctx, backupActivity.GetBackupVault, deleteBackupParams.BackupVaultUUID).Get(ctx, &dbBackupVault)
	if err != nil {
		return nil, err
	}

	dbBackup := &datamodel.Backup{}
	err = workflow.ExecuteActivity(ctx, backupActivity.GetBackup, deleteBackupParams.BackupVaultUUID, deleteBackupParams.BackupUUID, deleteBackupParams.AccountName).Get(ctx, &dbBackup)
	if err != nil {
		return nil, err
	}

	var isVolumeDeleted bool
	err = workflow.ExecuteActivity(ctx, backupActivity.IsVolumeDeleted, dbBackup.VolumeUUID).Get(ctx, &isVolumeDeleted)
	if err != nil {
		return nil, err
	}

	bucketDetails, err := getBucketDetailsForBucket(dbBackupVault, dbBackup.Attributes.BucketName)
	if err != nil {
		return nil, err
	}

	if isVolumeDeleted {
		// var hmacKeys commonparams.HmacKeys
		// err = workflow.ExecuteActivity(ctx, backupActivity.CreateHmacKeys, &commonparams.HmacKeyCreateParams{
		//	ServiceAccount: bucketDetails.ServiceAccountName,
		//	ProjectNumber:  bucketDetails.TenantProjectNumber,
		// }).Get(ctx, &hmacKeys)
		// if err != nil {
		//	return nil, err
		// }
		//
		// defer func() {
		//	// Delete HMAC keys after workflow execution
		//	if err = workflow.ExecuteActivity(ctx, backupActivity.DeleteHmacKeys, dbBackupVault.BucketDetails[0].TenantProjectNumber, hmacKeys.AccessKey, bucketDetails.ServiceAccountName).Get(ctx, nil); err != nil {
		//		util.GetLogger(ctx).Errorf("Failed to delete HMAC keys: %v", err)
		//	}
		// }()

		// Call ADC Workflow
		// adcParams := &commonparams.ADCParams{
		//	AccountName:      deleteBackupParams.AccountName,
		//	DestEndpointUUID: dbBackup.Attributes.EndpointUUID,
		//	SnapshotUUID:     dbBackup.Attributes.SnapshotID,
		//	BucketName:       dbBackup.Attributes.BucketName,
		//	AccessKey:        hmacKeys.AccessKey,
		//	SecretKey:        hmacKeys.SecretKey,
		//	ProvideType:      ObjStoreProviderType,
		//	ServerURL:        ObjStoreServer,
		//	Port:             adcPort,
		// }
		// TODO : Not Removing above code as we need to handle delete orphan backup
	} else {
		var backupCount int64
		err = workflow.ExecuteActivity(ctx, backupActivity.GetBackupCountByVolumeUUID, dbBackup.VolumeUUID).Get(ctx, &backupCount)
		if err != nil {
			return nil, err
		}

		var volume *datamodel.Volume
		err = workflow.ExecuteActivity(ctx, backupActivity.GetVolume, dbBackup.VolumeUUID).Get(ctx, &volume)
		if err != nil {
			return nil, err
		}

		var dbNodes []*datamodel.Node
		err = workflow.ExecuteActivity(ctx, activities.CommonActivities.GetNode, &volume.PoolID).Get(ctx, &dbNodes)
		if err != nil {
			return nil, err
		}
		node := commonparams.CreateNodeForProvider(commonparams.NodeProviderInput{Nodes: dbNodes, Username: volume.Pool.Username, Password: volume.Pool.Password})

		var objStore *commonparams.CloudTarget
		err = workflow.ExecuteActivity(ctx, backupActivity.GetObjectStore, node, bucketDetails.BucketName).Get(ctx, &objStore)
		if err != nil {
			return nil, err
		}

		var ontapAsyncResponse *vsa.OntapAsyncResponse
		if backupCount == 1 {
			smDestinationPath, err := getSmDestinationPath(dbBackupVault, volume)
			if err != nil {
				return nil, err
			}
			var snapmirrorRelationship *commonparams.SnapmirrorRelationship
			err = workflow.ExecuteActivity(ctx, backupActivity.GetSnapmirror, node, getSmSourcePath(volume), smDestinationPath).Get(ctx, &snapmirrorRelationship)
			if err != nil {
				return nil, err
			}

			err = workflow.ExecuteActivity(ctx, backupActivity.DeleteSnapmirror, node, snapmirrorRelationship.UUID).Get(ctx, &ontapAsyncResponse)
			if err != nil {
				return nil, err
			}
			err = WaitForONTAPJob(ctx, ontapAsyncResponse, node, time.Minute*10)
			if err != nil {
				return nil, fmt.Errorf("failed to delete snapmirror: %w", err)
			}

			err = workflow.ExecuteActivity(ctx, backupActivity.DeleteCloudEndpoint, node, objStore.UUID, dbBackup.Attributes.EndpointUUID).Get(ctx, &ontapAsyncResponse)
			if err != nil {
				return nil, err
			}
			err = WaitForONTAPJob(ctx, ontapAsyncResponse, node, time.Minute*10)
			if err != nil {
				return nil, fmt.Errorf("failed to delete cloud endpoint: %w", err)
			}

			err = workflow.ExecuteActivity(ctx, backupActivity.DeleteSnapshotForBackup, node, dbBackup.Attributes.SnapshotID, volume.VolumeAttributes.ExternalUUID).Get(ctx, nil)
			if err != nil {
				return nil, err
			}
		} else {
			err = workflow.ExecuteActivity(ctx, backupActivity.DeleteSnapshotFromObjectStore, node, objStore.UUID, dbBackup.Attributes.EndpointUUID, dbBackup.Attributes.SnapshotID).Get(ctx, &ontapAsyncResponse)
			if err != nil {
				return nil, err
			}
		}
	}

	err = workflow.ExecuteActivity(ctx, backupActivity.DeleteBackup, deleteBackupParams.BackupUUID).Get(ctx, &dbBackup)
	if err != nil {
		return nil, err
	}

	return nil, err
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
			InitialInterval:    retryPolicy.InitialInterval,
			BackoffCoefficient: retryPolicy.BackoffCoefficient,
			MaximumInterval:    retryPolicy.MaximumInterval,
			MaximumAttempts:    int32(retryPolicy.MaximumAttempts),
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	// Get backup from DB
	dbBackup := &datamodel.Backup{}
	err = workflow.ExecuteActivity(ctx, backupActivity.GetBackup, params.BackupVaultUUID, params.BackupUUID, params.AccountName).Get(ctx, &dbBackup)
	if err != nil {
		return fmt.Errorf("failed to get backup: %w", err)
	}
	err = workflow.ExecuteActivity(ctx, backupActivity.UpdateBackupError, dbBackup, errString).Get(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to update backup error: %w", err)
	}
	return nil
}

// WaitForONTAPJob waits for an ONTAP job to complete, taking a workflow context, job UUID, node, and timeout as input.
func WaitForONTAPJob(ctx workflow.Context, jobResponse *vsa.OntapAsyncResponse, node *models.Node, timeout time.Duration) error {
	if jobResponse == nil || jobResponse.JobUUID == "" {
		return nil
	}
	var job *vsa.OntapJob
	startTime := time.Now()

	for {
		// Check if the timeout has been reached
		if time.Since(startTime) > timeout {
			return fmt.Errorf("ontap job %s timed out after %v", jobResponse.JobUUID, timeout)
		}

		// Execute the activity to get the ONTAP job status
		err := workflow.ExecuteActivity(ctx, activities.CommonActivities.GetOntapJob, jobResponse.JobUUID, node).Get(ctx, &job)
		if err != nil {
			return err
		}

		// Check the job state
		switch job.State {
		case "failure":
			if job.Error != nil {
				return errors.New(job.Error.Message)
			}
			return fmt.Errorf("ontap job %s failed with no error message", job.UUID)
		case "success":
			return nil
		}

		// Sleep for a short duration before checking again
		err = workflow.Sleep(ctx, 5*time.Second)
		if err != nil {
			return fmt.Errorf("failed to sleep while waiting for ONTAP job %s: %w", jobResponse.JobUUID, err)
		}
	}
}
