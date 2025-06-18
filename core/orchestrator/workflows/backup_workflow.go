package workflows

import (
	"fmt"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type BackupCreateWorkflow struct {
	BaseWorkflow
	SE *database.Storage
}

var _ WorkflowInterface = &BackupCreateWorkflow{}

const backupComment = "VCP-Backup"

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
		err := backupWf.Revert(ctx, backup, volume, err.Error())
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
	node := CreateNodeForProviderWithPool(dbNodes, volume.Pool)
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
	snapmirrorRelationship := &commonparams.SnapmirrorRelationship{}
	err = workflow.ExecuteActivity(ctx, backupActivity.SnapmirrorGetorCreate, node, getSmSourcePath(volume), smDestinationPath).Get(ctx, &snapmirrorRelationship)
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
	err = workflow.ExecuteActivity(ctx, backupActivity.SnapmirrorTransferPoll, node, snapmirrorRelationship.UUID, snapshotName).Get(ctx, nil)
	if err != nil {
		return nil, err
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
		node := CreateNodeForProviderWithPool(dbNodes, volume.Pool)
		err = workflow.ExecuteActivity(ctx, backupActivity.DeleteBackupSnapshot, node, backup.Attributes.SnapshotID, volume.VolumeAttributes.ExternalUUID).Get(ctx, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func getSnapshotName(backup *datamodel.Backup) string {
	return fmt.Sprintf("adhoc-:%s", backup.Name)
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
