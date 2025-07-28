package workflows

import (
	"fmt"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
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
	SE *database.Storage
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
	ObjStoreProviderType = "GoogleCloud"
	ObjStoreServer       = "storage.googleapis.com"
	BackupComment        = "VCP-Backup"
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
	node := commonparams.CreateNodeForProvider(commonparams.NodeProviderInput{Nodes: dbNodes, Password: volume.Pool.PoolCredentials.Password, SecretID: volume.Pool.PoolCredentials.SecretID, DeploymentName: volume.Pool.DeploymentName, CertificateID: volume.Pool.PoolCredentials.CertificateID, AuthType: volume.Pool.PoolCredentials.AuthType})
	objStore := &commonparams.CloudTarget{}
	var objStoreName string
	err = workflow.ExecuteActivity(ctx, backupActivity.GetObjStoreNameActivity, backupVault, volume).Get(ctx, &objStoreName)
	if err != nil {
		return nil, err
	}
	var bucketDetails *datamodel.BucketDetails
	err = workflow.ExecuteActivity(ctx, backupActivity.GetBucketDetailsActivity, backupVault, volume).Get(ctx, &bucketDetails)
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
	var smDestinationPath string
	err = workflow.ExecuteActivity(ctx, backupActivity.GetSmDestinationPathActivity, backupVault, volume).Get(ctx, &smDestinationPath)
	if err != nil {
		return nil, err
	}
	var smSourcePath string
	err = workflow.ExecuteActivity(ctx, backupActivity.GetSmSourcePathActivity, volume).Get(ctx, &smSourcePath)
	if err != nil {
		return nil, err
	}
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
	err = workflow.ExecuteActivity(ctx, backupActivity.SnapshotCreate, node, volume.VolumeAttributes.ExternalUUID, snapshotName, BackupComment).Get(ctx, &snapshotResponse)
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
			err := workflow.Sleep(ctx, Wait) // Wait before polling again
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
		node := commonparams.CreateNodeForProvider(commonparams.NodeProviderInput{Nodes: dbNodes, Password: volume.Pool.PoolCredentials.Password, SecretID: volume.Pool.PoolCredentials.SecretID, DeploymentName: volume.Pool.DeploymentName, CertificateID: volume.Pool.PoolCredentials.CertificateID, AuthType: volume.Pool.PoolCredentials.AuthType})
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
		node := commonparams.CreateNodeForProvider(commonparams.NodeProviderInput{Nodes: dbNodes, Password: volume.Pool.PoolCredentials.Password, SecretID: volume.Pool.PoolCredentials.SecretID, DeploymentName: volume.Pool.DeploymentName, CertificateID: volume.Pool.PoolCredentials.CertificateID, AuthType: volume.Pool.PoolCredentials.AuthType})

		var objStore *commonparams.CloudTarget
		err = workflow.ExecuteActivity(ctx, backupActivity.GetObjectStore, node, bucketDetails.BucketName).Get(ctx, &objStore)
		if err != nil {
			return nil, err
		}

		var ontapAsyncResponse *vsa.OntapAsyncResponse
		if backupCount == 1 {
			var smDestinationPath string
			err = workflow.ExecuteActivity(ctx, backupActivity.GetSmDestinationPathActivity, dbBackupVault, volume).Get(ctx, &smDestinationPath)
			if err != nil {
				return nil, err
			}
			var snapmirrorRelationship *commonparams.SnapmirrorRelationship
			var smSourcePath string
			err = workflow.ExecuteActivity(ctx, backupActivity.GetSmSourcePathActivity, volume).Get(ctx, &smSourcePath)
			if err != nil {
				return nil, err
			}
			err = workflow.ExecuteActivity(ctx, backupActivity.GetSnapmirror, node, smSourcePath, smDestinationPath).Get(ctx, &snapmirrorRelationship)
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
			var isBackupShared bool
			err = workflow.ExecuteActivity(ctx, backupActivity.IsBackupShared, dbBackup).Get(ctx, &isBackupShared)
			if err != nil {
				return nil, err
			}
			if !isBackupShared {
				err = workflow.ExecuteActivity(ctx, backupActivity.DeleteSnapshotFromObjectStore, node, objStore.UUID, dbBackup.Attributes.EndpointUUID, dbBackup.Attributes.SnapshotID).Get(ctx, &ontapAsyncResponse)
				if err != nil {
					return nil, err
				}
				err = WaitForONTAPJob(ctx, ontapAsyncResponse, node, time.Minute*10)
				if err != nil {
					return nil, fmt.Errorf("failed to delete cloud endpoint: %w", err)
				}
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
	_, err = backupWf.Run(ctx, backup)
	if err != nil {
		backupWf.Status = WorkflowStatusFailed
		err = backupWf.UpdateJobStatus(ctx, string(models.JobsStateDONE), err)
		if err != nil {
			return nil, err
		}
	}
	backupWf.Status = WorkflowStatusCompleted
	err = backupWf.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	logger.Debug("Backup update workflow completed successfully")
	return nil, err
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
func (wf *backupUpdateWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, error) {
	backup := args[0].(*datamodel.Backup)
	backupUpdateActivity := &activities.BackupActivity{}
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, err
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: int32(retryPolicy.MaximumAttempts),
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	defer func() {
		if err != nil {
			// If an error occurs, update the backup state to ERROR
			errorActivity := workflow.ExecuteActivity(ctx, backupUpdateActivity.UpdateBackupError, backup, err.Error())
			if errorActivity.Get(ctx, nil) != nil {
				util.GetLogger(ctx).Errorf("Failed to update backup state to ERROR: %v", err)
			}
			return
		}
	}()
	err = workflow.ExecuteActivity(ctx, backupUpdateActivity.UpdateBackup, backup).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	return nil, nil
}
