package backgroundworkflows

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	scheduledBackupTimestampFormat = "2006-01-02-150405"
)

var (
	hydrationEnabled          = env.GetBool("GCP_HYDRATE_ENABLED", true)
	scheduledWeeklyBackupDay  = env.GetInt("SCHEDULED_WEEKLY_BACKUP_DAY", 1)  // Default to Monday (0=Sunday, 1=Monday, ..., 6=Saturday)
	scheduledMonthlyBackupDay = env.GetInt("SCHEDULED_MONTHLY_BACKUP_DAY", 1) // Default to 1st day of the month
)

type baseScheduledBackupWorkflow struct {
	workflows.BaseWorkflow
}

type createScheduledBackupInitWorkflow struct {
	baseScheduledBackupWorkflow
}

// Enforcing workflows.WorkflowInterface interface on all the scheduled backup workflows
var (
	_ workflows.WorkflowInterface = &createScheduledBackupInitWorkflow{}
	_ workflows.WorkflowInterface = &createScheduledBackupWorkflow{}
	_ workflows.WorkflowInterface = &deleteScheduledBackupWorkflow{}
)

// CreateScheduledBackupInitWorkflow initializes the scheduled backup workflow for a given backup policy.
func CreateScheduledBackupInitWorkflow(ctx workflow.Context, backupPolicy *datamodel.BackupPolicy) error {
	createScheduledBackupInitWF := new(createScheduledBackupInitWorkflow)
	createdJob, err := createScheduledBackupInitWF.CreateJob(
		ctx, backupPolicy.AccountID, backupPolicy.Name, string(models.JobTypeInitCreateScheduledBackup))
	if err != nil {
		return err
	}

	err = createScheduledBackupInitWF.Setup(ctx, createdJob)
	if err != nil {
		return err
	}
	createScheduledBackupInitWF.Status = workflows.WorkflowStatusRunning

	_, workflowErr := createScheduledBackupInitWF.Run(ctx, backupPolicy)
	if workflowErr != nil {
		createScheduledBackupInitWF.Status = workflows.WorkflowStatusFailed
		err2 := createScheduledBackupInitWF.UpdateJobStatus(ctx, string(models.JobsStateERROR), workflowErr)
		if err2 != nil {
			createScheduledBackupInitWF.Logger.Errorf("Failed to update job status: %v", err2)
		}
		return workflowErr
	}
	createScheduledBackupInitWF.Status = workflows.WorkflowStatusCompleted
	err2 := createScheduledBackupInitWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	if err2 != nil {
		createScheduledBackupInitWF.Logger.Errorf("Failed to update job status: %v", err2)
		return err2
	}
	return nil
}

// Setup initializes the workflow with necessary parameters and sets up a query handler for status.
func (wf *createScheduledBackupInitWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	job := input.(*datamodel.Job)
	wf.ID = job.UUID
	wf.Status = workflows.WorkflowStatusCreated
	ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{"workflowID": wf.ID})
	logger := util.GetLogger(ctx)
	wf.Logger = logger

	return workflow.SetQueryHandler(ctx, "status", func() (*workflows.WorkflowStatus, error) {
		return &workflows.WorkflowStatus{
			ID:     wf.ID,
			Status: wf.Status,
		}, nil
	})
}

// Run executes the scheduled backup workflow for the given backup policy.
func (wf *createScheduledBackupInitWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	backupPolicy := args[0].(*datamodel.BackupPolicy)
	wf.Logger.Infof("scheduled backup workflow triggered for the backup policy: %s", backupPolicy.UUID)

	retryPolicy, err := workflows.PopulateRetryPolicyParams()
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
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
	rollbackManager := common.NewRollbackManager()
	scheduledBackupActivities := backgroundactivities.ScheduledBackupActivity{}

	defer func() {
		if err != nil {
			disconnectedCtx, _ := workflow.NewDisconnectedContext(ctx)
			rollbackManager.ExecuteRollback(disconnectedCtx, err)
		}
	}()

	var volumes []*datamodel.Volume
	err = workflow.ExecuteActivity(ctx, scheduledBackupActivities.GetVolumesByBackupPolicyUUID, backupPolicy.UUID, backupPolicy.AccountID).Get(ctx, &volumes)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	ctx = workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		ParentClosePolicy: enums.PARENT_CLOSE_POLICY_ABANDON,
	})
	for _, volume := range volumes {
		wf.Logger.Infof("Creating scheduled backup for volume: %s with backup policy: %s", volume.UUID, backupPolicy.UUID)
		_ = workflow.ExecuteChildWorkflow(
			ctx,
			CreateScheduledBackupWorkflow,
			volume,
			backupPolicy,
		)
	}
	return nil, nil
}

type createScheduledBackupWorkflow struct {
	baseScheduledBackupWorkflow
}

// CreateScheduledBackupWorkflow creates a scheduled backup for a given volume and backup policy.
func CreateScheduledBackupWorkflow(ctx workflow.Context, volume *datamodel.Volume, backupPolicy *datamodel.BackupPolicy) error {
	createScheduledBackupWF := new(createScheduledBackupWorkflow)
	createdJob, err := createScheduledBackupWF.CreateJob(
		ctx, backupPolicy.AccountID, volume.Name, string(models.JobTypeCreateScheduledBackup))
	if err != nil {
		return workflows.ConvertToVSAError(err)
	}

	err = createScheduledBackupWF.Setup(ctx, createdJob)
	if err != nil {
		return err
	}
	createScheduledBackupWF.Status = workflows.WorkflowStatusRunning

	_, workflowErr := createScheduledBackupWF.Run(ctx, volume, backupPolicy, createdJob)
	if workflowErr != nil {
		if workflow.IsContinueAsNewError(workflowErr.OriginalErr) {
			return workflowErr
		}
		createScheduledBackupWF.Status = workflows.WorkflowStatusFailed
		err2 := createScheduledBackupWF.UpdateJobStatus(ctx, string(models.JobsStateERROR), workflowErr)
		if err2 != nil {
			createScheduledBackupWF.Logger.Errorf("Failed to update job status: %v", err2)
		}
		return workflowErr
	}
	createScheduledBackupWF.Status = workflows.WorkflowStatusCompleted
	err2 := createScheduledBackupWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	if err2 != nil {
		createScheduledBackupWF.Logger.Errorf("Failed to update job status: %v", err2)
		return err2
	}
	return nil
}

// CreateScheduledBackupWorkflowWithContext processes scheduled backup with context for continuation
func CreateScheduledBackupWorkflowWithContext(ctx workflow.Context, scheduledBackupContext *activities.BackupActivitiesContext) error {
	createScheduledBackupWF := new(createScheduledBackupWorkflow)
	createdJob := scheduledBackupContext.ScheduledBackupParams.Job
	err := createScheduledBackupWF.Setup(ctx, createdJob)
	if err != nil {
		return err
	}
	createScheduledBackupWF.Status = workflows.WorkflowStatusRunning

	_, workflowErr := createScheduledBackupWF.RunScheduledBackupWithContext(ctx, scheduledBackupContext)
	if workflowErr != nil {
		if workflow.IsContinueAsNewError(workflowErr.OriginalErr) {
			return workflowErr
		}
		createScheduledBackupWF.Status = workflows.WorkflowStatusFailed
		err2 := createScheduledBackupWF.UpdateJobStatus(ctx, string(models.JobsStateERROR), workflowErr)
		if err2 != nil {
			createScheduledBackupWF.Logger.Errorf("Failed to update job status: %v", err2)
		}
		return workflowErr
	}

	createScheduledBackupWF.Status = workflows.WorkflowStatusCompleted
	err2 := createScheduledBackupWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	if err2 != nil {
		createScheduledBackupWF.Logger.Errorf("Failed to update job status: %v", err2)
		return err2
	}
	return nil
}

// Setup initializes the workflow with necessary parameters and sets up a query handler for status.
func (wf *createScheduledBackupWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	job := input.(*datamodel.Job)
	wf.ID = job.UUID
	wf.Status = workflows.WorkflowStatusCreated
	ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{"workflowID": wf.ID})
	logger := util.GetLogger(ctx)
	wf.Logger = logger

	return workflow.SetQueryHandler(ctx, "status", func() (*workflows.WorkflowStatus, error) {
		return &workflows.WorkflowStatus{
			ID:     wf.ID,
			Status: wf.Status,
		}, nil
	})
}

// Run executes the scheduled backup workflow for the given volume and backup policy.
func (wf *createScheduledBackupWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	volume := args[0].(*datamodel.Volume)
	backupPolicy := args[1].(*datamodel.BackupPolicy)
	job := args[2].(*datamodel.Job)
	scheduledBackupActivitiesContext := &activities.BackupActivitiesContext{
		BackupWorkflowInit: &activities.BackupWorkflowInput{
			Volume: volume,
		},
		ScheduledBackupParams: &activities.ScheduledBackupParams{
			BackupPolicy: backupPolicy,
			Job:          job,
		},
	}
	wf.Logger.Infof("create scheduled backup workflow triggered for the backup policy: %s, volume: %s", backupPolicy.UUID, volume.UUID)
	return wf.RunScheduledBackupWithContext(ctx, scheduledBackupActivitiesContext)
}

// RunScheduledBackupWithContext executes the scheduled backup workflow with context for continuation
func (wf *createScheduledBackupWorkflow) RunScheduledBackupWithContext(ctx workflow.Context, scheduledBackupContext *activities.BackupActivitiesContext) (interface{}, *vsaerrors.CustomError) {
	volume := scheduledBackupContext.BackupWorkflowInit.Volume
	backupPolicy := scheduledBackupContext.ScheduledBackupParams.BackupPolicy
	retryPolicy, err := workflows.PopulateRetryPolicyParams()
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
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
	var backups []*datamodel.Backup
	preTransferRollbackManager := common.NewRollbackManager()
	postTransferRollbackManager := common.NewRollbackManager()
	backupActivities := &activities.BackupActivity{}
	scheduledBackupActivities := backgroundactivities.ScheduledBackupActivity{}

	var preTransferErr, postTransferErr error
	defer func() {
		if postTransferErr != nil {
			disconnectedCtx, _ := workflow.NewDisconnectedContext(ctx)
			postTransferRollbackManager.ExecuteRollback(disconnectedCtx, postTransferErr)
		} else if preTransferErr != nil {
			disconnectedCtx, _ := workflow.NewDisconnectedContext(ctx)
			preTransferRollbackManager.ExecuteRollback(disconnectedCtx, preTransferErr)
		}
	}()
	if isContinuation {
		wf.Logger.Info("Resuming backup workflow from continuation",
			"workflowID", wf.ID,
			"continuedFromRunID", info.OriginalRunID,
			"snapshotName", scheduledBackupContext.SnapshotName,
			"transferStatus", scheduledBackupContext.TransferStatus)
	} else {
		var backupVault *datamodel.BackupVault
		preTransferErr = workflow.ExecuteActivity(ctx, backupActivities.GetBackupVault, volume.DataProtection.BackupVaultID).Get(ctx, &backupVault)
		if preTransferErr != nil {
			return nil, workflows.ConvertToVSAError(preTransferErr)
		}
		scheduledBackupContext.BackupWorkflowInit.BackupVault = backupVault
		backupPolicy := scheduledBackupContext.ScheduledBackupParams.BackupPolicy
		timestamp := workflow.Now(ctx).Format(scheduledBackupTimestampFormat)
		if backupPolicy.DailyBackupsToKeep >= 2 {
			var backup *datamodel.Backup
			preTransferErr = workflow.ExecuteActivity(ctx, scheduledBackupActivities.CreateScheduledBackup, volume, backupVault, timestamp, common.ScheduleTagDaily).Get(ctx, &backup)
			if preTransferErr != nil {
				return nil, workflows.ConvertToVSAError(preTransferErr)
			}
			preTransferRollbackManager.AddActivity(backupActivities.DeleteBackup, backup.UUID)
			backups = append(backups, backup)
		}

		today := workflow.Now(ctx).Weekday()
		if backupPolicy.WeeklyBackupsToKeep > 0 && today == time.Weekday(scheduledWeeklyBackupDay) {
			var backup *datamodel.Backup
			preTransferErr = workflow.ExecuteActivity(ctx, scheduledBackupActivities.CreateScheduledBackup, volume, backupVault, timestamp, common.ScheduleTagWeekly).Get(ctx, &backup)
			if preTransferErr != nil {
				return nil, workflows.ConvertToVSAError(preTransferErr)
			}
			preTransferRollbackManager.AddActivity(backupActivities.DeleteBackup, backup.UUID)
			backups = append(backups, backup)
		}

		_, _, day := workflow.Now(ctx).Date()
		if backupPolicy.MonthlyBackupsToKeep > 0 && day == scheduledMonthlyBackupDay {
			var backup *datamodel.Backup
			preTransferErr = workflow.ExecuteActivity(ctx, scheduledBackupActivities.CreateScheduledBackup, volume, backupVault, timestamp, common.ScheduleTagMonthly).Get(ctx, &backup)
			if preTransferErr != nil {
				return nil, workflows.ConvertToVSAError(preTransferErr)
			}
			preTransferRollbackManager.AddActivity(backupActivities.DeleteBackup, backup.UUID)
			backups = append(backups, backup)
		}

		// Exit early if there are no backups to create (e.g., daily backup retention is 0 and no weekly or monthly backups are required)
		if len(backups) == 0 {
			return nil, nil
		}
		scheduledBackupContext.ScheduledBackupParams.Backups = backups

		var dbNodes []*datamodel.Node
		preTransferErr = workflow.ExecuteActivity(ctx, activities.CommonActivities.GetNode, &volume.PoolID).Get(ctx, &dbNodes)
		if preTransferErr != nil {
			return nil, workflows.ConvertToVSAError(preTransferErr)
		}

		node := hyperscaler.CreateNodeForProvider(hyperscaler.NodeProviderInput{
			Nodes:          dbNodes,
			Password:       volume.Pool.PoolCredentials.Password,
			SecretID:       volume.Pool.PoolCredentials.SecretID,
			DeploymentName: volume.Pool.DeploymentName,
			CertificateID:  volume.Pool.PoolCredentials.CertificateID,
			AuthType:       volume.Pool.PoolCredentials.AuthType,
		})

		scheduledBackupContext.Node = node
		var objectStoreName string
		objectStoreName, preTransferErr = activities.GetObjStoreName(backupVault, volume)
		if preTransferErr != nil {
			return nil, workflows.ConvertToVSAError(preTransferErr)
		}
		var bucketDetails *datamodel.BucketDetails
		bucketDetails, preTransferErr = activities.GetBucketDetails(backupVault, volume)
		if preTransferErr != nil {
			return nil, workflows.ConvertToVSAError(preTransferErr)
		}

		scheduledBackupContext.BucketDetails = bucketDetails
		scheduledBackupContext.ObjStoreName = objectStoreName

		cloudTarget := &common.CloudTarget{}
		preTransferErr = workflow.ExecuteActivity(ctx, backupActivities.GetOrCreateObjectStore, node, objectStoreName, bucketDetails.BucketName).Get(ctx, &cloudTarget)
		if preTransferErr != nil {
			return nil, workflows.ConvertToVSAError(preTransferErr)
		}
		scheduledBackupContext.ObjStore = cloudTarget
		snapmirrorRelationship := &common.SnapmirrorRelationship{}
		smSourcePath := activities.GetSmSourcePath(volume)
		smDestinationPath := fmt.Sprintf("%s:/objstore/%s", cloudTarget.Name, volume.UUID)
		SnapmirrorRelationshipParams := &common.SnapmirrorRelationshipParams{
			SourcePath:      smSourcePath,
			DestinationPath: smDestinationPath,
			SourceUUID:      nil,
			IsRestore:       false,
		}
		scheduledBackupContext.SnapmirrorRelationship = snapmirrorRelationship
		preTransferErr = workflow.ExecuteActivity(ctx, backupActivities.SnapmirrorGetOrCreate, node, &SnapmirrorRelationshipParams).Get(ctx, &snapmirrorRelationship)
		if preTransferErr != nil {
			return nil, workflows.ConvertToVSAError(preTransferErr)
		}
		if snapmirrorRelationship.DestinationUUID == nil {
			preTransferErr = vsaerrors.NewVCPError(vsaerrors.ErrResourceEmptyError, fmt.Errorf("DestinationUUID not found in snapmirror relationship"))
			return nil, workflows.ConvertToVSAError(preTransferErr)
		}

		var snapshotName string
		preTransferErr = workflow.ExecuteActivity(ctx, scheduledBackupActivities.GenerateScheduledSnapshotName, timestamp).Get(ctx, &snapshotName)
		if preTransferErr != nil {
			return nil, workflows.ConvertToVSAError(preTransferErr)
		}
		scheduledBackupContext.SnapshotName = snapshotName
		var dbSnapshot *datamodel.Snapshot
		preTransferErr = workflow.ExecuteActivity(ctx, scheduledBackupActivities.CreateBackupSnapshotInDB, volume, snapshotName).Get(ctx, &dbSnapshot)
		if preTransferErr != nil {
			return nil, workflows.ConvertToVSAError(preTransferErr)
		}
		scheduledBackupContext.DbSnapshot = dbSnapshot
		preTransferRollbackManager.AddActivity(scheduledBackupActivities.DeleteBackupSnapshotInDB, dbSnapshot.UUID)

		ontapSnapshot := &vsa.SnapshotProviderResponse{}
		preTransferErr = workflow.ExecuteActivity(ctx, backupActivities.SnapshotCreate, node, volume.VolumeAttributes.ExternalUUID, snapshotName, workflows.BackupComment).Get(ctx, &ontapSnapshot)
		if preTransferErr != nil {
			return nil, workflows.ConvertToVSAError(preTransferErr)
		}
		scheduledBackupContext.ScheduledBackupParams.OntapSnapshot = ontapSnapshot

		preTransferRollbackManager.AddActivity(backupActivities.DeleteBackupSnapshot, node, ontapSnapshot.ExternalUUID, volume.VolumeAttributes.ExternalUUID)

		preTransferErr = workflow.ExecuteActivity(ctx, scheduledBackupActivities.UpdateBackupSnapshotInDB, dbSnapshot, ontapSnapshot).Get(ctx, &dbSnapshot)
		if preTransferErr != nil {
			return nil, workflows.ConvertToVSAError(preTransferErr)
		}

		preTransferErr = workflow.ExecuteActivity(ctx, backupActivities.SnapmirrorTransfer, node, snapmirrorRelationship.UUID, snapshotName).Get(ctx, nil)
		if preTransferErr != nil {
			return nil, workflows.ConvertToVSAError(preTransferErr)
		}
	}

	err = wf.PollTransferStatusWithContinueAsNew(ctx, scheduledBackupContext)
	if err != nil {
		if !workflow.IsContinueAsNewError(err) {
			preTransferErr = err
		}
		return nil, workflows.ConvertToVSAError(err)
	}

	backups = scheduledBackupContext.ScheduledBackupParams.Backups
	for _, backup := range backups {
		backup.Attributes.SnapshotName = scheduledBackupContext.SnapshotName
		backup.Attributes.SnapshotID = scheduledBackupContext.ScheduledBackupParams.OntapSnapshot.ExternalUUID
		backup.Attributes.VolumeName = volume.Name
		backup.Attributes.SourceVolumeZone = volume.Pool.PoolAttributes.PrimaryZone
		backup.Attributes.IsRegionalHA = volume.Pool.PoolAttributes.IsRegionalHA
		backup.Attributes.SnapshotCreationTime = workflow.Now(ctx).String()
		backup.Attributes.BucketName = scheduledBackupContext.BucketDetails.BucketName
		backup.Attributes.ServiceAccountName = scheduledBackupContext.BucketDetails.ServiceAccountName
		backup.Attributes.AccountIdentifier = volume.Account.Name
		backup.Attributes.BackupPolicyName = backupPolicy.Name
		backup.Attributes.Protocols = volume.VolumeAttributes.Protocols
		backup.Attributes.ObjectStoreUUID = scheduledBackupContext.ObjStore.UUID
		backup.Attributes.EndpointUUID = *scheduledBackupContext.SnapmirrorRelationship.DestinationUUID
		backup.AssetMetadata = &datamodel.AssetMetadata{
			ChildAssets: []datamodel.ChildAsset{
				{
					AssetType:  common.BackupAssetType,
					AssetNames: []string{fmt.Sprintf("//storage.googleapis.com/%s", backup.Attributes.BucketName)},
				},
			},
		}

		postTransferRollbackManager.AddActivity(backupActivities.UpdateBackupError, backup)
		postTransferErr = workflow.ExecuteActivity(ctx, backupActivities.FinishBackup, backup).Get(ctx, nil)
		if postTransferErr != nil {
			return nil, workflows.ConvertToVSAError(postTransferErr)
		}
	}
	// Update ConstituentCount for a backup from Volume
	if scheduledBackupContext.BackupWorkflowInit.Volume.LargeVolumeAttributes != nil && scheduledBackupContext.BackupWorkflowInit.Volume.LargeVolumeAttributes.LargeCapacity {
		postTransferErr = workflow.ExecuteActivity(ctx, backupActivities.UpdateConstituentCountForBackup, scheduledBackupContext).Get(ctx, &scheduledBackupContext)
		if postTransferErr != nil {
			return nil, workflows.ConvertToVSAError(postTransferErr)
		}
	}

	backup := backups[len(backups)-1]
	var objectStoreEndpointInfo vsa.SmObjectStoreEndpointt
	err = workflow.ExecuteActivity(ctx, backupActivities.GetObjectStoreEndpointInfo, scheduledBackupContext.Node, scheduledBackupContext.ObjStore.UUID, backup.Attributes.EndpointUUID).Get(ctx, &objectStoreEndpointInfo)
	if err != nil {
		wf.Logger.Errorf("Failed to get object store endpoint info for volume %s: %v", volume.Name, err)
	} else {
		backup.LatestLogicalBackupSize = *objectStoreEndpointInfo.LogicalSize
	}

	var objectStoreSnapshot *vsa.SmObjectStoreEndpointSnapshot
	err = workflow.ExecuteActivity(ctx, backupActivities.GetSnapshotFromObjectStore, scheduledBackupContext.Node, scheduledBackupContext.ObjStore.UUID, backup.Attributes.EndpointUUID, backup.Attributes.SnapshotID).Get(ctx, &objectStoreSnapshot)
	if err != nil {
		wf.Logger.Errorf("Failed to get snapshot from object store for volume %s: %v", volume.Name, err)
	} else {
		if objectStoreSnapshot.LogicalSize != nil {
			backup.SizeInBytes = *objectStoreSnapshot.LogicalSize
		} else {
			wf.Logger.Errorf("Logical size is nil for snapshot %s in object store for volume %s", backup.Attributes.SnapshotID, volume.Name)
			backup.SizeInBytes = 0
		}
	}

	// Update backup size fields in both backup and volume tables
	err = workflow.ExecuteActivity(ctx, scheduledBackupActivities.UpdateBackupSize, backup, volume).Get(ctx, nil)
	if err != nil {
		wf.Logger.Errorf("Failed to update backup size fields for volume %s: %v", volume.Name, err)
	}

	if hydrationEnabled {
		location := utils.GetLocation(*scheduledBackupContext.DbSnapshot)
		err = workflow.ExecuteActivity(ctx, backupActivities.HydrateSnapshotToCCFEActivity,
			scheduledBackupContext.DbSnapshot,
			volume.Name,
			location,
			volume.Account.Name).Get(ctx, nil)
		if err != nil {
			// Log the error but don't fail the entire workflow
			wf.Logger.Errorf("Failed to hydrate snapshot to CCFE for backup %s: %v", backup.Name, err)
		}

		postTransferErr = workflow.ExecuteActivity(ctx, scheduledBackupActivities.HydrateCreatedBackupsToCCFE, volume, backups, scheduledBackupContext.BackupWorkflowInit.BackupVault.Name).Get(ctx, nil)
		if postTransferErr != nil {
			return nil, workflows.ConvertToVSAError(postTransferErr)
		}
	}

	// Create BackupMetadata entry if this is the first backup for the volume
	err = workflow.ExecuteActivity(ctx, backupActivities.CreateBackupMetadataIfFirstBackupActivity, volume).Get(ctx, nil)
	if err != nil {
		// Log the error but don't fail the entire backup workflow
		wf.Logger.Errorf("Failed to create BackupMetadata for volume %s: %v", volume.UUID, err)
	}

	ctx = workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		ParentClosePolicy: enums.PARENT_CLOSE_POLICY_ABANDON,
	})
	_ = workflow.ExecuteChildWorkflow(ctx, DeleteScheduledBackupWorkflow, volume, scheduledBackupContext.ScheduledBackupParams.BackupPolicy)
	return nil, nil
}

type deleteScheduledBackupWorkflow struct {
	baseScheduledBackupWorkflow
}

// DeleteScheduledBackupWorkflow removes older scheduled backups for a volume and backup policy according to daily, weekly, and monthly retention limits.
func DeleteScheduledBackupWorkflow(ctx workflow.Context, volume *datamodel.Volume, backupPolicy *datamodel.BackupPolicy) error {
	deleteScheduledBackupWF := new(deleteScheduledBackupWorkflow)
	createdJob, err := deleteScheduledBackupWF.CreateJob(
		ctx, backupPolicy.AccountID, volume.Name, string(models.JobTypeDeleteScheduledBackup))
	if err != nil {
		return err
	}

	err = deleteScheduledBackupWF.Setup(ctx, createdJob)
	if err != nil {
		return err
	}
	deleteScheduledBackupWF.Status = workflows.WorkflowStatusRunning

	_, workflowErr := deleteScheduledBackupWF.Run(ctx, volume, backupPolicy)
	if workflowErr != nil {
		deleteScheduledBackupWF.Status = workflows.WorkflowStatusFailed
		err2 := deleteScheduledBackupWF.UpdateJobStatus(ctx, string(models.JobsStateERROR), workflowErr)
		if err2 != nil {
			deleteScheduledBackupWF.Logger.Errorf("Failed to update job status: %v", err2)
		}
		return workflowErr
	}
	deleteScheduledBackupWF.Status = workflows.WorkflowStatusCompleted
	err2 := deleteScheduledBackupWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	if err2 != nil {
		deleteScheduledBackupWF.Logger.Errorf("Failed to update job status: %v", err2)
		return err2
	}
	return nil
}

// Setup initializes the workflow with necessary parameters and sets up a query handler for status.
func (wf *deleteScheduledBackupWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	job := input.(*datamodel.Job)
	wf.ID = job.UUID
	wf.Status = workflows.WorkflowStatusCreated
	ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{"workflowID": wf.ID})
	logger := util.GetLogger(ctx)
	wf.Logger = logger

	return workflow.SetQueryHandler(ctx, "status", func() (*workflows.WorkflowStatus, error) {
		return &workflows.WorkflowStatus{
			ID:     wf.ID,
			Status: wf.Status,
		}, nil
	})
}

// Run executes the scheduled backup deletion workflow for the given volume and backup policy.
func (wf *deleteScheduledBackupWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	volume := args[0].(*datamodel.Volume)
	backupPolicy := args[1].(*datamodel.BackupPolicy)
	wf.Logger.Infof("delete scheduled backup workflow triggered for the backup policy: %s, volume: %s", backupPolicy.UUID, volume.UUID)

	retryPolicy, err := workflows.PopulateRetryPolicyParams()
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
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

	rollbackManager := common.NewRollbackManager()
	defer func() {
		if err != nil {
			disconnectedCtx, _ := workflow.NewDisconnectedContext(ctx)
			rollbackManager.ExecuteRollback(disconnectedCtx, err)
		}
	}()

	scheduledBackupActivities := backgroundactivities.ScheduledBackupActivity{}
	backupActivities := &activities.BackupActivity{}

	var backupVault *datamodel.BackupVault
	err = workflow.ExecuteActivity(ctx, backupActivities.GetBackupVault, volume.DataProtection.BackupVaultID).Get(ctx, &backupVault)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	var backupToBeDeleted []*datamodel.Backup
	err = workflow.ExecuteActivity(ctx, scheduledBackupActivities.FetchScheduledBackupForDeletion, volume, backupPolicy).Get(ctx, &backupToBeDeleted)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	// Exit early if there are no backups to delete
	if len(backupToBeDeleted) == 0 {
		return nil, nil
	}

	var dbNodes []*datamodel.Node
	err = workflow.ExecuteActivity(ctx, activities.CommonActivities.GetNode, &volume.PoolID).Get(ctx, &dbNodes)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}
	node := hyperscaler.CreateNodeForProvider(hyperscaler.NodeProviderInput{
		Nodes:          dbNodes,
		Password:       volume.Pool.PoolCredentials.Password,
		SecretID:       volume.Pool.PoolCredentials.SecretID,
		DeploymentName: volume.Pool.DeploymentName,
		CertificateID:  volume.Pool.PoolCredentials.CertificateID,
		AuthType:       volume.Pool.PoolCredentials.AuthType},
	)

	var objectStoreName string
	err = workflow.ExecuteActivity(ctx, backupActivities.GetObjStoreNameActivity, backupVault, volume).Get(ctx, &objectStoreName)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	var objStore *common.CloudTarget
	err = workflow.ExecuteActivity(ctx, backupActivities.GetObjectStore, node, objectStoreName).Get(ctx, &objStore)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	for _, backup := range backupToBeDeleted {
		var isSharedBackup bool
		err = workflow.ExecuteActivity(ctx, backupActivities.IsBackupShared, backup).Get(ctx, &isSharedBackup)
		if err != nil {
			wf.Logger.Errorf("Failed to check if backup %s is shared: %v", backup.Name, err)
			return nil, workflows.ConvertToVSAError(err)
		}
		if !isSharedBackup {
			rollbackManager.AddActivity(backupActivities.UpdateBackupError, backup)
			var ontapAsyncResponse *vsa.OntapAsyncResponse
			err = workflow.ExecuteActivity(ctx, backupActivities.DeleteSnapshotFromObjectStore, node, objStore.UUID, backup.Attributes.EndpointUUID, backup.Attributes.SnapshotID).Get(ctx, &ontapAsyncResponse)
			if err != nil {
				return nil, workflows.ConvertToVSAError(err)
			}

			err = workflows.WaitForONTAPJob(ctx, ontapAsyncResponse, node, time.Minute*120)
			if err != nil {
				return nil, workflows.ConvertToVSAError(fmt.Errorf("failed to delete cloud endpoint: %w", err))
			}
		} else {
			rollbackManager.AddActivity(backupActivities.MarkBackupAvailable, backup)
		}

		err = workflow.ExecuteActivity(ctx, backupActivities.DeleteBackup, backup.UUID).Get(ctx, nil)
		if err != nil {
			wf.Logger.Errorf("Failed to delete backup %s: %v", backup.Name, err)
			return nil, workflows.ConvertToVSAError(fmt.Errorf("failed to delete backup %s: %w", backup.Name, err))
		}

		if hydrationEnabled {
			// Hydrate snapshot deletions to CCFE for each deleted backup
			var snapshot *datamodel.Snapshot
			// snapshotErr is used instead of err to avoid the execution of rollbackManager for snapshot hydration errors
			snapshotHydrationErr := workflow.ExecuteActivity(ctx, scheduledBackupActivities.GetSnapshotByNameAndVolumeID, backup.Attributes.SnapshotName, volume.AccountID, volume.ID).Get(ctx, &snapshot)
			if snapshotHydrationErr != nil {
				// Log the error but don't fail the workflow
				wf.Logger.Errorf("Failed to get snapshot from database to hydrate deletion %v", snapshotHydrationErr)
			} else {
				// Delete snapshot entry from the database
				snapshotHydrationErr = workflow.ExecuteActivity(ctx, scheduledBackupActivities.DeleteBackupSnapshotInDB, snapshot.UUID).Get(ctx, nil)
				if snapshotHydrationErr != nil {
					wf.Logger.Errorf("Failed to delete snapshot in the database %s: %v", snapshot.Name, snapshotHydrationErr)
				}

				// Hydrate snapshot deletion to CCFE
				location := utils.GetLocation(*snapshot)
				snapshot.State = models.LifeCycleStateDeleted
				snapshot.StateDetails = models.LifeCycleStateDeletedDetails
				snapshotHydrationErr = workflow.ExecuteActivity(ctx, backupActivities.HydrateSnapshotDeletionToCCFEActivity,
					snapshot,
					volume.Name,
					location,
					volume.Account.Name).Get(ctx, nil)
				if snapshotHydrationErr != nil {
					// Log the error but don't fail the entire workflow
					wf.Logger.Errorf("Failed to hydrate snapshot deletion to CCFE for backup %s: %v", backup.Name, snapshotHydrationErr)
				}
			}
		}
	}
	// Hydrate all deleted backups to CCFE after processing all backups
	err = workflow.ExecuteActivity(ctx, scheduledBackupActivities.HydrateDeletedBackupsToCCFE, volume, backupToBeDeleted, backupVault.Name).Get(ctx, nil)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	return nil, nil
}

func (wf *baseScheduledBackupWorkflow) CreateJob(ctx workflow.Context, accountID int64, resourceName, jobType string) (*datamodel.Job, error) {
	logger := util.GetLogger(ctx)

	// The job state is set to PROCESSING here because the workflow itself is creating the job
	job := &datamodel.Job{
		AccountID:    sql.NullInt64{Int64: accountID, Valid: true},
		ResourceName: resourceName,
		Type:         jobType,
		State:        string(models.JobsStatePROCESSING),
	}

	commonActivities := activities.CommonActivities{}
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 60 * time.Second,
	})

	var createdJob *datamodel.Job
	err := workflow.ExecuteActivity(ctx, commonActivities.CreateJob, job).Get(ctx, &createdJob)
	if err != nil {
		logger.Errorf("Failed to create job: %v", err)
		return nil, workflows.ConvertToVSAError(err)
	}
	return createdJob, nil
}

func (wf *createScheduledBackupWorkflow) PollTransferStatusWithContinueAsNew(ctx workflow.Context, backupActivitiesContext *activities.BackupActivitiesContext) error {
	return workflows.PollTransferStatusWithContinueAsNewCommon(ctx, backupActivitiesContext, CreateScheduledBackupWorkflowWithContext, backupActivitiesContext)
}
