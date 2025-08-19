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
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	scheduledBackupTimestampFormat = "2006-01-02-150405"
	scheduleTagDaily               = "daily"
	scheduleTagWeekly              = "weekly"
	scheduleTagMonthly             = "monthly"
)

var (
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

	_, customErr := createScheduledBackupInitWF.Run(ctx, backupPolicy)

	if customErr != nil {
		createScheduledBackupInitWF.Status = workflows.WorkflowStatusFailed
		err2 := createScheduledBackupInitWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), customErr)
		if err2 != nil {
			createScheduledBackupInitWF.Logger.Errorf("Failed to update job status: %v", err2)
		}
		return customErr
	}
	createScheduledBackupInitWF.Status = workflows.WorkflowStatusCompleted
	err2 := createScheduledBackupInitWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	if err2 != nil {
		createScheduledBackupInitWF.Logger.Errorf("Failed to update job status: %v", err2)
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

	futures := make([]workflow.Future, len(volumes))
	for i, volume := range volumes {
		wf.Logger.Infof("Creating scheduled backup for volume: %s with backup policy: %s", volume.UUID, backupPolicy.UUID)
		futures[i] = workflow.ExecuteChildWorkflow(
			ctx,
			CreateScheduledBackupWorkflow,
			volume,
			backupPolicy,
		)
	}

	selector := workflow.NewSelector(ctx)
	for _, future := range futures {
		selector.AddFuture(future, func(f workflow.Future) {
			var result string
			err = f.Get(ctx, &result)
			if err != nil {
				wf.Logger.Errorf("Scheduled backup failed: %v", err)
			}
		})
	}

	for range futures {
		selector.Select(ctx)
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

	_, customErr := createScheduledBackupWF.Run(ctx, volume, backupPolicy)

	if customErr != nil {
		createScheduledBackupWF.Status = workflows.WorkflowStatusFailed
		err2 := createScheduledBackupWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), customErr)
		if err2 != nil {
			createScheduledBackupWF.Logger.Errorf("Failed to update job status: %v", err2)
		}
		return customErr
	}
	createScheduledBackupWF.Status = workflows.WorkflowStatusCompleted
	err2 := createScheduledBackupWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	if err2 != nil {
		createScheduledBackupWF.Logger.Errorf("Failed to update job status: %v", err2)
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
	wf.Logger.Infof("create scheduled backup workflow triggered for the backup policy: %s, volume: %s", backupPolicy.UUID, volume.UUID)

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
	backupActivities := &activities.BackupActivity{}
	scheduledBackupActivities := backgroundactivities.ScheduledBackupActivity{}

	defer func() {
		if err != nil {
			disconnectedCtx, _ := workflow.NewDisconnectedContext(ctx)
			rollbackManager.ExecuteRollback(disconnectedCtx, err)
		}
	}()

	var backupVault *datamodel.BackupVault
	err = workflow.ExecuteActivity(ctx, backupActivities.GetBackupVault, volume.DataProtection.BackupVaultID).Get(ctx, &backupVault)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	var backups []*datamodel.Backup
	timestamp := workflow.Now(ctx).Format(scheduledBackupTimestampFormat)
	if backupPolicy.DailyBackupsToKeep >= 2 {
		var backup *datamodel.Backup
		err = workflow.ExecuteActivity(ctx, scheduledBackupActivities.CreateScheduledBackup, volume, backupVault, timestamp, scheduleTagDaily).Get(ctx, &backup)
		if err != nil {
			return nil, workflows.ConvertToVSAError(err)
		}
		rollbackManager.AddActivity(backupActivities.DeleteBackup, backup.UUID)
		backups = append(backups, backup)
	}

	today := workflow.Now(ctx).Weekday()
	if backupPolicy.WeeklyBackupsToKeep > 0 && today == time.Weekday(scheduledWeeklyBackupDay) {
		var backup *datamodel.Backup
		err = workflow.ExecuteActivity(ctx, scheduledBackupActivities.CreateScheduledBackup, volume, backupVault, timestamp, scheduleTagWeekly).Get(ctx, &backup)
		if err != nil {
			return nil, workflows.ConvertToVSAError(err)
		}
		rollbackManager.AddActivity(backupActivities.DeleteBackup, backup.UUID)
		backups = append(backups, backup)
	}

	_, _, day := workflow.Now(ctx).Date()
	if backupPolicy.MonthlyBackupsToKeep > 0 && day == scheduledMonthlyBackupDay {
		var backup *datamodel.Backup
		err = workflow.ExecuteActivity(ctx, scheduledBackupActivities.CreateScheduledBackup, volume, backupVault, timestamp, scheduleTagMonthly).Get(ctx, &backup)
		if err != nil {
			return nil, workflows.ConvertToVSAError(err)
		}
		rollbackManager.AddActivity(backupActivities.DeleteBackup, backup.UUID)
		backups = append(backups, backup)
	}

	// Exit early if there are no backups to create (e.g., daily backup retention is 0 and no weekly or monthly backups are required)
	if len(backups) == 0 {
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
		DeploymentName: volume.Pool.DeploymentName},
	)

	var snapshotName string
	err = workflow.ExecuteActivity(ctx, scheduledBackupActivities.GenerateScheduledSnapshotName, timestamp).Get(ctx, &snapshotName)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	var objectStoreName string
	err = workflow.ExecuteActivity(ctx, backupActivities.GetObjStoreNameActivity, backupVault, volume).Get(ctx, &objectStoreName)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}
	var bucketDetails *datamodel.BucketDetails
	err = workflow.ExecuteActivity(ctx, backupActivities.GetBucketDetailsActivity, backupVault, volume).Get(ctx, &bucketDetails)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}
	bucketName := bucketDetails.BucketName

	cloudTarget := &common.CloudTarget{}
	err = workflow.ExecuteActivity(ctx, backupActivities.GetOrCreateObjectStore, node, objectStoreName, bucketName).Get(ctx, &cloudTarget)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	snapmirrorRelationship := &common.SnapmirrorRelationship{}
	var smSourcePath string
	err = workflow.ExecuteActivity(ctx, backupActivities.GetSmSourcePathActivity, volume).Get(ctx, &smSourcePath)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}
	smDestinationPath := fmt.Sprintf("%s:/objstore/%s", cloudTarget.Name, volume.UUID)
	SnapmirrorRelationshipParams := &common.SnapmirrorRelationshipParams{
		SourcePath:      smSourcePath,
		DestinationPath: smDestinationPath,
		SourceUUID:      nil,
		IsRestore:       false,
	}
	err = workflow.ExecuteActivity(ctx, backupActivities.SnapmirrorGetOrCreate, node, &SnapmirrorRelationshipParams).Get(ctx, &snapmirrorRelationship)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	snapshotResponse := &vsa.SnapshotProviderResponse{}
	err = workflow.ExecuteActivity(ctx, backupActivities.SnapshotCreate, node, volume.VolumeAttributes.ExternalUUID, snapshotName, workflows.BackupComment).Get(ctx, &snapshotResponse)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}
	rollbackManager.AddActivity(backupActivities.DeleteBackupSnapshot, node, volume.VolumeAttributes.ExternalUUID, snapshotName)

	err = workflow.ExecuteActivity(ctx, backupActivities.SnapmirrorTransfer, node, snapmirrorRelationship.UUID, snapshotName).Get(ctx, nil)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	done := false
	var status string
	for !done {
		err = workflow.ExecuteActivity(ctx, backupActivities.GetSnapmirrorTransferStatus, node, snapmirrorRelationship.UUID, snapshotName).Get(ctx, &status)
		if err != nil {
			return nil, workflows.ConvertToVSAError(err)
		}
		switch status {
		case activities.SmStatusTransferring:
			err = workflow.Sleep(ctx, workflows.Wait) // Wait before polling again
			if err != nil {
				return nil, workflows.ConvertToVSAError(fmt.Errorf("failed to sleep during snapmirror transfer polling: %w", err))
			}
		case activities.SmStatusSuccess:
			done = true
		case activities.SmStatusFailed:
			return nil, workflows.ConvertToVSAError(fmt.Errorf("snapmirror transfer failed for snapshot %s with status: %s", snapshotName, status))
		}
	}

	for _, backup := range backups {
		backup.Attributes.SnapshotName = snapshotName
		backup.Attributes.SnapshotID = snapshotResponse.ExternalUUID
		backup.Attributes.SnapshotCreationTime = workflow.Now(ctx).String()
		backup.Attributes.BucketName = bucketName
		backup.Attributes.ServiceAccountName = bucketDetails.ServiceAccountName
		if snapmirrorRelationship != nil && snapmirrorRelationship.DestinationUUID != nil {
			backup.Attributes.EndpointUUID = *snapmirrorRelationship.DestinationUUID
		}

		err = workflow.ExecuteActivity(ctx, backupActivities.FinishBackup, backup).Get(ctx, nil)
		if err != nil {
			return nil, workflows.ConvertToVSAError(err)
		}
	}
	rollbackManager.AddActivity(backupActivities.DeleteSnapshotFromObjectStore, node, cloudTarget.UUID, backups[0].Attributes.EndpointUUID, backups[0].Attributes.SnapshotID)

	err = workflow.ExecuteActivity(ctx, scheduledBackupActivities.HydrateCreatedBackupsToCCFE, volume, backups, backupVault.Name).Get(ctx, nil)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteChildWorkflow(ctx, DeleteScheduledBackupWorkflow, volume, backupPolicy).Get(ctx, nil)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

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

	_, customErr := deleteScheduledBackupWF.Run(ctx, volume, backupPolicy)

	if customErr != nil {
		deleteScheduledBackupWF.Status = workflows.WorkflowStatusFailed
		err2 := deleteScheduledBackupWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), customErr)
		if err2 != nil {
			deleteScheduledBackupWF.Logger.Errorf("Failed to update job status: %v", err2)
		}
		return customErr
	}
	deleteScheduledBackupWF.Status = workflows.WorkflowStatusCompleted
	err2 := deleteScheduledBackupWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	if err2 != nil {
		deleteScheduledBackupWF.Logger.Errorf("Failed to update job status: %v", err2)
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
			var ontapAsyncResponse *vsa.OntapAsyncResponse
			err = workflow.ExecuteActivity(ctx, backupActivities.DeleteSnapshotFromObjectStore, node, objStore.UUID, backup.Attributes.EndpointUUID, backup.Attributes.SnapshotID).Get(ctx, &ontapAsyncResponse)
			if err != nil {
				return nil, workflows.ConvertToVSAError(err)
			}

			err = workflows.WaitForONTAPJob(ctx, ontapAsyncResponse, node, time.Minute*10)
			if err != nil {
				return nil, workflows.ConvertToVSAError(fmt.Errorf("failed to delete cloud endpoint: %w", err))
			}
		}
		err = workflow.ExecuteActivity(ctx, backupActivities.DeleteBackup, backup.UUID).Get(ctx, nil)
		if err != nil {
			wf.Logger.Errorf("Failed to delete backup %s: %v", backup.Name, err)
			return nil, workflows.ConvertToVSAError(fmt.Errorf("failed to delete backup %s: %w", backup.Name, err))
		}
	}

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
		StartToCloseTimeout: 10 * time.Second,
	})

	var createdJob *datamodel.Job
	err := workflow.ExecuteActivity(ctx, commonActivities.CreateJob, job).Get(ctx, &createdJob)
	if err != nil {
		logger.Errorf("Failed to create job: %v", err)
		return nil, workflows.ConvertToVSAError(err)
	}
	return createdJob, nil
}
