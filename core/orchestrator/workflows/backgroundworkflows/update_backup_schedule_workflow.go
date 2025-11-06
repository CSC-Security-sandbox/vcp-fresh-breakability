package backgroundworkflows

import (
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	databaseUtils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// updateBackupScheduleWorkflow implements the WorkflowInterface for updating backup schedule task queues.
type updateBackupScheduleWorkflow struct {
	workflows.BaseWorkflow
}

// Enforcing the WorkflowInterface on updateBackupScheduleWorkflow
var _ workflows.WorkflowInterface = &updateBackupScheduleWorkflow{}

// UpdateBackupScheduleWorkflow is the entry point for the backup schedule update workflow.
// It sets up the workflow and runs the main backup schedule update logic.
func UpdateBackupScheduleWorkflow(ctx workflow.Context) error {
	updateBackupScheduleWF := new(updateBackupScheduleWorkflow)

	err := updateBackupScheduleWF.Setup(ctx, nil)
	if err != nil {
		return err
	}
	updateBackupScheduleWF.Status = workflows.WorkflowStatusRunning

	_, customErr := updateBackupScheduleWF.Run(ctx)
	if customErr != nil {
		updateBackupScheduleWF.Status = workflows.WorkflowStatusFailed
		return customErr
	}
	updateBackupScheduleWF.Status = workflows.WorkflowStatusCompleted
	return nil
}

// Setup initializes the workflow context, logger, and query handlers.
// It must be called before running the workflow logic.
func (wf *updateBackupScheduleWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.Status = workflows.WorkflowStatusCreated
	ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{
		"workflowID": wf.ID,
		"requestID":  utils.RandomUUID(),
	})
	logger := util.GetLogger(ctx)
	wf.Logger = logger

	return workflow.SetQueryHandler(ctx, "status", func() (*workflows.WorkflowStatus, error) {
		return &workflows.WorkflowStatus{
			ID:     wf.ID,
			Status: wf.Status,
		}, nil
	})
}

// Run executes the main backup schedule update logic: paginates through backup policies and updates their task queues.
func (wf *updateBackupScheduleWorkflow) Run(ctx workflow.Context, _ ...interface{}) (interface{}, *vsaerrors.CustomError) {
	logger := util.GetLogger(ctx)
	logger.Infof("Starting UpdateBackupScheduleWorkflow")

	retryPolicy, err := workflows.PopulateRetryPolicyParams()
	if err != nil {
		logger.Errorf("Failed to populate retry policy params: %v", err)
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
	ctx = workflow.WithActivityOptions(ctx, ao)

	// Create activity instance
	updateBackupScheduleActivity := &backgroundactivities.UpdateBackupScheduleActivity{}

	// Paginate through backup policies
	offset := 0
	for {
		pagination := &databaseUtils.Pagination{
			Offset: offset,
			Limit:  backgroundactivities.DefaultBackupPolicyBatchSize,
		}

		// Store the output from GetBackupPolicies in a local variable
		var backupPolicies []*datamodel.BackupPolicy
		err = workflow.ExecuteActivity(ctx, updateBackupScheduleActivity.GetBackupPolicies, pagination).Get(ctx, &backupPolicies)
		if err != nil {
			logger.Errorf("Failed to get backup policies at offset %d: %v", offset, err)
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrInternalServerError, fmt.Errorf("failed to get backup policies: %w", err))
		}

		// If no more policies returned, break the loop
		if len(backupPolicies) == 0 {
			logger.Infof("No more backup policies to process. Completed at offset %d", offset)
			break
		}

		logger.Infof("Retrieved %d backup policies at offset %d", len(backupPolicies), offset)

		// Update task queue for each backup policy schedule
		for _, backupPolicy := range backupPolicies {
			err = workflow.ExecuteActivity(ctx, updateBackupScheduleActivity.UpdateBackupScheduleTaskQueue, backupPolicy.UUID).Get(ctx, nil)
			if err != nil {
				logger.Errorf("Failed to update task queue for backup policy %s: %v", backupPolicy.UUID, err)
				// Continue processing other policies even if one fails
			}
		}

		// If we got fewer policies than the batch size, we've reached the end
		if len(backupPolicies) < backgroundactivities.DefaultBackupPolicyBatchSize {
			logger.Infof("Reached end of backup policies. Total processed: %d", offset+len(backupPolicies))
			break
		}

		// Move to next batch
		offset += backgroundactivities.DefaultBackupPolicyBatchSize
	}

	logger.Infof("UpdateBackupScheduleWorkflow completed successfully")
	return nil, nil
}
