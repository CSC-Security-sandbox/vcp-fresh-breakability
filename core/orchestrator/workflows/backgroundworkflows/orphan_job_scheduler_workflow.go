package backgroundworkflows

import (
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// OrphanJobSchedulerWorkflow finds all jobs with PENDING status and triggers ExecuteWorkflow to start the appropriate workflow
func OrphanJobSchedulerWorkflow(ctx workflow.Context) error {
	ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{
		"workflowID": workflow.GetInfo(ctx).WorkflowExecution.ID,
		// Adding a unique request ID for tracking purposes
		"requestID": utils.RandomUUID(),
	})
	logger := util.GetLogger(ctx)
	logger.Infof("Starting OrphanJobSchedulerWorkflow")

	retryPolicy, err := workflows.PopulateRetryPolicyParams()
	if err != nil {
		logger.Errorf("Failed to populate retry policy params: %v", err)
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

	// Create activity instance
	pendingJobActivity := &backgroundactivities.OrphanJobActivity{}
	// Execute the activity to fetch and process pending jobs (no parameters needed)
	err = workflow.ExecuteActivity(ctx, pendingJobActivity.OrphanJobsActivity).Get(ctx, nil)
	if err != nil {
		logger.Errorf("Failed to process pending jobs: %v", err)
		return fmt.Errorf("failed to process pending jobs: %w", err)
	}

	logger.Infof("OrphanJobSchedulerWorkflow completed successfully")
	return nil
}
