package backgroundworkflows

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

var (
	enableSyncPoolZIZS    = env.GetBool("ENABLE_SYNC_POOL_ZIZS", false)
	syncPoolZIZSBatchSize = env.GetInt("SYNC_POOL_ZIZS_BATCH_SIZE", 5)
)

// SyncPoolZIZSDetailsWorkflow orchestrates the pool ZI/ZS compliance sync process for all pools
func SyncPoolZIZSDetailsWorkflow(ctx workflow.Context) error {
	logger := util.GetLogger(ctx)
	if !enableSyncPoolZIZS {
		logger.Info("SyncPoolZIZS workflow is disabled via ENABLE_SYNC_POOL_ZIZS environment variable")
		return nil
	}
	ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{
		"workflowID": workflow.GetInfo(ctx).WorkflowExecution.ID,
		"requestID":  utils.RandomUUID(),
	})

	retryPolicy, err := workflows.PopulateRetryPolicyParams()
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

	logger.Info("Starting pool ZI/ZS compliance sync workflow for all pools")

	ctx = workflow.WithActivityOptions(ctx, ao)
	commonActivities := &activities.CommonActivities{}

	filterState := []string{models.LifeCycleStateREADY}
	var poolIdentifiers []*database.PoolIdentifier
	err = workflow.ExecuteActivity(ctx, commonActivities.ListPoolsUUID, filterState).Get(ctx, &poolIdentifiers)
	if err != nil {
		logger.Error("ListPools activity failed during SyncPoolZIZSDetailsWorkflow.", "Error", err)
		return err
	}

	if poolIdentifiers == nil {
		logger.Error("listPools activity returned empty list during SyncPoolZIZSDetailsWorkflow")
		return nil
	}

	logger.Info("Successfully fetched undeleted pools", "count", len(poolIdentifiers))

	// Step 2: Process pools asynchronously in batches of 5
	batchSize := syncPoolZIZSBatchSize
	for i := 0; i < len(poolIdentifiers); i += batchSize {
		end := i + batchSize
		if end > len(poolIdentifiers) {
			end = len(poolIdentifiers)
		}

		batch := poolIdentifiers[i:end]
		logger.Info("Processing batch of pools", "batchStart", i+1, "batchEnd", end, "batchSize", len(batch))

		// Start all workflows in the current batch asynchronously
		var futures []workflow.Future
		for _, pool := range batch {
			ctxWithParentWFID := util.AddExtraLoggerFields(ctx, map[string]interface{}{
				"parentWorkflowID": workflow.GetInfo(ctx).WorkflowExecution.ID,
			})

			future := workflow.ExecuteChildWorkflow(ctxWithParentWFID, workflows.SyncPoolComplianceForPoolWorkflow, pool)
			futures = append(futures, future)
		}

		// Wait for all workflows in the current batch to complete
		for j, future := range futures {
			err = future.Get(ctx, nil)
			if err != nil {
				// Logging and not returning error to ensure that the other pools can still be processed.
				logger.Warnf("Failed to complete sync pool compliance workflow for pool %s, error: %v", batch[j].Name, err)
			}
		}

		logger.Info("Completed batch of pools", "batchStart", i+1, "batchEnd", end)
	}

	logger.Info("All pool compliance workflows started successfully")
	return nil
}
