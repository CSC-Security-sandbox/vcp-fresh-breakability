package workflows

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// ReconcileHarvestNodeGroupMapParams optionally configures the reconciliation workflow.
type ReconcileHarvestNodeGroupMapParams struct {
	PageSize int // page size used inside ListAllMapsWithDeletedNodes; 0 = default
}

// ReconcileHarvestNodeGroupMapWorkflow runs two activities: (1) ListAllMapsWithDeletedNodes collects
// the full list of node group maps whose node is DELETED (pagination loop runs inside the activity),
// (2) ReconcileNodeGroupMapsBatch issues Harvest delete and DB soft-delete for that entire list.
func ReconcileHarvestNodeGroupMapWorkflow(ctx workflow.Context, params *ReconcileHarvestNodeGroupMapParams) error {
	logger := util.GetLogger(ctx)
	logger.Infof("ReconcileHarvestNodeGroupMapWorkflow started")

	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return err
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		HeartbeatTimeout:    retryPolicy.HeartBeatTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    retryPolicy.InitialInterval,
			BackoffCoefficient: retryPolicy.BackoffCoefficient,
			MaximumInterval:    retryPolicy.MaximumInterval,
			MaximumAttempts:    int32(retryPolicy.MaximumAttempts),
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	reconcileActivity := &activities.UnRegisterNodeFromHarvestActivity{}

	var listResult activities.ListAllMapsWithDeletedNodesResult
	listParams := &activities.ListAllMapsWithDeletedNodesParams{}
	if params != nil && params.PageSize > 0 {
		listParams.PageSize = params.PageSize
	}
	err = workflow.ExecuteActivity(ctx, reconcileActivity.ListAllMapsWithDeletedNodes, listParams).Get(ctx, &listResult)
	if err != nil {
		logger.Warnf("ListAllMapsWithDeletedNodes failed: %v", err)
		return err
	}

	totalReconciled := 0
	if len(listResult.MapsToReconcile) > 0 {
		batchParams := &activities.ReconcileNodeGroupMapsBatchParams{Maps: listResult.MapsToReconcile}
		var batchResult activities.ReconcileNodeGroupMapsBatchResult
		err = workflow.ExecuteActivity(ctx, reconcileActivity.ReconcileNodeGroupMapsBatch, batchParams).Get(ctx, &batchResult)
		if err != nil {
			logger.Warnf("ReconcileNodeGroupMapsBatch failed: %v", err)
			return err
		}
		totalReconciled = batchResult.Reconciled
	}

	logger.Infof("ReconcileHarvestNodeGroupMapWorkflow completed: %d records reconciled", totalReconciled)
	return nil
}
