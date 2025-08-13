package backgroundworkflows

import (
	"context"
	"fmt"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	temporalutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

var (
	// SyncSnapshotsPoolWFPlaceholder is the placeholder for the workflowID of the child workflow that synchronizes snapshots for a specific pool.
	SyncSnapshotsPoolWFPlaceholder = "Account_%d_Location_%s_Pool_%s_Ops_SyncSnapshotsForPool"
)

func SyncVSASnapshotsWorkflow(ctx workflow.Context) error {
	ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{
		"workflowID": workflow.GetInfo(ctx).WorkflowExecution.ID,
		// Adding a unique request ID for tracking purposes
		"requestID": utils.RandomUUID(),
	})
	logger := util.GetLogger(ctx)

	retryPolicy, err := workflows.PopulateRetryPolicyParams()
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
	syncSnapshotActivity := &backgroundactivities.SyncSnapshotActivity{}

	var pools []*datamodel.Pool
	err = workflow.ExecuteActivity(ctx, syncSnapshotActivity.ListPools).Get(ctx, &pools)
	if err != nil {
		logger.Error("ListPools activity failed.", "Error", err)
		return err
	}

	for _, pool := range pools {
		location, err := utils.GetLocationFromVendorID(pool.VendorID)
		if err != nil {
			logger.Errorf("Failed to get location from vendor ID for pool %s, error: %v", pool.Name, err)
			continue
		}
		syncSnapshotForPoolWFID := fmt.Sprintf(SyncSnapshotsPoolWFPlaceholder, pool.AccountID, location, pool.Name)

		var isWFRunning *bool
		syncSnapshotWFRunningCheck := &SyncSnapshotWFRunningCheck{}
		err = workflow.ExecuteActivity(ctx, syncSnapshotWFRunningCheck.IsSyncSnapshotForPoolRunning, syncSnapshotForPoolWFID).Get(ctx, &isWFRunning)
		if err != nil {
			// If checking the workflow status, fails, we try to start a new one.
			logger.Warn("Failed to check if sync snapshot workflow is running.", "Error", err, "PoolName", pool.Name)
		}

		// If the workflow is already running, skip starting a new one.
		// This prevents multiple workflows from running concurrently for the same pool.
		if isWFRunning != nil && *isWFRunning {
			logger.Info("Sync snapshot workflow is already running for pool, skipping", "PoolName", pool.Name, "WorkflowID", syncSnapshotForPoolWFID)
			continue
		}

		cwo := workflow.ChildWorkflowOptions{
			WorkflowID:            syncSnapshotForPoolWFID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
			// Since it's an independent pool level workflow, we can set the ParentClosePolicy to abandon.
			// This means that if the parent workflow is closed, this child workflow will continue to run.
			// And since we have a workflow timeout set, it will eventually complete or fail and not orphan out.
			ParentClosePolicy:  enums.PARENT_CLOSE_POLICY_ABANDON,
			WorkflowRunTimeout: temporalutils.GetWorkflowGlobalTimeout(),
		}
		ctxWithCWO := workflow.WithChildOptions(ctx, cwo)

		// Add extra logger fields to the child workflow context for better traceability.
		// This will help in identifying the parent workflow ID in the logs of the child workflow.
		// It is useful for debugging and tracing the execution flow of the workflows.
		ctxWithCWO = util.AddExtraLoggerFields(ctxWithCWO, map[string]interface{}{
			"parentWorkflowID": workflow.GetInfo(ctx).WorkflowExecution.ID,
		})

		logger.Info("Starting asynchronous synchronization of snapshots for pool", "PoolName", pool.Name)
		workflow.ExecuteChildWorkflow(ctxWithCWO, SyncSnapshotsForPoolWorkflow, pool)
	}

	// Sleep for a short duration to allow the last child workflow to initiate before the parent workflow completes.
	err = workflow.Sleep(ctx, 1*time.Second)
	if err != nil {
		logger.Error("Failed to sleep after starting child workflows", "Error", err)
	}

	return nil
}

type SyncSnapshotsForPoolWF struct {
	workflows.BaseWorkflow
}

var _ workflows.WorkflowInterface = &SyncSnapshotsForPoolWF{}

func SyncSnapshotsForPoolWorkflow(ctx workflow.Context, pool *datamodel.Pool) error {
	syncSnapshotPoolWF := new(SyncSnapshotsForPoolWF)
	err := syncSnapshotPoolWF.Setup(ctx, pool)
	if err != nil {
		return err
	}
	syncSnapshotPoolWF.Status = workflows.WorkflowStatusRunning
	_, errRun := syncSnapshotPoolWF.Run(ctx, pool)
	if errRun != nil {
		syncSnapshotPoolWF.Status = workflows.WorkflowStatusFailed
		syncSnapshotPoolWF.Logger.Error("Failed to sync snapshots for pool", "PoolName", pool.Name, "Error", errRun)
		return errRun
	}
	syncSnapshotPoolWF.Status = workflows.WorkflowStatusCompleted
	syncSnapshotPoolWF.Logger.Info("Sync snapshot pool completed successfully for pool", "PoolName", pool.Name)
	return nil
}

func (wf *SyncSnapshotsForPoolWF) Setup(ctx workflow.Context, input interface{}) error {
	pool := input.(*datamodel.Pool)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = pool.Account.Name
	wf.Status = workflows.WorkflowStatusCreated
	ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{"workflowID": wf.ID, "customerID": wf.CustomerID})
	logger := util.GetLogger(ctx)
	wf.Logger = logger

	return workflow.SetQueryHandler(ctx, workflows.StatusQueryName, func() (*workflows.WorkflowStatus, error) {
		return &workflows.WorkflowStatus{
			ID:         wf.ID,
			Status:     wf.Status,
			CustomerID: wf.CustomerID,
		}, nil
	})
}

func (wf *SyncSnapshotsForPoolWF) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	logger := wf.Logger
	pool := args[0].(*datamodel.Pool)

	retryPolicy, err := workflows.PopulateRetryPolicyParams()
	if err != nil {
		return nil, vsaerrors.ExtractCustomError(err)
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
	syncSnapshotActivity := &backgroundactivities.SyncSnapshotActivity{}
	logger.Infof("Starting synchronization of snapshots for pool: %s", pool.Name)

	var ontapVolSnapshotResp *backgroundactivities.GetOntapVolumesAndSnapshotsForPoolReturnValue
	err = workflow.ExecuteActivity(ctx, syncSnapshotActivity.GetOntapVolumesAndSnapshotsForPool, pool).Get(ctx, &ontapVolSnapshotResp)
	if err != nil {
		logger.Error("GetOntapVolumesAndSnapshotsForPool activity execution failed.", "Error", err, "PoolName", pool.Name)
		return nil, vsaerrors.ExtractCustomError(err)
	}

	var dbVolSnapshotResp *backgroundactivities.GetDBVolumeAndSnapshotsForPoolReturnValue
	err = workflow.ExecuteActivity(ctx, syncSnapshotActivity.GetDBVolumeAndSnapshotsForPool, pool).Get(ctx, &dbVolSnapshotResp)
	if err != nil {
		logger.Error("GetDBVolumeAndSnapshotsForPool activity execution failed.", "Error", err, "PoolName", pool.Name)
		return nil, vsaerrors.ExtractCustomError(err)
	}

	var processSnapshotsResp *backgroundactivities.ProcessSnapshotsReturnValue
	err = workflow.ExecuteActivity(
		ctx,
		syncSnapshotActivity.ProcessSnapshots,
		ontapVolSnapshotResp.OntapVolumeMap,
		ontapVolSnapshotResp.OntapSnapshots,
		dbVolSnapshotResp.DBVolumeMap,
		dbVolSnapshotResp.DBSnapshots,
	).Get(ctx, &processSnapshotsResp)
	if err != nil {
		logger.Error("ProcessSnapshots activity execution failed.", "Error", err, "PoolName", pool.Name)
		return nil, vsaerrors.ExtractCustomError(err)
	}

	var deletedSnapshots []*datamodel.Snapshot
	err = workflow.ExecuteActivity(ctx, syncSnapshotActivity.SyncDeletedSnapshotsToDatabase, processSnapshotsResp.DeleteIDs).Get(ctx, &deletedSnapshots)
	if err != nil {
		logger.Error("SyncDeletedSnapshotsToDatabase activity execution failed.", "Error", err, "PoolName", pool.Name)
		return nil, vsaerrors.ExtractCustomError(err)
	}

	var createdSnapshots []*datamodel.Snapshot
	err = workflow.ExecuteActivity(ctx, syncSnapshotActivity.SyncNewSnapshotsToDatabase, processSnapshotsResp.NewIDs, processSnapshotsResp.NewSSMap, dbVolSnapshotResp.DBVolumeMap, pool).Get(ctx, &createdSnapshots)
	if err != nil {
		logger.Error("SyncNewSnapshotsToDatabase activity execution failed.", "Error", err, "PoolName", pool.Name)
		return nil, vsaerrors.ExtractCustomError(err)
	}

	err = workflow.ExecuteActivity(ctx, syncSnapshotActivity.SyncUpdatedSnapshotsToDatabase, processSnapshotsResp.UpdatedIDs, processSnapshotsResp.UpdatedSSMap, dbVolSnapshotResp.DBSnapshotMap).Get(ctx, nil)
	if err != nil {
		logger.Error("SyncUpdatedSnapshotsToDatabase activity execution failed.", "Error", err, "PoolName", pool.Name)
		return nil, vsaerrors.ExtractCustomError(err)
	}

	err = workflow.ExecuteActivity(ctx, syncSnapshotActivity.SyncWronglyDeletedSnapshotsToDatabase, processSnapshotsResp.WronglyDeletedIDs, processSnapshotsResp.WronglyDeletedSnapshotsMap, dbVolSnapshotResp.DBSnapshotMap).Get(ctx, nil)
	if err != nil {
		logger.Error("SyncWronglyDeletedSnapshotsToDatabase activity execution failed.", "Error", err, "PoolName", pool.Name)
		return nil, vsaerrors.ExtractCustomError(err)
	}

	err = workflow.ExecuteActivity(ctx, syncSnapshotActivity.HydrateSnapshotsToCCFE, createdSnapshots, deletedSnapshots).Get(ctx, nil)
	if err != nil {
		logger.Error("HydrateSnapshotsToCCFE activity execution failed.", "Error", err, "PoolName", pool.Name)
		return nil, vsaerrors.ExtractCustomError(err)
	}
	return nil, nil
}

var fetchTemporalClient = _fetchTemporalClient

func _fetchTemporalClient(ctx context.Context) client.Client {
	return activity.GetClient(ctx)
}

type SyncSnapshotWFRunningCheck struct{}

// IsSyncSnapshotForPoolRunning checks if a sync snapshot workflow is already running for the given workflow ID.
func (a *SyncSnapshotWFRunningCheck) IsSyncSnapshotForPoolRunning(ctx context.Context, workflowID string) (bool, error) {
	temporalClient := fetchTemporalClient(ctx)
	// Sending empty string as the runID will query the latest workflow run with the given workflowID.
	syncWFStatus, err := workflows.QueryWorkflowStatus(ctx, temporalClient, workflowID, "")
	if err != nil {
		return false, err
	}

	if syncWFStatus.Status == workflows.WorkflowStatusFailed {
		// If the workflow has failed, we can consider it as not running.
		return false, nil
	}

	if syncWFStatus.Status != workflows.WorkflowStatusCompleted {
		return true, nil
	}
	return false, nil
}
