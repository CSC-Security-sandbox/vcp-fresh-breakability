package backgroundworkflows

import (
	"context"
	"fmt"
	"strconv"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
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
			InitialInterval:        retryPolicy.InitialInterval,
			BackoffCoefficient:     retryPolicy.BackoffCoefficient,
			MaximumInterval:        retryPolicy.MaximumInterval,
			MaximumAttempts:        int32(retryPolicy.MaximumAttempts),
			NonRetryableErrorTypes: []string{"PanicError"},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	commonActivities := &activities.CommonActivities{}

	var poolIdentifiers []*database.PoolIdentifier
	err = workflow.ExecuteActivity(ctx, commonActivities.ListPoolsUUID).Get(ctx, &poolIdentifiers)
	if err != nil {
		logger.Error("ListPools activity failed.", "Error", err)
		return err
	}

	for _, pool := range poolIdentifiers {
		location, err := utils.GetLocationFromVendorID(pool.VendorID)
		if err != nil {
			logger.Errorf("Failed to get location from vendor ID for pool %s, error: %v", pool.Name, err)
			continue
		}
		syncSnapshotForPoolWFID := fmt.Sprintf(SyncSnapshotsPoolWFPlaceholder, pool.AccountID, location, pool.Name)
		startSyncPoolWFActivity := &StartSyncSnapshotForPoolActivity{}
		ctxWithParentWFID := util.AddExtraLoggerFields(ctx, map[string]interface{}{
			"parentWorkflowID": workflow.GetInfo(ctx).WorkflowExecution.ID,
		})

		err = workflow.ExecuteActivity(ctxWithParentWFID, startSyncPoolWFActivity.StartSyncSnapshotForPoolWFActivity, syncSnapshotForPoolWFID, pool).Get(ctxWithParentWFID, nil)
		if err != nil {
			// Logging and not returning error to ensure that the other pools can still be processed.
			logger.Warnf("Failed to start sync snapshot workflow for pool %s, error: %v", pool.Name, err)
		}
	}
	return nil
}

type SyncSnapshotsForPoolWF struct {
	workflows.BaseWorkflow
}

var _ workflows.WorkflowInterface = &SyncSnapshotsForPoolWF{}

func SyncSnapshotsForPoolWorkflow(ctx workflow.Context, pool *database.PoolIdentifier) error {
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
	pool := input.(*database.PoolIdentifier)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = strconv.FormatInt(pool.AccountID, 10)
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
	poolIdentifier := args[0].(*database.PoolIdentifier)

	retryPolicy, err := workflows.PopulateRetryPolicyParams()
	if err != nil {
		return nil, vsaerrors.ExtractCustomError(err)
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
	syncSnapshotActivity := &backgroundactivities.SyncSnapshotActivity{}
	logger.Infof("Starting synchronization of snapshots for pool: %s", poolIdentifier.Name)

	var pool *datamodel.Pool
	err = workflow.ExecuteActivity(ctx, syncSnapshotActivity.FetchPoolByUUID, poolIdentifier.UUID, poolIdentifier.AccountID).Get(ctx, &pool)
	if err != nil {
		logger.Error("FetchPoolByUUID activity execution failed.", "Error", err, "PoolName", poolIdentifier.Name)
		return nil, vsaerrors.ExtractCustomError(err)
	}

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

type StartSyncSnapshotForPoolActivity struct{}

// StartSyncSnapshotForPoolWFActivity starts a workflow to synchronize snapshots for a specific pool asynchronously.
func (a *StartSyncSnapshotForPoolActivity) StartSyncSnapshotForPoolWFActivity(ctx context.Context, workflowID string, poolIdentifier *database.PoolIdentifier) error {
	temporalClient := fetchTemporalClient(ctx)
	logger := util.GetLogger(ctx)

	logger.Info("Starting asynchronous synchronization of snapshots for pool", "PoolName", poolIdentifier.Name)

	_, err := temporalClient.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             temporalutils.BackgroundTaskQueue,
			ID:                    workflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
			WorkflowRunTimeout:    temporalutils.GetWorkflowGlobalTimeout(),
			// If a workflow with the same ID is already running, use the existing one.
			// This prevents duplicate workflows for the same pool.
			WorkflowIDConflictPolicy: enums.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		},
		SyncSnapshotsForPoolWorkflow,
		poolIdentifier,
	)
	if err != nil {
		logger.Errorf("Failed to start workflow for pool: %s, error: %v", poolIdentifier.Name, err)
		return err
	}

	logger.Infof("Started sync workflow for pool: %s with workflow ID: %s", poolIdentifier.Name, workflowID)

	return nil
}
