package backgroundworkflows

import (
	"database/sql"
	"fmt"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"strings"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	temporalutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

const (
	aggregateTieringFullnessThresholdDefault = 50
	aggregateTieringFullnessThresholdFull    = 100
	poolAutoTierAutoResizeWFIDPlaceholder    = "Account_%d_Location_%s_Pool_%s_Ops_AutoTiering-HotTier-AutoResize"
)

var (
	ConsecutiveUpdatePoolTimeGapAllowedMinutes = env.GetFloat64("CONSECUTIVE_UPDATE_POOL_TIME_GAP_ALLOWED_MINUTES", 240)
	AutoTieringSyncActivityMaxAttempts         = env.GetInt("AUTO_TIERING_SYNC_ACTIVITY_MAX_ATTEMPTS", 1)
)

// SyncVSAAutoTieringWorkflow is a workflow that synchronizes auto-tiering for all pools.
func SyncVSAAutoTieringWorkflow(ctx workflow.Context) error {
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
			MaximumAttempts:        int32(AutoTieringSyncActivityMaxAttempts),
			NonRetryableErrorTypes: []string{"PanicError"},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	autoTierActivity := &backgroundactivities.AutoTierSyncActivity{}
	commonActivities := &activities.CommonActivities{}

	var pools []*database.PoolIdentifier
	err = workflow.ExecuteActivity(ctx, commonActivities.ListPoolsUUID).Get(ctx, &pools)
	if err != nil {
		logger.Errorf("ListPools activity failed with error: %v", err)
		return err
	}

	// Assuming the map format is -> map[poolUUID]map[consumptionType]consumptionValue, where consumptionType can be "hotTier" or "coldTier"
	var poolsConsumptionMap map[string]map[string]float64
	err = workflow.ExecuteActivity(ctx, autoTierActivity.GetPoolsTierConsumptionFromOntap, pools).Get(ctx, &poolsConsumptionMap)
	if err != nil {
		logger.Errorf("Fetching auto-tier consumption from metrics db activity failed with error: %v", err)
		return err
	}

	var segregatedPools map[string][]*database.PoolIdentifier
	err = workflow.ExecuteActivity(ctx, autoTierActivity.SegregatePools, pools, poolsConsumptionMap).Get(ctx, &segregatedPools)
	if err != nil {
		logger.Errorf("SegregatePools activity failed with error: %v", err)
		return err
	}

	pauseResumeWFFuturesList := make(map[string]struct {
		Future  workflow.ChildWorkflowFuture
		Context workflow.Context
	})

	// Iterate over the segregated pools and start child workflows to pause or resume auto-tiering.
	for operation, poolIdentifiers := range segregatedPools {
		if operation == backgroundactivities.PoolsToAutoResizeKey {
			// Auto-resize operations will be handled separately after this loop.
			continue
		}
		for _, pool := range poolIdentifiers {
			childWFCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
				TaskQueue:             temporalutils.BackgroundTaskQueue,
				WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
				WorkflowTaskTimeout:   temporalutils.GetWorkflowGlobalTimeout(),
				// Since it's an independent pool level workflow, we can set the ParentClosePolicy to abandon.
				// This means that if the parent workflow is closed, this child workflow will continue to run.
				// And since we have a workflow timeout set, it will eventually complete or fail and not orphan out.
				ParentClosePolicy: enums.PARENT_CLOSE_POLICY_ABANDON,
			})

			// Add extra logger fields to the child workflow context for better traceability.
			// This will help in identifying the parent workflow ID in the logs of the child workflow.
			// It is useful for debugging and tracing the execution flow of the workflows.
			childWFCtx = util.AddExtraLoggerFields(childWFCtx, map[string]interface{}{
				"parentWorkflowID": workflow.GetInfo(ctx).WorkflowExecution.ID,
			})

			// Execute the child workflow asynchronously to pause or resume auto-tiering for the pool.
			fut := workflow.ExecuteChildWorkflow(childWFCtx, AutoTieringPauseResumeWorkflow, pool, operation)
			// Store the future and context in the map with the pool UUID as the key.
			pauseResumeWFFuturesList[pool.UUID] = struct {
				Future  workflow.ChildWorkflowFuture
				Context workflow.Context
			}{
				Future:  fut,
				Context: childWFCtx,
			}
		}
	}

	// Wait for all child workflows to complete. This will ensure that we avoid any
	// race conditions on the pool resource state update.
	for poolID, wfFuture := range pauseResumeWFFuturesList {
		// Wait for each child workflow to complete.
		err = wfFuture.Future.Get(wfFuture.Context, nil)
		if err != nil {
			logger.Errorf("Child workflow for pool %s pause/resume failed with error: %v", poolID, err)
			// Continue processing other child workflows even if one fails.
			continue
		}
		logger.Infof("Child workflow for pool %s pause/resume completed successfully", poolID)
	}

	for _, pool := range segregatedPools[backgroundactivities.PoolsToAutoResizeKey] {
		location, err := utils.GetLocationFromVendorID(pool.VendorID)
		if err != nil {
			logger.Errorf("Failed to get location from vendor ID, error: %v", err)
			return err
		}
		workflowID := fmt.Sprintf(poolAutoTierAutoResizeWFIDPlaceholder, pool.AccountID, location, pool.UUID)

		var lastExecutionTime *time.Time
		err = workflow.ExecuteActivity(ctx, (&activities.WFLastExecutionActivity{}).GetWorkflowLastExecutionTime, workflowID).Get(ctx, &lastExecutionTime)
		if err != nil {
			logger.Errorf("Failed to get last workflow completion time for pool %s: %v", pool.Name, err)
			continue
		}

		// If the last completion time is within 4 hours, skip the auto-resize for this pool.
		// This completion time is fetched from the previous run of the auto-resize workflow.
		// This run could be a success or a failure run. We will skip running again in both cases.
		if workflow.Now(ctx).Sub(*lastExecutionTime).Minutes() <= ConsecutiveUpdatePoolTimeGapAllowedMinutes {
			logger.Infof("Skipping auto-resize for pool %s in location %s for account %d as last run was within 4 hours", pool.Name, location, pool.AccountID)
			continue
		}

		childWFCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			TaskQueue:             temporalutils.BackgroundTaskQueue,
			WorkflowID:            workflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
			WorkflowTaskTimeout:   temporalutils.GetWorkflowGlobalTimeout(),
			// Since it's an independent pool level workflow, we can set the ParentClosePolicy to
			// abandon. This means that if the parent workflow is closed, this child workflow will continue to run.
			// And since we have a workflow timeout set, it will eventually complete or fail and not orphan out.
			ParentClosePolicy: enums.PARENT_CLOSE_POLICY_ABANDON,
		})
		logger.Infof("Starting workflow to auto-resize hot tier in pool %s with ID %s", pool.Name, pool.UUID)

		// Add extra logger fields to the child workflow context for better traceability.
		// This will help in identifying the parent workflow ID in the logs of the child workflow.
		// It is useful for debugging and tracing the execution flow of the workflows.
		childWFCtx = util.AddExtraLoggerFields(childWFCtx, map[string]interface{}{
			"parentWorkflowID": workflow.GetInfo(ctx).WorkflowExecution.ID,
		})

		// Execute the child workflow asynchronously to auto-resize hot tier for the pool and hydrating it to CCFE.
		workflow.ExecuteChildWorkflow(childWFCtx, AutoTieringHotTierAutoResizeWorkflow, pool)
	}

	// Sleep for a few seconds to ensure that the workflow does not complete immediately.
	// This is to ensure that the last workflow gets time to start.
	err = workflow.Sleep(ctx, 2*time.Second)
	if err != nil {
		return err
	}

	return nil
}

// AutoTieringPauseResumeWorkflow is a workflow that pauses or resumes auto-tiering for a specific pool.
func AutoTieringPauseResumeWorkflow(ctx workflow.Context, poolIdentifier database.PoolIdentifier, operation string) error {
	ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{
		"workflowID": workflow.GetInfo(ctx).WorkflowExecution.ID,
		"customerID": poolIdentifier.AccountID,
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
			MaximumAttempts:        int32(AutoTieringSyncActivityMaxAttempts),
			NonRetryableErrorTypes: []string{"PanicError"},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	autoTierActivity := &backgroundactivities.AutoTierSyncActivity{}
	syncSnapshotActivity := &backgroundactivities.SyncSnapshotActivity{}
	poolActivity := &activities.PoolActivity{}

	if operation == backgroundactivities.PoolsToPauseKey {
		logger.Infof("Starting child workflow to pause auto-tiering in pool %s with ID %s", poolIdentifier.Name, poolIdentifier.UUID)
	} else {
		logger.Infof("Starting child workflow to resume auto-tiering in pool %s with ID %s", poolIdentifier.Name, poolIdentifier.UUID)
	}

	var pool *datamodel.Pool
	err = workflow.ExecuteActivity(ctx, syncSnapshotActivity.FetchPoolByUUID, poolIdentifier.UUID, poolIdentifier.AccountID).Get(ctx, &pool)
	if err != nil {
		logger.Errorf("FetchPoolByUUID activity execution failed for pool %s. Error: %v", poolIdentifier.Name, err)
		return err
	}

	// Update the pool state to 'UPDATING' to ensure that the pool is in a consistent state.
	// This will help to avoid any race conditions on the pool resource.
	err = workflow.ExecuteActivity(ctx, poolActivity.UpdatingPool, pool).Get(ctx, nil)
	if err != nil {
		return err
	}

	var dbNodes []*datamodel.Node
	err = workflow.ExecuteActivity(ctx, activities.CommonActivities.GetNode, &pool.ID).Get(ctx, &dbNodes)
	if err != nil {
		return err
	}

	node := hyperscaler.CreateNodeForProvider(hyperscaler.NodeProviderInput{
		Nodes:          dbNodes,
		Password:       pool.PoolCredentials.Password,
		SecretID:       pool.PoolCredentials.SecretID,
		DeploymentName: pool.DeploymentName,
		CertificateID:  pool.PoolCredentials.CertificateID,
		AuthType:       pool.PoolCredentials.AuthType,
	})

	tieringFullnessThreshold := aggregateTieringFullnessThresholdDefault
	if operation == backgroundactivities.PoolsToPauseKey {
		pool.AutoTieringConfig.TieringPaused = true
		// If auto tiering is paused, we set the auto-tiering threshold to 100%
		tieringFullnessThreshold = aggregateTieringFullnessThresholdFull
	} else {
		pool.AutoTieringConfig.TieringPaused = false
	}

	err = workflow.ExecuteActivity(ctx, autoTierActivity.UpdateAggregateInOntap, node, tieringFullnessThreshold).Get(ctx, nil)
	if err != nil {
		logger.Errorf("Failed to execute PauseResumeActivity for pool %s: %v", pool.Name, err)
		return err
	}

	err = workflow.ExecuteActivity(ctx, poolActivity.UpdatedPool, pool).Get(ctx, nil)
	if err != nil {
		return err
	}

	if operation == backgroundactivities.PoolsToPauseKey {
		logger.Infof("Successfully finished child workflow to pause auto-tiering in pool %s with ID %s", poolIdentifier.Name, poolIdentifier.UUID)
	} else {
		logger.Infof("Successfully finished child workflow to resume auto-tiering in pool %s with ID %s", poolIdentifier.Name, poolIdentifier.UUID)
	}

	return nil
}

type autoTieringHotTierAutoResizeWorkflow struct {
	completionTime time.Time
}

// AutoTieringHotTierAutoResizeWorkflow is a workflow that handles the auto-resize of the hot tier for a specific pool.
func AutoTieringHotTierAutoResizeWorkflow(ctx workflow.Context, pool *database.PoolIdentifier) error {
	wf := new(autoTieringHotTierAutoResizeWorkflow)
	if err := wf.Setup(ctx, pool); err != nil {
		return err
	}
	_, err := wf.Run(ctx, pool)
	if err != nil {
		wf.completionTime = workflow.Now(ctx)
		return err
	}
	wf.completionTime = workflow.Now(ctx)
	return nil
}

func (wf *autoTieringHotTierAutoResizeWorkflow) Setup(ctx workflow.Context, pool *database.PoolIdentifier) error {
	info := workflow.GetInfo(ctx)
	ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{"workflowID": info.WorkflowExecution.ID, "customerID": pool.AccountID})
	wf.completionTime = workflow.Now(ctx)
	return workflow.SetQueryHandler(ctx, workflows.StatusQueryName, func() (*time.Time, error) {
		return &wf.completionTime, nil
	})
}

func (wf *autoTieringHotTierAutoResizeWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, error) {
	poolIdentifier := args[0].(*database.PoolIdentifier)
	logger := util.GetLogger(ctx)
	retryPolicy, err := workflows.PopulateRetryPolicyParams()
	if err != nil {
		return nil, err
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        retryPolicy.InitialInterval,
			BackoffCoefficient:     retryPolicy.BackoffCoefficient,
			MaximumInterval:        retryPolicy.MaximumInterval,
			MaximumAttempts:        int32(AutoTieringSyncActivityMaxAttempts),
			NonRetryableErrorTypes: []string{"PanicError"},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	commonActivities := activities.CommonActivities{}
	syncSnapshotActivity := &backgroundactivities.SyncSnapshotActivity{}
	poolActivity := &activities.PoolActivity{}

	var pool *datamodel.Pool
	err = workflow.ExecuteActivity(ctx, syncSnapshotActivity.FetchPoolByUUID, poolIdentifier.UUID, poolIdentifier.AccountID).Get(ctx, &pool)
	if err != nil {
		logger.Errorf("FetchPoolByUUID activity execution failed for pool %s. Error: %v", poolIdentifier.Name, err)
		return nil, err
	}

	// The job state is set to PROCESSING here because the workflow itself is creating the job
	job := &datamodel.Job{
		Type:         string(models.JobTypeUpdatePool),
		State:        string(models.JobsStatePROCESSING),
		IsAdminJob:   true,
		ResourceName: pool.UUID,
		AccountID:    sql.NullInt64{Int64: pool.AccountID, Valid: true},
	}

	poolRegionParts := strings.Split(pool.VendorID, "-")[:len(strings.Split(pool.VendorID, "-"))-1]
	poolRegion := strings.Join(poolRegionParts, "-")

	// We are increasing the hot tier size by 10%. The result is in round off GiB.
	newHotTierSizeInBytes := uint64(int64(float64(pool.AutoTieringConfig.HotTierSizeInBytes)*(1+0.01*backgroundactivities.AutoTierHotTierAutoResizeIncreasePercent)/(1<<30))) * (1 << 30)

	poolUpdateParams := &common.UpdatePoolParams{
		HotTierSizeInBytes:        newHotTierSizeInBytes,
		AutoResizeTriggeredUpdate: true,
		// Copying below fields as it is since they are required internally within the update pool workflow.
		AllowAutoTiering:        pool.AllowAutoTiering,
		EnableHotTierAutoResize: pool.AutoTieringConfig.EnableHotTierAutoResize,
		AccountName:             pool.Account.Name,
		PoolId:                  pool.UUID,
		SizeInBytes:             uint64(pool.SizeInBytes),
		TotalThroughputMibps:    pool.PoolAttributes.ThroughputMibps,
		TotalIops:               &pool.PoolAttributes.Iops,
		Description:             pool.Description,
		Region:                  poolRegion,
	}

	var createdJob *datamodel.Job
	err = workflow.ExecuteActivity(ctx, commonActivities.CreateJob, job).Get(ctx, &createdJob)
	if err != nil {
		return nil, err
	}

	err = workflow.ExecuteActivity(ctx, poolActivity.UpdatingPool, pool).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	childWFCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		WorkflowID:            createdJob.WorkflowID,
		TaskQueue:             temporalutils.BackgroundTaskQueue,
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		WorkflowTaskTimeout:   temporalutils.GetWorkflowGlobalTimeout(),
		ParentClosePolicy:     enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
	})
	err = workflow.ExecuteChildWorkflow(childWFCtx, workflows.UpdatePoolWorkflow, poolUpdateParams, pool).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	return nil, nil
}
