package expertMode

import (
	"errors"
	"fmt"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/vlm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	expertmodeactivities "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/expert_mode_activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	hyperscalermodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	rbacActivityStartToClose   = 3 * time.Minute
	rbacActivityInitialBackoff = 5 * time.Second
	rbacActivityMaxInterval    = 2 * time.Minute
	rbacActivityMaxAttempts    = 3
	rbacPoolBatchSize          = 5 // Number of pools to process in parallel per batch
)

// failedPoolInfo tracks a pool that failed during RBAC update
type failedPoolInfo struct {
	poolUUID string
	err      error
}

// updateRBACForPoolsWorkflow implements the WorkflowInterface for checking and updating RBAC hashes for pools.
type updateRBACForPoolsWorkflow struct {
	workflows.BaseWorkflow
	SE *database.Storage
}

// Enforcing the WorkflowInterface on updateRBACForPoolsWorkflow
var _ workflows.WorkflowInterface = &updateRBACForPoolsWorkflow{}

// UpdateRbacForPoolsWorkflow iterates through all active ONTAP mode pools and compares
// the hash of the latest RBAC file from GCS bucket for their ONTAP version to determine if an update is required.
func UpdateRbacForPoolsWorkflow(ctx workflow.Context) error {
	rbacWF := new(updateRBACForPoolsWorkflow)
	err := rbacWF.Setup(ctx, nil)
	if err != nil {
		return workflows.ConvertToVSAError(err)
	}

	if err = rbacWF.EnsureJobState(ctx, models.JobsStateNEW); err != nil {
		return workflows.ConvertToVSAError(err)
	}
	rbacWF.Status = workflows.WorkflowStatusRunning
	err = rbacWF.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		rbacWF.Status = workflows.WorkflowStatusFailed
		// Preserve the original error - try to update job status to ERROR (best effort)
		// but return the original error to maintain consistency with error handling at lines 62-70
		originalErr := err
		updateErr := rbacWF.UpdateJobStatus(ctx, string(models.JobsStateERROR), originalErr)
		if updateErr != nil {
			rbacWF.Logger.Errorf("Failed to update job status to Error for UpdateRbacForPoolsWorkflow: %v", updateErr)
		}
		return workflows.ConvertToVSAError(originalErr)
	}
	_, customErr := rbacWF.Run(ctx)
	if customErr != nil {
		rbacWF.Status = workflows.WorkflowStatusFailed
		// Preserve the original error - try to update job status to ERROR (best effort)
		// but always return the original error to maintain consistency with error handling at lines 58-65
		updateErr := rbacWF.UpdateJobStatus(ctx, string(models.JobsStateERROR), customErr)
		if updateErr != nil {
			rbacWF.Logger.Errorf("Failed to update job status to Error for UpdateRbacForPoolsWorkflow: %v", updateErr)
		}
		return workflows.ConvertToVSAError(customErr)
	}
	rbacWF.Status = workflows.WorkflowStatusCompleted
	err = rbacWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	if err != nil {
		return err
	}
	return nil
}

func (wf *updateRBACForPoolsWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.Status = workflows.WorkflowStatusCreated

	// Get correlationID from context if available, otherwise generate a new one
	correlationID := utils.RandomUUID()
	if existingCorrelationID, err := utils.GetCorrelationIDFromWorkflowContextLoggerFields(ctx); err == nil {
		correlationID = existingCorrelationID
	}

	// Add workflowID and correlationID to logger fields so they propagate to child workflows
	ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{
		"workflowID":                            wf.ID,
		string(middleware.RequestCorrelationID): correlationID,
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

// UpdateSinglePoolRbacChildWorkflow is a child workflow that processes a single pool's RBAC update
func UpdateSinglePoolRbacChildWorkflow(ctx workflow.Context, poolDetails expertmodeactivities.PoolDetailsWithRbacHash) error {
	// Get correlationID from context (guaranteed to be set by parent workflow)
	correlationID := utils.RandomUUID()
	if existingCorrelationID, err := utils.GetCorrelationIDFromWorkflowContextLoggerFields(ctx); err == nil {
		correlationID = existingCorrelationID
	}

	// Add workflow ID, request ID, and correlation ID to context for VLM workflows
	workflowInfo := workflow.GetInfo(ctx)
	ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{
		"workflowID":                            workflowInfo.WorkflowExecution.ID,
		"requestID":                             utils.RandomUUID(),
		string(middleware.RequestCorrelationID): correlationID,
	})
	logger := util.GetLogger(ctx)
	ao := workflow.ActivityOptions{
		TaskQueue:           workflowengine.BackgroundTaskQueue,
		StartToCloseTimeout: rbacActivityStartToClose,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        rbacActivityInitialBackoff,
			BackoffCoefficient:     2.0,
			MaximumInterval:        rbacActivityMaxInterval,
			MaximumAttempts:        int32(rbacActivityMaxAttempts),
			NonRetryableErrorTypes: []string{"PanicError"},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	rbacActivity := &expertmodeactivities.RBACUpdateActivity{}
	poolActivity := &activities.PoolActivity{}

	// Get pool by UUID first - we need it to get the ONTAP version for the file path
	var pool *datamodel.Pool
	err := workflow.ExecuteActivity(ctx, rbacActivity.GetPoolByUUID, poolDetails.PoolUUID).Get(ctx, &pool)
	if err != nil {
		logger.Errorf("Failed to get pool for pooluuid, %s error :%v", poolDetails.PoolUUID, err)
		return err
	}

	// Initialize bucket file details with ONTAP version from pool
	var ontapVersion string
	if pool.BuildInfo != nil && pool.BuildInfo.OntapVersion != "" {
		ontapVersion = pool.BuildInfo.OntapVersion
	} else {
		logger.Errorf("Pool %s does not have ONTAP version in BuildInfo", poolDetails.PoolUUID)
		return vsaerrors.WrapAsNonRetryableTemporalApplicationError(
			vsaerrors.NewVCPError(vsaerrors.ErrBadRequest, errors.New("pool missing ONTAP version")))
	}

	bucketFileDetails := &hyperscalermodels.BucketFileDetails{
		BucketName:     activities.ExpertModeRbacBucketName,
		FileUrl:        utils.GenerateRbacFilePath(activities.ExpertModeRbacFilePath, ontapVersion),
		FileHashSHA256: poolDetails.LatestRbacHash,
	}

	// Get ONTAP credentials
	var ontapCredentials *vlm.OntapCredentials
	err = workflow.ExecuteActivity(ctx, poolActivity.GetOnTapCredentials, pool).Get(ctx, &ontapCredentials)
	if err != nil {
		logger.Errorf("Failed to get ontap credentials error :%v", err)
		return err
	}

	// Get expert mode credentials
	var expertModeCredentials *vlm.OntapCredentials
	err = workflow.ExecuteActivity(ctx, poolActivity.GetExpertModeCredentials, pool).Get(ctx, &expertModeCredentials)
	if err != nil {
		logger.Errorf("Failed to get expert mode credentials error :%v", err)
		return err
	}

	// Parse VLM config
	var vlmConfig *vlm.VLMConfig
	err = workflow.ExecuteActivity(ctx, poolActivity.ParseVlmConfig, pool).Get(ctx, &vlmConfig)
	if err != nil {
		logger.Errorf("Failed to parse vlm config error: %v", err)
		return err
	}

	// Prepare create VSA expert mode request
	createVSAExpertModeUserReq := &vlm.OntapExpertModeUserConfig{}
	err = workflow.ExecuteActivity(ctx, poolActivity.PrepareCreateVSAExpertModeReq, *vlmConfig, *ontapCredentials, *expertModeCredentials, pool, bucketFileDetails).Get(ctx, createVSAExpertModeUserReq)
	if err != nil {
		logger.Errorf("Failed to create ONTAP expert mode error : %s", err)
		return err
	}

	// Create VSA expert mode user
	vsaClientWorkflowManager := vlm.NewVSAClientWorkflowManager()
	ontapExpertModeUserResponse, err := vsaClientWorkflowManager.CreateVSAExpertModeUser(ctx, createVSAExpertModeUserReq)
	if err != nil {
		logger.Errorf("Failed to update VSA expert mode user : %s, err:%v", poolDetails.PoolUUID, err)
		return err
	}

	// Update RBAC file checksum
	bucketFileDetails.FileHashSHA256 = ontapExpertModeUserResponse.RbacFileChecksum

	// Update RBAC checksum in pool
	if err = workflow.ExecuteActivity(ctx, poolActivity.UpdateRbacCheckSumInPool, pool, bucketFileDetails).Get(ctx, nil); err != nil {
		logger.Errorf("Failed to update RBAC for pool uuid: %s, err:%v", poolDetails.PoolUUID, err)
		return err
	}

	return nil
}

func (wf *updateRBACForPoolsWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: rbacActivityStartToClose,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        rbacActivityInitialBackoff,
			BackoffCoefficient:     2.0,
			MaximumInterval:        rbacActivityMaxInterval,
			MaximumAttempts:        int32(rbacActivityMaxAttempts),
			NonRetryableErrorTypes: []string{"PanicError"},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	// Step 1: Get all active ONTAP mode pools
	var pools []*datamodel.Pool
	rbacActivity := &expertmodeactivities.RBACUpdateActivity{}
	if err := workflow.ExecuteActivity(ctx, rbacActivity.ListActiveExpertModePools).Get(ctx, &pools); err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	if len(pools) == 0 {
		// Valid scenario: no active expert mode pools exist. Complete successfully with no work.
		wf.Logger.Infof("No active ONTAP pools found; skipping RBAC update workflow")
		return nil, nil
	}

	// Get pools grouped by ONTAP version with RBAC hash
	var poolsByVersion map[string][]expertmodeactivities.PoolDetailWithCurrentHash
	if err := workflow.ExecuteActivity(ctx, rbacActivity.GetPoolsDetailsByOntapVersion, pools).Get(ctx, &poolsByVersion); err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	var poolListNeedUpdate []expertmodeactivities.PoolDetailsWithRbacHash
	if err := workflow.ExecuteActivity(ctx, rbacActivity.GetLatestRbacHashForAllOntapVersion, &poolsByVersion).Get(ctx, &poolListNeedUpdate); err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	if len(poolListNeedUpdate) == 0 {
		// No pools need updating
		wf.Logger.Infof("No pools need RBAC update")
		return nil, nil
	}

	// Track failed pools and their errors
	var failedPools []failedPoolInfo

	// Process pools in batches with parallel execution within each batch
	batchSize := rbacPoolBatchSize
	for i := 0; i < len(poolListNeedUpdate); i += batchSize {
		end := i + batchSize
		if end > len(poolListNeedUpdate) {
			end = len(poolListNeedUpdate)
		}

		batch := poolListNeedUpdate[i:end]
		wf.Logger.Infof("Processing batch of pools for RBAC update: batchStart=%d, batchEnd=%d, batchSize=%d", i+1, end, len(batch))

		// Start all child workflows in the current batch in parallel
		var futures []workflow.ChildWorkflowFuture
		for _, poolDetails := range batch {
			childWorkflowID := workflow.GetInfo(ctx).WorkflowExecution.ID + "-pool-" + poolDetails.PoolUUID
			childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
				WorkflowID:            childWorkflowID,
				TaskQueue:             workflowengine.BackgroundTaskQueue,
				WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			})

			// Get correlationID from parent context and add it to child context
			// This ensures the child workflow receives the parent's correlationID
			correlationID := utils.RandomUUID()
			if existingCorrelationID, err := utils.GetCorrelationIDFromWorkflowContextLoggerFields(ctx); err == nil {
				correlationID = existingCorrelationID
			}

			// Add correlationID to child workflow context so it propagates to the child workflow
			childCtx = util.AddExtraLoggerFields(childCtx, map[string]interface{}{
				string(middleware.RequestCorrelationID): correlationID,
			})

			future := workflow.ExecuteChildWorkflow(childCtx, UpdateSinglePoolRbacChildWorkflow, poolDetails)
			futures = append(futures, future)
		}

		// Wait for all child workflows in the current batch to complete
		for j, future := range futures {
			err := future.Get(ctx, nil)
			if err != nil {
				// Track failed pools
				failedPools = append(failedPools, failedPoolInfo{
					poolUUID: batch[j].PoolUUID,
					err:      err,
				})
				wf.Logger.Errorf("Failed to execute child workflow for pool uuid: %s, error: %v", batch[j].PoolUUID, err)
			}
		}

		wf.Logger.Infof("Completed batch of pools for RBAC update: batchStart=%d, batchEnd=%d", i+1, end)
	}

	// Return error if any pools failed
	if len(failedPools) > 0 {
		// Build detailed error message with all failed pools
		errorMsg := fmt.Sprintf("RBAC update failed for %d pool(s): ", len(failedPools))
		for i, failed := range failedPools {
			if i > 0 {
				errorMsg += "; "
			}
			errorMsg += fmt.Sprintf("pool %s: %v", failed.poolUUID, failed.err)
		}
		wf.Logger.Errorf("RBAC update workflow completed with failures: %s", errorMsg)
		return nil, workflows.ConvertToVSAError(errors.New(errorMsg))
	}

	wf.Logger.Infof("RBAC update workflow completed successfully for all %d pools", len(poolListNeedUpdate))
	return nil, nil
}
