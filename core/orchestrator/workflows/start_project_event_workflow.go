package workflows

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/vlm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/resource_events_activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	gcpserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

var (
	CVPJobRetryMaxAttempts           = env.GetInt("CVP_JOB_RETRY_MAX_ATTEMPTS", 10)
	InitialRetryIntervalForCVPClient = env.GetString("CVP_CLIENT_RETRY_INTERVAL", "60s")
	VSAOperationTimeout              = func() time.Duration {
		if timeout := env.GetString("VSA_OPERATION_TIMEOUT", "45m"); timeout != "" {
			if d, err := time.ParseDuration(timeout); err == nil {
				return d
			}
		}
		return time.Hour // Default to 1 hour
	}()
)

// PoolProcessingResult represents the result of processing a single pool
type PoolProcessingResult struct {
	PoolUUID     string
	PoolName     string
	Success      bool
	ErrorMessage string
}

// ClusterOperationResults represents the results of processing all pools
type ClusterOperationResults struct {
	TotalPools      int
	SuccessfulPools int
	FailedPools     int
	Results         []PoolProcessingResult
}

// StartProjectEventOffStateWorkflow is a workflow that handles the OFF state for StartProjectEvent.
type startProjectEventOffStateWorkflow struct {
	BaseWorkflow
}

// Enforcing the WorkflowInterface on startProjectEventOffStateWorkflow
var _ WorkflowInterface = &startProjectEventOffStateWorkflow{}

// StartProjectEventOffStateWorkflow is a workflow that handles the OFF state for StartProjectEvent.
func StartProjectEventOffStateWorkflow(ctx workflow.Context, params *common.StartProjectEventParams) (interface{}, error) {
	log := util.GetLogger(ctx)
	startProjectEventWorkflow := new(startProjectEventOffStateWorkflow)
	var customErr *vsaerrors.CustomError

	err := startProjectEventWorkflow.Setup(ctx, params)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	startProjectEventWorkflow.Status = WorkflowStatusRunning
	err = startProjectEventWorkflow.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		customErr = ConvertToVSAError(err)
		return nil, customErr
	}
	defer func() {
		if customErr != nil {
			startProjectEventWorkflow.Status = WorkflowStatusFailed
			err = startProjectEventWorkflow.UpdateJobStatus(ctx, string(models.JobsStateERROR), customErr)
			if err != nil {
				log.Errorf("startProjectEventOffStateWorkflow failed to update job status: %v", err)
			}
		} else {
			startProjectEventWorkflow.Status = WorkflowStatusCompleted
			err = startProjectEventWorkflow.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
		}
	}()

	_, customErr = startProjectEventWorkflow.Run(ctx, params)
	if customErr != nil {
		log.Errorf("startProjectEventOffStateWorkflow workflow completed with error: %v", customErr)
		return nil, customErr
	}
	log.Infof("startProjectEventOffStateWorkflow workflow completed successfully")
	return nil, nil
}

func (s *startProjectEventOffStateWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	startProjectEventOffStateParams := input.(*common.StartProjectEventParams)
	info := workflow.GetInfo(ctx)
	s.CustomerID = startProjectEventOffStateParams.ProjectNumber
	s.Status = WorkflowStatusCreated
	s.ID = info.WorkflowExecution.ID
	ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{"workflowID": s.ID, "customerID": s.CustomerID})
	logger := util.GetLogger(ctx)
	s.Logger = logger

	return workflow.SetQueryHandler(ctx, "status", func() (*WorkflowStatus, error) {
		return &WorkflowStatus{
			ID:         s.ID,
			Status:     s.Status,
			CustomerID: s.CustomerID,
		}, nil
	})
}

func (s *startProjectEventOffStateWorkflow) Run(ctx workflow.Context, args ...interface{}) (result interface{}, customErr *vsaerrors.CustomError) {
	logger := util.GetLogger(ctx)
	startProjectEventParams := args[0].(*common.StartProjectEventParams)
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{}

	// Determine if this is a zone-specific operation
	isZone := startProjectEventParams.Zone != ""

	var err error
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        retryPolicy.InitialInterval,
			BackoffCoefficient:     retryPolicy.BackoffCoefficient,
			MaximumInterval:        retryPolicy.MaximumInterval,
			MaximumAttempts:        int32(retryPolicy.MaximumAttempts),
			NonRetryableErrorTypes: []string{"NonRetryableError", "PanicError"},
		},
	}
	ao1 := ao
	ao1.RetryPolicy.MaximumAttempts = int32(CVPJobRetryMaxAttempts)
	ao1.RetryPolicy.InitialInterval, err = time.ParseDuration(InitialRetryIntervalForCVPClient)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	ctx = workflow.WithActivityOptions(ctx, ao)
	ao2 := ao1

	accountStateToSet := models.AccountStateEnabled // default to failure state, will be changed to success state only on completion
	defer func() {
		// Skip account state update if this is a zone-specific operation
		if isZone {
			logger.Infof("Zone specified (%s), skipping account state update", startProjectEventParams.Zone)
			return
		}

		updateErr := workflow.ExecuteActivity(ctx, startProjectEventActivity.UpdateAccountStateForHandleResource, startProjectEventParams.ProjectNumber, accountStateToSet).Get(ctx, nil)
		if updateErr != nil {
			logger.Errorf("Failed to update account state to %s: %v", accountStateToSet, updateErr)
			// Return error if we failed to update account state and the target state was success (AccountStateHyperscalerDisabled)
			if accountStateToSet == models.AccountStateHyperscalerDisabled {
				customErr = ConvertToVSAError(updateErr)
				return
			}
		} else {
			logger.Infof("Successfully updated account state to %s", accountStateToSet)
		}
	}()

	// Set account state to "disabling" before starting VSA operations - skip if Zone is specified
	if !isZone {
		err = workflow.ExecuteActivity(ctx, startProjectEventActivity.UpdateAccountStateForHandleResource, startProjectEventParams.ProjectNumber, models.AccountStateDisabling).Get(ctx, nil)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
	} else {
		logger.Infof("Zone specified (%s), skipping account state transition to DISABLING", startProjectEventParams.Zone)
	}

	// Now evaluate all results and determine final outcome
	var hasVSAFailure bool
	var hasSDEFailure bool

	// Check if LocationId is specified and pass it to activities for skip logic
	var vsaOperationsChan workflow.ReceiveChannel
	var vsaResults *ClusterOperationResults
	var poolList []*datamodel.PoolView
	var filterResult *resource_events_activities.PoolFilterResult
	vsaStartTime := time.Now() // Always capture start time for timeout calculations

	// Always fetch pools but pass LocationId to let activities handle skip logic
	poolActivity := &resource_events_activities.StartProjectEventActivity{}
	var allPoolList []*datamodel.PoolView

	err = workflow.ExecuteActivity(ctx, poolActivity.ListPoolsForAccount, startProjectEventParams.ProjectNumber, gcpserver.ResourceStateUpdateV1betaStateOFF, isZone).Get(ctx, &allPoolList)
	if err != nil {
		logger.Errorf("Failed to list pools for project %s: %v", startProjectEventParams.ProjectNumber, err)
		return nil, ConvertToVSAError(err)
	}

	// Filter pools to exclude those in transient states and check associated resources
	err = workflow.ExecuteActivity(ctx, startProjectEventActivity.FilterPoolsForClusterOperations, allPoolList, isZone).Get(ctx, &filterResult)
	if err != nil {
		logger.Errorf("Failed to filter pools for cluster operations: %v", err)
		return nil, ConvertToVSAError(err)
	}

	poolList = filterResult.FilteredPools
	if filterResult.VSAError {
		logger.Errorf("Some pools/volumes/snapshots are in transient states. Job will fail even if available pools are processed.")
		hasVSAFailure = true // Mark as failure when transient states are detected
	}

	if len(poolList) == 0 {
		logger.Info("No pools found for project, skipping cluster operations")
		// Create empty results for no pools
		vsaResults = &ClusterOperationResults{
			TotalPools:      0,
			SuccessfulPools: 0,
			FailedPools:     0,
			Results:         []PoolProcessingResult{},
		}
	} else {
		// Start VSA cluster operations in background asynchronously
		vsaOperationsChan, err = executeVSAClusterOperations(ctx, startProjectEventParams, vlm.ClusterPowerOff, poolList, s.processClusterForOFFState, models.LifeCycleStateDisabled, isZone)
		if err != nil {
			logger.Errorf("Failed to start VSA cluster operations: %v", err)
			// Note: This error path should never occur as executeVSAClusterOperations always returns nil error
		}
	}

	// Start SDE operations immediately (not in background) if CVP_HOST is configured
	var sdeError error
	var sdeResult *common.StartProjectEventResult
	if cvp.CVP_HOST != "" {
		logger.Info("Starting SDE operations")
		// Step 1: Start SDE operation
		err = workflow.ExecuteActivity(ctx, startProjectEventActivity.StartProjectEventForSDEActivity, startProjectEventParams).Get(ctx, &sdeResult)
		if err != nil {
			logger.Errorf("Failed to start SDE operation: %v", err)
			sdeError = err
		}
	}

	// Now collect VSA operation results at the end (this may take up to 45 minutes)
	if len(poolList) > 0 {
		var timeoutOccurred bool
		vsaResults, timeoutOccurred = waitForVSAOperationsWithTimeout(ctx, vsaOperationsChan, poolList)
		// Don't return early on timeout - mark as error but continue to handle SDE
		if timeoutOccurred {
			logger.Errorf("VSA cluster operations timed out after %v", VSAOperationTimeout)
			// Mark VSA as failed but don't return yet - we need to wait for SDE completion
			if vsaResults == nil {
				vsaResults = &ClusterOperationResults{
					TotalPools:      len(poolList),
					SuccessfulPools: 0,
					FailedPools:     len(poolList),
					Results:         []PoolProcessingResult{},
				}
			}
		}
	}

	// Calculate VSA end time and SDE start time, then determine remaining time for SDE polling
	if sdeResult != nil && sdeError == nil {
		remainingTime := VSAOperationTimeout - time.Since(vsaStartTime)

		// Check if SDE is already done
		if sdeResult.Done != nil && *sdeResult.Done {
			logger.Info("SDE operation was already completed")
		} else {
			ao2.StartToCloseTimeout = remainingTime
			if remainingTime < ao2.RetryPolicy.InitialInterval {
				ao2.RetryPolicy.MaximumAttempts = 1
			}
			sdeCtx := workflow.WithActivityOptions(ctx, ao2)

			err = workflow.ExecuteActivity(sdeCtx, startProjectEventActivity.PollStartProjectEventSDEOperationActivity, startProjectEventParams, sdeResult).Get(sdeCtx, nil)
			if err != nil {
				logger.Errorf("SDE polling failed: %v", err)
				sdeError = err
			} else {
				logger.Info("SDE operation completed successfully via polling")
			}
		}
	}

	// Check VSA operation results
	if vsaResults != nil && vsaResults.FailedPools > 0 {
		hasVSAFailure = true
		logger.Errorf("VSA operations failed: %d out of %d pools failed", vsaResults.FailedPools, vsaResults.TotalPools)
	}

	// Check SDE operation results
	if sdeError != nil {
		hasSDEFailure = true
		logger.Errorf("SDE operations failed: %v", sdeError)
	}

	// Handle failures - account state remains at failure state (models.AccountStateEnabled)
	if hasVSAFailure || hasSDEFailure {
		logger.Error("One or more operations failed, account state remains enabled")
		// Return appropriate error based on what failed
		if hasVSAFailure && hasSDEFailure {
			// Check if VSA failure is due to transient states
			if filterResult != nil && filterResult.VSAError && (vsaResults == nil || vsaResults.FailedPools == 0) {
				return nil, ConvertToVSAError(vsaerrors.NewVCPError(vsaerrors.ErrVSAClusterOperationFailed, fmt.Errorf("Job failed due to pools/volumes/snapshots in transient states. SDE also failed: %v", sdeError)))
			}
			return nil, ConvertToVSAError(vsaerrors.NewVCPError(vsaerrors.ErrVSAClusterOperationFailed, fmt.Errorf("Both VSA and SDE operations failed. VSA: %d pools failed, SDE: %v", vsaResults.FailedPools, sdeError)))
		} else if hasVSAFailure {
			// Check if VSA failure is due to transient states
			if filterResult != nil && filterResult.VSAError && (vsaResults == nil || vsaResults.FailedPools == 0) {
				return nil, ConvertToVSAError(vsaerrors.NewVCPError(vsaerrors.ErrVSAClusterOperationFailed, errors.New("Job failed due to pools/volumes/snapshots in transient states")))
			}
			return nil, ConvertToVSAError(vsaerrors.NewVCPError(vsaerrors.ErrVSAClusterOperationFailed, errors.New("One or more cluster power off operations failed")))
		} else {
			return nil, ConvertToVSAError(sdeError)
		}
	}

	// All operations completed successfully - set account state to success state
	if !isZone {
		accountStateToSet = models.AccountStateHyperscalerDisabled
	} else {
		logger.Infof("Zone specified (%s), skipping account state transition to HYPERSCALERDISABLED", startProjectEventParams.Zone)
	}
	logger.Info("Start Project Event OFF state operations completed successfully")
	return nil, nil
}

// StartProjectEventOnStateWorkflow is a workflow that handles the ON state for StartProjectEvent.
type startProjectEventOnStateWorkflow struct {
	BaseWorkflow
	SE *database.Storage
}

// StartProjectEventOnStateWorkflow is a workflow that handles the OFF state for StartProjectEvent.
func StartProjectEventOnStateWorkflow(ctx workflow.Context, params *common.StartProjectEventParams) (interface{}, error) {
	log := util.GetLogger(ctx)
	startProjectEventWorkflow := new(startProjectEventOnStateWorkflow)
	var customErr *vsaerrors.CustomError

	err := startProjectEventWorkflow.Setup(ctx, params)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	startProjectEventWorkflow.Status = WorkflowStatusRunning
	err = startProjectEventWorkflow.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		customErr = ConvertToVSAError(err)
		return nil, customErr
	}
	defer func() {
		if customErr != nil {
			startProjectEventWorkflow.Status = WorkflowStatusFailed
			err = startProjectEventWorkflow.UpdateJobStatus(ctx, string(models.JobsStateERROR), customErr)
			if err != nil {
				log.Errorf("startProjectEventOffStateWorkflow failed to update job status: %v", err)
			}
		} else {
			startProjectEventWorkflow.Status = WorkflowStatusCompleted
			err = startProjectEventWorkflow.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
		}
	}()

	_, customErr = startProjectEventWorkflow.Run(ctx, params)
	if customErr != nil {
		log.Errorf("startProjectEventOnStateWorkflow workflow completed with error: %v", customErr)
		return nil, customErr
	}
	log.Infof("startProjectEventOnStateWorkflow workflow completed successfully")
	return nil, nil
}

func (s *startProjectEventOnStateWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	startProjectEventOnStateParams := input.(*common.StartProjectEventParams)
	info := workflow.GetInfo(ctx)
	s.CustomerID = startProjectEventOnStateParams.ProjectNumber
	s.Status = WorkflowStatusCreated
	s.ID = info.WorkflowExecution.ID
	ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{"workflowID": s.ID, "customerID": s.CustomerID})
	logger := util.GetLogger(ctx)
	s.Logger = logger

	return workflow.SetQueryHandler(ctx, "status", func() (*WorkflowStatus, error) {
		return &WorkflowStatus{
			ID:         s.ID,
			Status:     s.Status,
			CustomerID: s.CustomerID,
		}, nil
	})
}

func (s *startProjectEventOnStateWorkflow) Run(ctx workflow.Context, args ...interface{}) (result interface{}, customErr *vsaerrors.CustomError) {
	logger := util.GetLogger(ctx)
	startProjectEventParams := args[0].(*common.StartProjectEventParams)
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{}

	// Determine if this is a zone-specific operation
	isZone := startProjectEventParams.Zone != ""

	var err error
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        retryPolicy.InitialInterval,
			BackoffCoefficient:     retryPolicy.BackoffCoefficient,
			MaximumInterval:        retryPolicy.MaximumInterval,
			MaximumAttempts:        int32(retryPolicy.MaximumAttempts),
			NonRetryableErrorTypes: []string{"NonRetryableError", "PanicError"},
		},
	}

	ao1 := ao
	ao1.RetryPolicy.MaximumAttempts = int32(CVPJobRetryMaxAttempts)
	ao1.RetryPolicy.InitialInterval, err = time.ParseDuration(InitialRetryIntervalForCVPClient)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	ctx = workflow.WithActivityOptions(ctx, ao)
	ao2 := ao1

	accountStateToSet := models.AccountStateHyperscalerDisabled // default to failure state, will be changed to success state only on completion
	defer func() {
		// Skip account state update if this is a zone-specific operation
		if isZone {
			logger.Infof("Zone specified (%s), skipping account state update", startProjectEventParams.Zone)
			return
		}

		updateErr := workflow.ExecuteActivity(ctx, startProjectEventActivity.UpdateAccountStateForHandleResource, startProjectEventParams.ProjectNumber, accountStateToSet).Get(ctx, nil)
		if updateErr != nil {
			logger.Errorf("Failed to update account state to %s: %v", accountStateToSet, updateErr)
			// Return error if we failed to update account state and the target state was success (AccountStateEnabled)
			if accountStateToSet == models.AccountStateEnabled {
				customErr = ConvertToVSAError(updateErr)
				return
			}
		} else {
			logger.Infof("Successfully updated account state to %s", accountStateToSet)
		}
	}()

	// Set account state to "enabling" before starting VSA operations - skip if Zone is specified
	if !isZone {
		err = workflow.ExecuteActivity(ctx, startProjectEventActivity.UpdateAccountStateForHandleResource, startProjectEventParams.ProjectNumber, models.AccountStateEnabling).Get(ctx, nil)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
	} else {
		logger.Infof("Zone specified (%s), skipping account state transition to ENABLING", startProjectEventParams.Zone)
	}

	// Check if LocationId is specified and pass it to activities for skip logic
	var vsaOperationsChan workflow.ReceiveChannel
	var vsaResults *ClusterOperationResults
	var poolList []*datamodel.PoolView
	vsaStartTime := time.Now() // Always capture start time for timeout calculations

	// Always fetch pools but pass LocationId to let activities handle skip logic
	poolActivity := &resource_events_activities.StartProjectEventActivity{}

	err = workflow.ExecuteActivity(ctx, poolActivity.ListPoolsForAccount, startProjectEventParams.ProjectNumber, gcpserver.ResourceStateUpdateV1betaStateON, isZone).Get(ctx, &poolList)
	if err != nil {
		logger.Errorf("Failed to list pools for project %s: %v", startProjectEventParams.ProjectNumber, err)
		return nil, ConvertToVSAError(err)
	}

	if len(poolList) == 0 {
		logger.Info("No pools found for project, skipping cluster operations")
		// Create empty results for no pools
		vsaResults = &ClusterOperationResults{
			TotalPools:      0,
			SuccessfulPools: 0,
			FailedPools:     0,
			Results:         []PoolProcessingResult{},
		}
	} else {
		// Start VSA cluster operations in background asynchronously
		vsaOperationsChan, err = executeVSAClusterOperations(ctx, startProjectEventParams, vlm.ClusterPowerOn, poolList, s.processClusterForONState, models.LifeCycleStateREADY, isZone)
		if err != nil {
			logger.Errorf("Failed to start VSA cluster operations: %v", err)
			// Note: This error path should never occur as executeVSAClusterOperations always returns nil error
		}
	}

	// Start SDE operations immediately (not in background) if CVP_HOST is configured
	var sdeError error
	var sdeResult *common.StartProjectEventResult
	if cvp.CVP_HOST != "" {
		logger.Info("Starting SDE operations")
		// Start SDE operation
		err = workflow.ExecuteActivity(ctx, startProjectEventActivity.StartProjectEventForSDEActivity, startProjectEventParams).Get(ctx, &sdeResult)
		if err != nil {
			logger.Errorf("Failed to start SDE operation: %v", err)
			sdeError = err
		} else {
			// Check if SDE completed immediately
			if sdeResult.Done != nil && *sdeResult.Done {
				logger.Info("SDE operation completed immediately")
			} else {
				logger.Info("SDE operation started, will poll after VSA operations complete")
			}
		}
	}

	// Now collect VSA operation results at the end (this may take up to 45 minutes)
	if len(poolList) > 0 {
		var timeoutOccurred bool
		vsaResults, timeoutOccurred = waitForVSAOperationsWithTimeout(ctx, vsaOperationsChan, poolList)

		// Don't return early on timeout - mark as error but continue to handle SDE
		if timeoutOccurred {
			logger.Errorf("VSA cluster operations timed out after %v", VSAOperationTimeout)
			// Mark VSA as failed but don't return yet - we need to wait for SDE completion
			if vsaResults == nil {
				vsaResults = &ClusterOperationResults{
					TotalPools:      len(poolList),
					SuccessfulPools: 0,
					FailedPools:     len(poolList),
					Results:         []PoolProcessingResult{},
				}
			}
		}
	}

	// Calculate VSA end time and SDE start time, then determine remaining time for SDE polling
	if sdeResult != nil && sdeError == nil {
		remainingTime := VSAOperationTimeout - time.Since(vsaStartTime)

		// Check if SDE is already done
		if sdeResult.Done != nil && *sdeResult.Done {
			logger.Info("SDE operation was already completed")
		} else {
			ao2.StartToCloseTimeout = remainingTime
			if remainingTime < ao2.RetryPolicy.InitialInterval {
				ao2.RetryPolicy.MaximumAttempts = 1
			}
			sdeCtx := workflow.WithActivityOptions(ctx, ao2)
			err = workflow.ExecuteActivity(sdeCtx, startProjectEventActivity.PollStartProjectEventSDEOperationActivity, startProjectEventParams, sdeResult).Get(sdeCtx, nil)
			if err != nil {
				logger.Errorf("SDE polling failed: %v", err)
				sdeError = err
			} else {
				logger.Info("SDE operation completed successfully via polling")
			}
		}
	}

	// Now evaluate all results and determine final outcome
	var hasVSAFailure bool
	var hasSDEFailure bool

	// Check VSA operation results
	if vsaResults != nil && vsaResults.FailedPools > 0 {
		hasVSAFailure = true
		logger.Errorf("VSA operations failed: %d out of %d pools failed", vsaResults.FailedPools, vsaResults.TotalPools)
	}

	// Check SDE operation results
	if sdeError != nil {
		hasSDEFailure = true
		logger.Errorf("SDE operations failed: %v", sdeError)
	}

	// Handle failures - account state remains at failure state (models.AccountStateHyperscalerDisabled)
	if hasVSAFailure || hasSDEFailure {
		logger.Error("One or more operations failed, account state remains disabled")

		// Return appropriate error based on what failed
		if hasVSAFailure && hasSDEFailure {
			return nil, ConvertToVSAError(vsaerrors.NewVCPError(vsaerrors.ErrVSAClusterOperationFailed, fmt.Errorf("Both VSA and SDE operations failed. VSA: %d pools failed, SDE: %v", vsaResults.FailedPools, sdeError)))
		} else if hasVSAFailure {
			return nil, ConvertToVSAError(vsaerrors.NewVCPError(vsaerrors.ErrVSAClusterOperationFailed, errors.New("One or more cluster power on operations failed")))
		} else {
			return nil, ConvertToVSAError(sdeError)
		}
	}

	// All operations completed successfully - set account state to success state
	if !isZone {
		accountStateToSet = models.AccountStateEnabled
	} else {
		logger.Infof("Zone specified (%s), skipping account state transition to ENABLED", startProjectEventParams.Zone)
	}
	logger.Info("Start Project Event ON state operations completed successfully")
	return nil, nil
}

// executeVSAClusterOperations performs cluster operations for any state (ON/OFF) in parallel and returns a channel for polling
func executeVSAClusterOperations(ctx workflow.Context, params *common.StartProjectEventParams, operationType string, poolList []*datamodel.PoolView, processorFunc func(workflow.Context, vlm.VlmWorkflowClient, *datamodel.PoolView, *common.StartProjectEventParams, bool) error, targetLifecycleState string, isZone bool) (workflow.ReceiveChannel, error) {
	logger := util.GetLogger(ctx)

	logger.Infof("Found %d pools for project %s to perform cluster %s operations", len(poolList), params.ProjectNumber, operationType)

	// Create a channel to return results to the workflow
	resultChannel := workflow.NewChannel(ctx)

	// Start cluster operations in background and return channel immediately
	workflow.Go(ctx, func(ctx workflow.Context) {
		// Process all pools in parallel using Temporal's workflow concurrency with channels
		results := ClusterOperationResults{
			TotalPools: len(poolList),
			Results:    make([]PoolProcessingResult, 0, len(poolList)),
		}

		// Create a channel to collect results from parallel operations
		internalResultsChan := workflow.NewBufferedChannel(ctx, len(poolList))

		// Process each pool's VSA cluster in parallel using workflow.Go
		for _, pool := range poolList {
			currentPool := pool // capture loop variable

			workflow.Go(ctx, func(ctx workflow.Context) {
				result := PoolProcessingResult{
					PoolUUID: currentPool.UUID,
					PoolName: currentPool.Name,
					Success:  true,
				}

				// Get VSA client workflow manager for this pool operation
				vsaClientWorkflowManager := GetNewVSAClientWorkflowManager()

				// Execute the actual processor function for this pool
				errOp := processorFunc(ctx, vsaClientWorkflowManager, currentPool, params, isZone)
				if errOp != nil {
					result.Success = false
					result.ErrorMessage = errOp.Error()
					logger.Error("Failed to process cluster operations for currentPool", "poolName", currentPool.Name, "poolUUID", currentPool.UUID, "error", errOp)
				} else {
					logger.Info("Successfully processed cluster operations for currentPool", "poolName", currentPool.Name, "poolUUID", currentPool.UUID)
				}

				internalResultsChan.Send(ctx, result)
			})
		}

		// Collect results from all parallel operations
		for i := 0; i < len(poolList); i++ {
			var result PoolProcessingResult
			internalResultsChan.Receive(ctx, &result)
			results.Results = append(results.Results, result)

			if result.Success {
				// Update pool lifecycle state to target state on successful operation
				poolActivity := &activities.PoolActivity{}
				pool := &datamodel.Pool{}
				pool.UUID = result.PoolUUID
				var stateDetails string
				switch targetLifecycleState {
				case models.LifeCycleStateDisabled:
					stateDetails = models.LifeCycleStateDisabledDetails
				case models.LifeCycleStateREADY:
					stateDetails = models.LifeCycleStateAvailableDetails
				}

				err := workflow.ExecuteActivity(ctx, poolActivity.UpdatePoolState, pool, targetLifecycleState, stateDetails).Get(ctx, nil)
				if err != nil {
					logger.Warnf("Failed to update pool %s lifecycle state to %s: %v", result.PoolName, targetLifecycleState, err)
					// Mark as failed since we couldn't update the pool state
					result.Success = false
					result.ErrorMessage = fmt.Sprintf("Failed to update pool lifecycle state: %v", err)
					results.FailedPools++
				} else {
					logger.Infof("Successfully updated pool %s lifecycle state to %s", result.PoolName, targetLifecycleState)
					results.SuccessfulPools++
				}
			} else {
				results.FailedPools++
			}
		}

		// Log summary
		logger.Infof("Cluster %s operations summary: Total=%d, Successful=%d, Failed=%d",
			operationType, results.TotalPools, results.SuccessfulPools, results.FailedPools)

		// Log failed pools for debugging
		if results.FailedPools > 0 {
			logger.Warnf("Failed pools:")
			for _, result := range results.Results {
				if !result.Success {
					logger.Warnf("  - Pool %s (%s): %s", result.PoolName, result.PoolUUID, result.ErrorMessage)
				}
			}
		}

		// Send final results to the workflow through the return channel
		resultChannel.Send(ctx, &results)
	})

	return resultChannel, nil
}

// waitForVSAOperationsWithTimeout waits for VSA operations to complete with a configurable timeout
// This is a reusable helper function that handles the timeout logic for both ON and OFF state workflows
func waitForVSAOperationsWithTimeout(ctx workflow.Context, vsaOperationsChan workflow.ReceiveChannel, poolList []*datamodel.PoolView) (*ClusterOperationResults, bool) {
	logger := util.GetLogger(ctx)
	logger.Infof("Waiting for VSA cluster operations to complete (timeout: %v)...", VSAOperationTimeout)

	// Create a selector to wait for either VSA completion or timeout
	selector := workflow.NewSelector(ctx)
	var vsaResults *ClusterOperationResults
	var timeoutOccurred bool

	// Add VSA operations channel
	selector.AddReceive(vsaOperationsChan, func(c workflow.ReceiveChannel, more bool) {
		c.Receive(ctx, &vsaResults)
	})

	// Add timeout using the configurable constant
	selector.AddFuture(workflow.NewTimer(ctx, VSAOperationTimeout), func(f workflow.Future) {
		timeoutOccurred = true
		logger.Warnf("VSA cluster operations timed out after %v", VSAOperationTimeout)
		// Create timeout result for all pools
		vsaResults = &ClusterOperationResults{
			TotalPools:      len(poolList),
			SuccessfulPools: 0,
			FailedPools:     len(poolList),
			Results:         make([]PoolProcessingResult, 0, len(poolList)),
		}
		// Add timeout results for each pool
		for _, pool := range poolList {
			vsaResults.Results = append(vsaResults.Results, PoolProcessingResult{
				PoolUUID:     pool.UUID,
				PoolName:     pool.Name,
				Success:      false,
				ErrorMessage: fmt.Sprintf("Operation timed out after %v", VSAOperationTimeout),
			})
		}
	})

	// Wait for either completion or timeout
	selector.Select(ctx)

	if timeoutOccurred {
		logger.Errorf("VSA cluster operations timed out after %v", VSAOperationTimeout)
	} else {
		logger.Info("VSA cluster operations completed",
			"totalPools", vsaResults.TotalPools,
			"successfulPools", vsaResults.SuccessfulPools,
			"failedPools", vsaResults.FailedPools)
	}

	return vsaResults, timeoutOccurred
}

// processClusterForONState performs power cycle for a single cluster
func (s *startProjectEventOnStateWorkflow) processClusterForONState(ctx workflow.Context, vsaClientWorkflowManager vlm.VlmWorkflowClient, pool *datamodel.PoolView, params *common.StartProjectEventParams, isZone bool) error {
	logger := util.GetLogger(ctx)

	// Skip VSA operations if this is a zone-specific operation
	if isZone {
		logger.Infof("Zone specified (%s), skipping VSA cluster power on operation for pool %s", params.Zone, pool.Name)
		return nil
	}

	poolActivity := &activities.PoolActivity{}

	// Prepare VLM config and credentials from pool data
	ontapCredentials := &vlm.OntapCredentials{}
	currentVlmConfig := &vlm.VLMConfig{}

	if err := json.Unmarshal([]byte(pool.VLMConfig), currentVlmConfig); err != nil {
		return ConvertToVSAError(err)
	}

	err := workflow.ExecuteActivity(ctx, poolActivity.GetOnTapCredentials, pool).Get(ctx, &ontapCredentials)
	if err != nil {
		return ConvertToVSAError(err)
	}

	clusterPowerOpRequest := &vlm.ClusterPowerOpRequest{
		VLMConfig:        *currentVlmConfig,
		OntapCredentials: *ontapCredentials,
		Operation:        vlm.ClusterPowerOn,
	}

	// Power on the cluster
	logger.Info("Starting cluster power on operation", "deploymentID", currentVlmConfig.Deployment.DeploymentID)

	err = vsaClientWorkflowManager.ClusterPowerOp(ctx, clusterPowerOpRequest)
	if err != nil {
		logger.Errorf("Cluster power on failed for deployment %s: %v", currentVlmConfig.Deployment.DeploymentID, err)
		return err
	}

	logger.Info("Cluster power on operation completed successfully", "deploymentID", currentVlmConfig.Deployment.DeploymentID)
	return nil
}

// processClusterForOFFState performs health check and power cycle for a single cluster shutdown
func (s *startProjectEventOffStateWorkflow) processClusterForOFFState(ctx workflow.Context, vsaClientWorkflowManager vlm.VlmWorkflowClient, pool *datamodel.PoolView, params *common.StartProjectEventParams, isZone bool) error {
	logger := util.GetLogger(ctx)

	// Skip VSA operations if this is a zone-specific operation
	if isZone {
		logger.Infof("Zone specified (%s), skipping VSA cluster power off operation for pool %s", params.Zone, pool.Name)
		return nil
	}

	poolActivity := &activities.PoolActivity{}

	// Prepare VLM config and credentials from pool data
	ontapCredentials := &vlm.OntapCredentials{}
	currentVlmConfig := &vlm.VLMConfig{}

	if err := json.Unmarshal([]byte(pool.VLMConfig), currentVlmConfig); err != nil {
		return ConvertToVSAError(err)
	}

	err := workflow.ExecuteActivity(ctx, poolActivity.GetOnTapCredentials, pool).Get(ctx, &ontapCredentials)
	if err != nil {
		return ConvertToVSAError(err)
	}

	// Step 1: Validate cluster health (non-blocking)
	logger.Info("Starting cluster health validation before shutdown", "deploymentID", currentVlmConfig.Deployment.DeploymentID)

	validateClusterHealthRequest := &vlm.ValidateClusterHealthRequest{
		VLMConfig:            *currentVlmConfig,
		OntapCredentials:     *ontapCredentials,
		TriggerASUPOnFailure: true,
	}

	err = vsaClientWorkflowManager.ValidateClusterHealth(ctx, validateClusterHealthRequest)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "cloud vm is offline") {
			logger.Infof("Cluster is already powered off for deployment %s, considering power-off operation successful", currentVlmConfig.Deployment.DeploymentID)
			return nil
		}
		logger.Errorf("Cluster health check encountered an error for deployment %s: %v", currentVlmConfig.Deployment.DeploymentID, err)
		return err
	}

	logger.Info("Cluster health validated successfully", "deploymentID", currentVlmConfig.Deployment.DeploymentID)

	// Step 2: Power off the cluster
	logger.Info("Starting cluster power off operation", "deploymentID", currentVlmConfig.Deployment.DeploymentID)

	clusterPowerOpRequest := &vlm.ClusterPowerOpRequest{
		VLMConfig:        *currentVlmConfig,
		OntapCredentials: *ontapCredentials,
		Operation:        vlm.ClusterPowerOff,
	}

	err = vsaClientWorkflowManager.ClusterPowerOp(ctx, clusterPowerOpRequest)
	if err != nil {
		logger.Errorf("Cluster power off failed for deployment %s: %v", currentVlmConfig.Deployment.DeploymentID, err)
		return err
	}

	logger.Info("Cluster power off operation completed successfully", "deploymentID", currentVlmConfig.Deployment.DeploymentID)
	return nil
}
