package backgroundworkflows

import (
	"fmt"
	"reflect"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	temporalUtils "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// GenericParentWorkflowConfig contains configuration for the generic parent workflow
type GenericParentWorkflowConfig struct {
	WorkflowName          string
	BatchSize             int
	ChildWorkflowTimeout  time.Duration
	GetTotalCountActivity interface{}
	ChildWorkflowFunc     interface{}
}

// GenericChildWorkflowConfig contains configuration for the generic child workflow
type GenericChildWorkflowConfig struct {
	WorkflowName            string
	ActivityBatchSize       int
	MaxConcurrentActivities int
	ActivityTimeoutMinutes  int
	ListDataActivity        interface{}
	ProcessBatchActivity    interface{}
}

// PoolError represents a pool error with pool name and error message
type PoolError struct {
	PoolName string
	Error    string
}

// GenericChildWorkflowResult represents the result of a generic child workflow
type GenericChildWorkflowResult struct {
	WorkflowID          string
	TotalItemsProcessed int
	SuccessfulItems     int
	FailedItems         int
	Error               string
	FailedPoolNames     []string
}

// GenericParentWorkflowResult represents the result of a generic parent workflow
type GenericParentWorkflowResult struct {
	TotalItemsProcessed int
	TotalSuccessful     int
	TotalFailed         int
	ChildResults        []*GenericChildWorkflowResult
}

// GenericParentWorkflow is a reusable parent workflow that orchestrates multiple child workflows
func GenericParentWorkflow(ctx workflow.Context, config GenericParentWorkflowConfig) (*GenericParentWorkflowResult, error) {
	ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{
		"workflowID": workflow.GetInfo(ctx).WorkflowExecution.ID,
		"requestID":  utils.RandomUUID(),
	})
	logger := util.GetLogger(ctx)

	retryPolicy, err := workflows.PopulateRetryPolicyParams()
	if err != nil {
		return nil, err
	}

	// Activity options for master workflow activities
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

	childWorkflowTimeout := config.ChildWorkflowTimeout
	if childWorkflowTimeout == 0 {
		childWorkflowTimeout = temporalUtils.GetWorkflowGlobalTimeout() // Use global timeout if not specified
	}
	childWorkflowOptions := workflow.ChildWorkflowOptions{
		WorkflowRunTimeout: childWorkflowTimeout,
	}
	ctx = workflow.WithChildOptions(ctx, childWorkflowOptions)

	// Get total count first
	var totalCount int
	err = workflow.ExecuteActivity(ctx, config.GetTotalCountActivity).Get(ctx, &totalCount)
	if err != nil {
		logger.Error("Failed to get total count", "Error", err)
		return nil, err
	}
	logger.Infof("Starting Parent workflow '%s' -> Processing %d total items", config.WorkflowName, totalCount)

	// Number of child workflows needed
	numChildWorkflows := (totalCount + config.BatchSize - 1) / config.BatchSize

	// Create child workflows with controlled concurrency
	childFutures := make(map[string]workflow.ChildWorkflowFuture)
	var allResults []*GenericChildWorkflowResult

	// Start all child workflows immediately with controlled concurrency
	logger.Infof("Parent workflow '%s' -> Starting all %d child workflows", config.WorkflowName, numChildWorkflows)

	// Start child workflows with chunked data fetching
	for i := 0; i < numChildWorkflows; i++ {
		offset := i * config.BatchSize
		limit := config.BatchSize
		if offset+limit > totalCount {
			limit = totalCount - offset
		}

		childWorkflowID := workflow.GetInfo(ctx).WorkflowExecution.ID + "-child-" + utils.RandomUUID()
		childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			WorkflowID: childWorkflowID,
		})

		// Pass offset and limit to child workflow for chunked fetching
		future := workflow.ExecuteChildWorkflow(childCtx, config.ChildWorkflowFunc, offset, limit)
		childFutures[childWorkflowID] = future
	}

	// Wait for all child workflows to complete
	for childWorkflowID, future := range childFutures {
		var result *GenericChildWorkflowResult
		err = future.Get(ctx, &result)

		if err != nil {
			logger.Errorf("Parent workflow '%s' -> Child workflow '%s' failed. Error: %v", config.WorkflowName, childWorkflowID, err)
			// Continue with other workflows even if one fails
			allResults = append(allResults, &GenericChildWorkflowResult{
				WorkflowID: childWorkflowID,
				Error:      err.Error(),
			})
		} else {
			allResults = append(allResults, result)
			if result != nil {
				logger.Infof("Parent workflow '%s' -> Child workflow '%s' completed. %d items processed, %d successful, %d failed", config.WorkflowName, childWorkflowID, result.TotalItemsProcessed, result.SuccessfulItems, result.FailedItems)
			}
		}
	}

	// Aggregate results
	totalProcessed := 0
	totalSuccessful := 0
	totalFailed := 0

	for _, result := range allResults {
		if result != nil {
			totalProcessed += result.TotalItemsProcessed
			totalSuccessful += result.SuccessfulItems
			totalFailed += result.FailedItems
		}
	}
	logger.Infof("Parent workflow '%s' completed. -> Total items processed: %d, successful: %d, failed: %d", config.WorkflowName, totalProcessed, totalSuccessful, totalFailed)

	return &GenericParentWorkflowResult{
		TotalItemsProcessed: totalProcessed,
		TotalSuccessful:     totalSuccessful,
		TotalFailed:         totalFailed,
		ChildResults:        allResults,
	}, nil
}

// GenericChildWorkflow is a reusable child workflow that processes data in batches
func GenericChildWorkflow(ctx workflow.Context, offset, limit int, config GenericChildWorkflowConfig) (*GenericChildWorkflowResult, error) {
	// Get parent request ID from context if available
	parentRequestID := ""
	if parentCtx := workflow.GetInfo(ctx).ParentWorkflowExecution; parentCtx != nil {
		parentRequestID = parentCtx.ID
	}
	if parentRequestID == "" {
		parentRequestID = utils.RandomUUID()
	}

	ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{
		"workflowID": workflow.GetInfo(ctx).WorkflowExecution.ID,
		"offset":     offset,
		"limit":      limit,
		"requestID":  parentRequestID,
	})
	logger := util.GetLogger(ctx)
	logger.Infof("Starting child workflow '%s' for items %d to %d", config.WorkflowName, offset, offset+limit-1)

	retryPolicy, err := workflows.PopulateRetryPolicyParams()
	if err != nil {
		return &GenericChildWorkflowResult{
			WorkflowID: workflow.GetInfo(ctx).WorkflowExecution.ID,
			Error:      err.Error(),
		}, err
	}

	// Activity options for child workflow activities
	defaultActivityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts:        1, // No retries by default
			NonRetryableErrorTypes: []string{"PanicError"},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, defaultActivityOptions)

	// Retry options specifically for ListDataActivity
	listDataRetryOptions := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        retryPolicy.InitialInterval,
			BackoffCoefficient:     retryPolicy.BackoffCoefficient,
			MaximumInterval:        retryPolicy.MaximumInterval,
			MaximumAttempts:        int32(retryPolicy.MaximumAttempts),
			NonRetryableErrorTypes: []string{"PanicError"},
		},
	}
	ctxWithListDataRetry := workflow.WithActivityOptions(ctx, listDataRetryOptions)

	// Fetch all assigned data in one database call using generic approach
	var totalItemsProcessed int
	var allResults []interface{}
	var fetchedData interface{}

	err = workflow.ExecuteActivity(ctxWithListDataRetry, config.ListDataActivity, offset, limit).Get(ctx, &fetchedData)
	if err != nil {
		errorMsg := fmt.Sprintf("ListData activity failed. Error: %v, Offset: %d, Limit: %d", err, offset, limit)
		logger.Errorf("Child workflow '%s' -> %s", config.WorkflowName, errorMsg)
		return &GenericChildWorkflowResult{
			WorkflowID: workflow.GetInfo(ctx).WorkflowExecution.ID,
			Error:      err.Error(),
		}, err
	}

	// Process the fetched data generically
	totalItemsProcessed, allResults = processGenericData(ctx, fetchedData, config, logger)
	logger.Debugf("Child workflow '%s' -> fetched %d items in one call, processing with max %d concurrent activities", config.WorkflowName, totalItemsProcessed, config.MaxConcurrentActivities)

	// Aggregate results from all completed activities
	totalSuccessCount := 0
	totalFailureCount := 0
	var allFailedPoolNames []string
	var allFailedPoolErrors []PoolError

	for _, result := range allResults {
		if resultMap, ok := result.(map[string]interface{}); ok {
			if val, exists := resultMap["Successful"]; exists {
				if successful, ok := val.(float64); ok {
					totalSuccessCount += int(successful)
				}
			}
			if val, exists := resultMap["Failed"]; exists {
				if failed, ok := val.(float64); ok {
					totalFailureCount += int(failed)
				}
			}
			// Collect failed pool names
			if val, exists := resultMap["FailedPoolNames"]; exists {
				if failedPoolNames, ok := val.([]interface{}); ok {
					for _, poolName := range failedPoolNames {
						if name, ok := poolName.(string); ok {
							allFailedPoolNames = append(allFailedPoolNames, name)
						}
					}
				}
			}
			// Collect failed pool errors (for logging, not returned in workflow result)
			if val, exists := resultMap["FailedPoolErrors"]; exists {
				if failedPoolErrors, ok := val.([]interface{}); ok {
					for _, poolError := range failedPoolErrors {
						if poolErrorMap, ok := poolError.(map[string]interface{}); ok {
							poolName, _ := poolErrorMap["PoolName"].(string)
							errorMsg, _ := poolErrorMap["Error"].(string)
							allFailedPoolErrors = append(allFailedPoolErrors, PoolError{
								PoolName: poolName,
								Error:    errorMsg,
							})
						}
					}
				}
			}
		} else {
			logger.Warnf("Child Workflow '%s' -> Unexpected result type: %T", config.WorkflowName, result)
		}
	}
	logger.Infof("Child workflow '%s' completed -> Total items processed: %d, successful: %d, failed: %d", config.WorkflowName, totalItemsProcessed, totalSuccessCount, totalFailureCount)

	return &GenericChildWorkflowResult{
		WorkflowID:          workflow.GetInfo(ctx).WorkflowExecution.ID,
		TotalItemsProcessed: totalItemsProcessed,
		SuccessfulItems:     totalSuccessCount,
		FailedItems:         totalFailureCount,
		FailedPoolNames:     allFailedPoolNames,
	}, nil
}

// processGenericData processes any slice type generically
func processGenericData(ctx workflow.Context, fetchedData interface{}, config GenericChildWorkflowConfig, logger log.Logger) (int, []interface{}) {
	var allResults []interface{}

	items := reflect.ValueOf(fetchedData)
	if items.Kind() != reflect.Slice {
		errorMsg := fmt.Sprintf("Child Workflow '%s'-> expected slice, got %T", config.WorkflowName, fetchedData)
		logger.Errorf(errorMsg)
		return 0, allResults
	}

	totalItems := items.Len()
	currentIndex := 0

	// Process items in controlled batches to limit RPS
	for currentIndex < totalItems {
		// Process a batch of activities with controlled concurrency
		var batchFutures []workflow.Future
		var batchDataBatches [][]interface{}

		// Start up to MaxConcurrentActivities activities
		for i := 0; i < config.MaxConcurrentActivities && currentIndex < totalItems; i++ {
			batchSize := config.ActivityBatchSize
			if currentIndex+batchSize > totalItems {
				batchSize = totalItems - currentIndex
			}

			// Extract a batch of items from the fetched list
			dataBatch := make([]interface{}, batchSize)
			for j := 0; j < batchSize; j++ {
				dataBatch[j] = items.Index(currentIndex + j).Interface()
			}
			batchDataBatches = append(batchDataBatches, dataBatch)
			logger.Infof("Child Workflow '%s'-> Batch %d with %d items (index: %d to %d)", config.WorkflowName, i+1, len(dataBatch), currentIndex, currentIndex+len(dataBatch)-1)

			// Process this batch immediately
			future := workflow.ExecuteActivity(ctx, config.ProcessBatchActivity, dataBatch)
			batchFutures = append(batchFutures, future)

			currentIndex += len(dataBatch)
		}

		// Wait for current batch of activities to complete
		if len(batchFutures) > 0 {
			for i, future := range batchFutures {
				dataBatch := batchDataBatches[i]
				var batchResult interface{}
				err := future.Get(ctx, &batchResult)

				if err != nil {
					errorMsg := fmt.Sprintf("Child Workflow '%s' -> Batch %d failed: %v", config.WorkflowName, i, err)
					logger.Errorf(errorMsg)
					allResults = append(allResults, map[string]interface{}{
						"error":      err.Error(),
						"batch_size": len(dataBatch),
					})
				} else {
					allResults = append(allResults, batchResult)
				}
			}
		}
	}
	return totalItems, allResults
}
