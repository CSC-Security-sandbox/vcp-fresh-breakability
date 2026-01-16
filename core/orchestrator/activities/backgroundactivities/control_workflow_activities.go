package backgroundactivities

import (
	"context"
	"fmt"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
)

// ExecuteWorkflowSequentiallyFunc is a function type for executing workflows sequentially
type ExecuteWorkflowSequentiallyFunc func(
	temporal client.Client,
	ctx context.Context,
	sequenceWfOptions client.StartWorkflowOptions,
	wfFunction interface{},
	wfOptions workflow.ChildWorkflowOptions,
	wfArgs ...interface{},
) error

// ExecuteWorkflowSequentially is a variable that holds the function to execute workflows sequentially.
// This is set by RegisterWorkflowExecutor to avoid circular dependencies.
// It can also be mocked in tests.
var ExecuteWorkflowSequentially ExecuteWorkflowSequentiallyFunc

// RegisterWorkflowExecutor sets the function for executing workflows sequentially.
// This is called by the workflows package to register its implementation.
func RegisterWorkflowExecutor(fn ExecuteWorkflowSequentiallyFunc) {
	ExecuteWorkflowSequentially = fn
}

// ControlWorkflowActivity provides activities for executing workflows through control workflows
type ControlWorkflowActivity struct{}

// ExecutePoolCertificateRotationSequentially executes certificate rotation for a pool through the control workflow.
// This ensures only one workflow runs at a time on the pool, preventing race conditions.
func (a *ControlWorkflowActivity) ExecutePoolCertificateRotationSequentially(ctx context.Context, poolUUID string, workflowTimeout time.Duration) error {
	logger := util.GetLogger(ctx)
	logger.Infof("Executing certificate rotation sequentially for pool: %s", poolUUID)

	// Get temporal client
	temporalClient, err := workflowengine.FetchTemporalClient()
	if err != nil {
		logger.Errorf("Failed to get Temporal client: %v", err)
		return err
	}

	// Generate control workflow ID based on pool UUID
	// All pool operations for this pool will use the same control workflow ID
	controlWorkflowID := fmt.Sprintf(common.PoolOperationsSeq, poolUUID)

	// Generate unique workflow ID for this specific certificate rotation execution
	childWorkflowID := fmt.Sprintf("PoolCertRotation_%s_%d", poolUUID, time.Now().UnixNano())

	logger.Infof("Using control workflow ID: %s for pool: %s", controlWorkflowID, poolUUID)
	logger.Infof("Certificate rotation workflow ID: %s", childWorkflowID)

	// Execute the certificate rotation workflow through the control workflow
	// This ensures sequential execution with other pool operations
	err = ExecuteWorkflowSequentially(
		temporalClient,
		ctx,
		client.StartWorkflowOptions{
			TaskQueue: workflowengine.CustomerTaskQueue,
			ID:        controlWorkflowID,
		},
		common.RotatePoolCertificateWorkflowName,
		workflow.ChildWorkflowOptions{
			TaskQueue:          workflowengine.CustomerTaskQueue,
			WorkflowID:         childWorkflowID,
			WorkflowRunTimeout: workflowTimeout,
		},
		poolUUID,
	)

	if err != nil {
		logger.Errorf("Failed to execute certificate rotation sequentially for pool %s: %v", poolUUID, err)
		return err
	}

	logger.Infof("Successfully queued certificate rotation for pool: %s", poolUUID)
	return nil
}

// ExecutePoolPasswordRotationSequentially executes password rotation for a pool through the control workflow.
// This ensures only one workflow runs at a time on the pool, preventing race conditions.
func (a *ControlWorkflowActivity) ExecutePoolPasswordRotationSequentially(ctx context.Context, poolUUID string, workflowTimeout time.Duration) error {
	logger := util.GetLogger(ctx)
	logger.Infof("Executing password rotation sequentially for pool: %s", poolUUID)

	// Get temporal client
	temporalClient, err := workflowengine.FetchTemporalClient()
	if err != nil {
		logger.Errorf("Failed to get Temporal client: %v", err)
		return err
	}

	// Generate control workflow ID based on pool UUID
	controlWorkflowID := fmt.Sprintf(common.PoolOperationsSeq, poolUUID)

	// Generate unique workflow ID for this specific password rotation execution
	childWorkflowID := fmt.Sprintf("PoolPasswordRotation_%s_%d", poolUUID, time.Now().UnixNano())

	logger.Infof("Using control workflow ID: %s for pool: %s", controlWorkflowID, poolUUID)
	logger.Infof("Password rotation workflow ID: %s", childWorkflowID)

	// Execute the password rotation workflow through the control workflow
	err = ExecuteWorkflowSequentially(
		temporalClient,
		ctx,
		client.StartWorkflowOptions{
			TaskQueue: workflowengine.CustomerTaskQueue,
			ID:        controlWorkflowID,
		},
		common.RotatePoolPasswordWorkflowName,
		workflow.ChildWorkflowOptions{
			TaskQueue:          workflowengine.CustomerTaskQueue,
			WorkflowID:         childWorkflowID,
			WorkflowRunTimeout: workflowTimeout,
		},
		poolUUID,
	)

	if err != nil {
		logger.Errorf("Failed to execute password rotation sequentially for pool %s: %v", poolUUID, err)
		return err
	}

	logger.Infof("Successfully queued password rotation for pool: %s", poolUUID)
	return nil
}
