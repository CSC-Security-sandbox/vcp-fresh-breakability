# Workflow Cancellation Framework

## Overview

This document describes the generic workflow cancellation framework that enables graceful cancellation of long-running create workflows when a delete request is received for a resource that is still in the `CREATING` state. The framework is designed to be reusable across all resource types (pools, volumes, backups, etc.) and provides a standardized approach to handling cancellation signals in Temporal workflows.

## Purpose

The cancellation framework addresses the need to:
- Gracefully cancel ongoing resource creation workflows when deletion is requested
- Ensure proper cleanup of partially created resources through rollback mechanisms
- Provide a consistent, reusable pattern for cancellation handling across all resource types
- Support scenarios where external systems (like CCFE) trigger cleanup calls while resources are still being created

## Design Documentation

- [Acknowledge the Deletion call for All Resources when CCFE timed out](https://confluence.ngage.netapp.com/spaces/~harihar2/pages/1373860712/Acknowledge+the+Deletion+call+for+All+Resources+when+CCFE+timed+out)

## Architecture

### Core Components

The framework consists of two main components:

1. **Cancellation Common Library** (`core/orchestrator/common/cancellation.go`)
   - Provides reusable cancellation handling infrastructure
   - Defines interfaces and helper functions for cancellation operations
   - Implements the generic cancellation handler for delete workflows

2. **Cancellation Activities** (`core/orchestrator/activities/cancellation_activities.go`)
   - Provides Temporal activities for cancellation operations
   - Wraps Temporal client operations for workflow cancellation
   - Implements the `CancellationActivityMethods` interface

## Component Details

### 1. Cancellation Common Library

#### WorkflowCancellationHandler

The `WorkflowCancellationHandler` manages cancellation signal reception in create workflows.

**Location**: `core/orchestrator/common/cancellation.go`

**Key Methods**:

- **`NewWorkflowCancellationHandler(ctx, signalName, resourceUUID, resourceName)`**
  - Creates a new cancellation handler for a workflow
  - Sets up a signal channel to receive cancellation signals
  - Parameters:
    - `ctx`: Workflow context
    - `signalName`: Name of the cancellation signal (defaults to `"cancel-resource-creation"` if empty)
    - `resourceUUID`: UUID of the resource being created
    - `resourceName`: Human-readable name of the resource type (e.g., "pool", "volume")

- **`CheckCancellation(ctx)`**
  - Non-blocking check for cancellation signals at workflow checkpoints
  - Should be called before starting new activities
  - Returns an error if cancellation was detected, nil otherwise
  - Uses Temporal's selector pattern for non-blocking signal reception

- **`IsCancelled()`**
  - Returns `true` if cancellation was detected, `false` otherwise
  - Can be used in defer blocks to determine if rollback should be executed

**Usage Example** (from Pool Create Workflow):

```go
// Initialize cancellation handler
cancellationHandler := common.NewWorkflowCancellationHandler(
    ctx, 
    "cancel-pool-creation", 
    dbPool.UUID, 
    "pool",
)

// Helper function to check for cancellation
checkCancellation := func() *vsaerrors.CustomError {
    if err := cancellationHandler.CheckCancellation(ctx); err != nil {
        return ConvertToVSAError(vsaerrors.New(err.Error()))
    }
    return nil
}

// Check before starting activities
if cancelErr := checkCancellation(); cancelErr != nil {
    return nil, cancelErr
}

// Execute activity
err = workflow.ExecuteActivity(ctx, someActivity).Get(ctx, &result)
```

#### HandleCancellationInDeleteWorkflow

The `HandleCancellationInDeleteWorkflow` function provides a generic handler for delete workflows to cancel ongoing create workflows.

**Signature**:

```go
func HandleCancellationInDeleteWorkflow(
    ctx workflow.Context,
    params WorkflowCancellationParams,
    getCreateJobActivity interface{}, // Activity function
    cancellationActivity CancellationActivityMethods,
    commonActivity CommonActivityMethods,
) error
```

**Parameters**:

- **`params`**: `WorkflowCancellationParams` containing:
  - `ResourceUUID`: UUID of the resource being deleted
  - `CorrelationID`: Correlation ID from the delete request
  - `CreateJobType`: Type of the create job (e.g., `models.JobTypeCreatePool`)
  - `SignalName`: Name of the cancellation signal (defaults to `"cancel-resource-creation"`)
  - `CancellationAckTimeout`: Timeout for graceful cancellation (default: 5 minutes)
  - `ForceTerminationAckTimeout`: Timeout for force termination acknowledgment (default: 30 seconds)

- **`getCreateJobActivity`**: Activity function that retrieves the create job
  - Signature: `func(ctx, resourceUUID, correlationID, jobType string) (*CreateJobResult, error)`
  - Should return the create job's UUID and workflow ID

- **`cancellationActivity`**: Implements `CancellationActivityMethods` interface
  - Provides activities for checking workflow status, sending signals, waiting for acknowledgment, and force termination

- **`commonActivity`**: Implements `CommonActivityMethods` interface
  - Provides activity for updating job status

**Flow**:

1. Validates parameters and sets defaults
2. Retrieves the create job using the provided activity
3. Checks if the create workflow is still running
4. If running:
   - Sends cancellation signal to the create workflow
   - Waits for graceful cancellation acknowledgment (with timeout)
   - If timeout exceeded, forcefully terminates the workflow
   - Waits for force termination acknowledgment
5. Updates create job state to `ERROR` with cancellation details
6. Returns nil (errors are logged but don't block deletion)

**Usage Example** (from Pool Delete Workflow):

```go
cancellationParams := common.WorkflowCancellationParams{
    ResourceUUID:               dbPool.UUID,
    CorrelationID:              correlationID,
    CreateJobType:              models.JobTypeCreatePool,
    SignalName:                 "cancel-pool-creation",
    CancellationAckTimeout:     time.Duration(5) * time.Minute,
    ForceTerminationAckTimeout: time.Duration(30) * time.Second,
}

cancellationActivity := &activities.CancellationActivity{}
commonActivity := &activities.CommonActivities{}

if dbPool.State == models.LifeCycleStateCreating {
    if cancelErr := common.HandleCancellationInDeleteWorkflow(
        ctx, 
        cancellationParams, 
        poolActivity.GetCreateJobByResourceUUID, 
        cancellationActivity, 
        commonActivity,
    ); cancelErr != nil {
        wf.Logger.Warnf("Error handling cancellation: %v, proceeding with deletion", cancelErr)
    }
}
```

#### Helper Functions

**`IsWorkflowRunning(ctx, temporalClient, workflowID)`**
- Checks if a workflow is still running
- Uses Temporal's `DescribeWorkflowExecution` API
- Returns `true` if workflow status is `RUNNING`, `false` otherwise

**`WaitForWorkflowCancellationAck(ctx, temporalClient, workflowID, timeout)`**
- Waits for a workflow to complete or cancel with a timeout
- Returns `true` if workflow completed/cancelled, `false` if timeout
- Handles various error conditions gracefully

### 2. Cancellation Activities

The `CancellationActivity` struct provides Temporal activities for cancellation operations.

**Location**: `core/orchestrator/activities/cancellation_activities.go`

**Key Activities**:

- **`IsWorkflowRunningActivity(ctx, workflowID)`**
  - Checks if a workflow is still running
  - Wraps `common.IsWorkflowRunning()`
  - Returns `(bool, error)`

- **`SendCancelSignalActivity(ctx, workflowID, signalName, signalData)`**
  - Sends a cancellation signal to a workflow
  - Uses Temporal's `SignalWorkflow` API
  - Returns error if signal sending fails

- **`WaitForWorkflowCancellationAckActivity(ctx, workflowID, timeout)`**
  - Waits for a workflow to be cancelled or completed
  - Wraps `common.WaitForWorkflowCancellationAck()`
  - Returns `(bool, error)` - `true` if acknowledged, `false` if timeout

- **`ForceCancelWorkflowActivity(ctx, workflowID)`**
  - Forcefully terminates a workflow and its child workflows
  - Uses Temporal's `TerminateWorkflow` API
  - Automatically terminates all child workflows
  - Returns error if termination fails

**Temporal Client Handling**:

The activity can work with either:
- A Temporal client provided during initialization (`NewCancellationActivity(temporalClient)`)
- A Temporal client retrieved from the activity context (`activity.GetClient(ctx)`)

This flexibility allows the activity to work in different deployment scenarios.

## Interfaces

### CancellationActivityMethods

Interface that must be implemented by cancellation activities:

```go
type CancellationActivityMethods interface {
    IsWorkflowRunningActivity(ctx context.Context, workflowID string) (bool, error)
    SendCancelSignalActivity(ctx context.Context, workflowID string, signalName string, signalData string) error
    WaitForWorkflowCancellationAckActivity(ctx context.Context, workflowID string, timeout time.Duration) (bool, error)
    ForceCancelWorkflowActivity(ctx context.Context, workflowID string) error
}
```

### CommonActivityMethods

Interface for common activities needed by the cancellation handler:

```go
type CommonActivityMethods interface {
    UpdateJobStatus(ctx context.Context, job *datamodel.Job) error
}
```

## Configuration

### Default Values

- **Default Signal Name**: `"cancel-resource-creation"`
- **Default Cancellation Timeout**: 5 minutes
- **Default Force Termination Timeout**: 30 seconds

### Customization

Resource-specific implementations can customize:
- Signal names (e.g., `"cancel-pool-creation"`, `"cancel-volume-creation"`)
- Timeout values based on resource creation duration
- Checkpoint placement in create workflows

## Integration Pattern

### For Create Workflows

1. **Initialize Handler**:
   ```go
   cancellationHandler := common.NewWorkflowCancellationHandler(
       ctx, 
       resourceSpecificSignalName, 
       resourceUUID, 
       resourceName,
   )
   ```

2. **Add Checkpoints**:
   - Before starting new activities
   - After major activity completions
   - At workflow decision points

3. **Handle Cancellation in Defer**:
   ```go
   defer func() {
       if err != nil || cancellationHandler.IsCancelled() {
           if cancellationHandler.IsCancelled() {
               // Execute rollback for cancellation
               rollbackManager.ExecuteRollback(ctx, cancellationError)
           } else {
               // Execute rollback for error
               rollbackManager.ExecuteRollback(ctx, err)
           }
       }
   }()
   ```

### For Delete Workflows

1. **Check Resource State**:
   ```go
   if resource.State == models.LifeCycleStateCreating {
       // Handle cancellation
   }
   ```

2. **Prepare Parameters**:
   ```go
   cancellationParams := common.WorkflowCancellationParams{
       ResourceUUID:               resourceUUID,
       CorrelationID:              correlationID,
       CreateJobType:              models.JobTypeCreateResource,
       SignalName:                 resourceSpecificSignalName,
       CancellationAckTimeout:     customTimeout,
       ForceTerminationAckTimeout:  customForceTimeout,
   }
   ```

3. **Invoke Handler**:
   ```go
   cancellationActivity := &activities.CancellationActivity{}
   commonActivity := &activities.CommonActivities{}
   
   err := common.HandleCancellationInDeleteWorkflow(
       ctx,
       cancellationParams,
       resourceActivity.GetCreateJobByResourceUUID,
       cancellationActivity,
       commonActivity,
   )
   ```

## Best Practices

### Checkpoint Placement

- Place checkpoints **before** starting new activities that create resources
- Place checkpoints **after** completing major phases of resource creation
- Avoid placing checkpoints inside tight loops or high-frequency operations
- Balance between responsiveness and overhead

### Signal Naming

- Use resource-specific signal names for clarity (e.g., `"cancel-pool-creation"`)
- Follow naming convention: `"cancel-{resource-type}-creation"`
- Document signal names in resource-specific documentation

### Timeout Configuration

- Set `CancellationAckTimeout` based on typical resource creation duration
- Set `ForceTerminationAckTimeout` to a short duration (30 seconds is usually sufficient)
- Consider resource complexity when setting timeouts
- Make timeouts configurable via environment variables

### Error Handling

- The cancellation handler logs errors but doesn't block deletion
- Create workflows should handle cancellation gracefully
- Rollback should be idempotent and handle partial resource creation
- Job state updates should always be attempted, even if cancellation fails

### Child Workflow Cancellation

- Configure child workflows with `PARENT_CLOSE_POLICY_REQUEST_CANCEL`
- This ensures child workflows are cancelled when parent is cancelled
- Prevents orphaned child workflows

**Example**:

```go
childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
    ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
})
err = workflow.ExecuteChildWorkflow(childCtx, ChildWorkflow, args).Get(ctx, &result)
```

### Key Design Decisions

1. **Native Temporal Support**: Uses Temporal's built-in cancellation mechanism designed specifically for this use case
2. **Immediate Response**: Signals are delivered asynchronously but reliably, allowing the create workflow to respond promptly
3. **Better Resource Management**: The create workflow can execute its rollback logic in a controlled manner
4. **Reduced Database Load**: Uses Temporal's event-driven architecture instead of periodic database polling
5. **Reusable Framework**: Built on a generic cancellation framework that can be extended to other resources

### Pool-Specific Implementation

#### 1. Create Pool Workflow Modifications

The create pool workflow (`core/orchestrator/workflows/pool_workflows.go`) has been enhanced with:

- **Cancellation Handler Setup**: Initializes `WorkflowCancellationHandler` at workflow start
- **Checkpoint Integration**: `CheckCancellation()` calls at strategic points:
    - Before starting new activities
    - After major activity completions
    - At workflow decision points
- **Rollback on Cancellation**: When cancellation is detected, the workflow:
    - Stops executing new activities
    - Initiates rollback of partially created resources via `RollbackManager`
    - Updates pool state appropriately
- **Child Workflow Cancellation**: Child workflows use `PARENT_CLOSE_POLICY_REQUEST_CANCEL` to ensure they are cancelled when parent is cancelled

#### 4. Delete Pool Workflow Modifications

The delete pool workflow has been enhanced with:

- **State Detection**: Checks if pool is in `CREATING` state
- **Correlation ID Validation**: Validates that delete request has matching correlation ID with create request
- **Cancellation Handling**: Invokes `HandleCancellationInDeleteWorkflow()` when pool is in `CREATING` state
- **Conditional Cleanup**: Skips cleanup activities for resources that haven't been created yet

#### 5. Pool Orchestrator Modifications (`core/orchestrator/pool.go`)

The `_deletePool` function has been enhanced with:

- **Correlation ID Validation**: When pool is in `CREATING` state:
    - Validates correlation ID is present
    - Retrieves create job and validates correlation ID matches
    - Skips state update to `DELETING` if correlation IDs match (cancellation will be handled in workflow)

## Testing

### Unit Testing

The framework includes comprehensive unit tests:

- **`cancellation_test.go`**: Tests for cancellation handler and helper functions
- **`cancellation_activities_test.go`**: Tests for cancellation activities

### Integration Testing

When implementing cancellation for a new resource:

1. Test cancellation at various checkpoints
2. Test graceful cancellation acknowledgment
3. Test force termination scenarios
4. Test rollback execution on cancellation
5. Test child workflow cancellation
6. Test error handling and edge cases

## Extensibility

The framework is designed to be extensible:

- **New Resource Types**: Simply implement the required activities and use the framework
- **Custom Signal Names**: Use resource-specific signal names
- **Custom Timeouts**: Configure timeouts per resource type
- **Additional Activities**: Extend `CancellationActivityMethods` interface if needed

## Example: Pool Resource

The pool resource serves as the reference implementation:

- **Signal Name**: `"cancel-pool-creation"`
- **Checkpoints**: 12+ strategic checkpoints throughout create workflow
- **Timeouts**: Configurable via `POOL_WORKFLOW_CANCELLATION_TIMEOUT` and `POOL_WORKFLOW_FORCE_CANCEL_WAIT_TIMEOUT`
- **Rollback**: Comprehensive rollback via `RollbackManager`
- **Child Workflows**: `DataSubnetSequentialPoller` and `ConfigureNetworkWorkflow` use `REQUEST_CANCEL` policy

## Related Documentation

- [Temporal Documentation: Workflow Cancellation](https://docs.temporal.io/workflows#cancellation)
- [Temporal Documentation: Signals](https://docs.temporal.io/workflows#signals)

## Summary

The workflow cancellation framework provides a robust, reusable solution for handling cancellation of long-running create workflows. By leveraging Temporal's native cancellation mechanisms and providing a standardized interface, the framework enables consistent cancellation handling across all resource types while maintaining flexibility for resource-specific customization.

