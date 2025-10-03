# Control Workflows

This document describes the control workflows in the VSA Control Plane system, including workflow orchestration, sequencing, and control operations.

## Overview

Control workflows manage workflow orchestration and sequencing operations in the VSA Control Plane. These workflows provide coordination and control mechanisms for executing multiple workflows in sequence or parallel.

## Workflow Types

### 1. Sequence Workflow

**File**: `core/orchestrator/workflows/control_workflow.go`

**Purpose**: Executes child workflows sequentially based on signals received.

**Entry Point**: `SequenceWorkflow(ctx workflow.Context)`

#### Workflow Structure

```go
type SignalWorkflowParams struct {
    Function string
    Args     []interface{}
    Options  workflow.ChildWorkflowOptions
}
```

#### Constants

**Workflow ID Patterns**:
```go
const (
    // VolumeCreateDeleteSnapshotDeleteSeq is a placeholder used for sequence workflow instance that runs all
    // volume CREATE & DELETE operation and snapshot DELETE calls for a specific pool sequentially.
    VolumeCreateDeleteSnapshotDeleteSeq = "Account_%d_Location_%s_Pool_%s_Ops_Volume-CD-Snapshot-D"

    // PoolSubnetCreate is a placeholder used for sequence workflow instance that runs all
    // subnet create operation for a specific account and VPC sequentially.
    PoolSubnetCreate = "Account_%d_VPC_%s_Ops_PoolSubnet-C"

    // Signal is the name of the signal used to call sequential workflows.
    Signal = "req"
)
```

#### Execution Flow

1. **Signal Reception**:
   - Listen for signals on the `req` channel
   - Receive `SignalWorkflowParams` containing workflow details
   - Extract workflow function, arguments, and options

2. **Child Workflow Execution**:
   - Execute child workflow with provided parameters
   - Handle child workflow execution errors
   - Log execution results

3. **Timeout Management**:
   - Use 3-second timer for idle timeout
   - Check for pending signals before exiting
   - Exit workflow if no signals received

4. **Error Handling**:
   - Log child workflow execution errors
   - Continue processing other signals
   - Maintain workflow state

#### Signal Processing

```go
selector.AddReceive(signalChan, func(c workflow.ReceiveChannel, more bool) {
    var signalWf SignalWorkflowParams
    c.Receive(ctx, &signalWf)

    ctx = workflow.WithChildOptions(ctx, signalWf.Options)
    if err := workflow.ExecuteChildWorkflow(ctx, signalWf.Function, signalWf.Args...).Get(ctx, nil); err != nil {
        logger.Error("Failed to execute child workflow", "error", err)
        return
    }
})
```

#### Timeout Handling

```go
timeout := workflow.NewTimer(ctx, 3*time.Second)

selector.AddFuture(timeout, func(f workflow.Future) {
    exitFlag = true
})

if exitFlag {
    if selector.HasPending() {
        continue
    }
    logger.Info("Current value reached threshold, exiting workflow")
    break
}
```

### 2. Execute Workflow Sequentially

**Purpose**: Sends signals to sequence workflows to execute child workflows sequentially.

**Entry Point**: `ExecuteWorkflowSequentially(temporal client.Client, ctx context.Context, sequenceWfOptions client.StartWorkflowOptions, workflowFunc interface{}, args ...interface{})`

#### Parameters

- **temporal**: Temporal client instance
- **ctx**: Go context
- **sequenceWfOptions**: Workflow start options for the sequence workflow
- **workflowFunc**: Function to execute as child workflow
- **args**: Arguments for the child workflow

#### Execution Flow

1. **Sequence Workflow Check**:
   - Check if sequence workflow is already running
   - Start new instance if not running
   - Use provided workflow options

2. **Signal Sending**:
   - Send signal to sequence workflow
   - Include workflow function and arguments
   - Handle signal sending errors

3. **Workflow Management**:
   - Manage sequence workflow lifecycle
   - Handle workflow execution errors
   - Provide execution status

## Workflow Orchestration

### Workflow Sequencing

**Sequential Execution**:
- Execute workflows one after another
- Maintain execution order
- Handle dependencies between workflows

**Parallel Execution**:
- Execute multiple workflows simultaneously
- Coordinate parallel execution
- Handle parallel workflow results

### Workflow Control

**Workflow Lifecycle Management**:
- Start workflows
- Monitor workflow execution
- Handle workflow completion
- Manage workflow failures

**Workflow Coordination**:
- Coordinate between parent and child workflows
- Handle workflow dependencies
- Manage workflow state transitions

## Error Handling

### Error Types

**Non-Retryable Errors**:
- `PanicError`: System panic errors
- `ValidationError`: Parameter validation errors
- `WorkflowNotFoundError`: Missing workflow errors

**Retryable Errors**:
- `NetworkTimeoutError`: Network operation timeouts
- `WorkflowExecutionError`: Workflow execution failures
- `SignalError`: Signal sending failures

### Error Recovery

**Workflow Error Recovery**:
- Retry failed workflows
- Handle workflow timeouts
- Recover from workflow failures
- Maintain workflow state consistency

## Configuration

### Workflow Options

**Sequence Workflow Options**:
- **Workflow ID**: Unique identifier for sequence workflow
- **Task Queue**: Task queue for workflow execution
- **Workflow Run Timeout**: Maximum execution time
- **Workflow ID Reuse Policy**: Policy for workflow ID reuse

**Child Workflow Options**:
- **Workflow Execution Timeout**: Child workflow timeout
- **Task Queue**: Task queue for child workflow
- **Workflow ID**: Child workflow identifier

### Signal Configuration

**Signal Parameters**:
- **Signal Name**: Name of the signal channel
- **Signal Payload**: Data sent with signal
- **Signal Timeout**: Maximum time to wait for signal

## Monitoring and Metrics

### Control Workflow Metrics

**Available Metrics**:
- Sequence workflow execution duration
- Child workflow execution success rate
- Signal processing metrics
- Workflow orchestration metrics

### Health Checks

**Control Workflow Health Monitoring**:
- Sequence workflow status
- Child workflow execution status
- Signal processing status
- Workflow coordination status

## Performance Considerations

### Control Workflow Performance

**Optimization Features**:
- Parallel signal processing
- Efficient workflow coordination
- Resource utilization monitoring
- Workflow execution optimization

### Resource Management

**Resource Limits**:
- Concurrent workflow execution
- Signal processing limits
- Memory usage limits
- Network bandwidth limits

## Security Considerations

### Control Workflow Security

**Access Control**:
- Role-based access control
- Workflow execution permissions
- Audit logging for all operations

### Workflow Security

**Workflow Protection**:
- Secure workflow execution
- Workflow data protection
- Secure signal transmission
- Workflow state protection

## Testing

### Test Coverage

Each control workflow has comprehensive test coverage:

- **Unit Tests**: Individual workflow function testing
- **Integration Tests**: End-to-end workflow testing
- **Mock Activities**: Mock implementations for testing
- **Error Scenarios**: Testing failure and retry scenarios
- **Signal Processing Tests**: Testing signal handling logic

### Test Files

- `control_workflow_test.go`: Main test file
- Mock implementations for all activities
- Test data fixtures for various scenarios

## Related Documentation

- [Volume Workflows](../core/volume-workflows.md)
- [Pool Workflows](../core/pool-workflows.md)
- [Temporal Debugging Guide](../../guides/temporal-debugging.md)