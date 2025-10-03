# Queuing Mechanism for Jobs

## Context

Certain flows in VSA interact and try to change common resources. For example, CREATE VOLUME and DELETE VOLUME both try to change the common resource, i.e., host group/i-group. The call to these flows can come almost concurrently. To avoid an unexpected outcome, such calls need to be executed in a sequential manner. Hence, we need a queuing mechanism in VSA.

The VSA Control Plane requires a robust queuing mechanism to handle concurrent operations on shared resources while maintaining data consistency and preventing race conditions.

## Decision

We implement a queuing mechanism using Temporal's built-in capabilities to ensure sequential execution of workflows that operate on common resources. The solution leverages Temporal's SignalWithStartWorkflow, Parent-Child Workflow relationships, and Selectors to create a control workflow that manages sequential execution.

### Key Components

1. **Control Workflow (SequenceWorkflow)**: A long-running workflow that listens for signals and executes child workflows sequentially
2. **SignalWithStartWorkflow**: Ensures signals are delivered to the same control workflow instance
3. **Workflow ID Patterns**: Structured naming convention for control workflow identification
4. **Resource-Level Queuing**: Queue operations based on resource identifiers (pool, volume, etc.)

## Implementation Details

### Control Workflow Architecture

```go
// SequenceWorkflow is a workflow that listens for signals and executes child workflows sequentially.
func SequenceWorkflow(ctx workflow.Context) error {
    logger := workflow.GetLogger(ctx)
    exitFlag := false
    signalChan := workflow.GetSignalChannel(ctx, Signal)
    
    // Timer to check if workflow should exit when idle
    timeout := workflow.NewTimer(ctx, 3*time.Second)
    
    for {
        selector := workflow.NewSelector(ctx)
        
        selector.AddReceive(signalChan, func(c workflow.ReceiveChannel, more bool) {
            var signalWf SignalWorkflowParams
            c.Receive(ctx, &signalWf)
            
            ctx = workflow.WithChildOptions(ctx, signalWf.Options)
            if err := workflow.ExecuteChildWorkflow(ctx, signalWf.Function, signalWf.Args...).Get(ctx, nil); err != nil {
                logger.Error("Failed to execute child workflow", "error", err)
                return
            }
        })
        
        selector.AddFuture(timeout, func(f workflow.Future) {
            exitFlag = true
        })
        
        selector.Select(ctx)
        if exitFlag {
            if selector.HasPending() {
                continue
            }
            logger.Info("Current value reached threshold, exiting workflow")
            break
        }
    }
    return nil
}
```

### Signal Parameters

```go
type SignalWorkflowParams struct {
    Function string
    Args     []interface{}
    Options  workflow.ChildWorkflowOptions
}
```

### Workflow ID Patterns

The system uses structured workflow ID patterns to ensure proper resource isolation:

```go
const (
    // Volume operations for a specific pool
    VolumeCreateDeleteSnapshotDeleteSeq = "Account_%d_Location_%s_Pool_%s_Ops_Volume-CD-Snapshot-D"
    
    // Pool subnet operations for a specific account and VPC
    PoolSubnetCreate = "Account_%d_VPC_%s_Ops_PoolSubnet-C"
    
    // Signal name for sequential workflows
    Signal = "req"
)
```

### Sequential Execution Interface

```go
func ExecuteWorkflowSequentially(
    temporal client.Client, 
    ctx context.Context, 
    sequenceWfOptions client.StartWorkflowOptions, 
    wfFunction interface{}, 
    wfOptions workflow.ChildWorkflowOptions, 
    wfArgs ...interface{}
) error {
    // Validate parameters
    if err := validateWorkflowParams(sequenceWfOptions.ID, wfOptions); err != nil {
        return customerrors.New(fmt.Sprintf("Invalid parameters for sequence workflow execution, error: %v", err))
    }
    
    // Set default task queues
    if wfOptions.TaskQueue == "" {
        wfOptions.TaskQueue = workflowengine.CustomerTaskQueue
    }
    if sequenceWfOptions.TaskQueue == "" {
        sequenceWfOptions.TaskQueue = workflowengine.CustomerTaskQueue
    }
    
    // SignalWithStartWorkflow ensures atomic signal delivery
    _, err := temporal.SignalWithStartWorkflow(
        ctx,
        sequenceWfOptions.ID,
        Signal,
        SignalWorkflowParams{
            Function: getWorkflowName(wfFunction),
            Args:     wfArgs,
            Options:  wfOptions,
        },
        sequenceWfOptions,
        SequenceWorkflow,
    )
    return err
}
```

## Resource Interaction Analysis

### Pool Operations

| Operation 1 | Operation 2 | Queueing Required | Reason |
|-------------|-------------|-------------------|---------|
| CREATE | CREATE | NO | First call passes, subsequent calls fail due to duplicate name |
| CREATE | UPDATE | NO | Pre-validation check handles this |
| CREATE | DELETE | NO | Pre-validation check handles this |
| UPDATE | UPDATE | NO | Subsequent calls fail due to resource already updating |
| UPDATE | DELETE | NO | Pre-validation check handles this |
| DELETE | UPDATE | NO | Pre-validation check handles this |

### Volume Operations (Different Volumes)

| Operation 1 | Operation 2 | Queueing Required | Reason |
|-------------|-------------|-------------------|---------|
| CREATE | CREATE | YES | Both operations modify host group |
| CREATE | DELETE | YES | First volume CREATE and last volume DELETE modify host group |
| UPDATE | UPDATE | NO | Can run in parallel |
| UPDATE | CREATE | NO | Can run in parallel |
| UPDATE | DELETE | NO | Can run in parallel |
| DELETE | DELETE | YES | Both operations modify host group |
| DELETE | CREATE | YES | First volume CREATE and last volume DELETE modify host group |
| DELETE | UPDATE | NO | Can run in parallel |

### Volume Operations (Same Volume)

| Operation 1 | Operation 2 | Queueing Required | Reason |
|-------------|-------------|-------------------|---------|
| CREATE | CREATE | NO | API calls are idempotent |
| CREATE | UPDATE | NO | Blocked by code - UPDATE not allowed on CREATING resource |
| CREATE | DELETE | NO | Handled by resource transitioning check |
| UPDATE | UPDATE | NO | Subsequent calls return jobID for first one |
| UPDATE | DELETE | NO | Blocked by code if required |
| DELETE | UPDATE | NO | Blocked by code |

### Snapshot Operations

| Scenario | Queueing Required | Reason |
|----------|-------------------|---------|
| Different Volumes | NO | Independent operations |
| Different Snapshots (Same Volume) | NO | Independent operations |
| Same Snapshot | NO | No complex operations requiring queuing |

## Usage Examples

### Volume Operations

```go
// Control workflow ID for volume operations on a specific pool
controlWorkflowID := fmt.Sprintf(workflows.VolumeCreateDeleteSnapshotDeleteSeq, 
    dbVolume.Account.ID, location, dbVolume.Pool.Name)

err = workflows.ExecuteWorkflowSequentially(
    temporal,
    ctx,
    client.StartWorkflowOptions{
        TaskQueue: workflowengine.CustomerTaskQueue,
        ID:        controlWorkflowID,
    },
    workflows.CreateVolumeWorkflow,
    workflow.ChildWorkflowOptions{
        TaskQueue:             workflowengine.CustomerTaskQueue,
        WorkflowID:            createdJob.WorkflowID,
        WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
        WorkflowRunTimeout:    workflowengine.GetWorkflowGlobalTimeout(),
    },
    params,
    dbVolume,
)
```

### Pool Subnet Operations

```go
// Control workflow ID for pool subnet operations
controlWorkflowID := fmt.Sprintf(workflows.PoolSubnetCreate, pool.Account.ID, vpcName)

err = workflows.ExecuteWorkflowSequentially(
    temporalClient,
    ctx,
    client.StartWorkflowOptions{
        TaskQueue: workflowengine.CustomerTaskQueue,
        ID:        controlWorkflowID,
    },
    workflows.PoolDataSubnetWorkFlow,
    workflow.ChildWorkflowOptions{
        TaskQueue:             workflowengine.CustomerTaskQueue,
        WorkflowID:            createdJob.WorkflowID,
        WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
        WorkflowRunTimeout:    workflowengine.GetWorkflowGlobalTimeout(),
    },
    params,
    pool.UUID,
    tenantProjectNumber,
)
```

## Temporal Concepts Used

### Parent-Child Workflow
- Parent workflow (SequenceWorkflow) manages child workflow execution
- Enables hierarchical workflow structures for complex business processes
- Child workflows execute sequentially under parent control

### Signals
- Asynchronous messages sent to running workflows
- Temporal ensures signals are executed in the order they are sent
- No waiting for response, enabling non-blocking communication

### SignalWithStartWorkflow
- Atomic operation that starts workflow if not running, or signals existing workflow
- Ensures signals are never lost
- Guarantees single control workflow instance per resource

### Future and Timer
- Future represents eventual result of asynchronous operations
- Timer provides timeout mechanism for workflow lifecycle management
- Selector blocks on multiple channels and futures

## Implementation Phases

### Phase 1: Core Queuing Implementation
- **Volume Operations**: CREATE and DELETE volume operations for same pool
- **Pool Operations**: Pool CREATE/DELETE operations (if required)
- **Control Workflow**: Basic SequenceWorkflow implementation
- **Signal Handling**: SignalWithStartWorkflow integration

### Phase 2: Enhanced Queuing
- **Snapshot Operations**: Snapshot CREATE/DELETE operations
- **Advanced Scenarios**: Complex resource interaction patterns
- **Performance Optimization**: Workflow execution optimization
- **Monitoring**: Enhanced observability and metrics

### Phase 3: Advanced Features
- **Dynamic Queuing**: Runtime queue configuration
- **Priority Queuing**: Priority-based execution order
- **Conditional Queuing**: Context-aware queuing decisions
- **Queue Management**: Queue status and management APIs

## Configuration and Management

### Workflow ID Naming Convention

```
(Sequence-Uniqueness-ID)_Ops_(Resource1-C/U/D)-(Resource2-C/U/D)-...-(ResourceN-C/U/D)
```

Where:
- **C** = CREATE
- **U** = UPDATE  
- **D** = DELETE

### Task Queue Configuration

- **Customer Task Queue**: Default queue for customer operations
- **Background Task Queue**: Used for background operations
- **Queue Isolation**: Separate queues for different operation types

### Timeout Configuration

- **Workflow Timeout**: 3 seconds idle timeout for control workflow
- **Child Workflow Timeout**: Configurable per child workflow
- **Signal Timeout**: Immediate signal delivery

## Monitoring and Observability

### Workflow Metrics

- **Control Workflow Status**: Running, completed, failed states
- **Signal Processing**: Signal receive and processing rates
- **Child Workflow Execution**: Success/failure rates and duration
- **Queue Depth**: Number of pending operations per resource

### Logging

```go
logger := workflow.GetLogger(ctx)
logger.Info("Starting sequence workflow", "workflowID", workflowID)
logger.Error("Failed to execute child workflow", "error", err)
logger.Info("Current value reached threshold, exiting workflow")
```

### Health Checks

- **Control Workflow Health**: Query handlers for workflow status
- **Resource Queue Status**: Per-resource queue status monitoring
- **Signal Processing Health**: Signal processing performance metrics

## Testing Strategy

### Unit Tests

```go
func TestExecuteWorkflowSequentially_Success(t *testing.T) {
    temporal := workflowEngineMock.NewMockTemporalTestClient(t)
    var ts testsuite.WorkflowTestSuite
    env := ts.NewTestWorkflowEnvironment()
    
    // Mock SignalWithStartWorkflow
    temporal.EXPECT().SignalWithStartWorkflow(
        ctx,
        "test-sequence-workflow-id",
        Signal,
        mock.Anything,
        mock.Anything,
        mock.Anything,
    ).Return(nil, nil)
    
    err := ExecuteWorkflowSequentially(
        temporal,
        ctx,
        client.StartWorkflowOptions{ID: "test-sequence-workflow-id"},
        WorkflowTest,
        workflow.ChildWorkflowOptions{WorkflowID: "test-workflow-id"},
    )
    
    assert.NoError(t, err)
}
```

### Integration Tests

- **End-to-End Workflow Testing**: Complete workflow execution testing
- **Concurrent Operation Testing**: Multiple concurrent operations testing
- **Resource Isolation Testing**: Different resource operations testing
- **Error Scenario Testing**: Failure and recovery testing

## Performance Considerations

### Scalability

- **Resource Isolation**: Each resource has its own control workflow
- **Parallel Execution**: Different resources can execute in parallel
- **Signal Efficiency**: Minimal overhead for signal processing
- **Memory Usage**: Control workflows are lightweight and stateless

### Optimization

- **Workflow Reuse**: Control workflows are reused across operations
- **Signal Batching**: Multiple signals processed efficiently
- **Timeout Management**: Automatic cleanup of idle workflows
- **Resource Cleanup**: Proper cleanup of completed workflows

## Security Considerations

### Access Control

- **Workflow Isolation**: Resource-based workflow isolation
- **Signal Validation**: Signal parameter validation
- **Workflow Authorization**: Workflow execution authorization
- **Resource Permissions**: Resource-level access control

### Data Protection

- **Signal Encryption**: Secure signal transmission
- **Workflow State**: Secure workflow state management
- **Audit Logging**: Comprehensive audit trail
- **Error Handling**: Secure error handling and reporting

## Consequences

### Positive

- **Resource Consistency**: Prevents race conditions on shared resources
- **Sequential Execution**: Ensures proper operation ordering
- **Temporal Integration**: Leverages Temporal's built-in capabilities
- **Scalability**: Supports high-volume concurrent operations
- **Maintainability**: Simple interface for developers

### Negative

- **Increased Complexity**: Additional complexity in workflow management
- **Performance Overhead**: Signal processing overhead
- **Resource Usage**: Additional Temporal resources for control workflows
- **Debugging Complexity**: More complex debugging scenarios

### Risks

- **Signal Loss**: Potential signal loss in edge cases
- **Workflow Deadlock**: Potential deadlock scenarios
- **Resource Exhaustion**: Control workflow resource exhaustion
- **Error Propagation**: Error propagation from child workflows

## Future Enhancements

### Advanced Queuing

- **Priority Queues**: Priority-based operation execution
- **Conditional Queuing**: Context-aware queuing decisions
- **Dynamic Queuing**: Runtime queue configuration
- **Queue Analytics**: Advanced queue performance analytics

### Integration Improvements

- **API Integration**: REST APIs for queue management
- **Dashboard Integration**: Visual queue monitoring
- **Alerting**: Automated alerting for queue issues
- **Metrics Integration**: Enhanced metrics collection

## References

- [Control Workflow Implementation](../../../core/orchestrator/workflows/control_workflow.go)
- [Volume Operations Usage](../../../core/orchestrator/volume.go)
- [Pool Operations Usage](../../../core/orchestrator/workflows/pool_workflows.go)
- [Temporal Signals Documentation](https://docs.temporal.io/workflows#signals)
- [Temporal Parent-Child Workflows](https://docs.temporal.io/workflows#parent-child-workflow)
- [Temporal Selectors](https://docs.temporal.io/workflows#selectors)