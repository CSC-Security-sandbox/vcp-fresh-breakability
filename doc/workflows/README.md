# VSA Control Plane Workflows Documentation

This directory contains detailed documentation for all workflows in the VSA Control Plane system. Each workflow is documented with its purpose, activities, child workflows, and execution flow.

## Workflow Categories

### Core Workflows
- [Volume Workflows](./core/volume-workflows.md) - Volume lifecycle management
- [Pool Workflows](./core/pool-workflows.md) - Storage pool management
- [Cluster Workflows](./core/cluster-workflows.md) - VSA cluster management
- [Backup Workflows](./core/backup-workflows.md) - Backup and restore operations
- [Snapshot Workflows](./core/snapshot-workflows.md) - Snapshot management
- [ADC Workflows](./core/adc-workflows.md) - Application Data Collection

### Background Workflows
- [Scheduled Backup Workflows](./background/scheduled-backup-workflows.md) - Automated backup scheduling
- [Resource Cleanup Workflows](./background/resource-cleanup-workflows.md) - Resource cleanup and maintenance

### Replication Workflows
- [Replication Workflows](./replication/replication-workflows.md) - Volume replication management

### KMS Workflows
- [KMS Workflows](./kms/kms-workflows.md) - Key Management Service operations

### FlexCache Workflows
- [FlexCache Workflows](./flexcache/flexcache-workflows.md) - FlexCache volume management

### Control Workflows
- [Control Workflows](./control/control-workflows.md) - Workflow orchestration and control

## Workflow Architecture

All workflows in the VSA Control Plane follow a consistent architecture pattern:

### Base Workflow Structure
```go
type BaseWorkflow struct {
    ID         string
    Status     string
    CustomerID string
    Logger     log.Logger
}
```

### Workflow Interface
```go
type WorkflowInterface interface {
    Setup(ctx workflow.Context, input interface{}) error
    Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError)
    UpdateJobStatus(ctx workflow.Context, status string, err error) error
}
```

### Common Workflow States
- `CREATED` - Workflow has been created but not started
- `RUNNING` - Workflow is currently executing
- `COMPLETED` - Workflow completed successfully
- `FAILED` - Workflow failed with an error
- `CANCELLED` - Workflow was cancelled
- `TIMEOUT` - Workflow timed out
- `RETRY` - Workflow is retrying after a failure
- `PAUSED` - Workflow is paused
- `RESUMED` - Workflow was resumed from pause
- `ABORTED` - Workflow was aborted
- `PENDING` - Workflow is pending execution

## Error Handling

All workflows implement consistent error handling patterns:

1. **Error Conversion**: Convert errors to `*vsaerrors.CustomError` for consistent error handling
2. **Rollback Management**: Implement rollback mechanisms for failed operations
3. **Retry Policies**: Configure retry policies for transient failures
4. **Logging**: Comprehensive logging at all levels with correlation IDs

## Retry Policies

Workflows use configurable retry policies with the following parameters:

- `InitialInterval`: Initial retry interval (default: 5s)
- `BackoffCoefficient`: Backoff multiplier (default: 2.0)
- `MaximumInterval`: Maximum retry interval (default: 5m)
- `MaximumAttempts`: Maximum retry attempts (default: 3)
- `StartToCloseTimeout`: Activity timeout (default: 55m)

## Activity Options

All activities use consistent activity options:

```go
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
```

## Logging and Tracing

All workflows integrate with the slog logging framework:

- **Context Propagation**: Request correlation IDs are propagated through workflows
- **Structured Logging**: JSON-formatted logs with consistent field naming
- **OpenTelemetry Integration**: Automatic trace and span ID inclusion
- **Error Correlation**: Errors are correlated with workflow execution context

## Testing

All workflows have comprehensive test coverage:

- **Unit Tests**: Individual workflow function testing
- **Integration Tests**: End-to-end workflow testing
- **Mock Activities**: Mock implementations for testing
- **Error Scenarios**: Testing failure and retry scenarios

## Monitoring and Observability

Workflows provide extensive monitoring capabilities:

- **Metrics**: Performance and execution metrics
- **Tracing**: Distributed tracing across workflow execution
- **Logging**: Structured logging for debugging and analysis
- **Health Checks**: Workflow health and status monitoring

## Related Documentation

- [Temporal Debugging Guide](../guides/temporal-debugging.md)
- [Logging Framework ADR](../architecture/decisions/0011-slog-logging-framework.md)
- [API Documentation](../api/overview.md)