# FlexCache Workflows

This document describes the FlexCache workflows in the VSA Control Plane system, including FlexCache volume creation, management, and deletion operations.

## Overview

FlexCache workflows manage FlexCache volume operations for VSA clusters, including volume creation, configuration, and deletion. FlexCache provides intelligent caching capabilities for distributed storage environments.

## Workflow Types

### 1. Create FlexCache Volume Workflow

**File**: `core/orchestrator/workflows/flexcache_workflows/flexcache_volume_create_workflow.go`

**Purpose**: Creates new FlexCache volumes with proper configuration and cluster peer setup.

**Entry Point**: `CreateFlexCacheWorkflow(ctx workflow.Context, params *common.CreateVolumeParams, volume *datamodel.Volume)`

#### Workflow Structure

```go
type flexCacheCreateWorkflow struct {
    workflows.BaseWorkflow
}
```

#### Configuration

**Environment Variables**:
```go
var (
    clusterPeerTimeout  = env.GetDuration("CLUSTER_PEER_TIMEOUT", 60*time.Minute)
    clusterPeerInterval = env.GetDuration("CLUSTER_PEER_INTERVAL", 15*time.Second)
)
```

#### Activities

**FlexCache Creation Activities**:
- `CreateFlexCacheVolumeInDB`: Creates FlexCache volume record in database
- `ValidateFlexCacheParameters`: Validates FlexCache creation parameters
- `CreateFlexCacheVolumeInVSA`: Creates FlexCache volume in VSA cluster
- `ValidateFlexCacheCreation`: Validates FlexCache creation success
- `UpdateFlexCacheStatus`: Updates FlexCache status in database
- `ConfigureFlexCacheSettings`: Configures FlexCache settings

**Cluster Peer Activities**:
- `SetupClusterPeer`: Sets up cluster peer relationship
- `ValidateClusterPeerConnection`: Validates cluster peer connection
- `ConfigureClusterPeerTimeout`: Configures cluster peer timeout
- `MonitorClusterPeerHealth`: Monitors cluster peer health

**VSA Integration Activities**:
- `ConfigureFlexCacheForSVM`: Configures FlexCache for Storage Virtual Machine
- `ValidateFlexCacheReachability`: Validates FlexCache connectivity
- `SetupFlexCacheMounts`: Sets up FlexCache mount points
- `UpdateFlexCacheMetadata`: Updates FlexCache metadata

#### Execution Flow

1. **Pre-creation Phase**:
   - Validate FlexCache parameters
   - Check cluster peer availability
   - Prepare FlexCache configuration

2. **Cluster Peer Setup**:
   - Set up cluster peer relationship
   - Validate cluster peer connection
   - Configure cluster peer timeout

3. **FlexCache Creation**:
   - Create FlexCache volume in VSA cluster
   - Validate FlexCache creation
   - Configure FlexCache settings

4. **Database Update**:
   - Update FlexCache information
   - Set FlexCache status
   - Configure metadata

5. **Validation and Cleanup**:
   - Validate FlexCache creation
   - Update job status
   - Handle rollback if errors occur

#### Error Handling

```go
if customErr != nil {
    flexCacheWf.Status = workflows.WorkflowStatusFailed
    err2 := flexCacheWf.UpdateJobStatus(ctx, string(models.JobsStateERROR), customErr)
    if err2 != nil {
        log.Errorf("Failed to update job status to Done with error for CreateFlexCacheWorkflow: %v", err2)
        return err2
    }
    return customErr
}
```

### 2. Delete FlexCache Volume Workflow

**File**: `core/orchestrator/workflows/flexcache_workflows/flexcache_volume_delete_workflow.go`

**Purpose**: Safely deletes FlexCache volumes with proper cleanup.

**Entry Point**: `DeleteFlexCacheWorkflow(ctx workflow.Context, params *common.DeleteVolumeParams, volume *datamodel.Volume)`

#### Workflow Structure

```go
type flexCacheDeleteWorkflow struct {
    workflows.BaseWorkflow
}
```

#### Activities

**FlexCache Deletion Activities**:
- `DeleteFlexCacheVolumeFromVSA`: Removes FlexCache volume from VSA cluster
- `CleanupFlexCacheResources`: Cleans up associated resources
- `UpdateFlexCacheStateInDB`: Updates FlexCache state to deleted
- `ValidateFlexCacheDeletion`: Ensures safe deletion
- `CleanupFlexCacheMetadata`: Cleans up FlexCache metadata

**Cluster Peer Cleanup Activities**:
- `CleanupClusterPeerConnection`: Cleans up cluster peer connection
- `ValidateClusterPeerCleanup`: Validates cluster peer cleanup
- `UpdateClusterPeerStatus`: Updates cluster peer status

#### Execution Flow

1. **Pre-deletion Checks**: Verify FlexCache can be safely deleted
2. **Cluster Peer Cleanup**: Clean up cluster peer connection
3. **VSA Deletion**: Delete FlexCache volume from VSA cluster
4. **Resource Cleanup**: Clean up associated resources
5. **Database Update**: Update FlexCache state
6. **Final Cleanup**: Complete cleanup operations

## FlexCache Management

### FlexCache Configuration

**FlexCache Settings**:
- **Cache Size**: FlexCache volume size
- **Origin Volume**: Source volume for caching
- **Cache Policy**: Caching policy configuration
- **Cluster Peer**: Target cluster for caching

### Cluster Peer Management

**Cluster Peer Settings**:
- **Peer Timeout**: Cluster peer connection timeout (default: 60 minutes)
- **Peer Interval**: Cluster peer health check interval (default: 15 seconds)
- **Peer Health**: Cluster peer health monitoring
- **Peer Cleanup**: Cluster peer cleanup on deletion

### FlexCache Lifecycle

**States**:
- `CREATED`: FlexCache has been created but not started
- `RUNNING`: FlexCache is actively running
- `STOPPED`: FlexCache is stopped
- `FAILED`: FlexCache has failed
- `DELETED`: FlexCache has been deleted

## Error Handling

### Error Types

**Non-Retryable Errors**:
- `PanicError`: System panic errors
- `ValidationError`: Parameter validation errors
- `FlexCacheNotFoundError`: Missing FlexCache errors

**Retryable Errors**:
- `NetworkTimeoutError`: Network operation timeouts
- `ClusterPeerError`: Cluster peer operation failures
- `VSAOperationError`: VSA operation failures

### Rollback Management

FlexCache workflows implement rollback for failed operations:

```go
defer func() {
    if err != nil {
        err2 := workflow.ExecuteActivity(ctx, flexCacheActivity.UpdateFlexCacheStateInDB, 
            flexCache.UUID, models.LifeCycleStateError, models.LifeCycleStateCreationErrorDetails).Get(ctx, nil)
        if err2 != nil {
            log.Errorf("Failed to update FlexCache state in DB to error: %v", err2)
        }
    }
}()
```

## Configuration

### Retry Policies

All FlexCache workflows use configurable retry policies:

```go
retryPolicy, err := workflows.PopulateRetryPolicyParams()
if err != nil {
    return nil, workflows.ConvertToVSAError(err)
}
```

### Activity Options

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

## Monitoring and Metrics

### FlexCache Metrics

**Available Metrics**:
- FlexCache creation duration
- FlexCache operation success rate
- Cluster peer connectivity metrics
- FlexCache performance metrics

### Health Checks

**FlexCache Health Monitoring**:
- FlexCache connectivity status
- Cluster peer health status
- FlexCache performance metrics
- Cache hit/miss ratios

## Performance Considerations

### FlexCache Performance

**Optimization Features**:
- Parallel FlexCache operations
- Cluster peer optimization
- Cache performance tuning
- Resource utilization monitoring

### Resource Management

**Resource Limits**:
- Concurrent FlexCache operations
- Cluster peer connection limits
- Network bandwidth limits
- Storage space limits

## Security Considerations

### FlexCache Security

**Access Control**:
- Role-based access control
- FlexCache permissions
- Audit logging for all operations

### Data Protection

**Data Security**:
- Encrypted FlexCache data
- Secure cluster peer communications
- Data integrity validation
- Secure mount points

## Testing

### Test Coverage

Each FlexCache workflow has comprehensive test coverage:

- **Unit Tests**: Individual workflow function testing
- **Integration Tests**: End-to-end workflow testing
- **Mock Activities**: Mock implementations for testing
- **Error Scenarios**: Testing failure and retry scenarios
- **VSA Integration Tests**: Testing with actual VSA resources

### Test Files

- `flexcache_volume_create_workflow_test.go`: Create workflow tests
- `flexcache_volume_delete_workflow_test.go`: Delete workflow tests
- Mock implementations for all activities

## Related Documentation

- [Volume Workflows](../core/volume-workflows.md)
- [Pool Workflows](../core/pool-workflows.md)
- [Cluster Workflows](../core/cluster-workflows.md)
- [Temporal Debugging Guide](../../guides/temporal-debugging.md)