# Cluster Workflows

This document describes the cluster-related workflows in the VSA Control Plane system, including cluster peer management, node registration, and cluster health monitoring.

## Overview

Cluster workflows manage VSA cluster operations, including cluster peer relationships, node registration with harvest farms, and cluster health monitoring. These workflows ensure proper cluster connectivity and management.

## Workflow Types

### 1. Accept Cluster Peer Workflow

**File**: `core/orchestrator/workflows/cluster_workflows.go`

**Purpose**: Establishes cluster peer relationships between VSA clusters.

**Entry Point**: `AcceptClusterPeerWorkflow(ctx workflow.Context, params *common.ClusterPeerParams, pool *datamodel.Pool)`

#### Workflow Structure

```go
type clusterPeerWorkflow struct {
    BaseWorkflow
    SE *database.Storage
}
```

#### Activities

**Cluster Peer Activities**:
- `AcceptClusterPeerInDB`: Creates cluster peer record in database
- `ValidateClusterPeerParameters`: Validates cluster peer parameters
- `EstablishClusterPeerConnection`: Establishes connection between clusters
- `ValidateClusterPeerConnection`: Validates peer connection
- `UpdateClusterPeerStatus`: Updates peer status in database

**Network Activities**:
- `ConfigureClusterNetworking`: Configures cluster networking
- `ValidateNetworkConnectivity`: Validates network connectivity
- `SetupClusterRouting`: Sets up cluster routing

#### Execution Flow

1. **Pre-connection Phase**:
   - Validate cluster peer parameters
   - Check cluster availability
   - Prepare connection configuration

2. **Connection Establishment**:
   - Establish cluster peer connection
   - Configure networking
   - Validate connectivity

3. **Database Update**:
   - Update cluster peer information
   - Set peer status
   - Configure metadata

4. **Validation and Cleanup**:
   - Validate peer connection
   - Update job status
   - Handle rollback if errors occur

### 2. Register Node to Harvest Farm Workflow

**File**: `core/orchestrator/workflows/register_node_to_harvest_farm_workflow.go`

**Purpose**: Registers VSA nodes with harvest farms for monitoring and management.

**Entry Point**: `RegisterNodeToHarvestFarmWorkflow(ctx workflow.Context, params *common.RegisterNodeParams, node *datamodel.Node)`

#### Workflow Structure

```go
type registerNodeWorkflow struct {
    BaseWorkflow
    SE *database.Storage
}
```

#### Activities

**Node Registration Activities**:
- `RegisterNodeInDB`: Creates node record in database
- `ValidateNodeParameters`: Validates node registration parameters
- `RegisterNodeWithHarvestFarm`: Registers node with harvest farm
- `ValidateNodeRegistration`: Validates node registration
- `UpdateNodeStatus`: Updates node status in database

**Harvest Farm Activities**:
- `ConnectToHarvestFarm`: Establishes connection to harvest farm
- `ConfigureNodeMonitoring`: Configures node monitoring
- `ValidateHarvestFarmConnection`: Validates harvest farm connection

#### Execution Flow

1. **Pre-registration Phase**:
   - Validate node parameters
   - Check harvest farm availability
   - Prepare registration configuration

2. **Node Registration**:
   - Register node with harvest farm
   - Configure monitoring
   - Validate registration

3. **Database Update**:
   - Update node information
   - Set node status
   - Configure metadata

4. **Validation and Cleanup**:
   - Validate node registration
   - Update job status
   - Handle rollback if errors occur

### 3. Unregister Node from Harvest Farm Workflow

**File**: `core/orchestrator/workflows/unregister_node_to_harvest_farm_workflow.go`

**Purpose**: Unregisters VSA nodes from harvest farms.

**Entry Point**: `UnregisterNodeFromHarvestFarmWorkflow(ctx workflow.Context, params *common.UnregisterNodeParams, node *datamodel.Node)`

#### Workflow Structure

```go
type unregisterNodeWorkflow struct {
    BaseWorkflow
    SE *database.Storage
}
```

#### Activities

**Node Unregistration Activities**:
- `UnregisterNodeFromHarvestFarm`: Unregisters node from harvest farm
- `CleanupNodeResources`: Cleans up node resources
- `UpdateNodeStateInDB`: Updates node state to unregistered
- `ValidateNodeUnregistration`: Ensures safe unregistration

#### Execution Flow

1. **Pre-unregistration Checks**: Verify node can be safely unregistered
2. **Harvest Farm Unregistration**: Unregister node from harvest farm
3. **Resource Cleanup**: Clean up node resources
4. **Database Update**: Update node state
5. **Final Cleanup**: Complete cleanup operations

## Cluster Management

### Cluster Health Monitoring

**Health Check Activities**:
- `CheckClusterHealth`: Monitors cluster health status
- `ValidateClusterConnectivity`: Validates cluster connectivity
- `MonitorClusterPerformance`: Monitors cluster performance
- `UpdateClusterHealthStatus`: Updates cluster health status

### Node Management

**Node Lifecycle**:
- **Registration**: Node is registered with harvest farm
- **Active**: Node is active and monitored
- **Unregistered**: Node is unregistered from harvest farm
- **Failed**: Node has failed health checks

### Cluster Peer Management

**Peer Lifecycle**:
- **Pending**: Peer connection is pending
- **Connected**: Peer connection is established
- **Disconnected**: Peer connection is lost
- **Failed**: Peer connection has failed

## Error Handling

### Error Types

**Non-Retryable Errors**:
- `PanicError`: System panic errors
- `ValidationError`: Parameter validation errors
- `ClusterNotFoundError`: Missing cluster errors

**Retryable Errors**:
- `NetworkTimeoutError`: Network operation timeouts
- `ClusterOperationError`: Cluster operation failures
- `HarvestFarmError`: Harvest farm operation failures

### Rollback Management

Cluster workflows implement rollback for failed operations:

```go
defer func() {
    if err != nil {
        err2 := workflow.ExecuteActivity(ctx, clusterActivity.UpdateClusterStateInDB, 
            cluster.UUID, models.LifeCycleStateError, models.LifeCycleStateCreationErrorDetails).Get(ctx, nil)
        if err2 != nil {
            log.Errorf("Failed to update cluster state in DB to error: %v", err2)
        }
    }
}()
```

## Configuration

### Retry Policies

All cluster workflows use configurable retry policies:

```go
retryPolicy, err := PopulateRetryPolicyParams()
if err != nil {
    return nil, ConvertToVSAError(err)
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

### Cluster Metrics

**Available Metrics**:
- Cluster peer connection duration
- Node registration success rate
- Cluster health status
- Network connectivity metrics

### Health Checks

**Cluster Health Monitoring**:
- Cluster connectivity status
- Node health status
- Harvest farm connectivity
- Performance metrics

## Performance Considerations

### Cluster Performance

**Optimization Features**:
- Parallel cluster operations
- Connection pooling
- Health check optimization
- Resource utilization monitoring

### Resource Management

**Resource Limits**:
- Concurrent cluster operations
- Network bandwidth limits
- Harvest farm connection limits

## Security Considerations

### Cluster Security

**Access Control**:
- Role-based access control
- Cluster access permissions
- Audit logging for all operations

### Network Security

**Network Protection**:
- Secure cluster communications
- Network encryption
- Firewall configuration

## Testing

### Test Coverage

Each cluster workflow has comprehensive test coverage:

- **Unit Tests**: Individual workflow function testing
- **Integration Tests**: End-to-end workflow testing
- **Mock Activities**: Mock implementations for testing
- **Error Scenarios**: Testing failure and retry scenarios
- **Cluster Integration Tests**: Testing with actual cluster resources

### Test Files

- `cluster_workflows_test.go`: Main test file
- `register_node_to_harvest_farm_workflow_test.go`: Node registration tests
- `unregister_node_to_harvest_farm_workflow_test.go`: Node unregistration tests
- Mock implementations for all activities

## Related Documentation

- [Pool Workflows](./pool-workflows.md)
- [Volume Workflows](./volume-workflows.md)
- [Temporal Debugging Guide](../../guides/temporal-debugging.md)