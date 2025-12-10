# Pool Workflows

This document describes the pool-related workflows in the VSA Control Plane system, including pool creation, management, and configuration operations.

## Overview

Pool workflows manage the complete lifecycle of storage pools in the VSA Control Plane, from creation to deletion. They handle VSA cluster provisioning, network configuration, KMS setup, and resource management.

## Workflow Types

### 1. Create Pool Workflow

**File**: `core/orchestrator/workflows/pool_workflows.go`

**Purpose**: Creates new storage pools with VSA clusters and associated infrastructure.

**Entry Point**: `CreatePoolWorkflow(ctx workflow.Context, params *common.CreatePoolParams, pool *datamodel.Pool)`

#### Workflow Structure

```go
type createPoolWorkflow struct {
    BaseWorkflow
    SE *database.Storage
}
```

#### Activities

**Pool Creation Activities**:
- `CreatePoolInDB`: Creates pool record in database
- `ValidatePoolParameters`: Validates pool creation parameters
- `CheckResourceAvailability`: Verifies resource availability
- `CreateVSAInstances`: Provisions VSA instances
- `ConfigureNetwork`: Sets up network configuration
- `SetupKMS`: Configures Key Management Service
- `ValidatePoolCreation`: Validates pool creation success

**VSA Management Activities**:
- `DeployVSAInstances`: Deploys VSA instances to GCP
- `ConfigureVSACluster`: Configures VSA cluster settings
- `SetupVSANetworking`: Configures VSA networking
- `ValidateVSAHealth`: Validates VSA cluster health

**Network Activities**:
- `CreateVPC`: Creates Virtual Private Cloud
- `CreateSubnets`: Creates network subnets
- `ConfigureFirewall`: Sets up firewall rules
- `SetupNAT`: Configures Network Address Translation

**KMS Activities**:
- `ConfigureKMSForSVM`: Configures KMS for Storage Virtual Machine
- `ValidateKMSReachability`: Validates KMS connectivity
- `SetupKMSKeys`: Sets up encryption keys

#### Execution Flow

1. **Pre-creation Phase**:
   - Validate pool parameters
   - Check resource availability
   - Prepare infrastructure configuration

2. **Infrastructure Creation**:
   - Create VPC and subnets
   - Configure firewall rules
   - Set up NAT gateway

3. **VSA Deployment**:
   - Deploy VSA instances
   - Configure VSA cluster
   - Set up VSA networking

4. **KMS Configuration**:
   - Configure KMS for SVM
   - Validate KMS connectivity
   - Set up encryption keys

5. **Validation and Cleanup**:
   - Validate pool creation
   - Update database records
   - Handle rollback if errors occur

#### Configuration

**Environment Variables**:
```go
var (
    setupNwHeartbeatTimeout                              = env.GetUint64("SETUP_NW_HEARTBEAT_TIMEOUT_SEC", 300)
    vmrsConfigPath                                       = env.GetString("VMRS_CONFIG_PATH", "/config/vmrs_gcp.yaml")
    maxNodesPerGroup                                     = env.GetInt("MAX_NODES_PER_GROUP", 200)
    enableMetrics                                        = env.GetBool("ENABLE_METRICS", false)
    enableUniqueSerialNumberGeneration                   = env.GetBool("ENABLE_UNIQUE_SERIAL_NUMBER_GENERATION", false)
    vsaImageName                                         = env.GetString("VSA_IMAGE_NAME", "x-9-17-1p2-gcnv")
    mediatorImage                                        = env.GetString("VSA_MEDIATOR_IMAGE_NAME", "cvo-mediator-x-9-18-1rc1")
    waitTimeForGCPOperationInSec                         = env.GetInt("WAIT_TIME_FOR_GCP_OPERATION_IN_SEC", 10)
    disableVsaCleanupOnVLMFailure                        = env.GetBool("DISABLE_VSA_CLEANUP_ON_VLM_FAILURE", false)
    enableAutoVolOfflineCronForGCPKMS                    = env.GetBool("ENABLE_AUTO_VOL_OFFLINE_CRON_FOR_GCP_KMS", true)
    ginLoggingFeatureFlag                                = env.GetBool("GIN_LOGGING_FEATURE", false)
)
```

### 2. Pool Data Subnet Workflow

**Purpose**: Manages data subnet creation and configuration for pools.

**Entry Point**: `PoolDataSubnetWorkflow(ctx workflow.Context, params *common.PoolDataSubnetParams, pool *datamodel.Pool)`

#### Workflow Structure

```go
type poolDataSubnetWorkFlow struct {
    BaseWorkflow
    SE             *database.Storage
    TenancyDetails *common.TenancyInfo
}
```

#### Activities

**Subnet Management Activities**:
- `CreateDataSubnet`: Creates data subnet
- `ConfigureSubnetRouting`: Configures subnet routing
- `ValidateSubnetConfiguration`: Validates subnet setup
- `UpdatePoolSubnetInfo`: Updates pool subnet information

### 3. Pool Update Workflow

**Purpose**: Updates existing pool configurations and properties.

**Entry Point**: `UpdatePoolWorkflow(ctx workflow.Context, params *common.UpdatePoolParams, pool *datamodel.Pool)`

#### Activities

**Pool Update Activities**:
- `UpdatePoolInDB`: Updates pool information in database
- `UpdateVSAConfiguration`: Updates VSA cluster configuration
- `ValidatePoolUpdate`: Validates update parameters
- `ApplyPoolChanges`: Applies configuration changes

### 4. Pool Delete Workflow

**Purpose**: Safely deletes pools with proper resource cleanup.

**Entry Point**: `DeletePoolWorkflow(ctx workflow.Context, params *common.DeletePoolParams, pool *datamodel.Pool)`

#### Activities

**Pool Deletion Activities**:
- `DeletePoolFromVSA`: Removes pool from VSA cluster
- `CleanupPoolResources`: Cleans up associated resources
- `DeleteVSAInstances`: Deletes VSA instances
- `CleanupNetworkResources`: Cleans up network resources
- `UpdatePoolStateInDB`: Updates pool state to deleted

## VSA Management

### VSA Instance Configuration

**Image Configuration**:
- **VSA Image**: `x-9-18-1rc1` (default)
- **Mediator Image**: `cvo-mediator-x-9-18-1rc1` (default)

**Instance Settings**:
- **Max Nodes Per Group**: 200 (configurable)
- **Heartbeat Timeout**: 300 seconds (configurable)
- **GCP Operation Wait Time**: 10 seconds (configurable)

### Network Configuration

**VPC and Subnet Management**:
- Creates VPC for each pool
- Configures subnets for different purposes
- Sets up firewall rules
- Configures NAT gateway

**Network Types**:
- **Management Network**: For VSA management
- **Data Network**: For data traffic
- **RSM Network**: For RSM operations
- **IC Network**: For Interconnect

## KMS Integration

### KMS Configuration

**KMS Activities**:
- `ConfigureKmsConfigForSvmActivity`: Configures KMS for SVM
- `VerifyKmsConfigReachability`: Verifies KMS connectivity
- `SetupKMSKeys`: Sets up encryption keys

**KMS Features**:
- **Auto Volume Offline**: Enabled by default for GCP KMS
- **KMS Reachability**: Validated during pool creation
- **Encryption Keys**: Automatically configured

## Error Handling

### Rollback Management

Pool workflows implement comprehensive rollback:

```go
rollbackManager := common.NewRollbackManager()
defer func() {
    if err != nil {
        rollbackManager.ExecuteRollback(ctx, err)
    }
}()
```

### Error Types

**Non-Retryable Errors**:
- `PanicError`: System panic errors
- `ValidationError`: Parameter validation errors
- `ResourceNotFoundError`: Missing resource errors

**Retryable Errors**:
- `NetworkTimeoutError`: Network operation timeouts
- `GCPOperationError`: GCP operation failures
- `VSAClusterError`: VSA cluster operation failures

## Monitoring and Metrics

### Metrics Collection

**Enabled Metrics** (when `ENABLE_METRICS=true`):
- Pool creation duration
- VSA deployment metrics
- Network configuration metrics
- KMS operation metrics

### Health Checks

**Pool Health Monitoring**:
- VSA cluster health status
- Network connectivity status
- KMS reachability status
- Resource utilization metrics

## Configuration Management

### VMRS Configuration

**Configuration File**: `/config/vmrs_gcp.yaml` (default)

**VMRS Features**:
- Volume Management Resource Service
- Resource allocation and management
- Performance optimization

### Feature Flags

**Available Feature Flags**:
- `ENABLE_METRICS`: Enable metrics collection
- `ENABLE_UNIQUE_SERIAL_NUMBER_GENERATION`: Enable unique serial number generation
- `DISABLE_VSA_CLEANUP_ON_VLM_FAILURE`: Disable VSA cleanup on VLM failure
- `ENABLE_AUTO_VOL_OFFLINE_CRON_FOR_GCP_KMS`: Enable auto volume offline for GCP KMS
- `GIN_LOGGING_FEATURE`: Enable Gin logging

## Testing

### Test Coverage

Each pool workflow has comprehensive test coverage:

- **Unit Tests**: Individual workflow function testing
- **Integration Tests**: End-to-end workflow testing
- **Mock Activities**: Mock implementations for testing
- **Error Scenarios**: Testing failure and retry scenarios
- **GCP Integration Tests**: Testing with actual GCP resources

### Test Files

- `pool_workflows_test.go`: Main test file
- Mock implementations for all activities
- Test data fixtures for various scenarios

## Performance Considerations

### Resource Limits

**Node Limits**:
- **Max Nodes Per Group**: 200 (configurable)
- **Concurrent Operations**: Limited by GCP quotas
- **Memory Usage**: Optimized for large-scale deployments

### Timeout Configuration

**Operation Timeouts**:
- **Setup Network Heartbeat**: 300 seconds
- **GCP Operation Wait**: 10 seconds
- **VSA Deployment**: Variable based on instance count

## Security Considerations

### Network Security

**Firewall Rules**:
- Restrictive inbound rules
- Secure outbound connections
- VPC-level security groups

### KMS Security

**Encryption**:
- Customer-managed encryption keys
- KMS key rotation
- Secure key storage

## Related Documentation

- [Volume Workflows](./volume-workflows.md)
- [Cluster Workflows](./cluster-workflows.md)
- [KMS Workflows](../kms/kms-workflows.md)
- [Temporal Debugging Guide](../../guides/temporal-debugging.md)