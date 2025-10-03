# Replication Workflows

This document describes the replication workflows in the VSA Control Plane system, including volume replication creation, management, and cleanup operations.

## Overview

Replication workflows manage volume replication operations between VSA clusters, including replication creation, updates, deletion, and management. These workflows ensure data consistency and availability across multiple clusters.

## Workflow Types

### 1. Create Volume Replication Workflow

**File**: `core/orchestrator/workflows/replicationWorkflows/replication_create_workflow.go`

**Purpose**: Creates new volume replication relationships between VSA clusters.

**Entry Point**: `CreateVolumeReplicationWorkflow(ctx workflow.Context, params *common.CreateVolumeReplicationParams, volumeRep *datamodel.VolumeReplication, event *replication.CreateReplicationEvent)`

#### Workflow Structure

```go
type createVolumeReplicationWorkflow struct {
    workflows.BaseWorkflow
    SE *database.Storage
}
```

#### Configuration

**Environment Variables**:
```go
var (
    ReplicationJobsRetryMaxAttempts = env.GetInt("REPLICATION_JOBS_RETRY_MAX_ATTEMPTS", 10)
)
```

#### Activities

**Replication Creation Activities**:
- `CreateReplicationInDB`: Creates replication record in database
- `ValidateReplicationParameters`: Validates replication creation parameters
- `CreateReplicationInVSA`: Creates replication in VSA cluster
- `ValidateReplicationCreation`: Validates replication creation success
- `UpdateReplicationStatus`: Updates replication status in database
- `ConfigureReplicationSettings`: Configures replication settings

**VSA Integration Activities**:
- `CreateVolumeReplication`: Creates volume replication in VSA
- `ValidateReplicationIntegrity`: Validates replication data integrity
- `ConfigureReplicationAccess`: Configures replication access permissions
- `UpdateReplicationMetadata`: Updates replication metadata

#### Execution Flow

1. **Pre-creation Phase**:
   - Validate replication parameters
   - Check source and destination cluster availability
   - Prepare replication configuration

2. **Replication Creation**:
   - Create replication in VSA cluster
   - Validate replication creation
   - Configure replication settings

3. **Database Update**:
   - Update replication information
   - Set replication status
   - Configure metadata

4. **Validation and Cleanup**:
   - Validate replication creation
   - Update job status
   - Handle rollback if errors occur

#### Error Handling

```go
if customErr != nil {
    volumeRepWf.Status = workflows.WorkflowStatusFailed
    err = volumeRepWf.UpdateJobStatus(ctx, string(models.JobsStateERROR), customErr)
    return nil, err
}
```

### 2. Update Volume Replication Workflow

**File**: `core/orchestrator/workflows/replicationWorkflows/replication_update_workflow.go`

**Purpose**: Updates existing volume replication configurations.

**Entry Point**: `UpdateVolumeReplicationWorkflow(ctx workflow.Context, params *common.UpdateVolumeReplicationParams, volumeRep *datamodel.VolumeReplication)`

#### Activities

**Replication Update Activities**:
- `UpdateReplicationInDB`: Updates replication information in database
- `UpdateReplicationInVSA`: Updates replication in VSA cluster
- `ValidateReplicationUpdate`: Validates update parameters
- `ApplyReplicationChanges`: Applies configuration changes
- `UpdateReplicationMetadata`: Updates replication metadata

#### Execution Flow

1. **Validation**: Validate update parameters
2. **VSA Update**: Update replication in VSA cluster
3. **Database Update**: Update replication information
4. **Verification**: Verify changes were applied correctly

### 3. Delete Volume Replication Workflow

**File**: `core/orchestrator/workflows/replicationWorkflows/replication_delete_workflow.go`

**Purpose**: Safely deletes volume replication relationships.

**Entry Point**: `DeleteVolumeReplicationWorkflow(ctx workflow.Context, params *common.DeleteVolumeReplicationParams, volumeRep *datamodel.VolumeReplication)`

#### Activities

**Replication Deletion Activities**:
- `DeleteReplicationFromVSA`: Removes replication from VSA cluster
- `CleanupReplicationResources`: Cleans up associated resources
- `UpdateReplicationStateInDB`: Updates replication state to deleted
- `ValidateReplicationDeletion`: Ensures safe deletion
- `CleanupReplicationMetadata`: Cleans up replication metadata

#### Execution Flow

1. **Pre-deletion Checks**: Verify replication can be safely deleted
2. **VSA Deletion**: Delete replication from VSA cluster
3. **Resource Cleanup**: Clean up associated resources
4. **Database Update**: Update replication state
5. **Final Cleanup**: Complete cleanup operations

### 4. Resume Volume Replication Workflow

**File**: `core/orchestrator/workflows/replicationWorkflows/replication_resume_workflow.go`

**Purpose**: Resumes paused volume replication operations.

**Entry Point**: `ResumeVolumeReplicationWorkflow(ctx workflow.Context, params *common.ResumeVolumeReplicationParams, volumeRep *datamodel.VolumeReplication)`

#### Activities

**Replication Resume Activities**:
- `ResumeReplicationInVSA`: Resumes replication in VSA cluster
- `ValidateReplicationResume`: Validates resume operation
- `UpdateReplicationStatus`: Updates replication status
- `MonitorReplicationHealth`: Monitors replication health

### 5. Stop Volume Replication Workflow

**File**: `core/orchestrator/workflows/replicationWorkflows/replication_stop_workflow.go`

**Purpose**: Stops active volume replication operations.

**Entry Point**: `StopVolumeReplicationWorkflow(ctx workflow.Context, params *common.StopVolumeReplicationParams, volumeRep *datamodel.VolumeReplication)`

#### Activities

**Replication Stop Activities**:
- `StopReplicationInVSA`: Stops replication in VSA cluster
- `ValidateReplicationStop`: Validates stop operation
- `UpdateReplicationStatus`: Updates replication status
- `CleanupReplicationResources`: Cleans up replication resources

### 6. Release Volume Replication Workflow

**File**: `core/orchestrator/workflows/replicationWorkflows/replication_release_workflow.go`

**Purpose**: Releases volume replication resources and relationships.

**Entry Point**: `ReleaseVolumeReplicationWorkflow(ctx workflow.Context, params *common.ReleaseVolumeReplicationParams, volumeRep *datamodel.VolumeReplication)`

#### Activities

**Replication Release Activities**:
- `ReleaseReplicationResources`: Releases replication resources
- `ValidateReplicationRelease`: Validates release operation
- `UpdateReplicationStatus`: Updates replication status
- `CleanupReplicationMetadata`: Cleans up replication metadata

## Replication Internal Workflows

### 1. Replication Internal Create Workflow

**File**: `core/orchestrator/workflows/replicationWorkflows/replication_internal_create_workflow.go`

**Purpose**: Internal workflow for creating replication relationships.

**Entry Point**: `ReplicationInternalCreateWorkflow(ctx workflow.Context, params *common.ReplicationInternalCreateParams)`

#### Activities

**Internal Create Activities**:
- `CreateInternalReplication`: Creates internal replication
- `ValidateInternalReplication`: Validates internal replication
- `UpdateInternalReplicationStatus`: Updates internal replication status

### 2. Replication Internal Update Workflow

**File**: `core/orchestrator/workflows/replicationWorkflows/replication_internal_update_workflow.go`

**Purpose**: Internal workflow for updating replication relationships.

**Entry Point**: `ReplicationInternalUpdateWorkflow(ctx workflow.Context, params *common.ReplicationInternalUpdateParams)`

#### Activities

**Internal Update Activities**:
- `UpdateInternalReplication`: Updates internal replication
- `ValidateInternalReplicationUpdate`: Validates internal replication update
- `UpdateInternalReplicationStatus`: Updates internal replication status

### 3. Replication Internal Delete Workflow

**File**: `core/orchestrator/workflows/replicationWorkflows/replication_internal_delete_workflow.go`

**Purpose**: Internal workflow for deleting replication relationships.

**Entry Point**: `ReplicationInternalDeleteWorkflow(ctx workflow.Context, params *common.ReplicationInternalDeleteParams)`

#### Activities

**Internal Delete Activities**:
- `DeleteInternalReplication`: Deletes internal replication
- `ValidateInternalReplicationDeletion`: Validates internal replication deletion
- `CleanupInternalReplicationResources`: Cleans up internal replication resources

### 4. Replication Internal Resume Workflow

**File**: `core/orchestrator/workflows/replicationWorkflows/replication_internal_resume_workflow.go`

**Purpose**: Internal workflow for resuming replication relationships.

**Entry Point**: `ReplicationInternalResumeWorkflow(ctx workflow.Context, params *common.ReplicationInternalResumeParams)`

#### Activities

**Internal Resume Activities**:
- `ResumeInternalReplication`: Resumes internal replication
- `ValidateInternalReplicationResume`: Validates internal replication resume
- `UpdateInternalReplicationStatus`: Updates internal replication status

### 5. Replication Internal Stop Workflow

**File**: `core/orchestrator/workflows/replicationWorkflows/replication_internal_stop_workflow.go`

**Purpose**: Internal workflow for stopping replication relationships.

**Entry Point**: `ReplicationInternalStopWorkflow(ctx workflow.Context, params *common.ReplicationInternalStopParams)`

#### Activities

**Internal Stop Activities**:
- `StopInternalReplication`: Stops internal replication
- `ValidateInternalReplicationStop`: Validates internal replication stop
- `UpdateInternalReplicationStatus`: Updates internal replication status

### 6. Replication Internal Mount Job Workflow

**File**: `core/orchestrator/workflows/replicationWorkflows/replication_internal_mountJob_workflow.go`

**Purpose**: Internal workflow for mounting replication jobs.

**Entry Point**: `ReplicationInternalMountJobWorkflow(ctx workflow.Context, params *common.ReplicationInternalMountJobParams)`

#### Activities

**Internal Mount Job Activities**:
- `MountReplicationJob`: Mounts replication job
- `ValidateReplicationJobMount`: Validates replication job mount
- `UpdateReplicationJobStatus`: Updates replication job status

### 7. Replication Internal Snapshot Delete Workflow

**File**: `core/orchestrator/workflows/replicationWorkflows/replication_internal_snapshot_delete_workflow.go`

**Purpose**: Internal workflow for deleting replication snapshots.

**Entry Point**: `ReplicationInternalSnapshotDeleteWorkflow(ctx workflow.Context, params *common.ReplicationInternalSnapshotDeleteParams)`

#### Activities

**Internal Snapshot Delete Activities**:
- `DeleteReplicationSnapshot`: Deletes replication snapshot
- `ValidateReplicationSnapshotDeletion`: Validates replication snapshot deletion
- `CleanupReplicationSnapshotResources`: Cleans up replication snapshot resources

### 8. Replication Internal Get Multiple Workflow

**File**: `core/orchestrator/workflows/replicationWorkflows/replication_internal_getMultiple_workflow.go`

**Purpose**: Internal workflow for retrieving multiple replication relationships.

**Entry Point**: `ReplicationInternalGetMultipleWorkflow(ctx workflow.Context, params *common.ReplicationInternalGetMultipleParams)`

#### Activities

**Internal Get Multiple Activities**:
- `GetMultipleReplications`: Retrieves multiple replications
- `ValidateReplicationData`: Validates replication data
- `FormatReplicationResponse`: Formats replication response

## Replication Management

### Replication Lifecycle

**States**:
- `CREATED`: Replication has been created but not started
- `RUNNING`: Replication is actively running
- `PAUSED`: Replication is paused
- `STOPPED`: Replication is stopped
- `FAILED`: Replication has failed
- `DELETED`: Replication has been deleted

### Replication Types

**Synchronous Replication**:
- Real-time data replication
- Immediate consistency
- Higher performance impact

**Asynchronous Replication**:
- Near real-time data replication
- Eventual consistency
- Lower performance impact

### Replication Configuration

**Replication Settings**:
- **Replication Type**: Synchronous or Asynchronous
- **Replication Schedule**: When to replicate
- **Retention Policy**: How long to keep replication data
- **Compression**: Whether to compress replication data
- **Encryption**: Whether to encrypt replication data

## Error Handling

### Error Types

**Non-Retryable Errors**:
- `PanicError`: System panic errors
- `ValidationError`: Parameter validation errors
- `ReplicationNotFoundError`: Missing replication errors

**Retryable Errors**:
- `NetworkTimeoutError`: Network operation timeouts
- `VSAOperationError`: VSA operation failures
- `ReplicationError`: Replication operation failures

### Rollback Management

Replication workflows implement rollback for failed operations:

```go
defer func() {
    if err != nil {
        err2 := workflow.ExecuteActivity(ctx, replicationActivity.UpdateReplicationStateInDB, 
            replication.UUID, models.LifeCycleStateError, models.LifeCycleStateCreationErrorDetails).Get(ctx, nil)
        if err2 != nil {
            log.Errorf("Failed to update replication state in DB to error: %v", err2)
        }
    }
}()
```

## Configuration

### Retry Policies

All replication workflows use configurable retry policies:

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

### Replication Metrics

**Available Metrics**:
- Replication creation duration
- Replication success rate
- Replication lag metrics
- Replication throughput metrics

### Health Checks

**Replication Health Monitoring**:
- Replication connectivity status
- Replication data integrity
- Replication performance metrics
- Replication error rates

## Performance Considerations

### Replication Performance

**Optimization Features**:
- Parallel replication operations
- Compression and deduplication
- Bandwidth throttling
- Replication scheduling

### Resource Management

**Resource Limits**:
- Concurrent replication operations
- Network bandwidth limits
- Storage space limits
- CPU usage limits

## Security Considerations

### Replication Security

**Access Control**:
- Role-based access control
- Replication permissions
- Audit logging for all operations

### Data Protection

**Data Security**:
- Encrypted replication data
- Secure replication channels
- Data integrity validation
- Secure key management

## Testing

### Test Coverage

Each replication workflow has comprehensive test coverage:

- **Unit Tests**: Individual workflow function testing
- **Integration Tests**: End-to-end workflow testing
- **Mock Activities**: Mock implementations for testing
- **Error Scenarios**: Testing failure and retry scenarios
- **VSA Integration Tests**: Testing with actual VSA resources

### Test Files

- `replication_create_workflow_test.go`: Create workflow tests
- `replication_update_workflow_test.go`: Update workflow tests
- `replication_delete_workflow_test.go`: Delete workflow tests
- `replication_internal_*_workflow_test.go`: Internal workflow tests

## Related Documentation

- [Volume Workflows](../core/volume-workflows.md)
- [Pool Workflows](../core/pool-workflows.md)
- [Temporal Debugging Guide](../../guides/temporal-debugging.md)