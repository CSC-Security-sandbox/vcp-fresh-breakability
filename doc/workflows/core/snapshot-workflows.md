# Snapshot Workflows

This document describes the snapshot-related workflows in the VSA Control Plane system, including snapshot creation, deletion, update, and management operations.

## Overview

Snapshot workflows manage the complete lifecycle of volume snapshots in the VSA Control Plane, from creation to deletion. They handle snapshot operations, retention policies, and integration with backup workflows.

## Workflow Types

### 1. Create Snapshot Workflow

**File**: `core/orchestrator/workflows/snapshot_create_workflow.go`

**Purpose**: Creates new volume snapshots with proper validation and metadata management.

**Entry Point**: `CreateSnapshotWorkflow(ctx workflow.Context, params *common.CreateSnapshotParams, snapshot *datamodel.Snapshot)`

#### Workflow Structure

```go
type snapshotCreateWorkflow struct {
    BaseWorkflow
    SE database.Storage
}
```

#### Activities

**Snapshot Creation Activities**:
- `CreateSnapshotInDB`: Creates snapshot record in database
- `ValidateSnapshotParameters`: Validates snapshot creation parameters
- `CreateSnapshotInVSA`: Creates snapshot in VSA cluster
- `ValidateSnapshotCreation`: Validates snapshot creation success
- `UpdateSnapshotStatus`: Updates snapshot status in database
- `ConfigureSnapshotRetention`: Configures snapshot retention policy

**VSA Integration Activities**:
- `CreateVolumeSnapshot`: Creates snapshot of source volume
- `ValidateSnapshotIntegrity`: Validates snapshot data integrity
- `ConfigureSnapshotAccess`: Configures snapshot access permissions
- `UpdateSnapshotMetadata`: Updates snapshot metadata

#### Execution Flow

1. **Pre-creation Phase**:
   - Validate snapshot parameters
   - Check volume availability
   - Prepare snapshot configuration

2. **Snapshot Creation**:
   - Create snapshot in VSA cluster
   - Validate snapshot creation
   - Configure retention policy

3. **Database Update**:
   - Update snapshot information
   - Set snapshot status
   - Configure metadata

4. **Validation and Cleanup**:
   - Validate snapshot creation
   - Update job status
   - Handle rollback if errors occur

#### Error Handling

```go
if customErr != nil {
    snapshotWf.Status = WorkflowStatusFailed
    jobUpdateErr := snapshotWf.UpdateJobStatus(ctx, string(models.JobsStateERROR), customErr)
    if jobUpdateErr != nil {
        logger.Errorf("Failed to update job status to Done with error for CreateSnapshotWorkflow: %v", jobUpdateErr)
        return nil, jobUpdateErr
    }
    return nil, customErr
}
```

### 2. Delete Snapshot Workflow

**File**: `core/orchestrator/workflows/snapshot_delete_workflow.go`

**Purpose**: Safely deletes snapshots with proper cleanup.

**Entry Point**: `DeleteSnapshotWorkflow(ctx workflow.Context, params *common.DeleteSnapshotParams, snapshot *datamodel.Snapshot)`

#### Workflow Structure

```go
type snapshotDeleteWorkflow struct {
    BaseWorkflow
    SE database.Storage
}
```

#### Activities

**Snapshot Deletion Activities**:
- `DeleteSnapshotFromVSA`: Removes snapshot from VSA cluster
- `CleanupSnapshotResources`: Cleans up associated resources
- `UpdateSnapshotStateInDB`: Updates snapshot state to deleted
- `ValidateSnapshotDeletion`: Ensures safe deletion
- `CleanupSnapshotMetadata`: Cleans up snapshot metadata

#### Execution Flow

1. **Pre-deletion Checks**: Verify snapshot can be safely deleted
2. **VSA Deletion**: Delete snapshot from VSA cluster
3. **Resource Cleanup**: Clean up associated resources
4. **Database Update**: Update snapshot state
5. **Final Cleanup**: Complete cleanup operations

### 3. Update Snapshot Workflow

**File**: `core/orchestrator/workflows/snapshot_update_workflow.go`

**Purpose**: Updates snapshot properties and configurations.

**Entry Point**: `UpdateSnapshotWorkflow(ctx workflow.Context, params *common.UpdateSnapshotParams, snapshot *datamodel.Snapshot)`

#### Workflow Structure

```go
type snapshotUpdateWorkflow struct {
    BaseWorkflow
    SE database.Storage
}
```

#### Activities

**Snapshot Update Activities**:
- `UpdateSnapshotInDB`: Updates snapshot information in database
- `UpdateSnapshotInVSA`: Updates snapshot in VSA cluster
- `ValidateSnapshotUpdate`: Validates update parameters
- `ApplySnapshotChanges`: Applies configuration changes
- `UpdateSnapshotMetadata`: Updates snapshot metadata

#### Execution Flow

1. **Validation**: Validate update parameters
2. **VSA Update**: Update snapshot in VSA cluster
3. **Database Update**: Update snapshot information
4. **Verification**: Verify changes were applied correctly

## Snapshot Management

### Snapshot Lifecycle

**States**:
- `CREATED`: Snapshot has been created but not processed
- `PROCESSING`: Snapshot is being created
- `COMPLETED`: Snapshot created successfully
- `FAILED`: Snapshot creation failed
- `DELETED`: Snapshot has been deleted

### Retention Policies

**Retention Configuration**:
- **Default Retention**: Configurable per snapshot
- **Automatic Cleanup**: Old snapshots are automatically cleaned up
- **Retention Validation**: Ensures retention policies are followed

### Snapshot Metadata

**Metadata Fields**:
- **Snapshot ID**: Unique identifier
- **Volume ID**: Source volume identifier
- **Creation Time**: When snapshot was created
- **Size**: Snapshot size in bytes
- **Retention Policy**: Retention configuration
- **Status**: Current snapshot status

## Integration with Backup Workflows

### Backup Integration

**Backup Workflow Integration**:
- Snapshots are used as the source for backups
- Snapshot creation triggers backup workflows
- Backup workflows manage snapshot lifecycle

**Backup Activities**:
- `CreateSnapshotForBackup`: Creates snapshot for backup
- `ValidateSnapshotForBackup`: Validates snapshot for backup
- `CleanupSnapshotAfterBackup`: Cleans up snapshot after backup

### Volume Integration

**Volume Workflow Integration**:
- Snapshots are created from volumes
- Volume deletion may require snapshot cleanup
- Volume restore may use snapshots

## Error Handling

### Error Types

**Non-Retryable Errors**:
- `PanicError`: System panic errors
- `ValidationError`: Parameter validation errors
- `VolumeNotFoundError`: Missing volume errors

**Retryable Errors**:
- `NetworkTimeoutError`: Network operation timeouts
- `VSAOperationError`: VSA operation failures
- `SnapshotError`: Snapshot operation failures

### Rollback Management

Snapshot workflows implement rollback for failed operations:

```go
defer func() {
    if err != nil {
        err2 := workflow.ExecuteActivity(ctx, snapshotActivity.UpdateSnapshotStateInDB, 
            snapshot.UUID, models.LifeCycleStateError, models.LifeCycleStateCreationErrorDetails).Get(ctx, nil)
        if err2 != nil {
            log.Errorf("Failed to update snapshot state in DB to error: %v", err2)
        }
    }
}()
```

## Configuration

### Retry Policies

All snapshot workflows use configurable retry policies:

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

### Snapshot Metrics

**Available Metrics**:
- Snapshot creation duration
- Snapshot size and compression ratio
- Snapshot retention compliance
- Snapshot deletion metrics

### Health Checks

**Snapshot Health Monitoring**:
- VSA cluster connectivity status
- Snapshot integrity status
- Retention policy compliance
- Storage utilization metrics

## Performance Considerations

### Snapshot Performance

**Optimization Features**:
- Incremental snapshot support
- Compression and deduplication
- Parallel snapshot operations
- Storage optimization

### Resource Management

**Resource Limits**:
- Concurrent snapshot operations
- Storage space limits
- Network bandwidth limits

## Security Considerations

### Snapshot Security

**Access Control**:
- Role-based access control
- Snapshot access permissions
- Audit logging for all operations

### Data Protection

**Data Integrity**:
- Snapshot integrity validation
- Checksum verification
- Secure snapshot storage

## Testing

### Test Coverage

Each snapshot workflow has comprehensive test coverage:

- **Unit Tests**: Individual workflow function testing
- **Integration Tests**: End-to-end workflow testing
- **Mock Activities**: Mock implementations for testing
- **Error Scenarios**: Testing failure and retry scenarios
- **VSA Integration Tests**: Testing with actual VSA resources

### Test Files

- `snapshot_create_workflow_test.go`: Create workflow tests
- `snapshot_delete_workflow_test.go`: Delete workflow tests
- `snapshot_update_workflow_test.go`: Update workflow tests
- Mock implementations for all activities

## Related Documentation

- [Volume Workflows](./volume-workflows.md)
- [Backup Workflows](./backup-workflows.md)
- [Temporal Debugging Guide](../../guides/temporal-debugging.md)