# Scheduled Backup Workflows

This document describes the scheduled backup workflows in the VSA Control Plane system, including automated backup scheduling, execution, and management operations.

## Overview

Scheduled backup workflows manage automated backup operations based on backup policies. These workflows handle backup scheduling, execution, and cleanup operations with comprehensive error handling and monitoring.

## Workflow Types

### 1. Create Scheduled Backup Init Workflow

**File**: `core/orchestrator/workflows/backgroundworkflows/scheduled_backup_workflows.go`

**Purpose**: Initializes scheduled backup workflows for backup policies.

**Entry Point**: `CreateScheduledBackupInitWorkflow(ctx workflow.Context, backupPolicy *datamodel.BackupPolicy)`

#### Workflow Structure

```go
type createScheduledBackupInitWorkflow struct {
    baseScheduledBackupWorkflow
}

type baseScheduledBackupWorkflow struct {
    workflows.BaseWorkflow
}
```

#### Configuration

**Environment Variables**:
```go
var (
    hydrationEnabled          = env.GetBool("GCP_HYDRATE_ENABLED", true)
    scheduledWeeklyBackupDay  = env.GetInt("SCHEDULED_WEEKLY_BACKUP_DAY", 1)  // Default to Monday
    scheduledMonthlyBackupDay = env.GetInt("SCHEDULED_MONTHLY_BACKUP_DAY", 1) // Default to 1st day of the month
)
```

**Constants**:
```go
const (
    scheduledBackupTimestampFormat = "2006-01-02-150405"
)
```

#### Activities

**Scheduled Backup Init Activities**:
- `InitializeScheduledBackup`: Initializes scheduled backup for policy
- `ValidateBackupPolicy`: Validates backup policy parameters
- `SetupBackupSchedule`: Sets up backup schedule
- `CreateBackupJobs`: Creates backup jobs for scheduled execution
- `ValidateScheduledBackupInit`: Validates initialization success

#### Execution Flow

1. **Initialization Phase**:
   - Create job record in database
   - Validate backup policy parameters
   - Set up workflow context

2. **Schedule Setup**:
   - Configure backup schedule based on policy
   - Create backup jobs for scheduled execution
   - Set up monitoring and tracking

3. **Validation and Cleanup**:
   - Validate scheduled backup initialization
   - Update job status
   - Handle rollback if errors occur

### 2. Create Scheduled Backup Workflow

**Purpose**: Executes scheduled backup operations.

**Entry Point**: `CreateScheduledBackupWorkflow(ctx workflow.Context, backupPolicy *datamodel.BackupPolicy, volume *datamodel.Volume)`

#### Workflow Structure

```go
type createScheduledBackupWorkflow struct {
    baseScheduledBackupWorkflow
}
```

#### Activities

**Scheduled Backup Activities**:
- `ExecuteScheduledBackup`: Executes scheduled backup operation
- `CreateBackupSnapshot`: Creates backup snapshot
- `UploadBackupToVault`: Uploads backup to vault
- `ValidateBackupExecution`: Validates backup execution
- `UpdateBackupStatus`: Updates backup status

#### Execution Flow

1. **Pre-execution Phase**:
   - Validate backup policy and volume
   - Check backup vault availability
   - Prepare backup configuration

2. **Backup Execution**:
   - Create volume snapshot
   - Upload backup to vault
   - Validate backup integrity

3. **Post-execution Phase**:
   - Update backup status
   - Clean up temporary resources
   - Schedule next backup if applicable

### 3. Delete Scheduled Backup Workflow

**Purpose**: Deletes scheduled backup workflows and cleans up resources.

**Entry Point**: `DeleteScheduledBackupWorkflow(ctx workflow.Context, backupPolicy *datamodel.BackupPolicy)`

#### Workflow Structure

```go
type deleteScheduledBackupWorkflow struct {
    baseScheduledBackupWorkflow
}
```

#### Activities

**Scheduled Backup Deletion Activities**:
- `DeleteScheduledBackupJobs`: Deletes scheduled backup jobs
- `CleanupBackupResources`: Cleans up backup resources
- `UpdateBackupPolicyStatus`: Updates backup policy status
- `ValidateScheduledBackupDeletion`: Ensures safe deletion

#### Execution Flow

1. **Pre-deletion Phase**:
   - Validate deletion parameters
   - Check for active backup operations
   - Prepare cleanup configuration

2. **Deletion Phase**:
   - Delete scheduled backup jobs
   - Clean up backup resources
   - Update backup policy status

3. **Post-deletion Phase**:
   - Validate deletion completion
   - Update job status
   - Complete cleanup operations

## Backup Scheduling

### Schedule Types

**Daily Backups**:
- Executed every day at specified time
- Configurable time and timezone
- Automatic cleanup of old backups

**Weekly Backups**:
- Executed on specified day of week
- Default: Monday (configurable)
- Retention policy based on weekly schedule

**Monthly Backups**:
- Executed on specified day of month
- Default: 1st day of month (configurable)
- Long-term retention policy

### Schedule Configuration

**Backup Policy Parameters**:
- **Schedule Type**: Daily, Weekly, Monthly
- **Execution Time**: Time of day for backup execution
- **Retention Period**: How long to keep backups
- **Backup Vault**: Target vault for backups
- **Volume Selection**: Which volumes to backup

## Sync Workflows

### 1. Sync VSA Snapshots Parent Workflow

**File**: `core/orchestrator/workflows/backgroundworkflows/sync_vsa_snapshots_parent_workflow.go`

**Purpose**: Orchestrates VSA snapshot synchronization operations.

**Entry Point**: `SyncVSASnapshotsParentWorkflow(ctx workflow.Context, params *common.SyncVSASnapshotsParams)`

#### Activities

**Sync Parent Activities**:
- `InitializeSyncOperation`: Initializes sync operation
- `ValidateSyncParameters`: Validates sync parameters
- `CoordinateChildWorkflows`: Coordinates child workflow execution
- `ValidateSyncCompletion`: Validates sync completion

### 2. Sync VSA Snapshots Child Workflow

**File**: `core/orchestrator/workflows/backgroundworkflows/sync_vsa_snapshots_child_workflow.go`

**Purpose**: Executes individual VSA snapshot synchronization operations.

**Entry Point**: `SyncVSASnapshotsChildWorkflow(ctx workflow.Context, params *common.SyncVSASnapshotsChildParams)`

#### Activities

**Sync Child Activities**:
- `SyncSnapshotFromVSA`: Syncs snapshot from VSA
- `UpdateSnapshotInDB`: Updates snapshot in database
- `ValidateSnapshotSync`: Validates snapshot sync
- `CleanupSyncResources`: Cleans up sync resources

### 3. Sync Backup ZIZS Workflow

**File**: `core/orchestrator/workflows/backgroundworkflows/sync_backup_zizs_workflow.go`

**Purpose**: Synchronizes backup data with ZIZS (Zero-Impact Zero-Service) systems.

**Entry Point**: `SyncBackupZIZSWorkflow(ctx workflow.Context, params *common.SyncBackupZIZSParams)`

#### Activities

**ZIZS Sync Activities**:
- `SyncBackupToZIZS`: Syncs backup to ZIZS system
- `ValidateZIZSSync`: Validates ZIZS sync
- `UpdateZIZSStatus`: Updates ZIZS status
- `CleanupZIZSResources`: Cleans up ZIZS resources

## Resource Cleanup Workflows

### 1. Resource Cleanup Parent Workflow

**File**: `core/orchestrator/workflows/backgroundworkflows/resource_cleanup_parent_workflow.go`

**Purpose**: Orchestrates resource cleanup operations.

**Entry Point**: `ResourceCleanupParentWorkflow(ctx workflow.Context, params *common.ResourceCleanupParams)`

#### Activities

**Cleanup Parent Activities**:
- `InitializeCleanupOperation`: Initializes cleanup operation
- `ValidateCleanupParameters`: Validates cleanup parameters
- `CoordinateChildCleanup`: Coordinates child cleanup workflows
- `ValidateCleanupCompletion`: Validates cleanup completion

### 2. Resource Cleanup Child Workflow

**File**: `core/orchestrator/workflows/backgroundworkflows/resource_cleanup_child_workflow.go`

**Purpose**: Executes individual resource cleanup operations.

**Entry Point**: `ResourceCleanupChildWorkflow(ctx workflow.Context, params *common.ResourceCleanupChildParams)`

#### Activities

**Cleanup Child Activities**:
- `CleanupResource`: Cleans up specific resource
- `ValidateResourceCleanup`: Validates resource cleanup
- `UpdateResourceStatus`: Updates resource status
- `LogCleanupOperation`: Logs cleanup operation

### 3. Hard Delete Workflow

**File**: `core/orchestrator/workflows/backgroundworkflows/hard_delete_workflow.go`

**Purpose**: Performs hard deletion of resources with complete cleanup.

**Entry Point**: `HardDeleteWorkflow(ctx workflow.Context, params *common.HardDeleteParams)`

#### Activities

**Hard Delete Activities**:
- `ValidateHardDelete`: Validates hard delete parameters
- `DeleteResourceCompletely`: Deletes resource completely
- `CleanupAllReferences`: Cleans up all references
- `ValidateHardDeleteCompletion`: Validates hard delete completion

## Orphan Job Scheduler Workflow

### Orphan Job Scheduler Workflow

**File**: `core/orchestrator/workflows/backgroundworkflows/orphan_job_scheduler_workflow.go`

**Purpose**: Finds and processes orphaned jobs that are stuck in PENDING status.

**Entry Point**: `OrphanJobSchedulerWorkflow(ctx workflow.Context)`

#### Workflow Structure

```go
func OrphanJobSchedulerWorkflow(ctx workflow.Context) error {
    ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{
        "workflowID": workflow.GetInfo(ctx).WorkflowExecution.ID,
        "requestID": utils.RandomUUID(),
    })
    logger := util.GetLogger(ctx)
    logger.Infof("Starting OrphanJobSchedulerWorkflow")
    // ... workflow logic
}
```

#### Activities

**Orphan Job Activities**:
- `FindOrphanedJobs`: Finds jobs stuck in PENDING status
- `ProcessOrphanedJob`: Processes individual orphaned job
- `UpdateJobStatus`: Updates job status
- `ValidateOrphanJobProcessing`: Validates orphan job processing

#### Execution Flow

1. **Discovery Phase**:
   - Find jobs with PENDING status
   - Identify orphaned jobs
   - Prepare processing queue

2. **Processing Phase**:
   - Process each orphaned job
   - Update job status
   - Handle job failures

3. **Validation Phase**:
   - Validate processing completion
   - Update monitoring metrics
   - Log processing results

## Error Handling

### Error Types

**Non-Retryable Errors**:
- `PanicError`: System panic errors
- `ValidationError`: Parameter validation errors
- `PolicyNotFoundError`: Missing backup policy errors

**Retryable Errors**:
- `NetworkTimeoutError`: Network operation timeouts
- `BackupOperationError`: Backup operation failures
- `SyncOperationError`: Sync operation failures

### Rollback Management

Background workflows implement rollback for failed operations:

```go
defer func() {
    if err != nil {
        err2 := workflow.ExecuteActivity(ctx, backgroundActivity.UpdateJobStatus, 
            job.UUID, string(models.JobsStateERROR), err).Get(ctx, nil)
        if err2 != nil {
            log.Errorf("Failed to update job status to error: %v", err2)
        }
    }
}()
```

## Configuration

### Retry Policies

All background workflows use configurable retry policies:

```go
retryPolicy, err := workflows.PopulateRetryPolicyParams()
if err != nil {
    logger.Errorf("Failed to populate retry policy params: %v", err)
    return err
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

### Background Workflow Metrics

**Available Metrics**:
- Scheduled backup execution duration
- Sync operation success rate
- Resource cleanup metrics
- Orphan job processing metrics

### Health Checks

**Background Workflow Health Monitoring**:
- Scheduled backup status
- Sync operation status
- Resource cleanup status
- Orphan job processing status

## Performance Considerations

### Background Workflow Performance

**Optimization Features**:
- Parallel execution of child workflows
- Batch processing of operations
- Resource utilization monitoring
- Automatic scaling based on load

### Resource Management

**Resource Limits**:
- Concurrent background operations
- Memory usage limits
- Network bandwidth limits
- Storage limits

## Security Considerations

### Background Workflow Security

**Access Control**:
- Role-based access control
- Background operation permissions
- Audit logging for all operations

### Data Protection

**Data Security**:
- Encrypted backup data
- Secure sync operations
- Secure cleanup operations
- Audit trail for all operations

## Testing

### Test Coverage

Each background workflow has comprehensive test coverage:

- **Unit Tests**: Individual workflow function testing
- **Integration Tests**: End-to-end workflow testing
- **Mock Activities**: Mock implementations for testing
- **Error Scenarios**: Testing failure and retry scenarios
- **Scheduling Tests**: Testing backup scheduling logic

### Test Files

- `scheduled_backup_workflows_test.go`: Scheduled backup tests
- `sync_vsa_snapshots_parent_workflow_test.go`: Sync parent tests
- `sync_vsa_snapshots_child_workflow_test.go`: Sync child tests
- `resource_cleanup_workflow_test.go`: Cleanup workflow tests
- `orphan_job_scheduler_workflow_test.go`: Orphan job tests

## Related Documentation

- [Backup Workflows](../core/backup-workflows.md)
- [Volume Workflows](../core/volume-workflows.md)
- [Temporal Debugging Guide](../../guides/temporal-debugging.md)