# Backup Workflows

This document describes the backup-related workflows in the VSA Control Plane system, including backup creation, deletion, restoration, and management operations.

## Overview

Backup workflows manage the complete lifecycle of volume backups in the VSA Control Plane, from creation to deletion. They handle backup vault management, snapshot operations, and restore procedures with comprehensive error handling and rollback mechanisms.

## Workflow Types

### 1. Create Backup Workflow

**File**: `core/orchestrator/workflows/backup_workflow.go`

**Purpose**: Creates new volume backups with snapshot management and vault storage.

**Entry Point**: `CreateBackupWorkflow(ctx workflow.Context, params *commonparams.CreateBackupParams, backup *datamodel.Backup, backupVault *datamodel.BackupVault, volume *datamodel.Volume)`

#### Workflow Structure

```go
type BackupCreateWorkflow struct {
    BaseWorkflow
    SE *database.Storage
}
```

#### Activities

**Backup Creation Activities**:
- `CreateBackupInDB`: Creates backup record in database
- `ValidateBackupParameters`: Validates backup creation parameters
- `CreateSnapshot`: Creates volume snapshot
- `UploadToBackupVault`: Uploads backup to vault
- `ValidateBackupCreation`: Validates backup creation success
- `UpdateBackupStatus`: Updates backup status in database

**Snapshot Management Activities**:
- `CreateVolumeSnapshot`: Creates snapshot of source volume
- `ValidateSnapshotCreation`: Validates snapshot creation
- `ConfigureSnapshotRetention`: Configures snapshot retention policy
- `CleanupOldSnapshots`: Cleans up old snapshots

**Vault Management Activities**:
- `ValidateBackupVault`: Validates backup vault availability
- `UploadBackupData`: Uploads backup data to vault
- `VerifyBackupIntegrity`: Verifies backup data integrity
- `UpdateVaultMetadata`: Updates vault metadata

#### Execution Flow

1. **Pre-creation Phase**:
   - Validate backup parameters
   - Check backup vault availability
   - Prepare backup configuration

2. **Snapshot Creation**:
   - Create volume snapshot
   - Validate snapshot creation
   - Configure snapshot retention

3. **Backup Upload**:
   - Upload backup to vault
   - Verify backup integrity
   - Update vault metadata

4. **Validation and Cleanup**:
   - Validate backup creation
   - Update database records
   - Handle rollback if errors occur

#### Configuration

**Constants**:
```go
const (
    BackupComment        = "VCP-Backup"
    BackupMaxWaitTimeCap = 15 * time.Minute   // Maximum wait time cap
    adcWorkflowTimeout   = 7 * 24 * time.Hour // 7 days timeout
)
```

**Environment Variables**:
```go
Wait = time.Duration(env.GetUint("ONTAP_REST_ASYNC_POLL_WAIT_SECONDS", 3)) * time.Second
```

### 2. Delete Backup Workflow

**Purpose**: Safely deletes backups with proper cleanup.

**Entry Point**: `DeleteBackupWorkflow(ctx workflow.Context, params *commonparams.DeleteBackupParams, backup *datamodel.Backup)`

#### Workflow Structure

```go
type BackupDeleteWorkflow struct {
    BaseWorkflow
    SE              *database.Storage
    deleteInitiated bool
}
```

#### Activities

**Backup Deletion Activities**:
- `DeleteBackupFromVault`: Removes backup from vault
- `DeleteSnapshot`: Deletes associated snapshot
- `CleanupBackupResources`: Cleans up associated resources
- `UpdateBackupStateInDB`: Updates backup state to deleted
- `ValidateBackupDeletion`: Ensures safe deletion

#### Execution Flow

1. **Pre-deletion Checks**: Verify backup can be safely deleted
2. **Vault Cleanup**: Remove backup from vault
3. **Snapshot Deletion**: Delete associated snapshot
4. **Database Update**: Update backup state
5. **Final Cleanup**: Complete cleanup operations

### 3. Backup Restore Workflow

**File**: `core/orchestrator/workflows/backup_restore_workflow.go`

**Purpose**: Restores volumes from backups.

**Entry Point**: `RestoreBackupWorkflow(ctx workflow.Context, params *commonparams.RestoreBackupParams, backup *datamodel.Backup, volume *datamodel.Volume)`

#### Activities

**Backup Restore Activities**:
- `ValidateBackupRestore`: Validates restore parameters
- `DownloadFromBackupVault`: Downloads backup from vault
- `CreateVolumeFromBackup`: Creates volume from backup
- `ValidateRestoreOperation`: Validates restore success
- `UpdateVolumeState`: Updates volume state after restore

#### Execution Flow

1. **Validation**: Validate restore parameters
2. **Download**: Download backup from vault
3. **Restore**: Create volume from backup
4. **Verification**: Verify restore success
5. **Update**: Update volume and backup states

### 4. Backup Update Workflow

**Purpose**: Updates backup properties and configurations.

**Entry Point**: `UpdateBackupWorkflow(ctx workflow.Context, params *commonparams.UpdateBackupParams, backup *datamodel.Backup)`

#### Workflow Structure

```go
type backupUpdateWorkflow struct {
    BaseWorkflow
    SE database.Storage
}
```

#### Activities

**Backup Update Activities**:
- `UpdateBackupInDB`: Updates backup information in database
- `UpdateBackupVaultMetadata`: Updates vault metadata
- `ValidateBackupUpdate`: Validates update parameters
- `ApplyBackupChanges`: Applies configuration changes

## Backup Vault Workflows

### 1. Create Backup Vault Workflow

**File**: `core/orchestrator/workflows/backup_vault_workflows.go`

**Purpose**: Creates new backup vaults for storing backups.

**Entry Point**: `CreateBackupVaultWorkflow(ctx workflow.Context, params *commonparams.CreateBackupVaultParams, vault *datamodel.BackupVault)`

#### Activities

**Vault Creation Activities**:
- `CreateVaultInDB`: Creates vault record in database
- `ValidateVaultParameters`: Validates vault creation parameters
- `CreateVaultStorage`: Creates vault storage infrastructure
- `ConfigureVaultAccess`: Configures vault access permissions
- `ValidateVaultCreation`: Validates vault creation success

### 2. Delete Backup Vault Workflow

**Purpose**: Safely deletes backup vaults with proper cleanup.

**Entry Point**: `DeleteBackupVaultWorkflow(ctx workflow.Context, params *commonparams.DeleteBackupVaultParams, vault *datamodel.BackupVault)`

#### Activities

**Vault Deletion Activities**:
- `DeleteVaultStorage`: Removes vault storage infrastructure
- `CleanupVaultResources`: Cleans up associated resources
- `UpdateVaultStateInDB`: Updates vault state to deleted
- `ValidateVaultDeletion`: Ensures safe deletion

## Backup Policy Workflows

### 1. Create Backup Policy Workflow

**File**: `core/orchestrator/workflows/backup_policy_workflows.go`

**Purpose**: Creates new backup policies for automated backup scheduling.

**Entry Point**: `CreateBackupPolicyWorkflow(ctx workflow.Context, params *commonparams.CreateBackupPolicyParams, policy *datamodel.BackupPolicy)`

#### Activities

**Policy Creation Activities**:
- `CreatePolicyInDB`: Creates policy record in database
- `ValidatePolicyParameters`: Validates policy creation parameters
- `ConfigurePolicySchedule`: Configures backup schedule
- `SetupPolicyTriggers`: Sets up policy triggers
- `ValidatePolicyCreation`: Validates policy creation success

### 2. Update Backup Policy Workflow

**Purpose**: Updates existing backup policies.

**Entry Point**: `UpdateBackupPolicyWorkflow(ctx workflow.Context, params *commonparams.UpdateBackupPolicyParams, policy *datamodel.BackupPolicy)`

#### Activities

**Policy Update Activities**:
- `UpdatePolicyInDB`: Updates policy information in database
- `UpdatePolicySchedule`: Updates backup schedule
- `ValidatePolicyUpdate`: Validates update parameters
- `ApplyPolicyChanges`: Applies configuration changes

## Error Handling

### Rollback Management

Backup workflows implement comprehensive rollback:

```go
defer func() {
    if customErr != nil {
        err2 := backupWf.Revert(ctx, backup, volume, customErr.OriginalErr.Error())
        if err2 != nil {
            backupWf.Logger.Errorf("Failed to execute rollback for workflow %s: %v", backupWf.ID, err2)
        }
    }
}()
```

### Error Types

**Non-Retryable Errors**:
- `PanicError`: System panic errors
- `ValidationError`: Parameter validation errors
- `VaultNotFoundError`: Missing vault errors

**Retryable Errors**:
- `NetworkTimeoutError`: Network operation timeouts
- `VaultOperationError`: Vault operation failures
- `SnapshotError`: Snapshot operation failures

## Configuration

### Timeout Configuration

**Operation Timeouts**:
- **Backup Max Wait Time**: 15 minutes
- **ADC Workflow Timeout**: 7 days
- **Async Poll Wait**: 3 seconds (configurable)

### Retry Policies

All backup workflows use configurable retry policies:

```go
retryPolicy, err := PopulateRetryPolicyParams()
if err != nil {
    return nil, ConvertToVSAError(err)
}
```

## Monitoring and Metrics

### Backup Metrics

**Available Metrics**:
- Backup creation duration
- Backup size and compression ratio
- Vault utilization metrics
- Restore operation metrics

### Health Checks

**Backup Health Monitoring**:
- Vault connectivity status
- Backup integrity status
- Snapshot retention compliance
- Policy execution status

## Security Considerations

### Backup Security

**Encryption**:
- Customer-managed encryption keys
- End-to-end encryption for backup data
- Secure key storage in vault

### Access Control

**Vault Access**:
- Role-based access control
- Audit logging for all operations
- Secure API authentication

## Performance Considerations

### Backup Performance

**Optimization Features**:
- Incremental backup support
- Compression and deduplication
- Parallel upload/download
- Bandwidth throttling

### Resource Management

**Resource Limits**:
- Concurrent backup operations
- Vault storage limits
- Network bandwidth limits

## Testing

### Test Coverage

Each backup workflow has comprehensive test coverage:

- **Unit Tests**: Individual workflow function testing
- **Integration Tests**: End-to-end workflow testing
- **Mock Activities**: Mock implementations for testing
- **Error Scenarios**: Testing failure and retry scenarios
- **Vault Integration Tests**: Testing with actual vault resources

### Test Files

- `backup_workflow_test.go`: Main test file
- `backup_vault_workflows_test.go`: Vault workflow tests
- `backup_policy_workflows_test.go`: Policy workflow tests
- Mock implementations for all activities

## Related Documentation

- [Volume Workflows](./volume-workflows.md)
- [Snapshot Workflows](./snapshot-workflows.md)
- [Background Workflows](../background/scheduled-backup-workflows.md)
- [Temporal Debugging Guide](../../guides/temporal-debugging.md)