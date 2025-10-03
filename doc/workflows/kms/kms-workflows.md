# KMS Workflows

This document describes the KMS (Key Management Service) workflows in the VSA Control Plane system, including KMS configuration creation, management, migration, and cleanup operations.

## Overview

KMS workflows manage Key Management Service operations for VSA clusters, including KMS configuration, key management, encryption setup, and migration operations. These workflows ensure proper encryption key management and security compliance.

## Workflow Types

### 1. Create KMS Config Workflow

**File**: `core/orchestrator/workflows/kms_workflows/kms_config_create_workflow.go`

**Purpose**: Creates new KMS configurations for VSA clusters.

**Entry Point**: `CreateKmsConfigWorkflow(ctx workflow.Context, params *common.CreateKmsConfigParams, kmsConfig *datamodel.KmsConfig)`

#### Workflow Structure

```go
type createKmsConfigWorkflow struct {
    workflows.BaseWorkflow
}
```

#### Configuration

**Environment Variables**:
```go
var (
    cvpMaxPollTimeout = env.GetUint64("CVP_JOB_POLL_TIMEOUT_MIN", 20)
    cvpPollInterval   = env.GetUint64("CVP_JOB_POLL_INTERVAL_SEC", 30)
)
```

#### Activities

**KMS Config Creation Activities**:
- `CreateKmsConfigInDB`: Creates KMS config record in database
- `ValidateKmsConfigParameters`: Validates KMS config creation parameters
- `CreateKmsConfigInVSA`: Creates KMS config in VSA cluster
- `ValidateKmsConfigCreation`: Validates KMS config creation success
- `UpdateKmsConfigStatus`: Updates KMS config status in database
- `ConfigureKmsEncryption`: Configures KMS encryption settings

**VSA Integration Activities**:
- `ConfigureKmsForSVM`: Configures KMS for Storage Virtual Machine
- `ValidateKmsReachability`: Validates KMS connectivity
- `SetupKmsKeys`: Sets up encryption keys
- `UpdateKmsMetadata`: Updates KMS metadata

#### Execution Flow

1. **Pre-creation Phase**:
   - Validate KMS config parameters
   - Check KMS service availability
   - Prepare KMS configuration

2. **KMS Config Creation**:
   - Create KMS config in VSA cluster
   - Validate KMS config creation
   - Configure encryption settings

3. **Database Update**:
   - Update KMS config information
   - Set KMS config status
   - Configure metadata

4. **Validation and Cleanup**:
   - Validate KMS config creation
   - Update job status
   - Handle rollback if errors occur

#### Error Handling

```go
if customErr != nil {
    kmsConfigWorkflow.Status = workflows.WorkflowStatusFailed
    err = kmsConfigWorkflow.UpdateJobStatus(ctx, string(models.JobsStateERROR), customErr)
    return nil, workflows.ConvertToVSAError(err)
}
```

### 2. Update KMS Config Workflow

**File**: `core/orchestrator/workflows/kms_workflows/kms_config_update_workflow.go`

**Purpose**: Updates existing KMS configurations.

**Entry Point**: `UpdateKmsConfigWorkflow(ctx workflow.Context, params *common.UpdateKmsConfigParams, kmsConfig *datamodel.KmsConfig)`

#### Activities

**KMS Config Update Activities**:
- `UpdateKmsConfigInDB`: Updates KMS config information in database
- `UpdateKmsConfigInVSA`: Updates KMS config in VSA cluster
- `ValidateKmsConfigUpdate`: Validates update parameters
- `ApplyKmsConfigChanges`: Applies configuration changes
- `UpdateKmsConfigMetadata`: Updates KMS config metadata

#### Execution Flow

1. **Validation**: Validate update parameters
2. **VSA Update**: Update KMS config in VSA cluster
3. **Database Update**: Update KMS config information
4. **Verification**: Verify changes were applied correctly

### 3. Delete KMS Config Workflow

**File**: `core/orchestrator/workflows/kms_workflows/kms_config_delete_workflow.go`

**Purpose**: Safely deletes KMS configurations.

**Entry Point**: `DeleteKmsConfigWorkflow(ctx workflow.Context, params *common.DeleteKmsConfigParams, kmsConfig *datamodel.KmsConfig)`

#### Activities

**KMS Config Deletion Activities**:
- `DeleteKmsConfigFromVSA`: Removes KMS config from VSA cluster
- `CleanupKmsConfigResources`: Cleans up associated resources
- `UpdateKmsConfigStateInDB`: Updates KMS config state to deleted
- `ValidateKmsConfigDeletion`: Ensures safe deletion
- `CleanupKmsConfigMetadata`: Cleans up KMS config metadata

#### Execution Flow

1. **Pre-deletion Checks**: Verify KMS config can be safely deleted
2. **VSA Deletion**: Delete KMS config from VSA cluster
3. **Resource Cleanup**: Clean up associated resources
4. **Database Update**: Update KMS config state
5. **Final Cleanup**: Complete cleanup operations

### 4. Migrate KMS Config Workflow

**File**: `core/orchestrator/workflows/kms_workflows/kms_config_migrate_workflow.go`

**Purpose**: Migrates KMS configurations between different KMS providers or settings.

**Entry Point**: `MigrateKmsConfigWorkflow(ctx workflow.Context, params *common.MigrateKmsConfigParams, kmsConfig *datamodel.KmsConfig)`

#### Activities

**KMS Config Migration Activities**:
- `ValidateKmsConfigMigration`: Validates migration parameters
- `BackupKmsConfig`: Backs up current KMS config
- `CreateNewKmsConfig`: Creates new KMS config
- `MigrateKmsKeys`: Migrates encryption keys
- `ValidateKmsConfigMigration`: Validates migration success
- `CleanupOldKmsConfig`: Cleans up old KMS config

#### Execution Flow

1. **Pre-migration Phase**:
   - Validate migration parameters
   - Backup current KMS config
   - Prepare new KMS config

2. **Migration Phase**:
   - Create new KMS config
   - Migrate encryption keys
   - Validate migration

3. **Post-migration Phase**:
   - Clean up old KMS config
   - Update database records
   - Validate migration completion

## KMS Management

### KMS Configuration

**KMS Settings**:
- **Provider Type**: Google Cloud KMS, AWS KMS, Azure Key Vault
- **Key Ring**: KMS key ring identifier
- **Key Name**: Encryption key name
- **Location**: KMS service location
- **Project ID**: Cloud project identifier

### Encryption Keys

**Key Management**:
- **Key Creation**: Automatic key creation
- **Key Rotation**: Automatic key rotation
- **Key Access**: Role-based key access
- **Key Backup**: Secure key backup

### KMS Integration

**VSA Integration**:
- **SVM Configuration**: KMS configuration for Storage Virtual Machine
- **Volume Encryption**: Volume-level encryption
- **Snapshot Encryption**: Snapshot-level encryption
- **Backup Encryption**: Backup-level encryption

## Error Handling

### Error Types

**Non-Retryable Errors**:
- `PanicError`: System panic errors
- `ValidationError`: Parameter validation errors
- `KmsConfigNotFoundError`: Missing KMS config errors

**Retryable Errors**:
- `NetworkTimeoutError`: Network operation timeouts
- `KmsOperationError`: KMS operation failures
- `VSAOperationError`: VSA operation failures

### Rollback Management

KMS workflows implement rollback for failed operations:

```go
rollbackManager := common.NewRollbackManager()
defer func() {
    if err != nil {
        rollbackManager.ExecuteRollback(ctx, err)
    }
}()
```

## Configuration

### Retry Policies

All KMS workflows use configurable retry policies:

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

### KMS Metrics

**Available Metrics**:
- KMS config creation duration
- KMS operation success rate
- Encryption key usage metrics
- KMS service health metrics

### Health Checks

**KMS Health Monitoring**:
- KMS service connectivity status
- Encryption key availability
- KMS config validation status
- Performance metrics

## Performance Considerations

### KMS Performance

**Optimization Features**:
- Parallel KMS operations
- Key caching and reuse
- Batch key operations
- Connection pooling

### Resource Management

**Resource Limits**:
- Concurrent KMS operations
- Key storage limits
- Network bandwidth limits
- API rate limits

## Security Considerations

### KMS Security

**Access Control**:
- Role-based access control
- KMS permissions
- Audit logging for all operations
- Secure key storage

### Data Protection

**Data Security**:
- Encrypted key storage
- Secure key transmission
- Key rotation policies
- Secure key backup

## Testing

### Test Coverage

Each KMS workflow has comprehensive test coverage:

- **Unit Tests**: Individual workflow function testing
- **Integration Tests**: End-to-end workflow testing
- **Mock Activities**: Mock implementations for testing
- **Error Scenarios**: Testing failure and retry scenarios
- **KMS Integration Tests**: Testing with actual KMS services

### Test Files

- `kms_config_create_workflow_test.go`: Create workflow tests
- `kms_config_update_workflow_test.go`: Update workflow tests
- `kms_config_delete_workflow_test.go`: Delete workflow tests
- `kms_config_migrate_workflow_test.go`: Migration workflow tests

## Related Documentation

- [Pool Workflows](../core/pool-workflows.md)
- [Volume Workflows](../core/volume-workflows.md)
- [Temporal Debugging Guide](../../guides/temporal-debugging.md)