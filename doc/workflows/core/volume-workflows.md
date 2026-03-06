# Volume Workflows

This document describes the volume-related workflows in the VSA Control Plane system, including volume creation, update, deletion, refresh, and revert operations.

## Overview

Volume workflows manage the complete lifecycle of storage volumes in the VSA Control Plane, from creation to deletion. They handle both block (SAN) and file (NAS) protocols, with support for backup restoration and snapshot operations.

## Workflow Types

### 1. Volume Create Workflow

**File**: `core/orchestrator/workflows/volume_create_workflow.go`

**Purpose**: Creates new storage volumes with support for both block and file protocols.

**Entry Point**: `CreateVolumeWorkflow(ctx workflow.Context, params *common.CreateVolumeParams, volume *datamodel.Volume, backupVault *datamodel.BackupVault, backup *datamodel.Backup)`

#### Workflow Structure

```go
type volumeCreateWorkflow struct {
    BaseWorkflow
}
```

#### Phases

The volume creation process is divided into two phases:

- **Pre-Provisioning Phase**: Initial setup and validation
- **Post-Provisioning Phase**: Final configuration and cleanup

#### Child Workflows

Based on volume protocol, different child workflows are selected:

**Block Volumes (SAN Protocols)**:
- `PreBlockVolumeWorkflow`: Pre-provisioning for block volumes
- `PostBlockVolumeWorkflow`: Post-provisioning for block volumes

**File Volumes (NAS Protocols)**:
- `PreFileVolumeWorkflow`: Pre-provisioning for file volumes  
- `PostFileVolumeWorkflow`: Post-provisioning for file volumes

#### Activities

**VolumeCreateActivity** (`activities.VolumeCreateActivity`):
- `CreateVolumeInVSA`: Creates volume in VSA cluster
- `UpdateVolumeStateInDB`: Updates volume state in database
- `ConfigureVolumeProtocols`: Configures volume protocols
- `ValidateVolumeCreation`: Validates volume creation parameters
- `SetupVolumeMounts`: Sets up volume mount points
- `ConfigureVolumePermissions`: Configures volume access permissions
- `UpdateVolumeAutoTieringPolicyInONTAP`: Sets auto-tiering policy on a clone after creation (see below)

#### Execution Flow

1. **Setup Phase**:
   - Initialize workflow context and logger
   - Set up query handlers for status monitoring
   - Configure retry policies

2. **Pre-Provisioning Phase**:
   - Validate volume parameters
   - Check resource availability
   - Prepare volume configuration

3. **Volume Creation**:
   - Execute volume creation activity
   - Handle protocol-specific configuration
   - Update database with volume information

4. **Post-Provisioning Phase**:
   - Configure volume mounts
   - Set up access permissions
   - Perform final validation

5. **Cleanup**:
   - Update job status
   - Handle rollback if errors occur
   - Log completion status

#### Auto-Tiering in Volume Creation

When a volume is created on an auto-tiering-enabled pool, the tiering policy is applied during the `CreateVolumeInONTAP` activity via `CreateAutoTieringParams`. Key behaviours:

- **Policy translation for block volumes**: The `auto` policy maps to `snapshot-only` on ONTAP for SAN/block volumes. The `all` policy is not allowed for block volumes.
- **Pool pause awareness**: If the pool's `TieringStatus` is `PAUSED` or `PARTIALLY_PAUSED` and the requested policy is `all`, the volume is created with tiering policy `none` and `cloudWriteMode=false`. The correct policy is restored when the pool resumes auto-tiering (via the background sync workflow).
- **Aggregate selection**: When `allowAutoTiering` is true on the pool, aggregate distribution for large volumes uses CV-limit-based calculation instead of space-limit-based calculation.

**Clone creation** requires special handling because ONTAP does not accept a tiering policy during thin clone creation. The workflow:
1. Creates the clone without a tiering policy (ONTAP inherits the parent's policy).
2. Immediately calls `UpdateVolumeAutoTieringPolicyInONTAP` to set the desired policy on the clone.
3. If auto-tiering is disabled for the clone, explicitly sets policy to `none` to prevent inheriting the parent's policy.

See [ADR-0013: Thin Clone Behaviour](../../architecture/decisions/0013-auto-tiering-thin-clone-behaviour-decision.md) for the full decision record and validated scenarios.

#### Error Handling

- **Rollback Management**: Automatic rollback on failure
- **Retry Policies**: Configurable retry for transient failures
- **Error Conversion**: Consistent error handling with `*vsaerrors.CustomError`

#### Configuration

**Retry Policy**:
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

### 2. Volume Update Workflow

**File**: `core/orchestrator/workflows/volume_update_workflow.go`

**Purpose**: Updates existing volume properties and configurations.

**Entry Point**: `UpdateVolumeWorkflow(ctx workflow.Context, params *common.UpdateVolumeParams, volume *datamodel.Volume)`

#### Activities

**VolumeUpdateActivity** (`activities.VolumeUpdateActivity`):
- `UpdateVolumeInVSA`: Updates volume in VSA cluster
- `UpdateVolumeInDB`: Updates volume information in database
- `ValidateVolumeUpdate`: Validates update parameters
- `ApplyVolumeChanges`: Applies configuration changes

**FlexCacheVolumeUpdateActivity** (`flexcache_activities.UpdateFlexCacheVolumeInONTAP`):
- `UpdateFlexCacheVolume`: Updates FlexCache volume in VSA cluster

#### Execution Flow

1. **Validation**: Validate update parameters
2. **Update VSA**: Apply changes to VSA cluster
3. **Update Database**: Update volume information in database
4. **Verification**: Verify changes were applied correctly

#### Auto-Tiering in Volume Update

The update workflow detects auto-tiering changes by comparing the incoming `AutoTieringPolicy` against the existing volume state. An ONTAP update is triggered when any of these differ:

- `autoTieringEnabled` (enable/disable toggle)
- `coolingThresholdDays` (cooling period change)
- `tieringPolicy` (policy change, e.g. `auto` → `all`)

The `updateAutoTieringParams` function in `volume_update_activities.go` handles policy translation:

- **Pool pause awareness**: Same as volume creation — if the pool is paused/partially-paused and the policy is `all`, the volume is set to `none` on ONTAP. The background sync workflow will correct this when the pool resumes.
- **DB update**: The `auto_tiering_enabled`, `auto_tiering_policy` (JSON), and related fields are persisted in the volume record. Fields updated include `tieringPolicy`, `coolingThresholdDays`, `retrievalPolicy`, `hotTierBypassModeEnabled`, and `cloudWriteModeEnabled`.
- **Block volumes**: The `all` policy is not allowed; validation rejects it before reaching the workflow.

### 3. Volume Delete Workflow

**File**: `core/orchestrator/workflows/volume_delete_workflow.go`

**Purpose**: Safely deletes volumes with proper cleanup.

**Entry Point**: `DeleteVolumeWorkflow(ctx workflow.Context, params *common.DeleteVolumeParams, volume *datamodel.Volume)`

#### Activities

**VolumeDeleteActivity** (`activities.VolumeDeleteActivity`):
- `DeleteVolumeFromVSA`: Removes volume from VSA cluster
- `CleanupVolumeResources`: Cleans up associated resources
- `UpdateVolumeStateInDB`: Updates volume state to deleted
- `ValidateVolumeDeletion`: Ensures safe deletion

#### Execution Flow

1. **Pre-deletion Checks**: Verify volume can be safely deleted
2. **Resource Cleanup**: Remove associated resources
3. **VSA Deletion**: Delete volume from VSA cluster
4. **Database Update**: Update volume state
5. **Final Cleanup**: Complete cleanup operations

### 4. Volume Refresh Workflow

**File**: `core/orchestrator/workflows/volume_refresh_workflow.go`

**Purpose**: Refreshes volume information and synchronizes state.

**Entry Point**: `RefreshVolumeWorkflow(ctx workflow.Context, params *common.RefreshVolumeParams, volume *datamodel.Volume)`

#### Activities

**VolumeRefreshActivity** (`activities.VolumeRefreshActivity`):
- `SyncVolumeFromVSA`: Synchronizes volume data from VSA
- `UpdateVolumeInDB`: Updates database with current state
- `ValidateVolumeState`: Validates volume state consistency

### 5. Volume Revert Workflow

**File**: `core/orchestrator/workflows/volume_revert_workflow.go`

**Purpose**: Reverts volume to a previous state or snapshot.

**Entry Point**: `RevertVolumeWorkflow(ctx workflow.Context, params *common.RevertVolumeParams, volume *datamodel.Volume)`

#### Activities

**VolumeRevertActivity** (`activities.VolumeRevertActivity`):
- `RevertVolumeToSnapshot`: Reverts volume to specified snapshot
- `ValidateRevertOperation`: Validates revert parameters
- `UpdateVolumeState`: Updates volume state after revert

### 6. Volume Update in Replication Workflow

**File**: `core/orchestrator/workflows/volume_update_in_replication_workflow.go`

**Purpose**: Updates volume properties during replication operations.

**Entry Point**: `UpdateVolumeInReplicationWorkflow(ctx workflow.Context, params *common.UpdateVolumeInReplicationParams, volume *datamodel.Volume)`

## Common Patterns

### Error Handling

All volume workflows implement consistent error handling:

```go
defer func() {
    if err != nil {
        err2 := workflow.ExecuteActivity(ctx, volumeActivity.UpdateVolumeStateInDB, 
            dbVolume.UUID, models.LifeCycleStateError, models.LifeCycleStateCreationErrorDetails).Get(ctx, nil)
        if err2 != nil {
            log.Errorf("Failed to update volume state in DB to error: %v", err2)
        }
    }
}()
```

### Rollback Management

Volume workflows use a rollback manager for cleanup:

```go
rollbackManager := common.NewRollbackManager()
defer func() {
    if err != nil {
        rollbackManager.ExecuteRollback(ctx, err)
    }
}()
```

### Logging

All workflows use structured logging with correlation IDs:

```go
log := util.GetLogger(ctx)
log.Info("Starting volume operation", "volumeID", volume.UUID, "operation", "create")
```

## Configuration

### Environment Variables

- `START_TO_CLOSE_WORKFLOW_TIMEOUT`: Workflow timeout (default: 55m)
- `RETRY_INTERVAL`: Retry interval (default: 5s)
- `RETRY_MAX_ATTEMPTS`: Maximum retry attempts (default: 3)
- `RETRY_MAX_INTERVAL`: Maximum retry interval (default: 5m)
- `RETRY_BACKOFF_COEFFICIENT`: Backoff coefficient (default: 2.0)

### Retry Policies

All volume workflows use configurable retry policies:

```go
retryPolicy, err := PopulateRetryPolicyParams()
if err != nil {
    return nil, ConvertToVSAError(err)
}
```

## Testing

Each workflow has comprehensive test coverage:

- **Unit Tests**: Individual workflow function testing
- **Integration Tests**: End-to-end workflow testing
- **Mock Activities**: Mock implementations for testing
- **Error Scenarios**: Testing failure and retry scenarios

## Monitoring

Volume workflows provide extensive monitoring:

- **Metrics**: Volume operation metrics
- **Tracing**: Distributed tracing across workflow execution
- **Logging**: Structured logging for debugging
- **Health Checks**: Workflow health monitoring

## Related Documentation

- [Pool Workflows](./pool-workflows.md)
- [Backup Workflows](./backup-workflows.md)
- [Snapshot Workflows](./snapshot-workflows.md)
- [Auto-Tiering Background Workflows](../background/auto-tiering-workflows.md)
- [Auto-Tiering Design](../../architecture/designs/0005-vsa-auto-tiering-design.md)
- [ADR-0013: Thin Clone Behaviour](../../architecture/decisions/0013-auto-tiering-thin-clone-behaviour-decision.md)
- [Temporal Debugging Guide](../../guides/temporal-debugging.md)