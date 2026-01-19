# Workflow Supervisor: UPDATE and DELETE Job Support

## Overview

Extended the workflow supervisor task to handle stale UPDATE and DELETE operations for pools, volumes, backups, snapshots, volume replications, and KMS configurations, similar to existing CREATE operation handling. When UPDATE or DELETE jobs remain in `NEW` state beyond the grace period and their workflows time out, the supervisor now reverts the resource state from `UPDATING` or `DELETING` back to the previous state, ensuring resources are not permanently stuck in transitional states and preserving important state information such as ERROR conditions.

## Components

- **Supervisor Task Runner (`core/tasks/workflow_supervisor_task.go`)**
  - Extended handler registration to include UPDATE and DELETE handlers for all supported resource types.
  - Handlers are registered alongside existing CREATE handlers and follow the same event-driven pattern.

- **Supervisor Handlers (`core/tasks/supervisor-handler/`)**
  - New handlers implement state reversion logic for UPDATE and DELETE operations:
    - `pool_update_supervisor_handler.go`: Reverts pool state from `UPDATING` to previous state.
    - `pool_delete_supervisor_handler.go`: Reverts pool state from `DELETING` to previous state.
    - `volume_update_supervisor_handler.go`: Reverts volume state from `UPDATING` to previous state.
    - `volume_delete_supervisor_handler.go`: Reverts volume state from `DELETING` to previous state.
    - `backup_update_supervisor_handler.go`: Reverts backup state from `UPDATING` to previous state.
    - `backup_delete_supervisor_handler.go`: Reverts backup state from `DELETING` to previous state.
    - `snapshot_delete_supervisor_handler.go`: Reverts snapshot state from `DELETING` to previous state.
    - `replication_update_supervisor_handler.go`: Reverts replication state from `UPDATING` to previous state.
    - `replication_delete_supervisor_handler.go`: Reverts replication state from `DELETING` to previous state.
    - `kms_delete_supervisor_handler.go`: Reverts KMS config state from `DELETING` to previous state.
  - Handlers run cleanup routines under a `30s` timeout, consistent with existing handlers.

## Key Design Choices

- **Previous State Storage**: Previous state is stored in `JobAttributes` when UPDATE/DELETE jobs are created, allowing handlers to restore the exact state the resource was in before the operation.
- **State Reversion vs Cleanup**: Unlike CREATE handlers that delete partially created resources, UPDATE/DELETE handlers only revert state since no external changes occur when workflows don't start (for NEW state jobs).
- **State Preservation**: Resources in ERROR, DISABLED, or other non-standard states are correctly preserved when operations timeout, preventing loss of important state information.
- **Backward Compatibility**: Handlers gracefully handle jobs created before this change by falling back to default states (AVAILABLE for pools/backups/replications, READY for volumes/snapshots, CREATED for KMS configs) when previous state is not stored.
- **Additional Metadata Storage**: For resources requiring additional lookup parameters (backups, snapshots), metadata is stored in `JobAttributes.PayloadAttributes` to enable proper resource retrieval during cleanup.

## Handler Responsibilities

### Pool Update Cleanup

- Fetch the pool (`GetPoolByUUID`).
- Verify pool is in `UPDATING` state (skip if already in different state).
- Read previous state from `JobAttributes.PreviousState` and `JobAttributes.PreviousStateDetails`.
- Revert pool state using `UpdatePoolState` to restore previous state.
- Fallback to `AVAILABLE` state if previous state not stored (backward compatibility).

### Pool Delete Cleanup

- Fetch the pool (`GetPoolByUUID`).
- Verify pool is in `DELETING` state (skip if already in different state).
- Read previous state from `JobAttributes.PreviousState` and `JobAttributes.PreviousStateDetails`.
- Revert pool state using `UpdatePoolState` to restore previous state.
- Fallback to `AVAILABLE` state if previous state not stored (backward compatibility).

### Volume Update Cleanup

- Fetch the volume (`GetVolume`).
- Verify volume is in `UPDATING` state (skip if already in different state).
- Read previous state from `JobAttributes.PreviousState` and `JobAttributes.PreviousStateDetails`.
- Revert volume state using `UpdateVolumeFields` to restore previous state.
- Fallback to `READY` state if previous state not stored (backward compatibility).

### Volume Delete Cleanup

- Fetch the volume (`GetVolume`).
- Verify volume is in `DELETING` state (skip if already in different state).
- Read previous state from `JobAttributes.PreviousState` and `JobAttributes.PreviousStateDetails`.
- Revert volume state using `UpdateVolumeFields` to restore previous state.
- Fallback to `READY` state if previous state not stored (backward compatibility).

### Backup Update Cleanup

- Extract backup vault UUID and account name from `JobAttributes.PayloadAttributes`.
- Fetch the backup (`GetBackup`).
- Verify backup is in `UPDATING` state (skip if already in different state).
- Read previous state from `JobAttributes.PreviousState` and `JobAttributes.PreviousStateDetails`.
- Revert backup state using `UpdateBackupState` to restore previous state.
- Fallback to `AVAILABLE` state if previous state not stored (backward compatibility).

### Backup Delete Cleanup

- Extract backup vault UUID and account name from `JobAttributes.PayloadAttributes`.
- Fetch the backup (`GetBackup`).
- Verify backup is in `DELETING` state (skip if already in different state).
- Read previous state from `JobAttributes.PreviousState` and `JobAttributes.PreviousStateDetails`.
- Revert backup state using `UpdateBackupState` to restore previous state.
- Fallback to `AVAILABLE` state if previous state not stored (backward compatibility).

### Snapshot Delete Cleanup

- Extract account ID and volume ID from `JobAttributes.PayloadAttributes`.
- Fetch the snapshot (`GetSnapshotByUUID`).
- Verify snapshot is in `DELETING` state (skip if already in different state).
- Read previous state from `JobAttributes.PreviousState` and `JobAttributes.PreviousStateDetails`.
- Revert snapshot state using `UpdateSnapshot` to restore previous state.
- Fallback to `READY` state if previous state not stored (backward compatibility).

### Replication Update Cleanup

- Fetch the replication (`GetVolumeReplication`).
- Verify replication is in `UPDATING` state (skip if already in different state).
- Read previous state from `JobAttributes.PreviousState` and `JobAttributes.PreviousStateDetails`.
- Revert replication state using `UpdateVolumeReplicationStates` to restore previous state.
- Fallback to `AVAILABLE` state if previous state not stored (backward compatibility).

### Replication Delete Cleanup

- Fetch the replication (`GetVolumeReplication`).
- Verify replication is in `DELETING` state (skip if already in different state).
- Read previous state from `JobAttributes.PreviousState` and `JobAttributes.PreviousStateDetails`.
- Revert replication state using `UpdateVolumeReplicationStates` to restore previous state.
- Fallback to `AVAILABLE` state if previous state not stored (backward compatibility).

### KMS Config Delete Cleanup

- Fetch the KMS config (`GetKmsConfig`).
- Verify KMS config is in `DELETING` state (skip if already in different state).
- Read previous state from `JobAttributes.PreviousState` and `JobAttributes.PreviousStateDetails`.
- Revert KMS config state using `UpdateKmsConfigState` to restore previous state.
- Fallback to `CREATED` state if previous state not stored (backward compatibility).

All handlers tolerate "not found" conditions and skip reversion if the resource is not in the expected transitional state, supporting idempotent retries.

## Differences from CREATE Handlers

| Operation | Handler Action | Reason |
|-----------|----------------|--------|
| CREATE | **Delete** partially created resource | Resource was partially created, needs cleanup |
| UPDATE | **Revert** state only | No external changes (workflow didn't start) |
| DELETE | **Revert** state only | No external changes (workflow didn't start) |

For NEW state jobs, UPDATE and DELETE operations only change the VCP database state to `UPDATING`/`DELETING` before the workflow starts. Since the workflow never executes, no external systems (ONTAP, GCP, VLM) are modified, so only state reversion is needed.

## Data Model Changes

- **JobAttributes** (`core/datamodel/models.go`): Added `PreviousState` and `PreviousStateDetails` fields to store resource state before UPDATE/DELETE operations.
- **Job Creation** (`core/orchestrator/`): Previous state is captured and stored in `JobAttributes` when creating UPDATE/DELETE jobs, before the resource state is changed to `UPDATING`/`DELETING`. For resources requiring additional lookup parameters (backups, snapshots), metadata is stored in `JobAttributes.PayloadAttributes`.

## Supported Job Types

### Pool Operations
- `UPDATE_POOL`
- `UPDATE_LARGE_POOL`
- `DELETE_POOL`
- `DELETE_LARGE_POOL`

### Volume Operations
- `UPDATE_VOLUME`
- `UPDATE_VOLUME_IN_REPLICATION`
- `DELETE_VOLUME`
- `DELETE_LARGE_VOLUME`
- `FLEXCACHE_DELETE_VOLUME`

### Backup Operations
- `UPDATE_BACKUP`
- `DELETE_BACKUP`

### Snapshot Operations
- `DELETE_SNAPSHOT`

### Replication Operations
- `UPDATE_VOLUME_REPLICATION_INTERNAL`
- `UPDATE_VOLUME_REPLICATION`
- `UPDATE_VOLUME_REPLICATION_ATTRIBUTES`
- `DELETE_VOLUME_REPLICATION_INTERNAL`
- `DELETE_VOLUME_REPLICATION`

### KMS Config Operations
- `DELETE_KMS_CONFIG`

## Failure Handling

- If the resource is not found, handlers log a warning and skip cleanup (resource may have been deleted by another process).
- If the resource is not in the expected transitional state (`UPDATING` or `DELETING`), handlers skip reversion to avoid incorrect state changes.
- Handler errors are logged, and the job state remains unchanged (still `NEW`), allowing future runs to retry cleanup.
- Job state transitions to `ERROR` only after handler cleanup completes without error.

## Configuration

- Uses existing supervisor configuration:
  - `WORKFLOW_SUPERVISOR_NOT_FOUND_GRACE_PERIOD`: Grace period for missing workflows (default 5 minutes).
  - `resourceCleanupTimeout`: Hard-coded 30s timeout for handler execution.
  - `temporalDescribeTimeout`: Hard-coded 15s timeout for describe calls.

## Testing

Unit tests should cover:

- Each handler's behavior for timeout events, missing resources, and successful state reversion.
- Handler behavior when resource is not in expected transitional state.
- Handler behavior when previous state is not stored (backward compatibility).
- Handler behavior when resource was in ERROR state before operation.
- Handler behavior when additional metadata (backup vault UUID, account ID, etc.) is missing or incorrect.

Test files:

- `core/tasks/supervisor-handler/pool_update_supervisor_handler_test.go` (to be created)
- `core/tasks/supervisor-handler/pool_delete_supervisor_handler_test.go` (to be created)
- `core/tasks/supervisor-handler/volume_update_supervisor_handler_test.go` (to be created)
- `core/tasks/supervisor-handler/volume_delete_supervisor_handler_test.go` (to be created)
- `core/tasks/supervisor-handler/backup_update_supervisor_handler_test.go` (to be created)
- `core/tasks/supervisor-handler/backup_delete_supervisor_handler_test.go` (to be created)
- `core/tasks/supervisor-handler/snapshot_delete_supervisor_handler_test.go` (to be created)
- `core/tasks/supervisor-handler/replication_update_supervisor_handler_test.go` (to be created)
- `core/tasks/supervisor-handler/replication_delete_supervisor_handler_test.go` (to be created)
- `core/tasks/supervisor-handler/kms_delete_supervisor_handler_test.go` (to be created)
