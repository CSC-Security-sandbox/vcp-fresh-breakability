# Workflow Supervisor Task

## Overview

The workflow supervisor task is responsible for detecting long-running or stalled Temporal workflows and performing compensating cleanup in VCP when workflows do not complete successfully. It runs as part of the background job system and evaluates jobs that are still in `NEW` state after orchestration started. When a workflow times out or is not found after a grace period, the supervisor triggers resource-specific handlers to clean up partially provisioned artifacts in the control plane, ensuring consistency and preventing leaked resources.

## Components

- **Supervisor Task Runner (`core/tasks/workflow_supervisor_task.go`)**
  - Loads candidate jobs in `NEW` state from the database for supported job types.
  - Describes the Temporal workflow execution for each job to determine the current status.
  - Applies a grace period when the workflow execution is not found (to handle eventual consistency right after job creation).
  - Emits cleanup events to registered handlers when workflows time out or remain missing beyond the grace window.
  - Acquires a transactional `SELECT ... FOR UPDATE` lock on each job before terminating the workflow; if the row is no longer in `NEW` state the supervisor skips termination and cleanup for that job to avoid double-processing.

- **Supervisor Handlers (`core/tasks/supervisor-handler/`)**
  - Implement resource-specific cleanup logic invoked when the supervisor detects a timeout.
  - Current handlers:
    - `kms_supervisor_handler.go`: Cleans up CMEK configuration resources in VCP and SDE, including the auxiliary `SDE_KMS_CREATE` job newly created when provisioning CMEK through the proxy.
    - `pool_supervisor_handler.go`: Cleans up pool metadata from VCP.
    - `volume_supervisor_handler.go`: Cleans up volume metadata and related child resources from VCP.
    - `backup_supervisor_handler.go`: Handles backup related jobs (unchanged).
    - `snapshot_supervisor_handler.go`: Handles snapshot jobs (unchanged).
    - `replication_supervisor_handler.go`: Handles replication jobs (unchanged).
  - Handlers advertise supported job types and run cleanup routines under a `30s` timeout to avoid prolonged retries.

## Key Design Choices

- **Grace Period for Missing Workflows**: `WORKFLOW_SUPERVISOR_NOT_FOUND_GRACE_PERIOD` defaults to 5 minutes, preventing cleanup while the workflow might still be starting.
- **Event-driven Cleanup**: Handlers receive an `EventTimeout` signal after the workflow state check completes. The supervisor marks jobs as `ERROR` only after handler cleanup succeeds, allowing future supervisor runs to retry cleanup if needed.
- **Modular Handlers**: Handlers are independently testable and register the job types they support, simplifying addition of new cleanup routines.
- **Temporal Describe API**: The supervisor relies on `DescribeWorkflowExecution` to differentiate between `TIMED_OUT`, running, or missing workflows.

## Handler Responsibilities

### CMEK & SDE Cleanup

- Fetch the KMS config metadata (`GetKmsConfig`).
- Attempt to delete corresponding SDE configuration if attributes are present.
- Remove the config from VCP (`DeleteKmsConfig`) with timeout detail for auditing.
- Mark the paired `SDE_KMS_CREATE` job as `ERROR` so that background processors and dashboards reflect that the SDE operation was cleaned up.

### Pool Cleanup

- Fetch the pool (`GetPoolByUUID`).
- Remove the pool from VCP (`DeletePool`).

### Volume Cleanup

- Delete volume metadata and child resources via `DeleteVolumeAndChildResources`.

### Additional Handlers

- Backup, snapshot, and replication handlers follow the same pattern; they were unaffected by the latest CMEK/SDE changes and continue to implement idempotent cleanup for their respective job types.

All handlers tolerate "not found" conditions to support idempotent retries.

## Failure Handling

- If Temporal describe calls fail with an error other than `NotFound`, the supervisor logs and skips cleanup.
- Handler errors are logged, and the job state remains unchanged (still `NEW`), allowing future runs to retry cleanup.
- Job state transitions to `ERROR` only after handler cleanup completes without error (and only if the supervisor was able to lock the job row).

## Configuration

- `WORKFLOW_SUPERVISOR_NOT_FOUND_GRACE_PERIOD`: Grace period for missing workflows.
- `resourceCleanupTimeout`: Hard-coded 30s timeout for handler execution.
- `temporalDescribeTimeout`: Hard-coded 15s timeout for describe calls.

## Testing

Unit tests cover:

- Each handler’s behavior for timeout events, missing resources, and successful cleanup.
- Supervisor runner scenarios for grace-period logic, timed-out workflows, non-timeout statuses, and handler error propagation.
- Lock acquisition behaviour: tests now confirm that jobs found in a non-`NEW` state are skipped without invoking cleanup or Temporal termination, preventing double processing.

See:

- `core/tasks/workflow_supervisor_task_test.go`
- `core/tasks/supervisor-handler/*_test.go`
