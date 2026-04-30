# Split Workflow Error Handling and Orphan Job Scheduling

## Overview

When a customer triggers a thin-clone volume split, the VCP control plane calls ONTAP to initiate the data-movement operation and then immediately starts a Temporal workflow (`VolumePollSplitWorkflow`) to poll ONTAP until the split completes. Prior to this design, any failure that occurred after ONTAP accepted the split — including a transient Temporal unavailability — caused VCP to return an error to the caller and run the standard deferred cleanup (mark job deleted, revert clone state). This was factually incorrect: ONTAP had already begun moving data, so the split was still in progress. The customer saw a misleading error while the backend was working normally, and the job state did not reflect reality.

This document describes the two-part solution:

1. **Orphan Job Scheduling** — how VCP recovers when it successfully starts a split in ONTAP but cannot immediately attach a Temporal workflow.
2. **Background Goroutine for DB Update** — how VCP handles the case where the database is temporarily unavailable when trying to persist the `WAIT_FOR_TEMPORAL` job state.

---

## Problem Statement

### The Premature-Error Problem

The split API call follows this sequence:

1. Create a `SPLIT_CLONE_VOLUME` job in state `NEW`.
2. Update the clone state to `SPLITTING` and reserve `clones_shared_bytes`.
3. Call `InitiateSplitVolume` on ONTAP — the split begins asynchronously in the storage backend.
4. Start `VolumePollSplitWorkflow` via Temporal to poll ONTAP and complete the lifecycle.

Step 3 is the point of no return. Once ONTAP accepts the split, data movement has started and there is no VCP-level revert. If step 4 fails (e.g. Temporal is momentarily unavailable), returning an error to the caller would:

- Trigger the deferred job-delete, making the job invisible to monitoring.
- Optionally revert the clone state from `SPLITTING` back to `CLONED`, misrepresenting the actual ONTAP state.
- Force the customer to retry when no retry is needed; ONTAP is already working.

The real situation is: the split is running, VCP just lost the handle to observe it.

### The DB-Update Problem

Even after deciding to place the job in `WAIT_FOR_TEMPORAL` and return HTTP 200, there is a secondary failure path: the `UpdateJob` call to persist the new state may itself fail because the database is temporarily down or overloaded. If VCP waits for the DB update synchronously before sending the response, the customer's request hangs for the duration of all DB retries. If the update never succeeds and VCP returns an error anyway, the same premature-error problem recurs.

---

## Solution: Part 1 — Orphan Job Scheduling

### Job State: `WAIT_FOR_TEMPORAL`

A new job state, `WAIT_FOR_TEMPORAL`, acts as a durable signal that the underlying ONTAP operation was accepted but the Temporal workflow could not be started. Jobs in this state are picked up by the **orphan job scheduler**, which runs as a periodic background workflow on the `ORPHANED_JOB_SCHEDULER` cron (every 2 minutes).

When `VolumePollSplitWorkflow` fails to start:

1. The split job is moved to state `WAIT_FOR_TEMPORAL` (the goroutine mechanism described in Part 2 handles this when the DB itself is slow).
2. The clone state remains `SPLITTING` — consistent with what ONTAP knows.
3. The API returns HTTP 200. The split LRO continues normally from the customer's perspective.

### Orphan Job Activity

`OrphanJobsActivity` (in `backgroundactivities/orphan_job_activities.go`) runs inside the orphan-job scheduler Temporal workflow. Each execution:

1. Queries all jobs in state `WAIT_FOR_TEMPORAL`.
2. For each job, looks up the registered `WorkflowMapping` by job type.
3. Calls `PrepareWorkflowArgs` on the type-specific `OrphanJobWorkflowManager` to reconstruct the arguments needed to start the workflow from durable DB state.
4. Starts the Temporal workflow using the **same** `WorkflowID` as the original attempt, with reuse policy `ALLOW_DUPLICATE_FAILED_ONLY`. This is idempotent: if the workflow already started on a previous retry, Temporal rejects the duplicate start and the job will naturally transition out of `WAIT_FOR_TEMPORAL` when the workflow completes.
5. Increments the `CurrentRetryCount` on each attempt. If `CurrentRetryCount` reaches `WaitForTemporalJobMaxRetryCount` (5), the job is marked `ERROR` and `FailedWorkflowJob` is called to record the terminal failure.

### `OrphanJobWorkflowManager` Interface

Every job type that can enter `WAIT_FOR_TEMPORAL` implements this interface:

```go
type OrphanJobWorkflowManager interface {
    PrepareWorkflowArgs(ctx context.Context, se database.Storage, job *datamodel.Job) ([]interface{}, error)
    FailedWorkflowJob(ctx context.Context, se database.Storage, job *datamodel.Job, reason string) error
}
```

- **`PrepareWorkflowArgs`** — Reads the durable state written at split initiation time (ONTAP job UUID stored in `VolumeAttributes.SplitJobUUID`, pool and node info from DB associations) and returns the argument list required by `VolumePollSplitWorkflow`. No in-memory context is required; everything is reconstructed from the database.
- **`FailedWorkflowJob`** — Called when all retries are exhausted. For the split job type, this updates `CloneParentInfo.State` to `ERROR_IN_SPLITTING` so that the volume reflects the unrecoverable failure.

### Persisting the ONTAP Job UUID

Before attempting to start `VolumePollSplitWorkflow`, the split handler writes the ONTAP job UUID returned by `InitiateSplitVolume` into `VolumeAttributes.SplitJobUUID`. This step is critical: if the workflow start fails and the job enters `WAIT_FOR_TEMPORAL`, the orphan processor can reconstruct the exact same workflow arguments from DB state alone. Without this persisted UUID, re-starting the workflow would require a fresh ONTAP call, risking duplicate split requests.

### Job Type Registration

The split job type is registered in `jobTypeToWorkflowMapping`:

```go
models.JobTypeSplitVolume: {
    workflowFunc:   "VolumePollSplitWorkflow",
    getArgsFunc:    &SplitVolumeArgs{},
    taskQueue:      workflowengine.BackgroundTaskQueue,
    timeoutSeconds: int(workflowengine.GetSplitVolumeWorkflowTimeout().Seconds()),
},
```

This ensures the orphan processor uses the background task queue (same as the original dispatch) and the same workflow timeout, preserving parity with the initial attempt.

### Retry and Terminal-Failure Flow

```
OrphanJobsActivity runs every 2 minutes
│
├── Job in WAIT_FOR_TEMPORAL found
│   ├── Increment CurrentRetryCount
│   ├── CurrentRetryCount < 5?
│   │   ├── YES → PrepareWorkflowArgs → ExecuteWorkflow
│   │   │         ├── Success → job transitions out (workflow takes over)
│   │   │         └── Failure → leave in WAIT_FOR_TEMPORAL, retry next sweep
│   │   └── NO  → Mark job ERROR
│   │             FailedWorkflowJob → set CloneParentInfo.State = ERROR_IN_SPLITTING
│   └── (continue to next job)
```

### Feature Flag

Orphan-job processing can be disabled via `ORPHAN_JOB_PROCESSING_ENABLED` (default `true`). The split path into `WAIT_FOR_TEMPORAL` is additionally gated on `SPLIT_WAIT_FOR_TEMPORAL_ENABLED` (default `false`, enabled per environment). This two-flag design allows the orphan scheduler to run for other job types (e.g. KMS) without enabling the split path, and vice versa.

---

## Solution: Part 2 — Background Goroutine for DB Update

### Why a Goroutine?

Once the orphan-job mechanism is in place, the split handler must call `UpdateJob(WAIT_FOR_TEMPORAL)` before returning HTTP 200. However, if the database is unavailable at that moment, a synchronous retry loop would block the request thread for up to tens of seconds, degrading API latency and potentially timing out the gRPC/HTTP connection on the caller's side.

The solution is to fire the DB update in a **separate goroutine**, allowing the main request thread to return HTTP 200 immediately. The goroutine retries the update independently, and the caller is not penalised for transient DB issues.

### Goroutine Lifecycle

```
Main goroutine                          Background goroutine
─────────────────────────────────       ──────────────────────────────────────
ExecuteWorkflow → fails (Temporal down)
│
├── SPLIT_WAIT_FOR_TEMPORAL_ENABLED?
│   YES
│   ├── Launch goroutine (captures jobUUID, trackingID, workflowStartErr)
│   ├── err = nil  (suppress deferred job-delete & clone-state revert)
│   └── return HTTP 200 ──────────────► goroutine starts
│                                        │
│                                        ├── attempt 1: UpdateJob(WAIT_FOR_TEMPORAL)
│                                        │   ├── success → return
│                                        │   └── failure → sleep 1s, double delay
│                                        ├── attempt 2 … up to 5 attempts
│                                        │   delay: 1s → 2s → 4s → 8s → max 16s
│                                        │   total window: ~45s (bgCtx timeout)
│                                        └── all retries exhausted?
│                                            └── DeleteJob (so job does not linger
│                                                in NEW state, invisible to orphan
│                                                scheduler)
```

### Why `err = nil` Is Set Before Returning

The `_splitStartVolume` function has two active deferred closures at the point of workflow dispatch:

1. **Job-delete defer** — deletes the job if `err != nil` at function return.
2. **Clone-state revert defer** — reverts `SPLITTING` back to `CLONED` and restores `clones_shared_bytes` if `err != nil` and the split was not yet initiated (or transitions to `ERROR_IN_SPLITTING` for ONTAP-terminal errors).

Setting `err = nil` before returning prevents both defers from running. This is intentional: ONTAP has already accepted the split, so the clone state must remain `SPLITTING`, `clones_shared_bytes` must stay at 0 (the reserved value), and the job must not be deleted. The background goroutine owns the job state from this point forward.

### Retry Parameters

| Parameter | Value | Source |
|---|---|---|
| Max retries | 5 | `waitForTemporalUpdateMaxRetries` |
| Initial delay | 1 second | `waitForTemporalUpdateInitDelay` |
| Max delay per step | 16 seconds | `waitForTemporalUpdateMaxDelay` |
| Total context timeout | 45 seconds | `waitForTemporalBgTimeout` |

The delay doubles after each failed attempt (exponential backoff), capped at 16 seconds. If the DB recovers within the 45-second window, the job is placed in `WAIT_FOR_TEMPORAL` and will be picked up by the next orphan-job sweep (within 2 minutes). If all retries are exhausted, `DeleteJob` is called so the orphan scheduler does not encounter a stale `NEW`-state job it cannot process.

### Goroutine Context Independence

The goroutine uses a **fresh `context.Background()`** with its own timeout (`waitForTemporalBgTimeout`), not the incoming request context. This is necessary because the request context is cancelled when the HTTP handler returns. Reusing the request context would cancel the goroutine's DB call immediately after HTTP 200 is sent.

---

## State Machine for Split Jobs

```
                       ┌─────────────────────────────────────────────────┐
                       │ Split API called                                │
                       └──────────────────────┬──────────────────────────┘
                                              │
                                              ▼
                               ┌──────────────────────────┐
                               │  Job created: state=NEW  │
                               └──────────────┬───────────┘
                                              │
                               InitiateSplitVolume (ONTAP)
                                              │
                              ┌───────────────┴──────────────────┐
                              │ Success                          │ Failure
                              ▼                                  ▼
                  ┌─────────────────────┐            ┌──────────────────────────┐
                  │  splitInitiated=true│            │  Revert state → CLONED   │
                  │  Persist SplitJobUUID│           │  Restore clones_shared_bytes│
                  └──────────┬──────────┘            │  Delete job              │
                             │                       └──────────────────────────┘
                    ExecuteWorkflow (Temporal)
                             │
             ┌───────────────┴───────────────────────┐
             │ Success                               │ Failure
             ▼                                       ▼
 ┌────────────────────────┐           ┌──────────────────────────────────┐
 │ VolumePollSplitWorkflow│           │ SPLIT_WAIT_FOR_TEMPORAL_ENABLED? │
 │ polls ONTAP → Done     │           └─────────────┬────────────────────┘
 │ State → READY          │                         │
 └────────────────────────┘          ┌──────────────┴───────────────────┐
                                     │ YES                              │ NO
                                     ▼                                  ▼
                         ┌────────────────────────┐       ┌────────────────────────┐
                         │ Goroutine: UpdateJob    │       │ Return error to caller │
                         │ state=WAIT_FOR_TEMPORAL │       │ Defers run:            │
                         │ (retries up to 5×)      │       │  - job deleted         │
                         │ Main: return HTTP 200   │       │  - state reverted      │
                         └──────────┬─────────────┘       └────────────────────────┘
                                    │
                         OrphanJobsActivity (every 2 min)
                                    │
                         ┌──────────┴──────────────────────────────────┐
                         │ retry < 5                                   │ retry ≥ 5
                         ▼                                             ▼
             ┌─────────────────────────┐               ┌────────────────────────────┐
             │ PrepareWorkflowArgs     │               │ FailedWorkflowJob          │
             │ ExecuteWorkflow →       │               │ State → ERROR_IN_SPLITTING │
             │ VolumePollSplitWorkflow │               │ Job → ERROR                │
             └─────────────────────────┘               └────────────────────────────┘
```

---

## Consistency Guarantees

| Scenario | ONTAP State | VCP Clone State | Job State | Customer Experience |
|---|---|---|---|---|
| Temporal unavailable, DB healthy | Splitting | `SPLITTING` | `WAIT_FOR_TEMPORAL` | HTTP 200, split continues |
| Temporal unavailable, DB recovers within 45s | Splitting | `SPLITTING` | `WAIT_FOR_TEMPORAL` → picked up by orphan scheduler | HTTP 200, split continues |
| Temporal unavailable, DB exhausts all retries | Splitting | `SPLITTING` | `NEW` (job deleted after goroutine exhaustion) | HTTP 200; split still runs in ONTAP; no job to track — gap in visibility |
| Orphan scheduler cannot start workflow within 5 retries | Splitting (may be complete) | `SPLITTING` | `ERROR` → `ERROR_IN_SPLITTING` | Visible terminal error; operator must investigate |
| Split never reached ONTAP | Not started | `CLONED` (reverted) | Deleted | HTTP error returned; customer can retry |

---

## Configuration Reference

| Variable | Default | Description |
|---|---|---|
| `SPLIT_WAIT_FOR_TEMPORAL_ENABLED` | `false` | Enable the `WAIT_FOR_TEMPORAL` path for split jobs |
| `ORPHAN_JOB_PROCESSING_ENABLED` | `true` | Enable the orphan job scheduler activity |
| `ORPHANED_JOB_SCHEDULER` cron | Every 2 minutes | How often the orphan scheduler runs |

Internal constants (not environment-configurable):

| Constant | Value | Description |
|---|---|---|
| `waitForTemporalUpdateMaxRetries` | 5 | Max DB update attempts in the goroutine |
| `waitForTemporalUpdateInitDelay` | 1s | Initial backoff delay |
| `waitForTemporalUpdateMaxDelay` | 16s | Maximum backoff delay per step |
| `waitForTemporalBgTimeout` | 45s | Total context lifetime for the goroutine |
| `WaitForTemporalJobMaxRetryCount` | 5 | Max orphan-scheduler attempts per job |

---

## Related Components

| Component | Location | Role |
|---|---|---|
| `_splitStartVolume` | `core/orchestrator/factory/gcp/volume.go` | Orchestrates split initiation, workflow dispatch, and goroutine launch |
| `VolumePollSplitWorkflow` | `core/orchestrator/workflows/` | Polls ONTAP split job and completes volume lifecycle |
| `OrphanJobsActivity` | `core/orchestrator/activities/backgroundactivities/orphan_job_activities.go` | Finds and re-submits `WAIT_FOR_TEMPORAL` jobs |
| `SplitVolumeArgs` | `core/orchestrator/activities/backgroundactivities/split_volume_orphan_job_workflow_manager.go` | Implements `OrphanJobWorkflowManager` for split jobs |
| `OrphanJobSchedulerWorkflow` | Background workflows | Temporal cron workflow wrapping `OrphanJobsActivity` |
| `JobsStateWaitForTemporal` | `core/models/job.go` | Sentinel job state for orphan-eligible jobs |

---


