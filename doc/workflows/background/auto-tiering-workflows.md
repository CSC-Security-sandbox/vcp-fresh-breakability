# Auto-Tiering Background Workflows

This document describes the auto-tiering background workflows in the VSA Control Plane system, including the periodic sync workflow, pause/resume child workflows, and hot-tier auto-resize child workflows.

## Overview

Auto-tiering background workflows manage the automated synchronization of tiering consumption data from ONTAP, enforce pause/resume policies based on pool capacity constraints, and handle hot-tier auto-resize when usage thresholds are met. These workflows run on a scheduled cadence and operate at the pool level across all accounts.

## Related Documents

- [Auto-Tiering Design](../../architecture/designs/0005-vsa-auto-tiering-design.md)
- [ADR-0012: Pause/Resume Intermediary States](../../architecture/decisions/0012-auto-tiering-pause-resume-intermediary-state-addition-decision.md)
- [ADR-0013: Thin Clone Behaviour](../../architecture/decisions/0013-auto-tiering-thin-clone-behaviour-decision.md)
- [Pool Workflows](../core/pool-workflows.md)
- [Volume Workflows](../core/volume-workflows.md)
- [Temporal Debugging Guide](../../guides/temporal-debugging.md)

## Schedule

The `SYNC_VSA_AUTO_TIERING` job is registered with cron expression `*/5 * * * *` (every 5 minutes) in `core/scheduler/adminbackgroundjobs/admin_background_jobs.json`.

The job can be disabled by setting the environment variable `AUTO_TIERING_ENABLED=false` (default: `true`).

## Workflow Types

### 1. SyncVSAAutoTieringWorkflow (Parent)

**File**: `core/orchestrator/workflows/backgroundworkflows/sync_auto_tiering_workflows.go`

**Purpose**: Top-level scheduled workflow that orchestrates consumption data collection, pool segregation, pause/resume operations, and hot-tier auto-resize across all eligible pools.

**Entry Point**: `SyncVSAAutoTieringWorkflow(ctx workflow.Context) error`

**Task Queue**: Background task queue

#### Configuration

| Environment Variable | Default | Description |
|---|---|---|
| `SYNC_VSA_AUTO_TIERING_WORKFLOW_START_TO_CLOSE_TIMEOUT_SEC` | 600 | Activity start-to-close timeout (seconds) |
| `SYNC_VSA_AUTO_TIERING_WORKFLOW_HEARTBEAT_TIMEOUT_SEC` | 600 | Activity heartbeat timeout (seconds) |
| `AUTO_TIERING_SYNC_ACTIVITY_MAX_ATTEMPTS` | 1 | Max retry attempts for sync activities |
| `CONSECUTIVE_UPDATE_POOL_TIME_GAP_ALLOWED_MINUTES` | 240 | Minimum gap (minutes) between consecutive auto-resize runs for the same pool |

#### Activity Sequence

```
SyncVSAAutoTieringWorkflow
│
├── 1. ListPoolsUUID
│       Filter: READY, Updating, Degraded states
│       Returns: list of PoolIdentifier
│
├── 2. FetchAndSavePoolsTieringInfo
│       For each auto-tiering-enabled pool (parallel):
│         - Fetch pool details from DB
│         - Get ONTAP provider
│         - Fix legacy tiering fullness threshold (50 → 0) if needed
│         - Fetch volumes from ONTAP
│         - Calculate hot/cold tier consumption per volume
│         - Persist per-volume tiering footprints to DB (non-ONTAP mode only)
│       Returns: map[poolUUID] → {hotTier, coldTier} consumption
│
├── 3. UpdatePoolTieringConsumptionInDB
│       Persists aggregate hot/cold consumption per pool to the pool table.
│       NOTE: Failure here is logged but does NOT abort the workflow.
│
├── 4. SegregatePools
│       For each auto-tiering-enabled pool:
│         Pause condition:  hotTierSize + coldTierConsumption >= poolSize
│                           AND pool is NOT already paused
│                           AND pool is NOT in ONTAP mode
│         Resume condition: hotTierSize + coldTierConsumption < poolSize
│                           AND pool is NOT already resumed
│                           AND pool is NOT in ONTAP mode
│         Auto-resize condition (checked only if not eligible for pause):
│           - enableHotTierAutoResize = true
│           - hotTierSizeInBytes > 0
│           - pool state = READY
│           - no volumes have bypass mode enabled
│           - hot tier usage % >= threshold (default 100%)
│           - new hot tier size + cold tier < pool size
│       Returns: {poolsToPause, poolsToResume, poolsToAutoResize}
│
├── 5. Spawn AutoTieringPauseResumeWorkflow child workflows
│       One child per pool, ParentClosePolicy = ABANDON
│       Waits for ALL pause/resume children to complete before proceeding
│       Failures are logged; other children continue
│
├── 6. Spawn AutoTieringHotTierAutoResizeWorkflow child workflows
│       One child per pool, ParentClosePolicy = ABANDON
│       Skipped if last execution was within CONSECUTIVE_UPDATE_POOL_TIME_GAP_ALLOWED_MINUTES
│       Fires asynchronously (parent does NOT wait)
│
└── 7. Sleep 2s (allow last child to start), then return
```

#### Error Handling

- `ListPoolsUUID` failure: workflow returns error (fatal).
- `FetchAndSavePoolsTieringInfo` failure: workflow returns error (fatal).
- `UpdatePoolTieringConsumptionInDB` failure: logged, workflow continues.
- `SegregatePools` failure: workflow returns error (fatal).
- Pause/resume child failure: logged, other children continue, parent succeeds.
- Auto-resize `GetWorkflowLastExecutionTime` failure: that pool is skipped.

---

### 2. AutoTieringPauseResumeWorkflow (Child)

**File**: `core/orchestrator/workflows/backgroundworkflows/sync_auto_tiering_workflows.go`

**Purpose**: Pauses or resumes auto-tiering for a single pool by toggling volume-level tiering policies and aggregate-level fullness thresholds in ONTAP, then persisting the resulting status to the database.

**Entry Point**: `AutoTieringPauseResumeWorkflow(ctx workflow.Context, poolIdentifier database.PoolIdentifier, operation string) error`

**Operations**: `"poolsToPause"` or `"poolsToResume"`

#### Configuration

| Environment Variable | Default | Description |
|---|---|---|
| `AUTO_TIERING_PAUSE_RESUME_WORKFLOW_START_TO_CLOSE_TIMEOUT_SEC` | 600 | Activity start-to-close timeout (seconds) |
| `AUTO_TIERING_PAUSE_RESUME_WORKFLOW_HEARTBEAT_TIMEOUT_SEC` | 600 | Activity heartbeat timeout (seconds) |
| `AUTO_TIERING_PAUSE_RESUME_ACTIVITY_MAX_ATTEMPTS` | 3 | Max retry attempts |

#### Activity Sequence

```
AutoTieringPauseResumeWorkflow(poolIdentifier, operation)
│
├── 1. FetchPoolByUUID
│       Retrieves full pool from DB
│
├── 2. GetNode
│       Fetches cluster node details for ONTAP connectivity
│
├── 3. ToggleHotTierBypassModeForPoolVolumes
│       For each volume with hotTierBypassMode enabled:
│         Pause:  set tiering policy → none, cloudWriteMode → false
│         Resume: set tiering policy → all,  cloudWriteMode → true
│       On failure: status becomes PARTIALLY_PAUSED or PARTIALLY_RESUMED
│
├── 4. ParseVlmConfig → extract aggregate names
│
├── 5. UpdateAggregatesInOntap
│       Pause:  set tiering fullness threshold → 100% (stop tiering)
│       Resume: set tiering fullness threshold → 0%   (allow tiering)
│       On failure: status becomes PARTIALLY_PAUSED or PARTIALLY_RESUMED
│
└── 6. UpdatePoolTieringThresholdAndStatus
        Persists final tiering threshold + status to DB
```

#### Tiering Status State Machine

```
              Pause success          Resume success
  RESUMED ──────────────────► PAUSED ──────────────────► RESUMED
     │                          │
     │ Pause partial failure    │ Resume partial failure
     ▼                          ▼
  PARTIALLY_PAUSED          PARTIALLY_RESUMED
     │                          │
     │ Next sync retry          │ Next sync retry
     └──── (re-evaluated) ──────┘
```

States:
- **PAUSED**: Both volume toggles and aggregate threshold update succeeded for pause.
- **RESUMED**: Both succeeded for resume.
- **PARTIALLY_PAUSED**: Either `ToggleHotTierBypassModeForPoolVolumes` or `UpdateAggregatesInOntap` failed during a pause operation. Retried in the next sync cycle.
- **PARTIALLY_RESUMED**: Same, but during a resume operation.

See [ADR-0012](../../architecture/decisions/0012-auto-tiering-pause-resume-intermediary-state-addition-decision.md) for the rationale behind intermediary states.

---

### 3. AutoTieringHotTierAutoResizeWorkflow (Child)

**File**: `core/orchestrator/workflows/backgroundworkflows/sync_auto_tiering_workflows.go`

**Purpose**: Automatically increases the hot-tier size for a pool when hot-tier usage exceeds the configured threshold, by creating an admin UpdatePool job.

**Entry Point**: `AutoTieringHotTierAutoResizeWorkflow(ctx workflow.Context, pool *database.PoolIdentifier) error`

#### Configuration

| Environment Variable | Default | Description |
|---|---|---|
| `AUTO_TIERING_HOT_TIER_AUTO_RESIZE_WORKFLOW_START_TO_CLOSE_TIMEOUT_SEC` | 600 | Activity start-to-close timeout (seconds) |
| `AUTO_TIERING_HOT_TIER_AUTO_RESIZE_WORKFLOW_HEARTBEAT_TIMEOUT_SEC` | 600 | Activity heartbeat timeout (seconds) |
| `AUTO_TIER_HOT_TIER_AUTO_RESIZE_THRESHOLD_PERCENT` | 100 | Hot-tier usage % that triggers resize |
| `AUTO_TIER_HOT_TIER_AUTO_RESIZE_INCREASE_PERCENT` | 10 | Percentage increase applied to hot-tier size |

#### Activity Sequence

```
AutoTieringHotTierAutoResizeWorkflow(pool)
│
├── 1. FetchPoolByUUID
│
├── 2. Calculate new hot tier size
│       newSize = roundToGiB(currentHotTier * (1 + increasePercent/100))
│
├── 3. CreateJob (admin UpdatePool job)
│
├── 4. UpdatingPool (set pool state to Updating)
│
└── 5. Execute UpdatePoolWorkflow (child)
        Uses the standard pool update workflow with:
          - AutoResizeTriggeredUpdate = true
          - New HotTierSizeInBytes
```

#### Deduplication

The parent workflow checks the last execution time via a Temporal query (`StatusQueryName`) on the previous workflow run. If the last run (success or failure) was within `CONSECUTIVE_UPDATE_POOL_TIME_GAP_ALLOWED_MINUTES` (default 4 hours), the auto-resize is skipped for that pool.

The workflow ID follows the pattern: `Account_{accountID}_Location_{location}_Pool_{poolUUID}_Ops_AutoTiering-HotTier-AutoResize`

---

## Activities

### AutoTierSyncActivity

**File**: `core/orchestrator/activities/backgroundactivities/sync_auto_tiering_activities.go`

| Activity | Description |
|---|---|
| `FetchAndSavePoolsTieringInfo` | Fetches ONTAP volumes for all auto-tiering-enabled pools in parallel, calculates hot/cold tier footprints per volume using logical-space-corrected ratios, and bulk-updates volume tiering fields in DB. |
| `UpdatePoolTieringConsumptionInDB` | Persists per-pool hot/cold tier consumption totals to the pool table. |
| `SegregatePools` | Evaluates each pool's capacity constraints and auto-resize eligibility in parallel, producing three buckets: pause, resume, auto-resize. |
| `ToggleHotTierBypassModeForPoolVolumes` | For volumes with `hotTierBypassModeEnabled`, toggles the ONTAP tiering policy between `all`/`none` and enables/disables `cloudWriteMode`. Collects errors across volumes; returns aggregated error. |
| `UpdateAggregatesInOntap` | Updates the `tieringFullnessThreshold` on each aggregate in the cluster (0% for resume, 100% for pause). |
| `UpdatePoolTieringThresholdAndStatus` | Persists the tiering fullness threshold and tiering status to the pool table. |

### Hot/Cold Tier Consumption Calculation

The consumption calculation in `calculateAndUpdateHotColdTierConsumption` corrects for ONTAP data reduction:

```
ratio = capacityTierFootprint / (capacityTierFootprint + performanceTierFootprint)
logicalColdTierConsumption = logicalSpaceUsed * ratio
```

This avoids over-counting when ONTAP compression/deduplication is applied.

---

## Monitoring and Observability

- **Structured logging**: All workflows and activities log with `workflowID`, `customerID`, `parentWorkflowID`, and `requestID` fields.
- **Heartbeats**: Every activity records heartbeats at key milestones for Temporal's dead-worker detection.
- **Temporal UI**: Workflows are visible in the Temporal UI under the background task queue. Child workflows can be traced via `parentWorkflowID` in logs.

## Testing

Test files:
- `core/orchestrator/workflows/backgroundworkflows/sync_auto_tiering_workflows_test.go`
- `core/orchestrator/activities/backgroundactivities/sync_auto_tiering_activities_test.go`

Key test scenarios:
- Full success (sync → pause/resume → auto-resize)
- ListPools error (fatal)
- FetchAndSavePoolsTieringInfo error (fatal)
- UpdatePoolTieringConsumptionInDB error (non-fatal, workflow continues)
- SegregatePools error (fatal)
- Child workflow failure (logged, parent continues)
- Auto-resize skipped due to recent execution (< 4 hours)
- GetWorkflowLastExecutionTime error (pool skipped, parent continues)
- Pause/resume with multiple aggregates (all names passed)
- Partial aggregate update failure (status → PARTIALLY_PAUSED / PARTIALLY_RESUMED)
- ONTAP mode pool auto-resize
