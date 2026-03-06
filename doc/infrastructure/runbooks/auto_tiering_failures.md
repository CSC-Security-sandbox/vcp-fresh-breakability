# Runbook for auto_tiering_failures

This runbook provides debugging guidance for auto-tiering related failures, covering the sync workflow, pause/resume operations, hot-tier auto-resize, and volume-level tiering policy issues.

# Alert Information

| Field               | Description                                                                                   |
| :-----------------: | :------------------------------------------------------------------------------------------: |
| Alert Name          | auto_tiering_failures                                                                        |
| Date of Creation    | Mar 2026                                                                                     |
| Severity            | Warning / Error (depending on scope)                                                         |

# Debugging Steps (Guidelines)

1. **Acknowledge the Alert:**
    - Acknowledge the alert in the monitoring system to prevent repeated notifications.
    - Record the time of acknowledgment.

2. **Gather Initial Context:**
    - Identify the affected pool UUID, account ID, and project.
    - Determine which workflow failed: parent sync, pause/resume child, or auto-resize child.
    - Check the Temporal UI for the workflow execution status on the background task queue.

3. **Validate the Alert:**
    - Confirm the failure is not a transient ONTAP connectivity issue that self-resolved on the next 5-minute sync.
    - Check if the pool is stuck in `PARTIALLY_PAUSED` or `PARTIALLY_RESUMED` state across multiple sync cycles.

4. **Identify the Root Cause** (see detailed sections below).

5. **Implement a Solution/Mitigation:**
    - **Temporary Mitigation:** Retry the Temporal workflow manually via the Temporal UI if safe.
    - **Permanent Fix:** Address the underlying ONTAP, DB, or configuration issue.

6. **Verify the Fix:**
    - Confirm the pool's `TieringStatus` transitions to `PAUSED` or `RESUMED` (not partial).
    - Check that consumption values in the pool table update on the next sync cycle.

7. **Document the Resolution:**
    - Record root cause, steps taken, and any runbook updates.

---

# Failure Scenarios

## Scenario 1: SyncVSAAutoTieringWorkflow Fails Entirely

**Symptoms**: The parent workflow (`SyncVSAAutoTieringWorkflow`) fails. No pause/resume or auto-resize operations execute.

**Common Causes**:
- `ListPoolsUUID` activity failure (DB connectivity issue).
- `FetchAndSavePoolsTieringInfo` activity failure (ONTAP unreachable for all pools).
- `SegregatePools` activity failure (DB query error).

**Debugging**:
1. Open the Temporal UI, locate the failed workflow on the background task queue.
2. Check which activity failed (the first failed activity in the execution history).
3. Review the activity error details and stack trace.
4. Check DB connectivity: verify the Cloud SQL instance is healthy.
5. If ONTAP-related: check cluster health and network connectivity from VCP workers.

**Resolution**:
- DB issues: verify Cloud SQL health, check connection pool limits, confirm IAM auth is working.
- ONTAP issues: verify cluster is online, check VPN/peering, confirm ONTAP credentials in the pool record.
- The workflow will auto-retry on the next 5-minute cron schedule.

---

## Scenario 2: Pool Stuck in PARTIALLY_PAUSED or PARTIALLY_RESUMED

**Symptoms**: A pool's `TieringStatus` is `PARTIALLY_PAUSED` or `PARTIALLY_RESUMED` and does not recover across multiple sync cycles (15+ minutes).

**Background**: The pause/resume workflow has two steps:
1. `ToggleHotTierBypassModeForPoolVolumes` — toggles individual volume tiering policies on ONTAP.
2. `UpdateAggregatesInOntap` — sets the aggregate-level `tieringFullnessThreshold` (0% or 100%).

If either step fails, the pool enters a partial state. The next sync cycle should re-evaluate and retry.

**Debugging**:
1. Query the pool's current `TieringStatus` and `TieringFullnessThreshold` from the DB.
2. In the Temporal UI, find the `AutoTieringPauseResumeWorkflow` child workflow for this pool (search by pool UUID in the workflow ID).
3. Check which step failed:
    - If `ToggleHotTierBypassModeForPoolVolumes` failed: one or more volume updates on ONTAP failed. Check the activity error for specific volume names.
    - If `UpdateAggregatesInOntap` failed: aggregate update on ONTAP failed. Check aggregate names in the error.
4. Verify ONTAP cluster health: `cluster show`, `aggregate show`.
5. Check if the aggregate exists and is online.
6. For volume toggle failures: check if the volume is still in a valid state on ONTAP (`volume show -fields tiering-policy`).

**Resolution**:
- If ONTAP cluster was temporarily unreachable: wait for the next sync cycle to retry.
- If a specific volume is in a bad state on ONTAP: fix the volume state manually, then the next sync will complete the operation.
- If the aggregate no longer exists (pool was migrated/rebuilt): update the pool's VLM config or fix the aggregate reference.
- Manual override: directly update `TieringStatus` in the DB if the ONTAP state has been manually corrected.

---

## Scenario 3: Hot-Tier Auto-Resize Not Triggering

**Symptoms**: A pool's hot tier is full but no auto-resize workflow runs.

**Preconditions for auto-resize**:
- `enableHotTierAutoResize` = true on the pool.
- `allowAutoTiering` = true.
- Pool state = `READY`.
- No volumes in the pool have `hotTierBypassModeEnabled`.
- Hot-tier usage % >= `AUTO_TIER_HOT_TIER_AUTO_RESIZE_THRESHOLD_PERCENT` (default 100%).
- `newHotTierSize + coldTierConsumption < poolSize`.
- Last auto-resize run was > `CONSECUTIVE_UPDATE_POOL_TIME_GAP_ALLOWED_MINUTES` (default 240 min) ago.

**Debugging**:
1. Verify all preconditions above by querying the pool record and its `AutoTieringConfig`.
2. Check if any volumes have bypass mode enabled (this blocks auto-resize).
3. Check the consumption data: is the hot-tier consumption actually at the threshold?
4. Look for the `GetWorkflowLastExecutionTime` activity result in the parent workflow — was the pool skipped due to a recent run?
5. Check if the pool is being classified as `poolsToPause` instead (pause takes priority over resize, since resizing a pool about to breach total capacity is not useful).

**Resolution**:
- If bypass-mode volumes are blocking: this is by design. Resizing with bypass-mode volumes could exceed pool capacity.
- If the 4-hour cooldown is preventing resize: wait, or adjust `CONSECUTIVE_UPDATE_POOL_TIME_GAP_ALLOWED_MINUTES` if urgency demands it.
- If consumption data is stale: check `FetchAndSavePoolsTieringInfo` for errors; ONTAP connectivity may be the issue.

---

## Scenario 4: Auto-Resize Workflow Fails

**Symptoms**: The `AutoTieringHotTierAutoResizeWorkflow` starts but fails.

**Common Causes**:
- `FetchPoolByUUID` fails (pool deleted or DB issue).
- `CreateJob` fails (job table constraint violation).
- `UpdatingPool` fails (pool is already in `Updating` state from another operation).
- `UpdatePoolWorkflow` child fails (standard pool update failure — Hyperdisk attach, VLM config update, etc.).

**Debugging**:
1. Find the auto-resize workflow in the Temporal UI (workflow ID pattern: `Account_{id}_Location_{loc}_Pool_{uuid}_Ops_AutoTiering-HotTier-AutoResize`).
2. Identify the failed activity.
3. If `UpdatingPool` failed: another pool operation (user-initiated update, resize) may be in progress. Check the pool state and running jobs.
4. If the child `UpdatePoolWorkflow` failed: debug as a standard pool update failure (see [create_pool_failures runbook](./create_pool_failures.md)).

**Resolution**:
- If pool was locked by another update: wait for it to complete. The next sync will retry after the 4-hour cooldown.
- If the UpdatePoolWorkflow failed at a Hyperdisk step: check GCP quotas and Hyperdisk API health.
- The auto-resize will be retried on the next eligible sync cycle.

---

## Scenario 5: Volume Tiering Policy Mismatch (DB vs ONTAP)

**Symptoms**: A volume's tiering policy in the VCP database does not match the actual policy on ONTAP. This can happen after clone creation, pause/resume operations, or manual ONTAP changes.

**Debugging**:
1. Get the volume's tiering policy from the VCP DB (`auto_tiering_policy` JSON field in the volumes table).
2. Check the actual policy on ONTAP: `volume show -vserver <svm> -volume <vol> -fields tiering-policy`.
3. Compare the two. Common mismatches:
    - Clone created without AT policy (ONTAP inherits parent's policy, DB may show `none`).
    - Pause/resume toggled ONTAP policy but DB update failed (partial state).
    - Manual ONTAP change not reflected in DB.

**Resolution**:
- If the DB is wrong: trigger a volume update with the correct tiering policy to re-sync.
- If ONTAP is wrong: trigger a volume update to push the DB policy to ONTAP.
- For clone-related mismatches, see [ADR-0013](../../architecture/decisions/0013-auto-tiering-thin-clone-behaviour-decision.md).

---

## Scenario 6: Legacy Tiering Fullness Threshold (50 → 0 Migration)

**Symptoms**: A pool has `tieringFullnessThreshold = 50` in the DB even though it is not paused.

**Background**: Older pools may have been created with a threshold of 50%. The `FetchAndSavePoolsTieringInfo` activity detects this and auto-corrects to 0% on ONTAP aggregates and in the DB, but only for pools that are not paused/partially-paused.

**Debugging**:
1. Check if the pool's `TieringStatus` is `RESUMED` or `PARTIALLY_RESUMED`.
2. If yes and threshold is still 50: the auto-correction may have failed. Check the sync workflow logs for errors on that pool.

**Resolution**:
- Fix ONTAP aggregate threshold manually: `storage aggregate modify -aggregate <name> -tiering-fullness-threshold 0`.
- Update the pool DB record: set `TieringFullnessThreshold = 0`.

---

# Key Log Queries

Search for auto-tiering workflow logs in Cloud Logging:

```
# All auto-tiering sync workflow logs for a specific execution
# 1. In Temporal UI (or the admin_job_specs table), locate the SYNC_VSA_AUTO_TIERING admin job
# 2. Copy the actual workflow execution ID (UUID) for the failing run
# 3. Substitute it into the query below
resource.type="k8s_container"
jsonPayload.workflowID="<SYNC_WORKFLOW_EXECUTION_ID>"

# (Optional) Find all auto-tiering workflows by workflow type
resource.type="k8s_container"
jsonPayload.workflowType=~"AutoTiering"
# Pause/resume child workflow for a specific pool
resource.type="k8s_container"
jsonPayload.message=~"auto-tiering"
jsonPayload.message=~"<POOL_UUID>"

# Auto-resize workflow for a specific pool
resource.type="k8s_container"
jsonPayload.workflowID=~"AutoTiering-HotTier-AutoResize"
jsonPayload.message=~"<POOL_UUID>"
```

# Key Database Queries

```sql
-- Check pool auto-tiering config
SELECT uuid, name, allow_auto_tiering, auto_tiering_config
FROM pools
WHERE uuid = '<POOL_UUID>';

-- Find pools stuck in partial states
SELECT uuid, name, auto_tiering_config->>'tiering_status' as tiering_status
FROM pools
WHERE auto_tiering_config->>'tiering_status' IN ('PARTIALLY_PAUSED', 'PARTIALLY_RESUMED');

-- Check volume tiering policy
SELECT uuid, name, auto_tiering_enabled, auto_tiering_policy
FROM volumes
WHERE pool_id = (SELECT id FROM pools WHERE uuid = '<POOL_UUID>');
```

# Related Documentation

- [Auto-Tiering Workflows](../../workflows/background/auto-tiering-workflows.md)
- [Auto-Tiering Design](../../architecture/designs/0005-vsa-auto-tiering-design.md)
- [ADR-0012: Pause/Resume Intermediary States](../../architecture/decisions/0012-auto-tiering-pause-resume-intermediary-state-addition-decision.md)
- [Create Pool Failures Runbook](./create_pool_failures.md)
- [Temporal Debugging Guide](../../guides/temporal-debugging.md)
