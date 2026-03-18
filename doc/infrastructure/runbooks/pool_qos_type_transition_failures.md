# Runbook for pool_qos_type_transition_failures

This runbook covers pool update (qosType transition) failures and stuck operations when changing a pool's QoS type between **auto** and **manual** (VSCP-3632). On **manual→auto** transition, the workflow deletes **all** volume performance groups (VPGs) for the pool (not only auto-generated ones) and registers rollback for each deleted VPG so they can be restored on failure.

# Alert Information

| Field               | Description                                                                                   |
| :-----------------: | :------------------------------------------------------------------------------------------: |
| Alert Name          | pool_qos_type_transition_failures                                                            |
| Alert Context       | Pool update (qosType transition) failures or stuck operations                               |
| Severity            | Error                                                                                        |
| Related Card        | VSCP-3632                                                                                    |

# How to Find the Job / Workflow

Use one of the following to locate the failing operation:

| Identifier        | Where to get it | Use |
|-------------------|-----------------|-----|
| **Correlation ID** | API response header `x-correlation-id`, or from logs | Filter GCP Cloud Logging by `jsonPayload.correlation_id` or `"CORRELATION_ID"` to get the full request trace. |
| **Pool ID / UUID** | Pool resource (e.g. `GET pool`), or from the update request | Query DB (`pools` by `uuid`) or use to identify the pool in recovery steps. |
| **Operation ID**   | Returned by API when update is long-running (e.g. 202) | Poll operation status; use with Temporal or job APIs to find workflow/job. |
| **Workflow ID**    | Temporal UI or worker logs | Correlate via `correlation_id` in logs; workflow name typically includes pool identifier. |

**Log correlation:** In GCP Cloud Logging, filter by correlation id to see API → orchestrator → workflow → activities. Key fields: `correlation_id`, `workflow_id`, `job_id`, `message`, `severity`, `error`.

# Common Failures and Causes

| Symptom / Error | Likely Cause | Mitigation (what to do) |
|-----------------|--------------|-------------------------|
| **404 on ONTAP** (modify SVM, QoS) | SVM `external_uuid` in DB does not match ONTAP SVM UUID. DB may have `00000000-0000-0000-0000-000000000000` or stale value. | **DB:** In the vcp database, update the pool’s SVM record so that `svm_details.external_uuid` (or the equivalent column for the SVM’s ONTAP UUID) is set to the SVM UUID returned by ONTAP for that vserver. Obtain the correct UUID from ONTAP (e.g. GET /api/svm/svms by name), then update the row. Retry the transition or run a restore to baseline. |
| **Policy not found / QoS policy missing on ONTAP** | After auto→manual, QPG was removed from SVM. Manual→auto (or rollback/restore) needs the pool’s QoS policy to exist on ONTAP. | **VSA/ONTAP:** Create the pool’s QoS policy on the vserver in ONTAP. The policy name must be `{svm_name}-qos-policy` (e.g. `vcp-vsim-svm-qos-policy`). Set throughput and IOPS to match the pool. Then retry or run a restore to baseline. |
| **Partial transition or failed rollback** | Workflow failed mid-transition (e.g. after RemoveQoSPolicyFromSVM but before VPG create, or after unassign but before apply QPG). Rollback may have failed. | **DB:** Restore pool and VPG state to a known baseline (e.g. clear or repopulate `volume_performance_groups` and `volumes.volume_performance_group_id` for the pool as needed). **VSA/ONTAP:** Remove any leftover transition QoS policy from the vserver; apply or remove the pool QPG on the vserver to match the desired baseline (auto vs manual). If the pool is stuck in **UPDATING**, also set the pool’s `state` to `READY` and mark the stuck UPDATE_POOL job as ERROR (see step 5). |
| **Pool stuck in UPDATING; vserver has no QPG** | Workflow failed and rollback did not run to completion. Pool state and vserver QoS were never reverted. | **VSA/ONTAP:** Apply the pool’s QoS policy to the vserver so the vserver has the expected qos-policy-group. **DB:** Set the pool’s `state` to `READY` and clear `state_details`; mark the stuck UPDATE_POOL (or UPDATE_LARGE_POOL) job as ERROR so the pool is no longer stuck (see step 5). |
| **Manual→auto completed but vserver has no QPG** | `ModifyQoSPolicyAndApplyToSVM` was skipped or apply failed. | **VSA/ONTAP:** Apply the pool’s QoS policy to the vserver (name `{svm_name}-qos-policy`, throughput/IOPS matching pool) so the vserver has the qos-policy-group. |
| **violates foreign key constraint** (manual→auto) | Volumes (including soft-deleted) still reference a VPG when the workflow tries to hard-delete it. | **DB:** Set `volume_performance_group_id` to NULL for all volumes in the pool that reference a VPG (including soft-deleted volumes) so no rows reference the VPG before it is deleted. The workflow does this before deleting VPGs; if you see this on an older run, restore to baseline and retry. |
| **Workflow stuck / job never completes** | Temporal activity timeout, worker down, or dependency (DB/ONTAP) unreachable. | Check Temporal UI for workflow status and failed activities; check worker and dependency health. If state is inconsistent, restore DB and ONTAP to a known baseline (edit pool/VPG/volume state in DB and create/delete QoS objects in ONTAP as needed). |
| **Validation error (400)** | Request `qosType` not `"auto"` or `"manual"`, or other validation. | Fix request body; ensure `qosType` is exactly `"auto"` or `"manual"`. |

**Restore vserver QPG after manual→auto (no QPG on vserver):** **VSA/ONTAP:** Create the pool’s QoS policy on the vserver (name `{svm_name}-qos-policy`) and apply it to the vserver so the vserver has the qos-policy-group. Use pool throughput/IOPS for the policy.

# Recovery Steps

1. **Gather context**
   - Correlation ID, pool UUID, operation/job ID from API or logs.
   - Note current pool state (API `GET pool` → `qosType`; DB `pools.qos_type`; optionally ONTAP vserver and volume QoS).

2. **If ONTAP returns 404 (SVM not found / wrong UUID)**
   - Get the SVM UUID from ONTAP (e.g. GET /api/svm/svms by vserver name).
   - **DB:** In the vcp database, update the pool’s SVM record so that the SVM’s `external_uuid` (or equivalent) is set to that ONTAP SVM UUID. Then retry the transition or run a restore to baseline.

3. **If QoS policy is missing on ONTAP**
   - **VSA/ONTAP:** Create the pool’s QoS policy on the vserver in ONTAP. Name must be `{svm_name}-qos-policy`. Set throughput and IOPS to match the pool. Then retry or run a restore to baseline.

4. **Restore to known-good state (partial transition / failed rollback)**
   - **DB:** Restore pool and VPG state: e.g. delete or update rows in `volume_performance_groups` for the pool so that only the desired baseline remains; set `volumes.volume_performance_group_id` to NULL or to the correct VPG ID as needed; ensure `pools.qos_type` and pool state match the desired baseline.
   - **VSA/ONTAP:** Remove any leftover transition QoS policy from the vserver (e.g. policy named like `{pool_name}-vpg`). For baseline **auto**, apply the pool’s QoS policy to the vserver (name `{svm_name}-qos-policy`). For baseline **manual**, ensure the converted VPG’s QoS policy exists on the vserver and volumes are assigned as needed.
   - If the pool is stuck in **UPDATING**, also complete step 5.

5. **If pool is stuck in UPDATING after failed transition**
   - **DB:** Set the pool’s `state` to `READY` and clear `state_details` for the pool (table `pools`, filter by pool UUID). Mark the stuck update job as ERROR: in table `jobs`, set `state` to `ERROR` and set `error_details` to a note such as "Recovered after failed qosType transition (runbook)" for the job whose `job_attributes` contains the pool UUID and whose `job_type` is UPDATE_POOL or UPDATE_LARGE_POOL and whose `state` is NEW, PROCESSING, or WAIT_FOR_TEMPORAL.
   - **VSA/ONTAP:** Confirm the vserver has the expected qos-policy-group (e.g. via ONTAP CLI or REST). If not, apply the pool’s QoS policy to the vserver as in step 4.

6. **Optional: fix SVM UUID**
   - **DB:** Sync the pool’s SVM `external_uuid` from ONTAP (get UUID from ONTAP, then update the SVM record in the vcp database) before other restore steps if the root cause was SVM UUID mismatch.

# Debugging Steps (Guidelines)

1. **Acknowledge the Alert**  
   Record time and alert details.

2. **Gather Initial Context**  
   Correlation ID, pool UUID, operation/job ID; current `qosType` from API and DB; recent logs for that correlation id.

3. **Validate the Alert**  
   Confirm the pool update was a qosType change (auto↔manual); check workflow/job status in Temporal or job API.

4. **Identify Root Cause**  
   - **Logs:** Filter by correlation_id; look for first ERROR (RemoveQoSPolicyFromSVM, CreateVPG, AssignQoSPolicyToVolume, ModifyQoSPolicyAndApplyToSVM, etc.).
   - **DB:** Check `pools.qos_type`, `volume_performance_groups`, `volumes.volume_performance_group_id` for the pool.
   - **ONTAP:** Check vserver qos-policy-group and volume qos-policy-group (REST or CLI) to see if state is partial (e.g. QPG cleared but no per-volume policy).

5. **Implement Solution**  
   Apply the mitigation from the table above (patch SVM UUID, create QoS policy, or restore to baseline using the recovery steps above). Retry transition only if state is consistent; otherwise restore first.

6. **Verify**  
   GET pool → expected `qosType`; DB and ONTAP match (Section 8.3 of plan). Re-run transition if safe.

7. **Document**  
   Record root cause, steps taken, and any runbook updates.

# Observability (Logging)

qosType transition workflow logs the following so you can find and triage transitions in GCP Cloud Logging (filter by `correlation_id` or `workflow_id`):

| Log message | When | Fields |
|-------------|------|--------|
| **qosType transition started** | Start of auto→manual or manual→auto | `direction` (auto_to_manual \| manual_to_auto), `poolUUID` |
| **qosType auto→manual: removing QPG from vserver** | After GetNode, before RemoveQoSPolicyFromSVM | — |
| **qosType manual→auto: applying QPG to vserver** | Before ModifyQoSPolicyAndApplyToSVM | — |
| **qosType transition completed** | Success | `direction`, `poolUUID` |
| **qosType transition failed** | Any activity failure (before rollback) | `direction`, `poolUUID`, `error` |

There are no dedicated Prometheus metrics for qosType transition; observability is via structured logs (ADR-0011). Use `correlation_id`, `workflow_id`, and `job_id` from log payloads to trace the full request and correlate with Temporal.

# Useful Links and References

| Resource | Path / link |
|----------|-------------|
| **Master plan (VSCP-3632)** | [VSCP-3632_POOL_QOS_TYPE_TRANSITION_PLAN.md](../../../VSCP-3632_POOL_QOS_TYPE_TRANSITION_PLAN.md) — Section 8 (pinned environment, pool, test-driven order), Section 8.6 (restore). |
| **Workflow docs** | [doc/workflows/core/pool-workflows.md](../../workflows/core/pool-workflows.md) — Pool workflows and update flow. |
| **Temporal debugging** | [doc/guides/temporal-debugging.md](../../guides/temporal-debugging.md) — Finding workflows and history. |

---

**Tip:** For any qosType transition failure, correlate logs by `correlation_id`, then check DB and ONTAP state. If state is partial or unknown, restore DB and ONTAP to a known baseline using the steps above before retrying the transition.
