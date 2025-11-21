# Runbook: Debugging and Fixing Volume Creation Errors

This runbook provides a structured approach to investigating and resolving errors encountered during **volume creation** in the GCP pool orchestration system. It leverages the provided architecture and troubleshooting guidelines.

---

## Debugging Steps (Guidelines)

### 1. Acknowledge the Alert
- **Action:** Acknowledge the alert in the monitoring system to prevent repeated notifications.
- **Record:** Note the time of acknowledgment for future reference.

---

### 2. Gather Initial Context
- **Review Alert Details:** Examine the alert description, severity, and any associated dashboards or logs.
- **Identify Impacted Components:** Determine which pool, workflow, or API endpoint is affected (e.g., `V1betaCreateVolume`, `CreateVolumeWorkflow`).
- **Check Recent Changes:** Review deployment logs, configuration changes, or infrastructure updates that may have impacted volume creation.

---

### 3. Validate the Alert
- **Confirm Legitimacy:** Ensure the alert is not a false positive by cross-referencing with system metrics and logs.
- **Verify Triggering Data:** Check the specific metrics or error messages that caused the alert (e.g., API 400/409/500 errors, workflow failures).

---

### 4. Identify the Root Cause

#### a. Logs Analysis
- **API Layer:** Check logs from the volume creation endpoint for validation errors, idempotency conflicts, or malformed requests.
- **Orchestrator/Workflow:** Review logs from orchestrator and workflow components for workflow failures, validation pipeline errors, or activity exceptions related to volume creation.
- **DB/Repository:** Inspect logs from the database/repository layer for persistence errors, transaction failures, or unique constraint violations during volume creation.

#### b. Metrics Review
- **Resource Utilization:** Examine CPU, memory, disk I/O, and network metrics for the affected worker nodes.
- **Workflow Metrics:** Check Temporal workflow status, activity durations, and error rates for volume creation workflows.

#### c. System Health Check
- **Temporal:** Ensure Temporal workers are running and task queues are healthy.
- **Database:** Verify DB connectivity, schema migrations, and performance.

#### d. Dependency Check
- **GCP Services:** Confirm required APIs (Service Networking, IAM, GCS, Secret Manager) are enabled and quotas are sufficient.
- **Networking:** Validate VPC, subnet, and firewall configurations for the pool and volume.

#### e. Configuration Review
- **Feature Flags:** Check relevant environment flags (e.g., `REGIONAL_SUPPORT_ENABLED`, `AUTO_TIERING_ENABLED`) that may impact volume creation.
- **Volume Parameters:** Ensure volume creation parameters (size, throughput, IOPS, zone, pool association) are within allowed ranges and reference a valid, READY pool.

---

### 5. Formulate a Hypothesis
- Based on the above analysis, hypothesize the root cause. Common causes include:
    - Invalid volume parameters (size, type, pool reference).
    - DB transaction failures or unique constraint violations.
    - GCP API errors (quota, permissions, disabled APIs).
    - Temporal workflow startup or activity failures.
    - Misconfigured feature flags or environment variables.
    - Pool not in READY state or insufficient pool capacity.

---

### 6. Implement a Solution/Mitigation

#### a. Temporary Mitigation
- **Retry Operation:** If the error is transient (e.g., DB connectivity, GCP API rate limit), retry the volume creation request.
- **Manual Intervention:** For stuck workflows, manually resume or retry failed Temporal activities.

#### b. Permanent Fix
- **Parameter Correction:** Update volume creation parameters to comply with validation rules.
- **Configuration Update:** Fix misconfigured environment flags or feature toggles.
- **Infrastructure Scaling:** Increase quotas or resource limits in GCP if resource exhaustion is detected.
- **Code Changes:** Address bugs in validation logic, workflow orchestration, or DB persistence.
- **Pool State:** Ensure the target pool is in READY state and has sufficient capacity for the requested volume.

---

### 7. Verify the Fix
- **Monitor System:** Ensure the alert clears and volume creation succeeds.
- **Run Tests:** Execute volume creation tests to confirm resolution.
- **Check Logs:** Validate absence of related errors in logs.

---

### 8. Document the Resolution
- **Record Details:** Note the root cause, resolution steps, and any temporary mitigations applied.
- **Update Runbook:** Incorporate new insights or steps discovered during troubleshooting.
- **Share Learnings:** Communicate findings with the team to prevent recurrence.

---

## Useful Tools and Resources

| Tool/Resource              | Location/Link (to be filled by team) |
|----------------------------|--------------------------------------|
| Monitoring System          | File                                 |
| Logging Platform           | File                                 |
| Documentation Wiki         | File                                 |
| Team Communication Channel | File                                 |

---

## Common Volume Creation Error Scenarios & Quick Fixes

| Symptom                                   | Probable Cause                                   | Mitigation Steps                                   |
|--------------------------------------------|--------------------------------------------------|----------------------------------------------------|
| 400: Invalid volume parameters             | Size/type/pool/throughput/IOPS out of bounds     | Correct request payload; check feature flags        |
| 409: Volume exists or in CREATING state    | Idempotency conflict or duplicate resourceId      | Use distinct resourceId; reuse operation if CREATING|
| 500: DB/Workflow start failure             | DB connectivity, Temporal worker down             | Check DB/Temporal health; retry after recovery      |
| GCP API errors (quota, permissions)        | Disabled APIs, insufficient quotas, IAM issues    | Enable APIs; increase quotas; fix IAM permissions   |
| Workflow stuck or failed                   | Temporal worker outage, non-retryable error       | Resume/retry workflow; fix underlying issue         |
| Pool not READY or insufficient capacity    | Pool in CREATING/UPDATING/DELETING state, or full | Wait for pool to be READY; free up space or scale pool|
| Network/Service Networking errors          | Misconfigured VPC, subnet, firewall, SN host      | Validate network setup; fix configuration           |

---

## Example Troubleshooting Flow

1. **Alert:** Volume creation failed with 400 error.
2. **Acknowledge:** Mark alert as acknowledged at 10:15 UTC.
3. **Context:** Review logs; error message indicates volume size exceeds pool capacity.
4. **Validate:** Confirm error is legitimate; payload requests 10TiB, pool has only 5TiB free.
5. **Root Cause:** Insufficient pool capacity for requested volume.
6. **Mitigation:** Choose a smaller volume size or free up space in the pool, then retry.
7. **Verify:** Volume creation succeeds; alert clears.
8. **Document:** Update runbook with new pool capacity monitoring steps; notify team.

---

## Notes

- Always check for upstream GCP issues (quotas, API outages) before deep-diving into code.
- Use Temporal and DB logs to pinpoint workflow or persistence failures.
- Ensure all feature flags are correctly set for the intended volume type and region.
- Confirm the referenced pool is in READY state and has sufficient capacity.

---