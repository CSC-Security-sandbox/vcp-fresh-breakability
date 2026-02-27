# Runbook for Long-Running Workflows

This runbook provides a structured approach to **identifying, investigating, and responding** when the Long-Running Workflows alert fires, indicating that at least one workflow completion took longer than 24 hours (based on workflow duration metric).

## ⚠️ Important: Process Adherence Required

**This document is a diagnostic and investigation guide, NOT an implementation manual.**

- **Purpose:** Guide support personnel on **where to check**, **what to check**, and **how to identify** long-running or stuck workflows
- **Do NOT:** Cancel or terminate workflows without following proper change management and approval
- **Always:** Validate which workflow type(s) and which specific run(s) are involved before taking action
- **Escalate:** When in doubt, escalate to SME or Incident manager before terminating any workflow

All remediation actions (e.g., workflow cancellation) **MUST** go through proper change management (tickets, approvals, business confirmation).

---

# Alert Information

| Field               | Description                                                                                   |
| :-----------------: | :------------------------------------------------------------------------------------------: |
| Alert Name          | Long-Running Workflows                                                                        |
| Alert Link          | [GCP Monitoring Alert](https://console.cloud.google.com/monitoring/alerting/policies?project=vsa-monitoring-stage) (use project-specific link) |
| Alert Threshold     | Metric `custom.googleapis.com/workflow_duration` — 99th percentile > 86400 seconds (24 hours) for task queues matching `.*customer.*` |
| Date of Creation    | [Date]                                                                                        |
| SME                 | The Subject Matter Expert responsible for this alert or system.                               |
| Severity            | Error                                                                                         |

**What this alert means:** At least one workflow **completion** in the alignment window had a duration greater than 24 hours. The metric is based on **completed** workflow runs (duration recorded when the workflow finishes). It does **not** directly show "workflows currently running > 24h"; for that, use database or Temporal visibility (see Section 4 and Accessing Temporal Workflow Information).

---

# Debugging Steps (Guidelines)

**Purpose of this section:** Guide support personnel on **where to check** and **what to check** to identify which workflows are long-running and whether they are expected or stuck.

## 1. Acknowledge the Alert

- **Action:** Acknowledge the alert in the monitoring system to prevent repeated notifications.
- **Record:** Note the time of acknowledgment and the alert payload (workflow type, if shown).
- **Initial Assessment:** A long-running workflow may be expected (e.g., large pool create, volume replication) or may indicate a stuck workflow requiring investigation.

## 2. Gather Initial Context

- **Review Alert Details:**
  - Check the alert description and which **workflow type** (metric label `workflowType`) exceeded the threshold.
  - Note the **task queue** filter (customer-related queues).
  - Check the metric in GCP Monitoring for `custom.googleapis.com/workflow_duration` — confirm the 99th percentile (or max) value and time range.
- **Check Recent Activity:**
  - Review whether recent deployments or high load could explain long-running workflows.
  - Check if the workflow type is known to run for a long time (e.g., pool create, volume replication, backup).
- **Identify Scope:**
  - Determine if this affects a specific GCP project, region, or cluster.
  - Note the monitoring project and the VCP (control plane) project if different.

## 3. Validate the Alert

- **Confirm Legitimacy:**
  - In GCP Metrics Explorer, query `custom.googleapis.com/workflow_duration` with filter `metric.labels.taskqueue = monitoring.regex.full_match(".*customer.*")` and resource type `k8s_cluster`.
  - Verify that the 99th percentile (or max) over the alert window exceeds 86400 seconds (24 hours) for at least one workflow type.
- **Understand the Metric:**
  - This metric records **duration of completed** workflow runs. A fire means "we observed at least one completion that took > 24h," not necessarily "there is a workflow currently running > 24h." To find **currently** long-running workflows, use the database or Temporal visibility (Section 4 and below).

## 4. Identify the Root Cause

### A. Metrics Review

#### Workflow Duration Metric

- **Metric:** `custom.googleapis.com/workflow_duration`
- **What to Check:**
  - Which **workflow type** (e.g., `CreatePoolWorkflow`, `CreateVolumeWorkflow`, `CreateVolumeReplicationWorkflow`) has duration > 24h.
  - Whether the high duration is sustained or a one-off (single long run vs many).
  - Group by `metric.label.workflowType` to see which workflow types are contributing.
- **Labels:** `workflowType`, `taskqueue`, and any other labels available in your ingestion.

#### Related Metrics

- **Visibility / request metrics:** If available, check `visibility_persistence_requests` or similar to correlate with workflow start/complete volume.
- **Worker health:** Worker task slots used/available, poller counts, to rule out worker capacity issues causing delays.

### B. Identify Currently Running Workflows (> 24h)

The alert is based on **completed** workflow duration. To find workflows **currently** running longer than 24 hours:

#### Database Query (VCP jobs table)

- **Location:** VCP database — `jobs` table.
- **Query:** Jobs in running state (e.g. `NEW`, `PROCESSING`) that started more than 24 hours ago:

```sql
SELECT uuid, workflow_id, type, state, created_at, updated_at, correlation_id, resource_name
FROM jobs
WHERE state IN ('NEW', 'PROCESSING')
  AND created_at < NOW() - INTERVAL '24 hours'
  AND deleted_at IS NULL
ORDER BY created_at ASC;
```

- Use `workflow_id` with Temporal CLI (tctl) to describe or show history (see "Accessing Temporal Workflow Information" below).

#### Temporal Visibility (List running workflows)

- Use tctl to list workflows that are still running and started more than 24 hours ago. See **Accessing Temporal Workflow Information** below for commands.

### C. Logs Analysis

- **Location:** Worker and core-api logs in GCP Logging.
- **What to Check:**
  - Logs for the **correlation_id** or **workflow_id** of the long-running job(s) found in the database.
  - Activity heartbeat logs, activity timeouts, or errors that might explain why a workflow is still running (e.g., repeated retries, external dependency stuck).
- **Key Fields:** `correlation_id`, `workflow_id`, `job_id`, `message`, `severity`, `error`.

### D. Dependency Check

- **External systems:** Pool create, volume create, replication, and backup workflows depend on GCP, ONTAP, and other services. Check for:
  - GCP operation timeouts or long-running operations.
  - ONTAP or storage backend slowness or errors.
- **Temporal:** Ensure Temporal cluster and worker are healthy; task queue backlog or worker capacity can delay workflow progress but usually do not alone cause 24h+ runs without other factors.

### E. Configuration Review

- **Workflow timeouts:** Refer to workflow docs (e.g. `doc/workflows/core/pool-workflows.md`, `doc/workflows/core/volume-workflows.md`) for expected timeouts (e.g. activity timeouts, workflow run timeout). Some workflows are designed to run for hours (e.g. pool create, replication).
- **Environment:** Confirm you are looking at the correct GCP project and cluster for the alert.

---

## 5. Formulate a Hypothesis

Based on the gathered information, classify the situation:

### Common Scenarios

1. **Expected long-running workflow**
   - Workflow type is known to run for many hours (e.g., large pool create, volume replication, long backup). No action required other than documenting and optionally notifying stakeholders if desired.

2. **Single stuck or slow workflow**
   - One or a few workflow runs took > 24h due to a one-off issue (e.g., external dependency slow, retries). Identify the specific run(s) and correlation_id; decide with SME whether to let it complete or to cancel per process.

3. **Recurring long duration for a workflow type**
   - A particular workflow type consistently shows duration > 24h. May indicate design issue, timeout misconfiguration, or recurring dependency failure. Escalate to SME for analysis and potential design/code/config change.

4. **Worker or infrastructure issue**
   - Worker capacity, Temporal backlog, or infrastructure problems delay workflow progress. Address worker/infra and re-check metrics.

---

## 6. Implement a Solution / Mitigation

**⚠️ Process Adherence Required**

**Do NOT cancel or terminate workflows without:**
1. Confirming the workflow type and run (workflow_id / correlation_id).
2. Checking with SME or business whether the run can be safely cancelled.
3. Following change management (ticket, approval, documentation).
4. Using the correct procedure (e.g., Temporal cancel/terminate or API if applicable) as per your org’s runbooks.

### A. No Action (Expected Long-Running)

- If the workflow type is known to run > 24h and the run is progressing (e.g., heartbeats, activity logs), document the finding and close the alert. Optionally add a dashboard or documentation note to reduce future alert noise.

### B. Investigate and Monitor

- For a single long-running run: add the workflow_id/correlation_id to a ticket, monitor logs and Temporal status. If it completes or fails, document the outcome. If it remains stuck beyond a reasonable window, escalate per Section 6.C.

### C. Escalate for Cancellation or Deeper Fix

- **When:** Workflow is confirmed stuck, or business requests cancellation.
- **Steps:**
  1. Open a ticket with: workflow_id, workflow type, correlation_id, job UUID, created_at, and summary of findings.
  2. Get approval from SME/Incident manager for cancel/terminate (if applicable).
  3. Use Temporal CLI (see below) to cancel/terminate only after approval.
  4. Update job state in VCP if required by your procedures (e.g., via API or DB update per runbook).
  5. Document actions and outcome in the ticket.

### D. Permanent Fix (Later)

- The proper long-term solution is the **custom metric for jobs running > 24h** (scheduled workflow + activity + DB query + gauge) as discussed in design. Use this runbook for immediate response until that metric and alerts are in place.

---

## 7. Verify the Fix

- **If no action taken:** Confirm in metrics that the alert auto-closes when the condition no longer holds; document that the run was expected.
- **If a workflow was cancelled:** Verify in Temporal that the workflow is in Cancelled/Terminated state; verify job state in VCP if applicable; confirm alert closes and ticket is updated.

---

## 8. Document the Resolution

- Record in the ticket: root cause (workflow type, expected vs stuck), steps taken, and any follow-up (e.g., design change for timeouts, or enabling the dedicated long-running-jobs metric).

---

# Accessing Temporal Workflow Information

**Prerequisites:**
- `tctl` CLI tool available locally or accessible from admintools pod
- Access to the Temporal namespace for the VCP environment
- Workflow ID (same as job UUID from database, found in `jobs` table `workflow_id` column)

**Common Commands:**

1. **Describe workflow status:**
   ```bash
   tctl --namespace <vcp-namespace> workflow describe --workflow-id <workflow_id>
   ```
   - Shows workflow status, execution time, and basic information
   - Use to check if workflow is running, completed, timed out, or failed

2. **Show workflow history:**
   ```bash
   tctl --namespace <vcp-namespace> workflow show --workflow-id <workflow_id>
   ```
   - Shows detailed execution history with all activities
   - Use to see which activity is running or last completed, and for errors

3. **List running workflows:**
   ```bash
   tctl --namespace <vcp-namespace> workflow list --query 'ExecutionStatus=Running'
   ```
   - Lists currently running workflows
   - Use to identify workflows that may be running > 24h (cross-check start time with `workflow describe` or DB `created_at`)

4. **Filter by workflow type:**
   ```bash
   tctl --namespace <vcp-namespace> workflow list --query 'ExecutionStatus=Running AND WorkflowType="CreatePoolWorkflow"'
   ```
   - Replace `CreatePoolWorkflow` with the type from the alert or DB.

**Using from admintools pod (for restricted clusters):**
```bash
kubectl -n <ops-namespace> exec -it deploy/admintools -- /bin/sh -c "tctl --namespace <vcp-namespace> workflow describe --workflow-id <workflow_id>"
```

**Finding workflow IDs from database (jobs running > 24h):**
```sql
SELECT uuid, workflow_id, type, state, created_at, updated_at
FROM jobs
WHERE state IN ('NEW', 'PROCESSING')
  AND created_at < NOW() - INTERVAL '24 hours'
ORDER BY created_at ASC;
```
- Use the `workflow_id` column value with tctl commands above.

**Reference:** See `doc/guides/temporal-debugging.md` for more tctl commands and troubleshooting patterns.

---

# Operational Readiness Checklist

- [ ] Alert condition understood (workflow_duration 99th percentile > 24h for customer task queues)
- [ ] Workflow type(s) exceeding threshold identified from metric labels
- [ ] Database queried for jobs in NEW/PROCESSING with `created_at` > 24h ago
- [ ] Temporal used to list/describe running workflows where applicable
- [ ] Logs reviewed for correlation_id / workflow_id of long-running runs
- [ ] Decision made: expected vs stuck; no action vs escalate/cancel per process
- [ ] Any cancellation or terminate done only after approval and documented in ticket

---

# Useful Tools and Resources

- **Monitoring:** GCP Monitoring — metric `custom.googleapis.com/workflow_duration`, filter by `taskqueue` and group by `workflowType`
- **Logging:** GCP Logging — worker and core-api logs; filter by `correlation_id`, `workflow_id`, or `job_id`
- **Database:** VCP database — `jobs` table for job state, `workflow_id`, `type`, `created_at`
- **Temporal CLI:** `doc/guides/temporal-debugging.md` — describe, show, list workflows; cancel/terminate only per process
- **Workflow docs:** `doc/workflows/` — pool, volume, replication, backup workflows and timeouts
- **Error taxonomy:** `doc/api/error-taxonomy.md`, `core/errors/README.md`
