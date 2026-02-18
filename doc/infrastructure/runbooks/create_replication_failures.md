# Runbook for create_replication_failures

This runbook provides a structured approach to **identifying, investigating, and diagnosing** volume replication creation failures in the VSA Control Plane system.

## ⚠️ Important: Process Adherence Required

**This document is a diagnostic and investigation guide, NOT an implementation manual.**

- **Purpose:** Guide support personnel on **where to check**, **what to check**, and **how to identify** root causes
- **Do NOT:** Implement fixes, workarounds, or changes without following proper change management processes
- **Always:** Follow the change management process outlined in Section 6.D before taking any remediation actions
- **Escalate:** When in doubt, escalate to SME or Incident manager rather than implementing changes directly

All fixes, workarounds, and configuration changes **MUST** go through proper change management (tickets, approvals, testing, deployment).

# Alert Information

| Field               | Description                                                                                   |
| :-----------------: | :------------------------------------------------------------------------------------------: |
| Alert Name          | create_replication_failures                                                                     |
| Alert Link          | [GCP Monitoring Alert](https://console.cloud.google.com/monitoring/alerting/policies/[POLICY_ID]?project=vsa-monitoring-prod) |
| Alert Threshold     | Above 1                                                                                        |
| Date of Creation    | [Date]                                                                                         |
| SME                 | The Subject Matter Expert responsible for this alert or system.                                |
| Severity            | Error                                                                                          |

# Debugging Steps (Guidelines)

**Purpose of this section:** Guide support personnel on **where to check** and **what to check** to identify root causes. This is an **investigation and diagnostic guide**, not an implementation manual.

## 1. Acknowledge the Alert
- **Action:** Acknowledge the alert in the monitoring system to prevent repeated notifications.
- **Record:** Note the time of acknowledgment for future reference.
- **Initial Assessment:** Check if this is a single failure or a pattern (multiple replications failing).

## 2. Gather Initial Context
- **Review Alert Details:**
  - Check the alert description, severity, and any associated dashboards or logs.
  - Identify the affected replication name, project number, location, and operation/job ID.
  - Note the correlation ID and request ID for log correlation.
- **Check Recent Changes:**
  - Review deployment logs, configuration changes, or infrastructure updates.
  - Check if any recent changes to replication validation logic or ONTAP cluster configuration.
  - Verify if environment variables for replication timeouts were modified.
- **Identify Scope:**
  - Determine if this affects a specific region, project, or is system-wide.
  - Check if other replications are being created successfully.

## 3. Validate the Alert
- **Confirm Legitimacy:**
  - Verify the replication is actually in error state (not a false positive).
  - Check the API response, DB state, and workflow/job status.
  - Confirm the job type is `CREATE_VOLUME_REPLICATION`.
- **Verify Error Type:**
  - Check if it's a client-side error (400/409) or server-side error (500/503).
  - Review the error message and tracking ID from the job.

## 4. Identify the Root Cause

### A. Logs Analysis

#### API Layer Logs
- **Location:** `google-proxy` service logs
- **What to Check:**
  - Validation errors for replication parameters (sourceVolumeUUID, destinationVolumeUUID, replicationSchedule).
  - Error messages indicating which parameter failed validation.
  - HTTP status codes (400 for validation, 500 for internal errors).
- **Key Fields:** `correlation_id`, `request_id`, `job_type`, `error_details`

#### Orchestrator/Workflow Logs
- **Location:** `worker` service logs, Temporal workflow logs
- **What to Check:**
  - Workflow execution errors and activity failures.
  - Replication validation errors.
  - ONTAP replication API errors (key access, permissions, reachability).
  - SnapMirror relationship creation errors.
  - Timeout errors (replication uses replication activity timeout).
- **Key Fields:** `workflow_id`, `activity_type`, `error`, `tracking_id`

#### Database Logs
- **Location:** Database connection and query logs
- **What to Check:**
  - Replication creation record in database.
  - Job state transitions (NEW → PROCESSING → ERROR).
  - Unique constraint violations (duplicate replication names/resource IDs).
  - Transaction failures or deadlocks.

#### ONTAP Cluster Logs
- **Location:** ONTAP cluster logs
- **What to Check:**
  - SnapMirror relationship operations.
  - SnapMirror policy operations.
  - Volume replication operations.
  - ONTAP cluster availability and connectivity.

### B. Metrics Review

#### System Health Metrics
- **API Error Rates:** Check for spikes in 400/500 errors around alert time.
- **Workflow Success Rates:** Compare replication creation success rates.
- **Database Health:** Connection pool usage, query latency, transaction failures.
- **Temporal Metrics:** Workflow execution times, activity durations, retry counts.

#### Resource Utilization
- **Replication Quotas:** Check ONTAP cluster replication limits and SnapMirror relationship quotas.
- **Volume Limits:** Check maximum replication relationships per volume.
- **Worker Capacity:** Verify workers are available and not overloaded.

### C. System Health Check

#### Database Connectivity
- Verify DB connectivity from orchestrator and worker services.
- Check for connection pool exhaustion.
- Review DB migration status (ensure all migrations applied).

#### Temporal Infrastructure
- **Worker Status:** Ensure Temporal workers are running and healthy.
  - **Check via Metrics:** Review Temporal worker metrics in monitoring project (worker health, task processing rate).
  - **Check via CLI:** Use `tctl --namespace <vcp-namespace> workflow list --limit 10` to verify workers are processing tasks.
- **Workflow Registration:** Verify `CreateVolumeReplicationWorkflow` is registered.
  - **Check via CLI:** Use `tctl --namespace <vcp-namespace> workflow describe --workflow-id <workflow_id>` to verify workflow type.
- **Task Queue:** Check `CustomerTaskQueue` is processing tasks.
  - **Check via Metrics:** Review task queue depth and processing rate metrics in monitoring project.
  - **Check via CLI:** Use `tctl --namespace <vcp-namespace> task-queue describe --task-queue CustomerTaskQueue` to check queue status.
- **Workflow Status:** Review workflow execution status for the failed job.
  - **Check via Metrics:** Review workflow execution metrics (duration, success rate, error rate) in monitoring project.
  - **Check via CLI:** Use `tctl --namespace <vcp-namespace> workflow describe --workflow-id <workflow_id>` to get workflow status and history.

#### ONTAP Cluster Availability
- **ONTAP Cluster:** Verify ONTAP cluster is accessible and healthy.
- **Source Volume:** Verify source volume exists and is in READY state.
- **Destination Volume:** Verify destination volume exists and is in READY state.
- **Network:** Verify network connectivity between source and destination clusters.

### D. Dependency Check

#### Replication Requirements
- **Source Volume:** Must exist and be in READY state.
- **Destination Volume:** Must exist and be in READY state.
- **Different Regions:** Source and destination volumes must be in different regions.
- **Replication Enabled:** Both volumes must have replication enabled.
- **SnapMirror Policy:** Valid SnapMirror policy must be configured.

#### Volume Requirements
- **Source Volume State:** Must be READY and not in use by another replication.
- **Destination Volume State:** Must be READY and available for replication.
- **Network Connectivity:** Source and destination clusters must have network connectivity.
- **SnapMirror Relationship:** No existing SnapMirror relationship between the volumes.

### E. Configuration Review

#### Replication Specific Parameters

**Source Volume UUID Validation:**
- **Format:** Must be a valid volume UUID in READY state
- **Error Message:** "Invalid source volume UUID" or "Source volume not found"

**Destination Volume UUID Validation:**
- **Format:** Must be a valid volume UUID in READY state, different region from source
- **Error Message:** "Invalid destination volume UUID" or "Destination volume not found"

**Replication Schedule Validation:**
- **Format:** Must be a valid replication schedule (if specified)
- **Error Message:** "Invalid replication schedule"

**Replication Relationship Validation:**
- **Format:** Source and destination volumes must be in different regions
- **Error Message:** "Replication relationship already exists" or "Volumes must be in different regions"

#### Environment Variables

**Replication Workflow Timeouts:**
- `StartToCloseTimeoutForReplicationActivities` (default: varies by activity) - **Activity timeout for replication workflows**
- `REPLICATION_JOBS_RETRY_MAX_ATTEMPTS` (default: 10) - **Maximum retry attempts for replication jobs**
- `CVP_JOB_POLL_TIMEOUT_MIN` (default: 10 minutes)
- `CVP_JOB_POLL_INTERVAL_SEC` (default: 30 seconds)

## 5. Formulate a Hypothesis

Based on the gathered information, develop a hypothesis about the root cause:

### Common Root Causes for Replication Failures

1. **Validation Errors (400):**
   - Invalid source or destination volume UUID
   - Invalid replication schedule
   - Source and destination volumes in same region
   - Replication schedule not specified

2. **Resource Constraints (409/422):**
   - Duplicate replication relationship already exists
   - Source volume not found or not in READY state
   - Destination volume not found or not in READY state
   - Maximum replication relationships per volume exceeded

3. **Configuration Errors (400/422):**
   - ONTAP cluster not accessible
   - Replication not enabled on volumes
   - Network connectivity issues between clusters
   - SnapMirror policy not configured

4. **Infrastructure Failures (500/503):**
   - Database connectivity issues
   - Temporal worker unavailable
   - ONTAP cluster outages or rate limiting
   - SnapMirror relationship creation failures

5. **Timeout Errors:**
   - Workflow timeout (replication activity timeout exceeded)
   - Activity timeout (heartbeat timeout exceeded)
   - ONTAP replication operation timeout

## 6. Implement a Solution/Mitigation

**⚠️ CRITICAL: Process Adherence Required**

**This section describes potential solutions and mitigation strategies for reference. DO NOT implement any fixes or workarounds without:**
1. Creating a bug ticket first (see Section 6.D)
2. Obtaining proper approvals (SME, Managers)
3. Following change management process (tickets, approvals, testing, deployment)
4. Documenting all actions in the bug ticket

**First, determine if this is a **client-side error** (400/409) or **server-side error** (500/503) to apply the appropriate resolution strategy.**

### A. Client-Side Errors (400/409) - Customer Action Required

**Characteristics:**
- HTTP status codes: 400 (Bad Request), 409 (Conflict), 422 (Unprocessable Entity)
- Validation errors or parameter issues
- Error message indicates invalid input parameters
- Job may not be created (validation fails before job creation)

**Action Required:** **Inform the customer** - Create a google support case for customer communication.

#### A.1. Validation Errors (400) - Inform Customer

**Common Validation Errors:**
- "Invalid source or destination volume UUID"
- "Invalid replication schedule"
- "Replication schedule not specified: [parameter]"
- "Destination volume not found"
- "Source volume not found"

**Steps to Inform Customer:**
1. **Identify the Specific Error:**
   - Extract the exact error message from logs/API response.
   - Identify which parameter(s) failed validation.
   - Note the tracking ID and correlation ID for reference.

2. **Prepare Customer Communication:**
   - **Template Message (Customer-Facing):**
     ```
     Your volume replication creation request failed due to invalid parameters.
     
     Error: [Exact error message]
     Operation ID: [Operation ID] (if available - use this to check operation status)
     Correlation ID: [Correlation ID] (if you provided x-correlation-id header)
     
     Required Corrections:
     - [Specific parameter] must be [correct value/format]
     - [Additional corrections if multiple parameters]
     
     Please review the volume replication requirements and resubmit with corrected parameters.
     Reference: [Link to API documentation or requirements]
     ```
   
   - **Internal Reference (for support case notes, not sent to customer):**
     - Tracking ID: [Tracking ID] (internal error code)
     - Job UUID: [Job UUID] (if job was created)
     - Workflow ID: [Workflow ID] (if workflow was started)

3. **Provide Correct Parameter Values:**
   - **Source Volume UUID:** Must be a valid volume UUID in READY state
   - **Destination Volume UUID:** Must be a valid volume UUID in READY state, in a different region from source
   - **Replication Schedule:** Must be a valid replication schedule (if specified)
   - **Volumes:** Both source and destination volumes must exist and be in READY state

4. **Document the Interaction:**
   - Record customer contact details and communication.
   - Note the specific parameters that were incorrect.
   - Track if this is a recurring issue (may indicate documentation gaps).

#### A.2. Conflict Errors (409) - Inform Customer

**Common Conflict Errors:**
- "Replication relationship already exists between these volumes"
- Duplicate replication relationship conflicts

**Steps to Inform Customer:**
1. **Check Replication State:**
   - If replication exists in `CREATING` state: Inform customer they can query the operation status.
   - If replication exists in other state: Inform customer that a replication relationship already exists between these volumes.

2. **Customer Communication:**
   - **If Replication in CREATING:**
     ```
     A replication relationship between these volumes is currently being created.
     Operation ID: [operation_id]
     You can query this operation to check status or wait for completion.
     ```
   - **If Replication Exists:**
     ```
     A replication relationship already exists between the source and destination volumes.
     Please use different volumes or delete the existing replication relationship first.
     ```

3. **No Internal Action Required:**
   - Do not modify customer data.
   - Do not delete existing replications.
   - Customer must resolve the conflict.

### B. Server-Side Errors (500/503) - Internal Action Required

**Characteristics:**
- HTTP status codes: 500 (Internal Server Error), 503 (Service Unavailable)
- Database errors, workflow failures, infrastructure issues
- System-level problems requiring internal fixes
- Job may be created but workflow fails

**Action Required:** **Internal team action** - We can apply fixes, workarounds, and code changes **ONLY after following proper change management process (Section 6.D)**.

#### B.1. Temporary Mitigation (Require Change Management)

**⚠️ MANDATORY: Change Management Process Required**

**DO NOT implement any workarounds without following the change management process in Section 6.D.**

**This section describes potential workarounds for reference only. All workarounds MUST:**
1. Be tracked in a bug ticket (created first - see Section 6.D, Step 1)
2. Have approval from SME and Managers (documented in ticket)
3. Follow proper change management process (tickets, approvals, testing, deployment)
4. Be documented with rationale and impact assessment

**If you are unsure about any workaround, escalate to SME or team lead.**

##### For Database Errors (500)
- **Action:** Escalate to database team/SME - Database is a managed service shared by multiple applications.
- **Steps:**
  1. **Verify the Issue:**
     - Check DB connectivity from orchestrator and worker services.
     - Verify if error is specific to replication creation or system-wide.
     - Check application logs for database connection errors.
  2. **Gather Information for Escalation:**
     - Error message and tracking ID
     - Correlation ID and workflow ID
     - Timestamp and duration of issue
     - Affected services (orchestrator, worker, or both)
     - Check if other applications are also affected
  3. **Escalate to Database Team:**
     - Create ticket with full error details and context
     - Include logs showing database connection/query failures
     - Note if this is a transient issue or persistent
     - Do not attempt database-level workarounds (connection pool changes, migrations, restarts) as these affect all applications
  4. **Monitor and Coordinate:**
     - Wait for database team resolution
     - Monitor for recovery
     - Retry replication creation after database team confirms fix

##### For Workflow/Temporal Errors (500)
- **Action:** Restore workflow execution capability.
- **Steps:**
  1. **Verify Temporal workers are running and healthy:**
     - **Check Metrics:** Review worker health metrics in monitoring project (worker count, heartbeat status).
     - **Check CLI:** Use `tctl --namespace <vcp-namespace> workflow list --limit 10` to verify workers are processing.
  2. **Check if `CreateVolumeReplicationWorkflow` is registered:**
     - **Check CLI:** Use `tctl --namespace <vcp-namespace> workflow describe --workflow-id <workflow_id>` to verify workflow type.
  3. **Verify task queue is processing tasks:**
     - **Check Metrics:** Review `CustomerTaskQueue` metrics in monitoring project (queue depth, processing rate).
     - **Check CLI:** Use `tctl --namespace <vcp-namespace> task-queue describe --task-queue CustomerTaskQueue`.
  4. **Review workflow execution status:**
     - **Check Metrics:** Review workflow execution metrics in monitoring project (duration, status, error details).
     - **Check CLI:** Use `tctl --namespace <vcp-namespace> workflow describe --workflow-id <workflow_id>` to get full workflow history.
- **Workaround (if approved):**
  - Restart Temporal workers if they're stuck (check worker metrics first).
  - Manually retry failed workflows via CLI: `tctl --namespace <vcp-namespace> workflow signal --workflow-id <id> --signal-name retry` (if safe).
  - Temporarily increase worker capacity if metrics show overload (queue depth increasing, processing rate decreasing).

##### For ONTAP/Replication Errors (500/503)
- **Action:** Resolve ONTAP cluster or replication issues.
- **Steps:**
  1. Check ONTAP cluster status and availability.
  2. Verify source and destination volumes exist and are accessible.
  3. Check SnapMirror policy configuration.
  4. Verify network connectivity between source and destination clusters.
  5. Check for ONTAP cluster quota/rate limit errors.
- **Workaround (if approved):**
  - Retry replication operations if transient failures.
  - Verify and fix SnapMirror policy configuration.
  - Check ONTAP cluster quotas and request increases if needed.

##### For Timeout Errors
- **Action:** Determine if timeout is legitimate or indicates a stuck operation.
- **Steps:**
  1. Check ONTAP cluster for ongoing SnapMirror operations.
  2. **Verify workflow status:**
     - **Check Metrics:** Review workflow execution duration metrics in monitoring project to see if it's progressing.
     - **Check CLI:** Use `tctl --namespace <vcp-namespace> workflow describe --workflow-id <workflow_id>` to check workflow status and last activity time.
  3. **Check if operation is progressing:**
     - **Check Metrics:** Review activity heartbeat metrics in monitoring project (should see regular heartbeats if progressing).
     - **Check CLI:** Use `tctl --namespace <vcp-namespace> workflow show --workflow-id <workflow_id>` to see activity execution history and heartbeats.
  4. Determine if replication activity timeout is sufficient for the operation (compare actual duration from metrics vs timeout).
- **Workaround (if approved):**
  - If operation is stuck (no heartbeats, no progress in metrics): Cancel via CLI `tctl --namespace <vcp-namespace> workflow cancel --workflow-id <id>` and retry (after cleanup).
  - If operation is slow but progressing (heartbeats present, metrics show progress): Increase timeout temporarily.
  - If recurring: Investigate why operations take longer than expected (review metrics for patterns).

#### B.2. Permanent Fixes (Require Change Management)

**⚠️ MANDATORY: Change Management Process Required**

**DO NOT implement any permanent fixes without following the change management process in Section 6.D.**

**This section describes potential permanent fixes for reference only. All fixes MUST:**
1. Be tracked in a bug ticket (created first - see Section 6.D, Step 1)
2. Go through proper planning, design, and approval process
3. Follow change management process (tickets, approvals, testing, deployment)
4. Be properly tested and reviewed before deployment

##### Configuration Updates
- **Environment Variables:**
  - Update replication timeout values if operations legitimately need more time.
  - Update `StartToCloseTimeoutForReplicationActivities` if needed.
  - Update via deployment process with proper testing.

##### Infrastructure Scaling
- **Worker Capacity:**
  - Scale Temporal workers if consistently overloaded.
  - Update worker deployment configuration.
  - Monitor worker performance after scaling.
- **Database Scaling:**
  - Scale database if connection pool or performance is an issue.
  - Coordinate with DB team for scaling operations.
  - Update connection pool configuration.

##### Code/Workflow Fixes
- **Bug Fixes:**
  1. **Raise Bug Ticket:**
     - Create bug ticket with full details (error, logs, reproduction steps).
     - Assign to appropriate team (orchestrator, workflow, replication).
     - Set priority based on impact (P0 for production outages, P1 for frequent failures).
  2. **Code Changes:**
     - Fix bugs in validation logic, workflow orchestration, or activity implementations.
     - Add proper error handling and logging.
     - Write unit and integration tests.
  3. **Testing:**
     - Test fixes in development/staging environment.
     - Verify fix resolves the issue without introducing regressions.
  4. **Deployment:**
     - Deploy via standard CI/CD process.
     - Monitor after deployment to verify fix.
- **Timeout Adjustments:**
  - Increase timeouts if operations legitimately take longer.
  - Update `StartToCloseTimeoutForReplicationActivities` if needed.
  - Document timeout changes and rationale.
- **Error Handling Improvements:**
  - Improve error messages to guide users to correct parameters.
  - Add more detailed logging for debugging.
  - Enhance error taxonomy and tracking.

##### Documentation Updates
- **API Documentation:**
  - Update API docs if parameter validation errors are common.
  - Add examples of correct replication creation requests.
  - Document common pitfalls and how to avoid them.
- **Runbook Updates:**
  - Add new troubleshooting steps discovered.
  - Update common symptoms table with new patterns.
  - Document workarounds that were effective.

### C. Error Classification Decision Tree

```
Is HTTP status 400, 409, or 422?
├─ YES → Client-Side Error
│   ├─ Validation error (400)?
│   │   └─ Inform customer to correct parameters
│   ├─ Conflict error (409)?
│   │   └─ Inform customer about duplicate resource
│   └─ Quota error (422)?
│       ├─ Customer GCP quota?
│       │   └─ Inform customer to increase quotas
│       └─ System quota?
│           └─ Treat as server-side error
│
└─ NO (500, 503, or other) → Server-Side Error
    ├─ Database error?
    │   └─ Escalate to database team
    ├─ Workflow/Temporal error?
    │   └─ Apply workflow workaround or fix
    ├─ ONTAP/Replication error?
    │   └─ Check ONTAP cluster, SnapMirror policy, network
    └─ Unknown error?
        └─ Investigate and classify, then apply appropriate fix
```

### D. Change Management Process for Server-Side Fixes

**⚠️ THIS IS THE MANDATORY PROCESS - Follow this for ALL fixes and workarounds**

**Before implementing ANY fix or workaround described in Sections 6.B.1 or 6.B.2, you MUST follow this process.**

**Step 1: Create Bug Ticket**
1. **Create Ticket:** Raise a bug ticket against the issue with full details:
   - Error details, logs, correlation ID, tracking ID
   - Root cause analysis
   - Impact assessment
   - Affected customers/operations

**Step 2: Decide on Workaround (Based on Bug Ticket)**
2. **Assess Need for Workaround:**
   - Review bug ticket to determine if immediate workaround is needed
   - Consider: severity, customer impact, availability of permanent fix timeline
   - Determine if workaround is safe and won't cause data loss
3. **If Workaround Needed:**
   - **Get Approval:** Obtain approval from SME and Managers (document in bug ticket)
   - **Document in Ticket:** Record workaround applied, time, rationale, and steps taken
   - **Apply Workaround:** Execute approved workaround following change management process
   - **Monitor:** Watch for resolution and any side effects (update ticket with findings)
4. **If No Workaround Needed:**
   - Proceed directly to permanent fix planning

**Step 3: Permanent Fix (Tracked in Bug Ticket)**
5. **Plan:** Design solution, estimate effort, get approval (document in bug ticket)
6. **Implement:** Code changes, tests, documentation updates
7. **Test:** Unit tests, integration tests, staging validation
8. **Review:** Code review, architecture review if needed
9. **Deploy:** Deploy via CI/CD with proper monitoring
10. **Verify:** Confirm fix resolves the issue in production
11. **Close:** Close ticket and update documentation

## 7. Verify the Fix

- **Monitor System:**
  - Verify the alert clears and no new failures occur.
  - Check that the replication transitions to `READY` or `IN_USE` state.
  - Monitor subsequent replication creations for success.
- **Run Tests:**
  - Execute replication creation tests with various configurations.
  - Verify edge cases (different volume combinations, regions, replication schedules).
- **Check Logs and Metrics:**
  - Confirm absence of related errors in logs.
  - **Verify successful workflow completion:**
    - **Check Metrics:** Review workflow completion metrics in monitoring project (success rate, duration).
    - **Check CLI:** Use `tctl --namespace <vcp-namespace> workflow describe --workflow-id <workflow_id>` to verify workflow status is COMPLETED.

## 8. Document the Resolution

- **Record Details:**
  - Document the root cause, resolution steps, and any temporary mitigations applied.
  - Note the specific parameter values that caused the failure.
  - Record any configuration changes made.
- **Update Runbook:**
  - Add new insights or steps discovered during troubleshooting.
  - Update common symptoms table with new patterns.
- **Share Learnings:**
  - Communicate findings with the team to prevent recurrence.
  - Update API documentation if parameter validation errors were common.

---

## Quick Reference: Common Symptoms & Fixes

| Symptom | Likely Cause | Mitigation |
|---------|--------------|------------|
| 400: "Invalid source or destination volume UUID" | Invalid volume UUID | Verify volume UUIDs are correct and volumes exist |
| 400: "Invalid replication schedule" | Invalid schedule format | Use valid replication schedule format |
| 400: "Source volume not found" | Volume doesn't exist | Verify source volume exists and is in READY state |
| 400: "Destination volume not found" | Volume doesn't exist | Verify destination volume exists and is in READY state |
| 400: "Volumes must be in different regions" | Same region | Source and destination volumes must be in different regions |
| 409: Replication relationship exists | Duplicate relationship | Delete existing replication or use different volumes |
| 500: DB errors | Connectivity, schema | Escalate to DB team |
| Workflow not starting | Temporal/worker | Check Temporal, worker logs |
| Workflow timeout | Operation taking too long | Check ONTAP cluster operations, consider increasing timeout |
| ONTAP cluster unavailable | Cluster outage | Check ONTAP cluster status, wait for recovery |
| SnapMirror policy errors | Policy not configured | Verify SnapMirror policy is configured correctly |
| Replication stuck in CREATING | Workflow failure | Inspect logs, retry or cleanup |

---

## Replication Specific Configuration Reference

### Replication Relationship Requirements
- **Source Volume:** Must exist and be in READY state
- **Destination Volume:** Must exist and be in READY state, in a different region
- **Network:** Source and destination clusters must have network connectivity
- **SnapMirror Policy:** Valid SnapMirror policy must be configured

### Timeout Configuration
- **Activity Timeout:** Configurable via `StartToCloseTimeoutForReplicationActivities` (varies by activity type)
- **Retry Attempts:** `REPLICATION_JOBS_RETRY_MAX_ATTEMPTS` (default: 10)

### ONTAP Cluster Requirements
- **ONTAP Cluster:** Must be accessible and healthy
- **Source Volume:** Must exist and be accessible
- **Destination Volume:** Must exist and be accessible
- **Network:** Must have connectivity between source and destination clusters

---

## Accessing Temporal Workflow Information

**Note:** Temporal Web UI is not directly exposed. Use the following methods to access workflow information:

### Method 1: Temporal CLI (tctl)

**Prerequisites:**
- `tctl` CLI tool available locally or accessible from admintools pod
- Access to the Temporal namespace
- Workflow ID (same as job UUID from database)

**Common Commands:**

1. **Describe Workflow Status:**
   ```bash
   tctl --namespace <vcp-namespace> workflow describe --workflow-id <workflow_id>
   ```
   - Shows workflow status, execution time, and basic information
   - Use this to check if workflow is running, completed, or failed

2. **Show Workflow History:**
   ```bash
   tctl --namespace <vcp-namespace> workflow show --workflow-id <workflow_id>
   ```
   - Shows detailed execution history with all activities
   - Use this to see which activity failed and why
   - Shows activity inputs, outputs, and errors

3. **List Workflows:**
   ```bash
   tctl --namespace <vcp-namespace> workflow list --query 'WorkflowType="CreateVolumeReplicationWorkflow"'
   ```
   - Lists workflows matching the query
   - Use this to find workflows by type or status

**Using from admintools pod (for restricted clusters):**
```bash
kubectl -n <ops-namespace> exec -it deploy/admintools -- /bin/sh -c "tctl --namespace <vcp-namespace> workflow describe --workflow-id <workflow_id>"
```

### Method 2: Monitoring Metrics (Recommended)

**Location:** Monitoring project (GCP Monitoring)

**Available Metrics:**
- **Workflow Execution Metrics:**
  - Workflow status distribution (RUNNING, COMPLETED, FAILED, TIMED_OUT)
  - Workflow execution duration
  - Workflow start/completion rates
  - Workflow error rates by type

- **Activity Execution Metrics:**
  - Activity execution status (SUCCESS, FAILED, TIMED_OUT)
  - Activity duration by type
  - Activity retry counts
  - Activity heartbeat status

**How to Access:**
1. Navigate to GCP Monitoring project
2. Use metric explorer or create custom dashboards
3. Filter by:
   - `workflow_type` = "CreateVolumeReplicationWorkflow"
   - `job_type` = "CREATE_VOLUME_REPLICATION"
   - `workflow_id` = "<workflow_id>"
   - `correlation_id` = "<correlation_id>"

### Method 3: Database Queries

**Job Status:**
```sql
SELECT uuid, state, error_details, tracking_id, workflow_id, created_at, updated_at 
FROM jobs 
WHERE uuid = '<job_uuid>' OR workflow_id = '<workflow_id>';
```

**Replication Status:**
```sql
SELECT uuid, state, state_details, source_volume_uuid, destination_volume_uuid, replication_schedule
FROM volume_replications
WHERE uuid = '<replication_uuid>';
```

---

## Operational Readiness Checklist

### Pre-Creation Verification
- [ ] Source volume UUID is valid and volume exists
- [ ] Destination volume UUID is valid and volume exists
- [ ] Source and destination volumes are in different regions
- [ ] Both volumes are in READY state
- [ ] No existing replication relationship between the volumes
- [ ] SnapMirror policy is configured

### Infrastructure Verification
- [ ] Temporal workflows and workers registered
- [ ] DB migrations applied and healthy
- [ ] ONTAP clusters accessible and healthy
- [ ] Network connectivity between source and destination clusters
- [ ] Replication enabled on both volumes

### Configuration Verification
- [ ] `StartToCloseTimeoutForReplicationActivities` is set appropriately
- [ ] `REPLICATION_JOBS_RETRY_MAX_ATTEMPTS` is configured correctly

---

**Tip:**  
For any error, always check the logs for the specific component (API, Orchestrator, Workflow, DB, ONTAP) and correlate with the operation/job ID and correlation ID for targeted debugging. Replication creation involves SnapMirror relationship setup between volumes in different regions and uses activity-specific timeouts configured via `StartToCloseTimeoutForReplicationActivities`.

---

# Useful Tools and Resources

* **Monitoring System:** [GCP Monitoring](https://console.cloud.google.com/monitoring)
  - Temporal workflow and activity metrics
  - Worker health and task queue metrics
  - Filter by `workflow_type`, `job_type`, `workflow_id`, or `correlation_id`
* **Logging Platform:** [GCP Logging](https://console.cloud.google.com/logs)
  - API logs (google-proxy service)
  - Orchestrator logs (core-api service)
  - Workflow/activity logs (worker service)
  - Filter by `correlation_id` or `workflow_id`
* **Temporal CLI (tctl):**
  - **Reference:** `doc/guides/temporal-debugging.md`
  - **Common Commands:**
    - `tctl --namespace <vcp-namespace> workflow describe --workflow-id <id>` - Get workflow status
    - `tctl --namespace <vcp-namespace> workflow show --workflow-id <id>` - Get detailed execution history
    - `tctl --namespace <vcp-namespace> workflow list --query 'WorkflowType="CreateVolumeReplicationWorkflow"'` - List workflows
  - **Access:** Via local `tctl` or admintools pod: `kubectl exec -it deploy/admintools -- tctl ...`
* **Temporal Metrics:**
  - Available in monitoring project
  - Workflow execution metrics (status, duration, error rates)
  - Activity execution metrics (status, duration, retry counts)
  - Worker health metrics (worker count, heartbeat status)
  - Task queue metrics (queue depth, processing rate)
* **Troubleshooting Guide:** https://confluence.ngage.netapp.com/spaces/VSCP/pages/1273328576/Pool+Volume+CRUD+Operations+Troubleshooting+Guide

