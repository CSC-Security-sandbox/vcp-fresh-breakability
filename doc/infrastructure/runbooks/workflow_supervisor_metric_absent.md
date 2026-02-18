# Runbook for workflow_supervisor_metric_absent

This runbook provides a structured approach to **identifying, investigating, and diagnosing** when the workflow supervisor task metric stops emitting, indicating the supervisor task is not running.

## ⚠️ Important: Process Adherence Required

**This document is a diagnostic and investigation guide, NOT an implementation manual.**

- **Purpose:** Guide support personnel on **where to check**, **what to check**, and **how to identify** root causes
- **Do NOT:** Implement fixes, workarounds, or changes without following proper change management processes
- **Always:** Follow the change management process outlined in Section 6.C before taking any remediation actions
- **Escalate:** When in doubt, escalate to SME or Incident manager rather than implementing changes directly

All fixes, workarounds, and configuration changes **MUST** go through proper change management (tickets, approvals, testing, deployment).

# Alert Information

| Field               | Description                                                                                   |
| :-----------------: | :------------------------------------------------------------------------------------------: |
| Alert Name          | workflow_supervisor_metric_absent                                                                     |
| Alert Link          | [GCP Monitoring Alert](https://console.cloud.google.com/monitoring/alerting/policies/[POLICY_ID]?project=vsa-monitoring-prod) |
| Alert Threshold     | Metric `vcp.background.task.last_run_timestamp` with tag `task=WORKFLOW_SUPERVISOR_SWEEP` has not been updated for 10 minutes |
| Date of Creation    | [Date]                                                                                         |
| SME                 | The Subject Matter Expert responsible for this alert or system.                                |
| Severity            | Critical                                                                                          |

# Debugging Steps (Guidelines)

**Purpose of this section:** Guide support personnel on **where to check** and **what to check** to identify root causes. This is an **investigation and diagnostic guide**, not an implementation manual.

## 1. Acknowledge the Alert
- **Action:** Acknowledge the alert in the monitoring system to prevent repeated notifications.
- **Record:** Note the time of acknowledgment for future reference.
- **Initial Assessment:** This is a critical alert - the workflow supervisor is not running, which means timed-out workflows are not being cleaned up.

## 2. Gather Initial Context
- **Review Alert Details:**
  - Check the alert description, severity, and any associated dashboards or logs.
  - Verify the metric `vcp.background.task.last_run_timestamp` with tag `task=WORKFLOW_SUPERVISOR_SWEEP` has not been updated.
  - Check when the last successful run occurred (from the metric value).
  - Note the expected run frequency: every 5 minutes.
- **Check Recent Changes:**
  - Review deployment logs, configuration changes, or infrastructure updates.
  - Check if any recent changes to core-api service or background task scheduler.
  - Verify if environment variables for supervisor were modified.
- **Identify Scope:**
  - Determine if this affects a specific region, environment, or is system-wide.
  - Check if other background tasks are running (e.g., `SYNC_VSA_CLUSTER_HEALTH_STATUS`).

## 3. Validate the Alert
- **Confirm Legitimacy:**
  - Verify the metric `vcp.background.task.last_run_timestamp` with tag `task=WORKFLOW_SUPERVISOR_SWEEP` has not been updated for at least 10 minutes.
  - Check the metric `vcp.background.task.runs` with tag `task=WORKFLOW_SUPERVISOR_SWEEP` to see if it's incrementing.
  - Verify the core-api service is running and healthy.
- **Verify Error Type:**
  - Check if it's a service availability issue (core-api down) or a scheduler issue (service running but supervisor not executing).

## 4. Identify the Root Cause

### A. Logs Analysis

#### Core-API Service Logs
- **Location:** `core-api` service logs
- **What to Check:**
  - Service startup logs - verify service started successfully.
  - Background task scheduler initialization logs - look for "Starting background task scheduler" or "Background task scheduler started successfully".
  - Supervisor task execution logs - look for "[WorkflowSupervisorTask] Starting workflow supervisor task".
  - Error logs related to supervisor task - look for "Failed to schedule Workflow Supervisor Task" or "Failed to acquire lock".
  - Service crash or restart logs.
- **Key Fields:** `correlation_id`, `jobType`, `task`, `error`

#### Background Task Scheduler Logs
- **Location:** `core-api` service logs
- **What to Check:**
  - Cron scheduler registration errors.
  - Lock acquisition failures.
  - Database connection errors when trying to create/update admin job spec.
  - Temporal client connection errors.

#### Database Logs
- **Location:** Database connection and query logs
- **What to Check:**
  - Core-api service database connectivity.
  - Admin job spec table queries (check if `WORKFLOW_SUPERVISOR_SWEEP` job spec exists).
  - Lock acquisition queries (check if lock is stuck).

### B. Metrics Review

#### Service Health Metrics
- **Core-API Service Availability:** Check if core-api service is running and healthy.
- **Background Task Metrics:** 
  - Check `vcp.background.task.runs` with tag `task=WORKFLOW_SUPERVISOR_SWEEP` - should increment every 5 minutes.
  - Check `vcp.background.task.last_run_timestamp` with tag `task=WORKFLOW_SUPERVISOR_SWEEP` - should update every 5 minutes.
  - Check `vcp.background.task.errors` with tag `task=WORKFLOW_SUPERVISOR_SWEEP` - check for errors with reasons: `schedule_registration`, `create_admin_job_spec`, `acquire_lock`, `load_job_spec`.
- **Other Background Tasks:** Check if other background tasks (e.g., `SYNC_VSA_CLUSTER_HEALTH_STATUS`) are running to determine if it's a general scheduler issue or supervisor-specific.

#### Resource Utilization
- **Core-API Pod Health:** Check pod status, restarts, resource usage (CPU, memory).
- **Database Connectivity:** Check database connection pool usage and latency.
- **Temporal Connectivity:** Check Temporal client connection status.

### C. System Health Check

#### Core-API Service Status
- **Pod Status:** Verify core-api pods are running and not in CrashLoopBackOff or Error state.
- **Service Availability:** Check if core-api service is responding to health checks.
- **Recent Restarts:** Check if core-api pods have restarted recently (may indicate crashes).

#### Database Connectivity
- Verify DB connectivity from core-api service.
- Check for connection pool exhaustion.
- Review DB migration status (ensure all migrations applied).

#### Temporal Infrastructure
- **Temporal Client:** Verify Temporal client is initialized and connected.
- **Temporal Availability:** Check if Temporal cluster is accessible from core-api service.

#### Background Task Scheduler
- **Scheduler Status:** Verify cron scheduler is running (check logs for "Background task scheduler started successfully").
- **Lock Status:** Check if admin job spec lock is stuck (query `admin_job_specs` table for `WORKFLOW_SUPERVISOR_SWEEP`).

### D. Dependency Check

#### Core-API Service Requirements
- **Service Running:** Core-api service must be running and healthy.
- **Database Access:** Core-api must have database connectivity to create/update admin job specs.
- **Temporal Access:** Core-api must have Temporal client connectivity.
- **Cron Scheduler:** Background task scheduler must be initialized and running.

#### Supervisor Task Requirements
- **Cron Schedule:** Supervisor runs every 5 minutes (`0 */5 * * * *`).
- **Lock Acquisition:** Supervisor uses database-backed locking with 300-second timeout.
- **Handlers Registered:** Supervisor handlers must be registered for cleanup operations.

### E. Configuration Review

#### Supervisor Task Configuration
- **Cron Expression:** `0 */5 * * * *` (every 5 minutes)
- **Lock Timeout:** 300 seconds (5 minutes)
- **Grace Period:** `WORKFLOW_SUPERVISOR_NOT_FOUND_GRACE_PERIOD` (default: 5 minutes)

#### Environment Variables
- **Database Configuration:** Verify database connection settings are correct.
- **Temporal Configuration:** Verify Temporal client configuration is correct.
- **Service Configuration:** Verify core-api service configuration is correct.

## 5. Formulate a Hypothesis

Based on the gathered information, develop a hypothesis about the root cause:

### Common Root Causes for Supervisor Metric Not Emitting

1. **Service Availability Issues:**
   - Core-api service is down or crashed
   - Core-api pods are in CrashLoopBackOff or Error state
   - Service is restarting repeatedly

2. **Scheduler Issues:**
   - Background task scheduler failed to start
   - Cron scheduler registration failed
   - Scheduler stopped or crashed

3. **Database Issues:**
   - Database connectivity problems
   - Admin job spec table issues
   - Lock acquisition failures

4. **Temporal Issues:**
   - Temporal client initialization failed
   - Temporal cluster connectivity issues

5. **Configuration Issues:**
   - Incorrect cron expression
   - Missing environment variables
   - Configuration errors preventing scheduler startup

## 6. Implement a Solution/Mitigation

**⚠️ CRITICAL: Process Adherence Required**

**This section describes potential solutions and mitigation strategies for reference. DO NOT implement any fixes or workarounds without:**
1. Creating a bug ticket first (see Section 6.C)
2. Obtaining proper approvals (SME, Managers)
3. Following change management process (tickets, approvals, testing, deployment)
4. Documenting all actions in the bug ticket

**This is a server-side issue requiring internal team action. We can apply fixes, workarounds, and code changes **ONLY after following proper change management process (Section 6.C)**.**

### A. Immediate Mitigation (Require Change Management)

**⚠️ MANDATORY: Change Management Process Required**

**DO NOT implement any workarounds without following the change management process in Section 6.C.**

**This section describes potential workarounds for reference only. All workarounds MUST:**
1. Be tracked in a bug ticket (created first - see Section 6.C, Step 1)
2. Have approval from SME and Managers (documented in ticket)
3. Follow proper change management process (tickets, approvals, testing, deployment)
4. Be documented with rationale and impact assessment

**If you are unsure about any workaround, escalate to SME or team lead.**

#### A.1. Service Restart (If Service is Down or Stuck)

**Action:** Restart the core-api service to restore supervisor task execution.

**Steps:**
1. **Verify the Issue:**
   - Check core-api pod status and logs to confirm service is down or stuck.
   - Verify metric has not been emitted for 10+ minutes.
   - Check if other background tasks are also not running (indicates general scheduler issue).
   - Review logs for errors indicating service crash or scheduler failure.

2. **Gather Information for Ticket:**
   - Pod status and recent restart history
   - Last successful supervisor run timestamp (from metric)
   - Error logs from core-api service
   - Database connectivity status
   - Temporal connectivity status

3. **Create Bug Ticket:**
   - Document the issue with full details (metric not emitting, service status, logs).
   - Include impact assessment (timed-out workflows not being cleaned up).
   - Note if this is a recurring issue or one-time event.

4. **Get Approval for Restart:**
   - Obtain approval from SME and Managers (document in bug ticket).
   - Verify restart is safe (check for any in-flight operations).

5. **Restart Core-API Service (if approved):**
   - **Kubernetes Restart:**
     ```bash
     kubectl -n <namespace> rollout restart deployment/core-api
     ```
   - **Verify Restart:**
     - Wait for pods to be in Running state.
     - Check logs for "Starting VCP Core API Service" and "Background task scheduler started successfully".
     - Verify metric starts emitting again within 5-10 minutes.

6. **Monitor and Verify:**
   - Monitor `vcp.background.task.last_run_timestamp` metric - should update within 5-10 minutes after restart.
   - Check logs for supervisor task execution: "[WorkflowSupervisorTask] Starting workflow supervisor task".
   - Verify no errors in logs related to supervisor task.
   - Update bug ticket with restart results and verification.

#### A.2. Database Lock Issue (If Lock is Stuck)

**Action:** Clear stuck admin job spec lock if lock acquisition is failing.

**Steps:**
1. **Verify the Issue:**
   - Check admin job spec table for `WORKFLOW_SUPERVISOR_SWEEP` job.
   - Verify lock timestamp is older than 5 minutes (300 seconds).
   - Check logs for "Could not acquire lock" messages.

2. **Gather Information:**
   - Admin job spec record details (UUID, state, created_at, updated_at).
   - Lock timeout configuration (should be 300 seconds).
   - Logs showing lock acquisition failures.

3. **Escalate to Database Team:**
   - **DO NOT manually clear the lock** - this is a database operation that should be handled by the DB team.
   - Create ticket with full error details and context.
   - Include admin job spec record details.
   - Note if this is a transient issue or persistent.

4. **Alternative (if approved and safe):**
   - If lock is clearly stuck (updated_at is > 5 minutes old and no pods are running the task), coordinate with DB team to clear the lock.
   - **Warning:** Only do this if you're certain no pod is actively running the supervisor task.

#### A.3. Scheduler Registration Issue (If Scheduler Failed to Start)

**Action:** Verify and fix scheduler registration if scheduler failed to start.

**Steps:**
1. **Verify the Issue:**
   - Check logs for "Failed to schedule Workflow Supervisor Task" errors.
   - Verify cron scheduler started successfully.
   - Check for scheduler registration errors.

2. **Gather Information:**
   - Scheduler registration error logs.
   - Cron expression configuration.
   - Service startup logs.

3. **Root Cause Analysis:**
   - If scheduler registration failed: Check cron expression format and configuration.
   - If scheduler crashed: Check for panics or errors in scheduler code.
   - If service failed to start: Check service startup logs for errors.

4. **Fix (if approved):**
   - Fix configuration errors if found.
   - Restart service if scheduler registration failed due to transient issues.
   - Coordinate with development team if code changes are needed.

### B. Permanent Fixes (Require Change Management)

**⚠️ MANDATORY: Change Management Process Required**

**DO NOT implement any permanent fixes without following the change management process in Section 6.C.**

**This section describes potential permanent fixes for reference only. All fixes MUST:**
1. Be tracked in a bug ticket (created first - see Section 6.C, Step 1)
2. Go through proper planning, design, and approval process
3. Follow change management process (tickets, approvals, testing, deployment)
4. Be properly tested and reviewed before deployment

#### Configuration Updates
- **Environment Variables:**
  - Update supervisor configuration if needed.
  - Update via deployment process with proper testing.

#### Infrastructure Improvements
- **Service Health Monitoring:**
  - Add health checks for background task scheduler.
  - Add alerts for scheduler failures.
  - Improve service restart policies.

#### Code/Service Fixes
- **Bug Fixes:**
  1. **Raise Bug Ticket:**
     - Create bug ticket with full details (metric not emitting, service status, logs).
     - Assign to appropriate team (core-api, scheduler, infrastructure).
     - Set priority based on impact (P0 for production outages).
  2. **Code Changes:**
     - Fix bugs in scheduler initialization or supervisor task execution.
     - Add proper error handling and logging.
     - Add health checks for supervisor task.
     - Write unit and integration tests.
  3. **Testing:**
     - Test fixes in development/staging environment.
     - Verify fix resolves the issue without introducing regressions.
  4. **Deployment:**
     - Deploy via standard CI/CD process.
     - Monitor after deployment to verify fix.

#### Documentation Updates
- **Runbook Updates:**
  - Add new troubleshooting steps discovered.
  - Update common symptoms table with new patterns.
  - Document workarounds that were effective.

### C. Change Management Process for Fixes

**⚠️ THIS IS THE MANDATORY PROCESS - Follow this for ALL fixes and workarounds**

**Before implementing ANY fix or workaround described in Sections 6.A or 6.B, you MUST follow this process.**

**Step 1: Create Bug Ticket**
1. **Create Ticket:** Raise a bug ticket against the issue with full details:
   - Metric not emitting details (last run timestamp, duration)
   - Service status and logs
   - Root cause analysis
   - Impact assessment (timed-out workflows not being cleaned up)
   - Affected environments

**Step 2: Decide on Workaround (Based on Bug Ticket)**
2. **Assess Need for Workaround:**
   - Review bug ticket to determine if immediate workaround is needed
   - Consider: severity (critical - supervisor not running), customer impact (workflows not cleaned up), availability of permanent fix timeline
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

- **Monitor Metrics:**
  - Verify `vcp.background.task.last_run_timestamp` with tag `task=WORKFLOW_SUPERVISOR_SWEEP` starts updating again.
  - Verify `vcp.background.task.runs` with tag `task=WORKFLOW_SUPERVISOR_SWEEP` starts incrementing.
  - Monitor for at least 2-3 supervisor cycles (10-15 minutes) to ensure stability.
- **Check Logs:**
  - Verify supervisor task execution logs: "[WorkflowSupervisorTask] Starting workflow supervisor task".
  - Confirm no errors in supervisor task execution.
  - Verify scheduler is running: "Background task scheduler started successfully".
- **Verify Service Health:**
  - Check core-api service is running and healthy.
  - Verify no repeated restarts or crashes.
  - Check database and Temporal connectivity.

## 8. Document the Resolution

- **Record Details:**
  - Document the root cause, resolution steps, and any temporary mitigations applied.
  - Note the service restart time and verification results.
  - Record any configuration changes made.
- **Update Runbook:**
  - Add new insights or steps discovered during troubleshooting.
  - Update common symptoms table with new patterns.
  - Document workarounds that were effective.
- **Share Learnings:**
  - Communicate findings with the team to prevent recurrence.
  - Update monitoring and alerting if gaps were discovered.

---

## Quick Reference: Common Symptoms & Fixes

| Symptom | Likely Cause | Mitigation |
|---------|--------------|------------|
| Metric not updated for 10+ minutes | Core-api service down | Restart core-api service |
| Metric not updated, service running | Scheduler not started | Check scheduler logs, restart service |
| Metric not updated, scheduler running | Lock acquisition failed | Check database lock, escalate to DB team |
| Metric not updated, lock acquired | Supervisor task execution failed | Check supervisor task logs for errors |
| Service in CrashLoopBackOff | Service crashing on startup | Check startup logs, fix configuration errors |
| Scheduler registration failed | Cron expression error | Fix cron expression configuration |
| Database connection errors | DB connectivity issues | Escalate to DB team |
| Temporal client errors | Temporal connectivity issues | Check Temporal cluster status |

---

## Accessing Metrics and Logs

### Method 1: GCP Monitoring (Recommended)

**Location:** Monitoring project (GCP Monitoring)

**Available Metrics:**
- **Background Task Metrics:**
  - `vcp.background.task.runs` - Counter of task executions (tag: `task=WORKFLOW_SUPERVISOR_SWEEP`)
  - `vcp.background.task.last_run_timestamp` - Timestamp of last run (tag: `task=WORKFLOW_SUPERVISOR_SWEEP`)
  - `vcp.background.task.errors` - Error counter (tag: `task=WORKFLOW_SUPERVISOR_SWEEP`, `reason`)

**How to Access:**
1. Navigate to GCP Monitoring project
2. Use metric explorer or create custom dashboards
3. Filter by:
   - Metric: `vcp.background.task.last_run_timestamp`
   - Tag: `task=WORKFLOW_SUPERVISOR_SWEEP`
4. Check the metric value - should be recent (within last 5 minutes)

**Alert Query Example:**
```
resource.type="k8s_container"
metric.type="custom.googleapis.com/opencensus/vcp.background.task.last_run_timestamp"
metric.label.task="WORKFLOW_SUPERVISOR_SWEEP"
```

### Method 2: Service Logs

**Location:** GCP Logging

**Log Filters:**
- Service: `core-api`
- Look for: `"[WorkflowSupervisorTask] Starting workflow supervisor task"`
- Look for: `"Starting workflow supervisor task with lock"`
- Look for: `"Background task scheduler started successfully"`
- Look for: `"Failed to schedule Workflow Supervisor Task"`

**Example Query:**
```
resource.type="k8s_container"
resource.labels.container_name="core-api"
jsonPayload.message=~"WorkflowSupervisorTask|workflow supervisor"
```

### Method 3: Database Queries

**Admin Job Spec Status:**
```sql
SELECT uuid, job_type, state, cron_expression, created_at, updated_at
FROM admin_job_specs
WHERE job_type = 'WORKFLOW_SUPERVISOR_SWEEP';
```

**Check Lock Status:**
- If `updated_at` is older than 5 minutes and no pods are running the task, the lock may be stuck.

### Method 4: Temporal CLI (tctl) - Checking Workflows

**Note:** When the supervisor is not running, you may need to manually check workflows that should have been cleaned up.

**Prerequisites:**
- `tctl` CLI tool available locally or accessible from admintools pod
- Access to the Temporal namespace
- Workflow ID (same as job UUID from database, found in `jobs` table `workflow_id` column)

**Common Commands:**

1. **Describe Workflow Status:**
   ```bash
   tctl --namespace <vcp-namespace> workflow describe --workflow-id <workflow_id>
   ```
   - Shows workflow status, execution time, and basic information
   - Use this to check if workflow is running, completed, timed out, or failed
   - Useful for verifying workflows that supervisor should be cleaning up

2. **Show Workflow History:**
   ```bash
   tctl --namespace <vcp-namespace> workflow show --workflow-id <workflow_id>
   ```
   - Shows detailed execution history with all activities
   - Use this to see which activity failed and why
   - Shows activity inputs, outputs, and errors
   - Useful for understanding why a workflow timed out

3. **List Timed-Out Workflows:**
   ```bash
   tctl --namespace <vcp-namespace> workflow list --query 'ExecutionStatus=TimedOut'
   ```
   - Lists workflows that have timed out
   - Use this to find workflows that supervisor should be cleaning up
   - Filter by workflow type if needed: `--query 'ExecutionStatus=TimedOut AND WorkflowType="CreateVolumeWorkflow"'`

4. **List Running Workflows (to check for stuck workflows):**
   ```bash
   tctl --namespace <vcp-namespace> workflow list --query 'ExecutionStatus=Running'
   ```
   - Lists currently running workflows
   - Use this to identify workflows that may be stuck and need supervisor intervention

**Using from admintools pod (for restricted clusters):**
```bash
kubectl -n <ops-namespace> exec -it deploy/admintools -- /bin/sh -c "tctl --namespace <vcp-namespace> workflow describe --workflow-id <workflow_id>"
```

**Finding Workflow IDs from Database:**
```sql
SELECT uuid, workflow_id, type, state, created_at, updated_at
FROM jobs
WHERE state = 'NEW' AND created_at < NOW() - INTERVAL '5 minutes'
ORDER BY created_at DESC;
```
- This finds jobs in NEW state that are older than 5 minutes (supervisor grace period)
- Use the `workflow_id` column value with tctl commands above

**Reference:** See `doc/guides/temporal-debugging.md` for more tctl commands and troubleshooting patterns.

---

## Operational Readiness Checklist

### Service Health Verification
- [ ] Core-api service is running and healthy
- [ ] Core-api pods are in Running state (not CrashLoopBackOff or Error)
- [ ] No recent pod restarts (unless intentional)
- [ ] Service is responding to health checks

### Scheduler Verification
- [ ] Background task scheduler started successfully (check logs)
- [ ] Cron scheduler is running
- [ ] Supervisor task is scheduled (check logs for registration)

### Database Verification
- [ ] Database connectivity from core-api service
- [ ] Admin job spec record exists for `WORKFLOW_SUPERVISOR_SWEEP`
- [ ] Lock is not stuck (updated_at is recent or lock timeout has passed)

### Temporal Verification
- [ ] Temporal client is initialized
- [ ] Temporal cluster is accessible from core-api service

### Metrics Verification
- [ ] `vcp.background.task.last_run_timestamp` with tag `task=WORKFLOW_SUPERVISOR_SWEEP` is updating every 5 minutes
- [ ] `vcp.background.task.runs` with tag `task=WORKFLOW_SUPERVISOR_SWEEP` is incrementing
- [ ] No errors in `vcp.background.task.errors` with tag `task=WORKFLOW_SUPERVISOR_SWEEP`

---

**Tip:**  
The workflow supervisor task is critical for cleaning up timed-out workflows. If it stops running, timed-out workflows will not be cleaned up, leading to resource leaks and inconsistent state. Always treat this as a critical issue and verify the supervisor is running after any service restarts or deployments.

---

# Useful Tools and Resources

* **Monitoring System:** [GCP Monitoring](https://console.cloud.google.com/monitoring)
  - Background task metrics (`vcp.background.task.runs`, `vcp.background.task.last_run_timestamp`)
  - Filter by `task=WORKFLOW_SUPERVISOR_SWEEP`
  - Service health metrics (pod status, restarts, resource usage)
* **Logging Platform:** [GCP Logging](https://console.cloud.google.com/logs)
  - Core-api service logs
  - Filter by service: `core-api`
  - Filter by messages containing "WorkflowSupervisorTask" or "workflow supervisor"
* **Kubernetes:**
  - **Pod Status:** `kubectl -n <namespace> get pods -l app=core-api`
  - **Pod Logs:** `kubectl -n <namespace> logs -l app=core-api --tail=100`
  - **Pod Restart:** `kubectl -n <namespace> rollout restart deployment/core-api`
* **Temporal CLI (tctl):**
  - **Reference:** `doc/guides/temporal-debugging.md`
  - **Common Commands:**
    - `tctl --namespace <vcp-namespace> workflow describe --workflow-id <id>` - Get workflow status
    - `tctl --namespace <vcp-namespace> workflow show --workflow-id <id>` - Get detailed execution history
    - `tctl --namespace <vcp-namespace> workflow list --query 'ExecutionStatus=TimedOut'` - List timed-out workflows
  - **Access:** Via local `tctl` or admintools pod: `kubectl exec -it deploy/admintools -- tctl ...`
* **Design Document:** `doc/architecture/designs/0015-workflow-supervisor-task.md`
* **Troubleshooting Guide:** https://confluence.ngage.netapp.com/spaces/VSCP/pages/1273328576/Pool+Volume+CRUD+Operations+Troubleshooting+Guide

