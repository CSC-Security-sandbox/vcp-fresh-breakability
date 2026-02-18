# Runbook for create_largevolume_failures

This runbook provides a structured approach to **identifying, investigating, and diagnosing** large capacity volume creation failures in the VSA Control Plane system.

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
| Alert Name          | create_largevolume_failures                                                                     |
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
- **Initial Assessment:** Check if this is a single failure or a pattern (multiple volumes failing).

## 2. Gather Initial Context
- **Review Alert Details:**
  - Check the alert description, severity, and any associated dashboards or logs.
  - Identify the affected volume name, pool name, project number, location, and operation/job ID.
  - Note the correlation ID and request ID for log correlation.
- **Check Recent Changes:**
  - Review deployment logs, configuration changes, or infrastructure updates.
  - Check if any recent changes to large volume validation logic or pool configuration.
  - Verify if environment variables for large volume limits were modified.
- **Identify Scope:**
  - Determine if this affects a specific region, project, pool, or is system-wide.
  - Check if other large volumes are being created successfully.
  - Verify if the target pool is a large capacity pool.

## 3. Validate the Alert
- **Confirm Legitimacy:**
  - Verify the volume is actually in error state (not a false positive).
  - Check the API response, DB state, and workflow/job status.
  - Confirm the job type is `CREATE_LARGE_VOLUME` (not `CREATE_VOLUME`).
  - Verify the target pool is a large capacity pool (`pool.largeCapacity = true`).
- **Verify Error Type:**
  - Check if it's a client-side error (400/409) or server-side error (500/503).
  - Review the error message and tracking ID from the job.

## 4. Identify the Root Cause

### A. Logs Analysis

#### API Layer Logs
- **Location:** `google-proxy` service logs
- **What to Check:**
  - Validation errors for large volume parameters (size, constituent volumes, protocols).
  - Error messages indicating which parameter failed validation.
  - HTTP status codes (400 for validation, 500 for internal errors).
  - Pool validation errors (large volumes must be in large capacity pools).
- **Key Fields:** `correlation_id`, `request_id`, `job_type`, `error_details`, `pool_id`

#### Orchestrator/Workflow Logs
- **Location:** `worker` service logs, Temporal workflow logs
- **What to Check:**
  - Workflow execution errors and activity failures.
  - Large volume validation errors from volume validation logic.
  - Pool capacity validation (large volumes require large capacity pools).
  - Constituent volume validation errors.
  - Timeout errors (large volumes use 30-minute timeout vs 10-minute for regular).
- **Key Fields:** `workflow_id`, `activity_type`, `error`, `tracking_id`, `pool_id`

#### Database Logs
- **Location:** Database connection and query logs
- **What to Check:**
  - Volume creation record in database.
  - Job state transitions (NEW → PROCESSING → ERROR).
  - Unique constraint violations (duplicate volume names/resource IDs).
  - Transaction failures or deadlocks.
  - Pool capacity check (verify pool is large capacity).

#### GCP Operation Logs
- **Location:** GCP Console → Operations
- **What to Check:**
  - ONTAP volume creation operations (longer for large volumes with constituent volumes).
  - Network provisioning operations (if needed).
  - Resource quota errors (large volumes may require more resources).

### B. Metrics Review

#### System Health Metrics
- **API Error Rates:** Check for spikes in 400/500 errors around alert time.
- **Workflow Success Rates:** Compare large volume vs regular volume success rates.
- **Database Health:** Connection pool usage, query latency, transaction failures.
- **Temporal Metrics:** Workflow execution times, activity durations, retry counts.

#### Resource Utilization
- **Pool Capacity:** Check if pool has sufficient capacity for the large volume.
- **Constituent Volume Limits:** Check if pool has sufficient constituent volume capacity.
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
- **Workflow Registration:** Verify `CreateVolumeWorkflow` is registered (same workflow, different job type).
  - **Check via CLI:** Use `tctl --namespace <vcp-namespace> workflow describe --workflow-id <workflow_id>` to verify workflow type.
- **Task Queue:** Check `CustomerTaskQueue` is processing tasks.
  - **Check via Metrics:** Review task queue depth and processing rate metrics in monitoring project.
  - **Check via CLI:** Use `tctl --namespace <vcp-namespace> task-queue describe --task-queue CustomerTaskQueue` to check queue status.
- **Workflow Status:** Review workflow execution status for the failed job.
  - **Check via Metrics:** Review workflow execution metrics (duration, success rate, error rate) in monitoring project.
  - **Check via CLI:** Use `tctl --namespace <vcp-namespace> workflow describe --workflow-id <workflow_id>` to get workflow status and history.

#### Pool Capacity Check
- **Verify Pool is Large Capacity:**
  - Check pool `largeCapacity` flag is `true` in database.
  - Large volumes **MUST** be created in large capacity pools.
  - Error: "Large capacity volumes can only be created in large capacity pools"
- **Check Pool Available Capacity:**
  - Verify pool has sufficient free space for the volume.
  - Check pool quota usage vs pool size.
- **Check Constituent Volume Capacity:**
  - Verify pool has sufficient constituent volume slots available.
  - Check aggregate-level constituent volume limits.

### D. Dependency Check

#### Pool Requirements
- **Large Capacity Pool:** Volume must be created in a pool with `largeCapacity: true`.
- **Pool State:** Pool must be in `READY` state (not `CREATING`, `UPDATING`, or `ERROR`).
- **Pool Capacity:** Pool must have sufficient free space for the volume.
- **Auto-Tiering:** If volume auto-tiering is requested, pool must have `allowAutoTiering: true`.

#### ONTAP/Data Plane
- **ONTAP Cluster:** Must be accessible and healthy.
- **Aggregate Capacity:** Must have sufficient space for constituent volumes.
- **Constituent Volume Limits:** Must not exceed per-aggregate or per-volume limits.

### E. Configuration Review

#### Large Volume Specific Parameters

**Size Validation:**
- **Minimum Size (without constituent volumes):** 4.8 TiB (`MIN_QUOTA_IN_BYTES_LARGE_VOLUME`)
- **Minimum Size (with constituent volumes):** 2.4 TiB (`MIN_QUOTA_IN_BYTES_LARGE_VOLUME_WITH_CV`)
- **Maximum Size (without auto-tiering):** 2.48 PiB (`MAX_LV_HOT_TIER_POOL_CAPACITY`)
- **Maximum Size (with auto-tiering):** 20 PiB (`MAX_QUOTA_IN_BYTES_LARGE_VOLUME`)
- **Error Message:** "Invalid volume capacity [size]. Must be between [min] and [max]."

**Constituent Volume Validation:**
- **Minimum Constituent Volume Size:** 100 GB (`minCVSizeInBytes`)
- **Maximum Constituent Volume Size:** 300 TiB (`maxCVSizeInBytes`)
- **Constituent Count Validation:**
  - Cannot be a prime number (if ≥ minimum prime config)
  - Must not exceed per-aggregate limits
  - Must not exceed per-volume limits
  - Default: 48 CVs (8 per aggregate × 6 aggregates)
- **Error Messages:**
  - "Constituent volume size cannot be less than 100 GB"
  - "Constituent volume size cannot be more than 300 TiB"
  - "Constituent volume count with [X] is not supported" (if prime)
  - "Large Volume constituent count cannot be greater than [X]"

**Protocol Validation:**
- **SAN Protocols:** NOT supported for large capacity volumes
- **Error Message:** "SAN protocols are not supported for large capacity volumes"

**Block Device Validation:**
- **BlockDevices:** NOT supported for large capacity volumes
- **BlockProperties:** NOT supported for large capacity volumes
- **Error Messages:**
  - "BlockDevices are not supported for large capacity volumes"
  - "BlockProperties are not supported for large capacity volumes"

**Pool Capacity Validation:**
- **Large Capacity Pool Required:** Volume `largeCapacity` must match pool `largeCapacity`
- **Error Message:** "Large capacity volumes can only be created in large capacity pools"

**Auto-Tiering Validation (if enabled):**
- **Pool Auto-Tiering:** Pool must have `allowAutoTiering: true`
- **Cooling Threshold:** Must be between 2 and 183 days
- **Error Messages:**
  - "Auto Tiering is not allowed for this volume. Please enable Auto Tiering on the Pool and try again"
  - "Auto Tiering Cooling Threshold days must be between 2 and 183 days"

#### Environment Variables

**Large Volume Specific:**
- `MIN_QUOTA_IN_BYTES_LARGE_VOLUME` (default: 4.8 TiB)
- `MIN_QUOTA_IN_BYTES_LARGE_VOLUME_WITH_CV` (default: 2.4 TiB)
- `MAX_QUOTA_IN_BYTES_LARGE_VOLUME` (default: 20 PiB)
- `MAX_LV_HOT_TIER_POOL_CAPACITY` (default: 2.48 PiB)

**Workflow Timeouts:**
- `VOLUME_ACTIVITIES_START_TO_CLOSE_TIMEOUT_SEC_LV` (default: 1800s = 30m) - **Longer than regular volumes (600s = 10m)**
- `VOLUME_ACTIVITIES_HEARTBEAT_TIMEOUT_SEC` (default: 300s = 5m)

**Feature Flags:**
- `AUTO_TIERING_ENABLED` - Must be `true` if auto-tiering is requested
- `ENABLE_MQOS` - Required for manual QoS type

## 5. Formulate a Hypothesis

Based on the gathered information, develop a hypothesis about the root cause:

### Common Root Causes for Large Volume Failures

1. **Validation Errors (400):**
   - Volume size below 4.8 TiB (or 2.4 TiB with CV) or above limits
   - Volume created in non-large-capacity pool
   - SAN protocols specified (not supported)
   - BlockDevices/BlockProperties specified (not supported)
   - Constituent volume count invalid (prime number, exceeds limits)
   - Constituent volume size out of range (100 GB - 300 TiB)
   - Auto-tiering requested but pool doesn't allow it
   - Cooling threshold out of range (2-183 days)

2. **Resource Constraints (409/422):**
   - Duplicate volume name/resource ID
   - Insufficient pool capacity
   - Insufficient constituent volume slots in pool
   - Pool not in READY state

3. **Configuration Errors (400/422):**
   - Large volume created in non-large-capacity pool
   - Auto-tiering requested but pool doesn't allow it
   - Feature flags disabled when required

4. **Infrastructure Failures (500/503):**
   - Database connectivity issues
   - Temporal worker unavailable
   - ONTAP cluster unavailable or unhealthy
   - Aggregate capacity issues

5. **Timeout Errors:**
   - Workflow timeout (30 minutes exceeded)
   - Activity timeout (heartbeat timeout exceeded)
   - ONTAP operation timeout

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
- "Invalid volume capacity [size]. Must be between [min] and [max]."
- "Large capacity volumes can only be created in large capacity pools"
- "SAN protocols are not supported for large capacity volumes"
- "BlockDevices are not supported for large capacity volumes"
- "BlockProperties are not supported for large capacity volumes"
- "Constituent volume size cannot be less than 100 GB"
- "Constituent volume size cannot be more than 300 TiB"
- "Constituent volume count with [X] is not supported"
- "Large Volume constituent count cannot be greater than [X]"
- "Auto Tiering is not allowed for this volume. Please enable Auto Tiering on the Pool and try again"
- "Auto Tiering Cooling Threshold days must be between 2 and 183 days"

**Steps to Inform Customer:**
1. **Identify the Specific Error:**
   - Extract the exact error message from logs/API response.
   - Identify which parameter(s) failed validation.
   - Note the tracking ID and correlation ID for reference.

2. **Prepare Customer Communication:**
   - **Template Message (Customer-Facing):**
     ```
     Your large volume creation request failed due to invalid parameters.
     
     Error: [Exact error message]
     Operation ID: [Operation ID] (if available - use this to check operation status)
     Correlation ID: [Correlation ID] (if you provided x-correlation-id header)
     
     Required Corrections:
     - [Specific parameter] must be [correct value/range]
     - [Additional corrections if multiple parameters]
     
     Please review the large volume requirements and resubmit with corrected parameters.
     Reference: [Link to API documentation or requirements]
     ```
   
   - **Internal Reference (for support case notes, not sent to customer):**
     - Tracking ID: [Tracking ID] (internal error code)
     - Job UUID: [Job UUID] (if job was created)
     - Workflow ID: [Workflow ID] (if workflow was started)
     - Pool ID: [Pool ID] (verify pool is large capacity)

3. **Provide Correct Parameter Values:**
   - **Size:** Must be between 4.8 TiB and 2.48 PiB (or 20 PiB with auto-tiering), or 2.4 TiB minimum with constituent volumes.
   - **Pool:** Must be a large capacity pool (`largeCapacity: true`).
   - **Protocols:** Must NOT include SAN protocols (NFS/SMB only).
   - **BlockDevices/BlockProperties:** Must NOT be specified.
   - **Constituent Volumes:** If specified, count must not be prime, size per CV must be 100 GB - 300 TiB.
   - **Auto-Tiering:** Pool must have `allowAutoTiering: true`, cooling threshold 2-183 days.

4. **Document the Interaction:**
   - Record customer contact details and communication.
   - Note the specific parameters that were incorrect.
   - Track if this is a recurring issue (may indicate documentation gaps).

#### A.2. Conflict Errors (409) - Inform Customer

**Common Conflict Errors:**
- "Volume with same resourceId already exists"
- Duplicate resource ID conflicts
- Pool not in READY state

**Steps to Inform Customer:**
1. **Check Volume/Pool State:**
   - If volume exists in `CREATING` state: Inform customer they can reuse the operation.
   - If volume exists in other state: Inform customer to use a unique `resourceId`.
   - If pool is not READY: Inform customer to wait for pool to be READY.

2. **Customer Communication:**
   - **If Volume in CREATING:**
     ```
     A volume with the same resourceId is currently being created.
     Operation ID: [operation_id]
     You can query this operation to check status or wait for completion.
     ```
   - **If Volume Exists:**
     ```
     A volume with resourceId '[resourceId]' already exists.
     Please use a unique resourceId for your new volume.
     ```
   - **If Pool Not READY:**
     ```
     The target pool is not in READY state. Please wait for the pool to be READY before creating volumes.
     ```

3. **No Internal Action Required:**
   - Do not modify customer data.
   - Do not delete existing volumes.
   - Customer must resolve the conflict.

#### A.3. Resource Quota Errors (422) - May Require Customer Action

**Note:** Some quota errors (like pool capacity) are customer-side, while others (like system quotas) are server-side.

**Customer-Side Quota Errors:**
- Pool capacity insufficient
- Constituent volume slots insufficient in pool
- Pool not in READY state

**Steps:**
1. **Verify Error Source:**
   - Check if error is from pool capacity constraints.
   - Verify if it's a system-wide quota issue.

2. **If Customer Pool Capacity:**
   - Inform customer to:
     - Check pool available capacity.
     - Create a larger pool or free up space in existing pool.
     - Check constituent volume capacity if using constituent volumes.
   - Provide pool capacity documentation link.

3. **If System Quota:**
   - Treat as server-side error (see Section B).

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
     - Verify if error is specific to large volume creation or system-wide.
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
     - Retry volume creation after database team confirms fix

##### For Workflow/Temporal Errors (500)
- **Action:** Restore workflow execution capability.
- **Steps:**
  1. **Verify Temporal workers are running and healthy:**
     - **Check Metrics:** Review worker health metrics in monitoring project (worker count, heartbeat status).
     - **Check CLI:** Use `tctl --namespace <vcp-namespace> workflow list --limit 10` to verify workers are processing.
  2. **Check if `CreateVolumeWorkflow` is registered:**
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

##### For ONTAP/Data Plane Errors (500/503)
- **Action:** Resolve ONTAP cluster or data plane issues.
- **Steps:**
  1. Check ONTAP cluster health and accessibility.
  2. Verify aggregate capacity and constituent volume limits.
  3. Check for ONTAP operation timeouts or failures.
  4. Verify ONTAP proxy service is healthy.
- **Workaround (if approved):**
  - Retry ONTAP operations if transient failures.
  - Check aggregate capacity and free up space if needed.
  - Verify constituent volume limits are not exceeded.

##### For Timeout Errors
- **Action:** Determine if timeout is legitimate or indicates a stuck operation.
- **Steps:**
  1. Check ONTAP console for ongoing operations.
  2. **Verify workflow status:**
     - **Check Metrics:** Review workflow execution duration metrics in monitoring project to see if it's progressing.
     - **Check CLI:** Use `tctl --namespace <vcp-namespace> workflow describe --workflow-id <workflow_id>` to check workflow status and last activity time.
  3. **Check if operation is progressing:**
     - **Check Metrics:** Review activity heartbeat metrics in monitoring project (should see regular heartbeats if progressing).
     - **Check CLI:** Use `tctl --namespace <vcp-namespace> workflow show --workflow-id <workflow_id>` to see activity execution history and heartbeats.
  4. Determine if 30-minute timeout is sufficient for the operation (compare actual duration from metrics vs timeout).
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
- **Feature Flags:**
  - Enable/disable feature flags (`AUTO_TIERING_ENABLED`, `ENABLE_MQOS`).
  - Update via configuration management system.
  - Document change and notify team.
- **Environment Variables:**
  - Update large volume limits if requirements change.
  - Modify timeout values if operations legitimately need more time.
  - Update via deployment process with proper testing.

##### Infrastructure Scaling
- **Pool Capacity:**
  - Ensure pools have sufficient capacity for large volumes.
  - Monitor pool capacity usage.
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
     - Assign to appropriate team (orchestrator, workflow, ONTAP proxy).
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
  - Update `VOLUME_ACTIVITIES_START_TO_CLOSE_TIMEOUT_SEC_LV` if needed.
  - Document timeout changes and rationale.
- **Error Handling Improvements:**
  - Improve error messages to guide users to correct parameters.
  - Add more detailed logging for debugging.
  - Enhance error taxonomy and tracking.

##### Documentation Updates
- **API Documentation:**
  - Update API docs if parameter validation errors are common.
  - Add examples of correct large volume creation requests.
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
│   │   └─ Inform customer about duplicate resource or pool state
│   └─ Quota error (422)?
│       ├─ Customer pool capacity?
│       │   └─ Inform customer to increase pool capacity
│       └─ System quota?
│           └─ Treat as server-side error
│
└─ NO (500, 503, or other) → Server-Side Error
    ├─ Database error?
    │   └─ Escalate to database team
    ├─ Workflow/Temporal error?
    │   └─ Apply workflow workaround or fix
    ├─ ONTAP/Data plane error?
    │   └─ Check ONTAP cluster health and capacity
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
  - Check that the volume transitions to `READY` state.
  - Monitor subsequent large volume creations for success.
- **Run Tests:**
  - Execute large volume creation tests with various configurations.
  - Verify edge cases (minimum size, maximum size, with/without constituent volumes, auto-tiering enabled/disabled).
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

# Large Volume Creation Error Debugging Detailed Runbook

## Step 1: Collect Failure Details

### Immediate Information Gathering
- **Error Message:** Record the exact error message from API response or logs.
- **Operation/Job ID:** Note the operation ID and job UUID returned by the API.
- **Request Payload:** Capture all parameters sent in the create volume request:
  - `largeCapacity: true` (must be set)
  - `quotaInBytes` (must be ≥ 4.8 TiB, or 2.4 TiB with CV)
  - `poolId` (must be a large capacity pool)
  - `largeVolumeConstituentCount` (if specified, must be valid)
  - `autoTieringPolicy` (if specified, pool must allow auto-tiering)
  - `protocols` (must NOT include SAN protocols)
  - `blockDevices` (must NOT be specified)
  - `blockProperties` (must NOT be specified)
- **Volume State:** Check the volume state in the database (should be `CREATING` or error state).
- **Job Type:** Verify job type is `CREATE_LARGE_VOLUME` (not `CREATE_VOLUME`).
- **Pool Information:** Verify pool is large capacity and in READY state.

### Log Correlation
- **Correlation ID:** Use to find all related logs across services.
- **Request ID:** Use to trace the request through the system.
- **Workflow ID:** Use to query Temporal workflow status.
- **Pool ID:** Use to verify pool capacity and state.

## Step 2: Identify Where the Failure Occurred

### Failure Point Detection

#### A. API/Validation Layer (400/409 errors, immediate response)
**Indicators:**
- HTTP 400 with validation error message
- HTTP 409 with conflict error
- Error returned immediately (no job created)
- Job not created in database

**Common Errors:**
- "Invalid volume capacity [size]. Must be between [min] and [max]."
- "Large capacity volumes can only be created in large capacity pools"
- "SAN protocols are not supported for large capacity volumes"
- "BlockDevices are not supported for large capacity volumes"
- "Constituent volume size cannot be less than 100 GB"
- "Constituent volume count with [X] is not supported"
- "Auto Tiering is not allowed for this volume. Please enable Auto Tiering on the Pool and try again"

#### B. Orchestrator/Database Layer (500 errors, job not created, DB errors)
**Indicators:**
- HTTP 500 error
- Job not created in database
- Database connection errors in logs
- Unique constraint violations

**Common Errors:**
- Database connection failures
- Transaction deadlocks
- Unique constraint violations (duplicate volume name/resource ID)
- Migration errors

#### C. Workflow/Temporal Layer (Job created, volume stuck in CREATING)
**Indicators:**
- Job created with state `NEW` or `PROCESSING`
- Volume stuck in `CREATING` state
- Workflow not progressing (no activity updates in metrics or CLI)
- Activity failures in workflow logs

**Common Errors:**
- Workflow not registered
- Temporal worker unavailable (check worker metrics)
- Activity timeout errors (check activity duration metrics)
- Workflow timeout (30 minutes exceeded - check workflow duration metrics)

**How to Verify:**
- **Check Metrics:** Review workflow execution metrics in monitoring project (workflow status, activity status, duration).
- **Check CLI:** Use `tctl --namespace <vcp-namespace> workflow describe --workflow-id <workflow_id>` to get workflow status and execution history.

#### D. ONTAP/Data Plane (Timeouts, capacity, operation errors)
**Indicators:**
- Workflow progresses but fails during ONTAP volume creation
- ONTAP operation errors in logs
- Aggregate capacity errors
- Constituent volume limit errors

**Common Errors:**
- "Insufficient aggregate capacity"
- "Constituent volume limit exceeded"
- "ONTAP operation timeout"
- "ONTAP cluster unavailable"

## Step 3: Troubleshoot by Component

### A. API/Validation Errors (400/409)

#### Large Volume Size Validation
**Check:**
- Is `largeCapacity: true` set in the request?
- Is `quotaInBytes` ≥ 4.8 TiB (or 2.4 TiB with constituent volumes)?
- Is `quotaInBytes` ≤ 2.48 PiB without auto-tiering?
- Is `quotaInBytes` ≤ 20 PiB with auto-tiering?
- Is the target pool a large capacity pool?

**Customer Action Required:**
- Customer must adjust `quotaInBytes` to meet requirements.
- Customer must ensure target pool is a large capacity pool.
- If auto-tiering is needed, customer must ensure pool allows auto-tiering and size is within 20 PiB limit.
- **Internal Action:** Inform customer of the error and provide correct parameter ranges.

#### Pool Capacity Validation
**Check:**
- Is the target pool a large capacity pool (`pool.largeCapacity = true`)?
- Is the pool in `READY` state?
- Does the pool have sufficient free capacity for the volume?

**Customer Action Required:**
- Customer must create volume in a large capacity pool.
- Customer must wait for pool to be READY before creating volumes.
- Customer must ensure pool has sufficient capacity.
- **Internal Action:** Inform customer about pool requirements.

#### Protocol Validation
**Check:**
- Does the request include SAN protocols (iSCSI, FC)?
- Large volumes only support NFS/SMB protocols.

**Customer Action Required:**
- Customer must remove SAN protocols from the request.
- **Internal Action:** Inform customer that SAN protocols are not supported for large volumes.

#### Block Device Validation
**Check:**
- Are `blockDevices` or `blockProperties` specified in the request?
- Large volumes do not support block devices.

**Customer Action Required:**
- Customer must remove `blockDevices` and `blockProperties` from the request.
- **Internal Action:** Inform customer that block devices are not supported for large volumes.

#### Constituent Volume Validation
**Check:**
- If `largeVolumeConstituentCount` is specified:
  - Is the count a prime number? (not allowed if ≥ minimum prime config)
  - Does the count exceed per-aggregate limits?
  - Does the count exceed per-volume limits?
  - Is the size per CV between 100 GB and 300 TiB?

**Customer Action Required:**
- Customer must adjust constituent volume count to be non-prime and within limits.
- Customer must ensure volume size divided by CV count is between 100 GB and 300 TiB.
- **Internal Action:** Inform customer of the error and provide correct parameter ranges.

#### Auto-Tiering Validation
**Check:**
- If auto-tiering is requested:
  - Does the pool have `allowAutoTiering: true`?
  - Is cooling threshold between 2 and 183 days?

**Action Required:**
- **If pool doesn't allow auto-tiering:** Customer must enable auto-tiering on the pool first (client-side fix).
- **If cooling threshold invalid:** Customer must adjust cooling threshold to 2-183 days (client-side fix).
- **Internal Action:** Determine root cause and apply appropriate fix (internal config change or inform customer).

#### Conflict Errors (409)
**Check:**
- Does a volume with the same `resourceId` already exist?
- If volume exists in `CREATING` state, can the operation be reused?
- Is the pool in READY state?

**Customer Action Required:**
- Customer must use a unique `resourceId`, or reuse the existing operation if volume is in `CREATING` state.
- Customer must wait for pool to be READY before creating volumes.
- **Internal Action:** Inform customer about the conflict and provide guidance on resolution.

### B. Orchestrator/DB Errors (500, job not created, DB errors)

#### Database Connectivity
**Check:**
- Can the orchestrator service connect to the database?
- Are there connection pool exhaustion errors?
- Are there database migration errors?

**Fix:**
- Escalate to database team (managed service).
- Verify database is accessible and healthy.
- Check connection pool configuration.
- Apply pending database migrations (coordinate with DB team).

#### Database Transaction Errors
**Check:**
- Are there transaction deadlock errors?
- Are there unique constraint violations?
- Are there foreign key constraint violations?

**Fix:**
- Retry after a short delay (deadlocks are usually transient).
- Verify volume name/resource ID is unique.
- Check for orphaned records causing constraint violations.

### C. Workflow/Temporal Startup Errors

#### Temporal Worker Status
**Check:**
- **Via Metrics:** Are Temporal workers running and healthy?
  - Review worker health metrics in monitoring project (worker count, heartbeat status, task processing rate).
- **Via CLI:** Use `tctl --namespace <vcp-namespace> workflow list --limit 10` to verify workers are processing tasks.
- **Via Metrics:** Is the `CustomerTaskQueue` processing tasks?
  - Review task queue metrics in monitoring project (queue depth, processing rate, task completion rate).
- **Via CLI:** Use `tctl --namespace <vcp-namespace> task-queue describe --task-queue CustomerTaskQueue` to check queue status.

**Fix:**
- Restart workers if metrics show they're down or unhealthy.
- Check worker logs for registration errors.
- Verify task queue configuration.

#### Workflow Registration
**Check:**
- **Via CLI:** Is `CreateVolumeWorkflow` registered in workers?
  - Use `tctl --namespace <vcp-namespace> workflow describe --workflow-id <workflow_id>` to verify workflow type.
- Are workflow versions compatible?
  - Check worker deployment logs for workflow registration.

**Fix:**
- Ensure workers have the latest workflow code.
- Verify workflow registration in worker startup logs.
- Redeploy workers if workflow registration is missing.

#### Workflow Execution
**Check:**
- **Via Metrics:** Does the workflow start?
  - Review workflow start metrics in monitoring project (workflow initiation rate, workflow count by status).
- **Via CLI:** Use `tctl --namespace <vcp-namespace> workflow describe --workflow-id <workflow_id>` to check workflow status.
- **Via Metrics:** Are activities being executed?
  - Review activity execution metrics in monitoring project (activity start rate, completion rate, duration).
- **Via Metrics:** Are there activity timeout errors?
  - Review activity timeout metrics in monitoring project (timeout count, timeout rate by activity type).

**Fix:**
- **Via CLI:** Use `tctl --namespace <vcp-namespace> workflow show --workflow-id <workflow_id>` to see detailed execution history.
- Verify activity timeouts are sufficient (30m for large volumes) - check timeout configuration.
- Check for non-retryable errors causing immediate failure - review error metrics and workflow history.

### D. ONTAP/Data Plane Errors

#### ONTAP Cluster Health
**Check:**
- Is ONTAP cluster accessible and healthy?
- Are aggregates available and healthy?
- Is ONTAP proxy service running?

**Fix:**
- Verify ONTAP cluster connectivity.
- Check aggregate health and capacity.
- Verify ONTAP proxy service is running.

#### Aggregate Capacity
**Check:**
- Do aggregates have sufficient capacity for the volume?
- Are constituent volume limits exceeded?

**Fix:**
- Check aggregate capacity and free up space if needed.
- Verify constituent volume limits are not exceeded.

#### ONTAP Operations
**Check:**
- Are ONTAP operations timing out?
- Are there ONTAP API errors?

**Fix:**
- Retry ONTAP operations if transient failures.
- Check ONTAP logs for detailed error information.
- Verify ONTAP API connectivity and authentication.

## Step 4: Check for Stuck or Partial State

### Volume Stuck in CREATING State

**Investigation Steps:**
1. **Query Job Status:**
   - Use API: `GET /v1beta/{name}/operations/{operation_id}`
   - Check job state in database: `SELECT state, error_details FROM jobs WHERE uuid = '<job_uuid>'`
   - **Query Temporal workflow:**
     - **Via Metrics:** Review workflow status metrics in monitoring project (workflow status distribution, execution duration).
     - **Via CLI:** Use `tctl --namespace <vcp-namespace> workflow describe --workflow-id <workflow_id>` to check workflow status and execution details.

2. **Inspect Workflow Execution:**
   - **Via Metrics:** Review workflow execution metrics in monitoring project (activity execution status, duration, error rates).
   - **Via CLI:** Use `tctl --namespace <vcp-namespace> workflow show --workflow-id <workflow_id>` to see detailed execution history.
   - Check which activity failed and why (from workflow history).
   - Look for timeout errors or retry exhaustion (check activity duration vs timeout metrics).

3. **Check ONTAP Operations:**
   - Review ONTAP logs for volume creation operations.
   - Check if volume was partially created in ONTAP.

4. **Verify Resource State:**
   - Check if volume was partially created in database.
   - Check if volume exists in ONTAP but not in database (orphaned state).
   - Identify which resources need cleanup.

**Remediation:**

**First, determine if this is a client-side or server-side issue:**
- **Client-side (400/409):** Inform customer - do not take internal action.
- **Server-side (500/503):** Apply internal remediation (see below).

**If Server-Side Error (500/503):**

- **If Workflow is Active:**
  - Wait for workflow to complete or timeout (30 minutes).
  - **Monitor progress:**
    - **Via Metrics:** Review workflow execution metrics in monitoring project (activity completion, duration trends).
    - **Via CLI:** Use `tctl --namespace <vcp-namespace> workflow describe --workflow-id <workflow_id>` periodically to check status.
  - Do not manually delete resources while workflow is active.

- **If Workflow is Stuck/Failed:**
  - **Identify the failed activity:**
    - **Via Metrics:** Review activity failure metrics in monitoring project (failed activity types, error rates).
    - **Via CLI:** Use `tctl --namespace <vcp-namespace> workflow show --workflow-id <workflow_id>` to see which activity failed.
  - **If safe and approved:** Retry the failed activity via CLI: `tctl --namespace <vcp-namespace> workflow signal --workflow-id <id> --signal-name retry`.
  - **If not safe:** Delete the volume to trigger cleanup and recreate (with change management approval).
  - Document the action taken and rationale.

- **If Resources are Orphaned:**
  - **With approval:** Manually clean up orphaned resources (database records, ONTAP volumes).
  - Delete the volume record from database.
  - Delete the volume from ONTAP if it exists.
  - Retry volume creation after cleanup.
  - Document cleanup actions for audit trail.

**If Client-Side Error (400/409):**
- **Do not take internal remediation actions.**
- Inform customer about the error and required corrections.
- Provide guidance on correct parameter values.
- Customer must resubmit with corrected parameters.

## Step 5: Escalate or Remediate

### Escalation Criteria
- Issue persists after following all troubleshooting steps.
- Root cause is unclear or requires code changes.
- Multiple volumes failing simultaneously (potential system-wide issue).
- Data loss or corruption risk.

### Information to Gather Before Escalation
- **Error Details:**
  - Full error message and stack trace.
  - Tracking ID and correlation ID.
  - Job UUID and workflow ID.
  - Pool ID and pool state.
- **Request Details:**
  - Complete request payload (sanitized).
  - Volume name, pool name, project, region, zone.
- **System State:**
  - Database volume and job records.
  - Temporal workflow execution details.
  - ONTAP volume status (if created).
- **Logs:**
  - API logs (google-proxy).
  - Orchestrator logs.
  - Workflow/activity logs (worker).
  - Database logs.
  - ONTAP proxy logs (if available).
- **Steps Taken:**
  - Document all troubleshooting steps attempted.
  - Note any temporary fixes applied.
  - Record any configuration changes made.

---

## Quick Reference: Common Symptoms & Fixes

| Symptom | Likely Cause | Mitigation |
|---------|--------------|------------|
| 400: "Invalid volume capacity" | Volume size out of range | Adjust size to 4.8 TiB - 2.48 PiB (or 20 PiB with AT) |
| 400: "Large capacity volumes can only be created in large capacity pools" | Pool is not large capacity | Create volume in a large capacity pool |
| 400: "SAN protocols are not supported" | SAN protocols specified | Remove SAN protocols, use NFS/SMB only |
| 400: "BlockDevices are not supported" | BlockDevices specified | Remove BlockDevices from request |
| 400: "Constituent volume size cannot be less than 100 GB" | CV size too small | Increase volume size or reduce CV count |
| 400: "Constituent volume count with [X] is not supported" | CV count is prime | Use non-prime CV count |
| 400: "Auto Tiering is not allowed" | Pool doesn't allow auto-tiering | Enable auto-tiering on pool first |
| 409: Volume exists | Duplicate resourceId | Use unique resourceId or reuse operation |
| 409: Pool not READY | Pool in wrong state | Wait for pool to be READY |
| 422: Insufficient pool capacity | Pool capacity exceeded | Increase pool size or free up space |
| 500: DB errors | Connectivity, schema | Escalate to DB team |
| Workflow not starting | Temporal/worker | Check Temporal, worker logs |
| Workflow timeout (30m) | Operation taking too long | Check ONTAP operations, consider increasing timeout |
| ONTAP cluster unavailable | Data plane issue | Check ONTAP cluster health |
| Insufficient aggregate capacity | Aggregate full | Free up space or use different aggregate |
| Constituent volume limit exceeded | CV limit reached | Reduce CV count or use different pool |

---

## Large Volume Specific Configuration Reference

### Size Limits
- **Minimum (without CV):** 4.8 TiB
- **Minimum (with CV):** 2.4 TiB
- **Maximum (no auto-tiering):** 2.48 PiB
- **Maximum (with auto-tiering):** 20 PiB

### Constituent Volume Limits
- **Minimum CV Size:** 100 GB
- **Maximum CV Size:** 300 TiB
- **CV Count:** Must not be prime (if ≥ minimum prime config), must not exceed per-aggregate/per-volume limits
- **Default CV Count:** 48 (8 per aggregate × 6 aggregates)

### Protocol Support
- **Supported:** NFS, SMB
- **NOT Supported:** SAN protocols (iSCSI, FC)

### Block Device Support
- **NOT Supported:** BlockDevices, BlockProperties

### Pool Requirements
- **Must be:** Large capacity pool (`largeCapacity: true`)
- **Must be:** In READY state
- **Must have:** Sufficient free capacity
- **Auto-Tiering:** Pool must have `allowAutoTiering: true` if volume auto-tiering is requested

### Infrastructure Requirements
- **Workflow Timeout:** 30 minutes (vs 10m for regular volumes)
- **Activity Timeout:** 5 minutes (heartbeat)

### Feature Flags
- `AUTO_TIERING_ENABLED` - Must be `true` for auto-tiering
- `ENABLE_MQOS` - Must be `true` for manual QoS

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
   tctl --namespace <vcp-namespace> workflow list --query 'WorkflowType="CreateVolumeWorkflow"'
   ```
   - Lists workflows matching the query
   - Use this to find workflows by type or status

4. **Query Workflow Status:**
   ```bash
   tctl --namespace <vcp-namespace> workflow query --workflow-id <workflow_id> --query-type "status"
   ```
   - Queries workflow for custom status information
   - Use this if workflow has a status query handler

5. **Signal Workflow (if needed):**
   ```bash
   tctl --namespace <vcp-namespace> workflow signal --workflow-id <workflow_id> --signal-name <SignalName> --input '"payload"'
   ```
   - Sends a signal to a running workflow
   - Use with caution and proper approval

6. **Cancel/Terminate Workflow (operator-only, use caution):**
   ```bash
   tctl --namespace <vcp-namespace> workflow cancel --workflow-id <workflow_id>
   tctl --namespace <vcp-namespace> workflow terminate --workflow-id <workflow_id> --reason "reason"
   ```
   - Use only with proper approval and change management
   - Document the reason for cancellation/termination

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

- **Worker Health Metrics:**
  - Worker count and health status
  - Task processing rate
  - Task queue depth
  - Worker heartbeat status

- **Task Queue Metrics:**
  - Queue depth by task queue name
  - Task processing rate
  - Task completion rate
  - Task failure rate

**How to Access:**
1. Navigate to GCP Monitoring project
2. Use metric explorer or create custom dashboards
3. Filter by:
   - `workflow_type` = "CreateVolumeWorkflow"
   - `job_type` = "CREATE_LARGE_VOLUME"
   - `workflow_id` = "<workflow_id>"
   - `correlation_id` = "<correlation_id>"

**Key Metrics to Monitor:**
- `temporal_workflow_execution_duration` - How long workflows take
- `temporal_workflow_status` - Current workflow status
- `temporal_activity_execution_status` - Activity success/failure
- `temporal_worker_health` - Worker availability
- `temporal_task_queue_depth` - Queue backlog

### Method 3: Database Queries

**Job Status:**
```sql
SELECT uuid, state, error_details, tracking_id, workflow_id, created_at, updated_at 
FROM jobs 
WHERE uuid = '<job_uuid>' OR workflow_id = '<workflow_id>';
```

**Volume and Pool Status:**
```sql
SELECT v.uuid, v.name, v.state, v.large_capacity, p.uuid as pool_id, p.name as pool_name, p.large_capacity as pool_large_capacity, p.state as pool_state
FROM volumes v
JOIN pools p ON v.pool_id = p.id
WHERE v.uuid = '<volume_uuid>';
```

**Workflow ID Lookup:**
- Workflow ID = Job UUID (from `jobs` table)
- Use workflow ID to query Temporal via CLI or metrics

## Operational Readiness Checklist

### Pre-Creation Verification
- [ ] Volume size is between 4.8 TiB and 2.48 PiB (or 20 PiB with auto-tiering)
- [ ] If using constituent volumes: size is ≥ 2.4 TiB, CV count is valid (non-prime, within limits)
- [ ] Target pool is a large capacity pool (`largeCapacity: true`)
- [ ] Pool is in READY state
- [ ] Pool has sufficient free capacity
- [ ] Protocols do NOT include SAN (NFS/SMB only)
- [ ] BlockDevices and BlockProperties are NOT specified
- [ ] If auto-tiering: pool allows auto-tiering, cooling threshold is 2-183 days
- [ ] `largeCapacity: true` is set in request

### Infrastructure Verification
- [ ] Temporal workflows and workers registered
- [ ] DB migrations applied and healthy
- [ ] ONTAP cluster accessible and healthy
- [ ] Aggregates have sufficient capacity
- [ ] Constituent volume limits not exceeded

### Configuration Verification
- [ ] Environment variables for large volume limits are set correctly
- [ ] `VOLUME_ACTIVITIES_START_TO_CLOSE_TIMEOUT_SEC_LV` is set to 1800s (30m) or appropriate value
- [ ] Feature flags are enabled as needed (`AUTO_TIERING_ENABLED`, `ENABLE_MQOS`)

---

**Tip:**  
For any error, always check the logs for the specific component (API, Orchestrator, Workflow, DB, ONTAP) and correlate with the operation/job ID and correlation ID for targeted debugging. Large volumes have stricter requirements than regular volumes (must be in large capacity pools, no SAN protocols, no block devices, longer timeouts), so ensure all parameters meet the large volume specifications.

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
    - `tctl workflow describe --workflow-id <id>` - Get workflow status
    - `tctl workflow show --workflow-id <id>` - Get detailed execution history
    - `tctl workflow list --query 'WorkflowType="CreateVolumeWorkflow"'` - List workflows
  - **Access:** Via local `tctl` or admintools pod: `kubectl exec -it deploy/admintools -- tctl ...`
* **Temporal Metrics:**
  - Available in monitoring project
  - Workflow execution metrics (status, duration, error rates)
  - Activity execution metrics (status, duration, retry counts)
  - Worker health metrics (worker count, heartbeat status)
  - Task queue metrics (queue depth, processing rate)
* **Troubleshooting Guide:** https://confluence.ngage.netapp.com/spaces/VSCP/pages/1273328576/Pool+Volume+CRUD+Operations+Troubleshooting+Guide


