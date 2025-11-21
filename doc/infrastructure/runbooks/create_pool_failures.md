# Runbook for create_pool_failures

This runbook template provides a structured approach to debugging alerts, ensuring consistent and efficient resolution.

# Alert Information

| Field               | Description                                                                                   |
| :-----------------: | :------------------------------------------------------------------------------------------: |
| Alert Name          | create_pool_failures                                                                         |
| Alert Link          | [GCP Monitoring Alert](https://console.cloud.google.com/monitoring/alerting/policies/10341008569341281121?project=vsa-monitoring-au-se1-ap-tst) |
| Alert Threshold     | Above 1                                                                                      |
| Date of Creation    | 28 Aug 2025                                                                                  |
| SME                 | The Subject Matter Expert responsible for this alert or system.                              |
| Severity            | Error                                                                                        |

# Debugging Steps (Guidelines)

1. **Acknowledge the Alert:**
    - Acknowledge the alert in the monitoring system to prevent repeated notifications.
    - Record the time of acknowledgment.

2. **Gather Initial Context:**
    - Review the alert description and any associated dashboards or logs.
    - Identify the affected pool, project, and operation/job ID.
    - Check recent deployments or configuration changes related to pool creation.

3. **Validate the Alert:**
    - Confirm that the alert is legitimate and not a false positive (e.g., verify pool is actually stuck or error is real).
    - Verify the API response, DB state, and workflow/job status.

4. **Identify the Root Cause:**
    - **Logs Analysis:**
        - Review API, orchestrator, workflow, and DB logs for errors or anomalies around the alert time.
        - Check for GCP operation errors in the GCP console.
    - **Metrics Review:**
        - Examine related metrics (API error rates, workflow success rates, DB health).
    - **System Health Check:**
        - Check DB connectivity, Temporal workflow status, and GCP resource quotas.
    - **Dependency Check:**
        - Investigate GCP APIs (Service Networking, Compute, IAM, Storage, DNS, Secret Manager, KMS).
        - Confirm service account permissions and SN host project configuration.
    - **Configuration Review:**
        - Validate pool creation parameters (`type`, `location`, `size`, `throughput`, `iops`, feature flags).
        - Ensure required environment flags are set (`REGIONAL_SUPPORT_ENABLED`, `AUTO_TIERING_ENABLED`).

5. **Formulate a Hypothesis:**
    - Based on the gathered information, develop a hypothesis about the potential root cause (e.g., invalid API payload, DB error, GCP quota issue, workflow failure).

6. **Implement a Solution/Mitigation:**
    - **Temporary Mitigation:**
        - Retry the failed workflow step if safe.
        - Increase GCP operation timeouts or quotas if needed.
        - Clean up stuck resources and attempt pool creation again.
    - **Permanent Fix:**
        - Correct invalid request parameters or configuration.
        - Fix DB schema/migration issues.
        - Update service account permissions or enable required GCP APIs.
        - Address workflow registration or Temporal worker issues.

7. **Verify the Fix:**
    - Monitor the system to ensure the alert is resolved and the pool transitions to `READY`.
    - Run tests or synthetic pool creation to confirm the fix is effective.

8. **Document the Resolution:**
    - Record the root cause, the steps taken to resolve the issue, and any lessons learned.
    - Update the runbook if new insights or steps are discovered during the debugging process.

---

# Pool Creation Error Debugging Detailed Runbook

## Step 1: Collect Failure Details
- Record the error message from the API or UI.
- Note the operation/job ID returned by the API.
- Capture the request payload (parameters sent to the API).
- Check the pool state in the DB (should be `CREATING` or error).

## Step 2: Identify Where the Failure Occurred
- **API/Validation:** 400/409 errors, immediate response.
- **Orchestrator/DB:** 500 errors, job not created, or DB errors in logs.
- **Workflow/Temporal:** Job created, but pool stuck in `CREATING`.
- **GCP Resource Provisioning:** Timeouts, quota, or permission errors in logs.

## Step 3: Troubleshoot by Component

### A. API/Validation Errors (400/409)
- Check request parameters:
    - Is `type=UNIFIED` (or `unified=true`) for unified pools?
    - No AD/LDAP fields for unified pools.
    - For regional pools: is `REGIONAL_SUPPORT_ENABLED=true`? Are `zone` and `secondaryZone` both set and different?
    - Is `size` within allowed min/max and a multiple of 1GiB?
    - Is `throughput`/`iops` within allowed range? (IOPS ≥ 16×throughput)
    - Are auto-tiering fields only set if `AUTO_TIERING_ENABLED=true`?
- If 409 (conflict):
    - Pool with same vendorId exists. If in `CREATING`, reuse operation; else, use a unique resourceId.

### B. Orchestrator/DB Errors (500, job not created, DB errors)
- Check DB connectivity and health.
- Review DB logs for unique constraint or migration errors.
- Retry after transient DB issues.
- Check whether the DB error was transient error
- Check if there was a rety in the code

### C. Workflow/Temporal Startup Errors
- Check if Temporal is reachable and the worker is running.
- Ensure the correct workflows are registered (`CreatePoolWorkflow`).
- Check task queue names and permissions.
- Review Temporal and worker logs for errors.

### D. GCP Resource Provisioning (timeouts, permission errors)
- **Tenancy/Subnet:**
    - Service Networking API enabled?
    - SN host project and permissions correct?
    - Increase `WAIT_TIME_FOR_GCP_OPERATION_IN_SEC` if timeouts.
- **VPC/Subnet/Firewall:**
    - GCP quotas sufficient?
    - Required APIs enabled?
    - Firewall rules/names not conflicting?
- **IAM/Service Account:**
    - Service account has required roles (e.g., `serviceAccountAdmin`, `projectIamAdmin`)?
    - Org policy allows creation?
- **GCS Bucket (Auto-Tiering):**
    - `AUTO_TIERING_ENABLED=true`?
    - Storage API enabled?
    - Project billing active?
- **Secrets (if used):**
    - Secret Manager API enabled?
    - Service account has access?
- **VMRS/VLM:**
    - Config file valid?
    - Quotas and instance types available?
    - Service account bindings correct?
- **ONTAP Version Fetch:**
    - Network reachability from worker to ONTAP nodes?
    - Credentials and certificates valid?
- **KMS (if used):**
    - KMS config present and correct?
    - Service account impersonation works?
    - DNS/firewall for KMS access?

## Step 4: Check for Stuck or Partial State
- Is the pool stuck in `CREATING`?
    - Query job status via API or Temporal UI.
    - Inspect workflow logs for failed activities.
    - Check underlying GCP operation status in the GCP console.
    - If safe, retry failed workflow step or delete pool to trigger cleanup and recreate.

## Step 5: Escalate or Remediate
- If the issue is not resolved:
    - Gather all logs, operation/job IDs, and error messages.
    - Document steps already taken.
    - Escalate to the next support tier or engineering with full context.

---

## Quick Reference: Common Symptoms & Fixes

| Symptom                        | Likely Cause                  | Mitigation                                 |
| ------------------------------ | ----------------------------- | ------------------------------------------- |
| 400 on create (type/AD/LDAP)   | Invalid params                | Fix request: type=UNIFIED, no AD/LDAP      |
| 400 on regional pool           | REGIONAL_SUPPORT_ENABLED or zone error | Set flag, provide correct zones      |
| 400 on size/throughput/iops    | Out of bounds                 | Adjust to allowed values                    |
| 409 Pool exists                | Duplicate vendorId            | Use unique resourceId                       |
| DB errors                      | Connectivity, schema          | Check DB, retry after fix                   |
| Workflow not starting          | Temporal/worker               | Check Temporal, worker logs                 |
| GCP op timeout                 | API/perm/quota                | Check GCP console, quotas, APIs             |
| SA/IAM errors                  | Permissions                   | Grant required roles                        |
| GCS/Secret errors              | API/billing                   | Enable APIs, check billing                  |
| Pool stuck in CREATING         | Workflow failure              | Inspect logs, retry or cleanup              |

---

## Operational Readiness Checklist

- Required GCP APIs enabled (Service Networking, Compute, IAM, Storage, DNS, Secret Manager, KMS).
- Service accounts have correct roles.
- SN host project configured.
- Temporal workflows and workers registered.
- DB migrations applied and healthy.

---

**Tip:**  
For any error, always check the logs for the specific component (API, Orchestrator, Workflow, DB) and correlate with the operation/job ID for targeted debugging.

---

# Useful Tools and Resources

* Monitoring System: [File](#)
* Logging Platform: [File](#)
* Documentation Wiki: [File](#)
https://confluence.ngage.netapp.com/spaces/VSCP/pages/1273328576/Pool+Volume+CRUD+Operations+Troubleshooting+Guide
* Team Communication Channel: [File](#)