# Runbook: Resolving VCP Internal Server Errors (HTTP 5xx)

This runbook provides a structured approach to investigating and resolving **VCP internal server errors (HTTP 5xx)** in GCP ONTAP-based storage pools, including ONTAP node, Mediator, and HA/zonal failure scenarios.

---

## Debugging Steps (Guidelines)

### 1. Acknowledge the Alert
- **Action:** Acknowledge the alert in the monitoring system to prevent repeated notifications.
- **Record:** Note the time of acknowledgment for future reference.

---

### 2. Gather Initial Context
- **Review Alert Details:** Examine the alert description, severity, and any associated dashboards or logs.
- **Identify Impacted Component:** Determine which API endpoint, workflow, or cluster/pool is affected.
- **Check Recent Changes:** Review deployment logs, configuration changes, or infrastructure updates that may have impacted the system.

---

### 3. Validate the Alert
- **Confirm Legitimacy:** Ensure the alert is not a false positive by cross-referencing with system metrics and logs.
- **Check Error Frequency:** Determine if the error is transient or persistent, and if it affects multiple requests or a specific operation.

---

### 4. Identify the Root Cause

#### a. Logs Analysis
- **API Layer:** Check logs for stack traces, panic messages, or unhandled exceptions in endpoints (e.g., `pool_endpoints.go`, `volume_endpoint.go`).
- **Orchestrator/Workflow:** Review orchestrator and workflow logs for failed activities, workflow start failures, or DB transaction errors.
- **ONTAP/Node Logs:** If the error relates to cluster operations, check ONTAP node logs for hardware/software failures, reboots, or panic events.
- **Mediator Logs:** For HA-related errors, review mediator logs for connectivity or service failures.

#### b. Metrics Review
- **Resource Utilization:** Examine CPU, memory, disk I/O, and network metrics for VCP workers, ONTAP nodes, and mediator instances.
- **Workflow Metrics:** Check Temporal workflow status, activity durations, and error rates.

#### c. System Health Check
- **Temporal:** Ensure Temporal workers are running and task queues are healthy.
- **Database:** Verify DB connectivity, schema migrations, and performance.
- **Cluster State:** Use `V1betaDescribePool` or `V1betaDescribeVolume` to confirm cluster/pool/volume states.

#### d. Dependency Check
- **GCP Services:** Confirm required APIs (Compute, IAM, Storage, DNS, Service Networking) are enabled and quotas are sufficient.
- **Networking:** Validate VPC, subnet, and firewall configurations for the cluster and mediator.
- **IAM/Service Accounts:** Ensure service accounts have not lost required permissions.

#### e. Failure Scenarios Mapping

| Symptom | Possible Root Cause | Diagnostic Steps |
|---------|--------------------|------------------|
| 5xx on pool/volume create/update/delete | DB transaction failure, workflow start failure, ONTAP node offline, mediator failure, network partition | Check DB logs, orchestrator logs, ONTAP/mediator health, network status |
| 5xx on describe/list APIs | DB connectivity, cluster state unknown, node/mediator unreachable | Check DB, cluster, and mediator logs; verify network connectivity |
| 5xx after takeover or reboot | Manual giveback required, node/mediator not autohealed | Check ONTAP logs, control plane status, schedule giveback if needed |
| 5xx after persistent failures | Hardware/software failure not mitigated by reboot, root cause unresolved | Escalate to support, submit ticket, replace hardware if needed |

---

### 5. Formulate a Hypothesis

- Based on the above, hypothesize the root cause:
    - DB transaction or connectivity failure
    - Workflow engine (Temporal) failure
    - ONTAP node hardware/software failure (transient or persistent)
    - Mediator failure (network or software)
    - Zonal/regional outage or network partition
    - Misconfiguration, quota exhaustion, or IAM permission loss

---

### 6. Implement a Solution/Mitigation

#### a. Temporary Mitigation
- **Retry Operation:** For transient errors (e.g., DB or network blips), retry the failed request.
- **Restart Services:** Restart ONTAP node, mediator, or VCP worker if stopped or hung.
- **Manual Giveback:** If cluster is stuck in takeover, schedule giveback via control plane.
- **Failover:** If HA is enabled, ensure failover is functioning.

#### b. Permanent Fix
- **Resolve DB Issues:** Fix schema, migrations, or connectivity problems.
- **Fix Workflow Engine:** Restart Temporal workers, fix task queue issues, or re-register workflows.
- **Replace Failed Hardware:** For persistent node or disk failures, replace hardware as per manual procedure.
- **Redeploy Mediator:** For unrecoverable mediator failures, redeploy with new boot disk and assign mailbox disks.
- **Root Cause Analysis:** For repeated or unexplained failures, escalate to support and submit a ticket.
- **Update Configuration:** Fix network/firewall issues, restore IAM permissions, or correct misconfigurations.

---

### 7. Verify the Fix
- **Monitor System:** Ensure the alert clears and the API returns 2xx responses.
- **Run Tests:** Execute pool/volume operations and health checks to confirm functionality.
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

## Common VCP 5xx Error Scenarios & Quick Fixes

| Symptom                                   | Probable Cause                                   | Mitigation Steps                                   |
|--------------------------------------------|--------------------------------------------------|----------------------------------------------------|
| 5xx on create/update/delete                | DB/workflow failure, ONTAP/mediator offline      | Check DB, workflow, ONTAP, mediator; retry or fix  |
| 5xx after takeover                        | Manual giveback required                         | Schedule giveback via control plane                |
| 5xx after reboot/panic                    | Node/mediator not autohealed                     | Restart node/mediator; replace hardware if needed  |
| 5xx after persistent failures              | Hardware/software failure, root cause unresolved | Escalate to support; submit ticket                 |
| 5xx on describe/list                      | DB connectivity, cluster state unknown           | Fix DB, check cluster/mediator health              |

---

## Example Troubleshooting Flow

1. **Alert:** VCP 5xx error detected at 14:00 UTC.
2. **Acknowledge:** Mark alert as acknowledged at 14:01 UTC.
3. **Context:** Review logs; error on volume create API; ONTAP node recently rebooted.
4. **Validate:** Confirm DB and workflow health; ONTAP node in takeover state, giveback not automatic.
5. **Root Cause:** Node failure, manual giveback required.
6. **Mitigation:** Schedule giveback via control plane; restart node if needed.
7. **Verify:** API returns 2xx; alert clears.
8. **Document:** Record outage and fix; update runbook; notify team.

---

## Notes

- For persistent or unexplained 5xx errors, always escalate to support and submit a ticket.
- Use ONTAP, mediator, and DB logs to pinpoint hardware, software, or network failures.
- Ensure all feature flags and configuration files are correctly set for the intended cluster type and region.
- Confirm the referenced pool/cluster is in `READY` state and all nodes are healthy.

---

**End of Runbook**