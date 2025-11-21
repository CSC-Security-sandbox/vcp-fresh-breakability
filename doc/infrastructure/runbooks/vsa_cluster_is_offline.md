# Runbook: Resolving Cluster Offline Alert (ONTAP, Mediator, HA/Zonal Scenarios)

This runbook provides a structured approach to investigating and resolving **cluster offline alerts** in GCP ONTAP-based storage pools, including handling ONTAP node, Mediator, and HA/zonal failures.

---

## Debugging Steps (Guidelines)

### 1. Acknowledge the Alert
- **Action:** Acknowledge the alert in the monitoring system to prevent repeated notifications.
- **Record:** Note the time of acknowledgment for future reference.

---

### 2. Gather Initial Context
- **Review Alert Details:** Examine the alert description, severity, and any associated dashboards or logs.
- **Identify Impacted Cluster:** Note the pool/cluster UUID, region/zone, and associated project.
- **Check Recent Changes:** Review deployment logs, configuration changes, or infrastructure updates that may have impacted the cluster.

---

### 3. Validate the Alert
- **Confirm Legitimacy:** Ensure the alert is not a false positive by cross-referencing with system metrics and logs.
- **Check Cluster State:** Use the API (`V1betaDescribePool`) or DB to confirm the cluster’s state (`READY`, `CREATING`, `UPDATING`, `DELETING`, `OFFLINE`).
- **Review Monitoring Data:** Check for recent health check failures, missed heartbeats, or connectivity issues.

---

### 4. Identify the Root Cause

#### a. ONTAP Node Failure Scenarios

| Scenario | Symptoms | Immediate Actions | Resolution |
|----------|----------|------------------|------------|
| **Single Node HW Failure (Network, <10 min)** | IO suspended (<60s), takeover by partner after 15s, reboot, mirror resync | Wait for autoheal (auto reboot, resync, giveback) | No action needed unless autoheal fails; monitor for completion |
| **Single Node HW Failure (Network, >10 min)** | Same as above, but automatic giveback not done | Wait for autoheal, then schedule giveback via control plane | Schedule giveback manually if required |
| **Single Node Total SW Failure (panic, mitigated by reboot)** | IO suspended (<60s), takeover+reboot | Wait for ONTAP self-recovery | No action unless recovery fails |
| **Single Node Total SW Failure (not mitigated by reboot)** | IO suspended, repeated failures | Escalate to support; root cause analysis | Replace node if needed; submit support ticket |
| **Single Node Partial Failure** | Service impact, unclear recovery | Escalate to support | Case-by-case; submit support ticket |
| **Double Node Failure (Network, <10 min)** | Both nodes reboot, service unavailable | Wait for autoheal | Monitor for service resumption |
| **Double Node Failure (Network, >10 min)** | Both nodes reboot, service unavailable, manual giveback required | Wait for autoheal, schedule giveback | Manual intervention for giveback |
| **Double Node Total SW Failure (mitigated by reboot)** | Service unavailable 5-10 min | Wait for autoheal | Monitor for service resumption |
| **Double Node Total SW Failure (not mitigated by reboot)** | Service unavailable, repeated failures | Escalate to support | Root cause analysis, manual intervention |
| **Boot Volume Failure** | IO pause (<60s), takeover | Replace boot volume, swap disk | Manual procedure |
| **Single Data Volume Failure** | IO slowed, disk failed | Replace failed disk, resync | Manual procedure |

#### b. Mediator Failure Scenarios

| Scenario | Symptoms | Immediate Actions | Resolution |
|----------|----------|------------------|------------|
| **Network Transiency (<2 min)** | iSCSI disconnect, ONTAP disables HA | Wait for autoheal | No action unless another failure occurs |
| **Network Transiency (>2 min)** | Same as above | Wait for autoheal | No action unless another failure occurs |
| **Mediator HW/Software Failure (recoverable)** | Mediator stopped, ONTAP disables HA | Restart mediator (control plane) | Monitor for recovery |
| **Mediator SW Failure (not recoverable)** | Mediator does not function | Redeploy mediator, replace boot disk if needed | Manual procedure, root cause analysis |
| **Linux Security Issues** | No impact | Install new boot disk during upgrade | Routine maintenance |

#### c. HA/Zonal Failure Scenarios

| Scenario | Symptoms | Immediate Actions | Resolution |
|----------|----------|------------------|------------|
| **Double AZ Outage (MAZ)** | Both ONTAP nodes and mediator down, loss of quorum | Wait for service restoration | Manual intervention if autoheal not done |
| **Single AZ Outage (MAZ)** | One ONTAP node down, partner initiates takeover | Wait for autoheal | Schedule giveback if required |
| **Regional Network Outage** | All instances lose communication | Both nodes commit suicide, service unavailable | Manual intervention required |
| **Individual Network Interface Outage** | VNIC failure, specific service impact | Investigate vNIC, restore connectivity | May require node panic/takeover |
| **Zonal Outage (Primary/Secondary/Mediator)** | Node or mediator loses communication | Takeover, reboot, resync | Schedule giveback if not automatic |

---

### 5. Formulate a Hypothesis

- Based on the above, hypothesize the root cause:
    - Node hardware or software failure (transient or persistent)
    - Mediator failure (network or software)
    - Zonal or regional outage
    - Network interface failure
    - Boot/data volume failure

---

### 6. Implement a Solution/Mitigation

#### a. Temporary Mitigation
- **Wait for Autoheal:** For transient failures, ONTAP and mediator may auto-recover.
- **Manual Giveback:** If takeover is not automatically given back, schedule via control plane.
- **Restart Services:** Restart ONTAP node or mediator if stopped.
- **Failover:** If HA is enabled, ensure failover is functioning.

#### b. Permanent Fix
- **Replace Failed Hardware:** For persistent node or disk failures, replace hardware as per manual procedure.
- **Redeploy Mediator:** For unrecoverable mediator failures, redeploy with new boot disk and assign mailbox disks.
- **Root Cause Analysis:** For repeated or unexplained failures, escalate to support and submit a ticket.
- **Update Configuration:** Fix network/firewall issues, restore IAM permissions, or correct misconfigurations.

---

### 7. Verify the Fix
- **Monitor System:** Ensure the alert clears and the cluster is reported as `READY` and online.
- **Run Tests:** Execute health checks and basic volume operations to confirm cluster functionality.
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

## Common Cluster Offline Scenarios & Quick Fixes

| Symptom                                   | Probable Cause                                   | Mitigation Steps                                   |
|--------------------------------------------|--------------------------------------------------|----------------------------------------------------|
| Cluster not responding to API/health checks| Node HW/SW failure, network partition, firewall   | Wait for autoheal; restart node; replace hardware  |
| Cluster stuck in takeover                  | Manual giveback required                         | Schedule giveback via control plane                |
| Mediator failure                          | Network or software failure                      | Restart or redeploy mediator                       |
| Double node failure                       | Zonal/regional outage, loss of quorum            | Wait for autoheal; manual intervention if needed   |
| Boot/data volume failure                   | Disk failure                                     | Replace disk; resync; manual procedure             |
| Persistent failure                        | Root cause unknown                               | Escalate to support; submit ticket                 |

---

## Example Troubleshooting Flow

1. **Alert:** Cluster offline detected at 11:00 UTC.
2. **Acknowledge:** Mark alert as acknowledged at 11:01 UTC.
3. **Context:** Review logs; ONTAP node rebooted, mediator unreachable.
4. **Validate:** Confirm cluster is in takeover state, giveback not automatic.
5. **Root Cause:** Network outage >10 min, mediator failure.
6. **Mitigation:** Restore network, restart mediator, schedule giveback via control plane.
7. **Verify:** Cluster comes back online; alert clears.
8. **Document:** Record outage and fix; update runbook; notify team.

---

## Notes

- For persistent or unexplained failures, always escalate to support and submit a ticket.
- Use ONTAP and mediator logs to pinpoint hardware, software, or network failures.
- Ensure all feature flags and configuration files are correctly set for the intended cluster type and region.
- Confirm the referenced pool/cluster is in `READY` state and all nodes are healthy.

---

**End of Runbook**