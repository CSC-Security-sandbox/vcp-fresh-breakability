# Runbook: Debugging `vsa_node_is_offline` Alert

---

## 1. **Acknowledge the Alert**

### Steps:
1. Open [Google Cloud Monitoring](https://console.cloud.google.com/monitoring/alerts?project=vsa-monitoring-au-se1-ap-tst).
2. Locate the `vsa_node_is_offline` alert in the alert list.
3. Click on the alert to open details.
4. Click “Acknowledge” to prevent duplicate notifications.
5. Record your name and the time of acknowledgment in your incident tracking system (e.g., Jira, PagerDuty).

---

## 2. **Gather Initial Context**

### Steps:
1. Review the alert details:
   - Node name (e.g., `ontap-node-1`)
   - Zone (e.g., `us-central1-a`)
   - Timestamp
   - Pool/cluster association
2. Open relevant dashboards in Google Cloud Monitoring or your ONTAP monitoring tool.
   - Check node status, recent failures, and cluster health.
3. Review recent changes:
   - In GCP Console, go to **Activity** and filter for the last 2 hours.
   - Look for VM restarts, disk detach/attach, firewall changes, or software deployments.
   - Ask your team about any manual interventions or scheduled maintenance.

---

## 3. **Validate the Alert**

### Steps:
1. Confirm there was no scheduled maintenance or known test.
2. Check VM status in GCP:
   - Go to **Compute Engine > VM Instances**.
   - Search for the node VM (e.g., `ontap-node-1`).
   - Check the status column for `RUNNING`, `TERMINATED`, `STOPPING`, or `PROVISIONING`.
3. Check node status in ONTAP:
   - SSH into the ONTAP management LIF or use System Manager.
   - Run:
     ```
     storage failover show
     node show
     ```
     Example output:
     ```
     Node           Partner        Possible State Description
     -------------- ------------- -------- ----------------------
     ontap-node-1   ontap-node-2  true      Waiting for giveback
     ontap-node-2   ontap-node-1  false     In takeover
     ```
     ```
     Node           Health  Eligibility
     -------------- ------- -----------
     ontap-node-1   false   true
     ontap-node-2   true    true
     ```
4. Check Temporal workflow/job logs:
   - Use your Temporal UI or logs to search for workflows related to the pool or node.
   - Look for failed activities or stuck jobs.

---

## 4. **Identify the Root Cause**

### a. **Logs Analysis**

1. ONTAP logs:
   - SSH to the ONTAP node (if reachable) or use System Manager.
   - Run:
     ```
     rows -num 50 -file /mroot/etc/log/ems.log
     rows -num 50 -file /mroot/etc/log/messages
     ```
   - Look for entries like `panic`, `takeover`, `disk failure`, or `network unreachable`.
2. GCP serial port logs:
   - In GCP Console, go to **Compute Engine > VM Instances**.
   - Click the node VM.
   - Click **Serial port 1 (console)**.
   - Look for boot errors, kernel panics, or disk errors.
3. GCP Operations/Logs Explorer:
   - Go to **Logging > Logs Explorer**.
   - Filter by `resource.type="gce_instance"` and the node name.
   - Look for `compute.instances.terminated`, `compute.instances.stopped`, or `compute.instances.restarted`.

### b. **Metrics Review**

1. Resource utilization:
   - Check for spikes or drops in CPU, memory, disk, or network metrics.
2. HA status:
   - In ONTAP CLI:
     ```
     storage failover show
     ```
   - Confirm which node is active and if takeover occurred.

### c. **System Health Check**

1. Node reachability:
   - Try SSH:
     ```
     ssh admin@<node-management-ip>
     ```
   - If unreachable, check GCP VM status and network/firewall rules.
2. Partner node status:
   - In ONTAP CLI, check if the partner node is up and has taken over.

### d. **Dependency Check**

1. Mediator status:
   - In GCP Console, go to **Compute Engine > VM Instances**.
   - Search for the mediator VM (e.g., `ontap-mediator-1`).
   - Check status is `RUNNING`.
   - In ONTAP CLI:
     ```
     storage mediator show
     ```
     Example output:
     ```
     Mediator: up
     ```
2. Network health:
   - In GCP Console, go to **VPC network > Firewall**.
   - Check for recent changes or rules blocking traffic.
   - Use **VPC network > Connectivity Tests** to verify connectivity.
3. GCP quotas:
   - Go to **IAM & Admin > Quotas**.
   - Filter for Compute Engine quotas.
   - Ensure you have not hit limits for CPUs, disks, or networks.

### e. **Configuration Review**

1. Node configuration:
   - In GCP Console, click the node VM.
   - Under **Disks**, check boot and data disk status.
2. HA settings:
   - In ONTAP CLI:
     ```
     storage failover show
     ```
   - Confirm both nodes are eligible and HA is enabled.

---

## 5. **Formulate a Hypothesis**

### Examples:
- **Primary Zone Failure:** Partner node is up and has taken over. Expect IO pause <60s, then recovery.
- **Secondary Zone Failure:** Primary node is up, but HA is degraded.
- **Mediator Zone Failure:** Both nodes up, but `storage mediator show` reports `down`. HA disabled.
- **Multiple Zone Failure:** Both nodes down. Service unavailable.
- **Disk Failure:** GCP disk status is `FAILED` or logs show disk errors.
- **Network Partition:** Node unreachable, but VM is running; firewall or VPC issue.

---

## 6. **Implement a Solution/Mitigation**

### a. **Temporary Mitigation**

1. **Failover:**
   - If partner node is healthy, ensure takeover is complete.
   - In ONTAP CLI:
     ```
     storage failover show
     ```
   - If not, manually initiate takeover:
     ```
     storage failover takeover -ofnode <offline-node>
     ```
2. **Restart Node:**
   - In GCP Console, select the node VM.
   - Click **Restart**.
3. **Restore Mediator:**
   - If mediator VM is down, select it in GCP Console and click **Start**.
   - After boot, in ONTAP CLI:
     ```
     storage mediator show
     ```
     Confirm status is `up`.
4. **Manual Giveback:**
   - If takeover occurred but giveback did not:
     ```
     storage failover giveback -fromnode <partner-node>
     ```

### b. **Permanent Fix**

1. **Replace Failed Disk:**
   - In GCP Console, click the node VM.
   - Under **Disks**, note the failed disk.
   - Detach the failed disk.
   - Create a new disk from snapshot or image.
   - Attach new disk to VM.
   - Restart VM.
2. **Fix Network/Firewall:**
   - In **VPC network > Firewall**, correct any rules blocking traffic.
   - In **VPC network > Connectivity Tests**, verify connectivity.
3. **Redeploy Mediator:**
   - If mediator is unrecoverable, create a new VM from the mediator image.
   - Assign the same IP.
   - In ONTAP, reconfigure mediator:
     ```
     storage mediator remove
     storage mediator add -address <new-mediator-ip>
     ```
4. **Update ONTAP Config:**
   - If configuration is incorrect, update settings and reboot if necessary.

---

## 7. **Verify the Fix**

### Steps:
1. **Monitor Node Status:**
   - In GCP Console, VM status should be `RUNNING`.
   - In ONTAP CLI:
     ```
     storage failover show
     node show
     ```
     Both nodes should be `true` for health and eligibility.
2. **Check HA State:**
   - In ONTAP CLI:
     ```
     storage failover show
     ```
     Both nodes should be possible and not in takeover.
3. **Alert Clearance:**
   - In Google Cloud Monitoring, confirm the alert is cleared.
4. **Run IO/Failover Tests:**
   - Use ONTAP CLI or System Manager to create a test volume and perform IO.
   - Optionally, simulate a failover to confirm recovery.

---

## 8. **Document the Resolution**

### Steps:
1. **Record Details:**
   - In your incident tracker (e.g., Jira), record:
     - Root cause
     - Steps taken
     - Resolution
     - Time to resolution
2. **Lessons Learned:**
   - Note any improvements for future incidents.
3. **Update Runbook:**
   - Add new troubleshooting steps as needed.

---

## Escalation

- If unable to resolve, escalate to the SME or open a support ticket.
- Attach:
  - All logs, commands run, and timeline of actions.

---

**End of Runbook**