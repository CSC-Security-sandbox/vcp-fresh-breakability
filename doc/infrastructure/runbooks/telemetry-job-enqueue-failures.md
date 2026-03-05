This runbook guides investigation and resolution when the **Job Enqueue Failures** alert fires: elevated failures while enqueueing jobs to the Telemetry job queue (e.g. \> 10 failures in 5 minutes).
## Alert Information

|  |  |
| ----- | :---- |
| **Alert Name** | Job Enqueue Failures (Telemetry) |
| **Severity** | Critical |
| **Threshold** | \> 10 failures in 5 minutes |
| **Duration** | 5 minutes |
| **Expected Response** | 15 minutes |
| **Notification Channels** | PagerDuty, Slack |
| **Primary Metrics** | `vcp_telemetry_jobs_enqueued_total` (labels: queue, job\_type, status), `vcp_telemetry_jobs_batch_enqueued_total` (labels: queue, status) |

## Related Metrics

|  |  |
| :---- | :---- |
| `vcp_telemetry_jobs_enqueued_total` | Single-job enqueue count by queue, job\_type, status. Filter status for failures. |
| `vcp_telemetry_jobs_batch_enqueued_total` | Batch enqueue count by queue, status (e.g. success, enqueue\_batch\_postgres\_failed). |

**Code reference:** `telemetry/utils/pgjobs.go` (Enqueue, EnqueueBatch) records via `RecordJobEnqueued` and `RecordJobBatchEnqueued`; `telemetry/api/endpoints/endpoint.go` enqueues jobs for Performance, Usage, GenerateReport.

## Debugging Steps

### **1\. Acknowledge the Alert**

* Acknowledge in GCP Cloud Monitoring and PagerDuty. Assign owner; expected response 15 minutes.

### **2\. Gather Initial Context**

* **Dashboard:** Open Telemetry Job Queue Health. Check enqueue rate and failure count by queue and job\_type.  
* **Time window:** Note the 5-minute window when failures exceeded the threshold.  
* **Queues affected:** Identify which queues show enqueue failures (e.g. performance, usage, bizops, billing\_retry, collection).  
* **Recent changes:** Review recent deployments, DB migrations, or config changes affecting Telemetry or the metrics DB.

### **3\. Validate the Alert**

* **Query (example):** `sum(increase(vcp_telemetry_jobs_enqueued_total{status!="success"}[5m])) > 10` or equivalent for batch enqueue failures.  
* Confirm failures are real (check application logs for enqueue errors) and not a metric mislabel.

### **4\. Identify Root Cause**

#### **a. Database (Telemetry DB)**

* **Connectivity:** Telemetry job queue uses the Telemetry DB (PostgreSQL). Enqueue \= INSERT into `jobs` table. Failures often indicate DB unreachable, timeout, or transaction failure.  
* **Checks:** DB connectivity from Telemetry pods; DB CPU/memory/connections; slow queries or locks on `jobs` table; schema/migrations applied.  
* **Logs:** Search Telemetry service logs for "failed inserting job", "failed to begin transaction", or DB driver errors in the alert window.

#### **b. Queue and Job Type**

* **By queue:** Failures on one queue (e.g. usage) may point to a burst of requests to that API; failures on all queues point to a shared cause (usually DB).  
* **By job type:** Check if a specific job type (e.g. ProcessPerformanceMetrics, ProcessUsageMetrics, BizOpsReport) is failing; could indicate marshaling errors or type registration issues (see `telemetry/utils/pgjobs.go` type registry).

#### **c. Marshaling and Validation**

* **Enqueue path:** Jobs are JSON-marshaled before insert. Marshal errors (e.g. unsupported type) are recorded and can contribute to "failure" status depending on how the metric is labeled.  
* **Logs:** Look for "failed marshaling" or serialization errors in Telemetry logs.

#### **d. Symptom → Cause Mapping**

|  |  |  |
| :---- | :---- | :---- |
| Enqueue failures on all queues | Telemetry DB down, timeout, or connection pool exhausted | Check DB health, connectivity, pool size, and Telemetry DB client config |
| Failures only on batch enqueue | PostgreSQL array/batch path failing; fallback to single-row insert may still succeed | Check batch enqueue status (e.g. enqueue\_batch\_postgres\_failed); DB compatibility and driver |
| Failures after deployment | Bad DB config, wrong DB URL, or migration failure | Compare env and config with last good deployment; verify migrations |
| Intermittent failures | DB overload, network blips, or transient lock contention | Check DB load and Telemetry request rate; consider retries and backoff |

### **5\. Implement Solution / Mitigation**

* **DB connectivity:** Restore DB connectivity; fix network/firewall; restart Telemetry if DB client is stuck.  
* **DB capacity:** Scale DB or tune connection pool; resolve slow queries or locks.  
* **Config/code:** Fix wrong DB URL or env; fix marshaling or type registration; roll back bad deployment if needed.  
* **Temporary:** If DB is read-only or under maintenance, acknowledge impact (enqueue will fail until DB is writable); communicate to stakeholders.

### **6\. Verify**

* Confirm enqueue failure rate drops to zero (or below threshold) and alert clears.  
* Trigger a few API calls that enqueue jobs (e.g. POST /v1/performance) and verify jobs appear in the queue and metrics show success.

### **7\. Document**

* Record root cause, actions taken, and any runbook updates. Share in Slack and post-mortem if needed.

## Useful Resources

|  |  |
| :---- | :---- |
| Job queue implementation | `telemetry/utils/pgjobs.go` |
| API handlers that enqueue | `telemetry/api/endpoints/endpoint.go` |
| Main (workers and queues) | `telemetry/main.go` |

