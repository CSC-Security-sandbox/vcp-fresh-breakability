This runbook guides investigation and resolution when the **Queue Backlog Growth** alert fires: sustained queue build-up caused by enqueue rate exceeding dequeue rate by more than 2x for 15 minutes.
## Alert Information

|  |  |
| ----- | :---- |
| **Alert Name** | Queue Backlog Growth (Telemetry) |
| **Severity** | Warning |
| **Threshold** | Enqueue rate exceeds dequeue rate by more than 2x |
| **Duration** | 15 minutes |
| **Expected Response** | 1 hour |
| **Notification Channels** | Slack |
| **Primary Metrics** | `vcp_telemetry_jobs_enqueued_total`, `vcp_telemetry_jobs_dequeued_total` (by queue) |

## Related Metrics

|  |  |
| :---- | :---- |
| `vcp_telemetry_jobs_enqueued_total` | Enqueued count by queue, job\_type, status. Use rate to compare with dequeue. |
| `vcp_telemetry_jobs_dequeued_total` | Dequeued count by queue, job\_type. Use rate for comparison. |
| `vcp_telemetry_jobs_processed_total` | Processed count by job\_type, queue, status; indicates throughput. |
| `vcp_telemetry_jobs_batch_enqueued_total` | Batch enqueue operations by queue, status. |

**Code reference:** Enqueue in `telemetry/utils/pgjobs.go` (Enqueue, EnqueueBatch); Dequeue in same file. Workers in `telemetry/main.go` run Dequeue for each queue (performance, usage, collection, bizops, billing\_retry). API endpoints enqueue jobs; workers dequeue and process.

## Debugging Steps

### **1\. Acknowledge the Alert**

* Acknowledge in GCP Cloud Monitoring. Assign owner; expected response 1 hour.

### **2\. Gather Initial Context**

* **Dashboard:** Open Telemetry Job Queue Health. Compare enqueue vs dequeue rates by queue over the 15-minute window.  
* **Queues affected:** Identify which queue(s) show enqueue rate \> 2x dequeue rate (e.g. usage, performance, collection, bizops, billing\_retry).  
* **Recent changes:** Review recent deployments, config, traffic changes, or new job types that could increase enqueue volume or slow workers.

### **3\. Validate the Alert**

* **Query (example):** Compare `sum(rate(vcp_telemetry_jobs_enqueued_total[15m])) by (queue)` with `sum(rate(vcp_telemetry_jobs_dequeued_total[15m])) by (queue)`.  
* Confirm the imbalance is real and sustained (not a brief spike). Check if backlog is growing in the jobs table (if queryable) or via processed vs enqueued delta.

### **4\. Identify Root Cause**

#### **a. By Queue**

* **Usage queue:** High API volume for usage ingestion; worker count or performance may be insufficient. Check ProcessUsageMetrics job duration and sink latency.  
* **Performance queue:** High volume of performance metric requests; processor or Google API may be slow. Check ProcessPerformanceMetrics and performance sink.  
* **Collection queue:** CollectMetrics jobs piling up; Google monitoring API or collector may be slow or failing.  
* **BizOps queue:** BizOpsReport jobs backing up; GCS or BizOps sink may be slow or failing.  
* **Billing\_retry queue:** ProcessBillingSubmission retries accumulating; billing/usage sink or Google billing API issues.

#### **b. Symptom → Cause Mapping**

|  |  |  |
| :---- | :---- | :---- |
|  |  |  |
| Enqueue rate suddenly high | Traffic spike, new integration, or misconfigured client | API request metrics; recent config or client changes |
| Dequeue rate low or flat | Workers down, scaled to zero, or crashing | Worker pod count, restarts, logs; K8s HPA |
| Dequeue rate normal but enqueue much higher | Sustained high load; need more workers | Compare historical enqueue rates; consider scaling workers |
| One queue affected only | Job type or sink specific (slow sink, failing job) | Job processing duration and failure rate for that queue; sink latency/errors |
| Workers slow (high job duration) | DB slow, external API slow, or CPU/memory pressure | DB and external API latency; worker resource metrics |

#### **c. Logs and Metrics**

* **Workers:** Check telemetry worker logs for errors, restarts, or long-running jobs. Confirm worker replica count and that pods are ready.  
* **Job duration:** Use `vcp_telemetry_jobs_processed_total` and application logs to see if jobs are taking longer than usual.  
* **Database:** Telemetry DB used for job queue; check connection pool, query latency, and locks (e.g. on jobs table).

### **5\. Implement Solution / Mitigation**

* **Scale workers:** Increase replica count for the affected queue(s) (e.g. HPA or manual scale) to raise dequeue rate. Prefer scaling out rather than over-provisioning a single pod.  
* **Fix worker instability:** If workers are crashing or OOMKilled, fix the root cause (bug, memory leak, or resource limits) and ensure restarts are healthy.  
* **Reduce enqueue rate (if inappropriate):** If a client or integration is enqueueing excessively, throttle or fix the source (e.g. misconfigured cron, duplicate requests).  
* **Optimize job processing:** If jobs are slow due to DB or external API, optimize queries, add caching, or improve backend latency; consider batching where applicable.  
* **Temporary:** If the cause is a known incident (e.g. Google API degradation), document impact and ETA; scale workers if it helps, and re-assess when the dependency recovers.

### **6\. Verify**

* Confirm enqueue vs dequeue rates move back toward balance (e.g. dequeue rate within 2x of enqueue) and alert clears.  
* Monitor queue depth or backlog (if visible) to ensure it is draining.

### **7\. Document**

* Record root cause, actions taken, and any runbook updates. Share in Slack if needed.

## Useful Resources

|  |  |
| :---- | :---- |
| Job queue (Enqueue, Dequeue) | `telemetry/utils/pgjobs.go` |
| Workers and queue config | `telemetry/main.go` |
| Job types | `telemetry/jobs/` (metrics.go, usageMetrics.go, collectMetrics.go, bizops\_report.go, billingRetry.go) |
| Usage sink | `telemetry/usage/sink.go` |
| Performance sink | `telemetry/performance/sink.go` |

