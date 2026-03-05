This runbook guides investigation and resolution when the **Job Processing Failures** alert fires: job processing fails repeatedly (e.g. \> 5 failed jobs in 5 minutes).
## Alert Information

|  |  |
| ----- | :---- |
| **Alert Name** | Job Processing Failures (Telemetry) |
| **Severity** | Critical |
| **Threshold** | \> 5 failed jobs in 5 minutes |
| **Duration** | 5 minutes |
| **Expected Response** | 15 minutes |
| **Notification Channels** | PagerDuty, Slack |
| **Primary Metric** | `vcp_telemetry_jobs_processed_total` (labels: job\_type, queue, status); filter `status="failed"` |

## Related Metrics

|  |  |
| :---- | :---- |
| `vcp_telemetry_jobs_processed_total` | Processed job count by job\_type, queue, status (success/failed). |
| `vcp_telemetry_jobs_dequeued_total` | Dequeued count by queue, job\_type. |
| `vcp_telemetry_jobs_enqueued_total` | Enqueued count; compare with processed to see backlog. |

**Code reference:** `telemetry/utils/pgjobs.go` (Dequeue) executes jobs and records `RecordJobProcessed` with status success/failed; workers in `telemetry/main.go` run Dequeue for each queue (performance, usage, collection, bizops, billing\_retry). Job types: `telemetry/jobs/` (ProcessPerformanceMetrics, ProcessUsageMetrics, CollectMetrics, BizOpsReport, ProcessBillingSubmission).

## Debugging Steps

### **1\. Acknowledge the Alert**

* Acknowledge in GCP Cloud Monitoring and PagerDuty. Assign owner; expected response 15 minutes.

### **2\. Gather Initial Context**

* **Dashboard:** Open Telemetry Job Queue Health. Check job success vs failure ratio and failure count by queue and job\_type.  
* **Time window:** Note the 5-minute window when failures exceeded the threshold.  
* **Queues/job types affected:** Identify which queue and job type (e.g. usage, ProcessUsageMetrics) are failing.  
* **Recent changes:** Review recent deployments, config, or data changes that could affect job execution (e.g. DB schema, external APIs, feature flags).

### **3\. Validate the Alert**

* **Query (example):** `sum(increase(vcp_telemetry_jobs_processed_total{status="failed"}[5m])) > 5`.  
* Confirm failures are real by checking Telemetry logs for job execution errors (e.g. "failing job always fails", DB errors, or panics in worker).

### **4\. Identify Root Cause**

#### **a. By Job Type**

* **ProcessPerformanceMetrics:** Collects and processes performance metrics; depends on VCP DB, Telemetry DB, Google metric provider, performance sink. Check DB connectivity, Google API availability, and sink delivery.  
* **ProcessUsageMetrics:** Aggregates and pushes usage/billing; depends on Telemetry DB, VCP DB, billing provider, usage sink. Check DB, aggregation logic, and Google billing API.  
* **CollectMetrics:** Collects metrics for a project; depends on Google metric provider and Telemetry DB. Check Google API and DB.  
* **BizOpsReport:** Generates BizOps report; depends on Telemetry DB, VCP DB, BizOps sink. Check DB and sink (e.g. GCS).  
* **ProcessBillingSubmission:** Handles billing submission retries; depends on Telemetry DB and usage sink. Check DB and Google billing API.

#### **b. Logs**

* **Telemetry workers:** Workers run in a loop (Dequeue → Load job → Perform). Errors from `Perform()` are recorded as failed and the job row is updated with status failed and error message. Search logs for the job type and queue, and for the error message stored in the job (e.g. "failed to get pool metrics", "sink delivery failed").  
* **Database:** Check Telemetry DB and VCP DB for connectivity errors, timeouts, or constraint violations during job execution.  
* **External APIs:** If the job calls Google APIs (monitoring, billing, service control), check Google Cloud status and quotas; 4xx/5xx from Google can cause job failure.

#### **c. Retry and Dead Letter**

* **Retry:** Jobs are retried up to MAX\_GOOGLE\_BILLING\_PUSH\_RETRY (default 5\) for failed status. Repeated failures for the same logical job can inflate the failure count; check if the same job is failing repeatedly (e.g. bad record) or many distinct jobs are failing (e.g. systemic issue).  
* **DB state:** Query `jobs` table (if allowed) for status=failed, error message, and attempt count to see failure reasons and retry state.

#### **d. Symptom → Cause Mapping**

|  |  |  |
| :---- | :---- | :---- |
|  |  |  |
| Failures across all job types | Telemetry DB or VCP DB down/timeout; worker crash loop | DB health; worker pod restarts and logs |
| Failures only for ProcessUsageMetrics | Billing/usage sink or Google billing API failure; bad aggregated data | Usage sink logs; Google billing API status and errors |
| Failures only for ProcessPerformanceMetrics | Google monitoring API or performance sink failure | Performance sink and Google API logs |
| Failures only for BizOpsReport | BizOps sink (e.g. GCS) failure or permission | GCS and BizOps sink logs; IAM |
| Failures only for CollectMetrics | Google monitoring API or project-specific issue | Google API and project config |
| Same job failing repeatedly | Bad input data, bug in job logic, or dependency always failing | Job error message; fix data or code |

### **5\. Implement Solution / Mitigation**

* **DB:** Restore DB connectivity or fix slow queries; fix schema or migrations if applicable.  
* **External API:** Address Google API errors (quota, credentials, or outage); add/improve retries and backoff.  
* **Sink:** Fix sink credentials, connectivity, or configuration (usage, performance, BizOps).  
* **Code/data:** Fix bug in job logic or invalid input data; deploy fix and optionally reset or reprocess failed jobs per operational procedure.  
* **Temporary:** If an external dependency is down, document impact (e.g. delayed billing or reports); re-run or backfill after dependency is restored if required.

### **6\. Verify**

* Confirm failure rate drops (e.g. no more than 5 failures in 5 minutes) and alert clears.  
* Trigger or wait for new jobs of the affected type and verify they complete successfully in logs and metrics.

### **7\. Document**

* Record root cause, actions taken, and any runbook updates. Share in Slack and post-mortem if needed.

## Useful Resources

|  |  |
| :---- | :---- |
| Job queue and Dequeue | `telemetry/utils/pgjobs.go` |
| Job types and Perform logic | `telemetry/jobs/` (metrics.go, usageMetrics.go, collectMetrics.go, bizops\_report.go, billingRetry.go) |
| Workers and queues | `telemetry/main.go` |
| Processor (used by performance job) | `telemetry/processor/processor.go` |
| Usage/Billing sink | `telemetry/usage/sink.go` |
| Performance sink | `telemetry/performance/sink.go` |

