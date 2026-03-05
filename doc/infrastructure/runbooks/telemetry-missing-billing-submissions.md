This runbook guides investigation and resolution when the **Missing Billing Submissions** alert fires: no billing data submission for any measured type for 1 hour.
## Alert Information

|  |  |
| ----- | :---- |
| **Alert Name** | Missing Billing Submissions (Telemetry) |
| **Severity** | Critical |
| **Threshold** | No submissions for 1 hour |
| **Duration** | 60 minutes |
| **Expected Response** | 30 minutes |
| **Notification Channels** | PagerDuty, Slack, Email |
| **Primary Metric** | `vcp_telemetry_billing_metrics_submission_total` (labels: sink\_type, measured\_type, timestamp) |

## Related Metrics

|  |  |
| :---- | :---- |
| `vcp_telemetry_billing_metrics_submission_total` | Gauge: submitted quantity per sink\_type, measured\_type, timestamp. Absence of submissions (no updates) triggers this alert. |
| `vcp_telemetry_metrics_delivered_total` | Sink delivery success/failure; billing submissions go through usage sink. |
| `vcp_telemetry_jobs_processed_total` | ProcessUsageMetrics and ProcessBillingSubmission job outcomes; failures can prevent billing submissions. |

**Code reference:** `RecordBillingMetricsSubmission` is called from `telemetry/usage/sink.go` in `processResponse` after processing usage metrics results (SinkType `"usage"`, measured\_type from aggregated usage). Billing submissions are produced by ProcessUsageMetrics and ProcessBillingSubmission jobs; the usage sink delivers to Google and then records the gauge.

---

## Debugging Steps

### **1\. Acknowledge the Alert**

* Acknowledge in GCP Cloud Monitoring and PagerDuty. Assign owner; expected response 30 minutes.

### **2\. Gather Initial Context**

* **Dashboard:** Open Billing Metrics view. Check submission volume by measured\_type and timeline; confirm no submissions in the last hour.  
* **Scope:** Determine if it's all measured types or specific ones (alert may be defined as "no submissions for any measured type").  
* **Recent changes:** Review recent deployments, config, aggregation schedule, or Google billing API changes.

### **3\. Validate the Alert**

* **Query (example):** Ensure no data points for `vcp_telemetry_billing_metrics_submission_total` in the last 1h (e.g. check for any increase or last timestamp). Alert condition is typically "no submissions for 1 hour".  
* Confirm this is not a planned maintenance window or intentional pause (e.g. no usage data in the period).

### **4\. Identify Root Cause**

#### **a. Upstream Pipeline**

* **No usage jobs running:** If ProcessUsageMetrics or ProcessBillingSubmission jobs are not running or not succeeding, no billing submissions will be recorded. Check job enqueue rate, processing failures, and worker health (`telemetry/jobs/`, `telemetry/main.go`).  
* **No aggregated data:** Aggregation may not have produced records (e.g. no usage in the period, aggregator failure, or DB issue). Check aggregator and `aggregated_usages` (or equivalent) table.  
* **Jobs failing before sink:** If jobs fail during collection or aggregation (before calling the usage sink), no delivery and no RecordBillingMetricsSubmission. Check Job Processing Failures and related runbooks.

#### **b. Usage Sink and Google API**

* **Sink delivery failures:** If the usage sink fails to deliver (credentials, connectivity, Google API errors), submissions may not complete and the gauge may not be updated. See **Sink Delivery Failures** runbook and `telemetry/usage/sink.go`.  
* **processResponse not reached:** RecordBillingMetricsSubmission is called in `processResponse` after processing results. If ReportMetrics never returns or the worker crashes before processResponse, no submission is recorded.

#### **c. Metric / Alert Configuration**

* **Gauge semantics:** The billing metric is a gauge updated on each submission. If the alert checks for "no new data" (e.g. no increase in 1h), ensure the condition matches how the gauge is updated (per measured\_type, timestamp label).  
* **Label cardinality:** Verify measured\_type and timestamp labels align with alert query (e.g. alert may key off specific measured types).

#### **d. Symptom → Cause Mapping**

|  |  |  |
| :---- | :---- | :---- |
| No jobs for usage/billing | API not receiving requests; scheduler/cron not enqueueing | Enqueue and processing rates for usage/billing\_retry queues |
| Jobs failing | DB, sink, or Google API failure | Job Processing Failures runbook; sink delivery metrics and logs |
| Jobs succeeding but no gauge update | Bug or path where RecordBillingMetricsSubmission is skipped | Code path in usage/sink.go processResponse; logs around processResponse |
| No usage data | No customer usage in period; aggregator or collector failure | Aggregator and collector logs; DB for aggregated\_usages |

### **5\. Implement Solution / Mitigation**

* **Restore job processing:** Fix worker or queue issues so ProcessUsageMetrics and ProcessBillingSubmission run and complete (see Job Processing Failures and Queue Backlog runbooks).  
* **Restore sink delivery:** Fix credentials, connectivity, or Google API issues so the usage sink can deliver (see Sink Delivery Failures runbook).  
* **Restore aggregation/collection:** If no aggregated usage is being produced, fix aggregator, collector, or DB so data flows into the billing pipeline.  
* **Code/config:** If the submission path is wrong or disabled (e.g. feature flag, config), fix and deploy. If alert logic is wrong, adjust alert and document.  
* **Billing impact:** Communicate with billing/revenue teams; determine if manual backfill or correction is needed for the gap period.

### **6\. Verify**

* Confirm billing submissions resume: `vcp_telemetry_billing_metrics_submission_total` shows updates for expected measured\_type(s) and alert clears.  
* Spot-check one submission flow end-to-end (job → sink → gauge update).

### **7\. Document**

* Record root cause, actions taken, and any runbook updates. Share in Slack and with billing/revenue; post-mortem if needed.

## Useful Resources

|  |  |
| :---- | :---- |
| Billing submission recording | `telemetry/usage/sink.go` (processResponse, RecordBillingMetricsSubmission) |
| ProcessUsageMetrics / ProcessBillingSubmission | `telemetry/jobs/` (usageMetrics.go, billingRetry.go) |
| Monitoring gauge definition | `telemetry/monitoring/metrics.go` |
| Usage sink delivery | `telemetry/usage/sink.go` |

