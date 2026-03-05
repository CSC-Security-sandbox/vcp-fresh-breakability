This runbook guides investigation and resolution when the **Billing Submission Anomaly** alert fires: significant drop in billing metric volume compared to baseline (e.g. ≥ 50% drop for 30 minutes).
## Alert Information

|  |  |
| ----- | :---- |
| **Alert Name** | Billing Submission Anomaly (Telemetry) |
| **Severity** | Warning |
| **Threshold** | ≥ 50% drop from baseline |
| **Duration** | 30 minutes |
| **Expected Response** | 2 hours |
| **Notification Channels** | Slack |
| **Primary Metric** | `vcp_telemetry_billing_metrics_submission_total` (labels: sink\_type, measured\_type, timestamp) |

## Related Metrics

|  |  |
| :---- | :---- |
| `vcp_telemetry_billing_metrics_submission_total` | Gauge: submitted quantity per sink\_type, measured\_type, timestamp. Volume drop indicates fewer successful submissions. |
| `vcp_telemetry_jobs_processed_total` | Job success/failure; failed usage/billing jobs reduce submissions. |
| `vcp_telemetry_metrics_delivered_total` | Sink delivery success/failure; failures reduce billing submission volume. |

**Code reference:** Billing submission volume is recorded in `telemetry/usage/sink.go` via `RecordBillingMetricsSubmission` (measured\_type, quantity, timestamp). Baseline is typically computed from historical submission volume (e.g. same hour previous day or rolling average).

## Debugging Steps

### **1\. Acknowledge the Alert**

* Acknowledge in GCP Cloud Monitoring. Assign owner; expected response 2 hours.

### **2\. Gather Initial Context**

* **Dashboard:** Open Billing Metrics view. Compare current submission volume (by measured\_type) to baseline over the 30-minute window.  
* **Scope:** Identify which measured\_type(s) or sink\_type show the drop (e.g. all vs one measured type).  
* **Recent changes:** Review deployments, config, traffic changes, or product/usage changes that could explain lower volume.

### **3\. Validate the Alert**

* **Query (example):** Compare `sum(vcp_telemetry_billing_metrics_submission_total)` or rate/increase over 30m to baseline (e.g. same window previous day). Confirm ≥ 50% drop is real and not due to alert baseline error (e.g. holiday, maintenance).  
* **Legitimate drop:** Consider whether lower usage (e.g. fewer customers, end of trial, seasonal drop) is expected; if so, document and optionally tune baseline or alert.

### **4\. Identify Root Cause**

#### **a. Pipeline Throughput**

* **Fewer jobs or job failures:** If ProcessUsageMetrics or ProcessBillingSubmission jobs are enqueued less often or failing more, submission volume will drop. Check enqueue/dequeue rates and Job Processing Failures.  
* **Queue backlog:** If the usage or billing\_retry queue is backing up, submissions may be delayed and volume in the window may appear low. See Queue Backlog Growth runbook.  
* **Worker capacity:** Fewer workers or worker restarts can reduce throughput and thus submission volume.

#### **b. Sink and Google API**

* **Sink delivery failures:** Partial or total failure of the usage sink (credentials, connectivity, Google API) will reduce successful submissions. Check `vcp_telemetry_metrics_delivered_total` and Sink Delivery Failures runbook.  
* **Google API throttling or errors:** Rate limits or 4xx/5xx can cause fewer successful deliveries and thus lower billing submission volume.

#### **c. Data and Aggregation**

* **Less source data:** Fewer usage events (e.g. collector/aggregator issues, or genuine drop in customer usage) will reduce aggregated usage and thus submissions. Check aggregator and collector metrics/logs.  
* **Filtering or validation:** Stricter validation or filtering in the pipeline could drop more records and reduce volume; check recent code or config changes.

#### **d. Symptom → Cause Mapping**

|  |  |  |
| :---- | :---- | :---- |
| Drop across all measured types | Systemic: workers, sink, or enqueue rate | Job processing and sink delivery metrics; worker health |
| Drop for one measured type | That measured type's data or path affected | Aggregation filters; job/sink logs for that type |
| Jobs failing more | DB, sink, or API errors | Job Processing Failures; sink and API logs |
| Jobs running but fewer submissions | Sink failures or partial success | vcp\_telemetry\_metrics\_delivered\_total; usage sink logs |
| Legitimate usage drop | Business/seasonal; no bug | Confirm with product/usage data; adjust baseline if needed |

### **5\. Implement Solution / Mitigation**

* **Pipeline:** Restore job processing and queue health (see Job Processing Failures, Queue Backlog runbooks). Scale workers if needed.  
* **Sink/API:** Fix sink delivery and Google API issues (see Sink Delivery Failures runbook). Address quota or throttling.  
* **Data/aggregation:** Fix collector or aggregator so source data and aggregated usage are correct; relax or fix validation if it was incorrectly dropping records.  
* **Baseline/alert:** If the drop is expected (e.g. usage change), document and consider updating baseline or alert sensitivity to reduce false positives.  
* **Billing impact:** If revenue is affected, coordinate with billing/revenue teams and document any correction or backfill.

### **6\. Verify**

* Confirm submission volume returns toward baseline and alert clears. Monitor for a few hours to ensure stability.

### **7\. Document**

* Record root cause, actions taken, and any runbook or alert updates. Share in Slack if needed.

## Useful Resources

|  |  |
| :---- | :---- |
| Billing submission recording | `telemetry/usage/sink.go` (processResponse, RecordBillingMetricsSubmission) |
| ProcessUsageMetrics / ProcessBillingSubmission | `telemetry/jobs/` (usageMetrics.go, billingRetry.go) |
| Usage sink delivery | `telemetry/usage/sink.go` |
| Aggregator | `telemetry/aggregator/` |

