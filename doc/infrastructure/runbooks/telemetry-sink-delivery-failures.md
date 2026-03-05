This runbook guides investigation and resolution when the **Sink Delivery Failures** alert fires: any failure detected delivering telemetry data to external sinks (within the configured duration, e.g. 5 minutes).
## Alert Information

|  |  |
| ----- | :---- |
| **Alert Name** | Sink Delivery Failures (Telemetry) |
| **Severity** | Critical |
| **Threshold** | Any failure detected |
| **Duration** | 5 minutes |
| **Expected Response** | 15 minutes |
| **Notification Channels** | PagerDuty, Slack |
| **Primary Metric** | `vcp_telemetry_metrics_delivered_total` (labels: sink\_type, status); filter `status="failed"` or use failed count |

## Related Metrics

|  |  |
| :---- | :---- |
| `vcp_telemetry_metrics_delivered_total` | Count of metrics delivered to sinks by sink\_type and status (success/failed). |
| `vcp_telemetry_jobs_processed_total` | Job outcomes; sink failures often cause job failures for usage/performance jobs. |

**Code reference:** `RecordSinkDelivered` is called from `telemetry/usage/sink.go` (usage sink, SinkType `"usage"`) and `telemetry/performance/sink.go` (performance sink, SinkType `"performance"`). Labels: `sink_type`, `status`. Delivery happens when jobs run (ProcessUsageMetrics, ProcessPerformanceMetrics) and push to Google APIs.

## Debugging Steps

### **1\. Acknowledge the Alert**

* Acknowledge in GCP Cloud Monitoring and PagerDuty. Assign owner; expected response 15 minutes.

### **2\. Gather Initial Context**

* **Dashboard:** Open Sink Delivery view. Check delivery rates and failure counts by sink\_type (usage, performance) over the alert window.  
* **Sink(s) affected:** Identify which sink\_type shows failures (usage, performance, or both).  
* **Recent changes:** Review recent deployments, config, credentials, or Google API changes.

### **3\. Validate the Alert**

* **Query (example):** `sum(increase(vcp_telemetry_metrics_delivered_total{status="failed"}[5m])) by (sink_type) > 0`.  
* Confirm failures are real by checking telemetry logs for sink delivery errors (e.g. Google API 4xx/5xx, auth errors, timeouts).

### **4\. Identify Root Cause**

#### **a. By Sink Type**

* **Usage sink:** Delivers aggregated usage/billing metrics to Google (e.g. Cloud Billing / usage reporting). Depends on `telemetry/usage/sink.go`, GoogleMetricsClient, credentials, and Google API availability. Used by ProcessUsageMetrics and billing submission jobs.  
* **Performance sink:** Delivers performance metrics to Google (e.g. Cloud Monitoring). Depends on `telemetry/performance/sink.go`, GoogleMetricsClient, credentials, and Google API. Used by ProcessPerformanceMetrics.

#### **b. Common Causes**

|  |  |
| :---- | :---- |
| **Credentials** | Service account key expired, wrong project, or missing IAM roles (e.g. monitoring.metricDescriptors.create, billing). Check secret rotation and IAM. |
| **Connectivity** | Network policy, proxy, or firewall blocking egress to Google APIs. Check pod egress and Google API endpoints (e.g. [monitoring.googleapis.com](http://monitoring.googleapis.com/), billing). |
| **Google API errors** | 4xx (quota, invalid request, auth) or 5xx (server error). Check response codes in logs and [Google Cloud Status](https://status.cloud.google.com/). |
| **Quota / rate limit** | Too many requests; backoff or reduce batch size. Check quota metrics and error messages. |
| **Invalid payload** | Malformed or rejected metric (e.g. schema, dimension). Check logs for specific error message and affected measured\_type or metric. |
| **Timeout** | Client or server timeout; increase timeout or optimize payload size. |

#### **c. Logs**

* **Usage sink:** Search telemetry logs for "usage", "ReportMetrics", "processResponse", "processMetricsResults", and Google API error messages. Check `telemetry/usage/sink.go` and `telemetry/googlePusher/GoogleMetricsClient.go`.  
* **Performance sink:** Search for "performance", "processAndFilterMetricsResults", "ReportMetrics", and Google API errors. Check `telemetry/performance/sink.go`.  
* **Google client:** `telemetry/googlePusher/GoogleMetricsClient.go` — look for HTTP status, response body, and retry behavior.

### **5\. Implement Solution / Mitigation**

* **Credentials:** Rotate or fix service account key; ensure correct project and IAM roles (e.g. roles/monitoring.metricWriter, billing-related roles for usage).  
* **Connectivity:** Fix network/proxy/firewall to allow egress to Google APIs; validate from a pod if needed.  
* **Google API:** Address quota (request increase or throttle), fix invalid payload (schema/validation), or wait out 5xx and rely on retries.  
* **Config:** Adjust timeouts, batch sizes, or retry limits in telemetry config if appropriate.  
* **Temporary:** If Google API is degraded, document impact (delayed billing or performance metrics); retries and billing\_retry queue may eventually succeed. Notify stakeholders.

### **6\. Verify**

* Confirm failure count stops increasing and alert clears. Trigger a small flow (e.g. one job) and check logs and `vcp_telemetry_metrics_delivered_total` for success.

### **7\. Document**

* Record root cause, actions taken, and any runbook updates. Share in Slack and post-mortem if needed.

## Useful Resources

|  |  |
| :---- | :---- |
| Usage sink and RecordSinkDelivered | `telemetry/usage/sink.go` |
| Performance sink and RecordSinkDelivered | `telemetry/performance/sink.go` |
| Google metrics client | `telemetry/googlePusher/GoogleMetricsClient.go` |
| Monitoring metrics (sink metric definition) | `telemetry/monitoring/metrics.go` |
| ProcessUsageMetrics / ProcessPerformanceMetrics | `telemetry/jobs/` (usageMetrics.go, metrics.go) |

