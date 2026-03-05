This runbook guides investigation and resolution when the **API High Error Rate** alert fires: proportion of 5xx API responses exceeds acceptable limits for the VCP Telemetry service.
## Alert Information

|  |  |
| ----- | :---- |
| **Alert Name** | API High Error Rate (Telemetry) |
| **Severity** | Critical |
| **Threshold** | \> 5% of API responses return 5xx errors |
| **Duration** | 5 minutes |
| **Expected Response** | 15 minutes |
| **Notification Channels** | PagerDuty, Slack |
| **Primary Metric** | `vcp_telemetry_api_requests_total` (labels: endpoint, method, status\_code) |

## Related Metrics

|  |  |
| :---- | :---- |
| `vcp_telemetry_api_requests_total` | Request count by endpoint, method, status\_code. Filter `status_code=~"5.."` for 5xx. |
| `vcp_telemetry_api_request_duration_seconds` | Latency histogram by endpoint, method. |

**Code reference:** `telemetry/monitoring/middleware.go` records each request via `RecordAPIRequest` and `RecordAPILatency`; `telemetry/api/endpoints/endpoint.go` implements handlers.  
---

## Debugging Steps

### **1\. Acknowledge the Alert**

* Acknowledge in GCP Cloud Monitoring (and PagerDuty if applicable) to stop repeat notifications.  
* Record acknowledgment time and assign an owner.

### **2\. Gather Initial Context**

* **Dashboard:** Open the Telemetry API Overview dashboard. Check request rate by endpoint and error rate by status code.  
* **Time window:** Note the 5-minute window when the threshold was exceeded.  
* **Endpoints affected:** Identify which endpoints show 5xx (e.g. `/v1/performance`, `/v1/usage`, `/v1/generateReport`).  
* **Recent changes:** Review recent deployments, config changes, or dependency updates for the Telemetry service.

### **3\. Validate the Alert**

* **Query:** Confirm 5xx rate: `sum(rate(vcp_telemetry_api_requests_total{status_code=~"5.."}[5m])) / sum(rate(vcp_telemetry_api_requests_total[5m])) > 0.05`.  
* **Exclude known maintenance:** Check if the spike aligns with a planned deployment or known incident.  
* **Cross-check logs:** Ensure application logs show corresponding 5xx responses (not a metric ingestion issue).

### **4\. Identify Root Cause**

#### **a. Logs**

* **Telemetry service logs:** Search for stack traces, panics, and HTTP 5xx responses in the time window. Correlate with endpoint and method.  
* **Upstream dependencies:** Telemetry depends on VCP DB, Telemetry DB, and (for some flows) Google APIs. Check DB connectivity errors, timeout errors, and Google API errors (e.g. 503, 429).

#### **b. Metrics**

* **By endpoint/method:** Use `vcp_telemetry_api_requests_total` and `vcp_telemetry_api_request_duration_seconds` to see which endpoint/method drives the 5xx rate.  
* **Resource usage:** Check CPU, memory, and connection pool usage of the Telemetry service pods.

#### **c. Dependency Health**

* **VCP DB:** Connectivity and latency from Telemetry to VCP DB.  
* **Telemetry DB:** Connectivity and latency; check for connection exhaustion or slow queries.

#### **d. Symptom → Cause Mapping**

|  |  |  |
| :---- | :---- | :---- |
| 5xx on `/v1/performance`, `/v1/usage`, `/v1/generateReport` | DB unavailable or timeout | Check Telemetry DB and VCP DB connectivity and logs |
| 5xx after deployment | Bad config, failed migration, or missing env | Review deployment manifest and env. check DB migrations |
| 5xx under high load | Connection pool exhaustion, timeouts | Check DB pool size, request duration histogram |
| 5xx on all endpoints | Service crash loop or proxy/load balancer issue | Check pod restarts, readiness/liveness, and ingress/proxy logs |

### **5\. Implement Solution / Mitigation**

* **DB connectivity:** Restore DB connectivity; restart DB client or service if needed; fix firewall/network if applicable.  
* **Timeouts / overload:** Increase timeouts or connection pool size; scale Telemetry replicas if CPU/memory saturated.  
* **Bad deployment:** Roll back to last known good revision; fix config or code and redeploy.

### **6\. Verify**

* Re-check 5xx rate: should drop below 5% and alert should clear.  
* Trigger a few successful requests to the affected endpoints and confirm 2xx in logs and metrics.

### **7\. Document**

* Record root cause, actions taken, and any runbook updates.  
* Share learnings in Slack and post-mortem if severity warrants.

