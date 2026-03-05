This runbook guides investigation and resolution when the **API Latency SLO Breach** alert fires: sustained API latency degradation (e.g. p95 exceeding 2 seconds) for the VCP Telemetry service.
## Alert Information

|  |  |
| ----- | :---- |
| **Alert Name** | API Latency SLO Breach (Telemetry) |
| **Severity** | Warning |
| **Threshold** | p95 latency exceeds 2 seconds |
| **Duration** | 10 minutes |
| **Expected Response** | 1 hour |
| **Notification Channels** | Slack |
| **Primary Metric** | `vcp_telemetry_api_request_duration_seconds` (histogram; labels: endpoint, method) |

## Related Metrics

|  |  |
| :---- | :---- |
| `vcp_telemetry_api_request_duration_seconds` | Latency histogram. Use for p50, p95, p99 by endpoint and method. |
| `vcp_telemetry_api_requests_total` | Request volume; high volume can correlate with latency. |

**Code reference:** `telemetry/monitoring/middleware.go` records latency per request; `telemetry/api/endpoints/endpoint.go` defines handlers.

## Debugging Steps

### **1\. Acknowledge the Alert**

* Acknowledge in GCP Cloud Monitoring and Slack. Assign owner; expected response is within 1 hour (Warning).

### **2\. Gather Initial Context**

* **Dashboard:** Open Telemetry API Overview. Check latency percentiles (p50, p95, p99) by endpoint and method.  
* **Time window:** Note the 10-minute window when p95 exceeded 2s.  
* **Endpoints affected:** Identify which endpoint/method pair(s) drive the breach (e.g. POST /v1/performance, GET /v1/usage).  
* **Traffic:** Check if request rate spiked during the same window.

### **3\. Validate the Alert**

* **Query (example):** `histogram_quantile(0.95, sum(rate(vcp_telemetry_api_request_duration_seconds_bucket[5m])) by (le, endpoint, method)) > 2`.  
* Confirm the breach is sustained (not a single spike) and that the metric is being scraped correctly.

### **4\. Identify Root Cause**

#### **a. By Endpoint**

* **Performance/Usage/Report endpoints:** These enqueue jobs and return quickly; if they are slow, the delay is usually in DB (enqueue) or in the handler (e.g. context, logging). Check DB latency and connection pool.  
* **Other endpoints:** If the API exposes more routes, check their backend (DB, external HTTP calls).

#### **b. Logs and Dependencies**

* **Telemetry service:** Look for slow request log lines, timeouts, or errors in the same time window.  
* **VCP DB / Telemetry DB:** Check DB latency, connection pool exhaustion, and slow queries. Enqueue is a single DB write; it should be fast unless DB is overloaded or network is slow.  
* **Google APIs:** Not typically in the request path for latency (jobs run async); only relevant if an endpoint synchronously calls Google.

#### **c. Infrastructure**

* **Pod resources:** CPU throttling or memory pressure can increase latency; check node and pod metrics.  
* **Network:** Cross-zone or cross-region DB access can add latency; check topology.

#### **d. Symptom → Cause Mapping**

|  |  |  |
| :---- | :---- | :---- |
| p95 high on all endpoints | DB slow or connection pool exhausted; pod overloaded | DB latency and pool metrics; pod CPU/memory |
| p95 high on one endpoint | Heavy logic or slow dependency for that route | Profile handler; check DB usage for that path |
| p95 high only under high QPS | Saturation (CPU, connections, or DB) | Scale replicas or DB; tune pool size |
| Intermittent p95 spikes | GC, node eviction, or network blips | Check GC metrics, node events, network errors |

### **5\. Implement Solution / Mitigation**

* **DB:** Optimize slow queries; increase connection pool; fix DB or network issues.  
* **Compute:** Scale Telemetry replicas, resolve CPU/memory pressure; fix node issues.

### **6\. Verify**

* Confirm p95 drops below 2s for the affected endpoint(s) and the alert clears.  
* Optionally run a short load test to validate under expected load.

### **7\. Document**

* Record root cause and actions. Update runbook if new patterns are found.

## Useful Resources

|  |  |
| :---- | :---- |
| Metrics middleware | `telemetry/monitoring/middleware.go` |
| API handlers | `telemetry/api/endpoints/endpoint.go` |

