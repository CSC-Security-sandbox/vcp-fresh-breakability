# Error Taxonomy & Mapping Guide

Purpose: Provide a consistent cross-service mapping between API error responses, internal error codes (vsaerrors), HTTP statuses, and component ownership.

## 1. API Error Envelope
All error responses (including Operation.error) conform to:
```json
{ "code": <number>, "message": "<human readable>" }
```
`code` maps to an internal categorized range. `message` is safe for end-user display (no secrets / PII).

## 2. Numeric Ranges
| Range | Category | Component Ownership | Typical HTTP |
|-------|----------|--------------------|-------------|
| 1000-1999 | Workflow Orchestration | Temporal / worker | 500 / 422 |
| 2000-2999 | Database / Persistence | DB layer / storage | 500 / 409 |
| 3000-3999 | Cloud Provider (GCP APIs, IAM, KMS) | hyperscaler/ | 422 / 503 |
| 4000-4999 | VSA Cluster Lifecycle | VLM, deployment | 500 / 409 |
| 5000-5999 | ONTAP / Data Plane | ontap-proxy | 422 / 500 |
| 6000-6999 | Validation / User Input | google-proxy / orchestrator | 400 / 422 |
| 7000-7999 | Rate / Quota / Throttle | google-proxy / hyperscaler | 429 |
| 8000-8999 | Security / Auth / KMS | hyperscaler / kms activities | 401 / 403 / 422 |
| 9000-9999 | Internal Reserved / Unknown | fallback mapper | 500 |

## 3. HTTP Status Mapping (Canonical)
| HTTP | Semantics | Examples |
|------|-----------|----------|
| 400 | Malformed request / syntax | JSON parse failure, invalid enum |
| 401 | Authentication failure | Missing auth token (upstream) |
| 403 | Authorization denied | Project not allowlisted |
| 404 | Resource not found | Unknown poolId, volumeId |
| 409 | Conflict / state mismatch | Duplicate resourceId, invalid resize |
| 422 | Semantic invalid | Snapshot not READY, zone mismatch |
| 429 | Throttled | Burst API quota exceeded |
| 500 | Internal error | Unexpected panic, DB outage |
| 501 | Not implemented | Unsupported feature route |
| 503 | Dependency unavailable | KMS transient, network outage |

## 4. Internal -> API Translation
```
core/errors/vsaerrors.CustomError{ Category, Code, Underlying }
 -> mapper (google-proxy middleware)
 -> HTTP status (category+context) + body {code,message}
```
Rules of thumb:
- Validation errors -> 422 unless strictly syntactic (400)
- Duplicate / existence -> 409
- Not found (GET) -> 404 (DELETE path returns 202/204 per design to avoid ambiguity)
- Retryable infrastructure -> 500 (client may retry with backoff)
- Cloud permission issues -> 403 (if IAM) or 422 (if key unreachable but credentials valid)

## 5. Component Ownership Table
| Component | Namespace Examples | Responsibility |
|----------|--------------------|----------------|
| google-proxy | google-proxy/api, middleware | Input validation, error normalization |
| orchestrator | core/orchestrator | Business workflow invocations |
| worker (Temporal) | workflow_engine, activities | Long-running logic, retries |
| hyperscaler | hyperscaler/* | Cloud API calls, IAM, VPC, KMS |
| VLM / deployment | vlm client | VM cluster allocation, scaling |
| ontap-proxy | ontap-proxy/* | ONTAP operations (volume, snapshot) |
| database | database/vcp | Persistence, migrations |

## 6. Common Error Scenarios
| Scenario | Error Code (Example) | HTTP | Mitigation |
|----------|----------------------|------|-----------|
| Pool size shrink attempt | 6001 | 409 | Only allow grow; instruct increase-only |
| Volume zone mismatch | 6002 | 422 | Recreate volume in correct zone |
| Snapshot name conflict | 6003 | 409 | Pick unique resourceId |
| KMS key permission denied | 8001 | 422 | Grant roles/cloudkms.cryptoKeyEncrypterDecrypter |
| ONTAP volume create timeout | 5001 | 500 | Inspect ontap-proxy logs; auto-retry if idempotent |
| Replication peering failure | 3005 | 503 | Check intercluster network, retry |

## 7. LRO Error Handling
Polling route always HTTP 200; Operation.error mirrors above mapping.
- Clients must inspect `error.code` not HTTP for failure semantics.

## 8. Retry Guidance (Client)
| Category | Retry? | Strategy |
|----------|--------|----------|
| Validation (6xxx) | No | Fix request |
| Conflict (409) | Conditional | Query resource; maybe idempotent reuse |
| Cloud transient (3xxx) | Yes | Exponential backoff + jitter |
| ONTAP transient (5xxx) | Yes | Backoff; monitor for saturation |
| DB failure (2xxx) | Yes | Short backoff; escalate if persistent |

## 9. Logging & Correlation
All errors include correlation ID (header X-Correlation-Id or generated) in structured logs; ensure log scrubbing removes secrets (passwords, keys).

## 10. Adding a New Error Code
1. Define constant in `core/errors` with category range.
2. Add mapping description to this table.
3. Update tests to assert translation -> HTTP & JSON body.
4. Document remediation if user actionable.

---
End of Error Taxonomy & Mapping Guide.

