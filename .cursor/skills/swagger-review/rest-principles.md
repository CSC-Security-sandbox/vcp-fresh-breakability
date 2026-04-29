# REST API Best Practices Reference

Reference material for the swagger-review skill. Read sections on-demand.

---

## 1. Resource Naming

### Nouns, not verbs
URLs identify resources; HTTP methods express the action.

| ❌ Avoid | ✅ Prefer |
|---------|---------|
| `POST /createVolume` | `POST /volumes` |
| `GET /getNetworkById` | `GET /networks/{id}` |
| `DELETE /deleteSnapshot` | `DELETE /snapshots/{id}` |
| `POST /listJobs` | `GET /jobs` |

### Plural nouns for collections
- `/volumes` — collection
- `/volumes/{volumeId}` — single resource
- Exception: singleton resources (`/me`, `/status`, `/health`) may be singular

### Hierarchy for relationships
Nest sub-resources to at most **two levels**:
```
/volumes/{volumeId}/snapshots            ✅
/volumes/{volumeId}/snapshots/{id}       ✅
/pools/{poolId}/volumes/{id}/snapshots   ⚠️ three levels — consider flattening
```

### Action sub-resources (non-CRUD operations)
Use a verb as a sub-resource after the resource identifier, always with POST:
```
POST /volumes/{volumeId}/revert         ✅
POST /replications/{id}/stop            ✅
POST /replications/{id}/resume          ✅
GET  /volumes/{volumeId}/revert         ❌ (safe GET with side effects)
```

### Case and separators
- Path segments: `lowercase-kebab-case` or `camelCase` — pick one and be consistent
- Path parameters: `camelCase` (`{volumeId}`, `{poolId}`)
- Query parameters: `camelCase` (`pageSize`, `orderBy`)
- Body properties: `camelCase`

---

## 2. HTTP Methods

| Method | Semantics | Body | Idempotent | Safe |
|--------|-----------|------|-----------|------|
| GET | Read resource(s) | Never | Yes | Yes |
| POST | Create / non-idempotent action | Yes | No | No |
| PUT | Full replace | Yes (full resource) | Yes | No |
| PATCH | Partial update | Yes (partial) | Conditional | No |
| DELETE | Remove | Rarely | Yes | No |
| OPTIONS | CORS preflight | No | Yes | Yes |
| HEAD | Metadata only | No | Yes | Yes |

### Common mistakes
- Using `GET` to trigger an action (not safe)
- Using `POST` when `PUT` is needed (create vs replace)
- Using `DELETE` with a required request body
- Using `PUT` for partial updates (should be `PATCH`)

---

## 3. HTTP Status Codes

### 2xx — Success

| Code | When to use |
|------|------------|
| `200 OK` | Successful GET, PUT, PATCH with response body |
| `201 Created` | Successful synchronous POST that creates a resource; include `Location: /resources/{id}` header |
| `202 Accepted` | Async operation started; body contains an operation/job resource |
| `204 No Content` | Successful DELETE or action with no response body |
| `206 Partial Content` | Paginated or range response |

### 3xx — Redirection
Rarely needed in REST APIs. Avoid unless implementing canonical URL redirects.

### 4xx — Client errors

| Code | When to use |
|------|------------|
| `400 Bad Request` | Malformed syntax, missing required fields, type errors |
| `401 Unauthorized` | Missing or invalid credentials (authentication failed) |
| `403 Forbidden` | Credentials valid but insufficient permissions (authorization failed) |
| `404 Not Found` | Resource does not exist |
| `405 Method Not Allowed` | HTTP method not supported on this path |
| `409 Conflict` | State conflict (duplicate key, incorrect state machine transition) |
| `410 Gone` | Resource permanently deleted |
| `422 Unprocessable Entity` | Syntax OK but semantic validation failed (e.g., end date before start date) |
| `429 Too Many Requests` | Rate limit exceeded; include `Retry-After` header |

### 5xx — Server errors

| Code | When to use |
|------|------------|
| `500 Internal Server Error` | Unexpected server-side failure |
| `502 Bad Gateway` | Upstream service unreachable |
| `503 Service Unavailable` | Temporary overload or maintenance |
| `504 Gateway Timeout` | Upstream service timed out |

---

## 4. Request & Response Design

### Request bodies
- Always reference a named schema (`$ref`) — avoid inline schema definitions
- Include `Content-Type: application/json` in `consumes`
- Validate and document all required fields

### Response bodies
- Always reference a named schema for 2xx responses
- List endpoints: wrap arrays in an envelope object for future extensibility:
  ```json
  { "items": [...], "nextPageToken": "...", "totalSize": 42 }
  ```
  (Returning a bare array prevents adding top-level fields without breaking clients)
- Synchronous create: return the created resource (full representation), not just the ID

### Pagination
Required on all list endpoints. Prefer cursor-based over offset-based:

| Parameter | Description |
|-----------|-------------|
| `pageSize` | Max items per page (query param) |
| `pageToken` | Opaque cursor for next page (query param) |
| `nextPageToken` | Response field; absent or empty means last page |
| `totalSize` | Optional total count (can be expensive) |

### Field masks (optional but recommended for PATCH)
Use a `updateMask` query parameter or body field to specify which fields to update.

---

## 5. Error Response Format

Define a single reusable error schema and use it for all 4xx/5xx responses.

**Recommended schema:**
```yaml
Error:
  type: object
  required: [code, message]
  properties:
    code:
      type: integer
      description: HTTP status code
    message:
      type: string
      description: Human-readable error description
    details:
      type: array
      items:
        type: object
        properties:
          field:
            type: string
          reason:
            type: string
    requestId:
      type: string
      description: Correlation/request ID for tracing
```

Never return:
- A bare string as the error body
- An empty body for 4xx responses
- Stack traces or internal details in production responses

---

## 6. Security

### Authentication schemes (pick one per API)
| Scheme | Swagger 2 type | Notes |
|--------|---------------|-------|
| API Key | `apiKey` | In header preferred over query param |
| Bearer JWT | `apiKey` with `in: header`, name `Authorization` | Or `oauth2` with implicit/authCode flow |
| OAuth2 | `oauth2` | Define scopes per endpoint |
| mTLS | Out of band | Document in description |

### Scope design
- Scopes should follow `resource:action` pattern: `volumes:read`, `pools:write`
- Minimal scope principle: list operations require `:read`, write operations require `:write`

### Spec requirements
- `securityDefinitions` declared at top level
- `security` block applied globally or per endpoint
- Sensitive data (passwords, tokens) never in path or query parameters

---

## 7. Documentation Quality

### operationId conventions
Follow `{verb}{Resource}` pattern consistently:

| Operation | Pattern | Example |
|-----------|---------|---------|
| List | `list{Resources}` | `listVolumes` |
| Get one | `get{Resource}` or `describe{Resource}` | `getVolume` |
| Create | `create{Resource}` | `createPool` |
| Update | `update{Resource}` | `updateSnapshot` |
| Delete | `delete{Resource}` | `deleteNetwork` |
| Action | `{verb}{Resource}` | `revertVolume`, `stopReplication` |

All `operationId` values must be **unique** across the entire spec.

### Description guidelines
- `summary`: ≤ 10 words, imperative mood ("List all volumes", "Create a pool")
- `description`: full sentence, explains behaviour, side effects, async nature
- Parameter descriptions: include type, format, constraints, example value
- Schema property descriptions: explain purpose and valid range, not just the name

### Examples
- Include at least one `example` block per request schema and list response
- Examples must be valid instances of their schema
- Name examples descriptively when using OAS 3 `examples` map

---

## 8. Versioning Strategy

| Approach | Pros | Cons |
|----------|------|------|
| URL path (`/v1/`) | Simple, cacheable, visible | URL changes on break |
| Query param (`?version=1`) | Non-breaking URL | Often ignored by caches |
| Header (`Accept-Version: v1`) | Clean URLs | Hidden, harder to test |

**Recommendation**: URL path versioning (`/v1/`, `/v2/`) for public APIs.

### Breaking vs non-breaking changes
Breaking (require version bump):
- Removing a field from a response
- Changing a field's type
- Removing or renaming an endpoint
- Adding a new required request field

Non-breaking (backward compatible):
- Adding an optional request field
- Adding a new response field
- Adding a new endpoint
- Adding a new optional query parameter

---

## 9. Common Violations Found in This Project's Spec

Quick reference of patterns to watch for in `doc/swagger.yaml`:

| Pattern | Violation type |
|---------|--------------|
| `GET /v1/network` (singular noun) | Naming — should be `/networks` |
| Verbs in paths (e.g., `/tenantMigration`) | Naming — use noun sub-resources |
| Missing 401/403 on authenticated endpoints | Status codes |
| Inline schema definitions in path responses | Schema — use `$ref` |
| List responses returning bare arrays | Schema — wrap in envelope |
| Missing `operationId` on some endpoints | Documentation |
| Mixed tag casing (`camelCase` vs `lowercase`) | Consistency |
