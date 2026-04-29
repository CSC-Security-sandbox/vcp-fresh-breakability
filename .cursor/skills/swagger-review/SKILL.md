---
name: swagger-review
description: Review an OpenAPI/Swagger specification for REST API best practices, naming conventions, HTTP method correctness, status codes, schema quality, security definitions, and documentation completeness. Use when the user asks to review, audit, lint, validate, or check a swagger spec, OpenAPI file, API spec, or REST API design. Default spec is doc/swagger.yaml.
---

# Swagger / OpenAPI REST Review

## Quick Start

When asked to review a swagger spec:

1. Identify the target file (default: `doc/swagger.yaml`)
2. Run the automated analysis script to extract a structural summary
3. Read key sections of the spec (paths, definitions, security, info)
4. Apply the review checklist in order
5. Output a structured report using the report template

## Step 1 — Run automated analysis

```bash
# First time only: create a venv with PyYAML
python3 -m venv /tmp/swagger-review-venv
/tmp/swagger-review-venv/bin/pip install pyyaml -q

# Run the analysis (default spec)
/tmp/swagger-review-venv/bin/python3 \
  .cursor/skills/swagger-review/scripts/analyze-swagger.py \
  doc/swagger.yaml
```

For a different spec file, pass its path as the argument. The venv persists across sessions; skip the setup lines if `/tmp/swagger-review-venv` already exists.

This prints a structured summary with: total operations and schemas, 🔴/🟡/🟢 issue counts by category (naming, status codes, schemas, security, documentation), and a machine-readable JSON block at the end.

Read the output carefully — use it as the basis for findings, not a replacement for manual review.

## Step 2 — Read the spec

Use the Read tool to read the spec. For large files (> 500 lines), read in sections:
- Lines 1–100: info block, host, schemes, consumes, produces, security definitions
- Then sample representative paths (CRUD resource, action endpoint, error-only path)
- Then definitions/components (schema quality)

## Step 3 — Apply the review checklist

Work through every category below. Mark each item ✅ Pass / ❌ Fail / ⚠️ Warning.

### A. API Info & Versioning
- [ ] `info.title`, `info.description`, `info.version` are populated
- [ ] Version is in the URL path (`/v1/`, `/v2/`) — not only in `info.version`
- [ ] Schemes include `https` (never `http` only in production specs)

### B. Resource Naming
- [ ] URL segments use **lowercase plural nouns** (`/volumes`, `/networks`, not `/getVolumes`, `/Volume`)
- [ ] No verbs in path segments for CRUD operations (`/create`, `/delete`, `/list` are violations)
- [ ] Action endpoints (non-CRUD) use POST + noun-verb sub-resource style (`/volumes/{id}/revert`, not `/revertVolume`)
- [ ] Path parameters use `camelCase` or `kebab-case` consistently (pick one, don't mix)
- [ ] No redundant path segments (`/v1/network/networks` is a violation)

### C. HTTP Methods
- [ ] GET for safe reads — no side effects, no body
- [ ] POST for resource creation and non-idempotent actions
- [ ] PUT for full replacement updates (body must represent the full resource)
- [ ] PATCH for partial updates (if present, body must be a partial schema)
- [ ] DELETE for removal — typically no body, returns 204 or 200
- [ ] No GET with a request body (violates HTTP semantics)
- [ ] OPTIONS endpoints are documented if CORS is relevant

### D. HTTP Status Codes
- [ ] `200 OK` for successful reads and full-body updates
- [ ] `201 Created` for successful synchronous resource creation (include `Location` header)
- [ ] `202 Accepted` for async operations (include operation resource in body)
- [ ] `204 No Content` for successful DELETEs with no response body
- [ ] `400 Bad Request` for validation failures (with error body)
- [ ] `401 Unauthorized` for missing/invalid credentials
- [ ] `403 Forbidden` for insufficient permissions
- [ ] `404 Not Found` for unknown resources
- [ ] `409 Conflict` for state conflicts (e.g., duplicate resource)
- [ ] `422 Unprocessable Entity` for semantic validation errors (optional but recommended)
- [ ] `500 Internal Server Error` documented on all endpoints
- [ ] List endpoints return `200` with array body (not `201`)
- [ ] No `200` returned from DELETE with an empty body (use `204`)

### E. Request & Response Schemas
- [ ] All POST/PUT/PATCH request bodies reference a named `$ref` schema (not inline)
- [ ] All 2xx responses reference a named `$ref` schema
- [ ] List responses return an array (or a wrapper object with an `items` array field) — never a bare object
- [ ] Schema properties use `camelCase` consistently
- [ ] Required fields are declared via `required: [...]`
- [ ] `enum` fields list all valid values with descriptions
- [ ] No `additionalProperties: true` on strict schemas (security risk)
- [ ] Pagination fields (`nextPageToken`, `pageSize`, `totalCount`) present on all list endpoints

### F. Error Responses
- [ ] A reusable error schema is defined (e.g., `#/definitions/Error` or `#/components/schemas/Error`)
- [ ] Error schema has at minimum: `code` (int or string), `message` (string)
- [ ] All 4xx and 5xx responses reference the shared error schema
- [ ] Error bodies are never empty for 4xx responses

### G. Parameters
- [ ] Path parameters match the `{paramName}` in the path exactly
- [ ] Query parameters for list endpoints: at minimum `pageSize` / `pageToken` (or equivalent)
- [ ] Common headers (correlation ID, auth token) are defined as reusable `$ref` parameters
- [ ] No sensitive data (passwords, tokens) in query parameters or path segments

### H. Security
- [ ] `securityDefinitions` (Swagger 2) or `securitySchemes` (OAS 3) are defined
- [ ] Every non-public endpoint has a `security` block applied
- [ ] Auth scheme is appropriate (OAuth2, API key, Bearer — not Basic over HTTP)
- [ ] Scopes are defined for OAuth2 flows

### I. Documentation Quality
- [ ] Every `operationId` is unique across the spec
- [ ] Every endpoint has a non-empty `summary` (≤ 10 words) and `description`
- [ ] Every path parameter has a `description`
- [ ] Every schema property has a `description`
- [ ] At least one `example` per request body and list response
- [ ] Tags are used to group related endpoints; all tags are declared in the global `tags` section

### J. Consistency
- [ ] Naming conventions are applied uniformly (don't mix `camelCase` and `snake_case` in the same spec)
- [ ] Pagination style is consistent across all list endpoints
- [ ] Error response format is consistent across all endpoints
- [ ] All resource IDs follow the same naming pattern (`{resourceId}`, `{resourceName}`, etc.)
- [ ] `operationId` follows a consistent pattern (e.g., `listVolumes`, `createPool`, `deleteSnapshot`)

## Step 4 — Output the report

Use this template:

```markdown
# Swagger Review: <filename>

**Spec version**: <info.version>  **Format**: Swagger 2.0 / OAS 3.x  **Date**: <today>

## Executive Summary
[2-3 sentences: overall quality, biggest risks, recommended priority]

## Findings

### Critical (must fix)
| # | Category | Location | Issue | Recommendation |
|---|----------|----------|-------|----------------|
| 1 | HTTP Methods | `GET /v1/network/create` | Verb in path for create operation | Change to `POST /v1/networks` |

### Warning (should fix)
| # | Category | Location | Issue | Recommendation |
|---|----------|----------|-------|----------------|

### Suggestion (nice to have)
| # | Category | Location | Issue | Recommendation |
|---|----------|----------|-------|----------------|

## Metrics
| Category | Pass | Fail | Warn |
|----------|------|------|------|
| Resource Naming | | | |
| HTTP Methods | | | |
| Status Codes | | | |
| Schemas | | | |
| Error Responses | | | |
| Security | | | |
| Documentation | | | |

## Top Priorities
1. [Most impactful fix]
2. [Second most impactful]
3. [Third most impactful]
```

## Severity Levels

| Level | Label | Meaning |
|-------|-------|---------|
| 🔴 | Critical | Breaks REST contract or causes client interop failures |
| 🟡 | Warning | Violates best practice, degrades developer experience |
| 🟢 | Suggestion | Polish improvement, no functional impact |

## Additional Resources

- For detailed REST principles reference, see [rest-principles.md](rest-principles.md)
- Script usage and output schema: see [scripts/analyze-swagger.py](scripts/analyze-swagger.py)
