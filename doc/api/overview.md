# Google Proxy API Overview

Version: ${major_version}.${minor_version}.${patch_version}

## Purpose
The Google Proxy service exposes the public Google Cloud NetApp Volumes (GCNV) facing REST API for provisioning and lifecycle management of NetApp Virtual Storage Appliance (VSA) backed resources. It translates hyperscaler requests into internal workflow-driven operations executed by the VSA Control Plane (Temporal workflows + activities).

## Architectural Role
- Entry point for GCNV control plane -> VSA Control Plane
- Enforces request validation, correlation, and error normalization
- Produces/consumes long‑running operation (LRO) resources while internal Temporal workflows progress
- Remains hyperscaler aware; core business logic stays cloud‑agnostic under `core/`

## Resource Domains (Public)
| Domain | Path Root | Purpose |
|-------|-----------|---------|
| Health | /health | Service health probe |
| Pools | /v1beta/.../pools | Capacity & performance backing for volumes |
| Volumes | /v1beta/.../volumes | File & block (NFS / iSCSI) storage entities |
| HostGroups | /v1beta/.../hostGroups | iSCSI initiator grouping / access control |
| ActiveDirectories | /v1beta/.../activeDirectories | CIFS / SMB directory integration credentials |
| BackupVaults | /v1beta/.../backupVaults | Logical containers for volume backups |
| Backups | /v1beta/.../backupVaults/{backupVaultId}/backups | Point-in-time volume data copies (off-box) |
| Snapshots | /v1beta/.../volumes/{volumeId}/snapshots | On-box point-in-time copies |
| Replications | /v1beta/.../volumes/{volumeResourceId}/replications | Cross-region / zone volume data replication |
| KMS Configurations | /v1beta/.../storage/kmsConfig | Customer managed encryption keys (CMEK) |
| Backup Policies | /v1beta/.../backupPolicies | Scheduled backup retention definitions |
| Resource Events | /v1beta/.../(start|finish)ProjectEvent, /handleResourceEvent | State/event propagation hooks |

(Internal / private operational endpoints reside under `/v1beta/internal/...` and are excluded from the public contract.)

## Common Conventions
### Identification
- All persistent resources use UUID v4 (36 char canonical form) as primary identifier.
- `resourceId` is caller supplied human readable name (unique per parent scope) and is distinct from the UUID.

### Standard Headers
- `X-Correlation-Id` (optional): End-to-end trace correlation. If absent a server value may be generated.
- Some volume backup creation endpoints accept `X-Netapp-Backup-Schedule` (boolean semantics) to influence scheduling.

### Long Running Operations (LRO)
Mutating requests commonly return HTTP 202 with an `Operation_v1beta` object:
```
{
  "name": "/v1beta/projects/{project}/locations/{loc}/operations/{operationId}",
  "done": false
}
```
Clients poll `GET /v1beta/projects/{projectNumber}/locations/{locationId}/operations/{operationId}` until `done=true`.
- On success: `response` field holds the terminal resource representation.
- On failure: `error` object populated (HTTP 200 still returned for polling endpoint). Use code + message.

### Error Model
Consistent error responses via JSON:
```
{
  "code": <number>,
  "message": "<human readable>"
}
```
HTTP status conveys class; `code` refines (maps internally to categorized VSA error ranges). Standard error component references: 400, 401, 403, 404, 409, 422, 429, 500, 501, 503.

### Lifecycle & State Fields
Most resources expose both:
- `*State` (or `lifeCycleState`): coarse state machine (CREATING, READY, UPDATING, DELETING, ERROR, etc.)
- `*StateDetails`: human readable status explanation.
Operations that are logically idempotent will return 202 with existing progressing operation or a completed 200/204 when already in target configuration.

### Labels
Selective resources accept `labels` (string -> string) for billing, grouping, or metadata. Validation: lowercase letters, digits, underscore, dash; must start with letter.

### Security & Encryption
- Default encryption type: `SERVICE_MANAGED`.
- CMEK flows: create KMS config, validate with `.../check`, optionally migrate volumes with `.../encryptVolumes` (returns LRO).

### Backups & Policies
- On-demand backups: POST backups under a vault.
- Scheduled backups: apply a `backupPolicyId` (policy defines retention limits). Volume `backupConfig` shows attachment + scheduling.

### Replication Model
- Source & destination volumes identified by distinct UUIDs and regions/zones.
- Async schedule enumerations (e.g., EVERY_10_MINUTES, HOURLY, DAILY...).
- Mutations (stop, resume, reverse, sync) always LRO.

### Snapshots vs Backups
- Snapshots: local, fast, space efficient, live in volume scope.
- Backups: vaulted (object storage), durable across failures, can seed volume creation.

### HostGroups & Block Volumes
- iSCSI block volumes reference HostGroups for access control (list of initiator IQNs & OS type).

### Resource Events
Project & resource event endpoints integrate with GCNV orchestration to propagate externally detected state changes into the VSA control plane.

## Pagination & Filtering
Current spec does not expose explicit pagination parameters—list endpoints return full collections within reasonable regional scale; future versions may introduce standard `pageSize` / `pageToken`.

## Idempotency Guidance
- Repeating POST with same semantic payload may yield 409 if resourceId already used.
- Safe retries for 5xx / network failures recommended for POST returning 202 (client should de‑duplicate via correlation id when possible).

## Versioning
`v1beta` indicates pre‑GA; breaking changes minimized but not guaranteed absent. Clients should be resilient to additive fields.

## Change Detection
No ETag today; clients wanting change detection poll and compare state or leverage higher level orchestration signals.

## Rate Limiting
429 indicates server enforced throttling (burst or sustained). Honor `Retry-After` if present.

## High Level Flows
1. Provisioning: Create Pool -> Create Volume -> (Optionally) Create Snapshot / Backup -> Attach Policy / Replication.
2. Block Volume Access: Create HostGroup -> Create iSCSI volume referencing HostGroup IDs.
3. Cross Region DR: Create replication (LRO) -> Poll until MIRRORED -> Perform stop/reverse/resume as needed.
4. Encryption Migration: Create KMS config -> check -> encryptVolumes (LRO) -> poll operations.

## Non-Public Endpoints
`/v1beta/internal/...` are operational, used by workflow activities for coordination (job status, replication attribute updates, peering). They are intentionally excluded from public client integrations.

## Compatibility Notes
- All timestamps are RFC 3339 UTC.
- Sizes expressed in bytes unless otherwise documented.
- Boolean toggles default to false when omitted.

## Support Boundaries
This API front ends managed workflows; some state transitions may take extended durations (minutes+) depending on infrastructure provisioning and data copy size.

---
This document summarizes the contract; for per-endpoint details refer to `endpoints.md` and the authoritative OpenAPI (`google-proxy/api/gcp-api.yaml`).

