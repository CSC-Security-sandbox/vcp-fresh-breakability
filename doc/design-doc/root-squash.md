# Root Squash & All Squash (Export Policy)

## Overview

This document describes the design and implementation of **root squash** and **all squash** behavior for NFS export policy rules in the VSA Control Plane. These settings control how NFS clients are mapped to anonymous users and whether root has privileged access, as exposed by the GCP API schema `SimpleExportPolicyRule_v1beta`.

**Relevant API schema**: `SimpleExportPolicyRule_v1beta` in `google-proxy/api/gcp-api.yaml`, with fields `hasRootAccess`, `allSquash`, and `anonUid`.

## API Contract (SimpleExportPolicyRule_v1beta)

### Fields

| Field | Type | Description |
|-------|------|-------------|
| **hasRootAccess** | string (nullable) | If enabled (`true` or `on`), the rule sets **no_root_squash**; if disabled (`false` or `off`), **root_squash** is set and root is mapped to the anonymous user (default 65534). Response normalizes to `true`/`false`. Default when null: `true`. |
| **allSquash** | boolean (nullable) | If `true`, **all** UIDs are mapped to the anonymous UID given by `anonUid`. When `allSquash` is true, `hasRootAccess` must be `false` for that rule. Default: `false`. |
| **anonUid** | int64 (nullable) | The anonymous UID (0–4294967295) to which UIDs are mapped when `allSquash` is true. **Required when `allSquash` is true.** |

### Constraints (enforced by implementation)

- When **allSquash** is `true` for a rule:
  - **anonUid** must be set (validation error: "AnonUid must be set when AllSquash is enabled").
  - **hasRootAccess** must not be true/on (validation error: "RootSquash cannot be enabled when AllSquash is true for the same rule").
  - **accessType** must be `READ_WRITE` (validation error: "AccessType must be READ_WRITE when AllSquash is enabled").
  - Kerberos options must not be enabled (validation error: "AllSquash cannot be enabled for Kerberos-enabled export rules").
- Only **one** rule with **allSquash** `true` is allowed per export policy (validation error: "only one AllSquash rule is allowed per export policy").

## Feature Flag

- **`IS_ALL_SQUASH_ENABLED`** (env, default `true`): When `true`, the proxy accepts and validates `allSquash` and `anonUid`, and maps them through to the internal model and ONTAP. When `false`, the allSquash path is not used (legacy behavior).

**Code reference**: `utils/util.go` — `IsAllSquashEnabled = env.GetBool("IS_ALL_SQUASH_ENABLED", true)`.

## Validation

### API layer (google-proxy)

- **`validateAllSquash(rules []SimpleExportPolicyRuleV1beta)`** in `google-proxy/api/endpoints/volume_endpoint.go`:
  - Ensures at most one rule has `allSquash == true`.
  - For each rule with `allSquash == true`: requires `anonUid` set, `accessType == READ_WRITE`, `hasRootAccess` not true/on, and no Kerberos flags set.
- Invoked during **Create** and **Update** volume when `utils.IsAllSquashEnabled` is true and export policy is present.

### Schema / generated validators

- **`SimpleExportPolicyRuleV1beta.Validate()`** in `google-proxy/api/gcp-servergen/oas_validators_gen.go`: validates `allowedClients` length, `hasRootAccess` enum, `accessType` enum, and `anonUid` range (0–4294967295) when set. It does **not** enforce the cross-field rule that `anonUid` is required when `allSquash` is true; that is enforced by `validateAllSquash`.

## Data Flow

### Request → internal model

1. **Create / Update volume** (e.g. `prepareCreateVolumeParams`, `prepareUpdateVolumeParams` in `volume_endpoint.go`):
   - If `IsAllSquashEnabled`, `validateAllSquash(exportPolicy.GetRules())` is called.
   - Each `SimpleExportPolicyRuleV1beta` is mapped to `models.ExportRule`:
     - `hasRootAccess` (true/on) → `Superuser: true`; (false/off) → `Superuser: false`.
     - `allSquash` → `AllSquash *bool`; `anonUid` → `AnonUid *int64`.
   - These `ExportRule` values are stored on the volume’s `FileProperties.ExportPolicy.ExportRules` and flow through orchestrator and activities.

**Code references**:
- Create: `google-proxy/api/endpoints/volume_endpoint.go` (e.g. ~602–636) — validation and mapping of rules including `AllSquash`, `AnonUid`, `HasRootAccess` → `Superuser`.
- Update: same file, ~1210–1262 — `validateAllSquash` and rule mapping with `AllSquash`/`AnonUid` when `IsAllSquashEnabled`.

### Internal model → ONTAP

- **`convertStorageExportPolicyRuleToONTAP`** in `core/vsa/export_policy.go`:
  - **Superuser (root access):** `rule.Superuser` → `SuperUserRule`: if true → `AnyAccessProtocol` (no_root_squash); if false → `NoneAccessProtocol` (root_squash).
  - **Anonymous user:** Default `anonUser = models.RootAnonymousUser` ("root"). When `IsAllSquashEnabled` and `rule.AllSquash != nil && *rule.AllSquash`, and `rule.AnonUid != nil`, `anonUser` is set from `rule.AnonUid` (numeric string). Otherwise, if `rule.AnonymousUser != ""`, that value is used.
  - Resulting ONTAP export rule uses `AnonymousUser: anonUser` and `SuperUserRule` as above.

**Code reference**: `core/vsa/export_policy.go` (e.g. lines 39–71).

### Internal model → Get Volume response

- When building the Get Volume response, each `ExportRule` is converted back to `SimpleExportPolicyRuleV1beta`:
  - `Superuser` → `HasRootAccess`: true → `"true"`, false → `"false"`.
  - `AllSquash` and `AnonUid` are only set on the response rule when they are non-nil (so unset rules do not expose `allSquash`/`anonUid` in the response).

**Code reference**: `google-proxy/api/endpoints/volume_endpoint.go` (e.g. ~1678–1710) — building `ruleV1beta` from `volume.FileProperties.ExportPolicy.ExportRules` with `AllSquash` and `AnonUid` only when explicitly set.

## Internal Types

- **GCP API (generated):** `SimpleExportPolicyRuleV1beta` in `google-proxy/api/gcp-servergen/oas_schemas_gen.go`: includes `AllSquash OptNilBool`, `AnonUid OptNilInt64`, `HasRootAccess OptNilSimpleExportPolicyRuleV1betaHasRootAccess`.
- **Core/internal:** `vsa.ExportRule` in `core/vsa/models.go`: `Superuser bool`, `AllSquash *bool`, `AnonUid *int64`, `AnonymousUser string`. Same shape is used in `core/models` and datamodel export rule types for volume create/update and export-policy application.
- **ONTAP:** Export rule uses `AnonymousUser` (string) and superuser behavior via `SuperUserRule`; all-squash semantics are achieved by mapping all UIDs to the given `AnonymousUser` (anonUid) when allSquash is true in the API.

## Workflows

- **Volume create:** Export policy (including allSquash/anonUid when enabled) is validated and stored on the volume; `CreateExportPolicyInOntap` in `core/orchestrator/activities/volume_create_activities.go` passes `AllSquash` and `AnonUid` on each rule to the VSA layer, which calls `convertStorageExportPolicyRuleToONTAP`.
- **Volume update:** Export policy updates are validated (including `validateAllSquash` when `IsAllSquashEnabled`), then applied via the update path; export policy rules are again converted with `convertStorageExportPolicyRuleToONTAP` when updating ONTAP.

## Summary

| Concept | API | Internal / ONTAP |
|--------|-----|-------------------|
| Root squash | `hasRootAccess: "false"` / `"off"` | `Superuser: false` → `SuperUserRule: NoneAccessProtocol` |
| No root squash | `hasRootAccess: "true"` / `"on"` | `Superuser: true` → `SuperUserRule: AnyAccessProtocol` |
| All squash | `allSquash: true` + `anonUid` (required) | `AllSquash: true`, `AnonUid` → `AnonymousUser` in ONTAP rule; `hasRootAccess` must be false |
| Anonymous UID | `anonUid` (0–4294967295) when `allSquash` true | Stored on rule; sent to ONTAP as `AnonymousUser` string |

The implementation ensures that **allSquash** is only allowed when root squash is effectively on (hasRootAccess false), and that **anonUid** is required and used as the single anonymous UID for that rule when allSquash is enabled.
