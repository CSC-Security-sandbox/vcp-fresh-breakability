---
name: google-proxy-api
description: Run REST API calls against the Google Proxy service (GCNV control plane). Use when the user asks to invoke, call, test, or curl any Google Proxy API endpoint such as pool, volume, snapshot, backup, replication, active directory, KMS, host group, backup vault, backup policy, quota rule, or volume performance group operations.
---

# Google Proxy REST API Runner

## Overview

This skill invokes REST API calls on the local Google Proxy service. All endpoints are defined in the OpenAPI spec at `google-proxy/api/gcp-api.yaml`.

## Connection Defaults

| Setting | Value |
|---------|-------|
| Base URL | `http://localhost:9000` |
| Auth | None required when `ENV=local` |
| Content-Type | `application/json` |

Always include `x-correlation-id` header with a descriptive value for traceability.

## How to Invoke

Use `curl` via the Shell tool. Always pretty-print JSON output with `python3 -m json.tool`.

```bash
curl -s -X <METHOD> \
  'http://localhost:9000/<path>' \
  -H 'Content-Type: application/json' \
  -H 'x-correlation-id: <descriptive-id>' \
  [-d '<json-body>'] \
  2>&1 | python3 -m json.tool
```

Use `required_permissions: ["all"]` on all Shell calls.

## Path Variables

All v1beta paths use these variables:

| Variable | Description | Example |
|----------|-------------|---------|
| `{projectNumber}` | GCP project number | `123456789` |
| `{locationId}` | Region or zone | `australia-southeast1-a` |
| `{poolId}` | Pool UUID (36 chars) | `578a3124-7fcc-02a1-cf73-f7f32f13ae84` |
| `{volumeId}` | Volume UUID (36 chars) | `9760acf5-4638-11e7-9bdb-020073ca0001` |
| `{snapshotId}` | Snapshot UUID | |
| `{backupVaultId}` | Backup vault UUID | |
| `{backupId}` | Backup UUID | |
| `{operationId}` | Operation UUID | |

If the user doesn't specify values, use `123456789` for project and `australia-southeast1-a` for location.

## Important Notes

- Zonal pools (`locationId` = zone like `region-a`) only support `serviceLevel: FLEX`
- Regional pools (`locationId` = region) require regional pool support to be enabled
- Pool creation requires `"type": "UNIFIED"` field
- Volume creation requires `poolId` (UUID), `protocols`, and `resourceId`
- Most create/update/delete operations return an async `Operation` — check progress via the describe operation endpoint
- For the full schema of each resource, read the OpenAPI spec at `google-proxy/api/gcp-api.yaml`

## API Catalog

For complete endpoint details including request/response schemas, see [api-reference.md](api-reference.md).

### Health
| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Server health check |

### Pools
| Method | Path | Description |
|--------|------|-------------|
| GET | `/v1beta/projects/{projectNumber}/locations/{locationId}/pools` | List pools |
| POST | `/v1beta/projects/{projectNumber}/locations/{locationId}/pools` | Create pool |
| GET | `/v1beta/projects/{projectNumber}/locations/{locationId}/pools/{poolId}` | Describe pool |
| PUT | `/v1beta/projects/{projectNumber}/locations/{locationId}/pools/{poolId}` | Update pool |
| DELETE | `/v1beta/projects/{projectNumber}/locations/{locationId}/pools/{poolId}` | Delete pool |
| POST | `/v1beta/projects/{projectNumber}/locations/{locationId}/getMultiplePools` | Get multiple pools |
| GET | `/v1beta/projects/{projectNumber}/locations/{locationId}/pools/{poolId}/backupConfigs` | Pool backup configs |
| POST | `/v1beta/projects/{projectNumber}/locations/{locationId}/pools/{poolId}/restoreBackup` | Restore ONTAP mode backup |

### Volumes
| Method | Path | Description |
|--------|------|-------------|
| GET | `/v1beta/projects/{projectNumber}/locations/{locationId}/volumes` | List volumes |
| POST | `/v1beta/projects/{projectNumber}/locations/{locationId}/volumes` | Create volume |
| GET | `/v1beta/projects/{projectNumber}/locations/{locationId}/volumes/{volumeId}` | Describe volume |
| PUT | `/v1beta/projects/{projectNumber}/locations/{locationId}/volumes/{volumeId}` | Update volume |
| DELETE | `/v1beta/projects/{projectNumber}/locations/{locationId}/volumes/{volumeId}` | Delete volume |
| POST | `/v1beta/projects/{projectNumber}/locations/{locationId}/getMultipleVolumes` | Get multiple volumes |
| POST | `/v1beta/projects/{projectNumber}/locations/{locationId}/volumes/{volumeId}/Revert` | Revert to snapshot |
| POST | `/v1beta/projects/{projectNumber}/locations/{locationId}/volumes/{volumeId}/clonesplit` | Split thin clone |
| POST | `/v1beta/projects/{projectNumber}/locations/{locationId}/volumes/{volumeResourceId}/establishPeering` | Establish flexcache peering |

### Snapshots
| Method | Path | Description |
|--------|------|-------------|
| GET | `.../volumes/{volumeId}/snapshots` | List snapshots |
| POST | `.../volumes/{volumeId}/snapshots` | Create snapshot |
| GET | `.../volumes/{volumeId}/snapshots/{snapshotId}` | Describe snapshot |
| PUT | `.../volumes/{volumeId}/snapshots/{snapshotId}` | Update snapshot |
| DELETE | `.../volumes/{volumeId}/snapshots/{snapshotId}` | Delete snapshot |
| POST | `.../volumes/{volumeId}/getMultipleSnapshots` | Get multiple snapshots |

### Replications
| Method | Path | Description |
|--------|------|-------------|
| GET | `.../locations/{locationId}/replications` | List replications |
| POST | `.../volumes/{volumeResourceId}/replications` | Create replication |
| PUT | `.../volumes/{volumeResourceId}/replications/{replicationResourceId}` | Update replication |
| DELETE | `.../volumes/{volumeResourceId}/replications/{replicationResourceId}` | Delete replication |
| POST | `.../replications/{replicationResourceId}/stop` | Stop replication |
| POST | `.../replications/{replicationResourceId}/resume` | Resume replication |
| POST | `.../replications/{replicationResourceId}/reverseAndResumeReplication` | Reverse and resume |
| POST | `.../replications/{replicationResourceId}/sync` | Sync replication |
| POST | `.../replications/{replicationResourceId}/establishPeering` | Establish peering |

### Backups & Backup Vaults
| Method | Path | Description |
|--------|------|-------------|
| GET | `.../backupVaults` | List backup vaults |
| POST | `.../backupVaults` | Create backup vault |
| GET | `.../backupVaults/{backupVaultId}` | Describe backup vault |
| PUT | `.../backupVaults/{backupVaultId}` | Update backup vault |
| DELETE | `.../backupVaults/{backupVaultId}` | Delete backup vault |
| GET | `.../backupVaults/{backupVaultId}/backups` | List backups |
| POST | `.../backupVaults/{backupVaultId}/backups` | Create backup |
| GET | `.../backupVaults/{backupVaultId}/backups/{backupId}` | Describe backup |
| PUT | `.../backupVaults/{backupVaultId}/backups/{backupId}` | Update backup |
| DELETE | `.../backupVaults/{backupVaultId}/backups/{backupId}` | Delete backup |
| POST | `.../backupVaults/{backupVaultId}/rotateCmekBackups` | Rotate CMEK |
| POST | `.../volumes/{volumeId}/restoreFilesFromBackup` | Restore files from backup |

### Backup Policies
| Method | Path | Description |
|--------|------|-------------|
| GET | `.../backupPolicies` | List backup policies |
| POST | `.../backupPolicies` | Create backup policy |
| GET | `.../backupPolicies/{backupPolicyId}` | Describe backup policy |
| PUT | `.../backupPolicies/{backupPolicyId}` | Update backup policy |
| DELETE | `.../backupPolicies/{backupPolicyId}` | Delete backup policy |

### Active Directories
| Method | Path | Description |
|--------|------|-------------|
| GET | `.../storage/activeDirectory` | List active directories |
| POST | `.../storage/activeDirectory` | Create active directory |
| GET | `.../storage/activeDirectory/{activeDirectoryId}` | Describe active directory |
| PUT | `.../storage/activeDirectory/{activeDirectoryId}` | Update active directory |
| DELETE | `.../storage/activeDirectory/{activeDirectoryId}` | Delete active directory |

### KMS Configurations
| Method | Path | Description |
|--------|------|-------------|
| GET | `.../storage/kmsConfig` | List KMS configs |
| POST | `.../storage/kmsConfig` | Create KMS config |
| GET | `.../storage/kmsConfig/{kmsConfigId}` | Describe KMS config |
| PUT | `.../storage/kmsConfig/{kmsConfigId}` | Update KMS config |
| DELETE | `.../storage/kmsConfig/{kmsConfigId}` | Delete KMS config |
| GET | `.../storage/kmsConfig/{kmsConfigId}/check` | Verify KMS reachability |
| POST | `.../storage/kmsConfig/{kmsConfigId}/encryptVolumes` | Migrate to CMEK |

### Host Groups
| Method | Path | Description |
|--------|------|-------------|
| GET | `.../hostGroups` | List host groups |
| POST | `.../hostGroups` | Create host group |
| GET | `.../hostGroups/{hostGroupId}` | Describe host group |
| PUT | `.../hostGroups/{hostGroupId}` | Update host group |
| DELETE | `.../hostGroups/{hostGroupId}` | Delete host group |

### Volume Performance Groups
| Method | Path | Description |
|--------|------|-------------|
| GET | `.../pools/{poolId}/volumePerformanceGroups` | List VPGs |
| POST | `.../pools/{poolId}/volumePerformanceGroups` | Create VPG |
| GET | `.../pools/{poolId}/volumePerformanceGroups/{vpgId}` | Describe VPG |
| PUT | `.../pools/{poolId}/volumePerformanceGroups/{vpgId}` | Update VPG |
| DELETE | `.../pools/{poolId}/volumePerformanceGroups/{vpgId}` | Delete VPG |

### Quota Rules
| Method | Path | Description |
|--------|------|-------------|
| GET | `.../volumes/{volumeId}/quotaRules` | List quota rules |
| POST | `.../volumes/{volumeId}/quotaRules` | Create quota rule |
| GET | `.../volumes/{volumeId}/quotaRules/{quotaRuleId}` | Describe quota rule |
| PUT | `.../volumes/{volumeId}/quotaRules/{quotaRuleId}` | Update quota rule |
| DELETE | `.../volumes/{volumeId}/quotaRules/{quotaRuleId}` | Delete quota rule |

### Operations (Async)
| Method | Path | Description |
|--------|------|-------------|
| GET | `.../operations/{operationId}` | Check operation status |

## Common Request Body Templates

### Create Pool (required: network, resourceId, serviceLevel, sizeInBytes, type)
```json
{
  "resourceId": "my-pool",
  "serviceLevel": "FLEX",
  "sizeInBytes": 2199023255552,
  "network": "projects/123456789/global/networks/test-network",
  "type": "UNIFIED"
}
```

### Create Volume (required: poolId, protocols, resourceId)
```json
{
  "poolId": "<pool-uuid>",
  "resourceId": "my-volume",
  "protocols": ["NFSv3"],
  "capacityGib": 100,
  "shareName": "my-share"
}
```

### Create Snapshot
```json
{
  "resourceId": "my-snapshot",
  "description": "Manual snapshot"
}
```

### Create Backup Vault
```json
{
  "resourceId": "my-backup-vault"
}
```

### Create Backup
```json
{
  "sourceVolumeId": "<volume-uuid>",
  "resourceId": "my-backup",
  "description": "Manual backup"
}
```

### Create Active Directory
```json
{
  "resourceId": "my-ad",
  "domain": "example.com",
  "site": "Default-First-Site-Name",
  "dns": "10.0.0.1",
  "netBIOS": "EXAMPLE",
  "username": "admin",
  "password": "secret",
  "organizationalUnit": "CN=Computers"
}
```

### Create Backup Policy
```json
{
  "resourceId": "my-backup-policy",
  "dailyBackupsToKeep": 7,
  "weeklyBackupsToKeep": 4,
  "monthlyBackupsToKeep": 12
}
```

## Workflow: Polling Async Operations

Most mutating operations (create, update, delete) return an async `Operation`. **Always poll the operation automatically** after a mutating call.

### Polling Procedure

1. Extract the `operationId` from the `name` field of the response (the UUID at the end of the path).
2. Run the poll loop below via the Shell tool with `block_until_ms` set high enough (e.g., 300000 for 5 minutes).
3. Report the final result to the user.

```bash
BASE_URL="http://localhost:9000"
OP_PATH="/v1beta/projects/<projectNumber>/locations/<locationId>/operations/<operationId>"
MAX_ATTEMPTS=60

for i in $(seq 1 $MAX_ATTEMPTS); do
  RESULT=$(curl -s "${BASE_URL}${OP_PATH}" 2>&1)
  DONE=$(echo "$RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('done', False))" 2>/dev/null)

  if [ "$DONE" = "True" ]; then
    echo "=== Operation completed (attempt $i) ==="
    echo "$RESULT" | python3 -m json.tool
    break
  fi

  echo "Poll $i: still in progress..."
  sleep 5
done

if [ "$DONE" != "True" ]; then
  echo "=== Operation did not complete after $MAX_ATTEMPTS polls ==="
  echo "$RESULT" | python3 -m json.tool
fi
```

### Interpreting Results

| Field | Meaning |
|-------|---------|
| `"done": false` | Operation still in progress, keep polling |
| `"done": true` + `"response": {...}` | Operation succeeded; `response` contains the resource |
| `"done": true` + `"error": {...}` | Operation failed; `error.code` and `error.message` describe the failure |

### When to Poll

- **Always** poll after: create pool, create volume, update pool, update volume, delete pool, delete volume, create/delete snapshot, create/delete replication, replication stop/resume/sync/reverse, create/delete backup, create/delete backup vault, create/delete active directory, create/delete KMS config, create/update/delete host group, create/update/delete VPG, create/update/delete quota rule, revert volume, split clone, encrypt volumes, rotate CMEK.
- **Never** poll after: list/describe (GET) operations, health check.
