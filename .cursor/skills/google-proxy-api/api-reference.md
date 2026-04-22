# Google Proxy API Reference

Full curl examples for every endpoint. Base URL: `http://localhost:9000`

All paths below use the prefix: `/v1beta/projects/{projectNumber}/locations/{locationId}`

Shorthand: `$BASE = http://localhost:9000/v1beta/projects/123456789/locations/australia-southeast1-a`

---

## Health

```bash
curl -s http://localhost:9000/health | python3 -m json.tool
```

---

## Pools

### List Pools
```bash
curl -s "$BASE/pools" | python3 -m json.tool
```

### Create Pool
Required fields: `resourceId`, `serviceLevel`, `sizeInBytes`, `network`, `type`

```bash
curl -s -X POST "$BASE/pools" \
  -H 'Content-Type: application/json' \
  -H 'x-correlation-id: create-pool-001' \
  -d '{
    "resourceId": "my-pool",
    "serviceLevel": "FLEX",
    "sizeInBytes": 2199023255552,
    "network": "projects/123456789/global/networks/test-network",
    "type": "UNIFIED"
  }' | python3 -m json.tool
```

Optional fields: `description`, `labels`, `allowAutoTiering`, `activeDirectoryConfigId`, `kmsConfigId`, `ldapEnabled`, `zone`, `secondaryZone`, `storageClass`

### Describe Pool
```bash
curl -s "$BASE/pools/{poolId}" | python3 -m json.tool
```

### Update Pool
```bash
curl -s -X PUT "$BASE/pools/{poolId}" \
  -H 'Content-Type: application/json' \
  -H 'x-correlation-id: update-pool-001' \
  -d '{
    "sizeInBytes": 4398046511104,
    "description": "Updated pool"
  }' | python3 -m json.tool
```

### Delete Pool
```bash
curl -s -X DELETE "$BASE/pools/{poolId}" \
  -H 'x-correlation-id: delete-pool-001' | python3 -m json.tool
```

### Get Multiple Pools
```bash
curl -s -X POST "$BASE/getMultiplePools" \
  -H 'Content-Type: application/json' \
  -d '{
    "poolIds": ["<uuid-1>", "<uuid-2>"]
  }' | python3 -m json.tool
```

### Get Backup Configs for Pool
```bash
curl -s "$BASE/pools/{poolId}/backupConfigs" | python3 -m json.tool
```

### Restore ONTAP Mode Backup
```bash
curl -s -X POST "$BASE/pools/{poolId}/restoreBackup" \
  -H 'Content-Type: application/json' \
  -H 'x-correlation-id: restore-backup-001' \
  -d '{
    "backupId": "<backup-uuid>",
    "volumeResourceId": "target-volume"
  }' | python3 -m json.tool
```

---

## Volumes

### List Volumes
```bash
curl -s "$BASE/volumes" | python3 -m json.tool
```

Query parameters: `poolId`, `includeDeleted`

### Create Volume
Required fields: `poolId`, `protocols`, `resourceId`

```bash
curl -s -X POST "$BASE/volumes" \
  -H 'Content-Type: application/json' \
  -H 'x-correlation-id: create-vol-001' \
  -d '{
    "poolId": "<pool-uuid>",
    "resourceId": "my-volume",
    "protocols": ["NFSv3"],
    "capacityGib": 100,
    "shareName": "my-share"
  }' | python3 -m json.tool
```

Optional fields: `description`, `labels`, `exportPolicy`, `smbSettings`, `snapshotPolicy`, `securityStyle`, `backupConfig`, `restrictedActions`, `snapReserve`, `snapshotDirectory`, `unixPermissions`, `tieringPolicy`

Protocols: `NFSv3`, `NFSv4`, `SMB`, `DUAL`, `ISCSI`, `FLEX`

### Describe Volume
```bash
curl -s "$BASE/volumes/{volumeId}" | python3 -m json.tool
```

### Update Volume
```bash
curl -s -X PUT "$BASE/volumes/{volumeId}" \
  -H 'Content-Type: application/json' \
  -H 'x-correlation-id: update-vol-001' \
  -d '{
    "capacityGib": 200,
    "description": "Updated volume"
  }' | python3 -m json.tool
```

### Delete Volume
```bash
curl -s -X DELETE "$BASE/volumes/{volumeId}" \
  -H 'x-correlation-id: delete-vol-001' | python3 -m json.tool
```

Query parameters: `force` (boolean)

### Get Multiple Volumes
```bash
curl -s -X POST "$BASE/getMultipleVolumes" \
  -H 'Content-Type: application/json' \
  -d '{
    "volumeIds": ["<uuid-1>", "<uuid-2>"]
  }' | python3 -m json.tool
```

### Revert Volume to Snapshot
```bash
curl -s -X POST "$BASE/volumes/{volumeId}/Revert" \
  -H 'Content-Type: application/json' \
  -H 'x-correlation-id: revert-vol-001' \
  -d '{
    "snapshotId": "<snapshot-uuid>"
  }' | python3 -m json.tool
```

### Split Thin Clone
```bash
curl -s -X POST "$BASE/volumes/{volumeId}/clonesplit" \
  -H 'x-correlation-id: split-clone-001' | python3 -m json.tool
```

### Establish Flexcache Peering
```bash
curl -s -X POST "$BASE/volumes/{volumeResourceId}/establishPeering" \
  -H 'Content-Type: application/json' \
  -H 'x-correlation-id: peering-001' \
  -d '{
    "peerClusterName": "cluster-name",
    "peerSvmName": "svm-name"
  }' | python3 -m json.tool
```

---

## Snapshots

All snapshot paths: `$BASE/volumes/{volumeId}/snapshots`

### List Snapshots
```bash
curl -s "$BASE/volumes/{volumeId}/snapshots" | python3 -m json.tool
```

### Create Snapshot
```bash
curl -s -X POST "$BASE/volumes/{volumeId}/snapshots" \
  -H 'Content-Type: application/json' \
  -H 'x-correlation-id: create-snap-001' \
  -d '{
    "resourceId": "my-snapshot",
    "description": "Manual snapshot"
  }' | python3 -m json.tool
```

### Describe Snapshot
```bash
curl -s "$BASE/volumes/{volumeId}/snapshots/{snapshotId}" | python3 -m json.tool
```

### Update Snapshot
```bash
curl -s -X PUT "$BASE/volumes/{volumeId}/snapshots/{snapshotId}" \
  -H 'Content-Type: application/json' \
  -H 'x-correlation-id: update-snap-001' \
  -d '{
    "description": "Updated snapshot"
  }' | python3 -m json.tool
```

### Delete Snapshot
```bash
curl -s -X DELETE "$BASE/volumes/{volumeId}/snapshots/{snapshotId}" \
  -H 'x-correlation-id: delete-snap-001' | python3 -m json.tool
```

### Get Multiple Snapshots
```bash
curl -s -X POST "$BASE/volumes/{volumeId}/getMultipleSnapshots" \
  -H 'Content-Type: application/json' \
  -d '{
    "snapshotIds": ["<uuid-1>", "<uuid-2>"]
  }' | python3 -m json.tool
```

---

## Replications

### List Replications
```bash
curl -s "$BASE/replications" | python3 -m json.tool
```

### Create Replication
```bash
curl -s -X POST "$BASE/volumes/{volumeResourceId}/replications" \
  -H 'Content-Type: application/json' \
  -H 'x-correlation-id: create-repl-001' \
  -d '{
    "resourceId": "my-replication",
    "destinationVolumeParameters": {
      "storagePool": "<dest-pool-uuid>",
      "volumeId": "<dest-volume-resource-id>",
      "shareName": "dest-share"
    },
    "replicationSchedule": "EVERY_10_MINUTES"
  }' | python3 -m json.tool
```

### Update Replication
```bash
curl -s -X PUT "$BASE/volumes/{volumeResourceId}/replications/{replicationResourceId}" \
  -H 'Content-Type: application/json' \
  -H 'x-correlation-id: update-repl-001' \
  -d '{
    "replicationSchedule": "HOURLY"
  }' | python3 -m json.tool
```

### Delete Replication
```bash
curl -s -X DELETE "$BASE/volumes/{volumeResourceId}/replications/{replicationResourceId}" \
  -H 'x-correlation-id: delete-repl-001' | python3 -m json.tool
```

### Stop Replication
```bash
curl -s -X POST "$BASE/volumes/{volumeResourceId}/replications/{replicationResourceId}/stop" \
  -H 'Content-Type: application/json' \
  -H 'x-correlation-id: stop-repl-001' \
  -d '{}' | python3 -m json.tool
```

### Resume Replication
```bash
curl -s -X POST "$BASE/volumes/{volumeResourceId}/replications/{replicationResourceId}/resume" \
  -H 'Content-Type: application/json' \
  -H 'x-correlation-id: resume-repl-001' \
  -d '{}' | python3 -m json.tool
```

### Reverse and Resume Replication
```bash
curl -s -X POST "$BASE/volumes/{volumeResourceId}/replications/{replicationResourceId}/reverseAndResumeReplication" \
  -H 'Content-Type: application/json' \
  -H 'x-correlation-id: reverse-repl-001' \
  -d '{}' | python3 -m json.tool
```

### Sync Replication
```bash
curl -s -X POST "$BASE/volumes/{volumeResourceId}/replications/{replicationResourceId}/sync" \
  -H 'Content-Type: application/json' \
  -H 'x-correlation-id: sync-repl-001' \
  -d '{}' | python3 -m json.tool
```

### Establish Replication Peering
```bash
curl -s -X POST "$BASE/volumes/{volumeResourceId}/replications/{replicationResourceId}/establishPeering" \
  -H 'Content-Type: application/json' \
  -H 'x-correlation-id: peer-repl-001' \
  -d '{
    "peerClusterName": "cluster-name",
    "peerSvmName": "svm-name"
  }' | python3 -m json.tool
```

---

## Backup Vaults

### List Backup Vaults
```bash
curl -s "$BASE/backupVaults" | python3 -m json.tool
```

### Create Backup Vault
```bash
curl -s -X POST "$BASE/backupVaults" \
  -H 'Content-Type: application/json' \
  -H 'x-correlation-id: create-bv-001' \
  -d '{
    "resourceId": "my-backup-vault"
  }' | python3 -m json.tool
```

### Describe Backup Vault
```bash
curl -s "$BASE/backupVaults/{backupVaultId}" | python3 -m json.tool
```

### Update Backup Vault
```bash
curl -s -X PUT "$BASE/backupVaults/{backupVaultId}" \
  -H 'Content-Type: application/json' \
  -H 'x-correlation-id: update-bv-001' \
  -d '{
    "description": "Updated vault"
  }' | python3 -m json.tool
```

### Delete Backup Vault
```bash
curl -s -X DELETE "$BASE/backupVaults/{backupVaultId}" \
  -H 'x-correlation-id: delete-bv-001' | python3 -m json.tool
```

### Rotate CMEK Backups
```bash
curl -s -X POST "$BASE/backupVaults/{backupVaultId}/rotateCmekBackups" \
  -H 'Content-Type: application/json' \
  -H 'x-correlation-id: rotate-cmek-001' \
  -d '{}' | python3 -m json.tool
```

---

## Backups

### List Backups
```bash
curl -s "$BASE/backupVaults/{backupVaultId}/backups" | python3 -m json.tool
```

### Create Backup
```bash
curl -s -X POST "$BASE/backupVaults/{backupVaultId}/backups" \
  -H 'Content-Type: application/json' \
  -H 'x-correlation-id: create-backup-001' \
  -d '{
    "sourceVolumeId": "<volume-uuid>",
    "resourceId": "my-backup",
    "description": "Manual backup"
  }' | python3 -m json.tool
```

### Describe Backup
```bash
curl -s "$BASE/backupVaults/{backupVaultId}/backups/{backupId}" | python3 -m json.tool
```

### Update Backup
```bash
curl -s -X PUT "$BASE/backupVaults/{backupVaultId}/backups/{backupId}" \
  -H 'Content-Type: application/json' \
  -H 'x-correlation-id: update-backup-001' \
  -d '{
    "description": "Updated backup"
  }' | python3 -m json.tool
```

### Delete Backup
```bash
curl -s -X DELETE "$BASE/backupVaults/{backupVaultId}/backups/{backupId}" \
  -H 'x-correlation-id: delete-backup-001' | python3 -m json.tool
```

### Restore Files from Backup
```bash
curl -s -X POST "$BASE/volumes/{volumeId}/restoreFilesFromBackup" \
  -H 'Content-Type: application/json' \
  -H 'x-correlation-id: restore-files-001' \
  -d '{
    "backupId": "<backup-uuid>",
    "filePaths": ["/path/to/file1", "/path/to/file2"]
  }' | python3 -m json.tool
```

---

## Backup Policies

### List Backup Policies
```bash
curl -s "$BASE/backupPolicies" | python3 -m json.tool
```

### Create Backup Policy
```bash
curl -s -X POST "$BASE/backupPolicies" \
  -H 'Content-Type: application/json' \
  -H 'x-correlation-id: create-bp-001' \
  -d '{
    "resourceId": "my-backup-policy",
    "dailyBackupsToKeep": 7,
    "weeklyBackupsToKeep": 4,
    "monthlyBackupsToKeep": 12
  }' | python3 -m json.tool
```

### Describe Backup Policy
```bash
curl -s "$BASE/backupPolicies/{backupPolicyId}" | python3 -m json.tool
```

### Update Backup Policy
```bash
curl -s -X PUT "$BASE/backupPolicies/{backupPolicyId}" \
  -H 'Content-Type: application/json' \
  -H 'x-correlation-id: update-bp-001' \
  -d '{
    "dailyBackupsToKeep": 14
  }' | python3 -m json.tool
```

### Delete Backup Policy
```bash
curl -s -X DELETE "$BASE/backupPolicies/{backupPolicyId}" \
  -H 'x-correlation-id: delete-bp-001' | python3 -m json.tool
```

---

## Active Directories

### List Active Directories
```bash
curl -s "$BASE/storage/activeDirectory" | python3 -m json.tool
```

### Create Active Directory
```bash
curl -s -X POST "$BASE/storage/activeDirectory" \
  -H 'Content-Type: application/json' \
  -H 'x-correlation-id: create-ad-001' \
  -d '{
    "resourceId": "my-ad",
    "domain": "example.com",
    "site": "Default-First-Site-Name",
    "dns": "10.0.0.1",
    "netBIOS": "EXAMPLE",
    "username": "admin",
    "password": "secret",
    "organizationalUnit": "CN=Computers"
  }' | python3 -m json.tool
```

### Describe Active Directory
```bash
curl -s "$BASE/storage/activeDirectory/{activeDirectoryId}" | python3 -m json.tool
```

### Update Active Directory
```bash
curl -s -X PUT "$BASE/storage/activeDirectory/{activeDirectoryId}" \
  -H 'Content-Type: application/json' \
  -H 'x-correlation-id: update-ad-001' \
  -d '{
    "dns": "10.0.0.2"
  }' | python3 -m json.tool
```

### Delete Active Directory
```bash
curl -s -X DELETE "$BASE/storage/activeDirectory/{activeDirectoryId}" \
  -H 'x-correlation-id: delete-ad-001' | python3 -m json.tool
```

---

## KMS Configurations

### List KMS Configs
```bash
curl -s "$BASE/storage/kmsConfig" | python3 -m json.tool
```

### Create KMS Config
```bash
curl -s -X POST "$BASE/storage/kmsConfig" \
  -H 'Content-Type: application/json' \
  -H 'x-correlation-id: create-kms-001' \
  -d '{
    "resourceId": "my-kms-config",
    "cryptoKeyName": "projects/my-project/locations/global/keyRings/my-ring/cryptoKeys/my-key"
  }' | python3 -m json.tool
```

### Describe KMS Config
```bash
curl -s "$BASE/storage/kmsConfig/{kmsConfigId}" | python3 -m json.tool
```

### Update KMS Config
```bash
curl -s -X PUT "$BASE/storage/kmsConfig/{kmsConfigId}" \
  -H 'Content-Type: application/json' \
  -H 'x-correlation-id: update-kms-001' \
  -d '{
    "cryptoKeyName": "projects/my-project/locations/global/keyRings/my-ring/cryptoKeys/new-key"
  }' | python3 -m json.tool
```

### Delete KMS Config
```bash
curl -s -X DELETE "$BASE/storage/kmsConfig/{kmsConfigId}" \
  -H 'x-correlation-id: delete-kms-001' | python3 -m json.tool
```

### Verify KMS Reachability
```bash
curl -s "$BASE/storage/kmsConfig/{kmsConfigId}/check" | python3 -m json.tool
```

### Encrypt Volumes (Migrate to CMEK)
```bash
curl -s -X POST "$BASE/storage/kmsConfig/{kmsConfigId}/encryptVolumes" \
  -H 'Content-Type: application/json' \
  -H 'x-correlation-id: encrypt-vols-001' \
  -d '{}' | python3 -m json.tool
```

---

## Host Groups

### List Host Groups
```bash
curl -s "$BASE/hostGroups" | python3 -m json.tool
```

### Create Host Group
```bash
curl -s -X POST "$BASE/hostGroups" \
  -H 'Content-Type: application/json' \
  -H 'x-correlation-id: create-hg-001' \
  -d '{
    "resourceId": "my-host-group",
    "protocol": "ISCSI",
    "initiators": [
      {"iqn": "iqn.2023-01.com.example:initiator1"}
    ]
  }' | python3 -m json.tool
```

### Describe Host Group
```bash
curl -s "$BASE/hostGroups/{hostGroupId}" | python3 -m json.tool
```

### Update Host Group
```bash
curl -s -X PUT "$BASE/hostGroups/{hostGroupId}" \
  -H 'Content-Type: application/json' \
  -H 'x-correlation-id: update-hg-001' \
  -d '{
    "initiators": [
      {"iqn": "iqn.2023-01.com.example:initiator1"},
      {"iqn": "iqn.2023-01.com.example:initiator2"}
    ]
  }' | python3 -m json.tool
```

### Delete Host Group
```bash
curl -s -X DELETE "$BASE/hostGroups/{hostGroupId}" \
  -H 'x-correlation-id: delete-hg-001' | python3 -m json.tool
```

---

## Volume Performance Groups

### List VPGs
```bash
curl -s "$BASE/pools/{poolId}/volumePerformanceGroups" | python3 -m json.tool
```

### Create VPG
```bash
curl -s -X POST "$BASE/pools/{poolId}/volumePerformanceGroups" \
  -H 'Content-Type: application/json' \
  -H 'x-correlation-id: create-vpg-001' \
  -d '{
    "resourceId": "my-vpg",
    "totalIops": 10000,
    "totalThroughputMibps": 128
  }' | python3 -m json.tool
```

### Describe VPG
```bash
curl -s "$BASE/pools/{poolId}/volumePerformanceGroups/{vpgId}" | python3 -m json.tool
```

### Update VPG
```bash
curl -s -X PUT "$BASE/pools/{poolId}/volumePerformanceGroups/{vpgId}" \
  -H 'Content-Type: application/json' \
  -H 'x-correlation-id: update-vpg-001' \
  -d '{
    "totalIops": 20000
  }' | python3 -m json.tool
```

### Delete VPG
```bash
curl -s -X DELETE "$BASE/pools/{poolId}/volumePerformanceGroups/{vpgId}" \
  -H 'x-correlation-id: delete-vpg-001' | python3 -m json.tool
```

---

## Quota Rules

### List Quota Rules
```bash
curl -s "$BASE/volumes/{volumeId}/quotaRules" | python3 -m json.tool
```

### Create Quota Rule
```bash
curl -s -X POST "$BASE/volumes/{volumeId}/quotaRules" \
  -H 'Content-Type: application/json' \
  -H 'x-correlation-id: create-qr-001' \
  -d '{
    "type": "INDIVIDUAL_USER_QUOTA",
    "target": "1001",
    "diskLimitMib": 10240,
    "description": "User quota"
  }' | python3 -m json.tool
```

Types: `INDIVIDUAL_USER_QUOTA`, `INDIVIDUAL_GROUP_QUOTA`, `DEFAULT_USER_QUOTA`, `DEFAULT_GROUP_QUOTA`

### Describe Quota Rule
```bash
curl -s "$BASE/volumes/{volumeId}/quotaRules/{quotaRuleId}" | python3 -m json.tool
```

### Update Quota Rule
```bash
curl -s -X PUT "$BASE/volumes/{volumeId}/quotaRules/{quotaRuleId}" \
  -H 'Content-Type: application/json' \
  -H 'x-correlation-id: update-qr-001' \
  -d '{
    "diskLimitMib": 20480
  }' | python3 -m json.tool
```

### Delete Quota Rule
```bash
curl -s -X DELETE "$BASE/volumes/{volumeId}/quotaRules/{quotaRuleId}" \
  -H 'x-correlation-id: delete-qr-001' | python3 -m json.tool
```

---

## Operations (Async)

### Describe Operation (single check)
```bash
curl -s "$BASE/operations/{operationId}" | python3 -m json.tool
```

### Poll Operation Until Complete (5-second interval)

After any mutating API call (create, update, delete, etc.), automatically poll the returned operation:

```bash
BASE_URL="http://localhost:9000"
OP_PATH="/v1beta/projects/123456789/locations/australia-southeast1-a/operations/{operationId}"
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

Response when in progress: `{"name": "...", "done": false, "response": "Job is in progress"}`
Response when complete: `{"name": "...", "done": true, "response": {...}}`
Response when failed: `{"name": "...", "done": true, "error": {...}}`

---

## Resource Events (1P Integration)

### Start Project Event
```bash
curl -s -X POST "$BASE/startProjectEvent" \
  -H 'Content-Type: application/json' \
  -H 'x-correlation-id: start-event-001' \
  -d '{...}' | python3 -m json.tool
```

### Handle Resource Event
```bash
curl -s -X PUT "$BASE/handleResourceEvent" \
  -H 'Content-Type: application/json' \
  -H 'x-correlation-id: resource-event-001' \
  -d '{...}' | python3 -m json.tool
```

### Finish Project Event
```bash
curl -s -X POST "$BASE/finishProjectEvent" \
  -H 'Content-Type: application/json' \
  -H 'x-correlation-id: finish-event-001' \
  -d '{...}' | python3 -m json.tool
```
