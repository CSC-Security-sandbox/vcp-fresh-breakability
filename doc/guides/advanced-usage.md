# Advanced Usage Guide (Backups, CRR, Clones)

This guide builds on getting-started.md and documents advanced features: Snapshots, Backups & Backup Vaults, Backup Policies, Cross‑Region / Cross‑Zone Replication (CRR), and Cloning (from snapshot or backup). It references existing design docs in `doc/architecture/designs/` and implementation hints in `google-proxy/api/endpoints/`.

Assumptions
- Base resources (Pool, Volume) already exist and are READY
- Replace placeholders: {PROJECT}, {REGION}, {POOL_ID}, {VOLUME_ID}, {VAULT_ID}, {BACKUP_ID}, {SNAPSHOT_ID}
- Base URL: http://localhost:8080 (adjust for your environment)
- Use `-H "X-Correlation-Id: adv-<n>"` on requests for traceability

---
# Local CRR development setup (two-region)

Cross-Region Replication (CRR) testing requires running a pair of VCP stacks and Temporal/VLM instances locally to simulate source and destination regions. This section documents the minimal local infra and example run commands.

DB Setup
You will need two DB instances running locally. One can be your existing local VCP DB; create a second DB for the paired region.

Option 1: Run a second Postgres container
```bash
docker run --name some-postgres-2 -e POSTGRES_PASSWORD=postgres -p 5433:5432 -d postgres

# Exec into the pod and create the DB
docker exec -it some-postgres-2 psql -U postgres -c "CREATE DATABASE vcp2 WITH OWNER = postgres CONNECTION LIMIT = -1;"
docker exec -it some-postgres-2 psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE vcp2 TO postgres;"
docker exec -it some-postgres-2 psql -U postgres -d vcp2 -c "GRANT ALL PRIVILEGES ON SCHEMA public TO postgres;"
```

Option 2: Create a new database/schema in the same Postgres instance
```sql
CREATE DATABASE vcp2 WITH OWNER = postgres CONNECTION LIMIT = -1;
GRANT ALL PRIVILEGES ON DATABASE vcp2 TO postgres;
GRANT ALL PRIVILEGES ON SCHEMA public TO postgres;
```

Note: update ports and names if they collide with existing local setups.

Temporal and VLM Worker (two instances)
Run two Temporal dev servers and two VLM worker containers (one per region). Example commands:

Region 1 (local source)
```bash
temporal server start-dev --ui-port 8070 --db-filename cluster_7233_1.db

docker run --rm -it \
  --name vlm-worker-region1 \
  -e PROVIDER="gcp" \
  -e SERVER_IP="host.docker.internal" \
  -e SERVER_PORT="7233" \
  -e TASK_QUEUE="vsa-lifecycle-manager-9.17.1" \
  -e DEBUG="false" \
  -e MAX_CONCURRENT_ACTIVITIES="1000" \
  -e MAX_CONCURRENT_WORKFLOWS="1000" \
  -e MAX_CONCURRENT_WORKFLOW_TASK_POLLERS="2" \
  -e GCE_METADATA_HOST="34.116.66.254:9090" \
  ghcr.io/vcp-vsa-control-plane/temporal-vlm:latest
```

Region 2 (local destination)
```bash
temporal server start-dev -p 7234 --ui-port 8071 --db-filename cluster_7234_2.db

docker run --rm -it \
  --name vlm-worker-region2 \
  -e PROVIDER="gcp" \
  -e SERVER_IP="host.docker.internal" \
  -e SERVER_PORT="7234" \
  -e TASK_QUEUE="vsa-lifecycle-manager-9.17.1" \
  -e DEBUG="false" \
  -e MAX_CONCURRENT_ACTIVITIES="1000" \
  -e MAX_CONCURRENT_WORKFLOWS="1000" \
  -e MAX_CONCURRENT_WORKFLOW_TASK_POLLERS="2" \
  -e GCE_METADATA_HOST="34.116.66.254:9090" \
  ghcr.io/vcp-vsa-control-plane/temporal-vlm:latest
```

Running Local & Remote Instances (IDE launch config)
If you use VS Code or GoLand, add launch configurations that point each service to its respective DB and Temporal address. Example (snippet, adapt env values):

```json
{
  "name": "Launch Google Proxy (Local)",
  "type": "go",
  "request": "launch",
  "mode": "auto",
  "program": "${workspaceFolder}/google-proxy/app.go",
  "env": {
    "ENV": "local",
    "DB_HOST": "localhost",
    "DB_PORT": "5432",
    "DB_NAME": "vcp1",
    "TEMPORAL_ADDRESS": "localhost:7233",
    "LOCAL_REGION": "us-east4",
    "CRR_ENABLED": "true",
    "VCP_PAIRED_REGIONS": "{\"us-east4\": \"localhost:8090\", \"us-central1\": \"localhost:8091\"}"
  }
}
```

Repeat the same for the second region with DB_NAME `vcp2`, TEMPORAL_ADDRESS `localhost:7234`, and LOCAL_REGION `us-central1`.

Reference
- Detailed internal notes: https://confluence.ngage.netapp.com/spaces/~gauravo/pages/1287829923/Local+Setup+for+CRR+with+VCP

---
## 1. Snapshots (Local Point‑in‑Time Copies)

Create a manual snapshot:

curl -X POST \
  http://localhost:8080/v1beta/projects/{PROJECT}/locations/{REGION}/volumes/{VOLUME_ID}/snapshots \
  -H 'Content-Type: application/json' \
  -H 'X-Correlation-Id: adv-snap-1' \
  -d '{"resourceId":"snap-manual-1","description":"manual snapshot"}'

- The API returns a 202 Operation. Poll the operation until `snapshotState = READY`.
- List snapshots:

curl http://localhost:8080/v1beta/projects/{PROJECT}/locations/{REGION}/volumes/{VOLUME_ID}/snapshots

- Update snapshot metadata (description):

curl -X PUT http://localhost:8080/.../snapshots/{SNAPSHOT_ID} -H 'Content-Type: application/json' -d '{"description":"updated"}'

- Delete a snapshot:

curl -X DELETE http://localhost:8080/.../snapshots/{SNAPSHOT_ID}


---
## 2. Backups & Backup Vaults

Create a Backup Vault (if not already present):

curl -X POST \
  http://localhost:8080/v1beta/projects/{PROJECT}/locations/{REGION}/backupVaults \
  -H 'Content-Type: application/json' \
  -H 'X-Correlation-Id: adv-vault-1' \
  -d '{"resourceId":"vault-demo","description":"demo vault"}'

List vaults:

curl http://localhost:8080/v1beta/projects/{PROJECT}/locations/{REGION}/backupVaults

Create an ad‑hoc backup for a volume into a vault:

curl -X POST \
  http://localhost:8080/v1beta/projects/{PROJECT}/locations/{REGION}/backupVaults/{VAULT_ID}/backups \
  -H 'Content-Type: application/json' \
  -H 'X-Correlation-Id: adv-backup-1' \
  -d '{"name":"backup-manual-1","volumeId":"{VOLUME_ID}","description":"manual backup"}'

List backups for a volume:

curl 'http://localhost:8080/v1beta/projects/{PROJECT}/locations/{REGION}/backupVaults/{VAULT_ID}/backups?volumeId={VOLUME_ID}'

Delete a backup:

curl -X DELETE http://localhost:8080/.../backupVaults/{VAULT_ID}/backups/{BACKUP_ID}

---
## 3. Backup Policies (Scheduled Retention)

Create a backup policy (example 7d/4w/3m retention):

curl -X POST \
  http://localhost:8080/v1beta/projects/{PROJECT}/locations/{REGION}/backupPolicies \
  -H 'Content-Type: application/json' \
  -H 'X-Correlation-Id: adv-policy-1' \
  -d '{
        "resourceId":"daily-basic",
        "dailyBackupLimit":7,
        "weeklyBackupLimit":4,
        "monthlyBackupLimit":3,
        "enabled":true,
        "description":"7d / 4w / 3m retention"
      }'

- List policies:

curl http://localhost:8080/v1beta/projects/{PROJECT}/locations/{REGION}/backupPolicies

- Attach policy to a volume via Volume Update if the API supports `backupConfig.backupPolicyId` (check OpenAPI schema). If not, attach at vault/policy level per future enhancement.

---
## 4. Cross‑Region / Cross‑Zone Replication (CRR)

Purpose: keep a destination volume in another region/zone synchronised with a source volume for DR or migration.

Prerequisites
- Source volume state = READY
- Destination region/zone has capacity; tile/mapping (or allow the control plane to create destination) per request body
- CRR feature flag enabled in the deployment: `CRR_ENABLED=true` (see `kubernetes/temporal` values and `google-proxy` config)

Create a replication (high‑level):

curl -X POST \
  http://localhost:8080/v1beta/projects/{PROJECT}/locations/{REGION}/volumes/{SOURCE_VOLUME_RESOURCE_ID}/replications \
  -H 'Content-Type: application/json' \
  -H 'X-Correlation-Id: adv-crr-1' \
  -d '{
        "resourceId":"rep-demo",
        "description":"demo replication",
        "replicationSchedule":"EVERY_10_MINUTES",
        "destination":{
          "volumeName":"projects/{PROJECT}/locations/{REGION2}/volumes/dest-resource-id"
        }
      }'

- Poll operation until `mirrorState = MIRRORED` and healthy flag true.
- Manual sync:

curl -X POST .../replications/{replicationResourceId}/sync -H 'X-Correlation-Id: adv-crr-sync'

- Stop replication (make destination writable):

curl -X POST .../replications/{replicationResourceId}/stop -H 'Content-Type: application/json' -d '{}' -H 'X-Correlation-Id: adv-crr-stop'

- Reverse/Failback:

curl -X POST .../replications/{replicationResourceId}/reverseAndResumeReplication -H 'X-Correlation-Id: adv-crr-reverse'

- Delete replication:

curl -X DELETE .../replications/{replicationResourceId} -H 'Content-Type: application/json' -d '{}' -H 'X-Correlation-Id: adv-crr-del'

Operational notes
- Transfer stats and health are available in replication `transferStats` fields.
- Auto‑tiering on destination needs consideration (see `0005-vsa-auto-tiering-design.md`).

---
## 5. Cloning a Volume

Two approaches:
1. From Snapshot (fast, local)
2. From Backup (remote/restore scenario)

### 5.1 Clone from Snapshot

If API supports `snapshotId` in volume create body:

curl -X POST \
  http://localhost:8080/v1beta/projects/{PROJECT}/locations/{REGION}/volumes \
  -H 'Content-Type: application/json' \
  -H 'X-Correlation-Id: adv-clone-snap' \
  -d '{
        "resourceId":"clone-from-snap1",
        "poolId":"{POOL_ID}",
        "snapshotId":"{SNAPSHOT_ID}",
        "protocols":["NFSV3"],
        "quotaInBytes":107374182400,
        "description":"clone from snapshot"
      }'

- Poll until READY. Snapshot clone is typically quick if same backend and region.

### 5.2 Restore / Clone from Backup

If API supports `backupId` in create body:

curl -X POST \
  http://localhost:8080/v1beta/projects/{PROJECT}/locations/{REGION}/volumes \
  -H 'Content-Type: application/json' \
  -H 'X-Correlation-Id: adv-clone-backup' \
  -d '{
        "resourceId":"restore-from-backup1",
        "poolId":"{POOL_ID}",
        "backupId":"{BACKUP_ID}",
        "protocols":["NFSV3"],
        "quotaInBytes":107374182400,
        "description":"restored from backup"
      }'

- Restore from backup may involve large data transfer and take longer; monitor operation progress.

---
## 6. Encryption Migration (CMEK) (Optional Advanced)

Create a KMS config:

curl -X POST /v1beta/projects/{PROJECT}/locations/{REGION}/storage/kmsConfig \
  -H 'Content-Type: application/json' \
  -H 'X-Correlation-Id: adv-kms-1' \
  -d '{"resourceId":"kms1","keyFullPath":"projects/p/locations/l/keyRings/r/cryptoKeys/k"}'

Check and migrate volumes to CMEK using provided endpoints; these are LROs — poll until done.

---
## 7. Operational Tips & Troubleshooting

Key poll fields
- Pool / Volume creation: `volumeState = READY`
- Replication: `mirrorState = MIRRORED` + `healthy=true`
- Backup: `lifeCycleState = READY`
- Snapshot: `snapshotState = READY`
- KMS migration: operation.done = true

Troubleshooting matrix (common symptoms):
- Replication stuck in PREPARING: network/peering not ready — verify inter‑region networking and firewall rules
- Backup size zero: volume quiesced or empty — validate volume usage and retry
- Clone slow from backup: large restore over network — prefer local snapshot clone if possible
- Encryption migration slow: many volumes/large data — batch and monitor

Log correlation
- Use `X-Correlation-Id` to link API operation -> Job row -> Temporal workflow -> Worker logs. Check the jobs table for job metadata and workflow ID.

Cleanup order (safe):
1. Stop / Delete replications
2. Delete restored/clone volumes
3. Delete snapshots (if needed)
4. Delete backups (if not retained by policy)
5. Delete backup policies (if unused)
6. Delete volumes
7. Delete host groups
8. Delete backup vaults (after backups purged)
9. Delete pools
10. Delete KMS configs (last)

Automate sequential polling for each LRO to avoid cascading failures.

---
## 8. Examples & Debugging pointers

- Inspect the replication endpoints and CRR flag in code:
  `google-proxy/api/endpoints/replication_endpoints.go` (search for `CRR_ENABLED`).
- Design docs:
  - `doc/architecture/designs/0006-volume-replication-endpoints-and-design.md`
  - `doc/architecture/designs/0005-vsa-auto-tiering-design.md`
- API schemas: `google-proxy/api/gcp-api.yaml` and `doc/api/endpoints.md` for full field names.
