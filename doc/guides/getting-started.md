# Getting Started with the VSA Control Plane

This guide helps a new developer bootstrap a local development environment and walk through creating the first Pool, HostGroup, Volume, and mounting the Volume.

---
## 1. Prerequisites
Install locally:
- Go (>= 1.21)
- Docker + Buildx
- Make
- Skaffold (if you want live dev loop)
- (Optional) PlantUML for diagrams

Have access to:
- A PostgreSQL instance (local container or cloud) – credentials you can place in `dev.env`
- GitHub Personal Access Token (PAT) with read access (export as `GHVSA_PAT`)

---
## 2. Clone the Repository
```bash
git clone <repo-url> vsa-control-plane
cd vsa-control-plane
```

---
## 3. Environment Setup
Copy / adjust environment example files if provided (you have `dev.env` / `skaffold.env`).

Export minimal required variables (example):
```bash
export GHVSA_PAT=$(gh auth token)
export DB_PASSWORD=postgres
export DB_USER=postgres
export DB_HOST=127.0.0.1
export DB_PORT=5432
export DB_NAME=vcp
```
(If the code expects a single DATABASE_URL style variable in config, adapt accordingly.)

Start a local Postgres (example via docker):
```bash
docker run -d --name vcp-pg -e POSTGRES_PASSWORD=$DB_PASSWORD -e POSTGRES_DB=$DB_NAME -p 5432:5432 postgres:15
```

---
## 4. Build Binaries (Dev Mode)
```bash
make build-all-binaries-dev
```
Produces binaries in `app/`:
- `google-proxy`
- `core`
- `vcp-worker`
- `telemetry`
- `ontap-proxy`

---
## 5. (Optional) Skaffold Dev Loop
If you have a local Kubernetes/dev environment:
```bash
make skaffold-dev
```
This recompiles and redeploys on changes.

---
## 6. Run Services Manually (Simple Local Flow)
6.1 Start temporal server
```bash
temporal server start-dev -p 7234 --ui-port 8071 --db-filename cluster_7234_2.db
```
6.2 Initialize VLM worker
```bash
docker run --rm -it \                                                           
--name vlm-worker-1 \
-e PROVIDER="gcp" \
-e SERVER_IP="host.docker.internal" \
-e SERVER_PORT="7234" \
-e TASK_QUEUE="vsa-lifecycle-manager-9.17.1" \
-e DEBUG="false" \
-e MAX_CONCURRENT_ACTIVITIES="1000" \
-e MAX_CONCURRENT_WORKFLOWS="1000" \
-e MAX_CONCURRENT_WORKFLOW_TASK_POLLERS="2" \
-e GCE_METADATA_HOST="34.116.66.254:9090" \
ghcr.io/vcp-vsa-control-plane/temporal-vlm:r9.17.1xn_7759115 // replace with latest tag
```
6.3 Run services In separate terminals (or use a supervisor)

```bash
./app/core
./app/ontap-proxy
./app/vcp-worker
./app/google-proxy
```
(Each service will read config from `config/development.yaml` plus env vars.)

---
## 7. Generate Code (When Specs Change)
```bash
make generate-google-proxy      # OpenAPI -> server stubs for google proxy
make generate-core-api          # Core API server generation
make generate-mocks             # Mock interfaces for tests
```

---
## 8. Creating Your First Resources via API
All public APIs are exposed by `google-proxy` service (OpenAPI: `google-proxy/api/gcp-api.yaml`). Replace placeholders:
- `{PROJECT}` = GCP project number (fake locally, e.g. 123456789)
- `{REGION}` = region id (e.g. us-east1)
- Set a correlation header: `-H "X-Correlation-Id: demo-1"`

Assume base URL: `http://localhost:8080` (adjust to actual bind address). Some builds might bind to `:8080` or `:8000` — check logs.

### 8.1 Create a Pool
```bash
curl -X POST \
  http://localhost:8080/v1beta/projects/{PROJECT}/locations/{REGION}/pools \
  -H 'Content-Type: application/json' \
  -H 'X-Correlation-Id: demo-1' \
  -d '{
        "resourceId": "demo-pool",
        "serviceLevel": "PREMIUM",
        "sizeInBytes": 2199023255552,
        "network": "projects/{PROJECT}/global/networks/vcp-demo",
        "allowAutoTiering": true,
        "description": "First demo pool"
      }'
```
Response: 202 Operation. Capture `operationId` from `name`.

Poll the operation:
```bash
curl http://localhost:8080/v1beta/projects/{PROJECT}/locations/{REGION}/operations/{operationId}
```
Wait until `done: true` and pool state becomes READY.

### 8.2 Create a HostGroup (for iSCSI)
```bash
curl -X POST \
  http://localhost:8080/v1beta/projects/{PROJECT}/locations/{REGION}/hostGroups \
  -H 'Content-Type: application/json' \
  -H 'X-Correlation-Id: demo-2' \
  -d '{
        "resourceId": "demo-hg",
        "type": "ISCSI_INITIATOR",
        "hosts": ["iqn.1998-01.com.vmware:example1"],
        "osType": "LINUX",
        "description": "Demo host group"
      }'
```
Poll the returned operation (if 202) until READY.

### 8.3 Create a Volume
For NFS example:
```bash
curl -X POST \
  http://localhost:8080/v1beta/projects/{PROJECT}/locations/{REGION}/volumes \
  -H 'Content-Type: application/json' \
  -H 'X-Correlation-Id: demo-3' \
  -d '{
        "resourceId": "demo-vol",
        "poolId": "<POOL_UUID_FROM_DESCRIBE>",
        "protocols": ["NFSV3"],
        "quotaInBytes": 107374182400,
        "snapReserve": 5,
        "unixPermissions": "0770",
        "description": "Demo volume"
      }'
```
(You can get the `poolId` by describing the pool: `GET /pools/{poolId}` — note: poolId is UUID, distinct from resourceId.)

Poll the operation until volume `volumeState` = READY. Capture `mountPoints` once ready (for NFS it shows export instructions).

For iSCSI volume (block) including HostGroup binding:
```bash
... same POST but with "protocols": ["ISCSI"],
"blockProperties": { "hostGroupIds": ["<HOSTGROUP_UUID>"] }
```

### 8.4 Mount the Volume
NFS Example (from operation example):
```bash
sudo mkdir -p /mnt/demo-vol
sudo mount -t nfs -o rw,hard,rsize=65536,wsize=65536,vers=3,tcp <export_ip>:/demo-vol /mnt/demo-vol
```
Adjust mount options per workload; verify with `df -h | grep demo-vol`.

iSCSI Example (high level):
1. Discover portal: `iscsiadm -m discovery -t sendtargets -p <target_ip>`
2. Login: `iscsiadm -m node --login`
3. New device appears (e.g., /dev/sdb) – create filesystem: `sudo mkfs.ext4 /dev/sdb`
4. Mount: `sudo mount /dev/sdb /mnt/block-vol`

---
## 9. List & Describe Resources
```bash
curl http://localhost:8080/v1beta/projects/{PROJECT}/locations/{REGION}/pools
curl http://localhost:8080/v1beta/projects/{PROJECT}/locations/{REGION}/volumes
curl http://localhost:8080/v1beta/projects/{PROJECT}/locations/{REGION}/hostGroups
```

---
## 10. Cleanup
```bash
# Delete volume
curl -X DELETE http://localhost:8080/v1beta/projects/{PROJECT}/locations/{REGION}/volumes/{volumeId}
# Delete host group
curl -X DELETE http://localhost:8080/v1beta/projects/{PROJECT}/locations/{REGION}/hostGroups/{hostGroupId}
# Delete pool
curl -X DELETE http://localhost:8080/v1beta/projects/{PROJECT}/locations/{REGION}/pools/{poolId}
```
Poll operations until done.

---
## 11. Running Tests
```bash
make test
```
For a single test:
```bash
make run-single-test TEST_NAME=TestBackupCreateWorkflow
```

### CI / vsacictl (reproduce GitHub Actions locally)

The repository's GitHub Actions use `vsacictl` (in the `cicd/` directory) to run unit tests and linting with CI flags. To reproduce CI behavior locally:

1) Build vsacictl and add it to your PATH:
```bash
cd cicd
go build -o ~/go/bin/vsacictl .
export PATH="${HOME}/go/bin:$PATH"
vsacictl --help
```

2) Run the CI-equivalent test command:
```bash
vsacictl test
```

Notes:
- Ensure required environment variables (DB credentials and any service-specific vars) are exported as described in section 3 before running tests. For example:
  export DB_PASSWORD=postgres
  export DB_USER=postgres
  export DB_HOST=127.0.0.1
  export DB_PORT=5432
  export DB_NAME=vcp
- If `vsacictl` is not required or unavailable, `make test` runs the unit tests using the standard Go tooling. `vsacictl` simply standardizes flags and environment used by CI.

---
## 12. Troubleshooting
| Issue | Tip |
|-------|-----|
| 500 errors | Check core / worker logs for Temporal or DB errors |
| LRO stuck | Describe operation; inspect `error` or workflow logs |
| Mount fails (NFS) | Ensure export IP reachable, firewall rules open |
| iSCSI login fails | Confirm HostGroup IQN matches initiator IQN |

---
You have now provisioned and mounted your first VSA-backed volume. Continue with `advanced-usage.md` for backups, replication and cloning.
