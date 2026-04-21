# VSA Control Plane – Architecture Cheat Sheet (Untracked)

> Personal notes to reuse across sessions. Do not commit to git.

## High-Level Components
- `google-proxy`: Public GCNV-facing REST ingress; validates/normalizes, returns LROs; wires orchestrator + Temporal client + Postgres.
- `core`: Internal API + schedulers; same orchestrator + Temporal wiring.
- `worker`: Temporal worker registering workflows/activities; serves Prometheus `/metrics`.
- `ontap-proxy`: Auth/rule-enforced passthrough to ONTAP REST; used by activities.
- `telemetry`: Ingests VCP + telemetry DBs, processes metrics/billing, exposes API + `/metrics`.
- Shared libs: `common` (config), `utils` (auth/middleware/env/errors), `clients` (ONTAP REST, CVP, GCP), `database` (Postgres + migrations), `workflow_engine/temporal` (Temporal client/config), `hyperscaler` (GCP provider), `core/models` + `core/datamodel`.

## Request → Workflow Flow (Create Volume)
1) `POST /v1beta/.../volumes` → `google-proxy/api/endpoints/volume_endpoint.go::V1betaCreateVolume`  
   - Validates feature flags, parses region/zone, optional pool fetch for SMB/Kerberos.  
   - Calls `Orchestrator.CreateVolume`, returns LRO operation id.
2) Orchestrator (`core/orchestrator/volume.go::CreateVolume`)  
   - Validates pool/zone/protocol/snapshot; persists volume/job in Postgres.  
   - Starts Temporal workflow via `workflows.NewWorkflowExecutor`.
3) Temporal workflow (`core/orchestrator/workflows/volume_create_workflow.go`)  
   - Selects block vs file child workflows (pre/post).  
   - Activities: NAS/LUN setup, firewall/NAS LIF, CIFS/AD/Kerberos, KMS, backup/replication, etc.; retries/backoff set.
4) Worker executes workflows/activities from task queues; updates DB state; hits ONTAP via `ontap-proxy`.
5) Client polls LRO; operation response holds final volume or error.

## Runtime/Deployment Notes
- State: Postgres (`database/vcp`); telemetry DB for metrics/billing.
- Temporal: Config in `workflow_engine/temporal`; task queues `CustomerTaskQueue`, `BackgroundTaskQueue`.
- Config/flags: `config/*.yaml` + env (e.g., `ENABLE_SMB`, `THIN_CLONE_GA_SUPPORT`, `AUTO_TIERING_ENABLED`).
- Deploy: Helm charts under `kubernetes/*`; Skaffold + `skaffold.env`; Dockerfiles per service; `make build-all-binaries-dev`.
- Observability: OpenTelemetry init in each service; Prometheus `/metrics` (worker, telemetry, proxy); Temporal UI for workflows; structured logging with correlation IDs.

## Quick Debug Pointers
- API issues: check `google-proxy` handlers and orchestrator calls; validate feature flags/env.
- Workflow/activity issues: inspect Temporal UI; see `core/orchestrator/workflows` + `activities`; worker logs.
- ONTAP calls: `ontap-proxy` middleware/rules or `clients/ontap-rest`.
- State/jobs: Postgres tables via `database/vcp`; job UUIDs returned in LRO `name`.

# VSA Control Plane - Cursor Rules

## Database Operations
- Use the MCP server (`user-postgres-local-query`) for all DB read queries.
- Use psql for DB writes/updates:
  ```
  /opt/homebrew/opt/libpq/bin/psql "postgres://postgres:postgres@localhost:5432/vcp?sslmode=disable" -c "<update-query>"
  ```

## Temporal / Worker Commands
- Start Temporal dev server:
  ```
  temporal server start-dev -p 7233 --ui-port 8234 --db-filename cluster4.db
  ```
- Run VLM worker (detached):
  ```
  docker run -d --rm --name vlm-worker \
    -e PROVIDER="gcp" \
    -e SERVER_IP="host.docker.internal" \
    -e SERVER_PORT="7233" \
    -e TASK_QUEUE="vsa-lifecycle-manager-9.18.1" \
    -e DEBUG="false" \
    -e MAX_CONCURRENT_ACTIVITIES="1000" \
    -e MAX_CONCURRENT_WORKFLOWS="1000" \
    -e MAX_CONCURRENT_WORKFLOW_TASK_POLLERS="2" \
    -e GCE_METADATA_HOST="34.116.66.254:9090" \
    -e ONTAP_CREDENTIAL_ENCRYPT_KEY="qL9rC9L0h+n8MQ7B5M0Ob+BtGP+WrKf+fy1z+q28lxI=" \
    ghcr.io/vcp-vsa-control-plane/temporal-vlm:R9.18.1x_7887576
  ```
- Tail VLM worker logs: `docker logs -f --tail 50 vlm-worker`
- Stop VLM worker: `docker stop vlm-worker`

## Architecture Reference
- See `doc/architecture/overview-notes.md` for project architecture summary including:
  - Component overview (google-proxy, core, worker, ontap-proxy, telemetry)
  - Create volume flow trace
  - Runtime/storage/config layers
  - Key code paths for extending/debugging

## Key Services
| Service | Purpose | Entry Point |
|---------|---------|-------------|
| google-proxy | Public GCNV REST API | `google-proxy/app.go` |
| core | Internal API + orchestrator | `core/app.go` |
| worker | Temporal workflows/activities | `worker/main.go` |
| ontap-proxy | ONTAP REST passthrough | `ontap-proxy/main.go` |
| telemetry | Metrics/billing | `telemetry/main.go` |

## Workflow Debugging
- Temporal UI: http://localhost:8234/namespaces/default/workflows
- Worker logs are in terminal files under `.cursor/projects/.../terminals/`
- Use `grep` on worker log files to filter by RunID or WorkflowID
