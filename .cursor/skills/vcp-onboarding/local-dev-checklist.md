# Local Dev Checklist

Combines GCP onboarding (`doc/guides/onboarding.md`) and local bootstrap (`doc/guides/getting-started.md`). Check items off as you go.

## Phase A — GCP consumer project (before local VCP)

Source: `doc/guides/onboarding.md`

- [ ] Clone Jira template NFSAAS-116530 for personal consumer project
- [ ] Email SRE with project ID request (template in onboarding guide §1.1)
- [ ] Enable Service Networking API on consumer project
- [ ] Reserve VPC peering address range (`/24` minimum)
- [ ] Connect VPC peering to NetApp endpoint (autopush tst/sqa)
- [ ] Enable custom route import/export on `sn-netapp-prod` peering
- [ ] Add project to internal allowlisting sheet (onboarding §3)
- [ ] Create first storage pool via CCFE autopush API (onboarding §4)

**Verify:** Pool reaches READY; `gcloud services vpc-peerings list` shows ACTIVE peering.

## Phase B — Workstation prerequisites

Source: `doc/guides/getting-started.md` §1

- [ ] Go ≥ 1.21
- [ ] Docker + buildx
- [ ] Make
- [ ] Skaffold (optional, for k8s dev loop)
- [ ] `gh` CLI with PAT (`GHVSA_PAT` or `gh auth token`)
- [ ] Temporal CLI (`temporal`) for local server

## Phase C — Clone and build

```bash
git clone <repo-url> vsa-control-plane
cd vsa-control-plane
export GHVSA_PAT=$(gh auth token)   # if needed for private deps
make build-all-binaries-dev
```

**Verify:** Binaries exist under `app/` — `google-proxy`, `core`, `vcp-worker`, `telemetry`, `ontap-proxy`.

## Phase D — Database

```bash
docker run -d --name vcp-pg \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=vcp \
  -p 5432:5432 postgres:15
```

Set env vars per getting-started §3 (`DB_HOST`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`).

**Verify:** `psql` connects; migrations run when core/worker start (or run migrator per project docs).

## Phase E — Temporal + VLM

1. Start Temporal dev server (ports per your `dev.env` — getting-started uses 7234/8071 example):

```bash
temporal server start-dev -p 7234 --ui-port 8071 --db-filename cluster.db
```

2. Start VLM worker container (ONTAP version / task queue must match your target cluster — see getting-started §6.2).

**Verify:**
- Temporal UI loads (e.g. http://localhost:8071)
- VLM worker container running: `docker ps | grep vlm`

## Phase F — Run VCP services

Manual start order (simplified — see getting-started for full flags):

1. `ontap-proxy`
2. `core`
3. `vcp-worker`
4. `google-proxy`
5. `telemetry` (if needed)

Or: `make skaffold-dev` with local Kubernetes (`doc/guides/minikube-deployment.md`).

**Verify:** Health endpoints respond; worker logs show workflow registration; no panic on startup.

## Phase G — Local config helper

For `env.development.local`, use the **local-config** skill:

```
Ask: "generate local env file" or use `.cursor/skills/local-config/SKILL.md`
```

## Phase H — First API smoke test

Use **google-proxy-api** skill or getting-started curl examples:

1. List pools or create test pool
2. Create host group
3. Create volume
4. Poll LRO until `done`

**Verify:** Volume reaches desired state in DB and (if wired) ONTAP.

## Common blockers

| Symptom | Check |
|---------|-------|
| 403 from CCFE | Allowlisting sheet, project IAM |
| Peering constraint error | Org policy exception (onboarding §2.7) |
| Worker not picking up workflows | Task queue name matches worker registration |
| DB connection refused | Postgres container up, env vars correct |
| VLM errors | ONTAP creds, metadata host, task queue version |

## When stuck

- Setup issues → re-read `doc/guides/onboarding.md` + `getting-started.md`
- Workflow failures locally → `/onboard debug`
- Production-like failures with correlation ID → `triagebot` (not for day-one setup)
