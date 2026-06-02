# Doc Index — Curated for Onboarding

Read these in order for a structured ramp. Full tree: `doc/`.

## Week 1 — Orientation

| Doc | Why |
|-----|-----|
| `doc/guides/onboarding.md` | GCP consumer project, PSA, first pool via CCFE |
| `doc/guides/getting-started.md` | Clone, build, Postgres, Temporal, VLM, smoke test |
| `doc/architecture/decisions/0010-temporal-as-orchestrator-engine.md` | Why workflows exist |
| `doc/architecture/decisions/0011-slog-logging-framework.md` | How we log |
| `doc/architecture/auto-gen-designs-docs/storage-ecosystem-design.md` | Resource relationships |

## Week 2–3 — Technical deep dives & root cause

| Doc | Why |
|-----|-----|
| `.cursor/skills/vcp-onboarding/deep-dive.md` | How failures work, RCA ladder, triagebot → proven fix |
| `.cursor/rules/triagebot.mdc` | Evidence-based triage pipeline |
| `.cursor/triagebot-agents/README.md` | Specialist agent layout |
| `doc/api/error-taxonomy.md` | Map errors to layers and HTTP |
| `doc/guides/temporal-debugging.md` | Workflow failure investigation |

**Exercise:** `/onboard deep-dive` → run `triagebot` on a staging correlation → compare to the merged fix.

## Week 2 — Depth by area

### API

| Doc | Why |
|-----|-----|
| `doc/api/overview.md` | API surface overview |
| `doc/api/error-taxonomy.md` | Error codes — mandatory for contributors |
| `doc/api/resources/pools.md` | Pool LRO and internals |
| `doc/api/resources/volumes.md` | Volume LRO and internals |
| `doc/api/resources/hostgroups.md` | Block access path |
| `doc/swagger.yaml` | OpenAPI source of truth |

### Workflows

| Doc | Why |
|-----|-----|
| `doc/workflows/README.md` | Catalog + patterns |
| `doc/workflows/core/volume-workflows.md` | Primary golden path |
| `doc/workflows/core/pool-workflows.md` | Cluster provisioning |
| `doc/workflows/core/backup-workflows.md` | Backup team entry |
| `doc/workflows/replication/replication-workflows.md` | Replication team entry |

### Architecture & design

| Doc | Why |
|-----|-----|
| `doc/architecture/HOWTO.md` | How to write ADRs/designs |
| `doc/architecture/designs/0018-workflow-cancellation-framework.md` | Cancellation semantics |
| `doc/architecture/designs/0015-workflow-supervisor-task.md` | Supervisor pattern |
| `doc/architecture/designs/0023-leaked-resources-framework.md` | Cleanup background jobs |
| `doc/architecture/vsa-cluster-upgrade-design.md` | Cluster upgrades |

### Operations & debugging

| Doc | Why |
|-----|-----|
| `doc/guides/temporal-debugging.md` | tctl + Temporal UI |
| `doc/guides/advanced-usage.md` | Backups, replication after basics |
| `doc/guides/minikube-deployment.md` | K8s local deploy |
| `doc/guides/ontap-version-consumption-and-testing.md` | ONTAP version matrix |

## Repo root essentials

| File | Why |
|------|-----|
| `CODING_GUIDELINES.md` | Lint rules, boundaries, patterns |
| `core/errors/README.md` | CustomError, Temporal integration |
| `README.md` | Project entry (if present) |

## Cursor tooling (this repo)

| Path | Why |
|------|-----|
| `.cursor/commands/onboard.md` | This onboarding command |
| `.cursor/skills/vcp-onboarding/` | Onboarding skill + references |
| `.cursor/commands/review.md` | PR self-review |
| `.cursor/commands/ontap.md` | ONTAP product expert |
| `.cursor/rules/triagebot.mdc` | Production incident triage |
| `.cursor/skills/local-config/` | Local env generation |
| `.cursor/skills/google-proxy-api/` | Invoke proxy APIs |

## Auto-generated resource designs

Under `doc/architecture/auto-gen-designs-docs/`:

- `pool-design.md`, `volume-design.md`, `hostgroup-design.md`
- `backup-ecosystem-design.md`, `backuppolicies-design.md`, `backupvault-design.md`
- `replication-design.md`, `replicationjobs-design.md`, `clusterpeer-design.md`
- `kmsconfig-design.md`, `activedirectorie-design.md`

Use as orientation; verify against code when details matter.

## External / internal (referenced in guides)

- Jira template NFSAAS-116530 — consumer project
- Internal allowlisting spreadsheet — `doc/guides/onboarding.md` §3
- CCFE autopush endpoint — pool create without local VCP

## What not to read on day one

- Every file in `doc/architecture/designs/` (50+ docs)
- Full `doc/swagger.yaml` in one sitting
- OCI-specific docs unless on OCI team

Use `/onboard` to go deeper on demand.
