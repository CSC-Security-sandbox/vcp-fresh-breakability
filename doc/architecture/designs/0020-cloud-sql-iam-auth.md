# Cloud SQL IAM Authentication (Kubernetes + Helm) Design Document

## Overview

This document describes the implementation of **Cloud SQL IAM DB authentication** across the VSA Control Plane Helm charts and runtime tooling.

The feature is controlled by a single flag:

- **`global.cloudSqlIamAuthEnabled`** (umbrella + core + worker charts)

When enabled, services connect to PostgreSQL via a **Cloud SQL Proxy v2 sidecar** using **Workload Identity + IAM DB auth** (no DB password). When disabled, services use the existing **password-mode** configuration.

## Goals

- **Enable IAM DB auth** for VCP components that talk to Postgres (core + workers; temporal optionally).
- **Avoid secrets** for DB password when IAM auth is enabled.
- **Make behavior deterministic** for both flag values (true/false), including env var precedence and chart defaults.
- **Keep backward compatibility** where older env keys might exist (e.g., `DB_USERNAME`).

## Non-Goals

- Setting up Workload Identity bindings (KSA↔GSA) and DB-level IAM users/permissions; those are deployment/environment responsibilities.
- Migrating existing environments to IAM auth automatically.
- Changing application code DB env var names (apps already read `DB_USER`, `DB_PASSWORD`, etc.).

## Terminology

- **KSA**: Kubernetes Service Account (e.g., `vcp-worker-ksa`, `vcp-core`).
- **GSA**: Google Service Account mapped via Workload Identity.
- **IAM DB username**: `"<ksa>@<project>.iam"` (this is what apps use as `DB_USER` when IAM auth is enabled).
- **Cloud SQL Proxy v2**: sidecar container (version is centrally configured via `global.cloudSqlProxy.image`).

## Flag and Runtime Contract

### Flag: `global.cloudSqlIamAuthEnabled`

- **`false` (default)**:
  - DB connectivity is **password-mode**
  - **No Cloud SQL Proxy sidecar** is injected
  - DB host remains the shared proxy service host (e.g. `cloud-sql-proxy.sde.svc.cluster.local` for workers) or whatever `database.host/hostIP` is configured for core/telemetry
  - **Secrets are required** (`DB_PASSWORD`, `METRICS_DB_PASSWORD`) and `DB_USER` is the regular DB username (e.g. `postgres`, `metrics`, `vcp-db-user`)

- **`true`**:
  - DB connectivity is **IAM-mode**
  - Cloud SQL Proxy sidecar is injected with:
    - `--auto-iam-authn`
    - `--private-ip`
    - `--port=5432`
  - Apps connect to:
    - `DB_HOST=127.0.0.1`
    - `DB_USER="<ksa>@<project>.iam"`
  - **DB password env vars are omitted** where applicable (Kubernetes and Cloud Run deployment tooling).

### Runtime env var: `CLOUD_SQL_IAM_AUTH_ENABLED`

Charts always emit:

- `CLOUD_SQL_IAM_AUTH_ENABLED: "true"` or `"false"`

This env var is used by:

- `tools/telemetry-deployer` to decide whether to:
  - add `--auto-iam-authn` to Cloud SQL Proxy args
  - skip fetching DB password secrets from Secret Manager
- `tools/migrate` to decide whether to shut down the Cloud SQL Proxy sidecar in Job contexts.

## Implementation by Component

## Worker Chart (`kubernetes/vcp-worker-chart`)

### Sidecar injection

- `templates/_helpers.tpl` introduces `worker.databaseProxyContainer` which renders a `cloud-sql-proxy` sidecar **only when `global.cloudSqlIamAuthEnabled=true`**.
- The instance connection name is resolved in this order:
  - `global.coreConfig.gcp.instanceConnectionName` (if provided)
  - else derived from `workerConfig.smcProjectId`, `workerConfig.localRegion`, and `"<project>-db-postgres"` instance name.

- `templates/deployment.yaml`:
  - injects `iam.gke.io/gke-metadata-server-enabled: "true"` nodeSelector when IAM auth is enabled
  - includes the sidecar via `{{- include "worker.databaseProxyContainer" ... }}`.

### DB env var rendering (ConfigMap)

- `templates/configMap.yaml`:
  - In password-mode (`cloudSqlIamAuthEnabled=false`):
    - sets `DB_HOST`, `DB_USER`, `METRICS_DB_HOST`, `METRICS_DB_USER` from values
  - In IAM-mode (`cloudSqlIamAuthEnabled=true`):
    - sets:
      - `DB_HOST="127.0.0.1"`
      - `DB_USER="<serviceAccount.name>@<project>.iam"`
      - metrics equivalents
    - sets `CLOUD_SQL_IAM_AUTH_ENABLED`.

### Secret key alignment (critical fix)

The app reads **`DB_USER`** and **`METRICS_DB_USER`** (not `DB_USERNAME`). To guarantee correct runtime behavior:

- `templates/secret.yaml` now emits `DB_USER` and `METRICS_DB_USER` **in password-mode** so that `envFrom` ordering can override ConfigMap values reliably.
- For backwards compatibility, `DB_USERNAME` / `METRICS_DB_USERNAME` are still written (password-mode).
- In IAM-mode, the Secret skips DB password/user fields (workers rely on IAM principal and proxy).

### Env precedence

`templates/deployment.yaml` uses:

- `envFrom: [configMapRef, secretRef]`

So in password-mode, any overlapping keys in the Secret win (important for `DB_USER`, `METRICS_DB_USER`).

## Core Chart (`kubernetes/vsa-control-plane/charts/core`)

### Sidecar injection

`templates/_helpers.tpl` adds:

- `core.databaseProxyContainer` for long-running deployments
- `core.databaseProxyContainerForJob` for Jobs (adds `--quitquitquit` and `--admin-port=9091`)

These are injected in:

- `templates/deployment.yaml` (deployment container list)
- `templates/dbmigrate-job.yaml` (job container list)

Node selector behavior mirrors workers:

- if IAM enabled → merge global nodeSelector + `iam.gke.io/gke-metadata-server-enabled: "true"`
- else apply global nodeSelector as-is.

### DB env var override strategy

Core uses a generated config helper (`core.generateConfigMapData`) that maps values into env vars. When IAM is enabled:

- `templates/_helpers.tpl` skips `dbHost/dbUser/metricsHost/metricsDbUser` keys from automatic generation
- `templates/configMap.yaml` appends explicit overrides (must come after generation):
  - `DB_HOST="127.0.0.1"`
  - `DB_USER="<serviceAccountName>@<project>.iam"`
  - metrics equivalents
  - `CLOUD_SQL_IAM_AUTH_ENABLED`.

### Secret behavior

- `templates/secret.yaml`:
  - in IAM-mode: writes `DB_USER` (and `METRICS_DB_USER` when enabled) as IAM principal
  - in password-mode: writes `DB_USER`, `DB_PASSWORD`, and metrics equivalents.

- When `secretManagerEnabled=true`, IAM-mode uses a Secret template with `mergePolicy: Merge` to overlay the IAM username keys while still allowing other secret-managed keys to exist.

### DB migrate job behavior

`templates/dbmigrate-job.yaml`:

- when IAM enabled:
  - sets `DB_HOST`/`METRICS_DB_HOST` to `127.0.0.1`
  - **omits** `DB_PASSWORD` / `METRICS_DB_PASSWORD` env vars
  - injects `CLOUD_SQL_IAM_AUTH_ENABLED` so `tools/migrate` can shutdown the proxy sidecar
  - includes `core.databaseProxyContainerForJob` sidecar.

## Telemetry Deployer (Kubernetes Job + Cloud Run)

### Kubernetes Job (`telemetry-deployer-job.yaml`)

`templates/telemetry-deployer-job.yaml` passes `CLOUD_SQL_IAM_AUTH_ENABLED` to the deployer tool and adjusts DB env vars:

- IAM-mode:
  - `DB_HOST=127.0.0.1` and `METRICS_DB_HOST=127.0.0.1`
  - `DB_USER` and `METRICS_DB_USER` are derived from the telemetry service account email:
    - `svc-sde-metrics-producer@<project>.iam.gserviceaccount.com` → `svc-sde-metrics-producer@<project>.iam`
  - DB password env vars are omitted

- password-mode:
  - uses Secret-based DB_USER / METRICS_DB_USER
  - includes DB password values.

### Cloud Run deployer tool (`tools/telemetry-deployer/main.go`)

The deployer:

- ensures `CLOUD_SQL_IAM_AUTH_ENABLED` is present in env var map (defaults merged in)
- if IAM enabled:
  - skips configuring DB password secrets from Secret Manager
  - adds `--auto-iam-authn` to Cloud SQL Proxy args
- always runs the Cloud SQL Proxy container in the Cloud Run revision alongside telemetry containers.

## Database Migration Tool (`tools/migrate/main.go`)

When IAM auth is enabled, the DB migrate Job runs with a Cloud SQL Proxy sidecar configured with:

- `--quitquitquit`
- `--admin-port=9091`

`tools/migrate` detects `CLOUD_SQL_IAM_AUTH_ENABLED=true` and sends a raw HTTP POST to:

- `127.0.0.1:9091/quitquitquit`

This allows the Job to terminate cleanly after migrations complete by shutting down the sidecar.

## Temporal Chart (`kubernetes/temporal`) (Optional IAM support)

Temporal chart includes its own IAM switch:

- `.Values.cloudSqlIamAuthEnabled` (and it also checks `.Values.global.cloudSqlIamAuthEnabled` for umbrella usage)

When enabled:

- sets SQL host to `127.0.0.1`
- sets SQL user to `temporal-ksa@<project>.iam` (project ID required)
- omits SQL password env vars
- injects Cloud SQL Proxy sidecar (with `--quitquitquit` / `--admin-port=9091`)
- merges nodeSelector with `iam.gke.io/gke-metadata-server-enabled: "true"`
- annotates the Temporal service account with:
  - `iam.gke.io/gcp-service-account: temporal-ksa@<project>.iam.gserviceaccount.com`

## End-to-End Flow

### Password-mode (`global.cloudSqlIamAuthEnabled=false`)

1. Helm renders DB host/user in ConfigMap from values.
2. Helm renders DB password (and DB_USER) in Secret.
3. Pods load env via `envFrom` (ConfigMap then Secret).
4. App connects to DB using:
   - `DB_HOST=<shared proxy host>`
   - `DB_USER=<password-mode username>`
   - `DB_PASSWORD=<secret>`

### IAM-mode (`global.cloudSqlIamAuthEnabled=true`)

1. Helm injects Cloud SQL Proxy v2 sidecar with `--auto-iam-authn`.
2. Helm sets:
   - `DB_HOST=127.0.0.1`
   - `DB_USER=<ksa>@<project>.iam`
   - `CLOUD_SQL_IAM_AUTH_ENABLED=true`
3. App connects without DB password; Cloud SQL Proxy uses Workload Identity to authenticate.

## Operational Notes / Troubleshooting

- **IAM enabled but DB auth fails**:
  - confirm Cloud SQL Proxy args include `--auto-iam-authn`
  - confirm `DB_USER` is `"<ksa>@<project>.iam"`
  - confirm KSA is bound to the correct GSA and that IAM DB user is authorized in Postgres.

- **Flag set to false but logs show IAM-style user**:
  - check that the deployed Secret contains `DB_USER` / `METRICS_DB_USER`
  - confirm `envFrom` order is ConfigMap first, Secret second.

- **Jobs hang when IAM enabled**:
  - ensure the Cloud SQL Proxy sidecar for Jobs has `--quitquitquit --admin-port=9091`
  - ensure the main container sends shutdown (e.g., `tools/migrate` does this automatically).

## Related Files (Implementation Map)

- **Worker chart**
  - `kubernetes/vcp-worker-chart/templates/_helpers.tpl`
  - `kubernetes/vcp-worker-chart/templates/deployment.yaml`
  - `kubernetes/vcp-worker-chart/templates/configMap.yaml`
  - `kubernetes/vcp-worker-chart/templates/secret.yaml`
  - `kubernetes/vcp-worker-chart/values.yaml`

- **Core chart**
  - `kubernetes/vsa-control-plane/charts/core/templates/_helpers.tpl`
  - `kubernetes/vsa-control-plane/charts/core/templates/configMap.yaml`
  - `kubernetes/vsa-control-plane/charts/core/templates/deployment.yaml`
  - `kubernetes/vsa-control-plane/charts/core/templates/secret.yaml`
  - `kubernetes/vsa-control-plane/charts/core/templates/dbmigrate-job.yaml`
  - `kubernetes/vsa-control-plane/charts/core/templates/telemetry-deployer-job.yaml`
  - `kubernetes/vsa-control-plane/charts/core/values.yaml`
  - `kubernetes/vsa-control-plane/values.yaml`

- **Tools**
  - `tools/telemetry-deployer/main.go`
  - `tools/migrate/main.go`

- **Temporal chart**
  - `kubernetes/temporal/templates/*`
  - `kubernetes/temporal/values.yaml`


