# Temporal Helm Chart Customizations (Status)

This document records what the vsa-control-plane project customizes in `kubernetes/temporal/` on top of the [public Temporal Helm chart](https://github.com/temporalio/helm-charts). **Current chart version:** 0.73.1 (manually customized; images aligned with public temporal/temporal 0.73.1). Use this doc when upgrading the chart or comparing against upstream.

**Chart location:** `kubernetes/temporal/`  


---

## Customization summary

| Area | What we add or change over public chart 0.73.1 |
|------|-------------------------------------------------|
| **Cloud SQL IAM (GCP)** | When `cloudSqlIamAuthEnabled: true`, we use GCP Workload Identity and a Cloud SQL Proxy sidecar instead of username/password. Includes: helpers in `_helpers.tpl` (`temporal.cloudSqlIamAuthEnabled`, `temporal.databaseProxyContainer`), SQL host → `127.0.0.1`, SQL user → `temporal-ksa@<project>.iam`, no password in config/env when IAM is enabled, `nodeSelector` for GKE metadata server (`iam.gke.io/gke-metadata-server-enabled: "true"`), and ServiceAccount annotation `iam.gke.io/gcp-service-account`. Values: `cloudSqlIamAuthEnabled`, `gcpProjectId`, `cloudSqlInstanceConnectionName`, `cloudSqlProxy.image`. |
| **External Secrets** | Optional use of External Secrets Operator instead of in-cluster Secrets. Template `temporal-externalsecrets.yaml` creates ExternalSecrets for default and visibility DB passwords; uses `ClusterSecretStore` name `main-cluster-secret-store`. Values: `secretManagerEnabled`, `externalSecrets.defaultStore.temporalDBPassword`, `externalSecrets.visibilityStore.temporalDBPassword`. When `secretManagerEnabled: true`, `server-secret.yaml` is not rendered (in-cluster Secret is skipped). |
| **Istio** | Extra template `istioPolicies.yaml` rendered only when `schema.istio.enabled: true`. Defines: PeerAuthentication (mTLS STRICT), AuthorizationPolicy (DENY all except from namespaces `release namespace`, `vcp-open-telemetry`, `vcp`, `vlm-ontap-*`), and DestinationRules for mTLS for frontend, admintools, web, and headless services. Set `schema.istio.enabled: false` in environment overrides for non-Istio environments. |
| **Probes** | Explicit liveness/readiness probe fields in `server-deployment.yaml` (frontend, internal-frontend, history, matching) and `web-deployment.yaml`: `failureThreshold`, `initialDelaySeconds`, `periodSeconds`, `successThreshold`, `timeoutSeconds`, and tcpSocket/http port. We keep a readiness probe for web. |
| **Schema jobs** | Per-store schema jobs (default + visibility) plus a separate namespace job in `server-job.yaml`. Schema job containers: optional wait for Istio proxy ready, create-database / setup-schema / update-schema, then optional quit of Istio proxy and Cloud SQL proxy. Controlled by `schema.istio.enabled` and `cloudSqlIamAuthEnabled`. Values: `schema.createDatabase`, `schema.setup`, `schema.update`, `schema.istio.enabled`. |
| **Topology spread** | Default `topologySpreadConstraints` in values for frontend, internal-frontend, history, matching, worker, and web: zone (`topology.kubernetes.io/zone`, `minDomains: 3`) and hostname spread. Public chart typically uses empty `topologySpreadConstraints: []`. |
| **Images** | Default image repositories point to internal registry: `server.image.repository` (e.g. `ghcr.io/vcp-vsa-control-plane/temporalio-server`), `admintools.image.repository`, `web.image.repository`; tags aligned with public 0.73.1 (server 1.29.1, admintools 1.29.1-tctl-1.18.4-cli-1.5.0, web 2.44.0). Override in environment values as needed. |

---

## Slim chart (manual customizations)

The chart in `kubernetes/temporal/` is a **slimmed** variant of the public chart (chart version **0.73.1**, appVersion 1.29.1):

- **SQL-only persistence:** Only SQL (Postgres 12) is supported for both default and visibility stores. Templates and helpers for Cassandra, Elasticsearch, MySQL/PostgreSQL subcharts have been removed. Set `server.config.persistence.default.driver` and visibility to `sql`; override `host`, `port`, `user`, `password` (or `existingSecret`) in environment override files (e.g. central Cloud SQL Proxy host or `cloudSqlIamAuthEnabled: true` with sidecar).
- **No optional backends:** Cassandra, Elasticsearch, Prometheus, Grafana and MySQL are not supported; related templates, helpers, and values have been removed (no subchart dependencies or conditional branches).
- **Istio optional:** `istioPolicies.yaml` is gated by `schema.istio.enabled`. Default in our values is `schema.istio.enabled: true`; set to `false` in overrides for environments without Istio e.g. NKDev
- **Cloud SQL Proxy:** When `cloudSqlIamAuthEnabled: true`, the Cloud SQL Proxy sidecar is added (see `temporal.databaseProxyContainer`); set `gcpProjectId` and `cloudSqlInstanceConnectionName` (or rely on auto-derived connection name). Alternatively use a central Cloud SQL Proxy via override `host`/`port` only.

All customizations are maintained manually in the chart.

**Current chart version:** 0.73.1 (manually customized; images aligned with public temporal/temporal 0.73.1).

---

## Upgrading to 0.73.1 images (image-only upgrade)

To move to the **container images** used by the public chart **0.73.1** (server 1.29.1, admintools 1.29.1-tctl-1.18.4-cli-1.5.0, web 2.44.0) **without** pulling the full 0.73.1 chart:

- **You can swap the image tags** in your chart/values and deploy. The current slim chart (0.73.1-based) is compatible:
  - Server config is mounted at `/etc/temporal/config/config_template.yaml`; the 1.29.x server uses the same path. The 0.73.1 chart’s new options (`setConfigFilePath`, `configMapsToMount`, `useEntrypointScript`) are for 1.30+ or optional; 0.73.1 defaults keep the same single-config mount behavior.
  - No new **GKE resource requirements** were added in public 0.73.1: no new resource requests, node selectors, or GKE-specific annotations. Upstream still uses `topologySpreadConstraints: []`; we keep our own topology spread in values.

**Image tags to use (0.73.1 equivalents):**

| Component   | Current (0.66.0)     | 0.73.1 images        |
|------------|----------------------|----------------------|
| Server     | 1.28.1               | **1.29.1**           |
| Admin tools| 1.28.1-tctl-1.18.4-cli-1.4.1 | **1.29.1-tctl-1.18.4-cli-1.5.0** |
| Web UI     | 2.39.0               | **2.44.0**          |

Update `Chart.yaml` `appVersion` and `values.yaml` (or your override files) with these tags, mirror the new images to your registry (`scripts/temporal/mirror-images.sh`), then upgrade.

---
