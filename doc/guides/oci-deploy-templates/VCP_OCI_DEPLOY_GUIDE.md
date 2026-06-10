# VCP OCI Deployment Guide — Quick Start

Deploy **oci-proxy**, **vcp-worker**, and **vlm-worker** on OKE by building
container images from the binaries provided in the release bundle, then
deploying with Helm.

---

## 1. Prerequisites

| Tool | Min version | Install |
|------|------------|---------|
| `helm` | 3.14+ | <https://helm.sh/docs/intro/install/> |
| `kubectl` | 1.28+ | <https://kubernetes.io/docs/tasks/tools/> |
| `jq` | 1.6+ | <https://jqlang.github.io/jq/download/> |
| `docker` | 20.10+ | <https://docs.docker.com/engine/install/> |

You also need:

- A running OKE cluster with `kubectl` context configured.
- OCIR credentials (tenancy namespace + auth token) for your target registry.
- OCI Vault, compartment, and compute image OCIDs for your environment.
- A PostgreSQL instance reachable from the cluster.
- A Temporal server reachable from the cluster (must be running **before** oci-proxy).

---

## 2. Unpack the release bundle

You will receive a release bundle from the VCP release team. It contains:

| Path | Description |
|------|-------------|
| `release-manifest.json` | Single source of truth for versions and artifacts |
| `VCP_OCI_DEPLOY_GUIDE.md` | This file |
| `oci-proxy-overrides.yaml` | Helm override template for oci-proxy |
| `vcp-worker-overrides.yaml` | Helm override template for vcp-worker |
| `vlm-worker-overrides.yaml` | Helm override template for vlm-worker |
| `oci-proxy-<TAG>.tgz` | Helm chart for oci-proxy |
| `oci-vcp-worker-chart-<TAG>.tgz` | Helm chart for vcp-worker |
| `oci-vlm-worker-<TAG>.tgz` | Helm chart for vlm-worker |
| `artifacts/vcp-worker` | vcp-worker Go binary |
| `artifacts/oci-proxy` | oci-proxy Go binary |
| `artifacts/vlm-worker` | vlm-worker binary |
| `worker/Dockerfile.oci` | Dockerfile for vcp-worker image |
| `oci-proxy/Dockerfile.oci` | Dockerfile for oci-proxy image |
| `vlm-worker/Dockerfile.oci` | Dockerfile for vlm-worker image |
| `common/vsa_config/vlm-config-oci.json` | VLM config baked into vcp-worker image |
| `config/vmrs_oci.yaml` | VM reservation config baked into vcp-worker image |

Extract everything into one working directory and stay there for all subsequent steps:

```bash
mkdir -p ~/oci && cd ~/oci
# Extract or copy the bundle contents here so the layout matches the table above
ls artifacts/ worker/ oci-proxy/ vlm-worker/ common/ config/ *.tgz *.yaml release-manifest.json
```

---

## 3. Set environment variables

Set these once — all subsequent steps use them:

```bash
export MANIFEST="release-manifest.json"
export TAG=$(jq -r '.metadata.version' "$MANIFEST")
export VLM_TAG=$(jq -r '.vcpArtifacts.containerRegistry.images[]
                          | select(.name=="vlm-worker") | .tag' "$MANIFEST")
export OCIR_HOST="<OCIR_HOSTNAME>"          # e.g. iad.ocir.io
export OCIR_NS="<OCIR_TENANCY_NS>"          # e.g. idqogasfjw45
export IMAGE_REGISTRY="${OCIR_HOST}/${OCIR_NS}"
export NS="vcp"

echo "TAG:          $TAG"
echo "VLM_TAG:      $VLM_TAG"
echo "Registry:     $IMAGE_REGISTRY"
echo "Namespace:    $NS"
```

> Verify all four values printed correctly before continuing.

---

## 4. Unpack Helm charts

```bash
# Run from ~/oci/
for CHART_TGZ in oci-proxy-*.tgz oci-vcp-worker-chart-*.tgz oci-vlm-worker-*.tgz; do
  [ -f "$CHART_TGZ" ] && tar xzf "$CHART_TGZ" && echo "Extracted: $CHART_TGZ"
done
```

Expected directories after extraction: `oci-proxy/`, `oci-vcp-worker-chart/`, `oci-vlm-worker/`.

> **Note:** `oci-proxy/` will also contain `Dockerfile.oci` from the bundle. That is expected.

---

## 5. Verify binaries

```bash
# Run from ~/oci/
chmod +x artifacts/vcp-worker artifacts/oci-proxy artifacts/vlm-worker
file artifacts/vcp-worker   # ELF 64-bit LSB executable
file artifacts/oci-proxy    # ELF 64-bit LSB executable
file artifacts/vlm-worker   # ELF 64-bit LSB executable
```

All three must show `ELF 64-bit LSB executable`. If any fail, contact the release team for a replacement binary.

---

## 6. Build container images

All three builds must run from `~/oci/` so the Dockerfile `COPY` paths resolve correctly.

```bash
# vcp-worker — bakes in vlm-config-oci.json and vmrs_oci.yaml
docker build \
  --build-arg BASE=gcr.io/distroless/base-debian12:nonroot \
  -f worker/Dockerfile.oci \
  -t "${IMAGE_REGISTRY}/vcp-worker:${TAG}" .

# oci-proxy — bakes in common/vsa_config/ directory
docker build \
  --build-arg BASE=gcr.io/distroless/base-debian12:nonroot \
  -f oci-proxy/Dockerfile.oci \
  -t "${IMAGE_REGISTRY}/oci-proxy:${TAG}" .

# vlm-worker — binary only, all config via Helm values
docker build \
  --build-arg BASE=gcr.io/distroless/base-debian12:nonroot \
  -f vlm-worker/Dockerfile.oci \
  -t "${IMAGE_REGISTRY}/vlm-worker:${VLM_TAG}" .
```

> **Custom base image:** Replace `gcr.io/distroless/base-debian12:nonroot` with your
> org's hardened base image if required.

What each build produces:

| Image | Binary → container path | Config baked in |
|-------|------------------------|-----------------|
| vcp-worker | `artifacts/vcp-worker` → `/app` | `vlm-config-oci.json`, `vmrs_oci.yaml` |
| oci-proxy | `artifacts/oci-proxy` → `/app` | `common/vsa_config/` directory |
| vlm-worker | `artifacts/vlm-worker` → `/app` | None — config via Helm values |

---

## 7. Push images to OCIR

```bash
# Login to OCIR
echo "<AUTH_TOKEN>" | docker login "$OCIR_HOST" \
  -u "${OCIR_NS}/oracleidentitycloudservice/<USER_EMAIL>" \
  --password-stdin

# Push all three images
docker push "${IMAGE_REGISTRY}/vcp-worker:${TAG}"
docker push "${IMAGE_REGISTRY}/oci-proxy:${TAG}"
docker push "${IMAGE_REGISTRY}/vlm-worker:${VLM_TAG}"
```

---

## 8. Fill in override templates

Copy and edit each override file — replace every `<PLACEHOLDER>` with your environment values:

```bash
cp oci-proxy-overrides.yaml      my-oci-proxy-overrides.yaml
cp vcp-worker-overrides.yaml     my-vcp-worker-overrides.yaml
cp vlm-worker-overrides.yaml     my-vlm-worker-overrides.yaml
```

Key placeholder reference:

| Placeholder | Value |
|-------------|-------|
| `<OCIR_HOSTNAME>` | Same as `$OCIR_HOST` (e.g. `iad.ocir.io`) |
| `<OCIR_TENANCY_NS>` | Same as `$OCIR_NS` (e.g. `idqogasfjw45`) |
| `<OCI_REGION>` | OCI region identifier (e.g. `us-ashburn-1`) |
| `<POSTGRES_HOST>` | OCI Database primary connection host |
| `<IMAGE_TAG>` | Value of `$TAG` — `echo $TAG` |
| `<VLM_IMAGE_TAG>` | Value of `$VLM_TAG` — `echo $VLM_TAG` |
| `<VSA_IMAGE_OCID>` | Compute image OCID for the VSA node |
| `<VSA_MEDIATOR_IMAGE_OCID>` | Compute image OCID for the VSA mediator |
| `<OCI_VAULT_OCID>` | OCI Vault OCID |
| `<OCI_VAULT_SECRET_OCID>` | OCI Vault secret OCID |
| `<OCI_MASTER_KEY_OCID>` | OCI Vault master encryption key OCID |
| `<OCI_COMPARTMENT_OCID>` | Target compartment OCID |
| `<ONTAP_VERSION>` | `jq -r '.metadata.ontapVersions[0]' "$MANIFEST"` |
| `<VSA_VLM_ENCRYPTION_KEY>` | 32-byte AES-256 key — **must be identical** in vcp-worker and vlm-worker overrides |

Verify no placeholders remain before deploying:

```bash
grep '<' my-oci-proxy-overrides.yaml my-vcp-worker-overrides.yaml my-vlm-worker-overrides.yaml
```

The command should produce no output.

---

## 9. Create namespace and OCIR pull secret

```bash
kubectl create namespace "$NS" 2>/dev/null || echo "Namespace already exists"

kubectl create secret docker-registry ocir-secret \
  --namespace "$NS" \
  --docker-server="$OCIR_HOST" \
  --docker-username="${OCIR_NS}/oracleidentitycloudservice/<USER_EMAIL>" \
  --docker-password="<AUTH_TOKEN>" \
  2>/dev/null || echo "Secret already exists"
```

---

## 10. Deploy with Helm

> **Important:** Temporal must be running and reachable **before** installing oci-proxy.
> oci-proxy blocks on Temporal connectivity at startup. If Temporal is not yet deployed,
> see [oci-temporal-deployment.md](../oci-temporal-deployment.md) first.
>
> Install order: **Temporal → vcp-worker → vlm-worker → oci-proxy**

```bash
helm upgrade --install vcp-worker ./oci-vcp-worker-chart -n "$NS" \
  -f ./oci-vcp-worker-chart/values.yaml -f my-vcp-worker-overrides.yaml

helm upgrade --install vlm-worker ./oci-vlm-worker -n "$NS" \
  -f ./oci-vlm-worker/values.yaml -f my-vlm-worker-overrides.yaml

helm upgrade --install oci-proxy ./oci-proxy -n "$NS" \
  -f ./oci-proxy/values.yaml -f my-oci-proxy-overrides.yaml
```

---

## 11. Verify

```bash
kubectl -n "$NS" get pods
```

All pods should reach `Running` / `1/1 Ready`. Then check logs:

```bash
kubectl -n "$NS" logs deploy/vcp-worker  --tail=20
kubectl -n "$NS" logs -l app=vlm-worker  --tail=20
kubectl -n "$NS" logs deploy/oci-proxy   --tail=20
```

Look for successful Temporal worker registration and database connectivity in each log.

---

## 12. Troubleshooting

| Symptom | Check |
|---------|-------|
| `ImagePullBackOff` | Verify `ocir-secret` exists in the namespace; confirm `$OCIR_HOST` in secret matches image hostname in overrides |
| `CrashLoopBackOff` on oci-proxy | Temporal not reachable — check `temporalAddress` in values and ensure Temporal is running |
| `CrashLoopBackOff` on vcp-worker or vlm-worker | DB host/credentials wrong, or `vsaVlmEncryptionKey` mismatch between the two |
| Helm install fails | Ensure chart `.tgz` files extracted correctly and directories exist |
| Missing placeholders | Run `grep '<' my-*-overrides.yaml` — must return no output before deploying |
| Pods stuck `Pending` | Check node capacity and namespace resource quotas |
| docker build `COPY` fails | Ensure all builds run from `~/oci/` — not from inside a subdirectory |
