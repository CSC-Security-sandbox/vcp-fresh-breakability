#!/usr/bin/env bash
# Bootstrap a fresh OKE/Kubernetes cluster for the VSA control plane:
#   1) Namespaces: temporal, vcp
#   2) ocir-secret (docker-registry) in both namespaces
#   3) temporal-default-store + temporal-visibility-store (Postgres password) in temporal
#   4) vcp-db-secret in vcp
#   5) Optional: vlm-oci-auth (OCI API private key) in vcp for vlm-worker without workload identity
#   6) Optional: helm upgrade --install all charts (RUN_HELM=1, default)
#
# Required environment variables:
#   OCIR_DOCKER_SERVER, OCIR_DOCKER_USERNAME, OCIR_DOCKER_PASSWORD
#   TEMPORAL_DB_PASSWORD
#   VCP_DB_PASSWORD
#   VCP_DB_USER (optional, default postgres)
#   VCP_DB_ADMIN_USER (optional, default postgres)
#   VCP_DB_ADMIN_PASSWORD (optional, defaults to VCP_DB_PASSWORD)
#
# Example:
#   set -a && source ./cluster-bootstrap.env && set +a
#   ./bootstrap-fresh-cluster.sh
#
# Secrets-only (no Helm):
#   RUN_HELM=0 ./bootstrap-fresh-cluster.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

TEMPORAL_NS="${TEMPORAL_NS:-temporal}"
VCP_NS="${VCP_NS:-vcp}"
RUN_HELM="${RUN_HELM:-1}"

VCP_DB_SECRET_NAME="${VCP_DB_SECRET_NAME:-vcp-db-secret}"
VCP_DB_USER="${VCP_DB_USER:-postgres}"
VCP_DB_ADMIN_USER="${VCP_DB_ADMIN_USER:-postgres}"
VCP_DB_ADMIN_PASSWORD="${VCP_DB_ADMIN_PASSWORD:-${VCP_DB_PASSWORD:-}}"

VLM_OCI_API_SECRET_NAME="${VLM_OCI_API_SECRET_NAME:-vlm-oci-auth}"
EXTRA_HELM_ARGS=${EXTRA_HELM_ARGS:-}

die() {
  echo "Error: $*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing command: $1"
}

require_nonempty() {
  local name=$1
  local val=${2:-}
  [[ -n "$val" ]] || die "environment variable $name must be set (non-empty)"
}

usage() {
  sed -n '1,35p' "$0" | sed 's/^# \{0,1\}//'
  exit 0
}

[[ "${1:-}" != "-h" && "${1:-}" != "--help" ]] || usage

require_cmd kubectl
require_nonempty OCIR_DOCKER_SERVER "${OCIR_DOCKER_SERVER:-}"
require_nonempty OCIR_DOCKER_USERNAME "${OCIR_DOCKER_USERNAME:-}"
require_nonempty OCIR_DOCKER_PASSWORD "${OCIR_DOCKER_PASSWORD:-}"
require_nonempty TEMPORAL_DB_PASSWORD "${TEMPORAL_DB_PASSWORD:-}"
require_nonempty VCP_DB_PASSWORD "${VCP_DB_PASSWORD:-}"
require_nonempty VCP_DB_ADMIN_PASSWORD "${VCP_DB_ADMIN_PASSWORD:-}"

echo "==> Applying namespaces: $TEMPORAL_NS, $VCP_NS"
kubectl apply -f - <<EOF
apiVersion: v1
kind: Namespace
metadata:
  name: ${TEMPORAL_NS}
---
apiVersion: v1
kind: Namespace
metadata:
  name: ${VCP_NS}
EOF

create_ocir_secret() {
  local ns=$1
  echo "==> docker-registry secret ocir-secret in namespace $ns"
  kubectl create secret docker-registry ocir-secret \
    --namespace "$ns" \
    --docker-server="$OCIR_DOCKER_SERVER" \
    --docker-username="$OCIR_DOCKER_USERNAME" \
    --docker-password="$OCIR_DOCKER_PASSWORD" \
    --dry-run=client -o yaml | kubectl apply -f -
}

create_ocir_secret "$TEMPORAL_NS"
create_ocir_secret "$VCP_NS"

echo "==> Temporal DB secrets in $TEMPORAL_NS (keys: password)"
kubectl create secret generic temporal-default-store \
  --namespace "$TEMPORAL_NS" \
  --from-literal=password="$TEMPORAL_DB_PASSWORD" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl create secret generic temporal-visibility-store \
  --namespace "$TEMPORAL_NS" \
  --from-literal=password="$TEMPORAL_DB_PASSWORD" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "==> VCP DB secret $VCP_DB_SECRET_NAME in $VCP_NS"
kubectl create secret generic "$VCP_DB_SECRET_NAME" \
  --namespace "$VCP_NS" \
  --from-literal=DB_USER="$VCP_DB_USER" \
  --from-literal=DB_PASSWORD="$VCP_DB_PASSWORD" \
  --from-literal=DB_ADMIN_USER="$VCP_DB_ADMIN_USER" \
  --from-literal=DB_ADMIN_PASSWORD="$VCP_DB_ADMIN_PASSWORD" \
  --dry-run=client -o yaml | kubectl apply -f -

if [[ "${VLM_CREATE_OCI_API_SECRET:-0}" == "1" ]]; then
  require_nonempty OCI_API_TENANCY "${OCI_API_TENANCY:-}"
  require_nonempty OCI_API_USER "${OCI_API_USER:-}"
  require_nonempty OCI_API_FINGERPRINT "${OCI_API_FINGERPRINT:-}"
  require_nonempty OCI_API_PRIVATE_KEY_FILE "${OCI_API_PRIVATE_KEY_FILE:-}"
  [[ -f "$OCI_API_PRIVATE_KEY_FILE" ]] || die "OCI_API_PRIVATE_KEY_FILE not a file: $OCI_API_PRIVATE_KEY_FILE"

  echo "==> OCI API secret $VLM_OCI_API_SECRET_NAME in $VCP_NS (for vlm-worker file-based auth)"
  kubectl create secret generic "$VLM_OCI_API_SECRET_NAME" \
    --namespace "$VCP_NS" \
    --from-literal=OCI_TENANCY="$OCI_API_TENANCY" \
    --from-literal=OCI_USER="$OCI_API_USER" \
    --from-literal=OCI_FINGERPRINT="$OCI_API_FINGERPRINT" \
    --from-file=OCI_PRIVATE_KEY="$OCI_API_PRIVATE_KEY_FILE" \
    --from-literal=OCI_PASSPHRASE="${OCI_API_PASSPHRASE:-}" \
    --dry-run=client -o yaml | kubectl apply -f -
fi

if [[ "$RUN_HELM" == "1" ]]; then
  install_script="$SCRIPT_DIR/install-charts-with-overrides.sh"
  [[ -x "$install_script" ]] || chmod +x "$install_script"
  echo "==> Helm: $install_script"
  TEMPORAL_NS="$TEMPORAL_NS" VCP_NS="$VCP_NS" EXTRA_HELM_ARGS="$EXTRA_HELM_ARGS" "$install_script"
else
  echo "==> RUN_HELM=0 — skipping Helm. When ready:"
  echo "    TEMPORAL_NS=$TEMPORAL_NS VCP_NS=$VCP_NS ./install-charts-with-overrides.sh"
fi

echo "==> Done. Verify:"
echo "    kubectl get secrets -n $TEMPORAL_NS"
echo "    kubectl get secrets -n $VCP_NS"
echo "    kubectl get pods -n $TEMPORAL_NS"
echo "    kubectl get pods -n $VCP_NS"
