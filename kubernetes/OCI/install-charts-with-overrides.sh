#!/usr/bin/env bash
# Install OCI Helm charts in dependency order with per-chart overrides.
#
# Order: temporal -> vcp-worker-chart -> vlm-worker -> oci-proxy
#
# Defaults (see scripts/create-vcp-secrets-from-temporal.sh): Temporal in "temporal" namespace;
# oci-proxy, vcp-worker, and vlm-worker in "vcp" namespace (DNS: temporal-frontend.temporal.svc...).
#
# Usage (from this kubernetes/OCI directory):
#   ./install-charts-with-overrides.sh
#   TEMPORAL_NS=my-temporal VCP_NS=my-vcp ./install-charts-with-overrides.sh
#   EXTRA_HELM_ARGS="--wait --timeout 15m" ./install-charts-with-overrides.sh
#
# Optional: different overrides file per chart:
#   VCP_WORKER_OVERRIDES="$PWD/vcp-worker-chart/overrides_nmobsa.yaml" ./install-charts-with-overrides.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Script lives next to chart directories (temporal/, oci-proxy/, ...).
OCI_ROOT="$SCRIPT_DIR"

TEMPORAL_NS="${TEMPORAL_NS:-temporal}"
VCP_NS="${VCP_NS:-vcp}"

TEMPORAL_OVERRIDES="${TEMPORAL_OVERRIDES:-$OCI_ROOT/temporal/overrides.yaml}"
OCI_PROXY_OVERRIDES="${OCI_PROXY_OVERRIDES:-$OCI_ROOT/oci-proxy/overrides.yaml}"
VCP_WORKER_OVERRIDES="${VCP_WORKER_OVERRIDES:-$OCI_ROOT/vcp-worker-chart/overrides.yaml}"
VLM_WORKER_OVERRIDES="${VLM_WORKER_OVERRIDES:-$OCI_ROOT/vlm-worker/overrides.yaml}"

# Extra args for every helm invocation (e.g. --wait --timeout 15m --atomic)
EXTRA_HELM_ARGS=${EXTRA_HELM_ARGS:-}

require_file() {
  local f=$1
  if [[ ! -f "$f" ]]; then
    echo "Error: missing file: $f" >&2
    exit 1
  fi
}

helm_install() {
  local release=$1
  local chart_dir=$2
  local namespace=$3
  local overrides=$4

  require_file "$chart_dir/Chart.yaml"
  require_file "$chart_dir/values.yaml"
  require_file "$overrides"

  echo "==> helm upgrade --install $release (namespace=$namespace)"
  # shellcheck disable=SC2086
  helm upgrade --install "$release" "$chart_dir" \
    --namespace "$namespace" \
    --create-namespace \
    -f "$chart_dir/values.yaml" \
    -f "$overrides" \
    $EXTRA_HELM_ARGS
}

helm_install temporal "$OCI_ROOT/temporal" "$TEMPORAL_NS" "$TEMPORAL_OVERRIDES"
helm_install vcp-worker "$OCI_ROOT/vcp-worker-chart" "$VCP_NS" "$VCP_WORKER_OVERRIDES"
helm_install vlm-worker "$OCI_ROOT/vlm-worker" "$VCP_NS" "$VLM_WORKER_OVERRIDES"
helm_install oci-proxy "$OCI_ROOT/oci-proxy" "$VCP_NS" "$OCI_PROXY_OVERRIDES"

echo "Done. Releases: temporal ($TEMPORAL_NS); vcp-worker, vlm-worker, oci-proxy ($VCP_NS)."
