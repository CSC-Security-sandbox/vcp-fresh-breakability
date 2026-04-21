#!/usr/bin/env bash
# Create a secret in the vcp namespace for use by vcp-worker, vlm-worker, and oci-proxy,
# using the same DB password as Temporal (temporal namespace). Temporal runs in "temporal"
# namespace; VCP charts run in "vcp" namespace and cannot reference secrets across namespaces.
#
# Usage:
#   ./create-vcp-secrets-from-temporal.sh [TEMPORAL_NAMESPACE] [VCP_NAMESPACE] [SECRET_NAME]
#
# Example (defaults: temporal, vcp, vcp-db-credentials):
#   ./create-vcp-secrets-from-temporal.sh
#
# Then set in values.yaml for vcp-worker-chart, vlm-worker, and oci-proxy:
#   database.useExistingSecret: true
#   database.existingSecretName: "vcp-db-credentials"
# (and for vlm-worker: workerConfig.useExistingSecret: true, workerConfig.existingSecretName: "vcp-db-credentials")

set -e

TEMPORAL_NS="${1:-temporal}"
VCP_NS="${2:-vcp}"
SECRET_NAME="${3:-vcp-db-credentials}"

# Get Temporal default store password (key: password)
TEMPORAL_PASSWORD=$(kubectl get secret temporal-default-store -n "$TEMPORAL_NS" -o jsonpath='{.data.password}' 2>/dev/null | base64 -d) || true
if [ -z "$TEMPORAL_PASSWORD" ]; then
  echo "Error: Could not read temporal-default-store secret in namespace $TEMPORAL_NS. Create it first (see README)." >&2
  exit 1
fi

# Optional: VLM encryption key (same value as you use for vsaVlmEncryptionKey). Set env to avoid prompt.
VLM_ENCRYPT_KEY="${ONTAP_CREDENTIAL_ENCRYPT_KEY:-}"
if [ -z "$VLM_ENCRYPT_KEY" ]; then
  echo "Enter ONTAP_CREDENTIAL_ENCRYPT_KEY (32-byte for AES-256; must match across vcp-worker and vlm-worker):"
  read -rs VLM_ENCRYPT_KEY
  echo
fi

# DB user for vcp database (often same as Temporal's postgres user)
DB_USER="${DB_USER:-postgres}"
METRICS_DB_USER="${METRICS_DB_USER:-metrics}"
DB_ADMIN_USER="${DB_ADMIN_USER:-postgres}"
# Use same password as Temporal for vcp DB if same Postgres server
DB_PASSWORD="${DB_PASSWORD:-$TEMPORAL_PASSWORD}"
VSA_NODE_USERNAME="${VSA_NODE_USERNAME:-admin}"
VSA_NODE_PASSWORD="${VSA_NODE_PASSWORD:-}"

kubectl create namespace "$VCP_NS" --dry-run=client -o yaml | kubectl apply -f -

kubectl create secret generic "$SECRET_NAME" -n "$VCP_NS" \
  --from-literal=DB_USER="$DB_USER" \
  --from-literal=DB_PASSWORD="$DB_PASSWORD" \
  --from-literal=DB_USERNAME="$DB_USER" \
  --from-literal=DB_ADMIN_USER="$DB_ADMIN_USER" \
  --from-literal=DB_ADMIN_USERNAME="$DB_ADMIN_USER" \
  --from-literal=DB_ADMIN_PASSWORD="$DB_PASSWORD" \
  --from-literal=METRICS_DB_USER="$METRICS_DB_USER" \
  --from-literal=METRICS_DB_PASSWORD="${METRICS_DB_PASSWORD:-$DB_PASSWORD}" \
  --from-literal=METRICS_DB_USERNAME="$METRICS_DB_USER" \
  --from-literal=VSA_NODE_USERNAME="$VSA_NODE_USERNAME" \
  --from-literal=VSA_NODE_PASSWORD="$VSA_NODE_PASSWORD" \
  --from-literal=ONTAP_CREDENTIAL_ENCRYPT_KEY="$VLM_ENCRYPT_KEY" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "Created/updated secret '$SECRET_NAME' in namespace '$VCP_NS'."
echo "Set useExistingSecret: true and existingSecretName: \"$SECRET_NAME\" in vcp-worker-chart, vlm-worker, and oci-proxy values."
