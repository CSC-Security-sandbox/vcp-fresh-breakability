#!/bin/bash
# This script creates the vcp namespace and a Kubernetes secret for GitHub Container Registry using a PAT from the environment.
# Usage: export GHVSA_PAT=your_pat; ./before_deploy.sh

set -euo pipefail

SECRET_NAME=ghcr-secret
NAMESPACE=vcp

# Create namespace if it doesn't exist
kubectl get namespace "$NAMESPACE" >/dev/null 2>&1 || kubectl create namespace "$NAMESPACE"

echo "Namespace '$NAMESPACE' ensured."

# Apply PVC configuration
kubectl apply -f "$(dirname "$0")/pvc.yaml"

echo "Applied PVC configuration."

if [[ -z "${GHVSA_PAT:-}" ]]; then
  echo "GHVSA_PAT environment variable is not set."
  exit 1
fi

kubectl create secret docker-registry "$SECRET_NAME" \
  --docker-server=ghcr.io \
  --docker-username=github \
  --docker-password="$GHVSA_PAT" \
  --docker-email=none@github.com \
  --namespace "$NAMESPACE" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "Kubernetes secret '$SECRET_NAME' created/updated in namespace '$NAMESPACE' for GitHub Container Registry."



