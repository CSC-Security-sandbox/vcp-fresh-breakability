#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

docker build \
  --build-arg SSH_CONFIG="StrictHostKeyChecking" \
  -t ghcr.io/vcp-vsa-control-plane/vsa-builder:v9 \
  --platform=linux/amd64 \
  --push \
  "$SCRIPT_DIR"
