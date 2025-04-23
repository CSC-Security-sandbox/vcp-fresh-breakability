#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Brings up binary
go mod tidy
GOOS=linux GOARCH=amd64 go build -o bin/build/linux/vsacictl .
echo "Binary built successfully at bin/build/linux/vsacictl."

# Build Docker image using environment variables
docker buildx build --platform linux/amd64 \
  --build-arg RUNNER="${RUNNER}" \
  --build-arg GO_VERSION="${GO_VERSION}" \
  --build-arg GO_FILENAME="${GO_FILENAME}" \
  --build-arg GO_FILENAME_SHA="${GO_FILENAME_SHA}" \
  -t ghcr.io/vcp-vsa-control-plane/vsacictl:<tag> .
echo "Docker image built successfully with tag vsacictl."

# Tag and push the Docker image
#docker tag vsacictl:v3 ghcr.io/vcp-vsa-control-plane/vsacictl:v3
#v3 is for example. Give your required <tag>
docker push ghcr.io/vcp-vsa-control-plane/vsacictl:<tag>
echo "Docker image tagged and pushed successfully to ghcr.io/vcp-vsa-control-plane/vsacictl."

# Exit successfully
exit 0