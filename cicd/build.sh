#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

if [ -z "$1" ]; then
  echo "Usage: $0 <version>"
  echo "Example: $0 v35"
  exit 1
fi

VERSION="$1"

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
  -t ghcr.io/vcp-vsa-control-plane/vsacictl:${VERSION} .
echo "Docker image built successfully with tag vsacictl:${VERSION}."

docker buildx build --platform linux/amd64 -t ghcr.io/vcp-vsa-control-plane/vsacictl:${VERSION} .

# Tag and push the Docker image
docker push ghcr.io/vcp-vsa-control-plane/vsacictl:${VERSION}
echo "Docker image tagged and pushed successfully to ghcr.io/vcp-vsa-control-plane/vsacictl:${VERSION}."

# Exit successfully
exit 0
