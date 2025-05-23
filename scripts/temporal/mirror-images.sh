#!/bin/bash
# Script to detect, pull, and push Temporal Docker images to a private container registry (GHCR or GCR)

set -e

# Default values
DEFAULT_CHART_VERSION="0.60.0"
CHART_VERSION="${TEMPORAL_CHART_VERSION:-$DEFAULT_CHART_VERSION}"
REPO_NAME="vcp-vsa-control-plane"
VALUES_FILE="/tmp/temporal/values.yaml"
DRY_RUN=false
REGISTRY_TYPE="ghcr" # Default registry type (ghcr or gcr)
GCR_REPO_URL="gcr.io" # Default GCR repository URL

# Check if /tmp/temporal exists and remove it
if [ -d "/tmp/temporal" ]; then
  echo "/tmp/temporal already exists. Removing it..."
  rm -rf /tmp/temporal
fi

# Check if yq is installed
if ! command -v yq &> /dev/null; then
  echo "Error: yq is not installed. Please install yq first."
  echo "You can install it using: brew install yq (macOS) or snap install yq (Linux)"
  exit 1
fi

# Function to display usage information
usage() {
  echo "Usage: $0 -o REPO_NAME [-t GITHUB_PAT] [-v VALUES_FILE] [-c CHART_VERSION] [-r REGISTRY_TYPE] [-g GCR_REPO_URL] [-d]"
  echo ""
  echo "Options:"
  echo "  -o REPO_NAME    GitHub organization or username (required)"
  echo "  -t GITHUB_PAT    GitHub Personal Access Token (if not provided, uses GITHUB_PAT env var)"
  echo "  -v VALUES_FILE   Path to values.yaml file (default: ../charts/temporal/values.yaml)"
  echo "  -c CHART_VERSION Temporal Helm chart version to use (default: $CHART_VERSION)"
  echo "  -r REGISTRY_TYPE Registry type to use (ghcr or gcr, default: ghcr)"
  echo "  -g GCR_REPO_URL  GCR repository URL (default: gcr.io, only used when REGISTRY_TYPE is gcr)"
  echo "  -d               Dry run (don't push images)"
  echo "  -h               Display this help message"
  echo ""
  echo "Environment Variables:"
  echo "  TEMPORAL_CHART_VERSION  Temporal Helm chart version to use (default: $DEFAULT_CHART_VERSION)"
  echo "                          This can be overridden by the -c option"
  exit 1
}

# Parse command line arguments
while getopts "o:t:v:c:r:g:dh" opt; do
  case $opt in
    o) REPO_NAME="$OPTARG" ;;
    t) GITHUB_PAT="$OPTARG" ;;
    v) VALUES_FILE="$OPTARG" ;;
    c) CHART_VERSION="$OPTARG" ;;
    r) REGISTRY_TYPE="$OPTARG" ;;
    g) GCR_REPO_URL="$OPTARG" ;;
    d) DRY_RUN=true ;;
    h) usage ;;
    *) usage ;;
  esac
done

# Check if required parameters are provided
if [ -z "$REPO_NAME" ]; then
  echo "Error: GitHub organization (-o) is required"
  usage
fi

# Validate registry type
if [ "$REGISTRY_TYPE" != "ghcr" ] && [ "$REGISTRY_TYPE" != "gcr" ]; then
  echo "Error: Invalid registry type (-r). Must be 'ghcr' or 'gcr'"
  usage
fi

if [ -z "$GITHUB_PAT" ] &&  [ "$REGISTRY_TYPE" = "ghcr" ] ; then
  # Try to use environment variable
  if [ -z "$GITHUB_PAT" ]; then
    echo "Warning: No GitHub PAT provided. You may need to authenticate manually with 'docker login'"
  fi
fi

# Pull the Temporal Helm chart
helm pull temporal/temporal --version "$CHART_VERSION" --untar --destination /tmp

# Check if values file exists
if [ ! -f "$VALUES_FILE" ]; then
  echo "Error: Values file not found: $VALUES_FILE"
  exit 1
fi

echo "=== Temporal Docker Image Mirror Tool ==="
echo "Destination image Repo: $REPO_NAME"
echo "Chart Version: $CHART_VERSION"
echo "Values File: $VALUES_FILE"
echo "Registry Type: $REGISTRY_TYPE"
if [ "$REGISTRY_TYPE" = "gcr" ]; then
  echo "GCR Repository URL: $GCR_REPO_URL"
fi
echo "Dry Run: $DRY_RUN"
echo ""

# Extract image information from values.yaml
echo "Detecting image versions from Helm chart..."

# Function to extract value from yaml file using yq
extract_value() {
  local file=$1
  local path=$2
  local value=$(yq eval "$path" "$file")
  echo "$value"
}

# Extract image repositories and tags using yq
# Note: Different versions of yq may require different syntax
# For yq v4.x: use '.server.image.repository' format
# For older versions: use 'select(.server.image.repository)' format
SERVER_REPO=$(yq eval '.server.image.repository' "$VALUES_FILE")
SERVER_TAG=$(yq eval '.server.image.tag' "$VALUES_FILE")

ADMIN_TOOLS_REPO=$(yq eval '.admintools.image.repository' "$VALUES_FILE")
ADMIN_TOOLS_TAG=$(yq eval '.admintools.image.tag' "$VALUES_FILE")

UI_REPO=$(yq eval '.web.image.repository' "$VALUES_FILE")
UI_TAG=$(yq eval '.web.image.tag' "$VALUES_FILE")

echo "Found images:"
echo "Server: $SERVER_REPO:$SERVER_TAG"
echo "Admin Tools: $ADMIN_TOOLS_REPO:$ADMIN_TOOLS_TAG"
echo "UI: $UI_REPO:$UI_TAG"
echo ""

# Login to the appropriate container registry if credentials are provided
if [ "$REGISTRY_TYPE" = "ghcr" ]; then
  if [ -n "$GITHUB_PAT" ]; then
    echo "Logging in to GitHub Container Registry..."
    echo "$GITHUB_PAT" | docker login ghcr.io -u "$REPO_NAME" --password-stdin
    echo ""
  fi
elif [ "$REGISTRY_TYPE" = "gcr" ]; then
  echo "Configuring Docker for Google Container Registry..."
  # Use gcloud auth configure-docker for GCR
  gcloud auth configure-docker "$GCR_REPO_URL"
  echo ""
fi

# Function to process an image
process_image() {
  local source_repo=$1
  local source_tag=$2
  local dest_name=$3

  echo "Processing $source_repo:$source_tag"

  # Create destination image name based on registry type
  local registry_url=""
  if [ "$REGISTRY_TYPE" = "ghcr" ]; then
    registry_url="ghcr.io/$REPO_NAME"
  elif [ "$REGISTRY_TYPE" = "gcr" ]; then
    registry_url="$GCR_REPO_URL/$REPO_NAME"
  fi

  local dest_image="$registry_url/$dest_name:$source_tag"

  echo "Pulling $source_repo:$source_tag (linux/amd64)..."
  docker pull --platform linux/amd64 "$source_repo:$source_tag"

  echo "Tagging as $dest_image..."
  docker tag "$source_repo:$source_tag" "$dest_image"

  if [ "$DRY_RUN" = false ]; then
    echo "Pushing $dest_image (linux/amd64)..."
    docker push "$dest_image"
  else
    echo "Dry run - skipping push of $dest_image (linux/amd64)"
  fi

  echo "Done with $dest_name"
  echo ""
}

# Process each image
process_image "$SERVER_REPO" "$SERVER_TAG" "temporalio-server"
process_image "$ADMIN_TOOLS_REPO" "$ADMIN_TOOLS_TAG" "temporalio-admin-tools"
process_image "$UI_REPO" "$UI_TAG" "temporalio-ui"

# Determine registry URL for summary
REGISTRY_URL=""
if [ "$REGISTRY_TYPE" = "ghcr" ]; then
  REGISTRY_URL="ghcr.io/$REPO_NAME"
elif [ "$REGISTRY_TYPE" = "gcr" ]; then
  REGISTRY_URL="$GCR_REPO_URL/$REPO_NAME"
fi

echo "=== Summary ==="
echo "Processed images (linux/amd64):"
echo "- $SERVER_REPO:$SERVER_TAG -> $REGISTRY_URL/temporalio-server:$SERVER_TAG (linux/amd64)"
echo "- $ADMIN_TOOLS_REPO:$ADMIN_TOOLS_TAG -> $REGISTRY_URL/temporalio-admin-tools:$ADMIN_TOOLS_TAG (linux/amd64)"
echo "- $UI_REPO:$UI_TAG -> $REGISTRY_URL/temporalio-ui:$UI_TAG (linux/amd64)"
echo ""

if [ "$DRY_RUN" = false ]; then
  if [ "$REGISTRY_TYPE" = "ghcr" ]; then
    echo "All images (linux/amd64) have been pushed to GitHub Container Registry"
  elif [ "$REGISTRY_TYPE" = "gcr" ]; then
    echo "All images (linux/amd64) have been pushed to Google Container Registry ($GCR_REPO_URL)"
  fi
  echo "Make sure the render helm chart is using the following versions: 'helm template test-release . | grep image:'"
  echo ""
  echo "temporal:"
  echo "  server:"
  echo "    image:"
  echo "      repository: $REGISTRY_URL/temporalio-server"
  echo "      tag: $SERVER_TAG"
  echo ""
  echo "  admintools:"
  echo "    image:"
  echo "      repository: $REGISTRY_URL/temporalio-admin-tools"
  echo "      tag: $ADMIN_TOOLS_TAG"
  echo ""
  echo "  web:"
  echo "    image:"
  echo "      repository: $REGISTRY_URL/temporalio-ui"
  echo "      tag: $UI_TAG"
else
  echo "Dry run completed. No images were pushed."
fi

echo ""
echo "Done!"
