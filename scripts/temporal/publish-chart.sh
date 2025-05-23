#!/bin/bash
# Script to package and publish Temporal Helm chart to GitHub or GCP artifact repository

set -e

# Default values
GITHUB_ORG="vcp-vsa-control-plane"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART_DIR="$(cd "${SCRIPT_DIR}/../../kubernetes/temporal" && pwd)"
DRY_RUN=false
REGISTRY_TYPE="ghcr" # Default registry type (ghcr or gcr)
GCR_REPO_URL="gcr.io" # Default GCR repository URL

# Check if yq is installed
if ! command -v yq &> /dev/null; then
  echo "Error: yq is not installed. Please install yq first."
  echo "You can install it using: brew install yq (macOS) or snap install yq (Linux)"
  exit 1
fi

# Function to extract value from yaml file using yq
# Note: Different versions of yq may require different syntax
# For yq v4.x: use '.version' format
# For older versions: use 'select(.version)' format
extract_value() {
  local file=$1
  local path=$2
  local value=$(yq eval "$path" "$file")
  echo "$value"
}

# Function to display usage information
usage() {
  echo "Usage: $0 -o GITHUB_ORG [-t GITHUB_PAT] [-r REGISTRY_TYPE] [-g GCR_REPO_URL] [-d]"
  echo ""
  echo "Options:"
  echo "  -o GITHUB_ORG    GitHub organization or username (required)"
  echo "  -t GITHUB_PAT    GitHub Personal Access Token (if not provided, uses GITHUB_PAT env var)"
  echo "  -r REGISTRY_TYPE Registry type to use (ghcr or gcr, default: ghcr)"
  echo "  -g GCR_REPO_URL  GCP artifact repository URL (default: gcr.io, only used when REGISTRY_TYPE is gcr)"
  echo "  -d               Dry run (don't push chart)"
  echo "  -h               Display this help message"
  exit 1
}

# Parse command line arguments
while getopts "o:t:r:g:dh" opt; do
  case $opt in
    o) GITHUB_ORG="$OPTARG" ;;
    t) GITHUB_PAT="$OPTARG" ;;
    r) REGISTRY_TYPE="$OPTARG" ;;
    g) GCR_REPO_URL="$OPTARG" ;;
    d) DRY_RUN=true ;;
    h) usage ;;
    *) usage ;;
  esac
done

# Check if required parameters are provided
if [ -z "$GITHUB_ORG" ]; then
  echo "Error: GitHub organization (-o) is required"
  usage
fi

# Validate registry type
if [ "$REGISTRY_TYPE" != "ghcr" ] && [ "$REGISTRY_TYPE" != "gcr" ]; then
  echo "Error: Invalid registry type (-r). Must be 'ghcr' or 'gcr'"
  usage
fi

if [ -z "$GITHUB_PAT" ] && [ "$REGISTRY_TYPE" = "ghcr" ]; then
  # Try to use environment variable
  echo "Warning: No GitHub PAT provided. You may need to authenticate manually."
fi

echo "=== Temporal Helm Chart Publishing Tool ==="
echo "Organization: $GITHUB_ORG"
echo "Chart Directory: $CHART_DIR"
echo "Registry Type: $REGISTRY_TYPE"
if [ "$REGISTRY_TYPE" = "gcr" ]; then
  echo "GCP Repository URL: $GCR_REPO_URL"
fi
echo "Dry Run: $DRY_RUN"
echo ""

# Get chart version and name from Chart.yaml using yq
CHART_VERSION=$(extract_value "$CHART_DIR/Chart.yaml" '.version')
CHART_NAME=$(extract_value "$CHART_DIR/Chart.yaml" '.name')
CHART_PACKAGE="${CHART_NAME}-${CHART_VERSION}.tgz"

echo "Chart Name: $CHART_NAME"
echo "Chart Version: $CHART_VERSION"
echo ""

# Package the chart
echo "Packaging Helm chart..."
cd "$CHART_DIR/.."
helm package "$CHART_NAME"

# Determine registry URL based on registry type
REGISTRY_URL=""
if [ "$REGISTRY_TYPE" = "ghcr" ]; then
  REGISTRY_URL="ghcr.io/$GITHUB_ORG"

  # GitHub Container Registry login method
  if [ -n "$GITHUB_PAT" ]; then
    echo "Logging in to GitHub Container Registry..."
    echo "$GITHUB_PAT" | helm registry login ghcr.io -u "$GITHUB_ORG" --password-stdin
  fi
elif [ "$REGISTRY_TYPE" = "gcr" ]; then
  REGISTRY_URL="$GCR_REPO_URL/$GITHUB_ORG"

  # GCP Artifact Registry login method
  echo "Configuring Helm for Google Container Registry..."
  gcloud auth configure-docker "$GCR_REPO_URL" --quiet
fi

if [ "$DRY_RUN" = false ]; then
  if [ "$REGISTRY_TYPE" = "ghcr" ]; then
    echo "Pushing chart to GitHub Container Registry..."
  elif [ "$REGISTRY_TYPE" = "gcr" ]; then
    echo "Pushing chart to GCP Artifact Registry..."
  fi
  helm push "$CHART_PACKAGE" "oci://$REGISTRY_URL"
else
  echo "Dry run - skipping push to registry"
fi

echo "Chart would be available at: oci://$REGISTRY_URL/$CHART_NAME"

# Clean up
if [ "$DRY_RUN" = false ]; then
  echo "Cleaning up..."
  rm -f "$CHART_PACKAGE"
fi

echo ""
echo "=== Summary ==="
if [ "$REGISTRY_TYPE" = "ghcr" ]; then
  echo "Chart published to GitHub Container Registry"
  echo "To use this chart, run:"
  echo "  helm install $CHART_NAME oci://ghcr.io/$GITHUB_ORG/$CHART_NAME --version $CHART_VERSION --values ghcr_values.yaml"
elif [ "$REGISTRY_TYPE" = "gcr" ]; then
  echo "Chart published to GCP Artifact Registry"
  echo "To use this chart, run:"
  echo "  helm install $CHART_NAME oci://$GCR_REPO_URL/$GITHUB_ORG/$CHART_NAME --version $CHART_VERSION --values gcr_values.yaml"
fi

echo ""
echo "Done!"
