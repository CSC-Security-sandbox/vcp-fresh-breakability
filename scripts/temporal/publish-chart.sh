#!/bin/bash
# Script to package and publish Temporal Helm chart to GitHub

set -e

# Default values
GITHUB_ORG="vcp-vsa-control-plane"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART_DIR="$(cd "${SCRIPT_DIR}/../../kubernetes/charts/temporal" && pwd)"
DRY_RUN=false

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
  echo "Usage: $0 -o GITHUB_ORG [-t GITHUB_PAT] [-d]"
  echo ""
  echo "Options:"
  echo "  -o GITHUB_ORG    GitHub organization or username (required)"
  echo "  -t GITHUB_PAT    GitHub Personal Access Token (if not provided, uses GITHUB_PAT env var)"
  echo "  -d               Dry run (don't push chart)"
  echo "  -h               Display this help message"
  exit 1
}

# Parse command line arguments
while getopts "o:t:dh" opt; do
  case $opt in
    o) GITHUB_ORG="$OPTARG" ;;
    t) GITHUB_PAT="$OPTARG" ;;
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


if [ -z "$GITHUB_PAT" ]; then
  # Try to use environment variable
  echo "Warning: No GitHub PAT provided. You may need to authenticate manually."
fi

echo "=== Temporal Helm Chart Publishing Tool ==="
echo "GitHub Organization: $GITHUB_ORG"
echo "Chart Directory: $CHART_DIR"
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

# GitHub Container Registry method
if [ -n "$GITHUB_PAT" ]; then
  echo "Logging in to GitHub Container Registry..."
  echo "$GITHUB_PAT" | helm registry login ghcr.io -u "$GITHUB_ORG" --password-stdin
fi

if [ "$DRY_RUN" = false ]; then
  echo "Pushing chart to GitHub Container Registry..."
  helm push "$CHART_PACKAGE" "oci://ghcr.io/$GITHUB_ORG"
else
  echo "Dry run - skipping push to GitHub Container Registry"
fi

echo "Chart would be available at: oci://ghcr.io/$GITHUB_ORG/$CHART_NAME"

# Clean up
if [ "$DRY_RUN" = false ]; then
  echo "Cleaning up..."
  rm -f "$CHART_PACKAGE"
fi

echo ""
echo "=== Summary ==="
echo "Chart published to GitHub Container Registry"
echo "To use this chart, run:"
echo "  helm install $CHART_NAME oci://ghcr.io/$GITHUB_ORG/$CHART_NAME --version $CHART_VERSION --values values.yaml"

echo ""
echo "Done!"
