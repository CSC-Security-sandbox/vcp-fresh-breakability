#!/bin/bash
# Script to generate a customized Temporal Helm chart for the project
# This script pulls the official Temporal Helm chart, customizes it, and stores it in the project's charts directory

set -e

# Default values
DEFAULT_CHART_VERSION="0.60.0"
CHART_VERSION="${TEMPORAL_CHART_VERSION:-$DEFAULT_CHART_VERSION}"
GITHUB_ORG="vcp-vsa-control-plane"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART_DIR="$(cd "${SCRIPT_DIR}/../../kubernetes/temporal" && pwd)"
TEMP_DIR="/tmp/temporal"
REGISTRY_TYPE="ghcr" # Default registry type (ghcr or gcr)
GCR_REPO_URL="gcr.io" # Default GCR repository URL

# Check if yq is installed
if ! command -v yq &> /dev/null; then
  echo "Error: yq is not installed. Please install yq first."
  echo "You can install it using: brew install yq (macOS) or snap install yq (Linux)"
  exit 1
fi

# Check if helm is installed
if ! command -v helm &> /dev/null; then
  echo "Error: helm is not installed. Please install helm first."
  echo "You can install it using: brew install helm (macOS) or follow instructions at https://helm.sh/docs/intro/install/"
  exit 1
fi

# Function to display usage information
usage() {
  echo "Usage: $0 [-v CHART_VERSION] [-o GITHUB_ORG] [-r REGISTRY_TYPE] [-g GCR_REPO_URL]"
  echo ""
  echo "Options:"
  echo "  -v CHART_VERSION  Temporal Helm chart version to use (default: $CHART_VERSION)"
  echo "  -o GITHUB_ORG     GitHub organization or username for image repositories (default: $GITHUB_ORG)"
  echo "  -r REGISTRY_TYPE  Registry type to use (ghcr or gcr, default: $REGISTRY_TYPE)"
  echo "  -g GCR_REPO_URL   GCR repository URL (default: $GCR_REPO_URL, only used when REGISTRY_TYPE is gcr)"
  echo "  -h                Display this help message"
  echo ""
  echo "Environment Variables:"
  echo "  TEMPORAL_CHART_VERSION  Temporal Helm chart version to use (default: $DEFAULT_CHART_VERSION)"
  echo "                          This can be overridden by the -v option"
  exit 1
}

# Parse command line arguments
while getopts "v:o:r:g:h" opt; do
  case $opt in
    v) CHART_VERSION="$OPTARG" ;;
    o) GITHUB_ORG="$OPTARG" ;;
    r) REGISTRY_TYPE="$OPTARG" ;;
    g) GCR_REPO_URL="$OPTARG" ;;
    h) usage ;;
    *) usage ;;
  esac
done

# Validate registry type
if [ "$REGISTRY_TYPE" != "ghcr" ] && [ "$REGISTRY_TYPE" != "gcr" ]; then
  echo "Error: Invalid registry type (-r). Must be 'ghcr' or 'gcr'"
  usage
fi

echo "=== Temporal Helm Chart Generator ==="
echo "Chart Version: $CHART_VERSION"
echo "GitHub Organization: $GITHUB_ORG"
echo "Chart Directory: $CHART_DIR"
echo "Registry Type: $REGISTRY_TYPE"
if [ "$REGISTRY_TYPE" = "gcr" ]; then
  echo "GCR Repository URL: $GCR_REPO_URL"
fi
echo ""

# Pull the Temporal Helm chart
echo "Pulling Temporal Helm chart version $CHART_VERSION..."
if [ -d "$TEMP_DIR" ]; then
  echo "Removing existing temporary directory..."
  rm -rf "$TEMP_DIR"
fi
helm pull temporal/temporal --version "$CHART_VERSION" --untar --destination /tmp

# Clear the destination directory
echo "Clearing destination directory: $CHART_DIR"
rm -rf "${CHART_DIR:?}/"*

# Copy the chart to the destination
echo "Copying chart to destination..."
cp -r "$TEMP_DIR"/* "$CHART_DIR"

# Remove unnecessary files
echo "Removing unnecessary files..."
rm -rf "$CHART_DIR/ci"
rm -rf "$CHART_DIR/values"
rm "$CHART_DIR/Chart.lock"

echo "Deleting unwanted subchart  dependencies. like prometheus, grafana, cassandra, etc..."
rm -rf "$CHART_DIR/charts/"
yq e 'del(.dependencies)' -i "$CHART_DIR/Chart.yaml"

# Function to recursively process Chart.yaml files
process_chart_yaml() {
  local chart_dir="$1"
  echo "Processing Chart.yaml in $chart_dir"

  # Process the main Chart.yaml file if it exists
  if [ -f "$chart_dir/Chart.yaml" ]; then
    echo "Deleting specified sections from $chart_dir/Chart.yaml"
    yq e 'del(.home)' -i "$chart_dir/Chart.yaml"
    yq e 'del(.icon)' -i "$chart_dir/Chart.yaml"
    yq e 'del(.maintainers)' -i "$chart_dir/Chart.yaml"
    yq e 'del(.sources)' -i "$chart_dir/Chart.yaml"
    yq e 'del(.annotations)' -i "$chart_dir/Chart.yaml"

    # Only delete dependencies from prometheus Chart.yaml
    if [[ "$chart_dir" == *"/prometheus" ]]; then
      yq e 'del(.dependencies)' -i "$chart_dir/Chart.yaml"
    fi
  fi
}

# Delete specified sections from Chart.yaml files recursively
echo "Deleting specified sections from Chart.yaml files recursively..."
process_chart_yaml "$CHART_DIR"

# Update image repositories in values.yaml
echo "Updating image repositories in values.yaml..."

# Determine registry URL based on registry type
REGISTRY_URL=""
if [ "$REGISTRY_TYPE" = "ghcr" ]; then
  REGISTRY_URL="ghcr.io/$GITHUB_ORG"
elif [ "$REGISTRY_TYPE" = "gcr" ]; then
  REGISTRY_URL="$GCR_REPO_URL/$GITHUB_ORG"
fi
echo "Registry URL: $REGISTRY_URL"
yq e ".server.image.repository = \"$REGISTRY_URL/temporalio-server\"" -i "$CHART_DIR/values.yaml"
yq e ".admintools.image.repository = \"$REGISTRY_URL/temporalio-admin-tools\"" -i "$CHART_DIR/values.yaml"
yq e ".web.image.repository = \"$REGISTRY_URL/temporalio-ui\"" -i "$CHART_DIR/values.yaml"

# Clean up
echo "Cleaning up temporary files..."
rm -rf "$TEMP_DIR"

echo ""
echo "=== Summary ==="
echo "Temporal Helm chart version $CHART_VERSION has been generated and customized"
echo "Chart location: $CHART_DIR"
if [ "$REGISTRY_TYPE" = "ghcr" ]; then
  echo "Image repositories have been updated to use ghcr.io/$GITHUB_ORG/"
elif [ "$REGISTRY_TYPE" = "gcr" ]; then
  echo "Image repositories have been updated to use $GCR_REPO_URL/$GITHUB_ORG/"
fi

echo "Validate the generated chart using the following command:"
echo "helm template ./kubernetes/temporal -f ./scripts/temporal/gcr_values.yaml "

echo ""
echo "Done!"
