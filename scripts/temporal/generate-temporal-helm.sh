#!/bin/bash
# Script to generate a customized Temporal Helm chart for the project
# This script pulls the official Temporal Helm chart, customizes it, and stores it in the project's charts directory

set -e

# Default values
DEFAULT_CHART_VERSION="0.60.0"
CHART_VERSION="${TEMPORAL_CHART_VERSION:-$DEFAULT_CHART_VERSION}"
GITHUB_ORG="vcp-vsa-control-plane"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART_DIR="$(cd "${SCRIPT_DIR}/../../kubernetes/charts/temporal" && pwd)"
TEMP_DIR="/tmp/temporal"

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
  echo "Usage: $0 [-v CHART_VERSION] [-o GITHUB_ORG]"
  echo ""
  echo "Options:"
  echo "  -v CHART_VERSION  Temporal Helm chart version to use (default: $CHART_VERSION)"
  echo "  -o GITHUB_ORG     GitHub organization or username for image repositories (default: $GITHUB_ORG)"
  echo "  -h                Display this help message"
  echo ""
  echo "Environment Variables:"
  echo "  TEMPORAL_CHART_VERSION  Temporal Helm chart version to use (default: $DEFAULT_CHART_VERSION)"
  echo "                          This can be overridden by the -v option"
  exit 1
}

# Parse command line arguments
while getopts "v:o:h" opt; do
  case $opt in
    v) CHART_VERSION="$OPTARG" ;;
    o) GITHUB_ORG="$OPTARG" ;;
    h) usage ;;
    *) usage ;;
  esac
done

echo "=== Temporal Helm Chart Generator ==="
echo "Chart Version: $CHART_VERSION"
echo "GitHub Organization: $GITHUB_ORG"
echo "Chart Directory: $CHART_DIR"
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

# Update repository values in Chart.yaml
echo "Updating repository values in Chart.yaml..."
yq e '.dependencies[].repository = "file://dev/null"' -i "$CHART_DIR/Chart.yaml"

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

  # Recursively process all subdirectories that might contain charts
  if [ -d "$chart_dir/charts" ]; then
    for subchart in "$chart_dir/charts"/*; do
      if [ -d "$subchart" ]; then
        process_chart_yaml "$subchart"
      fi
    done
  fi
}

# Delete specified sections from Chart.yaml files recursively
echo "Deleting specified sections from Chart.yaml files recursively..."
process_chart_yaml "$CHART_DIR"

# Update image repositories in values.yaml
echo "Updating image repositories in values.yaml..."
yq e ".server.image.repository = \"ghcr.io/$GITHUB_ORG/temporalio-server\"" -i "$CHART_DIR/values.yaml"
yq e ".admintools.image.repository = \"ghcr.io/$GITHUB_ORG/temporalio-admin-tools\"" -i "$CHART_DIR/values.yaml"
yq e ".web.image.repository = \"ghcr.io/$GITHUB_ORG/temporalio-ui\"" -i "$CHART_DIR/values.yaml"

# Clean up
echo "Cleaning up temporary files..."
rm -rf "$TEMP_DIR"

echo ""
echo "=== Summary ==="
echo "Temporal Helm chart version $CHART_VERSION has been generated and customized"
echo "Chart location: $CHART_DIR"
echo "Image repositories have been updated to use ghcr.io/$GITHUB_ORG/"

echo ""
echo "Done!"
