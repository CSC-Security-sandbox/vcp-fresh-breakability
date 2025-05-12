# Temporal Scripts and Helm Chart Management

This directory contains scripts for managing Temporal Helm charts and Docker images.

## Scripts Overview

### generate-temporal-helm.sh

**Purpose**: Generates a customized Temporal Helm chart for the project by pulling the official Temporal Helm chart, customizing it, and storing it in the project's charts directory.

**Usage**:
```bash
./generate-temporal-helm.sh [-v CHART_VERSION] [-o GITHUB_ORG]
```

**Parameters**:
- `-v CHART_VERSION`: Temporal Helm chart version to use (default: 0.60.0)
- `-o GITHUB_ORG`: GitHub organization or username for image repositories (default: vcp-vsa-control-plane)
- `-h`: Display help message

**Environment Variables**:
- `TEMPORAL_CHART_VERSION`: Temporal Helm chart version to use (can be overridden by the -v option)

**Example**:
```bash
# Generate Temporal Helm chart with default settings
./generate-temporal-helm.sh

# Generate Temporal Helm chart with specific version
./generate-temporal-helm.sh -v 0.61.0

# Generate Temporal Helm chart for a different GitHub organization
./generate-temporal-helm.sh -o my-organization
```

### mirror-images.sh

**Purpose**: Detects, pulls, and pushes Temporal Docker images to a private GitHub Container Registry.

**Usage**:
```bash
./mirror-images.sh -o GITHUB_ORG [-t GITHUB_PAT] [-v VALUES_FILE] [-c CHART_VERSION] [-d]
```

**Parameters**:
- `-o GITHUB_ORG`: GitHub organization or username (required)
- `-t GITHUB_PAT`: GitHub Personal Access Token (if not provided, uses GITHUB_PAT env var)
- `-v VALUES_FILE`: Path to values.yaml file (default: ../charts/temporal/values.yaml)
- `-c CHART_VERSION`: Temporal Helm chart version to use (default: 0.60.0)
- `-d`: Dry run (don't push images)
- `-h`: Display help message

**Environment Variables**:
- `TEMPORAL_CHART_VERSION`: Temporal Helm chart version to use (can be overridden by the -c option)

**Example**:
```bash
# Mirror images to GitHub Container Registry
./mirror-images.sh -o vcp-vsa-control-plane -t your-github-pat

# Dry run to see what would be mirrored without pushing
./mirror-images.sh -o vcp-vsa-control-plane -d

# Mirror images with specific chart version
./mirror-images.sh -o vcp-vsa-control-plane -c 0.61.0
```

### publish-chart.sh

**Purpose**: Packages and publishes the Temporal Helm chart to GitHub Container Registry.

**Usage**:
```bash
./publish-chart.sh -o GITHUB_ORG [-t GITHUB_PAT] [-d]
```

**Parameters**:
- `-o GITHUB_ORG`: GitHub organization or username (required)
- `-t GITHUB_PAT`: GitHub Personal Access Token (if not provided, uses GITHUB_PAT env var)
- `-d`: Dry run (don't push chart)
- `-h`: Display help message

**Example**:
```bash
# Publish chart to GitHub Container Registry
./publish-chart.sh -o vcp-vsa-control-plane -t your-github-pat

# Dry run to see what would be published without pushing
./publish-chart.sh -o vcp-vsa-control-plane -d
```

## Installing/Upgrading with Helm

**Note:** Before running helm install, make sure to update the `host`, `port`, and `password` values in the `./scripts/temporal/values.yaml` file to match your environment configuration.

```bash
# Set required environment variables
export TEMPORAL_RELEASE_NAME=test-release
export TEMPORAL_K8S_NAMESPACE=temporal
export VSA_TEMPORAL_HELM_CHART_REPO=oci://ghcr.io/vcp-vsa-control-plane/temporal
export VSA_TEMPORAL_HELM_CHART_VERSION=0.60.0

# Install/upgrade using Helm
helm upgrade --install $TEMPORAL_RELEASE_NAME $VSA_TEMPORAL_HELM_CHART_REPO --version $VSA_TEMPORAL_HELM_CHART_VERSION \
    --namespace $TEMPORAL_K8S_NAMESPACE \
    -f ./values.yaml
```

## Checking Rendered Helm Chart

Preview the rendered manifests without installing:

```bash
export TEMPORAL_RELEASE_NAME=test-release
export TEMPORAL_K8S_NAMESPACE=temporal
export VSA_TEMPORAL_HELM_CHART_REPO=oci://ghcr.io/vcp-vsa-control-plane/temporal
export VSA_TEMPORAL_HELM_CHART_VERSION=0.60.0

# Run helm template to see the rendered manifests
helm template $TEMPORAL_RELEASE_NAME $VSA_TEMPORAL_HELM_CHART_REPO --version $VSA_TEMPORAL_HELM_CHART_VERSION \
    --namespace $TEMPORAL_K8S_NAMESPACE \
    -f ./values.yaml
```
