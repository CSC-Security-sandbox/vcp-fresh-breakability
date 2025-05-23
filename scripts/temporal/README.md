# Temporal Scripts and Helm Chart Management

This directory contains scripts for managing Temporal Helm charts and Docker images.

## Values Files

This directory contains two values files for different container registries:

- **gcr_values.yaml**: Configuration for Google Container Registry (GCR)
- **ghcr_values.yaml**: Configuration for GitHub Container Registry (GHCR)

Choose the appropriate values file based on which container registry you're using for your deployment.

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

# Generate Temporal Helm chart for gcr
./generate-temporal-helm.sh -r gcr -g europe-north1-docker.pkg.dev -o gcnv-artifact-registry-nonprod/temporal-container-images -v 0.60.0
```

### mirror-images.sh

**Purpose**: Detects, pulls, and pushes Temporal Docker images to a private container registry (GHCR or GCR).

**Usage**:
```bash
./mirror-images.sh -o GITHUB_ORG [-t GITHUB_PAT] [-v VALUES_FILE] [-c CHART_VERSION] [-r REGISTRY_TYPE] [-g GCR_REPO_URL] [-d]
```

**Parameters**:
- `-o GITHUB_ORG`: GitHub organization or username (required)
- `-t GITHUB_PAT`: GitHub Personal Access Token (if not provided, uses GITHUB_PAT env var)
- `-v VALUES_FILE`: Path to values file (default: ../charts/temporal/values.yaml). Use either gcr_values.yaml or ghcr_values.yaml based on your target registry.
- `-c CHART_VERSION`: Temporal Helm chart version to use (default: 0.60.0)
- `-r REGISTRY_TYPE`: Registry type to use (ghcr or gcr, default: ghcr)
- `-g GCR_REPO_URL`: GCR repository URL (default: gcr.io, only used when REGISTRY_TYPE is gcr)
- `-d`: Dry run (don't push images)
- `-h`: Display help message

**Environment Variables**:
- `TEMPORAL_CHART_VERSION`: Temporal Helm chart version to use (can be overridden by the -c option)

**Example**:
```bash
# Mirror images to GitHub Container Registry (default)
./mirror-images.sh -o vcp-vsa-control-plane -t your-github-pat

# Mirror images to Google Container Registry (default gcr.io)
./mirror-images.sh -r gcr -o vcp-vsa-control-plane

# Mirror images to a specific Google Container Registry
./mirror-images.sh -r gcr -g europe-north1-docker.pkg.dev -o gcnv-artifact-registry-nonprod/temporal-container-images

# Dry run to see what would be mirrored without pushing
./mirror-images.sh -o vcp-vsa-control-plane -d

# Mirror images with specific chart version
./mirror-images.sh -o vcp-vsa-control-plane -c 0.61.0
```

### publish-chart.sh

**Purpose**: Packages and publishes the Temporal Helm chart to GitHub Container Registry or GCP Artifact Repository.

**Usage**:
```bash
./publish-chart.sh -o GITHUB_ORG [-t GITHUB_PAT] [-r REGISTRY_TYPE] [-g GCR_REPO_URL] [-d]
```

**Parameters**:
- `-o GITHUB_ORG`: GitHub organization or username (required)
- `-t GITHUB_PAT`: GitHub Personal Access Token (if not provided, uses GITHUB_PAT env var)
- `-r REGISTRY_TYPE`: Registry type to use (ghcr or gcr, default: ghcr)
- `-g GCR_REPO_URL`: GCP artifact repository URL (default: gcr.io, only used when REGISTRY_TYPE is gcr)
- `-d`: Dry run (don't push chart)
- `-h`: Display help message

**Example**:
```bash
# Publish chart to GitHub Container Registry (default)
./publish-chart.sh -o vcp-vsa-control-plane -t your-github-pat

# Publish chart to GCP Artifact Repository
./publish-chart.sh -r gcr -g europe-north1-docker.pkg.dev -o gcnv-artifact-registry-nonprod/temporal-helm-chart

# Dry run to see what would be published without pushing
./publish-chart.sh -o vcp-vsa-control-plane -d
```

## Installing/Upgrading with Helm

### Create an imagePull Secret in temporal namespace
```bash
export TEMPORAL_K8S_NAMESPACE=temporal
kubectl create secret docker-registry temporal-image-pull-secret \
  --namespace $TEMPORAL_K8S_NAMESPACE
  --docker-server=<registry-url> \
  --docker-username=<username> \
  --docker-password=<password> \
  --docker-email=<email>
```

**Note:** Before running helm install, make sure to update these if needed `imagePullSecrets`, `host`, `port`, and `password` values in the appropriate values file (`gcr_values.yaml` or `ghcr_values.yaml`) to match your environment configuration.

```bash
# Set required environment variables
export TEMPORAL_RELEASE_NAME=test-release
export TEMPORAL_K8S_NAMESPACE=temporal
export VSA_TEMPORAL_HELM_CHART_VERSION=0.60.0

# For GitHub Container Registry (GHCR)
export VSA_TEMPORAL_HELM_CHART_REPO=oci://ghcr.io/vcp-vsa-control-plane/temporal
helm upgrade --install $TEMPORAL_RELEASE_NAME $VSA_TEMPORAL_HELM_CHART_REPO --version $VSA_TEMPORAL_HELM_CHART_VERSION \
    --namespace $TEMPORAL_K8S_NAMESPACE \
    -f ./ghcr_values.yaml

# For Google Container Registry (GCR)
# export VSA_TEMPORAL_HELM_CHART_REPO=oci://europe-north1-docker.pkg.dev/gcnv-artifact-registry-nonprod/temporal-container-images/vcp-helm-chart
# helm upgrade --install $TEMPORAL_RELEASE_NAME $VSA_TEMPORAL_HELM_CHART_REPO --version $VSA_TEMPORAL_HELM_CHART_VERSION \
#     --namespace $TEMPORAL_K8S_NAMESPACE \
#     -f ./gcr_values.yaml
```

## Checking Rendered Helm Chart

Preview the rendered manifests without installing:

```bash
export TEMPORAL_RELEASE_NAME=test-release
export TEMPORAL_K8S_NAMESPACE=temporal
export VSA_TEMPORAL_HELM_CHART_VERSION=0.60.0

# For GitHub Container Registry (GHCR)
export VSA_TEMPORAL_HELM_CHART_REPO=oci://ghcr.io/vcp-vsa-control-plane/temporal
helm template $TEMPORAL_RELEASE_NAME $VSA_TEMPORAL_HELM_CHART_REPO --version $VSA_TEMPORAL_HELM_CHART_VERSION \
    --namespace $TEMPORAL_K8S_NAMESPACE \
    -f ./ghcr_values.yaml

# For Google Container Registry (GCR)
# export VSA_TEMPORAL_HELM_CHART_REPO=oci://gcr.io/your-project/temporal
# helm template $TEMPORAL_RELEASE_NAME $VSA_TEMPORAL_HELM_CHART_REPO --version $VSA_TEMPORAL_HELM_CHART_VERSION \
#     --namespace $TEMPORAL_K8S_NAMESPACE \
#     -f ./gcr_values.yaml

# For GCP Artifact Repository (example)
# export VSA_TEMPORAL_HELM_CHART_REPO=oci://europe-north1-docker.pkg.dev/gcnv-artifact-registry-nonprod/temporal-container-images/vcp-helm-chart
# helm template $TEMPORAL_RELEASE_NAME $VSA_TEMPORAL_HELM_CHART_REPO --version $VSA_TEMPORAL_HELM_CHART_VERSION \
#     --namespace $TEMPORAL_K8S_NAMESPACE \
#     -f ./gcr_values.yaml
```
