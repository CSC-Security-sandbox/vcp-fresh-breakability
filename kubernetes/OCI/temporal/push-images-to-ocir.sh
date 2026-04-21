#!/usr/bin/env bash
# Push Temporal images from Docker Hub to OCI Container Registry (OCIR).
# Optionally creates container repositories in a compartment if they do not exist (requires OCI CLI).
#
# Prereq: docker login <region>.ocir.io -u <tenancy-namespace>/<oci-username> -p <auth-token>
#   NOTE: <tenancy-namespace> is NOT the compartment name. Get it: oci os ns get
#
# Usage: ./push-images-to-ocir.sh [registry]
#   registry = region.ocir.io/<tenancy-namespace> (no trailing slash).
#
# Optional: set COMPARTMENT_OCID or COMPARTMENT_NAME to create repos if missing (avoids 403 on push).
#   export COMPARTMENT_OCID=ocid1.compartment.oc1..xxxxx
#   # or by name (OCI CLI required):
#   export COMPARTMENT_NAME=Images
#   ./push-images-to-ocir.sh iad.ocir.io/<tenancy-namespace>
#
# IAM: To create repos, your user/group needs in the target compartment, e.g.:
#   allow group <your-group> to manage repos in compartment Images
#   allow group <your-group> to read repos in compartment Images

set -e

# Registry: OCIR_REGISTRY env, then first arg, then default
REGISTRY="${OCIR_REGISTRY:-${1:-iad.ocir.io/idqogasfjw45}}"

# Resolve compartment: use COMPARTMENT_OCID if set; else look up by COMPARTMENT_NAME (default: Images)
if [[ -z "$COMPARTMENT_OCID" ]] && command -v oci &>/dev/null; then
  COMPARTMENT_NAME="${COMPARTMENT_NAME:-Images}"
  echo "Resolving compartment by name: $COMPARTMENT_NAME"
  COMPARTMENT_OCID=$(oci iam compartment list --compartment-id-in-subtree true --all \
    --query "data[?name=='$COMPARTMENT_NAME'].id | [0]" --raw-output 2>/dev/null || true)
  if [[ -z "$COMPARTMENT_OCID" || "$COMPARTMENT_OCID" == "None" || "$COMPARTMENT_OCID" == "null" ]]; then
    echo "Warning: Could not find compartment '$COMPARTMENT_NAME'. Set COMPARTMENT_OCID or create compartment."
    COMPARTMENT_OCID=""
  fi
fi

# Repositories to push (display-name in OCIR)
REPOS=(
  "temporalio/server"
  "temporalio/admin-tools"
  "temporalio/ui"
)

# Ensure a container repository exists in the given compartment (create if not).
# Requires: OCI CLI configured and COMPARTMENT_OCID set.
ensure_repo_exists() {
  local repo_name="$1"
  if [[ -z "$COMPARTMENT_OCID" ]]; then
    return 0
  fi
  local existing
  existing=$(oci artifacts container repository list \
    -c "$COMPARTMENT_OCID" \
    --display-name "$repo_name" \
    --lifecycle-state AVAILABLE \
    --query 'data[0].id' \
    --raw-output 2>/dev/null || true)
  if [[ -z "$existing" || "$existing" == "None" || "$existing" == "null" ]]; then
    echo "Creating container repository: $repo_name in compartment $COMPARTMENT_OCID"
    local err
    if ! err=$(oci artifacts container repository create \
      -c "$COMPARTMENT_OCID" \
      --display-name "$repo_name" \
      --wait-for-state AVAILABLE \
      --wait-interval-seconds 5 2>&1); then
      if echo "$err" | grep -qiE "not authorized|DENIED|Unauthorized"; then
        echo ""
        echo "ERROR: User not authorized to create container repositories in this compartment."
        echo ""
        echo "Ask your OCI administrator to add an IAM policy (Identity -> Policies), e.g.:"
        echo "  allow group <your-group-name> to manage repos in compartment Images"
        echo "  allow group <your-group-name> to read repos in compartment Images"
        echo ""
        echo "Alternatively, create the repositories manually in the Console:"
        echo "  Container Registry -> select Images compartment -> Create repository"
        echo "  Create: temporalio/server, temporalio/admin-tools, temporalio/ui"
        echo ""
        exit 1
      fi
      echo "Failed to create repository: $err"
      exit 1
    fi
    echo "Created: $repo_name"
  else
    echo "Repository already exists: $repo_name"
  fi
}

echo "Using registry: $REGISTRY"

# Create repos if COMPARTMENT_OCID is set and OCI CLI is available
if [[ -n "$COMPARTMENT_OCID" ]]; then
  if ! command -v oci &>/dev/null; then
    echo "Warning: COMPARTMENT_OCID is set but OCI CLI not found. Skipping repo creation."
  else
    echo "Ensuring container repositories exist in compartment..."
    for repo in "${REPOS[@]}"; do
      ensure_repo_exists "$repo"
    done
  fi
else
  echo "Tip: Set COMPARTMENT_NAME=Images or COMPARTMENT_OCID to auto-create repositories (avoids 403 Forbidden on push)."
fi

echo "Pulling from docker.io, tagging and pushing to $REGISTRY ..."

# Server
docker pull docker.io/temporalio/server:1.29.1
docker tag docker.io/temporalio/server:1.29.1 "$REGISTRY/temporalio/server:1.29.1"
docker push "$REGISTRY/temporalio/server:1.29.1"

# Admin tools
docker pull docker.io/temporalio/admin-tools:1.29
docker tag docker.io/temporalio/admin-tools:1.29 "$REGISTRY/temporalio/admin-tools:1.29"
docker push "$REGISTRY/temporalio/admin-tools:1.29"

# Web UI
docker pull docker.io/temporalio/ui:2.44.0
docker tag docker.io/temporalio/ui:2.44.0 "$REGISTRY/temporalio/ui:2.44.0"
docker push "$REGISTRY/temporalio/ui:2.44.0"

echo "Done. Images pushed to $REGISTRY"
