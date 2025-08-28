# Read environment variables from the environment or prompt if not set
if [[ -z "$GCE_METADATA_HOST" ]]; then
  read -rp "Enter GCE_METADATA_HOST: " GCE_METADATA_HOST
fi
if [[ -z "$VSA_NODE_PASSWORD" ]]; then
  read -rsp "Enter VSA_NODE_PASSWORD: " VSA_NODE_PASSWORD; echo
fi
if [[ -z "$VSA_NODE_USERNAME" ]]; then
  read -rp "Enter VSA_NODE_USERNAME: " VSA_NODE_USERNAME
fi
if [[ -z "$LOCAL_REGION" ]]; then
  read -rp "Enter LOCAL_REGION: " LOCAL_REGION
fi

# If variables are still empty, exit with error
if [[ -z "$GCE_METADATA_HOST" || -z "$VSA_NODE_PASSWORD" || -z "$VSA_NODE_USERNAME" || -z "$LOCAL_REGION" ]]; then
  echo "One or more required environment variables are not set. Exiting."
  exit 1
fi

# Update environment variables in vcp-worker deployment
kubectl set env deployment/vcp-worker \
  GCE_METADATA_HOST="$GCE_METADATA_HOST" \
  LOCAL_REGION="$LOCAL_REGION" \
  VSA_NODE_PASSWORD="$VSA_NODE_PASSWORD" \
  VSA_NODE_USERNAME="$VSA_NODE_USERNAME" -n vcp

# Update environment variables in google-proxy deployment
kubectl set env deployment/google-proxy \
  GCE_METADATA_HOST="$GCE_METADATA_HOST" \
  LOCAL_REGION="$LOCAL_REGION" \
  VSA_NODE_PASSWORD="$VSA_NODE_PASSWORD" \
  VSA_NODE_USERNAME="$VSA_NODE_USERNAME" -n vcp


# Update environment variables in harvest-farm deployment
kubectl set env deployment/harvest-farm \
  GCE_METADATA_HOST="35.189.45.145:9090" \
  VSA_NODE_PASSWORD="$VSA_NODE_PASSWORD" \
  VSA_NODE_USERNAME="$VSA_NODE_USERNAME" -n vcp

# Wait for 15 seconds before updating deployments
echo "Waiting 15 seconds for deployments to be ready..."
sleep 15