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

# Update environment variables in vcp-worker deployment
kubectl set env deployment/vcp-worker \
  GCE_METADATA_HOST="$GCE_METADATA_HOST" \
  VSA_NODE_PASSWORD="$VSA_NODE_PASSWORD" \
  VSA_NODE_USERNAME="$VSA_NODE_USERNAME" -n vcp

# Update environment variables in google-proxy deployment
kubectl set env deployment/google-proxy \
  GCE_METADATA_HOST="$GCE_METADATA_HOST" \
  VSA_NODE_PASSWORD="$VSA_NODE_PASSWORD" \
  VSA_NODE_USERNAME="$VSA_NODE_USERNAME" -n vcp

# Wait for vcp-worker pod to be running
kubectl wait --for=condition=Ready pod -l app=vcp-worker -n vcp --timeout=180s

# Wait for google-proxy pod to be running
kubectl wait --for=condition=Ready pod -l app=google-proxy -n vcp --timeout=180s


