> Note: Below commands are to be executed from the root of the repository.

## Auth setup
```bash
# Set up gcloud for docker
gcloud auth configure-docker europe-north1-docker.pkg.dev

# Set GHVSA_PAT environment variable for private golang module access
## The pat token repo:read accees and authorized for VCP-VSA-control-Plane org.
export GHVSA_PAT=<github_personal_access_token>
````

## Image push to Google Artifact Registry
```bash
# Base image
docker buildx build \
-t europe-north1-docker.pkg.dev/gcnv-artifact-registry-nonprod/temporal-worker-container-images/base:0.0.1 \
--build-arg GHVSA_PAT=$GHVSA_PAT \
--platform linux/amd64 \
-f common/Dockerfile . --push

# vcp-worker image
docker buildx build \
-t europe-north1-docker.pkg.dev/gcnv-artifact-registry-nonprod/temporal-worker-container-images/vcp-worker:0.0.1 \
--build-arg GHVSA_PAT=$GHVSA_PAT \
--build-arg BASE=europe-north1-docker.pkg.dev/gcnv-artifact-registry-nonprod/temporal-worker-container-images/base:0.0.1 \
--platform linux/amd64 \
-f worker/Dockerfile . --push
```

## Edit `kubernetes/vcp-worker/values.yaml` of helm chart
Here we need to add image `tag` and `repository` of pushed container images in `values.yaml` file of helm chart.
```yaml
image:
  repository: "europe-north1-docker.pkg.dev/gcnv-artifact-registry-nonprod/temporal-worker-container-images/vcp-worker"
  tag: "0.0.1"

```

## Pushing helm chart to Google Artifact Registry
```bash
helm package ./kubernetes/vcp-worker
helm push ./vcp-worker-0.0.1.tgz oci://europe-north1-docker.pkg.dev/gcnv-artifact-registry-nonprod/temporal-worker-helm-chart
```