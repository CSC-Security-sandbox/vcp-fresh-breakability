# Compile and Deploy to Minikube

This guide provides step-by-step instructions for compiling and deploying the VSA Control Plane to a local minikube cluster.

## Prerequisites

Install the following tools before starting:

1. **minikube** - [Installation guide](https://minikube.sigs.k8s.io/docs/start)
2. **helm** - [Installation guide](https://helm.sh/docs/intro/install/)
3. **Docker** - [Installation guide](https://docs.docker.com/get-docker/)
4. **Skaffold** - [Installation guide](https://skaffold.dev/docs/install/)
5. **GitHub CLI** - [Installation guide](https://cli.github.com)
6. **Make** - Usually pre-installed on macOS/Linux

Verify installations:
```bash
minikube version
helm version
docker --version
skaffold version
gh --version
make --version
```

## Step-by-Step Deployment

### Step 1: Start Minikube Cluster

```bash
minikube start
```

Wait for minikube to be ready. You can verify with:
```bash
kubectl get nodes
```

### Step 2: Set Environment Variables

Set required environment variables for authentication and configuration:

```bash
# GitHub Personal Access Token (required for pulling images from GHCR)
export GHVSA_PAT=$(gh auth token)
# Or manually set:
# export GHVSA_PAT=<your_github_pat>

# Database passwords (optional - not currently used by postgres.yaml)
export DB_PASSWORD=testpass
export DB_ADMIN_PASSWORD=testpass

# ONTAP node password (required for VSA operations)
export VSA_NODE_PASSWORD=<password-to-be-set-on-ontap>
```

**Note**: All non-authentication related environment variables are in `skaffold.env` and can be customized there.

### Step 3: Build Binaries

Compile all Go binaries with debug symbols enabled:

```bash
make build-all-binaries-dev
```

This command:
- Builds a Docker image with the Go build environment
- Compiles the following binaries with debug symbols (`-gcflags="all=-N -l"`):
  - `app/vcp-worker` - Worker service
  - `app/google-proxy` - Google Cloud Proxy
  - `app/core` - Core API service
  - `app/telemetry` - Telemetry service
  - `app/ontap-proxy` - ONTAP Proxy service

The binaries are placed in the `app/` directory.

### Step 4: Deploy to Minikube

Deploy all services to minikube using Skaffold:

```bash
make skaffold-dev
```

Or run both build and deploy in one command:
```bash
make build-all-binaries-dev skaffold-dev
```

**What happens during deployment:**
1. Skaffold loads environment variables from `skaffold.env`
2. Builds Docker images for all services using the binaries from `app/`
3. Deploys the following to minikube:
   - **PostgreSQL database** (StatefulSet in `default` namespace)
   - **Temporal** (via Helm chart in `default` namespace)
   - **Core API** (Deployment in `vcp` namespace)
   - **Google Proxy** (Deployment in `vcp` namespace)
   - **ONTAP Proxy** (Deployment in `vcp` namespace)
   - **VCP Worker** (Deployment in `vcp` namespace)
   - **Telemetry** (Deployment in `vcp` namespace)
   - **Harvest Farm** (Deployment in `vcp` namespace)
   - **Metrics Processor** (Deployment in `vcp` namespace)
4. Sets up port-forwarding for local access
5. Runs database migrations automatically (`RUN_MIGRATION_ON_START=true`)

### Step 5: Verify Deployment

Check that all pods are running:

```bash
# Check pods in vcp namespace
kubectl get pods -n vcp

# Check pods in default namespace (postgres, temporal)
kubectl get pods -n default

# Check all services
kubectl get svc -A
```

Wait for all pods to be in `Running` state. This may take a few minutes.

### Step 6: Access Services

Once deployed, services are accessible via port-forwarding:

- **Google Proxy**: http://localhost:9000
- **Core API**: http://localhost:9001
- **PostgreSQL**: `localhost:5432` (use any PostgreSQL client)
  - Database: `vcp`
  - User: `postgres`
  - Password: `testpass`
- **Temporal Web UI**: http://localhost:8080
- **Harvest Farm**: http://localhost:3000
- **Metrics Processor**: http://localhost:9090
- **ONTAP Proxy**: http://localhost:9003

Test database connection:
```bash
psql -h localhost -p 5432 -U postgres -d vcp
# Password: testpass
```

## Development Workflow

### Live Reloading

Skaffold automatically watches for code changes and rebuilds/redeploys services. This is enabled by default when using `skaffold-dev`.

To disable watch mode:
```bash
skaffold dev --no-watch
# or
skaffold dev --watch=false
```

### Debugging

Run Skaffold in debug mode to attach a debugger:

```bash
skaffold debug
```

Debug ports (when using `skaffold debug`):
- Core API: http://localhost:56268
- Google Proxy: http://localhost:56269
- Workflow Server: http://localhost:56270

### Viewing Logs

View logs from any service:

```bash
# Core API logs
kubectl logs -f deployment/core -n vcp

# Worker logs
kubectl logs -f deployment/vcp-worker -n vcp

# Google Proxy logs
kubectl logs -f deployment/google-proxy -n vcp

# All pods in vcp namespace
kubectl logs -f -l app=core -n vcp
```

## Troubleshooting

### Error: Duplicate key constraint violation

If you see database errors like:
```
pq: duplicate key value violates unique constraint "cluster_metadata_info_pkey"
```

**Solution**: Clean and reset minikube:

```bash
minikube delete
minikube start
make build-all-binaries-dev skaffold-dev
```

### Error: Not enough hosts to serve the request

**Solution**: Reset minikube cluster (see above).

### Pods stuck in CrashLoopBackOff

1. Check pod logs:
   ```bash
   kubectl logs <pod-name> -n <namespace>
   ```

2. Check pod events:
   ```bash
   kubectl describe pod <pod-name> -n <namespace>
   ```

3. Common issues:
   - Missing environment variables (check `skaffold.env`)
   - Database connection issues (ensure postgres pod is running)
   - Image pull errors (verify `GHVSA_PAT` is set correctly)

### Database Connection Issues

1. Verify postgres is running:
   ```bash
   kubectl get pods -n default | grep postgres
   ```

2. Check postgres logs:
   ```bash
   kubectl logs -f statefulset/postgres -n default
   ```

3. Test connection from within cluster:
   ```bash
   kubectl run -it --rm psql-test --image=postgres:14 --restart=Never -- \
     psql -h postgres.default.svc.cluster.local -U postgres -d vcp
   # Password: testpass
   ```

### Rebuilding After Code Changes

If you make code changes:

1. **With watch enabled** (default): Skaffold automatically rebuilds and redeploys
2. **Without watch**: Rebuild and redeploy manually:
   ```bash
   make build-all-binaries-dev
   # Then restart skaffold or manually update deployments
   ```

## Cleanup

To stop and remove everything:

```bash
# Stop skaffold (Ctrl+C if running in foreground)

# Delete minikube cluster
minikube delete

# Or just delete the namespace (keeps minikube running)
kubectl delete namespace vcp
```

## Additional Resources

- [Main README](../README.md) - General project information
- [Getting Started Guide](getting-started.md) - First-time setup guide
- [Skaffold Documentation](https://skaffold.dev/docs/) - Skaffold reference
- [Minikube Documentation](https://minikube.sigs.k8s.io/docs/) - Minikube reference
