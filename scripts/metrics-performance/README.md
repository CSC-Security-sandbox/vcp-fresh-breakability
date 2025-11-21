# Telemetry Performance Profiling Scripts

This directory contains scripts for profiling the telemetry service, capturing performance data, and uploading profiles to Google Cloud Storage. These tools help analyze CPU usage, memory allocation, goroutine behavior, and other performance metrics.

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Scripts](#scripts)
- [Quick Start](#quick-start)
- [Environment Variables](#environment-variables)
- [Usage Examples](#usage-examples)
- [Docker Deployment](#docker-deployment)
- [Cloud Run Deployment](#cloud-run-deployment)
- [Troubleshooting](#troubleshooting)

## Overview

The profiling scripts provide a comprehensive solution for:

- **Starting the telemetry service** with pprof profiling enabled
- **Capturing performance profiles** (CPU, memory, goroutines, traces, etc.)
- **Generating test metrics** using the hydrated metrics generator
- **Uploading profiles** to Google Cloud Storage for analysis
- **Automating the entire profiling workflow** in Docker/Cloud Run environments

## Prerequisites

### Local Development

- **Go 1.24+** installed and available in PATH
- **Bash shell** (version 4.0+)
- **curl** for health checks and profile capture
- **go tool pprof** (included with Go) for profile analysis
- **gcloud CLI** and **gsutil** (for GCS uploads, optional)
- **Environment file** (`tel_local.env`) with database and service configuration

### Docker/Cloud Run

- **Docker** (for local Docker builds)
- **Google Cloud SDK** (for Cloud Run deployment)
- **GCS bucket** (for profile storage, optional)

### Required Binaries

The scripts expect these binaries to be available:

- `telemetry` - The telemetry service binary
- `generate_hydrated_metrics` - Metrics generator tool (optional, for test data)

Binaries can be:
- In system PATH
- At `/usr/local/bin/` (Docker containers)
- At `${PROJECT_ROOT}/bin/` (local builds)
- Specified via environment variables

## Scripts

### 1. `automated_profile.sh`

**Purpose**: Main automation script that orchestrates the entire profiling workflow.

**Features**:
- Starts telemetry service with profiling enabled
- Generates test metrics using existing resources
- Captures CPU, trace, and periodic profiles
- Uploads profiles to GCS (if configured)
- Handles cleanup and error recovery

**Usage**:
```bash
./automated_profile.sh
```

**Key Functions**:
- `start_telemetry_background()` - Starts service in background
- `generate_metrics_for_past_hour()` - Generates test metrics
- `capture_cpu_profile_async()` - Captures CPU profile
- `capture_trace_profile_async()` - Captures trace profile
- `capture_periodic_profiles_async()` - Captures periodic profiles
- `upload_profiles_to_gcs()` - Uploads to GCS

### 2. `start_telemetry_with_profiling.sh`

**Purpose**: Standalone script to start telemetry service with profiling enabled and capture profiles manually.

**Usage**:
```bash
# Start the service
./start_telemetry_with_profiling.sh start

# Capture CPU profile (30 seconds)
./start_telemetry_with_profiling.sh cpu 30

# Capture memory profile
./start_telemetry_with_profiling.sh memory

# Capture all profiles
./start_telemetry_with_profiling.sh all 30

# Open pprof web UI
./start_telemetry_with_profiling.sh ui heap
```

**Commands**:
- `start` - Start the telemetry service
- `stop` - Stop the telemetry service
- `cpu <duration>` - Capture CPU profile
- `memory` - Capture memory/heap profile
- `all <duration>` - Capture all profile types
- `ui <profile>` - Open pprof web UI

### 3. `run_profiling_and_upload.sh`

**Purpose**: Complete profiling workflow script that starts service, captures profiles, and uploads to GCS.

**Usage**:
```bash
# With GCS upload
GCS_BUCKET=my-bucket ./run_profiling_and_upload.sh

# Without GCS upload
UPLOAD_TO_GCS=false ./run_profiling_and_upload.sh
```

**Workflow**:
1. Starts telemetry service with profiling
2. Waits for service to be ready
3. Optionally generates test metrics
4. Triggers usage endpoint
5. Captures profiling data
6. Uploads profiles to GCS
7. Cleans up and stops service

### 4. `upload_profiles_to_gcs.sh`

**Purpose**: Standalone script to upload existing profiles to Google Cloud Storage.

**Usage**:
```bash
GCS_BUCKET=my-bucket-name ./upload_profiles_to_gcs.sh
```

**Features**:
- Uploads all profiles from `PROFILE_DIR`
- Supports custom GCS prefix
- Validates GCS bucket access
- Provides upload progress

### 5. `deploy-cloud-run.sh`

**Purpose**: Builds Docker image and deploys profiling service to Google Cloud Run.

**Usage**:
```bash
PROJECT_ID=my-project-id \
GCS_BUCKET=my-bucket-name \
./deploy-cloud-run.sh
```

**Features**:
- Builds Docker image using `Dockerfile.profiling`
- Pushes to Google Container Registry
- Deploys to Cloud Run with proper configuration
- Sets up service account and permissions

### 6. `docker-compose.profiling.yml`

**Purpose**: Docker Compose configuration for easy local Docker deployment.

**Features**:
- Automated container build and run
- Volume mounts for profiles and environment files
- Environment variable configuration
- Port mapping for service and pprof UI
- Host gateway support for database access

**Usage**:
```bash
# Navigate to the project root (where go.mod is located)
cd /path/to/vsa-control-plane

# Set environment variables
export GCS_BUCKET=my-bucket-name
export GCS_PREFIX=profiles/

# Run with docker-compose (from project root)
docker-compose -f scripts/metrics-performance/docker-compose.profiling.yml up

# Or if running from scripts/metrics-performance directory:
# Update docker-compose.profiling.yml to set context: ../../
docker-compose -f docker-compose.profiling.yml up

# Run in detached mode
docker-compose -f scripts/metrics-performance/docker-compose.profiling.yml up -d

# View logs
docker-compose -f scripts/metrics-performance/docker-compose.profiling.yml logs -f

# Stop and remove
docker-compose -f scripts/metrics-performance/docker-compose.profiling.yml down
```

**Note**: The Dockerfile expects the project root as build context (where `go.mod` is located). When using docker-compose, either:
- Run from project root with full path: `docker-compose -f scripts/metrics-performance/docker-compose.profiling.yml up`
- Or update `docker-compose.profiling.yml` to set `context: ../../` if running from the scripts directory

**Configuration**:
- Automatically mounts `./profiles` directory
- Mounts `./tel_local.env` as read-only
- Supports GCP service account key mounting (optional)
- Configures `host.docker.internal` for host access
- Exposes ports 8080 (service) and 6060 (pprof)

### 7. `Dockerfile.profiling`

**Purpose**: Docker image definition for profiling environment.

**Features**:
- Multi-stage build for optimized image size
- Includes Go runtime with debug symbols
- Pre-built telemetry and metrics generator binaries
- All profiling scripts included
- Google Cloud SDK and gsutil pre-installed

**Build**:
```bash
docker buildx build \
  --platform linux/amd64 \
  --load \
  -f Dockerfile.profiling \
  -t telemetry-profiling:latest \
  .
```

### 8. `cloud-run-service.yaml`

**Purpose**: Kubernetes/Cloud Run service definition template.

**Configuration**:
- Resource limits (CPU: 2, Memory: 4Gi)
- Environment variables
- Port configuration
- Service account settings

## Quick Start

### Local Profiling

1. **Prepare environment**:
   ```bash
   export ENV_FILE=/path/to/tel_local.env
   export PROFILE_DIR=./profiles
   ```

2. **Start profiling**:
   ```bash
   ./automated_profile.sh
   ```

3. **View profiles**:
   ```bash
   go tool pprof -http=:6060 profiles/cpu_*.prof
   ```

### Docker Profiling

#### Option 1: Using Docker Compose (Recommended)

1. **Navigate to project root** (where `go.mod` is located):
   ```bash
   cd /path/to/vsa-control-plane
   ```

2. **Set environment variables**:
   ```bash
   export GCS_BUCKET=my-bucket-name
   export GCS_PREFIX=profiles/
   ```

3. **Run with docker-compose**:
   ```bash
   docker-compose -f scripts/metrics-performance/docker-compose.profiling.yml up
   ```

   This will automatically:
   - Build the Docker image
   - Mount volumes for profiles and environment file
   - Configure all environment variables
   - Expose ports 8080 and 6060

   **Note**: If running from `scripts/metrics-performance` directory, update `docker-compose.profiling.yml` to set `context: ../../` in the build section.

#### Option 2: Manual Docker Build

1. **Build Docker image**:
   ```bash
   docker buildx build \
     --platform linux/amd64 \
     --load \
     -f Dockerfile.profiling \
     -t telemetry-profiling:latest \
     .
   ```

2. **Run container**:
   ```bash
   docker run -it --rm \
     -v $(pwd)/profiles:/profiles \
     -v $(pwd)/tel_local.env:/workspace/tel_local.env \
     -e GCS_BUCKET=my-bucket \
     -e UPLOAD_TO_GCS=true \
     telemetry-profiling:latest
   ```

### Cloud Run Deployment

1. **Set environment variables**:
   ```bash
   export PROJECT_ID=my-project-id
   export GCS_BUCKET=my-bucket-name
   export REGION=us-central1
   ```

2. **Deploy**:
   ```bash
   ./deploy-cloud-run.sh
   ```

3. **Invoke service**:
   ```bash
   gcloud run services invoke telemetry-profiling \
     --region=$REGION \
     --project=$PROJECT_ID
   ```

## Environment Variables

### Service Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `METRICS_PORT` | `8080` | Port for telemetry service |
| `METRICS_SERVER_PORT` | `8080` | Alternative name for metrics port |
| `PPROF_PORT` | `6060` | Port for pprof web UI |
| `ENV_FILE` | `tel_local.env` | Path to environment configuration file |
| `ENABLE_PPROF` | `true` | Enable pprof endpoints |
| `MOCK_GOOGLE_METRICS` | `true` | Mock Google Cloud metrics (don't send) |

### Binary Paths

| Variable | Default | Description |
|----------|---------|-------------|
| `TELEMETRY_BINARY` | Auto-detected | Path to telemetry binary |
| `METRICS_GENERATOR_BINARY` | Auto-detected | Path to metrics generator binary |

### Profile Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `PROFILE_DIR` | `./profiles` | Directory for profile files |
| `CPU_PROFILE_DURATION` | `50` | CPU profile duration (seconds) |
| `TRACE_PROFILE_DURATION` | `10` | Trace profile duration (seconds) |
| `PERIODIC_PROFILE_DURATION` | `50` | Periodic profile capture duration (seconds) |
| `PERIODIC_PROFILE_INTERVAL` | `15` | Interval between periodic captures (seconds) |
| `PROFILE_DURATION` | `120` | Overall profiling duration (seconds) |

### GCS Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `GCS_BUCKET` | (required) | GCS bucket name for profile uploads |
| `GCS_PREFIX` | `profiles/` | Prefix for GCS object names |
| `UPLOAD_TO_GCS` | `true` | Enable/disable GCS uploads |

### Docker Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `DOCKER_HOST_IP` | `host.docker.internal` | Host IP for Docker container access |
| `PROJECT_ROOT` | Auto-detected | Project root directory |

### Workflow Control

| Variable | Default | Description |
|----------|---------|-------------|
| `GENERATE_METRICS` | `true` | Generate test metrics before profiling |
| `TRIGGER_USAGE_METRICS` | `true` | Trigger usage endpoint |
| `WAIT_MAX_ATTEMPTS` | `60` | Max attempts to wait for service readiness |
| `CORRELATION_ID` | Auto-generated | Correlation ID for API requests |

## Usage Examples

### Example 1: Basic Local Profiling

```bash
# Set environment
export ENV_FILE=./tel_local.env
export PROFILE_DIR=./profiles

# Run automated profiling
./automated_profile.sh

# Analyze CPU profile
go tool pprof -http=:6060 profiles/cpu_*.prof
```

### Example 2: Custom Profile Durations

```bash
CPU_PROFILE_DURATION=60 \
TRACE_PROFILE_DURATION=20 \
PERIODIC_PROFILE_INTERVAL=10 \
./automated_profile.sh
```

### Example 3: Manual Profile Capture

```bash
# Terminal 1: Start service
./start_telemetry_with_profiling.sh start

# Terminal 2: Capture profiles
./start_telemetry_with_profiling.sh cpu 60
./start_telemetry_with_profiling.sh memory
./start_telemetry_with_profiling.sh trace 10

# Terminal 3: View in web UI
./start_telemetry_with_profiling.sh ui cpu_*.prof
```

### Example 4: Profile and Upload to GCS

```bash
GCS_BUCKET=my-telemetry-profiles \
GCS_PREFIX=profiles/2024/01/ \
./run_profiling_and_upload.sh
```

### Example 5: Docker Compose (Easiest)

```bash
# Navigate to project root
cd /path/to/vsa-control-plane

# Set environment variables
export GCS_BUCKET=my-bucket
export CPU_PROFILE_DURATION=120
export PERIODIC_PROFILE_INTERVAL=30
export GENERATE_METRICS=true

# Run with docker-compose (from project root)
docker-compose -f scripts/metrics-performance/docker-compose.profiling.yml up

# Or run in background
docker-compose -f scripts/metrics-performance/docker-compose.profiling.yml up -d

# View logs
docker-compose -f scripts/metrics-performance/docker-compose.profiling.yml logs -f

# Stop container
docker-compose -f scripts/metrics-performance/docker-compose.profiling.yml down
```

### Example 6: Docker with Custom Configuration

```bash
docker run -it --rm \
  -v $(pwd)/profiles:/profiles \
  -v $(pwd)/tel_local.env:/workspace/tel_local.env \
  -e GCS_BUCKET=my-bucket \
  -e CPU_PROFILE_DURATION=120 \
  -e PERIODIC_PROFILE_INTERVAL=30 \
  -e UPLOAD_TO_GCS=true \
  telemetry-profiling:latest
```

### Example 7: Cloud Run Deployment

```bash
# Set variables
export PROJECT_ID=my-gcp-project
export GCS_BUCKET=telemetry-profiles
export REGION=us-central1
export SERVICE_NAME=telemetry-profiling

# Deploy
./deploy-cloud-run.sh

# Invoke
gcloud run services invoke $SERVICE_NAME \
  --region=$REGION \
  --project=$PROJECT_ID
```

## Docker Deployment

### Using Docker Compose (Recommended)

The easiest way to run profiling in Docker is using `docker-compose.profiling.yml`:

```bash
# Navigate to project root (where go.mod is located)
cd /path/to/vsa-control-plane

# Set required environment variables
export GCS_BUCKET=my-bucket-name
export GCS_PREFIX=profiles/

# Optional: Set custom configuration
export CPU_PROFILE_DURATION=120
export PERIODIC_PROFILE_INTERVAL=30
export GENERATE_METRICS=true

# Run the container (from project root)
docker-compose -f scripts/metrics-performance/docker-compose.profiling.yml up
```

**Important**: The Dockerfile expects the project root as build context. Always run docker-compose from the project root directory, or update `docker-compose.profiling.yml` to set `context: ../../` if running from the scripts directory.

**Docker Compose Features**:
- Automatic image building
- Volume mounts for profiles and environment files
- Environment variable configuration
- Port mapping (8080 for service, 6060 for pprof)
- Host gateway support for database access
- Optional GCP service account key mounting

**Docker Compose Commands**:
```bash
# Start in foreground
docker-compose -f docker-compose.profiling.yml up

# Start in background
docker-compose -f docker-compose.profiling.yml up -d

# View logs
docker-compose -f docker-compose.profiling.yml logs -f

# Stop container
docker-compose -f docker-compose.profiling.yml down

# Rebuild image
docker-compose -f docker-compose.profiling.yml build --no-cache
```

### Manual Docker Build

If you prefer manual Docker commands, you can build and run the image directly:

#### Building the Image

The `Dockerfile.profiling` builds a complete profiling environment:

```bash
docker buildx build \
  --platform linux/amd64 \
  --load \
  -f Dockerfile.profiling \
  -t telemetry-profiling:latest \
  .
```

#### Running the Container

```bash
docker run -it --rm \
  -v $(pwd)/profiles:/profiles \
  -v $(pwd)/tel_local.env:/workspace/tel_local.env \
  -e GCS_BUCKET=my-bucket \
  -e UPLOAD_TO_GCS=true \
  -p 8080:8080 \
  -p 6060:6060 \
  telemetry-profiling:latest
```

### Container Environment

The Docker image includes:
- Go runtime with debug symbols
- Telemetry binary (built with `-gcflags="all=-N -l"`)
- Metrics generator binary
- All profiling scripts
- Google Cloud SDK and gsutil
- Required system tools (curl, netstat, etc.)

### Docker Compose Configuration

The `docker-compose.profiling.yml` file provides:
- **Volume Mounts**:
  - `./profiles:/profiles` - Profile output directory
  - `./tel_local.env:/workspace/tel_local.env:ro` - Environment configuration (read-only)
  - Optional: `./gcp-key.json:/gcp-key.json:ro` - GCP service account key

- **Environment Variables**:
  - All profiling configuration variables
  - GCS bucket and prefix settings
  - Database host overrides for Docker networking

- **Port Mapping**:
  - `8080:8080` - Telemetry service
  - `6060:6060` - pprof web UI

- **Host Access**:
  - Configures `host.docker.internal` for accessing host services (databases, etc.)

## Cloud Run Deployment

### Prerequisites

1. **GCP Project** with billing enabled
2. **Service Account** with permissions:
   - Cloud Run Admin
   - Storage Admin (for GCS)
   - Service Account User
3. **GCS Bucket** for profile storage

### Deployment Steps

1. **Update `cloud-run-service.yaml`**:
   - Replace `PROJECT_ID` with your project ID
   - Update service account name
   - Configure GCS bucket name

2. **Deploy**:
   ```bash
   ./deploy-cloud-run.sh
   ```

3. **Verify**:
   ```bash
   gcloud run services describe telemetry-profiling \
     --region=$REGION \
     --project=$PROJECT_ID
   ```

### Cloud Run Configuration

- **CPU**: 2 cores
- **Memory**: 4Gi
- **Timeout**: 3600s (1 hour)
- **Concurrency**: 1 (single request at a time)
- **Min Instances**: 0 (scale to zero)
- **Max Instances**: 1

## Troubleshooting

### Service Won't Start

**Problem**: Port already in use
```bash
# Check what's using the port
lsof -i :8080
# or
netstat -tuln | grep 8080

# Kill the process or use a different port
export METRICS_PORT=9090
```

### Profiles Not Generated

**Problem**: Service not ready
```bash
# Check service logs
tail -f profiles/telemetry.log

# Check health endpoint
curl http://localhost:8080/health

# Increase wait time
export WAIT_MAX_ATTEMPTS=120
```

### GCS Upload Fails

**Problem**: Authentication or permissions
```bash
# Authenticate with GCP
gcloud auth login
gcloud auth application-default login

# Check bucket permissions
gsutil ls gs://$GCS_BUCKET

# Test upload manually
echo "test" | gsutil cp - gs://$GCS_BUCKET/test.txt
```

### Binary Not Found

**Problem**: Binary path incorrect
```bash
# Check if binary exists
which telemetry
which generate_hydrated_metrics

# Set explicit paths
export TELEMETRY_BINARY=/path/to/telemetry
export METRICS_GENERATOR_BINARY=/path/to/generate_hydrated_metrics
```

### Docker Container Issues

**Problem**: Database connection from container
```bash
# Set Docker host IP
export DOCKER_HOST_IP=host.docker.internal

# Or use host network (Linux only)
docker run --network=host ...
```

### Profile Analysis Issues

**Problem**: Cannot open profile in pprof
```bash
# Check profile file exists and is valid
file profiles/cpu_*.prof

# Try text output first
go tool pprof profiles/cpu_*.prof

# Check Go version compatibility
go version
```

## Profile Types

The scripts capture various profile types:

- **CPU Profile** (`cpu_*.prof`): CPU usage over time
- **Memory Profile** (`heap_*.prof`): Heap memory allocation
- **Goroutine Profile** (`goroutine_*.prof`): Goroutine stack traces
- **Block Profile** (`block_*.prof`): Blocking operations
- **Mutex Profile** (`mutex_*.prof`): Mutex contention
- **Allocs Profile** (`allocs_*.prof`): Memory allocations
- **Trace Profile** (`trace_*.out`): Detailed execution trace

## Analyzing Profiles

### Using pprof Web UI

```bash
go tool pprof -http=:6060 profiles/cpu_*.prof
```

Then open `http://localhost:6060` in your browser.

### Using pprof Command Line

```bash
# Top functions by CPU time
go tool pprof -top profiles/cpu_*.prof

# Graph view
go tool pprof -png profiles/cpu_*.prof > cpu_graph.png

# List functions
go tool pprof -list=function_name profiles/cpu_*.prof
```

### Analyzing Traces

```bash
go tool trace profiles/trace_*.out
```

Opens a web UI at `http://127.0.0.1:xxxxx` for trace analysis.

## Best Practices

1. **Profile Duration**: Use longer durations (60+ seconds) for accurate results
2. **Multiple Runs**: Capture multiple profiles to identify patterns
3. **Resource Limits**: Ensure adequate CPU/memory for profiling
4. **Clean Environment**: Start with a clean state for consistent results
5. **GCS Organization**: Use date-based prefixes for profile organization
6. **Retention**: Set GCS lifecycle policies to manage storage costs

## Additional Resources

- [Go pprof Documentation](https://golang.org/pkg/net/http/pprof/)
- [Go Tool pprof Guide](https://github.com/google/pprof/blob/main/doc/README.md)
- [Cloud Run Documentation](https://cloud.google.com/run/docs)
- [GCS Documentation](https://cloud.google.com/storage/docs)

## Support

For issues or questions:
1. Check the troubleshooting section
2. Review service logs in `PROFILE_DIR/telemetry.log`
3. Verify environment variables are set correctly
4. Check GCP service account permissions

