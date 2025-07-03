### VSA Control Plane

This repo hosts all the code for the VSA Control Plane. The VSA Control Plane is a set of microservices that are responsible for managing the lifecycle of a VSA.

### Code Layout
The code is organized into the following directories:

```
.
├── artifacts/              # Compiled binaries and build artifacts
├── builder/                # Dockerfiles and build scripts
├── checksums/              # Checksum files for various modules
├── cicd/                   # CI/CD scripts, Dockerfiles, and pipeline configs
├── clients/                # Client libraries for the VSA Control Plane
│   ├── core-api/
│   ├── cvp/
│   ├── google-proxy-client/
│   ├── hyperscaler/
│   ├── ontap-rest/
│   └── vlm/
├── common/                 # Shared/common code across services
├── config/                 # Configuration files (YAML, env, etc.)
├── core/                   # Core API (Hyperscaler Agnostic)
├── database/               # Database logic, mocks, and interfaces
├── doc/                    # Documentation and architecture diagrams
├── google-proxy/           # Google Proxy service and API
├── harvest-farm/           # Harvest farm logic and Kubernetes manifests
├── kubernetes/             # Helm charts and Kubernetes manifests
├── mocks/                  # Mock implementations for testing
├── poller/                 # Poller service and Operator
├── postgres/               # Postgres-related code and manifests
├── scripts/                # Utility scripts for code generation and verification
├── security/               # Network policies and security configs
├── telemetry/              # Telemetry service, API, and supporting code
├── tools/                  # External tools (Swagger, migration, etc.)
├── utils/                  # Utility functions and helpers
├── vsa_config/             # VSA configuration files and logic
├── worker/                 # Worker service and supporting code
└── workflow_engine/        # Workflow engine and Temporal client integration
```

### How to Run VSA Using Skaffold Locally (Minikube Cluster)

#### Prerequisites.
* [Install minikube](https://minikube.sigs.k8s.io/docs/start).
* [Install helm](https://helm.sh/docs/intro/install/).
* [Install Docker](https://docs.docker.com/get-docker/).
* [Install Skaffold](https://skaffold.dev/docs/install/).
* [Install Github CLI](https://cli.github.com)

#### Steps

##### 1. Start a minikube cluster.

```
minikube start
```

##### 2. Run Skaffold
Run the following command to start Skaffold:

```
export GHVSA_PAT=$(gh auth token)
export VSA_NODE_PASSWORD=<passwrod-to-be-set-on-ontap>
export VSA_NODE_USERNAME=<username-to-be-set-on-ontap>
export GCE_METADATA_HOST=<ip-of-remote-hosted-mock-server>

make build-all-binaries-dev skaffold-dev
```

This will build and deploy all the services to your local Kubernetes cluster. Once deployed, you can access the services using the following URLs:

- Google Proxy: http://127.0.0.1:9000
- Core Service: http://localhost:9001
- Postgres: http://127.0.0.1:5433
- Local Temporal Web: http://127.0.0.1:8080
- Workflow Server: http://localhost:9003
- Harvest Farm: http://127.0.0.1:3000
- Metrics Processor: http://127.0.0.1:9090

#### Debugging the code

```bash
skaffold debug
```
Once the services are deployed, you can attach a debugger to the services using the following URLs:

- Core API: http://localhost:56268
- Google Proxy: http://localhost:56269
- Workflow Server: http://localhost:56270

#### Live reloading the code

Skaffold will automatically watch for changes in the code and rebuild and redeploy the services. If this needs to be disabled, you can run the following command:

```bash 
    skaffold dev --no-watch
    # or
    skaffold dev --watch=false
```

#### Handling Errors

Error: pq: duplicate key value violates unique constraint "cluster_metadata_info_pkey"
Error: Not enough hosts to serve the request
Solution: Clean the local cluster.

If you are using Minikube, reset it by running the following commands:
```
minikube delete
minikube start
```

Then, redeploy:

```bash
export GHVSA_PAT=<your_github_pat>
skaffold dev
```

#### Linting locally
```bash
# Installing golangci-lint and vsacictl
brew install golangci-lint
brew upgrade golangci-lint
cd ./cicd
go build -o ~/go/bin/vsacictl .
cd ..

# Running vsacictl
vsacictl lint
```
