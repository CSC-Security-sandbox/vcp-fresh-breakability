### VSA Control Plane

This repo hosts all the code for the VSA Control Plane. The VSA Control Plane is a set of microservices that are responsible for managing the lifecycle of a VSA.

### Code Layout
The code is organized into the following directories:
```aiignore
.
├── clients - This directory contains the client libraries for the VSA Control Plane.
│   ├── core-api 
│   └── ontap-rest 
├── common -    This directory contains common code that is shared across the VSA Control Plane.
├── config -   This directory contains the configuration files for the VSA Control Plane.
├── core -   This directory contains the code for the Core API (Hyperscaler Agnostic).
│   ├── core-api -   This directory contains the core API code.
│   ├── datastores -   This directory contains the code for the datastores ( Database Connectivity and Persistence).
│   ├── kubernetes -   This directory contains the Kubernetes manifests for the Core API.
│   ├── models -   This directory contains the models for the Core API.
│   └── server -   This directory contains the server code for the Core API.
├── firestore-emulator -  This directory contains the code for the Firestore Emulator.
│   └── kubernetes -  
├── google-proxy -  This directory contains the code for the Google Proxy.
│   └── kubernetes
│   └── api - This directory contains the code for the Google Proxy API.
├── spanner-emulator -  This directory contains the code for the Spanner Emulator.
│   └── kubernetes 
├── tools - This directory contains all the external tools used in the repo. For ex Swagger Generator
├── workflow-executor -  This directory contains the code for the Workflow Executor.
│   ├── starter 
│   └── worker 
└── workflow-engine - This directory contains the code for the workflow client integration.
```

### How to Run VSA Using Skaffold Locally (Minikube Cluster)

#### Prerequisites.
* [Install minikube](https://minikube.sigs.k8s.io/docs/start).
* [Install helm](https://helm.sh/docs/intro/install/).
* [Install Docker](https://docs.docker.com/get-docker/).
* [Install Skaffold](https://skaffold.dev/docs/install/).
* [Install Github CLI](https://cli.github.com)

#### Steps

##### 1. Update Environment Variables in Google Proxy Deployment File

Modify the `google-proxy/kubernetes/deployment.yaml` and `worker/kubernetes/deployment.yaml` file to include the following environment variables:

```yaml
- name: VSA_NODE_PASSWORD
  value: <vsa_node_password_here>
- name: VSA_NODE_USERNAME
  value: <vsa_node_username_here>
```

##### 2. Run Mock Metadata Server
After starting Skaffold, ensure the mock metadata server is running.

```
go run tools/mock-metadata-server/app.go
```

##### 3. Start a minikube cluster.

```
minikube start
```

##### 4. Run Skaffold
Run the following command to start Skaffold:

```
export GHVSA_PAT=$(gh auth token)
skaffold dev
```

This will build and deploy all the services to your local Kubernetes cluster. Once deployed, you can access the services using the following URLs:

- Google Proxy: http://localhost:9000
- Core Service: http://localhost:9001
- Postgres: http://localhost:5432
- Local Temporal Web: http://localhost:8080
- Workflow Server: http://localhost:9003

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
