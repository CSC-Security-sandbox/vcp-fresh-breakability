### VSA Control Plane

This repo hosts all the code for the VSA Control Plane. The VSA Control Plane is a set of microservices that are responsible for managing the lifecycle of a VSA.

### Getting Started
This project uses Skaffold to manage the development and deployment of the VSA Control Plane. To get started, you will need to install Skaffold and Docker.

#### Install Skaffold
To install Skaffold, follow the instructions in the [Skaffold documentation](https://skaffold.dev/docs/install/).

#### Install Docker
To install Docker, follow the instructions in the [Docker documentation](https://docs.docker.com/get-docker/).

#### Code Layout
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
└── workflow-server - This directory contains the code for the Workflow Server.
    ├── config
    └── kubernetes
```
#### Running the code

From this directory, run

```bash
skaffold dev
```
All the services will be built and deployed to your local Kubernetes cluster. You can access the services using the following URLs:

Port forwarding deployment/core-api in namespace , remote port http -> 
Port forwarding deployment/google-proxy in namespace , remote port 8080 -> http://127.0.0.1:9000

- Core API: http://127.0.0.1:9001
- Google Proxy: http://127.0.0.1:9000
- Spanner Emulator: http://localhost:9002
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

### Running Temporal Service Locally

Prerequisites: 
- Install Helm

Deploy Services:
```bash
skaffold dev
```
This will deploy "google-proxy", "core-service", "postgres", and Temporal service in the "default" namespace of your local cluster.

#### Handling Errors: 
- pq: duplicate key value violates unique constraint "cluster_metadata_info_pkey"
- Not enough hosts to serve the request

Solution: Clean the local cluster. 

If using Minikube, reset it:

```bash
minikube delete
```

```bash
minikube start
```

Then, redeploy:

```bash
skaffold dev
```