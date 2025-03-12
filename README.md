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
│   ├── core-api -  Client library for the Core API.
│   └── ontap-rest - Client library for the ONTAP REST API.
├── common - This directory contains common code that is shared across the VSA Control Plane.
├── config - This directory contains the configuration files for the VSA Control Plane.
├── core-api - This directory contains the code for the Core API (Hyperscaler Agnostic).
│   ├── api - This directory contains the API code.
│   ├── datastores - This directory contains the code for the datastores ( Database Connectivity and Persistence).
│   ├── kubernetes - This directory contains the Kubernetes manifests for the Core API.
│   ├── models - This directory contains the models for the Core API.
│   └── server - This directory contains the server code for the Core API.
├── firestore-emulator - This directory contains the code for the Firestore Emulator.
│   └── kubernetes - This directory contains the Kubernetes manifests for the Firestore Emulator.
├── google-proxy - This directory contains the code for the Google Proxy.
│   └── kubernetes - This directory contains the Kubernetes manifests for the Google Proxy.
├── spanner-emulator - This directory contains the code for the Spanner Emulator.
│   └── kubernetes - This directory contains the Kubernetes manifests for the Spanner Emulator.
├── tools - This directory contains all the external tools used in the repo. For ex Swagger Generator
├── workflow-executor - This directory contains the code for the Workflow Executor.
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

Port forwarding pod/core-api-644cc45c78-9xhrl in namespace default, remote port 56268 -> http://127.0.0.1:56268
Forwarding container google-proxy-6b97d8566b-9gzf2/google-proxy to local port 56269.
Port forwarding pod/google-proxy-6b97d8566b-9gzf2 in namespace default, remote port 56268 -> http://127.0.0.1:56269
Forwarding container workflow-server-78b58796bc-r89z5/workflow-server to local port 56270.
Port forwarding pod/workflow-server-78b58796bc-r89z5 in namespace default, remote port 56268 -> http://127.0.0.1:56270

- Core API: http://localhost:56268
- Google Proxy: http://localhost:56269
- Workflow Server: http://localhost:56270