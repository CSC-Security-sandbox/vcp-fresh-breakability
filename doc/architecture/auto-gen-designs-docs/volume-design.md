# Volume Management Lifecycle Management Workflow Design

> 🤖 **Note**: This document was automatically generated using AI-enhanced analysis of the VSA Control Plane codebase.

## Table of Contents
1. [Overview](#overview)
2. [Data Model](#data-model)
3. [API Operations](#api-operations)
4. [Architecture Components](#architecture-components)
5. [Workflow Architecture](#workflow-architecture)

## Overview


The Volume Management workflow provides foundational infrastructure for the VSA Control Plane.

### Key Features
- **Feature Type**: Foundational
- **Workflow Type**: Management
- **Complexity**: Complex
- **Operations**: 26 API operations


## Data Model

### Entity Relationship Diagram - Volume_Management Data Structure

```mermaid
---
title: Volume_Management Entity Relationships and Dependencies
---
erDiagram
    VolumeManagement {
        int64 ID PK
        string UUID UK
        string Name
        string Description
        string State
        string StateDetails
        jsonb Attributes
        int64 AccountID FK
        timestamp CreatedAt
        timestamp UpdatedAt
        timestamp DeletedAt
    }
    
    Account {
        int64 ID PK
        string UUID UK
        string Name
        string Description
        string ProjectNumber
        string LocationID
    }
    
    Volume ||--o| Pool : belongs_to  
    Volume ||--o{ Snapshot : has_snapshots
    Volume ||--o{ VolumeReplication : source_replications
    Volume ||--o{ VolumeReplication : destination_replications
    Volume ||--o| Account : belongs_to
    
    Pool {
        int64 ID PK
        string UUID UK
        string Name
    }
    
    Snapshot {
        int64 ID PK
        string UUID UK
        string Name
        int64 VolumeID FK
    }
    
    VolumeReplication {
        int64 ID PK
        string UUID UK
        int64 SourceVolumeID FK
        int64 DestinationVolumeID FK
    }
```

### Domain Model Architecture

```mermaid
classDiagram
    class BaseModel {
        +int64 ID
        +string UUID
        +timestamp CreatedAt
        +timestamp UpdatedAt  
        +timestamp DeletedAt
        +Save() error
        +Delete() error
    }
    
    class VolumeManagement {
        +string Name
        +string Description
        +string State
        +jsonb Attributes
        +int64 AccountID
        +ValidateState() error
        +UpdateState(state) error
        +GetAttributes() map
    }
    
    class Account {
        +string Name
        +string ProjectNumber
        +string LocationID
        +GetResources() []Resource
    }
    
    BaseModel <|-- VolumeManagement
    BaseModel <|-- Account
    Account "1" *-- "*" VolumeManagement
```

### Core Attributes

| Field | Type | Description |
|-------|------|-------------|
| **ID** | `int64` | Primary key identifier |
| **UUID** | `string` | Universally unique identifier |
| **Name** | `string` | Human-readable resource name |
| **State** | `string` | Current lifecycle state |
| **Attributes** | `jsonb` | Resource-specific configuration |
| **AccountID** | `int64` | Associated account reference |



## API Operations

### Discovered Operations

Found 26 operations for Volume Management:

| Operation | Service |
|-----------|----------|
| get___v1beta_projects_projectNumber_locations_locationId_volumes_volumeId | google-proxy |
| put___v1beta_projects_projectNumber_locations_locationId_volumes_volumeId | google-proxy |
| delete___v1beta_projects_projectNumber_locations_locationId_volumes_volumeId | google-proxy |
| get___v1beta_projects_projectNumber_locations_locationId_volumes | google-proxy |
| post___v1beta_projects_projectNumber_locations_locationId_volumes | google-proxy |
| post___v1beta_projects_projectNumber_locations_locationId_getMultipleVolumes | google-proxy |
| post___v1beta_projects_projectNumber_locations_locationId_volumes_volumeResourceId_establishPeering | google-proxy |
| post___v1beta_projects_projectNumber_locations_locationId_volumes_volumeResourceId_replications | google-proxy |
| post___v1beta_projects_projectNumber_locations_locationId_volumes_volumeResourceId_getMultipleReplications | google-proxy |
| put___v1beta_projects_projectNumber_locations_locationId_volumes_volumeResourceId_replications_replicationResourceId | google-proxy |


## Architecture Components




### Communication Flow Diagram

The following diagram illustrates the API communication flow for Volume Management operations discovered from the API specifications:

```mermaid
sequenceDiagram
    participant Client
    participant GoogleProxy as google-proxy (Region A)
    participant CoreAPI as Core API
    participant VCPWorker as vcp-worker (Region A)
    participant Temporal
    participant Database as PostgreSQL
    participant VSA as ONTAP/VSA

    Note over Client, VSA: Volume Management API Operations Flow

    Note over Client: Operation 1: post___v1beta_projects_projectNumber_locations_locationId_volumes
    Client->>GoogleProxy: POST /v1beta/projects/{id}/locations/{id}/volumes
    GoogleProxy->>CoreAPI: Validate & Route Request
    CoreAPI->>Database: Query Volume Management
    Database-->>CoreAPI: Volume Management Data
    CoreAPI->>GoogleProxy: Volume Management Response
    GoogleProxy->>Client: 200 OK + Data

    Note over Client: Operation 2: get___v1beta_projects_projectNumber_locations_locationId_volumes_volumeId
    Client->>GoogleProxy: GET /v1beta/projects/{id}/locations/{id}/volumes/{id}
    GoogleProxy->>CoreAPI: Validate & Route Request
    CoreAPI->>Database: Query Volume Management
    Database-->>CoreAPI: Volume Management Data
    CoreAPI->>GoogleProxy: Volume Management Response
    GoogleProxy->>Client: 200 OK + Data

    Note over Client: Operation 3: put___v1beta_projects_projectNumber_locations_locationId_volumes_volumeId
    Client->>GoogleProxy: PUT /v1beta/projects/{id}/locations/{id}/volumes/{id}
    GoogleProxy->>CoreAPI: Validate & Route Request
    CoreAPI->>Database: Query Volume Management
    Database-->>CoreAPI: Volume Management Data
    CoreAPI->>GoogleProxy: Volume Management Response
    GoogleProxy->>Client: 200 OK + Data

    Note over Client: Operation 4: delete___v1beta_projects_projectNumber_locations_locationId_volumes_volumeId
    Client->>GoogleProxy: DELETE /v1beta/projects/{id}/locations/{id}/volumes/{id}
    GoogleProxy->>CoreAPI: Validate & Route Request
    CoreAPI->>VCPWorker: Delete Volume Management Workflow
    VCPWorker->>Temporal: Start Workflow
    Temporal->>Database: Persist State (CREATING)
    
    Note over VCPWorker: Execute Delete Activities
    VCPWorker->>Database: Update Resource State
    VCPWorker->>VSA: Configure Volume Management
    VSA-->>VCPWorker: Configuration Complete
    
    VCPWorker->>Database: Update State (AVAILABLE)
    VCPWorker->>Temporal: Workflow Complete
    Temporal->>CoreAPI: Operation Result
    CoreAPI->>GoogleProxy: 202 Operation (LRO)
    GoogleProxy->>Client: Operation ID for polling
```

**Key Components:**
- **Client**: External API consumer (gcloud, terraform, custom applications)
- **google-proxy**: Regional API gateway handling authentication and routing
- **Core API**: Business logic and orchestration layer
- **vcp-worker**: Temporal workflow worker executing background operations
- **Temporal**: Durable workflow engine managing long-running operations
- **PostgreSQL**: Persistent data store for resource state
- **ONTAP/VSA**: NetApp storage cluster for data plane operations

**Operation Types:**
- **Create**: 12 operation(s) - post___v1beta_projects_projectNumber_locations_locationId_volumes, post___v1beta_projects_projectNumber_locations_locationId_getMultipleVolumes, post___v1beta_projects_projectNumber_locations_locationId_volumes_volumeResourceId_establishPeering (+9 more)
- **Update**: 4 operation(s) - put___v1beta_projects_projectNumber_locations_locationId_volumes_volumeId, put___v1beta_projects_projectNumber_locations_locationId_volumes_volumeResourceId_replications_replicationResourceId, put___v1beta_internal_projects_projectNumber_locations_locationId_volumes_volumeId (+1 more)
- **Delete**: 4 operation(s) - delete___v1beta_projects_projectNumber_locations_locationId_volumes_volumeId, delete___v1beta_projects_projectNumber_locations_locationId_volumes_volumeResourceId_replications_replicationResourceId, delete___v1beta_internal_projects_projectNumber_locations_locationId_volumes_volumeId_snapmirrorSnapshots (+1 more)
- **Get**: 6 operation(s) - get___v1beta_projects_projectNumber_locations_locationId_volumes_volumeId, get___v1beta_projects_projectNumber_locations_locationId_volumes, get___v1beta_internal_projects_projectNumber_locations_locationId_volumes_volumeId (+3 more)

**Total Operations**: 26 API endpoints for Volume Management


### System Context Diagram

```mermaid
C4Context
    title Volume Lifecycle and Data Management System

    Person(customer, "Storage Customer", "Requires storage management")
    System(gcnv, "Google Cloud NetApp Volumes", "Managed storage service")
    System(vcp, "VSA Control Plane", "Volume Management lifecycle management")
    System(gcp, "Google Cloud Platform", "Cloud infrastructure")
    System(ontap, "NetApp ONTAP", "Storage cluster software")

    Rel(customer, gcnv, "Manages volume management")
    Rel(gcnv, vcp, "Delegates operations")
    Rel(vcp, gcp, "Provisions infrastructure")
    Rel(vcp, ontap, "Configures storage")
```

## Workflow Architecture

### State Machine - Volume State Management and Lifecycle Workflow

```mermaid
---
title: Volume State Management and Lifecycle Workflow
---
stateDiagram-v2
    [*] --> Creating
    Creating --> Active: Success
    Creating --> Failed: Error
    Active --> Updating: Update Request
    Active --> Deleting: Delete Request
    Updating --> Active: Success
    Updating --> Failed: Error
    Deleting --> Deleted: Success
    Deleting --> Failed: Error
    Failed --> [*]
    Deleted --> [*]
```

### Error Handling Strategy

- **Temporal Retries**: Automatic retry with exponential backoff
- **Compensation Logic**: Rollback on failure using Temporal compensation
- **Error Categorization**: Custom error codes (see `lib/errors/`)
- **State Management**: PostgreSQL transactions ensure consistency

## Deployment Considerations

### Performance
- Workflow timeout configurations based on operation complexity
- Activity-level retries for transient failures
- Database connection pooling for high throughput

### Security
- IAM-based authentication for GCP operations
- Workload Identity for service account access
- Encrypted secrets in database
- Audit logging for all operations

### Monitoring
- Temporal workflow metrics
- Custom metrics via telemetry service
- GCP Cloud Monitoring integration
- Alert policies for failure scenarios

---
*Generated by VSA Control Plane Documentation Generator with AI Enhancement*
*Last Updated: 2025-10-12*
