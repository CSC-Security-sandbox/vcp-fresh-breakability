# Hostgroup Management Lifecycle Management Workflow Design

> 🤖 **Note**: This document was automatically generated using AI-enhanced analysis of the VSA Control Plane codebase.

## Table of Contents
1. [Overview](#overview)
2. [Data Model](#data-model)
3. [API Operations](#api-operations)
4. [Architecture Components](#architecture-components)
5. [Workflow Architecture](#workflow-architecture)

## Overview


The Hostgroup Management workflow provides foundational infrastructure for the VSA Control Plane.

### Key Features
- **Feature Type**: Foundational
- **Workflow Type**: Management
- **Complexity**: Moderate
- **Operations**: 6 API operations


## Data Model

### Entity Relationship Diagram - Hostgroup_Management Data Structure

```mermaid
---
title: Hostgroup_Management Entity Relationships and Dependencies
---
erDiagram
    HostgroupManagement {
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
    
    Hostgroup_Management ||--o| Account : belongs_to
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
    
    class HostgroupManagement {
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
    
    BaseModel <|-- HostgroupManagement
    BaseModel <|-- Account
    Account "1" *-- "*" HostgroupManagement
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

Found 6 operations for Hostgroup Management:

| Operation | Service |
|-----------|----------|
| get___v1beta_projects_projectNumber_locations_locationId_hostGroups | google-proxy |
| post___v1beta_projects_projectNumber_locations_locationId_hostGroups | google-proxy |
| get___v1beta_projects_projectNumber_locations_locationId_hostGroups_hostGroupId | google-proxy |
| put___v1beta_projects_projectNumber_locations_locationId_hostGroups_hostGroupId | google-proxy |
| delete___v1beta_projects_projectNumber_locations_locationId_hostGroups_hostGroupId | google-proxy |
| post___v1beta_projects_projectNumber_locations_locationId_getMultipleHostGroups | google-proxy |


## Architecture Components




### Communication Flow Diagram

The following diagram illustrates the API communication flow for Hostgroup Management operations discovered from the API specifications:

```mermaid
sequenceDiagram
    participant Client
    participant GoogleProxy as google-proxy (Region A)
    participant CoreAPI as Core API
    participant VCPWorker as vcp-worker (Region A)
    participant Temporal
    participant Database as PostgreSQL
    participant VSA as ONTAP/VSA

    Note over Client, VSA: Hostgroup Management API Operations Flow

    Note over Client: Operation 1: post___v1beta_projects_projectNumber_locations_locationId_hostGroups
    Client->>GoogleProxy: POST /v1beta/projects/{id}/locations/{id}/hostGroups
    GoogleProxy->>CoreAPI: Validate & Route Request
    CoreAPI->>Database: Query Hostgroup Management
    Database-->>CoreAPI: Hostgroup Management Data
    CoreAPI->>GoogleProxy: Hostgroup Management Response
    GoogleProxy->>Client: 200 OK + Data

    Note over Client: Operation 2: get___v1beta_projects_projectNumber_locations_locationId_hostGroups
    Client->>GoogleProxy: GET /v1beta/projects/{id}/locations/{id}/hostGroups
    GoogleProxy->>CoreAPI: Validate & Route Request
    CoreAPI->>Database: Query Hostgroup Management
    Database-->>CoreAPI: Hostgroup Management Data
    CoreAPI->>GoogleProxy: Hostgroup Management Response
    GoogleProxy->>Client: 200 OK + Data

    Note over Client: Operation 3: put___v1beta_projects_projectNumber_locations_locationId_hostGroups_hostGroupId
    Client->>GoogleProxy: PUT /v1beta/projects/{id}/locations/{id}/hostGroups/{id}
    GoogleProxy->>CoreAPI: Validate & Route Request
    CoreAPI->>Database: Query Hostgroup Management
    Database-->>CoreAPI: Hostgroup Management Data
    CoreAPI->>GoogleProxy: Hostgroup Management Response
    GoogleProxy->>Client: 200 OK + Data

    Note over Client: Operation 4: delete___v1beta_projects_projectNumber_locations_locationId_hostGroups_hostGroupId
    Client->>GoogleProxy: DELETE /v1beta/projects/{id}/locations/{id}/hostGroups/{id}
    GoogleProxy->>CoreAPI: Validate & Route Request
    CoreAPI->>VCPWorker: Delete Hostgroup Management Workflow
    VCPWorker->>Temporal: Start Workflow
    Temporal->>Database: Persist State (CREATING)
    
    Note over VCPWorker: Execute Delete Activities
    VCPWorker->>Database: Update Resource State
    VCPWorker->>VSA: Configure Hostgroup Management
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
- **Create**: 2 operation(s) - post___v1beta_projects_projectNumber_locations_locationId_hostGroups, post___v1beta_projects_projectNumber_locations_locationId_getMultipleHostGroups
- **Update**: 1 operation(s) - put___v1beta_projects_projectNumber_locations_locationId_hostGroups_hostGroupId
- **Delete**: 1 operation(s) - delete___v1beta_projects_projectNumber_locations_locationId_hostGroups_hostGroupId
- **Get**: 2 operation(s) - get___v1beta_projects_projectNumber_locations_locationId_hostGroups, get___v1beta_projects_projectNumber_locations_locationId_hostGroups_hostGroupId

**Total Operations**: 6 API endpoints for Hostgroup Management


### System Context Diagram

```mermaid
C4Context
    title Host Group Access Control and Management System

    Person(customer, "Storage Customer", "Requires storage management")
    System(gcnv, "Google Cloud NetApp Volumes", "Managed storage service")
    System(vcp, "VSA Control Plane", "Hostgroup Management lifecycle management")
    System(gcp, "Google Cloud Platform", "Cloud infrastructure")
    System(ontap, "NetApp ONTAP", "Storage cluster software")

    Rel(customer, gcnv, "Manages hostgroup management")
    Rel(gcnv, vcp, "Delegates operations")
    Rel(vcp, gcp, "Provisions infrastructure")
    Rel(vcp, ontap, "Configures storage")
```

## Workflow Architecture

### State Machine - Host Group Management States

```mermaid
---
title: Host Group Management States
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
