# Backup Vault Design and Architecture

## Table of Contents
1. [Overview](#overview)
2. [Architecture Components](#architecture-components)
3. [Data Model](#data-model)
4. [Backup Vault Types](#backup-vault-types)
5. [Lifecycle Management](#lifecycle-management)
6. [API Design](#api-design)
7. [Workflow Architecture](#workflow-architecture)
8. [Storage Integration](#storage-integration)

## Overview

A Backup Vault is a storage entity in the VSA Control Plane that manages backup data with configurable retention policies and immutable storage capabilities.

### Key Features
- **Storage Container**: Organizes backups by account and region
- **Retention Policy Management**: Configurable backup retention with immutable options
- **Cross-Region Support**: Supports both in-region and cross-region backup vaults
- **Bucket Management**: Integrates with GCP storage buckets for data storage
- **Lifecycle State Management**: State tracking for all operations
- **Volume Association**: Links to volumes for backup operations
- **Lazy VCP Registration**: VCP database entries are created only when volumes are attached to the vault

### Important Architectural Note
Backup vaults are created directly in SDE (Storage Data Engine) and VCP database entries are created **lazily** when a volume is first attached to the backup vault. This means:
- Initial backup vault creation only involves SDE
- VCP database entries are created during volume creation workflow

## Architecture Components

### High-Level Architecture

```mermaid
graph TB
    subgraph "Client Layer"
        API[Google Cloud API]
        UI[Web UI]
    end
    
    subgraph "VSA Control Plane"
        GP[Google Proxy]
        OR[Orchestrator]
        WF[Workflow Engine]
        AC[Activities]
    end
    
    subgraph "Storage Layer"
        VCP[(VCP Database)]
        SDE[SDE/CVP]
        GCP[GCP Buckets]
    end
    
    subgraph "External Services"
        ONTAP[ONTAP Storage]
        TEMPORAL[Temporal Workflows]
    end
    
    API --> GP
    UI --> GP
    GP --> OR
    OR --> WF
    WF --> AC
    AC --> VCP
    AC --> SDE
    AC --> GCP
    AC --> ONTAP
    WF --> TEMPORAL
```

### Component Responsibilities

| Component | Responsibility |
|-----------|----------------|
| **Google Proxy** | API endpoint handling, request validation, response formatting |
| **Orchestrator** | Business logic, state management, workflow coordination |
| **Workflow Engine** | Long-running operation orchestration using Temporal |
| **Activities** | Individual operation implementations (CRUD, validation) |
| **VCP Database** | Local state persistence and metadata storage |
| **SDE/CVP** | External backup vault management service |
| **GCP Buckets** | Actual backup data storage |

## Data Model

### Core Backup Vault Entity

```mermaid
erDiagram
    BackupVault {
        int64 ID PK
        string UUID UK
        string Name
        int64 AccountID FK
        string RegionName
        string BackupRegionName
        string SourceRegionName
        string LifeCycleState
        string LifeCycleStateDetails
        string BackupVaultType
        string AccountVendorID
        string Description
        jsonb ImmutableAttributes
        string CrossRegionBackupVaultName
        jsonb BucketDetails
        timestamp CreatedAt
        timestamp UpdatedAt
        timestamp DeletedAt
    }
    
    Account {
        int64 ID PK
        string UUID UK
        string Name
        string VendorID
    }
    
    Backup {
        int64 ID PK
        string UUID UK
        string Name
        string VolumeUUID FK
        int64 BackupVaultID FK
        string State
        int64 SizeInBytes
        string Type
        timestamp CreatedAt
    }
    
    Volume {
        int64 ID PK
        string UUID UK
        string Name
        int64 AccountID FK
        jsonb DataProtection
    }
    
    DataProtection {
        bool ScheduledBackupEnabled
        string BackupVaultID
        string BackupPolicyId
        int64 BackupChainBytes
    }
    
    BackupPolicy {
        int64 ID PK
        string UUID UK
        string Name
        int64 AccountID FK
        int64 DailyBackupsToKeep
        int64 WeeklyBackupsToKeep
        int64 MonthlyBackupsToKeep
        bool PolicyEnabled
    }
    
    BackupVault ||--o{ Backup : contains
    Account ||--o{ BackupVault : owns
    Account ||--o{ Volume : owns
    Account ||--o{ BackupPolicy : owns
    Volume ||--o{ Backup : creates
    Volume ||--o| BackupVault : "attached via DataProtection"
    Volume ||--o| BackupPolicy : "attached via DataProtection"
```

### Immutable Attributes Structure

```json
{
  "BackupMinimumEnforcedRetentionDuration": 30,
  "IsDailyBackupImmutable": true,
  "IsWeeklyBackupImmutable": false,
  "IsMonthlyBackupImmutable": true,
  "IsAdhocBackupImmutable": false
}
```

### Bucket Details Structure

```json
[
  {
    "bucket_name": "backup-vault-bucket-123",
    "service_account_name": "backup-service-account",
    "vendor_subnet_id": "subnet-123",
    "tenant_project_number": "123456789"
  }
]
```

## Backup Vault Types

### 1. In-Region Backup Vault
- **Type**: `IN_REGION`
- **Purpose**: Stores backups in the same region as the source volume
- **Use Case**: Fast backup and restore operations, lower latency
- **Configuration**: Single region specification

### 2. Cross-Region Backup Vault
- **Type**: `CROSS_REGION`
- **Purpose**: Stores backups in a different region for disaster recovery
- **Use Case**: Geographic redundancy, compliance requirements
- **Configuration**: Source region and backup region specification

```mermaid
graph LR
    subgraph "Source Region"
        V1[Volume 1]
        V2[Volume 2]
        BV1[In-Region Vault]
    end
    
    subgraph "Backup Region"
        BV2[Cross-Region Vault]
        B1[Backup 1]
        B2[Backup 2]
    end
    
    V1 --> BV1
    V2 --> BV1
    BV1 --> BV2
    BV2 --> B1
    BV2 --> B2
```

## Lifecycle Management

### State Transitions

```mermaid
stateDiagram-v2
    [*] --> CREATING : Create Request
    CREATING --> READY : Creation Complete
    CREATING --> ERROR : Creation Failed
    
    READY --> UPDATING : Update Request
    UPDATING --> READY : Update Complete
    UPDATING --> ERROR : Update Failed
    
    READY --> DELETING : Delete Request
    DELETING --> DELETED : Deletion Complete
    DELETING --> ERROR : Deletion Failed
    
    ERROR --> READY : Retry/Recovery
    
    DELETED --> [*]
```

### State Descriptions

| State | Description | Details |
|-------|-------------|---------|
| `CREATING` | Initial creation in progress | Vault being provisioned in SDE and VCP |
| `READY` | Available for use | Vault ready to accept backups |
| `UPDATING` | Configuration update in progress | Retention policy or description being updated |
| `DELETING` | Deletion in progress | Vault and associated buckets being removed |
| `DELETED` | Soft deleted | Vault marked as deleted but data retained |
| `ERROR` | Operation failed | Requires manual intervention or retry |

## API Design

### REST Endpoints

```mermaid
graph TB
    subgraph "Backup Vault API Endpoints"
        CREATE["POST /v1beta/projects/{project}/locations/{location}/backupVaults"]
        LIST["GET /v1beta/projects/{project}/locations/{location}/backupVaults"]
        GET["GET /v1beta/projects/{project}/locations/{location}/backupVaults/{id}"]
        UPDATE["PATCH /v1beta/projects/{project}/locations/{location}/backupVaults/{id}"]
        DELETE["DELETE /v1beta/projects/{project}/locations/{location}/backupVaults/{id}"]
        MULTI["POST /v1beta/projects/{project}/locations/{location}/backupVaults:getMultiple"]
    end
    
    subgraph "Volume API Endpoints (Backup Vault Attachment)"
        VOL_CREATE["POST /v1beta/projects/{project}/locations/{location}/volumes"]
        VOL_UPDATE["PATCH /v1beta/projects/{project}/locations/{location}/volumes/{id}"]
        VOL_GET["GET /v1beta/projects/{project}/locations/{location}/volumes/{id}"]
    end
```

### Request/Response Models

#### Create Backup Vault Request
```json
{
  "resourceId": "my-backup-vault",
  "description": "Production backup vault",
  "backupRegion": "us-central1",
  "backupRetentionPolicy": {
    "backupMinimumEnforcedRetentionDays": 30,
    "dailyBackupImmutable": true,
    "weeklyBackupImmutable": false,
    "monthlyBackupImmutable": true,
    "manualBackupImmutable": false
  }
}
```

#### Backup Vault Response
```json
{
  "backupVaultId": "uuid-123",
  "resourceId": "my-backup-vault",
  "state": "READY",
  "stateDetails": "Available for use",
  "createdAt": "2024-01-01T00:00:00Z",
  "backupRegion": "us-central1",
  "sourceRegion": "us-east1",
  "backupVaultType": "CROSS_REGION",
  "backupRetentionPolicy": {
    "backupMinimumEnforcedRetentionDays": 30,
    "dailyBackupImmutable": true,
    "weeklyBackupImmutable": false,
    "monthlyBackupImmutable": true,
    "manualBackupImmutable": false
  }
}
```

#### Create Volume with Backup Vault Request
```json
{
  "volume": {
    "resourceId": "my-volume",
    "quotaInBytes": 1073741824,
    "poolId": "pool-123"
  },
  "backupConfig": {
    "backupVaultId": "projects/123/locations/us-central1/backupVaults/vault-123",
    "backupPolicyId": "projects/123/locations/us-central1/backupPolicies/policy-456",
    "scheduledBackupEnabled": true
  }
}
```

#### Update Volume Backup Configuration Request
```json
{
  "backupConfig": {
    "backupVaultId": "projects/123/locations/us-central1/backupVaults/new-vault-456",
    "scheduledBackupEnabled": false
  }
}
```

## Workflow Architecture

### Workflow Types

| Workflow | Purpose | Input Parameters | Output |
|----------|---------|------------------|--------|
| `UpdateBackupVaultWorkflow` | Update backup vault configuration | `BackupVaultParams`, `BackupVault` | `V1betaUpdateBackupVaultRes` |
| `DeleteBackupVaultWorkflow` | Delete backup vault and cleanup resources | `BackupVaultParams`, `BackupVault` | `V1betaDeleteBackupVaultRes` |

### Workflow Structure

```mermaid
classDiagram
    class BaseWorkflow {
        +string ID
        +string CustomerID
        +WorkflowStatus Status
        +Logger Logger
        +Setup(ctx, input) error
        +Run(ctx, args) (interface{}, CustomError)
        +UpdateJobStatus(ctx, state, err) error
    }
    
    class backupVaultUpdateWorkflow {
        +BaseWorkflow
        +Storage SE
    }
    
    class backupVaultDeleteWorkflow {
        +BaseWorkflow
        +Storage SE
    }
    
    BaseWorkflow <|-- backupVaultUpdateWorkflow
    BaseWorkflow <|-- backupVaultDeleteWorkflow
```

### Workflow Activities

| Activity | Purpose | Input | Output |
|----------|---------|-------|--------|
| `GetAuthJWTToken` | Get authentication token | `AccountName` | `JWT Token` |
| `UpdateBackupVaultInSDE` | Update backup vault in SDE | `BackupVaultParams` | `BackupVault` |
| `UpdateBackupVaultInVCP` | Update backup vault in VCP database | `BackupVault`, `BackupVault` | `BackupVault` |
| `DeleteBackupVaultInSDE` | Delete backup vault from SDE | `BackupVaultParams` | `BackupVault` |
| `DeleteBackupVaultBuckets` | Delete associated GCP buckets | `BackupVault` | `void` |
| `DeleteBackupVaultInVCP` | Delete backup vault from VCP database | `BackupVaultID` | `BackupVault` |
| `UpdateBackupVaultState` | Update backup vault state | `BackupVault`, `State`, `StateDetails` | `BackupVault` |
| `UpdateBackupVaultStateInCaseOfError` | Rollback state on error | `BackupVault`, `State`, `StateDetails` | `void` |
| `UpdateJobStatus` | Update job status | `JobID`, `State`, `Error` | `void` |

### Workflow Execution Details

#### Workflow Lifecycle

| Phase | Description | Activities |
|-------|-------------|------------|
| **Setup** | Initialize workflow with parameters | `Setup(ctx, input)` |
| **Processing** | Execute main workflow logic | `Run(ctx, args)` |
| **Completion** | Finalize workflow execution | `UpdateJobStatus(DONE)` |
| **Error Handling** | Handle failures and rollback | `UpdateJobStatus(ERROR)`, `UpdateBackupVaultStateInCaseOfError` |

#### Job Status Management

| Status | Description | Trigger |
|--------|-------------|---------|
| `NEW` | Job created, not started | Job creation |
| `PROCESSING` | Workflow execution in progress | Workflow start |
| `DONE` | Workflow completed successfully | Workflow completion |
| `ERROR` | Workflow failed | Workflow failure |

#### Error Handling and Rollback

| Error Type | Handling | Rollback Action |
|------------|----------|-----------------|
| **SDE Update Failure** | Mark workflow as failed | `UpdateBackupVaultStateInCaseOfError` |
| **VCP Update Failure** | Mark workflow as failed | `UpdateBackupVaultStateInCaseOfError` |
| **Authentication Failure** | Mark workflow as failed | `UpdateJobStatus(ERROR)` |
| **Bucket Deletion Failure** | Mark workflow as failed | Continue with VCP deletion |

#### Retry Policy Configuration

| Parameter | Value | Description |
|-----------|-------|-------------|
| `InitialInterval` | 5 seconds | Initial retry delay |
| `BackoffCoefficient` | 2.0 | Exponential backoff multiplier |
| `MaximumInterval` | 5 minutes | Maximum retry delay |
| `MaximumAttempts` | 3 | Maximum retry attempts |
| `NonRetryableErrorTypes` | `["PanicError"]` | Errors that should not be retried |

### Create Backup Vault Flow

```mermaid
sequenceDiagram
    participant Client
    participant GoogleProxy
    participant Orchestrator
    participant SDE
    participant VCP
    
    Client->>GoogleProxy: POST /backupVaults
    GoogleProxy->>Orchestrator: GetBackupVaultByNameAndOwnerID()
    Orchestrator-->>GoogleProxy: Check if exists in VCP
    
    alt Backup Vault exists in VCP
        GoogleProxy-->>Client: Return existing vault
    else Backup Vault not in VCP
        GoogleProxy->>SDE: V1betaCreateBackupVault()
        SDE-->>GoogleProxy: Vault Created in SDE
        GoogleProxy-->>Client: 201 Created (SDE response)
    end
    
    Note over VCP: VCP entry created later when<br/>volume is attached to vault
```

### VCP Entry Creation Flow

The VCP database entry for a backup vault is created **lazily** when a volume is attached to the backup vault, not during the initial backup vault creation. This happens in the volume creation workflow:

```mermaid
sequenceDiagram
    participant VolumeCreate
    participant VCP
    participant SDE
    
    VolumeCreate->>VCP: GetBackupVaultByUUIDndOwnerID()
    VCP-->>VolumeCreate: Not found
    
    VolumeCreate->>SDE: V1betaListBackupVaults()
    SDE-->>VolumeCreate: List of vaults
    
    VolumeCreate->>VolumeCreate: Find matching vault by ID
    VolumeCreate->>VolumeCreate: convertToBackupVaultDataModel()
    
    VolumeCreate->>VCP: CreateBackupVaultEntryInVCP()
    VCP-->>VolumeCreate: Entry created
    
    Note over VolumeCreate: Volume creation continues<br/>with backup vault attached
```

## Volume-Backup Vault Attachment

### Volume Creation with Backup Vault

When creating a volume with backup configuration, the backup vault is attached through the `BackupConfig` parameter:

```mermaid
sequenceDiagram
    participant Client
    participant GoogleProxy
    participant Orchestrator
    participant VolumeCreate
    participant VCP
    participant SDE
    
    Client->>GoogleProxy: POST /volumes<br/>{backupConfig: {backupVaultId: "vault-123"}}
    GoogleProxy->>GoogleProxy: _prepareCreateVolumeParams()
    Note over GoogleProxy: Extract BackupConfig from request<br/>Set param.DataProtection.BackupVaultID
    
    GoogleProxy->>Orchestrator: CreateVolume(params)
    Orchestrator->>VolumeCreate: ExecuteWorkflow()
    
    VolumeCreate->>VCP: GetBackupVaultByUUIDndOwnerID()
    VCP-->>VolumeCreate: Not found
    
    VolumeCreate->>SDE: V1betaListBackupVaults()
    SDE-->>VolumeCreate: List of vaults
    
    VolumeCreate->>VolumeCreate: Find matching vault by ID
    VolumeCreate->>VolumeCreate: convertToBackupVaultDataModel()
    
    VolumeCreate->>VCP: CreateBackupVaultEntryInVCP()
    VCP-->>VolumeCreate: Entry created
    
    VolumeCreate->>VolumeCreate: Continue volume creation<br/>with backup vault attached
    
    VolumeCreate-->>Orchestrator: Volume created
    Orchestrator-->>GoogleProxy: Volume created
    GoogleProxy-->>Client: 201 Created
```

### Volume Update with Backup Vault

Volumes can have their backup vault configuration updated through the volume update API:

```mermaid
sequenceDiagram
    participant Client
    participant GoogleProxy
    participant Orchestrator
    participant VolumeUpdate
    participant VCP
    
    Client->>GoogleProxy: PATCH /volumes/{id}<br/>{backupConfig: {backupVaultId: "new-vault-456"}}
    GoogleProxy->>GoogleProxy: _prepareUpdateVolumeParams()
    Note over GoogleProxy: Extract BackupConfig from request<br/>Set param.DataProtection.BackupVaultID
    
    GoogleProxy->>Orchestrator: UpdateVolumeV2(params)
    Orchestrator->>VolumeUpdate: ExecuteWorkflow()
    
    VolumeUpdate->>VCP: GetVolume()
    VCP-->>VolumeUpdate: Current volume data
    
    Note over VolumeUpdate: Validate backup vault change<br/>Check for existing backups
    
    alt Backup vault change allowed
        VolumeUpdate->>VCP: Update volume with new backup vault
        VCP-->>VolumeUpdate: Volume updated
        VolumeUpdate-->>Orchestrator: Update complete
    else Backup vault change not allowed
        VolumeUpdate-->>Orchestrator: Validation error
        Orchestrator-->>GoogleProxy: 400 Bad Request
        GoogleProxy-->>Client: Error response
    end
```

### Backup Configuration Structure

The backup configuration is passed through the `BackupConfig` parameter in both create and update operations:

```json
{
  "backupConfig": {
    "backupVaultId": "projects/123/locations/us-central1/backupVaults/vault-123",
    "backupPolicyId": "projects/123/locations/us-central1/backupPolicies/policy-456",
    "scheduledBackupEnabled": true,
    "backupChainBytes": 1073741824
  }
}
```

### Validation Rules

When attaching backup vaults to volumes, the system enforces several validation rules:

1. **Backup Vault Existence**: The backup vault must exist in SDE
2. **Cross-Region Restriction**: Cross-region backup vaults cannot be attached to volumes
3. **Backup Policy Validation**: If a backup policy is specified, it must be valid
4. **Existing Backups**: Cannot change backup vault if volume has existing backups
5. **Immutable Vault Updates**: Cannot update retention policy of vaults attached to volumes (returns error: "Immutable backup vaults are not supported for ISCSI volumes")

### Update Backup Vault Workflow

```mermaid
sequenceDiagram
    participant Client
    participant GoogleProxy
    participant Orchestrator
    participant Workflow
    participant Activities
    participant VCP
    participant SDE
    
    Client->>GoogleProxy: PATCH /backupVaults/{id}
    GoogleProxy->>Orchestrator: UpdateBackupVault()
    Orchestrator->>Workflow: ExecuteWorkflow()
    
    Workflow->>Workflow: Setup(ctx, params)
    Workflow->>Activities: UpdateJobStatus(PROCESSING)
    Activities-->>Workflow: Job Status Updated
    
    Workflow->>Activities: GetAuthJWTToken(AccountName)
    Activities-->>Workflow: JWT Token
    
    Workflow->>Activities: UpdateBackupVaultInSDE(BackupVaultParams)
    Activities->>SDE: V1betaUpdateBackupVault()
    SDE-->>Activities: Vault Updated
    Activities-->>Workflow: SDE Vault
    
    Workflow->>Activities: UpdateBackupVaultInVCP(SDEVault, VCPVault)
    Activities->>VCP: UpdateBackupVault()
    VCP-->>Activities: Vault Updated
    Activities-->>Workflow: VCP Vault
    
    Workflow->>Activities: UpdateJobStatus(DONE)
    Activities-->>Workflow: Job Status Updated
    
    Workflow-->>Orchestrator: Workflow Complete
    Orchestrator-->>GoogleProxy: Vault Updated
    GoogleProxy-->>Client: 200 OK
```

### Delete Backup Vault Workflow

```mermaid
sequenceDiagram
    participant Client
    participant GoogleProxy
    participant Orchestrator
    participant Workflow
    participant Activities
    participant SDE
    participant VCP
    participant GCP
    
    Client->>GoogleProxy: DELETE /backupVaults/{id}
    GoogleProxy->>Orchestrator: DeleteBackupVault()
    
    Note over Orchestrator: Validate no backups exist
    Note over Orchestrator: Validate no volumes attached
    
    Orchestrator->>Workflow: ExecuteWorkflow()
    
    Workflow->>Workflow: Setup(ctx, params)
    Workflow->>Activities: UpdateJobStatus(PROCESSING)
    Activities-->>Workflow: Job Status Updated
    
    Workflow->>Activities: GetAuthJWTToken(AccountName)
    Activities-->>Workflow: JWT Token
    
    Workflow->>Activities: DeleteBackupVaultInSDE(BackupVaultParams)
    Activities->>SDE: V1betaDeleteBackupVault()
    SDE-->>Activities: Vault Deleted
    Activities-->>Workflow: SDE Vault
    
    Workflow->>Activities: DeleteBackupVaultBuckets(BackupVault)
    Activities->>GCP: DeleteBuckets()
    GCP-->>Activities: Buckets Deleted
    Activities-->>Workflow: Buckets Deleted
    
    Workflow->>Activities: DeleteBackupVaultInVCP(BackupVaultID)
    Activities->>VCP: DeleteBackupVault()
    VCP-->>Activities: Vault Deleted
    Activities-->>Workflow: VCP Vault
    
    Workflow->>Activities: UpdateJobStatus(DONE)
    Activities-->>Workflow: Job Status Updated
    
    Workflow-->>Orchestrator: Workflow Complete
    Orchestrator-->>GoogleProxy: Vault Deleted
    GoogleProxy-->>Client: 200 OK
```

## Storage Integration

### GCP Bucket Management

```mermaid
graph TB
    subgraph "Backup Vault Storage Architecture"
        BV[Backup Vault]
        BD[Bucket Details]
        SA[Service Account]
        SN[Subnet]
        TP[Tenant Project]
        
        subgraph "GCP Storage"
            B1[Bucket 1]
            B2[Bucket 2]
            B3[Bucket N]
        end
        
        BV --> BD
        BD --> SA
        BD --> SN
        BD --> TP
        BD --> B1
        BD --> B2
        BD --> B3
    end
```




### Immutable Backup Configuration

| Backup Type | Immutable Setting | Description |
|-------------|------------------|-------------|
| Daily | `IsDailyBackupImmutable` | Daily backups cannot be deleted before retention period |
| Weekly | `IsWeeklyBackupImmutable` | Weekly backups cannot be deleted before retention period |
| Monthly | `IsMonthlyBackupImmutable` | Monthly backups cannot be deleted before retention period |
| Ad-hoc | `IsAdhocBackupImmutable` | Manual backups cannot be deleted before retention period |


### Database Indexes

Based on the code analysis, the following indexes are defined:

| Table | Index | Purpose |
|-------|-------|---------|
| `backup_vaults` | `(name)` | Fast lookup by backup vault name |
| `backup_vaults` | `(deleted_at)` | Soft delete filtering |
| `jobs` | `(state)` | Job state filtering |
| `jobs` | `(account_id)` | Job filtering by account |





