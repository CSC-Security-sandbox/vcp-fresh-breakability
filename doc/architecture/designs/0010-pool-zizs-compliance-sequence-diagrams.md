# Pool ZI/ZS Compliance Sequence Examples

This document provides sequence diagrams illustrating the key workflows for pool ZI/ZS compliance tracking.

## 1. Pool Creation with Compliance Sync Workflow

```mermaid
sequenceDiagram
    participant Client
    participant PoolWF as Pool Workflow
    participant Activities as Pool Activities
    participant ZIZSWF as ZI/ZS Compliance WF
    participant VLMClient as VLM Client
    parameter VLM as VLM Service
    participant DB as Database

    Client->>PoolWF: Create Pool Request
    PoolWF->>Activities: Execute Pool Creation Activities
    Activities->>DB: Create Pool Record
    DB-->>Activities: Pool Created
    Activities-->>PoolWF: Pool Creation Complete

    Note over PoolWF,ZIZSWF: Trigger Compliance Sync (Async)
    PoolWF->>ZIZSWF: ExecuteChildWorkflow(SyncPoolComplianceForPoolWorkflow)
    
    Note over PoolWF: Continue pool creation flow
    PoolWF->>Client: Pool Creation Response (Success)

    Note over ZIZSWF,VLM: Background compliance sync continues
    
    ZIZSWF->>Activities: FetchPoolData
    Activities->>DB: Query Pool Config
    DB-->>Activities: Pool Config
    Activities-->>ZIZSWF: VLM Config

    ZIZSWF->>VLMClient: GetClusterZiZsDetails(ProjectID, DeploymentID)
    VLMClient->>VLM: Execute Child Workflow
    
    Note over VLM: Process compliance check
    VLM-->>VLMClient: Resource Compliance Data
    VLMClient-->>ZIZSWF: Compliance Response

    ZIZSWF->>Activities: UpdatePoolCompliance(satisfyZI, satisfyZS, assetMetadata)
    Activities->>DB: Update Pool Compliance Fields
    DB-->>Activities: Update Complete
    Activities-->>ZIZSWF: Success

    Note over ZIZSWF: Workflow completes independently
```

## 2. Background Pool Compliance Sync Workflow

```mermaid
sequenceDiagram
    participant Scheduler as Cron Scheduler
    participant MainWF as SyncPoolZIZSDetailsWorkflow
    participant CommonAct as Common Activities
    participant ChildWF as SyncPoolComplianceForPoolWorkflow
    participant Activities as Pool Activities
    participant VLMClient as VLM Client
    participant VLM as VLM Service
    participant DB as Database

    Note over Scheduler: Every hour (0 * * * *)
    Scheduler->>MainWF: Trigger Background Sync

    MainWF->>CommonAct: ListPoolsUUID
    CommonAct->>DB: SELECT undeleted pools
    DB-->>CommonAct: Pool Identifiers List
    CommonAct-->>MainWF: Pool Identifiers

    Note over MainWF: Spawn child workflows for each pool
    loop For each pool
        MainWF->>ChildWF: ExecuteChildWorkflow(poolIdentifier)
        
        ChildWF->>Activities: FetchPoolData(poolUUID, accountID)
        Activities->>DB: Query Pool & VLM Config
        DB-->>Activities: Pool Configuration
        Activities-->>ChildWF: VLM Configuration

        ChildWF->>VLMClient: GetClusterZiZsDetails(req)
        VLMClient->>VLM: Child Workflow Execution
        
        Note over VLM: Analyze GCP resources for compliance
        VLM->>VLM: Check Zone Independence (ZI)
        VLM->>VLM: Check Zone Separation (ZS)
        VLM->>VLM: Collect Asset Metadata
        
        VLM-->>VLMClient: Compliance Data + Assets
        VLMClient-->>ChildWF: Resource Information

        ChildWF->>ChildWF: Process compliance data
        Note over ChildWF: Extract satisfyZI, satisfyZS, assetMetadata

        ChildWF->>Activities: UpdatePoolCompliance(poolUUID, satisfyZI, satisfyZS, assetMetadata)
        Activities->>DB: UPDATE pools SET satisfy_zi, satisfy_zs, asset_metadata
        DB-->>Activities: Update Success
        Activities-->>ChildWF: Update Complete
        
        ChildWF-->>MainWF: Pool Sync Complete
    end

    Note over MainWF: All pool syncs complete
    MainWF-->>Scheduler: Background Sync Complete
```

## 3. API Request Flow for Pool with Compliance Data

```mermaid
sequenceDiagram
    participant Client
    participant Proxy as Google Proxy
    participant CoreAPI as Core API
    participant Handler as Pool Handler
    parameter DB as Database

    Client->>Proxy: GET /pools/{poolID}
    Proxy->>CoreAPI: Forward Request
    CoreAPI->>Handler: Handle Pool Request
    
    Handler->>DB: SELECT pool with asset_metadata, satisfy_zi, satisfy_zs
    DB-->>Handler: Pool Record with Compliance Data
    
    Note over Handler: Include compliance fields in response
    Handler-->>CoreAPI: Pool Response Data
    CoreAPI-->>Proxy: Enhanced Pool Response
    Proxy-->>Client: Pool Data with ZI/ZS Compliance

    Note over Client: Client receives:<br/>- satisfies_pzi: boolean<br/>- satisfies_pzs: boolean<br/>- asset_metadata: object
```

## 4. VLM Compliance Data Processing Flow

```mermaid
sequenceDiagram
    participant ChildWF as SynchronousPoolComplianceForPoolWorkflow  
    participant VLMClient as VLM Workflow Client
    participant VLMWorkflow as GetClusterZiZsDetails Workflow
    participant GCPAPI as GCP APIs
    participant VLM as VLM Service

    ChildWF->>VLMClient: GetClusterZiZsDetails(request)
    
    VLMClient->>VLMClient: Prepare Child Workflow Options
    Note over VLMClient: Set timeout, retry policy, correlation ID
    
    VLMClient->>VLMWorkflow: Execute Child Workflow
    Note over VLMWorkflow: Temporal workflow execution
    
    VLMWorkflow->>VLM: Analyze GCP Resources
    VLM->>GCPAPI: Query Compute Instances
    GCPAPI-->>VLM: Instance Metadata
    VLM->>GCPAPI: Query Compute Disks  
    GCPAPI-->>VLM: Disk Metadata
    VLM->>GCPAPI: Query Network Resources
    GCPAPI-->>VLM: Network Metadata

    Note over VLM: Compliance Analysis
    VLM->>VLM: Check Zone Independence:<br/>- Resource availability across zones<br/>- Fault domain distribution
    VLM->>VLM: Check Zone Separation:<br/>- Resource isolation<br/>- Security boundaries

    VLM->>VLM: Collect Asset Metadata:<br/>- Group by asset_type<br/>- Collect asset_links

    VLM-->>VLMWorkflow: Resource Information Response
    Note over VLMWorkflow: Response structure:<br/>- GCPRI: map[string][]Resource<br/>- Resource.SatisfiesPzi<br/>- Resource.SatisfiesPzs<br/>- Resource.AssetType<br/>- Resource.AssetLink

    VLMWorkflow-->>VLMClient: Compliance Response
    VLMClient-->>ChildWF: Resource Information

    Note over ChildWF: Extract and process:<br/>- satisfyZI = all resources.SatisfiesPzi<br/>- satisfyZS = all resources.SatisfiesPzs<br/>- assetMetadata = group by AssetType
```

## 5. Error Handling and Retry Flow

```mermaid
sequenceDiagram
    participant MainWF as SyncPoolZIZSDetailsWorkflow
    participant ChildWF as SyncPoolComplianceForPoolWorkflow
    participant Activities as Pool Activities
    participant VLMClient as VLM Client
    participant VLM as VLM Service

    MainWF->>ChildWF: ExecuteChildWorkflow(pool)

    rect rgba(255, 170, 0, .3)
        Note over ChildWF,VLM: Error Scenarios
        ChildWF->>Activities: FetchPoolData
        Activities-->>ChildWF: Fetch Error
        
        alt Activity Retry Policy
            ChildWF->>ChildWF: Retry with exponential backoff
            ChildWF->>Activities: FetchPoolData (retry)
            Activities-->>ChildWF: Success
        else Max Attempts Reached
            ChildWF-->>MainWF: Workflow Failed
            Note over MainWF: Log error, continue with other pools
        end
    end

    rect rgba(255, 100, 100, .3)
        Note over ChildWF,VLM: High Latency/Timeout
        ChildWF->>VLMClient: GetClusterZiZsDetails
        VLMClient->>VLM: Child Workflow
        
        Note over VLM: Large deployment, slow processing
        VLMClient-->>VLMClient: Workflow Timeout
        
        alt Workflow Retry Policy  
            VLMClient->>VLMClient: Retry workflow execution
            VLMClient->>VLM: Re-execute Child Workflow
        else Timeout Exceeded
            VLMClient-->>ChildWF: VLM Timeout Error
            ChildWF-->>MainWF: Pool Sync Failed
        end
    end

    rect rgba(100, 255, 100, .3)
        Note over MainWF: Graceful Degradation
        MainWF->>MainWF: Continue processing other pools
        Note over MainWF: Individual pool failures don't affect others
    end
```

These sequence diagrams provide a comprehensive view of how the ZI/ZS compliance system operates across different scenarios, showing the integration points, error handling, and data flow between components.
