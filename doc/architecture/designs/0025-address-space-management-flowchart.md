# Address Space Management — Flow Charts

---

## Diagram 1: Pool Creation Control Flow (End-to-End)

```mermaid
flowchart TD
    A([Operator: POST /storagePools]) --> B[Google Proxy\napi/endpoints]
    B --> C[Core API\nPOST /v1/pools]
    C --> D[GCP Orchestrator\nfactory/gcp/pool.go]

    D --> E[(DB: INSERT pools\nstate = CREATING)]
    D --> F[(DB: INSERT jobs\nstate = NEW)]

    D --> G{ADDRESS_SPACE_MGMT\n_ENABLED = true?}

    G -->|No| H[RequestedRanges = nil]
    G -->|Yes| I[ParseProjectId\nvendorSubNetID\n→ hostProjectNumber + vpcName]
    I --> J[(DB: SELECT address_ranges\nWHERE vpc_name=?\nAND host_project_number=?\nAND lif_type='dataLIF'\nAND deleted_at IS NULL)]
    J --> K[Filter: lifecycle_state == 'CREATED'\nor 'IN_USE']
    K --> L[params.RequestedRanges\n= &#91;'new-nis-range2', ...&#93;]
    L --> M

    H --> M[ExecuteWorkflow\nCustomerTaskQueue\nCreatePoolWorkflow]
    M --> N[Temporal Server]
    N --> O[VCP Worker\nCustomerTaskQueue]

    style E fill:#dbeafe,stroke:#3b82f6
    style F fill:#dbeafe,stroke:#3b82f6
    style J fill:#dbeafe,stroke:#3b82f6
    style L fill:#dcfce7,stroke:#16a34a
```

---

## Diagram 2: CreatePoolWorkflow — Activities & DB Updates

```mermaid
flowchart TD
    START([CreatePoolWorkflow starts]) --> S1

    S1[GetJob] --> S2
    S2[(DB: jobs\nstate → PROCESSING)] --> S3

    S3[FindTenancyProject\n→ tenantProjectNumber] --> S4

    S4[[Child Workflow:\nDataSubnetSequentialPoller]] --> S4a
    S4a[GetAvailableSubnet\nor GetCreateDataSubnetOp\n+ WaitForOperation] --> S4b
    S4b[GCP Service Networking API\ncreatePeering with\nRequestedRanges] --> S4c
    S4c[GetSubnetFromOperation\n→ subnet.IpCidrRange\nallocatedSubnetCIDR] --> S4d
    S4d[GetTenancyInfo\nwf.TenancyDetails = tenancyInfo] --> S4e
    S4e[GetTenancyDetails\nreturns AllocatedSubnetCIDR] --> S4f
    S4f[(DB: UPDATE pools\nsubnet + tenancy info)] --> S5

    S5[SavePoolWithClusterDetails] --> S5a
    S5a[(DB: UPDATE pools\ncluster_details JSON\nAllocatedSubnetCIDR persisted)] --> S5b

    S5b{ADDRESS_SPACE_MGMT\n_ENABLED AND\nAllocatedSubnetCIDR != ''?} -->|Yes| S5c
    S5b -->|No| S6
    S5c[MarkAddressRangeInUse\nCIDR containment match\n→ UPDATE lifecycle_state='IN_USE'] --> S5d
    S5d[(DB: UPDATE address_ranges\nlifecycle_state = 'IN_USE')] --> S6

    S6[[Child Workflow:\nConfigureNetworkWorkflow]] --> S7
    S7[CreateServiceAccountWithStorageRole\n→ serviceAccount] --> S8
    S8[CreateAutoTierBucket] --> S9
    S9[CreateOnTapCredentials] --> S9a
    S9a[(DB: UPDATE pools\npool_credentials)] --> S10

    S10[IdentifyVMs\n→ vlmConfig] --> S11
    S11[IdentifySecondaryAndMediatorZone\n→ resolvedLocationInfo] --> S12

    S12[[Child Workflow:\nCreateVSAClusterDeployment\nVLM Worker]] --> S12a
    S12a[AllocateClusterSerialNumber] --> S12b
    S12b[Deploy VSA Cluster\non GCP VMs] --> S12c
    S12c[CreateCloudDNSRecords] --> S12d
    S12d[(DB: UPDATE pools\nnode details\nsvm details\ncluster_details)] --> S13

    S13[GetInterClusterLifsFromVLMConfig] --> S14
    S14[AllocateSVMName] --> S15

    S15[[Child Workflow:\nCreateVSASVM\nVLM Worker]] --> S15a
    S15a[CreateQoSPolicyAndApplyToSVM] --> S15b
    S15b[SaveSVMAndLifData] --> S15c
    S15c[(DB: UPDATE pools\nsvms table\nlifs table)] --> S16

    S16{Expert Mode\nEnabled?} -->|Yes| S16a
    S16a[CreateExpertModeCredentials\nCreateVSAExpertModeUser] --> S16b
    S16b[(DB: UPDATE pools\nexpert_mode_credentials)] --> S17
    S16 -->|No| S17

    S17[CreatedPool] --> S17a
    S17a[(DB: UPDATE pools\nstate → READY\nstate_details = 'Available for use')] --> S18

    S18[(DB: UPDATE jobs\nstate → DONE)] --> DONE([Workflow Complete ✓])

    ERR([Any activity fails]) --> R1
    R1[RollbackManager] --> R2[ErroredPool]
    R2 --> R3[(DB: UPDATE pools\nstate → ERROR)]
    R2 --> R4[DeletePoolResourcesOnRollback]
    R4 --> R5[(DB: UPDATE jobs\nstate → ERROR)]

    style S2 fill:#dbeafe,stroke:#3b82f6
    style S4f fill:#dbeafe,stroke:#3b82f6
    style S5a fill:#dbeafe,stroke:#3b82f6
    style S5d fill:#dbeafe,stroke:#3b82f6
    style S9a fill:#dbeafe,stroke:#3b82f6
    style S12d fill:#dbeafe,stroke:#3b82f6
    style S15c fill:#dbeafe,stroke:#3b82f6
    style S16b fill:#dbeafe,stroke:#3b82f6
    style S17a fill:#dcfce7,stroke:#16a34a
    style R3 fill:#fee2e2,stroke:#dc2626
    style R5 fill:#fee2e2,stroke:#dc2626
    style S4b fill:#fef9c3,stroke:#ca8a04
    style S5c fill:#fef9c3,stroke:#ca8a04
```

---

## Diagram 3: DeletePoolWorkflow — Address Range Reset

```mermaid
flowchart TD
    START([DeletePoolWorkflow starts]) --> D1

    D1[GetPool\n→ dbPool with\nClusterDetails.AllocatedSubnetCIDR] --> D2
    D2[DeletingPoolResources\npool.State = DELETING] --> D3

    D3{hasClusterDetails?} -->|No| D10
    D3 -->|Yes| D4

    D4[[Child Workflow:\nDataSubnetSequentialPoller Delete]] --> D4a
    D4a[[PoolDataSubnetWorkFlow Delete]] --> D4b
    D4b[GetPool\n→ load pool from DB] --> D4c
    D4c[GetPoolTenancyInfo\n→ getPoolAllocatedSubnetCIDR] --> D4d

    D4d{AllocatedSubnetCIDR\nin ClusterDetails?} -->|Yes: return directly| D4e
    D4d -->|No: legacy pool| D4f
    D4f[GCP GetSubnetwork\nLIVE LOOKUP\nbefore deletion] --> D4e
    D4e[tenancyInfo.AllocatedSubnetCIDR\n= '10.55.55.16/29'] --> D4g

    D4g[ReleaseDataSubnetOp\n+ WaitForGCPNetworkOperationStatus\n← subnet DELETED here] --> D4h
    D4h[wf.TenancyDetails = tenancyInfo\nstored in query handler] --> D4i
    D4i[GetTenancyDetails\nquery PoolDataSubnetWorkFlow\nreturns AllocatedSubnetCIDR] --> D5

    D5[dbPool.ClusterDetails\n.AllocatedSubnetCIDR\n= '10.55.55.16/29'] --> D6

    D6[MarkAddressRangesCreated\ndbPool] --> D6a
    D6a[getPoolAllocatedSubnetCIDR\nreads from dbPool.ClusterDetails\nno GCP call] --> D6b
    D6b[findAddressRangeContainingSubnet\nCIDR containment match\n→ targetRange] --> D6c
    D6c[ListPools network=pool.Network\nexcluding deleted pool] --> D6d

    D6d{Any remaining pool's\nsubnet within targetRange?} -->|Yes| D6e
    D6d -->|No: last pool using range| D6f
    D6e[Leave IN_USE\nreturn] --> D10
    D6f[(DB: UPDATE address_ranges\nlifecycle_state = 'CREATED')] --> D10

    D10[DeletePoolResources\npool soft-deleted in DB] --> DONE([Workflow Complete ✓])

    style D1 fill:#dbeafe,stroke:#3b82f6
    style D4f fill:#fef9c3,stroke:#ca8a04
    style D6f fill:#dcfce7,stroke:#16a34a
    style D10 fill:#dbeafe,stroke:#3b82f6
```

---

## Diagram 4: Address Range Lifecycle State Machine

```mermaid
stateDiagram-v2
    [*] --> CREATED : POST /v1/addressRange\n(INSERT address_ranges)

    CREATED --> IN_USE : MarkAddressRangeInUse\n(activity — post subnet creation)\nUPDATE lifecycle_state = 'IN_USE'

    IN_USE --> CREATED : MarkAddressRangesCreated\n(activity — on pool delete/rollback)\nonly if no other pool uses range\nUPDATE lifecycle_state = 'CREATED'

    CREATED --> DISABLED : PUT /v1/addressRange/{id}\n(operator disables range)\nUPDATE lifecycle_state = 'DISABLED'

    CREATED --> DELETED : DELETE /v1/addressRange/{id}\nSoft delete:\nUPDATE lifecycle_state = 'DELETED'\nSET deleted_at = NOW()

    DISABLED --> DELETED : DELETE /v1/addressRange/{id}

    note right of IN_USE
        Cannot be deleted.
        Only applyRouteAggregation
        field can be updated.
        Stays IN_USE while any pool
        on the network uses this range.
    end note

    note right of DELETED
        Row retained in DB.
        deleted_at IS NOT NULL.
        Excluded from all queries.
    end note
```

---

## Diagram 5: Address Range DB Interactions During Pool Lifecycle

```mermaid
sequenceDiagram
    participant Op as Operator
    participant GP as Google Proxy
    participant CA as Core API
    participant Orch as GCP Orchestrator
    participant TMP as Temporal
    participant WK as Worker (Activity)
    participant DB as PostgreSQL
    participant GCP as GCP Service Networking

    Note over Op,GCP: ── Register Address Range ──
    Op->>CA: POST /v1/addressRange\n{name, cidr, network, lifType}
    CA->>Orch: CreateAddressRange(ar)
    Orch->>DB: INSERT address_ranges\n(lifecycle_state='CREATED')
    DB-->>Op: 201 {addressRangeId, lifecycle_state: CREATED}

    Note over Op,GCP: ── Create Pool ──
    Op->>GP: POST /storagePools
    GP->>CA: POST /v1/pools
    CA->>Orch: createPool(params)
    Orch->>DB: INSERT pools (state=CREATING)
    Orch->>DB: INSERT jobs (state=NEW)
    Orch->>DB: SELECT address_ranges\nWHERE vpc_name=? AND lif_type='dataLIF'\nAND lifecycle_state IN ('CREATED','IN_USE')
    DB-->>Orch: [new-nis-range2]
    Orch->>TMP: ExecuteWorkflow(CreatePoolWorkflow)\nparams.RequestedRanges=['new-nis-range2']
    TMP-->>CA: workflow started
    CA-->>Op: 202 {poolId, jobId}

    Note over TMP,GCP: ── Inside CreatePoolWorkflow ──
    TMP->>WK: GetJob
    WK->>DB: UPDATE jobs (state=PROCESSING)

    TMP->>WK: FindTenancyProject
    TMP->>WK: [Child] DataSubnetSequentialPoller (Create)
    WK->>GCP: createPeering(RequestedRanges=['new-nis-range2'])
    GCP-->>WK: subnet {ipCidrRange: '10.55.55.16/29'}
    WK->>WK: wf.TenancyDetails.AllocatedSubnetCIDR = '10.55.55.16/29'
    WK->>DB: UPDATE pools (subnet, tenancy_info)

    TMP->>WK: SavePoolWithClusterDetails
    WK->>DB: UPDATE pools\ncluster_details.AllocatedSubnetCIDR='10.55.55.16/29'

    TMP->>WK: MarkAddressRangeInUse('10.55.55.16/29', network)
    WK->>DB: UPDATE address_ranges\nSET lifecycle_state='IN_USE'\nWHERE CIDR contains '10.55.55.16/29'

    TMP->>WK: CreateServiceAccountWithStorageRole
    TMP->>WK: CreateAutoTierBucket / CreateOnTapCredentials
    WK->>DB: UPDATE pools (pool_credentials)
    TMP->>WK: [Child] CreateVSAClusterDeployment
    WK->>DB: UPDATE pools (node_details, cluster_details)
    TMP->>WK: [Child] CreateVSASVM
    WK->>DB: INSERT/UPDATE svms, lifs
    TMP->>WK: CreatedPool
    WK->>DB: UPDATE pools (state=READY)
    WK->>DB: UPDATE jobs (state=DONE)

    Note over Op,GCP: ── Delete Pool ──
    Op->>GP: DELETE /storagePools/{id}
    GP->>CA: DELETE /v1/pools/{id}
    CA->>TMP: ExecuteWorkflow(DeletePoolWorkflow)

    TMP->>WK: GetPool
    WK->>DB: SELECT pools (ClusterDetails.AllocatedSubnetCIDR='10.55.55.16/29')

    TMP->>WK: [Child] DataSubnetSequentialPoller (Delete)
    WK->>WK: GetPoolTenancyInfo\nreads AllocatedSubnetCIDR from ClusterDetails\n(no GCP call for new pools)
    WK->>GCP: ReleaseDataSubnetOp (delete subnet)
    GCP-->>WK: subnet DELETED
    WK->>WK: wf.TenancyDetails.AllocatedSubnetCIDR = '10.55.55.16/29'\n(stored before return)

    TMP->>WK: MarkAddressRangesCreated(dbPool)
    WK->>DB: SELECT address_ranges (by CIDR containment)
    WK->>DB: SELECT pools WHERE network=? (check remaining pools)
    WK->>DB: UPDATE address_ranges\nSET lifecycle_state='CREATED'\n(only if no remaining pool uses range)

    TMP->>WK: DeletePoolResources
    WK->>DB: UPDATE pools (deleted_at=NOW())
    WK->>DB: UPDATE jobs (state=DONE)
```

---

## Diagram 6: Address Range Lookup Logic (factory/gcp/pool.go)

```mermaid
flowchart TD
    A[createPool called] --> B{ADDRESS_SPACE_MGMT\n_ENABLED = true\nAND VendorSubNetID != ''?}

    B -->|No| Z[RequestedRanges = nil\nskip lookup]

    B -->|Yes| C[ParseProjectId\nprojects/394353340554/global/networks/large-volumes-test]
    C --> D[hostProjectNumber = '394353340554'\nvpcName = 'large-volumes-test']

    D --> E{Both non-empty?}
    E -->|No| Z

    E -->|Yes| F[ListAddressRanges\nhostProjectNumber, vpcName\nlifType = 'dataLIF']
    F --> G[(SELECT FROM address_ranges\nWHERE host_project_number='394353340554'\nAND vpc_name='large-volumes-test'\nAND lif_type='dataLIF'\nAND deleted_at IS NULL)]

    G --> H{Error?}
    H -->|Yes| I[WARN: proceeding without\nRequestedRanges]
    I --> Z

    H -->|No| J[For each address range]
    J --> K{lifecycle_state\n== 'CREATED'?}
    K -->|No: IN_USE ranges\nalso included| L2[Append ar.Name\nIN_USE range can share]
    K -->|Yes| L[Append ar.Name to\nRequestedRanges]
    L --> J
    L2 --> J
    J --> M[params.RequestedRanges\n= all CREATED + IN_USE range names]
    M --> N[ExecuteWorkflow\nwith RequestedRanges]

    style G fill:#dbeafe,stroke:#3b82f6
    style M fill:#dcfce7,stroke:#16a34a
    style I fill:#fef9c3,stroke:#ca8a04
    style Z fill:#f3f4f6,stroke:#6b7280
```
