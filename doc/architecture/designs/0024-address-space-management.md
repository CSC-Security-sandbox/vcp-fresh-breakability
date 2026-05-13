# Address Space Management — Feature Implementation

> **Confluence page** — intended for the VCP team.  
> Covers: motivation, implementation details, control flow diagrams, and DB schema.

---

## 1. Overview

Address Space Management lets operators pre-register named IP CIDR ranges in VCP before pool creation.
When `ADDRESS_SPACE_MGMT_ENABLED=true`, these registered ranges are automatically passed as `RequestedRanges` to GCP Service Networking during pool creation, giving operators explicit control over which subnets GCP allocates for VSA data LIFs.

**Without this feature:** GCP picks any free block from the peering range — operators have no control.  
**With this feature:** GCP picks a block from the specific CIDR ranges the operator has registered.

---

## 2. Components Changed / Added

| Layer | File | What Changed |
|---|---|---|
| **DB Migration** | `database/vcp/migrations/post/0027_add_address_ranges.up.sql` | New `address_ranges` table |
| **Data Model** | `core/datamodel/models.go` | New `AddressRange` struct; `AllocatedSubnetCIDR` field added to `ClusterDetails` |
| **DB Layer** | `database/vcp/address_range.go` | CRUD + state-machine logic |
| **Storage Interface** | `database/vcp/interface.go` | 6 new interface methods |
| **Persistence Delegation** | `database/vcp/persistance_store.go` | Passthrough delegation |
| **Orchestrator** | `core/orchestrator/factory/gcp/address_range.go` | GCP orchestrator CRUD delegation |
| **Orchestrator Interface** | `core/orchestrator/factory/orchestrator.go` | 6 new interface methods |
| **Pool Factory (GCP)** | `core/orchestrator/factory/gcp/pool.go` | `RequestedRanges` lookup on pool create |
| **Pool Activities** | `core/orchestrator/activities/pool_activities.go` | `MarkAddressRangeInUse`, `MarkAddressRangesCreated`, `getPoolAllocatedSubnetCIDR` |
| **Pool Workflows** | `core/orchestrator/workflows/pool_workflows.go` | `MarkAddressRangeInUse` call post-subnet-create; `MarkAddressRangesCreated` call post-subnet-delete; `DataSubnetSequentialPoller` returns `TenancyInfo` for both create and delete; `PoolDataSubnetWorkFlow` stores pre-deletion CIDR in query handler |
| **Core API Handler** | `vcp-core/handlers/address_range_endpoint.go` | Full REST handler (CRUD + state update) |
| **OpenAPI Schema** | `vcp-core/api.yaml` | New paths + schemas |
| **Skaffold Config** | `skaffold/k8s/core.yaml`, `skaffold/k8s/vcp-worker.yaml` | `ADDRESS_SPACE_MGMT_ENABLED=true` |

---

## 3. DB Schema

### Table: `address_ranges`

```sql
CREATE TABLE IF NOT EXISTS address_ranges (
    id                           BIGSERIAL PRIMARY KEY,
    uuid                         VARCHAR(36) NOT NULL UNIQUE,
    name                         TEXT NOT NULL,
    address_range_cidr           TEXT NOT NULL,
    network                      TEXT NOT NULL,        -- full GCP network URL
    vpc_name                     TEXT NOT NULL,        -- parsed from network
    host_project_number          TEXT NOT NULL,        -- parsed from network
    lif_type                     TEXT NOT NULL DEFAULT 'dataLIF',
    lifecycle_state              TEXT NOT NULL DEFAULT 'CREATED',
    lifecycle_state_details      TEXT,
    apply_route_aggregation      BOOLEAN NOT NULL DEFAULT FALSE,
    route_aggregation_applied    BOOLEAN NOT NULL DEFAULT FALSE,
    route_aggregation_applied_at TIMESTAMPTZ,
    created_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at                   TIMESTAMPTZ            -- soft delete
);

CREATE INDEX idx_address_ranges_host_project ON address_ranges(host_project_number);
CREATE INDEX idx_address_ranges_vpc          ON address_ranges(vpc_name);
```

### Lifecycle States

```
CREATED ──(pool create)──► IN_USE ──(last pool on network deleted)──► CREATED
   │                          │
   │                          └──(another pool still uses this range)──► stays IN_USE
   │
   └──(operator update)──► DISABLED
   │
   └──(operator delete)──► DELETED  (soft delete, deleted_at set)
```

| State | Meaning |
|---|---|
| `CREATED` | Registered and available; passed to GCP as `RequestedRanges` on pool create |
| `IN_USE` | At least one active pool is using this range; still passed to GCP so new pools can share it |
| `DISABLED` | Excluded from pool creation (operator-managed) |
| `DELETED` | Soft-deleted; no longer returned by list |

### Uniqueness Constraints (enforced in code)
- No two active rows with the same `(vpc_name, host_project_number, address_range_cidr)`
- No two active rows with the same `(vpc_name, host_project_number, name)`
- Only **one** active `interclusterLIF` row per `(vpc_name, host_project_number)`

---

## 4. REST API

Base path served by **Core API** (`/v1/addressRange`).

| Method | Path | Description |
|---|---|---|
| `POST` | `/v1/addressRange` | Register a new address range |
| `GET` | `/v1/addressRange` | List (filterable by `hostProjectNumber`, `vpcName`, `lifType`, `addressRangeId`) |
| `GET` | `/v1/addressRange/{addressRangeId}` | Get a single range |
| `PUT` | `/v1/addressRange/{addressRangeId}` | Update name / CIDR / `applyRouteAggregation` |
| `PUT` | `/v1/addressRange/{addressRangeId}/updateState` | Transition lifecycle state |
| `DELETE` | `/v1/addressRange/{addressRangeId}` | Soft-delete (blocked if `IN_USE` or `route_aggregation_applied`) |

### Create Request Body

```json
{
  "addressRange":     "my-range-1",
  "addressRangeCidr": "10.55.55.0/24",
  "network":          "projects/394353340554/global/networks/large-volumes-test",
  "lifType":          "dataLIF"
}
```

The handler parses `network` to extract `vpcName` and `hostProjectNumber` automatically.

---

## 5. Control Flow Diagrams

### 5.1 — Register an Address Range (CRUD)

```
Operator
  │
  │  POST /v1/addressRange
  ▼
Core API (address_range_endpoint.go)
  │  parseNetworkString(network) → hostProjectNumber, vpcName
  │  GCPOrchestrator.CreateAddressRange(ctx, ar)
  ▼
GCP Orchestrator (factory/gcp/address_range.go)
  │  storage.CreateAddressRange(ctx, ar)
  ▼
DataStoreRepository (database/vcp/address_range.go)
  │  Duplicate CIDR check
  │  Duplicate name check
  │  interclusterLIF uniqueness check (if applicable)
  │  ar.UUID = RandomUUID()
  │  ar.LifeCycleState = "CREATED"
  │  GORM INSERT INTO address_ranges
  ▼
PostgreSQL (address_ranges table)
  │
  └──► 201 response with full AddressRange object
```

### 5.2 — Pool Creation with Address Space Management

```
Operator
  │
  │  POST /v1beta/projects/{proj}/locations/{loc}/storagePools
  ▼
Google Proxy (google-proxy/api/endpoints)
  │  Maps GCP request → CreatePoolParams{VendorSubNetID: "projects/.../networks/..."}
  │  Calls Core API: POST /v1/pools
  ▼
Core API → GCP Orchestrator (factory/gcp/pool.go :: createPool)
  │
  ├─ CreatePoolInDB()           → INSERT INTO pools (state=CREATING)
  ├─ CreateJob()                → INSERT INTO jobs
  │
  ├─ [ADDRESS_SPACE_MGMT_ENABLED=true]
  │     ParseProjectId(VendorSubNetID) → hostProjectNumber, vpcName
  │     ListAddressRanges(hostProjectNumber, vpcName, lifType="dataLIF")
  │       └─ SELECT * FROM address_ranges
  │              WHERE vpc_name=? AND host_project_number=?
  │              AND lif_type='dataLIF' AND deleted_at IS NULL
  │     Filter: LifeCycleState == "CREATED" || LifeCycleState == "IN_USE"
  │     (IN_USE included: another pool uses this range; GCP allocates a new block from it)
  │     params.RequestedRanges = ["my-range-1", ...]
  │
  └─ ExecuteWorkflow(CustomerTaskQueue, CreatePoolWorkflow, params)
       ▼
  Temporal Server
       ▼
  VCP Worker (CustomerTaskQueue)
       ▼
  CreatePoolWorkflow
    │  GetJob
    │  UpdateJobStatus
    │  FindTenancyProject
    │  [Child] DataSubnetSequentialPoller (Create)
    │            └─ PoolDataSubnetWorkFlow
    │                 GetAvailableSubnet / GetCreateDataSubnetOp
    │                 GCP Service Networking API
    │                   RequestedRanges: ["my-range-1"]
    │                 GetSubnetFromOperation → subnet.IpCidrRange (allocatedSubnetCIDR)
    │                 GetTenancyInfo → TenancyInfo{AllocatedSubnetCIDR: "10.55.55.16/29"}
    │                 UpdatePoolSubnet
    │                 wf.TenancyDetails = tenancyInfo   ← stored for query handler
    │            └─ GetTenancyDetails (query PoolDataSubnetWorkFlow)
    │                 returns TenancyInfo{AllocatedSubnetCIDR: "10.55.55.16/29"}
    │  SavePoolWithClusterDetails
    │    └─ DB: cluster_details.AllocatedSubnetCIDR = "10.55.55.16/29"  ← persisted
    │  [If ADDRESS_SPACE_MGMT_ENABLED && AllocatedSubnetCIDR != ""]
    │    MarkAddressRangeInUse(allocatedSubnetCIDR, network)
    │      └─ CIDR containment match → UPDATE address_ranges SET lifecycle_state='IN_USE'
    │  [Child] ConfigureNetworkWorkflow
    │  CreateServiceAccountWithStorageRole
    │  CreateAutoTierBucket
    │  CreateOnTapCredentials
    │  IdentifyVMs / IdentifySecondaryAndMediatorZone
    │  [Child] DeployCluster
    │  ...
    └─ pool.State → READY
```

### 5.3 — Pool Deletion with Address Space Management

```
DeletePoolWorkflow
  │
  ├─ GetPool                          → dbPool (with ClusterDetails.AllocatedSubnetCIDR from DB)
  ├─ DeletingPoolResources            → pool.State = DELETING
  │
  ├─ [hasClusterDetails]
  │   [Child] DataSubnetSequentialPoller (Delete)
  │            └─ PoolDataSubnetWorkFlow (Delete)
  │                 GetPool                → load pool from DB
  │                 GetPoolTenancyInfo     → getPoolAllocatedSubnetCIDR
  │                   1. If ClusterDetails.AllocatedSubnetCIDR != "" (new pools):
  │                        return it directly  ← no GCP call needed
  │                   2. Fallback (legacy pools without stored CIDR):
  │                        GCP GetSubnetwork by name  ← live lookup BEFORE deletion
  │                   3. If subnet already gone (out-of-band deletion):
  │                        return ""  ← CIDR unavailable; legacy fallback applies
  │                 tenancyInfo = TenancyInfo{AllocatedSubnetCIDR: "10.55.55.16/29"}
  │                 ReleaseDataSubnetOp + WaitForGCPNetworkOperationStatus
  │                   └─ GCP subnet is DELETED here
  │                 wf.TenancyDetails = tenancyInfo   ← stored BEFORE return
  │                 return tenancyInfo
  │            └─ GetTenancyDetails (query PoolDataSubnetWorkFlow)
  │                 returns TenancyInfo{AllocatedSubnetCIDR: "10.55.55.16/29"}
  │
  │   deletedSubnetDetails.AllocatedSubnetCIDR = "10.55.55.16/29"
  │   dbPool.ClusterDetails.AllocatedSubnetCIDR = "10.55.55.16/29"
  │
  │   MarkAddressRangesCreated(dbPool)
  │     └─ getPoolAllocatedSubnetCIDR → "10.55.55.16/29" (from dbPool.ClusterDetails)
  │     └─ findAddressRangeContainingSubnet(addressRanges, "10.55.55.16/29")
  │          → targetRange (the registered /24 that contains /29)
  │     └─ ListPools(network=pool.Network) → check remaining pools on same network
  │          For each remaining pool (excluding this one):
  │            getPoolAllocatedSubnetCIDR → remainingCIDR
  │            if subnetCIDRWithinRange(remainingCIDR, targetRange.AddressRangeCidr):
  │              → leave IN_USE (another pool still uses this range), return
  │     └─ If no remaining pool uses this range:
  │          UPDATE address_ranges SET lifecycle_state='CREATED'
  │
  ├─ DeletePoolResources              → pool soft-deleted in DB
  └─ ...
```

### 5.4 — Address Range DB State Transitions During Pool Lifecycle

```
                    ┌─────────────────────────────────────────────┐
                    │            address_ranges table              │
                    │                                              │
  Register Range    │   lifecycle_state = CREATED                  │
  ───────────────►  │   (available for pool creation)              │
                    │                                              │
  Pool Create       │   lifecycle_state = IN_USE                   │
  (MarkAddressRange │   (subnet allocated by GCP;                  │
   InUse activity)  │    AllocatedSubnetCIDR stored in             │
  ───────────────►  │    pool.ClusterDetails JSON)                 │
                    │                                              │
  Pool Delete /     │   lifecycle_state = CREATED                  │
  Rollback          │   (back to available;                        │
  (MarkAddressRanges│    CIDR read from ClusterDetails             │
   Created activity)│    before subnet deletion)                   │
  ───────────────►  │                                              │
                    └─────────────────────────────────────────────┘

  Operator DISABLE  lifecycle_state = DISABLED  (excluded from pool creation)
  Operator DELETE   lifecycle_state = DELETED + deleted_at set (soft delete)
```

### 5.5 — Delete an Address Range

```
Operator
  │
  │  DELETE /v1/addressRange/{uuid}
  ▼
Core API → GCP Orchestrator → DataStoreRepository
  │
  ├─ GetAddressRange(uuid)
  ├─ Guard: route_aggregation_applied == true  → 409 Conflict
  ├─ Guard: lifecycle_state == "IN_USE"        → 409 Conflict
  │
  └─ Soft delete:
       lifecycle_state        = "DELETED"
       lifecycle_state_details = "DELETED"
       deleted_at              = NOW()
       GORM Save
  ▼
PostgreSQL: row retained, deleted_at set; excluded from all future queries
```

---

## 6. AllocatedSubnetCIDR — How It Flows

`AllocatedSubnetCIDR` is the `/29` (or smaller) CIDR that GCP Service Networking actually carved out of the registered `/24` range for a pool's data subnet.

### On creation
1. `GetSubnetFromOperation` → `subnet.IpCidrRange` returned from GCP operation result.
2. Stored in `TenancyInfo.AllocatedSubnetCIDR` inside `PoolDataSubnetWorkFlow`.
3. Returned to `CreatePoolWorkflow` via `GetTenancyDetails`.
4. Persisted to `pool.ClusterDetails.AllocatedSubnetCIDR` in `SavePoolWithClusterDetails`.
5. Used by `MarkAddressRangeInUse` to identify which registered range contains the allocated subnet.

### On deletion
1. `getPoolAllocatedSubnetCIDR` reads `pool.ClusterDetails.AllocatedSubnetCIDR` from DB — **no GCP call for pools created with the new code**.
2. For legacy pools (field absent in DB), falls back to a live GCP `GetSubnetwork` call — this happens **before** `ReleaseDataSubnetOp` so the subnet still exists at lookup time.
3. The CIDR is captured in `TenancyInfo` and stored in `wf.TenancyDetails` **before** the subnet is deleted.
4. `DataSubnetSequentialPoller` (delete path) calls `GetTenancyDetails` after the subnet workflow completes, retrieving the pre-deletion CIDR.
5. The delete workflow updates `dbPool.ClusterDetails.AllocatedSubnetCIDR` from this result before calling `MarkAddressRangesCreated`.
6. `MarkAddressRangesCreated` uses the CIDR to find and reset the correct registered range.

---

## 7. Feature Flag

| Flag | Default | Where Set |
|---|---|---|
| `ADDRESS_SPACE_MGMT_ENABLED` | `false` | `skaffold/k8s/core.yaml`, `skaffold/k8s/vcp-worker.yaml`, `launch.json` |

**Behaviour when `false`:**
- `createPool` skips the address range lookup entirely — `RequestedRanges` is `nil`.
- `MarkAddressRangeInUse` / `MarkAddressRangesCreated` activities return immediately without any DB update.
- Pool creation and deletion proceed exactly as before.

---

## 8. Multi-Pool Behaviour (Shared IP Range)

Multiple pools can be created using the same registered IP range. GCP allocates a new `/29` subnet block from within the CIDR each time — until the range is exhausted, at which point GCP returns an error.

| Event | `RequestedRanges` sent to GCP | Address Range State |
|---|---|---|
| Register range `10.55.55.0/24` | — | `CREATED` |
| Create pool-1 | `["new-nis-range2"]` | `CREATED → IN_USE` |
| Create pool-2 | `["new-nis-range2"]` (IN_USE included) | stays `IN_USE` |
| Create pool-N | `["new-nis-range2"]` | stays `IN_USE` |
| GCP range exhausted | GCP returns error | — |
| Delete pool-1 (pool-2 still exists, uses same range) | — | stays `IN_USE` |
| Delete pool-2 (last pool using this range) | — | `IN_USE → CREATED` |

**How "last pool" is determined:**  
`MarkAddressRangesCreated` calls `ListPools(network=pool.Network)` and for each remaining pool (excluding the one being deleted) reads its `AllocatedSubnetCIDR`. If any remaining pool's subnet falls within `targetRange.AddressRangeCidr`, the range stays `IN_USE`. Only when no remaining pool uses the range is it reset to `CREATED`.

---

## 9. Validation Rules

| Rule | Layer |
|---|---|
| Duplicate CIDR per `(vpc_name, host_project_number)` blocked | `database/vcp/address_range.go` |
| Duplicate name per `(vpc_name, host_project_number)` blocked | `database/vcp/address_range.go` |
| Only one `interclusterLIF` per VPC | `database/vcp/address_range.go` |
| `IN_USE` range cannot be deleted | `database/vcp/address_range.go` |
| Range with `route_aggregation_applied=true` cannot be updated or deleted | `database/vcp/address_range.go` |
| `IN_USE` range: only `applyRouteAggregation` field is mutable | `database/vcp/address_range.go` |
| State transitions: only `CREATED ↔ IN_USE` via `UpdateAddressRangeState` | `database/vcp/address_range.go` |
| `lifType` field is required on create | Ogen-generated validation |

---

## 10. Key Code Pointers

| What | Path |
|---|---|
| DB migration | `database/vcp/migrations/post/0027_add_address_ranges.up.sql` |
| Data model struct | `core/datamodel/models.go` (`AddressRange`; `ClusterDetails.AllocatedSubnetCIDR`) |
| DB CRUD + state machine | `database/vcp/address_range.go` |
| Storage interface | `database/vcp/interface.go` |
| GCP orchestrator delegation | `core/orchestrator/factory/gcp/address_range.go` |
| Pool create + `RequestedRanges` lookup | `core/orchestrator/factory/gcp/pool.go` |
| `MarkAddressRangeInUse` | `core/orchestrator/activities/pool_activities.go` (~line 3844) |
| `MarkAddressRangesCreated` | `core/orchestrator/activities/pool_activities.go` (~line 3892) |
| `getPoolAllocatedSubnetCIDR` | `core/orchestrator/activities/pool_activities.go` (~line 3986) |
| `DataSubnetSequentialPoller` (create + delete) | `core/orchestrator/workflows/pool_workflows.go` (~line 1807) |
| `PoolDataSubnetWorkFlow` (delete case + `wf.TenancyDetails`) | `core/orchestrator/workflows/pool_workflows.go` (~line 1764) |
| REST handler | `vcp-core/handlers/address_range_endpoint.go` |
| OpenAPI spec | `vcp-core/api.yaml` |

---

## 11. Known Gaps / Future Work

- **OCI not implemented.** The OCI orchestrator has stub methods returning `nil` — address ranges are GCP-only today.

- **Route Aggregation** (`applyRouteAggregation` / `routeAggregationApplied`) fields are modelled and guarded in the DB layer but the aggregation workflow is not yet implemented.

- **Multi-range ambiguity on mark-in-use.** When multiple ranges are registered for the same VPC, VCP passes all of them as `RequestedRanges`. GCP picks one but does not indicate which named range it used. `MarkAddressRangeInUse` resolves this via CIDR containment on the allocated `/29` — this is accurate as long as registered CIDRs do not overlap. The `MarkAddressRangesCreated` counterpart uses the same containment approach on deletion.
