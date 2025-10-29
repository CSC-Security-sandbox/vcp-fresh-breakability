# Test Plan: Pool, Volume, Host Group & VMRS Integration for VSA Control Plane

**Document Version:** 1.0  
**Last Updated:** September 25, 2025  
**Status:** Approved  
**JIRA Tracker:** [CSA-1088](https://jira.ngage.netapp.com/browse/CSA-1088), [VSCP-776](https://jira.ngage.netapp.com/browse/VSCP-776), [CSA-1066](https://jira.ngage.netapp.com/browse/CSA-1066)  
**Component:** Pool, Volume, Host Group & VMRS Integration  
**Component Version:** 25083.0.0-RC.29

## 1. Introduction

### 1.1 Overview
This consolidated test plan merges prior Pool/Volume/HostGroup management coverage and the VMRS (Virtual Machine Right Sizing) / performance transition scenarios (from VSCP-776 and Confluence exports). It validates lifecycle operations, performance-based VM transitions, multi-tenant isolation, shared VPC scenarios, label & policy validation, negative/constraint enforcement, and operational resiliency.

### 1.2 Scope
- **In Scope:** Pool lifecycle, volume lifecycle, host group lifecycle, pool update constraints, VMRS performance transitions (throughput / IOPS / combined), shared VPC & multi-project operations, label validation, cross-region listing (where supported), negative API behavior, multi-field updates, audit & constraint validation, multi-tenant isolation, scale & performance.
- **Out of Scope:** Billing & detailed metrics aggregation (beyond functional signals), underlying ONTAP micro-benchmarking, full network throughput benchmarking, backup/restore (separate suite), CMEK specifics (separate suite), telemetry pipeline validation, UI-specific flows.

### 1.3 Related Requirements
- [CSA-1088] Core storage resource management
- [VSCP-776] Storage pool update & VMRS transition validation
- [CSA-1066] Host group & access management
- Architecture Decision: VMRS Preview (doc/architecture/decisions/0005-vmrs-for-preview.md)

## 2. Test Requirements

### 2.1 Requirements to be Tested
- Create / list / describe / delete pools (all service levels, unified FLEX)
- Update pools (allowed vs immutable fields)
- Enforce update constraints (size cannot shrink, zone immutable, etc.)
- Performance updates: throughput / IOPS / combined → VMRS transitions
- Label CRUD + validation (count, key/value length, UTF-8 encoded length)
- Volume lifecycle operations & snapshot policy linkage (basic)
- HostGroup lifecycle & access membership changes
- Multi-project & shared VPC isolation and scoping
- Cross-region / wildcard list behaviors (where enabled)
- Negative scenarios for invalid IDs, location formats, permissions
- Multi-field transactional updates & idempotent no-op updates
- VMRS constraint formula enforcement (IOPS ≥ Throughput × 16)
- Scale boundaries (pool count, volume count)
- Concurrency & operational resilience (simultaneous creates/updates)
- Known issues regression (VSCP-1738, VSCP-1758)

### 2.2 Requirements NOT in Scope
- Deep performance benchmarking outside acceptance thresholds
- Cost optimization algorithms (beyond VM instance selection)
- Automated billing export integrity
- End-user UI workflow acceptance (API level only here)

## 3. Test Environment Requirements
### 3.1 Infrastructure
- GCP project(s) (host + service for shared VPC tests)
- Regions/zones per config (at least 2 for isolation tests)
- VMRS configuration file `/config/vmrs_gcp.yaml`
- Temporal / workflow engine & Google proxy deployed

### 3.2 Test Data
- Pools across service levels (Standard, Premium, Extreme, FLEX(unified true), Flex)
- Volumes with varied sizes & snapshot policy examples
- Host groups with mixed host membership
- Label edge case datasets (empty key, >64 labels, overlength Unicode)

### 3.3 Dependencies
- IAM perms (create/list/update/delete pools/volumes/hostgroups)
- VMRS sizing logic & config
- Logging & metrics (for constraint validation)

## 4. Test Categories

### 4.1 Functional Testing
- Pool / volume / host group lifecycle
- VMRS trigger validation embedded within functional update scenarios

### 4.2 Integration Testing
- Multi-project (host + service) shared VPC
- Cross-region / wildcard listing (where enabled)
- Volume migration between pools
- End-to-end stack (pool→volume→host group access path)

### 4.3 Performance Testing
- VMRS performance transitions (throughput / IOPS / combined)
- Scale & concurrency (parallel create / update / delete)
- Threshold ramp & scheduling fairness

### 4.4 Security Testing
- Tenant isolation (no cross-project visibility)
- Access / permission enforcement (immutable field protection)
- Shared VPC isolation & scoped listing
- Label validation preventing metadata abuse

### 4.5 Negative Testing
- Forbidden updates (size shrink, zone migrate, immutable fields)
- Invalid identifiers, invalid location format
- Label constraints (count, key/value size, UTF-8 length)
- No suitable VM errors & performance constraint violations
- Non-existent resource operations (404)

## 5. Risk Assessment
| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| Incorrect VM sizing (VMRS mis-selection) | High | Medium | Constraint validation & regression scenarios |
| Pool update violates immutability | High | Low | Negative tests & schema enforcement |
| Performance transition instability | High | Medium | Staggered execution & monitoring |
| Label injection / malformed metadata | Medium | Medium | Validation & negative cases |
| Cross-project leakage | High | Low | Isolation & access denial tests |

## 6. Test Execution Strategy
### 6.1 Phases
1. Core lifecycle & validation
2. VMRS performance transitions & multi-field updates
3. Negative & isolation cases
4. Scale & concurrency stress
5. Regression & known issues
### 6.2 Automation Strategy
- API test harness (scripts / CI) for CRUD & updates
- Parametric generation for label & performance permutations
### 6.3 Manual Testing Strategy
- Long-running VMRS transitions (≈46 min each) scheduling
- Cross-project & shared VPC verification

## 7. Success Criteria
- 100% required test cases executed (P0/P1) with pass or accepted deviations
- All constraints enforced (no forbidden updates succeed)
- VM transitions align with expected instance mapping
- No data / state corruption during transitions

## 8. Test Schedule
- Phase 1: 2025-09-26 – 2025-09-30
- Phase 2: 2025-10-01 – 2025-10-06
- Phase 3: 2025-10-07 – 2025-10-11
- Phase 4: 2025-10-12 – 2025-10-16
- Closure: 2025-10-17 – 2025-10-18

## 9. Test Team
- **Test Lead:** TBD  
- **Test Engineers:** TBD  
- **Automation Engineers:** TBD  
- **Subject Matter Experts:** TBD  

## 10. Deliverables
- This consolidated test plan
- Test case matrix & automation scripts
- Execution & transition logs
- Defect & deviation reports

---
## Appendices

### Appendix A: Detailed Test Cases

#### Section 1: Pool Creation Test Cases

Summary Table:
| ID | Title | Priority | Type | Automated |
|----|-------|----------|------|-----------|
| TC-POOL-CREATE-001 | Basic creation (mandatory only) | P0 | Functional | Yes |
| TC-POOL-CREATE-002 | Creation across service levels | P0 | Functional | Yes |
| TC-POOL-CREATE-003 | Custom performance + VMRS selection | P0 | Functional | Yes |
| TC-POOL-CREATE-004 | Creation with valid labels | P1 | Functional | Yes |
| TC-POOL-CREATE-005 | Maximum capacity pool | P1 | Functional | Yes |
| TC-POOL-CREATE-006 | Random size distribution set | P2 | Functional | Yes |
| TC-POOL-CREATE-007 | Concurrent creation (same account/VPC) | P1 | Concurrency | Yes |
| TC-POOL-CREATE-008 | Cross-project isolation (shared VPC) | P1 | Isolation | Yes |
| TC-POOL-CREATE-009 | Cross-region creation | P2 | Functional | Yes |
| TC-POOL-CREATE-NEG-001 | Invalid service level | P0 | Negative | Yes |
| TC-POOL-CREATE-NEG-002 | Performance constraint violation | P0 | Negative | Yes |
| TC-POOL-CREATE-NEG-003 | No suitable VM (VMRS) | P1 | Negative | Yes |
| TC-POOL-CREATE-NEG-004 | Insufficient permissions | P0 | Negative | Yes |
| TC-POOL-CREATE-NEG-005 | Duplicate name conflict | P1 | Negative | Yes |

```
Scenario: TC-POOL-CREATE-001 Basic creation (mandatory only)
  Given an authenticated user with pool create permissions
    And a project with available pool quota
    And no existing pool named "pool-basic-001" in region "us-central1"
  When the user submits POST /v1/pools with { name, serviceLevel:"Standard", sizeGb: 1000 }
  Then the API responds 201 Created with a valid poolId
    And the pool provisioningStatus becomes READY within 120s
    And the persisted attributes match the request payload
    And an audit event "POOL_CREATE" is emitted
```
```
Scenario: TC-POOL-CREATE-002 Creation across service levels
  Given service levels Standard, Premium, Extreme, FLEX(unified true), Flex are allowed
  When the user creates one pool per service level with minimal required fields
  Then each create responds 201 Created
    And each pool reaches READY within 180s
    And serviceLevel on each pool equals the requested level
```
```
Scenario: TC-POOL-CREATE-003 Custom performance + VMRS selection
  Given a VMRS config enforcing IOPS ≥ Throughput×16
    And no existing custom performance pools
  When the user creates a pool with customPerformanceEnabled=true, totalThroughputMibps=165, totalIops=3000
  Then the API responds 201 Created
    And the chosen VM instance class matches VMRS expected sizing
    And metrics show provisioned IOPS ≥ 165×16
```
```
Scenario: TC-POOL-CREATE-004 Creation with valid labels
  Given label policy constraints (≤64 labels, key/value ≤63 chars, UTF-8 key bytes ≤128)
  When the user creates a pool with 5 valid labels
  Then the API responds 201 Created
    And all labels are persisted unchanged
    And no label validation errors are logged
```
```
Scenario: TC-POOL-CREATE-005 Maximum capacity pool
  Given documented maximum pool size MAX_POOL_SIZE_GB
    And sufficient quota for that size
  When the user creates a pool at MAX_POOL_SIZE_GB
  Then the API responds 201 Created
    And provisioning completes within 10m
    And reported sizeGb equals MAX_POOL_SIZE_GB
```
```
Scenario: TC-POOL-CREATE-006 Random size distribution set
  Given a list of N (3..5) random valid pool sizes within limits
  When the user creates pools for each size sequentially
  Then each request returns 201 Created
    And each pool size matches its requested value
```
```
Scenario: TC-POOL-CREATE-007 Concurrent creation (same account/VPC)
  Given the account has quota for 5 pools
  When 5 create requests are issued in parallel
  Then all requests succeed with 201 Created
    And no two operations exceed concurrency error thresholds
    And average READY time ≤ 180s
```
```
Scenario: TC-POOL-CREATE-008 Cross-project isolation (shared VPC)
  Given a host project H and a service project S (shared VPC configured)
  When a pool is created from project S referencing shared network in H
  Then the pool is created successfully
    And listing pools in project H does not expose pools owned by S (unless via combined view API)
    And IAM audit logs show correct originating project
```
```
Scenario: TC-POOL-CREATE-009 Cross-region creation
  Given regions us-central1 and us-east1 are enabled
  When the user creates a pool in each region
  Then both operations succeed
    And region field matches the target region for each pool
```
```
Scenario: TC-POOL-CREATE-NEG-001 Invalid service level
  Given allowed service levels Standard, Premium, Extreme, FLEX(unified true), Flex
  When the user attempts to create a pool with serviceLevel="UltraPlus"
  Then the API responds 400 Bad Request with errorCode "INVALID_SERVICE_LEVEL"
    And no pool resource is created
```
```
Scenario: TC-POOL-CREATE-NEG-002 Performance constraint violation
  Given VMRS constraint IOPS ≥ Throughput×16
  When the user creates a pool with totalThroughputMibps=200 and totalIops=1000 (violation)
  Then the API responds 400 Bad Request with errorCode "PERFORMANCE_CONSTRAINT_VIOLATION"
    And validation details include "IOPS must be ≥ Throughput×16"
```
```
Scenario: TC-POOL-CREATE-NEG-003 No suitable VM (VMRS)
  Given VMRS has no instance mapping for (throughput=10_000 MiBps, iops=2_000_000)
  When the user creates a pool requesting those performance values
  Then the API responds 422 Unprocessable Entity with errorCode "NO_SUITABLE_VM"
    And guidance text suggests reducing requested performance
```
```
Scenario: TC-POOL-CREATE-NEG-004 Insufficient permissions
  Given a user lacking the iam.pools.create permission
  When the user submits a create pool request
  Then the API responds 403 Forbidden with errorCode "ACCESS_DENIED"
    And no audit event of type POOL_CREATE success is emitted
```
```
Scenario: TC-POOL-CREATE-NEG-005 Duplicate name conflict
  Given an existing pool named "dup-pool-01" in region us-central1
  When the user attempts to create another pool with name "dup-pool-01" in the same region and account
  Then the API responds 409 Conflict with errorCode "POOL_NAME_ALREADY_EXISTS"
```
---
#### Section 2: Pool Update Test Cases

Summary Table:
| ID | TCASE | Title | Priority | Type | Automated |
|----|-------|-------|----------|------|-----------|
| TC-POOL-UPDATE-001 | 10 | Update description | P1 | Functional | Yes |
| TC-POOL-UPDATE-002 | 11 | Increase size | P0 | Functional | Yes |
| TC-POOL-UPDATE-THROUGHPUT | 12 | Update throughput (no transition) | P1 | Functional | Yes |
| TC-POOL-UPDATE-IOPS | 13 | Update IOPS (moderate) | P1 | Functional | Yes |
| TC-POOL-UPDATE-003 | 14 | Update labels | P1 | Functional | Yes |
| TC-POOL-UPDATE-004 | 22 | Multi-field update | P0 | Functional | Yes |
| TC-POOL-UPDATE-005 | 23 | No-op update (empty body) | P2 | Functional | Yes |
| TC-POOL-UPDATE-VMRS-001 | 24 | Throughput upgrade (transition) | P0 | VMRS | Yes |
| TC-POOL-UPDATE-VMRS-002 | 25 | Throughput downgrade | P0 | VMRS | Yes |
| TC-POOL-UPDATE-VMRS-003 | 26 | IOPS major upgrade | P0 | VMRS | Yes |
| TC-POOL-UPDATE-VMRS-004 | 27 | Combined size + throughput | P0 | VMRS | Yes |
| TC-POOL-UPDATE-NEG-001 | 1 | Prevent size reduction | P0 | Negative | Yes |
| TC-POOL-UPDATE-NEG-002 | 2 | Zone migration blocked | P0 | Negative | Yes |
| TC-POOL-UPDATE-NEG-003A | 3 | Global access immutable | P1 | Negative | Yes |
| TC-POOL-UPDATE-NEG-003B | 4 | Active Directory config immutable | P1 | Negative | Yes |
| TC-POOL-UPDATE-NEG-003C | 5 | Auto-tiering immutable | P1 | Negative | Yes |
| TC-POOL-UPDATE-NEG-004 | 6 | Hot tier size update blocked | P1 | Negative | Yes |
| TC-POOL-UPDATE-NEG-005 | 7 | Hot tier auto-resize blocked | P1 | Negative | Yes |
| TC-POOL-UPDATE-NEG-006 | 8 | QoS type immutable | P1 | Negative | Yes |
| TC-POOL-UPDATE-NEG-007 | 9 | customPerformanceEnabled cannot be disabled | P1 | Negative | Yes |
| TC-POOL-UPDATE-NEG-008 | 15 | Invalid label count >64 | P1 | Negative | Yes |
| TC-POOL-UPDATE-NEG-009 | 16 | Empty label key | P1 | Negative | Yes |
| TC-POOL-UPDATE-NEG-010 | 17 | Label key length >63 | P1 | Negative | Yes |
| TC-POOL-UPDATE-NEG-011 | 18 | Label value length >63 | P1 | Negative | Yes |
| TC-POOL-UPDATE-NEG-012 | 19 | UTF-8 key bytes >128 | P1 | Negative | Yes |
| TC-POOL-UPDATE-NEG-013 | 20 | Non-existent pool | P0 | Negative | Yes |
| TC-POOL-UPDATE-NEG-014 | 21 | Invalid location format | P1 | Negative | Yes |

```
Scenario: TC-POOL-UPDATE-002 Increase size (TCASE-11)
  Given an existing READY pool "pool-grow-01" with sizeGb=1000
  When the user PATCH /v1/pools/{id} with sizeGb=1500
  Then the API responds 200 OK
    And sizeGb becomes 1500
    And provisioningStatus returns to READY within 180s
    And an audit event "POOL_UPDATE" records oldSize=1000 newSize=1500
```
```
Scenario: TC-POOL-UPDATE-THROUGHPUT Update throughput (non-transition baseline, TCASE-12)
  Given a custom performance pool with throughput 65 MiBps and IOPS 1040
  When the user updates totalThroughputMibps to 100
  Then the API responds 200 OK
    And totalThroughputMibps=100 and totalIops unchanged
    And IOPS ≥ Throughput×16 still holds
```
```
Scenario: TC-POOL-UPDATE-IOPS Update IOPS (moderate, TCASE-13)
  Given a pool with totalThroughputMibps=165 and totalIops=3000
  When the user updates totalIops to 4000
  Then the API responds 200 OK
    And totalIops=4000
    And provisioned instance class unchanged (no transition threshold crossed)
```
```
Scenario: TC-POOL-UPDATE-004 Multi-field update (TCASE-22)
  Given a pool with sizeGb=2000 throughput=165 IOPS=3000 description="orig"
  When the user submits a multi-field PATCH changing description, sizeGb→2500, throughput→300, IOPS→4800
  Then the API responds 200 OK
    And all updated fields reflect new values
    And a single audit event enumerates all changed fields
```
```
Scenario: TC-POOL-UPDATE-005 No-op empty body (TCASE-23)
  Given a pool in READY state
  When the user submits PATCH /v1/pools/{id} with an empty JSON object
  Then the API responds 200 OK
    And no fields change
    And no new audit event is emitted (or a minimal no-op acknowledgement, per spec)
```
```
Scenario: TC-POOL-UPDATE-VMRS-001 Throughput upgrade transition (TCASE-24)
  Given a pool at throughput 65 MiBps on instance class c3-standard-4-lssd
  When throughput is increased to 1000 MiBps
  Then the API responds 202 Accepted (asynchronous)
    And progress events show VMRS transition start & complete within 45m
    And final instance class matches expected (e.g., class-44)
    And IOPS ≥ Throughput×16 constraint validated pre & post
```
```
Scenario: TC-POOL-UPDATE-VMRS-003 IOPS major upgrade (TCASE-26)
  Given a pool at 1,040 IOPS (65 MiBps) on a 4-core instance
  When totalIops is increased to 160000
  Then the API responds 202 Accepted
    And instance class upgrades to 88 cores
    And transition completes within 60m
    And no data loss or pool unavailability events occur
```
```
Scenario: TC-POOL-UPDATE-NEG-001 Prevent size reduction (TCASE-1)
  Given a pool sized 2000 GB
  When the user attempts to PATCH sizeGb=1500
  Then the API responds 400 Bad Request with errorCode "POOL_SIZE_DECREASE_FORBIDDEN"
    And sizeGb remains 2000
```
```
Scenario: TC-POOL-UPDATE-NEG-013 Non-existent pool (TCASE-20)
  Given no pool exists with id "deadbeef"
  When the user PATCH /v1/pools/deadbeef with sizeGb=2000
  Then the API responds 404 Not Found with errorCode "POOL_NOT_FOUND"
```
// (Representative scenarios shown; remaining negatives follow same template referencing table above.)

#### Section 3: Pool Deletion Test Cases

Summary Table:
| ID | Title | Priority | Type | Automated |
|----|-------|----------|------|-----------|
| TC-POOL-DELETE-001 | Delete empty pool | P0 | Functional | Yes |
| TC-POOL-DELETE-002 | Delete one of many pools | P1 | Functional | Yes |
| TC-POOL-DELETE-003 | Force delete with volumes | P0 | Functional | Yes |
| TC-POOL-DELETE-NEG-001 | Delete with volumes (no force) | P0 | Negative | Yes |
| TC-POOL-DELETE-NEG-002 | Delete non-existent pool | P1 | Negative | Yes |
| TC-POOL-DELETE-NEG-003 | Delete transitional state pool | P1 | Negative | Yes |

```
Scenario: TC-POOL-DELETE-001 Delete empty pool
  Given an existing READY pool with no volumes
  When the user DELETE /v1/pools/{id}
  Then the API responds 204 No Content
    And subsequent GET returns 404 after eventual consistency delay ≤30s
```
```
Scenario: TC-POOL-DELETE-NEG-001 Delete with volumes (no force)
  Given a READY pool containing 2 volumes
  When the user DELETE /v1/pools/{id} without force flag
  Then the API responds 400 Bad Request with errorCode "POOL_NOT_EMPTY"
    And the pool remains in READY
```
```
Scenario: TC-POOL-DELETE-003 Force delete with volumes
  Given a pool containing 2 volumes
  When the user DELETE /v1/pools/{id}?force=true
  Then the API responds 202 Accepted
    And volumes enter DELETING then are removed
    And pool enters DELETING then is gone within 10m
```

#### Section 4: Volume Management

Summary Table:
| ID | Title | Priority | Type | Automated |
|----|-------|----------|------|-----------|
| TC-VOLUME-CREATE-001 | Basic creation | P0 | Functional | Yes |
| TC-VOLUME-CREATE-002 | With snapshot policy | P1 | Functional | Yes |
| TC-VOLUME-CREATE-003 | Multiple volumes same pool | P1 | Functional | Yes |
| TC-VOLUME-UPDATE-001 | Expand size | P0 | Functional | Yes |
| TC-VOLUME-UPDATE-002 | Update labels | P1 | Functional | Yes |
| TC-VOLUME-DELETE-001 | Delete volume | P0 | Functional | Yes |
| TC-VOLUME-DELETE-002 | Delete volume with snapshots | P1 | Functional | Yes |

```
Scenario: TC-VOLUME-CREATE-001 Basic volume creation
  Given a READY pool with available capacity
  When the user creates a volume { name:"vol-basic-01", sizeGb:100 } in that pool
  Then the API responds 201 Created
    And volume status becomes READY within 120s
    And sizeGb=100 persisted
```
```
Scenario: TC-VOLUME-UPDATE-001 Expand size
  Given a volume sizeGb=100
  When the user PATCH /v1/volumes/{id} sizeGb=200
  Then the API responds 200 OK
    And sizeGb=200
    And I/O remains available during expansion
```
```
Scenario: TC-VOLUME-DELETE-002 Delete volume with snapshots
  Given a volume with 2 snapshots
  When the user DELETE /v1/volumes/{id}?cascade=true
  Then the API responds 202 Accepted
    And snapshots and volume are removed within 10m
```

#### Section 5: Host Group Management

Summary Table:
| ID | Title | Priority | Type | Automated |
|----|-------|----------|------|-----------|
| TC-HOSTGROUP-CREATE-001 | Basic creation + hosts | P0 | Functional | Yes |
| TC-HOSTGROUP-CREATE-002 | With access policies | P1 | Functional | Yes |
| TC-HOSTGROUP-UPDATE-001 | Add hosts | P1 | Functional | Yes |
| TC-HOSTGROUP-UPDATE-002 | Remove hosts | P1 | Functional | Yes |
| TC-HOSTGROUP-DELETE-001 | Delete empty host group | P0 | Functional | Yes |
| TC-HOSTGROUP-DELETE-002 | Delete with active connections | P1 | Negative | Yes |

```
Scenario: TC-HOSTGROUP-CREATE-001 Basic host group creation
  Given two registered hosts hostA and hostB
  When the user creates a host group with hosts [hostA, hostB]
  Then the API responds 201 Created
    And host group status READY
    And membership lists hostA and hostB
```
```
Scenario: TC-HOSTGROUP-UPDATE-002 Remove hosts
  Given a host group with hosts [hostA, hostB]
  When the user PATCH removes hostB
  Then the API responds 200 OK
    And membership now contains only hostA
```

#### Section 6: VMRS Performance Transitions & Constrained Scenarios

Representative Transition Scenarios:
```
Scenario: VMRS-TRANS-THROUGHPUT Upgrade mid→high
  Given a pool at 165 MiBps / 2640 IOPS on instance class mid-tier
  When throughput is increased to 1000 MiBps (IOPS auto-adjust 16000)
  Then a VMRS transition is initiated
    And the new instance class supports required throughput
    And transition completes ≤45m with zero FAILED steps
```
```
Scenario: VMRS-TRANS-DOWNGRADE Throughput downgrade
  Given a pool at 1000 MiBps / 16000 IOPS
  When throughput is decreased to 165 MiBps (IOPS 3000)
  Then the instance class downgrades appropriately
    And no residual over-provisioned resources remain
```
```
Scenario: VMRS-CONSTRAINT Violation blocked
  Given a pool at 165 MiBps / 3000 IOPS
  When the user attempts update totalThroughputMibps=500 with totalIops=4000 (< 500×16)
  Then API responds 400 Bad Request errorCode "PERFORMANCE_CONSTRAINT_VIOLATION"
```

#### Section 7: Integration & Multi-Project / Shared VPC / Isolation

Summary Table:
| ID | Title | Priority | Type | Automated |
|----|-------|----------|------|-----------|
| TC-INTEGRATION-001 | End-to-end stack | P0 | Integration | Yes |
| TC-INTEGRATION-002 | Volume migration between pools | P1 | Integration | Yes |
| TC-INTEGRATION-003 | Multi-tenant isolation | P0 | Security | Yes |
| TC-INTEGRATION-004 | Shared VPC deployment | P1 | Integration | Yes |
| TC-INTEGRATION-005 | Cross-region listing | P2 | Functional | Yes |
| TC-INTEGRATION-006 | Same names across projects | P2 | Isolation | Yes |

```
Scenario: TC-INTEGRATION-001 End-to-end stack
  Given a pool, a volume in that pool, and a host group with a host
  When the volume is attached (exposed) via host group access workflow
  Then host sees exported volume
    And I/O succeeds (read/write sample)
    And audit events for POOL_CREATE,VOLUME_CREATE,HOSTGROUP_CREATE,ATTACH exist
```
```
Scenario: TC-INTEGRATION-003 Multi-tenant isolation
  Given tenant A has poolA and tenant B has poolB
  When tenant A lists pools
  Then poolB is not returned
    And attempts by tenant B to update poolA return 403
```

#### Section 8: Performance & Scale

Summary Table:
| ID | Title | Priority | Type | Automated |
|----|-------|----------|------|-----------|
| TC-PERFORMANCE-001 | VMRS optimization threshold ramp | P1 | Performance | Yes |
| TC-PERFORMANCE-002 | Concurrent operations mix | P1 | Performance | Yes |
| TC-SCALE-001 | Max pool count per tenant | P2 | Scale | Yes |
| TC-SCALE-002 | Max volume count per pool | P2 | Scale | Yes |
| TC-SCALE-003 | Parallel VMRS transitions fairness | P2 | Performance | Yes |

```
Scenario: TC-PERFORMANCE-002 Concurrent operations mix
  Given plan to run 10 create, 10 update, 5 delete operations in parallel
  When operations are launched concurrently
  Then overall error rate < 2%
    And median create READY time ≤ 180s
    And no deadlocks or throttling errors exceed threshold
```
```
Scenario: TC-SCALE-001 Max pool count per tenant
  Given limit MAX_POOLS_PER_TENANT
  When the user creates pools up to that limit
  Then the final allowed create returns 201
    And one additional create attempt returns 429 or 400 with errorCode "POOL_LIMIT_REACHED"
```

#### Section 9: Error Handling & Edge Cases

Summary Table:
| ID | Title | Priority | Type | Automated |
|----|-------|----------|------|-----------|
| TC-ERROR-001 | GCP service disruption | P1 | Resilience | Partial |
| TC-ERROR-002 | Network connectivity interruptions | P1 | Resilience | Partial |
| TC-EDGE-001 | Resource limit boundary tests | P2 | Edge | Yes |
| TC-EDGE-002 | Rapid sequential updates | P2 | Consistency | Yes |
| TC-EDGE-003 | Idempotent retry of update requests | P1 | Consistency | Yes |

```
Scenario: TC-EDGE-003 Idempotent retry of update requests
  Given a pending update request with client retry token R
  When the same PATCH is retried 3 times due to client timeouts
  Then only one update is applied
    And API returns identical idempotency key and final resource version each retry
```
```
Scenario: TC-ERROR-001 GCP service disruption
  Given a pool update in progress
  When underlying dependency (e.g., compute API) returns transient 5xx for 2 minutes
  Then workflow retries with exponential backoff
    And update ultimately succeeds within SLA (≤45m) without manual intervention
```

#### Section 10: Test Summary & Metrics

| Area | Test Cases | P0 | P1 | P2 |
|------|------------|----|----|----|
| Pool Creation | 14 | 5 | 6 | 3 |
| Pool Updates (incl. VMRS) | 30 | 12 | 12 | 6 |
| Pool Deletion | 6 | 2 | 3 | 1 |
| Volume Management | 7 | 3 | 3 | 1 |
| Host Group | 6 | 3 | 2 | 1 |
| Integration & Isolation | 6 | 3 | 2 | 1 |
| Performance & Scale | 5 | 1 | 3 | 1 |
| Error & Edge | 5 | 1 | 2 | 2 |
| **Total** | **79** | **30** | **33** | **16** |

## VMRS Constraints
- Primary formula: IOPS ≥ Throughput × 16 (validated pre/post update)
- Large jump validation: ensure new instance class supports required headroom

## Known Issues / Tracking
| Issue | Description | Impact | Mitigation |
|-------|-------------|--------|------------|
| VSCP-1738 | CCFE Get Operation intermittent failure → duplicate PUT | Medium | Retry & log correlation IDs |
| VSCP-1758 | Multi-field update failure (ONTAP scale) | Medium | Segregate updates; capture failure traces |

### Appendix B: Test Data Specifications
- Service level matrix covering: Standard, Premium, Extreme, FLEX (unified true), Flex.
- Performance profiles: baseline (65 MiBps / 1,040 IOPS), mid (165 MiBps / 2,640 IOPS), high (1,000 MiBps / 16,000 IOPS), extreme (160,000 IOPS major jump).
- Label edge sets: >64 labels, empty key, >63 char key/value, >128 byte UTF-8 key.
- VMRS transition datasets: initial & target (IOPS, throughput, instance type pairs).
- Shared VPC projects: host project (export IP A), service project (export IP B).

### Appendix C: Environment Setup
1. Provision GCP projects (host & service) and enable required APIs.
2. Deploy workflow engine & Google proxy; configure IAM roles for pool/volume operations.
3. Place `vmrs_gcp.yaml` into `config/` and validate checksum if enforced.
4. Set region/zone environment variables; for cross-region tests redeploy with alternate region.
5. Initialize test data (baseline pool + volume) and seed label datasets.
6. Configure monitoring/log collection for capturing VMRS decision & operation IDs.

## Related Documents
- VMRS Config: `/config/vmrs_gcp.yaml`

## Known Limitations and Considerations
- Pool reuse cooling period (<4h) extends sequential VMRS scenario wall-clock time.
- Cross-region listing limited by single-region deployment variable (redeploy required for multi-region validation).
- Major performance jumps lengthen workflow duration (timeout risk in constrained environments).
- Volume migration assumes backend non-disruptive move; fallback not covered.
- Cost / billing validation out of scope (only instance selection correctness).

---
**Document Version:** 1.0  
**Review Status:** Approved  
**Data Protection Classification:** Internal