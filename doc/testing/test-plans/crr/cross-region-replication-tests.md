# Test Plan: Cross-Region Replication (CRR)

**Document Version:** 1.0  
**Last Updated:** September 25, 2025  
**Status:** Draft  
**JIRA Tracker:** [CSA-1082](https://jira.ngage.netapp.com/browse/CSA-1082)  
**Component:** Cross-Region Replication (CRR) & Data Replication Management  
**Component Version:** 25093.0.0-RC.32  
**Data Protection Classification:** Internal

## 1. Introduction

### 1.1 Overview
This document contains BDD-formatted test scenarios for Cross-Region Replication (CRR). CRR replicates volume data between regions for disaster recovery, protection, and business continuity.

### 1.2 Scope
- In Scope: CRR policy lifecycle, cross-region replication, failover/failback, consistency, CMEK & Backup integration, performance, security, error handling.
- Out of Scope: Non-CRR replication features unrelated to VSA volumes.

### 1.3 Related Requirements
- [CSA-1082](https://jira.ngage.netapp.com/browse/CSA-1082)
- SOC 2 Type II, GDPR

## 2. Test Requirements

### 2.1 Requirements to be Tested
- Policy lifecycle (Create/Update/Delete)
- Replication setup & monitoring
- Failover & failback operations
- Data consistency & integrity
- CMEK & Backup integration
- Performance, network resilience
- Security & compliance
- Error & recovery scenarios

### 2.2 Requirements NOT in Scope
- Non-CRR replication features

## 3. Test Environment Requirements
- Multi-region VSA deployment, CRR-enabled
- Source & target GCP projects / networking
- Monitoring & alerting stack

## 4. Test Categories
- Functional (policies, replication)
- Failover/Failback & DR
- Cross-feature integration
- Performance & monitoring
- Security & compliance
- Error handling & recovery

## 5. Risk Assessment
| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| Replication failure (network) | High | Medium | Redundant paths, alerting |
| Data corruption | High | Low | Integrity & checksum validation |
| Compliance violation | High | Low | Policy enforcement & auditing |
| Resource exhaustion | Medium | Medium | Throttling, monitoring |

## 6. Test Execution Strategy
1. Policies & replication baseline
2. Failover/failback + integration + performance
3. Security, compliance, error & recovery

## 7. Success Criteria
- RTO/RPO targets met
- Zero silent corruption
- All scenarios pass; compliance satisfied

## 8. Test Schedule
- Phase 1: 2025-09-26–10-02
- Phase 2: 2025-10-03–10-09
- Phase 3: 2025-10-10–10-16

## 9. Test Team
Roles TBD (Lead, Engineers, Automation, SMEs)

## 10. Deliverables
Suite (this file), execution & coverage reports, defects, automation artifacts

---

# Section 1: CRR Policy Creation 

## Scenario Summary
| ID | Title | Priority | Type | Automation |
|----|-------|----------|------|------------|
| TC-CRR-CREATE-001 | Basic CRR Policy Creation | P0 | Positive | Yes |
| TC-CRR-CREATE-002 | Policy with Custom Schedule | P1 | Positive | Yes |
| TC-CRR-CREATE-003 | Policy with CMEK Integration | P0 | Positive | Yes |
| TC-CRR-CREATE-004 | Multi-Region CRR Policy | P2 | Positive | Partial |
| TC-CRR-CREATE-NEG-001 | Invalid Region Configuration | P1 | Negative | Yes |
| TC-CRR-CREATE-NEG-002 | Insufficient Regional Permissions | P1 | Negative | Yes |

### Scenario: TC-CRR-CREATE-001 Basic CRR Policy Creation
Given valid source region us-west1 and target region us-east1 with network connectivity  
And required IAM permissions exist  
When I create a CRR policy with daily 02:00 schedule  
Then policy status transitions to READY  
And network peering / service accounts are provisioned  
And policy becomes assignable to volumes

### Scenario: TC-CRR-CREATE-002 Policy with Custom Schedule
Given regions us-central1 and europe-west1  
When I create a policy with HOURLY interval=4 start 00:00 and retention count=168  
Then replication jobs execute every 4 hours within ±2m tolerance  
And retention pruning enforces maximum 168 recovery points

### Scenario: TC-CRR-CREATE-003 Policy with CMEK Integration
Given CMEK policies available in both regions and cross-region key permissions  
When I create a CRR policy referencing CMEK encryption  
Then replication metadata captures encryption key references  
And replicated data remains encrypted end-to-end

### Scenario: TC-CRR-CREATE-004 Multi-Region CRR Policy
Given a source region and multiple target regions (≥2)  
When I create a multi-target replication policy  
Then independent replication streams initialize per target  
And each target's health/status is monitored separately

### Scenario: TC-CRR-CREATE-NEG-001 Invalid Region Configuration
Given an invalid region code or identical source & target region  
When I attempt policy creation  
Then the request fails with HTTP 400 INVALID_REGION  
And no partial policy persists

### Scenario: TC-CRR-CREATE-NEG-002 Insufficient Regional Permissions
Given caller lacks target region replication IAM roles  
When I submit creation request  
Then HTTP 403 FORBIDDEN is returned with missing permission detail  
And no security exposure occurs

---

# Section 2: Volume Replication 

## Scenario Summary
| ID | Title | Priority | Type | Automation |
|----|-------|----------|------|------------|
| TC-CRR-VOLUME-001 | Enable CRR for Existing Volume | P0 | Positive | Yes |
| TC-CRR-VOLUME-002 | Create Volume with CRR Enabled | P0 | Positive | Yes |
| TC-CRR-VOLUME-003 | Large Volume Replication | P1 | Positive | Partial |
| TC-CRR-CONSISTENCY-001 | Write During Replication | P0 | Positive | Yes |
| TC-CRR-CONSISTENCY-002 | Snapshot Consistency | P1 | Positive | Partial |

### Scenario: TC-CRR-VOLUME-001 Enable CRR for Existing Volume
Given an existing source volume with data  
When I assign a READY CRR policy with initial sync enabled  
Then a baseline replication starts and progresses to COMPLETE  
And target volume reflects consistent data snapshot

### Scenario: TC-CRR-VOLUME-002 Create Volume with CRR Enabled
Given a CRR policy  
When I create a new volume referencing the policy  
Then initial sync automatically begins  
And subsequent writes replicate per schedule with bounded lag

### Scenario: TC-CRR-VOLUME-003 Large Volume Replication
Given a >10TB volume with diverse data patterns  
When replication is initiated  
Then progress metrics report transferred bytes & ETA  
And bandwidth utilization remains within planned window

### Scenario: TC-CRR-CONSISTENCY-001 Write During Replication
Given an initial sync in progress  
When concurrent write workload executes  
Then writes after snapshot point are queued for subsequent delta replication  
And final target state achieves consistency with no lost writes

### Scenario: TC-CRR-CONSISTENCY-002 Snapshot Consistency
Given periodic source snapshots  
When a snapshot is taken during active replication  
Then snapshot lineage and metadata replicate appropriately  
And snapshot restore in target succeeds with preserved metadata

---

# Section 3: Failover & Disaster Recovery 

## Scenario Summary
| ID | Title | Priority | Type | Automation |
|----|-------|----------|------|------------|
| TC-CRR-FAILOVER-001 | Planned Failover Operation | P0 | Positive | Partial |
| TC-CRR-FAILOVER-002 | Failover with Active Applications | P0 | Positive | Partial |
| TC-CRR-FAILOVER-003 | Emergency Failover - Region Outage | P0 | Negative | Partial |
| TC-CRR-FAILOVER-004 | Partial Source Region Failure | P1 | Negative | Partial |
| TC-CRR-FAILBACK-001 | Planned Failback Operation | P1 | Positive | Partial |
| TC-CRR-FAILBACK-002 | Incremental Failback | P2 | Positive | Partial |

### Scenario: TC-CRR-FAILOVER-001 Planned Failover Operation
Given an actively replicating volume and documented runbook  
When I initiate planned failover  
Then source transitions to READ_ONLY and target promoted RW  
And RPO = 0 with RTO within target threshold

### Scenario: TC-CRR-FAILOVER-002 Failover with Active Applications
Given application I/O against source  
When I start failover  
Then in-flight writes drain before cutover  
And application reconnects to target with minimal disruption

### Scenario: TC-CRR-FAILOVER-003 Emergency Failover - Region Outage
Given the source region becomes unreachable  
When I trigger emergency failover  
Then target promotion succeeds without source coordination  
And resulting RPO is measured and within SLA (< defined max)

### Scenario: TC-CRR-FAILOVER-004 Partial Source Region Failure
Given degraded connectivity (intermittent packets)  
When failover evaluation occurs  
Then split-brain prevention blocks unsafe dual-primary state  
And decision logged with diagnostics

### Scenario: TC-CRR-FAILBACK-001 Planned Failback Operation
Given target currently primary post-failover and source restored  
When I synchronize deltas and promote source  
Then applications reconnect to source  
And replication direction reverses cleanly

### Scenario: TC-CRR-FAILBACK-002 Incremental Failback
Given accumulated changes on target  
When incremental failback is initiated  
Then only delta blocks transfer  
And total failback time is reduced vs full re-sync

---

# Section 4: Cross-Feature Integration 

## Scenario Summary
| ID | Title | Priority | Type | Automation |
|----|-------|----------|------|------------|
| TC-CRR-CMEK-001 | CRR with Customer-Managed Encryption | P0 | Positive | Yes |
| TC-CRR-BACKUP-001 | CRR with Backup Policies | P1 | Positive | Partial |
| TC-CRR-NETWORK-001 | CRR with VPC Peering | P1 | Positive | Partial |

### Scenario: TC-CRR-CMEK-001 CRR with Customer-Managed Encryption
Given a CMEK-encrypted source volume and target CMEK policy  
When replication runs  
Then encrypted data remains opaque in transit  
And failover preserves CMEK associations

### Scenario: TC-CRR-BACKUP-001 CRR with Backup Policies
Given a volume having both backup schedule and CRR  
When backup executes during replication window  
Then resource coordination prevents contention  
And backup artifacts accessible in both regions

### Scenario: TC-CRR-NETWORK-001 CRR with VPC Peering
Given custom VPC networks peered across regions  
When replication traffic flows  
Then security groups/firewalls allow only required ports  
And isolation boundaries remain intact

---

# Section 5: Performance & Monitoring 

## Scenario Summary
| ID | Title | Priority | Type | Automation |
|----|-------|----------|------|------------|
| TC-CRR-PERFORMANCE-001 | Replication Performance Baseline | P1 | Positive | Partial |
| TC-CRR-PERFORMANCE-002 | Impact on Source Volume Performance | P0 | Positive | Partial |
| TC-CRR-MONITOR-001 | Replication Status Monitoring | P1 | Positive | Yes |
| TC-CRR-MONITOR-002 | Network Connectivity Monitoring | P2 | Positive | Partial |

### Scenario: TC-CRR-PERFORMANCE-001 Replication Performance Baseline
Given volumes small/medium/large  
When initial and steady-state replication occur  
Then measured throughput & duration meet SLA baselines  
And metrics stored for trend analysis

### Scenario: TC-CRR-PERFORMANCE-002 Impact on Source Volume Performance
Given baseline latency/throughput prior to CRR  
When CRR enabled  
Then added latency < defined limit and throughput impact minimal  
And recommendations recorded if threshold exceeded

### Scenario: TC-CRR-MONITOR-001 Replication Status Monitoring
Given monitoring & alert rules configured  
When replication cycles execute  
Then dashboards show lag, last successful sync, backlog size  
And failure injection triggers alerts

### Scenario: TC-CRR-MONITOR-002 Network Connectivity Monitoring
Given inter-region network telemetry  
When latency and packet loss are artificially introduced  
Then alerts fire for threshold breaches  
And auto-retry logic mitigates transient issues

---

# Section 6: Security & Compliance 

## Scenario Summary
| ID | Title | Priority | Type | Automation |
|----|-------|----------|------|------------|
| TC-CRR-SECURITY-001 | Cross-Region Access Control | P0 | Positive | Yes |
| TC-CRR-SECURITY-002 | Data in Transit Protection | P0 | Positive | Yes |
| TC-CRR-COMPLIANCE-001 | Data Residency Compliance | P0 | Positive | Partial |

### Scenario: TC-CRR-SECURITY-001 Cross-Region Access Control
Given IAM roles scoped minimally for CRR  
When authorized and unauthorized principals attempt CRR operations  
Then authorized succeed and unauthorized receive PERMISSION_DENIED  
And audit logs record all attempts

### Scenario: TC-CRR-SECURITY-002 Data in Transit Protection
Given TLS 1.2+ enforced for replication channels  
When replication traffic is observed  
Then all data packets are encrypted  
And certificate rotation occurs seamlessly

### Scenario: TC-CRR-COMPLIANCE-001 Data Residency Compliance
Given residency constraints for specific datasets  
When CRR is configured  
Then replication only targets approved regions  
And compliance report lists zero residency violations

---

# Section 7: Error Handling & Recovery 

## Scenario Summary
| ID | Title | Priority | Type | Automation |
|----|-------|----------|------|------------|
| TC-CRR-ERROR-001 | Network Partition Handling | P1 | Negative | Partial |
| TC-CRR-ERROR-002 | Target Region Resource Exhaustion | P2 | Negative | Partial |
| TC-CRR-ERROR-003 | Source Data Corruption Detection | P1 | Negative | Partial |

### Scenario: TC-CRR-ERROR-001 Network Partition Handling
Given stable replication  
When a network partition isolates regions  
Then replication pauses gracefully without data divergence  
And resumes automatically processing backlog after recovery

### Scenario: TC-CRR-ERROR-002 Target Region Resource Exhaustion
Given nearing storage quota in target region  
When replication attempts new writes  
Then system emits QUOTA_EXCEEDED alert  
And throttles replication without corruption

### Scenario: TC-CRR-ERROR-003 Source Data Corruption Detection
Given corruption injected into source block range  
When checksum/validation runs  
Then corrupted segment is detected and excluded from replication  
And alert + remediation guidance provided

---

# Section 8: Test Summary and Metrics

## Test Coverage Summary
| Component | Test Cases | P0 (Critical) | P1 (High) | P2 (Medium) |
|-----------|------------|---------------|-----------|-------------|
| CRR Policy Creation | 6 | 2 | 2 | 2 |
| Volume Replication | 5 | 3 | 2 | 0 |
| Failover & DR | 6 | 4 | 2 | 0 |
| Cross-Feature Integration | 3 | 1 | 2 | 0 |
| Performance & Monitoring | 4 | 1 | 2 | 1 |
| Security & Compliance | 3 | 2 | 1 | 0 |
| Error Handling | 3 | 0 | 2 | 1 |
| **Total** | **30** | **13** | **13** | **4** |

## RTO / RPO Targets
- Planned Failover RTO < 15m; Emergency < 30m; Failback < 60m  
- Synchronous: RPO=0; Async: RPO < 1h; Emergency: RPO < 4h

## Network Performance Targets
- Inter-region average latency < 100ms  
- Additional replication-induced latency < 5ms  
- Monitoring interval 30s; alert within 2 intervals

## Known Limitations & Constraints
- Unsupported regions excluded from policy creation
- Max replicated volume size 100TB
- Stable inter-region connectivity required
- Cross-region transfer costs apply

## Related Documents
- CSA-1082 JIRA, SOC 2 Type II controls, GDPR compliance guidance

---

**Document Version:** 1.0  
**Review Status:** Draft  
**Data Protection Classification:** Internal
