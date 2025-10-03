# Test Plan: Snapshot Management - Adhoc & Scheduled Snapshots

**Document Version:** 1.0  
**Last Updated:** September 25, 2025  
**Status:** Approved  
**JIRA Tracker:** [CSA-1065](https://jira.ngage.netapp.com/browse/CSA-1065), [CSA-1086](https://jira.ngage.netapp.com/browse/CSA-1086)  
**Component:** Snapshot Management - Adhoc & Scheduled Snapshots  
**Component Version:** 25083.0.0-RC.29  
**Data Protection Classification:** Internal  

## 1. Introduction

### 1.1 Overview
This document contains comprehensive test cases for Snapshot Management functionality including both adhoc (on-demand) and scheduled snapshot operations. Snapshots provide point-in-time copies of volumes for data protection, recovery, and development/testing purposes.

### 1.2 Scope
- In Scope: Adhoc snapshot creation, scheduled snapshot policies, restoration, cloning, retention, integration with backup/CRR/CMEK, performance, error handling, and scale.
- Out of Scope: Non-snapshot data protection features not related to VSA volumes.

### 1.3 Related Requirements
- [CSA-1065](https://jira.ngage.netapp.com/browse/CSA-1065)

## 2. Test Requirements

### 2.1 Requirements to be Tested
- Adhoc and scheduled snapshot creation, management, and deletion
- Snapshot restoration and cloning operations
- Retention policy management
- Integration with backup, CRR, and CMEK features
- Performance and scale
- Error handling and recovery scenarios

### 2.2 Requirements NOT in Scope
- Non-snapshot data protection features not related to VSA volumes

## 3. Test Environment Requirements

### 3.1 Infrastructure
- VSA environment with snapshot-enabled volumes
- Multi-region storage targets
- Monitoring and alerting systems

### 3.2 Test Data
- Volumes with various data patterns and sizes
- Compliance-specific data sets

### 3.3 Dependencies
- GCP storage and network
- IAM permissions
- Integration with backup, CRR, CMEK

## 4. Test Categories

### 4.1 Functional Testing
- Adhoc and scheduled snapshot operations

### 4.2 Integration Testing
- Integration with backup, CRR, CMEK

### 4.3 Performance Testing
- Snapshot creation, restoration, and scale

### 4.4 Security Testing
- Data integrity, encryption, compliance

### 4.5 Negative Testing
- Error handling, quota, and service disruptions

## 5. Risk Assessment

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| Snapshot failure due to storage outage | High | Medium | Multi-region redundancy, alerting |
| Data corruption during snapshot | High | Low | Integrity checks, validation |
| Compliance violation | High | Low | Automated policy enforcement |
| Resource exhaustion | Medium | Medium | Monitoring, throttling |

## 6. Test Execution Strategy

### 6.1 Test Phases
1. **Phase 1**: Adhoc and scheduled snapshot validation
2. **Phase 2**: Restoration, cloning, retention, integration
3. **Phase 3**: Performance, error handling, scale

### 6.2 Automation Strategy
- Automated execution of snapshot operations, monitoring, and reporting using CI pipelines and scripts.

### 6.3 Manual Testing Strategy
- Manual validation of compliance, recovery, and edge cases.

## 7. Success Criteria
- All snapshot operations meet RTO/RPO and performance targets
- No data loss or corruption in any scenario
- All test cases pass as per expected results

## 8. Test Schedule
- Phase 1: 2025-09-19 to 2025-09-25
- Phase 2: 2025-09-26 to 2025-10-02
- Phase 3: 2025-10-03 to 2025-10-09

## 9. Test Team
- **Test Lead:** [TBD]
- **Test Engineers:** [TBD]
- **Automation Engineers:** [TBD]
- **Subject Matter Experts:** [TBD]

## 10. Deliverables
- Test cases document (this file)
- Test execution reports
- Defect reports
- Test automation scripts

---

# Section 1: Adhoc Snapshots 

## Scenario Summary
| ID | Title | Priority | Type | Automation |
|----|-------|----------|------|------------|
| TC-SNAP-ADHOC-001 | Basic Adhoc Snapshot Creation | P0 | Positive | Yes |
| TC-SNAP-ADHOC-002 | Concurrent Adhoc Snapshots | P1 | Positive | Yes |
| TC-SNAP-ADHOC-003 | Large Volume Adhoc Snapshot | P1 | Positive | Yes |
| TC-SNAP-ADHOC-004 | Active Volume Snapshot (I/O Load) | P0 | Positive | Yes |
| TC-SNAP-ADHOC-005 | Update Snapshot Metadata | P2 | Positive | Yes |
| TC-SNAP-ADHOC-006 | List and Filter Snapshots | P1 | Positive | Yes |
| TC-SNAP-ADHOC-007 | Delete Adhoc Snapshot | P0 | Positive | Yes |
| TC-SNAP-ADHOC-008 | Restore from Adhoc Snapshot | P0 | Positive | Yes |
| TC-SNAP-ADHOC-009 | Clone Volume from Snapshot | P1 | Positive | Yes |

### Scenario: TC-SNAP-ADHOC-001 Basic Adhoc Snapshot Creation
Given a Ready volume vol-12345 containing test data and sufficient quota  
When I submit a create snapshot request with name adhoc-snapshot-001  
Then a snapshot resource is created and transitions to Ready  
And snapshot metadata (size, timestamp, sourceVolumeId) is accurate  
And source volume remains fully accessible  
And data in snapshot matches point-in-time state

### Scenario: TC-SNAP-ADHOC-002 Concurrent Adhoc Snapshots
Given multiple Ready volumes (>= 3) and normal system load  
When I initiate parallel adhoc snapshot requests for each volume  
Then all snapshot operations progress concurrently without deadlock  
And each snapshot completes successfully  
And system resource metrics remain within thresholds

### Scenario: TC-SNAP-ADHOC-003 Large Volume Adhoc Snapshot
Given a volume > 10TB with populated data patterns  
When I initiate an adhoc snapshot  
Then progress reporting updates periodically  
And the snapshot completes successfully  
And performance impact to active workloads stays within policy limits

### Scenario: TC-SNAP-ADHOC-004 Active Volume Snapshot (I/O Load)
Given an active sustained I/O workload on a Ready volume  
When I create an adhoc snapshot during workload  
Then the snapshot completes with a consistent point-in-time view  
And no I/O errors occur  
And latency increase remains within acceptable bounds

### Scenario: TC-SNAP-ADHOC-005 Update Snapshot Metadata
Given an existing adhoc snapshot in Ready state  
When I update description and labels via PATCH request  
Then updated metadata is persisted  
And no change occurs to underlying data blocks

### Scenario: TC-SNAP-ADHOC-006 List and Filter Snapshots
Given multiple snapshots with varied labels and timestamps exist  
When I list snapshots with filter criteria (label.type=adhoc)  
Then only matching snapshots are returned  
And pagination and sorting behave correctly  
And full listing includes all known snapshots

### Scenario: TC-SNAP-ADHOC-007 Delete Adhoc Snapshot
Given a Ready snapshot and sufficient permissions  
When I request deletion of the snapshot  
Then the snapshot is removed  
And associated storage is reclaimed  
And an audit event is logged

### Scenario: TC-SNAP-ADHOC-008 Restore from Adhoc Snapshot
Given a volume with initial data and a snapshot taken pre-modification  
When I modify volume data and perform a FULL_RESTORE from the snapshot  
Then the volume data matches the snapshot point-in-time  
And restore completes within SLA  
And the volume re-enters Ready state

### Scenario: TC-SNAP-ADHOC-009 Clone Volume from Snapshot
Given a Ready snapshot of a source volume  
When I issue a clone request referencing the snapshot  
Then a new volume clone is created with identical data  
And subsequent writes to clone do not affect the source  
And clone performance aligns with specification

---

# Section 2: Scheduled Snapshots & Retention 

## Scenario Summary
| ID | Title | Priority | Type | Automation |
|----|-------|----------|------|------------|
| TC-SNAP-SCHED-001 | Basic Scheduled Snapshot Policy | P0 | Positive | Yes |
| TC-SNAP-SCHED-002 | Multi-Frequency Scheduled Policy | P1 | Positive | Yes |
| TC-SNAP-SCHED-003 | Timezone-Aware Scheduling | P2 | Positive | Yes |
| TC-SNAP-SCHED-004 | Scheduled Snapshot Execution Monitoring | P0 | Positive | Yes |
| TC-SNAP-SCHED-005 | High-Frequency (15m) Scheduling | P2 | Positive | Partial |
| TC-SNAP-SCHED-006 | Policy Assigned to Multiple Volumes | P1 | Positive | Yes |
| TC-SNAP-RETENTION-001 | Automatic Snapshot Cleanup | P0 | Positive | Yes |
| TC-SNAP-RETENTION-002 | Retention Policy Updates | P1 | Positive | Yes |
| TC-SNAP-RETENTION-003 | Retention Policy Edge Cases | P2 | Positive | Partial |

### Scenario: TC-SNAP-SCHED-001 Basic Scheduled Snapshot Policy
Given a Ready volume and scheduling service availability  
When I create a daily snapshot policy at 02:00 UTC with 7-day retention and assign it  
Then the policy status is Active  
And the first scheduled snapshot is created at the next 02:00 window  
And snapshot naming matches prefix and timestamp rules

### Scenario: TC-SNAP-SCHED-002 Multi-Frequency Scheduled Policy
Given a volume and a composite policy (hourly, daily, weekly) with distinct retentions  
When the policy is applied  
Then each schedule fires at its cadence  
And older snapshots are pruned respecting per-schedule retention  
And schedule conflicts (overlap) are serialized without failure

### Scenario: TC-SNAP-SCHED-003 Timezone-Aware Scheduling
Given policies defined in multiple timezones including DST transitions  
When scheduled execution spans a DST boundary  
Then snapshots occur at correct local times pre and post transition  
And no duplicate or missed executions occur due to timezone shift

### Scenario: TC-SNAP-SCHED-004 Scheduled Snapshot Execution Monitoring
Given a policy applied to multiple volumes  
When multiple cycles of execution occur  
Then execution timestamps fall within tolerance window  
And missed executions are flagged with alerts  
And execution history is queryable

### Scenario: TC-SNAP-SCHED-005 High-Frequency (15m) Scheduling
Given a policy with 15-minute interval applied to a capable volume  
When the system runs for >= 2 hours  
Then all expected snapshots (>= 8) are created  
And resource consumption remains controlled  
And no backlog queue forms

### Scenario: TC-SNAP-SCHED-006 Policy Assigned to Multiple Volumes
Given a snapshot policy and >= 5 volumes  
When I attach the policy to all volumes  
Then each volume receives independent snapshot instances  
And failures in one volume do not block others

### Scenario: TC-SNAP-RETENTION-001 Automatic Snapshot Cleanup
Given a policy with short retention (retain 2) and snapshots exceed this count  
When a new scheduled snapshot completes  
Then the oldest excess snapshots are deleted automatically  
And reclaimed capacity metrics update

### Scenario: TC-SNAP-RETENTION-002 Retention Policy Updates
Given an active policy with existing snapshots  
When I change retention count from 7 to 3  
Then excess snapshots beyond new limit are pruned  
And increasing retention later preserves existing snapshots

### Scenario: TC-SNAP-RETENTION-003 Retention Policy Edge Cases
Given system boundary limits for min=1 and max=N retention  
When I configure retention at boundaries  
Then validation accepts boundary values  
And rejects beyond-limit values with clear errors  
And conflict scenarios resolve deterministically

---

# Section 3: Restoration & Cloning 

## Scenario Summary
| ID | Title | Priority | Type | Automation |
|----|-------|----------|------|------------|
| TC-SNAP-RESTORE-001 | Complete Volume Restore | P0 | Positive | Yes |
| TC-SNAP-RESTORE-002 | Selective File Restore | P1 | Positive | Partial |
| TC-SNAP-RESTORE-003 | Cross-Region Snapshot Restore | P2 | Positive | Partial |
| TC-SNAP-CLONE-001 | Basic Volume Cloning | P0 | Positive | Yes |
| TC-SNAP-CLONE-002 | Multiple Clones from One Snapshot | P1 | Positive | Yes |
| TC-SNAP-CLONE-003 | Clone Size Modification | P1 | Positive | Partial |

### Scenario: TC-SNAP-RESTORE-001 Complete Volume Restore
Given a volume with test data and a snapshot taken pre-corruption  
When I corrupt or modify data then trigger full restore  
Then volume contents match snapshot data  
And restore completes within RTO  
And volume reports Ready afterward

### Scenario: TC-SNAP-RESTORE-002 Selective File Restore
Given a snapshot containing diverse file structure  
When specific files are deleted and a selective restore targets them  
Then only selected files are restored  
And original permissions and metadata are preserved

### Scenario: TC-SNAP-RESTORE-003 Cross-Region Snapshot Restore
Given a snapshot replicated to target region R2  
When I initiate restore in R2  
Then a volume is created with identical data integrity  
And cross-region latency stays within acceptable performance bounds

### Scenario: TC-SNAP-CLONE-001 Basic Volume Cloning
Given a Ready snapshot  
When I issue a clone request  
Then cloned volume is created with identical point-in-time data  
And operations on clone do not impact source

### Scenario: TC-SNAP-CLONE-002 Multiple Clones from One Snapshot
Given a snapshot and system capacity  
When I create multiple clones sequentially  
Then each clone is independent  
And snapshot remains unchanged

### Scenario: TC-SNAP-CLONE-003 Clone Size Modification
Given snapshot source size S  
When I create a clone larger than S  
Then additional capacity is available for new data  
And attempts to shrink below supported minimum are rejected

---

# Section 4: Cross-Feature Integration 

## Scenario Summary
| ID | Title | Priority | Type | Automation |
|----|-------|----------|------|------------|
| TC-SNAP-CMEK-001 | CMEK-Encrypted Snapshot Operations | P0 | Positive | Yes |
| TC-SNAP-CMEK-002 | Cross-Region Snapshots with CMEK | P1 | Positive | Partial |
| TC-SNAP-BACKUP-001 | Snapshot & Backup Coexistence | P1 | Positive | Yes |
| TC-SNAP-CRR-001 | Snapshot Replication with CRR | P1 | Positive | Yes |

### Scenario: TC-SNAP-CMEK-001 CMEK-Encrypted Snapshot Operations
Given a CMEK-encrypted volume  
When I create adhoc and scheduled snapshots  
Then snapshots inherit encryption metadata  
And restore and clone operations retain encryption  
And key access obeys least privilege

### Scenario: TC-SNAP-CMEK-002 Cross-Region Snapshots with CMEK
Given an encrypted volume in region R1 and regional key constraints  
When snapshots are replicated to R2  
Then appropriate keys or key references are available in R2  
And compliance and access checks pass

### Scenario: TC-SNAP-BACKUP-001 Snapshot & Backup Coexistence
Given a volume with active snapshot and backup policies  
When both schedules trigger  
Then operations execute without resource contention  
And independent restore paths remain valid

### Scenario: TC-SNAP-CRR-001 Snapshot Replication with CRR
Given a volume configured with CRR and snapshots enabled  
When snapshots are created in source region  
Then replicated snapshots appear in target region  
And failover preserves snapshot chain integrity

---

# Section 5: Performance & Scale 

## Scenario Summary
| ID | Title | Priority | Type | Automation |
|----|-------|----------|------|------------|
| TC-SNAP-PERF-001 | Snapshot Creation Performance Impact | P1 | Positive | Partial |
| TC-SNAP-PERF-002 | Scheduled Snapshot Performance Optimization | P2 | Positive | Partial |
| TC-SNAP-SCALE-001 | Maximum Snapshots per Volume | P2 | Positive | Partial |
| TC-SNAP-SCALE-002 | Large-Scale Snapshot Operations | P2 | Positive | Partial |

### Scenario: TC-SNAP-PERF-001 Snapshot Creation Performance Impact
Given baseline latency and throughput metrics collected  
When snapshots are created during workload  
Then latency increase < 20% and throughput reduction < 10%  
And performance returns to baseline post-completion

### Scenario: TC-SNAP-PERF-002 Scheduled Snapshot Performance Optimization
Given multiple volumes with staggered schedules  
When schedules execute over a test window  
Then resource peaks are smoothed  
And system avoids simultaneous heavy contention

### Scenario: TC-SNAP-SCALE-001 Maximum Snapshots per Volume
Given platform limit MaxSnapshots=1024  
When I create snapshots up to the limit  
Then the 1024th snapshot succeeds  
And creation beyond limit is rejected with clear error  
And retention policies still enforce pruning

### Scenario: TC-SNAP-SCALE-002 Large-Scale Snapshot Operations
Given a fleet of volumes each with active policies  
When concurrent snapshot operations run at scale  
Then system stability is maintained  
And scheduling algorithms distribute load efficiently

---

# Section 6: Error Handling & Integrity 

## Scenario Summary
| ID | Title | Priority | Type | Automation |
|----|-------|----------|------|------------|
| TC-SNAP-ERROR-001 | Storage Quota Exhaustion | P1 | Negative | Yes |
| TC-SNAP-ERROR-002 | Volume Unavailable During Snapshot | P1 | Negative | Yes |
| TC-SNAP-ERROR-003 | Snapshot Service Disruption | P2 | Negative | Partial |
| TC-SNAP-INTEGRITY-001 | Snapshot Data Integrity Validation | P0 | Positive | Partial |
| TC-SNAP-INTEGRITY-002 | Point-in-Time Consistency Validation | P0 | Positive | Partial |

### Scenario: TC-SNAP-ERROR-001 Storage Quota Exhaustion
Given volume quota is near exhaustion and a snapshot request is issued  
When snapshot creation pushes usage beyond quota  
Then operation fails gracefully with QUOTA_EXCEEDED  
And no partial snapshot artifacts remain  
And increasing quota later allows retry success

### Scenario: TC-SNAP-ERROR-002 Volume Unavailable During Snapshot
Given an in-progress snapshot operation  
When the source volume becomes unavailable  
Then the snapshot aborts safely  
And cleanup removes intermediate resources  
And status reflects failure reason

### Scenario: TC-SNAP-ERROR-003 Snapshot Service Disruption
Given multiple scheduled operations pending  
When snapshot service is disrupted  
Then operations queue without data corruption  
And on recovery missed schedules are reconciled  
And recovery time falls within policy

### Scenario: TC-SNAP-INTEGRITY-001 Snapshot Data Integrity Validation
Given snapshots captured with recorded checksums  
When periodic integrity validation runs  
Then computed hashes match stored baselines  
And any mismatch triggers alerting

### Scenario: TC-SNAP-INTEGRITY-002 Point-in-Time Consistency Validation
Given a transactional workload and snapshot taken mid-operations  
When application consistency checks run on snapshot  
Then data reflects a consistent commit boundary  
And no partial transactions appear

---

# Section 7: Test Summary and Metrics

## Test Coverage Summary

| Component | Test Cases | P0 (Critical) | P1 (High) | P2 (Medium) |
|-----------|------------|---------------|-----------|-------------|
| Adhoc Snapshots | 9 | 4 | 3 | 2 |
| Scheduled Snapshots | 6 | 2 | 2 | 2 |
| Snapshot Retention | 3 | 1 | 1 | 1 |
| Restoration & Cloning | 6 | 2 | 3 | 1 |
| Cross-Feature Integration | 4 | 2 | 2 | 0 |
| Performance & Scale | 4 | 0 | 2 | 2 |
| Error Handling & Recovery | 5 | 2 | 2 | 1 |
| **Total** | **37** | **13** | **15** | **9** |

## Snapshot Performance Targets

### Creation Performance
- **Small Volume Snapshots (< 1TB):** < 5 minutes
- **Medium Volume Snapshots (1-10TB):** < 30 minutes  
- **Large Volume Snapshots (> 10TB):** < 2 hours

### I/O Impact During Snapshots
- **Latency Increase:** < 20% during snapshot creation
- **Throughput Reduction:** < 10% during snapshot creation
- **Recovery Time:** < 30 seconds to baseline performance

## Retention and Storage Management

### Default Retention Policies
- **Adhoc Snapshots:** No automatic deletion (manual cleanup)
- **Hourly Snapshots:** 24 hours (24 snapshots)
- **Daily Snapshots:** 30 days (30 snapshots)
- **Weekly Snapshots:** 12 weeks (12 snapshots)
- **Monthly Snapshots:** 12 months (12 snapshots)

### Storage Efficiency
- **Snapshot Storage Overhead:** < 10% of source volume size
- **Incremental Efficiency:** > 90% space savings for incremental snapshots
- **Deduplication:** Cross-snapshot deduplication where supported

## Backup and Recovery Metrics

### Recovery Time Objectives (RTO)
- **File-Level Restore:** < 15 minutes
- **Volume Restore (< 1TB):** < 30 minutes
- **Volume Restore (> 10TB):** < 4 hours
- **Cross-Region Restore:** < 8 hours

### Recovery Point Objectives (RPO)
- **Adhoc Snapshots:** RPO = Time since last manual snapshot
- **Hourly Snapshots:** RPO < 1 hour
- **Daily Snapshots:** RPO < 24 hours

## Known Limitations and Constraints
- **Maximum Snapshots per Volume:** 1024 snapshots
- **Snapshot Size Limitations:** Same as source volume limits
- **Cross-Region Limitations:** Dependent on regional availability
- **Performance Impact:** Varies with volume size and activity

## Related Documentation

---

**Document Version:** 1.0  
**Review Status:** Approved  
**Data Protection Classification:** Internal
