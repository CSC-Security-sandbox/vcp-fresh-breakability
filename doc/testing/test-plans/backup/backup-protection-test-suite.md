# Test Plan: Backup Management - Policies, Scheduling & Operations

**Document Version:** 1.0  
**Last Updated:** September 25, 2025  
**Status:** In Review  
**JIRA Tracker:** [CSA-1068](https://jira.ngage.netapp.com/browse/CSA-1068)  
**Component:** Backup Management - Policies, Scheduling & Operations  
**Component Version:** 25093.0.0-RC.39  
**Data Protection Classification:** Internal

## 1. Introduction

### 1.1 Overview
This document contains comprehensive test cases for Backup Management functionality including backup policy management, scheduling, execution monitoring, and recovery operations. Backup provides long-term data protection, compliance, and disaster recovery capabilities for VSA volumes.

### 1.2 Scope
- In Scope: Backup policy management, scheduling, execution, monitoring, recovery, compliance, cross-region, integration with snapshots/CMEK/CRR, performance, error handling.
- Out of Scope: Non-backup data protection features not related to VSA volumes.

### 1.3 Related Requirements
- SOX Compliance
- GDPR Compliance
- HIPAA Compliance

## 2. Test Requirements

### 2.1 Requirements to be Tested
- Backup policy creation, configuration, and management
- Scheduled backup execution and monitoring
- Backup retention policies and lifecycle management
- Backup restoration and recovery operations
- Cross-region backup replication and storage
- Integration with snapshots, CMEK, and CRR features
- Performance optimization and resource management
- Compliance and audit trail requirements
- Error handling and disaster recovery scenarios

### 2.2 Requirements NOT in Scope
- Non-backup data protection features not related to VSA volumes

## 3. Test Environment Requirements

### 3.1 Infrastructure
- VSA environment with backup-enabled volumes
- Multi-region storage targets
- Monitoring and alerting systems

### 3.2 Test Data
- Volumes with various data patterns and sizes
- Compliance-specific data sets

### 3.3 Dependencies
- GCP storage and network
- IAM permissions
- Integration with snapshot, CMEK, CRR

## 4. Test Categories

### 4.1 Functional Testing
- Policy management, scheduling, lifecycle, restoration

### 4.2 Integration Testing
- CMEK, snapshot, CRR interactions

### 4.3 Performance Testing
- Backup throughput, impact, scaling

### 4.4 Security Testing
- Compliance, encryption, access, audit

### 4.5 Negative Testing
- Error, disaster, resource exhaustion

## 5. Risk Assessment

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| Backup failure due to storage outage | High | Medium | Multi-region redundancy, alerting |
| Data corruption during backup | High | Low | Integrity checks, validation |
| Compliance violation | High | Low | Automated policy enforcement |
| Resource exhaustion | Medium | Medium | Monitoring, throttling |

## 6. Test Execution Strategy

### 6.1 Test Phases
1. **Phase 1**: Policy and scheduling validation
2. **Phase 2**: Storage, lifecycle, and restoration
3. **Phase 3**: Integration, performance, error handling

### 6.2 Automation Strategy
- Automated execution of backup/restore, scheduling, monitoring, and reporting using CI pipelines and scripts.

### 6.3 Manual Testing Strategy
- Manual validation of compliance, legal hold, and disaster recovery scenarios.

## 7. Success Criteria
- All backup/restore operations meet RTO/RPO targets
- Compliance and audit requirements met
- No data loss or corruption in any scenario
- All test cases pass as per expected results

## 8. Test Schedule
- Phase 1: 2025-09-26 to 2025-10-02
- Phase 2: 2025-10-03 to 2025-10-09
- Phase 3: 2025-10-10 to 2025-10-16

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

# Section 1: Backup Policy Management

## Scenario Summary
| ID | Title | Priority | Type | Automation |
|----|-------|----------|------|------------|
| TC-BACKUP-POLICY-001 | Basic Backup Policy Creation | P0 | Positive | Yes |
| TC-BACKUP-POLICY-002 | Advanced Multi-Tier Retention Policy | P1 | Positive | Yes |
| TC-BACKUP-POLICY-003 | Compliance-Focused Retention Policy | P0 | Positive | Yes |
| TC-BACKUP-POLICY-004 | Update Active Backup Policy | P1 | Positive | Yes |
| TC-BACKUP-POLICY-005 | Backup Policy Deletion & Cleanup | P1 | Positive | Yes |
| TC-BACKUP-POLICY-006 | Policy Assignment Management | P1 | Positive | Yes |

### Scenario: TC-BACKUP-POLICY-001 Basic Backup Policy Creation
Given a Ready volume and accessible cloud storage target  
And valid retention tiers daily/weekly/monthly defined  
When I submit a create policy request with daily schedule at 02:00 UTC  
Then the policy is created with status ACTIVE  
And initial next execution time is computed  
And policy assignment to the target volume succeeds

### Scenario: TC-BACKUP-POLICY-002 Advanced Multi-Tier Retention Policy
Given tier definitions (hourly every 6h keep 4 days, daily keep 90 days, weekly keep 104 weeks, monthly keep 84 months)  
When I create the enterprise backup policy  
Then all tiers validate and store with correct storage classes  
And each tier exposes its independent retention counters  
And cost projection includes tiered storage classes

### Scenario: TC-BACKUP-POLICY-003 Compliance-Focused Retention Policy
Given compliance requires immutable backups and audit logging enabled  
When I create a compliance policy with immutability enabled  
Then created backups are flagged immutable until retention expiry  
And delete attempts during immutability window are rejected  
And audit events record policy creation and enforcement

### Scenario: TC-BACKUP-POLICY-004 Update Active Backup Policy
Given an ACTIVE policy assigned to multiple volumes  
When I update daily schedule time and adjust retention counts  
Then new schedule applies to subsequent (not in-flight) executions  
And retention engine re-evaluates existing backups  
And no ongoing backup operation is aborted

### Scenario: TC-BACKUP-POLICY-005 Backup Policy Deletion & Cleanup
Given a policy with existing backups and deletion mode=PRESERVE  
When I delete the policy  
Then policy moves to DELETING then is removed  
And backups remain accessible (orphaned->unmanaged state)  
And audit trail lists policy deletion and preservation decision

### Scenario: TC-BACKUP-POLICY-006 Policy Assignment Management
Given a policy and a bulk list of volumes (≥2)  
When I assign the policy with overrides (time=01:00 for vol-001)  
Then per-volume override appears only on specified volume  
And global changes still propagate to non-overridden fields  
And unassign followed by reassign restores default schedule

---

# Section 2: Backup Scheduling & Execution

## Scenario Summary
| ID | Title | Priority | Type | Automation |
|----|-------|----------|------|------------|
| TC-BACKUP-SCHED-001 | Scheduled Backup Execution Monitoring | P0 | Positive | Yes |
| TC-BACKUP-SCHED-002 | Concurrent Backup Execution | P1 | Positive | Yes |
| TC-BACKUP-SCHED-003 | Backup Scheduling Optimization | P2 | Positive | Partial |
| TC-BACKUP-EXEC-001 | Backup Progress Monitoring | P1 | Positive | Yes |
| TC-BACKUP-EXEC-002 | Backup Failure Detection & Alerting | P0 | Negative | Yes |
| TC-BACKUP-EXEC-003 | Long-Running Backup Management | P1 | Positive | Partial |

### Scenario: TC-BACKUP-SCHED-001 Scheduled Backup Execution Monitoring
Given multiple volumes each with distinct schedules  
When cycles execute over >2 schedule intervals  
Then each backup starts within configured tolerance (e.g., ±2m)  
And completion status reflects SUCCESS with accurate start/end timestamps

### Scenario: TC-BACKUP-SCHED-002 Concurrent Backup Execution
Given ≥3 volumes scheduled for same time slot  
When the scheduler triggers concurrent backups  
Then system parallelizes within concurrency limits  
And throttling engages if resource thresholds exceed policy  
And all backups complete without starvation

### Scenario: TC-BACKUP-SCHED-003 Backup Scheduling Optimization
Given overlapping schedules and optimization enabled  
When optimization cycle runs  
Then start times are redistributed to flatten peak load  
And aggregate resource utilization curve shows reduced spike  
And SLAs remain satisfied

### Scenario: TC-BACKUP-EXEC-001 Backup Progress Monitoring
Given a large backup in progress  
When I poll progress API periodically  
Then reported percent complete monotonically advances  
And ETA updates reflect remaining data size  
And final state transitions to SUCCESS with checksum recorded

### Scenario: TC-BACKUP-EXEC-002 Backup Failure Detection & Alerting
Given monitoring rules for FAILURE and SLA_MISS  
When a simulated storage target outage causes a backup failure  
Then failure classification = STORAGE_UNAVAILABLE  
And alert dispatched to on-call channel  
And automatic retry scheduled if policy allows

### Scenario: TC-BACKUP-EXEC-003 Long-Running Backup Management
Given a >50TB volume backup lasting several hours  
When a maintenance window triggers pause event  
Then backup enters PAUSED state safely  
And resume continues without data duplication  
And total elapsed active time within threshold

---

# Section 3: Backup Storage & Management

## Scenario Summary
| ID | Title | Priority | Type | Automation |
|----|-------|----------|------|------------|
| TC-BACKUP-STORAGE-001 | Multi-Region Backup Storage | P1 | Positive | Yes |
| TC-BACKUP-STORAGE-002 | Storage Tier Management | P2 | Positive | Partial |
| TC-BACKUP-STORAGE-003 | Backup Deduplication & Compression | P2 | Positive | Partial |
| TC-BACKUP-LIFECYCLE-001 | Automatic Backup Retention Cleanup | P0 | Positive | Yes |
| TC-BACKUP-LIFECYCLE-002 | Legal Hold & Litigation Support | P1 | Positive | Partial |
| TC-BACKUP-LIFECYCLE-003 | Backup Validation & Integrity Checking | P0 | Positive | Yes |

### Scenario: TC-BACKUP-STORAGE-001 Multi-Region Backup Storage
Given a policy listing primary and secondary regions (async replication)  
When a backup completes in primary  
Then replica objects appear in secondary regions after replication lag window  
And regional compliance tags match configuration  
And cross-region restore is permitted

### Scenario: TC-BACKUP-STORAGE-002 Storage Tier Management
Given tier transition rules (STANDARD->NEARLINE 30d, NEARLINE->COLDLINE 90d)  
When backups age past thresholds  
Then storageClass updates to next tier  
And retrieval latency metrics reflect tier characteristics  
And cost report shows expected reduction

### Scenario: TC-BACKUP-STORAGE-003 Backup Deduplication & Compression
Given two volumes with 70% identical data patterns  
When deduplicated backups run  
Then logical size > physical stored size by target ratio (≥3:1 dedupe + compression)  
And integrity verification of restored data succeeds

### Scenario: TC-BACKUP-LIFECYCLE-001 Automatic Backup Retention Cleanup
Given retention count=3 and 3 backups exist  
When a fourth backup completes  
Then the oldest backup is deleted automatically  
And reclaimed capacity is reflected in metrics  
And audit log records deletion reason=RETENTION

### Scenario: TC-BACKUP-LIFECYCLE-002 Legal Hold & Litigation Support
Given a backup under standard retention  
When I apply a legal hold with holdId litigation-case-2025-001  
Then retention cleanup skips that backup  
And hold metadata is visible in backup details  
And upon hold release normal retention evaluation resumes

### Scenario: TC-BACKUP-LIFECYCLE-003 Backup Validation & Integrity Checking
Given a backup with stored manifest checksums  
When periodic integrity job runs  
Then recalculated hashes match originals  
And a simulated corruption triggers ALERT_INTEGRITY_FAILURE  
And corruption quarantines affected object

---

# Section 4: Backup Restoration & Recovery

## Scenario Summary
| ID | Title | Priority | Type | Automation |
|----|-------|----------|------|------------|
| TC-BACKUP-RESTORE-001 | Complete Volume Restoration | P0 | Positive | Yes |
| TC-BACKUP-RESTORE-002 | Point-in-Time Recovery | P0 | Positive | Yes |
| TC-BACKUP-RESTORE-003 | Selective File/Directory Restoration | P1 | Positive | Partial |
| TC-BACKUP-RESTORE-004 | Cross-Region Disaster Recovery | P1 | Positive | Partial |
| TC-BACKUP-RECOVER-001 | RTO Validation | P0 | Positive | Yes |
| TC-BACKUP-RECOVER-002 | Large Scale Recovery Operations | P2 | Positive | Partial |
| TC-BACKUP-RECOVER-003 | Recovery Data Integrity Validation | P0 | Positive | Yes |

### Scenario: TC-BACKUP-RESTORE-001 Complete Volume Restoration
Given a FULL_VOLUME backup with integrity validated  
When I submit a restore request to a new pool  
Then a target volume is created and data streamed  
And final checksum matches backup manifest  
And volume enters Ready within RTO

### Scenario: TC-BACKUP-RESTORE-002 Point-in-Time Recovery
Given multiple backups around target timestamp T  
When I request recovery to time T  
Then system selects nearest backup <= T  
And recovered volume reflects data as of that point  
And RPO difference <= policy tolerance

### Scenario: TC-BACKUP-RESTORE-003 Selective File/Directory Restoration
Given backup catalog indexed for file-level entries  
When I select a set of files for restore to alternate path  
Then only those files are materialized  
And permissions & timestamps preserved  
And unrelated data remains untouched

### Scenario: TC-BACKUP-RESTORE-004 Cross-Region Disaster Recovery
Given primary region declared unavailable  
When I trigger DR restore from secondary region backup  
Then critical volumes are restored within DR RTO  
And applications connect to DR volumes successfully

### Scenario: TC-BACKUP-RECOVER-001 RTO Validation
Given defined RTO targets per size tier  
When I perform representative restores  
Then measured completion times fall within targets  
And results are recorded in RTO dashboard

### Scenario: TC-BACKUP-RECOVER-002 Large Scale Recovery Operations
Given multiple large backups selected concurrently  
When restores run in parallel  
Then resource scheduling prevents saturation  
And all completes within aggregate SLA window

### Scenario: TC-BACKUP-RECOVER-003 Recovery Data Integrity Validation
Given restored volumes from validated backups  
When I run end-to-end checksum verification  
Then all hashes match original recorded values  
And no corruption events logged

---

# Section 5: Cross-Feature Integration

## Scenario Summary
| ID | Title | Priority | Type | Automation |
|----|-------|----------|------|------------|
| TC-BACKUP-CMEK-001 | CMEK-Encrypted Volume Backups | P0 | Positive | Yes |
| TC-BACKUP-CMEK-002 | Cross-Project Key Management | P1 | Positive | Partial |
| TC-BACKUP-SNAP-001 | Backup + Snapshot Policy Coordination | P1 | Positive | Yes |
| TC-BACKUP-CRR-001 | Backup with Cross-Region Replication | P1 | Positive | Yes |

### Scenario: TC-BACKUP-CMEK-001 CMEK-Encrypted Volume Backups
Given a volume encrypted with CMEK key K  
When a backup executes  
Then backup metadata records key reference K  
And restoration requires valid access to K  
And key usage appears in audit logs

### Scenario: TC-BACKUP-CMEK-002 Cross-Project Key Management
Given volume in project A using CMEK key in project B  
When backup runs  
Then cross-project IAM permissions are validated  
And backup creation succeeds  
And restoration fails if key permission revoked

### Scenario: TC-BACKUP-SNAP-001 Backup + Snapshot Policy Coordination
Given both snapshot and backup policies with overlapping windows  
When coordinator evaluates schedule  
Then backup is deferred to avoid I/O spike  
And both artifacts produced within acceptable time horizon

### Scenario: TC-BACKUP-CRR-001 Backup with Cross-Region Replication
Given CRR active between region R1 and R2  
When backup policy runs in R1  
Then replica or secondary region also holds a backup copy (as configured)  
And failover to R2 preserves backup catalog continuity

---

# Section 6: Performance & Monitoring

## Scenario Summary
| ID | Title | Priority | Type | Automation |
|----|-------|----------|------|------------|
| TC-BACKUP-PERF-001 | Backup Performance Benchmarking | P1 | Positive | Partial |
| TC-BACKUP-PERF-002 | Backup Impact on Volume Performance | P2 | Positive | Partial |
| TC-BACKUP-MONITOR-001 | Comprehensive Backup Monitoring | P1 | Positive | Yes |
| TC-BACKUP-MONITOR-002 | Backup Compliance Reporting | P1 | Positive | Partial |

### Scenario: TC-BACKUP-PERF-001 Backup Performance Benchmarking
Given baseline performance targets per size tier  
When I execute representative backups  
Then measured durations meet targets  
And metrics are stored for trend analysis

### Scenario: TC-BACKUP-PERF-002 Backup Impact on Volume Performance
Given baseline latency/throughput metrics  
When backup runs during workload  
Then latency increase < 15% and throughput drop < 10%  
And metrics revert to baseline post-completion

### Scenario: TC-BACKUP-MONITOR-001 Comprehensive Backup Monitoring
Given monitoring dashboards subscribed to metrics  
When backups of various types run  
Then success/failure rate, duration, storage, cost metrics populate  
And anomaly detection flags injected fault scenario

### Scenario: TC-BACKUP-MONITOR-002 Backup Compliance Reporting
Given compliance rules for retention and encryption  
When reporting job runs for period P  
Then report lists coverage, violations=0, and evidence links  
And audit log completeness percentage > 99%

---

# Section 7: Error Handling & Disaster Recovery

## Scenario Summary
| ID | Title | Priority | Type | Automation |
|----|-------|----------|------|------------|
| TC-BACKUP-ERROR-001 | Storage Target Unavailability | P1 | Negative | Yes |
| TC-BACKUP-ERROR-002 | Network Connectivity Issues | P1 | Negative | Yes |
| TC-BACKUP-ERROR-003 | Resource Exhaustion Scenarios | P2 | Negative | Partial |
| TC-BACKUP-DR-001 | Complete Disaster Recovery Simulation | P0 | Positive | Partial |
| TC-BACKUP-DR-002 | Partial Disaster Recovery Testing | P1 | Positive | Partial |

### Scenario: TC-BACKUP-ERROR-001 Storage Target Unavailability
Given an active backup writing to storage target S  
When target S becomes unavailable  
Then backup transitions to FAILED with reason STORAGE_UNAVAILABLE  
And retry or failover target logic triggers if configured  
And no partial inconsistent backup remains

### Scenario: TC-BACKUP-ERROR-002 Network Connectivity Issues
Given remote storage accessible over network  
When intermittent packet loss is introduced  
Then backup retries transient failures with exponential backoff  
And operation resumes upon stability  
And final status SUCCESS without corruption

### Scenario: TC-BACKUP-ERROR-003 Resource Exhaustion Scenarios
Given system resource thresholds (CPU, memory, IO)  
When concurrent backups exceed throttle policy  
Then new backups queue in PENDING_THROTTLED state  
And existing backups continue at reduced rate  
And metrics show controlled degradation not failure

### Scenario: TC-BACKUP-DR-001 Complete Disaster Recovery Simulation
Given primary environment is simulated failed  
When DR runbook is executed using last successful backups  
Then critical systems restored within DR RTO  
And data integrity validations pass  
And runbook execution steps logged

### Scenario: TC-BACKUP-DR-002 Partial Disaster Recovery Testing
Given subset of volumes impacted by failure  
When selective restore for impacted volumes executes  
Then unaffected volumes remain online  
And recovery time improved over full DR  
And dependency checks pass

---

# Section 8: Test Summary and Metrics

## Test Coverage Summary

| Component | Test Cases | P0 (Critical) | P1 (High) | P2 (Medium) |
|-----------|------------|---------------|-----------|-------------|
| Policy Management | 6 | 2 | 3 | 1 |
| Scheduling & Execution | 6 | 2 | 3 | 1 |
| Storage & Management | 6 | 1 | 2 | 3 |
| Restoration & Recovery | 7 | 4 | 2 | 1 |
| Cross-Feature Integration | 4 | 2 | 2 | 0 |
| Performance & Monitoring | 4 | 0 | 3 | 1 |
| Error Handling & DR | 5 | 1 | 3 | 1 |
| **Total** | **38** | **12** | **18** | **8** |

## Backup Performance Targets

### Backup Creation Performance
- **Small Volume Backups (<1TB):** < 15 minutes
- **Medium Volume Backups (1-10TB):** < 2 hours
- **Large Volume Backups (>10TB):** < 8 hours
- **Incremental Backups:** < 20% of full backup time

### Recovery Performance Targets
- **File-Level Recovery:** < 30 minutes
- **Volume Recovery (<1TB):** < 1 hour
- **Volume Recovery (>10TB):** < 6 hours
- **Cross-Region Recovery:** < 12 hours

## Backup Storage Efficiency

### Deduplication and Compression
- **Deduplication Ratio:** > 3:1 for typical workloads
- **Compression Ratio:** > 2:1 additional savings
- **Incremental Efficiency:** > 95% space savings for incremental backups
- **Cross-Volume Deduplication:** Enabled where supported

### Storage Cost Optimization
- **Tier Transition Times:**
  - Standard to Nearline: 30 days
  - Nearline to Coldline: 90 days  
  - Coldline to Archive: 365 days
- **Cost Savings:** > 50% through automated tiering

## Backup Retention and Compliance

### Standard Retention Policies
- **Daily Backups:** 30 days retention
- **Weekly Backups:** 12 weeks retention
- **Monthly Backups:** 12 months retention
- **Yearly Backups:** 7 years retention (compliance)

### Recovery Time/Point Objectives
- **RTO Targets:**
  - Critical Systems: < 1 hour
  - Standard Systems: < 4 hours
  - Archive Data: < 24 hours
- **RPO Targets:**
  - Critical Data: < 1 hour (with frequent backups)
  - Standard Data: < 24 hours
  - Archive Data: < 1 week

## Compliance and Security

### Security Requirements
- **Encryption:** All backups encrypted in transit and at rest
- **Access Control:** Role-based access to backup operations
- **Audit Logging:** Complete audit trail for all operations
- **Legal Hold:** Support for litigation and regulatory holds

### Compliance Standards
- **SOX Compliance:** 7-year retention for financial data
- **GDPR Compliance:** Data sovereignty and deletion capabilities
- **HIPAA Compliance:** Encryption and access controls
- **Industry Standards:** Meets SOC 2 Type II requirements

## Known Limitations and Considerations

### Technical Limitations
- **Maximum Backup Size:** Limited by underlying storage capabilities
- **Concurrent Backups:** Throttling may apply under high load
- **Cross-Region Latency:** May impact backup and recovery performance
- **Storage Costs:** Long-term retention can be expensive

### Operational Considerations
- **Network Bandwidth:** Large backups require significant bandwidth
- **Backup Windows:** May need to coordinate with maintenance windows
- **Testing Requirements:** Regular DR testing recommended
- **Monitoring:** Continuous monitoring essential for backup health

## Related Documents


---

**Document Version:** 1.0  
**Review Status:** Draft  
**Data Protection Classification:** Internal
