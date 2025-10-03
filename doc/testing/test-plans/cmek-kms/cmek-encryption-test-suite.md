# Test Plan: Customer-Managed Encryption Keys (CMEK) & Key Management Service (KMS)

**Document Version:** 1.0  
**Last Updated:** September 25, 2025  
**Status:** Approved  
**JIRA Tracker:** [CSA-1072](https://jira.ngage.netapp.com/browse/CSA-1072), [CSA-1087](https://jira.ngage.netapp.com/browse/CSA-1087)  
**Component:** Customer-Managed Encryption Keys (CMEK) & Key Management Service (KMS)  
**Component Version:** 25083.0.0-RC.29  
**Data Protection Classification:** Internal

## 1. Introduction

### 1.1 Overview
This document contains comprehensive BDD-formatted test scenarios for Customer-Managed Encryption Keys (CMEK) and Key Management Service (KMS) integration. It supersedes the former minimal plan `cmek/cmek-kms-test-suite.md` by combining high-level integration validation (CSA-1087) with detailed lifecycle, security, performance, and compliance scenarios (CSA-1072). Scenarios cover the complete lifecycle of encryption key management, policy creation, cross-project key usage, and integration with VSA storage resources.

### 1.2 Scope
- In Scope: CMEK policy lifecycle, cross-project key management, service account validation, integration with pool/volume/backup, key rotation, security, compliance, error handling, performance, and scale.
- Out of Scope: Non-encryption features not related to VSA volumes.

### 1.3 Related Requirements
- [CSA-1072](https://jira.ngage.netapp.com/browse/CSA-1072)
- SOC 2 Type II, PCI DSS, HIPAA, GDPR

## 2. Test Requirements

### 2.1 Requirements to be Tested
- CMEK policy lifecycle management (Create, Update, Delete)
- Cross-project key management and permissions
- Service account creation and validation
- Integration with pool, volume, and backup operations
- Key rotation and security compliance
- Error handling and validation scenarios
- Performance and scale

### 2.2 Requirements NOT in Scope
- Non-encryption features not related to VSA volumes

## 3. Test Environment Requirements

### 3.1 Infrastructure
- GCP project(s) with KMS enabled
- Service accounts with least-privilege IAM roles
- Multi-region key and resource setup

### 3.2 Test Data
- KMS keys (standard, rotating, regional, cross-project)
- Pools, volumes, backups with & without CMEK

### 3.3 Dependencies
- GCP KMS
- IAM permissions
- Pool / Volume / Backup services

## 4. Test Categories

### 4.1 Functional Testing
- CMEK policy lifecycle & key management

### 4.2 Integration Testing
- Pool, volume, backup, CRR, cross-project

### 4.3 Performance Testing
- Encryption overhead, concurrent operations, scale

### 4.4 Security Testing
- Access control, audit, compliance

### 4.5 Negative Testing
- Invalid configs, outages, key revocation/deletion

## 5. Risk Assessment

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| Key mismanagement or loss | High | Low | Strict access control, backup, audit |
| Data inaccessibility due to key issues | High | Low | Recovery procedures, monitoring |
| Compliance violation | High | Low | Automated policy enforcement |
| Performance degradation | Medium | Medium | Monitoring, optimization |

## 6. Test Execution Strategy

### 6.1 Test Phases
1. Policy & key management validation
2. Integration, performance, compliance
3. Error handling, recovery, scale

### 6.2 Automation Strategy
- API-driven creation, rotation, assignment, monitoring; audit log validation

### 6.3 Manual Testing Strategy
- Compliance attestations, recovery runbooks, cross-project IAM validations

## 7. Success Criteria
- All CMEK operations meet security & compliance
- No data inaccessibility or integrity loss
- All scenarios pass within SLA targets

## 8. Test Schedule
- Phase 1: 2025-09-26 to 2025-10-02
- Phase 2: 2025-10-03 to 2025-10-09
- Phase 3: 2025-10-10 to 2025-10-16

## 9. Test Team
- Test Lead / Engineers / Automation / SMEs: [TBD]

## 10. Deliverables
- This BDD suite, execution & coverage reports, defects, automation artifacts

---

# Section 1: CMEK Policy Creation 

## Scenario Summary
| ID | Title | Priority | Type | Automation |
|----|-------|----------|------|------------|
| TC-CMEK-CREATE-001 | Basic CMEK Policy Creation | P0 | Positive | Yes |
| TC-CMEK-CREATE-002 | Policy with Cross-Project Key | P1 | Positive | Yes |
| TC-CMEK-CREATE-003 | Policy with Key Rotation | P1 | Positive | Partial |
| TC-CMEK-CREATE-004 | Policy with Regional Key | P1 | Positive | Partial |
| TC-CMEK-CREATE-NEG-001 | Invalid KMS Key Reference | P0 | Negative | Yes |
| TC-CMEK-CREATE-NEG-002 | Insufficient KMS Permissions | P1 | Negative | Yes |
| TC-CMEK-CREATE-NEG-003 | Disabled KMS Key | P2 | Negative | Yes |

### Scenario: TC-CMEK-CREATE-001 Basic CMEK Policy Creation
Given a valid GCP project with KMS enabled and an existing key  
And service account permissions include roles/cloudkms.cryptoKeyEncrypterDecrypter  
When I submit a create CMEK policy request referencing the key  
Then the policy is created with status PENDING then ACTIVE  
And the VSA DB stores SdeExternalUUID mapped to SDE policy UUID  
And required service accounts are created and auditable

### Scenario: TC-CMEK-CREATE-002 Policy with Cross-Project Key
Given a key in project B and policy creation in project A  
And cross-project IAM grants decrypt/encrypt permissions to A's service account  
When I create the CMEK policy referencing the external key  
Then validation of cross-project access succeeds  
And the policy becomes assignable to storage resources

### Scenario: TC-CMEK-CREATE-003 Policy with Key Rotation
Given a KMS key configured with automatic rotation schedule  
When I create a CMEK policy referencing the rotating key  
Then rotation metadata is captured  
And subsequent backup/volume accesses succeed after a key version rotates  
And audit logs show key version transitions

### Scenario: TC-CMEK-CREATE-004 Policy with Regional Key
Given regional keys in distinct locations (e.g., us-central1, europe-west1)  
When I create separate CMEK policies per region  
Then each policy enforces locality constraints  
And cross-region assignment attempts are rejected  
And regional compliance rules are satisfied

### Scenario: TC-CMEK-CREATE-NEG-001 Invalid KMS Key Reference
Given no key exists at the provided resource path  
When I submit a create CMEK policy request  
Then the API responds HTTP 400 with reason INVALID_KEY_REFERENCE  
And no partial policy persists

### Scenario: TC-CMEK-CREATE-NEG-002 Insufficient KMS Permissions
Given the service account lacks decrypt permission on the key  
When I attempt CMEK policy creation  
Then the API responds HTTP 403 PERMISSION_DENIED  
And audit logs record failed access attempt

### Scenario: TC-CMEK-CREATE-NEG-003 Disabled KMS Key
Given a key exists but is DISABLED  
When I attempt to create a CMEK policy referencing it  
Then request fails with HTTP 400 KEY_DISABLED  
And no policy record is stored

---

# Section 2: CMEK Policy Management 

## Scenario Summary
| ID | Title | Priority | Type | Automation |
|----|-------|----------|------|------------|
| TC-CMEK-UPDATE-001 | Update CMEK Policy Description | P1 | Positive | Yes |
| TC-CMEK-UPDATE-002 | Update Policy Labels | P2 | Positive | Yes |
| TC-CMEK-DELETE-001 | Delete Unused CMEK Policy | P0 | Positive | Yes |
| TC-CMEK-DELETE-NEG-001 | Delete Policy With Active Assignments | P0 | Negative | Yes |

### Scenario: TC-CMEK-UPDATE-001 Update CMEK Policy Description
Given an ACTIVE CMEK policy  
When I PATCH description field  
Then response = 202 Accepted and status eventually ACTIVE  
And new description persisted without affecting encrypted resources

### Scenario: TC-CMEK-UPDATE-002 Update Policy Labels
Given an ACTIVE CMEK policy  
When I PATCH labels with {environment=production, compliance=pci-dss}  
Then labels are searchable via filter API  
And encryption behavior unchanged

### Scenario: TC-CMEK-DELETE-001 Delete Unused CMEK Policy
Given a CMEK policy with zero active assignments  
When I issue a delete request  
Then related service accounts & metadata are cleaned  
And policy state transitions DELETING -> DELETED

### Scenario: TC-CMEK-DELETE-NEG-001 Delete Policy With Active Assignments
Given a CMEK policy assigned to at least one pool or volume  
When I attempt deletion  
Then deletion is blocked with HTTP 409 CONFLICT  
And policy remains ACTIVE

---

# Section 3: CMEK Integration 

## Scenario Summary
| ID | Title | Priority | Type | Automation |
|----|-------|----------|------|------------|
| TC-CMEK-POOL-001 | Create Pool with CMEK Policy | P0 | Positive | Yes |
| TC-CMEK-POOL-002 | Update Pool CMEK Policy | P1 | Positive | Partial |
| TC-CMEK-VOLUME-001 | Create Volume in CMEK Pool | P0 | Positive | Yes |
| TC-CMEK-VOLUME-002 | Volume Snapshot with CMEK | P1 | Positive | Partial |

### Scenario: TC-CMEK-POOL-001 Create Pool with CMEK Policy
Given an ACTIVE CMEK policy  
When I create a storage pool referencing the policy  
Then pool encryption metadata references the policy UUID  
And audit logs show key usage entries  
And performance metrics within encryption overhead target (<5-10%)

### Scenario: TC-CMEK-POOL-002 Update Pool CMEK Policy
Given a pool encrypted with policy A and a second policy B  
When I update pool to policy B  
Then re-encryption/migration process completes successfully  
And old key access revoked  
And no data loss occurs

### Scenario: TC-CMEK-VOLUME-001 Create Volume in CMEK Pool
Given an encrypted pool  
When I create a volume in that pool  
Then the volume inherits the pool's CMEK policy  
And read/write operations function normally  
And key usage appears in audit trail

### Scenario: TC-CMEK-VOLUME-002 Volume Snapshot with CMEK
Given a CMEK-encrypted volume with data  
When I create a snapshot  
Then snapshot metadata includes encryption key reference  
And restoring the snapshot preserves encryption properties

---

# Section 4: Cross-Feature Integration 

## Scenario Summary
| ID | Title | Priority | Type | Automation |
|----|-------|----------|------|------------|
| TC-CMEK-BACKUP-001 | Backup Encrypted Volume | P0 | Positive | Yes |
| TC-CMEK-BACKUP-002 | Cross-Region Backup with CMEK | P1 | Positive | Partial |
| TC-CMEK-CRR-001 | Cross-Region Replication with CMEK | P1 | Positive | Partial |

### Scenario: TC-CMEK-BACKUP-001 Backup Encrypted Volume
Given a CMEK-encrypted volume  
When a backup operation executes  
Then backup catalog records encryption key reference  
And restore requires valid key permissions

### Scenario: TC-CMEK-BACKUP-002 Cross-Region Backup with CMEK
Given regional key constraints for source and target regions  
When cross-region backup runs  
Then regional key locality rules are enforced  
And target backup metadata lists appropriate regional key

### Scenario: TC-CMEK-CRR-001 Cross-Region Replication with CMEK
Given replication configured from region R1 to R2  
When data changes replicate  
Then encryption at rest in R2 uses region-specific key  
And failover maintains access with correct key bindings

---

# Section 5: Security & Compliance 

## Scenario Summary
| ID | Title | Priority | Type | Automation |
|----|-------|----------|------|------------|
| TC-CMEK-SECURITY-001 | Service Account Permission Validation | P0 | Positive | Yes |
| TC-CMEK-SECURITY-002 | Key Access Audit Trail | P1 | Positive | Yes |
| TC-CMEK-COMPLIANCE-001 | Data Residency Compliance | P0 | Positive | Partial |
| TC-CMEK-COMPLIANCE-002 | Key Rotation Compliance | P1 | Positive | Partial |

### Scenario: TC-CMEK-SECURITY-001 Service Account Permission Validation
Given a service account bound to CMEK operations  
When I attempt allowed operations (encrypt/decrypt)  
Then they succeed  
And attempts for disallowed actions fail with PERMISSION_DENIED  
And no privilege escalation is possible

### Scenario: TC-CMEK-SECURITY-002 Key Access Audit Trail
Given audit logging enabled  
When I perform create/update/assign/encrypt operations  
Then each operation appears in audit logs with principal, timestamp, action  
And log integrity checks pass

### Scenario: TC-CMEK-COMPLIANCE-001 Data Residency Compliance
Given region-specific CMEK policies and residency constraints  
When I create resources declaring residency  
Then data & key usage remain within declared regions  
And compliance report shows zero violations

### Scenario: TC-CMEK-COMPLIANCE-002 Key Rotation Compliance
Given a rotation policy (e.g., 90-day interval)  
When scheduled rotation occurs  
Then new key version becomes active without access disruption  
And rotation event logged with old/new version references

---

# Section 6: Performance & Scale 

## Scenario Summary
| ID | Title | Priority | Type | Automation |
|----|-------|----------|------|------------|
| TC-CMEK-PERFORMANCE-001 | Encryption Performance Impact | P1 | Positive | Partial |
| TC-CMEK-PERFORMANCE-002 | Concurrent Key Operations | P2 | Positive | Partial |
| TC-CMEK-SCALE-001 | Maximum CMEK Policies per Project | P2 | Positive | Partial |
| TC-CMEK-SCALE-002 | Large-Scale Key Operations | P2 | Positive | Partial |

### Scenario: TC-CMEK-PERFORMANCE-001 Encryption Performance Impact
Given baseline latency/throughput without CMEK  
When I run identical workloads with CMEK  
Then latency overhead ≤ 10% and throughput within targets  
And metrics recorded for trend analysis

### Scenario: TC-CMEK-PERFORMANCE-002 Concurrent Key Operations
Given multiple simultaneous create/assign/rotate requests  
When executed under load  
Then all operations complete without deadlock  
And queueing/throttling metrics remain within policy

### Scenario: TC-CMEK-SCALE-001 Maximum CMEK Policies per Project
Given system documented maximum policy count N  
When I create N policies  
Then creation of N+1 is rejected with LIMIT_EXCEEDED  
And performance remains acceptable

### Scenario: TC-CMEK-SCALE-002 Large-Scale Key Operations
Given a batch of M (>> typical) rotation/validation operations  
When processed  
Then success rate = 100% without resource exhaustion  
And CPU/memory remain below alert thresholds

---

# Section 7: Error Handling & Recovery 

## Scenario Summary
| ID | Title | Priority | Type | Automation |
|----|-------|----------|------|------------|
| TC-CMEK-ERROR-001 | KMS Service Disruption | P1 | Negative | Partial |
| TC-CMEK-ERROR-002 | Key Deletion Impact | P0 | Negative | Yes |
| TC-CMEK-RECOVERY-001 | Key Restoration | P1 | Positive | Partial |

### Scenario: TC-CMEK-ERROR-001 KMS Service Disruption
Given active encrypted volumes  
When KMS API calls return transient errors/outage  
Then reads of already-encrypted data continue (cached keys)  
And new encryption operations fail gracefully with RETRYABLE status  
And system auto-recovers post-outage

### Scenario: TC-CMEK-ERROR-002 Key Deletion Impact
Given an encrypted resource bound to key K  
When key K is scheduled and then fully deleted  
Then dependent resources become INACCESSIBLE  
And user receives actionable remediation guidance  
And no silent data corruption occurs

### Scenario: TC-CMEK-RECOVERY-001 Key Restoration
Given a soft-deleted key within recovery window  
When key is restored  
Then previously inaccessible resources regain availability  
And access latency returns to baseline  
And recovery event logged

---

# Section 8: Test Summary and Metrics

## Test Coverage Summary

| Component | Test Cases | P0 (Critical) | P1 (High) | P2 (Medium) |
|-----------|------------|---------------|-----------|-------------|
| CMEK Policy Creation | 7 | 2 | 4 | 1 |
| Policy Management | 4 | 2 | 1 | 1 |
| Pool Integration | 2 | 1 | 1 | 0 |
| Volume Integration | 2 | 1 | 1 | 0 |
| Cross-Feature Integration | 3 | 1 | 2 | 0 |
| Security & Compliance | 4 | 2 | 2 | 0 |
| Performance & Scale | 4 | 0 | 1 | 3 |
| Error Handling & Recovery | 3 | 1 | 2 | 0 |
| **Total** | **29** | **10** | **14** | **5** |

## Security Requirements
- All encryption uses customer-managed keys meeting minimum strength
- Regular key rotation (e.g., ≤90 days) enforced & logged
- Immutable, integrity-protected audit trail for all key events
- Least privilege & MFA for key operations

## Compliance Framework Alignment
- SOC 2 Type II: Key management & access control evidence
- PCI DSS: Strong cryptography & key handling procedures
- HIPAA: Encryption of ePHI at rest/in transit
- GDPR: Data residency via regional key constraint

## Performance Targets
- Encryption overhead ≤ 10% latency, throughput ≥ 90% baseline
- Concurrent key ops scale linearly up to defined threshold
- Policy CRUD p95 latency < 1s under nominal load

## Known Limitations & Constraints
- Cross-project usage requires explicit IAM binding maintenance
- Regional keys cannot satisfy cross-region instant failover without pre-provisioned keys
- Large-scale rotations may extend beyond maintenance windows if not staggered
- Key restoration SLA 15–30 min may delay recovery

## Related Documentation
- CSA-1072 JIRA, Internal KMS integration design, Security audit procedures

---

**Document Version:** 1.0  
**Review Status:** Approved  
**Data Protection Classification:** Internal
