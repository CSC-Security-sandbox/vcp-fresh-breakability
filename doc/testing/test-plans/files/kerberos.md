# Test Plan: Kerberos Authentication for VSA NFS Volumes

**Document Version:** 1.0  
**Last Updated:** February 16, 2026  
**Status:** Draft  
**JIRA Tracker:** [TBD]  
**Component:** VSA Files – Kerberos (krb5, krb5i, krb5p) for NFSv4.1; AD dependency; export rules  
**Data Protection Classification:** Internal

## 1. Introduction

### 1.1 Overview
This document contains test cases for **Kerberos** authentication with VSA NFS volumes. It covers the three security levels (krb5, krb5i, krb5p), their use and modification on NFSv4.1 volumes, the restriction that NFSv3 volumes must not be created with Kerberos security levels, creation with all three levels on NFSv4.1, validation when AD is missing or invalid, invalid allowed-clients configuration, and add/delete of export rules on Kerberos-enabled volumes.

### 1.2 Scope
- **In Scope:** Kerberos authentication with krb5, krb5i, krb5p; NFSv4.1 volume create and modify security levels; NFSv3 + Kerberos (negative); NFSv4.1 with all three levels; AD prerequisite and invalid AD (IP/domain/realm); invalid allowedClients; export rule add/delete on krb volumes.
- **Out of Scope:** Kerberos KDC/AD server setup; non-NFS use of Kerberos in VSA.

### 1.3 Related Requirements
- Kerberos security levels (krb5, krb5i, krb5p) for NFS
- NFSv4.1 support for Kerberos; NFSv3 not supporting Kerberos levels
- AD (Active Directory) as prerequisite for krb-enabled volumes; validation of AD and allowedClients
- Export policy rules on Kerberos volumes (add/delete)

## 2. Test Requirements

### 2.1 Requirements to be Tested
- Authentication works with all three security levels (krb5, krb5i, krb5p); ensure all three are tested.
- Modification of Kerberos security levels from one to another (e.g. krb5 → krb5i → krb5p) for NFSv4.1 volumes; modifications succeed and authentication continues to work.
- NFSv3 volume with Kerberos security levels: volume must not be created with specified Kerberos levels (negative).
- NFSv4.1 volume with all three Kerberos security levels (krb5, krb5i, krb5p) configured together; volume supports all three.
- Volume creation fails when AD config is missing (first NFSv4.1 krb-enabled volume).
- Volume creation fails when AD details are invalid (IP/domain/realm).
- Volume creation fails or is rejected when allowedClients are invalid.
- Export rules under a Kerberos volume: delete rules, add extra rules, add then delete; operations succeed and authentication (where applicable) succeeds.

### 2.2 Requirements NOT in Scope
- Kerberos KDC or AD deployment; NFS client-side keytab or ticket behavior outside VSA control plane.

## 3. Test Environment Requirements

### 3.1 Infrastructure
- VSA environment with NFSv3 and NFSv4.1 volume support; Kerberos configuration (security levels); AD available for positive krb tests; ability to modify volume Kerberos settings and export policy.

### 3.2 Test Data
- Valid AD configuration (IP, domain, realm) for krb-enabled volumes; invalid AD and invalid allowed-clients for negative cases; NFSv3 and NFSv4.1 volume parameters.

### 3.3 Dependencies
- Active Directory (for krb-enabled NFSv4.1); VSA volume and export-policy APIs; NFS client for authentication verification.

## 4. Test Categories

### 4.1 Functional Testing
- Kerberos auth with krb5, krb5i, krb5p; modify security levels on NFSv4.1; NFSv4.1 with all three levels; add/delete export rules on krb volume.

### 4.2 Negative Testing
- NFSv3 with Kerberos levels (creation not allowed); no AD config; invalid AD; invalid allowedClients.

## 5. Risk Assessment

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| Krb misconfiguration | High | Medium | AD and allowed-clients validation; negative tests |
| NFSv3 + Kerberos allowed by mistake | Medium | Low | Explicit negative test (volume should not be created) |
| Export rule change breaking auth | Medium | Low | Add/delete tests; auth verification |

## 6. Test Execution Strategy

### 6.1 Test Phases
1. **Phase 1:** Kerberos auth with all three levels; modify levels on NFSv4.1; NFSv3 + Kerberos (negative); NFSv4.1 with all three levels.
2. **Phase 2:** AD prerequisite and invalid AD; invalid allowedClients; export rule add/delete on krb volume.

### 6.2 Automation Strategy
- API-driven volume create/update with Kerberos and export-policy; assertions on success/failure and (where possible) auth outcome.

### 6.3 Manual Testing Strategy
- Authentication verification (mount/access with krb5, krb5i, krb5p) where not automated.

## 7. Success Criteria
- All three security levels (krb5, krb5i, krb5p) authenticate successfully; modification between levels on NFSv4.1 succeeds.
- NFSv3 volume is not created with Kerberos security levels; NFSv4.1 supports all three levels when configured.
- Creation fails when AD is missing or invalid and when allowedClients are invalid.
- Export rules can be added and deleted on Kerberos volumes; operations succeed and authentication (where verified) succeeds.

## 8. Test Schedule
- [TBD]

## 9. Test Team
- **Test Lead:** [TBD]  
- **Test Engineers:** [TBD]

## 10. Deliverables
- This test plan, execution report, and automation artifacts.

---

# Appendix A: Test Case Tables

Test cases cover Kerberos security levels (krb5, krb5i, krb5p), NFSv4.1 vs NFSv3, AD dependency, allowedClients, and export rule add/delete.

---

## A.1 Kerberos Test Cases

| S. No. | Test Case ID   | Name | Test Step Description | Expected Result |
|--------|----------------|------|------------------------|-----------------|
| 1 | TC-KRB-01 | Kerberos authentication with all three security levels (krb5, krb5i, krb5p) | 1. Configure Kerberos with krb5; authenticate using krb5.<br>2. Configure Kerberos with krb5i; authenticate using krb5i.<br>3. Configure Kerberos with krb5p; authenticate using krb5p. | Ensure all three security levels are tested. Authentication should succeed with all three. |
| 2 | TC-KRB-02 | Modification of Kerberos security levels for NFSv4.1 volumes | 1. Create/have one NFSv4.1 volume with krb5.<br>2. Modify to krb5i.<br>3. Modify to krb5p. | Modifications should be successful and authentication should continue to work. |
| 3 | TC-KRB-03 | Create NFSv3 volume with Kerberos security levels (Negative) | 1. Create NFSv3 volume.<br>2. Configure Kerberos security levels (or attempt create with Kerberos). | Volume should not be created with specified Kerberos levels (NFSv3 does not support Kerberos in this way). |
| 4 | TC-KRB-04 | Create NFSv4.1 volume with all three Kerberos security levels | 1. Create NFSv4.1 volume.<br>2. Configure with krb5, krb5i, krb5p. | Volume should support all three security levels. |
| 5 | TC-KRB-05 | Create first NFSv4.1 volume with krb enabled – No AD configuration (Negative) | 1. With no AD configuration, attempt to create first NFSv4.1 volume with krb enabled. | Volume creation should fail due to missing AD config. |
| 6 | TC-KRB-06 | Create krb enabled volume with invalid AD configuration (Negative) | 1. With invalid AD IP / domain / realm config details, attempt to create first krb enabled volume. | Volume creation should fail. |
| 7 | TC-KRB-07 | Create NFSv4.1 volume with invalid allowedClients (Negative) | 1. Create NFSv4.1 volume with invalid allowedClients (invalid client configuration). | Volume creation should fail or be rejected. |
| 8 | TC-KRB-08 | Delete export rules under krb volume | 1. With krb volume created, delete export rules under krb volume. | Export rules should be deleted successfully. |
| 9 | TC-KRB-09 | Add extra export rules under krb volume | 1. With krb volume created, add extra export rules under krb volume. | Export rules should be added successfully. |
| 10 | TC-KRB-10 | Add and delete export rules under krb volume | 1. With krb volume created, add export rules.<br>2. Delete export rules. | Export rules should be added and deleted successfully. Authentication should succeed (where verified). |

---

## A.2 Kerberos Security Levels Summary

| Level | Description | Test Case |
|-------|-------------|-----------|
| krb5 | Authentication only | TC-KRB-01, TC-KRB-02 |
| krb5i | Authentication + integrity | TC-KRB-01, TC-KRB-02 |
| krb5p | Authentication + integrity + privacy | TC-KRB-01, TC-KRB-02 |

**Protocol support:** NFSv4.1 supports Kerberos (krb5/krb5i/krb5p); NFSv3 must not be created with Kerberos security levels (TC-KRB-03).

---
