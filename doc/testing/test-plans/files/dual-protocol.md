# Test Plan: Dual-Protocol Volumes (NFS + SMB) for VSA

**Document Version:** 1.0  
**Last Updated:** February 16, 2026  
**Status:** Draft  
**JIRA Tracker:** [TBD]  
**Component:** VSA Files – Dual-protocol volumes (NFSv3/NFSv4 + SMB); LDAP/AD; Kerberos; security style; unix permissions; protocol update validation  
**Data Protection Classification:** Internal

## 1. Introduction

### 1.1 Overview
This document contains test cases for **dual-protocol** volumes (NFSv3 or NFSv4 + SMB) in VSA. It covers creation with LDAP enabled vs disabled (pool), with AD up vs down or incorrect, with Kerberos (NFSv4+SMB only); update of volume attributes (name, size, service level, snapshot policy, unix permissions) and validation that protocol-type update and invalid protocol combinations are rejected; unix permissions with UNIX vs NTFS security style; Kerberos restriction to NFSv4; AD delete when dual-protocol volumes exist; GET and default ldapEnabled; and SMB share settings.

### 1.2 Scope
- **In Scope:** Dual-protocol create (NFSv3+SMB, NFSv4+SMB) with LDAP/AD/Kerberos; negative cases (LDAP disabled pool, AD down, incorrect AD, protocol update, three protocols, unix permissions with NTFS, Kerberos with NFSv3+SMB); update attributes (name, size, service level, snapshot policy, unix permissions) by security style; AD delete dependency; GET ldapEnabled; SMB share settings.
- **Out of Scope:** Single-protocol-only behavior; protocols other than NFSv3, NFSv4, SMB.

### 1.3 Related Requirements
- Dual protocol limited to two protocol types (e.g. NFSv3+SMB or NFSv4+SMB); no NFSv3+NFSv4+SMB.
- LDAP enabled on pool required for dual-protocol volume creation; AD required when using SMB.
- Kerberos enabled only for NFSv4 volumes; unix permissions only with NFS (and security style UNIX).
- AD cannot be deleted while dual-protocol volumes using it exist.

## 2. Test Requirements

### 2.1 Requirements to be Tested
- Create dual-protocol volume with LDAP disabled pool fails; with LDAP enabled succeeds (NFSv3+SMB, NFSv4+SMB).
- Create dual-protocol (NFSv4+SMB) with Kerberos succeeds; mount succeeds.
- Create dual-protocol with AD down or incorrect AD details fails.
- Update of protocol types (e.g. to invalid values) fails; creation/update with three protocol types fails.
- Update volume attributes (name, size, service level, snapshot policy) for dual-protocol with security style SMB and UNIX; unix permissions update when security style is UNIX vs NTFS (NTFS + unix permissions fails).
- Unix permissions only when security style is UNIX; creation with unix permissions and security style NTFS fails.
- Kerberos with dual (NFSv3+SMB) fails; error indicates Kerberos only for NFSv4.
- AD delete not allowed while dual-protocol volumes exist; allowed after all such volumes are deleted.
- GET dual-protocol volume returns ldapEnabled true by default; verify for non-dual as needed.
- Dual-protocol with different SMB share settings; mount NFS and SMB, I/O, delete; ldapEnabled default.

### 2.2 Requirements NOT in Scope
- Single-protocol NFS or SMB-only tests not specific to dual protocol.

## 3. Test Environment Requirements

### 3.1 Infrastructure
- VSA environment with pool (LDAP enabled/disabled), AD, and volume create/update/delete; NFS and SMB clients for mount and I/O.

### 3.2 Test Data
- Valid AD and pool config; invalid/incorrect AD; dual-protocol parameters (protocols, security style, unix permissions, SMB share settings).

### 3.3 Dependencies
- LDAP on pool; AD for SMB; VSA volume API; NFS and SMB clients.

## 4. Test Categories

### 4.1 Functional Testing
- Create dual-protocol (NFSv3+SMB, NFSv4+SMB) with LDAP/AD; create NFSv4+SMB with Kerberos; update attributes (name, size, service level, snapshot policy, unix permissions where allowed); GET ldapEnabled; SMB share settings; AD delete after volumes removed.

### 4.2 Negative Testing
- LDAP disabled pool; AD down or incorrect AD; protocol update to invalid value; three protocol types; unix permissions with NTFS; Kerberos with NFSv3+SMB; AD delete while dual-protocol volumes exist.

## 5. Risk Assessment

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| Dual protocol with wrong LDAP/AD | High | Medium | Negative tests (LDAP disabled, AD down, incorrect AD) |
| Protocol or attribute misuse | Medium | Low | Validation tests (protocol update, three protocols, unix perms) |
| AD deleted while in use | High | Low | Test AD delete when volumes present vs removed |

## 6. Test Execution Strategy

### 6.1 Test Phases
1. **Phase 1:** Create dual-protocol (positive and negative); LDAP, AD, Kerberos; protocol-type validation.
2. **Phase 2:** Update attributes by security style; unix permissions validation; GET ldapEnabled.
3. **Phase 3:** AD delete dependency; SMB share settings; mount and I/O.

### 6.2 Automation Strategy
- API-driven create/update/GET/delete with assertions; optional mount and I/O scripts.

### 6.3 Manual Testing Strategy
- Mount on NFS and SMB clients and perform I/O where not automated.

## 7. Success Criteria
- Creation fails when LDAP disabled, AD down, or incorrect AD; succeeds with LDAP and valid AD; NFSv4+SMB with Kerberos succeeds.
- Protocol update and three-protocol combination are rejected; unix permissions with NTFS and Kerberos with NFSv3+SMB are rejected.
- Attribute updates (name, size, service level, snapshot policy, unix permissions where allowed) succeed; GET shows ldapEnabled as specified.
- AD delete fails while dual-protocol volumes exist and succeeds after they are removed.

## 8. Test Schedule
- [TBD]

## 9. Test Team
- **Test Lead:** [TBD]  
- **Test Engineers:** [TBD]

## 10. Deliverables
- This test plan, execution report, and automation artifacts.

---

# Appendix A: Test Case Tables

Test cases cover dual-protocol (NFS+SMB) creation with LDAP/AD/Kerberos, attribute updates, protocol and unix-permissions validation, AD delete dependency, and ldapEnabled.

---

## A.1 Dual-Protocol Test Cases

| S. No. | Test Case ID   | Name | Test Step Description | Expected Result |
|--------|----------------|------|------------------------|-----------------|
| 1 | TC-DP-01 | Create dual protocol vol (NFSv3/4 + SMB) with LDAP disabled pool (Negative) | Create dual-protocol volume (NFSv3/4 + SMB) when pool has LDAP disabled. | It should fail. |
| 2 | TC-DP-02 | Create dual protocol vol (NFSv3/4 + SMB) with LDAP | Create dual-protocol volume with LDAP: NFSv3+SMB, NFSv4+SMB and likewise. | Creation should succeed. |
| 3 | TC-DP-03 | Create dual protocol vol (NFSv4 + SMB) with Kerberos | Create dual-protocol volume (NFSv4 + SMB) with Kerberos. Test mount. | Successful. Test mount successful. |
| 4 | TC-DP-04 | Create dual protocol vol (NFSv3/4 + SMB) with AD down (Negative) | Create dual-protocol volume (NFSv3/4 + SMB) when AD is down. | It should fail. |
| 5 | TC-DP-05 | Create dual protocol vol (NFSv3/4 + SMB) with incorrect AD details (Negative) | Create dual-protocol volume (NFSv3/4 + SMB) with incorrect AD details. | It should fail. |
| 6 | TC-DP-06 | Update protocol (Negative) | Attempt to update the protocol (e.g. change protocol types) on a dual-protocol volume. | Update should fail. |
| 7 | TC-DP-07 | Update dual protocol volume attributes (security style SMB) | Create dual protocol volume (NFSv3 + SMB) with security style SMB. Mount on SMB client, perform I/O. Update volume attributes (name, size, service level, snapshot policy). Mount on SMB client, perform I/O. | Volume creation successful; I/O successful. Updates and I/O succeed. |
| 8 | TC-DP-08 | Update dual protocol volume attributes (security style UNIX) | Create dual protocol volume (NFSv3 + SMB) with security style UNIX. Mount on SMB client, perform I/O. Update volume attributes (name, size, service level, snapshot policy, unix permissions). Mount on NFS client, perform I/O. | Volume creation successful; I/O successful. Updates and I/O succeed. |
| 9 | TC-DP-09 | Update fails: unix permissions with security style NTFS (Negative) | 1. Create dual protocol volume with security style SMB (NTFS).<br>2. Update volume by adding unix permissions attribute to 0555. | Volume update should fail. Message: Unix permissions is only supported with NFS protocol volumes; Create/Update SMB volume without unix permissions. |
| 10 | TC-DP-10 | Update fails: invalid protocol types (Negative) | 1. Create dual protocol volume (NFSv3, SMB).<br>2. Update volume protocol types to invalid value (e.g. nfv3, SIFS). | Volume update fails. Message: protocolTypes.0 in body should be one of [NFSV3 NFSV4 SMB]. |
| 11 | TC-DP-11 | Create/update fails: three protocol types (Negative) | Create volume with protocol types (NFSv3, NFSv4, SMB). | Volume creation fails. Message: Dual protocol can't have more than 2 protocol types. |
| 12 | TC-DP-12 | AD cannot be deleted while dual protocol volumes exist | Create dual protocol vol1 (SMB + NFSv4.1). Create dual protocol vol2 (SMB + NFSv3). Delete the AD. Create one NFSv3 volume vol3. Delete dual protocol vol2. Delete the AD. Delete dual protocol vol1. Delete the AD. | AD delete should not be allowed while dual protocol volumes exist. AD delete should succeed after all dual protocol volumes are deleted. |
| 13 | TC-DP-13 | GET dual protocol volumes – ldapEnabled default | Create dual protocol vol1 (NFSv3 + SMB); GET vol1. Create dual protocol vol2 (NFSv4.1 + SMB); GET vol2. Create regular NFSv3 vol3; GET vol3. | ldapEnabled parameter is true by default for dual protocol volumes (vol1, vol2). ldapEnabled behavior for non-dual (vol3) as per API spec. |
| 14 | TC-DP-14 | Unix permissions only when security style is UNIX | 1. Create dual protocol volume (NFSv3 + SMB) with unixPermissions 755 and security style unix.<br>2. Mount the volume on NFS client.<br>3. Perform I/O on the volume. | Volume creation successful; I/O successful. |
| 15 | TC-DP-15 | Unix permissions cannot be set when security style is NTFS (Negative) | Create dual protocol volume (NFSv3 + SMB) with unixPermissions 755 and security style ntfs. | Volume creation fails. Error: Unix permissions is only supported with NFS protocol volumes; Create/Update SMB volume without unix permissions. |
| 16 | TC-DP-16 | Kerberos only for NFSv4 (Negative) | Create dual protocol volume (NFSv3 + SMB) with kerberosEnabled true. | Volume creation fails. Message: Kerberos feature is enabled for only NFSv4 volumes. |
| 17 | TC-DP-17 | Dual protocol volume with different SMB share settings | Create dual protocol volume with different SMB share settings. Mount on NFS client and perform I/O. Mount on SMB client and perform I/O. Delete the volume. | Volume creation successful. Verify default value of ldapEnabled is true for dual protocol volumes. |

---

## A.2 Dual-Protocol Rules Summary

| Rule | Test Case(s) |
|------|----------------|
| Pool must have LDAP enabled for dual protocol | TC-DP-01, TC-DP-02 |
| AD required (up and correct) for dual protocol with SMB | TC-DP-04, TC-DP-05 |
| At most two protocol types for dual protocol | TC-DP-11 |
| Protocol type update / invalid protocol types rejected | TC-DP-06, TC-DP-10 |
| Kerberos only with NFSv4 | TC-DP-03, TC-DP-16 |
| Unix permissions only with security style UNIX / NFS | TC-DP-09, TC-DP-14, TC-DP-15 |
| AD cannot be deleted while dual protocol volumes exist | TC-DP-12 |
| ldapEnabled true by default for dual protocol | TC-DP-13, TC-DP-17 |

---
