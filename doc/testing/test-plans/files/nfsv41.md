# Test Plan: NFSv4.1 and Kerberos for VSA

**Document Version:** 1.0  
**Last Updated:** February 16, 2026  
**Status:** Draft  
**JIRA Tracker:** [TBD]  
**Component:** VSA Files – NFSv4.1 volumes; Kerberos (krb5, krb5i, krb5p); LDAP; export policy (hasRootAccess, allowedClients, accessType); UNIX permissions; AD dependency; protocol update validation  
**Data Protection Classification:** Internal

## 1. Introduction

### 1.1 Overview
This document contains test cases for **NFSv4.1** and **Kerberos** in VSA: Kerberos security levels (krb5, krb5i, krb5p) create and modify; NFSv3 with Kerberos (negative); optional Kerberos parameters; export policy display and convert Kerberos to non-Kerberos; RO/RW rules and validation; mount instructions and SPN; AD dependency (delete realm, NetBIOS/domain/kdcIP/adName update); LDAP with NFSv4.1 (deploy, inherit, allowLocalNFSUsersWithLdap, migrate volume); export policy (allowedClients, add/delete rules, hasRootAccess on/off, accessType ro/rw); all protocol mix (SMB, NFSv3, NFSv4.1, Kerberos) and protocol update negative; UNIX permissions; negative and boundary (AD invalid, DNS, NetBIOS, pod restart); concurrent Kerberos volume operations.

### 1.2 Scope
- **In Scope:** NFSv4.1 volume create/update with Kerberos; NFSv3 + Kerberos (rejected); export policy rules (Kerberos, hasRootAccess, allowedClients); AD/LDAP for Kerberos and SMB; LDAP inherit and allowLocalNFSUsersWithLdap; UNIX permissions; protocol update validation; 3P SO environment.
- **Out of Scope:** NFS server internals; non-NFSv4.1 protocols except where mixed in same pool.

### 1.3 Related Requirements
- Kerberos only for NFSv4.1; at least one Kerberos rule when kerberosEnabled; RO/RW rule validation.
- AD required for Kerberos; AD/realm delete when Kerberos volumes exist (not allowed).
- Export policy: hasRootAccess on/off, accessType ro/rw; allowedClients validation; rule limit (e.g. 20).
- LDAP inherit from pool; allowLocalNFSUsersWithLdap for local vs LDAP user access.

## 2. Test Requirements

### 2.1 Requirements to be Tested
- Create NFSv3 volume with Kerberos – fail; NFSv4.1 with all three Kerberos levels – succeed; modify Kerberos levels on NFSv4.1 – succeed; optional Kerberos params default; export policy display after modify; convert Kerberos to non-Kerberos (update not allowed); at least one Kerberos rule when enabled; mount instructions with sec and SPN; RO and RW rules (read-only vs read-write); RO and RW both false – fail.
- Kerberos auth for krb5, krb5i, krb5p; mount with sec=sys vs Kerberos (correct fail/success).
- AD: delete realm when Kerberos volumes exist – not allowed; NetBIOS name change – access/remount OK; domain change when Kerberos volumes exist – not allowed; kdcIP/adName update with Kerberos volumes – succeed.
- LDAP: deploy pool/volume with LDAP enabled/disabled/default; volume inherit; allowLocalNFSUsersWithLdap; migrate volume LDAP pool to non-LDAP pool (fail); root user when not on AD; 1024/1025 group memberships; LDAP signing disabled; AD shut down – access denied; update ldapEnabled (ignored); LDAP persistence after deleting non-Kerberos volumes.
- Export policy: invalid allowedClients – fail; delete/add rules on Kerberos volume; hasRootAccess on/off with accessType ro/rw; update hasRootAccess on→off and off→on; multiple rules; allowedClients length (e.g. 4096); invalid/unsupported hasRootAccess.
- All protocol: SMB, NFSv3, NFSv4.1, NFSv4.1+Kerberos in same pool; update NFSv3 to NFSv4.1 / dual / Kerberos – fail; Kerberos with NFSv3 – fail; 3P SO – NFSv4.1, dual, Kerberos create fail; dual (NFSv4.1+SMB) succeed.
- UNIX permission 0755, 0770→0755; Kerberos+LDAP volume with 0755.
- Negative: AD not complete, invalid AD, KDC/AD name not set, DNS order, NetBIOS boundary, pod restart during create.
- Concurrent: two accounts, same AD; parallel Kerberos volume create/delete/modify.

### 2.2 Requirements NOT in Scope
- NFS client/server protocol details beyond mount and R/W; KDC/AD server setup.

## 3. Test Environment Requirements

### 3.1 Infrastructure
- VSA with pool and volume APIs; AD; NFS client; ability to create NFSv3, NFSv4.1, SMB, dual-protocol, Kerberos volumes.

### 3.2 Test Data
- Valid AD and LDAP; Kerberos export rules; export policy (allowedClients, hasRootAccess, accessType); invalid AD and allowedClients for negative cases.

### 3.3 Dependencies
- AD; LDAP; pool and volume APIs; NFS client for mount and R/W.

## 4. Test Categories

### 4.1 Functional Testing
- Kerberos create/modify/RO/RW; LDAP deploy and inherit; export policy add/delete and hasRootAccess; UNIX permissions; all protocol mix; AD update (NetBIOS, kdcIP, adName).

### 4.2 Negative Testing
- NFSv3+Kerberos; Kerberos to non-Kerberos update; no Kerberos rule when enabled; RO+RW false; AD delete/domain change when Kerberos in use; invalid allowedClients; protocol update; 3P SO; AD invalid/KDC not set.

### 4.3 Integration Testing
- LDAP + Kerberos; allowLocalNFSUsersWithLdap; volume migrate between pools; concurrent Kerberos operations.

## 5. Risk Assessment

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| Kerberos/AD misconfiguration | High | Medium | AD dependency and negative tests |
| Export policy wrong (hasRootAccess) | Medium | Low | Explicit hasRootAccess and ro/rw tests |
| LDAP inherit or allowLocal wrong | High | Medium | LDAP and access tests |

## 6. Test Execution Strategy

### 6.1 Test Phases
1. **Phase 1:** Kerberos create and modify; RO/RW rules; mount and auth; AD dependency.
2. **Phase 2:** LDAP with NFSv4.1; export policy (rules, hasRootAccess, allowedClients); all protocol and protocol update.
3. **Phase 3:** UNIX permissions; negative and boundary; concurrent.

### 6.2 Automation Strategy
- API-driven create/update/GET; assertions on success/failure and export policy; optional mount and R/W scripts.

### 6.3 Manual Testing Strategy
- Mount with sec=sys vs Kerberos; R/W and root access verification.

## 7. Success Criteria
- Kerberos and LDAP tests pass as specified; export policy and hasRootAccess behave as expected; invalid combinations and protocol updates are rejected; AD/LDAP dependency and inheritance rules hold.

## 8. Test Schedule
- [TBD]

## 9. Test Team
- **Test Lead:** [TBD]  
- **Test Engineers:** [TBD]

## 10. Deliverables
- This test plan, execution report, and automation artifacts.

---

# Appendix A: Test Case Tables

Test cases are grouped by: Kerberos (NFSv4.1), NFSv4 with AD, NFSv4 with LDAP, Export Policies, All Protocol, UNIX Permissions, Negative & Boundary, and Concurrent.

---

## A.1 Kerberos (NFSv4.1)

| S No. | Test Case ID   | Name | Expected Result |
|-------|----------------|------|-----------------|
| 1 | TC-NFS41-KRB-01 | Create NFSv3 volume with Kerberos security levels (Negative) | Not allowed to create NFSv3 volume with Kerberos. |
| 2 | TC-NFS41-KRB-02 | Create NFSv4.1 volume with all three Kerberos levels (krb5, krb5i, krb5p) | Volume created; mount with all three levels; R/W successful; GET and ONTAP show protocol and security level. |
| 3 | TC-NFS41-KRB-03 | Modify Kerberos security levels on NFSv4.1 volumes | Modify vol1/vol2/vol3 from krb5/krb5i/krb5p to different levels; mount and R/W succeed; GET reflects change. |
| 4 | TC-NFS41-KRB-04 | Optional parameters in Kerberos export policy | Create NFSv4.1 with only Kerberos5ReadOnly true; others false. GET shows defaults false, Kerberos5ReadOnly true. |
| 5 | TC-NFS41-KRB-05 | Export policy displayed after modification | Create three Kerberos volumes (krb5, krb5i, krb5p); modify export policy via API; changes reflected. |
| 6 | TC-NFS41-KRB-06 | Convert Kerberos NFSv4.1 to non-Kerberos and vice versa | Kerberos→non-Kerberos update not allowed. Non-Kerberos→Kerberos not supported. |
| 7 | TC-NFS41-KRB-07 | When Kerberos enabled, at least one Kerberos rule required (Negative) | Create Kerberos-enabled volume without Kerberos rule in export policy – fail with proper error. |
| 8 | TC-NFS41-KRB-08 | Mount instructions with sec and SPN prefix | NFSv4.1 Kerberos volumes (krb5, krb5i, krb5p) show sec=krb5/krb5i/krb5p and SPN prefix `NFS-` + up to 10 chars from CIFS NetBIOS name. |
| 9 | TC-NFS41-KRB-09 | Kerberos RO rules | Create volumes with rorule krb5/krb5i/krb5p, rwrule true; then set rwrule false. Read allowed, write denied. |
| 10 | TC-NFS41-KRB-10 | Kerberos R/W rules | Create volumes with rorule false, rwrule krb5/krb5i/krb5p. Read and write allowed. |
| 11 | TC-NFS41-KRB-11 | RO and RW both false (Negative) | Create with rorule false, rwrule false – fail (mismatch KerberosEnabled and export policy). |
| 12 | TC-NFS41-KRB-12 | Kerberos auth for all levels; sec=sys vs Kerberos | vol1–4 NFSv4.1 (krb5, krb5i, krb5p, any); vol5 NFSv3. Mount vol1–4 with sec=sys – fail; vol5 with Kerberos – fail. Mount vol1–4 with Kerberos – succeed; vol5 with sec=sys – succeed. R/W correct. |

---

## A.2 NFSv4 with AD

| S No. | Test Case ID   | Name | Expected Result |
|-------|----------------|------|-----------------|
| 1 | TC-NFS41-AD-01 | Delete AD/realm while Kerberos volumes exist | Cannot delete AD until all Kerberos-enabled volumes deleted. After last Kerberos volume and pool deleted, AD delete succeeds. |
| 2 | TC-NFS41-AD-02 | Change AD CIFS NetBIOS server name | Create SMB, NFSv3, NFSv4.1 with/without Kerberos. Update AD NetBIOS name. Access and R/W succeed; remount if needed; mount instructions updated. |
| 3 | TC-NFS41-AD-03 | AD domain change when Kerberos volumes exist (Negative) | Domain update not allowed when Kerberos NFS volumes present – proper error. |
| 4 | TC-NFS41-AD-04 | Update kdcIP and adName with Kerberos volumes | Update kdcIP, adName, or both – succeed. After deleting Kerberos volumes, updates still succeed. |
| 5 | TC-NFS41-AD-05 | Add AD | AD configured; SMB volume created successfully. |
| 6 | TC-NFS41-AD-06 | Update AD (editable vs non-editable) when IN_USE | Pool LDAP enabled; deploy NFS and SMB (AD IN_USE). Editable AD params update succeed; non-editable – meaningful error. |
| 7 | TC-NFS41-AD-07 | AD state transitions | Deploy pool (LDAP); deploy NFS then SMB; delete volume; delete pool. AD state READY→IN_USE→READY (or cleanup) as specified. |

---

## A.3 NFSv4 with LDAP

| S No. | Test Case ID   | Name | Expected Result |
|-------|----------------|------|-----------------|
| 1 | TC-NFS41-LDAP-01 | Deploy pool/volume with LDAP enabled | Pool and NFSv3/SMB volumes deployed. Mount NFSv3; local denied (secd.authsys...), LDAP allowed; owner/group correct. SMB does not inherit LDAP from pool. |
| 2 | TC-NFS41-LDAP-02 | Deploy pool/volume with LDAP disabled | Pool and volumes deployed; listed with LDAP disabled. |
| 3 | TC-NFS41-LDAP-03 | Deploy with default LDAP attribute | LDAP disabled by default; volumes deployed. |
| 4 | TC-NFS41-LDAP-04 | Update LDAP attribute of pool/volume | LDAP immutable; update ignored; property unchanged. |
| 6 | TC-NFS41-LDAP-06 | Pass LDAP enabled in volume request with pool LDAP disabled | Volume request ignored; inherit pool (LDAP disabled). |
| 7 | TC-NFS41-LDAP-07 | Pass LDAP disabled in volume request with pool LDAP enabled | Volume request ignored; inherit pool (LDAP enabled). |
| 5 | TC-NFS41-LDAP-05 | Update LDAP attribute of volume | Create pool with LDAP; deploy NFS and SMB. Update LDAP for NFS and SMB volume. List. | LDAP immutable; update ignored; property unchanged. |
| 8 | TC-NFS41-LDAP-08 | Migrate volume from LDAP-enabled pool to LDAP-disabled pool (Negative) | Update NFS/SMB volume to different pool – fail. |
| 9 | TC-NFS41-LDAP-09 | allowLocalNFSUsersWithLdap disabled; LDAP user lookup for Kerberos volumes | Disable allowLocalNFSUsersWithLdap on AD. Create NFSv4.1 Kerberos and NFSv3/NFSv4.1 normal volumes with ldapEnabled. Delete all non-Kerberos volumes. Ensure LDAP config not deleted. Access remaining Kerberos volumes with LDAP user. | User lookup from AD LDAP server when LDAP user accesses NFSv4.1 Kerberos volumes. |
| 10 | TC-NFS41-LDAP-10 | Migrate volume from LDAP-disabled pool to LDAP-enabled pool (Negative) | Update NFS/SMB volume to different pool – fail. |
| 11 | TC-NFS41-LDAP-11 | Migrate LDAP volume to pool with undefined ADconfigId (Negative) | Create pool with LDAP enabled; create NFS volume. Create pool p2 without AD. Update NFS volume to p2 poolID. | Update NFS fail. |
| 12 | TC-NFS41-LDAP-12 | Create NFSv4.1 volume with Kerberos and LDAP | Create NFSv4.1 with Kerberos and LDAP. Mount and R/W with LDAP user. | Volume creation succeed. Local users denied (secd.authsys...); LDAP allowed. |
| 13 | TC-NFS41-LDAP-13 | allowLocalNFSUsersWithLdap for LDAP-enabled volumes | Local denied, LDAP allowed when false; update to true – both allowed; owner/group correct. |
| 14 | TC-NFS41-LDAP-14 | Root user not present on AD | Root user allowed access. |
| 15 | TC-NFS41-LDAP-15 | LDAP user access with 1024 group memberships (NFSv3) | Pool with LDAP; LDAP user in 1024 groups; deploy NFSv3; access with local and LDAP users. | Mount successful; LDAP allowed; owner/group correct. |
| 16 | TC-NFS41-LDAP-16 | LDAP user access with 1025 group memberships (NFSv3) | Pool with LDAP; LDAP user in 1025 groups; deploy NFSv3; access with local and LDAP users. | LDAP allowed; owner/group correct; write succeed if LDAP returns proper GID. |
| 17 | TC-NFS41-LDAP-17 | Dual-Protocol (NFSv3 & NFSv4.1) with allowLocalNFSUsersWithLdap disabled | Set allowLocalNFSUsersWithLdap false; create vol1 NFSv3 & NFSv4.1; mount at two points (NFSv3 and NFSv4); access with local and LDAP. | Local denied (secd.authsys...); LDAP allowed; owner/group correct. |
| 18 | TC-NFS41-LDAP-18 | LDAP Signing disabled (NFSv3) | Disable LDAP Signing; pool LDAP enabled; deploy NFSv3; mount; access local and LDAP. | Local denied; LDAP allowed; no impact on LDAP behavior. |
| 19 | TC-NFS41-LDAP-19 | AD machine shut down – NFSv3 access | Pool LDAP enabled; deploy NFSv3; mount with root; shut down AD; access with local and LDAP. | Local and LDAP denied with appropriate error (e.g. lookup failure). |
| 20 | TC-NFS41-LDAP-20 | Global ILB and cross-regional SMB with LDAP-enabled pool | Deploy pool with LDAP and Global ILB; create NFS and SMB volumes; mount SMB from client in different regional AD. | Pool and SMB deployed; SMB mount work on different regional AD. |
| 21 | TC-NFS41-LDAP-21 | Pool/volume when LDAP not configured on AD machine | Pool with AD, AD machine without LDAP. NFSv4 volume deploy – fail (4xx LDAP not configured). SMB deploy – success. |
| 22 | TC-NFS41-LDAP-22 | Update ldapEnabled on existing NFSv3/NFSv4.1 volume | Update ldapEnabled true→false – ignored; value remains true. |
| 23 | TC-NFS41-LDAP-23 | LDAP persistence after deleting non-Kerberos volumes | Delete non-Kerberos volumes; LDAP config not deleted; LDAP user lookup still from AD for remaining Kerberos volumes. |

---

## A.4 NFSv4.1 Export Policies

| S No. | Test Case ID   | Name | Expected Result |
|-------|----------------|------|-----------------|
| 1 | TC-NFS41-EXP-01 | Invalid allowedClients (Negative) | Volume creation fail with proper error (e.g. alphanumeric, integer, invalid IP). |
| 2 | TC-NFS41-EXP-02 | Delete export rules under Kerberos volume | Create with 2 rules; delete rule #2; mount and R/W succeed. GET and config reflect change. |
| 3 | TC-NFS41-EXP-03 | Add export rules to Kerberos volume | Add rules; GET and config reflect new rules. |
| 4 | TC-NFS41-EXP-04 | Add and delete export rules | Add then delete rules; only original rule remains; R/W succeed. |
| 5 | TC-NFS41-EXP-05 | Verify volume creation with export-policy combinations (Negative) | Volume creation failed; update also fails. |
| 6 | TC-NFS41-EXP-06 | Export-policy rule with allowedClients comma-separated (length limit) | allowedClients separated by commas; length limit 4096 characters. Update with limit and observe; allowedClients work within limit. |
| 7 | TC-NFS41-EXP-07 | hasRootAccess: "true", accessType: READ_WRITE | Root access only for matching client. |
| 8 | TC-NFS41-EXP-08 | hasRootAccess: "false", accessType: READ_WRITE | Root access not granted. |
| 9 | TC-NFS41-EXP-09 | Multiple hasRootAccess export policies | Create with 3 rules: Rule 1 (hasRootAccess: "true", client 1), Rule 2 (hasRootAccess: "false", client 2), Rule 3 (no hasRootAccess). Mount; root access only for client 1. |
| 10 | TC-NFS41-EXP-10 | hasRootAccess: "true", accessType: READ_ONLY | Root read-only. |
| 11 | TC-NFS41-EXP-11 | hasRootAccess: "false", accessType: READ_ONLY | Root as nobody, read-only. |
| 12 | TC-NFS41-EXP-12 | Update export policy hasRootAccess: "true" to "false" | Initial root access granted; after update root access no longer granted. |
| 13 | TC-NFS41-EXP-13 | Update export policy hasRootAccess: "false" to "true" | After update root access granted for matching client. |
| 14 | TC-NFS41-EXP-14 | Update NFSv4.1 volume with multiple export policy rules | Create with 3 rules; mount (root to client 1 only). Edit to 5 new rules; verify root access per updated rules (e.g. client 2 RW, client 4 RO, denied 1/3). |
| 15 | TC-NFS41-EXP-15 | Create NFSv4.1 volume without specifying hasRootAccess / invalid hasRootAccess | Deploy with hasRootAccess invalid or NONE – fail. Deploy with hasRootAccess omitted – succeed; default behavior (root for matching client). |
| 16 | TC-NFS41-EXP-16 | Unsupported value for hasRootAccess (Negative) | Deploy with hasRootAccess: unsupported value. Volume deployment fail. |
| — | TC-NFS41-EXP-17 | Volume with >20 export-policy rules (Negative) | Create volume with 21 rules in body – volume creation fail. Update with >20 rules – update fail. |
| — | TC-NFS41-EXP-18 | protocolTypes nfsv3 with export rule nfsv4/nfsv4.1 (Negative) | Create volume protocolTypes=nfsv3 with rule nfsv4=true or nfsv4.1=true – fail. NFSv4.1 volume updated with rule nfsv3=true – fail. |
| — | TC-NFS41-EXP-19 | NFSv3 volume with rule nfsv3 and nfsv4.1 both true (Negative) | Create NFSv3 volume with one rule; patch with new rule nfsv3 and nfsv4.1 true – fail. |
| — | TC-NFS41-EXP-20 | Update failed volumes (Negative) | Update failed volumes (size, name, export-policy, snapshot-policy, etc.) – observe behavior per API spec. |

---

## A.5 All Protocol and Protocol Update

| S No. | Test Case ID   | Name | Expected Result |
|-------|----------------|------|-----------------|
| 1 | TC-NFS41-ALL-01 | All volume types in same pool | SMB, NFSv3, NFSv4.1, NFSv4.1+Kerberos – create and R/W succeed; validate from GET and ONTAP. |
| 2 | TC-NFS41-ALL-02 | Same as ALL-01 – vol1 SMB, vol2 NFSv3, vol3 NFSv4.1, vol4 NFSv4.1+Kerberos | R/W succeed on all; same vserver for user account. |
| 3 | TC-NFS41-ALL-03 | Update NFSv3 protocol to NFSv4.1 (Negative) | Update fail with proper error. |
| 4 | TC-NFS41-ALL-04 | Update NFSv3 protocol to NFSv3 & NFSv4.1 dual (Negative) | Update fail with proper error. |
| 5 | TC-NFS41-ALL-05 | Update NFSv3 protocol to NFSv4.1 & Kerberos (Negative) | Update fail with proper error. |
| 6 | TC-NFS41-ALL-06 | Update NFSv4.1 protocol to NFSv4.1 & Kerberos (Negative) | Update fail with proper error. |
| 7 | TC-NFS41-ALL-07 | Update NFSv4.1 protocol to NFSv3 (Negative) | Update fail with proper error. |
| 8 | TC-NFS41-ALL-08 | Update NFSv4.1 protocol to NFSv3 & NFSv4.1 dual (Negative) | Update fail with proper error. |
| 9 | TC-NFS41-ALL-09 | Create Kerberos-enabled volume with NFSv3 (Negative) | Volume creation fail. |
| 10 | TC-NFS41-ALL-10 | 3P Single-Protocol-Only: NFSv4.1, dual, Kerberos (Negative) | Volume creation fail. Account should have no volume present. |
| 11 | TC-NFS41-ALL-11 | Dual-Protocol (NFSv4.1 + SMB) | Volume creation succeed. |

---

## A.6 UNIX Permissions

| S No. | Test Case ID   | Name | Expected Result |
|-------|----------------|------|-----------------|
| 1 | TC-NFS41-UNIX-01 | UNIX permission 0755 for NFSv4.1 | Mount point 0755; others read+execute, not write; overview shows Unix Permissions. |
| 2 | TC-NFS41-UNIX-02 | UNIX permission 0755 for Kerberos & LDAP NFSv4.1 | Same as above. |
| 3 | TC-NFS41-UNIX-03 | UNIX permission 0755 for Dual-Protocol (NFSv3 & NFSv4.1) | 0755 on mount; others no write; check on both NFSv3 and NFSv4.1 mount points; overview shows Unix Permissions. |
| 4 | TC-NFS41-UNIX-04 | Update UNIX permission 0770 to 0755 | GET shows 0770 then 0755; others read+execute. |

---

## A.7 Negative & Boundary

| S No. | Test Case ID   | Name | Expected Result |
|-------|----------------|------|-----------------|
| 1 | TC-NFS41-NEG-01 | AD not complete – first Kerberos NFSv4.1 volume (Negative) | Volume creation fail. |
| 2 | TC-NFS41-NEG-02 | Invalid AD IP/domain/realm – Kerberos volume (Negative) | Volume creation fail. |
| 3 | TC-NFS41-NEG-03 | KDC IP and AD name not set in AD configuration (Negative) | Do not add KDC IP and AD name when adding AD. Create pool; create Kerberos volume. Volume create job fails in CVS (asynchronous). |
| 4 | TC-NFS41-NEG-04 | Multiple DNS: first proper, second wrong | Kerberos volume creation succeed (at least one DNS proper). |
| 5 | TC-NFS41-NEG-05 | Multiple DNS: first wrong, second proper | Kerberos volume creation succeed (at least one DNS proper). |
| 6 | TC-NFS41-NEG-06 | CIFS NetBIOS name boundary (e.g. 15 chars, 1 char); AES-256; mount and R/W | Mount and R/W succeed; NetBIOS name boundary and SPN/encryption as per spec. |
| 7 | TC-NFS41-NEG-07 | Pod restart during Kerberos volume create | Volume creation succeed. |

---

## A.8 Concurrent

| S No. | Test Case ID   | Name | Expected Result |
|-------|----------------|------|-----------------|
| 1 | TC-NFS41-CON-01 | Kerberos volumes from 2+ accounts, same AD | Create vol1/vol2 from two NetApp accounts, same AD; mount and R/W succeed. |
| 2 | TC-NFS41-CON-02 | Parallel Kerberos volume creation | Parallel create from two accounts – succeed; mount and R/W succeed. |
| 3 | TC-NFS41-CON-03 | Parallel Kerberos volume modify and delete | Parallel modify (e.g. krb5→krb5p) and delete – succeed. |

---
