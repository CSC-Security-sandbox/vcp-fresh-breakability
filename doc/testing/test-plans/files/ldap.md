# Test Plan: LDAP for VSA Pools and Volumes

**Document Version:** 1.0  
**Last Updated:** February 16, 2026  
**Status:** Draft  
**JIRA Tracker:** [TBD]  
**Component:** VSA – LDAP enable/disable on pool and volume; inheritance; AD/ADConfigId; allowLocalNFSUsersWithLdap; LDAP signing  
**Data Protection Classification:** Internal

## 1. Introduction

### 1.1 Overview
This document contains test cases for **LDAP** configuration on VSA pools and volumes: deploy pool/volume with LDAP enabled, disabled, or default (undefined attribute); deploy dual-protocol volumes in LDAP-enabled vs LDAP-disabled pools; update LDAP attribute (immutable – no operation); pool with LDAP enabled but undefined ADConfigId (fail); pool with LDAP and KMS enabled; volume request payload LDAP vs pool LDAP (inherit from pool); allowLocalNFSUsersWithLdap behavior (local vs LDAP user access); LDAP signing disabled; root user when not present on AD; and NFS/SMB volume deployment when LDAP or AD is not configured.

### 1.2 Scope
- **In Scope:** Pool and volume LDAP enabled/disabled/default; volume types (NFSv3, NFSv4, NFSv4 Kerberos, SMB, Both, Dual NFS+SMB) in LDAP-enabled/disabled pools; LDAP immutable on pool and volume; ADConfigId requirement; LDAP+KMS; inheritance of LDAP from pool; allowLocalNFSUsersWithLdap; LDAP signing; local vs LDAP user access; AD machine without LDAP.
- **Out of Scope:** LDAP server deployment; non-NFS/SMB protocols.

### 1.3 Related Requirements
- LDAP as pool-level attribute; volume inherits from pool (except SMB does not inherit LDAP from pool).
- LDAP is immutable – update requests ignored, no operation.
- ADConfigId required when pool has LDAP enabled.
- allowLocalNFSUsersWithLdap controls whether local users are allowed when LDAP is enabled.

## 2. Test Requirements

### 2.1 Requirements to be Tested
- Deploy pool with LDAP enabled; deploy volumes (NFSv3, NFSv4, NFSv4 Kerb, SMB, Both, Dual) within pool; list pools/volumes; mount NFSv3; access with local and LDAP user (local denied with secd.authsys.lookup error, LDAP allowed, owner/group correct).
- Deploy pool with LDAP disabled; deploy same volume types; list – pool and volumes with expected attributes.
- Deploy pool with default (undefined) LDAP attribute; deploy NFSv3, NFSv4 – LDAP disabled as default; volumes deployed.
- Deploy dual-protocol volumes in LDAP-disabled pool – volume deployment fails (precheck, LDAP not enabled for pool).
- Update LDAP attribute of pool (enabled→disabled, disabled→enabled) – immutable, no operation, LDAP unchanged.
- Update LDAP attribute of volume – immutable, no operation.
- Deploy pool with LDAP enabled but undefined ADConfigId – pool deployment should fail.
- Deploy pool with LDAP and KMS enabled; deploy volume types – pool/volumes deployed, KMS inherited by volumes.
- Pass LDAP disabled in volume request with pool LDAP enabled – volume request ignored, inherit from pool (LDAP enabled).
- Pass LDAP enabled in volume request with pool LDAP disabled – volume request ignored, inherit from pool (LDAP disabled).
- allowLocalNFSUsersWithLdap: set to false then true; repeat mount/access – local denied when false, LDAP allowed; when true both local and LDAP allowed.
- LDAP signing disabled; pool LDAP enabled; NFSv3 volume; access local and LDAP – local denied, LDAP allowed; root user when not on AD – root allowed.
- Deploy pool with AD where AD machine does not have LDAP configured – deploy NFS volume (fail – LDAP not configured), deploy SMB volume (success).

### 2.2 Requirements NOT in Scope
- LDAP server configuration; non-VSA AD/LDAP behavior.

## 3. Test Environment Requirements

### 3.1 Infrastructure
- VSA with pool and volume APIs; AD; NFS and SMB clients; ability to set LDAP on pool and (for inheritance tests) in volume request.

### 3.2 Test Data
- Valid AD; pool with LDAP enabled/disabled/default; volume types as above.

### 3.3 Dependencies
- AD; pool and volume APIs; NFS client for mount and user access tests.

## 4. Test Categories

### 4.1 Functional Testing
- Pool/volume deploy with LDAP enabled/disabled/default; list; mount and access (local vs LDAP); allowLocalNFSUsersWithLdap; LDAP+KMS.

### 4.2 Negative Testing
- Dual protocol in LDAP-disabled pool; pool LDAP enabled without ADConfigId; update LDAP (immutable); NFS volume when LDAP not configured on AD.

## 5. Risk Assessment

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| LDAP inheritance wrong | High | Medium | Explicit tests for volume LDAP vs pool LDAP |
| Immutable LDAP changed | Medium | Low | Update tests (expect no op) |
| Local vs LDAP access wrong | High | Medium | allowLocalNFSUsersWithLdap and access tests |

## 6. Test Execution Strategy

### 6.1 Test Phases
1. **Phase 1:** Deploy pool/volume with LDAP enabled/disabled/default; dual protocol in LDAP-disabled pool.
2. **Phase 2:** Update LDAP (pool and volume); ADConfigId; LDAP+KMS; volume request LDAP vs pool.
3. **Phase 3:** allowLocalNFSUsersWithLdap; LDAP signing; root user; AD without LDAP.

### 6.2 Automation Strategy
- API-driven pool/volume create and update; list; optional mount and user access scripts.

### 6.3 Manual Testing Strategy
- Mount NFS, access as local and LDAP user; verify error messages and owner/group.

## 7. Success Criteria
- Pool/volume deploy and list show expected LDAP attributes; volume inherits from pool (SMB does not inherit LDAP from pool).
- LDAP update is ignored (immutable); pool with LDAP and no ADConfigId fails; dual protocol in LDAP-disabled pool fails.
- Local vs LDAP access matches allowLocalNFSUsersWithLdap; NFS volume fails when LDAP not configured on AD; SMB succeeds.

## 8. Test Schedule
- [TBD]

## 9. Test Team
- **Test Lead:** [TBD]  
- **Test Engineers:** [TBD]

## 10. Deliverables
- This test plan, execution report, and automation artifacts.

---

# Appendix A: Test Case Tables

Test cases cover pool/volume LDAP enabled/disabled/default, dual-protocol in LDAP-disabled pool, LDAP immutable, ADConfigId requirement, LDAP+KMS, volume inheritance, allowLocalNFSUsersWithLdap, LDAP signing, and AD without LDAP.

---

## A.1 Deploy Pool/Volume with LDAP

| S. No. | Test Case ID | Name | Test Step Description | Expected Result |
|--------|--------------|------|------------------------|-----------------|
| 1 | TC-LDAP-01 | Deploy pool/volume with LDAP enabled | Create pool pool1 with LDAP enabled. Deploy volumes: NFSv3, NFSv4, NFSv4 Kerb, SMB, Both, Both Kerb, Dual (NFSv3+SMB) unix/ntfs, Dual (NFSv4+SMB) ntfs/unix, Dual with kerb. List pools/volumes. Mount NFSv3 with root. Access NFSv3 volume using local and LDAP user. | Pool deployed with expected attributes. Volumes deployed. Mount successful. Local users denied (secd.authsys.lookup...). LDAP user allowed; owner/group shown properly. |
| 2 | TC-LDAP-02 | Deploy pool/volume with LDAP disabled | Create pool pool1 with LDAP disabled. Deploy volumes: NFSv3, NFSv4, NFSv4 Kerb, SMB, Both, Both Kerb. List pools/volumes. | Pool and volumes deployed. Listed with expected attributes. |
| 3 | TC-LDAP-03 | Deploy pool/volume with default LDAP attribute | Create pool with undefined LDAP in request. Deploy NFSv3, NFSv4. List pools/volumes. | Pool deployed. LDAP disabled as default. Volumes deployed. Listed. |
| 4 | TC-LDAP-04 | Deploy dual-protocol volumes in LDAP-disabled pool (Negative) | Create pool with LDAP disabled. Deploy dual proto: (NFSv3+SMB) unix/ntfs, (NFSv4+SMB) ntfs/unix, with/without kerb. List pools. | Pool deployed. Volume deployment fails in precheck (LDAP not enabled for pool). Pool listed healthy. |
| 5 | TC-LDAP-07 | Deploy pool with LDAP enabled but undefined ADConfigId (Negative) | Deploy pool with LDAP enabled but undefined ADConfigId. | Pool deployment should fail (ADConfig required for LDAP pool). |
| 6 | TC-LDAP-08 | Deploy pool with LDAP and KMS enabled | Create pool with LDAP and KMS enabled. Deploy volume types (NFSv3, NFSv4, Kerb, SMB, Both, Dual). List pools/volumes. | Pool and volumes deployed. KMS inherited by all volumes. Listed. |

---

## A.2 Update LDAP Attribute (Immutable)

| S. No. | Test Case ID   | Name | Test Step Description | Expected Result |
|--------|----------------|------|------------------------|-----------------|
| 6 | TC-LDAP-06 | Update LDAP attribute of a pool | Update pool LDAP: enabled→disabled, disabled→enabled. List pool and volumes. | LDAP immutable. Update ignored (no operation). LDAP property unchanged for pool. |
| — | TC-LDAP-07V | Update LDAP attribute of a volume | Create pool with LDAP. Deploy NFS and SMB volumes. Update LDAP enabled attribute of volume: enabled→disabled, disabled→enabled. List volumes. | LDAP immutable. Update ignored. LDAP property unchanged for volumes. |

---

## A.3 Volume Request LDAP vs Pool LDAP (Inheritance)

| S. No. | Test Case ID   | Name | Test Step Description | Expected Result |
|--------|----------------|------|------------------------|-----------------|
| 9 | TC-LDAP-09 | Pass LDAP disabled in volume request with pool (LDAP enabled) – no impact | Create pool with LDAP enabled. Deploy volumes with LDAP disabled in request payload. List pool and volumes. Verify local vs LDAP user access (repeat step 3–4 from TC-LDAP-01). | Pool deployed. Volumes deployed. LDAP property in volume request ignored; volume inherits from pool. There should not be any impact on LDAP behavior (local denied, LDAP allowed as with pool LDAP enabled). |
| 10 | TC-LDAP-10 | Same as TC-LDAP-09 – verify list and attributes | Create pool with LDAP enabled. Deploy volumes with LDAP disabled in request. List pools and volumes. | Pool listed with expected attributes. Volumes listed with expected attributes and LDAP enabled (inherited). |
| 11 | TC-LDAP-11 | Pass LDAP enabled in volume request with pool (LDAP disabled) | Create pool with LDAP disabled. Deploy NFS and SMB volumes with LDAP enabled in request. List pool and volumes. | Pool deployed. Volumes deployed. LDAP in volume request ignored; volume inherits from pool (LDAP disabled). Pool listed with LDAP disabled; volumes listed with LDAP disabled. |
| 12 | TC-LDAP-12 | Same as TC-LDAP-11 – verify list and attributes | Create pool with LDAP disabled. Deploy volumes with LDAP enabled in request. List pool and volumes. | Pool listed with expected attributes and LDAP disabled. Volumes listed with expected attributes and LDAP disabled. |

---

## A.4 allowLocalNFSUsersWithLdap and LDAP Behavior

| S. No. | Test Case ID   | Name | Test Step Description | Expected Result |
|--------|----------------|------|------------------------|-----------------|
| 13 | TC-LDAP-13 | Verify allowLocalNFSUsersWithLdap for LDAP-enabled volumes | Create pool with LDAP. Deploy NFSv3 and SMB. Mount NFSv3, access with local and LDAP user. Update AD allowLocalNFSUsersWithLdap to false; repeat access. Update to true; repeat access. | Local denied (secd.authsys...), LDAP allowed when false. When true, both local and LDAP allowed; owner/group correct. No change in behavior after toggling as expected. |
| 14 | TC-LDAP-14 | Root user when not present on AD | Ensure AD has no domain user "root". Create pool with LDAP enabled. Create NFSv3 volume. Mount, access with root user. | Root user should be allowed access. |
| — | TC-LDAP-15 | LDAP signing disabled | Disable LDAP signing. Create pool with LDAP enabled. Deploy NFSv3. Mount, access with local and LDAP users. | Local denied, LDAP allowed. No impact on LDAP behavior. |
| — | TC-LDAP-16 | Pool with AD where AD machine does not have LDAP configured | Deploy pool with AD. Deploy NFS volume. Deploy SMB volume. | Pool deployment successful. NFS volume deployment fails (4xx – LDAP not configured). SMB volume deployment successful. |
| — | TC-LDAP-17 | Deploy pool with LDAP disabled; deploy NFS volumes with LDAP enabled in request | Deploy pool with LDAP disabled. Deploy NFS volumes within the pool with LDAP enabled in request payload. List pools and volumes. | Pool deployed. Volume deployed (LDAP from request ignored; inherit pool – LDAP disabled). List shows LDAP disabled. |
| — | TC-LDAP-18 | Deploy pool with LDAP enabled; deploy NFS volumes with LDAP disabled in request | Deploy pool with LDAP enabled. Deploy NFS volumes within the pool with LDAP disabled in request payload. List pools and volumes. | Pool deployed. Volume deployed (LDAP from request ignored; inherit pool – LDAP enabled). List shows LDAP enabled. |

---
