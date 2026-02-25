# Test Plan: Active Directory (AD) Credentials for VSA

**Document Version:** 1.0  
**Last Updated:** February 16, 2026  
**Status:** Draft  
**JIRA Tracker:** [TBD]  
**Component:** VSA – Active Directory credentials (SDE/SMB, encryptDCConnections / encryptADCommunication)  
**Data Protection Classification:** Internal

## 1. Introduction

### 1.1 Overview
This document contains test cases for Active Directory (AD) credential management in VSA: Create, List, Get, Update, and Delete AD credentials in a region (SDE). It covers validation (required/optional fields, security settings, user groups), error handling (invalid domain/DNS/netBIOS/resourceId, duplicates, length limits), and behavior when AD is "In Use" (NetBIOS/OU updates, encryptADCommunication, faulty AD recovery). It also covers SMB volume lifecycle with AD and encryptDCConnections/encryptADCommunication.

### 1.2 Scope
- **In Scope:** Create AD (required/optional/security/user groups), List/Get/Delete AD, Update AD (Created and In Use states), validation errors (400/409), encryptDCConnections/encryptADCommunication (POST/GET/PUT), NetBIOS and organizationalUnit update when AD is in use, faulty AD update and volume creation, SMB volumes on default/non-default and multi-level OU.
- **Out of Scope:** AD domain controller or LDAP behavior outside VSA API; non-SMB protocols.

### 1.3 Related Requirements
- Active Directory credential CRUD and validation
- encryptDCConnections / encryptADCommunication behavior in Create, Get, and Update
- AD "In Use" semantics (NetBIOS, OU updates, volume IO)

## 2. Test Requirements

### 2.1 Requirements to be Tested
- AD creation with all required fields (username, password, domain, DNS, netBIOS, resourceId) and optional fields (organizationalUnit, site, kdcIP, kdcHostname, description).
- AD creation with security settings (aesEncryption, ldapSigning, encryptDCConnections, allowLocalNFSUsersWithLdap) and user groups (administrators, backupOperators, securityOperators).
- List AD and Get AD by UUID; behavior after Delete (404 for deleted, list reflects deletions).
- Create/Update validation: invalid domain/DNS/netBIOS/resourceId format, duplicate resourceId (409), empty username/password, length limits (255), invalid user group format, duplicate user in groups.
- Delete AD (success when no volumes; 409 when AD is in use).
- Update AD in Created and In Use states; NetBIOS and organizationalUnit update when AD is in use; update with supported vs non-supported parameters.
- encryptDCConnections/encryptADCommunication: default on POST, GET response, PUT updates (true/false combinations); impact on volumes.
- Faulty AD: volume creation fails; update AD to valid config; subsequent volume creation succeeds.
- SMB volumes in non-default OU and multi-level OU.

### 2.2 Requirements NOT in Scope
- AD/LDAP server internals; non-SMB use of AD credentials.

## 3. Test Environment Requirements

### 3.1 Infrastructure
- VSA/SDE environment with AD credential API; region(s) for Create/List/Get/Update/Delete AD; storage pools and SMB volume support.

### 3.2 Test Data
- Valid AD details (domain, DNS, netBIOS, resourceId, username, password); optional OU, site, kdcIP, kdcHostname; invalid payloads for negative cases.

### 3.3 Dependencies
- VSA AD credential API; storage pool and SMB volume APIs; optional SDE volume support for encryptADCommunication and "In Use" scenarios.

## 4. Test Categories

### 4.1 Functional Testing
- Create AD (required, optional, security, user groups); List/Get/Delete; Update in Created and In Use states; encryptDCConnections/encryptADCommunication.

### 4.2 Integration Testing
- SMB volume create/mount/IO/update/delete with AD; NetBIOS/OU update with volumes in use; faulty AD update and new volume creation.

### 4.3 Negative Testing
- Invalid Create/Update payloads (400/409); Delete when in use (409); Get by non-existent/invalid UUID (404/400).

## 5. Risk Assessment

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| AD misconfiguration | High | Medium | Validation rules, negative tests |
| In-use AD update breaking volumes | High | Low | Update rules, IO verification |
| encryptADCommunication mismatch | Medium | Low | GET/PUT and volume-level checks |

## 6. Test Execution Strategy

### 6.1 Test Phases
1. **Phase 1:** Create AD (positive and negative), List, Get, Delete.
2. **Phase 2:** Update AD (Created and In Use); NetBIOS/OU; encryptADCommunication.
3. **Phase 3:** SMB volumes with AD; faulty AD recovery; non-default and multi-level OU.

### 6.2 Automation Strategy
- API-driven Create/List/Get/Update/Delete with assertions on status codes and response body; optional volume mount/IO where needed.

### 6.3 Manual Testing Strategy
- Where volume mount and IO are required, manual or semi-automated verification.

## 7. Success Criteria
- All validation tests return expected 400/409/404.
- All positive Create/Update/Delete/Get/List behave as specified.
- encryptDCConnections/encryptADCommunication default and updates match GET and (where applicable) volume behavior.
- NetBIOS/OU updates when AD is in use succeed as specified; volume IO continues to work.

## 8. Test Schedule
- [TBD]

## 9. Test Team
- **Test Lead:** [TBD]  
- **Test Engineers:** [TBD]

## 10. Deliverables
- This test plan, execution report, and automation artifacts.

---

# Appendix A: Test Case Tables

Test cases cover AD credential CRUD, validation (domain/DNS/netBIOS/resourceId, user groups), encryptDCConnections/encryptADCommunication, NetBIOS/OU update when AD is in use, and faulty AD recovery. TC-NFS-201/202/203 are alternate IDs for the encryptDCConnections scenarios (TC-AD-106/107/108).

---

## A.1 Create AD – Positive

| Test Case ID | Name / Description | Test Steps | Expected Result |
|--------------|--------------------|------------|-----------------|
| TC-AD-101 | Add AD credential in a region; Perform List AD | Create AD with valid details in SDE | AD created successfully in VSA; List AD shows valid details |
| TC-AD-102 | Create AD with all required fields | Create AD with username, password, domain, DNS, netBIOS, resourceId | AD created with all required fields populated |
| TC-AD-103 | Create AD with optional fields | Create AD with required fields plus organizationalUnit, site, kdcIP, kdcHostname, description | AD created with optional fields properly set |
| TC-AD-104 | Create AD with security settings | Create AD with aesEncryption=true, ldapSigning=true, encryptDCConnections=true, allowLocalNFSUsersWithLdap=true | AD created with all security settings enabled |
| TC-AD-105 | Create AD with user groups | Create AD with administrators, backupOperators, securityOperators arrays populated | AD created with user groups properly configured |
| TC-AD-106 | ADD AD without encryptDCConnections field | 1. ADD new AD without encryptDCConnections (default). 2. Check response does not have this field. 3. GET AD has encryptDCConnections = false | Successful |
| TC-AD-107 | ADD AD with encryptDCConnections = true | 1. ADD new AD with encryptDCConnections = true. 2. Check response. 3. GET AD has encryptDCConnections = true | Successful |
| TC-AD-108 | ADD AD with encryptDCConnections = false | 1. ADD new AD with encryptDCConnections = false. 2. Check response. 3. GET AD has encryptDCConnections = false | Successful |
| TC-AD-109 | Add multiple AD credentials; List AD | Create multiple AD credentials in SDE with valid details (within permissible limit) | Successful |
| TC-NFS-201 | ADD AD without encryptDCConnections (alternate ID) | Same scenario as TC-AD-106 | Successful |
| TC-NFS-202 | ADD AD with encryptDCConnections = true (alternate ID) | Same scenario as TC-AD-107 | Successful |
| TC-NFS-203 | ADD AD with encryptDCConnections = false (alternate ID) | Same scenario as TC-AD-108 | Successful |

---

## A.2 Create AD – Negative (Validation)

| Test Case ID | Name | Expected Result |
|--------------|------|-----------------|
| TC-AD-111 | Create AD with invalid DNS format | 400 Bad Request |
| TC-AD-112 | Create AD with invalid netBIOS length | 400 Bad Request |
| TC-AD-113 | Create AD with invalid resourceId format | 400 Bad Request |
| TC-AD-114 | Create AD with duplicate resourceId | 409 Conflict |
| TC-AD-115 | Create AD with empty username | 400 Bad Request |
| TC-AD-116 | Create AD with empty password | 400 Bad Request |
| TC-AD-117 | Create AD with username exceeding 255 chars | 400 Bad Request |
| TC-AD-118 | Create AD with password exceeding 255 chars | 400 Bad Request |
| TC-AD-119 | Create AD with invalid user group format (e.g. @ or \ in administrators/backupOperators) | 400 Bad Request |
| TC-AD-120 | Create AD with duplicate user in groups | 400 Bad Request |
| — | Create AD with invalid domain format (pattern ^[A-Za-z0-9](?:[A-Za-z0-9_-]*[A-Za-z0-9])?(\.[A-Za-z0-9]...) ) | 400 Bad Request |
| — | Create AD with DNS IP that doesn't match IP pattern | 400 Bad Request |
| — | Create AD with netBIOS name longer than 10 characters | 400 Bad Request |
| — | Create AD with resourceId that doesn't match pattern ^[a-z]([a-z0-9-]{0,61}[a-z0-9])? | 400 Bad Request |
| — | Create AD with resourceId that already exists in same region | 409 Conflict |

---

## A.3 Delete AD

| Test Case ID | Name | Test Steps | Expected Result |
|--------------|------|------------|-----------------|
| TC-AD-201 | Delete AD | Delete AD credential when no active volume is using it | AD deleted successfully; List AD shows correct details |
| TC-AD-202 | Delete non-existent AD | Attempt to delete AD credential that doesn't exist | As per API spec (e.g. 404 or no-op) |
| TC-AD-203 | Delete AD with active volumes | Attempt to delete AD when active volumes are using it | 409 Conflict (AD is in use) |

---

## A.4 Get AD / List AD

| Test Case ID | Name | Test Steps | Expected Result |
|--------------|------|------------|-----------------|
| TC-AD-301 | Get AD credential by UUID | Create AD in SDE; Get AD by UUID | AD created; Get returns valid details; encryptADCommunication in GET |
| TC-AD-302 | Get multiple AD credentials by UUID | Create 2 ADs; Get first by UUID; Get second by UUID | Both Get calls return expected details |
| TC-AD-303 | Get after delete | Create 2 ADs; Delete first; Get first (404); Get second | First Get 404 Not Found; second Get returns details |
| TC-AD-304 | List AD credentials | Create 2 ADs; List AD | List shows valid details for both |
| TC-AD-305 | List after delete | Create 2 ADs; Delete first; List AD | List shows only second AD |
| TC-AD-306 | Get AD by non-existent UUID | Create AD; Get by non-existent UUID | 404 Not Found |
| TC-AD-307 | Get AD by invalid UUID | Create AD; Get by invalid UUID | 400 Bad Request |

---

## A.5 Update AD – Positive (Created & In Use)

| Test Case ID | Name | Test Steps | Expected Result |
|--------------|------|------------|-----------------|
| TC-AD-401 | Update AD Credential | Update AD in VSA with valid details | AD updated; List/Get show updated details |
| TC-AD-402 | Update AD for Online/Available volume | Update AD credentials; create/delete volumes with new AD | Update successful; vol operations work with new AD |
| TC-AD-403 | Update NetBIOS when AD is in use (SD volume) | Create pool, SMB volume; mount, IO; validate AD "In Use"; update NetBIOS; validate prefix; IO; new volume; resize; IO; delete new volume | NetBIOS updated when In Use; IO works |
| TC-AD-404 | Update NetBIOS when AD is in use (SDE volume) | Repeat TC-AD-403 with existing volume in SDE | All steps successful |
| TC-AD-405 | Update organizationalUnit when AD is in use | Create pool, SMB volume; mount, IO; validate In Use; update OU; validate OU; IO; new volume; resize; IO; delete new volume | OU updated when In Use; IO works |
| TC-AD-406 | Update OU when AD is in use (SDE) | Repeat OU update flow with existing volume in SDE | All tests successful |
| TC-AD-407 | Update NetBIOS/OU with supported params (In Use) | Update NetBIOS, OU along with other supported params for "In Use" AD | Update successful; volume IO works |
| TC-AD-408 | Update NetBIOS/OU with non-supported params (In Use) | Update NetBIOS, OU along with non-supported params for "In Use" AD | Update fails; existing volume IO continues to work |
| TC-AD-409 | Update faulty AD attached to Pool | Create faulty AD; create pool; SMB creation fails; update AD to valid config; create/mount volume; IO; resize; delete volume; repeat with OU and multi-level OU | Volume creation fails with faulty AD; after update, volume creation and IO succeed |
| TC-AD-410 | Update encryptADCommunication (AD in Created) | Update encryptADCommunication; GET; try true→false, false→true, true→true, false→false; repeat for SDE volume | AD updated; GET returns new value; all PUT cases yield expected value |
| TC-AD-411 | Update encryptADCommunication (AD In Use) | Update encryptADCommunication for AD in use; GET; verify volume IO, update, delete | AD updated; GET correct; volume IO/update/delete fine |
| TC-AD-412 | Update SMB volume (no impact on Encrypt AD) | Update SMB volume | No impact on Encrypt AD communication setting |
| TC-AD-413 | SMB volume on non-default OU and multi-level OU | Create SMB volume in non-default OU, mount, IO; create SMB volume in multi-level non-default OU, IO | Volumes created successfully in both cases |
| TC-AD-414 | encryptADCommunication with volume lifecycle | Create AD with encryptADCommunication=true; create SMB vol; update AD to encryptADCommunication=false; create second SMB vol; GET both volumes; try true→false, false→true, etc.; repeat for SDE | First vol created; AD updated; second vol created; both volumes have encryptADCommunication=false; PUT cases as expected |
| TC-AD-425 | Update AD with correct detail and create another volume | Create volume with valid AD; update AD with incorrect details; create another volume | 1st creation succeeds; 2nd fails |
| TC-AD-426 | Update AD with incorrect then correct detail | Create volume with AD having incorrect details (fails); update AD with correct details; create another SMB volume | 1st creation fails; 2nd succeeds |

---

## A.6 Update AD – Negative (Validation)

| Test Case ID | Name | Expected Result |
|--------------|------|-----------------|
| TC-AD-415 | Update AD with invalid DNS format | 400 Bad Request |
| TC-AD-416 | Update AD with invalid netBIOS length | 400 Bad Request |
| TC-AD-417 | Update AD with invalid resourceId format | 400 Bad Request |
| TC-AD-418 | Update AD with duplicate resourceId | 409 Conflict |
| TC-AD-419 | Update AD with empty username | 400 Bad Request |
| TC-AD-420 | Update AD with empty password | 400 Bad Request |
| TC-AD-421 | Update AD with username exceeding 255 chars | 400 Bad Request |
| TC-AD-422 | Update AD with password exceeding 255 chars | 400 Bad Request |
| TC-AD-423 | Update AD with invalid user group format | 400 Bad Request |
| TC-AD-424 | Update AD with duplicate user in groups | 400 Bad Request |
| — | Update AD with invalid domain format | 400 Bad Request |
| — | Update AD with resourceId that doesn't match pattern | 400 Bad Request |

---
