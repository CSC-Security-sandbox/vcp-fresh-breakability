# Test Plan: SMB Volumes for VSA

**Document Version:** 1.0  
**Status:** Draft  
**Component:** VSA Files – SMB volume Create, Get, Update, Delete; AD dependency; encryption; size and attribute validation  

## 1. Introduction

### 1.1 Overview
This document contains test cases for **SMB volume** lifecycle in VSA: Create (with mandatory/default arguments, size in GiB/TiB, encryption, invalid attributes, AD reachability), Get Volume by ID and List Volumes, Update (size with/without data, supported/unsupported sizes, AD/DNS down, supported/non-supported fields, enable encryption), and Delete (success, delete when AD/DNS down for last/non-last volume, delete single and last SMB volume of pool). It also covers usage (usedBytes) and ACLs on SMB volumes.

### 1.2 Scope
- **In Scope:** SMB volume Create, Get, List, Update, Delete; size validation (1 TiB–100 TiB, invalid/unsupported sizes); default and mandatory arguments; encryption enabled/disabled; invalid attributes (securityStyle, exportPolicy, protocolTypes, smbShareSettings, backupId, token, locationId, projectNumber); AD/DNS reachability impact on create/update/delete; usedBytes and ACLs.
- **Out of Scope:** Non-SMB protocols; AD/LDAP server behavior outside VSA API.

### 1.3 Related Requirements
- SMB volume CRUD and REST API (GET by volume_id, List)
- Size rules (e.g. 1 TiB–100 TiB); unsupported size update (e.g. negative, 0, out of range)
- AD/DNS dependency for create, update, and delete (last vs non-last volume)
- Supported vs non-supported update fields; encryption enable on update
- Invalid attribute validation on create

## 2. Test Requirements

### 2.1 Requirements to be Tested
- **Get:** Get volume by ID after create; List volumes (e.g. 2 created) returns expected details.
- **Update:** Update SMB volume size without data and with data; update to different valid sizes (e.g. 20, 60, 140, 330, 500, 800, 1000 GiB); update with unsupported size (e.g. -200, -20, 0, 1800) fails; update when AD/DNS is down (behavior as specified); update supported fields without Snapshots & Backup (success); update non-supported fields (unsuccessful); update SMB volume to enable encryption (success).
- **Create:** SMB volume creation with mount to Windows client, data operations, and ACLs on files/directories; create with only mandatory arguments (default creation token, no snapshot policy); create with valid size in GiB/TiB (valid, 1 TiB, 100 TiB) and invalid (invalid size, under 1 TiB, over 100 TiB); usedBytes after populating data; create with invalid attributes (securityStyle e.g. unix, invalid size, exportPolicy, protocolTypes, smbShareSettings, backupId, invalid token, locationId, projectNumber) fails; create when AD is not reachable fails; create with encryption enabled/disabled.
- **Delete:** Delete SMB volume (success); delete when AD/DNS is down (last volume and non-last volume – delete not allowed when AD/DNS down, or as specified); delete SMB volumes one at a time; delete last SMB volume (but not last volume) of pool; create new SMB volume after delete.

### 2.2 Requirements NOT in Scope
- SMB server internals; non-SMB volume types for the same pool.

## 3. Test Environment Requirements

### 3.1 Infrastructure
- VSA environment with pool (e.g. 1 TiB) and SMB volume support; Windows client for mount and data/ACL operations; AD/DNS for positive cases; ability to simulate AD/DNS down.

### 3.2 Test Data
- Valid SMB volume parameters (size, share name, AD, etc.); invalid attributes and unsupported sizes for negative cases.

### 3.3 Dependencies
- VSA volume and pool APIs; AD/DNS; Windows client for mount and ACLs.

## 4. Test Categories

### 4.1 Functional Testing
- Create SMB volume (default/mandatory, size, encryption); Get/List; Update (size, supported fields, encryption); Delete; usedBytes; mount and ACLs.

### 4.2 Negative Testing
- Invalid attributes on create; invalid/unsupported size; AD not reachable on create; update with unsupported size or non-supported fields; delete when AD/DNS down (as specified).

## 5. Risk Assessment

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| Size/attribute misuse | Medium | Medium | Validation tests; invalid-attribute and unsupported-size cases |
| AD/DNS down breaking operations | High | Low | Explicit tests for create/update/delete when AD/DNS down |
| Update of non-supported fields accepted | Medium | Low | Negative test for non-supported fields |

## 6. Test Execution Strategy

### 6.1 Test Phases
1. **Phase 1:** Create (positive and negative), Get, List.
2. **Phase 2:** Update (size, supported/non-supported fields, encryption); AD/DNS down scenarios.
3. **Phase 3:** Delete (success, AD/DNS down, last/non-last, last SMB of pool); create new volume after delete.

### 6.2 Automation Strategy
- REST API–driven Create/Get/List/Update/Delete with assertions on status and response; optional mount/ACL scripts.

### 6.3 Manual Testing Strategy
- Windows mount, data operations, and ACL verification where not automated.

## 7. Success Criteria
- Get/List return expected volume details; Create succeeds for valid inputs and fails for invalid attributes and when AD is not reachable.
- Update succeeds for supported size and supported fields and encryption enable; fails for unsupported size and non-supported fields; behavior when AD/DNS down as specified.
- Delete succeeds when allowed; delete when AD/DNS down behaves as specified; delete single/last SMB of pool and create new volume after as expected.

---

# Appendix A: Test Case Tables

Test cases cover Get, Update, Create, and Delete SMB volume scenarios including size validation, encryption, AD/DNS dependency, and invalid attributes.

---

## A.1 Get Volume Cases

| Test Case ID   | Test Case | Test Steps | Expected Result |
|----------------|-----------|------------|-----------------|
| TC-SMB-GET-01 | Get Volume By Id | 1. Create a SMB volume.<br>2. Call REST GET to fetch the volume by volume_id. | The created volume with expected details should be returned by GET. |
| TC-SMB-GET-02 | Get Volumes (List) | 1. Create 2 SMB volumes.<br>2. Call REST GET API to list all volumes. | The created volumes with expected details should be returned in the list. |

---

## A.2 Update SMB Volume Cases

| Test Case ID   | Test Case | Test Steps | Expected Result |
|----------------|-----------|------------|-----------------|
| TC-SMB-UPD-01 | Update SMB volume size without data | Create a volume with no data; update the volume size. | The volume update should be successful. |
| TC-SMB-UPD-02 | Update SMB volume size with data | Create a volume with data; update the volume size. | The volume update should be successful. |
| TC-SMB-UPD-03 | Update SMB volume with different sizes | 1. Create a SMB pool of 1 TiB.<br>2. Create a volume of 10 GiB.<br>3. Update volume to different sizes [20, 60, 140, 330, 500, 800, 1000 GiB].<br>4. Validate the volume updated with each size. | The volume updates should be successful. |
| TC-SMB-UPD-04 | Update SMB volume with unsupported size | 1. Create a SMB pool of 1 TiB.<br>2. Create a SMB volume of 10 GiB.<br>3. Update with unsupported sizes (e.g. -200, -20, 0, 1800 GiB). | The volume update should be unsuccessful. |
| TC-SMB-UPD-05 | Update SMB volume when AD/DNS is down | 1. Create an SMB volume.<br>2. Make AD/DNS down.<br>3. Modify the SMB volume when AD/DNS is down. | The volume update should be successful (or as per product behavior). |
| TC-SMB-UPD-06 | Update supported fields without Snapshots & Backup | Update supported fields when volume has no Snapshots & Backup. | The volume update should be successful. |
| TC-SMB-UPD-07 | Update non-supported fields without Snapshots & Backup | Update non-supported fields when volume has no Snapshots & Backup. | The volume update should be unsuccessful. |
| TC-SMB-UPD-08 | Update SMB volume to enable encryption | Update a SMB volume to enable encryption. | The volume update should be successful. |

---

## A.3 Create SMB Volume Cases

| Test Case ID   | Test Case | Test Steps | Expected Result |
|----------------|-----------|------------|-----------------|
| TC-SMB-CRT-01 | SMB Volume Creation (full flow) | 1. Create a SMB volume.<br>2. Mount the volume to a Windows client and perform data operations.<br>3. Add ACLs to files/directories. | SMB volume created successfully. List volume shows correct attributes. Mount and data operations succeed. ACLs set successfully. |
| TC-SMB-CRT-02 | SMB volume creation with default arguments | Create a SMB volume with only mandatory arguments (default creation token, no snapshot policy). | SMB volume creation should succeed. |
| TC-SMB-CRT-03 | Create SMB volume with size in GiB/TiB | Create SMB volume with: valid size in GiB/TiB; invalid size; size under 1 TiB; size over 100 TiB; size 1 TiB; size 100 TiB. | Volume creation succeeds for valid size (e.g. 1 TiB, 100 TiB, in-range). Fails for invalid size and out-of-range. |
| TC-SMB-CRT-04 | Usage of SMB volume (usedBytes) | Create a SMB volume and populate data in it. | usedBytes for SMB volume should show correct details. |
| TC-SMB-CRT-05 | Create SMB volume with invalid attributes | Create a SMB volume with invalid attributes: invalid securityStyle (e.g. unix), invalid size, exportPolicy, invalid protocolTypes, invalid smbShareSettings, invalid backupId, invalid authorization Token, invalid locationId, invalid projectNumber. | SMB volume creation should fail for invalid attributes. |
| TC-SMB-CRT-06 | Create SMB volume when AD is not reachable | Create a first SMB volume when the AD is not UP. | Volume creation should fail. |
| TC-SMB-CRT-07 | Create SMB volume with encryption enabled | Create a SMB volume with encryption enabled. | SMB volume should be created with encryption enabled. |
| TC-SMB-CRT-08 | Create SMB volume with encryption disabled | Create a SMB volume with encryption disabled. | SMB volume should be created with encryption disabled. |

---

## A.4 Delete SMB Volume Cases

| Test Case ID   | Test Case | Test Steps | Expected Result |
|----------------|-----------|------------|-----------------|
| TC-SMB-DEL-01 | Delete SMB volume | Delete SMB volume (when allowed). | SMB volume should be deleted successfully. |
| TC-SMB-DEL-02 | Delete when AD/DNS is down (last volume) | Delete the last SMB volume when AD is down; when DNS is down. | Delete should not be allowed when AD/DNS is down (or as per product behavior). |
| TC-SMB-DEL-03 | Delete when AD/DNS is down (non-last volume) | Delete a non-last SMB volume when AD is down; when DNS is down. | As per product behavior (e.g. delete not allowed when AD/DNS down). |
| TC-SMB-DEL-04 | Delete SMB volumes and create new | Perform delete of SMB volumes (single volume at a time). Delete last SMB volume (but not last volume) of the pool. Create a new SMB volume after. | Deletes succeed; new SMB volume can be created after. |

---
