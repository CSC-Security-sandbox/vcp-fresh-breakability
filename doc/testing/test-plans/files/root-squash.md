# Test Plan: Root Squash & Export Policy (NFS)

**Document Version:** 1.0  
**Last Updated:** February 16, 2026  
**Status:** Draft  
**JIRA Tracker:** [TBD]  
**Component:** VSA Files – Export Policy (hasRootAccess, allSquash, anonUid, allowedClients, accessType)  
**Data Protection Classification:** Internal

## 1. Introduction

### 1.1 Overview
This document contains test cases for NFS export policy behavior related to **root squash** and related settings per the GCP API schema (SimpleExportPolicyRule_v1beta): `hasRootAccess`, `allSquash`, and `anonUid`. It covers Create and Update validation, API error handling, and expected Get Volume response mapping.

### 1.2 Scope
- **In Scope:** Create volume/export-policy validation (invalid field combinations), Update volume export-policy (hasRootAccess, allSquash, anonUid), Get Volume response fields (hasRootAccess, allSquash, anonUid, allowedClients, accessType).
- **Out of Scope:** Protocol-level NFS behavior (e.g., actual UID mapping on the wire), non-export-policy volume attributes.

### 1.3 Related Requirements
- Export policy rule specification (squash mode, anonymous UID, root access)
- API validation and error messages for invalid combinations

## 2. Test Requirements

### 2.1 Requirements to be Tested
- Rejection of invalid Create requests (e.g., `anonUid` when `allSquash` is false; `hasRootAccess` when `allSquash` is true).
- Correct mapping of Update request fields to Get Volume response (`hasRootAccess`, `allSquash`, `anonUid`).
- Rejection of invalid Update requests (e.g., `hasRootAccess` when `allSquash` is true).

### 2.2 Requirements NOT in Scope
- NFS client/server behavior and UID squashing on the wire; other export policy attributes not listed above.

## 3. Test Environment Requirements

### 3.1 Infrastructure
- VSA Control Plane with Files/export-policy support; ability to Create/Update volume and Get Volume.

### 3.2 Test Data
- Valid pool and volume configuration; export policy with various squash and root-access combinations.

### 3.3 Dependencies
- VSA Files API (Create Volume, Update Volume, Get Volume); export policy schema.

## 4. Test Categories

### 4.1 Functional Testing
- Create/Update with valid hasRootAccess, allSquash, anonUid; Get Volume response correctness.

### 4.2 Negative Testing
- Invalid Create/Update payloads and expected 400 error messages.

## 5. Risk Assessment

| Risk                    | Impact | Probability | Mitigation                    |
|-------------------------|--------|-------------|-------------------------------|
| Misconfigured export    | Medium | Low         | Validation rules, test coverage |
| Inconsistent API response | Medium | Low       | Schema checks, response tests  |

## 6. Test Execution Strategy

### 6.1 Test Phases
1. **Phase 1:** Create validation (negative cases).
2. **Phase 2:** Update validation and Get Volume response mapping.

### 6.2 Automation Strategy
- API-driven Create/Update/Get with assertion on status codes and response body.

### 6.3 Manual Testing Strategy
- Optional manual verification of NFS mount behavior where needed.

## 7. Success Criteria
- All invalid Create/Update requests return 400 with the specified error messages.
- All valid Update requests result in Get Volume response matching the expected table.

## 8. Test Schedule
- [TBD]

## 9. Test Team
- **Test Lead:** [TBD]
- **Test Engineers:** [TBD]

## 10. Deliverables
- This test plan, execution report, and automation artifacts.

---

# Appendix A: Test Case Tables

Test cases are derived from tabular test plans for Create (validation) and Update (expected Get Volume response). Test Case IDs (TC-55xxx) align with automation where applicable.

---

## A.1 Create Request – Validation (Negative)

These cases verify that invalid export-policy combinations on **Create** are rejected with **400** and the correct error message.

| Test Case ID | Create Request (payload) | Got (Get Volume response) / Expected | Verdict |
|--------------|--------------------------|--------------------------------------|---------|
| TC-55106 | `hasRootAccess: "false"`, `anonUid: 65535` (anonUid without allSquash) | `{ error: { code: 400, message: "Invalid export policy rule specified, anonUid is not supported when allSquash is false or unspecified.", status: INVALID_ARGUMENT } }` | Pass |
| TC-55107 | `allSquash: true`, `anonUid: 65535`, `hasRootAccess: "true"` | `{ error: { code: 400, message: "Invalid export policy rule specified, hasRootAccess must not be specified when allSquash is true.", status: INVALID_ARGUMENT } }` | Pass |

**Rules under test:**
- `anonUid` must only be used when `allSquash` is true (otherwise rejected).
- `hasRootAccess` must not be specified when `allSquash` is true.

---

## A.2 Update Request – Expected Get Volume Response

These cases verify that **Update** request fields map correctly to the **Get Volume** response (`hasRootAccess`, `allSquash`, `anonUid` per SimpleExportPolicyRule_v1beta).

| Test Case ID | Update Request (payload) | Expected (Get Volume response) | Verdict |
|--------------|---------------------------|---------------------------------|---------|
| TC-55084 | `hasRootAccess: "false"` | `hasRootAccess: "false"` (root squash) | Pass |
| TC-55085 | `hasRootAccess: "true"` | `hasRootAccess: "true"` (no root squash) | Pass |
| TC-55086 | `hasRootAccess: "false"` | `hasRootAccess: "false"` | Pass |
| TC-55087 | `allSquash: true`, `anonUid: 65535` | `hasRootAccess: "false"`, `allSquash: true`, `anonUid: 65535` | Pass |
| TC-55108 | `allSquash: true`, `anonUid: 65535`, `hasRootAccess: "true"` (negative) | `{ error: 400, "hasRootAccess must not be specified when allSquash is true" }` | Pass |
| TC-55089 | `hasRootAccess: "true"` | `hasRootAccess: "true"` | Pass |
| TC-55090 | `hasRootAccess: "true"` | `hasRootAccess: "true"` | Pass |
| TC-55092 | `hasRootAccess: "false"` | `hasRootAccess: "false"` | Pass |
| TC-55093 | `allSquash: true`, `anonUid: 65535` | `hasRootAccess: "false"`, `allSquash: true`, `anonUid: 65535` | Pass |
| TC-55095 | `hasRootAccess: "false"` | `hasRootAccess: "false"` | Pass |
| TC-55097 | `hasRootAccess: "false"` | `hasRootAccess: "false"` | Pass |
| TC-55099 | `allSquash: true`, `anonUid: 65535` | `hasRootAccess: "false"`, `allSquash: true`, `anonUid: 65535` | Pass |
| TC-55100 | `hasRootAccess: "true"` | `hasRootAccess: "true"` | Pass |
| TC-55101 | `hasRootAccess: "true"` | `hasRootAccess: "true"` | Pass |
| TC-55102 | `allSquash: true`, `anonUid: 65535` | `hasRootAccess: "false"`, `allSquash: true`, `anonUid: 65535` | Pass |
| TC-55103 | `hasRootAccess: "true"` | `hasRootAccess: "true"` | Pass |
| TC-55104 | `hasRootAccess: "true"` | `hasRootAccess: "true"` | Pass |
| TC-55105 | `hasRootAccess: "false"` | `hasRootAccess: "false"` | Pass |

**Mapping rules under test:**
- `hasRootAccess: "true"` → Get Volume response `hasRootAccess: "true"` (no root squash)
- `hasRootAccess: "false"` → Get Volume response `hasRootAccess: "false"` (root squash)
- `allSquash: true` with `anonUid` → `hasRootAccess: "false"`, `allSquash: true`, `anonUid` set accordingly (when allSquash is true, hasRootAccess must be false)
- Update with both `allSquash: true` and `hasRootAccess: "true"` → 400 (invalid)

---

## Appendix B: BDD-Style Scenario Sketches (Optional)

### Create – Reject anonUid when allSquash is false (TC-55106)
```gherkin
Scenario: Reject Create when anonUid is specified without allSquash
  Given an authenticated user with volume create permissions
    And a valid pool and export policy configuration
  When the user creates a volume with hasRootAccess="false" and anonUid=65535 (allSquash false or omitted)
  Then the API responds 400 Bad Request
    And the error message indicates anonUid is not supported when allSquash is false or unspecified
```

### Create – Reject hasRootAccess when allSquash is true (TC-55107)
```gherkin
Scenario: Reject Create when hasRootAccess is specified with allSquash true
  Given an authenticated user with volume create permissions
  When the user creates a volume with allSquash=true, anonUid=65535, and hasRootAccess="true"
  Then the API responds 400 Bad Request
    And the error message indicates hasRootAccess must not be specified when allSquash is true
```

### Update – Reject hasRootAccess with allSquash true (TC-55108)
```gherkin
Scenario: Reject Update when hasRootAccess is specified with allSquash true
  Given an existing volume with export policy
  When the user updates the volume with allSquash=true, anonUid=65535, and hasRootAccess="true"
  Then the API responds 400 Bad Request
    And the error message indicates hasRootAccess must not be specified when allSquash is true
```

### Update – Verify Get Volume reflects squash and root access (e.g. TC-55087)
```gherkin
Scenario: Update with allSquash and anonUid returns correct Get Volume response
  Given an existing volume
  When the user updates the volume with allSquash=true and anonUid=65535
  And the user gets the volume
  Then the response has hasRootAccess="false", allSquash=true, anonUid=65535
```

---
