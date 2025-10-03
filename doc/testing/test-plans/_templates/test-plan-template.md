# Test Plan Template

**Document Version:** 1.0  
**Last Updated:** [DATE]  
**Status:** [Draft|In Review|Approved|Work In Progress]  
**JIRA Tracker:** [JIRA-LINK]  
**Component:** [Component Name]  
**Data Protection Classification:** [Internal|Confidential|Critical|Public]

## 1. Introduction

### 1.1 Overview
[Brief description of what this test plan covers]

### 1.2 Scope
[What is included and excluded from this test plan]

### 1.3 Related Requirements
- [Requirement 1 with link]
- [Requirement 2 with link]

## 2. Test Requirements

### 2.1 Requirements to be Tested
[List of specific requirements that will be tested]

### 2.2 Requirements NOT in Scope
[List of requirements that will not be tested and why]

## 3. Test Environment Requirements

### 3.1 Infrastructure
[Required infrastructure, environments, and configurations]

### 3.2 Test Data
[Required test data, user accounts, and configurations]

### 3.3 Dependencies
[External dependencies, services, and prerequisites]

## 4. Test Categories

### 4.1 Functional Testing
- [Link to functional test cases]

### 4.2 Integration Testing
- [Link to integration test cases]

### 4.3 Performance Testing
- [Link to performance test cases]

### 4.4 Security Testing
- [Link to security test cases]

### 4.5 Negative Testing
- [Link to negative test cases]

## 5. Risk Assessment

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| [Risk 1] | [High/Medium/Low] | [High/Medium/Low] | [Mitigation strategy] |

## 6. Test Execution Strategy

### 6.1 Test Phases
1. **Phase 1**: [Description]
2. **Phase 2**: [Description]
3. **Phase 3**: [Description]

### 6.2 Automation Strategy
[Description of what will be automated and automation approach]

### 6.3 Manual Testing Strategy
[Description of manual testing approach]

## 7. Success Criteria
- [Criterion 1]
- [Criterion 2]
- [Criterion 3]

## 8. Test Schedule
[Timeline and milestones]

## 9. Test Team
- **Test Lead:** [Name]
- **Test Engineers:** [Names]
- **Automation Engineers:** [Names]
- **Subject Matter Experts:** [Names]

## 10. Deliverables
- [Test cases document]
- [Test execution reports]
- [Defect reports]
- [Test automation scripts]

---

## Appendices

### Appendix A: Test Case Summary
[Summary table of all test cases with IDs, priorities, and automation status]

#### Appendix A.1: Test Case Authoring Format (BDD Style)
Use Given / When / Then for consistency with BDD. Each test case SHOULD include:
- ID: TC-<AREA>-<NNN>
- Title: Short action/result description
- Tags: [P0|P1|P2], [Functional|Negative|Performance|Security|Integration], [Automated|Manual]
- Scenario (Gherkin): Given, When, Then (and optional And / But lines)
- Data / Preconditions: Non-trivial setup steps not expressible succinctly in Given lines
- Validation Steps: Mapped to Then clauses (1:1 where possible)
- Cleanup: Idempotent teardown actions (if required)

Sample:
```
Scenario: Create pool with mandatory fields only
  Given an authenticated user with pool create permissions
    And a project with available quota
    And no existing pool named "pool-basic-001" in region "us-central1"
  When the user submits a POST /v1/pools request with mandatory fields only
  Then the API responds 201 Created with a valid poolId
    And the pool provisioningStatus becomes READY within 120s
    And the pool attributes match the request payload
    And an audit event "POOL_CREATE" is emitted
```
Mapping Table (optional):
| Then # | Validation | Method | Automation |
|--------|-----------|--------|------------|
| 1 | HTTP 201 & schema | API schema validation | Yes |
| 2 | Status READY <120s | Poll describe endpoint | Yes |
| 3 | Field equality | JSON diff vs payload | Yes |
| 4 | Audit event present | Query logs | Yes |

Negative Example Skeleton:
```
Scenario: Reject pool creation with size below minimum
  Given an authenticated user with pool create permissions
  When the user creates a pool with sizeGb=10 (below minimum 100)
  Then the API responds 400 Bad Request with errorCode "POOL_SIZE_TOO_SMALL"
    And no pool resource is created
```

Guidelines:
- Keep GIVEN focused on state, not actions.
- Single primary WHEN per scenario.
- THEN lines are observable outcomes (state, side-effects, external integrations).
- Prefer deterministic time bounds (e.g., "within 120s").
- For long-running transitions, add an Acceptance Window (e.g., VMRS transition ≤ 45m).
- Use shared step snippets (automation harness) for recurring validations.

### Appendix B: Test Data Specifications
[Links to test data requirements and sample data]

### Appendix C: Environment Setup
[Detailed environment setup instructions]

## Related Documents
- [Link to related test plans]
- [Link to requirements documents]
- [Link to design documents]
