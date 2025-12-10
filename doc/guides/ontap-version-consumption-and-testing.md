# ONTAP Version Consumption and Testing Strategy

## Overview

This document provides a comprehensive guide for consuming new ONTAP versions in the VSA Control Plane (VCP) and outlines the testing strategy to validate the integration. This process ensures that new ONTAP versions are properly integrated, tested, and validated before production deployment while maintaining backward compatibility with existing clusters.

## Table of Contents

1. [ONTAP Version Consumption Process](#ontap-version-consumption-process)
2. [Files to Update](#files-to-update)
3. [Critical Backward Compatibility Requirements](#critical-backward-compatibility-requirements)
4. [Testing Strategy](#testing-strategy)
5. [Validation Checklist](#validation-checklist)
6. [Common Issues and Troubleshooting](#common-issues-and-troubleshooting)

---

## ONTAP Version Consumption Process

### Overview

When integrating a new ONTAP version (e.g., upgrading from `9.17.1P1` to `9.17.1P2`), several components need to be updated to ensure consistency across the system:

1. **VSA Image Names**: The VSA container image names (default for new pools)
2. **Mediator Image Names**: The mediator container image names (default for new pools)
3. **VLM Worker Image Mappings**: **ADD** new version mapping while **KEEPING** existing ones
4. **ONTAP Version Details**: The default ONTAP version configuration
5. **Test Files**: Unit test assertions that reference specific versions

### Integration Details

Based on PR #2112, the following changes are required when consuming a new ONTAP version:

#### Example: Upgrading from 9.17.1P1 to 9.17.1P2

- **VSA Image**: `x-9-17-1p1-gcnv` → `x-9-17-1p2-gcnv` (new default)
- **Mediator Image**: `cvo-mediator-x-9-17-1p1` → `cvo-mediator-x-9-17-1p2d1` (new default)
- **VLM Worker Image Tag (P2)**: `R9.17.1x_7882520` (new entry)
- **VLM Worker Image Digest (P2)**: `sha256:0b0a7f4063ba4e848af6f5d43c120c830ab1bcadf4f13e11710a3f15f066bf27` (new entry)
- **ONTAP Version**: `9.17.1P1` → `9.17.1P2` (new default)

**Important**: The old VLM worker image mappings for `9.17.1P1` must be **retained** to support existing clusters.

---

## Files to Update

### 1. Configuration Files

#### `common/vsa_config/vlm-config.json`
Update the default VSA and mediator image names for new pool deployments:
```json
{
  "images": {
    "vsa_image_name": "x-9-18-1rc1",
    "mediator_image_name": "cvo-mediator-x-9-18-1rc1"
  }
}
```

#### `kubernetes/vcp-worker-chart/values.yaml`
Update the worker configuration with new default values:
```yaml
workerConfig:
  vsaImageName: "x-9-18-1rc1"
  vsaMediatorImageName: "cvo-mediator-x-9-18-1rc1"
  ontapVersionDetails: "9.18.1RC1"  # ⚠️ CRITICAL: This is often missed!
```

#### `kubernetes/vsa-control-plane/values.yaml`
Update the core configuration:
```yaml
config:
  ontapVersionDetails: "9.18.1RC1"  # ⚠️ CRITICAL: This is often missed!
```

#### `kubernetes/vsa-control-plane/charts/core/values.yaml`
Update the core chart configuration:
```yaml
config:
  ontapVersionDetails: "9.18.1RC1"  # ⚠️ CRITICAL: This is often missed!
```

### 2. Source Code Files

#### `core/orchestrator/workflows/pool_workflows.go`
Update default environment variable values for new pool deployments:
```go
vsaImageName      = env.GetString("VSA_IMAGE_NAME", "x-9-18-1rc1")
mediatorImage     = env.GetString("VSA_MEDIATOR_IMAGE_NAME", "cvo-mediator-x-9-18-1rc1")
```

#### `utils/env/env.go`
Update the default ONTAP version details:
```go
CurrentOntapVersionDetails = GetString("ONTAP_VERSION_DETAILS", "9.18.1RC1")
```

#### `clients/vlm/vlm_workflow_client.go`
Update the default ONTAP version:
```go
OntapVersion = env.GetString("ONTAP_VERSION_DETAILS", "9.18.1RC1")
```

### 3. Kubernetes Deployment Files

#### `kubernetes/vlm-worker/values.yaml` ⚠️ **CRITICAL: ADD, DON'T REPLACE**

**This is the most critical file for backward compatibility.** You must **ADD** the new version mapping while **KEEPING** all existing version mappings.

**Incorrect Approach** (DO NOT DO THIS):
```yaml
ontapVersionVlmImageMappings:
  # ❌ WRONG: Replacing old version breaks existing clusters
  - ontapVersion: "9.17.1P2"
    vlmImageName: "vlm-worker"
    vlmImageTag: "R9.17.1x_7882520"
    vlmImageDigest: "sha256:0b0a7f4063ba4e848af6f5d43c120c830ab1bcadf4f13e11710a3f15f066bf27"
    secondary: true
    pullPolicy: IfNotPresent
```

**Correct Approach** (DO THIS):
```yaml
# Ontap Version mapping with VLM. To support more ontap versions, similar block needs to be added below.
# We should support previous versions as well, for backward compatibility.
ontapVersionVlmImageMappings:
  # VLM worker to support ONTAP patch version 9.17.1P2 (NEW - ADD THIS)
  - ontapVersion: "9.17.1P2"
    vlmImageName: "vlm-worker"
    vlmImageTag: "R9.17.1x_7882520"
    vlmImageDigest: "sha256:0b0a7f4063ba4e848af6f5d43c120c830ab1bcadf4f13e11710a3f15f066bf27"
    secondary: true
    pullPolicy: IfNotPresent
  # VLM worker to support ONTAP patch version 9.17.1P1 (KEEP THIS - existing clusters need it)
  - ontapVersion: "9.17.1P1"
    vlmImageName: "vlm-worker"
    vlmImageTag: "R9.17.1x_7855269"
    vlmImageDigest: "sha256:07d042905f40dfc4c566919ebc539b617d9a1fc38773ab9a26e7749fd85c8405"
    secondary: true
    pullPolicy: IfNotPresent
  # VLM worker to support ONTAP test patch versions like 9.17.1X50 or any other test patch version
  - ontapVersion: "9.17.1"
    vlmImageName: "vlm-worker"
    vlmImageTag: "R9.17.1x_7882520"  # Update to latest tag for test versions
    vlmImageDigest: "sha256:0b0a7f4063ba4e848af6f5d43c120c830ab1bcadf4f13e11710a3f15f066bf27"
    secondary: true
    pullPolicy: IfNotPresent
```

**Why This Matters**:
- Existing clusters running on `9.17.1P1` need the VLM worker image `R9.17.1x_7855269` to continue functioning
- New clusters will use `9.17.1P2` and need the VLM worker image `R9.17.1x_7882520`
- Both versions must coexist in the mapping to support both old and new clusters simultaneously
- Removing the old version mapping will cause existing clusters to fail

#### `skaffold/k8s/vlm-worker.yaml`
Update the VLM worker image tag for local development:
```yaml
containers:
  - name: vlm-worker
    image: ghcr.io/vcp-vsa-control-plane/vcp-container-images-us/vlm-worker:R9.17.1x_7882520
```

**Note**: This is for local development only. Production deployments use the mappings from `values.yaml`.

#### `skaffold/k8s/core.yaml`
Update the environment variable for local development:
```yaml
env:
  - name: ONTAP_VERSION_DETAILS
    value: "9.18.1RC1"
```

### 4. Test Files

#### `core/orchestrator/workflows/pool_workflows_test.go`
Update test assertions that reference specific image names:
```go
assert.Equal(t, "x-9-17-1p2-gcnv", req.VLMConfig.Deployment.Images.VSAImageName, "...")
assert.Equal(t, "cvo-mediator-x-9-18-1rc1", req.VLMConfig.Deployment.Images.MediatorImageName, "...")
```

### 5. Documentation Files

#### `doc/workflows/core/pool-workflows.md`
Update documentation to reflect new default values:
```markdown
**Image Configuration**:
- **VSA Image**: `x-9-18-1rc1` (default)
- **Mediator Image**: `cvo-mediator-x-9-18-1rc1` (default)
```

---

## Critical Backward Compatibility Requirements

### Why Backward Compatibility Matters

When consuming a new ONTAP version, the system must support **both** old and new versions simultaneously:

1. **Existing Clusters**: Clusters created before the integration are running on the old ONTAP version (e.g., `9.17.1P1`) and must continue to function
2. **New Clusters**: Clusters created after the integration will use the new ONTAP version (e.g., `9.17.1P2`)
3. **VLM Worker Images**: Each ONTAP version requires a specific VLM worker image version to function correctly

### Key Requirements

#### 1. VLM Worker Image Mappings Must Be Additive

**Rule**: Always **ADD** new version mappings, never **REPLACE** existing ones.

- ✅ **Correct**: Add `9.17.1P2` mapping while keeping `9.17.1P1` mapping
- ❌ **Incorrect**: Replace `9.17.1P1` mapping with `9.17.1P2` mapping

#### 2. Default Values vs. Existing Resources

- **Default values** (in code/config) determine what version **new** pools will use
- **Existing pools** continue using their original ONTAP version until explicitly upgraded
- **VLM worker mappings** must support all active ONTAP versions in the system

#### 3. Version Support Lifecycle

- When a new version is added, both old and new versions must be supported
- Old versions can only be removed after all clusters using them have been upgraded or deleted
- The system should support multiple ONTAP versions simultaneously

### Impact of Missing Backward Compatibility

If you remove the old VLM worker image mapping:

1. **Existing clusters fail**: Clusters on old ONTAP version cannot find their required VLM worker image
2. **Operations fail**: Update, create volume, and other operations on old clusters will fail
3. **Data access issues**: Volumes in old clusters may become inaccessible
4. **Production outages**: Critical production clusters may be affected

---

## Testing Strategy

The testing strategy is designed to validate both backward compatibility with existing resources and forward compatibility with new resources created after the integration. This ensures a smooth transition when consuming a new ONTAP version.

### Prerequisites

- Access to a test environment with VCP deployed
- Ability to create and manage pools, clusters, and volumes
- Access to the upgrade API for cluster/pool upgrades
- Monitoring tools to observe system behavior
- Ability to verify VLM worker deployments for different ONTAP versions

### Test Phases

#### Phase 1: Pre-Integration Setup

**Objective**: Create baseline resources using the current ONTAP version to test backward compatibility.

**Steps**:

1. **Create Clusters/Pools**
   - Create 2-3 pools/clusters using the current ONTAP version (e.g., `9.17.1P1`)
   - Verify successful creation
   - Document pool/cluster IDs and their current ONTAP versions
   - Verify VLM worker pods are running with the correct image for this version

2. **Create Volumes**
   - Create multiple volumes across the different pools
   - Test various volume configurations (different sizes, protocols, etc.)
   - Verify volumes are accessible and functional
   - Document volume IDs and their associated pools

3. **Baseline Validation**
   - Verify all pools are in healthy state
   - Verify all volumes are accessible
   - Verify VLM worker pods for old version are running correctly
   - Capture current system state (pool status, volume status, VLM worker status, etc.)

**Expected Outcome**: All resources created successfully and are functional. VLM workers for old version are running.

---

#### Phase 2: Integration Deployment

**Objective**: Deploy the new ONTAP version integration without disrupting existing resources.

**Steps**:

1. **Deploy Integration Changes**
   - Apply all configuration changes listed in [Files to Update](#files-to-update)
   - **Critical**: Verify `ontapVersionVlmImageMappings` includes both old and new versions
   - Deploy updated VCP components
   - Verify deployment is successful
   - Monitor for any immediate errors or warnings

2. **System Health Check**
   - Verify VCP services are running
   - Verify VLM workers for **both** old and new versions are running
   - Verify existing pools/clusters are still accessible
   - Verify existing volumes are still accessible
   - Check logs for any integration-related errors
   - Verify no VLM worker pods were terminated unexpectedly

3. **VLM Worker Validation**
   - Verify VLM worker pods for old version (e.g., `9.17.1P1`) are still running
   - Verify VLM worker pods for new version (e.g., `9.17.1P2`) are available
   - Check that both versions have their respective image tags

**Expected Outcome**: Integration deployed successfully, existing resources remain accessible, both old and new VLM worker versions are running.

---

#### Phase 3: Post-Integration Validation - New Resources

**Objective**: Validate that new resources can be created with the new ONTAP version.

**Steps**:

1. **Create New Pool/Cluster**
   - Create a new pool/cluster after integration
   - Verify it uses the new ONTAP version (e.g., `9.17.1P2`)
   - Verify pool/cluster creation completes successfully
   - Verify pool/cluster is in healthy state
   - Verify VLM worker pods for new version are running

2. **Create New Volumes**
   - Create multiple volumes in the new pool/cluster
   - Test various volume configurations
   - Verify volumes are accessible and functional
   - Verify volumes use the new ONTAP version

3. **Validation**
   - Verify new pool/cluster shows correct ONTAP version in cluster details
   - Verify new volumes are functional
   - Compare behavior between old and new pools
   - Verify VLM workers for both versions are running simultaneously

**Expected Outcome**: New resources created successfully with new ONTAP version. Both old and new VLM worker versions are running.

---

#### Phase 4: Backward Compatibility Testing - Existing Resources

**Objective**: Validate that existing resources (created before integration) continue to function correctly with their original ONTAP version.

**Steps**:

1. **Verify VLM Worker for Old Version**
   - Verify VLM worker pods for old version are still running
   - Verify they are using the correct image tag for old version
   - Verify they can communicate with old clusters

2. **Update Pool Operations**
   - Perform pool update operations on pools created in Phase 1
   - Verify updates complete successfully
   - Verify pool state remains healthy
   - Verify pool still shows original ONTAP version (e.g., `9.17.1P1`)
   - Verify VLM worker for old version handled the operation

3. **Update Volume Operations**
   - Perform volume update operations on volumes created in Phase 1
   - Test various update scenarios (size changes, attribute changes, etc.)
   - Verify updates complete successfully
   - Verify volumes remain accessible
   - Verify VLM worker for old version handled the operation

4. **Create Volume in Old Pool**
   - Create new volumes in pools created in Phase 1 (old ONTAP version)
   - Verify volumes are created successfully
   - Verify volumes are accessible
   - Verify volumes inherit the pool's ONTAP version
   - Verify VLM worker for old version handled the operation

5. **Delete Pool**
   - Select one pool from Phase 1 for deletion
   - Perform pool deletion
   - Verify deletion completes successfully
   - Verify all associated resources are cleaned up properly
   - Verify VLM worker for old version handled the operation

**Expected Outcome**: All operations on existing resources complete successfully without errors. Old VLM worker continues to function correctly.

---

#### Phase 5: Upgrade Testing

**Objective**: Validate the upgrade process from old ONTAP version to new ONTAP version.

**Steps**:

1. **Upgrade Cluster/Pool**
   - Select a pool/cluster from Phase 1 (old ONTAP version)
   - Use the upgrade API to upgrade to the new ONTAP version
   - Monitor the upgrade process
   - Verify upgrade completes successfully

2. **Post-Upgrade Validation**
   - Verify pool/cluster shows new ONTAP version in cluster details
   - Verify pool/cluster is in healthy state
   - Verify all volumes in the upgraded pool are still accessible
   - Verify volumes continue to function correctly
   - Verify VLM worker for new version is handling operations on upgraded pool

3. **VLM Worker Transition**
   - Verify the upgraded pool now uses VLM worker for new version
   - Verify old VLM worker is no longer needed for this pool
   - Verify operations on upgraded pool use new VLM worker

**Expected Outcome**: Upgrade completes successfully, pool shows new ONTAP version, all resources remain functional, VLM worker transition is successful.

---

#### Phase 6: Post-Upgrade Operations Testing

**Objective**: Validate operations on upgraded resources.

**Steps**:

1. **Update Pool Operations**
   - Perform pool update operations on the upgraded pool
   - Verify updates complete successfully
   - Verify pool state remains healthy
   - Verify pool shows new ONTAP version
   - Verify VLM worker for new version handled the operation

2. **Update Volume Operations**
   - Perform volume update operations on volumes in the upgraded pool
   - Test various update scenarios
   - Verify updates complete successfully
   - Verify volumes remain accessible
   - Verify VLM worker for new version handled the operation

3. **Create Volume in Upgraded Pool**
   - Create new volumes in the upgraded pool
   - Verify volumes are created successfully
   - Verify volumes are accessible
   - Verify volumes use the new ONTAP version
   - Verify VLM worker for new version handled the operation

**Expected Outcome**: All operations on upgraded resources complete successfully. New VLM worker handles all operations correctly.

---

### Test Scenarios Summary

| Phase | Test Scenario | Resources | ONTAP Version | VLM Worker | Expected Result |
|-------|--------------|-----------|---------------|------------|-----------------|
| 1 | Create pools/clusters | New | Old (9.17.1P1) | Old | Success |
| 1 | Create volumes | New | Old (9.17.1P1) | Old | Success |
| 2 | Deploy integration | N/A | N/A | Both | Success |
| 2 | Verify VLM workers | Existing | Both | Both | Both running |
| 3 | Create pool/cluster | New | New (9.17.1P2) | New | Success |
| 3 | Create volumes | New | New (9.17.1P2) | New | Success |
| 4 | Update pool | Existing | Old (9.17.1P1) | Old | Success |
| 4 | Update volume | Existing | Old (9.17.1P1) | Old | Success |
| 4 | Create volume | New in old pool | Old (9.17.1P1) | Old | Success |
| 4 | Delete pool | Existing | Old (9.17.1P1) | Old | Success |
| 5 | Upgrade pool | Existing | Old → New | Old → New | Success |
| 6 | Update pool | Upgraded | New (9.17.1P2) | New | Success |
| 6 | Update volume | Upgraded | New (9.17.1P2) | New | Success |
| 6 | Create volume | New in upgraded pool | New (9.17.1P2) | New | Success |

---

## Validation Checklist

Use this checklist to ensure all aspects of the integration and testing are covered:

### Integration Checklist

- [ ] Updated `common/vsa_config/vlm-config.json` with new VSA and mediator image names
- [ ] Updated `core/orchestrator/workflows/pool_workflows.go` with new default image names
- [ ] Updated `utils/env/env.go` with new default ONTAP version
- [ ] Updated `clients/vlm/vlm_workflow_client.go` with new default ONTAP version
- [ ] Updated `kubernetes/vcp-worker-chart/values.yaml` with new image names and `ontapVersionDetails`
- [ ] Updated `kubernetes/vsa-control-plane/values.yaml` with new `ontapVersionDetails`
- [ ] Updated `kubernetes/vsa-control-plane/charts/core/values.yaml` with new `ontapVersionDetails`
- [ ] **CRITICAL**: Updated `kubernetes/vlm-worker/values.yaml` - **ADDED** new version mapping while **KEEPING** old version mapping
- [ ] Updated `skaffold/k8s/vlm-worker.yaml` with new VLM image tag
- [ ] Updated `skaffold/k8s/core.yaml` with new `ONTAP_VERSION_DETAILS`
- [ ] Updated test files (`pool_workflows_test.go`) with new image names
- [ ] Updated documentation (`pool-workflows.md`) with new default values
- [ ] **VERIFIED**: Both old and new VLM worker image mappings exist in `ontapVersionVlmImageMappings`
- [ ] Verified all changes are consistent across files
- [ ] Code review completed with focus on backward compatibility

### Testing Checklist

#### Phase 1: Pre-Integration
- [ ] Created 2-3 pools/clusters with old ONTAP version
- [ ] Created multiple volumes across different pools
- [ ] Verified all resources are functional
- [ ] Verified VLM worker pods for old version are running
- [ ] Documented resource IDs and states

#### Phase 2: Integration
- [ ] Deployed integration changes
- [ ] **CRITICAL**: Verified both old and new VLM worker mappings exist
- [ ] Verified VCP services are running
- [ ] Verified VLM workers for **both** old and new versions are running
- [ ] Verified existing resources are still accessible
- [ ] Verified existing volumes are still accessible
- [ ] Checked logs for errors
- [ ] Verified no VLM worker pods were terminated unexpectedly

#### Phase 3: New Resources
- [ ] Created new pool/cluster with new ONTAP version
- [ ] Verified new pool uses new ONTAP version
- [ ] Verified VLM worker for new version is handling new pool
- [ ] Created volumes in new pool
- [ ] Verified new volumes are functional

#### Phase 4: Backward Compatibility
- [ ] Verified VLM worker for old version is still running
- [ ] Updated pools created in Phase 1
- [ ] Updated volumes created in Phase 1
- [ ] Created new volumes in old pools
- [ ] Deleted one pool from Phase 1
- [ ] Verified all operations completed successfully
- [ ] Verified old VLM worker handled all operations correctly

#### Phase 5: Upgrade
- [ ] Upgraded pool/cluster from old to new ONTAP version
- [ ] Verified upgrade completed successfully
- [ ] Verified pool shows new ONTAP version
- [ ] Verified all volumes remain accessible
- [ ] Verified VLM worker transition from old to new

#### Phase 6: Post-Upgrade
- [ ] Updated upgraded pool
- [ ] Updated volumes in upgraded pool
- [ ] Created new volumes in upgraded pool
- [ ] Verified all operations completed successfully
- [ ] Verified new VLM worker handled all operations correctly

### Final Validation
- [ ] All test phases completed successfully
- [ ] Both old and new VLM workers are running simultaneously
- [ ] No critical errors in logs
- [ ] System performance is acceptable
- [ ] Backward compatibility verified
- [ ] Documentation updated
- [ ] Ready for production deployment

---

## Common Issues and Troubleshooting

### Issue: Missing `ontapVersionDetails` Update

**Symptom**: New pools created after integration still show old ONTAP version in cluster details.

**Solution**: Ensure `ontapVersionDetails` is updated in:
- `kubernetes/vcp-worker-chart/values.yaml`
- `kubernetes/vsa-control-plane/values.yaml`
- `kubernetes/vsa-control-plane/charts/core/values.yaml`
- `utils/env/env.go`

### Issue: Replaced Old VLM Worker Mapping Instead of Adding New One

**Symptom**: Existing clusters on old ONTAP version fail operations, VLM worker pods for old version are missing, operations on old clusters return errors.

**Solution**: 
- **IMMEDIATE**: Restore the old VLM worker image mapping in `kubernetes/vlm-worker/values.yaml`
- Verify both old and new mappings exist in `ontapVersionVlmImageMappings`
- Redeploy VLM worker to restore old version support
- Verify existing clusters can perform operations again

**Prevention**: Always **ADD** new version mappings, never **REPLACE** existing ones.

### Issue: VLM Worker Not Using New Image

**Symptom**: VLM worker pods are not using the new image tag for new clusters.

**Solution**: 
- Verify `ontapVersionVlmImageMappings` in `kubernetes/vlm-worker/values.yaml` includes the new version
- Verify the new version mapping is correctly formatted
- Check that the deployment has been restarted
- Verify new clusters are correctly mapped to new VLM worker version

### Issue: Both VLM Workers Not Running

**Symptom**: Only one VLM worker version is running, or VLM workers are missing.

**Solution**:
- Verify `ontapVersionVlmImageMappings` includes both old and new versions
- Check VLM worker deployment configuration
- Verify Kubernetes has resources to run multiple VLM worker deployments
- Check logs for deployment errors
- Verify both versions are marked as `secondary: true` if needed

### Issue: Upgrade Fails

**Symptom**: Cluster upgrade workflow fails during execution.

**Solution**: 
- Check upgrade job status via API
- Review workflow logs in Temporal
- Verify target build images are available
- Verify VLM worker for new version is running
- Check VLM worker logs for errors
- Verify image version exists in database

### Issue: Operations Fail on Old Clusters After Integration

**Symptom**: Update, create volume, or other operations fail on clusters created before integration.

**Solution**:
- Verify old VLM worker image mapping still exists
- Verify VLM worker pods for old version are running
- Check VLM worker logs for old version
- Verify cluster's ONTAP version matches available VLM worker mapping
- Check for any version mismatch errors in logs

---

## Best Practices

### 1. Always Add, Never Replace

When updating `ontapVersionVlmImageMappings`, always add the new version entry at the top of the list while keeping all existing entries. This ensures backward compatibility.

### 2. Verify Before Deployment

Before deploying integration changes:
- Review `ontapVersionVlmImageMappings` to ensure both old and new versions are present
- Verify all files listed in the checklist are updated
- Test in a non-production environment first

### 3. Monitor VLM Workers

After deployment:
- Monitor VLM worker pod status for both versions
- Check logs for any version-related errors
- Verify existing clusters can still perform operations
- Verify new clusters use the correct VLM worker version

### 4. Gradual Rollout

Consider a gradual rollout:
- Deploy integration to a subset of environments first
- Monitor for issues before full deployment
- Have a rollback plan ready

### 5. Documentation

Keep documentation updated:
- Document which ONTAP versions are supported
- Document when old versions can be safely removed
- Update runbooks with version-specific procedures

---

## References

- [VSA Cluster Upgrade Design](../architecture/vsa-cluster-upgrade-design.md)
- [VCP Worker Versioning Design](../architecture/designs/0011-vcp-worker_versioning-design.md)
- PR #2112: Integration example for 9.17.1P2 (this PR has couple of mistakes, refer to this doc for correct process)

---