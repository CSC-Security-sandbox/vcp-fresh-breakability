# KMS Key Rotation - Multi-Key Support Design

**Status**: ✅ **IMPLEMENTED** - This design has been implemented using multiple keys in ServiceAccount with a child workflow pattern.

**Last Updated**: 2026-01-09

## Problem Statement

The previous KMS key rotation implementation had the following limitations:

1. **All-or-Nothing Approach**: If any SVM failed during rotation, the entire operation failed and all SVMs remained on the old key
2. **No State Tracking**: No way to track which SVMs were using the new key vs old key during rotation
3. **No Failure Recovery**: If rotation failed mid-way, some SVMs might be on new key while others were on old key, with no way to resume
4. **No Concurrent Support**: Could not handle multiple service account keys simultaneously during transition period
5. **Database Update Only After All Success**: Database was only updated after ALL SVMs succeeded, making it impossible to track partial progress

## Problems Solved

✅ **Per-Pool Failure Isolation**: Individual pool failures no longer block other pools from completing rotation
✅ **State Tracking**: Each SVM's key usage is tracked via `Svm.CurrentKmsKeyID`
✅ **Idempotent Operations**: All activities are idempotent and can be safely retried
✅ **Partial Progress Support**: Rotation can complete partially - successful pools use new key, failed pools can retry later
✅ **Resume Capability**: Workflow can resume from any failure point using Temporal's built-in retry mechanisms
✅ **Multi-Key Coexistence**: Both old and new keys can coexist during rotation transition period

## Architecture Context

- **ServiceAccount** belongs to a **KmsConfig**
- A **ServiceAccount** can have **multiple keys** (GCP allows this)
- Each **SVM** has one **KmsConfig** attached (configured during SVM creation)
- Each **Pool** has one **SVM**
- During rotation, we migrate **SVMs** (not pools) to use the new key
- Multiple keys can coexist on the same service account during transition

## Implementation

### Data Model

#### ServiceAccount - Multi-Key Support

```go
type ServiceAccount struct {
    BaseModel
    // Existing fields...
    Name                           string                    `gorm:"column:name"`
    ServiceAccountEmail            string                    `gorm:"column:service_account_email"`
    ServiceAccountPasswordLocation  string                    `gorm:"column:service_account_password_location"` // Primary/current key
    ServiceAccountAttributes       *ServiceAccountAttributes `gorm:"column:service_account_attributes;type:jsonb"`
}

type ServiceAccountAttributes struct {
    // Existing fields...
    
    // Multi-key support for rotation
    Keys []ServiceAccountKey `json:"keys,omitempty"` // Array of keys (old + new during rotation)
}

type ServiceAccountKey struct {
    KeyID        string    `json:"key_id"`         // GCP key ID
    KeyData      string    `json:"key_data"`       // Encrypted key data
    IsPrimary    bool      `json:"is_primary"`     // Is this the primary/current key
    CreatedAt    time.Time `json:"created_at"`     // When this key was created
    IsActive     bool      `json:"is_active"`      // Is this key still active (not deleted)
    // Note: RotationID field was removed - rotation state is tracked via workflow state
}
```

#### Track Which Key Each SVM Uses

```go
type Svm struct {
    // Existing fields...
    CurrentKmsKeyID string `gorm:"column:current_kms_key_id"` // Which key ID this SVM is using
}
```

### Rotation Workflow

The rotation is implemented as a child workflow `RotateKmsKeyChildWorkflow` that orchestrates the following phases:

#### Phase 1: Validation
1. **Activity**: `ValidateKeyRotationRequiredActivity`
   - Checks if rotation is actually needed
   - Validates KMS config state (must be INUSE or READY)
   - Checks if rotation is already in progress (multiple active keys)
   - Returns current key ID and service account with keys

#### Phase 2: Create New Key
1. **Activity**: `CreateServiceAccountKeyActivity`
   - Creates new service account key in GCP (idempotent - checks if key already exists)
   - Encrypts new key with KMS crypto key
   - Returns new key ID and encrypted key data

#### Phase 3: Store New Key in Database
1. **Activity**: `StoreNewKeyInDBActivity`
   - Adds new key to `ServiceAccount.ServiceAccountAttributes.Keys` array
   - Marks new key as `IsPrimary: false`, `IsActive: true`
   - Ensures old key is also in keys array if not already present
   - Idempotent - checks if key already exists before storing

#### Phase 4: Batch Pools for Migration
1. **Activity**: `BatchPoolsForKeyRotationActivity`
   - Gets all pools using this KMS config (excludes ERROR and DELETING states)
   - Validates no pools are in CREATING state (rotation fails if any pool is creating)
   - Returns list of pools for key rotation

#### Phase 5: Per-Pool Migration
1. **Activity**: `MigratePoolToNewKeyActivity` (called for each pool)
   - Gets pool's SVM
   - Checks if SVM already using new key (idempotent check)
   - Updates ONTAP KMS config with new key
   - Validates key reachability
   - If reachability fails: reverts to old key
   - Updates `Svm.CurrentKmsKeyID` to new key ID
   - Returns success/failure in result (doesn't fail workflow on individual pool failure)

#### Phase 6: Complete Rotation (only if all pools succeeded)
1. **Activity**: `CompleteKeyRotationActivity`
   - Checks if new key is already primary (idempotent)
   - Removes old key from `ServiceAccountAttributes.Keys` array
   - Sets new key as primary via `SetPrimaryKeyForServiceAccount`
   - Updates `ServiceAccount.ServiceAccountPasswordLocation` to new key

#### Phase 7: Delete Old Key from GCP
1. **Activity**: `DeleteOldSAKeyFromGCPActivity`
   - Deletes old key from GCP service account
   - Non-fatal - if deletion fails, rotation is still considered complete
   - Idempotent - treats 404 (key already deleted) as success

**Note**: The monolithic `RotateServiceAccountKey` activity was removed and replaced with this child workflow pattern for better separation of concerns and idempotency.

### ServiceAccount Structure During Rotation

```json
{
  "service_account_password_location": "encrypted_old_key_data",
  "service_account_attributes": {
    "keys": [
      {
        "key_id": "old_key_123",
        "key_data": "encrypted_old_key",
        "is_primary": true,
        "is_active": true,
        "created_at": "2024-01-01T00:00:00Z"
      },
      {
        "key_id": "new_key_456",
        "key_data": "encrypted_new_key",
        "is_primary": false,
        "is_active": true,
        "created_at": "2024-01-15T00:00:00Z"
      }
    ]
  }
}
```

### After All Pools Migrated

```json
{
  "service_account_password_location": "encrypted_new_key_data", // Updated to new key
  "service_account_attributes": {
    "keys": [
      {
        "key_id": "new_key_456",
        "key_data": "encrypted_new_key",
        "is_primary": true,
        "is_active": true,
        "created_at": "2024-01-15T00:00:00Z"
      }
      // Old key removed or marked is_active: false
    ]
  }
}
```

### Cleanup Strategy

1. **During Rotation**: Both keys in `ServiceAccountAttributes.Keys`
2. **After Migration**: 
   - Update `ServiceAccountPasswordLocation` to new key
   - Remove old key from `Keys` array (or mark `is_active: false`)
   - Delete old key from GCP
   - Clear `Svm.CurrentKmsKeyID` (all use primary now)

## Example Flow

```
1. Rotation Request for KmsConfig
   ↓
2. Get ServiceAccount from KmsConfig
   ↓
3. Validate rotation is needed (Phase 1)
   ↓
4. Create New Key in GCP (Phase 2)
   ↓
5. Store new key in ServiceAccountAttributes.Keys (Phase 3)
   ↓
6. Get all pools using this KMS config (Phase 4)
   ↓
7. For Each Pool (Phase 5):
   ├─ Pool 1: Migrate SVM → Success ✓
   ├─ Pool 2: Migrate SVM → Success ✓
   ├─ Pool 3: Migrate SVM → Failure ✗ (continues with other pools)
   └─ Pool 4: Migrate SVM → Success ✓
   ↓
8. Check Status:
   - 3 Successful, 1 Failed
   - Rotation State: Partial (both keys remain active)
   - ServiceAccount.ServiceAccountPasswordLocation: Still old key
   ↓
9. Retry Pool 3 (via workflow retry):
   - Migrate SVM → Success ✓
   ↓
10. All Pools Migrated:
    - Complete rotation (Phase 6): Set new key as primary
    - Delete old key from GCP (Phase 7)
    - Update ServiceAccount.ServiceAccountPasswordLocation to new key
```

## Key Benefits

1. **Resilience**: Single pool failure doesn't block others
2. **Observability**: Clear state tracking for each pool via `Svm.CurrentKmsKeyID`
3. **Recoverability**: Can resume and retry failed operations via Temporal retry policies
4. **Flexibility**: Can handle partial migrations gracefully
5. **Idempotency**: All activities are idempotent and safe to retry
6. **Safety**: Old key remains available during transition for rollback if needed

## Design Decisions

### Why We Don't Revert Failed Pool Key-Rotations Back to Older Keys

When a pool's key rotation fails, we do **not** attempt to revert successfully migrated pools back to the older key. Here's why:

**Problem Scenario:**

Consider a situation where one pool's key-rotation has failed:
```
K0 (Pool0 - older key, failed rotation)
K1 (Pool1 - newer key, successful rotation)
```

If we tried to revert Pool1 back to the older K0, but the revert itself were to fail:
```
K0 (Pool0 - older key)
K1 (Pool1 - newer key, failed revert)
```

This leaves us with two keys in an inconsistent state. If we extend this scenario with multiple pools over multiple iterations, we would end up with an unmanageable number of keys and unpredictable states.

**Our Approach:** Instead of reverting, we leave successfully migrated pools on the new key and retry the failed pools in subsequent rotation attempts. This ensures forward progress and avoids cascading failures from revert operations.

### Why We Maintain At Most Two Keys

GCP has a hard limit of **10 keys per service account**. Our design ensures we never exceed two active keys at any time. Here's why this constraint is critical:

**Problem Scenario:**

Consider a situation where one pool's key-rotation has failed across multiple iterations:

**Iteration 1:**
```
K0 (Pool0 - old key, failed rotation)
K0 (Pool1 - old key, failed rotation)
K1 (Pool2 - new key, successful rotation)
```

**Iteration 2** (if we created a new key K2 instead of reusing K1):
```
K0 (Pool0 - old key, failed rotation again)
K2 (Pool1 - newer key, successful rotation)
K1 (Pool2 - older key from previous rotation, failed rotation to newer key)
```

This leads to **three keys** (K0, K1, K2). If such failures were to be repeated over a number of pools and iterations, we would quickly exhaust the GCP limit of 10 keys per service account.

**Our Approach:** 
- We only create a new key when there is exactly one key (the primary)
- If rotation is already in progress (two keys exist), we continue migrating pools to the existing new key
- We only delete the old key after ALL pools have successfully migrated
- This ensures we never exceed two active keys, staying well within GCP's limits

### Summary

| Decision | Rationale |
|----------|-----------|
| No revert on partial failure | Prevents cascading failures and key proliferation from failed reverts |
| Maximum two keys | Respects GCP's 10-key limit and simplifies state management |
| Forward-only migration | Failed pools retry with the same new key in subsequent iterations |
| Old key deletion only after complete success | Ensures rollback capability until all pools are migrated |

## Implementation Details

### Activities

All activities are idempotent and check actual state before performing work:

- `ValidateKeyRotationRequiredActivity`: Validates if rotation is needed
- `CreateServiceAccountKeyActivity`: Creates new key in GCP (checks if exists)
- `StoreNewKeyInDBActivity`: Stores key in database (checks if exists)
- `BatchPoolsForKeyRotationActivity`: Get pools for Key-rotation
- `MigratePoolToNewKeyActivity`: Migrates individual pool's SVM
- `CompleteKeyRotationActivity`: Completes rotation (checks if already done)
- `DeleteOldSAKeyFromGCPActivity`: Deletes old key from GCP (idempotent)

### Workflow Pattern

- **Parent Workflow**: `RotateKmsConfigWorkflow` - Handles job status and calls child workflow
- **Child Workflow**: `RotateKmsKeyChildWorkflow` - Orchestrates the actual rotation phases
- **Benefits**: Better separation of concerns, easier testing, clearer error handling

### Failure Handling

- **Individual Pool Failure**: Workflow continues with other pools, failed pool can retry
- **Partial Success**: Both keys remain active, rotation can be retried later
- **Complete Success**: Old key is deleted from GCP, new key becomes primary
- **Non-Fatal Errors**: Key deletion failure doesn't fail the workflow (rotation is complete)
