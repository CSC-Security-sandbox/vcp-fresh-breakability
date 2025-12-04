# Auto-Tiering Pause Resume Intermediary State Addition Decision

## Overview

- The `TieringStatus` field (formerly `TieringPaused`) has been refactored from a boolean flag to an enum type to better represent the state of auto-tiering pause/resume operations. 
- This change allows for more granular tracking of the pause/resume workflow, particularly when handling partial successes or failures during the multi-step process.
- This change has been communicated and discussed with **Vignesh** & **Sridhar** as well.

## Enum States

The `TieringStatus` enum supports four distinct states now:

### 1. **PAUSED**
The auto-tiering is fully paused. All operations required to pause auto-tiering have been successfully completed:
- Tiering policy has been toggled from all to none Hot tier bypass mode enabled volumes on ontap
- Aggregate-level tiering fullness threshold has been set to 100%

### 2. **RESUMED**
The auto-tiering is fully resumed. All operations required to resume auto-tiering have been successfully completed:
- Tiering policy has been toggled from none to all Hot tier bypass mode enabled volumes on ontap
- Aggregate-level tiering fullness threshold has been set to 0%

### 3. **PARTIALLY_PAUSED**
The pause operation is in progress or has partially succeeded. This state indicates that:
- Some but not all pause operations have completed successfully
- The system is in an intermediate state between RESUMED and PAUSED
- A retry is needed to complete the pause operation

### 4. **PARTIALLY_RESUMED**
The resume operation is in progress or has partially succeeded. This state indicates that:
- Some but not all resume operations have completed successfully
- The system is in an intermediate state between PAUSED and RESUMED
- A retry is needed to complete the resume operation

## Rationale for Intermediary States

The auto-tiering pause/resume workflow consists of two major steps that must be executed atomically:

### Step 1: Toggle HotTierBypassMode for Pool Volumes
- For **pause**: Change the tiering policy of such volumes from all to none
- For **resume**: Change the tiering policy of such volumes from none to all
- This operation can fail for individual volumes or for all volumes in the pool

### Step 2: Update Aggregate-Level Tiering Fullness Threshold
- For **pause**: Set the aggregate tiering fullness threshold to 100% (preventing data from tiering to cold storage)
- For **resume**: Set the aggregate tiering fullness threshold to 0%
- This operation can fail due to ONTAP communication issues or other errors

## Problem Addressed

Without intermediary states, the system faces several challenges:

1. **Partial Success Handling**: If Step 1 succeeds but Step 2 fails (or vice versa), the boolean flag cannot accurately represent this mixed state
2. **Retry Logic**: The system needs to know which operations succeeded and which need to be retried in the next attempt
3. **Observability**: Operators need visibility into whether the system is in an intermediate state requiring attention

## Benefits of the New Approach

1. **Accurate State Representation**: The enum provides a clear indication of the current state of the pause/resume operation
2. **Retry-Friendly**: The intermediary states (`PARTIALLY_PAUSED` and `PARTIALLY_RESUMED`) signal that a retry is needed
3. **Improved Monitoring**: Operators can identify pools stuck in intermediary states and take corrective action
