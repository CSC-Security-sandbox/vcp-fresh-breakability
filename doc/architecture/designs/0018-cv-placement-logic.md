# Constituent Volume (CV) Placement Logic in ONTAP

## 1. Overview

This document describes the algorithm and design for distributing Constituent Volumes (CVs) across aggregates in ONTAP for large volume creation. The placement logic ensures optimal load balancing while respecting capacity and instance-type constraints.

### 1.1 Default Configuration
- **Default CVs per Aggregate**: 8
- **Default Total CVs for Large Pool**: 48 (6 aggregates × 8 CVs)
- When the user does not specify `LargeVolumeConstituentCount`, the system automatically sets it to the default of 48 CVs.

## 2. Problem Statement

When creating large volumes in ONTAP, the volume is split into multiple Constituent Volumes (CVs) that must be distributed across available aggregates. The placement must:
- Balance CVs evenly across aggregates
- Respect per-aggregate CV count limits (based on VM instance type)
- Respect per-aggregate space limits (for non-tiering pools)
- Ensure all aggregates are online before placement

## 3. Placement Algorithms

### 3.1 CV Limits Based Placement (`CalculateAggregatesForConstituentVolumesWithCVLimits`)
- Used when: `Pool.AllowAutoTiering == true`
- Constraint: Maximum CV count per aggregate
- Strategy: Greedy approach using **first and second minima** tracking
- Goal: Balance CV counts across aggregates by filling least-loaded aggregates first

### 3.2 Space Limits Based Placement (`CalculateAggregatesForConstituentVolumesWithSpaceLimits`)
- Used when: `Pool.AllowAutoTiering == false`
- Constraint: Available space per aggregate
- Strategy: Greedy approach using **first and second maxima** tracking
- Goal: Utilize aggregates with most available space first

## 4. Algorithm Design

### 4.1 Input Parameters
| Parameter | Description |
|-----------|-------------|
| `aggregates` | List of aggregates from ONTAP cluster |
| `largeVolumeConstituentCount` | Total number of CVs to place (defaults to 48 if not specified) |
| `totalNodes` | Total nodes in cluster (aggregates = nodes / 2) |
| `instanceType` | VM instance type (determines max CVs per aggregate) |
| `size` | Total volume size in bytes (for space-based placement) |
| `defaultConstituentsPerAggregate` | Default CVs per aggregate: 8 (env: `DEFAULT_CONSTITUENTS_PER_AGGREGATE`) |
| `numOfLvHAPairs` | Number of HA pairs for large capacity: 6 (env: `NUMBER_OF_HA_PAIRS_LARGE_CAPACITY`) |

### 4.2 Max Constituents Per Aggregate
Based on VM instance type:
| Instance Type | Max CVs per Aggregate |
|---------------|----------------------|
| `c3-standard-4-lssd` | 249 |
| `c3-standard-8-lssd` | 499 |
| `c3-standard-22-lssd`, `c3-standard-44-lssd`, `c3-standard-88-lssd` | 999 |
| Default | 999 |

### 4.3 Greedy Distribution Algorithm

```
1. Validate inputs:
   - Constituent count > 0
   - Aggregate count == totalNodes / 2
   - All aggregates must be online

2. Build eligible aggregates list:
   - Filter out aggregates at max capacity
   - Calculate available capacity per aggregate

3. Verify total capacity >= requested CVs

4. Distribution loop (while remaining CVs > 0):
   a. Find first minima (or maxima for space-based) aggregates
   b. Find second minima/maxima as target level
   c. Calculate CVs to place to reach target level
   d. Distribute CVs evenly across first minima aggregates
   e. Update remaining count

5. Calculate HCF (Highest Common Factor) of distribution
   - Used as multiplier for batch operations

6. Return flattened aggregate list and multiplier
```

### 4.4 Output Structure
```go
type AggregateDistributionResult struct {
    Aggregates     []string  // List of aggregate names (may repeat)
    AggrMultiplier int64     // HCF for batch operations
}
```

## 5. Integration with Volume Create Workflow

### 5.1 Workflow Integration Point
- Activity: `GetAggregatesFromOntap`
- Triggered when: `volume.LargeVolumeAttributes.LargeCapacity == true`
- Placement result passed to ONTAP volume creation API

### 5.2 Decision Flow
```
Volume Creation Request
        │
        ▼
Is Large Volume? ─── No ──→ Standard Volume Creation
        │
       Yes
        │
        ▼
Get Aggregates from ONTAP
        │
        ▼
Pool.AllowAutoTiering?
        │
   ┌────┴────┐
  Yes       No
   │         │
   ▼         ▼
CV Limits   Space Limits
Algorithm   Algorithm
   │         │
   └────┬────┘
        │
        ▼
Return Aggregate Distribution
        │
        ▼
Create Volume with CV Placement
```

## 6. Error Handling

| Error Code | Condition |
|------------|-----------|
| `ErrInvalidConstituentVolumeCount` | Constituent count <= 0 |
| `ErrOntapAggregateCountMismatch` | Aggregate count != expected (nodes/2) |
| `ErrOfflineAggregateError` | Any aggregate not in "online" state |
| `ErrNoAggregatesWithCapacity` | All aggregates at max capacity |
| `ErrInsufficientAggregateCapacity` | Total capacity < requested CVs |

## 7. Example Scenario

**Input:**
- 12 aggregates (24 nodes)
- 100 CVs to place
- Instance type: `c3-standard-22-lssd` (max 999 CVs/aggregate)
- Current CV counts: [10, 10, 15, 15, 20, 20, 25, 25, 30, 30, 35, 35]

**Algorithm Execution:**
1. First minima: aggregates with 10 CVs (2 aggregates)
2. Second minima: aggregates with 15 CVs
3. Place CVs to level up: (15-10) × 2 = 10 CVs distributed
4. Continue until 100 CVs placed

**Output:**
- Balanced distribution across aggregates
- HCF multiplier for efficient batch creation