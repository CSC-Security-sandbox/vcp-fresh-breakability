# Constituent Volume (CV) Placement - Known Behaviors & Limitations

## 1. Overview

This document describes known behaviors, edge cases, and limitations in the CV placement logic for large volumes. Understanding these behaviors is critical for capacity planning and troubleshooting volume creation failures.

## 2. CV Limits: Two Different Constraints

There are **two separate CV limits** that apply during placement:

### 2.1 Per-Volume Per-Aggregate Limit
```
maxConstituentVolumesPerVolumePerAggregate = 200
```
- **Meaning**: A single large volume can have at most **200 CVs placed on any one aggregate**
- **Scope**: Per volume, per aggregate
- **Source**: Environment variable `MAX_CONSTITUENT_VOLUMES_PER_VOLUME_PER_AGGREGATE`

### 2.2 Total CVs Per Aggregate Limit (Instance Type Based)
| Instance Type | Max Total CVs per Aggregate |
|---------------|----------------------------|
| `c3-standard-4-lssd` | 249 |
| `c3-standard-8-lssd` | 499 |
| `c3-standard-22-lssd` | 999 |
| `c3-standard-44-lssd` | 999 |
| `c3-standard-88-lssd` | 999 |

- **Meaning**: The aggregate can hold this many CVs **total across all volumes**
- **Scope**: Per aggregate, across all volumes
- **Note**: One CV slot is reserved for `vol0` (root volume), so effective limit is `max - 1`

### 2.3 How Both Limits Interact

**Example**: On a `c3-standard-8-lssd` cluster with 6 aggregates:
- Max CVs per aggregate (total): 499
- Max CVs per volume per aggregate: 200
- A single volume can have at most: `6 aggregates × 200 CVs = 1200 CVs`
- But if other volumes exist, the 499 total limit may be hit first

```
Aggregate A:
├── Volume1: 150 CVs  ─┐
├── Volume2: 150 CVs   │── Total: 350 CVs (under 499 limit)
├── Volume3: 100 CVs  ─┘
│
└── New Volume4 requesting 200 CVs on this aggregate?
    → Fails: 350 + 200 = 550 > 499 (total aggregate limit exceeded)
```

## 3. Space-Based Placement Limitation (Non-Tiering Pools)

### 3.1 Behavior Description

When `Pool.AllowAutoTiering = false`, CV placement uses **space-based constraints**. Each CV must fit within an aggregate's available space.

### 3.2 Example Scenario: 12 TiB Pool, 12 TiB Volume, 4 CVs

**Setup**:
- Pool size: 12 TiB
- HA pairs: 6 (standard for large capacity pools)
- Aggregates: 6 (one per HA pair)
- Space per aggregate: `12 TiB ÷ 6 = 2 TiB`

**Volume Request**:
- Volume size: 12 TiB
- CV count: 4
- CV size: `12 TiB ÷ 4 = 3 TiB per CV`

**Result**: ❌ **FAILS**

**Reason**: Each CV requires 3 TiB, but each aggregate only has 2 TiB of space. No aggregate can accommodate even a single CV.

```
┌─────────────────────────────────────────────────────────────┐
│                    12 TiB Pool                               │
├──────────┬──────────┬──────────┬──────────┬──────────┬──────┤
│  Aggr1   │  Aggr2   │  Aggr3   │  Aggr4   │  Aggr5   │Aggr6 │
│  2 TiB   │  2 TiB   │  2 TiB   │  2 TiB   │  2 TiB   │2 TiB │
└──────────┴──────────┴──────────┴──────────┴──────────┴──────┘

CV Size Required: 3 TiB
Available per Aggregate: 2 TiB

❌ 3 TiB > 2 TiB → Cannot place any CV
```

### 3.3 Does This Apply When Auto-Tiering is Enabled?

**No.** When `Pool.AllowAutoTiering = true`:
- The system uses `CalculateAggregatesForConstituentVolumesWithCVLimits` (CV count-based)
- Space constraints are **NOT** checked during placement
- Only the **CV count limits** (per-volume and per-aggregate) apply
- Data can tier to cloud storage, so local aggregate space is not the constraint

**Summary**:
| Auto-Tiering | Placement Algorithm | Space Checked? |
|--------------|---------------------|----------------|
| `false` | `WithSpaceLimits` | ✅ Yes |
| `true` | `WithCVLimits` | ❌ No |

## 4. CV Count Distribution Limitation

### 4.1 Behavior Description

When CV count is **not evenly distributable** across aggregates, placement may fail or result in suboptimal distribution.

### 4.2 Example Scenario: 12 TiB Pool, 12 TiB Volume, 10 CVs

**Setup**:
- Pool size: 12 TiB
- Aggregates: 6
- Space per aggregate: 2 TiB

**Volume Request**:
- Volume size: 12 TiB
- CV count: 10
- CV size: `12 TiB ÷ 10 = 1.2 TiB per CV`

**Distribution Challenge**:
- 10 CVs across 6 aggregates = `10 ÷ 6 = 1.67 CVs per aggregate`
- Cannot distribute evenly

**What Happens** (with space-based placement):

The greedy algorithm attempts to place CVs on aggregates with most available space:

```
Ideal even distribution: Not possible (10 ÷ 6 = 1.67)

Actual distribution attempt:
┌─────────────────────────────────────────────────────────────┐
│  Aggr1   │  Aggr2   │  Aggr3   │  Aggr4   │  Aggr5   │Aggr6 │
│  2 TiB   │  2 TiB   │  2 TiB   │  2 TiB   │  2 TiB   │2 TiB │
│          │          │          │          │          │      │
│ CV: 1.2  │ CV: 1.2  │ CV: 1.2  │ CV: 1.2  │ CV: 1.2  │CV:1.2│
│ Rem: 0.8 │ Rem: 0.8 │ Rem: 0.8 │ Rem: 0.8 │ Rem: 0.8 │Rem:0.8│
└─────────────────────────────────────────────────────────────┘

After placing 6 CVs (one per aggregate):
- 4 CVs remaining
- Each aggregate has 0.8 TiB remaining
- Each CV needs 1.2 TiB

❌ Cannot place remaining 4 CVs - insufficient space in all aggregates
```

**Result**: ❌ **FAILS** with error `ErrInsufficientAggregateCapacity`

### 4.3 Working CV Counts for 6 Aggregates

For optimal placement with 6 aggregates and space-based constraints:

| CV Count | Distribution | CV Size (12 TiB vol) | Fits in 2 TiB aggr? |
|----------|--------------|----------------------|---------------------|
| 6 | 1 per aggregate | 2 TiB | ✅ Yes (exact fit) |
| 12 | 2 per aggregate | 1 TiB | ✅ Yes |
| 18 | 3 per aggregate | 0.67 TiB | ✅ Yes |
| 24 | 4 per aggregate | 0.5 TiB | ✅ Yes |
| 10 | Uneven | 1.2 TiB | ❌ No |
| 8 | Uneven | 1.5 TiB | ❌ No |
| 4 | N/A | 3 TiB | ❌ No (CV > aggr) |

### 4.4 General Rule for Space-Based Placement

For successful CV placement with space-based constraints:
1. `CV_size ≤ per_aggregate_space`
2. `CV_count` should ideally be a multiple of aggregate count for even distribution
3. If not a multiple, remaining CVs must still fit within available aggregate space after initial distribution

**Formula**:
```
CV_size = volume_size ÷ CV_count
per_aggregate_space = pool_size ÷ aggregate_count

Requirement: CV_size ≤ per_aggregate_space
```

## 5. Summary of Known Limitations

| Limitation | When It Applies | Workaround |
|------------|-----------------|------------|
| CV size > aggregate space | `AllowAutoTiering = false` | Increase CV count or enable auto-tiering |
| Uneven CV distribution | `AllowAutoTiering = false` | Use CV count that's multiple of aggregate count |
| Per-volume per-aggregate limit (200) | Always | Distribute across more aggregates |
| Total aggregate CV limit (249/499/999) | Always | Use larger instance type or fewer CVs |
| Prime CV counts ≥ 7 blocked | Always | Use non-prime CV count |

## 6. Recommendations

1. **For non-tiering pools**: Choose CV counts that are multiples of the aggregate count (typically 6)
2. **For tiering pools**: CV count flexibility is higher since space constraints don't apply
3. **Check capacity first**: Ensure `volume_size ÷ CV_count ≤ pool_size ÷ aggregate_count`
4. **Avoid prime numbers**: CV counts that are prime (≥ 7) are rejected to ensure even distribution
5. **Monitor aggregate utilization**: Total CV count across all volumes per aggregate has limits based on instance type

