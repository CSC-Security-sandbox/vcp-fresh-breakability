# Auto-Tiering Pool Auto-Resize with Hot Tier Bypass Enabled Volumes Decision

## Overview

- Previously, pools containing any volume with `HotTierBypassModeEnabled` were excluded from hot tier auto-resize functionality.
- This decision documents the approach to enable hot tier auto-resize for such pools by excluding bypass-enabled volumes from the hot tier consumption calculation.
- **Feature Flag**: `ALLOW_AUTOGROW_FOR_HTBYPASS_VOLUME_CONTAINING_POOL` (default: `false`)
  - When `false`: Original behavior — pools with bypass-enabled volumes are skipped from auto-resize.
  - When `true`: New behavior — bypass-enabled volumes are excluded from hot tier consumption for auto-resize decisions.

## Problem

Volumes with `HotTierBypassModeEnabled=true` write data directly to the hot tier first, and ONTAP asynchronously moves this data to the cold tier. This causes temporary spikes in hot tier usage that can last a few minutes.

Previously, to avoid unnecessary auto-resize triggers from these temporary spikes, pools with any bypass-enabled volume were completely excluded from auto-resize consideration.

**Limitation**: This meant pools with even a single bypass-enabled volume could never benefit from hot tier auto-resize, regardless of actual sustained hot tier pressure from non-bypass volumes.

## Solution

Instead of excluding the entire pool, exclude only the hot tier consumption of bypass-enabled volumes from the calculation used for auto-resize decisions.

### Implementation

1. When calculating hot tier consumption for a pool, we compute two values:
   - **Total hot tier consumption** (all volumes) — saved to the pool in the database.
   - **Adjusted hot tier consumption** (excluding bypass-enabled volumes) — used only for auto-resize decisions.

2. The auto-resize decision logic uses the adjusted value, while all DB storage (pool and volume tiering fields) continues to reflect the actual total values.

## Benefits

1. **Pools with bypass volumes can now auto-resize**: Hot tier auto-grow is no longer blocked for pools just because they have bypass-enabled volumes.
2. **Accurate decision making**: Auto-resize decisions are based on stable hot tier usage, excluding temporary spikes from bypass volumes.
3. **No change to existing behavior for non-bypass pools**: Pools without bypass volumes continue to work exactly as before.
4. **DB values remain unchanged**: Pool and volume hot tier consumption stored in the database still reflects the actual total values.

## Trade-offs

- Bypass-enabled volumes do retain a small amount of stable hot tier consumption (typically 10-20 GiB) even after data moves to cold tier. This stable portion is excluded from auto-resize calculations, which means auto-resize may trigger slightly later than if we could perfectly distinguish between stable and transient hot tier usage. This is an acceptable trade-off to avoid false positives from temporary spikes.

## Example Scenarios

### Scenario 1: Bypass volume spike prevented from triggering auto-resize

**Setup:**
- Pool: 2 TiB, hot tier: 1 TiB (1024 GiB)
- Vol1 (bypass): actively writing → temporarily 200 GiB hot (spike), 800 GiB cold
- Vol2 (normal): 850 GiB hot

| Volume | Hot Tier | Included in Auto-Resize? |
|--------|----------|--------------------------|
| Vol1 (bypass) | 200 GiB (temporary) | ❌ No |
| Vol2 (normal) | 850 GiB | ✅ Yes |

**Result:** Usage = 850/1024 = 83% → auto-resize does NOT trigger.

The 200 GiB spike from bypass volume is temporary (ONTAP moves it to cold tier in minutes). Excluding it prevents unnecessary hot tier growth.

---

### Scenario 2: Auto-resize triggers correctly when stable usage hits threshold

**Setup:**
- Pool: 2 TiB, hot tier: 1 TiB (1024 GiB)
- Vol1 (bypass): stable → 10 GiB hot, 1014 GiB cold
- Vol2 (normal): 1024 GiB hot (full)

| Volume | Hot Tier | Included in Auto-Resize? |
|--------|----------|--------------------------|
| Vol1 (bypass) | 10 GiB | ❌ No |
| Vol2 (normal) | 1024 GiB | ✅ Yes |

**Result:** Usage = 1024/1024 = 100% → auto-resize triggers immediately.

No delay — the non-bypass volume alone fills the hot tier.

---

### Scenario 3: Small delay due to excluded stable bypass consumption

**Setup:**
- Pool: 2 TiB, hot tier: 1 TiB (1024 GiB)
- Vol1 (bypass): stable → 10 GiB hot, 1014 GiB cold
- Vol2 (normal): 1020 GiB hot

| Volume | Hot Tier | Included in Auto-Resize? |
|--------|----------|--------------------------|
| Vol1 (bypass) | 10 GiB | ❌ No |
| Vol2 (normal) | 1020 GiB | ✅ Yes |

**Result:**
- With exclusion: 1020/1024 = 99.6% → NOT triggered yet
- Actual total: 1030/1024 = 100.6% → would have triggered

Auto-resize triggers ~4 GiB later than "perfect", but this small delay (~0.4% of hot tier) prevents false triggers from temporary spikes during active writes.
