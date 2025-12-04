-- Migration: Add CloudWriteModeEnabled to AutoTieringPolicy in volumes table
-- Set to true for volumes with tiering_policy = "all", false for all others

-- First, add the field with false as default for all existing records
UPDATE volumes
SET auto_tiering_policy =
    jsonb_set(
        auto_tiering_policy,
        '{cloud_write_mode_enabled}',
        'false'::jsonb,
        true
    )
WHERE auto_tiering_policy IS NOT NULL 
  AND NOT (auto_tiering_policy ? 'cloud_write_mode_enabled');

-- Then, update to true for volumes with tiering_policy = "all"
UPDATE volumes
SET auto_tiering_policy =
    jsonb_set(
        auto_tiering_policy,
        '{cloud_write_mode_enabled}',
        'true'::jsonb,
        true
    )
WHERE auto_tiering_policy IS NOT NULL 
  AND auto_tiering_policy->>'tiering_policy' = 'all';

