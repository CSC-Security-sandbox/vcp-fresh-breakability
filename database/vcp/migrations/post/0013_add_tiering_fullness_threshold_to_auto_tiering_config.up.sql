-- Migration: Add TieringFullnessThreshold to AutoTieringConfig in pool table, defaulting to 50
UPDATE pools
SET auto_tiering_config =
    jsonb_set(
        auto_tiering_config,
        '{tiering_fullness_threshold}',
        '50'::jsonb,
        true
    )
WHERE auto_tiering_config IS NOT NULL AND NOT (auto_tiering_config ? 'tiering_fullness_threshold');

