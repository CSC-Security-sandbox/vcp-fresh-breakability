-- Migration: Add TieringPaused to AutoTieringConfig in pool table, defaulting to false
UPDATE pools
SET auto_tiering_config =
    jsonb_set(
        auto_tiering_config,
        '{tiering_paused}',
        'false'::jsonb,
        true
    )
WHERE auto_tiering_config IS NOT NULL AND NOT (auto_tiering_config ? 'tiering_paused');
