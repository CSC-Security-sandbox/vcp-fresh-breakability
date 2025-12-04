-- Migration: Rename TieringPaused to TieringStatus and convert from boolean to enum in AutoTieringConfig in pool table
-- Step 1: Convert boolean values to enum strings
UPDATE pools
SET auto_tiering_config =
    jsonb_set(
        auto_tiering_config,
        '{tiering_paused}',
        CASE 
            WHEN (auto_tiering_config->>'tiering_paused')::boolean = true THEN '"PAUSED"'::jsonb
            ELSE '"RESUMED"'::jsonb
        END,
        true
    )
WHERE auto_tiering_config IS NOT NULL AND (auto_tiering_config ? 'tiering_paused');

-- Step 2: Rename field from tiering_paused to tiering_status
UPDATE pools
SET auto_tiering_config =
    auto_tiering_config - 'tiering_paused' || 
    jsonb_build_object('tiering_status', auto_tiering_config->'tiering_paused')
WHERE auto_tiering_config IS NOT NULL AND (auto_tiering_config ? 'tiering_paused');

