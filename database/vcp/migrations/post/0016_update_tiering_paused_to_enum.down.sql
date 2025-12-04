-- Migration: Revert TieringStatus to TieringPaused and convert from enum to boolean in AutoTieringConfig in pool table
-- Step 1: Rename field from tiering_status back to tiering_paused
UPDATE pools
SET auto_tiering_config =
    auto_tiering_config - 'tiering_status' || 
    jsonb_build_object('tiering_paused', auto_tiering_config->'tiering_status')
WHERE auto_tiering_config IS NOT NULL AND (auto_tiering_config ? 'tiering_status');

-- Step 2: Convert enum strings back to boolean
UPDATE pools
SET auto_tiering_config =
    jsonb_set(
        auto_tiering_config,
        '{tiering_paused}',
        CASE 
            WHEN auto_tiering_config->>'tiering_paused' = 'PAUSED' THEN 'true'::jsonb
            ELSE 'false'::jsonb
        END,
        true
    )
WHERE auto_tiering_config IS NOT NULL AND (auto_tiering_config ? 'tiering_paused');

