-- Migration: Add HotTierConsumption and ColdTierConsumption to AutoTieringConfig in pool table, defaulting to 0
UPDATE pools
SET auto_tiering_config =
    jsonb_set(
        jsonb_set(
            auto_tiering_config,
            '{hot_tier_consumption}',
            '0'::jsonb,
            true
        ),
        '{cold_tier_consumption}',
        '0'::jsonb,
        true
    )
WHERE auto_tiering_config IS NOT NULL 
  AND (NOT (auto_tiering_config ? 'hot_tier_consumption') 
       OR NOT (auto_tiering_config ? 'cold_tier_consumption'));

