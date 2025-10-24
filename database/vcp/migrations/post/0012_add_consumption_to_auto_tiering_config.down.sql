-- Migration: Remove HotTierConsumption and ColdTierConsumption from AutoTieringConfig in pool table
UPDATE pools
SET auto_tiering_config = auto_tiering_config - 'hot_tier_consumption' - 'cold_tier_consumption'
WHERE auto_tiering_config IS NOT NULL 
  AND (auto_tiering_config ? 'hot_tier_consumption' 
       OR auto_tiering_config ? 'cold_tier_consumption');

