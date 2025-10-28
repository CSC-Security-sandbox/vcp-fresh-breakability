-- Migration: Remove TieringFullnessThreshold from AutoTieringConfig in pool table
UPDATE pools
SET auto_tiering_config = auto_tiering_config - 'tiering_fullness_threshold'
WHERE auto_tiering_config ? 'tiering_fullness_threshold';

