-- Migration: Remove TieringPaused from AutoTieringConfig in pool table
UPDATE pools
SET auto_tiering_config = auto_tiering_config - 'tiering_paused'
WHERE auto_tiering_config ? 'tiering_paused';
