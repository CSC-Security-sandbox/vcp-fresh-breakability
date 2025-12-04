-- Migration: Remove CloudWriteModeEnabled from AutoTieringPolicy in volumes table
UPDATE volumes
SET auto_tiering_policy = auto_tiering_policy - 'cloud_write_mode_enabled'
WHERE auto_tiering_policy IS NOT NULL 
  AND auto_tiering_policy ? 'cloud_write_mode_enabled';

