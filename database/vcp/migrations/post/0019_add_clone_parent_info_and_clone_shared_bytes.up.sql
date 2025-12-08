-- Migration: Add clone_parent_info to volume_attributes JSONB column in volumes table
-- This migration adds the clone_parent_info field to existing volume_attributes JSONB objects
-- The field will be added as NULL for volumes that don't have it, which is safe since it's optional

-- Add clone_parent_info field to volume_attributes (if not already present)
-- This is a no-op for volumes that already have the field, making it idempotent
UPDATE volumes
SET volume_attributes = 
    CASE 
        WHEN volume_attributes IS NULL THEN '{"clone_parent_info": null}'::jsonb
        WHEN NOT (volume_attributes ? 'clone_parent_info') THEN 
            jsonb_set(volume_attributes, '{clone_parent_info}', 'null'::jsonb, true)
        ELSE volume_attributes
    END
WHERE volume_attributes IS NULL 
   OR NOT (volume_attributes ? 'clone_parent_info');
