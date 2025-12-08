-- Migration: Remove clone_parent_info from volume_attributes JSONB column in volumes table
-- This migration removes the clone_parent_info field from volume_attributes JSONB objects
UPDATE volumes
SET volume_attributes = volume_attributes - 'clone_parent_info'
WHERE volume_attributes IS NOT NULL 
  AND volume_attributes ? 'clone_parent_info';

