-- Migration: Rollback - Remove cmek_attributes JSONB column from backup_vaults table

-- Drop the cmek_attributes JSONB column
ALTER TABLE backup_vaults
DROP COLUMN IF EXISTS cmek_attributes;