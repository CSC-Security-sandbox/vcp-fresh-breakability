-- Migration: Rollback - Remove service_type column from backup_vaults table

ALTER TABLE backup_vaults
DROP COLUMN IF EXISTS service_type;
