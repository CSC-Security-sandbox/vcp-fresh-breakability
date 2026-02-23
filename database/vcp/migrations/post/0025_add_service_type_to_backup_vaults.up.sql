-- Migration: Add service_type column to backup_vaults table

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'backup_vaults' AND column_name = 'service_type'
    ) THEN
        ALTER TABLE backup_vaults ADD COLUMN service_type VARCHAR(50) DEFAULT 'GCNV';
    END IF;
END $$;
