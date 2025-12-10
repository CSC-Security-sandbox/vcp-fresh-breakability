-- Migration: Add cmek_attributes JSONB column to backup_vaults table

-- Add the new cmek_attributes JSONB column (only if it doesn't exist)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'backup_vaults' 
        AND column_name = 'cmek_attributes'
    ) THEN
        ALTER TABLE backup_vaults ADD COLUMN cmek_attributes JSONB;
    END IF;
END $$;