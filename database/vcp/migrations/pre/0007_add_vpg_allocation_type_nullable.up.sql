-- Ensure allocation_type exists before AutoMigrate enforces NOT NULL.
-- Guard for fresh installs where volume_performance_groups may not exist yet.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = current_schema()
          AND table_name = 'volume_performance_groups'
    ) THEN
        ALTER TABLE volume_performance_groups
            ADD COLUMN IF NOT EXISTS allocation_type VARCHAR(32);
    END IF;
END;
$$;
