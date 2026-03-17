DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'backup_vaults'
          AND column_name = 'service_type'
          AND table_schema = current_schema()
    ) THEN
        ALTER TABLE backup_vaults
            ALTER COLUMN service_type TYPE varchar(20);
    END IF;
END;
$$;
