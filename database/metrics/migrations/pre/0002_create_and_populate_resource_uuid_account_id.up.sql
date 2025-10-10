DO $$
BEGIN
    -- Add resource_uuid column if it does not exist
    IF EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_name='aggregated_usages'
) THEN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='aggregated_usages' AND column_name='resource_uuid'
    ) THEN
        EXECUTE 'ALTER TABLE aggregated_usages ADD COLUMN resource_uuid varchar(255);';
END IF;
END IF;

    -- Add account_id column if it does not exist
IF EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_name='aggregated_usages'
) THEN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='aggregated_usages' AND column_name='account_id'
    ) THEN
        EXECUTE 'ALTER TABLE aggregated_usages ADD COLUMN account_id varchar(255);';
END IF;
END IF;

    -- Update resource_uuid to 'unknown' only if NULL
IF EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_name='aggregated_usages'
) THEN
    EXECUTE 'UPDATE aggregated_usages SET resource_uuid = ''unknown'' WHERE resource_uuid IS NULL;';
END IF;

-- Update account_id to 'unknown' only if NULL
IF EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_name='aggregated_usages'
) THEN
    EXECUTE 'UPDATE aggregated_usages SET account_id = ''test'' WHERE account_id IS NULL;';
END IF;

END$$;