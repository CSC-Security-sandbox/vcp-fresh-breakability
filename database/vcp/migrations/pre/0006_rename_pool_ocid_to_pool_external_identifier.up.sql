DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'pools'
          AND column_name = 'pool_ocid'
          AND table_schema = current_schema()
    ) THEN
        ALTER TABLE pools RENAME COLUMN pool_ocid TO pool_external_identifier;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_indexes
        WHERE schemaname = current_schema()
          AND tablename = 'pools'
          AND indexname = 'idx_pools_pool_ocid_unique'
    ) THEN
        ALTER INDEX idx_pools_pool_ocid_unique
            RENAME TO idx_pools_pool_external_identifier_unique;
    END IF;
END;
$$;
