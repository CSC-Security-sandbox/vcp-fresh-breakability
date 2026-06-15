-- Rollback: Restore volume_performance_groups.is_shared from allocation_type

ALTER TABLE volume_performance_groups
    ADD COLUMN IF NOT EXISTS is_shared BOOLEAN;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'volume_performance_groups'
          AND column_name = 'allocation_type'
    ) THEN
        UPDATE volume_performance_groups
        SET is_shared = CASE WHEN allocation_type = 'PER_VOLUME' THEN false ELSE true END
        WHERE is_shared IS NULL;
    ELSE
        UPDATE volume_performance_groups
        SET is_shared = true
        WHERE is_shared IS NULL;
    END IF;
END $$;

ALTER TABLE volume_performance_groups
    ALTER COLUMN is_shared SET NOT NULL;

ALTER TABLE volume_performance_groups
    DROP COLUMN IF EXISTS allocation_type;
