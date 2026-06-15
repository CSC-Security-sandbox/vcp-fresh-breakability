-- Migration: Replace volume_performance_groups.is_shared (bool) with allocation_type (enum string)

ALTER TABLE volume_performance_groups
    ADD COLUMN IF NOT EXISTS allocation_type VARCHAR(32);

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'volume_performance_groups'
          AND column_name = 'is_shared'
    ) THEN
        UPDATE volume_performance_groups
        SET allocation_type = CASE WHEN is_shared THEN 'SHARED' ELSE 'PER_VOLUME' END
        WHERE allocation_type IS NULL;
    ELSE
        -- Fresh schemas may already be allocation_type-only.
        UPDATE volume_performance_groups
        SET allocation_type = 'SHARED'
        WHERE allocation_type IS NULL;
    END IF;
END $$;

ALTER TABLE volume_performance_groups
    ALTER COLUMN allocation_type SET NOT NULL;

ALTER TABLE volume_performance_groups
    DROP COLUMN IF EXISTS is_shared;
