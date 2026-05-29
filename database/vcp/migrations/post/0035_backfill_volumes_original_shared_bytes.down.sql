-- Rollback for 0034: remove the original_shared_bytes key from volume_attributes
-- for every volume that has it. Safe to re-run; jsonb minus a missing key is a
-- no-op. We only touch rows where the key is actually present to avoid
-- unnecessary table churn.
--
-- Wrapped in a DO block with EXCEPTION WHEN OTHERS so the rollback can never
-- leave schema_migrations_post in a dirty state — same rationale as up.sql.
-- The key being a JSONB attribute (not a column) means a partial rollback is
-- harmless: any rows still carrying the orphaned key are ignored by every
-- runtime read path except `_splitStopVolume`, which gates on
-- `VolumeAttributes.OriginalSharedBytes != nil` and falls back gracefully.
DO $rollback_pass$
BEGIN
    UPDATE volumes
    SET volume_attributes = volume_attributes - 'original_shared_bytes'
    WHERE volume_attributes IS NOT NULL
      AND volume_attributes ? 'original_shared_bytes';
EXCEPTION
    WHEN OTHERS THEN
        RAISE WARNING 'migration 0034 rollback (remove original_shared_bytes) skipped due to error [%]: %', SQLSTATE, SQLERRM;
END
$rollback_pass$;
