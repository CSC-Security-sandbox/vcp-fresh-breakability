-- Rollback Migration: Revert volume_uuid backfill for snapshot jobs
-- Purpose: Revert snapshot jobs back to old format where ResourceUUID contains volume UUID
--
-- WARNING: This rollback is conservative and only reverts jobs where we can verify
--          that volume_uuid matches an existing volume. This is because:
--          1. We cannot distinguish between migrated jobs and new jobs created with the new format
--          2. We cannot restore the original resource_uuid (snapshot UUID) for migrated jobs
--
-- This migration:
-- 1. Finds snapshot jobs that have volume_uuid set
-- 2. Verifies the volume_uuid matches an existing volume
-- 3. Moves volume_uuid back to resource_uuid (restores old format)
-- 4. Keeps volume_uuid in job_attributes (for schema compatibility)

UPDATE jobs
SET job_attributes = jsonb_set(
    COALESCE(job_attributes, '{}'::jsonb),
    '{resource_uuid}',
    to_jsonb(job_attributes->>'volume_uuid')
),
updated_at = CURRENT_TIMESTAMP
FROM volumes
WHERE jobs.type IN ('CREATE_SNAPSHOT', 'DELETE_SNAPSHOT')
  AND jobs.deleted_at IS NULL
  AND jobs.job_attributes IS NOT NULL
  AND jobs.job_attributes->>'volume_uuid' IS NOT NULL
  AND jobs.job_attributes->>'volume_uuid' != ''
  AND volumes.uuid = jobs.job_attributes->>'volume_uuid'
  AND volumes.deleted_at IS NULL;

-- Note: We do NOT remove volume_uuid from job_attributes as it's now part of the schema.
-- The field will remain but will be ignored by old code that only reads resource_uuid.
-- Jobs created with the new format (where resource_uuid = snapshot UUID) will have
-- resource_uuid set to volume UUID after rollback

