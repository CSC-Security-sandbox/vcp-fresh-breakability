-- Migration: Backfill volume_uuid for snapshot jobs
-- Purpose: Migrate old snapshot jobs where ResourceUUID contains volume UUID
--          to new format where ResourceUUID contains snapshot UUID and VolumeUUID contains volume UUID
--
-- This migration:
-- 1. Finds snapshot jobs (CreateSnapshot, DeleteSnapshot) without volume_uuid
-- 2. Gets volume UUID from resource_uuid (old format)
-- 3. Gets snapshot UUID by looking up snapshot by name and volume
-- 4. Updates job_attributes: moves resource_uuid -> volume_uuid, sets resource_uuid = snapshot UUID

UPDATE jobs
SET job_attributes = jsonb_set(
    jsonb_set(
        COALESCE(job_attributes, '{}'::jsonb),
        '{volume_uuid}',
        to_jsonb(job_attributes->>'resource_uuid')
    ),
    '{resource_uuid}',
    to_jsonb(snapshots.uuid)
),
updated_at = CURRENT_TIMESTAMP
FROM volumes
JOIN snapshots ON snapshots.volume_id = volumes.id
WHERE jobs.type IN ('CREATE_SNAPSHOT', 'DELETE_SNAPSHOT')
  AND jobs.deleted_at IS NULL
  AND jobs.job_attributes IS NOT NULL
  AND jobs.job_attributes->>'resource_uuid' IS NOT NULL
  AND jobs.job_attributes->>'resource_uuid' != ''
  AND (jobs.job_attributes->>'volume_uuid' IS NULL OR jobs.job_attributes->>'volume_uuid' = '')
  AND jobs.resource_name IS NOT NULL
  AND jobs.resource_name != ''
  AND jobs.account_id IS NOT NULL
  AND volumes.uuid = jobs.job_attributes->>'resource_uuid'
  AND snapshots.name = jobs.resource_name
  AND snapshots.account_id = jobs.account_id;

-- Note: We allow soft-deleted snapshots and volumes because the snapshot UUID still exists in the database and to maintain the consistency in job_attributes format even for historical jobs
