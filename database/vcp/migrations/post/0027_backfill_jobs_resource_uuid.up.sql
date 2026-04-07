-- Backfill resource_uuid column from job_attributes JSONB.
-- The column is added by GORM AutoMigrate when the Job struct gains ResourceUUID.
-- This migration populates it for existing rows.
UPDATE jobs
SET resource_uuid = job_attributes->>'resource_uuid'
WHERE (resource_uuid IS NULL OR resource_uuid = '')
  AND job_attributes IS NOT NULL
  AND job_attributes->>'resource_uuid' IS NOT NULL
  AND job_attributes->>'resource_uuid' != '';
