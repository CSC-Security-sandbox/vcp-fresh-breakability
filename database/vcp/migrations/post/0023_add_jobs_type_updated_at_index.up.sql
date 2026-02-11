-- Query pattern being optimized:
--   SELECT ... FROM jobs
--   WHERE type = ? AND updated_at >= ? AND updated_at <= ? AND deleted_at IS NULL
--   ORDER BY updated_at
--
-- Index strategy:
-- This composite index optimizes queries that filter by job type and updated_at time range.
-- The index order (type, updated_at) is optimal because:
-- 1. type is used for equality filtering (WHERE type = ?)
-- 2. updated_at is used for range filtering (updated_at >= ? AND updated_at <= ?)
-- 3. updated_at is also used for ordering (ORDER BY updated_at)

CREATE INDEX IF NOT EXISTS idx_jobs_type_updated_at 
ON jobs (type, updated_at) 
WHERE deleted_at IS NULL;
