-- Rollback: Remove composite index on jobs table
DROP INDEX IF EXISTS idx_jobs_type_updated_at;
