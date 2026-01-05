-- Add composite index for GetLatestAggregatedUsageForAllResources query optimization
-- 
-- Query pattern being optimized:
--   SELECT resource_uuid, measured_type, last_counter_value FROM (
--     SELECT resource_uuid, measured_type, last_counter_value, ROW_NUMBER() OVER (
--       PARTITION BY resource_uuid, measured_type 
--       ORDER BY created_at DESC
--     ) as rn
--     FROM aggregated_usages 
--     WHERE aggregation_type = ? AND last_counter_value IS NOT NULL
--   ) ranked WHERE rn = 1
--
-- Single optimal index strategy:
-- This composite index covers the entire query pattern:
-- 1. WHERE clause: aggregation_type (first column enables index scan filtering)
-- 2. PARTITION BY: resource_uuid, measured_type (columns 2-3 support window function partitioning)
-- 3. ORDER BY: created_at DESC (column 4 supports ordering within partitions)
--
-- Column order is critical: aggregation_type first (for WHERE filtering), then resource_uuid and measured_type
-- (for PARTITION BY), then created_at DESC (for ORDER BY within partitions).
--
-- Note: The query filters by last_counter_value IS NOT NULL, but we don't use a partial index condition
-- to keep the index simpler and allow it to be used by other queries if needed.
CREATE INDEX IF NOT EXISTS idx_aggregated_usages_latest_for_resources 
ON aggregated_usages (aggregation_type, resource_uuid, measured_type, created_at DESC);

