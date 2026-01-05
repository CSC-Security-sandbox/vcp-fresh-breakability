-- Create unique index for aggregated_usage table
-- This index ensures uniqueness across resource_uuid, aggregation_end, aggregation_start, measured_type, and resource_type

CREATE UNIQUE INDEX IF NOT EXISTS idx_aggregated_usage_unique
    ON aggregated_usages (resource_uuid, aggregation_end, aggregation_start, measured_type, resource_type);