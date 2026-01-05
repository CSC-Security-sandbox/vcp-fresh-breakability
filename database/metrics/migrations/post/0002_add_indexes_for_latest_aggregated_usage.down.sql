-- Remove index added for GetLatestAggregatedUsageForAllResources query optimization

DROP INDEX IF EXISTS idx_aggregated_usages_latest_for_resources;

