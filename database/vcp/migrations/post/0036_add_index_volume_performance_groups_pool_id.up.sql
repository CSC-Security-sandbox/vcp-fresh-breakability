-- Per-pool VPG cap count (non-deleted rows only).
CREATE INDEX IF NOT EXISTS idx_volume_performance_groups_pool_id
ON volume_performance_groups (pool_id)
WHERE deleted_at IS NULL;
