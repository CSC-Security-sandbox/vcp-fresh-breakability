-- Enforces unique Harvest poller port per node group for active (non-deleted) mappings.
-- Safety net for AssignTwoNodesToTwoGroups and GetFirstAvailablePort under concurrency.
CREATE UNIQUE INDEX IF NOT EXISTS idx_node_node_group_maps_group_port_active_uq
    ON node_node_group_maps (node_group_id, ((harvest_config ->> 'PORT')))
WHERE deleted_at IS NULL
  AND (harvest_config ->> 'PORT') IS NOT NULL
  AND (harvest_config ->> 'PORT') <> '';
