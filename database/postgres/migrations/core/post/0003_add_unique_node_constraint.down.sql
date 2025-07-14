-- Remove unique constraint on node_id
DROP INDEX IF EXISTS idx_node_node_group_maps_node_id_unique;
