-- Add unique constraint on node_id to prevent duplicate node mappings
-- This ensures a node can only be assigned to one node group

-- First, remove any duplicate mappings if they exist (keep the most recent one)
WITH ranked_mappings AS (SELECT id,
                                ROW_NUMBER() OVER (PARTITION BY node_id ORDER BY created_at DESC) as rn
                         FROM node_node_group_maps
                         WHERE deleted_at IS NULL)
DELETE
FROM node_node_group_maps
WHERE id IN (SELECT id
             FROM ranked_mappings
             WHERE rn > 1);

-- Add the unique constraint on node_id
CREATE UNIQUE INDEX IF NOT EXISTS idx_node_node_group_maps_node_id_unique
    ON node_node_group_maps (node_id)
    WHERE deleted_at IS NULL;
