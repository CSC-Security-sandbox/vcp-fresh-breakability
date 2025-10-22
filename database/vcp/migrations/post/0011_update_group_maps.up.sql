UPDATE node_node_group_maps nnm
SET harvest_config = nnm.harvest_config || jsonb_build_object(
        'AUTH_TYPE', COALESCE((p.pool_credentials->>'auth_type')::int, 0),
        'SECRET_ID', COALESCE(p.pool_credentials->>'secret_id', '')
                                           )
    FROM nodes n
JOIN pools p ON n.pool_id = p.id
WHERE nnm.node_id = n.id
  AND nnm.harvest_config IS NOT NULL
  AND (nnm.harvest_config->>'SECRET_ID' IS NULL OR nnm.harvest_config->>'SECRET_ID' = '');