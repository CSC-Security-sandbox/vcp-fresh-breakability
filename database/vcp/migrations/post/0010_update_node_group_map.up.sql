UPDATE node_node_group_maps nnm
SET harvest_config = nnm.harvest_config || jsonb_build_object(
        'IS_REGIONAL_HA',
        CASE
            WHEN p.pool_attributes IS NOT NULL
                THEN COALESCE((p.pool_attributes->>'is_regional_ha')::boolean, false)
            ELSE false
            END,
        'TENANT_PROJECT', COALESCE(p.cluster_details->>'regional_tenant_project', ''),
        'DEPLOYMENT_NAME', COALESCE(p.deployment_name, 'default-deployment'),
        'POOL_NAME', COALESCE(p.name, 'default-pool')
                                           )
    FROM nodes n
JOIN pools p ON n.pool_id = p.id
WHERE nnm.node_id = n.id
  AND nnm.harvest_config IS NOT NULL
  AND (
    nnm.harvest_config->>'IS_REGIONAL_HA' IS NULL
   OR nnm.harvest_config->>'TENANT_PROJECT' IS NULL
   OR nnm.harvest_config->>'DEPLOYMENT_NAME' IS NULL
   OR nnm.harvest_config->>'POOL_NAME' IS NULL
    );