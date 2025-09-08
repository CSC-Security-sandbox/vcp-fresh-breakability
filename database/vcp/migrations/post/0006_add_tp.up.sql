CREATE INDEX if not exists idx_pools_regional_tenant_project
ON pools ((cluster_details->>'regional_tenant_project'));