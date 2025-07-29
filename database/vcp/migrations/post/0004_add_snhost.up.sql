-- Copy sn_host_project from cluster_details JSONB to sn_host_project column
UPDATE pools
SET sn_host_project = cluster_details->>'sn_host_project'
WHERE cluster_details ? 'sn_host_project';