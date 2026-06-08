-- Prevent duplicate active external cluster hosts for the same location and host name.
-- Partial index allows re-onboarding after soft-delete (deleted_at IS NULL only).

CREATE UNIQUE INDEX IF NOT EXISTS idx_external_cluster_hosts_location_host_unique
    ON external_cluster_hosts (location_id, host_name)
    WHERE deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_external_cluster_hosts_host_name
    ON external_cluster_hosts (host_name)
    WHERE deleted_at IS NULL;
