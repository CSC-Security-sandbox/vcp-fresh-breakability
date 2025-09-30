-- Update cluster_details.reserved_ips_in_subnet for legacy records
-- This migration populates the reserved_ips_in_subnet field based on existing subnet_names
-- Each subnet gets 6 reserved IPs (totalIPPerHAPair constant value)

-- NOTE: Pools without subnet_names will be skipped and will use default IP counting
-- in the application logic (6 IPs per pool regardless of subnet)

UPDATE pools
SET
    cluster_details = jsonb_set(
        cluster_details,
        '{reserved_ips_in_subnet}',
        (
            SELECT jsonb_agg(
                jsonb_build_object(
                    'subnet_name', subnet_name,
                    'ips_reserved', 6
                )
            )
            FROM jsonb_array_elements_text(cluster_details -> 'subnet_names') AS subnet_name
        )
    )
WHERE
    -- Only update pools that have subnet_names but no reserved_ips_in_subnet.
    -- Having where clause helps during migration reruns or failures.
    deleted_at IS NULL
    AND cluster_details ? 'subnet_names'
    AND cluster_details -> 'subnet_names' IS NOT NULL
    AND jsonb_array_length(cluster_details -> 'subnet_names') > 0
    AND (
        NOT (cluster_details ? 'reserved_ips_in_subnet')
        OR cluster_details -> 'reserved_ips_in_subnet' IS NULL
        OR jsonb_array_length(cluster_details -> 'reserved_ips_in_subnet') = 0
    );

