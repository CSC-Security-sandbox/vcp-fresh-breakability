DELETE FROM aggregated_usages
WHERE id NOT IN (
    SELECT MIN(id)
    FROM aggregated_usages
    GROUP BY resource_uuid, aggregation_start, aggregation_end, measured_type, resource_type
);