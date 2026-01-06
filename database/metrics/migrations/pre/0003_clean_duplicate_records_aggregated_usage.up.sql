-- Remove duplicate records from aggregated_usages table
-- Keep the record with the smallest id (oldest) for each unique combination
-- Only proceed if duplicates exist to avoid unnecessary work

DO $$
DECLARE
    duplicate_count integer;
BEGIN
    -- Check if any duplicates exist
    SELECT COUNT(*) INTO duplicate_count 
    FROM (
        SELECT resource_uuid, aggregation_start, aggregation_end, measured_type, resource_type
        FROM aggregated_usages
        GROUP BY resource_uuid, aggregation_start, aggregation_end, measured_type, resource_type
        HAVING COUNT(*) > 1
        LIMIT 1
    ) duplicates;
    
    -- Only run the DELETE if duplicates are found
    IF duplicate_count > 0 THEN
        DELETE FROM aggregated_usages 
        WHERE id NOT IN (
            SELECT id FROM (
                SELECT id, ROW_NUMBER() OVER (
                    PARTITION BY resource_uuid, aggregation_start, aggregation_end, measured_type, resource_type
                    ORDER BY id ASC  -- Keep the oldest record (smallest id)
                ) as rn
                FROM aggregated_usages
            ) ranked WHERE rn = 1
        );
    END IF;
END $$;