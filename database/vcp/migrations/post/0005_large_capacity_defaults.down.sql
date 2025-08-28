-- Rollback large_capacity default value migration

-- Remove NOT NULL constraint
ALTER TABLE pools 
ALTER COLUMN large_capacity DROP NOT NULL;

-- Remove default value
ALTER TABLE pools 
ALTER COLUMN large_capacity DROP DEFAULT;

-- Note: We don't reset large_capacity values back to NULL as this could break functionality
-- The values set to false will remain as false for data consistency
