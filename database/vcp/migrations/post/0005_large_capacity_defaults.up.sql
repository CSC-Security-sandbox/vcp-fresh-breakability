-- Set default values for large_capacity field where it may be null
-- This ensures all existing pools have a proper large_capacity value

-- Update any NULL large_capacity values to false (standard pools)
UPDATE pools 
SET large_capacity = false 
WHERE large_capacity IS NULL;

-- Ensure the column has a proper default for future inserts
ALTER TABLE pools 
ALTER COLUMN large_capacity SET DEFAULT false;

-- Add NOT NULL constraint to prevent future null values
ALTER TABLE pools 
ALTER COLUMN large_capacity SET NOT NULL;
