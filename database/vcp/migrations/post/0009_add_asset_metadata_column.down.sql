-- Rollback: Remove satisfyZI and satisfyZS columns from pools table

-- Drop the columns
ALTER TABLE pools
DROP COLUMN IF EXISTS satisfy_zi;

ALTER TABLE pools
DROP COLUMN IF EXISTS satisfy_zs;

-- Rollback: Remove assetMetadata column from pools table

-- Remove the column
ALTER TABLE pools 
DROP COLUMN IF EXISTS asset_metadata;



