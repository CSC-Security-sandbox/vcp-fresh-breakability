DROP INDEX IF EXISTS idx_pools_pool_ocid_unique;

ALTER TABLE pools DROP COLUMN IF EXISTS pool_ocid;
