ALTER TABLE pools ADD COLUMN IF NOT EXISTS pool_ocid TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS idx_pools_pool_ocid_unique
    ON pools (pool_ocid)
    WHERE pool_ocid <> '';
