-- Backfill denormalized fields in pool_attributes and volume_attributes
-- This migration populates account_name, deployment_name, and is_regional_ha
-- in the attributes JSONB fields to eliminate JOIN operations in queries

-- Update pool_attributes.account_name for all existing pools
-- Edge cases handled:
-- - Pools with NULL or missing account_id are skipped (foreign key constraint ensures account exists)
-- - Accounts with deleted_at are included (soft-deleted accounts still have valid names)
-- - Accounts with NULL name will result in empty string
UPDATE pools
SET pool_attributes = COALESCE(pool_attributes, '{}'::jsonb) || jsonb_build_object(
        'account_name', COALESCE(accounts.name, '')
                                                                )
    FROM accounts
WHERE pools.account_id = accounts.id
  AND pools.deleted_at IS NULL
  AND (
    pool_attributes IS NULL
   OR pool_attributes = '{}'::jsonb
   OR pool_attributes->>'account_name' IS NULL
   OR pool_attributes->>'account_name' = ''
    );

-- Update volume_attributes with account_name, deployment_name, and is_regional_ha for all existing volumes
-- Edge cases handled:
-- - Volumes with missing account_id or pool_id are skipped (foreign key constraints ensure they exist)
-- - Volumes with deleted pools are still updated (pool may be soft-deleted but data is valid)
-- - Boolean conversion for is_regional_ha safely handles NULL and invalid values
-- - Accounts or pools with NULL names/deployment_names result in empty strings
UPDATE volumes
SET volume_attributes = COALESCE(volume_attributes, '{}'::jsonb) || jsonb_build_object(
        'account_name', COALESCE(accounts.name, ''),
        'deployment_name', COALESCE(pools.deployment_name, ''),
        'is_regional_ha', CASE
                              WHEN pools.pool_attributes IS NOT NULL
                                  AND pools.pool_attributes->>'is_regional_ha' IS NOT NULL
            THEN COALESCE((pools.pool_attributes->>'is_regional_ha')::boolean, false)
            ELSE false
            END
                                                                    )
    FROM accounts, pools
WHERE volumes.pool_id = pools.id
  AND volumes.account_id = accounts.id
  AND volumes.deleted_at IS NULL
  AND (
    volume_attributes IS NULL
   OR volume_attributes = '{}'::jsonb
   OR volume_attributes->>'account_name' IS NULL
   OR volume_attributes->>'account_name' = ''
   OR volume_attributes->>'deployment_name' IS NULL
   OR volume_attributes->>'deployment_name' = ''
   OR volume_attributes->>'is_regional_ha' IS NULL
    );
