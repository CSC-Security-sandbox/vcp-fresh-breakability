-- Update cluster_details.external_name for legacy records that match the old pattern
UPDATE pools
SET
    cluster_details = jsonb_set(
        cluster_details,
        '{external_name}',
        to_jsonb(
            (
                cluster_details ->> 'external_name'
            ) || '-cluster'
        )
    )
FROM accounts
WHERE
    pools.account_id = accounts.id
    AND pools.cluster_details ->> 'external_name' = pools.name || '-' || accounts.name;

-- (Optional) Enforce NOT NULL constraint after filling
-- ALTER TABLE pools ALTER COLUMN deployment_name SET NOT NULL;