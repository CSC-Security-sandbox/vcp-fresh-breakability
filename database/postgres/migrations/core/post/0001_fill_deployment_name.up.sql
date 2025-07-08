-- Fill deployment_name for existing pools using legacy logic: poolName + '-' + accountName
-- Assumes pools.account_id references accounts.id and accounts.name is the account name

UPDATE pools
SET
    deployment_name = pools.name || '-' || accounts.name
FROM accounts
WHERE
    pools.account_id = accounts.id
    AND (
        pools.deployment_name IS NULL
        OR pools.deployment_name = ''
    );
