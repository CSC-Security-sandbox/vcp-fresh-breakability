UPDATE pools
SET pool_credentials = jsonb_set(
    pool_credentials, 
    '{username}', 
    '"vcp_admin"'::jsonb, 
    true
)
WHERE deleted_at IS NULL
  AND (
    pool_credentials->>'username' IS NULL
    OR pool_credentials->>'username' = ''
    OR TRIM(pool_credentials->>'username') = ''
  );
