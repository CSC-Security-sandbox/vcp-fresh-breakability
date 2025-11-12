UPDATE pools
SET api_access_mode = 'GCNV'
WHERE deleted_at IS NULL
  AND (
    api_access_mode IS NULL
        OR api_access_mode = ''
        OR TRIM(api_access_mode) = ''
    );

