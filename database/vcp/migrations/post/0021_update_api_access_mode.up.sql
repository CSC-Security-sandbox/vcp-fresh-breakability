UPDATE pools
SET api_access_mode = 'DEFAULT'
WHERE deleted_at IS NULL
  AND api_access_mode = 'GCNV';