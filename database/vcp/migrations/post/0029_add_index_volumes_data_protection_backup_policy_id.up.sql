-- Speeds up filters and aggregates on backup_policy_id extracted from data_protection jsonb
-- (e.g. ListBackupPolicyVolumeCount, GetVolumeCountByBackupPolicyID, vault/policy lookups).
CREATE INDEX IF NOT EXISTS idx_volumes_data_protection_backup_policy_id
ON volumes ((data_protection->>'backup_policy_id'))
WHERE deleted_at IS NULL
  AND (data_protection->>'backup_policy_id') IS NOT NULL
  AND (data_protection->>'backup_policy_id') <> '';
