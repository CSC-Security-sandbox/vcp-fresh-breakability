-- Add is_expert_mode_backup column to backup_chain_histories.
-- Existing rows are backfilled from backups.attributes->>'is_expert_mode_backup' via the volume UUID.
-- Rows that cannot be matched (orphaned histories) safely default to false (standard/DEFAULT mode).
UPDATE backup_chain_histories h
SET is_expert_mode_backup = COALESCE(
    (
        SELECT (b.attributes->>'is_expert_mode_backup')::boolean
        FROM backups b
        WHERE b.volume_uuid = h.resource_uuid
          AND b.deleted_at IS NULL
        LIMIT 1
    ),
    false
)
WHERE is_expert_mode_backup = false;
