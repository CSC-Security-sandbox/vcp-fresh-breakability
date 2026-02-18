-- Migration: Fill backup_chain_history with backup lifecycle events
-- This migration creates timeline entries for backup events (create, modify, delete)
-- following the pattern: each size change or state change creates a new history entry
-- with deleted_at marking when that version/size was superseded or deleted.

-- PostgresSQL 13+ has gen_random_uuid() built-in, no extension needed

-- Create backup chain history entries for all backups
-- Each backup creates a history entry, with older backups marked as deleted
-- when the next backup is created (deleted_at = next backup's created_at)
INSERT INTO backup_chain_histories (
    uuid,
    resource_name,
    size,
    resource_uuid,
    created_at,
    updated_at,
    deleted_at,
    consumer_id,
    deployment_name
)
SELECT
    gen_random_uuid()::text,
    COALESCE(v.name, 'unknown-volume') as volume_name,
    COALESCE(b.latest_logical_backup_size, 0) as size,
    b.volume_uuid,
    COALESCE(b.created_at, NOW()) as created_at,
    COALESCE(b.updated_at, NOW()) as updated_at,
    -- Set deleted_at to the created_at of the next backup, or keep original for latest
    CASE
        WHEN next_backup.next_created_at IS NOT NULL THEN next_backup.next_created_at
        ELSE b.deleted_at -- Keep original deleted_at for latest backup (or if backup is actually deleted)
    END as deleted_at,
    b.attributes->>'account_identifier' as consumer_id,
    bv.name as deployment_name
FROM backups b
LEFT JOIN volumes v ON b.volume_uuid = v.uuid
LEFT JOIN backup_vaults bv ON b.backup_vault_id = bv.id
LEFT JOIN (
    -- Find the next backup's created_at for each backup using window function
    SELECT
        id,
        volume_uuid,
        LEAD(created_at) OVER (PARTITION BY volume_uuid ORDER BY created_at, id) as next_created_at
    FROM backups
    WHERE volume_uuid IS NOT NULL AND volume_uuid != ''
) next_backup ON b.id = next_backup.id AND b.volume_uuid = next_backup.volume_uuid
WHERE b.volume_uuid IS NOT NULL
  AND b.volume_uuid != ''
  AND NOT EXISTS (
    SELECT 1 FROM backup_chain_histories h WHERE h.resource_uuid = b.volume_uuid
  )
ORDER BY b.volume_uuid, b.created_at;