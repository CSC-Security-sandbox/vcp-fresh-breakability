-- Backfill VolumeAttributes.OriginalSharedBytes for legacy thin-clone volumes.
--
-- Background:
--   `original_shared_bytes` is a new optional key under the JSONB
--   `volume_attributes` column. It records the immutable at-creation shared-
--   bytes baseline for a thin clone and is used by the synchronous splitStop
--   API to report a meaningful remaining-shared-bytes value
--   (= original * (100 - split_complete_percent) / 100).
--
--   The split-start path zeroes `volumes.clones_shared_bytes` to reserve pool
--   capacity, so for clones whose split has already been initiated the at-
--   creation value cannot be recovered from that column alone. For those rows
--   we recover the baseline from the parent snapshot's
--   `logical_size_used_in_bytes` — the exact value the create path uses when
--   it first populates `clones_shared_bytes`.
--
-- Idempotent:
--   Each pass skips rows that already carry a non-null `original_shared_bytes`,
--   so re-running the migration is a no-op.
--
-- Not strictly required for schema:
--   This is a JSONB key, so no DDL is involved; this migration only seeds
--   data. Rows that are not backfilled remain functional — splitStop will
--   simply leave `cloneSharedBytes` at its DB value (typically 0 during a
--   split) instead of computing a remainder.
--
-- Failure isolation (never leave schema_migrations_post dirty):
--   This file is structured so that NO unhandled error can escape and abort
--   the migration. Each backfill statement is wrapped in a PL/pgSQL DO block
--   with `EXCEPTION WHEN OTHERS` that downgrades any runtime error to a
--   non-fatal `RAISE WARNING`. The reasoning:
--     * This is a best-effort data backfill, not a schema change. Clones
--       that are not backfilled degrade gracefully at runtime (splitStop
--       falls back to the raw DB value for legacy rows).
--     * A failed migration would mark this version dirty in
--       schema_migrations_post, blocking every subsequent migration until
--       manually cleaned up. That trade-off is strictly worse than a partial
--       backfill.
--   Cast safety is enforced up front (digit-count cap below bigint range),
--   so the EXCEPTION clause is a defence-in-depth net, not a hot path.

-- Pass 1: clones not currently mid-split.
--   `clones_shared_bytes` still holds the at-creation value, so we can copy
--   it directly. Covers the vast majority of legacy clones.
--
--   `clones_shared_bytes` is the column the create path captured the
--   parent-snapshot logical size into at clone-create time, and it is never
--   mutated between then and splitStart. For clones that have not yet been
--   split, this is strictly more authoritative than re-reading the snapshot
--   row today (snapshot logical size is resynced from ONTAP by
--   sync_snapshot_activities and can drift).
DO $backfill_pass1$
BEGIN
    UPDATE volumes
    SET volume_attributes = jsonb_set(
        volume_attributes,
        '{original_shared_bytes}',
        -- Explicit numeric cast guarantees `to_jsonb` produces a JSON
        -- number (not a string) regardless of how GORM mapped the Go
        -- `uint64` field (numeric, bigint, etc.). The Go side decodes
        -- *uint64 from JSON numbers, so this preserves round-trip
        -- correctness.
        to_jsonb(clones_shared_bytes::numeric),
        true
    )
    WHERE volume_attributes IS NOT NULL
      AND volume_attributes ? 'clone_parent_info'
      AND volume_attributes->'clone_parent_info' IS NOT NULL
      AND volume_attributes->'clone_parent_info' <> 'null'::jsonb
      AND (
            NOT (volume_attributes ? 'original_shared_bytes')
            OR volume_attributes->'original_shared_bytes' IS NULL
            OR volume_attributes->'original_shared_bytes' = 'null'::jsonb
          )
      AND clones_shared_bytes > 0;
EXCEPTION
    WHEN OTHERS THEN
        RAISE WARNING 'migration 0034 pass 1 (backfill original_shared_bytes from clones_shared_bytes) skipped due to error [%]: %', SQLSTATE, SQLERRM;
END
$backfill_pass1$;

-- Pass 2: clones currently mid-split.
--   `clones_shared_bytes` was zeroed by split-start, so we recover the
--   baseline from the parent snapshot referenced in
--   volume_attributes.clone_parent_info.parent_snapshot_uuid. Both volumes
--   and snapshots are addressed by the `uuid` column on BaseModel.
--
-- Cast safety:
--   The regex `^[0-9]{1,18}$` caps the matched digit count at 18, keeping
--   the resulting numeric value strictly below 10^18 — comfortably under
--   PostgreSQL bigint's max of 9,223,372,036,854,775,807 (~9.22 × 10^18).
--   This makes the `::bigint` cast unable to overflow regardless of what
--   sync_snapshot_activities or out-of-band tooling has written into the
--   JSONB. Snapshots whose logical size exceeds 10^18 bytes (1 EB) are not
--   produced by any realistic VCP workflow.
DO $backfill_pass2$
BEGIN
    UPDATE volumes v
    SET volume_attributes = jsonb_set(
        v.volume_attributes,
        '{original_shared_bytes}',
        to_jsonb((s.snapshot_attributes->>'logical_size_used_in_bytes')::bigint),
        true
    )
    FROM snapshots s
    WHERE v.volume_attributes IS NOT NULL
      AND v.volume_attributes ? 'clone_parent_info'
      AND v.volume_attributes->'clone_parent_info' IS NOT NULL
      AND v.volume_attributes->'clone_parent_info' <> 'null'::jsonb
      AND (
            NOT (v.volume_attributes ? 'original_shared_bytes')
            OR v.volume_attributes->'original_shared_bytes' IS NULL
            OR v.volume_attributes->'original_shared_bytes' = 'null'::jsonb
          )
      AND v.clones_shared_bytes = 0
      AND s.uuid = v.volume_attributes->'clone_parent_info'->>'parent_snapshot_uuid'
      AND s.snapshot_attributes IS NOT NULL
      AND (s.snapshot_attributes->>'logical_size_used_in_bytes') IS NOT NULL
      AND (s.snapshot_attributes->>'logical_size_used_in_bytes') ~ '^[0-9]{1,18}$'
      AND (s.snapshot_attributes->>'logical_size_used_in_bytes')::bigint > 0;
EXCEPTION
    WHEN OTHERS THEN
        RAISE WARNING 'migration 0034 pass 2 (backfill original_shared_bytes from parent snapshot) skipped due to error [%]: %', SQLSTATE, SQLERRM;
END
$backfill_pass2$;
