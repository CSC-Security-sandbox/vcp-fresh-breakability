-- Backfill VolumeAttributes.CloneParentInfo.state for legacy thin-clone volumes.
--
-- Background:
--   The `state` field under `clone_parent_info` (a nested key in the JSONB
--   `volume_attributes` column) is the wire-level enum surfaced as
--   `cloneDetails.state` on the GCNV API. New code paths populate it on every
--   relevant transition (create-clone, splitStart, splitStop, terminal split
--   failure), but two legacy populations exist in production data:
--
--     (1) Volumes created before split tracking shipped have either a missing
--         or empty `state` key and would render with no state at all on
--         describe / list responses.
--     (2) Volumes that went through a split before VSCP-6010 (PR #3656) carry
--         the pre-rename wire values "CLONED" / "SPLITTING" / "SPLIT_FAILED"
--         which no longer match the API enum. They render with a value that
--         the OpenAPI validator and downstream consumers no longer recognise.
--
--   This migration normalises both populations in four ordered passes (no
--   ONTAP round-trip required):
--
--     Pass 0 — deterministic 1:1 rename of pre-VSCP-6010 legacy values to the
--              current SPLIT_STATE_* enum. The legacy value itself carries
--              the classification, so no signals are consulted.
--     Passes 1-3 — for rows still without a state (missing/empty), derive
--                  state from DB signals:
--     - SPLIT_STATE_IN_PROGRESS  — split-start ran (clones_shared_bytes
--                                  zeroed) AND there is a recent split job
--                                  still in NEW or PROCESSING.
--     - SPLIT_STATE_FAILED       — split-start ran AND the latest split job
--                                  is in terminal ERROR.
--     - SPLIT_STATE_NOT_SPLITTING — everything else (the vast majority:
--                                  clones that have never been split, plus
--                                  clones whose split has completed and
--                                  whose `clone_parent_info` has not yet
--                                  been cleared by refresh, plus orphan or
--                                  WAIT_FOR_TEMPORAL jobs which are not a
--                                  reliable in-progress signal).
--
--   `state_details` is deliberately left untouched. New failures populate it
--   via ClassifyONTAPSplitError (catalog-message strings, safe to expose to
--   CCFE). Legacy ONTAP error strings on old job rows may contain raw ONTAP
--   text, so the safest backfill is to skip the field entirely; the API
--   contract allows it to be absent / empty.
--
-- Idempotent:
--   Pass 0 matches only the three legacy literals, so once it runs the
--   table contains no legacy values and a re-run matches zero rows.
--   Passes 1-3 each guard on `state` being missing/null/empty inside
--   clone_parent_info, so they no-op on re-run. Passes are ordered by
--   specificity (IN_PROGRESS → FAILED → NOT_SPLITTING catch-all), so later
--   passes cannot overwrite earlier ones. Pass 0 runs first so that any
--   legacy value is normalised to a current value before the signal-based
--   passes evaluate the idempotency guard.
--
-- Failure isolation (never leave schema_migrations_post dirty):
--   Each pass is wrapped in a PL/pgSQL DO block with `EXCEPTION WHEN OTHERS`
--   that downgrades any runtime error to a RAISE WARNING. Same rationale as
--   migration 0035: this is a best-effort data backfill, not a schema
--   change. Rows that aren't backfilled continue to work — the runtime read
--   path treats a missing state as equivalent to NOT_SPLITTING, and the
--   periodic volume-refresh activity will reconcile from ONTAP on its next
--   tick.
--
-- Job-to-volume join:
--   `jobs.resource_uuid` is unique per volume for split jobs (one split
--   workflow per volume at a time), so the join needs only resource_uuid;
--   no account_id correlation is required.
--
--   Why the `deleted_at IS NULL` predicate is on every latest-job subquery:
--     1. Correctness — jobs are soft-deleted via
--        DataStoreRepository.DeleteJob, and GORM bumps `updated_at` on the
--        delete. Without this predicate, a soft-deleted job row could win
--        the DISTINCT ON race against a real earlier job and misclassify
--        the volume.
--     2. Index usage — the `idx_jobs_type_updated_at` index added in
--        migration 0023 is partial on `deleted_at IS NULL`, so it only
--        participates in plans that include this predicate.
--
--   What the existing index does and does not do for this query:
--     - DOES help filter `WHERE type = 'SPLIT_CLONE_VOLUME'
--       AND deleted_at IS NULL` (used as an Index/Bitmap Index Scan).
--     - DOES NOT order or group by resource_uuid, so the
--       `ORDER BY resource_uuid, updated_at DESC` and DISTINCT ON still
--       go through an in-memory sort + uniq on top of the filtered set.
--   That trade-off is acceptable for a one-shot data backfill — the
--   filtered subset (split jobs only) is small enough that the sort cost
--   is negligible. We deliberately do NOT add a new index just for this
--   migration; introducing one would outlive its purpose and cost write
--   amplification on the jobs table forever.

-- Pass 0: rename pre-VSCP-6010 legacy state values to the current enum.
--   The legacy value already carries the classification, so this is a pure
--   1:1 remap with no signal lookups:
--     'CLONED'       → 'SPLIT_STATE_NOT_SPLITTING'
--     'SPLITTING'    → 'SPLIT_STATE_IN_PROGRESS'
--     'SPLIT_FAILED' → 'SPLIT_STATE_FAILED'
--   Running this before passes 1-3 means by the time their idempotency
--   guard evaluates `state is missing/null/empty`, no legacy literals remain
--   to confuse it. Trusting the legacy literal here (rather than re-deriving
--   from signals) is intentional: the literal is more authoritative than
--   re-derivation when `clones_shared_bytes` was set by older split-start
--   code with different reservation semantics, or when the original split
--   job row has been garbage-collected.
--
--   create_if_missing=false: we only overwrite an existing key, never add
--   one. Rows without a `state` key remain handled by passes 1-3.
DO $pass0_rename_legacy$
BEGIN
    UPDATE volumes
    SET volume_attributes = jsonb_set(
        volume_attributes,
        '{clone_parent_info,state}',
        CASE volume_attributes->'clone_parent_info'->>'state'
            WHEN 'CLONED'       THEN '"SPLIT_STATE_NOT_SPLITTING"'::jsonb
            WHEN 'SPLITTING'    THEN '"SPLIT_STATE_IN_PROGRESS"'::jsonb
            WHEN 'SPLIT_FAILED' THEN '"SPLIT_STATE_FAILED"'::jsonb
        END,
        false
    )
    WHERE volume_attributes IS NOT NULL
      AND volume_attributes ? 'clone_parent_info'
      AND volume_attributes->'clone_parent_info' IS NOT NULL
      AND volume_attributes->'clone_parent_info' <> 'null'::jsonb
      AND volume_attributes->'clone_parent_info' ? 'state'
      AND volume_attributes->'clone_parent_info'->>'state' IN (
          'CLONED', 'SPLITTING', 'SPLIT_FAILED'
      );
EXCEPTION
    WHEN OTHERS THEN
        RAISE WARNING 'migration 0037 pass 0 (rename legacy state values) skipped due to error [%]: %', SQLSTATE, SQLERRM;
END
$pass0_rename_legacy$;

-- Pass 1: clones with an in-flight split job.
--   Criteria:
--     - clones_shared_bytes = 0 (split-start has run; reservation in effect)
--     - latest SPLIT_CLONE_VOLUME job for this volume is NEW or PROCESSING
--   WAIT_FOR_TEMPORAL is intentionally excluded — jobs stuck in that state
--   are often orphans from a failed workflow start, not active splits.
DO $pass1_in_progress$
BEGIN
    UPDATE volumes v
    SET volume_attributes = jsonb_set(
        v.volume_attributes,
        '{clone_parent_info,state}',
        '"SPLIT_STATE_IN_PROGRESS"'::jsonb,
        true
    )
    FROM (
        SELECT DISTINCT ON (resource_uuid)
               resource_uuid,
               state
        FROM jobs
        WHERE type = 'SPLIT_CLONE_VOLUME'
          AND deleted_at IS NULL
        ORDER BY resource_uuid, updated_at DESC
    ) latest
    WHERE v.uuid = latest.resource_uuid
      AND latest.state IN ('NEW', 'PROCESSING')
      AND v.clones_shared_bytes = 0
      AND v.volume_attributes IS NOT NULL
      AND v.volume_attributes ? 'clone_parent_info'
      AND v.volume_attributes->'clone_parent_info' IS NOT NULL
      AND v.volume_attributes->'clone_parent_info' <> 'null'::jsonb
      AND (
            NOT (v.volume_attributes->'clone_parent_info' ? 'state')
            OR v.volume_attributes->'clone_parent_info'->>'state' IS NULL
            OR v.volume_attributes->'clone_parent_info'->>'state' = ''
          );
EXCEPTION
    WHEN OTHERS THEN
        RAISE WARNING 'migration 0037 pass 1 (set IN_PROGRESS) skipped due to error [%]: %', SQLSTATE, SQLERRM;
END
$pass1_in_progress$;

-- Pass 2: clones whose latest split job ended in ERROR.
--   Criteria:
--     - clones_shared_bytes = 0
--     - latest SPLIT_CLONE_VOLUME job for this volume is in ERROR
--   `state_details` is intentionally NOT set; see header for the
--   ONTAP-text-leak rationale. The API contract permits `state_details`
--   to be absent/empty even when state == SPLIT_STATE_FAILED.
DO $pass2_failed$
BEGIN
    UPDATE volumes v
    SET volume_attributes = jsonb_set(
        v.volume_attributes,
        '{clone_parent_info,state}',
        '"SPLIT_STATE_FAILED"'::jsonb,
        true
    )
    FROM (
        SELECT DISTINCT ON (resource_uuid)
               resource_uuid,
               state
        FROM jobs
        WHERE type = 'SPLIT_CLONE_VOLUME'
          AND deleted_at IS NULL
        ORDER BY resource_uuid, updated_at DESC
    ) latest
    WHERE v.uuid = latest.resource_uuid
      AND latest.state = 'ERROR'
      AND v.clones_shared_bytes = 0
      AND v.volume_attributes IS NOT NULL
      AND v.volume_attributes ? 'clone_parent_info'
      AND v.volume_attributes->'clone_parent_info' IS NOT NULL
      AND v.volume_attributes->'clone_parent_info' <> 'null'::jsonb
      AND (
            NOT (v.volume_attributes->'clone_parent_info' ? 'state')
            OR v.volume_attributes->'clone_parent_info'->>'state' IS NULL
            OR v.volume_attributes->'clone_parent_info'->>'state' = ''
          );
EXCEPTION
    WHEN OTHERS THEN
        RAISE WARNING 'migration 0037 pass 2 (set FAILED) skipped due to error [%]: %', SQLSTATE, SQLERRM;
END
$pass2_failed$;

-- Pass 3: catch-all — everything still without a state.
--   Includes:
--     - clones with clones_shared_bytes > 0 (never split)
--     - clones with no SPLIT_CLONE_VOLUME job rows
--     - clones whose latest split job is DONE or WAIT_FOR_TEMPORAL
--   The DONE-job-but-clone-info-still-present case is an anomaly handled
--   silently here; the periodic refresh activity is responsible for
--   clearing the orphaned clone metadata in a separate pass.
DO $pass3_not_splitting$
BEGIN
    UPDATE volumes
    SET volume_attributes = jsonb_set(
        volume_attributes,
        '{clone_parent_info,state}',
        '"SPLIT_STATE_NOT_SPLITTING"'::jsonb,
        true
    )
    WHERE volume_attributes IS NOT NULL
      AND volume_attributes ? 'clone_parent_info'
      AND volume_attributes->'clone_parent_info' IS NOT NULL
      AND volume_attributes->'clone_parent_info' <> 'null'::jsonb
      AND (
            NOT (volume_attributes->'clone_parent_info' ? 'state')
            OR volume_attributes->'clone_parent_info'->>'state' IS NULL
            OR volume_attributes->'clone_parent_info'->>'state' = ''
          );
EXCEPTION
    WHEN OTHERS THEN
        RAISE WARNING 'migration 0037 pass 3 (set NOT_SPLITTING) skipped due to error [%]: %', SQLSTATE, SQLERRM;
END
$pass3_not_splitting$;
