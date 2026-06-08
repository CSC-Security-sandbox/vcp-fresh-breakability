-- Rollback for 0037: remove the `state` key from clone_parent_info for every
-- volume that has it. Idempotent and safe to re-run; the JSONB `#-` operator
-- on a missing path is a no-op. We only touch rows where the nested key is
-- actually present to minimise table churn.
--
-- Note: `state_details` is intentionally left untouched. The up migration
-- never writes it, so any value present is from runtime code (which the
-- forward migration does not own). Removing it here would risk discarding
-- legitimate runtime data.
--
-- Wrapped in a DO block with EXCEPTION WHEN OTHERS so the rollback cannot
-- leave schema_migrations_post in a dirty state — same rationale as up.sql.
DO $rollback_state$
BEGIN
    UPDATE volumes
    SET volume_attributes = volume_attributes #- '{clone_parent_info,state}'
    WHERE volume_attributes IS NOT NULL
      AND volume_attributes ? 'clone_parent_info'
      AND volume_attributes->'clone_parent_info' ? 'state';
EXCEPTION
    WHEN OTHERS THEN
        RAISE WARNING 'migration 0037 rollback (remove clone_parent_info.state) skipped due to error [%]: %', SQLSTATE, SQLERRM;
END
$rollback_state$;
