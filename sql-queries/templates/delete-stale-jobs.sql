-- ============================================================
-- Template: Delete Stale Jobs
-- ============================================================
-- TICKET: JIRA-XXXX
-- AUTHOR: your-email@company.com
-- DESCRIPTION: Clean up stale jobs stuck in running state
-- AFFECTED_ROWS_EXPECTED: [expected count]
-- ============================================================

-- First, verify what will be deleted (run this SELECT manually)
-- SELECT id, uuid, state, job_type, created_at
-- FROM jobs 
-- WHERE state = 'running'
--   AND created_at < NOW() - INTERVAL '24 hours';

DELETE FROM jobs 
WHERE uuid IN (
    'uuid-1',
    'uuid-2'
    -- Add specific UUIDs here
)
AND state = 'running';  -- Safety: only delete if still in running state

