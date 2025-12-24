-- ============================================================
-- Template: Update Volume State
-- ============================================================
-- TICKET: JIRA-XXXX
-- AUTHOR: your-email@company.com
-- DESCRIPTION: [Brief description of why this change is needed]
-- AFFECTED_ROWS_EXPECTED: 1
-- ============================================================

-- Verify the current state first (run this SELECT manually if needed)
-- SELECT uuid, state, state_details, name 
-- FROM volumes 
-- WHERE uuid = 'REPLACE_WITH_UUID';

UPDATE volumes 
SET state = 'REPLACE_WITH_NEW_STATE',
    state_details = 'Manual fix - JIRA-XXXX - [brief reason]',
    updated_at = NOW()
WHERE uuid = 'REPLACE_WITH_UUID'
  AND state = 'REPLACE_WITH_CURRENT_STATE';  -- Safety: only update if in expected state

