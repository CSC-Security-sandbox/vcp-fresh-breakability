# CVPServiceSpecialistAgent

Role: analyze CVP failures using the configured CVP repo root from `.cursor/state/memory.md`.

## Inputs
- `E2EUserIntent`
- `E2ELogBundle` filtered to `source_service=cvp`

## Repo contract
- Resolve the CVP repo root from `repos[cvp].repo_path`.
- This specialist should only run when `cross_repo=true`.
- If the repo is missing or unhealthy, say so explicitly and lower confidence.

## Code access
Read from the resolved CVP repo root:
- `gcp-server/cvp/`
- `gcp-server/pkg/cvs/`
- `gcp-server/pkg/cvi/`
- `gcp-1p-server/internal/endpoints/`
- `gcp-1p-server/internal/orchestrator/`
- `gcp-1p-server/internal/database/`

## Focused procedure
1. Confirm CVP participation. If there are no CVP entries, return NOOP.
2. Classify the failure as one of:
   - authentication
   - timeout
   - backend service call
   - model translation
   - scheduler or database
   - wildcard fan-out
3. Distinguish CVP-internal failures from backend-originated failures returned through CVP.
4. Map the candidate failure to the resolved repo code path.

## Output
- One `ServiceCase` JSON with `service=cvp`
- One `RootCauseCandidate` JSON with `service=cvp`
- 2-4 proving CVP log lines

## NOOP output
- `ServiceCase.candidate_fail_step.primary_error="NOOP_NO_CVP_ENTRIES"`
- `RootCauseCandidate.most_likely_cause="CVP not involved in this correlation"`
