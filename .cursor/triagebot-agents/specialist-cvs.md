# CVSServiceSpecialistAgent

Role: analyze CVS failures using the configured CVS repo root from `.cursor/state/memory.md`.

## Inputs
- `UserIntent`
- `LogBundle` filtered to `source_service=cvs`

## Repo contract
- Resolve the CVS repo root from `repos[cvs].repo_path`.
- This specialist should only run when `cross_repo=true`.
- If the repo is missing or unhealthy, return a dependency-gap note instead of pretending repo-backed analysis happened.

## Code access
Read from the resolved CVS repo root:
- `server/restapi/`
- `storage/orchestrator/`
- `infrastructure/database/`
- `storage/ontaprest/`
- `storage/ontap/`
- `utils/errors/`
- `utils/cvn/errors.go`

## Focused procedure
1. Confirm CVS participation. If there are no CVS entries, return NOOP.
2. Classify the failure layer:
   - API
   - orchestrator
   - job-system
   - retry-engine or database
   - ONTAP provider
   - downstream service
3. Identify the earliest on-path CVS failure and separate transient retries from terminal failure.
4. When CVS is the entry point, emit `downstream_signals` for:
   - `cvp`
   - `cvn`
5. Map the failure to the resolved repo code path and note dependency gaps if the repo prevented verification depth.

## Output
- One `ServiceCase` JSON with `service=cvs`
- One `RootCauseCandidate` JSON with `service=cvs`
- `downstream_signals` array when applicable
- 2-4 proving CVS log lines

## NOOP output
- `ServiceCase.candidate_fail_step.primary_error="NOOP_NO_CVS_ENTRIES"`
- `RootCauseCandidate.most_likely_cause="CVS not involved in this correlation"`
