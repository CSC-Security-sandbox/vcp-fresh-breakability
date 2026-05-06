# LogFetcherAgent

Role: fetch a correlation trace from Cloud Logging, then expand by `job_id` for the orchestration trace and by resource id for resource history, and emit a deterministic, facts-only `LogBundle`.

## Inputs
- `UserIntent.project`
- `UserIntent.correlation_id`
- `triage_config.cross_repo` from `.cursor/state/memory.md`

## Hard requirements
- Always overwrite `triagebot_logs/<correlation_id>.json`.
- Prefer the local helper tool instead of ad hoc shell logic:
  - `.cursor/rules/tools/log-tool.py`
- Start with a correlation-id fetch. Do not pre-filter by service on the first pass.
- Use `--format=json`.
- Retry the initial correlation fetch up to 3 times with progressive widening.
- Treat fetch as failed if the correlation fetch command fails, the file is empty, or the output is clearly an error response.
- Keep the bundle full-size. Do not sample.
- Do not infer the request path here. Routing is a separate step.
- Expansion is allowed only after the initial correlation bundle exists.
- Run `job` expansion before `resource` expansion so resource ids surfaced inside job/activity logs are available to the resource pass.
- The `resource` pass is for resource history (recent prior operations on the same resource); it is best-effort context, not the primary trace.

## Tool-based procedure
Use the helper tool from the repo root:
```bash
python3 .cursor/rules/tools/log-tool.py fetch-and-bundle \
  --project "<from UserIntent.project>" \
  --correlation-id "<from UserIntent.correlation_id>"
```

Add `--cross-repo` only when `triage_config.cross_repo=true`.

Default expansion behavior for `fetch-and-bundle` (3 passes):
1. **Pass 1 — correlation**: fetch by `correlation_id` (the bare id is used as a free-text search term, not as a `key=value` filter, so it matches wherever the id appears in the log payload).
2. **Pass 2 — job**: from pass 1, collect `job_id` values (with `workflow_id` values folded in as aliases since they overlap on VCP). Run **one** gcloud call with an OR of those bare ids, scoped to the traced time window padded ±15 min, freshness 30d, capped at 8 ids.
3. **Pass 3 — resource history**: from passes 1+2, collect resource ids (`pool_id`, `volume_id`, `snapshot_id`, `backup_id`, `creationToken`, `resource_id`, `resource_name`, etc.; values shorter than 4 chars or all-zero are skipped). Run one gcloud call with an OR of those bare ids, **no time clause**, freshness ladder **7d → 15d → 30d**: try 7d first, widen to 15d if fewer than 10 lines came back, then to 30d on the same condition. Stop once the threshold is met or the ladder is exhausted. Capped at 8 resource ids.
4. Merge and deduplicate all fetched entries into one final raw log file and one final `LogBundle`. Each entry carries `fetch_origin ∈ {correlation, job, resource}` so downstream agents can tell which logs came from which pass.

Notes:
- `request_id`, `tracking_id`, and Google operation ids are **extracted into `derived_pivots` for evidence** but are **not** used as fetch drivers.
- Pass-2 and pass-3 failures are non-fatal — they degrade the bundle but do not abort triage. Each attempted pass is recorded in `LogBundle.fetch_summary.expansion_attempts` with `status="ok"`, `"error"`, or `"skipped"` (and the error message when applicable) so downstream agents can surface incomplete coverage instead of treating a thin bundle as complete.
- `fetch_origin=resource` entries are resource history, not the primary trace. Downstream consumers must not pick them as the failed step, terminal event, or boundary candidate; they are corroborating context only. The log fetcher just labels them — enforcement lives in the coordinator and specialist rules.
- Use `--no-expand` only when the coordinator explicitly wants correlation-only behavior (e.g. a known-narrow follow-up fetch).

Expected artifacts:
- raw logs: `triagebot_logs/<correlation_id>.json`
- normalized bundle: `triagebot_logs/<correlation_id>.bundle.json`

Read the generated bundle file and use its contents as the `LogBundle`.

Mode behavior:
- default -> include VCP log entries only and suppress downstream boundary artifacts
- `--cross-repo` -> include VCP, CVS, CVP, and CVN entries plus boundary artifacts

## Manual fallback
Only if the helper tool is unavailable, use the equivalent shell fetch procedure with 3 progressive attempts and then build the bundle with:
```bash
python3 .cursor/rules/tools/log-tool.py \
  bundle \
  --log-file "triagebot_logs/<correlation_id>.json"
```

Add `--cross-repo` only when `triage_config.cross_repo=true`.

## Normalization rules
For each entry, extract:
- `event_id`
- `timestamp`
- `timestamp_ns`
- `severity`
- `component`
- `source_service`
- `message`
- `message_template`
- `fetch_origin` (`correlation | job | resource`)
- `correlation_id`
- `related_ids.workflow_id`
- `related_ids.job_id`
- `related_ids.tracking_id`
- `related_ids.request_id`
- `error_signature`
- `error.code`
- `error.message`
- `error.stack`
- `payload_fragment`

Classify `source_service` by component:
- `vcp`: `vsa-control-plane`, `core-api`, `worker`, `google-proxy`, `vlm-worker`, `vsa-lifecycle-manager`
- `cvs`: `cloud-volumes-service`, `cloud-volumes-infrastructure`, `cloud-volumes-internal`, `cloud-volumes-service-worker`
- `cvp`: `cloud-volumes-proxy`, `cloud-volumes-proxy-1p`
- `cvn`: `cloud-volumes-network`
- `unknown`: anything else

Treat VLM/lifecycle-manager logs as `vcp` while preserving their raw `component`.

Also collect bundle-level project context when evidenced:
- `tenant_project_id`
- `tenant_project_number`
- `subnet_host_project`

## Derived bundle structures
Build:
- `fetch_summary`
- `derived_pivots`
- `error_inventory`
- `cross_service_calls`
- `google_operation_hints`
- `boundary_candidates`
- `terminal_events`
- `recovered_error_signatures`
- `last_error_by_service`
- `service_breakdown`
- `severity_counts`
- `time_window`

## Output
- One `LogBundle` JSON block.
- One short status line:
  - `fetch_status=success entries=<n> window=<start..end> services=<service-list> origins=<origin-list> resource_lookback=<7d|15d|30d|none> failed_expansions=<job|resource|none|comma-list>`
  - `fetch_status=failure entries=0 window=<na>`
