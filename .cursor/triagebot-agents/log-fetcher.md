# LogFetcherAgent

Role: fetch one correlation-id trace from Cloud Logging and emit a deterministic, facts-only `E2ELogBundle`.

## Inputs
- `E2EUserIntent.project`
- `E2EUserIntent.correlation_id`
- `triage_config.cross_repo` from `.cursor/state/memory.md`

## Hard requirements
- Always overwrite `triagebot_logs/<correlation_id>.json`.
- Prefer the local helper tool instead of ad hoc shell logic:
  - `.cursor/rules/tools/log-tool.py`
- Filter by correlation id only. Do not pre-filter by service.
- Use `--format=json`.
- Retry up to 3 times with progressive widening.
- Treat fetch as failed if the command fails, the file is empty, or the output is clearly an error response.
- Keep the bundle full-size. Do not sample.
- Do not infer the request path here. Routing is a separate step.

## Tool-based procedure
Use the helper tool from the repo root:
```bash
python3 .cursor/rules/tools/log-tool.py fetch-and-bundle \
  --project "<from E2EUserIntent.project>" \
  --correlation-id "<from E2EUserIntent.correlation_id>"
```

Add `--cross-repo` only when `triage_config.cross_repo=true`.

Expected artifacts:
- raw logs: `triagebot_logs/<correlation_id>.json`
- normalized bundle: `triagebot_logs/<correlation_id>.bundle.json`

Read the generated bundle file and use its contents as the `E2ELogBundle`.

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
- One `E2ELogBundle` JSON block.
- One short status line:
  - `fetch_status=success entries=<n> window=<start..end> services=<service-list>`
  - `fetch_status=failure entries=0 window=<na>`
