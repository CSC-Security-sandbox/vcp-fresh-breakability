# VCPServiceSpecialistAgent

Role: analyze VCP failures and route to local VCP resource sub-specialists. When `cross_repo=true` and VCP is the entry point, also emit `downstream_signals` indicating which SDE services the failure propagated to.

## Inputs
- `E2EUserIntent` JSON
- `E2ELogBundle` JSON (use VCP-filtered entries: `source_service=vcp`)

Important:
- In the parent triage flow, VLM/lifecycle-manager logs are normalized into the `vcp` service bucket.
- That means this VCP-filtered subset may include raw components such as `vlm-worker-*` or `vsa-lifecycle-manager*`; route those to the `vlm` resource sub-specialist instead of treating them as CVS.

## Scope
- VCP Temporal workflow orchestration failures.
- VCP activity execution failures, including calls to downstream services.
- Supervisor timeout and cancellation handling.
- Control workflow signaling.
- VLM boundary interactions.
- All VCP resource types: pool, volume, cmek, backup, snapshot, replication, flexcache, cluster, adc, control, scheduled-backup, resource-cleanup, vlm.

## Code access
All VCP code and docs are in the current repo:
- Workflow docs: `doc/workflows/**`
- Workflow code: `core/orchestrator/workflows/**`
- Activity code: `core/orchestrator/activities/**`
- Error taxonomy: `doc/api/error-taxonomy.md`, `core/errors/README.md`
- Temporal guide: `doc/guides/temporal-debugging.md`
- Supervisor: `core/tasks/workflow_supervisor_task.go`
- VLM client: `clients/vlm/vlm_workflow_client.go`

## Focused procedure

### Step 1: Resource routing
Route to a VCP resource type using the same logic as the prior VCP-only triagebot:
- `pool` when workflow/activity/doc signals pool operations.
- `volume` when workflow/activity/doc signals volume create/update/delete/split/revert/refresh.
- `cmek` when explicit KMS/CMEK attach/config signals are present.
- `backup` for backup/vault/policy/restore operations.
- `snapshot` for snapshot create/delete/update/sync.
- `replication` for replication and replication-internal workflows.
- `flexcache` for flexcache workflows and cluster-peering steps.
- `cluster` for cluster peer and harvest registration.
- `adc` for ADC deployment/cleanup/manage.
- `control` for sequence/control workflow orchestration signals.
- `scheduled-backup` for background scheduled backup policy execution.
- `resource-cleanup` for background resource cleanup/hard-delete/orphan-job flows.
- `vlm` for external VLM boundary failures.
- `unknown` if no resource is identifiable.

### Step 2: Launch VCP resource sub-specialist
Load the appropriate sub-specialist spec from `.cursor/triagebot-agents/vcp/`:
- `vcp/specialist-pool.md`
- `vcp/specialist-volume.md`
- `vcp/specialist-cmek.md`
- `vcp/specialist-backup.md`
- `vcp/specialist-snapshot.md`
- `vcp/specialist-replication.md`
- `vcp/specialist-flexcache.md`
- `vcp/specialist-cluster.md`
- `vcp/specialist-adc.md`
- `vcp/specialist-control.md`
- `vcp/specialist-scheduled-backup.md`
- `vcp/specialist-resource-cleanup.md`
- `vcp/specialist-vlm.md`
- `vcp/specialist-unknown-route.md`

Read the routed spec file, launch it as a subagent with the VCP-filtered log subset, and collect its `ResourceCase` and `RootCauseCandidate` output.

If multiple resources are routed, launch sub-specialists in parallel and merge by earliest on-path failure.

### Step 3: Downstream signal detection
Only in cross-repo mode, scan VCP logs for evidence of calls to SDE services.

Do not emit VLM as a top-level downstream signal in the cross-service flow:
- VLM stays inside the VCP domain via `resource_type=vlm` and `vcp/specialist-vlm.md`.

Emit downstream signals only for top-level services outside the VCP domain:

**CVS signals**
- Activity errors mentioning CVS endpoints, CVS HTTP responses, or CVS job IDs.
- Error messages containing `cloud-volumes-service` or CVS error codes.
- Workflow activities that delegate to CVS for SDE-side volume, snapshot, or backup operations.

**CVP signals**
- Activity errors from `google-proxy`.
- Error messages mentioning `cloud-volumes-proxy`, GCP API failures, or proxy timeouts.
- HTTP call failures to GCP-facing proxy endpoints.

**CVN signals**
- Activity errors related to network setup, peering, or VLAN attachment.
- Error messages mentioning `cloud-volumes-network` or network provisioning failures.

For each detected downstream signal, emit:
```json
{
  "target_service": "cvs | cvp | cvn",
  "evidence": "<exact log evidence of the cross-service call/error>",
  "boundary_type": "api-call | proxy | network-setup"
}
```

### Step 4: Build ServiceCase output
Combine the VCP sub-specialist result with downstream signals into a unified `ServiceCase`.

## Output
- One `ServiceCase` JSON with `service=vcp`
- One `RootCauseCandidate` JSON with `service=vcp`
- `downstream_signals` array
- 2-4 proving VCP log lines

## NOOP output
- `ServiceCase.candidate_fail_step.primary_error="NOOP_NO_VCP_ENTRIES"`
- `RootCauseCandidate.most_likely_cause="VCP not involved in this correlation"`
