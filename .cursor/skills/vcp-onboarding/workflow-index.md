# VCP Workflow Index

Quick map from **resource / operation** → **workflow file** → **doc**. Full catalog: `doc/workflows/README.md`.

## How workflows are organized

```
core/orchestrator/workflows/
├── *_workflow.go          # Main workflow implementations
├── pool_workflows.go      # Pool create/update/delete
├── volume_create_workflow.go
├── cluster_workflows.go
├── backup_workflow.go
├── replicationWorkflows/  # Replication sub-package
└── oci/                   # OCI-specific workflows
```

Activities live in `core/orchestrator/activities/` (and subdirs). Orchestrator entry points: `core/orchestrator/factory/gcp/*.go` and `factory/oci/*.go`.

## Core resource workflows

| Resource | Operation | Workflow file | Doc |
|----------|-----------|---------------|-----|
| Pool | Create / update / delete | `pool_workflows.go` | `doc/workflows/core/pool-workflows.md` |
| Volume | Create | `volume_create_workflow.go` | `doc/workflows/core/volume-workflows.md` |
| Volume | Update | `volume_update_workflow.go` | same |
| Volume | Delete | `volume_delete_workflow.go` | same |
| Volume | Refresh / revert | `volume_refresh_workflow.go`, `volume_revert_workflow.go` | same |
| Host group | CRUD | `hostgroup_workflow.go` | — |
| Snapshot | Create / delete | `snapshot_create_workflow.go`, `snapshot_delete_workflow.go` | `doc/workflows/core/snapshot-workflows.md` |
| Cluster | Lifecycle | `cluster_workflows.go` | `doc/workflows/core/cluster-workflows.md` |
| Active Directory | Sync / bind | `active_directory_workflows.go` | `doc/workflows/core/adc-workflows.md` |
| ADC | Collection | `adc_workflow.go` | same |

## Backup ecosystem

| Resource | Workflow file | Doc |
|----------|---------------|-----|
| Backup | `backup_workflow.go` | `doc/workflows/core/backup-workflows.md` |
| Backup restore | `backup_restore_workflow.go` | same |
| Backup vault | `backup_vault_workflows.go`, `backup_vault_cmek_workflows.go` | — |
| Backup policy | `backup_policy_workflows.go` | — |
| Scheduled backup | (background) | `doc/workflows/background/scheduled-backup-workflows.md` |

## Replication & FlexCache

| Resource | Location | Doc |
|----------|----------|-----|
| Volume replication | `replicationWorkflows/` | `doc/workflows/replication/replication-workflows.md` |
| FlexCache | factory + workflows | `doc/workflows/flexcache/flexcache-workflows.md` |

## KMS & security

| Resource | Workflow file | Doc |
|----------|---------------|-----|
| KMS config | `kms_activities/` + related workflows | `doc/workflows/kms/kms-workflows.md` |
| Kerberos | `kerberos_workflow.go` | — |
| Quota rules | `quota_rule_*_workflow.go` | — |

## Background & control

| Area | Doc |
|------|-----|
| Auto-tiering | `doc/workflows/background/auto-tiering-workflows.md` |
| Resource cleanup | `doc/workflows/background/resource-cleanup-workflows.md` |
| Control / supervisor | `control_workflow.go` — `doc/workflows/control/control-workflows.md` |
| Resource events | `handle_resource_event_workflow.go` | `doc/architecture/designs/0007-resource-event-endpoints-and-design.md` |
| Project events | `start_project_event_workflow.go`, `finish_project_event_workflow.go` | — |
| Harvest / telemetry | `harvest_upgrade_workflow.go`, `register_node_to_harvest_farm_workflow.go` | `doc/architecture/designs/0016-harvest-collector-system.md` |

## Volume performance groups

| Operation | Workflow file |
|-----------|---------------|
| Create | `volume_performance_group_create_workflow.go` |
| Update | `volume_performance_group_update_workflow.go` |
| Delete | `volume_performance_group_delete_workflow.go` |

## Workflow interface pattern

All workflows implement a common pattern (see `doc/workflows/README.md`):

- `Setup` — init context, query handlers, retry policies
- `Run` — orchestrate activities and child workflows
- `UpdateJobStatus` — persist job state on terminal outcomes

**Rule:** No I/O in workflow code. If it blocks or talks to the outside world, it belongs in an **activity**.

## Finding the right file quickly

```bash
# List workflow files for a resource
ls core/orchestrator/workflows/*volume*

# Grep workflow registration (worker)
rg "RegisterWorkflow" worker/

# Grep activity used by a workflow
rg "VolumeCreateActivity" core/orchestrator/
```

## Workflow docs vs code

`doc/workflows/` describes intent and flow. **Source of truth is the `.go` files** — docs can drift. When onboarding, read workflow doc first, then skim `Run()` in the workflow file.

## OCI parallel paths

OCI-specific workflows: `core/orchestrator/workflows/oci/`. Orchestrator: `core/orchestrator/factory/oci/oci_orchestrator.go`.

If your team is GCP-only, defer OCI until needed.
