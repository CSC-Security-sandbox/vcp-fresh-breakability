package detectors

import (
	"context"
	"fmt"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/model"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/vmscan"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"sort"
	"strings"
	"time"
)

const (
	ReasonVMOrphanPoolMissing = "vm_orphan_pool_missing"

	vmDetectorWorkflowIDPrefix = "leaked-resources-scan-gce-instances"

	// defaultVMLabelKey is the GCE label whose value identifies the owning
	// pool. VLM stamps this label on every VM it creates. Override via
	// LEAKED_RESOURCES_VM_LABEL_KEY when the convention changes.
	defaultVMLabelKey = "pool_uuid"

	// defaultVMMinAgeHours is the minimum VM age before it is considered
	// for leak reporting. Newly created VMs may temporarily lack a matching
	// pool row (create flow in flight), so we wait until they have settled.
	defaultVMMinAgeHours = 6
)

// VMDetector reports GCE VMs whose pool_uuid label does not match any
// non-deleted Pool in VCP. It runs in the core pod but defers the actual
// GCE Compute API calls to a Temporal workflow on the worker pod (which is
// the only pod whose service account holds compute permissions).
//
// Pipeline contract: implements model.Detector. Returns ([]model.LeakRecord, error).
// Soft-failure semantics: when the Temporal client is unavailable or the
// workflow itself fails, Detect logs and returns (nil, err) so the pipeline
// records a per-detector failure without crashing the cron run.
type VMDetector struct {
	// submitWorkflow is injectable for tests. In production it forwards to
	// workflowengine.FetchTemporalClient + ExecuteWorkflow + run.Get.
	submitWorkflow func(ctx context.Context, in vmscan.ScanInput) (*vmscan.ScanOutput, error)

	labelKey  string
	minVMAge  time.Duration
	taskQueue string
}

// NewVMDetector returns a detector wired to the production Temporal path.
// Honours these env vars:
//   - LEAKED_RESOURCES_VM_LABEL_KEY (default "pool_uuid")
//   - LEAKED_RESOURCES_VM_MIN_AGE_HOURS (default 6)
func NewVMDetector() *VMDetector {
	d := &VMDetector{
		labelKey:  env.GetString("LEAKED_RESOURCES_VM_LABEL_KEY", defaultVMLabelKey),
		minVMAge:  defaultVMMinAge(),
		taskQueue: workflowengine.BackgroundTaskQueue,
	}
	d.submitWorkflow = d.submitWorkflowViaTemporal
	return d
}

func defaultVMMinAge() time.Duration {
	h := envGetInt("LEAKED_RESOURCES_VM_MIN_AGE_HOURS", defaultVMMinAgeHours)
	if h < 1 {
		h = defaultVMMinAgeHours
	}
	return time.Duration(h) * time.Hour
}

func (d *VMDetector) Name() string {
	return "vm_orphan"
}

// Detect finds orphaned GCP Compute VMs in regional tenant projects.
// Algorithm:
//  1. List all RTPs (including from soft-deleted pools) — this is what makes
//     the detector catch VMs whose owning pool was deleted from VCP but whose
//     GCE resources were never cleaned up.
//  2. Build activePoolUUIDs from non-deleted pools.
//  3. Submit ScanGCEInstancesWorkflow to the worker (synchronous via run.Get).
//  4. For each returned VM: skip if pool_uuid is in activePoolUUIDs;
//     otherwise emit a LeakRecord (subject to min-age grace window).
func (d *VMDetector) Detect(ctx context.Context, storage database.Storage) ([]model.LeakRecord, error) {
	logger := util.GetLogger(ctx)
	if d == nil {
		return nil, nil
	}

	rtpProjects, err := storage.ListAllTpProjects(ctx)
	if err != nil {
		return nil, fmt.Errorf("vm detector: list all tp projects: %w", err)
	}
	if len(rtpProjects) == 0 {
		logger.Info("vm_orphan detector: no tenant projects found, skipping scan")
		return nil, nil
	}

	pools, err := storage.ListPools(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("vm detector: list pools: %w", err)
	}
	activePoolUUIDs := make(map[string]struct{}, len(pools))
	for _, p := range pools {
		if p != nil && p.UUID != "" {
			activePoolUUIDs[p.UUID] = struct{}{}
		}
	}

	sort.Strings(rtpProjects)
	logger.Infof("vm_orphan detector started: rtp_projects=%d active_pools=%d label_key=%q min_vm_age=%s",
		len(rtpProjects), len(activePoolUUIDs), d.labelKey, d.minVMAge.String())

	out, err := d.submitWorkflow(ctx, vmscan.ScanInput{ProjectIDs: rtpProjects})
	if err != nil {
		return nil, fmt.Errorf("vm detector: scan workflow: %w", err)
	}
	if out == nil {
		logger.Warn("vm_orphan detector: workflow returned nil output")
		return nil, nil
	}
	if len(out.PartialFailures) > 0 {
		for _, f := range out.PartialFailures {
			logger.Warnf("vm_orphan detector: project %s partial failure: %s", f.Project, f.Error)
		}
	}

	now := time.Now().UTC()
	var records []model.LeakRecord
	for _, vm := range out.Items {
		poolUUID := vm.Labels[d.labelKey]

		if poolUUID == "" {
			if !vmIsOlderThan(vm.CreationTimestamp, now, d.minVMAge) {
				continue
			}
			records = append(records, buildVMLeakRecord(vm, poolUUID))
			continue
		}
		if _, active := activePoolUUIDs[poolUUID]; !active {
			if !vmIsOlderThan(vm.CreationTimestamp, now, d.minVMAge) {
				continue
			}
			records = append(records, buildVMLeakRecord(vm, poolUUID))
		}
	}

	if len(records) == 0 {
		logger.Info("vm_orphan detector: no leaked VMs found")
	} else {
		logger.Infof("vm_orphan detector: found %d leaked VM(s)", len(records))
	}

	return records, nil
}

func (d *VMDetector) submitWorkflowViaTemporal(ctx context.Context, in vmscan.ScanInput) (*vmscan.ScanOutput, error) {
	if workflowengine.FetchTemporalClient == nil {
		return nil, fmt.Errorf("temporal client accessor not initialized")
	}
	c, err := workflowengine.FetchTemporalClient()
	if err != nil {
		return nil, fmt.Errorf("get temporal client: %w", err)
	}
	if c == nil {
		return nil, fmt.Errorf("temporal client is nil")
	}

	wfID := fmt.Sprintf("%s-%s", vmDetectorWorkflowIDPrefix, time.Now().UTC().Format("20060102T150405Z"))
	run, err := c.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:                d.taskQueue,
			ID:                       wfID,
			WorkflowIDConflictPolicy: enums.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
			WorkflowExecutionTimeout: 30 * time.Minute,
		},
		vmscan.WorkflowName,
		in,
	)
	if err != nil {
		return nil, fmt.Errorf("execute workflow: %w", err)
	}

	var out vmscan.ScanOutput
	if err := run.Get(ctx, &out); err != nil {
		return nil, fmt.Errorf("workflow run failed: %w", err)
	}
	return &out, nil
}

// vmIsOlderThan returns true if the GCE creationTimestamp is older than minAge.
// Empty / unparseable timestamps are treated as old (fail-open) so we still
// flag VMs that GCE failed to populate the timestamp on rather than silently
// hiding them.
func vmIsOlderThan(creationTimestamp string, now time.Time, minAge time.Duration) bool {
	if creationTimestamp == "" {
		return true
	}
	t, err := time.Parse(time.RFC3339, creationTimestamp)
	if err != nil {
		return true
	}
	return now.Sub(t) >= minAge
}

func buildVMLeakRecord(vm vmscan.GCEInstanceItem, poolUUID string) model.LeakRecord {
	resourceID := vm.SelfLink
	if resourceID == "" {
		resourceID = fmt.Sprintf("projects/%s/zones/%s/instances/%s", vm.Project, vm.Zone, vm.Name)
	}

	region := vm.Zone
	if i := strings.LastIndex(vm.Zone, "-"); i > 0 {
		region = vm.Zone[:i]
	}

	extra := map[string]string{
		"pool_uuid":     poolUUID,
		"deployment_id": vm.Labels["deployment_id"],
		"machine_type":  vm.MachineType,
		"status":        vm.Status,
		"zone":          vm.Zone,
	}
	return model.LeakRecord{
		ResourceType: model.ResourceTypeVM,
		ResourceID:   resourceID,
		ResourceName: vm.Name,
		ProjectID:    vm.Project,
		Region:       region,
		Reason:       ReasonVMOrphanPoolMissing,
		Extra:        extra,
	}
}
