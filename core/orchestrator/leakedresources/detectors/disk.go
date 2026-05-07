package detectors

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/diskscan"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/model"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

const (
	ReasonDiskOrphanPoolMissing = "disk_orphan_pool_missing"

	diskDetectorWorkflowIDPrefix = "leaked-resources-scan-gce-disks"

	// defaultDiskLabelKey is the GCE label whose value identifies the owning
	// pool. VLM stamps this label on every disk it creates. Override via
	// LEAKED_RESOURCES_DISK_LABEL_KEY when the convention changes.
	defaultDiskLabelKey = "pool_uuid"

	// defaultDiskMinAgeHours is the minimum disk age before it is considered
	// for leak reporting. Newly created disks may temporarily lack a matching
	// pool row (create flow in flight), so we wait until they have settled.
	defaultDiskMinAgeHours = 6
)

// DiskDetector reports GCE disks whose pool_uuid label does not match any
// non-deleted Pool in VCP. It runs in the core pod but defers the actual
// GCE Compute API calls to a Temporal workflow on the worker pod (which is
// the only pod whose service account holds compute permissions).
//
// Pipeline contract: implements model.Detector. Returns ([]model.LeakRecord, error).
// Soft-failure semantics: when the Temporal client is unavailable or the
// workflow itself fails, Detect logs and returns (nil, err) so the pipeline
// records a per-detector failure without crashing the cron run.
type DiskDetector struct {
	// submitWorkflow is injectable for tests. In production it forwards to
	// workflowengine.FetchTemporalClient + ExecuteWorkflow + run.Get.
	submitWorkflow func(ctx context.Context, in diskscan.ScanInput) (*diskscan.ScanOutput, error)

	labelKey   string
	minDiskAge time.Duration
	taskQueue  string
}

// NewDiskDetector returns a detector wired to the production Temporal path.
// Honours these env vars:
//   - LEAKED_RESOURCES_DISK_LABEL_KEY (default "pool_uuid")
//   - LEAKED_RESOURCES_DISK_MIN_AGE_HOURS (default 6)
func NewDiskDetector() *DiskDetector {
	d := &DiskDetector{
		labelKey:   env.GetString("LEAKED_RESOURCES_DISK_LABEL_KEY", defaultDiskLabelKey),
		minDiskAge: defaultDiskMinAge(),
		taskQueue:  workflowengine.BackgroundTaskQueue,
	}
	d.submitWorkflow = d.submitWorkflowViaTemporal
	return d
}

func defaultDiskMinAge() time.Duration {
	h := envGetInt("LEAKED_RESOURCES_DISK_MIN_AGE_HOURS", defaultDiskMinAgeHours)
	if h < 1 {
		h = defaultDiskMinAgeHours
	}
	return time.Duration(h) * time.Hour
}

func (d *DiskDetector) Name() string {
	return "disk_orphan"
}

// Detect finds orphaned GCP Compute disks in regional tenant projects.
// Algorithm:
//  1. Build activePoolUUIDs from non-deleted pools.
//  2. Collect all RTPs (including from soft-deleted pools).
//  3. Submit ScanGCEDisksWorkflow to the worker (synchronous via run.Get).
//  4. For each returned disk: skip if pool_uuid is in activePoolUUIDs;
//     otherwise emit a LeakRecord.
func (d *DiskDetector) Detect(ctx context.Context, storage database.Storage) ([]model.LeakRecord, error) {
	logger := util.GetLogger(ctx)
	if d == nil {
		return nil, nil
	}

	rtpProjects, err := storage.ListAllTpProjects(ctx)
	if err != nil {
		return nil, fmt.Errorf("disk detector: list all tp projects: %w", err)
	}
	if len(rtpProjects) == 0 {
		logger.Info("disk_orphan detector: no tenant projects found, skipping scan")
		return nil, nil
	}

	pools, err := storage.ListPools(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("disk detector: list pools: %w", err)
	}
	activePoolUUIDs := make(map[string]struct{}, len(pools))
	for _, p := range pools {
		if p != nil && p.UUID != "" {
			activePoolUUIDs[p.UUID] = struct{}{}
		}
	}

	sort.Strings(rtpProjects)
	logger.Infof("disk_orphan detector started: rtp_projects=%d active_pools=%d label_key=%q min_disk_age=%s",
		len(rtpProjects), len(activePoolUUIDs), d.labelKey, d.minDiskAge.String())

	out, err := d.submitWorkflow(ctx, diskscan.ScanInput{
		ProjectIDs: rtpProjects,
	})
	if err != nil {
		return nil, fmt.Errorf("disk detector: scan workflow: %w", err)
	}
	if out == nil {
		logger.Warn("disk_orphan detector: workflow returned nil output")
		return nil, nil
	}
	if len(out.PartialFailures) > 0 {
		for _, f := range out.PartialFailures {
			logger.Warnf("disk_orphan detector: project %s partial failure: %s", f.Project, f.Error)
		}
	}

	now := time.Now().UTC()
	var records []model.LeakRecord
	for _, disk := range out.Items {
		poolUUID := disk.Labels[d.labelKey]

		if poolUUID == "" {
			if !diskIsOlderThan(disk.CreationTimestamp, now, d.minDiskAge) {
				continue
			}
			records = append(records, buildDiskLeakRecord(disk, poolUUID))
			continue
		}
		if _, active := activePoolUUIDs[poolUUID]; !active {
			if !diskIsOlderThan(disk.CreationTimestamp, now, d.minDiskAge) {
				continue
			}
			records = append(records, buildDiskLeakRecord(disk, poolUUID))
		}
	}

	if len(records) == 0 {
		logger.Info("disk_orphan detector: no leaked disks found")
	} else {
		logger.Infof("disk_orphan detector: found %d leaked disk(s)", len(records))
	}

	return records, nil
}

func (d *DiskDetector) submitWorkflowViaTemporal(ctx context.Context, in diskscan.ScanInput) (*diskscan.ScanOutput, error) {
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

	wfID := fmt.Sprintf("%s-%s", diskDetectorWorkflowIDPrefix, time.Now().UTC().Format("20060102T150405Z"))
	run, err := c.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:                d.taskQueue,
			ID:                       wfID,
			WorkflowIDConflictPolicy: enums.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
			WorkflowExecutionTimeout: 30 * time.Minute,
		},
		diskscan.WorkflowName,
		in,
	)
	if err != nil {
		return nil, fmt.Errorf("execute workflow: %w", err)
	}

	var out diskscan.ScanOutput
	if err := run.Get(ctx, &out); err != nil {
		return nil, fmt.Errorf("workflow run failed: %w", err)
	}
	return &out, nil
}

// diskIsOlderThan returns true if the GCE creationTimestamp is older than minAge.
// Empty / unparseable timestamps are treated as old (fail-open) so we still
// flag disks that GCE failed to populate the timestamp on rather than silently
// hiding them.
func diskIsOlderThan(creationTimestamp string, now time.Time, minAge time.Duration) bool {
	if creationTimestamp == "" {
		return true
	}
	t, err := time.Parse(time.RFC3339, creationTimestamp)
	if err != nil {
		return true
	}
	return now.Sub(t) >= minAge
}

func buildDiskLeakRecord(disk diskscan.GCEDiskItem, poolUUID string) model.LeakRecord {
	resourceID := disk.SelfLink
	if resourceID == "" {
		resourceID = fmt.Sprintf("projects/%s/zones/%s/disks/%s", disk.Project, disk.Zone, disk.Name)
	}

	region := disk.Zone
	if i := strings.LastIndex(disk.Zone, "-"); i > 0 {
		region = disk.Zone[:i]
	}

	extra := map[string]string{
		"pool_uuid":     poolUUID,
		"deployment_id": disk.Labels["deployment_id"],
		"disk_type":     disk.Type,
		"size_gb":       fmt.Sprintf("%d", disk.SizeGB),
		"zone":          disk.Zone,
	}
	return model.LeakRecord{
		ResourceType: model.ResourceTypeDisk,
		ResourceID:   resourceID,
		ResourceName: disk.Name,
		ProjectID:    disk.Project,
		Region:       region,
		Reason:       ReasonDiskOrphanPoolMissing,
		Extra:        extra,
	}
}
