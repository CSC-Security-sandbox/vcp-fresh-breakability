package detectors

import (
	"context"
	"fmt"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/ipscan"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/model"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	hyperscalerleakedresources "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/leakedresources"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"sort"
	"strings"
	"time"
)

var envGetInt = env.GetInt

const (
	// ReasonInternalReservedIPUnassignedCapacity: INTERNAL reserved address in a pool subnet, no users,
	// reserved longer than threshold (uses GCP creationTimestamp as age proxy — see detector doc).
	ReasonInternalReservedIPUnassignedCapacity = "internal_reserved_ip_unassigned_capacity"

	internalIPDetectorWorkflowIDPrefix = "leaked-resources-scan-regional-addresses"
)

// InternalReservedIPDetector reports INTERNAL regional addresses that consume subnet capacity without assignment.
// Age uses the Compute API creationTimestamp (reservation exists ≥ threshold). That matches "never attached"
// staleness; detached-after-use is not distinguished without external state.
//
// The detector runs in the core pod but defers the actual GCE Compute API
// calls to a Temporal workflow on the worker pod (which is the only pod
// whose service account holds compute permissions in production). Same
// pattern as VMDetector and DiskDetector.
type InternalReservedIPDetector struct {
	// submitWorkflow is injectable for tests. In production it forwards to
	// workflowengine.FetchTemporalClient + ExecuteWorkflow + run.Get.
	submitWorkflow func(ctx context.Context, in ipscan.ScanInput) (*ipscan.ScanOutput, error)

	minReservationAge time.Duration
	taskQueue         string
}

// NewInternalReservedIPDetector builds a detector. minReservationAge ≤ 0 defaults to 6h.
func NewInternalReservedIPDetector(minReservationAge time.Duration) *InternalReservedIPDetector {
	if minReservationAge <= 0 {
		minReservationAge = 6 * time.Hour
	}
	d := &InternalReservedIPDetector{
		minReservationAge: minReservationAge,
		taskQueue:         workflowengine.BackgroundTaskQueue,
	}
	d.submitWorkflow = d.submitWorkflowViaTemporal
	return d
}

// DefaultInternalReservedIPMinAge returns LEAKED_RESOURCES_INTERNAL_IP_MIN_AGE_HOURS (default 6) as duration.
func DefaultInternalReservedIPMinAge() time.Duration {
	h := envGetInt("LEAKED_RESOURCES_INTERNAL_IP_MIN_AGE_HOURS", 6)
	if h < 1 {
		h = 6
	}
	return time.Duration(h) * time.Hour
}

func (d *InternalReservedIPDetector) Name() string {
	return "internal_reserved_ip"
}

// Detect implements model.Detector.
//
// Flow:
//  1. Build (project, region) → subnet → poolUUIDs map from active pools.
//  2. Submit ScanRegionalAddressesWorkflow with the unique (project, region) pairs.
//     The workflow runs the GCE Compute API calls on the worker pod and returns
//     the address listings grouped per pair.
//  3. Re-apply the original per-(project, region) policy loop locally on the
//     returned addresses, filtering by status / users / age / subnet.
func (d *InternalReservedIPDetector) Detect(ctx context.Context, storage database.Storage) ([]model.LeakRecord, error) {
	logger := util.GetLogger(ctx)
	if d == nil {
		return nil, nil
	}
	logger.Infof("internal_reserved_ip detector started (min_reservation_age=%s)", d.minReservationAge.String())

	pools, err := storage.ListPools(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("internal_reserved_ip detector: list pools: %w", err)
	}

	type projRegionKey struct {
		projectID string
		region    string
	}
	poolSetByProjRegionSubnet := make(map[projRegionKey]map[string]map[string]struct{})

	for _, p := range pools {
		if p == nil || p.PoolAttributes == nil || p.PoolAttributes.PrimaryZone == "" {
			continue
		}
		if p.State != models.LifeCycleStateREADY {
			continue
		}
		region, _, err := utils.ParseRegionAndZone(p.PoolAttributes.PrimaryZone)
		if err != nil || region == "" {
			logger.Debugf("internal_reserved_ip detector: skip pool %s primary_zone %q: %v", p.UUID, p.PoolAttributes.PrimaryZone, err)
			continue
		}
		cd := p.ClusterDetails
		if cd.RegionalTenantProject == "" || len(cd.SubnetNames) == 0 {
			continue
		}
		projects := []string{cd.RegionalTenantProject}
		if cd.SnHostProject != "" && cd.SnHostProject != cd.RegionalTenantProject {
			projects = append(projects, cd.SnHostProject)
		}
		for _, proj := range projects {
			k := projRegionKey{projectID: proj, region: region}
			if poolSetByProjRegionSubnet[k] == nil {
				poolSetByProjRegionSubnet[k] = make(map[string]map[string]struct{})
			}
			for _, sn := range cd.SubnetNames {
				if sn == "" {
					continue
				}
				if poolSetByProjRegionSubnet[k][sn] == nil {
					poolSetByProjRegionSubnet[k][sn] = make(map[string]struct{})
				}
				poolSetByProjRegionSubnet[k][sn][p.UUID] = struct{}{}
			}
		}
	}
	logger.Infof("internal_reserved_ip detector: checking %d project/region scope(s)", len(poolSetByProjRegionSubnet))
	if len(poolSetByProjRegionSubnet) == 0 {
		return nil, nil
	}

	targets := make([]ipscan.ProjectRegion, 0, len(poolSetByProjRegionSubnet))
	for k := range poolSetByProjRegionSubnet {
		targets = append(targets, ipscan.ProjectRegion{Project: k.projectID, Region: k.region})
	}
	sort.Slice(targets, func(i, j int) bool {
		if targets[i].Project != targets[j].Project {
			return targets[i].Project < targets[j].Project
		}
		return targets[i].Region < targets[j].Region
	})

	out, err := d.submitWorkflow(ctx, ipscan.ScanInput{Targets: targets})
	if err != nil {
		return nil, fmt.Errorf("internal_reserved_ip detector: scan workflow: %w", err)
	}
	if out == nil {
		logger.Warn("internal_reserved_ip detector: workflow returned nil output")
		return nil, nil
	}
	for _, f := range out.PartialFailures {
		logger.Warnf("internal_reserved_ip detector: project=%s region=%s partial failure: %s", f.Project, f.Region, f.Error)
	}

	cutoff := time.Now().UTC().Add(-d.minReservationAge)
	var records []model.LeakRecord

	for _, group := range out.Results {
		k := projRegionKey{projectID: group.Project, region: group.Region}
		snPools, ok := poolSetByProjRegionSubnet[k]
		if !ok {
			// Workflow returned a result for a (project, region) we didn't ask
			// about; defensive — skip silently.
			continue
		}
		for _, a := range group.Addresses {
			if !strings.EqualFold(strings.TrimSpace(a.AddressType), "INTERNAL") {
				continue
			}
			if !strings.EqualFold(strings.TrimSpace(a.Status), "RESERVED") {
				continue
			}
			if len(a.Users) > 0 {
				continue
			}
			// Reservation must be at least minReservationAge old (GCP creationTimestamp proxy).
			if !a.CreationTimeParsed || a.CreationTime.After(cutoff) {
				continue
			}
			base := hyperscalerleakedresources.SubnetworkBaseName(a.Subnetwork)
			if base == "" {
				continue
			}
			poolUUIDsForSubnet, ok := snPools[base]
			if !ok || len(poolUUIDsForSubnet) == 0 {
				continue
			}
			key := a.ResourceName
			if key == "" {
				key = fmt.Sprintf("projects/%s/regions/%s/addresses/%s", group.Project, group.Region, a.Name)
			}
			poolUUIDList := make([]string, 0, len(poolUUIDsForSubnet))
			for u := range poolUUIDsForSubnet {
				poolUUIDList = append(poolUUIDList, u)
			}
			sort.Strings(poolUUIDList) // deterministic for tests/logs
			records = append(records, model.LeakRecord{
				ResourceType: model.ResourceTypeInternalReservedIP,
				ResourceID:   key,
				ResourceName: a.Name,
				ProjectID:    group.Project,
				Region:       group.Region,
				Reason:       ReasonInternalReservedIPUnassignedCapacity,
				Extra: map[string]string{
					"ip":                  a.IP,
					"subnet":              base,
					"pool_uuids":          strings.Join(poolUUIDList, ","),
					"creation_timestamp":  a.CreationTimestamp,
					"min_reservation_age": d.minReservationAge.String(),
					"age_basis":           "gcp_creation_timestamp",
				},
			})
		}
	}
	if len(records) == 0 {
		logger.Info("internal_reserved_ip detector: no leaked internal reserved IPs found")
	} else {
		logger.Infof("internal_reserved_ip detector: found %d leaked internal reserved IP(s)", len(records))
	}

	return records, nil
}

func (d *InternalReservedIPDetector) submitWorkflowViaTemporal(ctx context.Context, in ipscan.ScanInput) (*ipscan.ScanOutput, error) {
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

	wfID := fmt.Sprintf("%s-%s", internalIPDetectorWorkflowIDPrefix, time.Now().UTC().Format("20060102T150405Z"))
	run, err := c.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:                d.taskQueue,
			ID:                       wfID,
			WorkflowIDConflictPolicy: enums.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
			WorkflowExecutionTimeout: 30 * time.Minute,
		},
		ipscan.WorkflowName,
		in,
	)
	if err != nil {
		return nil, fmt.Errorf("execute workflow: %w", err)
	}

	var out ipscan.ScanOutput
	if err := run.Get(ctx, &out); err != nil {
		return nil, fmt.Errorf("workflow run failed: %w", err)
	}
	return &out, nil
}
