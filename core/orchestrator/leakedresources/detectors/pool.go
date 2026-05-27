// Package detectors implements resource-specific leak detection (pool, volume, snapshot, etc.).
package detectors

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/model"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/resourcescope"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// Leak reason constants for pool detector.
const (
	ReasonInCCFENotInVCP = "in_ccfe_not_in_vcp"
	ReasonInVCPNotInCCFE = "in_vcp_not_in_ccfe"
)

// CCFEPoolFetcher returns the live CCFE pool snapshot for one project
// across multiple locations in a single call. Production wires this to a
// Temporal client that synchronously executes FetchCCFEPoolsWorkflow on
// the background task queue (which fans the per-location CCFE list calls
// out as parallel activities); tests substitute an in-process fake.
//
// The returned map's keys are the locations the workflow successfully
// fetched. A missing key signals "fetch failed for this location after
// retries" — the detector treats that the same way it treats a
// (per-location) nil value, i.e. skip the diff for that pair so a
// transient CCFE outage cannot false-flag every VCP pool as a leak.
type CCFEPoolFetcher interface {
	FetchCCFEPools(ctx context.Context, projectID string, locations []string) (map[string][]resourcescope.CachedPool, error)
}

// ProjectLocationLister returns the (projectID, location) pairs the
// detector should fetch CCFE pools for on each tick. Production wires this
// to resourcescope.EnumerateProjectLocationKeys, which produces (account ×
// (region + zones)) pairs so every zone in LOCAL_REGION is diffed (even
// zones where VCP currently has no pools — those still surface
// in_ccfe_not_in_vcp leaks against pools that exist only in CCFE).
type ProjectLocationLister interface {
	ListProjectLocations(ctx context.Context) ([]resourcescope.ProjectLocation, error)
}

// PoolDetector detects pool leaks by diffing VCP pools against the live
// CCFE snapshot fetched per project (one workflow per VCP account, fanning
// per-location CCFE calls out as parallel activities). Pairs are
// enumerated from the accounts table cross-joined with the zones reported
// for LOCAL_REGION, so the diff is symmetric: VCP pools missing from CCFE
// show up as in_vcp_not_in_ccfe, and CCFE pools missing from VCP show up
// as in_ccfe_not_in_vcp — even in zones where VCP has no rows yet.
//
// CCFE is read on every detector tick (no DB cache), so the diff is always
// against up-to-date data.
type PoolDetector struct {
	fetcher CCFEPoolFetcher
	lister  ProjectLocationLister
}

// NewPoolDetector returns a detector that compares VCP pools against the
// CCFE snapshot fetched live per project across the (projectID, location)
// pairs enumerated by the given ProjectLocationLister.
func NewPoolDetector(fetcher CCFEPoolFetcher, lister ProjectLocationLister) *PoolDetector {
	return &PoolDetector{fetcher: fetcher, lister: lister}
}

// Name implements model.Detector.
func (d *PoolDetector) Name() string {
	return "pool"
}

// Detect implements model.Detector. It enumerates (projectID, location)
// pairs from the configured ProjectLocationLister, groups them by
// projectID, and fires one FetchCCFEPoolsWorkflow per project. The
// workflow fans the per-location CCFE list calls out as parallel
// activities and returns a map of location → pools. The detector then
// diffs each location against the VCP-side groups produced by
// resourcescope.GroupPoolsByProjectLocation and emits in_ccfe_not_in_vcp /
// in_vcp_not_in_ccfe records.
//
// A nil/missing entry for a location causes that pair to be skipped so a
// transient CCFE outage cannot false-flag every VCP pool as a leak. Any
// error from ProjectLocationLister surfaces as a detector error so the
// pipeline can log it once and move on to the next detector.
func (d *PoolDetector) Detect(ctx context.Context, storage database.Storage) ([]model.LeakRecord, error) {
	logger := util.GetLogger(ctx)
	var records []model.LeakRecord

	// Selective preload: we only need Account.Name, PoolAttributes, Name and
	// UUID below, so skip the expensive KmsConfig / ActiveDirectory
	// preloads that storage.ListPools performs unconditionally. This matters
	// because the detector runs on every leaked-resources tick.
	pools, err := storage.ListPoolsSelective(ctx, nil, database.PoolPreloadOptions{})
	if err != nil {
		return nil, err
	}
	groups := resourcescope.GroupPoolsByProjectLocation(ctx, pools)

	pairs, err := d.lister.ListProjectLocations(ctx)
	if err != nil {
		return nil, err
	}
	if len(pairs) == 0 {
		return records, nil
	}

	// Group enumerated pairs by project so we can fire one workflow per
	// account instead of one per (project, location). Within each project
	// the workflow fans the locations out to parallel activities.
	locationsByProject := make(map[string][]string)
	projectOrder := make([]string, 0)
	for _, pair := range pairs {
		if _, seen := locationsByProject[pair.ProjectID]; !seen {
			projectOrder = append(projectOrder, pair.ProjectID)
		}
		locationsByProject[pair.ProjectID] = append(locationsByProject[pair.ProjectID], pair.Location)
	}

	failedProjects := 0
	skippedLocations := 0
	totalProjects := len(projectOrder)
	lastDecile := 0
	for i, projectID := range projectOrder {
		locations := locationsByProject[projectID]

		poolsByLocation, err := d.fetcher.FetchCCFEPools(ctx, projectID, locations)
		if err != nil {
			logger.Warnf("pool detector: FetchCCFEPools failed project=%s: %v", projectID, err)
			failedProjects++
		} else {
			for _, location := range locations {
				key := resourcescope.ProjectLocation{ProjectID: projectID, Location: location}

				ccfePools, present := poolsByLocation[location]
				if !present || ccfePools == nil {
					skippedLocations++
					continue
				}

				// Build the VCP-side comparison set keyed on Pool.UUID — names
				// can be reused across recreations, UUIDs cannot, so UUID is
				// the only stable identity for the diff. Pools without a UUID
				// are dropped defensively (legacy rows shouldn't exist given
				// the unique index, but we don't want a stray "" to collide
				// with a CCFE entry that also happened to have an empty
				// netappUuid).
				vcpPools := groups[key]
				vcpByUUID := make(map[string]*datamodel.PoolView, len(vcpPools))
				for _, p := range vcpPools {
					if p.UUID != "" {
						vcpByUUID[p.UUID] = p
					}
				}
				ccfeByUUID := make(map[string]resourcescope.CachedPool, len(ccfePools))
				for _, cp := range ccfePools {
					if cp.UUID == "" {
						continue
					}
					ccfeByUUID[cp.UUID] = cp
				}

				// In CCFE but not in VCP — UUID is the leaked identifier; carry
				// the CCFE-side resource name into the record so operators can
				// see a human-readable handle without having to cross-reference
				// the UUID.
				for uuid, cp := range ccfeByUUID {
					if _, inVCP := vcpByUUID[uuid]; !inVCP {
						records = append(records, model.LeakRecord{
							ResourceType: model.ResourceTypePool,
							ResourceID:   uuid,
							ResourceName: cp.Name,
							ProjectID:    projectID,
							Region:       location,
							Reason:       ReasonInCCFENotInVCP,
							Extra:        map[string]string{"uuid": uuid},
						})
					}
				}
				// In VCP but not in CCFE.
				for uuid, p := range vcpByUUID {
					if _, inCCFE := ccfeByUUID[uuid]; !inCCFE {
						records = append(records, model.LeakRecord{
							ResourceType: model.ResourceTypePool,
							ResourceID:   uuid,
							ResourceName: p.Name,
							ProjectID:    projectID,
							Region:       location,
							Reason:       ReasonInVCPNotInCCFE,
							Extra:        map[string]string{"uuid": uuid},
						})
					}
				}
			}
		}

		// Progress logging at 10% gaps. Each decile boundary is reported at
		// most once; when totalProjects is small, multiple deciles can be
		// crossed in one iteration and we only log the highest one reached.
		done := i + 1
		decile := (done * 10) / totalProjects
		if decile > lastDecile {
			logger.Infof("pool detector: progress %d%% (accounts_done=%d accounts_left=%d)",
				decile*10, done, totalProjects-done)
			lastDecile = decile
		}
	}

	logger.Infof("pool detector: completed vcp_rows=%d pairs=%d projects=%d failed_projects=%d skipped_locations=%d leaks=%d",
		len(pools), len(pairs), len(projectOrder), failedProjects, skippedLocations, len(records))

	return records, nil
}
