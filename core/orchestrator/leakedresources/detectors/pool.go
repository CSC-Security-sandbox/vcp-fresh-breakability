// Package detectors implements resource-specific leak detection (pool, volume, snapshot, etc.).
package detectors

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/model"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// Leak reason constants for pool detector.
const (
	ReasonInCCFENotInVCP = "in_ccfe_not_in_vcp"
	ReasonInVCPNotInCCFE = "in_vcp_not_in_ccfe"
)

// CCFEPoolLister lists pool resource names from CCFE for a given project and location.
// Implemented by leakedresources/ccfe.Client; injectable for tests.
type CCFEPoolLister interface {
	ListStoragePools(ctx context.Context, projectID, location string) ([]string, error)
}

// PoolDetector detects pool leaks by comparing VCP and CCFE (CCFE vs VCP only).
type PoolDetector struct {
	ccfe CCFEPoolLister
}

// NewPoolDetector returns a detector that compares VCP pools with CCFE for each (project, location).
// Location is either a region (e.g. australia-southeast1) or a zone (e.g. australia-southeast1-a) so both regional and zonal pools are covered.
func NewPoolDetector(ccfe CCFEPoolLister) *PoolDetector {
	return &PoolDetector{ccfe: ccfe}
}

// Name implements model.Detector.
func (d *PoolDetector) Name() string {
	return "pool"
}

// Detect implements model.Detector. It lists pools from VCP, builds (projectID, location) pairs where
// location is the zone (for zonal pools) or region (for regional pools), and for each pair compares
// with CCFE to produce leak records (in_ccfe_not_in_vcp, in_vcp_not_in_ccfe). This covers both
// regional (locations/{region}/pools) and zonal (locations/{zone}/pools) lists.
func (d *PoolDetector) Detect(ctx context.Context, storage database.Storage) ([]model.LeakRecord, error) {
	logger := util.GetLogger(ctx)
	var records []model.LeakRecord

	// List all non-deleted pools from VCP (with Account preloaded for project ID).
	pools, err := storage.ListPools(ctx, nil)
	if err != nil {
		return nil, err
	}
	if len(pools) == 0 {
		return records, nil
	}

	// Group pools by (projectID, location). Location is zone when PrimaryZone is a zone (e.g. australia-southeast1-a),
	// or region when PrimaryZone is a region (e.g. australia-southeast1), so CCFE is queried with the same scope as the list API.
	type projectLocation struct {
		projectID string
		location  string
	}
	groups := make(map[projectLocation][]*datamodel.PoolView)
	for _, p := range pools {
		if p.Account == nil || p.PoolAttributes == nil || p.PoolAttributes.PrimaryZone == "" {
			continue
		}
		region, zone, err := utils.ParseRegionAndZone(p.PoolAttributes.PrimaryZone)
		if err != nil {
			logger.Debugf("pool %s: skip invalid primary_zone %q: %v", p.UUID, p.PoolAttributes.PrimaryZone, err)
			continue
		}
		// Use zone as location when this is a zonal pool (zone != ""); otherwise use region for regional pools.
		location := zone
		if location == "" {
			location = region
		}
		key := projectLocation{projectID: p.Account.Name, location: location}
		groups[key] = append(groups[key], p)
	}

	for key, vcpPools := range groups {
		// Build set of VCP pool resource names for this (project, location).
		vcpNames := make(map[string]*datamodel.PoolView)
		for _, p := range vcpPools {
			if p.Name != "" {
				vcpNames[p.Name] = p
			}
		}

		// Fetch CCFE pool list for this (project, location). Location is region or zone; CCFE list uses same path.
		// If CCFE is disabled (nil) or errors, skip comparison.
		ccfeNames, err := d.ccfe.ListStoragePools(ctx, key.projectID, key.location)
		if err != nil {
			logger.Warnf("pool detector: CCFE list failed for project=%s location=%s: %v", key.projectID, key.location, err)
			continue
		}
		if ccfeNames == nil {
			// CCFE disabled (e.g. GCP_HYDRATE_BASE_URL empty); skip this pair to avoid false in_vcp_not_in_ccfe.
			continue
		}
		ccfeSet := make(map[string]struct{})
		for _, n := range ccfeNames {
			ccfeSet[n] = struct{}{}
		}

		// In CCFE but not in VCP.
		for _, n := range ccfeNames {
			if _, inVCP := vcpNames[n]; !inVCP {
				records = append(records, model.LeakRecord{
					ResourceType: model.ResourceTypePool,
					ResourceID:   n,
					ResourceName: n,
					ProjectID:    key.projectID,
					Region:       key.location,
					Reason:       ReasonInCCFENotInVCP,
				})
			}
		}
		// In VCP but not in CCFE.
		for name, p := range vcpNames {
			if _, inCCFE := ccfeSet[name]; !inCCFE {
				records = append(records, model.LeakRecord{
					ResourceType: model.ResourceTypePool,
					ResourceID:   p.UUID,
					ResourceName: p.Name,
					ProjectID:    key.projectID,
					Region:       key.location,
					Reason:       ReasonInVCPNotInCCFE,
					Extra:        map[string]string{"uuid": p.UUID},
				})
			}
		}
	}

	return records, nil
}
