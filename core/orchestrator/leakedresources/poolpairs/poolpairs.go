// Package poolpairs derives canonical (projectID, location) pairs from VCP
// pools for use by the leaked-resources pool detector. Living in its own
// package keeps the helper importable from both the detector and the
// background workflow (FetchStoragePoolsWorkflow consumes CachedPool from
// here) without dragging in the whole pipeline.
package poolpairs

import (
	"context"
	"errors"
	"sort"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// ZoneFetcher is the abstraction the pool detector uses to learn which
// zones live in a region. The production implementation lives in the
// detectors package and synchronously runs GetRegionZonesWorkflow on the
// background task queue, so the underlying GCP compute.zones.list call
// stays on the worker. Tests substitute an in-process fake.
//
// A nil slice with no error means "no zones available" (for example when
// SECRET_MANAGER_PROJECT_ID is unset); the enumerator then degrades to
// region-only keys instead of failing the whole detector run.
type ZoneFetcher interface {
	GetRegionZones(ctx context.Context, region string) ([]string, error)
}

// PoolProjectLocation identifies a single (projectID, location) pair.
// Location is the region for regional HA pools and the zone for zonal
// pools (falling back to the region when the zone parses empty). The same
// pair can be expressed as a single string via Key() so it can be used as
// a stable map key.
type PoolProjectLocation struct {
	ProjectID string
	Location  string
}

// CachedPool is the per-pool record returned by FetchStoragePoolsWorkflow
// for a single (project, location). The same struct is used by the pool
// detector when diffing live CCFE results against VCP DB so the two stay
// in lockstep on field shape and JSON tags.
//
// UUID is the comparison key. CCFE returns it as "netappUuid", which mirrors
// the VCP Pool.UUID assigned at create time, so the diff is unambiguous even
// when a pool is renamed or recreated under the same name.
//
// Name is kept alongside UUID purely for human-readable leak records and
// operational logs; it is not part of the diff.
type CachedPool struct {
	UUID string `json:"uuid"`
	Name string `json:"name,omitempty"`
}

// Key returns the canonical "<projectID>/<location>" representation. It is
// kept as a stable string form so logs and metrics emitted by the detector
// and the workflow read the same way.
func (p PoolProjectLocation) Key() string {
	return p.ProjectID + "/" + p.Location
}

// GroupPoolsByProjectLocation buckets the given VCP pools by (projectID,
// location): region for IsRegionalHA pools, zone for zonal pools, region as
// fallback when the zone parses empty. Pools missing an Account,
// PoolAttributes or PrimaryZone are skipped (and logged at debug level), as
// are pools whose PrimaryZone fails to parse.
func GroupPoolsByProjectLocation(ctx context.Context, pools []*datamodel.PoolView) map[PoolProjectLocation][]*datamodel.PoolView {
	logger := util.GetLogger(ctx)
	groups := make(map[PoolProjectLocation][]*datamodel.PoolView)
	for _, p := range pools {
		if p.Account == nil || p.PoolAttributes == nil || p.PoolAttributes.PrimaryZone == "" {
			continue
		}
		region, zone, err := utils.ParseRegionAndZone(p.PoolAttributes.PrimaryZone)
		if err != nil {
			logger.Debugf("pool %s: skip invalid primary_zone %q: %v", p.UUID, p.PoolAttributes.PrimaryZone, err)
			continue
		}
		var location string
		if p.PoolAttributes.IsRegionalHA {
			location = region
		} else {
			location = zone
			if location == "" {
				location = region
			}
		}
		if location == "" {
			continue
		}
		key := PoolProjectLocation{ProjectID: p.Account.Name, Location: location}
		groups[key] = append(groups[key], p)
	}
	return groups
}

// EnumerateProjectLocationKeys returns the (projectID, location) pairs the
// leaked-resources pool detector should fetch CCFE pools for. For each
// active VCP project (sourced from the accounts table) it emits:
//
//   - one pair for the LOCAL_REGION (covers regional-HA pools), and
//   - one pair per zone in LOCAL_REGION returned by zoneFetcher (covers
//     zonal pools in every zone, including ones where VCP currently has no
//     pool, so "in_ccfe_not_in_vcp" leaks still surface there).
//
// CCFE accepts either region or zone in the location segment of its list
// path, so this gives the diff full coverage of both regional and zonal
// pools without scanning the pools table for keys.
//
// Returns an error when LOCAL_REGION is unset (the detector then logs the
// failure and emits no records — same observable behaviour as the previous
// scheduled-cache implementation, which short-circuited and left the cache
// empty). A zoneFetcher failure is tolerated: the per-project region key
// is still emitted and the enumerator logs a warning.
func EnumerateProjectLocationKeys(ctx context.Context, storage database.Storage, zoneFetcher ZoneFetcher) ([]PoolProjectLocation, error) {
	logger := util.GetLogger(ctx)

	region := env.Region
	if region == "" {
		return nil, errors.New("LOCAL_REGION is not set; cannot enumerate (project, location) pairs for leaked-resources fetch")
	}
	if zoneFetcher == nil {
		return nil, errors.New("zoneFetcher is nil; cannot enumerate zones for leaked-resources fetch")
	}

	accounts, err := storage.ListAccountsForTelemetry(ctx, nil)
	if err != nil {
		return nil, err
	}

	// Skip the GetRegionZones call entirely when there is nothing to enumerate
	// against. Saves one workflow per detector tick on a freshly bootstrapped
	// shard with zero accounts.
	hasAccount := false
	for _, a := range accounts {
		if a != nil && a.Name != "" {
			hasAccount = true
			break
		}
	}
	var zones []string
	if hasAccount {
		fetched, fetchErr := zoneFetcher.GetRegionZones(ctx, region)
		if fetchErr != nil {
			logger.Warnf("poolpairs.EnumerateProjectLocationKeys: GetRegionZones failed region=%s: %v; falling back to region-only keys",
				region, fetchErr)
		} else {
			zones = fetched
		}
		sort.Strings(zones)
	}

	seen := make(map[PoolProjectLocation]struct{}, len(accounts)*(1+len(zones)))
	pairs := make([]PoolProjectLocation, 0, len(accounts)*(1+len(zones)))
	add := func(projectNumber, location string) {
		key := PoolProjectLocation{ProjectID: projectNumber, Location: location}
		if _, dup := seen[key]; dup {
			return
		}
		seen[key] = struct{}{}
		pairs = append(pairs, key)
	}
	for _, a := range accounts {
		if a == nil || a.Name == "" {
			continue
		}
		add(a.Name, region)
		for _, z := range zones {
			if z == "" {
				continue
			}
			add(a.Name, z)
		}
	}
	logger.Infof("poolpairs.EnumerateProjectLocationKeys: built %d pair(s) from %d account(s), region=%s, zones=%d",
		len(pairs), len(accounts), region, len(zones))
	return pairs, nil
}
