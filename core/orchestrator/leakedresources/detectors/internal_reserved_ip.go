package detectors

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/model"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	hyperscalerleakedresources "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/leakedresources"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var envGetInt = env.GetInt

const (
	// ReasonInternalReservedIPUnassignedCapacity: INTERNAL reserved address in a pool subnet, no users,
	// reserved longer than threshold (uses GCP creationTimestamp as age proxy — see detector doc).
	ReasonInternalReservedIPUnassignedCapacity = "internal_reserved_ip_unassigned_capacity"
)

// RegionalAddressLister lists regional addresses (mocked in tests).
type RegionalAddressLister interface {
	ListRegionalAddresses(ctx context.Context, projectID, region string) ([]hyperscalerleakedresources.RegionalAddress, error)
}

// InternalReservedIPDetector reports INTERNAL regional addresses that consume subnet capacity without assignment.
// Age uses the Compute API creationTimestamp (reservation exists ≥ threshold). That matches "never attached"
// staleness; detached-after-use is not distinguished without external state.
type InternalReservedIPDetector struct {
	lister            RegionalAddressLister
	minReservationAge time.Duration
}

// NewInternalReservedIPDetector builds a detector. minReservationAge ≤ 0 defaults to 6h.
func NewInternalReservedIPDetector(lister RegionalAddressLister, minReservationAge time.Duration) *InternalReservedIPDetector {
	if minReservationAge <= 0 {
		minReservationAge = 6 * time.Hour
	}
	return &InternalReservedIPDetector{lister: lister, minReservationAge: minReservationAge}
}

// DefaultInternalReservedIPMinAge returns LEAKED_RESOURCES_INTERNAL_IP_MIN_AGE_HOURS (default 6) as duration.
func DefaultInternalReservedIPMinAge() time.Duration {
	h := envGetInt("LEAKED_RESOURCES_INTERNAL_IP_MIN_AGE_HOURS", 6)
	if h < 1 {
		h = 6
	}
	return time.Duration(h) * time.Hour
}

// Name implements model.Detector.
func (d *InternalReservedIPDetector) Name() string {
	return "internal_reserved_ip"
}

// Detect implements model.Detector.
func (d *InternalReservedIPDetector) Detect(ctx context.Context, storage database.Storage) ([]model.LeakRecord, error) {
	logger := util.GetLogger(ctx)
	if d == nil || d.lister == nil {
		return nil, nil
	}
	logger.Infof("internal_reserved_ip detector started (min_reservation_age=%s)", d.minReservationAge.String())

	pools, err := storage.ListPools(ctx, nil)
	if err != nil {
		return nil, err
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

	cutoff := time.Now().UTC().Add(-d.minReservationAge)
	var records []model.LeakRecord

	for pr, snPools := range poolSetByProjRegionSubnet {
		addrs, err := d.lister.ListRegionalAddresses(ctx, pr.projectID, pr.region)
		if err != nil {
			logger.Warnf("internal_reserved_ip detector: list addresses failed project=%s region=%s: %v", pr.projectID, pr.region, err)
			continue
		}
		for _, a := range addrs {
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
				key = fmt.Sprintf("projects/%s/regions/%s/addresses/%s", pr.projectID, pr.region, a.Name)
			}
			poolUUIDList := make([]string, 0, len(poolUUIDsForSubnet))
			for u := range poolUUIDsForSubnet {
				poolUUIDList = append(poolUUIDList, u)
			}
			poolUUIDs := strings.Join(poolUUIDList, ",")
			records = append(records, model.LeakRecord{
				ResourceType: model.ResourceTypeInternalReservedIP,
				ResourceID:   key,
				ResourceName: a.Name,
				ProjectID:    pr.projectID,
				Region:       pr.region,
				Reason:       ReasonInternalReservedIPUnassignedCapacity,
				Extra: map[string]string{
					"ip":                  a.IP,
					"subnet":              base,
					"pool_uuids":          poolUUIDs,
					"creation_timestamp":  a.CreationTimestamp,
					"min_reservation_age": d.minReservationAge.String(),
					"age_basis":           "gcp_creation_timestamp",
				},
			})
		}
	}
	if len(records) == 0 {
		logger.Info("internal_reserved_ip detector: no leaked internal reserved IPs found")
	}

	return records, nil
}
