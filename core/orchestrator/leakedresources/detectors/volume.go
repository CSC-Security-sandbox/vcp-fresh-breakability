// Package detectors implements resource-specific leak detection (pool, volume, snapshot, etc.).
// This file implements volume orphan detection: volumes whose pool is missing or deleted.

package detectors

import (
	"context"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/model"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
)

// Volume leak reason for orphan (pool missing).
const ReasonVolumeOrphanPoolMissing = "volume_orphan_pool_missing"

// VolumeOrphanDetector detects volumes that reference a missing or deleted pool (orphans).
type VolumeOrphanDetector struct{}

// NewVolumeOrphanDetector returns a detector that finds volumes whose pool_id is not in the set of valid (non-deleted) pools.
func NewVolumeOrphanDetector() *VolumeOrphanDetector {
	return &VolumeOrphanDetector{}
}

// Name implements model.Detector.
func (d *VolumeOrphanDetector) Name() string {
	return "volume_orphan"
}

// Detect implements model.Detector. It lists non-deleted pools to build a set of valid pool IDs,
// then lists non-deleted volumes and reports any volume whose pool_id is not in that set.
func (d *VolumeOrphanDetector) Detect(ctx context.Context, storage database.Storage) ([]model.LeakRecord, error) {
	var records []model.LeakRecord

	// Build set of valid (non-deleted) pool IDs.
	pools, err := storage.ListPools(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("list pools: %w", err)
	}
	validPoolIDs := make(map[int64]struct{}, len(pools))
	for _, p := range pools {
		validPoolIDs[p.ID] = struct{}{}
	}

	// List all non-deleted volumes (nil conditions = no filter; GORM excludes soft-deleted by default).
	volumes, err := storage.ListVolumes(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("list volumes: %w", err)
	}

	for _, vol := range volumes {
		if _, ok := validPoolIDs[vol.PoolID]; ok {
			continue
		}
		// Volume's pool is missing or deleted -> orphan.
		projectID := ""
		if vol.Account != nil {
			projectID = vol.Account.Name
		}
		extra := map[string]string{"pool_id": fmt.Sprintf("%d", vol.PoolID)}
		records = append(records, model.LeakRecord{
			ResourceType: model.ResourceTypeVolume,
			ResourceID:   vol.UUID,
			ResourceName: vol.Name,
			ProjectID:    projectID,
			Reason:       ReasonVolumeOrphanPoolMissing,
			Extra:        extra,
		})
	}

	return records, nil
}
