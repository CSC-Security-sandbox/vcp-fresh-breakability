// Package detectors implements resource-specific leak detection (pool, volume, snapshot, etc.).
// This file implements snapshot orphan detection: snapshots whose volume is missing or deleted.

package detectors

import (
	"context"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/model"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
)

// Snapshot leak reason for orphan (volume missing).
const ReasonSnapshotOrphanVolumeMissing = "snapshot_orphan_volume_missing"

// SnapshotOrphanDetector detects snapshots that reference a missing or deleted volume (orphans).
type SnapshotOrphanDetector struct{}

// NewSnapshotOrphanDetector returns a detector that finds snapshots whose volume_id is not in the set of valid (non-deleted) volumes.
func NewSnapshotOrphanDetector() *SnapshotOrphanDetector {
	return &SnapshotOrphanDetector{}
}

// Name implements model.Detector.
func (d *SnapshotOrphanDetector) Name() string {
	return "snapshot_orphan"
}

// Detect implements model.Detector. It lists non-deleted volumes to build a set of valid volume IDs,
// then lists non-deleted snapshots and reports any snapshot whose volume_id is not in that set.
// GetSnapshotsWithCondition only preloads Volume (not Account), so we build an accountID→name map
// once per run to fill ProjectID in the report.
//
// Database load: This performs three full table scans (accounts, volumes, snapshots). We cannot
// add filters that reduce scope without losing correctness: we need all volume IDs to detect
// orphans, and all snapshots to check each one. The accounts scan could be reduced to only
// account IDs that appear in orphan snapshots if the storage layer supported GetAccountsByIDs
// or GetAccounts with an id IN (...) filter; today GetAccounts has no such filter.
func (d *SnapshotOrphanDetector) Detect(ctx context.Context, storage database.Storage) ([]model.LeakRecord, error) {
	var records []model.LeakRecord

	// Build accountID → account name map so we can set ProjectID (snapshots don't preload Account).
	accountIDToName, err := d.accountIDToNameMap(ctx, storage)
	if err != nil {
		return nil, fmt.Errorf("list accounts: %w", err)
	}

	// Build set of valid (non-deleted) volume IDs.
	volumes, err := storage.ListVolumes(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("list volumes: %w", err)
	}
	validVolumeIDs := make(map[int64]struct{}, len(volumes))
	for _, v := range volumes {
		validVolumeIDs[v.ID] = struct{}{}
	}

	// List all non-deleted snapshots (empty filter = no conditions; GORM excludes soft-deleted by default).
	emptyFilter := dbutils.CreateFilterWithConditions()
	snapshots, err := storage.GetSnapshotsWithCondition(ctx, *emptyFilter)
	if err != nil {
		return nil, fmt.Errorf("list snapshots: %w", err)
	}

	for _, snap := range snapshots {
		if _, ok := validVolumeIDs[snap.VolumeID]; ok {
			continue
		}
		// Snapshot's volume is missing or deleted -> orphan.
		projectID := accountIDToName[snap.AccountID]
		extra := map[string]string{
			"volume_id":  fmt.Sprintf("%d", snap.VolumeID),
			"account_id": fmt.Sprintf("%d", snap.AccountID),
		}
		records = append(records, model.LeakRecord{
			ResourceType: model.ResourceTypeSnapshot,
			ResourceID:   snap.UUID,
			ResourceName: snap.Name,
			ProjectID:    projectID,
			Reason:       ReasonSnapshotOrphanVolumeMissing,
			Extra:        extra,
		})
	}

	return records, nil
}

// accountIDToNameMap returns a map of account ID to account name for non-deleted accounts.
// Used to set ProjectID in leak records when the snapshot query does not preload Account.
func (d *SnapshotOrphanDetector) accountIDToNameMap(ctx context.Context, storage database.Storage) (map[int64]string, error) {
	accounts, err := storage.GetAccounts(ctx, false, nil)
	if err != nil {
		return nil, err
	}
	m := make(map[int64]string, len(accounts))
	for _, a := range accounts {
		m[a.ID] = a.Name
	}
	return m, nil
}
