package backgroundactivities

import (
	"context"
	"regexp"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/hydrationActivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/repository"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"gorm.io/gorm"
)

const (
	SnapshotTypeScheduled       = "scheduled"
	SnapshotTypeAdHoc           = "ad-hoc"
	SnapshotTypeBackupScheduled = "backup-scheduled"

	FlexGroupConstituent = "flexgroup_constituent"
)

var (
	snapshotSyncChunkSize = env.GetInt("CVS_SNAPSHOT_SYNC_CHUNK_SIZE", 200)
	hydrationEnabled      = env.GetBool("GCP_HYDRATE_ENABLED", true)

	scheduledRegExp  = regexp.MustCompile(`^(hourly|daily|weekly|monthly)\..*$`)
	snapmirrorRegExp = regexp.MustCompile(`^snapmirror\.[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}_.*$`)

	GetOntapRestProviderForPool           = _getOntapRestProviderForPool
	filterOntapVolumesAndSnapshots        = _filterOntapVolumesAndSnapshots
	processSnapshotSync                   = _processSnapshotSync
	syncDeletedSnapshotsToDatabase        = _syncDeletedSnapshotsToDatabase
	syncNewSnapshotsToDatabase            = _syncNewSnapshotsToDatabase
	syncUpdatedSnapshotsToDatabase        = _syncUpdatedSnapshotsToDatabase
	syncWronglyDeletedSnapshotsToDatabase = _syncWronglyDeletedSnapshotsToDatabase
	hydrateBatchSnapshotsToCCFE           = hydrationActivities.HydrateBatchSnapshotstoCCFE
)

type SyncSnapshotActivity struct {
	SE database.Storage
}

func (a *SyncSnapshotActivity) ListPools(ctx context.Context) ([]*datamodel.Pool, error) {
	logger := util.GetLogger(ctx)
	se := a.SE

	conditions := [][]interface{}{{"state = ?", models.LifeCycleStateREADY}}
	poolViews, err := se.ListPools(ctx, conditions)
	if err != nil {
		logger.Errorf("Failed to list pools: %v", err)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}

	var pools []*datamodel.Pool
	for _, poolView := range poolViews {
		pools = append(pools, repository.ConvertPoolViewToPool(poolView))
	}
	return pools, nil
}

func (a *SyncSnapshotActivity) SynchronizeSnapshots(ctx context.Context, pools []*datamodel.Pool) error {
	logger := util.GetLogger(ctx)
	se := a.SE

	for _, pool := range pools {
		provider, err := GetOntapRestProviderForPool(ctx, se, pool)
		if err != nil {
			logger.Errorf("Failed to get ONTAP rest provider: %v", err)
			return err
		}

		ontapVolumes, err := provider.GetVolumes()
		if err != nil {
			logger.Errorf("Failed to get ONTAP volumes for the pool: %s, %v", pool.UUID, err)
			return err
		}

		var ontapSnapshots []*vsa.Snapshot
		for _, volume := range ontapVolumes {
			volumeSnapshots, err := provider.GetSnapshots(*volume.UUID)
			if err != nil {
				logger.Errorf("Failed to get snapshots from ONTAP for volume %s: %v", *volume.UUID, err)
				return err
			}
			ontapSnapshots = append(ontapSnapshots, volumeSnapshots...)
		}

		ontapVolumeMap, ontapSnapshots := filterOntapVolumesAndSnapshots(ontapVolumes, ontapSnapshots)

		dbVolumes, err := se.GetVolumesByPoolID(ctx, pool.ID)
		if err != nil {
			logger.Errorf("Failed to get volumes from database for pool %s: %v", pool.UUID, err)
			return err
		}

		var (
			dbVolumeIDs []int64
			dbVolumeMap = make(map[string]*datamodel.Volume, len(dbVolumes))
		)
		for _, v := range dbVolumes {
			dbVolumeIDs = append(dbVolumeIDs, v.ID)
			dbVolumeMap[v.VolumeAttributes.ExternalUUID] = v
		}

		dbSnapshots, err := se.GetSnapshotsByVolumeIDs(ctx, dbVolumeIDs)
		if err != nil {
			logger.Errorf("Failed to get snapshots from database for pool %s: %v", pool.UUID, err)
			return err
		}

		var dbSnapshotMap = make(map[string]*datamodel.Snapshot, len(dbSnapshots))
		for _, dbSnapshot := range dbSnapshots {
			dbSnapshotMap[dbSnapshot.SnapshotAttributes.ExternalUUID] = dbSnapshot
		}

		newSSMap, updatedSSMap, wronglyDeletedSnapshotsMap, newIds, updatedIDs, deleteIDs, wronglyDeletedIds :=
			processSnapshotSync(ctx, ontapVolumeMap, ontapSnapshots, dbVolumeMap, dbSnapshots)

		deletedSnapshots, err := syncDeletedSnapshotsToDatabase(ctx, deleteIDs, se)
		if err != nil {
			logger.Errorf("Failed to sync deleted snapshots to database: %v", err)
			return err
		}

		createdSnapshots, err := syncNewSnapshotsToDatabase(ctx, newIds, newSSMap, se, dbVolumeMap, pool)
		if err != nil {
			logger.Errorf("Failed to sync new snapshots to database: %v", err)
			return err
		}

		_, err = syncUpdatedSnapshotsToDatabase(ctx, updatedIDs, updatedSSMap, se, dbSnapshotMap)
		if err != nil {
			logger.Errorf("Failed to sync updated snapshots to database: %v", err)
			return err
		}

		_, err = syncWronglyDeletedSnapshotsToDatabase(ctx, wronglyDeletedIds, wronglyDeletedSnapshotsMap, se, dbSnapshotMap)
		if err != nil {
			logger.Errorf("Failed to sync wrongly deleted snapshots to database: %v", err)
			return err
		}

		if hydrationEnabled {
			err = hydrateBatchSnapshotsToCCFE(ctx, createdSnapshots, deletedSnapshots)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func _filterOntapVolumesAndSnapshots(volumes []*vsa.Volume, snapshots []*vsa.Snapshot) (map[string]*vsa.Volume, []*vsa.Snapshot) {
	volumeMap := make(map[string]*vsa.Volume, len(volumes))
	for _, volume := range volumes {
		if *volume.IsSvmRoot {
			continue
		}
		volumeMap[*volume.UUID] = volume
		volumeMap[*volume.Svm.Name+*volume.Name] = volume
	}

	var filteredSnapshots []*vsa.Snapshot
	for _, snapshot := range snapshots {
		name := *snapshot.Name

		vol, ok := volumeMap[*snapshot.ProvenanceVolume.UUID]
		if !ok || *vol.Name != *snapshot.Volume.Name {
			vol, ok = volumeMap[*snapshot.Svm.Name+*snapshot.Volume.Name]
			if !ok {
				continue
			}
		}

		if *vol.Style == FlexGroupConstituent {
			continue
		}

		var snapshotType string
		if snapshot.SnapmirrorLabel != nil || scheduledRegExp.MatchString(name) || snapmirrorRegExp.MatchString(name) {
			snapshotType = SnapshotTypeScheduled
		} else {
			snapshotType = SnapshotTypeAdHoc
		}

		snapshot.ExternalVolumeUUID = vol.ExternalUUID
		snapshot.Type = snapshotType
		filteredSnapshots = append(filteredSnapshots, snapshot)
	}

	return volumeMap, filteredSnapshots
}

func _syncWronglyDeletedSnapshotsToDatabase(ctx context.Context, wronglyDeletedSnapshots []string, wronglyDeletedSnapshotsMap map[string]*vsa.Snapshot, se database.Storage, dbSnapshotsMap map[string]*datamodel.Snapshot) ([]*datamodel.Snapshot, error) {
	var unDeletedSnapshots []*datamodel.Snapshot
	wronglyDeletedChunks := utils.SplitStringSliceIntoChunks(wronglyDeletedSnapshots, snapshotSyncChunkSize)
	for _, wronglyDeletedChunk := range wronglyDeletedChunks {
		for _, key := range wronglyDeletedChunk {
			snapshot := wronglyDeletedSnapshotsMap[key]

			dbSnapshot, err := se.GetWronglyDeletedSnapshot(ctx, snapshot.ExternalUUID)
			if err != nil {
				return nil, err
			}

			err = se.UnDeleteSnapshot(ctx, dbSnapshot)
			unDeletedSnapshots = append(unDeletedSnapshots, dbSnapshot)
			if err != nil {
				return nil, err
			}
		}
	}

	return unDeletedSnapshots, nil
}

func _syncUpdatedSnapshotsToDatabase(ctx context.Context, updatedSnapshots []string, updatedSSMap map[string]*vsa.Snapshot, se database.Storage, dbSnapshotsMap map[string]*datamodel.Snapshot) ([]*datamodel.Snapshot, error) {
	var updatedDbSnapshots []*datamodel.Snapshot
	updateChunks := utils.SplitStringSliceIntoChunks(updatedSnapshots, snapshotSyncChunkSize)
	for _, updateChunk := range updateChunks {
		for _, key := range updateChunk {
			snapshot := updatedSSMap[key]

			dbSnapshot, err := se.UpdateSnapshot(ctx, &datamodel.Snapshot{
				BaseModel: datamodel.BaseModel{
					ID: dbSnapshotsMap[snapshot.ExternalUUID].ID,
				},
				Name: *snapshot.Name,
				SnapshotAttributes: &datamodel.SnapshotAttributes{
					SizeInBytes:            snapshot.SizeInBytes,
					LogicalSizeUsedInBytes: snapshot.LogicalSizeUsedInBytes,
					ExternalUUID:           snapshot.ExternalUUID,
				},
			})
			if err != nil {
				return nil, err
			}
			updatedDbSnapshots = append(updatedDbSnapshots, dbSnapshot)
		}
	}
	return updatedDbSnapshots, nil
}

func _syncNewSnapshotsToDatabase(ctx context.Context, newSnapshots []string, newSSMap map[string]*vsa.Snapshot, se database.Storage, dbVolumeMap map[string]*datamodel.Volume, pool *datamodel.Pool) ([]*datamodel.Snapshot, error) {
	var createdSnapshots []*datamodel.Snapshot
	newChunks := utils.SplitStringSliceIntoChunks(newSnapshots, snapshotSyncChunkSize)
	for _, newChunk := range newChunks {
		for _, key := range newChunk {
			snapshot := newSSMap[key]
			dbSnapshot, err := se.CreatingSnapshot(ctx, &datamodel.Snapshot{
				Name:      *snapshot.Name,
				VolumeID:  dbVolumeMap[snapshot.ExternalVolumeUUID].ID,
				AccountID: pool.AccountID,
				Volume:    dbVolumeMap[snapshot.ExternalVolumeUUID],
				Account:   pool.Account,
				SnapshotAttributes: &datamodel.SnapshotAttributes{
					SizeInBytes:            snapshot.SizeInBytes,
					LogicalSizeUsedInBytes: snapshot.LogicalSizeUsedInBytes,
					ExternalUUID:           snapshot.ExternalUUID,
				},
			})
			if err != nil {
				return nil, err
			}

			dbSnapshot.State = models.LifeCycleStateREADY
			dbSnapshot.StateDetails = models.LifeCycleStateReadyDetails
			_, err = se.UpdateSnapshot(ctx, dbSnapshot)
			createdSnapshots = append(createdSnapshots, dbSnapshot)
			if err != nil {
				return nil, err
			}
		}
	}
	return createdSnapshots, nil
}

func _syncDeletedSnapshotsToDatabase(ctx context.Context, deleteIDs []int64, se database.Storage) ([]*datamodel.Snapshot, error) {
	var deletedSnapshots []*datamodel.Snapshot
	deleteChunks := utils.SplitIntSliceIntoChunks(deleteIDs, snapshotSyncChunkSize)
	for _, deleteChunk := range deleteChunks {
		deletedSnapshotChunk, err := se.BatchDeleteSnapshots(ctx, deleteChunk)
		if err != nil {
			return nil, err
		}
		deletedSnapshots = append(deletedSnapshots, deletedSnapshotChunk...)
	}
	return deletedSnapshots, nil
}

func _getOntapRestProviderForPool(ctx context.Context, se database.Storage, pool *datamodel.Pool) (vsa.Provider, error) {
	nodes, err := se.GetNodesByPoolID(ctx, pool.ID)
	if err != nil {
		if vsaerrors.Is(err, gorm.ErrRecordNotFound) {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrResourceNotFound, err)
		}
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}

	node := &models.Node{
		EndpointAddress: nodes[0].EndpointAddress,
		Username:        pool.Username,
		Password:        pool.Password,
	}

	provider := activities.GetProviderByNode(ctx, node)
	return provider, nil
}

func _processSnapshotSync(ctx context.Context, ontapVolumeMap map[string]*vsa.Volume, ontapSnapshots []*vsa.Snapshot, dbVolumeMap map[string]*datamodel.Volume, dbSnapshots []*datamodel.Snapshot) (
	newSSMap map[string]*vsa.Snapshot, updatedSSMap map[string]*vsa.Snapshot, wronglyDeletedSnapshotsMap map[string]*vsa.Snapshot,
	newIDs []string, updatedIDs []string, deleteIDs []int64, wronglyDeletedIDs []string) {
	logger := util.GetLogger(ctx)

	dbSnapshotMap := make(map[string]*datamodel.Snapshot, len(dbSnapshots))
	dbSnapshotExternalUUIDMap := make(map[string]struct{}, len(dbSnapshots))
	for _, dbSnapshot := range dbSnapshots {
		dbSnapshotExternalUUIDMap[dbSnapshot.SnapshotAttributes.ExternalUUID] = struct{}{}
		dbSnapshotMap[dbSnapshot.SnapshotAttributes.ExternalUUID+"."+dbSnapshot.Volume.VolumeAttributes.ExternalUUID] = dbSnapshot
	}

	newSSMap = make(map[string]*vsa.Snapshot, len(ontapSnapshots))
	updatedSSMap = make(map[string]*vsa.Snapshot, len(ontapSnapshots))
	existingSSMap := make(map[string]*vsa.Snapshot, len(dbSnapshotMap))
	wronglyDeletedSnapshotsMap = make(map[string]*vsa.Snapshot, len(ontapSnapshots))

	for _, snapshot := range ontapSnapshots {
		key := snapshot.ExternalUUID + "." + snapshot.ExternalVolumeUUID

		if dbSnapshot, ok := dbSnapshotMap[key]; ok {
			existingSSMap[key] = snapshot

			if dbSnapshot.SnapshotAttributes.SizeInBytes != snapshot.SizeInBytes || dbSnapshot.SnapshotAttributes.LogicalSizeUsedInBytes != snapshot.LogicalSizeUsedInBytes || dbSnapshot.Name != *snapshot.Name {
				updatedSSMap[key] = snapshot
				updatedIDs = append(updatedIDs, key)
			}
		} else {
			_, volumeExistsInOntap := ontapVolumeMap[snapshot.ExternalVolumeUUID]
			if !volumeExistsInOntap {
				continue
			}

			_, isCloned := dbSnapshotExternalUUIDMap[snapshot.ExternalUUID]

			if snapshot.Type != SnapshotTypeAdHoc || isCloned {
				newSSMap[key] = snapshot
				newIDs = append(newIDs, key)
			} else {
				wronglyDeletedSnapshotsMap[key] = snapshot
				wronglyDeletedIDs = append(wronglyDeletedIDs, key)
			}
		}
	}

	for _, dbSnapshot := range dbSnapshots {
		key := dbSnapshot.SnapshotAttributes.ExternalUUID + "." + dbSnapshot.Volume.VolumeAttributes.ExternalUUID
		_, isNew := newSSMap[key]
		_, stillExists := existingSSMap[key]
		if !stillExists && !isNew {
			_, volExistsInOntap := ontapVolumeMap[dbSnapshot.Volume.VolumeAttributes.ExternalUUID]
			if !volExistsInOntap {
				if dbSnapshot.UUID != "" {
					logger.Warn("Skipped deleting snapshot from database - ONTAP volume is missing.")
				}
				continue
			}

			shouldDelete := dbSnapshot.SnapshotAttributes.Type != SnapshotTypeAdHoc

			if shouldDelete {
				deleteIDs = append(deleteIDs, dbSnapshot.ID)
			}
		}
	}
	return
}
