package backgroundactivities

import (
	"context"
	"fmt"
	"regexp"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/hydrationActivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	utils2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
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

	filter := utils2.CreateFilterWithConditions(utils2.NewFilterCondition("state", "=", models.LifeCycleStateREADY))
	poolViews, err := se.ListPools(ctx, filter)
	if err != nil {
		logger.Errorf("Failed to list pools: %v", err)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}

	var pools []*datamodel.Pool
	for _, poolView := range poolViews {
		pools = append(pools, database.ConvertPoolViewToPool(poolView))
	}
	return pools, nil
}

func (a *SyncSnapshotActivity) SynchronizeSnapshots(ctx context.Context, pools []*datamodel.Pool) error {
	logger := util.GetLogger(ctx)
	se := a.SE
	var errors []string

	for _, pool := range pools {
		provider, err := GetOntapRestProviderForPool(ctx, se, pool)
		if err != nil {
			errMsg := fmt.Sprintf("Failed to get ONTAP rest provider for pool %v: %v", pool.UUID, err)
			logger.Errorf(errMsg)
			errors = append(errors, errMsg)
			continue
		}

		ontapVolumes, err := provider.GetVolumes()
		if err != nil {
			errMsg := fmt.Sprintf("Failed to get ONTAP volumes for the pool: %s, %v", pool.UUID, err)
			logger.Errorf(errMsg)
			errors = append(errors, errMsg)
			continue
		}

		var ontapSnapshots []*vsa.Snapshot
		for _, volume := range ontapVolumes {
			volumeSnapshots, err := provider.GetSnapshots(*volume.UUID)
			if err != nil {
				errMsg := fmt.Sprintf("Failed to get snapshots from ONTAP for volume %s: %v", *volume.UUID, err)
				logger.Errorf(errMsg)
				errors = append(errors, errMsg)
				continue
			}
			ontapSnapshots = append(ontapSnapshots, volumeSnapshots...)
		}

		ontapVolumeMap, ontapSnapshots := filterOntapVolumesAndSnapshots(ontapVolumes, ontapSnapshots)

		dbVolumes, err := se.GetVolumesByPoolID(ctx, pool.ID)
		if err != nil {
			errMsg := fmt.Sprintf("Failed to get volumes from database for pool %s: %v", pool.UUID, err)
			logger.Errorf(errMsg)
			errors = append(errors, errMsg)
			continue
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
			errMsg := fmt.Sprintf("Failed to get snapshots from database for pool %s: %v", pool.UUID, err)
			logger.Errorf(errMsg)
			errors = append(errors, errMsg)
			continue
		}

		var dbSnapshotMap = make(map[string]*datamodel.Snapshot, len(dbSnapshots))
		for _, dbSnapshot := range dbSnapshots {
			dbSnapshotMap[dbSnapshot.SnapshotAttributes.ExternalUUID] = dbSnapshot
		}

		newSSMap, updatedSSMap, wronglyDeletedSnapshotsMap, newIds, updatedIDs, deleteIDs, wronglyDeletedIds :=
			processSnapshotSync(ctx, ontapVolumeMap, ontapSnapshots, dbVolumeMap, dbSnapshots)

		deletedSnapshots, err := syncDeletedSnapshotsToDatabase(ctx, deleteIDs, se)
		if err != nil {
			errMsg := fmt.Sprintf("Failed to sync deleted snapshots to database: %v", err)
			logger.Errorf(errMsg)
			errors = append(errors, errMsg)
			continue
		}

		createdSnapshots, err := syncNewSnapshotsToDatabase(ctx, newIds, newSSMap, se, dbVolumeMap, pool)
		if err != nil {
			errMsg := fmt.Sprintf("Failed to sync new snapshots to database: %v", err)
			logger.Errorf(errMsg)
			errors = append(errors, errMsg)
			continue
		}

		_, err = syncUpdatedSnapshotsToDatabase(ctx, updatedIDs, updatedSSMap, se, dbSnapshotMap)
		if err != nil {
			errMsg := fmt.Sprintf("Failed to sync updated snapshots to database: %v", err)
			logger.Errorf(errMsg)
			errors = append(errors, errMsg)
			continue
		}

		_, err = syncWronglyDeletedSnapshotsToDatabase(ctx, wronglyDeletedIds, wronglyDeletedSnapshotsMap, se, dbSnapshotMap)
		if err != nil {
			errMsg := fmt.Sprintf("Failed to sync wrongly deleted snapshots to database: %v", err)
			logger.Errorf(errMsg)
			errors = append(errors, errMsg)
			continue
		}

		if hydrationEnabled && (len(createdSnapshots) > 0 || len(deletedSnapshots) > 0) {
			err = hydrateBatchSnapshotsToCCFE(ctx, createdSnapshots, deletedSnapshots)
			if err != nil {
				errMsg := fmt.Sprintf("Failed to hydrate snapshots to CCFE: %v", err)
				logger.Errorf(errMsg)
				errors = append(errors, errMsg)
				continue
			}
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("snapshot Synchronization completed with errors: %v", errors)
	}

	return nil
}

func _filterOntapVolumesAndSnapshots(volumes []*vsa.Volume, snapshots []*vsa.Snapshot) (map[string]*vsa.Volume, []*vsa.Snapshot) {
	volumeMap := make(map[string]*vsa.Volume, len(volumes))
	for _, volume := range volumes {
		if volume == nil || volume.IsSvmRoot == nil || *volume.IsSvmRoot {
			continue
		}
		if volume.UUID == nil || volume.Svm == nil || volume.Svm.Name == nil || volume.Name == nil {
			continue
		}
		volumeMap[*volume.UUID] = volume
		volumeMap[*volume.Svm.Name+*volume.Name] = volume
	}

	var filteredSnapshots []*vsa.Snapshot
	for _, snapshot := range snapshots {
		if snapshot == nil || snapshot.Name == nil || snapshot.ProvenanceVolume == nil || snapshot.ProvenanceVolume.UUID == nil || snapshot.Volume == nil || snapshot.Volume.Name == nil {
			continue
		}

		name := *snapshot.Name

		vol, ok := volumeMap[*snapshot.ProvenanceVolume.UUID]
		if !ok || vol.Name == nil || *vol.Name != *snapshot.Volume.Name {
			vol, ok = volumeMap[*snapshot.Svm.Name+*snapshot.Volume.Name]
			if !ok {
				continue
			}
		}

		if vol.Style == nil || *vol.Style == FlexGroupConstituent {
			continue
		}

		var snapshotType string
		if snapshot.SnapmirrorLabel != nil || scheduledRegExp.MatchString(name) || snapmirrorRegExp.MatchString(name) {
			snapshotType = SnapshotTypeScheduled
		} else {
			snapshotType = SnapshotTypeAdHoc
		}

		if vol.ExternalUUID == "" {
			continue
		}

		snapshot.ExternalVolumeUUID = vol.ExternalUUID
		snapshot.Type = snapshotType
		filteredSnapshots = append(filteredSnapshots, snapshot)
	}

	return volumeMap, filteredSnapshots
}

func _syncWronglyDeletedSnapshotsToDatabase(ctx context.Context, wronglyDeletedSnapshots []string, wronglyDeletedSnapshotsMap map[string]*vsa.Snapshot, se database.Storage, dbSnapshotsMap map[string]*datamodel.Snapshot) ([]*datamodel.Snapshot, error) {
	var externalUUIDs []string
	for _, key := range wronglyDeletedSnapshots {
		snapshot := wronglyDeletedSnapshotsMap[key]
		externalUUIDs = append(externalUUIDs, snapshot.ExternalUUID)
	}
	if len(externalUUIDs) == 0 {
		return nil, nil
	}
	allUndeleted := []*datamodel.Snapshot{}
	for i := 0; i < len(externalUUIDs); i += snapshotSyncChunkSize {
		end := i + snapshotSyncChunkSize
		if end > len(externalUUIDs) {
			end = len(externalUUIDs)
		}
		chunk := externalUUIDs[i:end]
		snapshotsToUnDelete, err := se.BatchGetWronglyDeletedSnapshots(ctx, chunk)
		if err != nil {
			return nil, err
		}
		if len(snapshotsToUnDelete) == 0 {
			continue
		}
		if err := se.BatchUnDeleteSnapshots(ctx, snapshotsToUnDelete); err != nil {
			return nil, err
		}
		allUndeleted = append(allUndeleted, snapshotsToUnDelete...)
	}
	return allUndeleted, nil
}

func _syncUpdatedSnapshotsToDatabase(ctx context.Context, updatedSnapshots []string, updatedSSMap map[string]*vsa.Snapshot, se database.Storage, dbSnapshotsMap map[string]*datamodel.Snapshot) ([]*datamodel.Snapshot, error) {
	var snapshotsToUpdate []*datamodel.Snapshot
	for _, key := range updatedSnapshots {
		snapshot := updatedSSMap[key]
		dbSnapshot := dbSnapshotsMap[snapshot.ExternalUUID]
		if dbSnapshot == nil {
			continue
		}
		snapshotsToUpdate = append(snapshotsToUpdate, &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{
				UUID: dbSnapshot.UUID,
				ID:   dbSnapshot.ID,
			},
			Name: *snapshot.Name,
			SnapshotAttributes: &datamodel.SnapshotAttributes{
				SizeInBytes:            snapshot.SizeInBytes,
				LogicalSizeUsedInBytes: snapshot.LogicalSizeUsedInBytes,
				ExternalUUID:           snapshot.ExternalUUID,
			},
			State:        dbSnapshot.State,
			StateDetails: dbSnapshot.StateDetails,
		})
	}
	if len(snapshotsToUpdate) == 0 {
		return nil, nil
	}
	var allUpdated []*datamodel.Snapshot
	for i := 0; i < len(snapshotsToUpdate); i += snapshotSyncChunkSize {
		end := i + snapshotSyncChunkSize
		if end > len(snapshotsToUpdate) {
			end = len(snapshotsToUpdate)
		}
		chunk := snapshotsToUpdate[i:end]
		if err := se.BatchUpdateSnapshots(ctx, chunk); err != nil {
			return nil, err
		}
		allUpdated = append(allUpdated, chunk...)
	}
	return allUpdated, nil
}

func _syncNewSnapshotsToDatabase(ctx context.Context, newSnapshots []string, newSSMap map[string]*vsa.Snapshot, se database.Storage, dbVolumeMap map[string]*datamodel.Volume, pool *datamodel.Pool) ([]*datamodel.Snapshot, error) {
	var snapshotsToCreate []*datamodel.Snapshot
	for _, key := range newSnapshots {
		snapshot := newSSMap[key]
		vol := dbVolumeMap[snapshot.ExternalVolumeUUID]
		if vol == nil {
			continue
		}
		snapshotsToCreate = append(snapshotsToCreate, &datamodel.Snapshot{
			Name:      *snapshot.Name,
			VolumeID:  vol.ID,
			AccountID: pool.AccountID,
			Volume:    vol,
			Account:   pool.Account,
			SnapshotAttributes: &datamodel.SnapshotAttributes{
				SizeInBytes:            snapshot.SizeInBytes,
				LogicalSizeUsedInBytes: snapshot.LogicalSizeUsedInBytes,
				ExternalUUID:           snapshot.ExternalUUID,
			},
			State:        models.LifeCycleStateREADY,
			StateDetails: models.LifeCycleStateAvailableDetails,
		})
	}
	if len(snapshotsToCreate) == 0 {
		return nil, nil
	}

	var createdSnapshots []*datamodel.Snapshot
	var allCreatedUUIDs []string
	for i := 0; i < len(snapshotsToCreate); i += snapshotSyncChunkSize {
		end := i + snapshotSyncChunkSize
		if end > len(snapshotsToCreate) {
			end = len(snapshotsToCreate)
		}
		chunk := snapshotsToCreate[i:end]
		createdUUIDs, err := se.BatchCreateSnapshots(ctx, chunk, true)
		if err != nil {
			return nil, err
		}
		allCreatedUUIDs = append(allCreatedUUIDs, createdUUIDs...)
	}

	if len(allCreatedUUIDs) > 0 {
		createdSnapshots, err := se.BatchGetSnapshotsByUUIDs(ctx, allCreatedUUIDs)
		if err != nil {
			return nil, err
		}
		return createdSnapshots, nil
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

	node := hyperscaler.CreateNodeForProvider(hyperscaler.NodeProviderInput{
		Nodes:          nodes,
		Password:       pool.PoolCredentials.Password,
		SecretID:       pool.PoolCredentials.SecretID,
		CertificateID:  pool.PoolCredentials.CertificateID,
		DeploymentName: pool.DeploymentName,
		AuthType:       pool.PoolCredentials.AuthType,
	})

	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
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

			shouldDelete := dbSnapshot.Type != SnapshotTypeAdHoc

			if shouldDelete {
				deleteIDs = append(deleteIDs, dbSnapshot.ID)
			}
		}
	}
	return
}
