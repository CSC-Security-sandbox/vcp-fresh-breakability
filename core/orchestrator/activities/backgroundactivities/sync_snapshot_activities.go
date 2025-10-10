package backgroundactivities

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/hydrationActivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	utils2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"gorm.io/gorm"
)

const (
	SnapshotTypeScheduled = "scheduled"
	SnapshotTypeAdHoc     = "ad-hoc"
	SnapshotTypeBackup    = "backup"

	FlexGroupConstituent = "flexgroup_constituent"
)

var (
	snapshotSyncChunkSize              = env.GetInt("CVS_SNAPSHOT_SYNC_CHUNK_SIZE", 200)
	hydrationEnabled                   = env.GetBool("GCP_HYDRATE_ENABLED", true)
	snapshotSyncMaxConcurrency         = env.GetInt("SNAPSHOT_SYNC_MAX_CONCURRENCY", 20)
	ontapSnapshotFetchConcurrencyLimit = env.GetInt("ONTAP_SNAPSHOT_FETCH_MAX_CONCURRENCY", 5)

	scheduledRegExp  = regexp.MustCompile(`^(hourly|daily|weekly|monthly)\..*$`)
	snapmirrorRegExp = regexp.MustCompile(`^snapmirror\.[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}_.*$`)

	GetOntapRestProviderForPool           = _getOntapRestProviderForPool
	GetOntapRestProviderForPoolFastConn   = _getOntapRestProviderForPoolFastConn
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

// ListPoolsUUIDPaginated returns a paginated list of pool identifiers in READY state from the database.
func (a *SyncSnapshotActivity) ListPoolsUUIDPaginated(ctx context.Context, offset, limit int) ([]*database.PoolIdentifier, error) {
	logger := util.GetLogger(ctx)
	se := a.SE

	filter := utils2.CreateFilterWithConditions(utils2.NewFilterCondition("state", "=", models.LifeCycleStateREADY))
	pools, err := se.ListPoolUUIDsPaginated(ctx, filter, offset, limit)
	if err != nil {
		logger.Errorf("Failed to list pools: %v", err)
		return nil, fmt.Errorf("failed to list pools")
	}

	logger.Infof("Found %d pools (offset: %d, limit: %d)", len(pools), offset, limit)
	return pools, nil
}

// GetTotalPoolCount Returns the total count of pools in READY state from the database.
func (a *SyncSnapshotActivity) GetTotalPoolCount(ctx context.Context) (int, error) {
	logger := util.GetLogger(ctx)
	se := a.SE

	filter := utils2.CreateFilterWithConditions(utils2.NewFilterCondition("state", "=", models.LifeCycleStateREADY))
	count, err := se.GetPoolsCount(ctx, filter)
	if err != nil {
		logger.Errorf("Failed to count pools: %v", err)
		return 0, fmt.Errorf("failed to count pools")
	}

	logger.Debugf("Total pools count: %d", count)
	return int(count), nil
}

func (a *SyncSnapshotActivity) FetchPoolByUUID(ctx context.Context, poolUUID string, accountID int64) (*datamodel.Pool, error) {
	logger := util.GetLogger(ctx)
	se := a.SE

	pool, err := se.GetPool(ctx, poolUUID, accountID)
	if err != nil {
		logger.Errorf("Failed to get pool, error: %v", err)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}

	return database.ConvertPoolViewToPool(pool), nil
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
	logger := util.GetLogger(ctx)
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
			logger.Errorf("Failed to get wrongly deleted snapshots: %v", err)
			return nil, err
		}
		if len(snapshotsToUnDelete) == 0 {
			continue
		}
		if err := se.BatchUnDeleteSnapshots(ctx, snapshotsToUnDelete); err != nil {
			logger.Errorf("Failed to undelete snapshots: %v", err)
			return nil, err
		}
		allUndeleted = append(allUndeleted, snapshotsToUnDelete...)
	}
	return allUndeleted, nil
}

func _syncUpdatedSnapshotsToDatabase(ctx context.Context, updatedSnapshots []string, updatedSSMap map[string]*vsa.Snapshot, se database.Storage, dbSnapshotsMap map[string]*datamodel.Snapshot) ([]*datamodel.Snapshot, error) {
	logger := util.GetLogger(ctx)
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
			logger.Errorf("Failed to update snapshots: %v", err)
			return nil, err
		}
		allUpdated = append(allUpdated, chunk...)
	}
	return allUpdated, nil
}

func _syncNewSnapshotsToDatabase(ctx context.Context, newSnapshots []string, newSSMap map[string]*vsa.Snapshot, se database.Storage, dbVolumeMap map[string]*datamodel.Volume, pool *datamodel.Pool) ([]*datamodel.Snapshot, error) {
	logger := util.GetLogger(ctx)
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
			Type:         snapshot.Type,
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
			logger.Errorf("Failed to create snapshots: %v", err)
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
	logger := util.GetLogger(ctx)
	var deletedSnapshots []*datamodel.Snapshot
	deleteChunks := utils.SplitIntSliceIntoChunks(deleteIDs, snapshotSyncChunkSize)
	for _, deleteChunk := range deleteChunks {
		deletedSnapshotChunk, err := se.BatchDeleteSnapshots(ctx, deleteChunk)
		if err != nil {
			logger.Errorf("Failed to delete snapshots: %v", err)
			return nil, err
		}
		deletedSnapshots = append(deletedSnapshots, deletedSnapshotChunk...)
	}
	return deletedSnapshots, nil
}

func _getOntapRestProviderForPool(ctx context.Context, se database.Storage, pool *datamodel.Pool) (vsa.Provider, error) {
	logger := util.GetLogger(ctx)
	nodes, err := se.GetNodesByPoolID(ctx, pool.ID)
	if err != nil {
		logger.Errorf("Failed to get nodes for pool: %v", err)
		if vsaerrors.Is(err, gorm.ErrRecordNotFound) {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrResourceNotFound, err)
		}
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}

	if len(nodes) == 0 {
		logger.Errorf("No nodes found for pool %s", pool.UUID)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrResourceNotFound, fmt.Errorf("no nodes found for pool %s", pool.UUID))
	}

	if pool.PoolCredentials == nil {
		logger.Errorf("Pool credentials not found for pool %s", pool.UUID)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrResourceNotFound, fmt.Errorf("pool credentials not found for pool %s", pool.UUID))
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
		logger.Errorf("Failed to get provider by node: %v", err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return provider, nil
}

func _getOntapRestProviderForPoolFastConn(ctx context.Context, se database.Storage, pool *datamodel.Pool) (vsa.Provider, error) {
	logger := util.GetLogger(ctx)
	nodes, err := se.GetNodesByPoolID(ctx, pool.ID)
	if err != nil {
		if vsaerrors.Is(err, gorm.ErrRecordNotFound) {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrResourceNotFound, err)
		}
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}

	if len(nodes) == 0 {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrResourceNotFound, fmt.Errorf("no nodes found for pool %s", pool.UUID))
	}

	if pool.PoolCredentials == nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrResourceNotFound, fmt.Errorf("pool credentials not found for pool %s", pool.UUID))
	}

	node := hyperscaler.CreateNodeForProvider(hyperscaler.NodeProviderInput{
		Nodes:          nodes,
		Password:       pool.PoolCredentials.Password,
		SecretID:       pool.PoolCredentials.SecretID,
		CertificateID:  pool.PoolCredentials.CertificateID,
		DeploymentName: pool.DeploymentName,
		AuthType:       pool.PoolCredentials.AuthType,
	})

	provider, err := hyperscaler.GetProviderByNodeWithFastConnection(ctx, node)
	if err != nil {
		logger.Errorf("Failed to get provider by node with fast connection: %v", err)
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

type GetOntapVolumesAndSnapshotsForPoolReturnValue struct {
	OntapVolumeMap map[string]*vsa.Volume
	OntapSnapshots []*vsa.Snapshot
}

func (a *SyncSnapshotActivity) GetOntapVolumesAndSnapshotsForPool(ctx context.Context, pool *datamodel.Pool) (*GetOntapVolumesAndSnapshotsForPoolReturnValue, error) {
	logger := util.GetLogger(ctx)
	se := a.SE
	provider, err := GetOntapRestProviderForPool(ctx, se, pool)
	if err != nil || provider == nil {
		errMsg := fmt.Sprintf("Failed to get ONTAP rest provider for pool %v: %v", pool.UUID, err)
		logger.Errorf(errMsg)
		return nil, err
	}

	// Fetch ONTAP volumes with a 30-second timeout to prevent long retries.
	done := make(chan struct{})
	var ontapVolumes []*vsa.Volume
	var volumesErr error

	go func() {
		ontapVolumes, volumesErr = provider.GetVolumes()
		close(done)
	}()

	select {
	case <-done:
		if volumesErr != nil {
			errMsg := fmt.Sprintf("Failed to get ONTAP volumes for the pool: %s, %v", pool.UUID, volumesErr)
			logger.Errorf(errMsg)
			return nil, volumesErr
		}
	case <-time.After(30 * time.Second):
		errMsg := fmt.Sprintf("Timeout getting ONTAP volumes for pool %s after 30 seconds", pool.UUID)
		logger.Errorf(errMsg)
		return nil, errors.New(errMsg)
	case <-ctx.Done():
		errMsg := fmt.Sprintf("Context cancelled while getting ONTAP volumes for pool %s", pool.UUID)
		logger.Errorf(errMsg)
		return nil, errors.New(errMsg)
	}

	// Get snapshots in parallel with controlled concurrency
	var ontapSnapshots []*vsa.Snapshot
	var wg sync.WaitGroup
	var mu sync.Mutex

	maxConcurrency := ontapSnapshotFetchConcurrencyLimit
	if maxConcurrency <= 0 {
		maxConcurrency = 5
	}
	semaphore := make(chan struct{}, maxConcurrency)

	for _, volume := range ontapVolumes {
		wg.Add(1)
		go func(vol *vsa.Volume) {
			defer wg.Done()
			semaphore <- struct{}{}        // Acquire slot
			defer func() { <-semaphore }() // Release slot

			volumeSnapshots, err := provider.GetSnapshots(*vol.UUID)
			if err != nil {
				logger.Errorf("Failed to get snapshots from ONTAP for volume %s: %v", *vol.UUID, err)
				return
			}
			mu.Lock()
			ontapSnapshots = append(ontapSnapshots, volumeSnapshots...)
			mu.Unlock()
		}(volume)
	}
	wg.Wait()

	ontapVolumeMap, ontapSnapshots := filterOntapVolumesAndSnapshots(ontapVolumes, ontapSnapshots)

	return &GetOntapVolumesAndSnapshotsForPoolReturnValue{
		OntapVolumeMap: ontapVolumeMap,
		OntapSnapshots: ontapSnapshots,
	}, nil
}

type GetDBVolumeAndSnapshotsForPoolReturnValue struct {
	DBVolumeMap   map[string]*datamodel.Volume
	DBSnapshotMap map[string]*datamodel.Snapshot
	DBSnapshots   []*datamodel.Snapshot
}

func (a *SyncSnapshotActivity) GetDBVolumeAndSnapshotsForPool(ctx context.Context, pool *datamodel.Pool) (*GetDBVolumeAndSnapshotsForPoolReturnValue, error) {
	se := a.SE
	logger := util.GetLogger(ctx)
	dbVolumes, err := se.GetVolumesByPoolID(ctx, pool.ID)
	if err != nil {
		errMsg := fmt.Sprintf("Failed to get volumes from database for pool %s: %v", pool.UUID, err)
		logger.Errorf(errMsg)
		return nil, err
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
		return nil, err
	}

	var dbSnapshotMap = make(map[string]*datamodel.Snapshot, len(dbSnapshots))
	for _, dbSnapshot := range dbSnapshots {
		dbSnapshotMap[dbSnapshot.SnapshotAttributes.ExternalUUID] = dbSnapshot
	}

	return &GetDBVolumeAndSnapshotsForPoolReturnValue{
		DBVolumeMap:   dbVolumeMap,
		DBSnapshotMap: dbSnapshotMap,
		DBSnapshots:   dbSnapshots,
	}, nil
}

type ProcessSnapshotsReturnValue struct {
	NewSSMap                   map[string]*vsa.Snapshot
	UpdatedSSMap               map[string]*vsa.Snapshot
	WronglyDeletedSnapshotsMap map[string]*vsa.Snapshot
	NewIDs                     []string
	UpdatedIDs                 []string
	DeleteIDs                  []int64
	WronglyDeletedIDs          []string
}

func (a *SyncSnapshotActivity) ProcessSnapshots(ctx context.Context, ontapVolumeMap map[string]*vsa.Volume, ontapSnapshots []*vsa.Snapshot, dbVolumeMap map[string]*datamodel.Volume, dbSnapshots []*datamodel.Snapshot) (*ProcessSnapshotsReturnValue, error) {
	newSSMap, updatedSSMap, wronglyDeletedSnapshotsMap, newIds, updatedIDs, deleteIDs, wronglyDeletedIds :=
		processSnapshotSync(ctx, ontapVolumeMap, ontapSnapshots, dbVolumeMap, dbSnapshots)

	return &ProcessSnapshotsReturnValue{
		NewSSMap:                   newSSMap,
		UpdatedSSMap:               updatedSSMap,
		WronglyDeletedSnapshotsMap: wronglyDeletedSnapshotsMap,
		NewIDs:                     newIds,
		UpdatedIDs:                 updatedIDs,
		DeleteIDs:                  deleteIDs,
		WronglyDeletedIDs:          wronglyDeletedIds,
	}, nil
}

func (a *SyncSnapshotActivity) SyncDeletedSnapshotsToDatabase(ctx context.Context, deleteIDs []int64) ([]*datamodel.Snapshot, error) {
	return syncDeletedSnapshotsToDatabase(ctx, deleteIDs, a.SE)
}

func (a *SyncSnapshotActivity) SyncNewSnapshotsToDatabase(ctx context.Context, newSnapshots []string, newSSMap map[string]*vsa.Snapshot, dbVolumeMap map[string]*datamodel.Volume, pool *datamodel.Pool) ([]*datamodel.Snapshot, error) {
	return syncNewSnapshotsToDatabase(ctx, newSnapshots, newSSMap, a.SE, dbVolumeMap, pool)
}

func (a *SyncSnapshotActivity) SyncUpdatedSnapshotsToDatabase(ctx context.Context, updatedSnapshots []string, updatedSSMap map[string]*vsa.Snapshot, dbSnapshotsMap map[string]*datamodel.Snapshot) ([]*datamodel.Snapshot, error) {
	return syncUpdatedSnapshotsToDatabase(ctx, updatedSnapshots, updatedSSMap, a.SE, dbSnapshotsMap)
}

func (a *SyncSnapshotActivity) SyncWronglyDeletedSnapshotsToDatabase(ctx context.Context, wronglyDeletedSnapshots []string, wronglyDeletedSnapshotsMap map[string]*vsa.Snapshot) ([]*datamodel.Snapshot, error) {
	return syncWronglyDeletedSnapshotsToDatabase(ctx, wronglyDeletedSnapshots, wronglyDeletedSnapshotsMap, a.SE, nil)
}

func (a *SyncSnapshotActivity) HydrateSnapshotsToCCFE(ctx context.Context, createdSnapshots []*datamodel.Snapshot, deletedSnapshots []*datamodel.Snapshot) error {
	logger := util.GetLogger(ctx)
	if len(createdSnapshots) == 0 && len(deletedSnapshots) == 0 {
		logger.Info("No snapshots to hydrate to CCFE, skipping hydration")
		return nil
	}

	if hydrationEnabled {
		return hydrateBatchSnapshotsToCCFE(ctx, createdSnapshots, deletedSnapshots)
	} else {
		logger.Warn("Hydration is disabled, skipping snapshot hydration to CCFE")
	}
	return nil
}

// SyncSnapshotsForPoolBatchReturnValue represents the return value for batch processing
type SyncSnapshotsForPoolBatchReturnValue struct {
	TotalProcessed       int
	Successful           int
	Failed               int
	FailedResourceNames  []string
	FailedResourceErrors []ParentChildWorkflowError
}

// SyncSnapshotsForPoolBatchActivity processes a batch of pools for snapshot synchronization
func (a *SyncSnapshotActivity) SyncSnapshotsForPoolBatchActivity(ctx context.Context, poolIdentifiers []*database.PoolIdentifier) (*SyncSnapshotsForPoolBatchReturnValue, error) {
	logger := util.GetLogger(ctx)
	if len(poolIdentifiers) == 0 {
		return &SyncSnapshotsForPoolBatchReturnValue{}, nil
	}

	logger.Infof("Starting batch processing for snapshot synchronization: total pools = %d", len(poolIdentifiers))
	result := &SyncSnapshotsForPoolBatchReturnValue{
		TotalProcessed: len(poolIdentifiers),
	}

	// Process each pool in the batch with controlled concurrency
	var wg sync.WaitGroup
	var mu sync.Mutex
	var FailedResourceNames []string
	var FailedResourceErrors []ParentChildWorkflowError

	// Concurrent goroutines to process the snapshot sync for each pool
	semaphore := make(chan struct{}, snapshotSyncMaxConcurrency)

	for _, poolIdentifier := range poolIdentifiers {
		wg.Add(1)
		go func(pid *database.PoolIdentifier) {
			defer wg.Done()

			// Acquire semaphore
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			err := a.processPoolSnapshotSync(ctx, pid)

			mu.Lock()
			if err != nil {
				logger.Errorf("Failed to sync snapshots for pool %s: %v", pid.Name, err)
				result.Failed++
				FailedResourceNames = append(FailedResourceNames, pid.Name)
				FailedResourceErrors = append(FailedResourceErrors, ParentChildWorkflowError{
					ResourceName: pid.Name,
					Error:        err.Error(),
				})
			} else {
				result.Successful++
			}
			mu.Unlock()
		}(poolIdentifier)
	}
	// Wait for all goroutines to complete
	wg.Wait()
	logger.Infof("Snapshot batch processing completed: total pools = %d, successful = %d, failed = %d", result.TotalProcessed, result.Successful, result.Failed)

	if result.Failed > 0 {
		logger.Warnf("Snapshot sync failed for %d pools. Failed pools: %v", result.Failed, FailedResourceNames)
	}

	result.FailedResourceNames = FailedResourceNames
	result.FailedResourceErrors = FailedResourceErrors
	return result, nil
}

// processPoolSnapshotSync processes snapshot synchronization for a single pool
func (a *SyncSnapshotActivity) processPoolSnapshotSync(ctx context.Context, poolIdentifier *database.PoolIdentifier) error {
	logger := util.GetLogger(ctx)
	logger.Infof("Starting processPoolSnapshotSync for pool: %s", poolIdentifier.Name)

	// Fetch pool details
	pool, err := a.FetchPoolByUUID(ctx, poolIdentifier.UUID, poolIdentifier.AccountID)
	if err != nil {
		logger.Errorf("Failed to fetch pool %s: %v", poolIdentifier.Name, err)
		return fmt.Errorf("FetchPoolByUUID Failed")
	}

	// Get ONTAP volumes and snapshots
	ontapVolSnapshotResp, err := a.GetOntapVolumesAndSnapshotsForPool(ctx, pool)
	if err != nil {
		logger.Errorf("Failed to get ONTAP volumes and snapshots for pool %s: %v", pool.Name, err)
		return fmt.Errorf("GetOntapVolumesAndSnapshotsForPool Failed")
	}

	// Get DB volumes and snapshots
	dbVolSnapshotResp, err := a.GetDBVolumeAndSnapshotsForPool(ctx, pool)
	if err != nil {
		logger.Errorf("Failed to get ONTAP volumes and snapshots for pool %s: %v", pool.Name, err)
		return fmt.Errorf("GetDBVolumeAndSnapshotsForPool Failed")
	}

	// Process snapshots to determine what needs to be created, updated, or deleted
	processSnapshotsResp, err := a.ProcessSnapshots(
		ctx,
		ontapVolSnapshotResp.OntapVolumeMap,
		ontapVolSnapshotResp.OntapSnapshots,
		dbVolSnapshotResp.DBVolumeMap,
		dbVolSnapshotResp.DBSnapshots,
	)
	if err != nil {
		logger.Errorf("Failed to process snapshots for pool %s: %v", pool.Name, err)
		return fmt.Errorf("ProcessSnapshots Failed")
	}

	// Sync deleted snapshots to database
	deletedSnapshots, err := a.SyncDeletedSnapshotsToDatabase(ctx, processSnapshotsResp.DeleteIDs)
	if err != nil {
		logger.Errorf("Failed to sync deleted snapshots for pool %s: %v", pool.Name, err)
		return fmt.Errorf("SyncDeletedSnapshotsToDatabase Failed")
	}

	// Sync new snapshots to database
	createdSnapshots, err := a.SyncNewSnapshotsToDatabase(ctx, processSnapshotsResp.NewIDs, processSnapshotsResp.NewSSMap, dbVolSnapshotResp.DBVolumeMap, pool)
	if err != nil {
		logger.Errorf("Failed to sync new snapshots for pool %s: %v", pool.Name, err)
		return fmt.Errorf("SyncNewSnapshotsToDatabase Failed")
	}

	// Sync updated snapshots to database
	_, err = a.SyncUpdatedSnapshotsToDatabase(ctx, processSnapshotsResp.UpdatedIDs, processSnapshotsResp.UpdatedSSMap, dbVolSnapshotResp.DBSnapshotMap)
	if err != nil {
		logger.Errorf("Failed to sync updated snapshots for pool %s: %v", pool.Name, err)
		return fmt.Errorf("SyncUpdatedSnapshotsToDatabase Failed")
	}

	// Sync wrongly deleted snapshots to database
	_, err = a.SyncWronglyDeletedSnapshotsToDatabase(ctx, processSnapshotsResp.WronglyDeletedIDs, processSnapshotsResp.WronglyDeletedSnapshotsMap)
	if err != nil {
		logger.Errorf("Failed to sync wrongly deleted snapshots for pool %s: %v", pool.Name, err)
		return fmt.Errorf("SyncWronglyDeletedSnapshotsToDatabase Failed")
	}

	// Hydrate snapshots to CCFE
	err = a.HydrateSnapshotsToCCFE(ctx, createdSnapshots, deletedSnapshots)
	if err != nil {
		logger.Errorf("Failed to hydrate snapshots to CCFE for pool %s: %v", pool.Name, err)
		return fmt.Errorf("HydrateSnapshotsToCCFE Failed")
	}

	logger.Infof("Successfully completed snapshot synchronization for pool: %s", poolIdentifier.Name)
	return nil
}
