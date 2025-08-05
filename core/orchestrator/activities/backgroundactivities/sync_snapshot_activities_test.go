package backgroundactivities

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	ontaprestmodel "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"gorm.io/gorm"
)

func TestListPools(t *testing.T) {
	t.Run("ListPoolsSuccess", func(tt *testing.T) {
		ctx := context.TODO()
		mockStorage := database.NewMockStorage(tt)

		mockStorage.On("ListPools", ctx, mock.Anything).Return(
			[]*datamodel.PoolView{{Pool: datamodel.Pool{}, VolumeCount: 1}}, nil)

		syncSnapshotActivity := SyncSnapshotActivity{SE: mockStorage}
		pools, err := syncSnapshotActivity.ListPools(ctx)
		assert.NoError(tt, err)
		assert.Equal(tt, len(pools), 1)
		mockStorage.AssertExpectations(tt)
	})
	t.Run("ListPoolsFailure", func(tt *testing.T) {
		ctx := context.TODO()
		mockStorage := database.NewMockStorage(tt)

		mockStorage.On("ListPools", ctx, mock.Anything).Return(nil, errors.New("failed to list pools"))

		syncSnapshotActivity := SyncSnapshotActivity{SE: mockStorage}
		pools, err := syncSnapshotActivity.ListPools(ctx)
		assert.Error(tt, err)
		assert.Nil(tt, pools)
		mockStorage.AssertExpectations(tt)
	})
}

func TestSynchronizeSnapshots(t *testing.T) {
	t.Run("SynchronizeSnapshotsSuccess", func(tt *testing.T) {
		ctx := context.TODO()
		mockStorage := database.NewMockStorage(tt)
		mockProvider := vsa.NewMockProvider(tt)

		originalHydrationEnabled := hydrationEnabled
		originalHydrateBatchSnapshotsToCCFE := hydrateBatchSnapshotsToCCFE
		originalGetOntapRestProviderForPool := GetOntapRestProviderForPool
		originalFilterOntapVolumesAndSnapshots := filterOntapVolumesAndSnapshots
		originalProcessSnapshotSync := processSnapshotSync
		originalSyncDeletedSnapshotsToDatabase := syncDeletedSnapshotsToDatabase
		originalSyncNewSnapshotsToDatabase := syncNewSnapshotsToDatabase
		originalSyncUpdatedSnapshotsToDatabase := syncUpdatedSnapshotsToDatabase
		originalSyncUndeletedSnapshotsToDatabase := syncWronglyDeletedSnapshotsToDatabase
		defer func() {
			GetOntapRestProviderForPool = originalGetOntapRestProviderForPool
			filterOntapVolumesAndSnapshots = originalFilterOntapVolumesAndSnapshots
			processSnapshotSync = originalProcessSnapshotSync
			syncDeletedSnapshotsToDatabase = originalSyncDeletedSnapshotsToDatabase
			syncNewSnapshotsToDatabase = originalSyncNewSnapshotsToDatabase
			syncUpdatedSnapshotsToDatabase = originalSyncUpdatedSnapshotsToDatabase
			syncWronglyDeletedSnapshotsToDatabase = originalSyncUndeletedSnapshotsToDatabase
			hydrationEnabled = originalHydrationEnabled
			hydrateBatchSnapshotsToCCFE = originalHydrateBatchSnapshotsToCCFE
		}()
		hydrationEnabled = true

		GetOntapRestProviderForPool = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) (vsa.Provider, error) {
			return mockProvider, nil
		}
		filterOntapVolumesAndSnapshots = func(volumes []*vsa.Volume, snapshots []*vsa.Snapshot) (map[string]*vsa.Volume, []*vsa.Snapshot) {
			return make(map[string]*vsa.Volume), []*vsa.Snapshot{}
		}
		processSnapshotSync = func(ctx context.Context, ontapVolumeMap map[string]*vsa.Volume, ontapSnapshots []*vsa.Snapshot, dbVolumeMap map[string]*datamodel.Volume, dbSnapshots []*datamodel.Snapshot) (
			newSSMap map[string]*vsa.Snapshot, updatedSSMap map[string]*vsa.Snapshot, wronglyDeletedSnapshotsMap map[string]*vsa.Snapshot,
			newIDs []string, updatedIDs []string, deleteIDs []int64, wronglyDeletedIDs []string) {
			return
		}
		syncDeletedSnapshotsToDatabase = func(ctx context.Context, deleteIDs []int64, se database.Storage) ([]*datamodel.Snapshot, error) {
			return nil, nil
		}
		syncNewSnapshotsToDatabase = func(ctx context.Context, newIds []string, newSSMap map[string]*vsa.Snapshot, se database.Storage, dbVolumeMap map[string]*datamodel.Volume, pool *datamodel.Pool) ([]*datamodel.Snapshot, error) {
			return nil, nil
		}
		syncUpdatedSnapshotsToDatabase = func(ctx context.Context, updatedIDs []string, updatedSSMap map[string]*vsa.Snapshot, se database.Storage, dbSnapshotsMap map[string]*datamodel.Snapshot) ([]*datamodel.Snapshot, error) {
			return nil, nil
		}
		syncWronglyDeletedSnapshotsToDatabase = func(ctx context.Context, wronglyDeletedIds []string, wronglyDeletedSnapshotsMap map[string]*vsa.Snapshot, se database.Storage, dbSnapshotsMap map[string]*datamodel.Snapshot) ([]*datamodel.Snapshot, error) {
			return nil, nil
		}
		hydrateBatchSnapshotsToCCFE = func(ctx context.Context, createdSnapshots []*datamodel.Snapshot, deletedSnapshots []*datamodel.Snapshot) error {
			return nil
		}

		mockProvider.On("GetVolumes").Return([]*vsa.Volume{{Volume: ontaprestmodel.Volume{UUID: nillable.ToPointer("test-volume-uuid")}}}, nil)
		mockProvider.On("GetSnapshots", mock.Anything).Return([]*vsa.Snapshot{{ExternalUUID: "snapshot-uuid-1"}}, nil)
		mockStorage.On("GetVolumesByPoolID", mock.Anything, mock.Anything).Return(
			[]*datamodel.Volume{{BaseModel: datamodel.BaseModel{ID: 1}, VolumeAttributes: &datamodel.VolumeAttributes{ExternalUUID: "test-volume-uuid"}}}, nil)
		mockStorage.On("GetSnapshotsByVolumeIDs", mock.Anything, mock.Anything).Return(
			[]*datamodel.Snapshot{{SnapshotAttributes: &datamodel.SnapshotAttributes{ExternalUUID: "test-snapshot-uuid"}}}, nil)

		syncSnapshotActivity := SyncSnapshotActivity{SE: mockStorage}
		err := syncSnapshotActivity.SynchronizeSnapshots(ctx, []*datamodel.Pool{{BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"}}})
		assert.NoError(tt, err)
	})
	t.Run("SynchronizeSnapshots_GetOntapRestProviderForPool_Failed", func(tt *testing.T) {
		ctx := context.TODO()
		mockStorage := database.NewMockStorage(tt)

		originalGetOntapRestProviderForPool := GetOntapRestProviderForPool
		defer func() {
			GetOntapRestProviderForPool = originalGetOntapRestProviderForPool
		}()

		GetOntapRestProviderForPool = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) (vsa.Provider, error) {
			return nil, errors.New("failed to get ONTAP REST provider for pool")
		}

		syncSnapshotActivity := SyncSnapshotActivity{SE: mockStorage}
		err := syncSnapshotActivity.SynchronizeSnapshots(ctx, []*datamodel.Pool{{BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"}}})
		assert.Error(tt, err)
	})
	t.Run("SynchronizeSnapshots_GetOntapVolumes_Failed", func(tt *testing.T) {
		ctx := context.TODO()
		mockStorage := database.NewMockStorage(tt)
		mockProvider := vsa.NewMockProvider(tt)

		originalGetOntapRestProviderForPool := GetOntapRestProviderForPool
		defer func() {
			GetOntapRestProviderForPool = originalGetOntapRestProviderForPool
		}()

		GetOntapRestProviderForPool = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("GetVolumes").Return(nil, errors.New("failed to get volumes from ONTAP REST API"))

		syncSnapshotActivity := SyncSnapshotActivity{SE: mockStorage}
		err := syncSnapshotActivity.SynchronizeSnapshots(ctx, []*datamodel.Pool{{BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"}}})
		assert.Error(tt, err)
	})
	t.Run("SynchronizeSnapshots_GetOntapSnapshots_Failed", func(tt *testing.T) {
		ctx := context.TODO()
		mockStorage := database.NewMockStorage(tt)
		mockProvider := vsa.NewMockProvider(tt)

		originalGetOntapRestProviderForPool := GetOntapRestProviderForPool
		defer func() {
			GetOntapRestProviderForPool = originalGetOntapRestProviderForPool
		}()

		GetOntapRestProviderForPool = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("GetVolumes").Return([]*vsa.Volume{{Volume: ontaprestmodel.Volume{UUID: nillable.ToPointer("test-volume-uuid")}}}, nil)
		mockProvider.On("GetSnapshots", mock.Anything).Return(nil, errors.New("failed to get snapshots from ONTAP REST API"))
		mockStorage.On("GetVolumesByPoolID", mock.Anything, mock.Anything).Return(
			nil, errors.New("failed to get volumes from the database"))

		syncSnapshotActivity := SyncSnapshotActivity{SE: mockStorage}
		err := syncSnapshotActivity.SynchronizeSnapshots(ctx, []*datamodel.Pool{{BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"}}})
		assert.Error(tt, err)
	})
	t.Run("SynchronizeSnapshots_GetDatabaseVolumes_Failed", func(tt *testing.T) {
		ctx := context.TODO()
		mockStorage := database.NewMockStorage(tt)
		mockProvider := vsa.NewMockProvider(tt)

		originalGetOntapRestProviderForPool := GetOntapRestProviderForPool
		originalFilterOntapVolumesAndSnapshots := filterOntapVolumesAndSnapshots
		defer func() {
			GetOntapRestProviderForPool = originalGetOntapRestProviderForPool
			filterOntapVolumesAndSnapshots = originalFilterOntapVolumesAndSnapshots
		}()

		GetOntapRestProviderForPool = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) (vsa.Provider, error) {
			return mockProvider, nil
		}
		filterOntapVolumesAndSnapshots = func(volumes []*vsa.Volume, snapshots []*vsa.Snapshot) (map[string]*vsa.Volume, []*vsa.Snapshot) {
			return make(map[string]*vsa.Volume), []*vsa.Snapshot{}
		}

		mockProvider.On("GetVolumes").Return([]*vsa.Volume{{Volume: ontaprestmodel.Volume{UUID: nillable.ToPointer("test-volume-uuid")}}}, nil)
		mockProvider.On("GetSnapshots", mock.Anything).Return([]*vsa.Snapshot{{ExternalUUID: "snapshot-uuid-1"}}, nil)
		mockStorage.On("GetVolumesByPoolID", mock.Anything, mock.Anything).Return(
			nil, errors.New("failed to get volumes from the database"))

		syncSnapshotActivity := SyncSnapshotActivity{SE: mockStorage}
		err := syncSnapshotActivity.SynchronizeSnapshots(ctx, []*datamodel.Pool{{BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"}}})
		assert.Error(tt, err)
	})
	t.Run("SynchronizeSnapshots_GetDatabaseSnapshots_Failed", func(tt *testing.T) {
		ctx := context.TODO()
		mockStorage := database.NewMockStorage(tt)
		mockProvider := vsa.NewMockProvider(tt)

		originalGetOntapRestProviderForPool := GetOntapRestProviderForPool
		originalFilterOntapVolumesAndSnapshots := filterOntapVolumesAndSnapshots
		defer func() {
			GetOntapRestProviderForPool = originalGetOntapRestProviderForPool
			filterOntapVolumesAndSnapshots = originalFilterOntapVolumesAndSnapshots
		}()

		GetOntapRestProviderForPool = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) (vsa.Provider, error) {
			return mockProvider, nil
		}
		filterOntapVolumesAndSnapshots = func(volumes []*vsa.Volume, snapshots []*vsa.Snapshot) (map[string]*vsa.Volume, []*vsa.Snapshot) {
			return make(map[string]*vsa.Volume), []*vsa.Snapshot{}
		}

		mockProvider.On("GetVolumes").Return([]*vsa.Volume{{Volume: ontaprestmodel.Volume{UUID: nillable.ToPointer("test-volume-uuid")}}}, nil)
		mockProvider.On("GetSnapshots", mock.Anything).Return([]*vsa.Snapshot{{ExternalUUID: "snapshot-uuid-1"}}, nil)
		mockStorage.On("GetVolumesByPoolID", mock.Anything, mock.Anything).Return(
			[]*datamodel.Volume{{BaseModel: datamodel.BaseModel{ID: 1}, VolumeAttributes: &datamodel.VolumeAttributes{ExternalUUID: "test-volume-uuid"}}}, nil)
		mockStorage.On("GetSnapshotsByVolumeIDs", mock.Anything, mock.Anything).Return(
			nil, errors.New("failed to get snapshots from the database"))

		syncSnapshotActivity := SyncSnapshotActivity{SE: mockStorage}
		err := syncSnapshotActivity.SynchronizeSnapshots(ctx, []*datamodel.Pool{{BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"}}})
		assert.Error(tt, err)
	})
	t.Run("SynchronizeSnapshots_SyncDeletedSnapshotsToDatabase_Failed", func(tt *testing.T) {
		ctx := context.TODO()
		mockStorage := database.NewMockStorage(tt)
		mockProvider := vsa.NewMockProvider(tt)

		originalGetOntapRestProviderForPool := GetOntapRestProviderForPool
		originalFilterOntapVolumesAndSnapshots := filterOntapVolumesAndSnapshots
		originalProcessSnapshotSync := processSnapshotSync
		originalSyncDeletedSnapshotsToDatabase := syncDeletedSnapshotsToDatabase
		defer func() {
			GetOntapRestProviderForPool = originalGetOntapRestProviderForPool
			filterOntapVolumesAndSnapshots = originalFilterOntapVolumesAndSnapshots
			processSnapshotSync = originalProcessSnapshotSync
			syncDeletedSnapshotsToDatabase = originalSyncDeletedSnapshotsToDatabase
		}()

		GetOntapRestProviderForPool = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) (vsa.Provider, error) {
			return mockProvider, nil
		}
		filterOntapVolumesAndSnapshots = func(volumes []*vsa.Volume, snapshots []*vsa.Snapshot) (map[string]*vsa.Volume, []*vsa.Snapshot) {
			return make(map[string]*vsa.Volume), []*vsa.Snapshot{}
		}
		processSnapshotSync = func(ctx context.Context, ontapVolumeMap map[string]*vsa.Volume, ontapSnapshots []*vsa.Snapshot, dbVolumeMap map[string]*datamodel.Volume, dbSnapshots []*datamodel.Snapshot) (
			newSSMap map[string]*vsa.Snapshot, updatedSSMap map[string]*vsa.Snapshot, wronglyDeletedSnapshotsMap map[string]*vsa.Snapshot,
			newIDs []string, updatedIDs []string, deleteIDs []int64, wronglyDeletedIDs []string) {
			return
		}
		syncDeletedSnapshotsToDatabase = func(ctx context.Context, deleteIDs []int64, se database.Storage) ([]*datamodel.Snapshot, error) {
			return nil, errors.New("failed to sync snapshots to database")
		}

		mockProvider.On("GetVolumes").Return([]*vsa.Volume{{Volume: ontaprestmodel.Volume{UUID: nillable.ToPointer("test-volume-uuid")}}}, nil)
		mockProvider.On("GetSnapshots", mock.Anything).Return([]*vsa.Snapshot{{ExternalUUID: "snapshot-uuid-1"}}, nil)
		mockStorage.On("GetVolumesByPoolID", mock.Anything, mock.Anything).Return(
			[]*datamodel.Volume{{BaseModel: datamodel.BaseModel{ID: 1}, VolumeAttributes: &datamodel.VolumeAttributes{ExternalUUID: "test-volume-uuid"}}}, nil)
		mockStorage.On("GetSnapshotsByVolumeIDs", mock.Anything, mock.Anything).Return(
			[]*datamodel.Snapshot{{SnapshotAttributes: &datamodel.SnapshotAttributes{ExternalUUID: "test-snapshot-uuid"}}}, nil)

		syncSnapshotActivity := SyncSnapshotActivity{SE: mockStorage}
		err := syncSnapshotActivity.SynchronizeSnapshots(ctx, []*datamodel.Pool{{BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"}}})
		assert.Error(tt, err)
	})
	t.Run("SynchronizeSnapshots_SyncNewSnapshotsToDatabase_Failed", func(tt *testing.T) {
		ctx := context.TODO()
		mockStorage := database.NewMockStorage(tt)
		mockProvider := vsa.NewMockProvider(tt)

		originalGetOntapRestProviderForPool := GetOntapRestProviderForPool
		originalFilterOntapVolumesAndSnapshots := filterOntapVolumesAndSnapshots
		originalProcessSnapshotSync := processSnapshotSync
		originalSyncDeletedSnapshotsToDatabase := syncDeletedSnapshotsToDatabase
		originalSyncNewSnapshotsToDatabase := syncNewSnapshotsToDatabase
		defer func() {
			GetOntapRestProviderForPool = originalGetOntapRestProviderForPool
			filterOntapVolumesAndSnapshots = originalFilterOntapVolumesAndSnapshots
			processSnapshotSync = originalProcessSnapshotSync
			syncDeletedSnapshotsToDatabase = originalSyncDeletedSnapshotsToDatabase
			syncNewSnapshotsToDatabase = originalSyncNewSnapshotsToDatabase
		}()

		GetOntapRestProviderForPool = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) (vsa.Provider, error) {
			return mockProvider, nil
		}
		filterOntapVolumesAndSnapshots = func(volumes []*vsa.Volume, snapshots []*vsa.Snapshot) (map[string]*vsa.Volume, []*vsa.Snapshot) {
			return make(map[string]*vsa.Volume), []*vsa.Snapshot{}
		}
		processSnapshotSync = func(ctx context.Context, ontapVolumeMap map[string]*vsa.Volume, ontapSnapshots []*vsa.Snapshot, dbVolumeMap map[string]*datamodel.Volume, dbSnapshots []*datamodel.Snapshot) (
			newSSMap map[string]*vsa.Snapshot, updatedSSMap map[string]*vsa.Snapshot, wronglyDeletedSnapshotsMap map[string]*vsa.Snapshot,
			newIDs []string, updatedIDs []string, deleteIDs []int64, wronglyDeletedIDs []string) {
			return
		}
		syncDeletedSnapshotsToDatabase = func(ctx context.Context, deleteIDs []int64, se database.Storage) ([]*datamodel.Snapshot, error) {
			return nil, nil
		}
		syncNewSnapshotsToDatabase = func(ctx context.Context, newIds []string, newSSMap map[string]*vsa.Snapshot, se database.Storage, dbVolumeMap map[string]*datamodel.Volume, pool *datamodel.Pool) ([]*datamodel.Snapshot, error) {
			return nil, errors.New("failed to sync snapshots to database")
		}

		mockProvider.On("GetVolumes").Return([]*vsa.Volume{{Volume: ontaprestmodel.Volume{UUID: nillable.ToPointer("test-volume-uuid")}}}, nil)
		mockProvider.On("GetSnapshots", mock.Anything).Return([]*vsa.Snapshot{{ExternalUUID: "snapshot-uuid-1"}}, nil)
		mockStorage.On("GetVolumesByPoolID", mock.Anything, mock.Anything).Return(
			[]*datamodel.Volume{{BaseModel: datamodel.BaseModel{ID: 1}, VolumeAttributes: &datamodel.VolumeAttributes{ExternalUUID: "test-volume-uuid"}}}, nil)
		mockStorage.On("GetSnapshotsByVolumeIDs", mock.Anything, mock.Anything).Return(
			[]*datamodel.Snapshot{{SnapshotAttributes: &datamodel.SnapshotAttributes{ExternalUUID: "test-snapshot-uuid"}}}, nil)

		syncSnapshotActivity := SyncSnapshotActivity{SE: mockStorage}
		err := syncSnapshotActivity.SynchronizeSnapshots(ctx, []*datamodel.Pool{{BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"}}})
		assert.Error(tt, err)
	})
	t.Run("SynchronizeSnapshots_SyncUpdatedSnapshotsToDatabase_Failed", func(tt *testing.T) {
		ctx := context.TODO()
		mockStorage := database.NewMockStorage(tt)
		mockProvider := vsa.NewMockProvider(tt)

		originalGetOntapRestProviderForPool := GetOntapRestProviderForPool
		originalFilterOntapVolumesAndSnapshots := filterOntapVolumesAndSnapshots
		originalProcessSnapshotSync := processSnapshotSync
		originalSyncDeletedSnapshotsToDatabase := syncDeletedSnapshotsToDatabase
		originalSyncNewSnapshotsToDatabase := syncNewSnapshotsToDatabase
		originalSyncUpdatedSnapshotsToDatabase := syncUpdatedSnapshotsToDatabase
		defer func() {
			GetOntapRestProviderForPool = originalGetOntapRestProviderForPool
			filterOntapVolumesAndSnapshots = originalFilterOntapVolumesAndSnapshots
			processSnapshotSync = originalProcessSnapshotSync
			syncDeletedSnapshotsToDatabase = originalSyncDeletedSnapshotsToDatabase
			syncNewSnapshotsToDatabase = originalSyncNewSnapshotsToDatabase
			syncUpdatedSnapshotsToDatabase = originalSyncUpdatedSnapshotsToDatabase
		}()

		GetOntapRestProviderForPool = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) (vsa.Provider, error) {
			return mockProvider, nil
		}
		filterOntapVolumesAndSnapshots = func(volumes []*vsa.Volume, snapshots []*vsa.Snapshot) (map[string]*vsa.Volume, []*vsa.Snapshot) {
			return make(map[string]*vsa.Volume), []*vsa.Snapshot{}
		}
		processSnapshotSync = func(ctx context.Context, ontapVolumeMap map[string]*vsa.Volume, ontapSnapshots []*vsa.Snapshot, dbVolumeMap map[string]*datamodel.Volume, dbSnapshots []*datamodel.Snapshot) (
			newSSMap map[string]*vsa.Snapshot, updatedSSMap map[string]*vsa.Snapshot, wronglyDeletedSnapshotsMap map[string]*vsa.Snapshot,
			newIDs []string, updatedIDs []string, deleteIDs []int64, wronglyDeletedIDs []string) {
			return
		}
		syncDeletedSnapshotsToDatabase = func(ctx context.Context, deleteIDs []int64, se database.Storage) ([]*datamodel.Snapshot, error) {
			return nil, nil
		}
		syncNewSnapshotsToDatabase = func(ctx context.Context, newIds []string, newSSMap map[string]*vsa.Snapshot, se database.Storage, dbVolumeMap map[string]*datamodel.Volume, pool *datamodel.Pool) ([]*datamodel.Snapshot, error) {
			return nil, nil
		}
		syncUpdatedSnapshotsToDatabase = func(ctx context.Context, updatedIDs []string, updatedSSMap map[string]*vsa.Snapshot, se database.Storage, dbSnapshotsMap map[string]*datamodel.Snapshot) ([]*datamodel.Snapshot, error) {
			return nil, errors.New("failed to sync snapshots to database")
		}

		mockProvider.On("GetVolumes").Return([]*vsa.Volume{{Volume: ontaprestmodel.Volume{UUID: nillable.ToPointer("test-volume-uuid")}}}, nil)
		mockProvider.On("GetSnapshots", mock.Anything).Return([]*vsa.Snapshot{{ExternalUUID: "snapshot-uuid-1"}}, nil)
		mockStorage.On("GetVolumesByPoolID", mock.Anything, mock.Anything).Return(
			[]*datamodel.Volume{{BaseModel: datamodel.BaseModel{ID: 1}, VolumeAttributes: &datamodel.VolumeAttributes{ExternalUUID: "test-volume-uuid"}}}, nil)
		mockStorage.On("GetSnapshotsByVolumeIDs", mock.Anything, mock.Anything).Return(
			[]*datamodel.Snapshot{{SnapshotAttributes: &datamodel.SnapshotAttributes{ExternalUUID: "test-snapshot-uuid"}}}, nil)

		syncSnapshotActivity := SyncSnapshotActivity{SE: mockStorage}
		err := syncSnapshotActivity.SynchronizeSnapshots(ctx, []*datamodel.Pool{{BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"}}})
		assert.Error(tt, err)
	})
	t.Run("SynchronizeSnapshotsSyncWronglyDeletedSnapshotsToDatabase_Failed", func(tt *testing.T) {
		ctx := context.TODO()
		mockStorage := database.NewMockStorage(tt)
		mockProvider := vsa.NewMockProvider(tt)

		originalGetOntapRestProviderForPool := GetOntapRestProviderForPool
		originalFilterOntapVolumesAndSnapshots := filterOntapVolumesAndSnapshots
		originalProcessSnapshotSync := processSnapshotSync
		originalSyncDeletedSnapshotsToDatabase := syncDeletedSnapshotsToDatabase
		originalSyncNewSnapshotsToDatabase := syncNewSnapshotsToDatabase
		originalSyncUpdatedSnapshotsToDatabase := syncUpdatedSnapshotsToDatabase
		originalSyncUndeletedSnapshotsToDatabase := syncWronglyDeletedSnapshotsToDatabase
		defer func() {
			GetOntapRestProviderForPool = originalGetOntapRestProviderForPool
			filterOntapVolumesAndSnapshots = originalFilterOntapVolumesAndSnapshots
			processSnapshotSync = originalProcessSnapshotSync
			syncDeletedSnapshotsToDatabase = originalSyncDeletedSnapshotsToDatabase
			syncNewSnapshotsToDatabase = originalSyncNewSnapshotsToDatabase
			syncUpdatedSnapshotsToDatabase = originalSyncUpdatedSnapshotsToDatabase
			syncWronglyDeletedSnapshotsToDatabase = originalSyncUndeletedSnapshotsToDatabase
		}()

		GetOntapRestProviderForPool = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) (vsa.Provider, error) {
			return mockProvider, nil
		}
		filterOntapVolumesAndSnapshots = func(volumes []*vsa.Volume, snapshots []*vsa.Snapshot) (map[string]*vsa.Volume, []*vsa.Snapshot) {
			return make(map[string]*vsa.Volume), []*vsa.Snapshot{}
		}
		processSnapshotSync = func(ctx context.Context, ontapVolumeMap map[string]*vsa.Volume, ontapSnapshots []*vsa.Snapshot, dbVolumeMap map[string]*datamodel.Volume, dbSnapshots []*datamodel.Snapshot) (
			newSSMap map[string]*vsa.Snapshot, updatedSSMap map[string]*vsa.Snapshot, wronglyDeletedSnapshotsMap map[string]*vsa.Snapshot,
			newIDs []string, updatedIDs []string, deleteIDs []int64, wronglyDeletedIDs []string) {
			return
		}
		syncDeletedSnapshotsToDatabase = func(ctx context.Context, deleteIDs []int64, se database.Storage) ([]*datamodel.Snapshot, error) {
			return nil, nil
		}
		syncNewSnapshotsToDatabase = func(ctx context.Context, newIds []string, newSSMap map[string]*vsa.Snapshot, se database.Storage, dbVolumeMap map[string]*datamodel.Volume, pool *datamodel.Pool) ([]*datamodel.Snapshot, error) {
			return nil, nil
		}
		syncUpdatedSnapshotsToDatabase = func(ctx context.Context, updatedIDs []string, updatedSSMap map[string]*vsa.Snapshot, se database.Storage, dbSnapshotsMap map[string]*datamodel.Snapshot) ([]*datamodel.Snapshot, error) {
			return nil, nil
		}
		syncWronglyDeletedSnapshotsToDatabase = func(ctx context.Context, wronglyDeletedIds []string, wronglyDeletedSnapshotsMap map[string]*vsa.Snapshot, se database.Storage, dbSnapshotsMap map[string]*datamodel.Snapshot) ([]*datamodel.Snapshot, error) {
			return nil, errors.New("failed to sync wrongly deleted snapshots to database")
		}

		mockProvider.On("GetVolumes").Return([]*vsa.Volume{{Volume: ontaprestmodel.Volume{UUID: nillable.ToPointer("test-volume-uuid")}}}, nil)
		mockProvider.On("GetSnapshots", mock.Anything).Return([]*vsa.Snapshot{{ExternalUUID: "snapshot-uuid-1"}}, nil)
		mockStorage.On("GetVolumesByPoolID", mock.Anything, mock.Anything).Return(
			[]*datamodel.Volume{{BaseModel: datamodel.BaseModel{ID: 1}, VolumeAttributes: &datamodel.VolumeAttributes{ExternalUUID: "test-volume-uuid"}}}, nil)
		mockStorage.On("GetSnapshotsByVolumeIDs", mock.Anything, mock.Anything).Return(
			[]*datamodel.Snapshot{{SnapshotAttributes: &datamodel.SnapshotAttributes{ExternalUUID: "test-snapshot-uuid"}}}, nil)

		syncSnapshotActivity := SyncSnapshotActivity{SE: mockStorage}
		err := syncSnapshotActivity.SynchronizeSnapshots(ctx, []*datamodel.Pool{{BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"}}})
		assert.Error(tt, err)
	})

	t.Run("HydrationEnabledHydrateBatchSnapshotEnabled_HydrateBatchSnapshotsToCCFEFailed", func(tt *testing.T) {
		ctx := context.TODO()
		mockStorage := database.NewMockStorage(tt)
		mockProvider := vsa.NewMockProvider(tt)

		originalHydrationEnabled := hydrationEnabled
		originalHydrateBatchSnapshotsToCCFE := hydrateBatchSnapshotsToCCFE
		originalGetOntapRestProviderForPool := GetOntapRestProviderForPool
		originalFilterOntapVolumesAndSnapshots := filterOntapVolumesAndSnapshots
		originalProcessSnapshotSync := processSnapshotSync
		originalSyncDeletedSnapshotsToDatabase := syncDeletedSnapshotsToDatabase
		originalSyncNewSnapshotsToDatabase := syncNewSnapshotsToDatabase
		originalSyncUpdatedSnapshotsToDatabase := syncUpdatedSnapshotsToDatabase
		originalSyncUndeletedSnapshotsToDatabase := syncWronglyDeletedSnapshotsToDatabase
		defer func() {
			GetOntapRestProviderForPool = originalGetOntapRestProviderForPool
			filterOntapVolumesAndSnapshots = originalFilterOntapVolumesAndSnapshots
			processSnapshotSync = originalProcessSnapshotSync
			syncDeletedSnapshotsToDatabase = originalSyncDeletedSnapshotsToDatabase
			syncNewSnapshotsToDatabase = originalSyncNewSnapshotsToDatabase
			syncUpdatedSnapshotsToDatabase = originalSyncUpdatedSnapshotsToDatabase
			syncWronglyDeletedSnapshotsToDatabase = originalSyncUndeletedSnapshotsToDatabase
			hydrateBatchSnapshotsToCCFE = originalHydrateBatchSnapshotsToCCFE
			hydrationEnabled = originalHydrationEnabled
			hydrateBatchSnapshotsToCCFE = originalHydrateBatchSnapshotsToCCFE
		}()
		hydrationEnabled = true

		GetOntapRestProviderForPool = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) (vsa.Provider, error) {
			return mockProvider, nil
		}
		filterOntapVolumesAndSnapshots = func(volumes []*vsa.Volume, snapshots []*vsa.Snapshot) (map[string]*vsa.Volume, []*vsa.Snapshot) {
			return make(map[string]*vsa.Volume), []*vsa.Snapshot{}
		}
		processSnapshotSync = func(ctx context.Context, ontapVolumeMap map[string]*vsa.Volume, ontapSnapshots []*vsa.Snapshot, dbVolumeMap map[string]*datamodel.Volume, dbSnapshots []*datamodel.Snapshot) (
			newSSMap map[string]*vsa.Snapshot, updatedSSMap map[string]*vsa.Snapshot, wronglyDeletedSnapshotsMap map[string]*vsa.Snapshot,
			newIDs []string, updatedIDs []string, deleteIDs []int64, wronglyDeletedIDs []string) {
			return
		}
		syncDeletedSnapshotsToDatabase = func(ctx context.Context, deleteIDs []int64, se database.Storage) ([]*datamodel.Snapshot, error) {
			return nil, nil
		}
		syncNewSnapshotsToDatabase = func(ctx context.Context, newIds []string, newSSMap map[string]*vsa.Snapshot, se database.Storage, dbVolumeMap map[string]*datamodel.Volume, pool *datamodel.Pool) ([]*datamodel.Snapshot, error) {
			return nil, nil
		}
		syncUpdatedSnapshotsToDatabase = func(ctx context.Context, updatedIDs []string, updatedSSMap map[string]*vsa.Snapshot, se database.Storage, dbSnapshotsMap map[string]*datamodel.Snapshot) ([]*datamodel.Snapshot, error) {
			return nil, nil
		}
		syncWronglyDeletedSnapshotsToDatabase = func(ctx context.Context, wronglyDeletedIds []string, wronglyDeletedSnapshotsMap map[string]*vsa.Snapshot, se database.Storage, dbSnapshotsMap map[string]*datamodel.Snapshot) ([]*datamodel.Snapshot, error) {
			return nil, nil
		}

		hydrateBatchSnapshotsToCCFE = func(ctx context.Context, createdSnapshots []*datamodel.Snapshot, deletedSnapshots []*datamodel.Snapshot) error {
			return errors.New("failed to hydrate batch snapshots to CCFE")
		}

		mockProvider.On("GetVolumes").Return([]*vsa.Volume{{Volume: ontaprestmodel.Volume{UUID: nillable.ToPointer("test-volume-uuid")}}}, nil)
		mockProvider.On("GetSnapshots", mock.Anything).Return([]*vsa.Snapshot{{ExternalUUID: "snapshot-uuid-1"}}, nil)
		mockStorage.On("GetVolumesByPoolID", mock.Anything, mock.Anything).Return(
			[]*datamodel.Volume{{BaseModel: datamodel.BaseModel{ID: 1}, VolumeAttributes: &datamodel.VolumeAttributes{ExternalUUID: "test-volume-uuid"}}}, nil)
		mockStorage.On("GetSnapshotsByVolumeIDs", mock.Anything, mock.Anything).Return(
			[]*datamodel.Snapshot{{SnapshotAttributes: &datamodel.SnapshotAttributes{ExternalUUID: "test-snapshot-uuid"}}}, nil)

		syncSnapshotActivity := SyncSnapshotActivity{SE: mockStorage}
		err := syncSnapshotActivity.SynchronizeSnapshots(ctx, []*datamodel.Pool{{BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"}}})
		assert.Error(tt, err)
	})
	t.Run("HydrationEnabledHydrateBatchSnapshotDisabled_HydrateSnapshotsToCCFEFailed", func(tt *testing.T) {
		ctx := context.TODO()
		mockStorage := database.NewMockStorage(tt)
		mockProvider := vsa.NewMockProvider(tt)

		originalHydrationEnabled := hydrationEnabled
		hydrationEnabled = true
		originalGetOntapRestProviderForPool := GetOntapRestProviderForPool
		originalFilterOntapVolumesAndSnapshots := filterOntapVolumesAndSnapshots
		originalProcessSnapshotSync := processSnapshotSync
		originalSyncDeletedSnapshotsToDatabase := syncDeletedSnapshotsToDatabase
		originalSyncNewSnapshotsToDatabase := syncNewSnapshotsToDatabase
		originalSyncUpdatedSnapshotsToDatabase := syncUpdatedSnapshotsToDatabase
		originalSyncUndeletedSnapshotsToDatabase := syncWronglyDeletedSnapshotsToDatabase
		defer func() {
			GetOntapRestProviderForPool = originalGetOntapRestProviderForPool
			filterOntapVolumesAndSnapshots = originalFilterOntapVolumesAndSnapshots
			processSnapshotSync = originalProcessSnapshotSync
			syncDeletedSnapshotsToDatabase = originalSyncDeletedSnapshotsToDatabase
			syncNewSnapshotsToDatabase = originalSyncNewSnapshotsToDatabase
			syncUpdatedSnapshotsToDatabase = originalSyncUpdatedSnapshotsToDatabase
			syncWronglyDeletedSnapshotsToDatabase = originalSyncUndeletedSnapshotsToDatabase
			hydrationEnabled = originalHydrationEnabled
		}()

		GetOntapRestProviderForPool = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) (vsa.Provider, error) {
			return mockProvider, nil
		}
		filterOntapVolumesAndSnapshots = func(volumes []*vsa.Volume, snapshots []*vsa.Snapshot) (map[string]*vsa.Volume, []*vsa.Snapshot) {
			return make(map[string]*vsa.Volume), []*vsa.Snapshot{}
		}
		processSnapshotSync = func(ctx context.Context, ontapVolumeMap map[string]*vsa.Volume, ontapSnapshots []*vsa.Snapshot, dbVolumeMap map[string]*datamodel.Volume, dbSnapshots []*datamodel.Snapshot) (
			newSSMap map[string]*vsa.Snapshot, updatedSSMap map[string]*vsa.Snapshot, wronglyDeletedSnapshotsMap map[string]*vsa.Snapshot,
			newIDs []string, updatedIDs []string, deleteIDs []int64, wronglyDeletedIDs []string) {
			return
		}
		syncDeletedSnapshotsToDatabase = func(ctx context.Context, deleteIDs []int64, se database.Storage) ([]*datamodel.Snapshot, error) {
			return nil, nil
		}
		syncNewSnapshotsToDatabase = func(ctx context.Context, newIds []string, newSSMap map[string]*vsa.Snapshot, se database.Storage, dbVolumeMap map[string]*datamodel.Volume, pool *datamodel.Pool) ([]*datamodel.Snapshot, error) {
			return nil, nil
		}
		syncUpdatedSnapshotsToDatabase = func(ctx context.Context, updatedIDs []string, updatedSSMap map[string]*vsa.Snapshot, se database.Storage, dbSnapshotsMap map[string]*datamodel.Snapshot) ([]*datamodel.Snapshot, error) {
			return nil, nil
		}
		syncWronglyDeletedSnapshotsToDatabase = func(ctx context.Context, wronglyDeletedIds []string, wronglyDeletedSnapshotsMap map[string]*vsa.Snapshot, se database.Storage, dbSnapshotsMap map[string]*datamodel.Snapshot) ([]*datamodel.Snapshot, error) {
			return nil, nil
		}

		mockProvider.On("GetVolumes").Return([]*vsa.Volume{{Volume: ontaprestmodel.Volume{UUID: nillable.ToPointer("test-volume-uuid")}}}, nil)
		mockProvider.On("GetSnapshots", mock.Anything).Return([]*vsa.Snapshot{{ExternalUUID: "snapshot-uuid-1"}}, nil)
		mockStorage.On("GetVolumesByPoolID", mock.Anything, mock.Anything).Return(
			[]*datamodel.Volume{{BaseModel: datamodel.BaseModel{ID: 1}, VolumeAttributes: &datamodel.VolumeAttributes{ExternalUUID: "test-volume-uuid"}}}, nil)
		mockStorage.On("GetSnapshotsByVolumeIDs", mock.Anything, mock.Anything).Return(
			[]*datamodel.Snapshot{{SnapshotAttributes: &datamodel.SnapshotAttributes{ExternalUUID: "test-snapshot-uuid"}}}, nil)

		syncSnapshotActivity := SyncSnapshotActivity{SE: mockStorage}
		err := syncSnapshotActivity.SynchronizeSnapshots(ctx, []*datamodel.Pool{{BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"}}})
		assert.Error(tt, err)
	})
	t.Run("HydrationDisabled", func(tt *testing.T) {
		ctx := context.TODO()
		mockStorage := database.NewMockStorage(tt)
		mockProvider := vsa.NewMockProvider(tt)

		originalHydrationEnabled := hydrationEnabled
		originalGetOntapRestProviderForPool := GetOntapRestProviderForPool
		originalFilterOntapVolumesAndSnapshots := filterOntapVolumesAndSnapshots
		originalProcessSnapshotSync := processSnapshotSync
		originalSyncDeletedSnapshotsToDatabase := syncDeletedSnapshotsToDatabase
		originalSyncNewSnapshotsToDatabase := syncNewSnapshotsToDatabase
		originalSyncUpdatedSnapshotsToDatabase := syncUpdatedSnapshotsToDatabase
		originalSyncUndeletedSnapshotsToDatabase := syncWronglyDeletedSnapshotsToDatabase
		defer func() {
			GetOntapRestProviderForPool = originalGetOntapRestProviderForPool
			filterOntapVolumesAndSnapshots = originalFilterOntapVolumesAndSnapshots
			processSnapshotSync = originalProcessSnapshotSync
			syncDeletedSnapshotsToDatabase = originalSyncDeletedSnapshotsToDatabase
			syncNewSnapshotsToDatabase = originalSyncNewSnapshotsToDatabase
			syncUpdatedSnapshotsToDatabase = originalSyncUpdatedSnapshotsToDatabase
			syncWronglyDeletedSnapshotsToDatabase = originalSyncUndeletedSnapshotsToDatabase
			hydrationEnabled = originalHydrationEnabled
		}()

		hydrationEnabled = false

		GetOntapRestProviderForPool = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) (vsa.Provider, error) {
			return mockProvider, nil
		}
		filterOntapVolumesAndSnapshots = func(volumes []*vsa.Volume, snapshots []*vsa.Snapshot) (map[string]*vsa.Volume, []*vsa.Snapshot) {
			return make(map[string]*vsa.Volume), []*vsa.Snapshot{}
		}
		processSnapshotSync = func(ctx context.Context, ontapVolumeMap map[string]*vsa.Volume, ontapSnapshots []*vsa.Snapshot, dbVolumeMap map[string]*datamodel.Volume, dbSnapshots []*datamodel.Snapshot) (
			newSSMap map[string]*vsa.Snapshot, updatedSSMap map[string]*vsa.Snapshot, wronglyDeletedSnapshotsMap map[string]*vsa.Snapshot,
			newIDs []string, updatedIDs []string, deleteIDs []int64, wronglyDeletedIDs []string) {
			return
		}
		syncDeletedSnapshotsToDatabase = func(ctx context.Context, deleteIDs []int64, se database.Storage) ([]*datamodel.Snapshot, error) {
			return nil, nil
		}
		syncNewSnapshotsToDatabase = func(ctx context.Context, newIds []string, newSSMap map[string]*vsa.Snapshot, se database.Storage, dbVolumeMap map[string]*datamodel.Volume, pool *datamodel.Pool) ([]*datamodel.Snapshot, error) {
			return nil, nil
		}
		syncUpdatedSnapshotsToDatabase = func(ctx context.Context, updatedIDs []string, updatedSSMap map[string]*vsa.Snapshot, se database.Storage, dbSnapshotsMap map[string]*datamodel.Snapshot) ([]*datamodel.Snapshot, error) {
			return nil, nil
		}
		syncWronglyDeletedSnapshotsToDatabase = func(ctx context.Context, wronglyDeletedIds []string, wronglyDeletedSnapshotsMap map[string]*vsa.Snapshot, se database.Storage, dbSnapshotsMap map[string]*datamodel.Snapshot) ([]*datamodel.Snapshot, error) {
			return nil, nil
		}

		mockProvider.On("GetVolumes").Return([]*vsa.Volume{{Volume: ontaprestmodel.Volume{UUID: nillable.ToPointer("test-volume-uuid")}}}, nil)
		mockProvider.On("GetSnapshots", mock.Anything).Return([]*vsa.Snapshot{{ExternalUUID: "snapshot-uuid-1"}}, nil)
		mockStorage.On("GetVolumesByPoolID", mock.Anything, mock.Anything).Return(
			[]*datamodel.Volume{{BaseModel: datamodel.BaseModel{ID: 1}, VolumeAttributes: &datamodel.VolumeAttributes{ExternalUUID: "test-volume-uuid"}}}, nil)
		mockStorage.On("GetSnapshotsByVolumeIDs", mock.Anything, mock.Anything).Return(
			[]*datamodel.Snapshot{{SnapshotAttributes: &datamodel.SnapshotAttributes{ExternalUUID: "test-snapshot-uuid"}}}, nil)

		syncSnapshotActivity := SyncSnapshotActivity{SE: mockStorage}
		err := syncSnapshotActivity.SynchronizeSnapshots(ctx, []*datamodel.Pool{{BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"}}})
		assert.NoError(tt, err)
	})

	t.Run("TestSynchronizeSnapshots_ProceedAfterError", func(tt *testing.T) {
		ctx := context.TODO()
		mockStorage := database.NewMockStorage(t)
		mockProvider := vsa.NewMockProvider(t)

		originalGetOntapRestProviderForPool := GetOntapRestProviderForPool
		originalFilterOntapVolumesAndSnapshots := filterOntapVolumesAndSnapshots
		originalProcessSnapshotSync := processSnapshotSync
		originalSyncDeletedSnapshotsToDatabase := syncDeletedSnapshotsToDatabase
		originalSyncNewSnapshotsToDatabase := syncNewSnapshotsToDatabase
		originalSyncUpdatedSnapshotsToDatabase := syncUpdatedSnapshotsToDatabase
		originalSyncUndeletedSnapshotsToDatabase := syncWronglyDeletedSnapshotsToDatabase
		defer func() {
			GetOntapRestProviderForPool = originalGetOntapRestProviderForPool
			filterOntapVolumesAndSnapshots = originalFilterOntapVolumesAndSnapshots
			processSnapshotSync = originalProcessSnapshotSync
			syncDeletedSnapshotsToDatabase = originalSyncDeletedSnapshotsToDatabase
			syncNewSnapshotsToDatabase = originalSyncNewSnapshotsToDatabase
			syncUpdatedSnapshotsToDatabase = originalSyncUpdatedSnapshotsToDatabase
			syncWronglyDeletedSnapshotsToDatabase = originalSyncUndeletedSnapshotsToDatabase
		}()

		GetOntapRestProviderForPool = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) (vsa.Provider, error) {
			return mockProvider, nil
		}
		filterOntapVolumesAndSnapshots = func(volumes []*vsa.Volume, snapshots []*vsa.Snapshot) (map[string]*vsa.Volume, []*vsa.Snapshot) {
			return make(map[string]*vsa.Volume), []*vsa.Snapshot{}
		}
		processSnapshotSync = func(ctx context.Context, ontapVolumeMap map[string]*vsa.Volume, ontapSnapshots []*vsa.Snapshot, dbVolumeMap map[string]*datamodel.Volume, dbSnapshots []*datamodel.Snapshot) (
			newSSMap map[string]*vsa.Snapshot, updatedSSMap map[string]*vsa.Snapshot, wronglyDeletedSnapshotsMap map[string]*vsa.Snapshot,
			newIDs []string, updatedIDs []string, deleteIDs []int64, wronglyDeletedIDs []string) {
			return
		}
		syncDeletedSnapshotsToDatabase = func(ctx context.Context, deleteIDs []int64, se database.Storage) ([]*datamodel.Snapshot, error) {
			return nil, nil
		}
		syncNewSnapshotsToDatabase = func(ctx context.Context, newIds []string, newSSMap map[string]*vsa.Snapshot, se database.Storage, dbVolumeMap map[string]*datamodel.Volume, pool *datamodel.Pool) ([]*datamodel.Snapshot, error) {
			return nil, nil
		}
		syncUpdatedSnapshotsToDatabase = func(ctx context.Context, updatedIDs []string, updatedSSMap map[string]*vsa.Snapshot, se database.Storage, dbSnapshotsMap map[string]*datamodel.Snapshot) ([]*datamodel.Snapshot, error) {
			return nil, nil
		}
		syncWronglyDeletedSnapshotsToDatabase = func(ctx context.Context, wronglyDeletedIds []string, wronglyDeletedSnapshotsMap map[string]*vsa.Snapshot, se database.Storage, dbSnapshotsMap map[string]*datamodel.Snapshot) ([]*datamodel.Snapshot, error) {
			return nil, nil
		}

		mockProvider.On("GetVolumes").Return([]*vsa.Volume{
			{Volume: ontaprestmodel.Volume{UUID: nillable.ToPointer("test-volume-uuid-1")}},
			{Volume: ontaprestmodel.Volume{UUID: nillable.ToPointer("test-volume-uuid-2")}},
		}, nil)

		mockProvider.On("GetSnapshots", "test-volume-uuid-1").Return(nil, errors.New("failed to get snapshots from ONTAP REST API for volume 1"))
		mockProvider.On("GetSnapshots", "test-volume-uuid-2").Return([]*vsa.Snapshot{{ExternalUUID: "snapshot-uuid-2"}}, nil)

		mockStorage.On("GetVolumesByPoolID", mock.Anything, mock.Anything).Return(
			[]*datamodel.Volume{
				{BaseModel: datamodel.BaseModel{ID: 1}, VolumeAttributes: &datamodel.VolumeAttributes{ExternalUUID: "test-volume-uuid-1"}},
				{BaseModel: datamodel.BaseModel{ID: 2}, VolumeAttributes: &datamodel.VolumeAttributes{ExternalUUID: "test-volume-uuid-2"}},
			}, nil)
		mockStorage.On("GetSnapshotsByVolumeIDs", mock.Anything, mock.Anything).Return(
			[]*datamodel.Snapshot{
				{SnapshotAttributes: &datamodel.SnapshotAttributes{ExternalUUID: "test-snapshot-uuid-1"}},
				{SnapshotAttributes: &datamodel.SnapshotAttributes{ExternalUUID: "test-snapshot-uuid-2"}},
			}, nil)

		syncSnapshotActivity := SyncSnapshotActivity{SE: mockStorage}
		err := syncSnapshotActivity.SynchronizeSnapshots(ctx, []*datamodel.Pool{{BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"}}})
		assert.Error(t, err)
		// Ensure that the error is logged, but the process continues for the second volume
		mockProvider.AssertCalled(t, "GetSnapshots", "test-volume-uuid-2")
	})
}

func TestFilterOntapVolumesAndSnapshots(t *testing.T) {
	volumes := []*vsa.Volume{
		{
			Volume: ontaprestmodel.Volume{
				IsSvmRoot: nillable.ToPointer(true),
			},
			ExternalUUID: "some-external-uuid",
		},
		{
			Volume: ontaprestmodel.Volume{
				Name:      nillable.ToPointer("test-volume"),
				UUID:      nillable.ToPointer("test-volume-uuid"),
				Style:     nillable.ToPointer("flexvol"),
				IsSvmRoot: nillable.ToPointer(false),
				Svm: &ontaprestmodel.VolumeInlineSvm{
					Name: nillable.ToPointer("test-volume-svm"),
				},
			},
			ExternalUUID: "some-external-uuid",
		},
		{
			Volume: ontaprestmodel.Volume{
				Name:      nillable.ToPointer("test-flexgroup-constituent-volume"),
				UUID:      nillable.ToPointer("flex-group-constituent-uuid"),
				Style:     nillable.ToPointer(FlexGroupConstituent),
				IsSvmRoot: nillable.ToPointer(false),
				Svm: &ontaprestmodel.VolumeInlineSvm{
					Name: nillable.ToPointer("test-flexgroup-constituent-volume-svm"),
				},
			},
			ExternalUUID: "some-external-uuid",
		},
	}

	snapshots := []*vsa.Snapshot{
		{
			Snapshot: ontaprestmodel.Snapshot{
				Name: nillable.ToPointer("orphaned-snapshot"),
				ProvenanceVolume: &ontaprestmodel.SnapshotInlineProvenanceVolume{
					UUID: nillable.ToPointer("orphaned-snapshot-uuid"),
				},
				Volume: &ontaprestmodel.SnapshotInlineVolume{
					Name: nillable.ToPointer("non-existent-volume"),
				},
				Svm: &ontaprestmodel.SnapshotInlineSvm{
					Name: nillable.ToPointer("orphaned-svm"),
				},
			},
		},
		{
			Snapshot: ontaprestmodel.Snapshot{
				Name: nillable.ToPointer("hourly-test-snapshot"),
				ProvenanceVolume: &ontaprestmodel.SnapshotInlineProvenanceVolume{
					UUID: nillable.ToPointer("test-volume-uuid"),
				},
				Volume: &ontaprestmodel.SnapshotInlineVolume{
					Name: nillable.ToPointer("test-volume"),
				},
				Svm: &ontaprestmodel.SnapshotInlineSvm{
					Name: nillable.ToPointer("test-svm"),
				},
				SnapmirrorLabel: nillable.ToPointer("scheduled"),
			},
		},
		{
			Snapshot: ontaprestmodel.Snapshot{
				Name: nillable.ToPointer("test-snapshot"),
				ProvenanceVolume: &ontaprestmodel.SnapshotInlineProvenanceVolume{
					UUID: nillable.ToPointer("flex-group-constituent-uuid"),
				},
				Volume: &ontaprestmodel.SnapshotInlineVolume{
					Name: nillable.ToPointer("test-flexgroup-constituent-volume"),
				},
				Svm: &ontaprestmodel.SnapshotInlineSvm{
					Name: nillable.ToPointer("test-svm"),
				},
				SnapmirrorLabel: nillable.ToPointer("scheduled"),
			},
		},
		{
			Snapshot: ontaprestmodel.Snapshot{
				Name: nillable.ToPointer("scheduled-backup-shashank-0000-11-22-547823"),
				ProvenanceVolume: &ontaprestmodel.SnapshotInlineProvenanceVolume{
					UUID: nillable.ToPointer("test-volume-uuid"),
				},
				Volume: &ontaprestmodel.SnapshotInlineVolume{
					Name: nillable.ToPointer("test-volume"),
				},
				Svm: &ontaprestmodel.SnapshotInlineSvm{
					Name: nillable.ToPointer("test-vm"),
				},
			},
		},
		{
			Snapshot: ontaprestmodel.Snapshot{
				Name: nillable.ToPointer("test-snapshot"),
				ProvenanceVolume: &ontaprestmodel.SnapshotInlineProvenanceVolume{
					UUID: nillable.ToPointer("test-volume-uuid"),
				},
				Volume: &ontaprestmodel.SnapshotInlineVolume{
					Name: nillable.ToPointer("test-volume"),
				},
				Svm: &ontaprestmodel.SnapshotInlineSvm{
					Name: nillable.ToPointer("test-svm"),
				},
			},
		},
	}

	volumeMap, filteredSnapshots := filterOntapVolumesAndSnapshots(volumes, snapshots)
	assert.Len(t, volumeMap, 4)
	assert.Len(t, filteredSnapshots, 3)
}

func TestSyncWronglyDeletedSnapshotsToDatabase(t *testing.T) {
	ctx := context.TODO()
	t.Run("SyncWronglyDeletedSnapshotsToDatabaseSuccess", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		dbSnapshotsMap := map[string]*datamodel.Snapshot{
			"wrongly-deleted-snapshot-uuid": {
				BaseModel:    datamodel.BaseModel{ID: 1},
				State:        models.LifeCycleStateDeleted,
				StateDetails: models.LifeCycleStateDeletedDetails,
			},
		}
		wronglyDeletedSnapshotsMap := map[string]*vsa.Snapshot{
			"snapshot-uuid.volume-uuid": {
				Snapshot: ontaprestmodel.Snapshot{
					Name: nillable.ToPointer("wrongly-deleted-snapshot"),
					ProvenanceVolume: &ontaprestmodel.SnapshotInlineProvenanceVolume{
						UUID: nillable.ToPointer("test-volume-uuid"),
					},
					Volume: &ontaprestmodel.SnapshotInlineVolume{
						Name: nillable.ToPointer("test-volume"),
					},
					Svm: &ontaprestmodel.SnapshotInlineSvm{
						Name: nillable.ToPointer("test-svm"),
					},
				},
				ExternalUUID: "wrongly-deleted-snapshot-uuid",
			},
		}

		mockStorage.On("GetWronglyDeletedSnapshot", mock.Anything, mock.Anything).Return(
			&datamodel.Snapshot{State: models.LifeCycleStateDeleted, StateDetails: models.LifeCycleStateDeletedDetails}, nil)
		mockStorage.On("UnDeleteSnapshot", ctx, mock.Anything).Return(nil)

		_, err := syncWronglyDeletedSnapshotsToDatabase(ctx, []string{"snapshot-uuid.volume-uuid"}, wronglyDeletedSnapshotsMap, mockStorage, dbSnapshotsMap)
		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
	})
	t.Run("SyncWronglyDeletedSnapshotsToDatabaseFailed", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		dbSnapshotsMap := map[string]*datamodel.Snapshot{
			"wrongly-deleted-snapshot-uuid": {
				BaseModel:    datamodel.BaseModel{ID: 1},
				State:        models.LifeCycleStateDeleted,
				StateDetails: models.LifeCycleStateDeletedDetails,
			},
		}
		wronglyDeletedSnapshotsMap := map[string]*vsa.Snapshot{
			"snapshot-uuid.volume-uuid": {
				Snapshot: ontaprestmodel.Snapshot{
					Name: nillable.ToPointer("wrongly-deleted-snapshot"),
					ProvenanceVolume: &ontaprestmodel.SnapshotInlineProvenanceVolume{
						UUID: nillable.ToPointer("test-volume-uuid"),
					},
					Volume: &ontaprestmodel.SnapshotInlineVolume{
						Name: nillable.ToPointer("test-volume"),
					},
					Svm: &ontaprestmodel.SnapshotInlineSvm{
						Name: nillable.ToPointer("test-svm"),
					},
				},
				ExternalUUID: "wrongly-deleted-snapshot-uuid",
			},
		}

		mockStorage.On("GetWronglyDeletedSnapshot", mock.Anything, mock.Anything).Return(
			&datamodel.Snapshot{State: models.LifeCycleStateDeleted, StateDetails: models.LifeCycleStateDeletedDetails}, nil)
		mockStorage.On("UnDeleteSnapshot", ctx, mock.Anything).Return(errors.New("could not update snapshot"))

		_, err := syncWronglyDeletedSnapshotsToDatabase(ctx, []string{"snapshot-uuid.volume-uuid"}, wronglyDeletedSnapshotsMap, mockStorage, dbSnapshotsMap)
		assert.Error(t, err)
		mockStorage.AssertExpectations(t)
	})
}

func TestSyncUpdatedSnapshotsToDatabase(t *testing.T) {
	ctx := context.TODO()
	t.Run("SyncUpdatedSnapshotsToDatabaseSuccess", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		dbSnapshotsMap := map[string]*datamodel.Snapshot{
			"updated-snapshot-uuid": {
				BaseModel:    datamodel.BaseModel{ID: 1},
				State:        models.LifeCycleStateREADY,
				StateDetails: models.LifeCycleStateReadyDetails,
			},
		}
		updatedSSMap := map[string]*vsa.Snapshot{
			"snapshot-uuid.volume-uuid": {
				Snapshot: ontaprestmodel.Snapshot{
					Name: nillable.ToPointer("updated-snapshot"),
					ProvenanceVolume: &ontaprestmodel.SnapshotInlineProvenanceVolume{
						UUID: nillable.ToPointer("test-volume-uuid"),
					},
					Volume: &ontaprestmodel.SnapshotInlineVolume{
						Name: nillable.ToPointer("test-volume"),
					},
					Svm: &ontaprestmodel.SnapshotInlineSvm{
						Name: nillable.ToPointer("test-svm"),
					},
				},
				ExternalUUID:           "updated-snapshot-uuid",
				SizeInBytes:            122880,
				LogicalSizeUsedInBytes: 122880,
			},
		}

		mockStorage.On("UpdateSnapshot", ctx, mock.Anything).Return(
			&datamodel.Snapshot{BaseModel: datamodel.BaseModel{ID: 1}}, nil)

		updatedDbSnapshots, err := syncUpdatedSnapshotsToDatabase(ctx, []string{"snapshot-uuid.volume-uuid"}, updatedSSMap, mockStorage, dbSnapshotsMap)
		assert.NoError(t, err)
		assert.Len(t, updatedDbSnapshots, 1)
		mockStorage.AssertExpectations(t)
	})
	t.Run("SyncUpdatedSnapshotsToDatabaseFailed", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		dbSnapshotsMap := map[string]*datamodel.Snapshot{
			"updated-snapshot-uuid": {
				BaseModel:    datamodel.BaseModel{ID: 1},
				State:        models.LifeCycleStateREADY,
				StateDetails: models.LifeCycleStateReadyDetails,
			},
		}
		updatedSSMap := map[string]*vsa.Snapshot{
			"snapshot-uuid.volume-uuid": {
				Snapshot: ontaprestmodel.Snapshot{
					Name: nillable.ToPointer("updated-snapshot"),
					ProvenanceVolume: &ontaprestmodel.SnapshotInlineProvenanceVolume{
						UUID: nillable.ToPointer("test-volume-uuid"),
					},
					Volume: &ontaprestmodel.SnapshotInlineVolume{
						Name: nillable.ToPointer("test-volume"),
					},
					Svm: &ontaprestmodel.SnapshotInlineSvm{
						Name: nillable.ToPointer("test-svm"),
					},
				},
				ExternalUUID:           "updated-snapshot-uuid",
				SizeInBytes:            122880,
				LogicalSizeUsedInBytes: 122880,
			},
		}

		mockStorage.On("UpdateSnapshot", ctx, mock.Anything).Return(
			nil, errors.New("could not update snapshot"))

		updatedDbSnapshot, err := syncUpdatedSnapshotsToDatabase(ctx, []string{"snapshot-uuid.volume-uuid"}, updatedSSMap, mockStorage, dbSnapshotsMap)
		assert.Error(t, err)
		assert.Nil(t, updatedDbSnapshot)
		mockStorage.AssertExpectations(t)
	})
}

func TestSyncNewSnapshotsToDatabase(t *testing.T) {
	ctx := context.TODO()
	t.Run("SyncUpdatedSnapshotsToDatabaseSuccess", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		dbVolumeMap := map[string]*datamodel.Volume{
			"test-volume-uuid": {
				BaseModel: datamodel.BaseModel{ID: 1},
			},
		}
		pool := &datamodel.Pool{AccountID: 1}
		newSSMap := map[string]*vsa.Snapshot{
			"snapshot-uuid.volume-uuid": {
				Snapshot: ontaprestmodel.Snapshot{
					Name: nillable.ToPointer("new-snapshot"),
					ProvenanceVolume: &ontaprestmodel.SnapshotInlineProvenanceVolume{
						UUID: nillable.ToPointer("test-volume-uuid"),
					},
					Volume: &ontaprestmodel.SnapshotInlineVolume{
						Name: nillable.ToPointer("test-volume"),
					},
					Svm: &ontaprestmodel.SnapshotInlineSvm{
						Name: nillable.ToPointer("test-svm"),
					},
				},
				ExternalUUID:           "new-snapshot-uuid",
				ExternalVolumeUUID:     "test-volume-uuid",
				SizeInBytes:            122880,
				LogicalSizeUsedInBytes: 122880,
			},
		}

		mockStorage.On("CreatingSnapshot", mock.Anything, mock.Anything).Return(
			&datamodel.Snapshot{
				BaseModel:    datamodel.BaseModel{ID: 1},
				State:        models.LifeCycleStateCreating,
				StateDetails: models.LifeCycleStateCreatingDetails,
			}, nil)
		mockStorage.On("UpdateSnapshot", ctx, mock.Anything).Return(
			&datamodel.Snapshot{BaseModel: datamodel.BaseModel{ID: 1}, State: models.LifeCycleStateREADY, StateDetails: models.LifeCycleStateReadyDetails}, nil)

		_, err := syncNewSnapshotsToDatabase(ctx, []string{"snapshot-uuid.volume-uuid"}, newSSMap, mockStorage, dbVolumeMap, pool)
		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
	})
	t.Run("SyncUpdatedSnapshotsToDatabase_SnapshotCreationFailed", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		dbVolumeMap := map[string]*datamodel.Volume{
			"test-volume-uuid": {
				BaseModel: datamodel.BaseModel{ID: 1},
			},
		}
		pool := &datamodel.Pool{AccountID: 1}
		newSSMap := map[string]*vsa.Snapshot{
			"snapshot-uuid.volume-uuid": {
				Snapshot: ontaprestmodel.Snapshot{
					Name: nillable.ToPointer("new-snapshot"),
					ProvenanceVolume: &ontaprestmodel.SnapshotInlineProvenanceVolume{
						UUID: nillable.ToPointer("test-volume-uuid"),
					},
					Volume: &ontaprestmodel.SnapshotInlineVolume{
						Name: nillable.ToPointer("test-volume"),
					},
					Svm: &ontaprestmodel.SnapshotInlineSvm{
						Name: nillable.ToPointer("test-svm"),
					},
				},
				ExternalUUID:           "new-snapshot-uuid",
				ExternalVolumeUUID:     "test-volume-uuid",
				SizeInBytes:            122880,
				LogicalSizeUsedInBytes: 122880,
			},
		}

		mockStorage.On("CreatingSnapshot", mock.Anything, mock.Anything).Return(
			nil, errors.New("could not create snapshot"))

		_, err := syncNewSnapshotsToDatabase(ctx, []string{"snapshot-uuid.volume-uuid"}, newSSMap, mockStorage, dbVolumeMap, pool)
		assert.Error(t, err)
		mockStorage.AssertExpectations(t)
	})

	t.Run("SyncUpdatedSnapshotsToDatabase_SnapshotUpdateFailed", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		dbVolumeMap := map[string]*datamodel.Volume{
			"test-volume-uuid": {
				BaseModel: datamodel.BaseModel{ID: 1},
			},
		}
		pool := &datamodel.Pool{AccountID: 1}
		newSSMap := map[string]*vsa.Snapshot{
			"snapshot-uuid.volume-uuid": {
				Snapshot: ontaprestmodel.Snapshot{
					Name: nillable.ToPointer("new-snapshot"),
					ProvenanceVolume: &ontaprestmodel.SnapshotInlineProvenanceVolume{
						UUID: nillable.ToPointer("test-volume-uuid"),
					},
					Volume: &ontaprestmodel.SnapshotInlineVolume{
						Name: nillable.ToPointer("test-volume"),
					},
					Svm: &ontaprestmodel.SnapshotInlineSvm{
						Name: nillable.ToPointer("test-svm"),
					},
				},
				ExternalUUID:           "new-snapshot-uuid",
				ExternalVolumeUUID:     "test-volume-uuid",
				SizeInBytes:            122880,
				LogicalSizeUsedInBytes: 122880,
			},
		}

		mockStorage.On("CreatingSnapshot", mock.Anything, mock.Anything).Return(
			&datamodel.Snapshot{
				BaseModel:    datamodel.BaseModel{ID: 1},
				State:        models.LifeCycleStateCreating,
				StateDetails: models.LifeCycleStateCreatingDetails,
			}, nil)

		mockStorage.On("UpdateSnapshot", ctx, mock.Anything).Return(
			&datamodel.Snapshot{BaseModel: datamodel.BaseModel{ID: 1}}, errors.New("could not update snapshot"))

		_, err := syncNewSnapshotsToDatabase(ctx, []string{"snapshot-uuid.volume-uuid"}, newSSMap, mockStorage, dbVolumeMap, pool)
		assert.Error(tt, err)
		mockStorage.AssertExpectations(tt)
	})
}

func TestSyncDeletedSnapshotsToDatabase(t *testing.T) {
	t.Run("SyncDeletedSnapshotsToDatabaseSuccess", func(tt *testing.T) {
		ctx := context.TODO()
		mockStorage := database.NewMockStorage(tt)

		deleteIDs := []int64{1, 2, 3}
		mockStorage.On("BatchDeleteSnapshots", ctx, deleteIDs).Return(
			[]*datamodel.Snapshot{
				{BaseModel: datamodel.BaseModel{ID: 1}},
				{BaseModel: datamodel.BaseModel{ID: 2}},
				{BaseModel: datamodel.BaseModel{ID: 3}},
			}, nil)

		deletedSnapshots, err := syncDeletedSnapshotsToDatabase(ctx, deleteIDs, mockStorage)
		assert.NoError(tt, err)
		assert.Len(tt, deletedSnapshots, 3)
		mockStorage.AssertExpectations(tt)
	})
	t.Run("SyncDeletedSnapshotsToDatabaseFailed", func(tt *testing.T) {
		ctx := context.TODO()
		mockStorage := database.NewMockStorage(tt)

		deleteIDs := []int64{1, 2, 3}
		mockStorage.On("BatchDeleteSnapshots", ctx, deleteIDs).Return(nil, errors.New("could not delete snapshots"))

		deletedSnapshots, err := syncDeletedSnapshotsToDatabase(ctx, deleteIDs, mockStorage)
		assert.Error(tt, err)
		assert.Nil(t, deletedSnapshots)
		mockStorage.AssertExpectations(tt)
	})
}

func TestGetOntapRestProviderForPool(t *testing.T) {
	ctx := context.TODO()

	t.Run("GetOntapRestProviderForPool_Success", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1},
			PoolCredentials: &datamodel.PoolCredentials{
				Password: "abcd",
			},
		}
		node := &datamodel.Node{
			EndpointAddress: "1.2.3.4",
		}
		mockStorage.On("GetNodesByPoolID", ctx, pool.ID).Return([]*datamodel.Node{node}, nil)

		// Patch activities.GetProviderByNode to return a mock provider
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
		mockProvider := new(vsa.MockProvider)
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		provider, err := GetOntapRestProviderForPool(ctx, mockStorage, pool)
		assert.NoError(t, err)
		assert.Equal(t, mockProvider, provider)
		mockStorage.AssertExpectations(t)
	})
	t.Run("GetOntapRestProviderForPool_PoolNotFoundError", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(t)
		pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}

		mockStorage.On("GetNodesByPoolID", ctx, pool.ID).Return(nil, gorm.ErrRecordNotFound)
		provider, err := GetOntapRestProviderForPool(ctx, mockStorage, pool)
		assert.Nil(t, provider)
		assert.Error(t, err)
		mockStorage.AssertExpectations(t)
	})
	t.Run("GetOntapRestProviderForPool_CouldNotGetNodes", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(t)
		pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}

		mockStorage.On("GetNodesByPoolID", ctx, pool.ID).Return(nil, errors.New("could not get nodes"))
		provider, err := GetOntapRestProviderForPool(ctx, mockStorage, pool)
		assert.Nil(t, provider)
		assert.Error(t, err)
		mockStorage.AssertExpectations(t)
	})
}

func TestProcessSnapshotSync(t *testing.T) {
	ctx := context.Background()
	ontapSnapshots := []*vsa.Snapshot{
		// Adds entries to existingSSMap and updatedSSMap
		{
			ExternalUUID:           "test-snapshot-uuid-1",
			ExternalVolumeUUID:     "test-volume-uuid-1",
			SizeInBytes:            122800,
			LogicalSizeUsedInBytes: 122800,
		},
		// Adds entry into newDiskSSMap
		{
			ExternalUUID:       "test-snapshot-uuid-2",
			ExternalVolumeUUID: "test-volume-uuid-1",
			Type:               SnapshotTypeBackupScheduled,
		},
		// Adds entry into newSSMap
		{
			ExternalUUID:       "test-snapshot-uuid-3",
			ExternalVolumeUUID: "test-volume-uuid-2",
			Type:               SnapshotTypeAdHoc,
		},
		// Adds entries into wronglyDeletedSnapshotsMap
		{
			ExternalUUID:       "test-snapshot-uuid-4",
			ExternalVolumeUUID: "test-volume-uuid-2",
			Type:               SnapshotTypeAdHoc,
		},
		// Does not add entries into any of the slices/maps
		{
			ExternalUUID:       "test-snapshot-uuid-5",
			ExternalVolumeUUID: "test-volume-uuid-3",
			Type:               SnapshotTypeAdHoc,
		},
	}

	dbSnapshots := []*datamodel.Snapshot{
		// Does not add entry into deletedIDs
		{
			BaseModel: datamodel.BaseModel{ID: 1},
			SnapshotAttributes: &datamodel.SnapshotAttributes{
				ExternalUUID:           "test-snapshot-uuid-1",
				SizeInBytes:            122,
				LogicalSizeUsedInBytes: 122,
			},
			Volume: &datamodel.Volume{
				BaseModel: datamodel.BaseModel{ID: 1},
				VolumeAttributes: &datamodel.VolumeAttributes{
					ExternalUUID: "test-volume-uuid-1",
				},
			},
		},
		// Adds an entry into deletedIDs
		{
			BaseModel: datamodel.BaseModel{ID: 3},
			Type:      SnapshotTypeBackupScheduled,
			SnapshotAttributes: &datamodel.SnapshotAttributes{
				ExternalUUID: "test-snapshot-uuid-3",
			},
			Volume: &datamodel.Volume{
				BaseModel: datamodel.BaseModel{ID: 1},
				VolumeAttributes: &datamodel.VolumeAttributes{
					ExternalUUID: "test-volume-uuid-1",
				},
			},
		},
		// Does not add entry into deletedIDs (volume doesn't exist on ONTAP)
		{
			BaseModel: datamodel.BaseModel{ID: 6, UUID: "test-snapshot-uuid-6"},
			Type:      SnapshotTypeBackupScheduled,
			SnapshotAttributes: &datamodel.SnapshotAttributes{
				ExternalUUID: "test-snapshot-uuid-6",
			},
			Volume: &datamodel.Volume{
				BaseModel: datamodel.BaseModel{ID: 6},
				VolumeAttributes: &datamodel.VolumeAttributes{
					ExternalUUID: "test-volume-uuid-6",
				},
			},
		},
	}

	ontapVolumeMap := map[string]*vsa.Volume{
		"test-volume-uuid-1": {
			Volume: ontaprestmodel.Volume{
				Name: nillable.ToPointer("test-volume-1"),
				UUID: nillable.ToPointer("test-volume-uuid-1"),
			},
		},
		"test-volume-uuid-2": {
			Volume: ontaprestmodel.Volume{
				Name: nillable.ToPointer("test-volume-2"),
				UUID: nillable.ToPointer("test-volume-uuid-2"),
			},
		},
	}

	dbVolumeMap := map[string]*datamodel.Volume{
		"test-volume-uuid-1": {
			BaseModel:    datamodel.BaseModel{ID: 1},
			State:        models.LifeCycleStateREADY,
			StateDetails: models.LifeCycleStateReadyDetails,
			VolumeAttributes: &datamodel.VolumeAttributes{
				ExternalUUID: "test-volume-uuid-1",
			},
		},
		"test-volume-uuid-2": {
			BaseModel:    datamodel.BaseModel{ID: 2},
			State:        models.LifeCycleStateREADY,
			StateDetails: models.LifeCycleStateReadyDetails,
			VolumeAttributes: &datamodel.VolumeAttributes{
				ExternalUUID: "test-volume-uuid-2",
			},
		},
	}

	newSSMap, updatedSSMap, wronglyDeletedSnapshotsMap, newIDs, updatedIDs, deletedIDs, wronglyDeletedIDs :=
		processSnapshotSync(ctx, ontapVolumeMap, ontapSnapshots, dbVolumeMap, dbSnapshots)

	assert.Len(t, newSSMap, 2)
	assert.Len(t, updatedSSMap, 1)
	assert.Len(t, wronglyDeletedSnapshotsMap, 1)
	assert.Len(t, newIDs, 2)
	assert.Len(t, updatedIDs, 1)
	assert.Len(t, deletedIDs, 1)
	assert.Len(t, wronglyDeletedIDs, 1)
}
