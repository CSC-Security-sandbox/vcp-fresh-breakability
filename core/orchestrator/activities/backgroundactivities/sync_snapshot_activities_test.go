package backgroundactivities

import (
	"context"
	"errors"
	"strings"
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

		// Mock the batch get wrongly deleted snapshots call
		mockStorage.On("BatchGetWronglyDeletedSnapshots", ctx, []string{"wrongly-deleted-snapshot-uuid"}).Return(
			[]*datamodel.Snapshot{{State: models.LifeCycleStateDeleted, StateDetails: models.LifeCycleStateDeletedDetails}}, nil)
		// Mock the batch undelete snapshots call
		mockStorage.On("BatchUnDeleteSnapshots", ctx, mock.AnythingOfType("[]*datamodel.Snapshot")).Return(nil)

		_, err := syncWronglyDeletedSnapshotsToDatabase(ctx, []string{"snapshot-uuid.volume-uuid"}, wronglyDeletedSnapshotsMap, mockStorage, dbSnapshotsMap)
		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
	})
	t.Run("SyncWronglyDeletedSnapshotsToDatabaseBatchGetFailed", func(tt *testing.T) {
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

		// Mock the batch get wrongly deleted snapshots call to return error
		mockStorage.On("BatchGetWronglyDeletedSnapshots", ctx, []string{"wrongly-deleted-snapshot-uuid"}).Return(
			nil, errors.New("could not get wrongly deleted snapshots"))

		_, err := syncWronglyDeletedSnapshotsToDatabase(ctx, []string{"snapshot-uuid.volume-uuid"}, wronglyDeletedSnapshotsMap, mockStorage, dbSnapshotsMap)
		assert.Error(t, err)
		mockStorage.AssertExpectations(t)
	})
	t.Run("SyncWronglyDeletedSnapshotsToDatabaseBatchUnDeleteFailed", func(tt *testing.T) {
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

		// Mock the batch get wrongly deleted snapshots call
		mockStorage.On("BatchGetWronglyDeletedSnapshots", ctx, []string{"wrongly-deleted-snapshot-uuid"}).Return(
			[]*datamodel.Snapshot{{State: models.LifeCycleStateDeleted, StateDetails: models.LifeCycleStateDeletedDetails}}, nil)
		// Mock the batch undelete snapshots call to return error
		mockStorage.On("BatchUnDeleteSnapshots", ctx, mock.AnythingOfType("[]*datamodel.Snapshot")).Return(errors.New("could not undelete snapshots"))

		_, err := syncWronglyDeletedSnapshotsToDatabase(ctx, []string{"snapshot-uuid.volume-uuid"}, wronglyDeletedSnapshotsMap, mockStorage, dbSnapshotsMap)
		assert.Error(t, err)
		mockStorage.AssertExpectations(t)
	})
	t.Run("SyncWronglyDeletedSnapshotsToDatabaseWithEmptySnapshots", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		dbSnapshotsMap := map[string]*datamodel.Snapshot{}
		wronglyDeletedSnapshotsMap := map[string]*vsa.Snapshot{}

		result, err := syncWronglyDeletedSnapshotsToDatabase(ctx, []string{}, wronglyDeletedSnapshotsMap, mockStorage, dbSnapshotsMap)
		assert.NoError(t, err)
		assert.Nil(t, result)
		// No expectations to assert since no methods should be called
	})
	t.Run("SyncWronglyDeletedSnapshotsToDatabaseWithNoSnapshotsFound", func(tt *testing.T) {
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

		// Mock the batch get wrongly deleted snapshots call to return empty slice
		mockStorage.On("BatchGetWronglyDeletedSnapshots", ctx, []string{"wrongly-deleted-snapshot-uuid"}).Return(
			[]*datamodel.Snapshot{}, nil)

		result, err := syncWronglyDeletedSnapshotsToDatabase(ctx, []string{"snapshot-uuid.volume-uuid"}, wronglyDeletedSnapshotsMap, mockStorage, dbSnapshotsMap)
		assert.NoError(t, err)
		assert.Empty(t, result)
		mockStorage.AssertExpectations(t)
	})
}

func TestSyncUpdatedSnapshotsToDatabase(t *testing.T) {
	ctx := context.TODO()
	t.Run("SyncUpdatedSnapshotsToDatabaseSuccess", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		dbSnapshotsMap := map[string]*datamodel.Snapshot{
			"updated-snapshot-uuid": {
				BaseModel:    datamodel.BaseModel{ID: 1, UUID: "updated-snapshot-uuid"},
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

		// Mock the batch update snapshots call
		mockStorage.On("BatchUpdateSnapshots", ctx, mock.AnythingOfType("[]*datamodel.Snapshot")).Return(nil)

		updatedDbSnapshots, err := syncUpdatedSnapshotsToDatabase(ctx, []string{"snapshot-uuid.volume-uuid"}, updatedSSMap, mockStorage, dbSnapshotsMap)
		assert.NoError(t, err)
		assert.Len(t, updatedDbSnapshots, 1)
		mockStorage.AssertExpectations(t)
	})
	t.Run("SyncUpdatedSnapshotsToDatabaseBatchUpdateFailed", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		dbSnapshotsMap := map[string]*datamodel.Snapshot{
			"updated-snapshot-uuid": {
				BaseModel:    datamodel.BaseModel{ID: 1, UUID: "updated-snapshot-uuid"},
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

		// Mock the batch update snapshots call to return error
		mockStorage.On("BatchUpdateSnapshots", ctx, mock.AnythingOfType("[]*datamodel.Snapshot")).Return(errors.New("could not update snapshots"))

		updatedDbSnapshot, err := syncUpdatedSnapshotsToDatabase(ctx, []string{"snapshot-uuid.volume-uuid"}, updatedSSMap, mockStorage, dbSnapshotsMap)
		assert.Error(t, err)
		assert.Nil(t, updatedDbSnapshot)
		mockStorage.AssertExpectations(t)
	})
	t.Run("SyncUpdatedSnapshotsToDatabaseWithEmptySnapshots", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		dbSnapshotsMap := map[string]*datamodel.Snapshot{}
		updatedSSMap := map[string]*vsa.Snapshot{}

		result, err := syncUpdatedSnapshotsToDatabase(ctx, []string{}, updatedSSMap, mockStorage, dbSnapshotsMap)
		assert.NoError(t, err)
		assert.Nil(t, result)
		// No expectations to assert since no methods should be called
	})
	t.Run("SyncUpdatedSnapshotsToDatabaseWithNonExistentSnapshot", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		dbSnapshotsMap := map[string]*datamodel.Snapshot{}
		updatedSSMap := map[string]*vsa.Snapshot{
			"snapshot-uuid.volume-uuid": {
				Snapshot: ontaprestmodel.Snapshot{
					Name: nillable.ToPointer("updated-snapshot"),
				},
				ExternalUUID: "non-existent-uuid",
			},
		}

		result, err := syncUpdatedSnapshotsToDatabase(ctx, []string{"snapshot-uuid.volume-uuid"}, updatedSSMap, mockStorage, dbSnapshotsMap)
		assert.NoError(t, err)
		assert.Nil(t, result)
		// No expectations to assert since no methods should be called when snapshot doesn't exist in DB
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

		mockStorage.On("BatchCreateSnapshots", ctx, mock.Anything, mock.Anything).Return(nil, nil)

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

		mockStorage.On("BatchCreateSnapshots", ctx, mock.Anything, mock.Anything).Return(
			nil, errors.New("could not create snapshot"))

		_, err := syncNewSnapshotsToDatabase(ctx, []string{"snapshot-uuid.volume-uuid"}, newSSMap, mockStorage, dbVolumeMap, pool)
		assert.Error(t, err)
		mockStorage.AssertExpectations(t)
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

	t.Run("GetOntapRestProviderForPool_NoNodesFound", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}

		mockStorage.On("GetNodesByPoolID", ctx, pool.ID).Return([]*datamodel.Node{}, nil)
		provider, err := GetOntapRestProviderForPool(ctx, mockStorage, pool)
		assert.Nil(t, provider)
		assert.Error(t, err)
		mockStorage.AssertExpectations(tt)
		if !strings.Contains(err.Error(), "no nodes found for pool") {
			t.Errorf("expected error %v, got %v", "no nodes found for pool", err)
		}
	})

	t.Run("GetOntapRestProviderForPool_NoCredentials", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1},
		}
		node := &datamodel.Node{
			EndpointAddress: "test-endpoint",
		}
		mockStorage.On("GetNodesByPoolID", ctx, pool.ID).Return([]*datamodel.Node{node}, nil)
		provider, err := GetOntapRestProviderForPool(ctx, mockStorage, pool)

		assert.Nil(t, provider)
		assert.Error(t, err)
		mockStorage.AssertExpectations(tt)
		if !strings.Contains(err.Error(), "pool credentials not found for pool") {
			t.Errorf("expected error %v, got %v", "pool credentials not found for pool", err)
		}
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

func TestGetDBVolumeAndSnapshotsForPool(t *testing.T) {
	t.Run("Success", func(tt *testing.T) {
		ctx := context.TODO()
		mockStorage := database.NewMockStorage(tt)
		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "pool-uuid"},
		}

		vol := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "vol-uuid",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{ExternalUUID: "vol-uuid"},
		}
		snapshot := &datamodel.Snapshot{
			SnapshotAttributes: &datamodel.SnapshotAttributes{ExternalUUID: "snap-uuid"},
			Volume:             vol,
		}

		mockStorage.On("GetVolumesByPoolID", ctx, pool.ID).Return([]*datamodel.Volume{vol}, nil)
		mockStorage.On("GetSnapshotsByVolumeIDs", ctx, []int64{vol.ID}).Return([]*datamodel.Snapshot{snapshot}, nil)

		activity := SyncSnapshotActivity{SE: mockStorage}
		result, err := activity.GetDBVolumeAndSnapshotsForPool(ctx, pool)
		assert.NoError(tt, err)
		assert.Equal(tt, vol, result.DBVolumeMap["vol-uuid"])
		assert.Equal(tt, snapshot, result.DBSnapshotMap["snap-uuid"])
		assert.Equal(tt, []*datamodel.Snapshot{snapshot}, result.DBSnapshots)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("GetVolumesByPoolIDError", func(tt *testing.T) {
		ctx := context.TODO()
		mockStorage := database.NewMockStorage(tt)
		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "pool-uuid"},
		}
		mockStorage.On("GetVolumesByPoolID", ctx, pool.ID).Return(nil, errors.New("db error"))

		activity := SyncSnapshotActivity{SE: mockStorage}
		result, err := activity.GetDBVolumeAndSnapshotsForPool(ctx, pool)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("GetSnapshotsByVolumeIDsError", func(tt *testing.T) {
		ctx := context.TODO()
		mockStorage := database.NewMockStorage(tt)
		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "pool-uuid"},
		}
		vol := &datamodel.Volume{BaseModel: datamodel.BaseModel{ID: 1, UUID: "vol-uuid"},
			VolumeAttributes: &datamodel.VolumeAttributes{ExternalUUID: "vol-uuid"}}
		mockStorage.On("GetVolumesByPoolID", ctx, pool.ID).Return([]*datamodel.Volume{vol}, nil)
		mockStorage.On("GetSnapshotsByVolumeIDs", ctx, []int64{vol.ID}).Return(nil, errors.New("db error"))

		activity := SyncSnapshotActivity{SE: mockStorage}
		result, err := activity.GetDBVolumeAndSnapshotsForPool(ctx, pool)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		mockStorage.AssertExpectations(tt)
	})
}

func TestSyncSnapshotActivity_ProcessSnapshots(t *testing.T) {
	activity := SyncSnapshotActivity{}
	ctx := context.TODO()
	ontapVolumeMap := map[string]*vsa.Volume{"vol-uuid": {ExternalUUID: "vol-uuid"}}
	ontapSnapshots := []*vsa.Snapshot{{ExternalUUID: "snap-uuid", ExternalVolumeUUID: "vol-uuid", Type: SnapshotTypeAdHoc, SizeInBytes: 1, LogicalSizeUsedInBytes: 1}}
	dbVolumeMap := map[string]*datamodel.Volume{"vol-uuid": {BaseModel: datamodel.BaseModel{ID: 1, UUID: "vol-uuid"}, VolumeAttributes: &datamodel.VolumeAttributes{ExternalUUID: "vol-uuid"}}}
	dbSnapshots := []*datamodel.Snapshot{}
	result, err := activity.ProcessSnapshots(ctx, ontapVolumeMap, ontapSnapshots, dbVolumeMap, dbSnapshots)
	assert.NoError(t, err)
	assert.NotNil(t, result)
}

func TestSyncSnapshotActivity_SyncDeletedSnapshotsToDatabase(t *testing.T) {
	ctx := context.TODO()
	mockStorage := database.NewMockStorage(t)
	activity := SyncSnapshotActivity{SE: mockStorage}
	deleteIDs := []int64{1, 2}
	mockStorage.On("BatchDeleteSnapshots", ctx, deleteIDs).Return([]*datamodel.Snapshot{{BaseModel: datamodel.BaseModel{ID: 1}}, {BaseModel: datamodel.BaseModel{ID: 2}}}, nil)
	result, err := activity.SyncDeletedSnapshotsToDatabase(ctx, deleteIDs)
	assert.NoError(t, err)
	assert.Len(t, result, 2)
	mockStorage.AssertExpectations(t)
}

func TestSyncSnapshotActivity_SyncNewSnapshotsToDatabase(t *testing.T) {
	ctx := context.TODO()
	mockStorage := database.NewMockStorage(t)
	activity := SyncSnapshotActivity{SE: mockStorage}
	dbVolumeMap := map[string]*datamodel.Volume{"vol-uuid": {BaseModel: datamodel.BaseModel{ID: 1, UUID: "vol-uuid"}, VolumeAttributes: &datamodel.VolumeAttributes{ExternalUUID: "vol-uuid"}}}
	pool := &datamodel.Pool{AccountID: 1}
	newSSMap := map[string]*vsa.Snapshot{"snap-uuid.vol-uuid": {ExternalUUID: "snap-uuid", ExternalVolumeUUID: "vol-uuid", Type: SnapshotTypeAdHoc, Snapshot: ontaprestmodel.Snapshot{Name: nillable.GetStringPtr("snap")}}}
	mockStorage.On("BatchCreateSnapshots", ctx, mock.Anything, true).Return([]string{"snap-uuid"}, nil)
	mockStorage.On("BatchGetSnapshotsByUUIDs", ctx, []string{"snap-uuid"}).Return([]*datamodel.Snapshot{{SnapshotAttributes: &datamodel.SnapshotAttributes{ExternalUUID: "snap-uuid"}}}, nil)
	result, err := activity.SyncNewSnapshotsToDatabase(ctx, []string{"snap-uuid.vol-uuid"}, newSSMap, dbVolumeMap, pool)
	assert.NoError(t, err)
	assert.Len(t, result, 1)
	mockStorage.AssertExpectations(t)
}

func TestSyncSnapshotActivity_SyncUpdatedSnapshotsToDatabase(t *testing.T) {
	ctx := context.TODO()
	mockStorage := database.NewMockStorage(t)
	activity := SyncSnapshotActivity{SE: mockStorage}
	dbSnapshotsMap := map[string]*datamodel.Snapshot{"snap-uuid": {BaseModel: datamodel.BaseModel{ID: 1, UUID: "snap-uuid"}, SnapshotAttributes: &datamodel.SnapshotAttributes{ExternalUUID: "snap-uuid"}, State: models.LifeCycleStateREADY, StateDetails: models.LifeCycleStateReadyDetails}}
	updatedSSMap := map[string]*vsa.Snapshot{"snap-uuid.vol-uuid": {ExternalUUID: "snap-uuid", SizeInBytes: 1, LogicalSizeUsedInBytes: 1, Snapshot: ontaprestmodel.Snapshot{Name: nillable.GetStringPtr("snap")}}}
	mockStorage.On("BatchUpdateSnapshots", ctx, mock.Anything).Return(nil)
	result, err := activity.SyncUpdatedSnapshotsToDatabase(ctx, []string{"snap-uuid.vol-uuid"}, updatedSSMap, dbSnapshotsMap)
	assert.NoError(t, err)
	assert.Len(t, result, 1)
	mockStorage.AssertExpectations(t)
}

func TestSyncSnapshotActivity_SyncWronglyDeletedSnapshotsToDatabase(t *testing.T) {
	ctx := context.TODO()
	mockStorage := database.NewMockStorage(t)
	activity := SyncSnapshotActivity{SE: mockStorage}
	wronglyDeletedSnapshotsMap := map[string]*vsa.Snapshot{"snap-uuid.vol-uuid": {ExternalUUID: "snap-uuid"}}
	mockStorage.On("BatchGetWronglyDeletedSnapshots", ctx, []string{"snap-uuid"}).Return([]*datamodel.Snapshot{{SnapshotAttributes: &datamodel.SnapshotAttributes{ExternalUUID: "snap-uuid"}}}, nil)
	mockStorage.On("BatchUnDeleteSnapshots", ctx, mock.Anything).Return(nil)
	result, err := activity.SyncWronglyDeletedSnapshotsToDatabase(ctx, []string{"snap-uuid.vol-uuid"}, wronglyDeletedSnapshotsMap)
	assert.NoError(t, err)
	assert.Len(t, result, 1)
	mockStorage.AssertExpectations(t)
}

func TestSyncSnapshotActivity_HydrateSnapshotsToCCFE(t *testing.T) {
	activity := SyncSnapshotActivity{}
	ctx := context.TODO()
	called := false
	original := hydrateBatchSnapshotsToCCFE
	hydrateBatchSnapshotsToCCFE = func(ctx context.Context, createdSnapshots, deletedSnapshots []*datamodel.Snapshot) error {
		called = true
		return nil
	}
	defer func() { hydrateBatchSnapshotsToCCFE = original }()
	created := []*datamodel.Snapshot{{SnapshotAttributes: &datamodel.SnapshotAttributes{ExternalUUID: "snap-uuid"}}}
	deleted := []*datamodel.Snapshot{{SnapshotAttributes: &datamodel.SnapshotAttributes{ExternalUUID: "del-uuid"}}}
	err := activity.HydrateSnapshotsToCCFE(ctx, created, deleted)
	assert.NoError(t, err)
	assert.True(t, called)
}

func TestSyncSnapshotActivity_GetOntapVolumesAndSnapshotsForPool(t *testing.T) {
	ctx := context.TODO()
	mockStorage := database.NewMockStorage(t)
	activity := SyncSnapshotActivity{SE: mockStorage}
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1, UUID: "pool-uuid"}, PoolCredentials: &datamodel.PoolCredentials{Password: "pass"}}

	mockProvider := new(vsa.MockProvider)
	vol := &vsa.Volume{Volume: ontaprestmodel.Volume{
		UUID:      nillable.ToPointer("vol-uuid"),
		Name:      nillable.ToPointer("vol-name"),
		Svm:       &ontaprestmodel.VolumeInlineSvm{Name: nillable.ToPointer("svm-name")},
		IsSvmRoot: nillable.ToPointer(false),
		Style:     nillable.ToPointer("flexvol"),
	}, ExternalUUID: "vol-uuid"}
	mockProvider.On("GetVolumes").Return([]*vsa.Volume{vol}, nil)
	mockProvider.On("GetSnapshots", "vol-uuid").Return([]*vsa.Snapshot{{
		Snapshot: ontaprestmodel.Snapshot{
			Name:             nillable.ToPointer("snap"),
			ProvenanceVolume: &ontaprestmodel.SnapshotInlineProvenanceVolume{UUID: nillable.ToPointer("vol-uuid")},
			Volume:           &ontaprestmodel.SnapshotInlineVolume{Name: nillable.ToPointer("vol-name")},
			Svm:              &ontaprestmodel.SnapshotInlineSvm{Name: nillable.ToPointer("svm-name")},
		},
		ExternalUUID:       "snap-uuid",
		ExternalVolumeUUID: "vol-uuid",
	}}, nil)

	originalGetProviderForPool := GetOntapRestProviderForPool
	GetOntapRestProviderForPool = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) (vsa.Provider, error) {
		return mockProvider, nil
	}
	defer func() { GetOntapRestProviderForPool = originalGetProviderForPool }()

	result, err := activity.GetOntapVolumesAndSnapshotsForPool(ctx, pool)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Contains(t, result.OntapVolumeMap, "vol-uuid")
	assert.Len(t, result.OntapSnapshots, 1)
	mockProvider.AssertExpectations(t)
}
