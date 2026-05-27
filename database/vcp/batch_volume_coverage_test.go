package database

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestPersistenceStore_BatchVolumeWrapperCoverage(t *testing.T) {
	logger := log.NewLogger()
	store, err := SetupStorageForTest(logger)
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	require.NoError(t, ClearInMemoryDB(store.DB()))

	account := &datamodel.Account{Name: "acct-1"}
	require.NoError(t, store.DB().Create(account).Error)

	activeDirectory := &datamodel.ActiveDirectory{AdName: "ad-1", AccountId: account.ID}
	require.NoError(t, store.DB().Create(activeDirectory).Error)

	kmsConfig := &datamodel.KmsConfig{Name: "kms-1", AccountID: account.ID}
	require.NoError(t, store.DB().Create(kmsConfig).Error)

	pool := &datamodel.Pool{
		Name:              "pool-1",
		AccountID:         account.ID,
		Account:           account,
		PoolAttributes:    &datamodel.PoolAttributes{PrimaryZone: "us-east4-a"},
		ActiveDirectoryID: sql.NullInt64{Int64: activeDirectory.ID, Valid: true},
		KmsConfigID:       sql.NullInt64{Int64: kmsConfig.ID, Valid: true},
	}
	require.NoError(t, store.DB().Create(pool).Error)

	volume := &datamodel.Volume{
		Name:             "volume-1",
		AccountID:        account.ID,
		PoolID:           pool.ID,
		Account:          account,
		Pool:             pool,
		VolumeAttributes: &datamodel.VolumeAttributes{},
	}
	require.NoError(t, store.DB().Create(volume).Error)

	replication := &datamodel.VolumeReplication{
		VolumeID:  volume.ID,
		AccountID: account.ID,
	}
	require.NoError(t, store.DB().Create(replication).Error)

	t.Run("GetMultipleVolumesSelective_Success", func(tt *testing.T) {
		res, err := store.GetMultipleVolumesSelective(ctx, [][]interface{}{{"uuid in ?", []string{volume.UUID}}}, VolumePreloadOptions{
			ActiveDirectory: true,
			KmsConfig:       true,
		})
		require.NoError(tt, err)
		require.Len(tt, res, 1)
		require.NotNil(tt, res[0].Pool)
		require.NotNil(tt, res[0].Pool.ActiveDirectory)
		require.NotNil(tt, res[0].Pool.KmsConfig)
	})

	t.Run("GetMultipleVolumesSelective_Error", func(tt *testing.T) {
		res, err := store.GetMultipleVolumesSelective(ctx, [][]interface{}{{"nonexistent_column = ?", 1}}, VolumePreloadOptions{})
		assert.Error(tt, err)
		assert.Nil(tt, res)
	})

	t.Run("GetReplicatedVolumeUUIDs_Success", func(tt *testing.T) {
		res, err := store.GetReplicatedVolumeUUIDs(ctx, []string{volume.UUID, "cvp-only"})
		require.NoError(tt, err)
		assert.Equal(tt, []string{volume.UUID}, res)
	})

	t.Run("GetReplicatedVolumeUUIDs_IgnoresSoftDeletedReplications", func(tt *testing.T) {
		require.NoError(tt, store.DB().Delete(replication).Error)

		res, err := store.GetReplicatedVolumeUUIDs(ctx, []string{volume.UUID, "cvp-only"})
		require.NoError(tt, err)
		assert.Empty(tt, res)
	})

	t.Run("GetReplicatedVolumeUUIDs_Error", func(tt *testing.T) {
		require.NoError(tt, store.DB().Exec("DROP TABLE volume_replications").Error)
		res, err := store.GetReplicatedVolumeUUIDs(ctx, []string{volume.UUID})
		assert.Error(tt, err)
		assert.Nil(tt, res)
	})
}

func TestPersistenceStore_GetMultipleVolumesSelective_WrapperLine(t *testing.T) {
	logger := log.NewLogger()
	store, err := SetupStorageForTest(logger)
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	_, err = store.GetMultipleVolumesSelective(context.Background(), [][]interface{}{{"nonexistent_column = ?", 1}}, VolumePreloadOptions{})
	assert.Error(t, err)
}
