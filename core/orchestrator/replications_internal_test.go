package orchestrator

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	workflowEngineMock "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine"
)

func TestGetMultipleReplicationsInternal(t *testing.T) {
	t.Run("WhenGetAccountWithNameReturnsError", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		defer func() { getAccountWithName = _getAccountWithName }()

		mockLogger := log.NewLogger()
		store, err := database.NewTestStorage(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		orch := Orchestrator{
			storage:  store,
			temporal: temporal,
		}

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.New("account not found")
		}

		_, err = orch.GetMultipleReplicationsInternal(ctx, "non_existent_account", []string{"replication-uuid-1", "replication-uuid-2"})

		assert.EqualError(tt, err, "account not found")
	})
	t.Run("WhenGetReplicationsFromDBReturnsNotFoundError", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.NewTestStorage(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		orch := Orchestrator{
			storage:  store,
			temporal: temporal,
		}

		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			tt.Fatalf("Failed to clean up test storage: %v", err)
		}
		replicationUUIDs := []string{"replication-1", "replication-2"}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			Name:    "test_pool",
			Account: account,
			ClusterDetails: datamodel.ClusterDetails{
				ExternalName: "external-cluster",
			},
		}
		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
			Account:   account,
			Pool:      pool,
			PoolID:    pool.ID,
		}
		err = store.DB().Create(volume).Error
		if err != nil {
			tt.Fatalf("Failed to create volume: %v", err)
		}
		expectedError := vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataNotFoundError, errors.NewNotFoundErr("replication", nil))

		_, err = orch.GetMultipleReplicationsInternal(ctx, "test_account", replicationUUIDs)

		assert.Equal(tt, err, expectedError)
	})
	t.Run("WhenSuccess", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.NewTestStorage(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		orch := Orchestrator{
			storage:  store,
			temporal: temporal,
		}

		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			tt.Fatalf("Failed to clean up test storage: %v", err)
		}
		replicationUUIDs := []string{"replication-1", "replication-2"}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			Name:    "test_pool",
			Account: account,
			ClusterDetails: datamodel.ClusterDetails{
				ExternalName: "external-cluster",
			},
		}
		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
			Account:   account,
			Pool:      pool,
			PoolID:    pool.ID,
		}
		err = store.DB().Create(volume).Error
		if err != nil {
			tt.Fatalf("Failed to create volume: %v", err)
		}

		replication1 := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "replication-1"},
			Name:      "replication_1",
			AccountID: account.ID,
			Account:   account,
			VolumeID:  volume.ID,
		}
		err = store.DB().Create(replication1).Error
		if err != nil {
			t.Fatalf("Failed to create replication 1: %v", err)
		}

		replication2 := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "replication-2"},
			Name:      "replication_2",
			AccountID: account.ID,
			Account:   account,
			VolumeID:  volume.ID,
		}
		err = store.DB().Create(replication2).Error
		if err != nil {
			t.Fatalf("Failed to create replication 2: %v", err)
		}

		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Once()

		resp, err := orch.GetMultipleReplicationsInternal(ctx, "test_account", replicationUUIDs)

		assert.NoError(tt, err)
		assert.Equal(tt, replication1.Name, resp[0].Name)
		assert.Equal(tt, replication2.Name, resp[1].Name)
	})
	t.Run("WhenWorkflowError", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.NewTestStorage(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		orch := Orchestrator{
			storage:  store,
			temporal: temporal,
		}

		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			tt.Fatalf("Failed to clean up test storage: %v", err)
		}
		replicationUUIDs := []string{"replication-1", "replication-2"}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			Name:    "test_pool",
			Account: account,
			ClusterDetails: datamodel.ClusterDetails{
				ExternalName: "external-cluster",
			},
		}
		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
			Account:   account,
			Pool:      pool,
			PoolID:    pool.ID,
		}
		err = store.DB().Create(volume).Error
		if err != nil {
			tt.Fatalf("Failed to create volume: %v", err)
		}

		replication1 := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "replication-1"},
			Name:      "replication_1",
			AccountID: account.ID,
			Account:   account,
			VolumeID:  volume.ID,
		}
		err = store.DB().Create(replication1).Error
		if err != nil {
			t.Fatalf("Failed to create replication 1: %v", err)
		}

		replication2 := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "replication-2"},
			Name:      "replication_2",
			AccountID: account.ID,
			Account:   account,
			VolumeID:  volume.ID,
		}
		err = store.DB().Create(replication2).Error
		if err != nil {
			t.Fatalf("Failed to create replication 2: %v", err)
		}

		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("workflow error")).Once()

		resp, err := orch.GetMultipleReplicationsInternal(ctx, "test_account", replicationUUIDs)

		assert.Error(tt, err)
		assert.ErrorContains(tt, err, "workflow error")
		assert.Nil(tt, resp)
	})
}
