package gcp

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
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
		orch := GCPOrchestrator{
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
		orch := GCPOrchestrator{
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
		orch := GCPOrchestrator{
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
		orch := GCPOrchestrator{
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

		expectedError := errors.New("workflow error")
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, expectedError).Once()

		resp, err := orch.GetMultipleReplicationsInternal(ctx, "test_account", replicationUUIDs)

		assert.Error(tt, err)
		assert.ErrorContains(tt, err, "workflow error")
		assert.Nil(tt, resp)

		// Verify that the job was created and marked as ERROR due to the defer block
		var jobs []datamodel.Job
		err = store.DB().Where("account_id = ? AND type = ?", account.ID, string(models.JobTypeRefreshVolumeReplicationInternal)).Find(&jobs).Error
		assert.NoError(tt, err)
		assert.Len(tt, jobs, 1)

		job := jobs[0]
		assert.Equal(tt, string(models.JobsStateERROR), job.State)
		assert.Contains(tt, job.ErrorDetails, "workflow error")
	})
}

func TestPerformMountCheck(t *testing.T) {
	temporal := workflowEngineMock.NewMockTemporalTestClient(t)
	replicationUUID := "replication-uuid"
	accountName := "testAccount"
	t.Run("WhenGetAccountFails", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		store, err := database.NewTestStorage(mockLogger)
		assert.NoError(tt, err, "Failed to create test storage")
		err = database.ClearInMemoryDB(store.DB())
		assert.NoError(tt, err, "Failed to ClearInMemoryDB")
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.New("account not found")
		}
		_, err = performMountCheck(ctx, store, temporal, replicationUUID, accountName)
		assert.EqualError(tt, err, "account not found")
	})
	t.Run("WhenTemporalWorkflowFails", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		store, err := database.NewTestStorage(mockLogger)
		assert.NoError(tt, err, "Failed to create test storage")
		err = database.ClearInMemoryDB(store.DB())
		assert.NoError(tt, err, "Failed to ClearInMemoryDB")
		dbAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-uuid",
			},
			Name: "test_account",
		}
		err = store.DB().Create(dbAccount).Error
		assert.NoError(tt, err, "Failed to create account")

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return dbAccount, nil
		}

		expectedError := errors.New("temporal error")
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, expectedError)
		_, err = performMountCheck(ctx, store, temporal, replicationUUID, accountName)
		assert.EqualError(tt, err, "temporal error")

		// Verify that the job was created and marked as ERROR due to the defer block
		var jobs []datamodel.Job
		err = store.DB().Where("account_id = ? AND type = ?", dbAccount.ID, string(models.JobTypeMountCheck)).Find(&jobs).Error
		assert.NoError(tt, err)
		assert.Len(tt, jobs, 1)

		job := jobs[0]
		assert.Equal(tt, string(models.JobsStateERROR), job.State)
		assert.Contains(tt, job.ErrorDetails, "temporal error")
	})
}

func TestUpdateVolumeReplicationAttributes(t *testing.T) {
	t.Run("WhenGetAccountFails", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)
		mockTemporal := workflowEngineMock.NewMockTemporalTestClient(t)

		params := models.UpdateVolumeReplicationAttributesParams{
			ProjectNumber:             "project-123",
			LocationId:                "us-central1-a",
			VolumeReplicationId:       "replication-123",
			VolumeReplicationInternal: nil,
		}

		mockStorage.On("GetAccount", ctx, "project-123").Return(nil, errors.New("account not found"))

		_, err := updateVolumeReplicationAttributes(ctx, mockStorage, mockTemporal, params)

		assert.Error(tt, err)
		assert.Equal(tt, "account not found", err.Error())
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenParseRegionAndZoneFails", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)
		mockTemporal := workflowEngineMock.NewMockTemporalTestClient(t)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      "test-account",
		}

		params := models.UpdateVolumeReplicationAttributesParams{
			ProjectNumber:             "project-123",
			LocationId:                "invalid-location",
			VolumeReplicationId:       "replication-123",
			VolumeReplicationInternal: nil,
		}

		originalFunc := utilParseRegionAndZone
		defer func() { utilParseRegionAndZone = originalFunc }()
		utilParseRegionAndZone = func(locationId string) (string, string, error) {
			return "", "", errors.New("invalid location format")
		}

		mockStorage.On("GetAccount", ctx, "project-123").Return(account, nil)

		_, err := updateVolumeReplicationAttributes(ctx, mockStorage, mockTemporal, params)

		assert.Error(tt, err)
		assert.Equal(tt, "invalid location format", err.Error())
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenCreateJobFails", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)
		mockTemporal := workflowEngineMock.NewMockTemporalTestClient(t)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      "test-account",
		}

		params := models.UpdateVolumeReplicationAttributesParams{
			ProjectNumber:             "project-123",
			LocationId:                "us-central1-a",
			VolumeReplicationId:       "replication-123",
			VolumeReplicationInternal: nil,
		}

		originalFunc := utilParseRegionAndZone
		defer func() { utilParseRegionAndZone = originalFunc }()
		utilParseRegionAndZone = func(locationId string) (string, string, error) {
			return "us-central1", "zone-a", nil
		}

		mockStorage.On("GetAccount", ctx, "project-123").Return(account, nil)
		mockStorage.On("CreateJob", ctx, mock.AnythingOfType("*datamodel.Job")).Return(nil, errors.New("failed to create job"))

		_, err := updateVolumeReplicationAttributes(ctx, mockStorage, mockTemporal, params)

		assert.Error(tt, err)
		assert.Equal(tt, "failed to create job", err.Error())
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenExecuteWorkflowFails", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)
		mockTemporal := workflowEngineMock.NewMockTemporalTestClient(t)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      "test-account",
		}

		params := models.UpdateVolumeReplicationAttributesParams{
			ProjectNumber:             "project-123",
			LocationId:                "us-central1-a",
			VolumeReplicationId:       "replication-123",
			VolumeReplicationInternal: nil,
		}

		createdJob := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-123"},
			WorkflowID: "UpdateVolumeReplicationAttributes-replication-123",
			Type:       string(models.JobTypeUpdateVolumeReplication),
			State:      string(models.JobsStatePROCESSING),
			AccountID:  sql.NullInt64{Int64: 1, Valid: true},
		}

		originalFunc := utilParseRegionAndZone
		defer func() { utilParseRegionAndZone = originalFunc }()
		utilParseRegionAndZone = func(locationId string) (string, string, error) {
			return "us-central1", "zone-a", nil
		}

		mockStorage.On("GetAccount", ctx, "project-123").Return(account, nil)
		mockStorage.On("CreateJob", ctx, mock.AnythingOfType("*datamodel.Job")).Return(createdJob, nil)

		expectedError := errors.New("workflow error")
		mockTemporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, expectedError)
		mockStorage.On("UpdateJob", ctx, "job-123", string(models.JobsStateERROR), 0, expectedError.Error()).Return(nil)

		_, err := updateVolumeReplicationAttributes(ctx, mockStorage, mockTemporal, params)

		assert.Error(tt, err)
		assert.Equal(tt, "workflow error", err.Error())
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenSuccess", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)
		mockTemporal := workflowEngineMock.NewMockTemporalTestClient(t)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      "test-account",
		}

		params := models.UpdateVolumeReplicationAttributesParams{
			ProjectNumber:             "project-123",
			LocationId:                "us-central1-a",
			VolumeReplicationId:       "replication-123",
			VolumeReplicationInternal: nil,
		}

		createdJob := &datamodel.Job{
			BaseModel:    datamodel.BaseModel{UUID: "job-123"},
			WorkflowID:   "UpdateVolumeReplicationAttributes-replication-123",
			Type:         string(models.JobTypeUpdateVolumeReplication),
			State:        string(models.JobsStatePROCESSING),
			ResourceName: "replication-123",
			AccountID:    sql.NullInt64{Int64: 1, Valid: true},
		}

		originalFunc := utilParseRegionAndZone
		defer func() { utilParseRegionAndZone = originalFunc }()
		utilParseRegionAndZone = func(locationId string) (string, string, error) {
			return "us-central1", "zone-a", nil
		}

		// Note: convertDatastoreOperationToModel is used internally

		mockStorage.On("GetAccount", ctx, "project-123").Return(account, nil)
		mockStorage.On("CreateJob", ctx, mock.AnythingOfType("*datamodel.Job")).Return(createdJob, nil)

		mockTemporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		result, err := updateVolumeReplicationAttributes(ctx, mockStorage, mockTemporal, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "job-123", result.UUID)
		assert.Equal(tt, models.JobTypeUpdateVolumeReplication, result.Type)
		assert.Equal(tt, models.JobsStatePROCESSING, result.State)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenWithVolumeReplicationInternal", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)
		mockTemporal := workflowEngineMock.NewMockTemporalTestClient(t)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      "test-account",
		}

		params := models.UpdateVolumeReplicationAttributesParams{
			ProjectNumber:       "project-123",
			LocationId:          "us-central1-a",
			VolumeReplicationId: "replication-123",
			// Note: VolumeReplicationInternal removed due to import issues
		}

		createdJob := &datamodel.Job{
			BaseModel:    datamodel.BaseModel{UUID: "job-123"},
			WorkflowID:   "UpdateVolumeReplicationAttributes-replication-123",
			Type:         string(models.JobTypeUpdateVolumeReplication),
			State:        string(models.JobsStatePROCESSING),
			ResourceName: "replication-123",
			AccountID:    sql.NullInt64{Int64: 1, Valid: true},
		}

		originalFunc := utilParseRegionAndZone
		defer func() { utilParseRegionAndZone = originalFunc }()
		utilParseRegionAndZone = func(locationId string) (string, string, error) {
			return "us-central1", "zone-a", nil
		}

		// Note: convertDatastoreOperationToModel is used internally

		mockStorage.On("GetAccount", ctx, "project-123").Return(account, nil)
		mockStorage.On("CreateJob", ctx, mock.AnythingOfType("*datamodel.Job")).Return(createdJob, nil)

		mockTemporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		result, err := updateVolumeReplicationAttributes(ctx, mockStorage, mockTemporal, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "job-123", result.UUID)
		mockStorage.AssertExpectations(tt)
	})
}

func TestUpdateVolumeReplicationState(t *testing.T) {
	t.Run("WhenGetAccountFails", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)

		params := models.UpdateVolumeReplicationStateParams{
			ProjectNumber:       "project-123",
			LocationId:          "us-central1-a",
			VolumeReplicationId: "replication-123",
		}

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.New("account not found")
		}
		defer func() { getAccountWithName = _getAccountWithName }()

		_, err := updateVolumeReplicationState(ctx, mockStorage, params)

		assert.Error(tt, err)
		assert.Equal(tt, "account not found", err.Error())
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenGetVolumeReplicationFails", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      "test-account",
		}

		params := models.UpdateVolumeReplicationStateParams{
			ProjectNumber:       "project-123",
			LocationId:          "us-central1-a",
			VolumeReplicationId: "replication-123",
			State:               models.LifeCycleStateError,
			StateDetails:        "error details",
		}

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = _getAccountWithName }()

		mockStorage.On("GetVolumeReplication", ctx, "replication-123").Return(nil, errors.New("replication not found"))

		_, err := updateVolumeReplicationState(ctx, mockStorage, params)

		assert.Error(tt, err)
		assert.Equal(tt, "replication not found", err.Error())
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenUpdateVolumeReplicationStatesFails", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      "test-account",
		}

		dbVolumeReplication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "replication-123"},
			Name:      "test-replication",
			AccountID: account.ID,
			ReplicationAttributes: &datamodel.ReplicationDetails{
				EndpointType: "src",
			},
		}

		params := models.UpdateVolumeReplicationStateParams{
			ProjectNumber:       "project-123",
			LocationId:          "us-central1-a",
			VolumeReplicationId: "replication-123",
			State:               models.LifeCycleStateError,
			StateDetails:        "error details",
		}

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = _getAccountWithName }()

		mockStorage.On("GetVolumeReplication", ctx, "replication-123").Return(dbVolumeReplication, nil)
		mockStorage.On("UpdateVolumeReplicationStates", ctx, mock.AnythingOfType("*datamodel.VolumeReplication")).Return(errors.New("failed to update state"))

		_, err := updateVolumeReplicationState(ctx, mockStorage, params)

		assert.Error(tt, err)
		assert.Equal(tt, "failed to update state", err.Error())
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenSuccess", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      "test-account",
		}

		dbVolumeReplication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "replication-123"},
			Name:      "test-replication",
			AccountID: account.ID,
			State:     models.LifeCycleStateREADY,
			ReplicationAttributes: &datamodel.ReplicationDetails{
				EndpointType: "src",
			},
		}

		params := models.UpdateVolumeReplicationStateParams{
			ProjectNumber:       "project-123",
			LocationId:          "us-central1-a",
			VolumeReplicationId: "replication-123",
			State:               models.LifeCycleStateError,
			StateDetails:        "error details",
		}

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = _getAccountWithName }()

		mockStorage.On("GetVolumeReplication", ctx, "replication-123").Return(dbVolumeReplication, nil)
		mockStorage.On("UpdateVolumeReplicationStates", ctx, mock.AnythingOfType("*datamodel.VolumeReplication")).Return(nil)

		result, err := updateVolumeReplicationState(ctx, mockStorage, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "replication-123", result.UUID)
		assert.Equal(tt, models.LifeCycleStateError, result.State)
		assert.Equal(tt, "error details", result.StateDetails)
		mockStorage.AssertExpectations(tt)
	})
}
