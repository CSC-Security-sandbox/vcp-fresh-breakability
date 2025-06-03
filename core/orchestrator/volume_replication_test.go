package orchestrator

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	workflow_engine_mock "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine"
)

func TestCreateVolumeReplication(t *testing.T) {
	t.Run("WhenGetAccountFails", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(t)

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.New("account not found")
		}
		params := &commonparams.CreateVolumeReplicationParams{
			VolumeReplication: &models.VolumeReplication{
				Account: &models.Account{
					Name: "test-account",
				},
			},
		}
		_, _, err := createVolumeReplication(ctx, mockStorage, mockTemporal, params)
		assert.NotNil(tt, err)
		assert.Equal(tt, "account not found", err.Error())
	})
	t.Run("WhenGetVolumeFails", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(t)
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{Name: "test-account"}, nil
		}

		params := &commonparams.CreateVolumeReplicationParams{
			VolumeReplication: &models.VolumeReplication{
				Account: &models.Account{
					Name: "test-account",
				},
				ReplicationAttributes: &models.ReplicationDetails{
					DestinationVolumeUUID: "non-existent-volume-uuid",
				},
			},
		}
		mockStorage.On("GetVolume", ctx, mock.Anything).Return(nil, errors.New("volume not found"))
		_, _, err := createVolumeReplication(ctx, mockStorage, mockTemporal, params)
		assert.NotNil(tt, err)
		assert.Equal(tt, "volume not found", err.Error())
	})
	t.Run("WhenCreateJobFails", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(t)

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{Name: "test-account"}, nil
		}
		mockStorage.On("GetVolume", ctx, mock.Anything).Return(nil, nil)

		params := &commonparams.CreateVolumeReplicationParams{
			VolumeReplication: &models.VolumeReplication{
				Account: &models.Account{
					BaseModel: models.BaseModel{
						ID: 1,
					},
					Name: "test-account",
				},
				Name: "test-replication",
				ReplicationAttributes: &models.ReplicationDetails{
					DestinationVolumeUUID: "test-volume-uuid",
				},
			},
		}
		mockStorage.On("CreateJob", ctx, mock.Anything).Return(nil, errors.New("failed to create job"))
		_, _, err := createVolumeReplication(ctx, mockStorage, mockTemporal, params)
		assert.NotNil(tt, err)
		assert.Equal(tt, "failed to create job", err.Error())
	})
	t.Run("WhenCreateVolumeReplicationDBFails", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(t)

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{Name: "test-account"}, nil
		}
		mockStorage.On("GetVolume", ctx, mock.Anything).Return(nil, nil)

		params := &commonparams.CreateVolumeReplicationParams{
			VolumeReplication: &models.VolumeReplication{
				Account: &models.Account{
					BaseModel: models.BaseModel{
						ID: 1,
					},
					Name: "test-account",
				},
				Name: "test-replication",
				ReplicationAttributes: &models.ReplicationDetails{
					DestinationVolumeUUID: "test-volume-uuid",
				},
			},
		}
		mockStorage.On("CreateJob", ctx, mock.Anything).Return(nil, nil)
		mockStorage.On("CreateVolumeReplication", ctx, mock.Anything).Return(nil, errors.New("failed to create volume replication in db"))
		_, _, err := createVolumeReplication(ctx, mockStorage, mockTemporal, params)
		assert.NotNil(tt, err)
		assert.Equal(tt, "failed to create volume replication in db", err.Error())
	})
	t.Run("WhenExecuteWorkflowFails", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(t)

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{Name: "test-account"}, nil
		}
		mockStorage.On("GetVolume", ctx, mock.Anything).Return(nil, nil)

		params := &commonparams.CreateVolumeReplicationParams{
			VolumeReplication: &models.VolumeReplication{
				Account: &models.Account{
					BaseModel: models.BaseModel{
						ID: 1,
					},
					Name: "test-account",
				},
				Name: "test-replication",
				ReplicationAttributes: &models.ReplicationDetails{
					DestinationVolumeUUID: "test-volume-uuid",
				},
			},
		}
		jobResponse := &datamodel.Job{
			BaseModel: datamodel.BaseModel{
				ID: 1,
			},
		}
		mockStorage.On("CreateJob", ctx, mock.Anything).Return(jobResponse, nil)
		mockStorage.On("CreateVolumeReplication", ctx, mock.Anything).Return(&datamodel.VolumeReplication{}, nil)
		mockTemporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to execute workflow"))
		_, _, err := createVolumeReplication(ctx, mockStorage, mockTemporal, params)
		assert.NotNil(tt, err)
		assert.Equal(tt, "failed to execute workflow", err.Error())
	})
	t.Run("WhenSuccess", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(t)
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID: 1,
			},
			Name: "test-account",
		}
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				ID: 1,
			},
		}
		mockStorage.On("GetVolume", ctx, mock.Anything).Return(volume, nil)

		params := &commonparams.CreateVolumeReplicationParams{
			VolumeReplication: &models.VolumeReplication{
				Account: &models.Account{
					BaseModel: models.BaseModel{
						ID: 1,
					},
					Name: "test-account",
				},
				Name: "test-replication",
				ReplicationAttributes: &models.ReplicationDetails{
					DestinationVolumeUUID: "test-volume-uuid",
				},
			},
		}
		jobResponse := &datamodel.Job{
			BaseModel: datamodel.BaseModel{
				ID: 1,
			},
			WorkflowID: "workflow-id",
		}
		replicationDb := &datamodel.VolumeReplication{
			Name:        params.VolumeReplication.Name,
			Description: params.VolumeReplication.Description,
			Uri:         params.VolumeReplication.Uri,
			RemoteUri:   params.VolumeReplication.RemoteUri,
			ReplicationAttributes: &datamodel.ReplicationDetails{
				EndpointType:               params.VolumeReplication.ReplicationAttributes.EndpointType,
				ReplicationType:            params.VolumeReplication.ReplicationAttributes.ReplicationType,
				ReplicationSchedule:        params.VolumeReplication.ReplicationAttributes.ReplicationSchedule,
				SourceVolumeUUID:           params.VolumeReplication.ReplicationAttributes.SourceVolumeUUID,
				SourceLocation:             params.VolumeReplication.ReplicationAttributes.SourceRegion,
				SourceHostName:             params.VolumeReplication.ReplicationAttributes.SourceHostName,
				SourceReplicationUUID:      params.VolumeReplication.ReplicationAttributes.SourceReplicationUUID,
				SourceSvmName:              params.VolumeReplication.ReplicationAttributes.SourceSvmName,
				SourceVolumeName:           params.VolumeReplication.ReplicationAttributes.SourceVolumeName,
				DestinationVolumeUUID:      params.VolumeReplication.ReplicationAttributes.DestinationVolumeUUID,
				DestinationLocation:        params.VolumeReplication.ReplicationAttributes.DestinationRegion,
				DestinationHostName:        params.VolumeReplication.ReplicationAttributes.DestinationHostName,
				DestinationReplicationUUID: params.VolumeReplication.ReplicationAttributes.DestinationReplicationUUID,
				DestinationSvmName:         params.VolumeReplication.ReplicationAttributes.DestinationSvmName,
				DestinationVolumeName:      params.VolumeReplication.ReplicationAttributes.DestinationVolumeName,
			},
			MirrorState:           params.VolumeReplication.MirrorState,
			RelationshipStatus:    params.VolumeReplication.RelationshipStatus,
			TotalProgress:         params.VolumeReplication.TotalProgress,
			TotalTransferBytes:    params.VolumeReplication.TotalTransferBytes,
			TotalTransferTimeSecs: params.VolumeReplication.TotalTransferTimeSecs,
			LastTransferSize:      params.VolumeReplication.LastTransferSize,
			LastTransferError:     params.VolumeReplication.LastTransferError,
			LastTransferDuration:  params.VolumeReplication.LastTransferDuration,
			LastTransferEndTime:   params.VolumeReplication.LastTransferEndTime,
			ProgressLastUpdated:   params.VolumeReplication.ProgressLastUpdated,
			LastUpdatedFromOntap:  params.VolumeReplication.LastUpdatedFromOntap,
			LagTime:               params.VolumeReplication.LagTime,
			AccountID:             account.ID,
			Account:               account,
			VolumeID:              params.VolumeReplication.VolumeID,
			Volume:                volume,
		}

		expectedResponse := convertDataStoreReplicationToModel(replicationDb)

		mockStorage.On("CreateJob", ctx, mock.Anything).Return(jobResponse, nil)
		mockStorage.On("CreateVolumeReplication", ctx, mock.Anything).Return(replicationDb, nil)
		mockTemporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		actualResponse, jobActualResponse, err := createVolumeReplication(ctx, mockStorage, mockTemporal, params)
		assert.Nil(tt, err)
		assert.Equal(tt, expectedResponse, actualResponse)
		assert.Equal(tt, jobResponse, jobActualResponse)
	})
}
