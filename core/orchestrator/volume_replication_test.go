package orchestrator

import (
	"context"
	errors2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/replication"
	gcpserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
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

func TestCreateVolumeReplicationInternal(t *testing.T) {
	t.Run("WhenGetAccountFails", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(t)

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.New("account not found")
		}
		params := &commonparams.CreateVolumeReplicationInternalParams{
			VolumeReplication: &models.VolumeReplication{
				Account: &models.Account{
					Name: "test-account",
				},
			},
		}
		_, _, err := _createVolumeReplicationInternal(ctx, mockStorage, mockTemporal, params)
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

		params := &commonparams.CreateVolumeReplicationInternalParams{
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
		_, _, err := createVolumeReplicationInternal(ctx, mockStorage, mockTemporal, params)
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

		params := &commonparams.CreateVolumeReplicationInternalParams{
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
		_, _, err := _createVolumeReplicationInternal(ctx, mockStorage, mockTemporal, params)
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

		params := &commonparams.CreateVolumeReplicationInternalParams{
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
		_, _, err := _createVolumeReplicationInternal(ctx, mockStorage, mockTemporal, params)
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

		params := &commonparams.CreateVolumeReplicationInternalParams{
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
		_, _, err := _createVolumeReplicationInternal(ctx, mockStorage, mockTemporal, params)
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

		params := &commonparams.CreateVolumeReplicationInternalParams{
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

		actualResponse, jobActualResponse, err := createVolumeReplicationInternal(ctx, mockStorage, mockTemporal, params)
		assert.Nil(tt, err)
		assert.Equal(tt, expectedResponse, actualResponse)
		assert.Equal(tt, jobResponse, jobActualResponse)
	})
}

func TestCreateVolumeReplication(t *testing.T) {
	t.Run("WhenGetOrCreateAccountFails", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(t)

		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.New("account not found")
		}

		params := &commonparams.CreateVolumeReplicationParams{
			AccountName: "test-account",
		}

		_, _, err := _createVolumeReplication(ctx, mockStorage, mockTemporal, params)
		assert.NotNil(tt, err)
		assert.Equal(tt, "account not found", err.Error())
	})

	t.Run("WhenGetVolumeByNameFails", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(t)

		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{Name: "test-account"}, nil
		}

		mockStorage.On("GetVolumeByName", ctx, mock.Anything).Return(nil, errors.New("volume not found"))

		params := &commonparams.CreateVolumeReplicationParams{
			AccountName:      "test-account",
			SourceVolumeName: "non-existent-volume",
		}

		_, _, err := _createVolumeReplication(ctx, mockStorage, mockTemporal, params)
		assert.NotNil(tt, err)
		assert.Equal(tt, "volume not found", err.Error())
	})

	t.Run("WhenCreateJobFails", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(t)

		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{Name: "test-account"}, nil
		}

		dbVol := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				ID: 1,
			},
			Pool: &datamodel.Pool{
				Name: "test-pool",
			},
		}
		mockStorage.On("GetVolumeByName", ctx, mock.Anything).Return(dbVol, nil)
		mockStorage.On("CreateJob", ctx, mock.Anything).Return(nil, errors.New("failed to create job"))

		params := &commonparams.CreateVolumeReplicationParams{
			AccountName:      "test-account",
			SourceVolumeName: "test-volume",
		}

		convertCreateReplicationParamsToEventParam = func(in *commonparams.CreateVolumeReplicationParams, out *replication.CreateReplicationEvent) error {
			return nil
		}

		validateCreateReplicationParams = func(ctx context.Context, event *replication.CreateReplicationEvent, se database.Storage) (*datamodel.VolumeReplication, error) {
			return &datamodel.VolumeReplication{}, nil
		}

		_, _, err := _createVolumeReplication(ctx, mockStorage, mockTemporal, params)
		assert.NotNil(tt, err)
		assert.Equal(tt, "failed to create job", err.Error())
	})

	t.Run("WhenCreateVolumeReplicationDBFails", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(t)

		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{Name: "test-account"}, nil
		}
		dbVol := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				ID: 1,
			},
			Pool: &datamodel.Pool{
				Name: "test-pool",
			},
		}
		dbRep := &datamodel.VolumeReplication{Name: "rep-1"}

		mockStorage.On("GetVolumeByName", ctx, mock.Anything).Return(dbVol, nil)
		mockStorage.On("CreateJob", ctx, mock.Anything).Return(&datamodel.Job{}, nil)
		mockStorage.On("CreateVolumeReplication", ctx, dbRep).Return(nil, errors.New("failed to create volume replication in db"))

		params := &commonparams.CreateVolumeReplicationParams{
			AccountName:      "test-account",
			SourceVolumeName: "test-volume",
		}

		convertCreateReplicationParamsToEventParam = func(in *commonparams.CreateVolumeReplicationParams, out *replication.CreateReplicationEvent) error {
			return nil
		}

		validateCreateReplicationParams = func(ctx context.Context, event *replication.CreateReplicationEvent, se database.Storage) (*datamodel.VolumeReplication, error) {
			return dbRep, nil
		}

		_, _, err := _createVolumeReplication(ctx, mockStorage, mockTemporal, params)
		assert.NotNil(tt, err)
		assert.Equal(tt, "failed to create volume replication in db", err.Error())
	})

	t.Run("WhenExecuteWorkflowFails", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(t)

		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{Name: "test-account"}, nil
		}
		dbVol := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				ID: 1,
			},
			Pool: &datamodel.Pool{
				Name: "test-pool",
			},
		}
		dbRep := &datamodel.VolumeReplication{Name: "rep-1"}

		mockStorage.On("GetVolumeByName", ctx, mock.Anything).Return(dbVol, nil)
		mockStorage.On("CreateJob", ctx, mock.Anything).Return(&datamodel.Job{WorkflowID: "workflow-id"}, nil)
		mockStorage.On("CreateVolumeReplication", ctx, dbRep).Return(dbRep, nil)
		mockTemporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to execute workflow"))

		params := &commonparams.CreateVolumeReplicationParams{
			AccountName:      "test-account",
			SourceVolumeName: "test-volume",
		}
		convertCreateReplicationParamsToEventParam = func(in *commonparams.CreateVolumeReplicationParams, out *replication.CreateReplicationEvent) error {
			return nil
		}

		validateCreateReplicationParams = func(ctx context.Context, event *replication.CreateReplicationEvent, se database.Storage) (*datamodel.VolumeReplication, error) {
			return dbRep, nil
		}

		_, _, err := _createVolumeReplication(ctx, mockStorage, mockTemporal, params)
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

		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}

		dbVol := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				ID: 1,
			},
			Pool: &datamodel.Pool{
				Name: "test-pool",
			},
		}

		mockStorage.On("GetVolumeByName", ctx, mock.Anything).Return(dbVol, nil)

		params := &commonparams.CreateVolumeReplicationParams{
			AccountName:      "test-account",
			SourceVolumeName: "test-volume",
		}

		jobResponse := &datamodel.Job{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "job-uuid",
			},
			WorkflowID: "workflow-id",
		}

		replicationDb := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "replication-uuid",
			},
			Name:         params.SourceVolumeName,
			AccountID:    account.ID,
			VolumeID:     dbVol.ID,
			Description:  "test replication",
			Uri:          "test-uri",
			RemoteUri:    "test-remote-uri",
			State:        models.LifeCycleStateREADY,
			StateDetails: models.LifeCycleStateAvailableDetails,
			ReplicationAttributes: &datamodel.ReplicationDetails{
				EndpointType:               "test-endpoint",
				ReplicationType:            "test-replication-type",
				ReplicationSchedule:        "test-schedule",
				SourceVolumeUUID:           "source-volume-uuid",
				SourceLocation:             "source-region",
				SourceHostName:             "source-host",
				SourceReplicationUUID:      "source-replication-uuid",
				SourceSvmName:              "source-svm",
				SourceVolumeName:           "source-volume",
				DestinationVolumeUUID:      "destination-volume-uuid",
				DestinationLocation:        "destination-region",
				DestinationHostName:        "destination-host",
				DestinationReplicationUUID: "destination-replication-uuid",
				DestinationSvmName:         "destination-svm",
				DestinationVolumeName:      "destination-volume",
			},
		}

		convertCreateReplicationParamsToEventParam = func(in *commonparams.CreateVolumeReplicationParams, out *replication.CreateReplicationEvent) error {
			return nil
		}

		validateCreateReplicationParams = func(ctx context.Context, event *replication.CreateReplicationEvent, se database.Storage) (*datamodel.VolumeReplication, error) {
			return replicationDb, nil
		}

		expectedResponse := convertDataStoreReplicationToModel(replicationDb)

		mockStorage.On("CreateJob", ctx, mock.Anything).Return(jobResponse, nil)
		mockStorage.On("CreateVolumeReplication", ctx, mock.Anything).Return(replicationDb, nil)
		mockTemporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		actualResponse, jobActualResponse, err := _createVolumeReplication(ctx, mockStorage, mockTemporal, params)
		assert.Nil(tt, err)
		assert.Equal(tt, expectedResponse, actualResponse)
		assert.Equal(tt, jobResponse.UUID, jobActualResponse)
	})
}

func TestConvertCreateReplicationParamsToEventParam(t *testing.T) {
	t.Run("WhenConversionSucceeds", func(tt *testing.T) {
		in := &commonparams.CreateVolumeReplicationParams{
			AccountName:      "test-account",
			Region:           "test-region",
			SourceVolumeName: "test-volume",
			Body: &gcpserver.ReplicationCreateV1beta{
				DestinationVolumeParameters: gcpserver.DestinationVolumeParametersV1beta{
					StoragePool: "projects/test-project/locations/test-location/pools/test-pool",
				},
			},
			CorrelationId: "test-correlation-id",
		}

		out := &replication.CreateReplicationEvent{}

		err := _convertCreateReplicationParamsToEventParam(in, out)
		assert.Nil(tt, err)
		assert.Equal(tt, "test-pool", out.DestinationPoolName)
		assert.Equal(tt, "test-location", out.DestinationLocationID)
		assert.Equal(tt, "test-project", out.DestinationProjectNumber)
		assert.Equal(tt, "test-account", out.SourceProjectNumber)
		assert.Equal(tt, "test-region", out.LocationID)
		assert.Equal(tt, "test-volume", out.VolumeResourceID)
		assert.Equal(tt, "test-correlation-id", *out.XCorrelationID)
	})

	t.Run("WhenUnmarshalFails", func(tt *testing.T) {
		in := &commonparams.CreateVolumeReplicationParams{
			AccountName: "test-account",
		}
		out := &replication.CreateReplicationEvent{}

		// Simulate unmarshal failure by passing invalid data
		replication.JsonUnMarshal = func(data []byte, v interface{}) error {
			return errors.New("unmarshal error")
		}
		err := _convertCreateReplicationParamsToEventParam(in, out)
		assert.NotNil(tt, err)
		assert.Equal(tt, err.(*errors2.CustomError).OriginalErr.Error(), "unmarshal error")
	})
}

func TestGetReplicationCount(t *testing.T) {
	t.Run("WhenStorageReturnsCount", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := new(database.MockStorage)
		mockOrchestrator := &Orchestrator{storage: mockStorage}

		projectNumber := "test-project"
		expectedCount := int64(5)

		mockStorage.On("GetVolumeReplicationCount", ctx, projectNumber).Return(expectedCount, nil)

		actualCount, err := mockOrchestrator.GetReplicationCount(ctx, projectNumber)
		assert.Nil(tt, err)
		assert.Equal(tt, expectedCount, actualCount)
	})

	t.Run("WhenStorageReturnsError", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := new(database.MockStorage)
		mockOrchestrator := &Orchestrator{storage: mockStorage}

		projectNumber := "test-project"
		expectedError := errors.New("database error")

		mockStorage.On("GetVolumeReplicationCount", ctx, projectNumber).Return(int64(0), expectedError)

		actualCount, err := mockOrchestrator.GetReplicationCount(ctx, projectNumber)
		assert.NotNil(tt, err)
		assert.Equal(tt, expectedError, err)
		assert.Equal(tt, int64(0), actualCount)
	})
}
