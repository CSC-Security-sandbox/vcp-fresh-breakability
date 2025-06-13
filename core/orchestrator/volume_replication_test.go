package orchestrator

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	errors2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/replication"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	gcpserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
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

func TestGetMultipleReplications(t *testing.T) {
	t.Run("WhenGetAccountWithNameReturnsError", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)
		defer func() { getAccountWithName = _getAccountWithName }()

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.New("account not found")
		}

		params := commonparams.GetMultipleReplicationsParams{
			AccountName: "test-account",
		}

		_, err := _getMultipleReplications(ctx, mockStorage, params)
		assert.NotNil(tt, err)
		assert.Equal(tt, "account not found", err.Error())
	})
	t.Run("WhenListVolumeReplicationsReturnsError", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)
		defer func() { getAccountWithName = _getAccountWithName }()

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{Name: "test-account"}, nil
		}

		params := commonparams.GetMultipleReplicationsParams{
			AccountName: "test-account",
		}

		mockStorage.On("ListVolumeReplications", ctx, mock.Anything).Return(nil, errors.New("failed to list replications"))

		_, err := _getMultipleReplications(ctx, mockStorage, params)
		assert.NotNil(tt, err)
		assert.ErrorContains(tt, err, "failed to list replications")
	})
	t.Run("WhenListVolumeReplicationsReturnsEmpty", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)
		defer func() { getAccountWithName = _getAccountWithName }()

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{Name: "test-account"}, nil
		}

		params := commonparams.GetMultipleReplicationsParams{
			AccountName: "test-account",
		}

		mockStorage.On("ListVolumeReplications", ctx, mock.Anything).Return([]*datamodel.VolumeReplication{}, nil)

		replications, err := _getMultipleReplications(ctx, mockStorage, params)
		assert.Nil(tt, err)
		assert.Empty(tt, replications)
	})
	t.Run("WhenUtilParseAndValidateRegionAndZone", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)
		defer func() {
			getAccountWithName = _getAccountWithName
			utilParseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{Name: "test-account"}, nil
		}

		utilParseAndValidateRegionAndZone = func(locationId string) (string, string, *gcpserver.Error) {
			return "", "", &gcpserver.Error{
				Code:    400,
				Message: "SomeError",
			}
		}

		params := commonparams.GetMultipleReplicationsParams{
			AccountName: "test-account",
		}

		replications := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					ID:        1,
					UUID:      "uuid-1",
					CreatedAt: time.Time{},
					UpdatedAt: time.Time{},
				},
				Name: "replication-1",
			},
		}

		expCustErr := errors2.CustomError{
			TrackingID: 0,
			Message:    "SomeError",
			Retriable:  false,
			HttpCode:   nillable.GetIntPtr(500),
		}
		expError := errors2.NewVCPError(errors2.ErrRegionZoneParsingError, &expCustErr)

		mockStorage.On("ListVolumeReplications", ctx, mock.Anything).Return(replications, nil)

		res, err := _getMultipleReplications(ctx, mockStorage, params)
		assert.Empty(tt, res)
		assert.EqualError(tt, err, expError.Error())
	})
	t.Run("WhenUtilParseRegionAndZoneReturnsError", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)
		defer func() {
			getAccountWithName = _getAccountWithName
			utilParseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
			utilParseRegionAndZone = utils.ParseRegionAndZone
		}()

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{Name: "test-account"}, nil
		}

		utilParseAndValidateRegionAndZone = func(locationId string) (string, string, *gcpserver.Error) {
			return "us-e4", "", nil
		}

		utilParseRegionAndZone = func(locationID string) (string, string, error) {
			return "", "", errors.New("SomeError")
		}

		params := commonparams.GetMultipleReplicationsParams{
			AccountName: "test-account",
		}

		replications := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					ID:        1,
					UUID:      "uuid-1",
					CreatedAt: time.Time{},
					UpdatedAt: time.Time{},
				},
				Name: "replication-1",
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation:        "us-e4",
					DestinationReplicationUUID: "replication-uuid-1",
				},
			},
		}

		expError := errors2.NewVCPError(errors2.ErrRegionZoneParsingError, errors.New("SomeError"))

		mockStorage.On("ListVolumeReplications", ctx, mock.Anything).Return(replications, nil)

		res, err := _getMultipleReplications(ctx, mockStorage, params)
		assert.Empty(tt, res)
		assert.EqualError(tt, err, expError.Error())
	})
	t.Run("WhenUtilParseRegionAndZoneReturnsErrorForSource", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)
		defer func() {
			getAccountWithName = _getAccountWithName
			utilParseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
			utilParseRegionAndZone = utils.ParseRegionAndZone
		}()

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{Name: "test-account"}, nil
		}

		utilParseAndValidateRegionAndZone = func(locationId string) (string, string, *gcpserver.Error) {
			return "us-e4", "", nil
		}

		utilParseRegionAndZone = func(locationID string) (string, string, error) {
			return "", "", errors.New("SomeError")
		}

		params := commonparams.GetMultipleReplicationsParams{
			AccountName: "test-account",
		}

		replications := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					ID:        1,
					UUID:      "uuid-1",
					CreatedAt: time.Time{},
					UpdatedAt: time.Time{},
				},
				Name: "replication-1",
				ReplicationAttributes: &datamodel.ReplicationDetails{
					SourceLocation:        "us-e4",
					SourceReplicationUUID: "replication-uuid-1",
				},
			},
		}

		expError := errors2.NewVCPError(errors2.ErrRegionZoneParsingError, errors.New("SomeError"))

		mockStorage.On("ListVolumeReplications", ctx, mock.Anything).Return(replications, nil)

		res, err := _getMultipleReplications(ctx, mockStorage, params)
		assert.Empty(tt, res)
		assert.EqualError(tt, err, expError.Error())
	})
	t.Run("WhenGetReplicationObjectsReturnsError", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)
		defer func() {
			getAccountWithName = _getAccountWithName
			getReplicationObjects = _getReplicationObjects
			utilParseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{Name: "test-account"}, nil
		}

		utilParseAndValidateRegionAndZone = func(locationId string) (string, string, *gcpserver.Error) {
			return "us-e4", "", nil
		}

		getReplicationObjects = func(ctx context.Context, regionReplicationMap map[string][]*datamodel.VolumeReplication, logger log.Logger, params commonparams.GetMultipleReplicationsParams) ([]*googleproxyclient.VolumeReplicationInternalV1beta, error) {
			return nil, errors.New("failed to get replication objects")
		}

		params := commonparams.GetMultipleReplicationsParams{
			AccountName: "test-account",
			LocationId:  "us-e4",
		}

		replications := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					ID:        1,
					UUID:      "uuid-1",
					CreatedAt: time.Time{},
					UpdatedAt: time.Time{},
				},
				Name: "replication-1",
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation:        "us-e4",
					DestinationReplicationUUID: "replication-uuid-1",
				},
			},
		}

		mockStorage.On("ListVolumeReplications", ctx, mock.Anything).Return(replications, nil)

		expError := errors.New("failed to get replication objects")

		res, err := _getMultipleReplications(ctx, mockStorage, params)
		assert.Empty(tt, res)
		assert.EqualError(tt, err, expError.Error())
	})
	t.Run("WhenHappyPath", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)
		defer func() {
			getAccountWithName = _getAccountWithName
			getReplicationObjects = _getReplicationObjects
			utilParseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{Name: "test-account"}, nil
		}

		utilParseAndValidateRegionAndZone = func(locationId string) (string, string, *gcpserver.Error) {
			return "us-e4", "", nil
		}

		getReplicationObjects = func(ctx context.Context, regionReplicationMap map[string][]*datamodel.VolumeReplication, logger log.Logger, params commonparams.GetMultipleReplicationsParams) ([]*googleproxyclient.VolumeReplicationInternalV1beta, error) {
			return []*googleproxyclient.VolumeReplicationInternalV1beta{
				{
					VolumeReplicationUuid: googleproxyclient.NewOptString("replication-uuid-1"),
					LifeCycleState:        googleproxyclient.NewOptVolumeReplicationInternalV1betaLifeCycleState(models.LifeCycleStateREADY),
					LifeCycleStateDetails: googleproxyclient.NewOptString(models.LifeCycleStateAvailableDetails),
					EndpointType:          models.DstEndpoint,
					ReplicationSchedule:   googleproxyclient.NewOptVolumeReplicationInternalV1betaReplicationSchedule(googleproxyclient.VolumeReplicationInternalV1betaReplicationScheduleHourly),
					RemoteRegion:          "us-e4",
					SourceHostName:        "source-host",
					SourceServerName:      "source-svm",
					SourceVolumeName:      "source-volume",
					SourceVolumeUuid:      googleproxyclient.NewOptString("source-volume-uuid"),
					SourcePoolUuid:        googleproxyclient.NewOptString("source-pool-uuid"),
					DestinationHostName:   "destination-host",
					DestinationServerName: "destination-svm",
					DestinationVolumeName: "destination-volume",
					DestinationVolumeUuid: googleproxyclient.NewOptString("destination-volume-uuid"),
					DestinationPoolUuid:   googleproxyclient.NewOptString("destination-pool-uuid"),
					Name:                  googleproxyclient.NewOptString("replication-1"),
					MirrorState:           googleproxyclient.NewOptVolumeReplicationInternalV1betaMirrorState(googleproxyclient.VolumeReplicationInternalV1betaMirrorStateMIRRORED),
					ReplicationType:       googleproxyclient.NewOptVolumeReplicationInternalV1betaReplicationType(googleproxyclient.VolumeReplicationInternalV1betaReplicationTypeCROSSREGIONREPLICATION),
					RelationshipStatus:    googleproxyclient.NewOptVolumeReplicationInternalV1betaRelationshipStatus(googleproxyclient.VolumeReplicationInternalV1betaRelationshipStatusIdle),
					TotalProgress:         googleproxyclient.NewOptInt64(100),
					Healthy:               googleproxyclient.NewOptBool(true),
					TotalTransferBytes:    googleproxyclient.NewOptInt64(1024 * 1024 * 1024), // 1 GB
					TotalTransferTimeSecs: googleproxyclient.NewOptInt64(3600),               // 1 hour
					LastTransferSize:      googleproxyclient.NewOptInt64(1024 * 1024 * 100),  // 100 MB
					LastTransferError:     googleproxyclient.NewOptString("No error"),
					LastTransferDuration:  googleproxyclient.NewOptInt64(300), // 5 minutes
					LastTransferEndTime:   googleproxyclient.NewOptDateTime(time.Now()),
					ProgressLastUpdated:   googleproxyclient.NewOptDateTime(time.Now()),
					LagTime:               googleproxyclient.NewOptInt64(60), // 1 minute
					CreatedAt:             googleproxyclient.NewOptDateTime(time.Now()),
					UpdatedAt:             googleproxyclient.NewOptDateTime(time.Now()),
					Jobs:                  nil,
					Description:           googleproxyclient.NewOptString("Test replication"),
				},
			}, nil
		}

		params := commonparams.GetMultipleReplicationsParams{
			AccountName: "test-account",
			LocationId:  "us-e4",
		}

		replications := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					ID:        1,
					UUID:      "uuid-1",
					CreatedAt: time.Time{},
					UpdatedAt: time.Time{},
				},
				Name: "replication-1",
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation:        "us-e4",
					DestinationReplicationUUID: "replication-uuid-1",
				},
			},
		}

		mockStorage.On("ListVolumeReplications", ctx, mock.Anything).Return(replications, nil)

		res, err := _getMultipleReplications(ctx, mockStorage, params)
		assert.Nil(tt, err)
		assert.Len(tt, res, 1)
		assert.Equal(tt, "replication-1", res[0].ResourceId.Value)
	})
}

func TestGetReplicationObjects(t *testing.T) {
	t.Run("WhenUtilsGetPairedRegionUriReturnsError", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		defer func() { utilsGetPairedRegionUri = utils.GetPairedRegionURI }()

		replications := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					ID:        1,
					UUID:      "uuid-1",
					CreatedAt: time.Time{},
					UpdatedAt: time.Time{},
				},
				Name: "replication-1",
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation:        "us-e4",
					DestinationReplicationUUID: "replication-uuid-1",
				},
				Uri:       "projects/45110233509/locations/us-east4/volumes/gosrcvolume1/replications/replication-name-6",
				RemoteUri: "projects/45110233509/locations/australia-southeast1/volumes/gosrcvolume1/replications/replication-name-6",
			},
		}

		utilsGetPairedRegionUri = func(locationId string) (string, error) {
			return "", errors.New("failed to get paired region URI")
		}

		replicationsMap := make(map[string][]*datamodel.VolumeReplication)
		replicationsMap["us-e4"] = replications

		expectedError := errors2.NewVCPError(errors2.ErrRegionZoneParsingError, errors.New("failed to get paired region URI"))

		_, err := _getReplicationObjects(ctx, replicationsMap, mockLogger, commonparams.GetMultipleReplicationsParams{})
		assert.NotNil(tt, err)
		assert.EqualError(tt, err, expectedError.Error())
	})
	t.Run("WhenGetProjectNumberForRegionReturnsError", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		defer func() {
			utilsGetPairedRegionUri = utils.GetPairedRegionURI
			GetProjectNumberForRegion = _getProjectNumberForRegion
		}()

		replications := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					ID:        1,
					UUID:      "uuid-1",
					CreatedAt: time.Time{},
					UpdatedAt: time.Time{},
				},
				Name: "replication-1",
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation:        "us-e4",
					DestinationReplicationUUID: "replication-uuid-1",
				},
				Uri:       "projects/45110233509/locations/us-east4/volumes/gosrcvolume1/replications/replication-name-6",
				RemoteUri: "projects/45110233509/locations/australia-southeast1/volumes/gosrcvolume1/replications/replication-name-6",
			},
		}

		utilsGetPairedRegionUri = func(locationId string) (string, error) {
			return "paired.region.uri", nil
		}

		GetProjectNumberForRegion = func(replication *datamodel.VolumeReplication, region string) (string, error) {
			return "", errors.New("failed to get project number for region")
		}

		replicationsMap := make(map[string][]*datamodel.VolumeReplication)
		replicationsMap["us-e4"] = replications

		expectedError := errors2.NewVCPError(errors2.ErrProjectParsingError, errors.New("failed to get project number for region"))

		_, err := _getReplicationObjects(ctx, replicationsMap, mockLogger, commonparams.GetMultipleReplicationsParams{})
		assert.NotNil(tt, err)
		assert.EqualError(tt, err, expectedError.Error())
	})
	t.Run("WhenAuthGetSignedJwtTokenReturnsError", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		defer func() {
			utilsGetPairedRegionUri = utils.GetPairedRegionURI
			GetProjectNumberForRegion = _getProjectNumberForRegion
			authGetSignedJwtToken = auth.GetSignedJwtToken
		}()

		replications := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					ID:        1,
					UUID:      "uuid-1",
					CreatedAt: time.Time{},
					UpdatedAt: time.Time{},
				},
				Name: "replication-1",
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation:        "us-e4",
					DestinationReplicationUUID: "replication-uuid-1",
				},
				Uri:       "projects/45110233509/locations/us-east4/volumes/gosrcvolume1/replications/replication-name-6",
				RemoteUri: "projects/45110233509/locations/australia-southeast1/volumes/gosrcvolume1/replications/replication-name-6",
			},
		}

		utilsGetPairedRegionUri = func(locationId string) (string, error) {
			return "paired.region.uri", nil
		}

		GetProjectNumberForRegion = func(replication *datamodel.VolumeReplication, region string) (string, error) {
			return "45110233509", nil
		}

		authGetSignedJwtToken = func(projectNumber string) (string, error) {
			return "", errors.New("failed to get signed JWT token")
		}

		replicationsMap := make(map[string][]*datamodel.VolumeReplication)
		replicationsMap["us-e4"] = replications

		expectedError := errors2.NewVCPError(errors2.ErrFailedToGenerateAccessToken, errors.New("failed to get signed JWT token"))

		_, err := _getReplicationObjects(ctx, replicationsMap, mockLogger, commonparams.GetMultipleReplicationsParams{})
		assert.NotNil(tt, err)
		assert.EqualError(tt, err, expectedError.Error())
	})
	t.Run("WhenGoogleProxyInternalGetMultipleReplicationsReturnsError", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		defer func() {
			utilsGetPairedRegionUri = utils.GetPairedRegionURI
			GetProjectNumberForRegion = _getProjectNumberForRegion
			authGetSignedJwtToken = auth.GetSignedJwtToken
			googleProxyInternalGetMultipleReplications = _googleProxyInternalGetMultipleReplications
		}()

		replications := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					ID:        1,
					UUID:      "uuid-1",
					CreatedAt: time.Time{},
					UpdatedAt: time.Time{},
				},
				Name: "replication-1",
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation:        "us-e4",
					DestinationReplicationUUID: "replication-uuid-1",
				},
				Uri:       "projects/45110233509/locations/us-east4/volumes/gosrcvolume1/replications/replication-name-6",
				RemoteUri: "projects/45110233509/locations/australia-southeast1/volumes/gosrcvolume1/replications/replication-name-6",
			},
		}

		utilsGetPairedRegionUri = func(locationId string) (string, error) {
			return "paired.region.uri", nil
		}

		GetProjectNumberForRegion = func(replication *datamodel.VolumeReplication, region string) (string, error) {
			return "45110233509", nil
		}

		authGetSignedJwtToken = func(projectNumber string) (string, error) {
			return "signed-jwt-token", nil
		}

		googleProxyInternalGetMultipleReplications = func(ctx context.Context, basePath, projectNumber, location, token string, body googleproxyclient.ReplicationIDListV1beta, logger log.Logger, paramz commonparams.GetMultipleReplicationsParams) ([]googleproxyclient.VolumeReplicationInternalV1beta, error) {
			return nil, errors.New("failed to get multiple replications")
		}

		replicationsMap := make(map[string][]*datamodel.VolumeReplication)
		replicationsMap["us-e4"] = replications

		expectedError := errors.New("failed to get multiple replications")
		_, err := _getReplicationObjects(ctx, replicationsMap, mockLogger, commonparams.GetMultipleReplicationsParams{})
		assert.NotNil(tt, err)
		assert.EqualError(tt, err, expectedError.Error())
	})
	t.Run("WhenHappyPath", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		defer func() {
			utilsGetPairedRegionUri = utils.GetPairedRegionURI
			authGetSignedJwtToken = auth.GetSignedJwtToken
			googleProxyInternalGetMultipleReplications = _googleProxyInternalGetMultipleReplications
		}()

		replications := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					ID:        1,
					UUID:      "uuid-1",
					CreatedAt: time.Time{},
					UpdatedAt: time.Time{},
				},
				Name: "replication-1",
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation:        "us-e4",
					DestinationReplicationUUID: "replication-uuid-1",
				},
				Uri:       "projects/45110233509/locations/us-east4/volumes/gosrcvolume1/replications/replication-name-6",
				RemoteUri: "projects/45110233509/locations/australia-southeast1/volumes/gosrcvolume1/replications/replication-name-6",
			},
		}

		utilsGetPairedRegionUri = func(locationId string) (string, error) {
			return "paired.region.uri", nil
		}

		authGetSignedJwtToken = func(projectNumber string) (string, error) {
			return "signed-jwt-token", nil
		}

		googleProxyInternalGetMultipleReplications = func(ctx context.Context, basePath, projectNumber, location, token string, body googleproxyclient.ReplicationIDListV1beta, logger log.Logger, paramz commonparams.GetMultipleReplicationsParams) ([]googleproxyclient.VolumeReplicationInternalV1beta, error) {
			return []googleproxyclient.VolumeReplicationInternalV1beta{
				{
					VolumeReplicationUuid: googleproxyclient.NewOptString("replication-uuid-1"),
					LifeCycleState:        googleproxyclient.NewOptVolumeReplicationInternalV1betaLifeCycleState(models.LifeCycleStateREADY),
					LifeCycleStateDetails: googleproxyclient.NewOptString(models.LifeCycleStateAvailableDetails),
					EndpointType:          models.DstEndpoint,
					ReplicationSchedule:   googleproxyclient.NewOptVolumeReplicationInternalV1betaReplicationSchedule(googleproxyclient.VolumeReplicationInternalV1betaReplicationScheduleHourly),
					RemoteRegion:          "us-e4",
					SourceHostName:        "source-host",
					SourceServerName:      "source-svm",
					SourceVolumeName:      "source-volume",
					SourceVolumeUuid:      googleproxyclient.NewOptString("source-volume-uuid"),
					SourcePoolUuid:        googleproxyclient.NewOptString("source-pool-uuid"),
					DestinationHostName:   "destination-host",
					DestinationServerName: "destination-svm",
					DestinationVolumeName: "destination-volume",
					DestinationVolumeUuid: googleproxyclient.NewOptString("destination-volume-uuid"),
					DestinationPoolUuid:   googleproxyclient.NewOptString("destination-pool-uuid"),
					Name:                  googleproxyclient.NewOptString("replication-1"),
					MirrorState:           googleproxyclient.NewOptVolumeReplicationInternalV1betaMirrorState(googleproxyclient.VolumeReplicationInternalV1betaMirrorStateMIRRORED),
					ReplicationType:       googleproxyclient.NewOptVolumeReplicationInternalV1betaReplicationType(googleproxyclient.VolumeReplicationInternalV1betaReplicationTypeCROSSREGIONREPLICATION),
					RelationshipStatus:    googleproxyclient.NewOptVolumeReplicationInternalV1betaRelationshipStatus(googleproxyclient.VolumeReplicationInternalV1betaRelationshipStatusIdle),
					TotalProgress:         googleproxyclient.NewOptInt64(100),
					Healthy:               googleproxyclient.NewOptBool(true),
					TotalTransferBytes:    googleproxyclient.NewOptInt64(1024 * 1024 * 1024), // 1 GB
					TotalTransferTimeSecs: googleproxyclient.NewOptInt64(3600),               // 1 hour
					LastTransferSize:      googleproxyclient.NewOptInt64(1024 * 1024 * 100),  // 100 MB
					LastTransferError:     googleproxyclient.NewOptString("No error"),
					LastTransferDuration:  googleproxyclient.NewOptInt64(300), // 5 minutes
					LastTransferEndTime:   googleproxyclient.NewOptDateTime(time.Now()),
					ProgressLastUpdated:   googleproxyclient.NewOptDateTime(time.Now()),
					LagTime:               googleproxyclient.NewOptInt64(60), // 1 minute
					CreatedAt:             googleproxyclient.NewOptDateTime(time.Now()),
					UpdatedAt:             googleproxyclient.NewOptDateTime(time.Now()),
					Jobs:                  nil,
					Description:           googleproxyclient.NewOptString("Test replication"),
				},
			}, nil
		}

		replicationsMap := make(map[string][]*datamodel.VolumeReplication)
		replicationsMap["us-e4"] = replications

		res, err := _getReplicationObjects(ctx, replicationsMap, mockLogger, commonparams.GetMultipleReplicationsParams{})
		assert.Nil(tt, err)
		assert.Len(tt, res, 1)
		assert.Equal(tt, "replication-1", res[0].Name.Value)
	})
	t.Run("WhenHappyPathWithEmptyResult", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		defer func() {
			utilsGetPairedRegionUri = utils.GetPairedRegionURI
			authGetSignedJwtToken = auth.GetSignedJwtToken
			googleProxyInternalGetMultipleReplications = _googleProxyInternalGetMultipleReplications
		}()

		replications := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					ID:        1,
					UUID:      "uuid-1",
					CreatedAt: time.Time{},
					UpdatedAt: time.Time{},
				},
				Name: "replication-1",
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation:        "us-e4",
					DestinationReplicationUUID: "replication-uuid-1",
				},
				Uri:       "projects/45110233509/locations/us-east4/volumes/gosrcvolume1/replications/replication-name-6",
				RemoteUri: "projects/45110233509/locations/australia-southeast1/volumes/gosrcvolume1/replications/replication-name-6",
			},
		}

		utilsGetPairedRegionUri = func(locationId string) (string, error) {
			return "paired.region.uri", nil
		}

		authGetSignedJwtToken = func(projectNumber string) (string, error) {
			return "signed-jwt-token", nil
		}

		googleProxyInternalGetMultipleReplications = func(ctx context.Context, basePath, projectNumber, location, token string, body googleproxyclient.ReplicationIDListV1beta, logger log.Logger, paramz commonparams.GetMultipleReplicationsParams) ([]googleproxyclient.VolumeReplicationInternalV1beta, error) {
			return []googleproxyclient.VolumeReplicationInternalV1beta{}, nil
		}

		replicationsMap := make(map[string][]*datamodel.VolumeReplication)
		replicationsMap["us-e4"] = replications

		res, err := _getReplicationObjects(ctx, replicationsMap, mockLogger, commonparams.GetMultipleReplicationsParams{})
		assert.Nil(tt, err)
		assert.Len(tt, res, 0)
	})
}

func TestGoogleProxyInternalGetMultipleReplications(t *testing.T) {
	t.Run("WhenGoogleProxyClientGetMultipleReplicationsReturnsError", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockClient := googleproxyclient.NewMockInvoker(t)

		basePath := "https://example.com"
		projectNumber := "1234567890"
		location := "us-central1"
		token := "test-token"
		body := googleproxyclient.ReplicationIDListV1beta{
			ReplicationUUIDs: []string{"replication-id-1"},
		}

		paramz := googleproxyclient.V1betaGetMultipleReplicationsInternalParams{
			ProjectNumber:  projectNumber,
			LocationId:     location,
			XCorrelationID: googleproxyclient.NewOptString("test-correlation-id"),
		}

		params := commonparams.GetMultipleReplicationsParams{
			ReplicationURIs: []string{"projects/1234567890/locations/us-central1/volumes/gosrcvolume1/replications/replication-id-1"},
			LocationId:      location,
			XCorrelationID:  "test-correlation-id",
		}

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockClient.EXPECT().V1betaGetMultipleReplicationsInternal(ctx, &body, paramz).Return(nil, errors.New("failed to get multiple replications"))

		expectedError := errors2.NewVCPError(errors2.ErrGoogleProxyInternalGetMultipleReplications, errors.New("failed to get multiple replications"))

		res, err := _googleProxyInternalGetMultipleReplications(ctx, basePath, projectNumber, location, token, body, mockLogger, params)
		assert.Nil(tt, res)
		assert.NotNil(tt, err)
		assert.Equal(tt, expectedError.Error(), err.Error())
	})
	t.Run("WhenBadRequestErrorIsReturned", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockClient := googleproxyclient.NewMockInvoker(t)

		basePath := "https://example.com"
		projectNumber := "1234567890"
		location := "us-central1"
		token := "test-token"
		body := googleproxyclient.ReplicationIDListV1beta{
			ReplicationUUIDs: []string{"replication-id-1"},
		}

		paramz := googleproxyclient.V1betaGetMultipleReplicationsInternalParams{
			ProjectNumber:  projectNumber,
			LocationId:     location,
			XCorrelationID: googleproxyclient.NewOptString("test-correlation-id"),
		}

		params := commonparams.GetMultipleReplicationsParams{
			ReplicationURIs: []string{"projects/1234567890/locations/us-central1/volumes/gosrcvolume1/replications/replication-id-1"},
			LocationId:      location,
			XCorrelationID:  "test-correlation-id",
		}

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		errResp := googleproxyclient.V1betaGetMultipleReplicationsInternalBadRequest{
			Code:    400,
			Message: "bad request error",
		}
		mockClient.EXPECT().V1betaGetMultipleReplicationsInternal(ctx, &body, paramz).Return(&errResp, nil)

		expectedError := errors2.NewVCPError(errors2.ErrGoogleProxyInternalGetMultipleReplications, errors.New("bad request error"))

		res, err := _googleProxyInternalGetMultipleReplications(ctx, basePath, projectNumber, location, token, body, mockLogger, params)
		assert.Nil(tt, res)
		assert.NotNil(tt, err)
		assert.Equal(tt, expectedError.Error(), err.Error())
	})
	t.Run("WhenInternalServerErrorIsReturned", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockClient := googleproxyclient.NewMockInvoker(t)

		basePath := "https://example.com"
		projectNumber := "1234567890"
		location := "us-central1"
		token := "test-token"
		body := googleproxyclient.ReplicationIDListV1beta{
			ReplicationUUIDs: []string{"replication-id-1"},
		}

		paramz := googleproxyclient.V1betaGetMultipleReplicationsInternalParams{
			ProjectNumber:  projectNumber,
			LocationId:     location,
			XCorrelationID: googleproxyclient.NewOptString("test-correlation-id"),
		}

		params := commonparams.GetMultipleReplicationsParams{
			ReplicationURIs: []string{"projects/1234567890/locations/us-central1/volumes/gosrcvolume1/replications/replication-id-1"},
			LocationId:      location,
			XCorrelationID:  "test-correlation-id",
		}

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		errResp := googleproxyclient.V1betaGetMultipleReplicationsInternalInternalServerError{
			Code:    500,
			Message: "internal server error",
		}
		mockClient.EXPECT().V1betaGetMultipleReplicationsInternal(ctx, &body, paramz).Return(&errResp, nil)

		expectedError := errors2.NewVCPError(errors2.ErrGoogleProxyInternalGetMultipleReplications, errors.New("internal server error"))

		res, err := _googleProxyInternalGetMultipleReplications(ctx, basePath, projectNumber, location, token, body, mockLogger, params)
		assert.Nil(tt, res)
		assert.NotNil(tt, err)
		assert.Equal(tt, expectedError.Error(), err.Error())
	})
	t.Run("WhenUnauthorizedErrorIsReturned", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockClient := googleproxyclient.NewMockInvoker(t)

		basePath := "https://example.com"
		projectNumber := "1234567890"
		location := "us-central1"
		token := "test-token"
		body := googleproxyclient.ReplicationIDListV1beta{
			ReplicationUUIDs: []string{"replication-id-1"},
		}

		paramz := googleproxyclient.V1betaGetMultipleReplicationsInternalParams{
			ProjectNumber:  projectNumber,
			LocationId:     location,
			XCorrelationID: googleproxyclient.NewOptString("test-correlation-id"),
		}

		params := commonparams.GetMultipleReplicationsParams{
			ReplicationURIs: []string{"projects/1234567890/locations/us-central1/volumes/gosrcvolume1/replications/replication-id-1"},
			LocationId:      location,
			XCorrelationID:  "test-correlation-id",
		}

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		errResp := googleproxyclient.V1betaGetMultipleReplicationsInternalUnauthorized{
			Code:    401,
			Message: "unauthorized error",
		}
		mockClient.EXPECT().V1betaGetMultipleReplicationsInternal(ctx, &body, paramz).Return(&errResp, nil)

		expectedError := errors2.NewVCPError(errors2.ErrGoogleProxyInternalGetMultipleReplications, errors.New("unauthorized error"))

		res, err := _googleProxyInternalGetMultipleReplications(ctx, basePath, projectNumber, location, token, body, mockLogger, params)
		assert.Nil(tt, res)
		assert.NotNil(tt, err)
		assert.Equal(tt, expectedError.Error(), err.Error())
	})
	t.Run("WhenNotFoundErrorIsReturned", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockClient := googleproxyclient.NewMockInvoker(t)

		basePath := "https://example.com"
		projectNumber := "1234567890"
		location := "us-central1"
		token := "test-token"
		body := googleproxyclient.ReplicationIDListV1beta{
			ReplicationUUIDs: []string{"replication-id-1"},
		}

		paramz := googleproxyclient.V1betaGetMultipleReplicationsInternalParams{
			ProjectNumber:  projectNumber,
			LocationId:     location,
			XCorrelationID: googleproxyclient.NewOptString("test-correlation-id"),
		}

		params := commonparams.GetMultipleReplicationsParams{
			ReplicationURIs: []string{"projects/1234567890/locations/us-central1/volumes/gosrcvolume1/replications/replication-id-1"},
			LocationId:      location,
			XCorrelationID:  "test-correlation-id",
		}

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		errResp := googleproxyclient.V1betaGetMultipleReplicationsInternalNotFound{
			Code:    404,
			Message: "not found error",
		}
		mockClient.EXPECT().V1betaGetMultipleReplicationsInternal(ctx, &body, paramz).Return(&errResp, nil)

		expectedError := errors2.NewVCPError(errors2.ErrGoogleProxyInternalGetMultipleReplications, errors.New("not found error"))

		res, err := _googleProxyInternalGetMultipleReplications(ctx, basePath, projectNumber, location, token, body, mockLogger, params)
		assert.Nil(tt, res)
		assert.NotNil(tt, err)
		assert.Equal(tt, expectedError.Error(), err.Error())
	})
	t.Run("WhenHappyPath", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockClient := googleproxyclient.NewMockInvoker(t)

		basePath := "https://example.com"
		projectNumber := "1234567890"
		location := "us-central1"
		token := "test-token"
		body := googleproxyclient.ReplicationIDListV1beta{
			ReplicationUUIDs: []string{"replication-id-1"},
		}

		paramz := googleproxyclient.V1betaGetMultipleReplicationsInternalParams{
			ProjectNumber:  projectNumber,
			LocationId:     location,
			XCorrelationID: googleproxyclient.NewOptString("test-correlation-id"),
		}

		params := commonparams.GetMultipleReplicationsParams{
			ReplicationURIs: []string{"projects/1234567890/locations/us-central1/volumes/gosrcvolume1/replications/replication-id-1"},
			LocationId:      location,
			XCorrelationID:  "test-correlation-id",
		}

		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		resp := googleproxyclient.V1betaGetMultipleReplicationsInternalOK{
			Replications: []googleproxyclient.VolumeReplicationInternalV1beta{
				{
					Name:                  googleproxyclient.NewOptString("replication-1"),
					DestinationVolumeUuid: googleproxyclient.NewOptString("destination-volume-uuid"),
					DestinationServerName: "destination-svm",
					DestinationHostName:   "destination-host",
					DestinationPoolUuid:   googleproxyclient.NewOptString("destination-pool-uuid"),
					MirrorState:           googleproxyclient.NewOptVolumeReplicationInternalV1betaMirrorState(googleproxyclient.VolumeReplicationInternalV1betaMirrorStateMIRRORED),
					Description:           googleproxyclient.NewOptString("Test replication"),
				},
			},
		}

		mockClient.EXPECT().V1betaGetMultipleReplicationsInternal(ctx, &body, paramz).Return(&resp, nil)

		res, err := _googleProxyInternalGetMultipleReplications(ctx, basePath, projectNumber, location, token, body, mockLogger, params)
		assert.Nil(tt, err)
		assert.Len(tt, res, 1)
		assert.Equal(tt, "replication-1", res[0].Name.Value)
		assert.Equal(tt, "destination-volume-uuid", res[0].DestinationVolumeUuid.Value)
	})
}

func TestConvertInternalReplicationToCCFEModel(t *testing.T) {
	t.Run("WhenHappyPath", func(tt *testing.T) {
		replication := &googleproxyclient.VolumeReplicationInternalV1beta{
			VolumeReplicationUuid: googleproxyclient.NewOptString("replication-uuid-1"),
			LifeCycleState:        googleproxyclient.NewOptVolumeReplicationInternalV1betaLifeCycleState(models.LifeCycleStateREADY),
			LifeCycleStateDetails: googleproxyclient.NewOptString(models.LifeCycleStateAvailableDetails),
			EndpointType:          models.DstEndpoint,
			ReplicationSchedule:   googleproxyclient.NewOptVolumeReplicationInternalV1betaReplicationSchedule(googleproxyclient.VolumeReplicationInternalV1betaReplicationScheduleHourly),
			RemoteRegion:          "us-e4",
			SourceHostName:        "source-host",
			SourceServerName:      "source-svm",
			SourceVolumeName:      "source-volume",
			SourceVolumeUuid:      googleproxyclient.NewOptString("source-volume-uuid"),
			SourcePoolUuid:        googleproxyclient.NewOptString("source-pool-uuid"),
			DestinationHostName:   "destination-host",
			DestinationServerName: "destination-svm",
			DestinationVolumeName: "destination-volume",
			DestinationVolumeUuid: googleproxyclient.NewOptString("destination-volume-uuid"),
			DestinationPoolUuid:   googleproxyclient.NewOptString("destination-pool-uuid"),
			Name:                  googleproxyclient.NewOptString("replication-1"),
			MirrorState:           googleproxyclient.NewOptVolumeReplicationInternalV1betaMirrorState(googleproxyclient.VolumeReplicationInternalV1betaMirrorStateMIRRORED),
			ReplicationType:       googleproxyclient.NewOptVolumeReplicationInternalV1betaReplicationType(googleproxyclient.VolumeReplicationInternalV1betaReplicationTypeCROSSREGIONREPLICATION),
			RelationshipStatus:    googleproxyclient.NewOptVolumeReplicationInternalV1betaRelationshipStatus(googleproxyclient.VolumeReplicationInternalV1betaRelationshipStatusIdle),
			TotalProgress:         googleproxyclient.NewOptInt64(100),
			Healthy:               googleproxyclient.NewOptBool(true),
			TotalTransferBytes:    googleproxyclient.NewOptInt64(1024 * 1024 * 1024), // 1 GB
			TotalTransferTimeSecs: googleproxyclient.NewOptInt64(3600),               // 1 hour
			LastTransferSize:      googleproxyclient.NewOptInt64(1024 * 1024 * 100),  // 100 MB
			LastTransferError:     googleproxyclient.NewOptString("No error"),
			LastTransferDuration:  googleproxyclient.NewOptInt64(300), // 5 minutes
			LastTransferEndTime:   googleproxyclient.NewOptDateTime(time.Now()),
			ProgressLastUpdated:   googleproxyclient.NewOptDateTime(time.Now()),
			LagTime:               googleproxyclient.NewOptInt64(60), // 1 minute
			CreatedAt:             googleproxyclient.NewOptDateTime(time.Now()),
			UpdatedAt:             googleproxyclient.NewOptDateTime(time.Now()),
			Jobs:                  nil,
			Description:           googleproxyclient.NewOptString("Test replication"),
		}

		result := convertInternalReplicationToCCFEModel(*replication, "us-e4")
		assert.NotNil(tt, result)
		assert.Equal(tt, "replication-1", result.ResourceId.Value)
		assert.Equal(tt, "destination-volume-uuid", result.Destination.Value.VolumeId.Value)
		assert.Equal(tt, gcpserver.ReplicationV1betaMirrorStateMIRRORED, result.MirrorState.Value)
		assert.Equal(tt, "Test replication", result.Description.Value)
	})
}

func TestMapInternalReplicationStateToCCFEState(t *testing.T) {
	tests := []struct {
		name     string
		input    googleproxyclient.VolumeReplicationInternalV1betaLifeCycleState
		expected gcpserver.ReplicationV1betaState
	}{
		{
			name:     "Available maps to READY",
			input:    googleproxyclient.VolumeReplicationInternalV1betaLifeCycleStateAvailable,
			expected: gcpserver.ReplicationV1betaStateREADY,
		},
		{
			name:     "CREATING maps to CREATING",
			input:    googleproxyclient.VolumeReplicationInternalV1betaLifeCycleStateCreating,
			expected: gcpserver.ReplicationV1betaStateCREATING,
		},
		{
			name:     "DELETING maps to DELETING",
			input:    googleproxyclient.VolumeReplicationInternalV1betaLifeCycleStateDeleting,
			expected: gcpserver.ReplicationV1betaStateDELETING,
		},
		{
			name:     "UPDATING maps to UPDATING",
			input:    googleproxyclient.VolumeReplicationInternalV1betaLifeCycleStateUpdating,
			expected: gcpserver.ReplicationV1betaStateUPDATING,
		},
		{
			name:     "Disabled maps to Disabled",
			input:    googleproxyclient.VolumeReplicationInternalV1betaLifeCycleStateDisabled,
			expected: gcpserver.ReplicationV1betaStateDISABLED,
		},
		{
			name:     "Unknown state maps to STATE_UNSPECIFIED",
			input:    googleproxyclient.VolumeReplicationInternalV1betaLifeCycleState("UNKNOWN"),
			expected: gcpserver.ReplicationV1betaStateSTATEUNSPECIFIED,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(tt *testing.T) {
			result := mapInternalReplicationStateToCCFEState(tc.input)
			assert.Equal(tt, tc.expected, result)
		})
	}
}

func TestMapInternalReplicationScheduleToCCFEReschedule(t *testing.T) {
	tests := []struct {
		name     string
		input    googleproxyclient.VolumeReplicationInternalV1betaReplicationSchedule
		expected gcpserver.ReplicationV1betaReplicationSchedule
	}{
		{
			name:     "Hourly maps to HOURLY",
			input:    googleproxyclient.VolumeReplicationInternalV1betaReplicationScheduleHourly,
			expected: gcpserver.ReplicationV1betaReplicationScheduleHOURLY,
		},
		{
			name:     "Daily maps to DAILY",
			input:    googleproxyclient.VolumeReplicationInternalV1betaReplicationScheduleDaily,
			expected: gcpserver.ReplicationV1betaReplicationScheduleDAILY,
		},
		{
			name:     "10 Minutely maps to Every 10 Minutes",
			input:    googleproxyclient.VolumeReplicationInternalV1betaReplicationSchedule10minutely,
			expected: gcpserver.ReplicationV1betaReplicationScheduleEVERY10MINUTES,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(tt *testing.T) {
			result := mapInternalReplicationScheduleToCCFEReschedule(tc.input)
			assert.Equal(tt, tc.expected, result)
		})
	}
}

func TestMapInternalReplicationMirrorStateToCCFEMirrorState(t *testing.T) {
	tests := []struct {
		name     string
		input    googleproxyclient.VolumeReplicationInternalV1betaMirrorState
		expected gcpserver.ReplicationV1betaMirrorState
	}{
		{
			name:     "MIRRORED maps to MIRRORED",
			input:    googleproxyclient.VolumeReplicationInternalV1betaMirrorStateMIRRORED,
			expected: gcpserver.ReplicationV1betaMirrorStateMIRRORED,
		},
		{
			name:     "Uninitialized maps to Uninitialized",
			input:    googleproxyclient.VolumeReplicationInternalV1betaMirrorStateUNINITIALIZED,
			expected: gcpserver.ReplicationV1betaMirrorStateUNINITIALIZED,
		},
		{
			name:     "Stopped maps to Stopped",
			input:    googleproxyclient.VolumeReplicationInternalV1betaMirrorStateSTOPPED,
			expected: gcpserver.ReplicationV1betaMirrorStateSTOPPED,
		},
		{
			name:     "Stopped maps to Stopped",
			input:    googleproxyclient.VolumeReplicationInternalV1betaMirrorStatePREPARING,
			expected: gcpserver.ReplicationV1betaMirrorStatePREPARING,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(tt *testing.T) {
			result := mapInternalReplicationMirrorStateToCCFEMirrorState(tc.input)
			assert.Equal(tt, tc.expected, result)
		})
	}
}
