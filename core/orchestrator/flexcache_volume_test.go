package orchestrator

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	workflowEngineMock "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine"
)

func TestCreateFlexCacheVolume(t *testing.T) {
	setupStore := func(tt *testing.T) (*log.MockLogger, database.Storage) {
		mockLogger := log.NewMockLogger(tt)
		mockLogger.EXPECT().InfoContext(mock.Anything, "Running AutoMigrate for model changes")
		// Create a PersistenceStore instance with the in-memory database
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			t.Fatalf("Failed to create test storage: %v", err)
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:           "test_pool",
			AccountID:      account.ID,
			DeploymentName: "test_pool_deployment",
			VendorID:       "/projects/project123/locations/location123/pools/pool123",
			PoolAttributes: &datamodel.PoolAttributes{
				PrimaryZone: "us-west1-a",
			},
		}

		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{UUID: "test-svm-uuid"},
			Name:      "test_svm",
			AccountID: account.ID,
			PoolID:    pool.ID,
			Pool:      pool,
		}

		err = store.DB().Create(svm).Error
		if err != nil {
			tt.Fatalf("Failed to create svm: %v", err)
		}

		return mockLogger, store
	}

	t.Run("Success", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger, store := setupStore(tt)
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		mm := newMonkeyMockAndPatch(tt)

		params := &common.CreateVolumeParams{
			AccountName:   "test_account",
			Region:        "test_region",
			Name:          "test_volume",
			VendorID:      "test_vendor",
			QuotaInBytes:  minQuotaInBytesPool,
			Protocols:     []string{"NFS"},
			Description:   "Some description",
			DisplayName:   "Some display name",
			PoolID:        "test-pool-uuid",
			CreationToken: "test-creation-token",
		}

		dbAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-uuid",
			},
			Name: "test_account",
		}

		mm.EXPECT().utilGetLogger(ctx).Return(mockLogger)
		mm.EXPECT().getOrCreateAccount(mock.Anything, mock.Anything, params.AccountName).Return(dbAccount, nil)
		mm.EXPECT().validateCreateVolumeParams(mock.Anything, mock.Anything, params, mock.Anything).Return(nil)
		mm.EXPECT().utilsGetLocationFromVendorID(mock.Anything).Return("location", nil)
		mm.EXPECT().workflowsExecuteWorkflowSequentially(
			temporal, ctx, mock.Anything,
			mock.AnythingOfType("func(internal.Context, *common.CreateVolumeParams, *datamodel.Volume) error"),
			mock.Anything, mock.Anything, mock.Anything).Return(nil)

		volume, _, err := _createFlexCacheVolume(ctx, store, temporal, params)
		assert.NotNil(tt, volume, "Expected nil volume")
		assert.NoError(tt, err, "error not found")
		assert.Equal(tt, volume.DisplayName, "test_volume")
		assert.Equal(tt, volume.AccountName, "test_account")
		assert.Equal(tt, volume.PoolID, "test-pool-uuid")
		assert.Equal(tt, volume.PoolName, "test_pool")
		assert.Equal(tt, volume.VendorID, "")
		assert.Equal(tt, volume.CreationToken, "test-creation-token")
		assert.Equal(tt, volume.Description, "Some description")
		assert.Equal(tt, volume.ProtocolTypes, []string{"NFS"})
		assert.Equal(tt, volume.QuotaInBytes, minQuotaInBytesPool)
		assert.Equal(tt, volume.LifeCycleState, "PREPARING")
	})

	t.Run("GetOrCreateAccount_Error", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger, store := setupStore(tt)
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		mm := newMonkeyMockAndPatch(tt)

		params := &common.CreateVolumeParams{
			AccountName:   "test_account",
			Region:        "test_region",
			Name:          "test_volume",
			VendorID:      "test_vendor",
			QuotaInBytes:  minQuotaInBytesPool,
			Protocols:     []string{"NFS"},
			Description:   "Some description",
			DisplayName:   "Some display name",
			PoolID:        "test-pool-uuid",
			CreationToken: "test-creation-token",
		}

		mm.EXPECT().utilGetLogger(ctx).Return(mockLogger)
		mm.EXPECT().getOrCreateAccount(mock.Anything, mock.Anything, params.AccountName).Return(nil, assert.AnError)

		volume, jobID, err := _createFlexCacheVolume(ctx, store, temporal, params)
		assert.Nil(tt, volume)
		assert.Empty(tt, jobID)
		assert.Error(tt, err)
		assert.Equal(tt, assert.AnError, err)
	})

	t.Run("GetPool_Error", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger, store := setupStore(tt)
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		mm := newMonkeyMockAndPatch(tt)

		params := &common.CreateVolumeParams{
			AccountName:   "test_account",
			Region:        "test_region",
			Name:          "test_volume",
			VendorID:      "test_vendor",
			QuotaInBytes:  minQuotaInBytesPool,
			Protocols:     []string{"NFS"},
			Description:   "Some description",
			DisplayName:   "Some display name",
			PoolID:        "non-existent-pool-uuid",
			CreationToken: "test-creation-token",
		}

		dbAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-uuid",
			},
			Name: "test_account",
		}

		mm.EXPECT().utilGetLogger(ctx).Return(mockLogger)
		mm.EXPECT().getOrCreateAccount(mock.Anything, mock.Anything, params.AccountName).Return(dbAccount, nil)

		volume, jobID, err := _createFlexCacheVolume(ctx, store, temporal, params)
		assert.Nil(tt, volume)
		assert.Empty(tt, jobID)
		assert.Error(tt, err)
	})

	t.Run("ValidateCreateVolumeParams_Error", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger, store := setupStore(tt)
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		mm := newMonkeyMockAndPatch(tt)

		params := &common.CreateVolumeParams{
			AccountName:   "test_account",
			Region:        "test_region",
			Name:          "test_volume",
			VendorID:      "test_vendor",
			QuotaInBytes:  minQuotaInBytesPool,
			Protocols:     []string{"NFS"},
			Description:   "Some description",
			DisplayName:   "Some display name",
			PoolID:        "test-pool-uuid",
			CreationToken: "test-creation-token",
		}

		dbAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-uuid",
			},
			Name: "test_account",
		}

		mm.EXPECT().utilGetLogger(ctx).Return(mockLogger)
		mm.EXPECT().getOrCreateAccount(mock.Anything, mock.Anything, params.AccountName).Return(dbAccount, nil)
		mm.EXPECT().validateCreateVolumeParams(mock.Anything, mock.Anything, params, mock.Anything).Return(assert.AnError)

		volume, jobID, err := _createFlexCacheVolume(ctx, store, temporal, params)
		assert.Nil(tt, volume)
		assert.Empty(tt, jobID)
		assert.Error(tt, err)
		assert.Equal(tt, assert.AnError, err)
	})

	t.Run("GetSvmForPoolID_Error", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger := log.NewMockLogger(tt)
		store := database.NewMockStorage(tt)
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		mm := newMonkeyMockAndPatch(tt)

		params := &common.CreateVolumeParams{
			AccountName:   "test_account",
			Region:        "test_region",
			Name:          "test_volume",
			VendorID:      "test_vendor",
			QuotaInBytes:  minQuotaInBytesPool,
			Protocols:     []string{"NFS"},
			Description:   "Some description",
			DisplayName:   "Some display name",
			PoolID:        "test-pool-uuid",
			CreationToken: "test-creation-token",
		}

		dbAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-uuid",
			},
			Name: "test_account",
		}

		mm.EXPECT().utilGetLogger(ctx).Return(mockLogger)
		mm.EXPECT().getOrCreateAccount(mock.Anything, mock.Anything, params.AccountName).Return(dbAccount, nil)
		store.EXPECT().GetPool(ctx, params.PoolID, dbAccount.ID).Return(&datamodel.PoolView{}, nil)
		mm.EXPECT().validateCreateVolumeParams(mock.Anything, mock.Anything, params, mock.Anything).Return(nil)
		store.EXPECT().GetSvmForPoolID(ctx, int64(0)).Return(nil, assert.AnError)

		volume, jobID, err := _createFlexCacheVolume(ctx, store, temporal, params)
		assert.Nil(tt, volume)
		assert.Empty(tt, jobID)
		assert.Error(tt, err)
	})

	t.Run("CreateVolume_Error", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger := log.NewMockLogger(tt)
		store := database.NewMockStorage(tt)
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		mm := newMonkeyMockAndPatch(tt)

		params := &common.CreateVolumeParams{
			AccountName:   "test_account",
			Region:        "test_region",
			Name:          "test_volume",
			VendorID:      "test_vendor",
			QuotaInBytes:  minQuotaInBytesPool,
			Protocols:     []string{"NFS"},
			Description:   "Some description",
			DisplayName:   "Some display name",
			PoolID:        "test-pool-uuid",
			CreationToken: "test-creation-token",
		}

		dbAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-uuid",
			},
			Name: "test_account",
		}

		mm.EXPECT().utilGetLogger(ctx).Return(mockLogger)
		mm.EXPECT().getOrCreateAccount(mock.Anything, mock.Anything, params.AccountName).Return(dbAccount, nil)
		store.EXPECT().GetPool(ctx, params.PoolID, dbAccount.ID).Return(&datamodel.PoolView{}, nil)
		mm.EXPECT().validateCreateVolumeParams(mock.Anything, mock.Anything, params, mock.Anything).Return(nil)
		store.EXPECT().GetSvmForPoolID(ctx, int64(0)).Return(&datamodel.Svm{}, nil)
		store.EXPECT().CreateVolume(ctx, mock.Anything).Return(nil, assert.AnError)

		volume, jobID, err := _createFlexCacheVolume(ctx, store, temporal, params)
		assert.Nil(tt, volume)
		assert.Empty(tt, jobID)
		assert.Error(tt, err)
	})

	t.Run("GetLocationFromVendorID_Error", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger, store := setupStore(tt)
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		mm := newMonkeyMockAndPatch(tt)

		params := &common.CreateVolumeParams{
			AccountName:   "test_account",
			Region:        "test_region",
			Name:          "test_volume",
			VendorID:      "test_vendor",
			QuotaInBytes:  minQuotaInBytesPool,
			Protocols:     []string{"NFS"},
			Description:   "Some description",
			DisplayName:   "Some display name",
			PoolID:        "test-pool-uuid",
			CreationToken: "test-creation-token",
		}

		dbAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-uuid",
			},
			Name: "test_account",
		}

		mm.EXPECT().utilGetLogger(ctx).Return(mockLogger)
		mm.EXPECT().getOrCreateAccount(mock.Anything, mock.Anything, params.AccountName).Return(dbAccount, nil)
		mm.EXPECT().validateCreateVolumeParams(mock.Anything, mock.Anything, params, mock.Anything).Return(nil)
		mm.EXPECT().utilsGetLocationFromVendorID(mock.Anything).Return("", assert.AnError)
		mockLogger.EXPECT().Errorf("Failed to get location from vendor ID for pool %s, error: %v", mock.Anything, assert.AnError)

		volume, jobID, err := _createFlexCacheVolume(ctx, store, temporal, params)
		assert.Nil(tt, volume)
		assert.Empty(tt, jobID)
		assert.Error(tt, err)
		assert.Equal(tt, assert.AnError, err)
	})

	t.Run("CreateJob_Error", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger := log.NewMockLogger(tt)
		store := database.NewMockStorage(tt)
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		mm := newMonkeyMockAndPatch(tt)

		params := &common.CreateVolumeParams{
			AccountName:   "test_account",
			Region:        "test_region",
			Name:          "test_volume",
			VendorID:      "test_vendor",
			QuotaInBytes:  minQuotaInBytesPool,
			Protocols:     []string{"NFS"},
			Description:   "Some description",
			DisplayName:   "Some display name",
			PoolID:        "test-pool-uuid",
			CreationToken: "test-creation-token",
		}

		dbAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-uuid",
			},
			Name: "test_account",
		}

		mm.EXPECT().utilGetLogger(ctx).Return(mockLogger)
		mm.EXPECT().getOrCreateAccount(mock.Anything, mock.Anything, params.AccountName).Return(dbAccount, nil)
		store.EXPECT().GetPool(ctx, params.PoolID, dbAccount.ID).Return(&datamodel.PoolView{}, nil)
		mm.EXPECT().validateCreateVolumeParams(mock.Anything, mock.Anything, params, mock.Anything).Return(nil)
		mm.EXPECT().utilsGetLocationFromVendorID(mock.Anything).Return("location", nil)
		store.EXPECT().GetSvmForPoolID(ctx, int64(0)).Return(&datamodel.Svm{}, nil)
		store.EXPECT().CreateVolume(ctx, mock.Anything).Return(&datamodel.Volume{
			Pool: &datamodel.Pool{},
		}, nil)
		store.EXPECT().CreateJob(ctx, mock.Anything).Return(nil, assert.AnError)
		mockLogger.EXPECT().Errorf("Failed to create job in database, error: %v", mock.Anything)

		volume, jobID, err := _createFlexCacheVolume(ctx, store, temporal, params)
		assert.Nil(tt, volume)
		assert.Empty(tt, jobID)
		assert.Error(tt, err)
	})

	t.Run("ExecuteWorkflowSequentially_Error", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger, store := setupStore(tt)
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		mm := newMonkeyMockAndPatch(tt)

		params := &common.CreateVolumeParams{
			AccountName:   "test_account",
			Region:        "test_region",
			Name:          "test_volume",
			VendorID:      "test_vendor",
			QuotaInBytes:  minQuotaInBytesPool,
			Protocols:     []string{"NFS"},
			Description:   "Some description",
			DisplayName:   "Some display name",
			PoolID:        "test-pool-uuid",
			CreationToken: "test-creation-token",
		}

		dbAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-uuid",
			},
			Name: "test_account",
		}

		mm.EXPECT().utilGetLogger(ctx).Return(mockLogger)
		mm.EXPECT().getOrCreateAccount(mock.Anything, mock.Anything, params.AccountName).Return(dbAccount, nil)
		mm.EXPECT().validateCreateVolumeParams(mock.Anything, mock.Anything, params, mock.Anything).Return(nil)
		mm.EXPECT().utilsGetLocationFromVendorID(mock.Anything).Return("location", nil)
		mm.EXPECT().workflowsExecuteWorkflowSequentially(
			temporal, ctx, mock.Anything,
			mock.AnythingOfType("func(internal.Context, *common.CreateVolumeParams, *datamodel.Volume) error"),
			mock.Anything, mock.Anything, mock.Anything).Return(assert.AnError)
		mockLogger.EXPECT().Errorf("Failed to start create FlexCache volume workflow, error: %v", assert.AnError)

		volume, jobID, err := _createFlexCacheVolume(ctx, store, temporal, params)
		assert.Nil(tt, volume)
		assert.Empty(tt, jobID)
		assert.Error(tt, err)
		assert.Equal(tt, assert.AnError, err)
	})

	t.Run("Success_WithFileProperties", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger, store := setupStore(tt)
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		mm := newMonkeyMockAndPatch(tt)

		params := &common.CreateVolumeParams{
			AccountName:   "test_account",
			Region:        "test_region",
			Name:          "test_volume",
			VendorID:      "test_vendor",
			QuotaInBytes:  minQuotaInBytesPool,
			Protocols:     []string{"NFS"},
			Description:   "Some description",
			DisplayName:   "Some display name",
			PoolID:        "test-pool-uuid",
			CreationToken: "test-creation-token",
			FileProperties: &models.FileProperties{
				ExportPolicy: &models.ExportPolicy{
					ExportPolicyName: "test-export-policy",
					ExportRules: []*models.ExportRule{
						{
							AllowedClients: "192.168.1.0/24",
							AccessType:     "ReadWrite",
							CIFS:           false,
							NFSv3:          true,
							NFSv4:          true,
							Index:          1,
						},
					},
				},
			},
		}

		dbAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-uuid",
			},
			Name: "test_account",
		}

		mm.EXPECT().utilGetLogger(ctx).Return(mockLogger)
		mm.EXPECT().getOrCreateAccount(mock.Anything, mock.Anything, params.AccountName).Return(dbAccount, nil)
		mm.EXPECT().validateCreateVolumeParams(mock.Anything, mock.Anything, params, mock.Anything).Return(nil)
		mm.EXPECT().utilsGetLocationFromVendorID(mock.Anything).Return("location", nil)
		mm.EXPECT().workflowsExecuteWorkflowSequentially(
			temporal, ctx, mock.Anything,
			mock.AnythingOfType("func(internal.Context, *common.CreateVolumeParams, *datamodel.Volume) error"),
			mock.Anything, mock.Anything, mock.Anything).Return(nil)

		volume, jobID, err := _createFlexCacheVolume(ctx, store, temporal, params)
		assert.NotNil(tt, volume, "Expected non-nil volume")
		assert.NotEmpty(tt, jobID, "Expected non-empty job ID")
		assert.NoError(tt, err, "Expected no error")
		assert.Equal(tt, volume.DisplayName, "test_volume")
		assert.Equal(tt, volume.AccountName, "test_account")
		assert.Equal(tt, volume.PoolID, "test-pool-uuid")
		assert.Equal(tt, volume.PoolName, "test_pool")
		assert.Equal(tt, volume.VendorID, "")
		assert.Equal(tt, volume.CreationToken, "test-creation-token")
		assert.Equal(tt, volume.Description, "Some description")
		assert.Equal(tt, volume.ProtocolTypes, []string{"NFS"})
		assert.Equal(tt, volume.QuotaInBytes, minQuotaInBytesPool)
		assert.Equal(tt, volume.LifeCycleState, "PREPARING")
	})

	t.Run("Success_WithFileProperties_NoExportPolicy", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger, store := setupStore(tt)
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		mm := newMonkeyMockAndPatch(tt)

		params := &common.CreateVolumeParams{
			AccountName:    "test_account",
			Region:         "test_region",
			Name:           "test_volume",
			VendorID:       "test_vendor",
			QuotaInBytes:   minQuotaInBytesPool,
			Protocols:      []string{"NFS"},
			Description:    "Some description",
			DisplayName:    "Some display name",
			PoolID:         "test-pool-uuid",
			CreationToken:  "test-creation-token",
			FileProperties: &models.FileProperties{
				// No ExportPolicy set
			},
		}

		dbAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-uuid",
			},
			Name: "test_account",
		}

		mm.EXPECT().utilGetLogger(ctx).Return(mockLogger)
		mm.EXPECT().getOrCreateAccount(mock.Anything, mock.Anything, params.AccountName).Return(dbAccount, nil)
		mm.EXPECT().validateCreateVolumeParams(mock.Anything, mock.Anything, params, mock.Anything).Return(nil)
		mm.EXPECT().utilsGetLocationFromVendorID(mock.Anything).Return("location", nil)
		mm.EXPECT().workflowsExecuteWorkflowSequentially(
			temporal, ctx, mock.Anything,
			mock.AnythingOfType("func(internal.Context, *common.CreateVolumeParams, *datamodel.Volume) error"),
			mock.Anything, mock.Anything, mock.Anything).Return(nil)

		volume, jobID, err := _createFlexCacheVolume(ctx, store, temporal, params)
		assert.NotNil(tt, volume, "Expected non-nil volume")
		assert.NotEmpty(tt, jobID, "Expected non-empty job ID")
		assert.NoError(tt, err, "Expected no error")
		assert.Equal(tt, volume.DisplayName, "test_volume")
		assert.Equal(tt, volume.AccountName, "test_account")
		assert.Equal(tt, volume.PoolID, "test-pool-uuid")
		assert.Equal(tt, volume.PoolName, "test_pool")
		assert.Equal(tt, volume.VendorID, "")
		assert.Equal(tt, volume.CreationToken, "test-creation-token")
		assert.Equal(tt, volume.Description, "Some description")
		assert.Equal(tt, volume.ProtocolTypes, []string{"NFS"})
		assert.Equal(tt, volume.QuotaInBytes, minQuotaInBytesPool)
		assert.Equal(tt, volume.LifeCycleState, "PREPARING")
	})
}

func TestOrchestrator_CreateFlexCacheVolume(t *testing.T) {
	ctx := context.Background()
	mm := newMonkeyMockAndPatch(t)
	mockStorage := &database.MockStorage{}
	mockTemporal := workflowEngineMock.NewMockTemporalTestClient(t)
	orch := &Orchestrator{
		storage:  mockStorage,
		temporal: mockTemporal,
	}

	params := &common.CreateVolumeParams{Name: "vol"}

	mm.EXPECT().createFlexCacheVolume(ctx, mockStorage, mockTemporal, params).Return(&models.Volume{DisplayName: "vol"}, "job-id", nil)

	// Act
	vol, jobID, err := orch.CreateFlexCacheVolume(ctx, params)
	if err != nil {
		return
	}

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, "vol", vol.DisplayName)
	assert.Equal(t, "job-id", jobID)
}

func Test_EstablishVolumePeering(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
	params := &common.EstablishVolumePeeringParams{
		AccountName:     "account",
		Region:          "region",
		Zone:            "zone",
		Name:            "test_volume",
		PeerClusterName: "peer-cluster",
		PeerAddresses:   []string{"1.1.1.1", "2.2.2.2"},
		ExpiryTime:      time.Now().Add(time.Hour),
		PeerSvmName:     "peer-svm",
		PeerVolumeName:  "peer-volume",
		Passphrase:      "passphrase",
	}

	volumeParams := &common.CreateVolumeParams{
		AccountName: params.AccountName,
		Region:      params.Region,
		Name:        params.Name,
		Zone:        params.Zone,
		CacheParameters: &models.CacheParameters{
			PeerClusterName: params.PeerClusterName,
			PeerSvmName:     params.PeerSvmName,
			PeerVolumeName:  params.PeerVolumeName,
			PeerIPAddresses: params.PeerAddresses,
			PeerExpiryTime:  &params.ExpiryTime,
			Passphrase:      &params.Passphrase,
		},
	}

	dbAccount := &datamodel.Account{
		BaseModel: datamodel.BaseModel{UUID: "test-uuid"},
		Name:      "test_account",
	}
	dbPool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
		Name:      "test_pool",
		VendorID:  "vendor-id",
	}
	dbVolume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "vol-uuid"},
		Name:      "test_volume",
		Account:   dbAccount,
		AccountID: dbAccount.ID,
		Pool:      dbPool,
		PoolID:    dbPool.ID,
	}
	t.Run("GetVolume_Error", func(tt *testing.T) {
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		mm := newMonkeyMockAndPatch(t)
		mockLogger := log.NewMockLogger(t)
		mockStorage := new(database.MockStorage)
		mockStorage.On("GetVolume", mock.Anything, params.Name).Return(nil, assert.AnError)
		mm.EXPECT().utilGetLogger(ctx).Return(mockLogger)

		vol, err := _establishFlexCacheVolumePeering(ctx, mockStorage, temporal, params)
		assert.Error(tt, err)
		assert.Nil(tt, vol)
	})

	t.Run("GetOrCreateAccount_Error", func(tt *testing.T) {
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		mm := newMonkeyMockAndPatch(t)
		mockLogger := log.NewMockLogger(t)
		mockStorage := new(database.MockStorage)
		mockStorage.On("GetVolume", mock.Anything, params.Name).Return(dbVolume, nil)
		mm.EXPECT().utilGetLogger(ctx).Return(mockLogger)
		mm.EXPECT().getOrCreateAccount(mock.Anything, mock.Anything, mock.Anything).Return(nil, assert.AnError)

		vol, err := _establishFlexCacheVolumePeering(ctx, mockStorage, temporal, params)
		assert.Error(tt, err)
		assert.Nil(tt, vol)
	})

	t.Run("CreateJob_Error", func(tt *testing.T) {
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		mm := newMonkeyMockAndPatch(t)
		mockLogger := log.NewMockLogger(t)
		mockStorage := new(database.MockStorage)
		mockStorage.On("GetVolume", mock.Anything, params.Name).Return(dbVolume, nil)
		mm.EXPECT().utilGetLogger(ctx).Return(mockLogger)
		mm.EXPECT().getOrCreateAccount(mock.Anything, mock.Anything, mock.Anything).Return(dbAccount, nil)
		mockStorage.On("CreateJob", mock.Anything, mock.Anything).Return(nil, assert.AnError)
		mockLogger.EXPECT().Errorf("Failed to create job in database, error: %v", assert.AnError)

		vol, err := _establishFlexCacheVolumePeering(ctx, mockStorage, temporal, params)
		assert.Error(tt, err)
		assert.Nil(tt, vol)
	})

	t.Run("WorkflowsExecuteWorkflowSequentially_Error", func(tt *testing.T) {
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		mm := newMonkeyMockAndPatch(t)
		mockLogger := log.NewMockLogger(t)
		mockStorage := new(database.MockStorage)
		mockStorage.On("GetVolume", mock.Anything, params.Name).Return(dbVolume, nil)
		mockStorage.On("CreateJob", mock.Anything, mock.Anything).Return(&datamodel.Job{WorkflowID: "wf-id"}, nil)
		mm.EXPECT().utilGetLogger(ctx).Return(mockLogger)
		mm.EXPECT().getOrCreateAccount(mock.Anything, mock.Anything, mock.Anything).Return(dbAccount, nil)
		mm.EXPECT().workflowsExecuteWorkflowSequentially(
			temporal, ctx, mock.Anything,
			mock.AnythingOfType("func(internal.Context, *common.CreateVolumeParams, *datamodel.Volume) error"),
			mock.Anything, volumeParams, dbVolume).Return(assert.AnError)
		mockLogger.EXPECT().Errorf("Failed to start establish volume peering workflow, error: %v", assert.AnError)

		vol, err := _establishFlexCacheVolumePeering(ctx, mockStorage, temporal, params)
		assert.Error(tt, err)
		assert.Nil(tt, vol)
	})

	t.Run("Success", func(tt *testing.T) {
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		mm := newMonkeyMockAndPatch(t)
		mockLogger := log.NewMockLogger(t)
		mockStorage := new(database.MockStorage)
		mockStorage.On("GetVolume", mock.Anything, params.Name).Return(dbVolume, nil)
		mockStorage.On("CreateJob", mock.Anything, mock.Anything).Return(&datamodel.Job{WorkflowID: "wf-id"}, nil)
		mm.EXPECT().utilGetLogger(ctx).Return(mockLogger)
		mm.EXPECT().getOrCreateAccount(mock.Anything, mock.Anything, mock.Anything).Return(dbAccount, nil)
		mm.EXPECT().workflowsExecuteWorkflowSequentially(
			temporal, ctx, mock.Anything,
			mock.AnythingOfType("func(internal.Context, *common.CreateVolumeParams, *datamodel.Volume) error"),
			mock.Anything, volumeParams, dbVolume).Return(nil)
		origConvert := convertDatastoreVolumeToModel
		convertDatastoreVolumeToModel = func(_ *datamodel.Volume, _ *[]string) *models.Volume {
			return &models.Volume{BaseModel: models.BaseModel{UUID: "vol-uuid"}}
		}
		defer func() { convertDatastoreVolumeToModel = origConvert }()
		vol, err := _establishFlexCacheVolumePeering(ctx, mockStorage, temporal, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, vol)
		assert.Equal(tt, "vol-uuid", vol.UUID)
	})
}

func TestConvertEstablishVolumePeeringParams(t *testing.T) {
	t.Run("Mappings", func(t *testing.T) {
		expiry := time.Now().Add(90 * time.Minute).UTC()
		src := &common.EstablishVolumePeeringParams{
			AccountName:     "acct",
			Region:          "region-1",
			Zone:            "zone-a",
			Name:            "vol-name",
			PeerSvmName:     "peer-svm",
			PeerVolumeName:  "peer-vol",
			PeerClusterName: "peer-cluster",
			PeerAddresses:   []string{"10.0.0.1", "10.0.0.2"},
			ExpiryTime:      expiry,
		}

		out := convertEstablishVolumePeeringParamsToCreateVolumeParams(src)
		assert.NotNil(t, out)
		assert.Equal(t, src.Name, out.Name)
		assert.Equal(t, src.AccountName, out.AccountName)
		assert.Equal(t, src.Region, out.Region)
		assert.Equal(t, src.Zone, out.Zone)
		if assert.NotNil(t, out.CacheParameters) {
			cp := out.CacheParameters
			assert.Equal(t, src.PeerSvmName, cp.PeerSvmName)
			assert.Equal(t, src.PeerVolumeName, cp.PeerVolumeName)
			assert.Equal(t, src.PeerClusterName, cp.PeerClusterName)
			assert.Equal(t, src.PeerAddresses, cp.PeerIPAddresses)
			assert.NotNil(t, cp.PeerExpiryTime)
			assert.Equal(t, expiry, *cp.PeerExpiryTime)
		}
	})
}
