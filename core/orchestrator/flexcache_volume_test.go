package orchestrator

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	cvpmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/flexcache"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	workflowEngineMock "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine"
)

func TestCreateFlexCacheVolume(t *testing.T) {
	vendorID := "/projects/project123/locations/location123/pools/pool123"
	location := "location123"
	requestURI := "test-uri"
	correlationID := "test-correlation-id"
	poolName := "test_pool"
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
			Name:           poolName,
			AccountID:      account.ID,
			DeploymentName: "test_pool_deployment",
			VendorID:       vendorID,
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
		peerExpiryTime := time.Now().Add(1 * time.Hour)

		params := &common.CreateVolumeParams{
			AccountName:     "test_account",
			Region:          "test_region",
			Name:            "test_volume",
			VendorID:        "test_vendor",
			QuotaInBytes:    minQuotaInBytesPool,
			Protocols:       []string{"NFS"},
			Description:     "Some description",
			DisplayName:     "Some display name",
			PoolID:          "test-pool-uuid",
			CreationToken:   "test-creation-token",
			CacheParameters: &models.CacheParameters{PeerExpiryTime: &peerExpiryTime},
		}

		dbAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-uuid",
			},
			Name: "test_account",
		}

		event := &flexcache.CreateFlexCacheEvent{
			LocationID:    location,
			ProjectNumber: params.AccountName,
			RequestUri:    requestURI,
			CorrelationID: &correlationID,
		}

		mm.EXPECT().utilGetLogger(ctx).Return(mockLogger)
		mm.EXPECT().getOrCreateAccount(ctx, store, params.AccountName).Return(dbAccount, nil)
		mm.EXPECT().validateCreateVolumeParams(ctx, store, params, mock.AnythingOfType("*datamodel.PoolView")).Return(nil)
		mm.EXPECT().utilsGetLocationFromVendorID(vendorID).Return(location, nil)
		mm.EXPECT().verifyCommandExpiryTime(&peerExpiryTime).Return(nil)
		mm.EXPECT().utilsGetRequestIDFromContext(ctx).Return(requestURI)
		mm.EXPECT().utilsGetCorrelationIDFromContext(ctx).Return(correlationID)
		mm.EXPECT().workflowsExecuteWorkflowSequentially(
			temporal, ctx,
			mock.AnythingOfType("StartWorkflowOptions"),
			mock.AnythingOfType("func(internal.Context, *common.CreateVolumeParams, *datamodel.Volume, *flexcache.CreateFlexCacheEvent) error"),
			mock.AnythingOfType("ChildWorkflowOptions"),
			params, mock.AnythingOfType("*datamodel.Volume"), event).Return(nil)

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
		mm.EXPECT().getOrCreateAccount(ctx, store, params.AccountName).Return(nil, assert.AnError)

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
		mm.EXPECT().getOrCreateAccount(ctx, store, params.AccountName).Return(dbAccount, nil)

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
		mm.EXPECT().getOrCreateAccount(ctx, store, params.AccountName).Return(dbAccount, nil)
		mm.EXPECT().validateCreateVolumeParams(ctx, store, params, mock.AnythingOfType("*datamodel.PoolView")).Return(assert.AnError)

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
		mm.EXPECT().getOrCreateAccount(ctx, store, params.AccountName).Return(dbAccount, nil)
		store.EXPECT().GetPool(ctx, params.PoolID, dbAccount.ID).Return(&datamodel.PoolView{}, nil)
		mm.EXPECT().validateCreateVolumeParams(ctx, store, params, mock.AnythingOfType("*datamodel.PoolView")).Return(nil)
		store.EXPECT().GetSvmForPoolID(ctx, int64(0)).Return(nil, assert.AnError)

		volume, jobID, err := _createFlexCacheVolume(ctx, store, temporal, params)
		assert.Nil(tt, volume)
		assert.Empty(tt, jobID)
		assert.Error(tt, err)
	})

	t.Run("verifyCommandExpiryTime_Error", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger := log.NewMockLogger(tt)
		store := database.NewMockStorage(tt)
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		mm := newMonkeyMockAndPatch(tt)
		peerExpiryTime := time.Now().Add(1 * time.Hour)

		params := &common.CreateVolumeParams{
			AccountName:     "test_account",
			Region:          "test_region",
			Name:            "test_volume",
			VendorID:        "test_vendor",
			QuotaInBytes:    minQuotaInBytesPool,
			Protocols:       []string{"NFS"},
			Description:     "Some description",
			DisplayName:     "Some display name",
			PoolID:          "test-pool-uuid",
			CreationToken:   "test-creation-token",
			CacheParameters: &models.CacheParameters{PeerExpiryTime: &peerExpiryTime},
		}

		dbAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-uuid",
			},
			Name: "test_account",
		}

		mm.EXPECT().utilGetLogger(ctx).Return(mockLogger)
		mm.EXPECT().getOrCreateAccount(ctx, store, params.AccountName).Return(dbAccount, nil)
		store.EXPECT().GetPool(ctx, params.PoolID, dbAccount.ID).Return(&datamodel.PoolView{}, nil)
		mm.EXPECT().validateCreateVolumeParams(ctx, store, params, mock.AnythingOfType("*datamodel.PoolView")).Return(nil)
		store.EXPECT().GetSvmForPoolID(ctx, int64(0)).Return(&datamodel.Svm{}, nil)
		mm.EXPECT().verifyCommandExpiryTime(&peerExpiryTime).Return(assert.AnError)

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
		peerExpiryTime := time.Now().Add(1 * time.Hour)

		params := &common.CreateVolumeParams{
			AccountName:     "test_account",
			Region:          "test_region",
			Name:            "test_volume",
			VendorID:        "test_vendor",
			QuotaInBytes:    minQuotaInBytesPool,
			Protocols:       []string{"NFS"},
			Description:     "Some description",
			DisplayName:     "Some display name",
			PoolID:          "test-pool-uuid",
			CreationToken:   "test-creation-token",
			CacheParameters: &models.CacheParameters{PeerExpiryTime: &peerExpiryTime},
		}

		dbAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-uuid",
			},
			Name: "test_account",
		}

		mockLogger.On("Errorf", mock.AnythingOfType("string"), mock.Anything).Return()
		mm.EXPECT().utilGetLogger(ctx).Return(mockLogger)
		mm.EXPECT().getOrCreateAccount(ctx, store, params.AccountName).Return(dbAccount, nil)
		mm.EXPECT().verifyCommandExpiryTime(&peerExpiryTime).Return(nil)
		store.EXPECT().GetPool(ctx, params.PoolID, dbAccount.ID).Return(&datamodel.PoolView{}, nil)
		mm.EXPECT().validateCreateVolumeParams(ctx, store, params, mock.AnythingOfType("*datamodel.PoolView")).Return(nil)
		store.EXPECT().GetSvmForPoolID(ctx, int64(0)).Return(&datamodel.Svm{}, nil)
		store.EXPECT().CreateVolume(ctx, mock.AnythingOfType("*datamodel.Volume")).Return(nil, assert.AnError)

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
		peerExpiryTime := time.Now().Add(1 * time.Hour)

		params := &common.CreateVolumeParams{
			AccountName:     "test_account",
			Region:          "test_region",
			Name:            "test_volume",
			VendorID:        "test_vendor",
			QuotaInBytes:    minQuotaInBytesPool,
			Protocols:       []string{"NFS"},
			Description:     "Some description",
			DisplayName:     "Some display name",
			PoolID:          "test-pool-uuid",
			CreationToken:   "test-creation-token",
			CacheParameters: &models.CacheParameters{PeerExpiryTime: &peerExpiryTime},
		}

		dbAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-uuid",
			},
			Name: "test_account",
		}

		mm.EXPECT().utilGetLogger(ctx).Return(mockLogger)
		mm.EXPECT().verifyCommandExpiryTime(&peerExpiryTime).Return(nil)
		mm.EXPECT().getOrCreateAccount(ctx, store, params.AccountName).Return(dbAccount, nil)
		mm.EXPECT().validateCreateVolumeParams(ctx, store, params, mock.AnythingOfType("*datamodel.PoolView")).Return(nil)
		mm.EXPECT().utilsGetLocationFromVendorID(vendorID).Return("", assert.AnError)
		mockLogger.EXPECT().Errorf("Failed to get location from vendor ID for pool %s, error: %v", poolName, assert.AnError)

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
		peerExpiryTime := time.Now().Add(1 * time.Hour)

		params := &common.CreateVolumeParams{
			AccountName:     "test_account",
			Region:          "test_region",
			Name:            "test_volume",
			VendorID:        "test_vendor",
			QuotaInBytes:    minQuotaInBytesPool,
			Protocols:       []string{"NFS"},
			Description:     "Some description",
			DisplayName:     "Some display name",
			PoolID:          "test-pool-uuid",
			CreationToken:   "test-creation-token",
			CacheParameters: &models.CacheParameters{PeerExpiryTime: &peerExpiryTime},
		}

		dbAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-uuid",
			},
			Name: "test_account",
		}

		mm.EXPECT().utilGetLogger(ctx).Return(mockLogger)
		mm.EXPECT().verifyCommandExpiryTime(&peerExpiryTime).Return(nil)
		mm.EXPECT().getOrCreateAccount(ctx, store, params.AccountName).Return(dbAccount, nil)
		store.EXPECT().GetPool(ctx, params.PoolID, dbAccount.ID).Return(&datamodel.PoolView{}, nil)
		mm.EXPECT().validateCreateVolumeParams(ctx, store, params, mock.AnythingOfType("*datamodel.PoolView")).Return(nil)
		mm.EXPECT().utilsGetLocationFromVendorID(vendorID).Return(location, nil)
		mm.EXPECT().utilsGetRequestIDFromContext(ctx).Return(requestURI)
		mm.EXPECT().utilsGetCorrelationIDFromContext(ctx).Return(correlationID)
		store.EXPECT().GetSvmForPoolID(ctx, int64(0)).Return(&datamodel.Svm{}, nil)
		store.EXPECT().CreateVolume(ctx, mock.AnythingOfType("*datamodel.Volume")).Return(&datamodel.Volume{
			Pool: &datamodel.Pool{
				VendorID: vendorID,
			},
		}, nil)
		store.EXPECT().CreateJob(ctx, mock.AnythingOfType("*datamodel.Job")).Return(nil, assert.AnError)
		mockLogger.EXPECT().Errorf("Failed to create job in database, error: %v", assert.AnError)

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
		peerExpiryTime := time.Now().Add(1 * time.Hour)

		params := &common.CreateVolumeParams{
			AccountName:     "test_account",
			Region:          "test_region",
			Name:            "test_volume",
			VendorID:        "test_vendor",
			QuotaInBytes:    minQuotaInBytesPool,
			Protocols:       []string{"NFS"},
			Description:     "Some description",
			DisplayName:     "Some display name",
			PoolID:          "test-pool-uuid",
			CreationToken:   "test-creation-token",
			CacheParameters: &models.CacheParameters{PeerExpiryTime: &peerExpiryTime},
		}

		dbAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-uuid",
			},
			Name: "test_account",
		}

		event := &flexcache.CreateFlexCacheEvent{
			LocationID:    location,
			ProjectNumber: params.AccountName,
			RequestUri:    requestURI,
			CorrelationID: &correlationID,
		}

		mm.EXPECT().utilGetLogger(ctx).Return(mockLogger)
		mm.EXPECT().verifyCommandExpiryTime(&peerExpiryTime).Return(nil)
		mm.EXPECT().getOrCreateAccount(ctx, store, params.AccountName).Return(dbAccount, nil)
		mm.EXPECT().validateCreateVolumeParams(ctx, store, params, mock.AnythingOfType("*datamodel.PoolView")).Return(nil)
		mm.EXPECT().utilsGetLocationFromVendorID(vendorID).Return(location, nil)
		mm.EXPECT().utilsGetRequestIDFromContext(ctx).Return(requestURI)
		mm.EXPECT().utilsGetCorrelationIDFromContext(ctx).Return(correlationID)
		mm.EXPECT().workflowsExecuteWorkflowSequentially(
			temporal, ctx,
			mock.AnythingOfType("StartWorkflowOptions"),
			mock.AnythingOfType("func(internal.Context, *common.CreateVolumeParams, *datamodel.Volume, *flexcache.CreateFlexCacheEvent) error"),
			mock.AnythingOfType("ChildWorkflowOptions"),
			params, mock.AnythingOfType("*datamodel.Volume"), event).Return(assert.AnError)
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
		peerExpiryTime := time.Now().Add(1 * time.Hour)

		params := &common.CreateVolumeParams{
			AccountName:     "test_account",
			Region:          "test_region",
			Name:            "test_volume",
			VendorID:        "test_vendor",
			QuotaInBytes:    minQuotaInBytesPool,
			Protocols:       []string{"NFS"},
			Description:     "Some description",
			DisplayName:     "Some display name",
			PoolID:          "test-pool-uuid",
			CreationToken:   "test-creation-token",
			CacheParameters: &models.CacheParameters{PeerExpiryTime: &peerExpiryTime},
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

		event := &flexcache.CreateFlexCacheEvent{
			LocationID:    location,
			ProjectNumber: params.AccountName,
			RequestUri:    requestURI,
			CorrelationID: &correlationID,
		}

		mm.EXPECT().utilGetLogger(ctx).Return(mockLogger)
		mm.EXPECT().verifyCommandExpiryTime(&peerExpiryTime).Return(nil)
		mm.EXPECT().getOrCreateAccount(ctx, store, params.AccountName).Return(dbAccount, nil)
		mm.EXPECT().validateCreateVolumeParams(ctx, store, params, mock.AnythingOfType("*datamodel.PoolView")).Return(nil)
		mm.EXPECT().utilsGetLocationFromVendorID(vendorID).Return(location, nil)
		mm.EXPECT().utilsGetRequestIDFromContext(ctx).Return(requestURI)
		mm.EXPECT().utilsGetCorrelationIDFromContext(ctx).Return(correlationID)
		mm.EXPECT().workflowsExecuteWorkflowSequentially(
			temporal, ctx,
			mock.AnythingOfType("StartWorkflowOptions"),
			mock.AnythingOfType("func(internal.Context, *common.CreateVolumeParams, *datamodel.Volume, *flexcache.CreateFlexCacheEvent) error"),
			mock.AnythingOfType("ChildWorkflowOptions"),
			params, mock.AnythingOfType("*datamodel.Volume"), event).Return(nil)

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
		peerExpiryTime := time.Now().Add(1 * time.Hour)

		params := &common.CreateVolumeParams{
			AccountName:     "test_account",
			Region:          "test_region",
			Name:            "test_volume",
			VendorID:        "test_vendor",
			QuotaInBytes:    minQuotaInBytesPool,
			Protocols:       []string{"NFS"},
			Description:     "Some description",
			DisplayName:     "Some display name",
			PoolID:          "test-pool-uuid",
			CreationToken:   "test-creation-token",
			CacheParameters: &models.CacheParameters{PeerExpiryTime: &peerExpiryTime},
			FileProperties:  &models.FileProperties{
				// No ExportPolicy set
			},
		}

		dbAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-uuid",
			},
			Name: "test_account",
		}

		event := &flexcache.CreateFlexCacheEvent{
			LocationID:    location,
			ProjectNumber: params.AccountName,
			RequestUri:    requestURI,
			CorrelationID: &correlationID,
		}

		mm.EXPECT().utilGetLogger(ctx).Return(mockLogger)
		mm.EXPECT().verifyCommandExpiryTime(&peerExpiryTime).Return(nil)
		mm.EXPECT().getOrCreateAccount(ctx, store, params.AccountName).Return(dbAccount, nil)
		mm.EXPECT().validateCreateVolumeParams(ctx, store, params, mock.AnythingOfType("*datamodel.PoolView")).Return(nil)
		mm.EXPECT().utilsGetLocationFromVendorID(vendorID).Return(location, nil)
		mm.EXPECT().utilsGetRequestIDFromContext(ctx).Return(requestURI)
		mm.EXPECT().utilsGetCorrelationIDFromContext(ctx).Return(correlationID)
		mm.EXPECT().workflowsExecuteWorkflowSequentially(
			temporal, ctx,
			mock.AnythingOfType("StartWorkflowOptions"),
			mock.AnythingOfType("func(internal.Context, *common.CreateVolumeParams, *datamodel.Volume, *flexcache.CreateFlexCacheEvent) error"),
			mock.AnythingOfType("ChildWorkflowOptions"),
			params, mock.AnythingOfType("*datamodel.Volume"), event).Return(nil)

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

func TestCheckAndCancelCreateWorkflowIfNeeded(t *testing.T) {
	t.Run("Success", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		store := database.NewMockStorage(tt)
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			State:     models.LifeCycleStatePreparing,
		}

		job := &datamodel.Job{WorkflowID: "workflow-id"}

		store.EXPECT().GetJobByResourceUUID(ctx, volume.UUID, string(models.JobTypeFlexCacheCreateVolume)).Return(job, nil)
		temporal.EXPECT().CancelWorkflow(ctx, job.WorkflowID, "").Return(nil)
		store.EXPECT().CancelRunningJobsForResource(ctx, volume.UUID).Return(nil)

		err := _checkAndCancelCreateWorkflowIfNeeded(ctx, store, temporal, volume)
		assert.NoError(tt, err)
		temporal.AssertExpectations(tt)
	})

	t.Run("CancelWorkflowError", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		store := database.NewMockStorage(tt)
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			State:     models.LifeCycleStatePreparing,
		}

		job := &datamodel.Job{WorkflowID: "workflow-id"}

		store.EXPECT().GetJobByResourceUUID(ctx, volume.UUID, string(models.JobTypeFlexCacheCreateVolume)).Return(job, nil)
		temporal.EXPECT().CancelWorkflow(ctx, job.WorkflowID, "").Return(assert.AnError)

		err := _checkAndCancelCreateWorkflowIfNeeded(ctx, store, temporal, volume)
		assert.Equal(tt, assert.AnError, err)
		store.AssertNotCalled(tt, "CancelRunningJobsForResource", mock.Anything, mock.Anything)
		temporal.AssertExpectations(tt)
	})

	t.Run("VolumeNotInPreparingState", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		store := database.NewMockStorage(tt)
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			State:     models.LifeCycleStateAvailable,
		}

		err := _checkAndCancelCreateWorkflowIfNeeded(ctx, store, temporal, volume)
		assert.NoError(tt, err)
		store.AssertNotCalled(tt, "GetJobByResourceUUID", mock.Anything, mock.Anything, mock.Anything)
		temporal.AssertNotCalled(tt, "CancelWorkflow", mock.Anything, mock.Anything, mock.Anything)
		store.AssertNotCalled(tt, "CancelRunningJobsForResource", mock.Anything, mock.Anything)
	})

	t.Run("CreateJobNotFound", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		store := database.NewMockStorage(tt)
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			State:     models.LifeCycleStatePreparing,
		}

		objectID := "job-id"
		notFoundErr := vsaerrors.NewNotFoundErr("Job", &objectID)

		store.EXPECT().GetJobByResourceUUID(ctx, volume.UUID, string(models.JobTypeFlexCacheCreateVolume)).Return(nil, notFoundErr)

		err := _checkAndCancelCreateWorkflowIfNeeded(ctx, store, temporal, volume)
		assert.NoError(tt, err)
		temporal.AssertNotCalled(tt, "CancelWorkflow", mock.Anything, mock.Anything, mock.Anything)
		store.AssertNotCalled(tt, "CancelRunningJobsForResource", mock.Anything, mock.Anything)
	})

	t.Run("GetJobByResourceUUID_OtherError", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		store := database.NewMockStorage(tt)
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			State:     models.LifeCycleStatePreparing,
		}

		store.EXPECT().GetJobByResourceUUID(ctx, volume.UUID, string(models.JobTypeFlexCacheCreateVolume)).Return(nil, assert.AnError)

		err := _checkAndCancelCreateWorkflowIfNeeded(ctx, store, temporal, volume)
		assert.Equal(tt, assert.AnError, err)
		temporal.AssertNotCalled(tt, "CancelWorkflow", mock.Anything, mock.Anything, mock.Anything)
		store.AssertNotCalled(tt, "CancelRunningJobsForResource", mock.Anything, mock.Anything)
	})

	t.Run("CancelRunningJobsForResource_Error", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		store := database.NewMockStorage(tt)
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			State:     models.LifeCycleStatePreparing,
		}

		job := &datamodel.Job{WorkflowID: "workflow-id"}

		store.EXPECT().GetJobByResourceUUID(ctx, volume.UUID, string(models.JobTypeFlexCacheCreateVolume)).Return(job, nil)
		temporal.EXPECT().CancelWorkflow(ctx, job.WorkflowID, "").Return(nil)
		store.EXPECT().CancelRunningJobsForResource(ctx, volume.UUID).Return(assert.AnError)

		err := _checkAndCancelCreateWorkflowIfNeeded(ctx, store, temporal, volume)
		assert.Equal(tt, assert.AnError, err)
		temporal.AssertExpectations(tt)
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
	vendorID := "/projects/project123/locations/location123/pools/pool123"
	location := "location123"
	requestURI := "test-uri"
	correlationID := "test-correlation-id"
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
	expiryTime := time.Now().Add(time.Hour)
	params := &common.EstablishVolumePeeringParams{
		AccountName:     "account",
		Region:          "region",
		Zone:            "zone",
		Name:            "test_volume",
		PeerClusterName: "peer-cluster",
		PeerAddresses:   []string{"1.1.1.1", "2.2.2.2"},
		ExpiryTime:      &expiryTime,
		PeerSvmName:     "peer-svm",
		PeerVolumeName:  "peer-volume",
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
			PeerExpiryTime:  params.ExpiryTime,
		},
	}

	dbAccount := &datamodel.Account{
		BaseModel: datamodel.BaseModel{UUID: "test-uuid"},
		Name:      "test_account",
	}
	dbPool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
		Name:      "test_pool",
		VendorID:  vendorID,
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
		mockStorage.On("GetVolumeByName", ctx, params.Name).Return(nil, assert.AnError)
		mm.EXPECT().utilGetLogger(ctx).Return(mockLogger)

		vol, _, err := _establishFlexCacheVolumePeering(ctx, mockStorage, temporal, params)
		assert.Error(tt, err)
		assert.Nil(tt, vol)
	})

	t.Run("GetOrCreateAccount_Error", func(tt *testing.T) {
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		mm := newMonkeyMockAndPatch(t)
		mockLogger := log.NewMockLogger(t)
		mockStorage := new(database.MockStorage)
		mockStorage.On("GetVolumeByName", ctx, params.Name).Return(dbVolume, nil)
		mm.EXPECT().utilGetLogger(ctx).Return(mockLogger)
		mm.EXPECT().getOrCreateAccount(ctx, mockStorage, params.AccountName).Return(nil, assert.AnError)

		vol, _, err := _establishFlexCacheVolumePeering(ctx, mockStorage, temporal, params)
		assert.Error(tt, err)
		assert.Nil(tt, vol)
	})

	t.Run("Is_Establish_Peering_Not_Needed", func(tt *testing.T) {
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		mm := newMonkeyMockAndPatch(t)
		mockLogger := log.NewMockLogger(t)
		mockStorage := new(database.MockStorage)
		mockStorage.On("GetVolumeByName", ctx, params.Name).Return(dbVolume, nil)
		mm.EXPECT().utilGetLogger(ctx).Return(mockLogger)
		mm.EXPECT().getOrCreateAccount(ctx, mockStorage, params.AccountName).Return(dbAccount, nil)
		mm.EXPECT().isEstablishVolumePeeringNeeded(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("", assert.AnError)
		mockLogger.EXPECT().Errorf("Establish volume peering pre-checks failed: %v", assert.AnError)

		vol, _, err := _establishFlexCacheVolumePeering(ctx, mockStorage, temporal, params)
		assert.Error(tt, err)
		assert.Nil(tt, vol)
	})

	t.Run("utilsGetLocationFromVendorID_error", func(tt *testing.T) {
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		mm := newMonkeyMockAndPatch(t)
		mockLogger := log.NewMockLogger(t)
		mockStorage := new(database.MockStorage)

		mockStorage.On("GetVolumeByName", ctx, params.Name).Return(dbVolume, nil)
		mm.EXPECT().utilGetLogger(ctx).Return(mockLogger)
		mm.EXPECT().getOrCreateAccount(ctx, mockStorage, params.AccountName).Return(dbAccount, nil)
		mm.EXPECT().isEstablishVolumePeeringNeeded(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("", nil)
		mm.EXPECT().utilsGetLocationFromVendorID(mock.Anything).Return("location", assert.AnError)
		mockLogger.EXPECT().Errorf("Failed to get location from vendor ID for pool %s, error: %v", "test_pool", assert.AnError)

		vol, _, err := _establishFlexCacheVolumePeering(ctx, mockStorage, temporal, params)
		assert.Error(tt, err)
		assert.Nil(tt, vol)
	})

	t.Run("verifyCommandExpiryTime_Error", func(tt *testing.T) {
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		mm := newMonkeyMockAndPatch(t)
		mockLogger := log.NewMockLogger(t)
		mockStorage := new(database.MockStorage)
		mockStorage.On("GetVolumeByName", ctx, params.Name).Return(dbVolume, nil)
		mm.EXPECT().utilGetLogger(ctx).Return(mockLogger)
		mm.EXPECT().getOrCreateAccount(ctx, mockStorage, params.AccountName).Return(dbAccount, nil)
		mm.EXPECT().utilsGetLocationFromVendorID(vendorID).Return(location, nil)
		mm.EXPECT().isEstablishVolumePeeringNeeded(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("", nil)
		mm.EXPECT().utilsGetRequestIDFromContext(ctx).Return(requestURI).Maybe()
		mm.EXPECT().utilsGetCorrelationIDFromContext(ctx).Return(correlationID).Maybe()
		mm.EXPECT().verifyCommandExpiryTime(params.ExpiryTime).Return(assert.AnError)

		vol, _, err := _establishFlexCacheVolumePeering(ctx, mockStorage, temporal, params)
		assert.Error(tt, err)
		assert.Nil(tt, vol)
	})

	t.Run("CreateJob_Error", func(tt *testing.T) {
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		mm := newMonkeyMockAndPatch(t)
		mockLogger := log.NewMockLogger(t)
		mockStorage := new(database.MockStorage)
		mockStorage.On("GetVolumeByName", ctx, params.Name).Return(dbVolume, nil)
		mm.EXPECT().utilGetLogger(ctx).Return(mockLogger)
		mm.EXPECT().verifyCommandExpiryTime(params.ExpiryTime).Return(nil)
		mm.EXPECT().getOrCreateAccount(ctx, mockStorage, params.AccountName).Return(dbAccount, nil)
		mm.EXPECT().utilsGetLocationFromVendorID(vendorID).Return(location, nil)
		mm.EXPECT().isEstablishVolumePeeringNeeded(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("", nil)
		mm.EXPECT().utilsGetRequestIDFromContext(ctx).Return(requestURI)
		mm.EXPECT().utilsGetCorrelationIDFromContext(ctx).Return(correlationID)
		mockStorage.On("CreateJob", ctx, mock.AnythingOfType("*datamodel.Job")).Return(nil, assert.AnError)
		mockLogger.EXPECT().Errorf("Failed to create job in database, error: %v", assert.AnError)

		vol, _, err := _establishFlexCacheVolumePeering(ctx, mockStorage, temporal, params)
		assert.Error(tt, err)
		assert.Nil(tt, vol)
	})

	t.Run("WorkflowsExecuteWorkflowSequentially_Error", func(tt *testing.T) {
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		mm := newMonkeyMockAndPatch(t)
		mockLogger := log.NewMockLogger(t)
		mockStorage := new(database.MockStorage)
		mockStorage.On("GetVolumeByName", ctx, params.Name).Return(dbVolume, nil)
		mockStorage.On("CreateJob", ctx, mock.AnythingOfType("*datamodel.Job")).Return(&datamodel.Job{WorkflowID: "wf-id"}, nil)

		event := &flexcache.CreateFlexCacheEvent{
			LocationID:    location,
			ProjectNumber: params.AccountName,
			RequestUri:    requestURI,
			CorrelationID: &correlationID,
		}

		mm.EXPECT().utilGetLogger(ctx).Return(mockLogger)
		mm.EXPECT().verifyCommandExpiryTime(params.ExpiryTime).Return(nil)
		mm.EXPECT().getOrCreateAccount(ctx, mockStorage, params.AccountName).Return(dbAccount, nil)
		mm.EXPECT().isEstablishVolumePeeringNeeded(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("", nil)
		mm.EXPECT().utilsGetLocationFromVendorID(vendorID).Return(location, nil)
		mm.EXPECT().utilsGetRequestIDFromContext(ctx).Return(requestURI)
		mm.EXPECT().utilsGetCorrelationIDFromContext(ctx).Return(correlationID)
		mm.EXPECT().workflowsExecuteWorkflowSequentially(
			temporal, ctx,
			mock.AnythingOfType("StartWorkflowOptions"),
			mock.AnythingOfType("func(internal.Context, *common.CreateVolumeParams, *datamodel.Volume, *flexcache.CreateFlexCacheEvent) error"),
			mock.AnythingOfType("ChildWorkflowOptions"),
			volumeParams, mock.AnythingOfType("*datamodel.Volume"), event).Return(assert.AnError)
		mockLogger.EXPECT().Errorf("Failed to start establish volume peering workflow, error: %v", assert.AnError)

		vol, _, err := _establishFlexCacheVolumePeering(ctx, mockStorage, temporal, params)
		assert.Error(tt, err)
		assert.Nil(tt, vol)
	})

	t.Run("Success", func(tt *testing.T) {
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		mm := newMonkeyMockAndPatch(t)
		mockLogger := log.NewMockLogger(t)
		mockStorage := new(database.MockStorage)
		mockStorage.On("GetVolumeByName", ctx, params.Name).Return(dbVolume, nil)
		mockStorage.On("CreateJob", ctx, mock.AnythingOfType("*datamodel.Job")).Return(&datamodel.Job{WorkflowID: "wf-id"}, nil)

		event := &flexcache.CreateFlexCacheEvent{
			LocationID:    location,
			ProjectNumber: params.AccountName,
			RequestUri:    requestURI,
			CorrelationID: &correlationID,
		}

		mm.EXPECT().utilGetLogger(ctx).Return(mockLogger)
		mm.EXPECT().verifyCommandExpiryTime(params.ExpiryTime).Return(nil)
		mm.EXPECT().getOrCreateAccount(ctx, mockStorage, params.AccountName).Return(dbAccount, nil)
		mm.EXPECT().utilsGetLocationFromVendorID(vendorID).Return(location, nil)
		mm.EXPECT().isEstablishVolumePeeringNeeded(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("", nil)
		mm.EXPECT().utilsGetRequestIDFromContext(ctx).Return(requestURI)
		mm.EXPECT().utilsGetCorrelationIDFromContext(ctx).Return(correlationID)
		mm.EXPECT().workflowsExecuteWorkflowSequentially(
			temporal, ctx,
			mock.AnythingOfType("StartWorkflowOptions"),
			mock.AnythingOfType("func(internal.Context, *common.CreateVolumeParams, *datamodel.Volume, *flexcache.CreateFlexCacheEvent) error"),
			mock.AnythingOfType("ChildWorkflowOptions"),
			volumeParams, mock.AnythingOfType("*datamodel.Volume"), event).Return(nil)
		origConvert := convertDatastoreVolumeToModel
		convertDatastoreVolumeToModel = func(_ *datamodel.Volume, _ *[]string) *models.Volume {
			return &models.Volume{BaseModel: models.BaseModel{UUID: "vol-uuid"}}
		}
		defer func() { convertDatastoreVolumeToModel = origConvert }()
		vol, _, err := _establishFlexCacheVolumePeering(ctx, mockStorage, temporal, params)
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
			ExpiryTime:      &expiry,
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

func Test_IsEstablishVolumePeeringNeeded(t *testing.T) {
	expiry := time.Now().Add(time.Hour).UTC()
	params := &common.EstablishVolumePeeringParams{
		AccountName:     "account",
		Region:          "region",
		Zone:            "zone",
		Name:            "test_volume",
		PeerClusterName: "peer-cluster",
		PeerAddresses:   []string{"1.1.1.1", "2.2.2.2"},
		ExpiryTime:      &expiry,
		PeerSvmName:     "peer-svm",
		PeerVolumeName:  "peer-volume",
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

	t.Run("verifyVolumeState_error", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mm := newMonkeyMockAndPatch(tt)
		mockStorage := new(database.MockStorage)
		mockLogger := log.NewMockLogger(tt)

		mm.EXPECT().utilGetLogger(ctx).Return(mockLogger)
		mm.EXPECT().verifyVolumeState(ctx, dbVolume).Return(assert.AnError)

		_, err := _isEstablishVolumePeeringNeeded(ctx, mockStorage, params, dbVolume)
		assert.Error(tt, err)
	})

	t.Run("verifyFlexCacheParameters_failed", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mm := newMonkeyMockAndPatch(tt)
		mockStorage := new(database.MockStorage)
		mockLogger := log.NewMockLogger(tt)

		mm.EXPECT().utilGetLogger(ctx).Return(mockLogger)
		mm.EXPECT().verifyVolumeState(ctx, dbVolume).Return(nil)
		mm.EXPECT().verifyFlexCacheParameters(ctx, params, dbVolume).Return(assert.AnError)

		_, err := _isEstablishVolumePeeringNeeded(ctx, mockStorage, params, dbVolume)
		assert.Error(tt, err)
	})

	t.Run("verifyClusterPeering_error", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mm := newMonkeyMockAndPatch(tt)
		mockStorage := new(database.MockStorage)
		mockLogger := log.NewMockLogger(tt)

		mm.EXPECT().utilGetLogger(ctx).Return(mockLogger)
		mm.EXPECT().verifyVolumeState(ctx, dbVolume).Return(nil)
		mm.EXPECT().verifyFlexCacheParameters(ctx, params, dbVolume).Return(nil)
		mm.EXPECT().verifyClusterPeering(ctx, dbVolume).Return(true)

		_, err := _isEstablishVolumePeeringNeeded(ctx, mockStorage, params, dbVolume)
		assert.Error(tt, err)
	})

	t.Run("checkForFlexCacheJobInProgress_error", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mm := newMonkeyMockAndPatch(tt)
		mockStorage := new(database.MockStorage)
		mockLogger := log.NewMockLogger(tt)

		mm.EXPECT().utilGetLogger(ctx).Return(mockLogger)
		mm.EXPECT().verifyVolumeState(ctx, dbVolume).Return(nil)
		mm.EXPECT().verifyFlexCacheParameters(ctx, params, dbVolume).Return(nil)
		mm.EXPECT().verifyClusterPeering(ctx, dbVolume).Return(false)
		mm.EXPECT().checkForFlexCacheJobInProgress(ctx, mockStorage, dbVolume, params).Return(false, "", assert.AnError)

		_, err := _isEstablishVolumePeeringNeeded(ctx, mockStorage, params, dbVolume)
		assert.Error(tt, err)
	})

	t.Run("checkForFlexCacheJobInProgress_job_in_progress", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mm := newMonkeyMockAndPatch(tt)
		mockStorage := new(database.MockStorage)
		mockLogger := log.NewMockLogger(tt)

		mm.EXPECT().utilGetLogger(ctx).Return(mockLogger)
		mm.EXPECT().verifyVolumeState(ctx, dbVolume).Return(nil)
		mm.EXPECT().verifyFlexCacheParameters(ctx, params, dbVolume).Return(nil)
		mm.EXPECT().verifyClusterPeering(ctx, dbVolume).Return(false)
		mm.EXPECT().checkForFlexCacheJobInProgress(ctx, mockStorage, dbVolume, params).Return(true, "jobUUID", nil)
		mockLogger.EXPECT().Infof("found an existing FlexCache job in progress for volume %s", "test_volume")

		jobUUID, err := _isEstablishVolumePeeringNeeded(ctx, mockStorage, params, dbVolume)
		assert.Nil(tt, err)
		assert.Equal(tt, "jobUUID", jobUUID)
	})

	t.Run("Establish_Volume_Peering_Needed", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mm := newMonkeyMockAndPatch(tt)
		mockStorage := new(database.MockStorage)
		mockLogger := log.NewMockLogger(tt)

		mm.EXPECT().utilGetLogger(ctx).Return(mockLogger)
		mm.EXPECT().verifyVolumeState(ctx, dbVolume).Return(nil)
		mm.EXPECT().verifyFlexCacheParameters(ctx, params, dbVolume).Return(nil)
		mm.EXPECT().verifyClusterPeering(ctx, dbVolume).Return(false)
		mm.EXPECT().checkForFlexCacheJobInProgress(ctx, mockStorage, dbVolume, params).Return(false, "", nil)

		_, err := _isEstablishVolumePeeringNeeded(ctx, mockStorage, params, dbVolume)
		assert.NoError(tt, err)
	})
}

func Test_VerifyFlexCacheParameters(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
	baseParams := &common.EstablishVolumePeeringParams{
		PeerClusterName: "peer-cluster",
		PeerSvmName:     "peer-svm",
		PeerVolumeName:  "peer-volume",
	}

	newVolume := func(cluster, svm, vol string) *datamodel.Volume {
		return &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "vol-uuid"},
			Name:      "test_volume",
			CacheParameters: &datamodel.CacheParameters{
				PeerClusterName: cluster,
				PeerSvmName:     svm,
				PeerVolumeName:  vol,
			},
		}
	}

	t.Run("Success", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		mockLogger := log.NewMockLogger(tt)
		mm.EXPECT().utilGetLogger(ctx).Return(mockLogger)
		mockLogger.EXPECT().Debugf(mock.Anything, mock.Anything).Return()

		vol := newVolume("peer-cluster", "peer-svm", "peer-volume")
		err := _verifyFlexCacheParameters(ctx, baseParams, vol)
		assert.NoError(tt, err)
	})

	t.Run("Mismatch_PeerClusterName", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		mockLogger := log.NewMockLogger(tt)
		mm.EXPECT().utilGetLogger(ctx).Return(mockLogger)
		mockLogger.EXPECT().Debugf(mock.Anything, mock.Anything).Return()

		vol := newVolume("different-cluster", "peer-svm", "peer-volume")
		err := _verifyFlexCacheParameters(ctx, baseParams, vol)
		assert.Error(tt, err)
	})

	t.Run("Mismatch_PeerSvmName", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		mockLogger := log.NewMockLogger(tt)
		mm.EXPECT().utilGetLogger(ctx).Return(mockLogger)
		mockLogger.EXPECT().Debugf(mock.Anything, mock.Anything).Return()

		vol := newVolume("peer-cluster", "other-svm", "peer-volume")
		err := _verifyFlexCacheParameters(ctx, baseParams, vol)
		assert.Error(tt, err)
	})

	t.Run("Mismatch_PeerVolumeName", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		mockLogger := log.NewMockLogger(tt)
		mm.EXPECT().utilGetLogger(ctx).Return(mockLogger)
		mockLogger.EXPECT().Debugf(mock.Anything, mock.Anything).Return()

		vol := newVolume("peer-cluster", "peer-svm", "different-volume")
		err := _verifyFlexCacheParameters(ctx, baseParams, vol)
		assert.Error(tt, err)
	})
}

func Test_VerifyClusterPeering(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

	newVolume := func(state string) *datamodel.Volume {
		return &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "vol-uuid"},
			Name:      "test_volume",
			CacheParameters: &datamodel.CacheParameters{
				CacheState: state,
			},
		}
	}

	setupLogger := func(tt *testing.T) *log.MockLogger {
		mockLogger := log.NewMockLogger(tt)
		mockLogger.EXPECT().Debugf(mock.Anything, mock.Anything).Maybe()
		mockLogger.EXPECT().Debugf(
			mock.Anything,
			mock.Anything, mock.Anything, mock.Anything, mock.Anything,
			mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		).Maybe()
		return mockLogger
	}

	t.Run("PeerState_PEERED", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		mockLogger := setupLogger(tt)
		mm.EXPECT().utilGetLogger(ctx).Return(mockLogger)

		vol := newVolume(string(cvpmodels.FlexCacheV1betaCacheStatePEERED))
		got := _verifyClusterPeering(ctx, vol)
		assert.Equal(tt, true, got)
	})

	t.Run("PeerState_EMPTY", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		mockLogger := setupLogger(tt)
		mm.EXPECT().utilGetLogger(ctx).Return(mockLogger)

		vol := newVolume("")
		got := _verifyClusterPeering(ctx, vol)
		assert.Equal(tt, false, got)
	})

	t.Run("PeerState_OTHER", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		mockLogger := setupLogger(tt)
		mm.EXPECT().utilGetLogger(ctx).Return(mockLogger)

		vol := newVolume("RANDOM_STATE")
		got := _verifyClusterPeering(ctx, vol)
		assert.Equal(tt, false, got)
	})
}

func Test_CheckForFlexCacheJobInProgress(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"k": "v"})

	baseParams := &common.EstablishVolumePeeringParams{
		PeerClusterName: "peer-cluster",
		PeerSvmName:     "peer-svm",
		PeerVolumeName:  "peer-vol",
		PeerAddresses:   []string{"10.0.0.1"},
	}

	dbVolume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "vol-uuid"},
		Name:      "vol-name",
		CacheParameters: &datamodel.CacheParameters{
			PeerClusterName: "peer-cluster",
			PeerSvmName:     "peer-svm",
			PeerVolumeName:  "peer-vol",
			PeerIpAddresses: []string{"10.0.0.1"},
		},
	}

	t.Run("ErrorOnGetJobs", func(tt *testing.T) {
		mockLogger := log.NewMockLogger(tt)
		mockLogger.EXPECT().Debugf(mock.Anything, mock.Anything).Maybe()
		store := database.NewMockStorage(tt)
		mm := newMonkeyMockAndPatch(tt)
		mm.EXPECT().utilGetLogger(ctx).Return(mockLogger)
		store.EXPECT().GetJobsWithCondition(ctx, mock.Anything).Return(nil, assert.AnError)

		inProgress, _, err := _checkForFlexCacheJobInProgress(ctx, store, dbVolume, baseParams)
		assert.False(tt, inProgress)
		assert.Error(tt, err)
		assert.Equal(tt, assert.AnError, err)
	})

	t.Run("JobsPresent", func(tt *testing.T) {
		mockLogger := log.NewMockLogger(tt)
		mockLogger.EXPECT().Debugf(mock.Anything, mock.Anything).Maybe()
		store := database.NewMockStorage(tt)
		mm := newMonkeyMockAndPatch(tt)
		mm.EXPECT().utilGetLogger(ctx).Return(mockLogger)
		store.EXPECT().GetJobsWithCondition(ctx, mock.Anything).Return([]*datamodel.Job{
			{Type: string(models.JobTypeFlexCacheEstablishPeering), State: string(models.JobsStatePROCESSING), BaseModel: datamodel.BaseModel{UUID: "job-uuid"}},
		}, nil)

		inProgress, jobUUID, err := _checkForFlexCacheJobInProgress(ctx, store, dbVolume, baseParams)
		assert.True(tt, inProgress)
		assert.NoError(tt, err)
		assert.Equal(tt, "job-uuid", jobUUID)
	})

	t.Run("NoJobs", func(tt *testing.T) {
		mockLogger := log.NewMockLogger(tt)
		mockLogger.EXPECT().Debugf(mock.Anything, mock.Anything).Maybe()
		store := database.NewMockStorage(tt)
		mm := newMonkeyMockAndPatch(tt)
		mm.EXPECT().utilGetLogger(ctx).Return(mockLogger)
		store.EXPECT().GetJobsWithCondition(ctx, mock.Anything).Return([]*datamodel.Job{}, nil)

		inProgress, _, err := _checkForFlexCacheJobInProgress(ctx, store, dbVolume, baseParams)
		assert.False(tt, inProgress)
		assert.NoError(tt, err)
	})
}

func Test_VerifyVolumeState_TwoCases(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"test": "value"})

	t.Run("Success_PREPARING", func(tt *testing.T) {
		vol := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "vol-uuid"},
			Name:      "vol",
			State:     models.LifeCycleStatePreparing,
			CacheParameters: &datamodel.CacheParameters{
				PeerClusterName: "peer",
			},
		}

		mm := newMonkeyMockAndPatch(tt)
		mockLogger := log.NewMockLogger(tt)
		mm.EXPECT().utilGetLogger(ctx).Return(mockLogger)
		mockLogger.EXPECT().Debugf(mock.Anything, mock.Anything, mock.Anything).Maybe()

		err := _verifyVolumeState(ctx, vol)
		assert.NoError(tt, err)
	})

	t.Run("Failure_READY", func(tt *testing.T) {
		vol := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "vol-uuid"},
			Name:      "vol",
			State:     models.LifeCycleStateREADY,
			CacheParameters: &datamodel.CacheParameters{
				PeerClusterName: "peer",
			},
		}

		mm := newMonkeyMockAndPatch(tt)
		mockLogger := log.NewMockLogger(tt)
		mm.EXPECT().utilGetLogger(ctx).Return(mockLogger)
		mockLogger.EXPECT().Debugf(mock.Anything, mock.Anything, mock.Anything).Maybe()

		err := _verifyVolumeState(ctx, vol)
		assert.Error(tt, err)
	})
}

func Test_verifyCommandExpiryTime(t *testing.T) {
	t.Run("NilExpiryTime", func(t *testing.T) {
		err := _verifyCommandExpiryTime(nil)
		assert.NoError(t, err)
	})

	t.Run("FutureExpiryTime", func(t *testing.T) {
		future := time.Now().Add(1 * time.Hour)
		err := _verifyCommandExpiryTime(&future)
		assert.NoError(t, err)
	})

	t.Run("PastExpiryTime", func(t *testing.T) {
		past := time.Now().Add(-1 * time.Hour)
		err := _verifyCommandExpiryTime(&past)
		assert.Error(t, err)
	})
}
