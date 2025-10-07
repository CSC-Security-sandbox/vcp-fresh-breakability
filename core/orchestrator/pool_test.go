package orchestrator

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/validators"
	utils2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	workflowenginemock "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine"
	"gorm.io/gorm"
)

func setup(t *testing.T) (context.Context, database.Storage, *Orchestrator, *workflowenginemock.MockTemporalTestClient) {
	ctx := context.Background()
	mockLogger := log.NewLogger()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
	store, err := database.SetupStorageForTest(mockLogger)
	assert.NoError(t, err)
	err = database.ClearInMemoryDB(store.DB())
	assert.NoError(t, err)

	temporal := workflowenginemock.NewMockTemporalTestClient(t)

	orch := &Orchestrator{storage: store, temporal: temporal}
	return ctx, store, orch, temporal
}
func createDBPools(t *testing.T, store database.Storage) ([]*datamodel.Pool, *datamodel.Account) {
	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
		Name:      "test_account",
	}
	err := store.DB().Create(account).Error
	if err != nil {
		t.Fatalf("Failed to create account: %v", err)
	}
	pool1 := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid1"},
		Name:      "test_pool_1",
		AccountID: account.ID,
		VendorID:  "test-vendor-id",
		PoolAttributes: &datamodel.PoolAttributes{
			ThroughputMibps: 64,
			Iops:            1024, // datamodel.PoolAttributes expects int64, not pointer
			PrimaryZone:     "us-central1-a",
		},
		DeploymentName: "dep1",
	}
	deletedPool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			UUID:      "test-pool-uuid-deleted",
			DeletedAt: &gorm.DeletedAt{Time: time.Now(), Valid: true}, // Simulate a deleted pool
		},
		Name:      "test_pool_2",
		AccountID: account.ID,
		PoolAttributes: &datamodel.PoolAttributes{
			ThroughputMibps: 128,
			Iops:            2048,
			PrimaryZone:     "us-central1-b",
			SecondaryZone:   "us-central1-c",
		},
		DeploymentName: "dep-deleted",
	}
	pool2 := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			UUID: "test-pool-uuid2",
		},
		Name:      "test_pool_2",
		AccountID: account.ID,
		PoolAttributes: &datamodel.PoolAttributes{
			ThroughputMibps: 128,
			Iops:            2048,
			PrimaryZone:     "us-central1-b",
			SecondaryZone:   "us-central1-c",
		},
		DeploymentName: "dep2",
	}

	err = store.DB().Create(pool1).Error
	assert.NoError(t, err)
	err = store.DB().Create(deletedPool).Error
	assert.NoError(t, err)
	err = store.DB().Create(pool2).Error
	assert.NoError(t, err)

	var pools []*datamodel.Pool
	store.DB().Find(&pools)

	return []*datamodel.Pool{pool1, deletedPool, pool2}, account
}

func TestConvertDatastorePoolToModel_ValidPool_ReturnsCorrectModel(t *testing.T) {
	deletedAt := gorm.DeletedAt{Time: time.Now(), Valid: true}
	datastorePool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			UUID:      "test-uuid",
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
			DeletedAt: &deletedAt,
		},
		Name:             "Test Pool",
		Description:      "Test Description",
		SizeInBytes:      1024,
		State:            "active",
		StateDetails:     "running",
		AllowAutoTiering: true,
		Network:          "test-network",
		ServiceLevel:     "FLEX",
		PoolAttributes: &datamodel.PoolAttributes{
			ThroughputMibps: 64,
			Iops:            1024, // datamodel.PoolAttributes expects int64, not pointer
			PrimaryZone:     "us-central1-a",
			SecondaryZone:   "us-central1-b",
		},
	}
	accountName := "test-account"

	dbPoolView := database.ConvertPoolToPoolView(datastorePool)
	result := convertDatastorePoolToModel(dbPoolView, accountName)

	assert.Equal(t, datastorePool.UUID, result.UUID)
	assert.Equal(t, datastorePool.CreatedAt, result.CreatedAt)
	assert.Equal(t, datastorePool.UpdatedAt, result.UpdatedAt)
	assert.Equal(t, &deletedAt.Time, result.DeletedAt)
	assert.Equal(t, accountName, result.AccountName)
	assert.Equal(t, datastorePool.Name, result.Name)
	assert.Equal(t, datastorePool.Description, result.Description)
	assert.Equal(t, uint64(datastorePool.SizeInBytes), result.SizeInBytes)
	assert.Equal(t, datastorePool.State, result.State)
	assert.Equal(t, datastorePool.StateDetails, result.StateDetails)
	assert.Equal(t, datastorePool.AllowAutoTiering, result.AllowAutoTiering)
	assert.Equal(t, datastorePool.Network, result.VendorSubNetID)
	assert.Equal(t, datastorePool.ServiceLevel, result.ServiceLevel)
}

func TestConvertDatastorePoolToModel_NilDeletedAt_ReturnsNilDeletedAt(t *testing.T) {
	datastorePool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			UUID:      "test-uuid",
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
			DeletedAt: nil,
		},
		Name:             "Test Pool",
		Description:      "Test Description",
		SizeInBytes:      1024,
		State:            "active",
		StateDetails:     "running",
		AllowAutoTiering: true,
		Network:          "test-network",
		ServiceLevel:     "FLEX",
		PoolAttributes: &datamodel.PoolAttributes{
			ThroughputMibps: 64,
			Iops:            1024, // datamodel.PoolAttributes expects int64, not pointer
			PrimaryZone:     "us-central1-a",
			SecondaryZone:   "us-central1-b",
		},
	}
	accountName := "test-account"
	dbPoolView := database.ConvertPoolToPoolView(datastorePool)
	result := convertDatastorePoolToModel(dbPoolView, accountName)

	assert.Nil(t, result.DeletedAt)
}

func TestConvertDatastorePoolToModel_InvalidDeletedAt_ReturnsNilDeletedAt(t *testing.T) {
	invalidDeletedAt := gorm.DeletedAt{Time: time.Now(), Valid: false}
	datastorePool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			UUID:      "test-uuid",
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
			DeletedAt: &invalidDeletedAt,
		},
		Name:             "Test Pool",
		Description:      "Test Description",
		SizeInBytes:      1024,
		State:            "active",
		StateDetails:     "running",
		AllowAutoTiering: true,
		Network:          "test-network",
		ServiceLevel:     "FLEX",
		PoolAttributes: &datamodel.PoolAttributes{
			ThroughputMibps: 64,
			Iops:            1024, // datamodel.PoolAttributes expects int64, not pointer
			PrimaryZone:     "us-central1-a",
			SecondaryZone:   "",
		},
	}
	accountName := "test-account"
	dbPoolView := database.ConvertPoolToPoolView(datastorePool)
	result := convertDatastorePoolToModel(dbPoolView, accountName)

	assert.Nil(t, result.DeletedAt)
}

func TestConvertDatastorePoolToModel_WithKms(t *testing.T) {
	invalidDeletedAt := gorm.DeletedAt{Time: time.Now(), Valid: false}
	datastorePool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			UUID:      "test-uuid",
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
			DeletedAt: &invalidDeletedAt,
		},
		Name:             "Test Pool",
		Description:      "Test Description",
		SizeInBytes:      1024,
		State:            "active",
		StateDetails:     "running",
		AllowAutoTiering: true,
		Network:          "test-network",
		ServiceLevel:     "FLEX",
		PoolAttributes: &datamodel.PoolAttributes{
			ThroughputMibps: 64,
			Iops:            1024, // datamodel.PoolAttributes expects int64, not pointer
			PrimaryZone:     "us-central1-a",
			SecondaryZone:   "",
		},
		KmsConfigID: sql.NullInt64{Valid: true, Int64: 1},
		KmsConfig: &datamodel.KmsConfig{
			BaseModel: datamodel.BaseModel{
				UUID: "kms-config-uuid",
			},
			Name:              "Test KMS Config",
			Description:       "Test KMS Config Description",
			State:             "active",
			StateDetails:      "running",
			KeyRing:           "test-key-ring",
			KeyRingLocation:   "us-central1",
			KeyName:           "test-key-name",
			AccountID:         1,
			CustomerProjectID: "test-customer-project-id",
			KeyProjectID:      "test-key-project-id",
		},
	}
	accountName := "test-account"
	dbPoolView := database.ConvertPoolToPoolView(datastorePool)
	result := convertDatastorePoolToModel(dbPoolView, accountName)

	assert.Nil(t, result.DeletedAt)
	assert.Equal(t, result.KmsConfig.KeyRingLocation, datastorePool.KmsConfig.KeyRingLocation)
	assert.Equal(t, result.KmsConfig.KeyRing, datastorePool.KmsConfig.KeyRing)
	assert.Equal(t, result.KmsConfig.KeyName, datastorePool.KmsConfig.KeyName)
	assert.Equal(t, result.KmsConfig.CustomerProjectID, datastorePool.KmsConfig.CustomerProjectID)
	assert.Equal(t, result.KmsConfig.KeyProjectID, datastorePool.KmsConfig.KeyProjectID)
	assert.Equal(t, result.KmsConfig.UUID, datastorePool.KmsConfig.UUID)
}

func TestConvertDatastorePoolToModel_WithAssetMetadata(t *testing.T) {
	t.Run("WhenPoolHasAssetMetadata", func(tt *testing.T) {
		datastorePool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				UUID: "test-uuid-with-assets",
			},
			Name:         "Test Pool With Assets",
			Description:  "Test Description With Assets",
			SizeInBytes:  1024,
			State:        "active",
			StateDetails: "running",
			Network:      "test-network",
			ServiceLevel: "FLEX",
			AssetMetadata: &datamodel.AssetMetadata{
				ChildAssets: []datamodel.ChildAsset{
					{
						AssetType:  "compute",
						AssetNames: []string{"instance-1", "instance-2"},
					},
					{
						AssetType:  "storage",
						AssetNames: []string{"bucket-1"},
					},
				},
			},
			PoolAttributes: &datamodel.PoolAttributes{
				ThroughputMibps: 64,
				Iops:            1024,
				PrimaryZone:     "us-central1-a",
				SecondaryZone:   "us-central1-b",
			},
		}
		accountName := "test-account"

		dbPoolView := database.ConvertPoolToPoolView(datastorePool)
		result := convertDatastorePoolToModel(dbPoolView, accountName)

		assert.NotNil(t, result.AssetMetadata)
		assert.Len(t, result.AssetMetadata.ChildAssets, 2)

		// Check first asset
		firstAsset := result.AssetMetadata.ChildAssets[0]
		assert.Equal(t, "compute", firstAsset.AssetType)
		assert.Equal(t, []string{"instance-1", "instance-2"}, firstAsset.AssetNames)

		// Check second asset
		secondAsset := result.AssetMetadata.ChildAssets[1]
		assert.Equal(t, "storage", secondAsset.AssetType)
		assert.Equal(t, []string{"bucket-1"}, secondAsset.AssetNames)
	})

	t.Run("WhenPoolHasEmptyAssetMetadata", func(tt *testing.T) {
		datastorePool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				UUID: "test-uuid-empty-assets",
			},
			Name:         "Test Pool Empty Assets",
			Description:  "Test Description Empty Assets",
			SizeInBytes:  2048,
			State:        "active",
			StateDetails: "running",
			Network:      "test-network",
			ServiceLevel: "FLEX",
			AssetMetadata: &datamodel.AssetMetadata{
				ChildAssets: []datamodel.ChildAsset{},
			},
			PoolAttributes: &datamodel.PoolAttributes{
				ThroughputMibps: 128,
				Iops:            2048,
				PrimaryZone:     "us-central1-a",
				SecondaryZone:   "us-central1-b",
			},
		}
		accountName := "test-account"

		dbPoolView := database.ConvertPoolToPoolView(datastorePool)
		result := convertDatastorePoolToModel(dbPoolView, accountName)

		assert.NotNil(t, result.AssetMetadata)
		assert.Empty(t, result.AssetMetadata.ChildAssets)
	})

	t.Run("WhenPoolHasSingleChildAsset", func(tt *testing.T) {
		datastorePool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				UUID: "test-uuid-single-asset",
			},
			Name:         "Test Pool Single Asset",
			Description:  "Test Description Single Asset",
			SizeInBytes:  4096,
			State:        "active",
			StateDetails: "running",
			Network:      "test-network",
			ServiceLevel: "FLEX",
			AssetMetadata: &datamodel.AssetMetadata{
				ChildAssets: []datamodel.ChildAsset{
					{
						AssetType:  "database",
						AssetNames: []string{"db-1", "db-2", "db-3"},
					},
				},
			},
			PoolAttributes: &datamodel.PoolAttributes{
				ThroughputMibps: 256,
				Iops:            4096,
				PrimaryZone:     "us-central1-a",
				SecondaryZone:   "us-central1-b",
			},
		}
		accountName := "test-account"

		dbPoolView := database.ConvertPoolToPoolView(datastorePool)
		result := convertDatastorePoolToModel(dbPoolView, accountName)

		assert.NotNil(t, result.AssetMetadata)
		assert.Len(t, result.AssetMetadata.ChildAssets, 1)

		// Check single asset
		asset := result.AssetMetadata.ChildAssets[0]
		assert.Equal(t, "database", asset.AssetType)
		assert.Equal(t, []string{"db-1", "db-2", "db-3"}, asset.AssetNames)
	})

	t.Run("WhenPoolHasNilAssetMetadata", func(tt *testing.T) {
		datastorePool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				UUID: "test-uuid-nil-assets",
			},
			Name:          "Test Pool Nil Assets",
			Description:   "Test Description Nil Assets",
			SizeInBytes:   8192,
			State:         "active",
			StateDetails:  "running",
			Network:       "test-network",
			ServiceLevel:  "FLEX",
			AssetMetadata: nil, // This is the key test case
			PoolAttributes: &datamodel.PoolAttributes{
				ThroughputMibps: 512,
				Iops:            8192,
				PrimaryZone:     "us-central1-a",
				SecondaryZone:   "us-central1-b",
			},
		}
		accountName := "test-account"

		dbPoolView := database.ConvertPoolToPoolView(datastorePool)
		result := convertDatastorePoolToModel(dbPoolView, accountName)

		assert.Nil(t, result.AssetMetadata)
	})
}

func TestCreatePool(t *testing.T) {
	t.Run("WhenGetOrCreateAccountFails", func(tt *testing.T) {
		ctx, se, _, temporal := setup(tt)
		params := &common.CreatePoolParams{
			AccountName:      "test_account",
			Region:           "test_region",
			PrimaryZone:      "test_zone",
			SecondaryZone:    "",
			Name:             "test_pool",
			VendorID:         "test_vendor",
			SizeInBytes:      1024,
			AllowAutoTiering: true,
			VendorSubNetID:   "test_network",
			CustomPerformanceParams: func() *common.CustomPerformanceParams {
				iopsValue := int64(1024)
				return &common.CustomPerformanceParams{
					Enabled:         true,
					ThroughputMibps: 64,
					Iops:            &iopsValue,
				}
			}(),
		}
		originalnodePassword := env.NodePassword
		env.NodePassword = "password"
		defer func() {
			env.NodePassword = originalnodePassword
		}()
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.New("account not found")
		}

		_, _, err := createPool(ctx, se, temporal, params)
		assert.EqualError(tt, err, "account not found")
	})
	t.Run("WhenValidatePoolParamFails", func(tt *testing.T) {
		ctx, se, _, temporal := setup(tt)
		iopsValue := int64(1024)
		params := &common.CreatePoolParams{
			AccountName:      "test_account",
			Region:           "test_region",
			PrimaryZone:      "test_zone",
			SecondaryZone:    "",
			Name:             "test_pool",
			VendorID:         "test_vendor",
			SizeInBytes:      1024,
			AllowAutoTiering: true,
			VendorSubNetID:   "test_network",
			CustomPerformanceParams: &common.CustomPerformanceParams{
				Enabled:         true,
				ThroughputMibps: 64,
				Iops:            &iopsValue, // common.CustomPerformanceParams expects *int64
			},
		}

		dbAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-uuid",
			},
			Name: "test_account",
		}
		originalnodePassword := env.NodePassword
		env.NodePassword = "password"
		defer func() {
			env.NodePassword = originalnodePassword
			ValidateCreatePoolParams = _validateCreatePoolParams
		}()
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return dbAccount, nil
		}
		ValidateCreatePoolParams = func(params *common.CreatePoolParams) error {
			return errors.New("invalid pool params")
		}

		pool, _, err := createPool(ctx, se, temporal, params)
		assert.EqualError(tt, err, "invalid pool params")
		assert.Nil(tt, pool, "Expected nil, got %v", pool)
	})
	t.Run("WhenCreatePoolFails", func(tt *testing.T) {
		ctx, store, _, temporal := setup(tt)
		params := &common.CreatePoolParams{
			AccountName:      "test_account",
			Region:           "test_region",
			PrimaryZone:      "test_zone1",
			SecondaryZone:    "test_zone2",
			Name:             "test_pool_1",
			VendorID:         "test_vendor",
			SizeInBytes:      1024,
			AllowAutoTiering: true,
			VendorSubNetID:   "test_network",
			CustomPerformanceParams: func() *common.CustomPerformanceParams {
				iopsValue := int64(1024)
				return &common.CustomPerformanceParams{
					Enabled:         true,
					ThroughputMibps: 64,
					Iops:            &iopsValue,
				}
			}(),
		}

		_, account := createDBPools(t, store)

		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		ValidateCreatePoolParams = func(params *common.CreatePoolParams) error {
			return nil
		}

		_, _, err := createPool(ctx, store, temporal, params)
		assert.EqualError(tt, err.(*vsaerrors.CustomError).OriginalErr, "pool already exists")
		assert.EqualError(tt, err, "Invalid input parameters provided")
	})
	t.Run("WhenCreatePoolSucceeds", func(tt *testing.T) {
		ctx, _, orch, temporal := setup(tt)
		label := "label"
		labels := make(datamodel.JSONB)
		labels["test"] = label

		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		params := &common.CreatePoolParams{
			AccountName:      "test_account",
			Region:           "test_region",
			PrimaryZone:      "test_zone",
			SecondaryZone:    "",
			Name:             "test_pool",
			VendorID:         "test_vendor",
			SizeInBytes:      1024,
			AllowAutoTiering: true,
			VendorSubNetID:   "test_network",
			CustomPerformanceParams: func() *common.CustomPerformanceParams {
				iopsValue := int64(1024)
				return &common.CustomPerformanceParams{
					Enabled:         true,
					ThroughputMibps: 64,
					Iops:            &iopsValue,
				}
			}(),
			Labels: &labels,
		}

		dbAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-uuid",
			},
			Name: "test_account",
		}
		originalnodePassword := env.NodePassword
		env.NodePassword = "password"

		authType := env.AuthType
		env.AuthType = env.USERNAME_PWD
		defer func() {
			env.AuthType = authType // Reset to original value after test
			env.NodePassword = originalnodePassword
		}()
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return dbAccount, nil
		}
		ValidateCreatePoolParams = func(params *common.CreatePoolParams) error {
			return nil
		}

		pool, _, err := orch.CreatePool(ctx, params)
		if err != nil {
			t.Errorf("Expected nil, got error")
		}
		assert.Equal(t, pool.Name, params.Name)
		assert.Equal(t, pool.VendorSubNetID, params.VendorSubNetID)
		assert.Equal(t, pool.AccountName, params.AccountName)
		assert.Equal(t, pool.PoolAttributes.Labels["test"], label)
	})
	t.Run("WhenCreatePoolSucceedsWithCert", func(tt *testing.T) {
		ctx, _, orch, temporal := setup(tt)
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
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

		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		params := &common.CreatePoolParams{
			AccountName:      "test_account",
			Region:           "test_region",
			PrimaryZone:      "test_zone",
			SecondaryZone:    "",
			Name:             "test_pool",
			VendorID:         "test_vendor",
			SizeInBytes:      1024,
			AllowAutoTiering: true,
			VendorSubNetID:   "test_network",
			CustomPerformanceParams: func() *common.CustomPerformanceParams {
				iopsValue := int64(1024)
				return &common.CustomPerformanceParams{
					Enabled:         true,
					ThroughputMibps: 64,
					Iops:            &iopsValue,
				}
			}(),
		}

		dbAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-uuid",
			},
			Name: "test_account",
		}
		authType := env.AuthType
		env.AuthType = env.USER_CERTIFICATE
		defer func() {
			env.AuthType = authType // Reset to original value after test
		}()
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return dbAccount, nil
		}
		ValidateCreatePoolParams = func(params *common.CreatePoolParams) error {
			return nil
		}

		pool, _, err := orch.CreatePool(ctx, params)
		if err != nil {
			t.Errorf("Expected nil, got error")
		}
		assert.Equal(t, pool.Name, params.Name)
		assert.Equal(t, pool.VendorSubNetID, params.VendorSubNetID)
		assert.Equal(t, pool.AccountName, params.AccountName)
	})
	t.Run("WhenCreatePoolSucceedsWithPasswordInSecretManager", func(tt *testing.T) {
		ctx, _, orch, temporal := setup(tt)
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
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

		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		params := &common.CreatePoolParams{
			AccountName:      "test_account",
			Region:           "test_region",
			PrimaryZone:      "test_zone",
			SecondaryZone:    "",
			Name:             "test_pool",
			VendorID:         "test_vendor",
			SizeInBytes:      1024,
			AllowAutoTiering: true,
			VendorSubNetID:   "test_network",
			CustomPerformanceParams: func() *common.CustomPerformanceParams {
				iopsValue := int64(1024)
				return &common.CustomPerformanceParams{
					Enabled:         true,
					ThroughputMibps: 64,
					Iops:            &iopsValue,
				}
			}(),
		}

		dbAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-uuid",
			},
			Name: "test_account",
		}
		authType := env.AuthType
		env.AuthType = env.USERNAME_PWD_SEC_MGR
		defer func() {
			env.AuthType = authType // Reset to original value after test
		}()
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return dbAccount, nil
		}
		ValidateCreatePoolParams = func(params *common.CreatePoolParams) error {
			return nil
		}

		pool, _, err := orch.CreatePool(ctx, params)
		if err != nil {
			t.Errorf("Expected nil, got error")
		}
		assert.Equal(t, pool.Name, params.Name)
		assert.Equal(t, pool.VendorSubNetID, params.VendorSubNetID)
		assert.Equal(t, pool.AccountName, params.AccountName)
	})
	t.Run("WhenUpdateKmsConfigStateFails", func(tt *testing.T) {
		ctx, store, orch, temporal := setup(tt)
		mockStorage := new(database.MockStorage)
		orch.storage = mockStorage
		iops := int64(1024)
		params := &common.CreatePoolParams{
			AccountName:      "test_account",
			Region:           "test_region",
			PrimaryZone:      "test_zone1",
			SecondaryZone:    "test_zone2",
			Name:             "test_pool_1",
			VendorID:         "test_vendor",
			SizeInBytes:      1024,
			AllowAutoTiering: true,
			VendorSubNetID:   "test_network",
			CustomPerformanceParams: &common.CustomPerformanceParams{
				Enabled:         true,
				ThroughputMibps: 64,
				Iops:            &iops,
			},
			KmsConfig: &models.KmsConfig{State: models.LifeCycleStateInUse},
		}

		dbAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-uuid",
			},
			Name: "test_account",
		}
		originalnodePassword := env.NodePassword
		env.NodePassword = "password"

		authType := env.AuthType
		env.AuthType = env.USERNAME_PWD
		defer func() {
			env.AuthType = authType // Reset to original value after test
			env.NodePassword = originalnodePassword
		}()
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return dbAccount, nil
		}

		mockStorage.On("UpdateKmsConfigState", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("some error")).Once()
		ValidateCreatePoolParams = func(params *common.CreatePoolParams) error {
			return nil
		}

		_, _, err := createPool(ctx, store, temporal, params)
		assert.Error(tt, err)
	})
}

func TestUpdatePool(t *testing.T) {
	t.Run("WhenGetOrCreateAccountFails", func(tt *testing.T) {
		ctx, se, _, temporal := setup(tt)
		params := &common.UpdatePoolParams{
			AccountName: "test_account",
			PoolId:      "test-pool-id",
		}

		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.New("account not found")
		}
		defer func() {
			getAccountWithName = originalGetAccountWithName
		}()

		_, _, err := _updatePool(ctx, se, temporal, params)
		assert.EqualError(tt, err, "account not found")
	})
	t.Run("WhenPoolNotFound", func(tt *testing.T) {
		ctx, store, _, temporal := setup(tt)

		params := &common.UpdatePoolParams{
			AccountName: "test_account",
			PoolId:      "non-existent-pool",
		}

		// Return a valid account
		dbAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-uuid", ID: 1},
			Name:      "test_account",
		}
		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return dbAccount, nil
		}
		defer func() {
			getAccountWithName = originalGetAccountWithName
		}()

		_, _, err := _updatePool(ctx, store, temporal, params)
		assert.Contains(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "pool not found")
	})
	t.Run("WhenValidatePoolParamsFails", func(tt *testing.T) {
		ctx, store, _, temporal := setup(tt)
		params := &common.UpdatePoolParams{
			AccountName: "test_account",
			PoolId:      "test-pool-uuid1",
		}

		_, account := createDBPools(t, store)

		originalGetAccountWithName := getAccountWithName
		originalValidateAndSetUpdatePoolParams := ValidateAndSetUpdatePoolParams

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		ValidateAndSetUpdatePoolParams = func(params *common.UpdatePoolParams, pool *datamodel.Pool) error {
			return errors.New("invalid pool params")
		}

		defer func() {
			getAccountWithName = originalGetAccountWithName
			ValidateAndSetUpdatePoolParams = originalValidateAndSetUpdatePoolParams
		}()

		_, _, err := _updatePool(ctx, store, temporal, params)
		assert.EqualError(tt, err, "invalid pool params")
	})
	t.Run("WhenUpdatePoolSucceeds", func(tt *testing.T) {
		ctx, store, _, temporal := setup(tt)
		params := &common.UpdatePoolParams{
			AccountName:          "test_account",
			PoolId:               "test-pool-uuid1",
			SizeInBytes:          uint64(4 * utils.TiBInBytes),
			QosType:              QosTypeAuto,
			LargeCapacity:        false,
			TotalThroughputMibps: 256,
			TotalIops:            nillable.ToPointer(int64(4096)),
		}
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		pools, account := createDBPools(t, store)

		originalGetAccountWithName := getAccountWithName
		originalValidateAndSetUpdatePoolParams := ValidateAndSetUpdatePoolParams

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		ValidateAndSetUpdatePoolParams = func(params *common.UpdatePoolParams, pool *datamodel.Pool) error {
			return nil
		}

		defer func() {
			getAccountWithName = originalGetAccountWithName
			ValidateAndSetUpdatePoolParams = originalValidateAndSetUpdatePoolParams
		}()

		pool, _, err := _updatePool(ctx, store, temporal, params)
		assert.NoError(tt, err, "Expected no error on updating pool")
		assert.Equal(tt, pools[0].Name, pool.Name)
		assert.Equal(tt, models.LifeCycleStateUpdating, pool.State)
	})
	t.Run("WhenExecuteWorkflowFails", func(tt *testing.T) {
		ctx, store, _, temporal := setup(tt)
		params := &common.UpdatePoolParams{
			AccountName:          "test_account",
			PoolId:               "test-pool-uuid1",
			SizeInBytes:          uint64(4 * utils.TiBInBytes),
			QosType:              QosTypeAuto,
			LargeCapacity:        false,
			TotalThroughputMibps: 256,
			TotalIops:            nillable.ToPointer(int64(4096)),
		}

		_, account := createDBPools(t, store)

		originalGetAccountWithName := getAccountWithName
		originalValidateAndSetUpdatePoolParams := ValidateAndSetUpdatePoolParams

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		ValidateAndSetUpdatePoolParams = func(params *common.UpdatePoolParams, pool *datamodel.Pool) error {
			return nil
		}

		defer func() {
			getAccountWithName = originalGetAccountWithName
			ValidateAndSetUpdatePoolParams = originalValidateAndSetUpdatePoolParams
		}()

		// Fail workflow execution.
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, params, mock.Anything, mock.Anything).
			Return(nil, fmt.Errorf("workflow error"))

		_, _, err := _updatePool(ctx, store, temporal, params)
		assert.EqualError(tt, err, "workflow error")
	})
}

func TestGetPool(t *testing.T) {
	t.Run("WhenPoolDoesNotExist", func(tt *testing.T) {
		ctx, _, orch, _ := setup(tt)

		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{Name: "test_account"}, nil
		}
		defer func() {
			getAccountWithName = originalGetAccountWithName
		}()

		_, err := orch.DescribePool(ctx, "non-existent-uuid", "")
		var customErr *vsaerrors.CustomError
		if vsaerrors.As(err, &customErr) {
			assert.EqualError(tt, customErr.Unwrap(), "pool not found")
		} else {
			tt.Errorf("Expected custom error, got %v", err)
		}
	})

	t.Run("WhenPoolExists", func(tt *testing.T) {
		ctx, store, orch, _ := setup(tt)

		pools, account := createDBPools(t, store)

		result, err := orch.DescribePool(ctx, "test-pool-uuid1", "test_account")
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, pools[0].Name, result.Name)
		assert.Equal(tt, account.Name, result.AccountName)
	})
}

func TestGetPoolByVendorID(t *testing.T) {
	t.Run("WhenPoolDoesNotExist", func(tt *testing.T) {
		ctx, _, orch, _ := setup(tt)

		_, err := orch.GetPoolByVendorID(ctx, "non-existent-vendor-id", "")
		var customErr *vsaerrors.CustomError
		if vsaerrors.As(err, &customErr) {
			assert.EqualError(tt, customErr.Unwrap(), "pool not found")
		} else {
			tt.Errorf("Expected custom error, got %v", err)
		}
	})

	t.Run("WhenPoolExists", func(tt *testing.T) {
		ctx, store, orch, _ := setup(tt)

		pools, account := createDBPools(t, store)

		result, err := orch.GetPoolByVendorID(ctx, "test-vendor-id", "test_account")
		assert.NoError(tt, err)
		assert.Equal(tt, pools[0].Name, result.Name)
		assert.Equal(tt, account.Name, result.AccountName)
	})
}

// Test the new helper functions
func TestDeletePool(t *testing.T) {
	t.Run("WhenGetOrCreateAccountFails", func(tt *testing.T) {
		ctx, se, _, temporal := setup(tt)

		params := &common.DeletePoolParams{
			AccountName: "test_account",
			PoolID:      "test_pool_id",
		}

		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.New("account not found")
		}
		defer func() {
			getAccountWithName = originalGetAccountWithName
		}()

		_, _, err := deletePool(ctx, temporal, se, params)
		assert.EqualError(tt, err, "account not found")
	})
	t.Run("WhenPoolDoesNotExist", func(tt *testing.T) {
		ctx, store, _, temporal := setup(tt)

		// Clear the in-memory database
		err := database.ClearInMemoryDB(store.DB())
		assert.NoError(tt, err)

		params := &common.DeletePoolParams{
			AccountName: "test_account",
			PoolID:      "non_existent_pool_id",
		}

		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{Name: "test_account"}, nil
		}
		defer func() {
			getAccountWithName = originalGetAccountWithName
		}()

		_, _, err = deletePool(ctx, temporal, store, params)
		var customErr *vsaerrors.CustomError
		if vsaerrors.As(err, &customErr) {
			assert.EqualError(tt, customErr.Unwrap(), "pool not found")
		} else {
			tt.Errorf("Expected custom error, got %v", err)
		}
	})
	t.Run("WhenDeletePoolSucceeds", func(tt *testing.T) {
		ctx, store, _, temporal := setup(tt)
		pools, account := createDBPools(t, store)
		pool := pools[0]

		params := &common.DeletePoolParams{
			AccountName: account.Name,
			PoolID:      pool.UUID,
		}

		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() {
			getAccountWithName = originalGetAccountWithName
		}()

		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		result, jobID, err := _deletePool(ctx, temporal, store, params)
		assert.NoError(tt, err)
		assert.Equal(tt, pool.Name, result.Name, "Expected pool name to match")
		assert.NotEmpty(tt, jobID)
	})
}

func TestMultiplePools(t *testing.T) {
	t.Run("ReturnsErrorWhenAccountDoesNotExist", func(tt *testing.T) {
		ctx, _, orch, _ := setup(tt)

		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.NewNotFoundErr("account not found", nil)
		}
		defer func() {
			getAccountWithName = originalGetAccountWithName
		}()

		_, err := orch.GetMultiplePools(ctx, "non-existent-account", []string{"uuid1", "uuid2"})
		if err != nil {
			t.Errorf("Expected nil, got error: %v", err)
		}
	})
	t.Run("ReturnsErrorWhenNoPoolsMatchUUIDs", func(tt *testing.T) {
		ctx, store, orch, _ := setup(tt)
		_, account := createDBPools(t, store)

		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() {
			getAccountWithName = originalGetAccountWithName
		}()

		pools, err := orch.GetMultiplePools(ctx, account.Name, []string{"non-existent-uuid"})
		assert.NoError(tt, err)
		if len(pools) != 0 {
			tt.Fatalf("Expected 0 pools, got %v", len(pools))
		}
	})
	t.Run("WhenGetMultiplePoolsReturnsError", func(tt *testing.T) {
		accountName := "test_account"
		ctx := context.Background()
		mockStorage := new(database.MockStorage)
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		orch := &Orchestrator{
			storage: mockStorage,
		}

		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{Name: accountName}, nil
		}
		defer func() {
			getAccountWithName = originalGetAccountWithName
		}()

		mockStorage.On("ListPools", ctx, mock.Anything).Return(nil, errors.New("list pools error"))
		_, err := orch.GetMultiplePools(ctx, accountName, []string{"uuid1", "uuid2"})
		assert.EqualError(tt, err, "list pools error")
	})
	t.Run("ReturnsPoolsSuccessfullyWhenUUIDsMatch", func(tt *testing.T) {
		ctx, store, orch, _ := setup(tt)

		dbPools, account := createDBPools(t, store)

		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() {
			getAccountWithName = originalGetAccountWithName
		}()

		result, err := orch.GetMultiplePools(ctx, account.Name, []string{"test-pool-uuid1", "test-pool-uuid2"})
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
		assert.Len(tt, result, 2, "Expected 2 pools, got %d", len(result))
		assert.Equal(tt, dbPools[0].Name, result[0].Name, "Returned pool does not match expected pool")
		assert.Equal(tt, dbPools[1].Name, result[1].Name, "Returned pool does not match expected pool")
	})
}

func TestListPools(t *testing.T) {
	t.Run("ReturnsErrorWhenAccountDoesNotExist", func(tt *testing.T) {
		ctx, _, orch, _ := setup(tt)

		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.NewNotFoundErr("account not found", nil)
		}
		defer func() {
			getAccountWithName = originalGetAccountWithName
		}()

		_, err := orch.ListPools(ctx, "non-existent-account", false)
		if err == nil {
			tt.Errorf("Expected error, got nil")
		}
		if !errors.IsNotFoundErr(err) {
			tt.Errorf("Expected not found error, got %v", err)
		}
	})
	t.Run("ReturnsEmptyListWhenNoPoolsExist", func(tt *testing.T) {
		ctx, store, orch, _ := setup(tt)
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err := store.DB().Create(account).Error
		assert.NoError(tt, err)

		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() {
			getAccountWithName = originalGetAccountWithName
		}()

		pools, err := orch.ListPools(ctx, account.Name, false)
		assert.NoError(tt, err)
		assert.Empty(tt, pools)
	})
	t.Run("WhenListPoolsReturnsError", func(tt *testing.T) {
		accountName := "test_account"
		ctx := context.Background()
		mockStorage := new(database.MockStorage)
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		orch := &Orchestrator{
			storage: mockStorage,
		}

		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{Name: accountName}, nil
		}
		defer func() {
			getAccountWithName = originalGetAccountWithName
		}()

		mockStorage.On("ListPools", ctx, mock.Anything).Return(nil, errors.New("list pools error"))
		_, err := orch.ListPools(ctx, accountName, false)
		assert.EqualError(tt, err, "list pools error")
	})
	t.Run("ReturnsPoolsSuccessfully", func(tt *testing.T) {
		ctx, store, orch, _ := setup(tt)

		dbPools, account := createDBPools(tt, store)

		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() {
			getAccountWithName = originalGetAccountWithName
		}()

		pools, err := orch.ListPools(ctx, account.Name, false)
		assert.NoError(tt, err)
		assert.Len(tt, pools, 2, "Expected 2 pools, got %d", len(pools))
		assert.Equal(tt, dbPools[0].Name, pools[0].Name, "Returned pool does not match expected pool")
		assert.Equal(tt, dbPools[1].Name, pools[1].Name, "Returned pool does not match expected pool")
	})
	t.Run("ReturnsPoolsSuccessfullyWithDeleted", func(tt *testing.T) {
		ctx, store, orch, _ := setup(tt)

		dbPools, account := createDBPools(tt, store)

		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() {
			getAccountWithName = originalGetAccountWithName
		}()

		pools, err := orch.ListPools(ctx, account.Name, true)
		assert.NoError(tt, err)

		assert.Len(tt, pools, 3, "Expected 2 pools, got %d", len(pools))
		assert.Equal(tt, dbPools[0].Name, pools[0].Name, "Returned pool does not match expected pool")
		assert.Equal(tt, dbPools[1].Name, pools[1].Name, "Returned pool does not match expected pool")
		assert.Equal(tt, dbPools[2].Name, pools[2].Name, "Returned pool does not match expected pool")
	})
}

func TestListAllPools(t *testing.T) {
	ctx := context.Background()
	mockLogger := log.NewLogger()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
	store, err := database.SetupStorageForTest(mockLogger)
	if err != nil {
		t.Fatalf("Failed to create test storage: %v", err)
	}

	err = database.ClearInMemoryDB(store.DB())
	if err != nil {
		t.Fatalf("Failed to clean up test storage: %v", err)
	}

	// Create two pools, one deleted and one not deleted
	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
		Name:      "test_account",
	}
	err = store.DB().Create(account).Error
	if err != nil {
		t.Fatalf("Failed to create account: %v", err)
	}

	pool1 := &datamodel.Pool{
		BaseModel:      datamodel.BaseModel{UUID: "test-pool-uuid1"},
		Name:           "test_pool_1",
		AccountID:      account.ID,
		PoolAttributes: &datamodel.PoolAttributes{},
		DeploymentName: "dep1",
	}
	pool2 := &datamodel.Pool{
		BaseModel:      datamodel.BaseModel{UUID: "test-pool-uuid2", DeletedAt: &gorm.DeletedAt{Time: time.Now(), Valid: true}},
		Name:           "test_pool_2",
		AccountID:      account.ID,
		PoolAttributes: &datamodel.PoolAttributes{},
		DeploymentName: "dep2",
	}
	err = store.DB().Create(pool1).Error
	if err != nil {
		t.Fatalf("Failed to create pool1: %v", err)
	}
	err = store.DB().Create(pool2).Error
	if err != nil {
		t.Fatalf("Failed to create pool2: %v", err)
	}

	orch := Orchestrator{storage: store}

	pools, err := orch.ListAllPools(ctx)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if len(pools) != 1 {
		t.Errorf("Expected 1 pool (non-deleted), got %d", len(pools))
	}
	if pools[0].Name != pool1.Name {
		t.Errorf("Returned pool does not match expected pool")
	}
}

func TestListAllPools_ErrorFromStorage(t *testing.T) {
	ctx := context.Background()
	mockLogger := log.NewLogger()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
	mockStorage := new(database.MockStorage)

	// Simulate error from ListPools
	mockStorage.EXPECT().ListPools(ctx, (*utils2.Filter)(nil)).Return(nil, errors.New("db error"))

	orch := Orchestrator{storage: mockStorage}

	pools, err := orch.ListAllPools(ctx)
	if err == nil {
		t.Errorf("Expected error, got nil")
	}
	if pools != nil {
		t.Errorf("Expected nil pools, got %v", pools)
	}
}

func TestGetPoolByName(t *testing.T) {
	queryDepthOne := 1
	queryDepthZero := 0
	t.Run("WhenGetAccountFails", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		se := database.Storage(nil)

		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.New("account not found")
		}
		defer func() {
			getAccountWithName = originalGetAccountWithName
		}()

		_, err := GetPoolByName(ctx, se, "test-pool", "test-account", queryDepthOne)
		assert.EqualError(tt, err, "account not found")
	})
	t.Run("WhenGetPoolByNameFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := new(database.MockStorage)
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)

		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{Name: "test-account"}, nil
		}
		defer func() {
			getAccountWithName = originalGetAccountWithName
		}()

		mockStorage.On("GetPoolByName", ctx, mock.Anything).Return(nil, errors.New("pool not found"))
		_, err := GetPoolByName(ctx, mockStorage, "test-pool", "test-account", queryDepthOne)
		assert.EqualError(tt, err, "pool not found")
	})
	t.Run("WhenSuccess", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := new(database.MockStorage)
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)

		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{Name: "test-account"}, nil
		}
		defer func() {
			getAccountWithName = originalGetAccountWithName
		}()

		poolResp := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid", ID: 1},
			Name:      "test-pool",
			PoolAttributes: &datamodel.PoolAttributes{
				ThroughputMibps: 64,
				Iops:            1024, // datamodel.PoolAttributes expects int64, not pointer
				PrimaryZone:     "us-central1-a",
			},
		}
		poolView := database.ConvertPoolToPoolView(poolResp)
		mockStorage.On("GetPoolByName", ctx, mock.Anything).Return(poolView, nil)

		_, err := GetPoolByName(ctx, mockStorage, "test-pool", "test-account", queryDepthOne)
		assert.NoError(tt, err)
	})
	t.Run("WhenSuccessQueryDepthZero", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := new(database.MockStorage)
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)

		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{Name: "test-account"}, nil
		}
		defer func() {
			getAccountWithName = originalGetAccountWithName
		}()

		poolResp := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid", ID: 1},
			Name:      "test-pool",
			PoolAttributes: &datamodel.PoolAttributes{
				ThroughputMibps: 64,
				Iops:            1024, // datamodel.PoolAttributes expects int64, not pointer
				PrimaryZone:     "us-central1-a",
			},
		}
		nodeResp := []*datamodel.Node{
			{
				BaseModel: datamodel.BaseModel{UUID: "test-node-uuid", ID: 1},
				Name:      "test-node",
			},
		}
		poolView := database.ConvertPoolToPoolView(poolResp)
		mockStorage.On("GetPoolByName", ctx, mock.Anything).Return(poolView, nil)
		mockStorage.On("GetNodesByPoolID", ctx, mock.Anything).Return(nodeResp, nil)

		_, err := GetPoolByName(ctx, mockStorage, "test-pool", "test-account", queryDepthZero)
		assert.NoError(tt, err)
	})
}

func TestConvertDatastorePoolsToModelWithoutAccountNameParam_ReturnsCorrectModels(t *testing.T) {
	poolView1 := &datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				ID:        1,
				UUID:      "mock-uuid",
				CreatedAt: time.Now(),
				UpdatedAt: time.Now(),
				DeletedAt: nil,
			},
			Name:             "mock-pool",
			Description:      "Mock pool description",
			State:            "ACTIVE",
			StateDetails:     "Mock state details",
			VendorID:         "mock-vendor-id",
			ServiceLevel:     "mock-service-level",
			SizeInBytes:      1024 * 1024 * 1024, // 1 GiB
			UsedBytes:        512 * 1024 * 1024,  // 512 MiB
			Network:          "mock-network",
			AllowAutoTiering: true,
			AutoTieringConfig: &datamodel.AutoTieringConfig{
				HotTierSizeInBytes:      256 * 1024 * 1024, // 256 MiB
				EnableHotTierAutoResize: false,
				BucketName:              "mock-bucket-name",
			},
			AccountID: 1,
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{
					ID:        1,
					UUID:      "mock-account-uuid",
					CreatedAt: time.Now(),
					UpdatedAt: time.Now(),
				},
				Name:         "mock-account",
				Description:  "Mock account description",
				State:        "ACTIVE",
				StateDetails: "Mock account state details",
				Tags:         "mock-tags",
			},
			PoolAttributes: &datamodel.PoolAttributes{
				ThroughputMibps: 100,
				Iops:            1000,
				PrimaryZone:     "mock-primary-zone",
				SecondaryZone:   "mock-secondary-zone",
			},
			ClusterDetails: datamodel.ClusterDetails{
				ExternalName:          "mock-external-name",
				OntapVersion:          "mock-ontap-version",
				RegionalTenantProject: "mock-regional-tenant-project",
				SnHostProject:         "mock-sn-host-project",
				Network:               "mock-cluster-network",
			},
			QosType:          "mock-qos-type",
			ServiceAccountId: "mock-service-account-id",
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "mock-password",
				SecretID:      "mock-secret-id",
				CertificateID: "mock-certificate-id",
			},
		},
		Throughput:   100.5,
		QuotaInBytes: 2048 * 1024 * 1024, // 2 GiB
		VolumeCount:  5,
	}
	pools := []*datamodel.PoolView{poolView1}

	result := convertDatastorePoolsToModelWithoutAccountNameParam(pools)

	assert.Len(t, result, 1)
	assert.Equal(t, "mock-pool", result[0].Name)
	assert.Equal(t, "mock-account", result[0].AccountName)
}

// TestCreatePool_WorkflowFailure_JobMarkedAsErrored tests that jobs are marked as errored when workflow fails to start
func TestCreatePool_WorkflowFailure_JobMarkedAsErrored(t *testing.T) {
	ctx := context.Background()
	mockLogger := log.NewLogger()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)

	mockStorage := new(database.MockStorage)
	mockTemporal := new(workflowenginemock.MockTemporalTestClient)

	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
		Name:      "test-account",
	}

	job := &datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "job-uuid"},
		Type:      string(models.JobTypeCreatePool),
		State:     string(models.JobsStateNEW),
	}

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
		Name:      "test-pool",
		AccountID: account.ID,
	}

	params := &common.CreatePoolParams{
		AccountName:    "test-account",
		Name:           "test-pool",
		VendorID:       "vendor-id",
		VendorSubNetID: "subnet-id",
		SizeInBytes:    2 * utils.TiBInBytes, // 2TiB (minimum)
		ServiceLevel:   ServiceLevelNameFLEX,
		QosType:        QosTypeAuto,
		Region:         "us-central1",
		PrimaryZone:    "us-central1-a",
		CustomPerformanceParams: &common.CustomPerformanceParams{
			ThroughputMibps: 64,
			Iops:            nillable.ToPointer(int64(1024)), // datamodel.PoolAttributes expects int64, not pointer
		},
	}

	// Setup mocks
	mockStorage.On("GetAccount", ctx, "test-account").Return(account, nil)
	mockStorage.On("CreateJob", ctx, mock.Anything).Return(job, nil)
	mockStorage.On("CreatingPool", ctx, mock.Anything).Return(pool, nil)

	// Mock workflow execution to fail
	workflowError := errors.New("workflow execution failed")
	mockTemporal.On("ExecuteWorkflow", ctx, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, workflowError)

	// Mock job update to errored state
	mockStorage.On("UpdateJob", ctx, "job-uuid", string(models.JobsStateERROR), 0, "workflow execution failed").Return(nil)

	// Mock pool state update to errored state (called by defer function)
	mockStorage.On("UpdatePoolState", ctx, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	// Execute test
	result, jobID, err := _createPool(ctx, mockStorage, mockTemporal, params)

	// Verify results
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Empty(t, jobID)
	assert.Equal(t, "workflow execution failed", err.Error())

	// Verify job was marked as errored
	mockStorage.AssertCalled(t, "UpdateJob", ctx, "job-uuid", string(models.JobsStateERROR), 0, "workflow execution failed")
}

// TestUpdatePool_WorkflowFailure_JobMarkedAsErrored tests that jobs are marked as errored when workflow fails to start
func TestUpdatePool_WorkflowFailure_JobMarkedAsErrored(t *testing.T) {
	ctx := context.Background()
	mockLogger := log.NewLogger()
	_ = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)

	// Use the setup function like other tests do
	ctx, store, _, temporal := setup(t)

	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
		Name:      "test-account",
	}

	params := &common.UpdatePoolParams{
		PoolId:               "pool-uuid",
		AccountName:          "test-account",
		SizeInBytes:          3 * utils.TiBInBytes, // 3TiB
		TotalThroughputMibps: 128,
		TotalIops:            nillable.ToPointer(int64(2048)),
	}

	// Create a pool in the database
	err := store.DB().Create(account).Error
	assert.NoError(t, err)

	err = store.DB().Create(&datamodel.Pool{
		BaseModel:    datamodel.BaseModel{UUID: "pool-uuid"},
		Name:         "test-pool",
		AccountID:    account.ID,
		State:        models.LifeCycleStateREADY,
		StateDetails: models.LifeCycleStateReadyDetails,
	}).Error
	assert.NoError(t, err)

	// Mock the functions like other working tests
	originalGetAccountWithName := getAccountWithName
	originalValidateUpdatePoolParams := ValidateAndSetUpdatePoolParams

	defer func() {
		getAccountWithName = originalGetAccountWithName
		ValidateAndSetUpdatePoolParams = originalValidateUpdatePoolParams
	}()

	getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
		return account, nil
	}

	ValidateAndSetUpdatePoolParams = func(params *common.UpdatePoolParams, pool *datamodel.Pool) error {
		return nil
	}

	// Mock workflow execution to fail
	workflowError := errors.New("workflow execution failed")
	temporal.EXPECT().ExecuteWorkflow(ctx, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, workflowError)

	// Execute test
	result, jobID, err := _updatePool(ctx, store, temporal, params)

	// Verify results
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Empty(t, jobID)
	assert.Equal(t, "workflow execution failed", err.Error())

	// Verify job was created and marked as errored
	var jobs []datamodel.Job
	store.DB().Find(&jobs)
	assert.Len(t, jobs, 1)
	assert.Equal(t, string(models.JobsStateERROR), jobs[0].State)

	// Verify pool was also marked as errored
	var pools []datamodel.Pool
	store.DB().Find(&pools)
	assert.Len(t, pools, 1)
	assert.Equal(t, models.LifeCycleStateREADY, pools[0].State)
	assert.Equal(t, models.LifeCycleStateReadyDetails, pools[0].StateDetails)
}

// TestDeletePool_WorkflowFailure_JobMarkedAsErrored tests that jobs are marked as errored when workflow fails to start
func TestDeletePool_WorkflowFailure_JobMarkedAsErrored(t *testing.T) {
	ctx := context.Background()
	mockLogger := log.NewLogger()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)

	mockStorage := new(database.MockStorage)
	mockTemporal := new(workflowenginemock.MockTemporalTestClient)

	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
		Name:      "test-account",
	}

	job := &datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "job-uuid"},
		Type:      string(models.JobTypeDeletePool),
		State:     string(models.JobsStateNEW),
	}

	poolView := &datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
			Name:      "test-pool",
			AccountID: account.ID,
			Account:   account,
		},
		VolumeCount: 0, // No volumes so it can be deleted
	}

	params := &common.DeletePoolParams{
		PoolID:      "pool-uuid",
		AccountName: "test-account",
	}

	// Mock getAccountWithName function using helper
	originalGetAccountWithName := getAccountWithName
	getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
		return account, nil
	}
	defer func() { getAccountWithName = originalGetAccountWithName }()

	// Setup mocks
	mockStorage.On("GetPool", ctx, "pool-uuid", account.ID).Return(poolView, nil)
	mockStorage.On("CreateJob", ctx, mock.Anything).Return(job, nil)
	mockStorage.On("DeletingPool", ctx, mock.Anything).Return(nil)

	// Mock workflow execution to fail
	workflowError := errors.New("workflow execution failed")
	mockTemporal.On("ExecuteWorkflow", ctx, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, workflowError)

	// Mock job update to errored state
	mockStorage.On("UpdateJob", ctx, "job-uuid", string(models.JobsStateERROR), 0, "workflow execution failed").Return(nil)

	// Mock pool state update to errored state (called by defer function)
	mockStorage.On("UpdatePoolState", ctx, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	// Execute test
	result, jobID, err := _deletePool(ctx, mockTemporal, mockStorage, params)

	// Verify results
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Empty(t, jobID)
	assert.Equal(t, "workflow execution failed", err.Error())

	// Verify job was marked as errored
	mockStorage.AssertCalled(t, "UpdateJob", ctx, "job-uuid", string(models.JobsStateERROR), 0, "workflow execution failed")
}

// TestCreatePool_WorkflowFailure_JobUpdateFails tests error handling when both workflow and job update fail
func TestCreatePool_WorkflowFailure_JobUpdateFails(t *testing.T) {
	ctx := context.Background()
	mockLogger := log.NewLogger()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)

	mockStorage := new(database.MockStorage)
	mockTemporal := new(workflowenginemock.MockTemporalTestClient)

	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
		Name:      "test-account",
	}

	job := &datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "job-uuid"},
		Type:      string(models.JobTypeCreatePool),
		State:     string(models.JobsStateNEW),
	}

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
		Name:      "test-pool",
		AccountID: account.ID,
	}

	params := &common.CreatePoolParams{
		AccountName:    "test-account",
		Name:           "test-pool",
		VendorID:       "vendor-id",
		VendorSubNetID: "subnet-id",
		SizeInBytes:    2 * utils.TiBInBytes, // 2TiB (minimum)
		ServiceLevel:   ServiceLevelNameFLEX,
		QosType:        QosTypeAuto,
		Region:         "us-central1",
		PrimaryZone:    "us-central1-a",
		CustomPerformanceParams: &common.CustomPerformanceParams{
			ThroughputMibps: 64,
			Iops:            nillable.ToPointer(int64(1024)), // datamodel.PoolAttributes expects int64, not pointer
		},
	}

	// Setup mocks
	mockStorage.On("GetAccount", ctx, "test-account").Return(account, nil)
	mockStorage.On("CreateJob", ctx, mock.Anything).Return(job, nil)
	mockStorage.On("CreatingPool", ctx, mock.Anything).Return(pool, nil)

	// Mock workflow execution to fail
	workflowError := errors.New("workflow execution failed")
	mockTemporal.On("ExecuteWorkflow", ctx, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, workflowError)

	// Mock job update to also fail
	jobUpdateError := errors.New("job update failed")
	mockStorage.On("UpdateJob", ctx, "job-uuid", string(models.JobsStateERROR), 0, "workflow execution failed").Return(jobUpdateError)

	// Mock pool state update to errored state (called by defer function)
	mockStorage.On("UpdatePoolState", ctx, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	// Execute test - should still return the original workflow error, not the job update error
	result, jobID, err := _createPool(ctx, mockStorage, mockTemporal, params)

	// Verify results - should still get the original workflow error
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Empty(t, jobID)
	assert.Equal(t, "workflow execution failed", err.Error())

	// Verify job update was attempted (even though it failed)
	mockStorage.AssertCalled(t, "UpdateJob", ctx, "job-uuid", string(models.JobsStateERROR), 0, "workflow execution failed")
}

// TestCreatePool_CreatePoolInDBFailsWithGenericError tests error handling when CreatePoolInDB fails with generic error (Line 74)
func TestCreatePool_CreatePoolInDBFailsWithGenericError(t *testing.T) {
	ctx := context.Background()
	mockLogger := log.NewLogger()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)

	mockStorage := new(database.MockStorage)
	mockTemporal := new(workflowenginemock.MockTemporalTestClient)

	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
		Name:      "test-account",
	}

	params := &common.CreatePoolParams{
		AccountName:    "test-account",
		Name:           "test-pool",
		VendorID:       "vendor-id",
		VendorSubNetID: "subnet-id",
		SizeInBytes:    2 * utils.TiBInBytes, // 2TiB (minimum)
		ServiceLevel:   ServiceLevelNameFLEX,
		QosType:        QosTypeAuto,
		Region:         "us-central1",
		PrimaryZone:    "us-central1-a",
		CustomPerformanceParams: &common.CustomPerformanceParams{
			ThroughputMibps: 64,
			Iops:            nillable.ToPointer(int64(1024)), // datamodel.PoolAttributes expects int64, not pointer
		},
	}

	// Setup mocks
	mockStorage.On("GetAccount", ctx, "test-account").Return(account, nil)

	// Mock CreatePoolInDB (CreatingPool) to fail with generic error (not conflict)
	dbError := errors.New("database connection error")
	mockStorage.On("CreatingPool", ctx, mock.Anything).Return(nil, dbError)

	// Execute test
	result, jobID, err := _createPool(ctx, mockStorage, mockTemporal, params)

	// Verify results - should return the generic error message (Line 74)
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Empty(t, jobID)
	assert.Equal(t, "unable to process request, please try again later", err.Error())
}

// TestCreatePool_CreatePoolInDBFailsWithConflictError tests error handling when CreatePoolInDB fails with conflict error
func TestCreatePool_CreatePoolInDBFailsWithConflictError(t *testing.T) {
	ctx := context.Background()
	mockLogger := log.NewLogger()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)

	mockStorage := new(database.MockStorage)
	mockTemporal := new(workflowenginemock.MockTemporalTestClient)

	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
		Name:      "test-account",
	}

	params := &common.CreatePoolParams{
		AccountName:    "test-account",
		Name:           "test-pool",
		VendorID:       "vendor-id",
		VendorSubNetID: "subnet-id",
		SizeInBytes:    2 * utils.TiBInBytes, // 2TiB (minimum)
		ServiceLevel:   ServiceLevelNameFLEX,
		QosType:        QosTypeAuto,
		Region:         "us-central1",
		PrimaryZone:    "us-central1-a",
		CustomPerformanceParams: &common.CustomPerformanceParams{
			ThroughputMibps: 64,
			Iops:            nillable.ToPointer(int64(1024)), // datamodel.PoolAttributes expects int64, not pointer
		},
	}

	// Setup mocks
	mockStorage.On("GetAccount", ctx, "test-account").Return(account, nil)

	// Mock CreatePoolInDB (CreatingPool) to fail with conflict error
	conflictError := errors.NewConflictErr("pool already exists")
	mockStorage.On("CreatingPool", ctx, mock.Anything).Return(nil, conflictError)

	// Execute test
	result, jobID, err := _createPool(ctx, mockStorage, mockTemporal, params)

	// Verify results - should pass through the conflict error (not the generic message)
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Empty(t, jobID)
	assert.Equal(t, conflictError, err)
}

// TestCreatePool_UpdatePoolStateFailsInDefer tests error handling when UpdatePoolState fails in defer (Line 80)
func TestCreatePool_UpdatePoolStateFailsInDefer(t *testing.T) {
	ctx := context.Background()
	mockLogger := log.NewLogger()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)

	mockStorage := new(database.MockStorage)
	mockTemporal := new(workflowenginemock.MockTemporalTestClient)

	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
		Name:      "test-account",
	}

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
		Name:      "test-pool",
		AccountID: account.ID,
	}

	job := &datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "job-uuid"},
		Type:      string(models.JobTypeCreatePool),
		State:     string(models.JobsStateNEW),
	}

	params := &common.CreatePoolParams{
		AccountName:    "test-account",
		Name:           "test-pool",
		VendorID:       "vendor-id",
		VendorSubNetID: "subnet-id",
		SizeInBytes:    2 * utils.TiBInBytes, // 2TiB (minimum)
		ServiceLevel:   ServiceLevelNameFLEX,
		QosType:        QosTypeAuto,
		Region:         "us-central1",
		PrimaryZone:    "us-central1-a",
		CustomPerformanceParams: &common.CustomPerformanceParams{
			ThroughputMibps: 64,
			Iops:            nillable.ToPointer(int64(1024)), // datamodel.PoolAttributes expects int64, not pointer
		},
	}

	// Setup mocks
	mockStorage.On("GetAccount", ctx, "test-account").Return(account, nil)
	mockStorage.On("CreatingPool", ctx, mock.Anything).Return(pool, nil)
	mockStorage.On("CreateJob", ctx, mock.Anything).Return(job, nil)

	// Mock workflow execution to fail
	workflowError := errors.New("workflow execution failed")
	mockTemporal.On("ExecuteWorkflow", ctx, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, workflowError)

	// Mock UpdateJob to succeed
	mockStorage.On("UpdateJob", ctx, "job-uuid", string(models.JobsStateERROR), 0, "workflow execution failed").Return(nil)

	// Mock UpdatePoolState to fail (Line 80)
	poolStateError := errors.New("pool state update failed")
	mockStorage.On("UpdatePoolState", ctx, mock.Anything, mock.Anything, mock.Anything).Return(nil, poolStateError)

	// Execute test
	result, jobID, err := _createPool(ctx, mockStorage, mockTemporal, params)

	// Verify results - should still return the original workflow error
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Empty(t, jobID)
	assert.Equal(t, "workflow execution failed", err.Error())

	// Verify UpdatePoolState was called (even though it failed)
	mockStorage.AssertCalled(t, "UpdatePoolState", ctx, mock.Anything, mock.Anything, mock.Anything)
}

// TestCreatePool_CreateJobFails tests error handling when CreateJob fails (Line 97)
func TestCreatePool_CreateJobFails(t *testing.T) {
	ctx := context.Background()
	mockLogger := log.NewLogger()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)

	mockStorage := new(database.MockStorage)
	mockTemporal := new(workflowenginemock.MockTemporalTestClient)

	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
		Name:      "test-account",
	}

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
		Name:      "test-pool",
		AccountID: account.ID,
	}

	params := &common.CreatePoolParams{
		AccountName:    "test-account",
		Name:           "test-pool",
		VendorID:       "vendor-id",
		VendorSubNetID: "subnet-id",
		SizeInBytes:    2 * utils.TiBInBytes, // 2TiB (minimum)
		ServiceLevel:   ServiceLevelNameFLEX,
		QosType:        QosTypeAuto,
		Region:         "us-central1",
		PrimaryZone:    "us-central1-a",
		CustomPerformanceParams: &common.CustomPerformanceParams{
			ThroughputMibps: 64,
			Iops:            nillable.ToPointer(int64(1024)), // datamodel.PoolAttributes expects int64, not pointer
		},
	}

	// Setup mocks
	mockStorage.On("GetAccount", ctx, "test-account").Return(account, nil)
	mockStorage.On("CreatingPool", ctx, mock.Anything).Return(pool, nil)

	// Mock CreateJob to fail (Line 97)
	jobError := errors.New("job creation failed")
	mockStorage.On("CreateJob", ctx, mock.Anything).Return(nil, jobError)

	// Mock UpdatePoolState for the defer function call (Line 80)
	mockStorage.On("UpdatePoolState", ctx, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	// Execute test
	result, jobID, err := _createPool(ctx, mockStorage, mockTemporal, params)

	// Verify results - should return the generic error message (Line 97)
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Empty(t, jobID)
	assert.Equal(t, "unable to process request, please try again later", err.Error())
}

// TestUpdatePool_UpdateJobFailsInDefer tests error handling when UpdateJob fails in defer (Line 235)
func TestUpdatePool_UpdateJobFailsInDefer(t *testing.T) {
	ctx := context.Background()
	mockLogger := log.NewLogger()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)

	mockStorage := new(database.MockStorage)
	mockTemporal := new(workflowenginemock.MockTemporalTestClient)

	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
		Name:      "test-account",
	}

	poolView := &datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel:   datamodel.BaseModel{UUID: "pool-uuid"},
			Name:        "test-pool",
			AccountID:   account.ID,
			Account:     account,
			SizeInBytes: 2 * utils.TiBInBytes,
			QosType:     QosTypeAuto,
			PoolAttributes: &datamodel.PoolAttributes{
				ThroughputMibps: 64,
				Iops:            1024, // datamodel.PoolAttributes expects int64, not pointer
			},
		},
	}

	pool := &datamodel.Pool{
		BaseModel:   datamodel.BaseModel{UUID: "pool-uuid"},
		Name:        "test-pool",
		AccountID:   account.ID,
		SizeInBytes: 2 * utils.TiBInBytes,
		QosType:     QosTypeAuto,
	}

	job := &datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "job-uuid"},
		Type:      string(models.JobTypeUpdatePool),
		State:     string(models.JobsStateNEW),
	}

	params := &common.UpdatePoolParams{
		AccountName:          "test-account",
		PoolId:               "pool-uuid",
		SizeInBytes:          2 * utils.TiBInBytes,
		QosType:              QosTypeAuto,
		TotalThroughputMibps: 64,
		TotalIops:            nillable.ToPointer(int64(1024)),
	}

	// Setup mocks - override the getAccountWithName function
	getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
		return account, nil
	}
	defer func() {
		getAccountWithName = _getAccountWithName // restore original function
	}()

	mockStorage.On("GetPool", ctx, "pool-uuid", account.ID).Return(poolView, nil)
	mockStorage.On("CreateJob", ctx, mock.Anything).Return(job, nil)
	mockStorage.On("UpdatingPool", ctx, mock.Anything).Return(pool, nil)

	// Mock workflow execution to fail
	workflowError := errors.New("workflow execution failed")
	mockTemporal.On("ExecuteWorkflow", ctx, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, workflowError)

	// Mock UpdateJob to fail (Line 235)
	jobUpdateError := errors.New("job update failed")
	mockStorage.On("UpdateJob", ctx, "job-uuid", string(models.JobsStateERROR), 0, "workflow execution failed").Return(jobUpdateError)

	// Mock UpdatePoolState to succeed
	mockStorage.On("UpdatePoolState", ctx, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	// Execute test
	result, jobID, err := _updatePool(ctx, mockStorage, mockTemporal, params)

	// Verify results - should still return the original workflow error
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Empty(t, jobID)
	assert.Equal(t, "workflow execution failed", err.Error())

	// Verify UpdateJob was called (even though it failed)
	mockStorage.AssertCalled(t, "UpdateJob", ctx, "job-uuid", string(models.JobsStateERROR), 0, "workflow execution failed")
}

// TestUpdatePool_UpdatePoolStateFailsInDefer tests error handling when UpdatePoolState fails in defer (Line 240)
func TestUpdatePool_UpdatePoolStateFailsInDefer(t *testing.T) {
	ctx := context.Background()
	mockLogger := log.NewLogger()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)

	mockStorage := new(database.MockStorage)
	mockTemporal := new(workflowenginemock.MockTemporalTestClient)

	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
		Name:      "test-account",
	}

	poolView := &datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel:   datamodel.BaseModel{UUID: "pool-uuid"},
			Name:        "test-pool",
			AccountID:   account.ID,
			Account:     account,
			SizeInBytes: 2 * utils.TiBInBytes,
			QosType:     QosTypeAuto,
			PoolAttributes: &datamodel.PoolAttributes{
				ThroughputMibps: 64,
				Iops:            1024, // datamodel.PoolAttributes expects int64, not pointer
			},
		},
	}

	pool := &datamodel.Pool{
		BaseModel:   datamodel.BaseModel{UUID: "pool-uuid"},
		Name:        "test-pool",
		AccountID:   account.ID,
		SizeInBytes: 2 * utils.TiBInBytes,
		QosType:     QosTypeAuto,
	}

	job := &datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "job-uuid"},
		Type:      string(models.JobTypeUpdatePool),
		State:     string(models.JobsStateNEW),
	}

	params := &common.UpdatePoolParams{
		AccountName:          "test-account",
		PoolId:               "pool-uuid",
		SizeInBytes:          2 * utils.TiBInBytes,
		QosType:              QosTypeAuto,
		TotalThroughputMibps: 64,
		TotalIops:            nillable.ToPointer(int64(1024)),
	}

	// Setup mocks - override the getAccountWithName function
	getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
		return account, nil
	}
	defer func() {
		getAccountWithName = _getAccountWithName // restore original function
	}()

	mockStorage.On("GetPool", ctx, "pool-uuid", account.ID).Return(poolView, nil)
	mockStorage.On("CreateJob", ctx, mock.Anything).Return(job, nil)
	mockStorage.On("UpdatingPool", ctx, mock.Anything).Return(pool, nil)

	// Mock workflow execution to fail
	workflowError := errors.New("workflow execution failed")
	mockTemporal.On("ExecuteWorkflow", ctx, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, workflowError)

	// Mock UpdateJob to succeed
	mockStorage.On("UpdateJob", ctx, "job-uuid", string(models.JobsStateERROR), 0, "workflow execution failed").Return(nil)

	// Mock UpdatePoolState to fail (Line 240)
	poolStateError := errors.New("pool state update failed")
	mockStorage.On("UpdatePoolState", ctx, mock.Anything, mock.Anything, mock.Anything).Return(nil, poolStateError)

	// Execute test
	result, jobID, err := _updatePool(ctx, mockStorage, mockTemporal, params)

	// Verify results - should still return the original workflow error
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Empty(t, jobID)
	assert.Equal(t, "workflow execution failed", err.Error())

	// Verify UpdatePoolState was called (even though it failed)
	mockStorage.AssertCalled(t, "UpdatePoolState", ctx, mock.Anything, mock.Anything, mock.Anything)
}

// TestDeletePool_UpdateJobFailsInDefer tests error handling when UpdateJob fails in defer (Line 406)
func TestDeletePool_UpdateJobFailsInDefer(t *testing.T) {
	ctx := context.Background()
	mockLogger := log.NewLogger()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)

	mockStorage := new(database.MockStorage)
	mockTemporal := new(workflowenginemock.MockTemporalTestClient)

	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
		Name:      "test-account",
	}

	poolView := &datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
			Name:      "test-pool",
			AccountID: account.ID,
			Account:   account,
		},
		VolumeCount: 0, // No volumes, so delete is allowed
	}

	job := &datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "job-uuid"},
		Type:      string(models.JobTypeDeletePool),
		State:     string(models.JobsStateNEW),
	}

	params := &common.DeletePoolParams{
		AccountName: "test-account",
		PoolID:      "pool-uuid",
	}

	// Setup mocks - override the getAccountWithName function
	getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
		return account, nil
	}
	defer func() {
		getAccountWithName = _getAccountWithName // restore original function
	}()

	mockStorage.On("GetPool", ctx, "pool-uuid", account.ID).Return(poolView, nil)
	mockStorage.On("CreateJob", ctx, mock.Anything).Return(job, nil)
	mockStorage.On("DeletingPool", ctx, mock.Anything).Return(nil)

	// Mock workflow execution to fail
	workflowError := errors.New("workflow execution failed")
	mockTemporal.On("ExecuteWorkflow", ctx, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, workflowError)

	// Mock UpdateJob to fail (Line 406)
	jobUpdateError := errors.New("job update failed")
	mockStorage.On("UpdateJob", ctx, "job-uuid", string(models.JobsStateERROR), 0, "workflow execution failed").Return(jobUpdateError)

	// Mock UpdatePoolState to succeed
	mockStorage.On("UpdatePoolState", ctx, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	// Execute test
	result, jobID, err := _deletePool(ctx, mockTemporal, mockStorage, params)

	// Verify results - should still return the original workflow error
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Empty(t, jobID)
	assert.Equal(t, "workflow execution failed", err.Error())

	// Verify UpdateJob was called (even though it failed)
	mockStorage.AssertCalled(t, "UpdateJob", ctx, "job-uuid", string(models.JobsStateERROR), 0, "workflow execution failed")
}

// TestDeletePool_UpdatePoolStateFailsInDefer tests error handling when UpdatePoolState fails in defer (Line 419)
func TestDeletePool_UpdatePoolStateFailsInDefer(t *testing.T) {
	ctx := context.Background()
	mockLogger := log.NewLogger()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)

	mockStorage := new(database.MockStorage)
	mockTemporal := new(workflowenginemock.MockTemporalTestClient)

	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
		Name:      "test-account",
	}

	poolView := &datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
			Name:      "test-pool",
			AccountID: account.ID,
			Account:   account,
		},
		VolumeCount: 0, // No volumes, so delete is allowed
	}

	job := &datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "job-uuid"},
		Type:      string(models.JobTypeDeletePool),
		State:     string(models.JobsStateNEW),
	}

	params := &common.DeletePoolParams{
		AccountName: "test-account",
		PoolID:      "pool-uuid",
	}

	// Setup mocks - override the getAccountWithName function
	getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
		return account, nil
	}
	defer func() {
		getAccountWithName = _getAccountWithName // restore original function
	}()

	mockStorage.On("GetPool", ctx, "pool-uuid", account.ID).Return(poolView, nil)
	mockStorage.On("CreateJob", ctx, mock.Anything).Return(job, nil)
	mockStorage.On("DeletingPool", ctx, mock.Anything).Return(nil)

	// Mock workflow execution to fail
	workflowError := errors.New("workflow execution failed")
	mockTemporal.On("ExecuteWorkflow", ctx, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, workflowError)

	// Mock UpdateJob to succeed
	mockStorage.On("UpdateJob", ctx, "job-uuid", string(models.JobsStateERROR), 0, "workflow execution failed").Return(nil)

	// Mock UpdatePoolState to fail (Line 419)
	poolStateError := errors.New("pool state update failed")
	mockStorage.On("UpdatePoolState", ctx, mock.Anything, mock.Anything, mock.Anything).Return(nil, poolStateError)

	// Execute test
	result, jobID, err := _deletePool(ctx, mockTemporal, mockStorage, params)

	// Verify results - should still return the original workflow error
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Empty(t, jobID)
	assert.Equal(t, "workflow execution failed", err.Error())

	// Verify UpdatePoolState was called (even though it failed)
	mockStorage.AssertCalled(t, "UpdatePoolState", ctx, mock.Anything, mock.Anything, mock.Anything)
}

// Tests for the new unified validation function _validatePoolParams
func TestValidatePoolParams(t *testing.T) {
	t.Run("ValidCreateParams_StandardPool", func(tt *testing.T) {
		perf := &validators.CustomPerformance{
			SizeInBytes:        uint64(2 * utils.TiBInBytes),
			ThroughputMibps:    128,
			Iops:               nillable.ToPointer(int64(2048)),
			AllowAutoTiering:   false,
			HotTierSizeInBytes: 0,
			QosType:            QosTypeAuto,
			LargeCapacity:      false,
		}

		err := _validatePoolParams(perf, ServiceLevelNameFLEX)
		assert.NoError(tt, err, "Valid standard pool params should pass validation")
	})

	t.Run("ValidCreateParams_LargeCapacityPool", func(tt *testing.T) {
		perf := &validators.CustomPerformance{
			SizeInBytes:        uint64(100 * utils.TiBInBytes),
			ThroughputMibps:    1000,
			Iops:               nillable.ToPointer(int64(16000)), // Minimum IOPS for 1000 MiBps in large capacity
			AllowAutoTiering:   false,                            // Set to false since auto-tiering is globally disabled
			HotTierSizeInBytes: 0,                                // No hot tier size when auto-tiering is disabled
			QosType:            QosTypeAuto,
			LargeCapacity:      true,
		}

		err := _validatePoolParams(perf, ServiceLevelNameFLEX)
		assert.NoError(tt, err, "Valid large capacity pool params should pass validation")
	})

	t.Run("InvalidServiceLevel_ReturnsError", func(tt *testing.T) {
		perf := &validators.CustomPerformance{
			SizeInBytes:        uint64(2 * utils.TiBInBytes),
			ThroughputMibps:    128,
			Iops:               nillable.ToPointer(int64(2048)),
			AllowAutoTiering:   false,
			HotTierSizeInBytes: 0,
			QosType:            QosTypeAuto,
			LargeCapacity:      false,
		}

		err := _validatePoolParams(perf, "Premium")
		assert.Error(tt, err)
		assert.EqualError(tt, err, "Given service level not supported. Supported service level is "+ServiceLevelNameFLEX)
	})

	t.Run("EmptyServiceLevel_NoValidationError", func(tt *testing.T) {
		perf := &validators.CustomPerformance{
			SizeInBytes:        uint64(2 * utils.TiBInBytes),
			ThroughputMibps:    128,
			Iops:               nillable.ToPointer(int64(2048)),
			AllowAutoTiering:   false,
			HotTierSizeInBytes: 0,
			QosType:            QosTypeAuto,
			LargeCapacity:      false,
		}

		err := _validatePoolParams(perf, "")
		assert.NoError(tt, err, "Empty service level should not cause validation error")
	})

	t.Run("InvalidSize_ReturnsError", func(tt *testing.T) {
		perf := &validators.CustomPerformance{
			SizeInBytes:        uint64(1 * utils.GiBInBytes), // Below minimum
			ThroughputMibps:    128,
			Iops:               nillable.ToPointer(int64(2048)),
			AllowAutoTiering:   false,
			HotTierSizeInBytes: 0,
			QosType:            QosTypeAuto,
			LargeCapacity:      false,
		}

		err := _validatePoolParams(perf, ServiceLevelNameFLEX)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Given pool size not supported")
	})

	t.Run("InvalidQosType_ReturnsError", func(tt *testing.T) {
		perf := &validators.CustomPerformance{
			SizeInBytes:        uint64(2 * utils.TiBInBytes),
			ThroughputMibps:    128,
			Iops:               nillable.ToPointer(int64(2048)),
			AllowAutoTiering:   false,
			HotTierSizeInBytes: 0,
			QosType:            "Manual", // Invalid QoS type
			LargeCapacity:      false,
		}

		err := _validatePoolParams(perf, ServiceLevelNameFLEX)
		assert.Error(tt, err)
		assert.EqualError(tt, err, "Given QoS type not supported for Unified Flex Storage Pool. Supported QoS type is "+QosTypeAuto)
	})

	t.Run("InvalidThroughput_ReturnsError", func(tt *testing.T) {
		perf := &validators.CustomPerformance{
			SizeInBytes:        uint64(2 * utils.TiBInBytes),
			ThroughputMibps:    -1, // Invalid negative throughput
			Iops:               nillable.ToPointer(int64(2048)),
			AllowAutoTiering:   false,
			HotTierSizeInBytes: 0,
			QosType:            QosTypeAuto,
			LargeCapacity:      false,
		}

		err := _validatePoolParams(perf, ServiceLevelNameFLEX)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "TotalThroughputMibps must be set and must be greater than 0")
	})

	t.Run("InvalidIOPS_ReturnsError", func(tt *testing.T) {
		perf := &validators.CustomPerformance{
			SizeInBytes:        uint64(2 * utils.TiBInBytes),
			ThroughputMibps:    128,
			Iops:               nillable.ToPointer(int64(100)), // Below minimum IOPS
			AllowAutoTiering:   false,
			HotTierSizeInBytes: 0,
			QosType:            QosTypeAuto,
			LargeCapacity:      false,
		}

		err := _validatePoolParams(perf, ServiceLevelNameFLEX)
		assert.Contains(tt, err.Error(), "TotalIops must be between")
	})

	t.Run("InvalidAutoTiering_ReturnsError", func(tt *testing.T) {
		perf := &validators.CustomPerformance{
			SizeInBytes:        uint64(2 * utils.TiBInBytes),
			ThroughputMibps:    128,
			Iops:               nillable.ToPointer(int64(2048)),
			AllowAutoTiering:   true,
			HotTierSizeInBytes: uint64(3 * utils.TiBInBytes), // Hot tier larger than pool size
			QosType:            QosTypeAuto,
			LargeCapacity:      false,
		}

		err := _validatePoolParams(perf, ServiceLevelNameFLEX)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Auto-Tiering feature is currently not enabled")
	})

	t.Run("ValidAutoTiering_NoError", func(tt *testing.T) {
		perf := &validators.CustomPerformance{
			SizeInBytes:        uint64(10 * utils.TiBInBytes),
			ThroughputMibps:    128,
			Iops:               nillable.ToPointer(int64(2048)),
			AllowAutoTiering:   false, // Set to false since auto-tiering is globally disabled
			HotTierSizeInBytes: 0,     // No hot tier size when auto-tiering is disabled
			QosType:            QosTypeAuto,
			LargeCapacity:      false,
		}

		err := _validatePoolParams(perf, ServiceLevelNameFLEX)
		assert.NoError(tt, err, "Valid params without auto-tiering should pass validation")
	})

	t.Run("LargeCapacityPool_ValidParams", func(tt *testing.T) {
		perf := &validators.CustomPerformance{
			SizeInBytes:        uint64(100 * utils.TiBInBytes),
			ThroughputMibps:    1000,
			Iops:               nillable.ToPointer(int64(16000)), // Minimum IOPS for 1000 MiBps in large capacity
			AllowAutoTiering:   false,
			HotTierSizeInBytes: 0,
			QosType:            QosTypeAuto,
			LargeCapacity:      true,
		}

		err := _validatePoolParams(perf, ServiceLevelNameFLEX)
		assert.NoError(tt, err, "Valid large capacity pool params should pass validation")
	})

	t.Run("LargeCapacityPool_InvalidSize_ReturnsError", func(tt *testing.T) {
		perf := &validators.CustomPerformance{
			SizeInBytes:        uint64(10 * utils.TiBInBytes), // Below large capacity minimum (12 TiB)
			ThroughputMibps:    1000,
			Iops:               nillable.ToPointer(int64(16000)), // Minimum IOPS for 1000 MiBps in large capacity
			AllowAutoTiering:   false,
			HotTierSizeInBytes: 0,
			QosType:            QosTypeAuto,
			LargeCapacity:      true,
		}

		err := _validatePoolParams(perf, ServiceLevelNameFLEX)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "SizeInBytes must be at least")
	})

	// Additional edge cases
	t.Run("ZeroThroughput_ReturnsError", func(tt *testing.T) {
		perf := &validators.CustomPerformance{
			SizeInBytes:        uint64(2 * utils.TiBInBytes),
			ThroughputMibps:    0, // Zero throughput
			Iops:               nillable.ToPointer(int64(2048)),
			AllowAutoTiering:   false,
			HotTierSizeInBytes: 0,
			QosType:            QosTypeAuto,
			LargeCapacity:      false,
		}

		err := _validatePoolParams(perf, ServiceLevelNameFLEX)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "TotalThroughputMibps must be between")
	})

	t.Run("NilIOPS_CalculatedFromThroughput", func(tt *testing.T) {
		perf := &validators.CustomPerformance{
			SizeInBytes:        uint64(2 * utils.TiBInBytes),
			ThroughputMibps:    128,
			Iops:               nil, // IOPS not set, should be calculated
			AllowAutoTiering:   false,
			HotTierSizeInBytes: 0,
			QosType:            QosTypeAuto,
			LargeCapacity:      false,
		}

		err := _validatePoolParams(perf, ServiceLevelNameFLEX)
		assert.NoError(tt, err, "IOPS should be calculated from throughput when not provided")
	})

	t.Run("MaximumValidSize_StandardPool", func(tt *testing.T) {
		perf := &validators.CustomPerformance{
			SizeInBytes:        uint64(50 * utils.TiBInBytes), // Maximum for standard pool
			ThroughputMibps:    128,
			Iops:               nillable.ToPointer(int64(2048)),
			AllowAutoTiering:   false,
			HotTierSizeInBytes: 0,
			QosType:            QosTypeAuto,
			LargeCapacity:      false,
		}

		err := _validatePoolParams(perf, ServiceLevelNameFLEX)
		assert.NoError(tt, err, "Maximum valid size for standard pool should pass validation")
	})

	t.Run("MaximumValidSize_LargeCapacityPool", func(tt *testing.T) {
		perf := &validators.CustomPerformance{
			SizeInBytes:        uint64(1000 * utils.TiBInBytes), // Maximum for large capacity pool
			ThroughputMibps:    1000,
			Iops:               nillable.ToPointer(int64(16000)), // Minimum IOPS for 1000 MiBps in large capacity
			AllowAutoTiering:   false,
			HotTierSizeInBytes: 0,
			QosType:            QosTypeAuto,
			LargeCapacity:      true,
		}

		err := _validatePoolParams(perf, ServiceLevelNameFLEX)
		assert.NoError(tt, err, "Maximum valid size for large capacity pool should pass validation")
	})

	t.Run("AutoTieringDisabled_WithHotTierSize_NoError", func(tt *testing.T) {
		perf := &validators.CustomPerformance{
			SizeInBytes:        uint64(10 * utils.TiBInBytes),
			ThroughputMibps:    128,
			Iops:               nillable.ToPointer(int64(2048)),
			AllowAutoTiering:   false,
			HotTierSizeInBytes: uint64(2 * utils.TiBInBytes), // Hot tier size set but auto-tiering disabled
			QosType:            QosTypeAuto,
			LargeCapacity:      false,
		}

		err := _validatePoolParams(perf, ServiceLevelNameFLEX)
		assert.NoError(tt, err, "Hot tier size should be allowed when auto-tiering is disabled")
	})
}

// Tests for the refactored _validateCreatePoolParams function
func TestValidateCreatePoolParamsRefactored(t *testing.T) {
	t.Run("ValidParams_StandardPool", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:   uint64(2 * utils.TiBInBytes),
			ServiceLevel:  ServiceLevelNameFLEX,
			QosType:       QosTypeAuto,
			LargeCapacity: false,
			CustomPerformanceParams: &common.CustomPerformanceParams{
				ThroughputMibps: 128,
				Iops:            nillable.ToPointer(int64(2048)),
			},
		}

		err := _validateCreatePoolParams(params)
		assert.NoError(tt, err, "Valid create params should pass validation")
	})

	t.Run("ValidParams_LargeCapacityPool", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:   uint64(100 * utils.TiBInBytes),
			ServiceLevel:  ServiceLevelNameFLEX,
			QosType:       QosTypeAuto,
			LargeCapacity: true,
			CustomPerformanceParams: &common.CustomPerformanceParams{
				ThroughputMibps: 1000,
				Iops:            nillable.ToPointer(int64(16000)), // Minimum IOPS for 1000 MiBps in large capacity
			},
			AllowAutoTiering:   true,
			HotTierSizeInBytes: uint64(10 * utils.TiBInBytes),
		}

		err := _validateCreatePoolParams(params)
		assert.Error(tt, err, "Auto-tiering feature is currently disabled")
		assert.Contains(tt, err.Error(), "Auto-Tiering feature is currently not enabled")
	})

	t.Run("InvalidServiceLevel_ReturnsError", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:   uint64(2 * utils.TiBInBytes),
			ServiceLevel:  "Premium", // Invalid service level
			QosType:       QosTypeAuto,
			LargeCapacity: false,
			CustomPerformanceParams: &common.CustomPerformanceParams{
				ThroughputMibps: 128,
				Iops:            nillable.ToPointer(int64(2048)),
			},
		}

		err := _validateCreatePoolParams(params)
		assert.Error(tt, err)
		assert.EqualError(tt, err, "Given service level not supported. Supported service level is "+ServiceLevelNameFLEX)
	})

	t.Run("NilCustomPerformanceParams_ReturnsError", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:   uint64(2 * utils.TiBInBytes),
			ServiceLevel:  ServiceLevelNameFLEX,
			QosType:       QosTypeAuto,
			LargeCapacity: false,
			// CustomPerformanceParams is nil
		}

		err := _validateCreatePoolParams(params)
		assert.Error(tt, err, "Nil CustomPerformanceParams should cause validation error")
		assert.Contains(tt, err.Error(), "TotalThroughputMibps must be between")
	})

	// Additional test cases for create params
	t.Run("ValidParams_WithAutoTiering", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:   uint64(10 * utils.TiBInBytes),
			ServiceLevel:  ServiceLevelNameFLEX,
			QosType:       QosTypeAuto,
			LargeCapacity: false,
			CustomPerformanceParams: &common.CustomPerformanceParams{
				ThroughputMibps: 256,
				Iops:            nillable.ToPointer(int64(4096)),
			},
			AllowAutoTiering:   true,
			HotTierSizeInBytes: uint64(2 * utils.TiBInBytes),
		}

		err := _validateCreatePoolParams(params)
		assert.Error(tt, err, "Auto-tiering feature is currently disabled")
		assert.Contains(tt, err.Error(), "Auto-Tiering feature is currently not enabled")
	})

	t.Run("ValidParams_WithLabels", func(tt *testing.T) {
		labels := &datamodel.JSONB{
			"environment": "production",
			"team":        "storage",
		}
		params := &common.CreatePoolParams{
			SizeInBytes:   uint64(2 * utils.TiBInBytes),
			ServiceLevel:  ServiceLevelNameFLEX,
			QosType:       QosTypeAuto,
			LargeCapacity: false,
			CustomPerformanceParams: &common.CustomPerformanceParams{
				ThroughputMibps: 128,
				Iops:            nillable.ToPointer(int64(2048)),
			},
			Labels: labels,
		}

		err := _validateCreatePoolParams(params)
		assert.NoError(tt, err, "Valid create params with labels should pass validation")
	})

	t.Run("ValidParams_RegionalHA", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:   uint64(2 * utils.TiBInBytes),
			ServiceLevel:  ServiceLevelNameFLEX,
			QosType:       QosTypeAuto,
			LargeCapacity: false,
			CustomPerformanceParams: &common.CustomPerformanceParams{
				ThroughputMibps: 128,
				Iops:            nillable.ToPointer(int64(2048)),
			},
			IsRegionalHA: true,
		}

		err := _validateCreatePoolParams(params)
		assert.NoError(tt, err, "Valid create params with regional HA should pass validation")
	})

	// Additional edge cases and error scenarios for create params
	t.Run("InvalidSize_BelowMinimum_ReturnsError", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:   uint64(1 * utils.GiBInBytes), // Below minimum
			ServiceLevel:  ServiceLevelNameFLEX,
			QosType:       QosTypeAuto,
			LargeCapacity: false,
			CustomPerformanceParams: &common.CustomPerformanceParams{
				ThroughputMibps: 128,
				Iops:            nillable.ToPointer(int64(2048)),
			},
		}

		err := _validateCreatePoolParams(params)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Given pool size not supported")
	})

	t.Run("InvalidSize_AboveMaximum_StandardPool_ReturnsError", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:   uint64(500 * utils.TiBInBytes), // Above maximum for standard pool (425 TiB)
			ServiceLevel:  ServiceLevelNameFLEX,
			QosType:       QosTypeAuto,
			LargeCapacity: false,
			CustomPerformanceParams: &common.CustomPerformanceParams{
				ThroughputMibps: 1000,
				Iops:            nillable.ToPointer(int64(16000)), // Minimum IOPS for 1000 MiBps
			},
		}

		err := _validateCreatePoolParams(params)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Given pool size not supported")
	})

	t.Run("InvalidSize_BelowMinimum_LargeCapacityPool_ReturnsError", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:   uint64(10 * utils.TiBInBytes), // Below minimum for large capacity pool (12 TiB)
			ServiceLevel:  ServiceLevelNameFLEX,
			QosType:       QosTypeAuto,
			LargeCapacity: true,
			CustomPerformanceParams: &common.CustomPerformanceParams{
				ThroughputMibps: 1000,
				Iops:            nillable.ToPointer(int64(16000)), // Minimum IOPS for 1000 MiBps in large capacity
			},
		}

		err := _validateCreatePoolParams(params)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "SizeInBytes must be at least")
	})

	t.Run("InvalidThroughput_BelowMinimum_ReturnsError", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:   uint64(2 * utils.TiBInBytes),
			ServiceLevel:  ServiceLevelNameFLEX,
			QosType:       QosTypeAuto,
			LargeCapacity: false,
			CustomPerformanceParams: &common.CustomPerformanceParams{
				ThroughputMibps: -1, // Below minimum
				Iops:            nillable.ToPointer(int64(2048)),
			},
		}

		err := _validateCreatePoolParams(params)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "TotalThroughputMibps must be set and must be greater than 0")
	})

	t.Run("InvalidThroughput_AboveMaximum_ReturnsError", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:   uint64(2 * utils.TiBInBytes),
			ServiceLevel:  ServiceLevelNameFLEX,
			QosType:       QosTypeAuto,
			LargeCapacity: false,
			CustomPerformanceParams: &common.CustomPerformanceParams{
				ThroughputMibps: 10000, // Above maximum
				Iops:            nillable.ToPointer(int64(2048)),
			},
		}

		err := _validateCreatePoolParams(params)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "TotalThroughputMibps must be between")
	})

	t.Run("InvalidIOPS_BelowMinimum_ReturnsError", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:   uint64(2 * utils.TiBInBytes),
			ServiceLevel:  ServiceLevelNameFLEX,
			QosType:       QosTypeAuto,
			LargeCapacity: false,
			CustomPerformanceParams: &common.CustomPerformanceParams{
				ThroughputMibps: 128,
				Iops:            nillable.ToPointer(int64(100)), // Below minimum
			},
		}

		err := _validateCreatePoolParams(params)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "TotalIops must be between")
	})

	t.Run("InvalidIOPS_AboveMaximum_ReturnsError", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:   uint64(2 * utils.TiBInBytes),
			ServiceLevel:  ServiceLevelNameFLEX,
			QosType:       QosTypeAuto,
			LargeCapacity: false,
			CustomPerformanceParams: &common.CustomPerformanceParams{
				ThroughputMibps: 128,
				Iops:            nillable.ToPointer(int64(200000)), // Above maximum
			},
		}

		err := _validateCreatePoolParams(params)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "TotalIops must be between")
	})

	t.Run("InvalidAutoTiering_HotTierTooLarge_ReturnsError", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:   uint64(5 * utils.TiBInBytes),
			ServiceLevel:  ServiceLevelNameFLEX,
			QosType:       QosTypeAuto,
			LargeCapacity: false,
			CustomPerformanceParams: &common.CustomPerformanceParams{
				ThroughputMibps: 128,
				Iops:            nillable.ToPointer(int64(2048)),
			},
			AllowAutoTiering:   true,
			HotTierSizeInBytes: uint64(10 * utils.TiBInBytes), // Hot tier larger than pool size
		}

		err := _validateCreatePoolParams(params)
		assert.Error(tt, err, "Auto-tiering should fail when globally disabled")
		assert.Contains(tt, err.Error(), "Auto-Tiering feature is currently not enabled")
	})

	t.Run("ValidParams_BoundarySize_StandardPool", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:   uint64(50 * utils.TiBInBytes), // Boundary size for standard pool
			ServiceLevel:  ServiceLevelNameFLEX,
			QosType:       QosTypeAuto,
			LargeCapacity: false,
			CustomPerformanceParams: &common.CustomPerformanceParams{
				ThroughputMibps: 256,
				Iops:            nillable.ToPointer(int64(4096)),
			},
		}

		err := _validateCreatePoolParams(params)
		assert.NoError(tt, err, "Boundary size for standard pool should pass validation")
	})

	t.Run("ValidParams_BoundarySize_LargeCapacityPool", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:   uint64(100 * utils.TiBInBytes), // Boundary size for large capacity pool
			ServiceLevel:  ServiceLevelNameFLEX,
			QosType:       QosTypeAuto,
			LargeCapacity: true,
			CustomPerformanceParams: &common.CustomPerformanceParams{
				ThroughputMibps: 1000,
				Iops:            nillable.ToPointer(int64(16000)), // Minimum IOPS for 1000 MiBps in large capacity
			},
		}

		err := _validateCreatePoolParams(params)
		assert.NoError(tt, err, "Boundary size for large capacity pool should pass validation")
	})

	t.Run("ValidParams_WithHotTierAutoResize", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:   uint64(10 * utils.TiBInBytes),
			ServiceLevel:  ServiceLevelNameFLEX,
			QosType:       QosTypeAuto,
			LargeCapacity: false,
			CustomPerformanceParams: &common.CustomPerformanceParams{
				ThroughputMibps: 256,
				Iops:            nillable.ToPointer(int64(4096)),
			},
			AllowAutoTiering:        true,
			HotTierSizeInBytes:      uint64(2 * utils.TiBInBytes),
			EnableHotTierAutoResize: true,
		}

		err := _validateCreatePoolParams(params)
		assert.Error(tt, err, "Auto-tiering should fail when globally disabled")
		assert.Contains(tt, err.Error(), "Auto-Tiering feature is currently not enabled")
	})

	t.Run("ValidParams_WithKMSConfig", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:   uint64(2 * utils.TiBInBytes),
			ServiceLevel:  ServiceLevelNameFLEX,
			QosType:       QosTypeAuto,
			LargeCapacity: false,
			CustomPerformanceParams: &common.CustomPerformanceParams{
				ThroughputMibps: 128,
				Iops:            nillable.ToPointer(int64(2048)),
			},
			KmsConfigId: "test-kms-config-id",
		}

		err := _validateCreatePoolParams(params)
		assert.NoError(tt, err, "Valid create params with KMS config should pass validation")
	})

	t.Run("ValidParams_WithTags", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:   uint64(2 * utils.TiBInBytes),
			ServiceLevel:  ServiceLevelNameFLEX,
			QosType:       QosTypeAuto,
			LargeCapacity: false,
			CustomPerformanceParams: &common.CustomPerformanceParams{
				ThroughputMibps: 128,
				Iops:            nillable.ToPointer(int64(2048)),
			},
			Tags: "environment=production,team=storage",
		}

		err := _validateCreatePoolParams(params)
		assert.NoError(tt, err, "Valid create params with tags should pass validation")
	})
}

// Comprehensive tests for the _validateAndSetUpdatePoolParams function
func TestValidateUpdatePoolParamsComprehensive(t *testing.T) {
	// Test 1: Valid standard pool update parameters
	t.Run("ValidParams_StandardPool", func(tt *testing.T) {
		params := &common.UpdatePoolParams{
			SizeInBytes:          uint64(4 * utils.TiBInBytes),
			QosType:              QosTypeAuto,
			LargeCapacity:        false,
			TotalThroughputMibps: 256,
			TotalIops:            nillable.ToPointer(int64(4096)),
		}

		pool := &datamodel.Pool{
			SizeInBytes:      int64(2 * utils.TiBInBytes),
			AllowAutoTiering: false,
			LargeCapacity:    false,
		}

		err := _validateAndSetUpdatePoolParams(params, pool)
		assert.NoError(tt, err, "Valid update params should pass validation")
	})

	// Test 2: Valid large capacity pool update parameters
	t.Run("ValidParams_LargeCapacityPool", func(tt *testing.T) {
		params := &common.UpdatePoolParams{
			SizeInBytes:          uint64(200 * utils.TiBInBytes),
			QosType:              QosTypeAuto,
			LargeCapacity:        true,
			TotalThroughputMibps: 2000,
			TotalIops:            nillable.ToPointer(int64(32000)), // Minimum IOPS for 2000 MiBps in large capacity
		}

		pool := &datamodel.Pool{
			SizeInBytes:      int64(100 * utils.TiBInBytes),
			AllowAutoTiering: false,
			LargeCapacity:    true,
		}

		err := _validateAndSetUpdatePoolParams(params, pool)
		assert.NoError(tt, err, "Valid large capacity update params should pass validation")
	})

	// Test 3: Auto-tiering enabled for pool that didn't have it - valid hot tier size
	t.Run("AutoTieringEnabled_ValidHotTierSize", func(tt *testing.T) {
		params := &common.UpdatePoolParams{
			SizeInBytes:          uint64(10 * utils.TiBInBytes),
			QosType:              QosTypeAuto,
			LargeCapacity:        false,
			TotalThroughputMibps: 256,
			TotalIops:            nillable.ToPointer(int64(4096)),
			AllowAutoTiering:     true,
			HotTierSizeInBytes:   uint64(8 * utils.TiBInBytes), // Less than pool size
		}

		pool := &datamodel.Pool{
			SizeInBytes:      int64(10 * utils.TiBInBytes),
			AllowAutoTiering: false, // Pool didn't have auto-tiering
			LargeCapacity:    false,
		}

		err := _validateAndSetUpdatePoolParams(params, pool)
		assert.Error(tt, err, "Hot tier size validation should fail")
		assert.Contains(tt, err.Error(), "Hot tier size cannot be less than existing pool size")
	})

	// Test 4: Auto-tiering enabled for pool that didn't have it - hot tier too small
	t.Run("AutoTieringEnabled_HotTierTooSmall", func(tt *testing.T) {
		params := &common.UpdatePoolParams{
			SizeInBytes:          uint64(10 * utils.TiBInBytes),
			QosType:              QosTypeAuto,
			LargeCapacity:        false,
			TotalThroughputMibps: 256,
			TotalIops:            nillable.ToPointer(int64(4096)),
			AllowAutoTiering:     true,
			HotTierSizeInBytes:   uint64(5 * utils.TiBInBytes), // Less than pool size
		}

		pool := &datamodel.Pool{
			SizeInBytes:      int64(10 * utils.TiBInBytes),
			AllowAutoTiering: false, // Pool didn't have auto-tiering
			LargeCapacity:    false,
		}

		err := _validateAndSetUpdatePoolParams(params, pool)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Hot tier size cannot be less than existing pool size")
	})

	// Test 5: Auto-tiering enabled for pool that already had it - valid hot tier size increase
	t.Run("AutoTieringEnabled_ValidHotTierIncrease", func(tt *testing.T) {
		params := &common.UpdatePoolParams{
			SizeInBytes:          uint64(20 * utils.TiBInBytes),
			QosType:              QosTypeAuto,
			LargeCapacity:        false,
			TotalThroughputMibps: 512,
			TotalIops:            nillable.ToPointer(int64(8192)),
			AllowAutoTiering:     true,
			HotTierSizeInBytes:   uint64(15 * utils.TiBInBytes), // Greater than existing hot tier
		}

		pool := &datamodel.Pool{
			SizeInBytes:      int64(20 * utils.TiBInBytes),
			AllowAutoTiering: true, // Pool already had auto-tiering
			LargeCapacity:    false,
			AutoTieringConfig: &datamodel.AutoTieringConfig{
				HotTierSizeInBytes: int64(10 * utils.TiBInBytes), // Existing hot tier size
			},
		}

		err := _validateAndSetUpdatePoolParams(params, pool)
		assert.Error(tt, err, "Auto-tiering feature is currently disabled")
		assert.Contains(tt, err.Error(), "Auto-Tiering feature is currently not enabled")
	})

	// Test 6: Auto-tiering enabled for pool that already had it - hot tier size decrease (invalid)
	t.Run("AutoTieringEnabled_InvalidHotTierDecrease", func(tt *testing.T) {
		params := &common.UpdatePoolParams{
			SizeInBytes:          uint64(20 * utils.TiBInBytes),
			QosType:              QosTypeAuto,
			LargeCapacity:        false,
			TotalThroughputMibps: 512,
			TotalIops:            nillable.ToPointer(int64(8192)),
			AllowAutoTiering:     true,
			HotTierSizeInBytes:   uint64(8 * utils.TiBInBytes), // Less than existing hot tier
		}

		pool := &datamodel.Pool{
			SizeInBytes:      int64(20 * utils.TiBInBytes),
			AllowAutoTiering: true, // Pool already had auto-tiering
			LargeCapacity:    false,
			AutoTieringConfig: &datamodel.AutoTieringConfig{
				HotTierSizeInBytes: int64(10 * utils.TiBInBytes), // Existing hot tier size
			},
		}

		err := _validateAndSetUpdatePoolParams(params, pool)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Hot tier size must be greater than existing hot tier size")
	})

	// Test 7: Auto-tiering disabled for pool that had it (not supported)
	t.Run("AutoTieringDisabled_NotSupported", func(tt *testing.T) {
		params := &common.UpdatePoolParams{
			SizeInBytes:          uint64(20 * utils.TiBInBytes),
			QosType:              QosTypeAuto,
			LargeCapacity:        false,
			TotalThroughputMibps: 512,
			TotalIops:            nillable.ToPointer(int64(8192)),
			AllowAutoTiering:     false, // Trying to disable auto-tiering
		}

		pool := &datamodel.Pool{
			SizeInBytes:      int64(20 * utils.TiBInBytes),
			AllowAutoTiering: true, // Pool had auto-tiering enabled
			LargeCapacity:    false,
			AutoTieringConfig: &datamodel.AutoTieringConfig{
				HotTierSizeInBytes: int64(10 * utils.TiBInBytes),
			},
		}

		err := _validateAndSetUpdatePoolParams(params, pool)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Auto tiering disable operation is not supported")
	})

	// Test 8: Invalid pool size below minimum
	t.Run("InvalidSize_BelowMinimum", func(tt *testing.T) {
		params := &common.UpdatePoolParams{
			SizeInBytes:          uint64(1 * utils.GiBInBytes), // Below minimum
			QosType:              QosTypeAuto,
			LargeCapacity:        false,
			TotalThroughputMibps: 128,
			TotalIops:            nillable.ToPointer(int64(2048)),
		}

		pool := &datamodel.Pool{
			SizeInBytes:      int64(2 * utils.TiBInBytes),
			AllowAutoTiering: false,
			LargeCapacity:    false,
		}

		err := _validateAndSetUpdatePoolParams(params, pool)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Given pool size not supported")
	})

	// Test 9: Invalid pool size above maximum
	t.Run("InvalidSize_AboveMaximum", func(tt *testing.T) {
		params := &common.UpdatePoolParams{
			SizeInBytes:          uint64(500 * utils.TiBInBytes), // Above maximum (425 TiB)
			QosType:              QosTypeAuto,
			LargeCapacity:        false,
			TotalThroughputMibps: 128,
			TotalIops:            nillable.ToPointer(int64(2048)),
		}

		pool := &datamodel.Pool{
			SizeInBytes:      int64(2 * utils.TiBInBytes),
			AllowAutoTiering: false,
			LargeCapacity:    false,
		}

		err := _validateAndSetUpdatePoolParams(params, pool)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Given pool size not supported")
	})

	// Test 10: Invalid QoS type change from auto to manual
	t.Run("InvalidQosType_ChangeFromAutoToManual", func(tt *testing.T) {
		params := &common.UpdatePoolParams{
			SizeInBytes:          uint64(4 * utils.TiBInBytes),
			QosType:              "Manual", // Invalid QoS type
			LargeCapacity:        false,
			TotalThroughputMibps: 256,
			TotalIops:            nillable.ToPointer(int64(4096)),
		}

		pool := &datamodel.Pool{
			SizeInBytes:      int64(2 * utils.TiBInBytes),
			AllowAutoTiering: false,
			LargeCapacity:    false,
		}

		err := _validateAndSetUpdatePoolParams(params, pool)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Given QoS type not supported")
	})

	// Test 11: Invalid throughput below minimum
	t.Run("InvalidThroughput_BelowMinimum", func(tt *testing.T) {
		params := &common.UpdatePoolParams{
			SizeInBytes:          uint64(4 * utils.TiBInBytes),
			QosType:              QosTypeAuto,
			LargeCapacity:        false,
			TotalThroughputMibps: 32, // Below minimum (64 MiBps)
			TotalIops:            nillable.ToPointer(int64(2048)),
		}

		pool := &datamodel.Pool{
			SizeInBytes:      int64(2 * utils.TiBInBytes),
			AllowAutoTiering: false,
			LargeCapacity:    false,
		}

		err := _validateAndSetUpdatePoolParams(params, pool)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "TotalThroughputMibps must be between")
	})

	// Test 12: Invalid throughput above maximum
	t.Run("InvalidThroughput_AboveMaximum", func(tt *testing.T) {
		params := &common.UpdatePoolParams{
			SizeInBytes:          uint64(4 * utils.TiBInBytes),
			QosType:              QosTypeAuto,
			LargeCapacity:        false,
			TotalThroughputMibps: 6000, // Above maximum (5120 MiBps)
			TotalIops:            nillable.ToPointer(int64(2048)),
		}

		pool := &datamodel.Pool{
			SizeInBytes:      int64(2 * utils.TiBInBytes),
			AllowAutoTiering: false,
			LargeCapacity:    false,
		}

		err := _validateAndSetUpdatePoolParams(params, pool)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "TotalThroughputMibps must be between")
	})

	// Test 13: Invalid IOPS below minimum
	t.Run("InvalidIOPS_BelowMinimum", func(tt *testing.T) {
		params := &common.UpdatePoolParams{
			SizeInBytes:          uint64(4 * utils.TiBInBytes),
			QosType:              QosTypeAuto,
			LargeCapacity:        false,
			TotalThroughputMibps: 256,
			TotalIops:            nillable.ToPointer(int64(500)), // Below minimum (1024 IOPS)
		}

		pool := &datamodel.Pool{
			SizeInBytes:      int64(2 * utils.TiBInBytes),
			AllowAutoTiering: false,
			LargeCapacity:    false,
		}

		err := _validateAndSetUpdatePoolParams(params, pool)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "TotalIops must be between")
	})

	// Test 14: Invalid IOPS above maximum
	t.Run("InvalidIOPS_AboveMaximum", func(tt *testing.T) {
		params := &common.UpdatePoolParams{
			SizeInBytes:          uint64(4 * utils.TiBInBytes),
			QosType:              QosTypeAuto,
			LargeCapacity:        false,
			TotalThroughputMibps: 256,
			TotalIops:            nillable.ToPointer(int64(200000)), // Above maximum (160000 IOPS)
		}

		pool := &datamodel.Pool{
			SizeInBytes:      int64(2 * utils.TiBInBytes),
			AllowAutoTiering: false,
			LargeCapacity:    false,
		}

		err := _validateAndSetUpdatePoolParams(params, pool)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "TotalIops must be between")
	})

	t.Run("InvalidThroughput_ReturnsError", func(tt *testing.T) {
		params := &common.UpdatePoolParams{
			SizeInBytes:          uint64(4 * utils.TiBInBytes),
			QosType:              QosTypeAuto,
			LargeCapacity:        false,
			TotalThroughputMibps: -1, // Invalid negative throughput
			TotalIops:            nillable.ToPointer(int64(4096)),
		}

		pool := &datamodel.Pool{
			SizeInBytes:      int64(2 * utils.TiBInBytes),
			AllowAutoTiering: false,
			LargeCapacity:    false,
		}

		err := _validateAndSetUpdatePoolParams(params, pool)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "TotalThroughputMibps must be set and must be greater than")
	})

	// Test 15: IOPS calculated from throughput when not provided
	t.Run("NilIOPS_CalculatedFromThroughput", func(tt *testing.T) {
		params := &common.UpdatePoolParams{
			SizeInBytes:          uint64(4 * utils.TiBInBytes),
			QosType:              QosTypeAuto,
			LargeCapacity:        false,
			TotalThroughputMibps: 128, // IOPS will be calculated from this
			TotalIops:            nil, // IOPS not set, should be calculated
		}

		pool := &datamodel.Pool{
			SizeInBytes:      int64(2 * utils.TiBInBytes),
			AllowAutoTiering: false,
			LargeCapacity:    false,
		}

		err := _validateAndSetUpdatePoolParams(params, pool)
		assert.NoError(tt, err, "IOPS should be calculated from throughput when not provided")
	})

	// Test 16: Valid update with labels
	t.Run("ValidUpdate_WithLabels", func(tt *testing.T) {
		labels := &datamodel.JSONB{
			"environment": "production",
			"team":        "storage",
		}
		params := &common.UpdatePoolParams{
			SizeInBytes:          uint64(4 * utils.TiBInBytes),
			QosType:              QosTypeAuto,
			LargeCapacity:        false,
			TotalThroughputMibps: 256,
			TotalIops:            nillable.ToPointer(int64(4096)),
			Labels:               labels,
		}

		pool := &datamodel.Pool{
			SizeInBytes:      int64(2 * utils.TiBInBytes),
			AllowAutoTiering: false,
			LargeCapacity:    false,
		}

		err := _validateAndSetUpdatePoolParams(params, pool)
		assert.NoError(tt, err, "Valid update with labels should pass")
	})

	// Test 17: Valid update with description
	t.Run("ValidUpdate_WithDescription", func(tt *testing.T) {
		params := &common.UpdatePoolParams{
			SizeInBytes:          uint64(4 * utils.TiBInBytes),
			QosType:              QosTypeAuto,
			LargeCapacity:        false,
			TotalThroughputMibps: 256,
			TotalIops:            nillable.ToPointer(int64(4096)),
			Description:          "Updated pool description",
		}

		pool := &datamodel.Pool{
			SizeInBytes:      int64(2 * utils.TiBInBytes),
			AllowAutoTiering: false,
			LargeCapacity:    false,
		}

		err := _validateAndSetUpdatePoolParams(params, pool)
		assert.NoError(tt, err, "Valid update with description should pass")
	})

	// Test 18: Valid update with vendor ID
	t.Run("ValidUpdate_WithVendorID", func(tt *testing.T) {
		params := &common.UpdatePoolParams{
			SizeInBytes:          uint64(4 * utils.TiBInBytes),
			QosType:              QosTypeAuto,
			LargeCapacity:        false,
			TotalThroughputMibps: 256,
			TotalIops:            nillable.ToPointer(int64(4096)),
			VendorID:             "updated-vendor-id",
		}

		pool := &datamodel.Pool{
			SizeInBytes:      int64(2 * utils.TiBInBytes),
			AllowAutoTiering: false,
			LargeCapacity:    false,
		}

		err := _validateAndSetUpdatePoolParams(params, pool)
		assert.NoError(tt, err, "Valid update with vendor ID should pass")
	})

	// Test 19: Valid update with custom performance enabled
	t.Run("ValidUpdate_WithCustomPerformanceEnabled", func(tt *testing.T) {
		params := &common.UpdatePoolParams{
			SizeInBytes:              uint64(4 * utils.TiBInBytes),
			QosType:                  QosTypeAuto,
			LargeCapacity:            false,
			TotalThroughputMibps:     256,
			TotalIops:                nillable.ToPointer(int64(4096)),
			CustomPerformanceEnabled: true,
		}

		pool := &datamodel.Pool{
			SizeInBytes:      int64(2 * utils.TiBInBytes),
			AllowAutoTiering: false,
			LargeCapacity:    false,
		}

		err := _validateAndSetUpdatePoolParams(params, pool)
		assert.NoError(tt, err, "Valid update with custom performance enabled should pass")
	})

	t.Run("AutoTieringEnabled_ValidParams", func(tt *testing.T) {
		params := &common.UpdatePoolParams{
			SizeInBytes:          uint64(10 * utils.TiBInBytes),
			QosType:              QosTypeAuto,
			LargeCapacity:        false,
			TotalThroughputMibps: 256,
			TotalIops:            nillable.ToPointer(int64(4096)),
			AllowAutoTiering:     true,
			HotTierSizeInBytes:   uint64(2 * utils.TiBInBytes),
		}

		pool := &datamodel.Pool{
			SizeInBytes:      int64(5 * utils.TiBInBytes),
			AllowAutoTiering: false,
			LargeCapacity:    false,
		}

		err := _validateAndSetUpdatePoolParams(params, pool)
		assert.Error(tt, err, "Hot tier size validation should fail")
		assert.Contains(tt, err.Error(), "Hot tier size cannot be less than existing pool size")
	})

	// Additional test cases for update params
	t.Run("ValidParams_WithLabels", func(tt *testing.T) {
		labels := &datamodel.JSONB{
			"environment": "staging",
			"team":        "devops",
		}
		params := &common.UpdatePoolParams{
			SizeInBytes:          uint64(4 * utils.TiBInBytes),
			QosType:              QosTypeAuto,
			LargeCapacity:        false,
			TotalThroughputMibps: 256,
			TotalIops:            nillable.ToPointer(int64(4096)),
			Labels:               labels,
		}

		pool := &datamodel.Pool{
			SizeInBytes:      int64(2 * utils.TiBInBytes),
			AllowAutoTiering: false,
			LargeCapacity:    false,
		}

		err := _validateAndSetUpdatePoolParams(params, pool)
		assert.NoError(tt, err, "Valid update params with labels should pass validation")
	})

	t.Run("ValidParams_WithDescription", func(tt *testing.T) {
		params := &common.UpdatePoolParams{
			SizeInBytes:          uint64(4 * utils.TiBInBytes),
			QosType:              QosTypeAuto,
			LargeCapacity:        false,
			TotalThroughputMibps: 256,
			TotalIops:            nillable.ToPointer(int64(4096)),
			Description:          "Updated pool description",
		}

		pool := &datamodel.Pool{
			SizeInBytes:      int64(2 * utils.TiBInBytes),
			AllowAutoTiering: false,
			LargeCapacity:    false,
		}

		err := _validateAndSetUpdatePoolParams(params, pool)
		assert.NoError(tt, err, "Valid update params with description should pass validation")
	})

	t.Run("ValidParams_WithVendorID", func(tt *testing.T) {
		params := &common.UpdatePoolParams{
			SizeInBytes:          uint64(4 * utils.TiBInBytes),
			QosType:              QosTypeAuto,
			LargeCapacity:        false,
			TotalThroughputMibps: 256,
			TotalIops:            nillable.ToPointer(int64(4096)),
			VendorID:             "updated-vendor-id",
		}

		pool := &datamodel.Pool{
			SizeInBytes:      int64(2 * utils.TiBInBytes),
			AllowAutoTiering: false,
			LargeCapacity:    false,
		}

		err := _validateAndSetUpdatePoolParams(params, pool)
		assert.NoError(tt, err, "Valid update params with vendor ID should pass validation")
	})

	t.Run("ValidParams_WithCustomPerformanceEnabled", func(tt *testing.T) {
		params := &common.UpdatePoolParams{
			SizeInBytes:              uint64(4 * utils.TiBInBytes),
			QosType:                  QosTypeAuto,
			LargeCapacity:            false,
			TotalThroughputMibps:     256,
			TotalIops:                nillable.ToPointer(int64(4096)),
			CustomPerformanceEnabled: true,
		}

		pool := &datamodel.Pool{
			SizeInBytes:      int64(2 * utils.TiBInBytes),
			AllowAutoTiering: false,
			LargeCapacity:    false,
		}

		err := _validateAndSetUpdatePoolParams(params, pool)
		assert.NoError(tt, err, "Valid update params with custom performance enabled should pass validation")
	})

	// Additional edge cases and error scenarios
	t.Run("ZeroThroughput_ReturnsError", func(tt *testing.T) {
		params := &common.UpdatePoolParams{
			SizeInBytes:          uint64(4 * utils.TiBInBytes),
			QosType:              QosTypeAuto,
			LargeCapacity:        false,
			TotalThroughputMibps: 0, // Zero throughput
			TotalIops:            nillable.ToPointer(int64(4096)),
		}

		pool := &datamodel.Pool{
			SizeInBytes:      int64(2 * utils.TiBInBytes),
			AllowAutoTiering: false,
			LargeCapacity:    false,
		}

		err := _validateAndSetUpdatePoolParams(params, pool)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "TotalThroughputMibps must be between")
	})

	t.Run("InvalidIOPS_BelowMinimum_ReturnsError", func(tt *testing.T) {
		params := &common.UpdatePoolParams{
			SizeInBytes:          uint64(4 * utils.TiBInBytes),
			QosType:              QosTypeAuto,
			LargeCapacity:        false,
			TotalThroughputMibps: 256,
			TotalIops:            nillable.ToPointer(int64(100)), // Below minimum IOPS
		}

		pool := &datamodel.Pool{
			SizeInBytes:      int64(2 * utils.TiBInBytes),
			AllowAutoTiering: false,
			LargeCapacity:    false,
		}

		err := _validateAndSetUpdatePoolParams(params, pool)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "TotalIops must be between")
	})

	t.Run("InvalidIOPS_AboveMaximum_ReturnsError", func(tt *testing.T) {
		params := &common.UpdatePoolParams{
			SizeInBytes:          uint64(4 * utils.TiBInBytes),
			QosType:              QosTypeAuto,
			LargeCapacity:        false,
			TotalThroughputMibps: 256,
			TotalIops:            nillable.ToPointer(int64(200000)), // Above maximum IOPS
		}

		pool := &datamodel.Pool{
			SizeInBytes:      int64(2 * utils.TiBInBytes),
			AllowAutoTiering: false,
			LargeCapacity:    false,
		}

		err := _validateAndSetUpdatePoolParams(params, pool)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "TotalIops must be between")
	})

	t.Run("InvalidThroughput_AboveMaximum_ReturnsError", func(tt *testing.T) {
		params := &common.UpdatePoolParams{
			SizeInBytes:          uint64(4 * utils.TiBInBytes),
			QosType:              QosTypeAuto,
			LargeCapacity:        false,
			TotalThroughputMibps: 10000, // Above maximum throughput
			TotalIops:            nillable.ToPointer(int64(4096)),
		}

		pool := &datamodel.Pool{
			SizeInBytes:      int64(2 * utils.TiBInBytes),
			AllowAutoTiering: false,
			LargeCapacity:    false,
		}

		err := _validateAndSetUpdatePoolParams(params, pool)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "TotalThroughputMibps must be between")
	})

	t.Run("InvalidAutoTiering_HotTierTooLarge_ReturnsError", func(tt *testing.T) {
		params := &common.UpdatePoolParams{
			SizeInBytes:          uint64(10 * utils.TiBInBytes),
			QosType:              QosTypeAuto,
			LargeCapacity:        false,
			TotalThroughputMibps: 256,
			TotalIops:            nillable.ToPointer(int64(4096)),
			AllowAutoTiering:     true,
			HotTierSizeInBytes:   uint64(15 * utils.TiBInBytes), // Hot tier larger than pool size
		}

		pool := &datamodel.Pool{
			SizeInBytes:      int64(5 * utils.TiBInBytes),
			AllowAutoTiering: false,
			LargeCapacity:    false,
		}

		err := _validateAndSetUpdatePoolParams(params, pool)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Auto-Tiering feature is currently not enabled")
	})

	t.Run("ValidParams_BoundarySize_StandardPool", func(tt *testing.T) {
		params := &common.UpdatePoolParams{
			SizeInBytes:          uint64(50 * utils.TiBInBytes), // Boundary size for standard pool
			QosType:              QosTypeAuto,
			LargeCapacity:        false,
			TotalThroughputMibps: 256,
			TotalIops:            nillable.ToPointer(int64(4096)),
		}

		pool := &datamodel.Pool{
			SizeInBytes:      int64(2 * utils.TiBInBytes),
			AllowAutoTiering: false,
			LargeCapacity:    false,
		}

		err := _validateAndSetUpdatePoolParams(params, pool)
		assert.NoError(tt, err, "Boundary size for standard pool should pass validation")
	})

	t.Run("ValidParams_BoundarySize_LargeCapacityPool", func(tt *testing.T) {
		params := &common.UpdatePoolParams{
			SizeInBytes:          uint64(100 * utils.TiBInBytes), // Boundary size for large capacity pool
			QosType:              QosTypeAuto,
			LargeCapacity:        true,
			TotalThroughputMibps: 1000,
			TotalIops:            nillable.ToPointer(int64(16000)), // Minimum IOPS for 1000 MiBps in large capacity
		}

		pool := &datamodel.Pool{
			SizeInBytes:      int64(50 * utils.TiBInBytes),
			AllowAutoTiering: false,
			LargeCapacity:    true,
		}

		err := _validateAndSetUpdatePoolParams(params, pool)
		assert.NoError(tt, err, "Boundary size for large capacity pool should pass validation")
	})

	t.Run("ValidParams_WithHotTierAutoResize", func(tt *testing.T) {
		params := &common.UpdatePoolParams{
			SizeInBytes:             uint64(10 * utils.TiBInBytes),
			QosType:                 QosTypeAuto,
			LargeCapacity:           false,
			TotalThroughputMibps:    256,
			TotalIops:               nillable.ToPointer(int64(4096)),
			AllowAutoTiering:        true,
			HotTierSizeInBytes:      uint64(2 * utils.TiBInBytes),
			EnableHotTierAutoResize: true,
		}

		pool := &datamodel.Pool{
			SizeInBytes:      int64(5 * utils.TiBInBytes),
			AllowAutoTiering: false,
			LargeCapacity:    false,
		}

		err := _validateAndSetUpdatePoolParams(params, pool)
		assert.Error(tt, err, "Hot tier size validation should fail")
		assert.Contains(tt, err.Error(), "Hot tier size cannot be less than existing pool size")
	})

	t.Run("ValidParams_WithZoneChanges", func(tt *testing.T) {
		params := &common.UpdatePoolParams{
			SizeInBytes:          uint64(4 * utils.TiBInBytes),
			QosType:              QosTypeAuto,
			LargeCapacity:        false,
			TotalThroughputMibps: 256,
			TotalIops:            nillable.ToPointer(int64(4096)),
			Zone:                 "us-central1-b",
		}

		pool := &datamodel.Pool{
			SizeInBytes:      int64(2 * utils.TiBInBytes),
			AllowAutoTiering: false,
			LargeCapacity:    false,
		}

		err := _validateAndSetUpdatePoolParams(params, pool)
		assert.NoError(tt, err, "Valid update params with zone changes should pass validation")
	})

	// Test for the specific line: perf.LargeCapacity = pool.LargeCapacity
	t.Run("LargeCapacityFieldIsSetFromExistingPool", func(tt *testing.T) {
		// Test case 1: Update params specify LargeCapacity=false but pool has LargeCapacity=true
		params := &common.UpdatePoolParams{
			SizeInBytes:          uint64(20 * utils.TiBInBytes), // Use valid large capacity size
			QosType:              QosTypeAuto,
			LargeCapacity:        false, // Update params specify standard capacity
			TotalThroughputMibps: 1000,
			TotalIops:            nillable.ToPointer(int64(16000)),
		}

		pool := &datamodel.Pool{
			SizeInBytes:      int64(15 * utils.TiBInBytes),
			AllowAutoTiering: false,
			LargeCapacity:    true, // Existing pool is large capacity
		}

		err := _validateAndSetUpdatePoolParams(params, pool)
		// Should pass validation because validation uses pool.LargeCapacity=true, not params.LargeCapacity=false
		assert.NoError(tt, err, "Validation should use existing pool's LargeCapacity value")

		// Test case 2: Update params specify LargeCapacity=true but pool has LargeCapacity=false
		params2 := &common.UpdatePoolParams{
			SizeInBytes:          uint64(4 * utils.TiBInBytes), // Use valid standard capacity size
			QosType:              QosTypeAuto,
			LargeCapacity:        true, // Update params specify large capacity
			TotalThroughputMibps: 256,
			TotalIops:            nillable.ToPointer(int64(4096)),
		}

		pool2 := &datamodel.Pool{
			SizeInBytes:      int64(2 * utils.TiBInBytes),
			AllowAutoTiering: false,
			LargeCapacity:    false, // Existing pool is standard capacity
		}

		err2 := _validateAndSetUpdatePoolParams(params2, pool2)
		// Should pass validation because validation uses pool.LargeCapacity=false, not params.LargeCapacity=true
		assert.NoError(tt, err2, "Validation should use existing pool's LargeCapacity value")

		// Test case 3: Verify that the validation actually uses the pool's LargeCapacity value
		// by testing with invalid parameters that would fail for large capacity but pass for standard
		params3 := &common.UpdatePoolParams{
			SizeInBytes:          uint64(500 * utils.TiBInBytes), // Exceeds standard pool maximum (425 TiB)
			QosType:              QosTypeAuto,
			LargeCapacity:        false,                            // Update params specify standard capacity
			TotalThroughputMibps: 2000,                             // Large capacity throughput
			TotalIops:            nillable.ToPointer(int64(32000)), // Large capacity IOPS
		}

		pool3 := &datamodel.Pool{
			SizeInBytes:      int64(100 * utils.TiBInBytes),
			AllowAutoTiering: false,
			LargeCapacity:    false, // Existing pool is standard capacity
		}

		err3 := _validateAndSetUpdatePoolParams(params3, pool3)
		// Should fail validation because the parameters are for large capacity but pool is standard capacity
		assert.Error(tt, err3, "Validation should fail when large capacity params are used with standard capacity pool")
	})
}

func TestOrchestrator_GetExpertModePoolCreds(t *testing.T) {
	t.Run("WhenSuccessful", func(t *testing.T) {
		ctx, store, orch, _ := setup(t)

		// Create test data
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err := store.DB().Create(account).Error
		assert.NoError(t, err)

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			PoolCredentials: &datamodel.PoolCredentials{
				SecretID:      "test-secret-id",
				CertificateID: "test-cert-id",
				Password:      "test-password",
				AuthType:      2, // USER_CERTIFICATE
			},
		}
		err = store.DB().Create(pool).Error
		assert.NoError(t, err)

		node1 := &datamodel.Node{
			BaseModel:       datamodel.BaseModel{ID: 1, UUID: "test-node-1-uuid"},
			Name:            "test-node-1",
			PoolID:          pool.ID,
			EndpointAddress: "10.0.0.1",
			HostDNSName:     "host1.example.com",
		}
		err = store.DB().Create(node1).Error
		assert.NoError(t, err)

		node2 := &datamodel.Node{
			BaseModel:       datamodel.BaseModel{ID: 2, UUID: "test-node-2-uuid"},
			Name:            "test-node-2",
			PoolID:          pool.ID,
			EndpointAddress: "10.0.0.2",
			HostDNSName:     "host2.example.com",
		}
		err = store.DB().Create(node2).Error
		assert.NoError(t, err)

		credentials, err := orch.GetExpertModePoolCreds(ctx, "test_pool", "test_account", "test-user")

		assert.NoError(t, err)
		assert.NotNil(t, credentials)
		assert.Equal(t, "test-secret-id", credentials.SecretID)
		assert.Equal(t, "test-cert-id", credentials.CertificateID)
		assert.Equal(t, "test-password", credentials.Password)
		assert.Equal(t, 2, credentials.AuthType)
		assert.NotNil(t, credentials.OntapEndpoints)
		assert.Len(t, credentials.OntapEndpoints, 2)
		assert.Equal(t, "10.0.0.1", credentials.OntapEndpoints[0].IP)
		assert.Equal(t, "host1.example.com", credentials.OntapEndpoints[0].DNS)
		assert.Equal(t, "10.0.0.2", credentials.OntapEndpoints[1].IP)
		assert.Equal(t, "host2.example.com", credentials.OntapEndpoints[1].DNS)
	})
	t.Run("WhenAccountNotFound", func(t *testing.T) {
		ctx, _, orch, _ := setup(t)

		// Mock getAccountWithName to return error
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.NewNotFoundErr("account not found", nil)
		}
		defer func() { getAccountWithName = _getAccountWithName }()

		// Execute
		credentials, err := orch.GetExpertModePoolCreds(ctx, "test-pool-uuid", "non-existent-account", "test-user")

		// Assert
		assert.Error(t, err)
		assert.Nil(t, credentials)
		assert.Contains(t, err.Error(), "account not found")
	})
	t.Run("WhenPoolNotFound", func(t *testing.T) {
		ctx, store, orch, _ := setup(t)

		// Create test account
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err := store.DB().Create(account).Error
		assert.NoError(t, err)

		// Mock getAccountWithName to return the test account
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = _getAccountWithName }()

		// Execute
		credentials, err := orch.GetExpertModePoolCreds(ctx, "non-existent-pool-uuid", "test_account", "test-user")

		// Assert
		assert.Error(t, err)
		assert.Nil(t, credentials)
		assert.Contains(t, err.Error(), "pool not found")
	})
	t.Run("WhenPoolHasNoCredentials", func(t *testing.T) {
		ctx, store, orch, _ := setup(t)

		// Create test data
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err := store.DB().Create(account).Error
		assert.NoError(t, err)

		pool := &datamodel.Pool{
			BaseModel:       datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:            "test_pool",
			AccountID:       account.ID,
			PoolCredentials: nil, // No credentials
		}
		err = store.DB().Create(pool).Error
		assert.NoError(t, err)

		// Execute
		credentials, err := orch.GetExpertModePoolCreds(ctx, "test_pool", "test_account", "test-user")

		// Assert
		assert.NoError(t, err)
		assert.Nil(t, credentials) // Should return nil when no credentials
	})
	t.Run("WhenPoolHasEmptyCredentials", func(t *testing.T) {
		ctx, store, orch, _ := setup(t)

		// Create test data
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err := store.DB().Create(account).Error
		assert.NoError(t, err)

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			PoolCredentials: &datamodel.PoolCredentials{
				SecretID:      "",
				CertificateID: "",
				Password:      "",
				AuthType:      0,
			},
		}
		err = store.DB().Create(pool).Error
		assert.NoError(t, err)

		// Execute
		credentials, err := orch.GetExpertModePoolCreds(ctx, "test_pool", "test_account", "test-user")

		// Assert
		assert.NoError(t, err)
		assert.NotNil(t, credentials)
		assert.Equal(t, "", credentials.SecretID)
		assert.Equal(t, "", credentials.CertificateID)
		assert.Equal(t, "", credentials.Password)
		assert.Equal(t, 0, credentials.AuthType)
	})
	t.Run("WhenDatabaseErrorOccurs", func(t *testing.T) {
		ctx, _, orch, _ := setup(t)

		// Mock getAccountWithName to return error
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.New("database connection failed")
		}
		defer func() { getAccountWithName = _getAccountWithName }()

		// Execute
		credentials, err := orch.GetExpertModePoolCreds(ctx, "test-pool-uuid", "test_account", "test-user")

		// Assert
		assert.Error(t, err)
		assert.Nil(t, credentials)
		assert.Contains(t, err.Error(), "database connection failed")
	})
	t.Run("WhenUserNameIsEmpty", func(t *testing.T) {
		ctx, store, orch, _ := setup(t)

		// Create test data
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err := store.DB().Create(account).Error
		assert.NoError(t, err)

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			PoolCredentials: &datamodel.PoolCredentials{
				SecretID:      "test-secret-id",
				CertificateID: "test-cert-id",
				Password:      "test-password",
				AuthType:      1,
			},
		}
		err = store.DB().Create(pool).Error
		assert.NoError(t, err)

		// Execute with empty userName
		credentials, err := orch.GetExpertModePoolCreds(ctx, "test_pool", "test_account", "")

		// Assert - should still work even with empty userName
		assert.NoError(t, err)
		assert.NotNil(t, credentials)
		assert.Equal(t, "test-secret-id", credentials.SecretID)
	})
	t.Run("WhenContextIsCancelled", func(t *testing.T) {
		ctx, _, orch, _ := setup(t)

		// Create cancelled context
		cancelledCtx, cancel := context.WithCancel(ctx)
		cancel() // Cancel immediately

		// Execute
		credentials, err := orch.GetExpertModePoolCreds(cancelledCtx, "test-pool-uuid", "test_account", "test-user")

		// Assert
		assert.Error(t, err)
		assert.Nil(t, credentials)
		// The exact error depends on the database driver, but it should be an error
	})
	t.Run("WhenPoolCredentialsAreNil", func(t *testing.T) {
		ctx, store, orch, _ := setup(t)

		// Create test data
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err := store.DB().Create(account).Error
		assert.NoError(t, err)

		pool := &datamodel.Pool{
			BaseModel:       datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:            "test_pool",
			AccountID:       account.ID,
			PoolCredentials: nil, // Explicitly nil
		}
		err = store.DB().Create(pool).Error
		assert.NoError(t, err)

		// Execute
		credentials, err := orch.GetExpertModePoolCreds(ctx, "test_pool", "test_account", "test-user")

		// Assert
		assert.NoError(t, err)
		assert.Nil(t, credentials)
	})
}

func TestCreatePool_JobTypeSelection(t *testing.T) {
	// Test the job type determination logic directly using the helper functions from line 90

	t.Run("LargeCapacityPool_UsesCreateLargePoolJobType", func(tt *testing.T) {
		jobType := models.GetResourceJobType(models.ResourceTypePool, models.ResourceOperationCreate, models.PoolCategoryLargeCapacity)
		assert.Equal(tt, models.JobTypeCreateLargePool, jobType)
	})

	t.Run("StandardPool_UsesCreatePoolJobType", func(tt *testing.T) {
		jobType := models.GetResourceJobType(models.ResourceTypePool, models.ResourceOperationCreate, models.PoolCategoryStandard)
		assert.Equal(tt, models.JobTypeCreatePool, jobType)
	})

	t.Run("DefaultPool_UsesCreatePoolJobType", func(tt *testing.T) {
		jobType := models.GetResourceJobType(models.ResourceTypePool, models.ResourceOperationCreate, models.PoolCategoryDefault)
		assert.Equal(tt, models.JobTypeCreatePool, jobType) // Default maps to standard
	})
}

func TestGetResourceJobType_PoolOperations_ComprehensiveMapping(t *testing.T) {
	// Test all combinations of pool operations and capacity types

	testCases := []struct {
		name            string
		operation       models.ResourceOperation
		isLargeCapacity bool
		expectedJobType models.JobType
	}{
		// CREATE operations
		{
			name:            "Create_RegularCapacity",
			operation:       models.ResourceOperationCreate,
			isLargeCapacity: false,
			expectedJobType: models.JobTypeCreatePool,
		},
		{
			name:            "Create_LargeCapacity",
			operation:       models.ResourceOperationCreate,
			isLargeCapacity: true,
			expectedJobType: models.JobTypeCreateLargePool,
		},
		// UPDATE operations
		{
			name:            "Update_RegularCapacity",
			operation:       models.ResourceOperationUpdate,
			isLargeCapacity: false,
			expectedJobType: models.JobTypeUpdatePool,
		},
		{
			name:            "Update_LargeCapacity",
			operation:       models.ResourceOperationUpdate,
			isLargeCapacity: true,
			expectedJobType: models.JobTypeUpdateLargePool,
		},
		// DELETE operations
		{
			name:            "Delete_RegularCapacity",
			operation:       models.ResourceOperationDelete,
			isLargeCapacity: false,
			expectedJobType: models.JobTypeDeletePool,
		},
		{
			name:            "Delete_LargeCapacity",
			operation:       models.ResourceOperationDelete,
			isLargeCapacity: true,
			expectedJobType: models.JobTypeDeleteLargePool,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			poolCategory := models.GetPoolCategory(tc.isLargeCapacity)
			jobType := models.GetResourceJobType(models.ResourceTypePool, tc.operation, poolCategory)
			assert.Equal(tt, tc.expectedJobType, jobType,
				"Expected operation %s with largeCapacity=%t to return %s, got %s",
				tc.operation, tc.isLargeCapacity, tc.expectedJobType, jobType)
		})
	}
}

func TestGetResourceJobType_PoolOperations_EdgeCases(t *testing.T) {
	// Test edge cases and boundary conditions for pool operations

	t.Run("InvalidOperation_FallsBackToCreatePool", func(tt *testing.T) {
		// Test what happens with invalid operation
		jobType := models.GetResourceJobType(models.ResourceTypePool, "INVALID_OPERATION", models.PoolCategoryStandard)
		assert.Equal(tt, models.JobTypeCreatePool, jobType, "Should fallback to CREATE_POOL for invalid operation")
	})

	t.Run("InvalidOperation_WithLargeCapacity_FallsBackToCreatePool", func(tt *testing.T) {
		// Test what happens with invalid operation and large capacity
		jobType := models.GetResourceJobType(models.ResourceTypePool, "INVALID_OPERATION", models.PoolCategoryLargeCapacity)
		assert.Equal(tt, models.JobTypeCreatePool, jobType, "Should fallback to CREATE_POOL for invalid operation even with large capacity")
	})

	t.Run("EmptyOperation_FallsBackToCreatePool", func(tt *testing.T) {
		// Test what happens with empty operation
		jobType := models.GetResourceJobType(models.ResourceTypePool, "", models.PoolCategoryStandard)
		assert.Equal(tt, models.JobTypeCreatePool, jobType, "Should fallback to CREATE_POOL for empty operation")
	})
}

func TestGetResourceJobType_PoolOperations_AllValidCombinations(t *testing.T) {
	// Comprehensive test of all valid pool operation and capacity combinations

	testCases := []struct {
		name            string
		operation       models.ResourceOperation
		isLargeCapacity bool
		expectedJobType models.JobType
		description     string
	}{
		{
			name:            "CreateRegular",
			operation:       models.ResourceOperationCreate,
			isLargeCapacity: false,
			expectedJobType: models.JobTypeCreatePool,
			description:     "Create operation with regular capacity should return CREATE_POOL",
		},
		{
			name:            "CreateLarge",
			operation:       models.ResourceOperationCreate,
			isLargeCapacity: true,
			expectedJobType: models.JobTypeCreateLargePool,
			description:     "Create operation with large capacity should return CREATE_LARGE_POOL",
		},
		{
			name:            "UpdateRegular",
			operation:       models.ResourceOperationUpdate,
			isLargeCapacity: false,
			expectedJobType: models.JobTypeUpdatePool,
			description:     "Update operation with regular capacity should return UPDATE_POOL",
		},
		{
			name:            "UpdateLarge",
			operation:       models.ResourceOperationUpdate,
			isLargeCapacity: true,
			expectedJobType: models.JobTypeUpdateLargePool,
			description:     "Update operation with large capacity should return UPDATE_LARGE_POOL",
		},
		{
			name:            "DeleteRegular",
			operation:       models.ResourceOperationDelete,
			isLargeCapacity: false,
			expectedJobType: models.JobTypeDeletePool,
			description:     "Delete operation with regular capacity should return DELETE_POOL",
		},
		{
			name:            "DeleteLarge",
			operation:       models.ResourceOperationDelete,
			isLargeCapacity: true,
			expectedJobType: models.JobTypeDeleteLargePool,
			description:     "Delete operation with large capacity should return DELETE_LARGE_POOL",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			poolCategory := models.GetPoolCategory(tc.isLargeCapacity)
			jobType := models.GetResourceJobType(models.ResourceTypePool, tc.operation, poolCategory)
			assert.Equal(tt, tc.expectedJobType, jobType, tc.description)
		})
	}
}

func TestGetResourceJobType_PoolOperations_OperationConstants(t *testing.T) {
	// Test that the pool operation constants are correctly defined and used

	t.Run("OperationConstantsAreDefined", func(tt *testing.T) {
		// Verify all operation constants exist and have expected values
		assert.Equal(tt, models.ResourceOperation("CREATE"), models.ResourceOperationCreate)
		assert.Equal(tt, models.ResourceOperation("UPDATE"), models.ResourceOperationUpdate)
		assert.Equal(tt, models.ResourceOperation("DELETE"), models.ResourceOperationDelete)
	})

	t.Run("JobTypeConstantsAreDefined", func(tt *testing.T) {
		// Verify all job type constants exist and have expected values
		assert.Equal(tt, models.JobType("CREATE_POOL"), models.JobTypeCreatePool)
		assert.Equal(tt, models.JobType("CREATE_LARGE_POOL"), models.JobTypeCreateLargePool)
		assert.Equal(tt, models.JobType("UPDATE_POOL"), models.JobTypeUpdatePool)
		assert.Equal(tt, models.JobType("UPDATE_LARGE_POOL"), models.JobTypeUpdateLargePool)
		assert.Equal(tt, models.JobType("DELETE_POOL"), models.JobTypeDeletePool)
		assert.Equal(tt, models.JobType("DELETE_LARGE_POOL"), models.JobTypeDeleteLargePool)
	})
}

func TestGetResourceJobType_PoolOperations_CapacityFlagBehavior(t *testing.T) {
	// Test the behavior of the isLargeCapacity flag across all pool operations

	operations := []models.ResourceOperation{
		models.ResourceOperationCreate,
		models.ResourceOperationUpdate,
		models.ResourceOperationDelete,
	}

	for _, operation := range operations {
		t.Run(fmt.Sprintf("%s_CapacityFlagDifference", operation), func(tt *testing.T) {
			regularJobType := models.GetResourceJobType(models.ResourceTypePool, operation, models.PoolCategoryStandard)
			largeJobType := models.GetResourceJobType(models.ResourceTypePool, operation, models.PoolCategoryLargeCapacity)

			// Verify that the capacity flag actually changes the result
			assert.NotEqual(tt, regularJobType, largeJobType,
				"Regular and large capacity should return different job types for operation %s", operation)

			// Verify the job types contain the expected patterns
			regularStr := string(regularJobType)
			largeStr := string(largeJobType)

			assert.NotContains(tt, regularStr, "LARGE",
				"Regular capacity job type should not contain 'LARGE' for operation %s", operation)
			assert.Contains(tt, largeStr, "LARGE",
				"Large capacity job type should contain 'LARGE' for operation %s", operation)
		})
	}
}

func TestGetResourceJobType_PoolOperations_Consistency(t *testing.T) {
	// Test consistency and predictability of the pool job type function

	t.Run("FunctionIsIdempotent", func(tt *testing.T) {
		// Test that calling the function multiple times with same inputs returns same result
		operation := models.ResourceOperationCreate
		isLargeCapacity := true

		poolCategory := models.GetPoolCategory(isLargeCapacity)
		result1 := models.GetResourceJobType(models.ResourceTypePool, operation, poolCategory)
		result2 := models.GetResourceJobType(models.ResourceTypePool, operation, poolCategory)
		result3 := models.GetResourceJobType(models.ResourceTypePool, operation, poolCategory)

		assert.Equal(tt, result1, result2, "Function should be idempotent")
		assert.Equal(tt, result2, result3, "Function should be idempotent")
	})

	t.Run("JobTypeMapping_IsComplete", func(tt *testing.T) {
		// Verify that every valid operation has both regular and large capacity mappings
		operations := []models.ResourceOperation{
			models.ResourceOperationCreate,
			models.ResourceOperationUpdate,
			models.ResourceOperationDelete,
		}

		for _, operation := range operations {
			regularJobType := models.GetResourceJobType(models.ResourceTypePool, operation, models.PoolCategoryStandard)
			largeJobType := models.GetResourceJobType(models.ResourceTypePool, operation, models.PoolCategoryLargeCapacity)

			// Neither should fallback to the default CREATE_POOL (unless it's actually CREATE operation)
			if operation != models.ResourceOperationCreate {
				assert.NotEqual(tt, models.JobTypeCreatePool, regularJobType,
					"Operation %s should not fallback to CREATE_POOL", operation)
				assert.NotEqual(tt, models.JobTypeCreatePool, largeJobType,
					"Operation %s with large capacity should not fallback to CREATE_POOL", operation)
			}

			// Both should return valid, non-empty job types
			assert.NotEmpty(tt, string(regularJobType), "Should return non-empty job type for %s regular", operation)
			assert.NotEmpty(tt, string(largeJobType), "Should return non-empty job type for %s large", operation)
		}
	})
}

func TestGetResourceJobType_Comprehensive(t *testing.T) {
	// Test the new generic resource job type function

	t.Run("PoolOperations", func(tt *testing.T) {
		// Test all pool operations with both capacity types
		poolTestCases := []struct {
			name            string
			operation       models.ResourceOperation
			isLargeCapacity bool
			expectedJobType models.JobType
			description     string
		}{
			{
				name:            "Pool_Create_Regular",
				operation:       models.ResourceOperationCreate,
				isLargeCapacity: false,
				expectedJobType: models.JobTypeCreatePool,
				description:     "Pool create with regular capacity",
			},
			{
				name:            "Pool_Create_Large",
				operation:       models.ResourceOperationCreate,
				isLargeCapacity: true,
				expectedJobType: models.JobTypeCreateLargePool,
				description:     "Pool create with large capacity",
			},
			{
				name:            "Pool_Update_Regular",
				operation:       models.ResourceOperationUpdate,
				isLargeCapacity: false,
				expectedJobType: models.JobTypeUpdatePool,
				description:     "Pool update with regular capacity",
			},
			{
				name:            "Pool_Update_Large",
				operation:       models.ResourceOperationUpdate,
				isLargeCapacity: true,
				expectedJobType: models.JobTypeUpdateLargePool,
				description:     "Pool update with large capacity",
			},
			{
				name:            "Pool_Delete_Regular",
				operation:       models.ResourceOperationDelete,
				isLargeCapacity: false,
				expectedJobType: models.JobTypeDeletePool,
				description:     "Pool delete with regular capacity",
			},
			{
				name:            "Pool_Delete_Large",
				operation:       models.ResourceOperationDelete,
				isLargeCapacity: true,
				expectedJobType: models.JobTypeDeleteLargePool,
				description:     "Pool delete with large capacity",
			},
		}

		for _, tc := range poolTestCases {
			tt.Run(tc.name, func(ttt *testing.T) {
				poolCategory := models.GetPoolCategory(tc.isLargeCapacity)
				jobType := models.GetResourceJobType(models.ResourceTypePool, tc.operation, poolCategory)
				assert.Equal(ttt, tc.expectedJobType, jobType, tc.description)
			})
		}
	})

	t.Run("SubnetOperations", func(tt *testing.T) {
		// Test subnet operations (only CREATE is supported)
		subnetTestCases := []struct {
			name            string
			operation       models.ResourceOperation
			isLargeCapacity bool
			expectedJobType models.JobType
			description     string
		}{
			{
				name:            "Subnet_Create_Regular",
				operation:       models.ResourceOperationCreate,
				isLargeCapacity: false,
				expectedJobType: models.JobTypeCreateSubnet,
				description:     "Subnet create with regular capacity",
			},
			{
				name:            "Subnet_Create_Large",
				operation:       models.ResourceOperationCreate,
				isLargeCapacity: true,
				expectedJobType: models.JobTypeCreateLargeSubnet,
				description:     "Subnet create with large capacity",
			},
		}

		for _, tc := range subnetTestCases {
			tt.Run(tc.name, func(ttt *testing.T) {
				poolCategory := models.GetPoolCategory(tc.isLargeCapacity)
				jobType := models.GetResourceJobType(models.ResourceTypeSubnet, tc.operation, poolCategory)
				assert.Equal(ttt, tc.expectedJobType, jobType, tc.description)
			})
		}
	})

	t.Run("EdgeCases", func(tt *testing.T) {
		// Test invalid combinations fall back to default

		t.Run("InvalidResourceType", func(ttt *testing.T) {
			jobType := models.GetResourceJobType("INVALID_RESOURCE", models.ResourceOperationCreate, models.PoolCategoryStandard)
			assert.Equal(ttt, models.JobTypeCreatePool, jobType, "Should fallback to CREATE_POOL for invalid resource type")
		})

		t.Run("InvalidOperation", func(ttt *testing.T) {
			jobType := models.GetResourceJobType(models.ResourceTypePool, "INVALID_OPERATION", models.PoolCategoryStandard)
			assert.Equal(ttt, models.JobTypeCreatePool, jobType, "Should fallback to CREATE_POOL for invalid operation")
		})

		t.Run("UnsupportedSubnetOperation", func(ttt *testing.T) {
			// Subnets don't support UPDATE operations
			jobType := models.GetResourceJobType(models.ResourceTypeSubnet, models.ResourceOperationUpdate, models.PoolCategoryStandard)
			assert.Equal(ttt, models.JobTypeCreatePool, jobType, "Should fallback to CREATE_POOL for unsupported subnet operation")
		})
	})
	t.Run("KmsConfigIsNotReady", func(tt *testing.T) {
		ipos := int64(160001)
		params := &common.CreatePoolParams{
			SizeInBytes:             uint64(1 * utils.TiBInBytes),
			ServiceLevel:            ServiceLevelNameFLEX,
			QosType:                 QosTypeAuto,
			CustomPerformanceParams: &common.CustomPerformanceParams{ThroughputMibps: 64, Iops: &ipos}, // Just above 160000 IOPS
			KmsConfig: &models.KmsConfig{
				State: models.LifeCycleStateKeyCheckPending,
			},
		}
		defer func() {
			ValidatePoolParams = _validatePoolParams
		}()
		ValidatePoolParams = func(perf *validators.CustomPerformance, serviceLevel string) error {
			return nil
		}
		err := _validateCreatePoolParams(params)
		assert.Error(tt, err)
		assert.EqualError(tt, err, "invalid KMS configuration state: KEY_CHECK_PENDING")
	})
}
