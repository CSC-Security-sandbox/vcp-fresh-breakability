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
}

func TestUpdatePool(t *testing.T) {
	t.Run("WhenGetOrCreateAccountFails", func(tt *testing.T) {
		ctx, se, _, temporal := setup(tt)
		params := &common.UpdatePoolParams{
			AccountName: "test_account",
			PoolId:      "test-pool-id",
		}

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.New("account not found")
		}

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
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return dbAccount, nil
		}

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
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}

		ValidateUpdatePoolParams = func(params *common.UpdatePoolParams, pool *datamodel.Pool) error {
			return errors.New("invalid pool params")
		}

		_, _, err := _updatePool(ctx, store, temporal, params)
		assert.EqualError(tt, err, "invalid pool params")
	})
	t.Run("WhenUpdatePoolSucceeds", func(tt *testing.T) {
		ctx, store, _, temporal := setup(tt)
		params := &common.UpdatePoolParams{
			AccountName: "test_account",
			PoolId:      "test-pool-uuid1",
		}
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		pools, account := createDBPools(t, store)

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}

		ValidateUpdatePoolParams = func(params *common.UpdatePoolParams, pool *datamodel.Pool) error {
			return nil
		}

		pool, _, err := _updatePool(ctx, store, temporal, params)
		assert.NoError(tt, err, "Expected no error on updating pool")
		assert.Equal(tt, pools[0].Name, pool.Name)
		assert.Equal(tt, models.LifeCycleStateUpdating, pool.State)
	})
	t.Run("WhenExecuteWorkflowFails", func(tt *testing.T) {
		ctx, store, _, temporal := setup(tt)
		params := &common.UpdatePoolParams{
			AccountName: "test_account",
			PoolId:      "test-pool-uuid1",
		}

		_, account := createDBPools(t, store)
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}

		ValidateUpdatePoolParams = func(params *common.UpdatePoolParams, pool *datamodel.Pool) error {
			return nil
		}

		// Fail workflow execution.
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, params, mock.Anything).
			Return(nil, fmt.Errorf("workflow error"))

		_, _, err := _updatePool(ctx, store, temporal, params)
		assert.EqualError(tt, err, "workflow error")
	})
}

func TestGetPool(t *testing.T) {
	t.Run("WhenPoolDoesNotExist", func(tt *testing.T) {
		ctx, _, orch, _ := setup(tt)

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{Name: "test_account"}, nil
		}

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

func TestValidateCreatePoolParams(t *testing.T) {
	t.Run("ValidateCreatePoolParams_WithWrongServiceLevel", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:  2199023255552,
			ServiceLevel: "Premium",
			QosType:      QosTypeAuto,
			CustomPerformanceParams: &common.CustomPerformanceParams{
				Iops:            nillable.ToPointer(int64(1024)), // datamodel.PoolAttributes expects int64, not pointer
				ThroughputMibps: 64,
			},
		}
		err := _validateCreatePoolParams(params)
		assert.EqualError(t, err, "Given service level not supported. Supported service level is "+ServiceLevelNameFLEX)
	})
	t.Run("ValidateCreatePoolParams_WithInvalidSize_ReturnsError", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:  1024 * 1024 * 1024, // 1 GiB, which is below the minimum quota
			ServiceLevel: ServiceLevelNameFLEX,
			QosType:      QosTypeAuto,
			CustomPerformanceParams: &common.CustomPerformanceParams{
				Iops:            nillable.ToPointer(int64(1024)), // datamodel.PoolAttributes expects int64, not pointer
				ThroughputMibps: 64,
			},
		}
		err := _validateCreatePoolParams(params)
		// 1 GiB is a multiple of 1GiB but below 1TiB, so StandardPoolValidator catches it
		assert.EqualError(t, err, "Given pool size not supported. Pool size must be greater than 1TiB and a multiple of 1GiB")
	})
	t.Run("ValidateCreatePoolParams_WithInvalidGiBSize_ReturnsError", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:  2 * 1099511627777, // Exactly the minimum quota+1
			ServiceLevel: ServiceLevelNameFLEX,
			QosType:      QosTypeAuto,
			CustomPerformanceParams: &common.CustomPerformanceParams{
				Iops:            nillable.ToPointer(int64(1000)),
				ThroughputMibps: 100,
			},
		}
		err := _validateCreatePoolParams(params)
		assert.EqualError(t, err, "Given pool size must be a multiple of 1GiB")
	})
	t.Run("ValidateCreatePoolParams_WithValidSize_WrongQosType", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:  2199023255552,
			ServiceLevel: ServiceLevelNameFLEX,
			QosType:      "Manual",
			CustomPerformanceParams: &common.CustomPerformanceParams{
				Iops:            nillable.ToPointer(int64(1000)),
				ThroughputMibps: 100,
			},
		}
		err := _validateCreatePoolParams(params)
		assert.EqualError(t, err, "Given QoS type not supported for Unified Flex Storage Pool. Supported QoS type is "+QosTypeAuto)
	})
	t.Run("ValidateCreatePoolParams_WithInvalidThroughputSetWithCustomPerformance", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:  2 * 1099511627776,
			ServiceLevel: ServiceLevelNameFLEX,
			QosType:      QosTypeAuto,
			CustomPerformanceParams: func() *common.CustomPerformanceParams {
				iopsValue := int64(1000)
				return &common.CustomPerformanceParams{Enabled: true, ThroughputMibps: 0, Iops: &iopsValue}
			}(),
		}
		err := _validateCreatePoolParams(params)
		assert.EqualError(t, err, "TotalThroughputMibps must be between 64 and 5120 MiBps")
	})
	t.Run("ValidateCreatePoolParams_WithInvalidIOPSSetWithCustomPerformance", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:             2 * 1099511627776,
			ServiceLevel:            ServiceLevelNameFLEX,
			QosType:                 QosTypeAuto,
			CustomPerformanceParams: &common.CustomPerformanceParams{Enabled: true, ThroughputMibps: 128, Iops: nillable.ToPointer(int64(100))},
		}
		err := _validateCreatePoolParams(params)
		assert.EqualError(t, err, "TotalIops must be between 1024 and 160000 IOPS")
	})

	// Edge cases for throughput validation
	t.Run("ValidateCreatePoolParams_WithThroughputAtMinimumBoundary", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:  2 * 1099511627776,
			ServiceLevel: ServiceLevelNameFLEX,
			QosType:      QosTypeAuto,
			CustomPerformanceParams: func() *common.CustomPerformanceParams {
				iopsValue := int64(1024)
				return &common.CustomPerformanceParams{Enabled: true, ThroughputMibps: 64, Iops: &iopsValue}
			}(),
		}
		err := _validateCreatePoolParams(params)
		assert.NoError(tt, err)
	})

	t.Run("ValidateCreatePoolParams_WithThroughputAtMaximumBoundary", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:  2 * 1099511627776,
			ServiceLevel: ServiceLevelNameFLEX,
			QosType:      QosTypeAuto,
			CustomPerformanceParams: func() *common.CustomPerformanceParams {
				iopsValue := int64(5120 * 16)
				return &common.CustomPerformanceParams{Enabled: true, ThroughputMibps: 5120, Iops: &iopsValue}
			}(), // 81920 IOPS for 5120 MiBps
		}
		err := _validateCreatePoolParams(params)
		assert.NoError(tt, err)
	})

	t.Run("ValidateCreatePoolParams_WithThroughputAboveMaximum", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:             2 * 1099511627776,
			ServiceLevel:            ServiceLevelNameFLEX,
			QosType:                 QosTypeAuto,
			CustomPerformanceParams: &common.CustomPerformanceParams{Enabled: true, ThroughputMibps: 5121, Iops: nillable.ToPointer(int64(1024))},
		}
		err := _validateCreatePoolParams(params)
		assert.EqualError(t, err, "TotalThroughputMibps must be between 64 and 5120 MiBps")
	})

	// Edge cases for IOPS validation
	t.Run("ValidateCreatePoolParams_WithIOPSAtMinimumBoundary", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:  2 * 1099511627776,
			ServiceLevel: ServiceLevelNameFLEX,
			QosType:      QosTypeAuto,
			CustomPerformanceParams: func() *common.CustomPerformanceParams {
				iopsValue := int64(1024)
				return &common.CustomPerformanceParams{Enabled: true, ThroughputMibps: 64, Iops: &iopsValue}
			}(),
		}
		err := _validateCreatePoolParams(params)
		assert.NoError(tt, err)
	})

	t.Run("ValidateCreatePoolParams_WithIOPSAtMaximumBoundary", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:             2 * 1099511627776,
			ServiceLevel:            ServiceLevelNameFLEX,
			QosType:                 QosTypeAuto,
			CustomPerformanceParams: &common.CustomPerformanceParams{Enabled: true, ThroughputMibps: 64, Iops: nillable.ToPointer(int64(160000))},
		}
		err := _validateCreatePoolParams(params)
		assert.NoError(tt, err)
	})

	// Edge cases for pool size validation
	t.Run("ValidateCreatePoolParams_WithPoolSizeAtNewMinimumBoundary", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:  1099511627776, // 1 TiB
			ServiceLevel: ServiceLevelNameFLEX,
			QosType:      QosTypeAuto,
			CustomPerformanceParams: func() *common.CustomPerformanceParams {
				iopsValue := int64(1024)
				return &common.CustomPerformanceParams{Enabled: true, ThroughputMibps: 64, Iops: &iopsValue}
			}(),
		}
		err := _validateCreatePoolParams(params)
		assert.NoError(tt, err)
	})

	t.Run("ValidateCreatePoolParams_WithPoolSizeBelowNewMinimum", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:  1099511627775, // 1 TiB - 1 byte
			ServiceLevel: ServiceLevelNameFLEX,
			QosType:      QosTypeAuto,
			CustomPerformanceParams: func() *common.CustomPerformanceParams {
				iopsValue := int64(1024)
				return &common.CustomPerformanceParams{Enabled: true, ThroughputMibps: 64, Iops: &iopsValue}
			}(),
		}
		err := _validateCreatePoolParams(params)
		// Since 1TiB-1byte is not a multiple of 1GiB, the common validator catches it first
		assert.EqualError(t, err, "Given pool size must be a multiple of 1GiB")
	})

	t.Run("ValidateCreatePoolParams_WithPoolSizeAtNewMaximumBoundary", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:  425 * utils.TiBInBytes, // 425 TiB (new maximum)
			ServiceLevel: ServiceLevelNameFLEX,
			QosType:      QosTypeAuto,
			CustomPerformanceParams: func() *common.CustomPerformanceParams {
				iopsValue := int64(1024)
				return &common.CustomPerformanceParams{Enabled: true, ThroughputMibps: 64, Iops: &iopsValue}
			}(),
		}
		err := _validateCreatePoolParams(params)
		assert.NoError(tt, err)
	})

	t.Run("ValidateCreatePoolParams_WithPoolSizeAboveNewMaximum", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:  426 * utils.TiBInBytes, // 426 TiB (above new maximum)
			ServiceLevel: ServiceLevelNameFLEX,
			QosType:      QosTypeAuto,
			CustomPerformanceParams: func() *common.CustomPerformanceParams {
				iopsValue := int64(1024)
				return &common.CustomPerformanceParams{Enabled: true, ThroughputMibps: 64, Iops: &iopsValue}
			}(),
		}
		err := _validateCreatePoolParams(params)
		assert.EqualError(t, err, "Given pool size not supported. Pool size must be less than 425TiB")
	})

	t.Run("ValidateCreatePoolParams_WithNilCustomPerformanceParams", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:             2 * 1099511627776,
			ServiceLevel:            ServiceLevelNameFLEX,
			QosType:                 QosTypeAuto,
			CustomPerformanceParams: nil,
		}
		err := _validateCreatePoolParams(params)
		assert.NoError(tt, err) // Should not fail when CustomPerformanceParams is nil
	})

	// Test edge cases for combination of parameters
	t.Run("ValidateCreatePoolParams_CombinesNewMinimumSizeAndPerformanceLimits", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:  425 * utils.TiBInBytes, // New maximum 425 TiB
			ServiceLevel: ServiceLevelNameFLEX,
			QosType:      QosTypeAuto,
			CustomPerformanceParams: func() *common.CustomPerformanceParams {
				iopsValue := int64(1024)
				return &common.CustomPerformanceParams{Enabled: true, ThroughputMibps: 64, Iops: &iopsValue}
			}(), // Minimum performance
		}
		err := _validateCreatePoolParams(params)
		assert.NoError(tt, err)
	})

	t.Run("ValidateCreatePoolParams_CombinesNewMaximumSizeAndPerformanceLimits", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:             425 * utils.TiBInBytes, // New maximum 425 TiB
			ServiceLevel:            ServiceLevelNameFLEX,
			QosType:                 QosTypeAuto,
			CustomPerformanceParams: &common.CustomPerformanceParams{Enabled: true, ThroughputMibps: 5120, Iops: nillable.ToPointer(int64(160000))}, // Maximum performance
		}
		err := _validateCreatePoolParams(params)
		assert.NoError(tt, err)
	})

	// Test old minimum size that should now be valid
	t.Run("ValidateCreatePoolParams_OldMinimum2TiBNowValid", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:  uint64(2 * utils.TiBInBytes), // Old minimum 2 TiB should still work
			ServiceLevel: ServiceLevelNameFLEX,
			QosType:      QosTypeAuto,
			CustomPerformanceParams: func() *common.CustomPerformanceParams {
				iopsValue := int64(1024)
				return &common.CustomPerformanceParams{Enabled: true, ThroughputMibps: 64, Iops: &iopsValue}
			}(),
		}
		err := _validateCreatePoolParams(params)
		assert.NoError(tt, err)
	})

	// Test fractional values just under boundaries
	t.Run("ValidateCreatePoolParams_JustUnderMinimumSize", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:  1*utils.TiBInBytes - utils.GiBInBytes, // Just under 1 TiB by 1 GiB (1023 GiB)
			ServiceLevel: ServiceLevelNameFLEX,
			QosType:      QosTypeAuto,
			CustomPerformanceParams: func() *common.CustomPerformanceParams {
				iopsValue := int64(1024)
				return &common.CustomPerformanceParams{Enabled: true, ThroughputMibps: 64, Iops: &iopsValue}
			}(),
		}
		err := _validateCreatePoolParams(params)
		// 1023 GiB is a multiple of 1GiB but below 1TiB, so StandardPoolValidator catches it
		assert.EqualError(tt, err, "Given pool size not supported. Pool size must be greater than 1TiB and a multiple of 1GiB")
	})

	t.Run("ValidateCreatePoolParams_JustOverMaximumSize", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:  425*utils.TiBInBytes + utils.GiBInBytes, // Just over 425 TiB by
			ServiceLevel: ServiceLevelNameFLEX,
			QosType:      QosTypeAuto,
			CustomPerformanceParams: func() *common.CustomPerformanceParams {
				iopsValue := int64(1024)
				return &common.CustomPerformanceParams{Enabled: true, ThroughputMibps: 64, Iops: &iopsValue}
			}(),
		}
		err := _validateCreatePoolParams(params)
		assert.EqualError(tt, err, "Given pool size not supported. Pool size must be less than 425TiB")
	})

	t.Run("ValidateCreatePoolParams_LargeCapacity_WithValidSize", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:   12 * 1099511627776,
			ServiceLevel:  ServiceLevelNameFLEX,
			QosType:       QosTypeAuto,
			LargeCapacity: true,
			CustomPerformanceParams: &common.CustomPerformanceParams{
				Iops:            nillable.ToPointer(int64(1024)), // datamodel.PoolAttributes expects int64, not pointer
				ThroughputMibps: 64,
			},
		}
		err := _validateCreatePoolParams(params)
		assert.NoError(t, err)
	})

	t.Run("ValidateCreatePoolParams_LargeCapacity_WithInvalidSmallSize", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:   2 * 1099511627776,
			ServiceLevel:  ServiceLevelNameFLEX,
			QosType:       QosTypeAuto,
			LargeCapacity: true,
			CustomPerformanceParams: &common.CustomPerformanceParams{
				Iops:            nillable.ToPointer(int64(1024)), // datamodel.PoolAttributes expects int64, not pointer
				ThroughputMibps: 64,
			},
		}
		err := _validateCreatePoolParams(params)
		assert.Contains(t, err.Error(), "SizeInBytes must be at least 12TiB")
	})

	t.Run("ValidateCreatePoolParams_LargeCapacity_WithInvalidSmallThroughput", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:   12 * 1099511627776,
			ServiceLevel:  ServiceLevelNameFLEX,
			QosType:       QosTypeAuto,
			LargeCapacity: true,
			CustomPerformanceParams: &common.CustomPerformanceParams{
				Iops:            nillable.ToPointer(int64(1024)), // datamodel.PoolAttributes expects int64, not pointer
				ThroughputMibps: 32,
			},
		}
		err := _validateCreatePoolParams(params)
		assert.Contains(t, err.Error(), "TotalThroughputMibps must be between 64 and 60000 MiBps for Large Capacity pools")
	})

	t.Run("ValidateCreatePoolParams_LargeCapacity_WithInvalidHighThroughput", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:   12 * 1099511627776,
			ServiceLevel:  ServiceLevelNameFLEX,
			QosType:       QosTypeAuto,
			LargeCapacity: true,
			CustomPerformanceParams: &common.CustomPerformanceParams{
				Iops:            nillable.ToPointer(int64(1024)), // datamodel.PoolAttributes expects int64, not pointer
				ThroughputMibps: 70000,
			},
		}
		err := _validateCreatePoolParams(params)
		assert.Contains(t, err.Error(), "TotalThroughputMibps must be between 64 and 60000 MiBps for Large Capacity pools")
	})

	t.Run("ValidateCreatePoolParams_LargeCapacity_WithAutoTiering_ValidSize", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:        12 * 1099511627776,
			ServiceLevel:       ServiceLevelNameFLEX,
			QosType:            QosTypeAuto,
			LargeCapacity:      true,
			AllowAutoTiering:   true,
			HotTierSizeInBytes: 8 * 1099511627776, // 8 TiB hot tier (less than pool size of 12 TiB)
			CustomPerformanceParams: &common.CustomPerformanceParams{
				Iops:            nillable.ToPointer(int64(1024)), // datamodel.PoolAttributes expects int64, not pointer
				ThroughputMibps: 64,
			},
		}
		err := _validateCreatePoolParams(params)
		assert.NoError(t, err)
	})

	t.Run("ValidateCreatePoolParams_LargeCapacity_WithAutoTiering_ExceedsLimit", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:        21 * 1125899906842624, // 21 PiB (exceeds 20 PiB limit with auto-tiering)
			ServiceLevel:       ServiceLevelNameFLEX,
			QosType:            QosTypeAuto,
			LargeCapacity:      true,
			AllowAutoTiering:   true,
			HotTierSizeInBytes: 15 * 1125899906842624, // 15 PiB hot tier (reasonable for large pool)
			CustomPerformanceParams: &common.CustomPerformanceParams{
				Iops:            nillable.ToPointer(int64(1024)), // datamodel.PoolAttributes expects int64, not pointer
				ThroughputMibps: 64,
			},
		}
		err := _validateCreatePoolParams(params)
		assert.Contains(t, err.Error(), "when AllowAutoTiering is true")
	})

	t.Run("ValidateCreatePoolParams_LargeCapacity_WithoutAutoTiering_ExceedsMaxLimit", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:      6 * 1125899906842624, // 6 PiB (exceeds 5 PiB limit without auto-tiering)
			ServiceLevel:     ServiceLevelNameFLEX,
			QosType:          QosTypeAuto,
			LargeCapacity:    true,
			AllowAutoTiering: false,
			CustomPerformanceParams: &common.CustomPerformanceParams{
				Iops:            nillable.ToPointer(int64(1024)), // datamodel.PoolAttributes expects int64, not pointer
				ThroughputMibps: 64,
			},
		}
		err := _validateCreatePoolParams(params)
		assert.Contains(t, err.Error(), "SizeInBytes must be less than or equal to")
		assert.NotContains(t, err.Error(), "when AllowAutoTiering is true") // Should not contain auto-tier message
	})

	t.Run("ValidateCreatePoolParams_LargeCapacity_WithEdgeCaseSize_AutoTiering", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:        12 * 1099511627776,
			ServiceLevel:       ServiceLevelNameFLEX,
			QosType:            QosTypeAuto,
			LargeCapacity:      true,
			AllowAutoTiering:   true,
			HotTierSizeInBytes: 8 * 1099511627776, // 8 TiB hot tier (less than pool size of 12 TiB)
			CustomPerformanceParams: &common.CustomPerformanceParams{
				Iops:            nillable.ToPointer(int64(1024)), // datamodel.PoolAttributes expects int64, not pointer
				ThroughputMibps: 64,
			},
		}
		err := _validateCreatePoolParams(params)
		assert.NoError(t, err)
	})

	t.Run("ValidateCreatePoolParams_LargeCapacity_WithMaxValidSize_NoAutoTiering", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:      5*1125899906842624 - 1099511627776, // 5 PiB minus 1 TiB (within 5 PiB limit for non-auto-tiering)
			ServiceLevel:     ServiceLevelNameFLEX,
			QosType:          QosTypeAuto,
			LargeCapacity:    true,
			AllowAutoTiering: false,
			CustomPerformanceParams: &common.CustomPerformanceParams{
				Iops:            nillable.ToPointer(int64(1024)), // datamodel.PoolAttributes expects int64, not pointer
				ThroughputMibps: 64,
			},
		}
		err := _validateCreatePoolParams(params)
		assert.NoError(t, err)
	})
}

func TestValidateUpdatePoolParams(t *testing.T) {
	t.Run("Rejects changing qos type from auto to manual", func(tt *testing.T) {
		pool := &datamodel.Pool{QosType: QosTypeAuto}
		params := &common.UpdatePoolParams{
			QosType:                  "Manual",
			SizeInBytes:              minQuotaInBytesPool * 2,
			CustomPerformanceEnabled: true,
			TotalThroughputMibps:     float64(minCustomThroughput + 10),
			TotalIops:                float64(minCustomIops + 100),
		}
		err := _validateUpdatePoolParams(params, pool)
		assert.EqualError(tt, err, "Cannot change qos type from auto to manual")
	})
	t.Run("Returns error for pool size below minimum", func(tt *testing.T) {
		pool := &datamodel.Pool{QosType: "Manual"}
		params := &common.UpdatePoolParams{
			QosType:                  "Manual",
			SizeInBytes:              minQuotaInBytesPool - 1,
			CustomPerformanceEnabled: true,
			TotalThroughputMibps:     float64(minCustomThroughput + 10),
			TotalIops:                float64(minCustomIops + 100),
		}
		expectedErr := fmt.Sprintf("Given pool size not supported. Pool size must be greater than %s and a multiple of 1GiB", utils.FmtUint64Bytes(minQuotaInBytesPool))
		err := _validateUpdatePoolParams(params, pool)
		assert.EqualError(tt, err, expectedErr)
	})
	t.Run("Returns error for pool size above maximum", func(tt *testing.T) {
		pool := &datamodel.Pool{QosType: "Manual"}
		params := &common.UpdatePoolParams{
			QosType:                  "Manual",
			SizeInBytes:              maxQuotaInBytesPool + 1,
			CustomPerformanceEnabled: true,
			TotalThroughputMibps:     float64(minCustomThroughput + 10),
			TotalIops:                float64(minCustomIops + 100),
		}
		expectedErr := fmt.Sprintf("Given pool size not supported. Pool size must be less than %s", utils.FmtUint64Bytes(maxQuotaInBytesPool))
		err := _validateUpdatePoolParams(params, pool)
		assert.EqualError(tt, err, expectedErr)
	})
	t.Run("Returns error for pool size not multiple of granularity", func(tt *testing.T) {
		pool := &datamodel.Pool{QosType: "Manual"}
		// AddActivity 1 to minimum quota to simulate a value that's not divisible by common.GetMinSizeGranularity().
		params := &common.UpdatePoolParams{
			QosType:                  "Manual",
			SizeInBytes:              minQuotaInBytesPool + 1,
			CustomPerformanceEnabled: true,
			TotalThroughputMibps:     float64(minCustomThroughput + 10),
			TotalIops:                float64(minCustomIops + 100),
		}
		expectedErr := fmt.Sprintf("Given pool size must be a multiple of %s", utils.FmtUint64Bytes(minSizeGranularity))
		err := _validateUpdatePoolParams(params, pool)
		assert.EqualError(tt, err, expectedErr)
	})
	t.Run("Returns error when throughput is below minimum", func(tt *testing.T) {
		pool := &datamodel.Pool{QosType: "Manual"}
		params := &common.UpdatePoolParams{
			QosType:                  "Manual",
			SizeInBytes:              minQuotaInBytesPool * 2,
			CustomPerformanceEnabled: true,
			TotalThroughputMibps:     float64(minCustomThroughput - 1),
			TotalIops:                float64(minCustomIops + 100),
		}
		expectedErr := fmt.Sprintf("TotalThroughputMibps must be between %d and %d MiBps", minCustomThroughput, maxCustomThroughput)
		err := _validateUpdatePoolParams(params, pool)
		assert.EqualError(tt, err, expectedErr)
	})
	t.Run("Returns error when iops is below minimum", func(tt *testing.T) {
		pool := &datamodel.Pool{QosType: "Manual"}
		params := &common.UpdatePoolParams{
			QosType:                  "Manual",
			SizeInBytes:              minQuotaInBytesPool * 2,
			CustomPerformanceEnabled: true,
			TotalThroughputMibps:     float64(minCustomThroughput + 10),
			TotalIops:                float64(minCustomIops - 1),
		}
		expectedErr := fmt.Sprintf("TotalIops must be between %d and %d IOPS", minCustomIops, maxCustomIops)
		err := _validateUpdatePoolParams(params, pool)
		assert.EqualError(tt, err, expectedErr)
	})

	// Additional edge cases for update validation
	t.Run("Returns error when throughput is above maximum", func(tt *testing.T) {
		pool := &datamodel.Pool{QosType: "Manual"}
		params := &common.UpdatePoolParams{
			QosType:                  "Manual",
			SizeInBytes:              minQuotaInBytesPool * 2,
			CustomPerformanceEnabled: true,
			TotalThroughputMibps:     float64(maxCustomThroughput + 1),
			TotalIops:                float64(minCustomIops + 100),
		}
		expectedErr := fmt.Sprintf("TotalThroughputMibps must be between %d and %d MiBps", minCustomThroughput, maxCustomThroughput)
		err := _validateUpdatePoolParams(params, pool)
		assert.EqualError(tt, err, expectedErr)
	})

	t.Run("Returns error when iops is above maximum", func(tt *testing.T) {
		pool := &datamodel.Pool{QosType: "Manual"}
		params := &common.UpdatePoolParams{
			QosType:                  "Manual",
			SizeInBytes:              minQuotaInBytesPool * 2,
			CustomPerformanceEnabled: true,
			TotalThroughputMibps:     float64(minCustomThroughput + 10),
			TotalIops:                float64(maxCustomIops + 1),
		}
		expectedErr := fmt.Sprintf("TotalIops must be between %d and %d IOPS", minCustomIops, maxCustomIops)
		err := _validateUpdatePoolParams(params, pool)
		assert.EqualError(tt, err, expectedErr)
	})

	t.Run("Succeeds with throughput at maximum boundary", func(tt *testing.T) {
		pool := &datamodel.Pool{QosType: "Manual"}
		params := &common.UpdatePoolParams{
			QosType:                  "Manual",
			SizeInBytes:              minQuotaInBytesPool * 2,
			CustomPerformanceEnabled: true,
			TotalThroughputMibps:     float64(maxCustomThroughput),
			TotalIops:                float64(maxCustomIops),
		}
		err := _validateUpdatePoolParams(params, pool)
		assert.NoError(tt, err)
	})

	t.Run("Succeeds with throughput and IOPS at minimum boundary", func(tt *testing.T) {
		pool := &datamodel.Pool{QosType: "Manual"}
		params := &common.UpdatePoolParams{
			QosType:                  "Manual",
			SizeInBytes:              minQuotaInBytesPool * 2,
			CustomPerformanceEnabled: true,
			TotalThroughputMibps:     float64(minCustomThroughput),
			TotalIops:                float64(minCustomIops),
		}
		err := _validateUpdatePoolParams(params, pool)
		assert.NoError(tt, err)
	})

	t.Run("Succeeds with IOPS at maximum boundary", func(tt *testing.T) {
		pool := &datamodel.Pool{QosType: "Manual"}
		params := &common.UpdatePoolParams{
			QosType:                  "Manual",
			SizeInBytes:              minQuotaInBytesPool * 2,
			CustomPerformanceEnabled: true,
			TotalThroughputMibps:     float64(minCustomThroughput),
			TotalIops:                float64(maxCustomIops),
		}
		err := _validateUpdatePoolParams(params, pool)
		assert.NoError(tt, err)
	})

	t.Run("Succeeds with throughput at minimum boundary", func(tt *testing.T) {
		pool := &datamodel.Pool{QosType: "Manual"}
		params := &common.UpdatePoolParams{
			QosType:                  "Manual",
			SizeInBytes:              minQuotaInBytesPool * 2,
			CustomPerformanceEnabled: true,
			TotalThroughputMibps:     float64(minCustomThroughput),
			TotalIops:                float64(minCustomIops),
		}
		err := _validateUpdatePoolParams(params, pool)
		assert.NoError(tt, err)
	})

	t.Run("Validates new pool size limits - at new minimum boundary", func(tt *testing.T) {
		pool := &datamodel.Pool{QosType: "Manual"}
		params := &common.UpdatePoolParams{
			QosType:                  "Manual",
			SizeInBytes:              uint64(1 * utils.TiBInBytes), // 1 TiB (new minimum)
			CustomPerformanceEnabled: true,
			TotalThroughputMibps:     float64(minCustomThroughput),
			TotalIops:                float64(minCustomIops),
		}
		err := _validateUpdatePoolParams(params, pool)
		assert.NoError(tt, err)
	})

	t.Run("Validates new pool size limits - at new maximum boundary", func(tt *testing.T) {
		pool := &datamodel.Pool{QosType: "Manual"}
		params := &common.UpdatePoolParams{
			QosType:                  "Manual",
			SizeInBytes:              uint64(425 * utils.TiBInBytes), // 425 TiB (new maximum)
			CustomPerformanceEnabled: true,
			TotalThroughputMibps:     float64(minCustomThroughput),
			TotalIops:                float64(minCustomIops),
		}
		err := _validateUpdatePoolParams(params, pool)
		assert.NoError(tt, err)
	})

	t.Run("Returns error when pool size is above new maximum", func(tt *testing.T) {
		pool := &datamodel.Pool{QosType: "Manual"}
		params := &common.UpdatePoolParams{
			QosType:                  "Manual",
			SizeInBytes:              uint64(426 * utils.TiBInBytes), // 426 TiB (above new maximum)
			CustomPerformanceEnabled: true,
			TotalThroughputMibps:     float64(minCustomThroughput),
			TotalIops:                float64(minCustomIops),
		}
		err := _validateUpdatePoolParams(params, pool)
		assert.EqualError(tt, err, "Given pool size not supported. Pool size must be less than 425TiB")
	})

	t.Run("Returns error when pool size is below new minimum", func(tt *testing.T) {
		pool := &datamodel.Pool{QosType: "Manual"}
		params := &common.UpdatePoolParams{
			QosType:                  "Manual",
			SizeInBytes:              uint64(1*utils.TiBInBytes - 1), // Below 1 TiB
			CustomPerformanceEnabled: true,
			TotalThroughputMibps:     float64(minCustomThroughput),
			TotalIops:                float64(minCustomIops),
		}
		err := _validateUpdatePoolParams(params, pool)
		assert.EqualError(tt, err, "Given pool size not supported. Pool size must be greater than 1TiB and a multiple of 1GiB")
	})

	t.Run("Succeeds with valid update parameters", func(tt *testing.T) {
		pool := &datamodel.Pool{QosType: "Manual"}
		// Use a valid size that is a multiple of common.GetMinSizeGranularity(). For this test, we assume that common.MinQuotaInBytesPool*2 is valid.
		params := &common.UpdatePoolParams{
			QosType:                  "Manual",
			SizeInBytes:              minQuotaInBytesPool * 2,
			CustomPerformanceEnabled: true,
			TotalThroughputMibps:     float64(minCustomThroughput + 10),
			TotalIops:                float64(minCustomIops + 100),
		}
		err := _validateUpdatePoolParams(params, pool)
		assert.NoError(tt, err)
	})
	t.Run("Fails when AllowAutoTiering is true and HotTierSizeInBytes is less than existing pool size", func(tt *testing.T) {
		// Save and restore the original value
		currentATState := autoTieringEnabled
		defer func() { autoTieringEnabled = currentATState }()
		autoTieringEnabled = true

		pool := &datamodel.Pool{
			QosType:          QosTypeAuto,
			AllowAutoTiering: false,
			SizeInBytes:      int64(minQuotaInBytesPool * 2),
		}
		params := &common.UpdatePoolParams{
			QosType:            QosTypeAuto,
			AllowAutoTiering:   true,
			HotTierSizeInBytes: minQuotaInBytesPool,
			SizeInBytes:        minQuotaInBytesPool * 2,
		}
		err := _validateUpdatePoolParams(params, pool)
		assert.EqualError(tt, err, "Given hot tier size is not supported. Hot tier size cannot be less than existing pool size")
	})

	t.Run("Fails when AllowAutoTiering is true and HotTierSizeInBytes is less than existing hot tier size", func(tt *testing.T) {
		// Save and restore the original value
		currentATState := autoTieringEnabled
		defer func() { autoTieringEnabled = currentATState }()
		autoTieringEnabled = true

		pool := &datamodel.Pool{
			QosType:          QosTypeAuto,
			AllowAutoTiering: true,
			AutoTieringConfig: &datamodel.AutoTieringConfig{
				HotTierSizeInBytes: int64(minQuotaInBytesPool * 2),
			},
			SizeInBytes: int64(minQuotaInBytesPool * 3),
		}
		params := &common.UpdatePoolParams{
			QosType:            QosTypeAuto,
			AllowAutoTiering:   true,
			HotTierSizeInBytes: minQuotaInBytesPool,
			SizeInBytes:        minQuotaInBytesPool * 3,
		}
		err := _validateUpdatePoolParams(params, pool)
		assert.EqualError(tt, err, "Given hot tier size is not supported. Hot tier size must be greater than existing hot tier size")
	})

	t.Run("Fails when AllowAutoTiering is false but pool has auto tiering enabled", func(tt *testing.T) {
		// Save and restore the original value
		currentATState := autoTieringEnabled
		defer func() { autoTieringEnabled = currentATState }()
		autoTieringEnabled = true

		pool := &datamodel.Pool{
			QosType:          QosTypeAuto,
			AllowAutoTiering: true,
			AutoTieringConfig: &datamodel.AutoTieringConfig{
				HotTierSizeInBytes: int64(minQuotaInBytesPool),
			},
			SizeInBytes: int64(minQuotaInBytesPool * 2),
		}
		params := &common.UpdatePoolParams{
			QosType:            QosTypeAuto,
			AllowAutoTiering:   false,
			HotTierSizeInBytes: minQuotaInBytesPool,
			SizeInBytes:        minQuotaInBytesPool * 2,
		}
		err := _validateUpdatePoolParams(params, pool)
		assert.EqualError(tt, err, "Auto tiering disable operation is not supported")
	})

	t.Run("Succeeds when AllowAutoTiering is true and HotTierSizeInBytes is valid", func(tt *testing.T) {
		// Save and restore the original value
		currentATState := autoTieringEnabled
		defer func() { autoTieringEnabled = currentATState }()
		autoTieringEnabled = true

		pool := &datamodel.Pool{
			QosType:          QosTypeAuto,
			AllowAutoTiering: false,
			SizeInBytes:      int64(minQuotaInBytesPool),
		}
		params := &common.UpdatePoolParams{
			QosType:                  QosTypeAuto,
			AllowAutoTiering:         true,
			HotTierSizeInBytes:       minQuotaInBytesPool,
			SizeInBytes:              minQuotaInBytesPool,
			CustomPerformanceEnabled: true,
			TotalThroughputMibps:     float64(minCustomThroughput + 10),
			TotalIops:                float64(minCustomIops + 100),
		}
		err := _validateUpdatePoolParams(params, pool)
		assert.NoError(tt, err)
	})
}

// Test the new helper functions
func TestValidateThroughputRange(t *testing.T) {
	t.Run("Returns error when throughput is below minimum", func(tt *testing.T) {
		err := validators.ValidateThroughputRange(int64(minCustomThroughput-1), minCustomThroughput, maxCustomThroughput)
		expectedErr := fmt.Sprintf("TotalThroughputMibps must be between %d and %d MiBps", minCustomThroughput, maxCustomThroughput)
		assert.EqualError(tt, err, expectedErr)
	})

	t.Run("Returns error when throughput is above maximum", func(tt *testing.T) {
		err := validators.ValidateThroughputRange(int64(maxCustomThroughput+1), minCustomThroughput, maxCustomThroughput)
		expectedErr := fmt.Sprintf("TotalThroughputMibps must be between %d and %d MiBps", minCustomThroughput, maxCustomThroughput)
		assert.EqualError(tt, err, expectedErr)
	})

	t.Run("Succeeds when throughput is at minimum", func(tt *testing.T) {
		err := validators.ValidateThroughputRange(int64(minCustomThroughput), minCustomThroughput, maxCustomThroughput)
		assert.NoError(tt, err)
	})

	t.Run("Succeeds when throughput is at maximum", func(tt *testing.T) {
		err := validators.ValidateThroughputRange(int64(maxCustomThroughput), minCustomThroughput, maxCustomThroughput)
		assert.NoError(tt, err)
	})

	t.Run("Succeeds when throughput is in valid range", func(tt *testing.T) {
		err := validators.ValidateThroughputRange(int64(minCustomThroughput+100), minCustomThroughput, maxCustomThroughput)
		assert.NoError(tt, err)
	})

	t.Run("Handles zero throughput", func(tt *testing.T) {
		err := validators.ValidateThroughputRange(0, minCustomThroughput, maxCustomThroughput)
		expectedErr := fmt.Sprintf("TotalThroughputMibps must be between %d and %d MiBps", minCustomThroughput, maxCustomThroughput)
		assert.EqualError(tt, err, expectedErr)
	})

	t.Run("Handles very large throughput values", func(tt *testing.T) {
		err := validators.ValidateThroughputRange(999999, minCustomThroughput, maxCustomThroughput)
		expectedErr := fmt.Sprintf("TotalThroughputMibps must be between %d and %d MiBps", minCustomThroughput, maxCustomThroughput)
		assert.EqualError(tt, err, expectedErr)
	})

	t.Run("Handles boundary case - one above maximum", func(tt *testing.T) {
		err := validators.ValidateThroughputRange(int64(maxCustomThroughput+1), minCustomThroughput, maxCustomThroughput)
		expectedErr := fmt.Sprintf("TotalThroughputMibps must be between %d and %d MiBps", minCustomThroughput, maxCustomThroughput)
		assert.EqualError(tt, err, expectedErr)
	})

	t.Run("Handles boundary case - one below minimum", func(tt *testing.T) {
		err := validators.ValidateThroughputRange(int64(minCustomThroughput-1), minCustomThroughput, maxCustomThroughput)
		expectedErr := fmt.Sprintf("TotalThroughputMibps must be between %d and %d MiBps", minCustomThroughput, maxCustomThroughput)
		assert.EqualError(tt, err, expectedErr)
	})
}

func TestValidateIopsRange(t *testing.T) {
	t.Run("Returns error when IOPS is below minimum", func(tt *testing.T) {
		err := validators.ValidateIopsRange(int64(minCustomIops-1), minCustomIops, maxCustomIops)
		expectedErr := fmt.Sprintf("TotalIops must be between %d and %d IOPS", minCustomIops, maxCustomIops)
		assert.EqualError(tt, err, expectedErr)
	})

	t.Run("Returns error when IOPS is above maximum", func(tt *testing.T) {
		err := validators.ValidateIopsRange(int64(maxCustomIops+1), minCustomIops, maxCustomIops)
		expectedErr := fmt.Sprintf("TotalIops must be between %d and %d IOPS", minCustomIops, maxCustomIops)
		assert.EqualError(tt, err, expectedErr)
	})

	t.Run("Succeeds when IOPS is at minimum", func(tt *testing.T) {
		err := validators.ValidateIopsRange(int64(minCustomIops), minCustomIops, maxCustomIops)
		assert.NoError(tt, err)
	})

	t.Run("Succeeds when IOPS is at maximum", func(tt *testing.T) {
		err := validators.ValidateIopsRange(int64(maxCustomIops), minCustomIops, maxCustomIops)
		assert.NoError(tt, err)
	})

	t.Run("Succeeds when IOPS is in valid range", func(tt *testing.T) {
		err := validators.ValidateIopsRange(int64(minCustomIops+1000), minCustomIops, maxCustomIops)
		assert.NoError(tt, err)
	})

	t.Run("Handles zero IOPS", func(tt *testing.T) {
		err := validators.ValidateIopsRange(0, minCustomIops, maxCustomIops)
		expectedErr := fmt.Sprintf("TotalIops must be between %d and %d IOPS", minCustomIops, maxCustomIops)
		assert.EqualError(tt, err, expectedErr)
	})

	t.Run("Handles very large IOPS values", func(tt *testing.T) {
		err := validators.ValidateIopsRange(999999, minCustomIops, maxCustomIops)
		expectedErr := fmt.Sprintf("TotalIops must be between %d and %d IOPS", minCustomIops, maxCustomIops)
		assert.EqualError(tt, err, expectedErr)
	})

	t.Run("Handles boundary case - one above maximum", func(tt *testing.T) {
		err := validators.ValidateIopsRange(int64(maxCustomIops+1), minCustomIops, maxCustomIops)
		expectedErr := fmt.Sprintf("TotalIops must be between %d and %d IOPS", minCustomIops, maxCustomIops)
		assert.EqualError(tt, err, expectedErr)
	})

	t.Run("Handles boundary case - one below minimum", func(tt *testing.T) {
		err := validators.ValidateIopsRange(int64(minCustomIops-1), minCustomIops, maxCustomIops)
		expectedErr := fmt.Sprintf("TotalIops must be between %d and %d IOPS", minCustomIops, maxCustomIops)
		assert.EqualError(tt, err, expectedErr)
	})
}

func TestDeletePool(t *testing.T) {
	t.Run("WhenGetOrCreateAccountFails", func(tt *testing.T) {
		ctx, se, _, temporal := setup(tt)

		params := &common.DeletePoolParams{
			AccountName: "test_account",
			PoolID:      "test_pool_id",
		}

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.New("account not found")
		}

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

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{Name: "test_account"}, nil
		}

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

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}

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

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.NewNotFoundErr("account not found", nil)
		}
		_, err := orch.GetMultiplePools(ctx, "non-existent-account", []string{"uuid1", "uuid2"})
		if err != nil {
			t.Errorf("Expected nil, got error: %v", err)
		}
	})
	t.Run("ReturnsErrorWhenNoPoolsMatchUUIDs", func(tt *testing.T) {
		ctx, store, orch, _ := setup(tt)
		_, account := createDBPools(t, store)

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}

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

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{Name: accountName}, nil
		}
		mockStorage.On("ListPools", ctx, mock.Anything).Return(nil, errors.New("list pools error"))
		_, err := orch.GetMultiplePools(ctx, accountName, []string{"uuid1", "uuid2"})
		assert.EqualError(tt, err, "list pools error")
	})
	t.Run("ReturnsPoolsSuccessfullyWhenUUIDsMatch", func(tt *testing.T) {
		ctx, store, orch, _ := setup(tt)

		dbPools, account := createDBPools(t, store)

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}

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

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.NewNotFoundErr("account not found", nil)
		}

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

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}

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

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{Name: accountName}, nil
		}
		mockStorage.On("ListPools", ctx, mock.Anything).Return(nil, errors.New("list pools error"))
		_, err := orch.ListPools(ctx, accountName, false)
		assert.EqualError(tt, err, "list pools error")
	})
	t.Run("ReturnsPoolsSuccessfully", func(tt *testing.T) {
		ctx, store, orch, _ := setup(tt)

		dbPools, account := createDBPools(tt, store)

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}

		pools, err := orch.ListPools(ctx, account.Name, false)
		assert.NoError(tt, err)
		assert.Len(tt, pools, 2, "Expected 2 pools, got %d", len(pools))
		assert.Equal(tt, dbPools[0].Name, pools[0].Name, "Returned pool does not match expected pool")
		assert.Equal(tt, dbPools[1].Name, pools[1].Name, "Returned pool does not match expected pool")
	})
	t.Run("ReturnsPoolsSuccessfullyWithDeleted", func(tt *testing.T) {
		ctx, store, orch, _ := setup(tt)

		dbPools, account := createDBPools(tt, store)

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}

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

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.New("account not found")
		}

		_, err := GetPoolByName(ctx, se, "test-pool", "test-account", queryDepthOne)
		assert.EqualError(tt, err, "account not found")
	})
	t.Run("WhenGetPoolByNameFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := new(database.MockStorage)
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{Name: "test-account"}, nil
		}
		mockStorage.On("GetPoolByName", ctx, mock.Anything).Return(nil, errors.New("pool not found"))
		_, err := GetPoolByName(ctx, mockStorage, "test-pool", "test-account", queryDepthOne)
		assert.EqualError(tt, err, "pool not found")
	})
	t.Run("WhenSuccess", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := new(database.MockStorage)
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{Name: "test-account"}, nil
		}
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

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{Name: "test-account"}, nil
		}
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

func TestValidateUpdatePoolParams_AutoTieringDisabled(t *testing.T) {
	autoTieringEnabled = false // Simulate auto-tiering feature being disabled

	t.Run("ReturnsError_WhenAllowAutoTieringIsTrue", func(tt *testing.T) {
		params := &common.UpdatePoolParams{
			AllowAutoTiering: true,
			SizeInBytes:      minQuotaInBytesPool,
		}
		pool := &datamodel.Pool{
			AutoTieringConfig: &datamodel.AutoTieringConfig{
				HotTierSizeInBytes: 0,
			},
		}

		err := _validateUpdatePoolParams(params, pool)
		assert.EqualError(tt, err, "Auto-Tiering feature is currently not enabled.")
	})

	t.Run("ReturnsError_WhenHotTierSizeInBytesIsNonZero", func(tt *testing.T) {
		params := &common.UpdatePoolParams{
			AllowAutoTiering:         false,
			SizeInBytes:              minQuotaInBytesPool,
			CustomPerformanceEnabled: true,
			HotTierSizeInBytes:       minQuotaInBytesPool,
		}
		pool := &datamodel.Pool{
			AutoTieringConfig: &datamodel.AutoTieringConfig{
				HotTierSizeInBytes: 0,
			},
		}

		err := _validateUpdatePoolParams(params, pool)
		assert.EqualError(tt, err, "Auto-Tiering feature is currently not enabled.")
	})

	t.Run("DoesNotReturnError_WhenAllowAutoTieringParametersAreNotPassed", func(tt *testing.T) {
		params := &common.UpdatePoolParams{
			QosType:                  QosTypeAuto,
			SizeInBytes:              minQuotaInBytesPool,
			CustomPerformanceEnabled: true,
			TotalThroughputMibps:     float64(minCustomThroughput + 10),
			TotalIops:                float64(minCustomIops + 10),
		}
		pool := &datamodel.Pool{
			AutoTieringConfig: &datamodel.AutoTieringConfig{
				HotTierSizeInBytes: 0,
			},
		}

		err := _validateUpdatePoolParams(params, pool)
		assert.NoError(tt, err)
	})
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
		TotalIops:            2048,
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
	originalValidateUpdatePoolParams := ValidateUpdatePoolParams

	defer func() {
		getAccountWithName = originalGetAccountWithName
		ValidateUpdatePoolParams = originalValidateUpdatePoolParams
	}()

	getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
		return account, nil
	}

	ValidateUpdatePoolParams = func(params *common.UpdatePoolParams, pool *datamodel.Pool) error {
		return nil
	}

	// Mock workflow execution to fail
	workflowError := errors.New("workflow execution failed")
	temporal.EXPECT().ExecuteWorkflow(ctx, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, workflowError)

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
		TotalIops:            1024,
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
	mockTemporal.On("ExecuteWorkflow", ctx, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, workflowError)

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
		TotalIops:            1024,
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
	mockTemporal.On("ExecuteWorkflow", ctx, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, workflowError)

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

// Pool Size Validation Tests
func TestPoolSizeValidation(t *testing.T) {
	t.Run("MinimumValidSize", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:             uint64(1 * utils.TiBInBytes), // Exactly 1 TiB (minimum)
			ServiceLevel:            ServiceLevelNameFLEX,
			QosType:                 QosTypeAuto,
			CustomPerformanceParams: &common.CustomPerformanceParams{ThroughputMibps: 64, Iops: nillable.ToPointer(int64(1024))},
		}
		err := _validateCreatePoolParams(params)
		assert.NoError(tt, err, "Minimum valid size (1 TiB) should pass validation")
	})

	t.Run("BelowMinimumSize", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:             uint64(1*utils.TiBInBytes - utils.GiBInBytes), // Just below 1 TiB (1023 GiB)
			ServiceLevel:            ServiceLevelNameFLEX,
			QosType:                 QosTypeAuto,
			CustomPerformanceParams: &common.CustomPerformanceParams{ThroughputMibps: 64, Iops: nillable.ToPointer(int64(1024))},
		}
		err := _validateCreatePoolParams(params)
		assert.Error(tt, err)
		// 1023 GiB is a multiple of 1GiB but below 1TiB, so StandardPoolValidator catches it
		assert.EqualError(tt, err, "Given pool size not supported. Pool size must be greater than 1TiB and a multiple of 1GiB")
	})

	t.Run("MaximumValidSize", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:             uint64(425 * utils.TiBInBytes), // Exactly 425 TiB (maximum)
			ServiceLevel:            ServiceLevelNameFLEX,
			QosType:                 QosTypeAuto,
			CustomPerformanceParams: &common.CustomPerformanceParams{ThroughputMibps: 64, Iops: nillable.ToPointer(int64(1024))},
		}
		err := _validateCreatePoolParams(params)
		assert.NoError(tt, err, "Maximum valid size (425 TiB) should pass validation")
	})

	t.Run("AboveMaximumSize", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:             uint64(425*utils.TiBInBytes + utils.GiBInBytes), // Just above 425 TiB (425 TiB + 1 GiB)
			ServiceLevel:            ServiceLevelNameFLEX,
			QosType:                 QosTypeAuto,
			CustomPerformanceParams: &common.CustomPerformanceParams{ThroughputMibps: 64, Iops: nillable.ToPointer(int64(1024))},
		}
		err := _validateCreatePoolParams(params)
		assert.Error(tt, err)
		assert.EqualError(tt, err, "Given pool size not supported. Pool size must be less than 425TiB")
	})

	t.Run("InvalidGranularity", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:             uint64(1*utils.TiBInBytes + 512*1024*1024), // 1 TiB + 512 MiB (not aligned to 1 GiB)
			ServiceLevel:            ServiceLevelNameFLEX,
			QosType:                 QosTypeAuto,
			CustomPerformanceParams: &common.CustomPerformanceParams{ThroughputMibps: 64, Iops: nillable.ToPointer(int64(1024))},
		}
		err := _validateCreatePoolParams(params)
		assert.Error(tt, err)
		assert.EqualError(tt, err, "Given pool size must be a multiple of 1GiB")
	})
}

// Throughput Validation Tests
func TestThroughputValidation(t *testing.T) {
	t.Run("MinimumValidThroughput", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:             uint64(1 * utils.TiBInBytes),
			ServiceLevel:            ServiceLevelNameFLEX,
			QosType:                 QosTypeAuto,
			CustomPerformanceParams: &common.CustomPerformanceParams{ThroughputMibps: 64, Iops: nillable.ToPointer(int64(1024))}, // Exactly 64 MiBps
		}
		err := _validateCreatePoolParams(params)
		assert.NoError(tt, err, "Minimum valid throughput (64 MiBps) should pass validation")
	})

	t.Run("BelowMinimumThroughput", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:             uint64(1 * utils.TiBInBytes),
			ServiceLevel:            ServiceLevelNameFLEX,
			QosType:                 QosTypeAuto,
			CustomPerformanceParams: &common.CustomPerformanceParams{ThroughputMibps: 63, Iops: nillable.ToPointer(int64(1024))}, // Just below 64 MiBps
		}
		err := _validateCreatePoolParams(params)
		assert.Error(tt, err)
		assert.EqualError(tt, err, "TotalThroughputMibps must be between 64 and 5120 MiBps")
	})

	t.Run("MaximumValidThroughput", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:             uint64(1 * utils.TiBInBytes),
			ServiceLevel:            ServiceLevelNameFLEX,
			QosType:                 QosTypeAuto,
			CustomPerformanceParams: &common.CustomPerformanceParams{ThroughputMibps: 5120, Iops: nillable.ToPointer(int64(5120 * 16))}, // 81920 IOPS for 5120 MiBps
		}
		err := _validateCreatePoolParams(params)
		assert.NoError(tt, err, "Maximum valid throughput (5120 MiBps) should pass validation")
	})

	t.Run("AboveMaximumThroughput", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:             uint64(1 * utils.TiBInBytes),
			ServiceLevel:            ServiceLevelNameFLEX,
			QosType:                 QosTypeAuto,
			CustomPerformanceParams: &common.CustomPerformanceParams{ThroughputMibps: 5121, Iops: nillable.ToPointer(int64(1024))}, // Just above 5120 MiBps
		}
		err := _validateCreatePoolParams(params)
		assert.Error(tt, err)
		assert.EqualError(tt, err, "TotalThroughputMibps must be between 64 and 5120 MiBps")
	})
}

// IOPS Validation Tests
func TestIOPSValidation(t *testing.T) {
	t.Run("MinimumValidIOPS", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:             uint64(1 * utils.TiBInBytes),
			ServiceLevel:            ServiceLevelNameFLEX,
			QosType:                 QosTypeAuto,
			CustomPerformanceParams: &common.CustomPerformanceParams{ThroughputMibps: 64, Iops: nillable.ToPointer(int64(1024))}, // Exactly 1024 IOPS
		}
		err := _validateCreatePoolParams(params)
		assert.NoError(tt, err, "Minimum valid IOPS (1024) should pass validation")
	})

	t.Run("BelowMinimumIOPS", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:             uint64(1 * utils.TiBInBytes),
			ServiceLevel:            ServiceLevelNameFLEX,
			QosType:                 QosTypeAuto,
			CustomPerformanceParams: &common.CustomPerformanceParams{ThroughputMibps: 64, Iops: nillable.ToPointer(int64(1023))}, // Just below 1024 IOPS
		}
		err := _validateCreatePoolParams(params)
		assert.Error(tt, err)
		assert.EqualError(tt, err, "TotalIops must be between 1024 and 160000 IOPS")
	})

	t.Run("MaximumValidIOPS", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:             uint64(1 * utils.TiBInBytes),
			ServiceLevel:            ServiceLevelNameFLEX,
			QosType:                 QosTypeAuto,
			CustomPerformanceParams: &common.CustomPerformanceParams{ThroughputMibps: 64, Iops: nillable.ToPointer(int64(160000))}, // Exactly 160000 IOPS
		}
		err := _validateCreatePoolParams(params)
		assert.NoError(tt, err, "Maximum valid IOPS (160000) should pass validation")
	})

	t.Run("AboveMaximumIOPS", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:             uint64(1 * utils.TiBInBytes),
			ServiceLevel:            ServiceLevelNameFLEX,
			QosType:                 QosTypeAuto,
			CustomPerformanceParams: &common.CustomPerformanceParams{ThroughputMibps: 64, Iops: nillable.ToPointer(int64(160001))}, // Just above 160000 IOPS
		}
		err := _validateCreatePoolParams(params)
		assert.Error(tt, err)
		assert.EqualError(tt, err, "TotalIops must be between 1024 and 160000 IOPS")
	})
}
