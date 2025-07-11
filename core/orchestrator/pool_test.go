package orchestrator

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	ontaprestmodel "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/repository"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
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
			Iops:            1024,
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
			Iops:            1024,
			PrimaryZone:     "us-central1-a",
			SecondaryZone:   "us-central1-b",
		},
	}
	accountName := "test-account"

	dbPoolView := repository.ConvertPoolToPoolView(datastorePool)
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
			Iops:            1024,
			PrimaryZone:     "us-central1-a",
			SecondaryZone:   "us-central1-b",
		},
	}
	accountName := "test-account"
	dbPoolView := repository.ConvertPoolToPoolView(datastorePool)
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
			Iops:            1024,
			PrimaryZone:     "us-central1-a",
			SecondaryZone:   "",
		},
	}
	accountName := "test-account"
	dbPoolView := repository.ConvertPoolToPoolView(datastorePool)
	result := convertDatastorePoolToModel(dbPoolView, accountName)

	assert.Nil(t, result.DeletedAt)
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
			CustomPerformanceParams: &common.CustomPerformanceParams{
				Enabled:         true,
				ThroughputMibps: 64,
				Iops:            1024,
			},
		}
		originalnodePassword := common.NodePassword
		common.NodePassword = "password"
		defer func() {
			common.NodePassword = originalnodePassword
		}()
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.New("account not found")
		}

		_, _, err := createPool(ctx, se, temporal, params)
		assert.EqualError(tt, err, "account not found")
	})
	t.Run("WhenValidatePoolParamFails", func(tt *testing.T) {
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
			CustomPerformanceParams: &common.CustomPerformanceParams{
				Enabled:         true,
				ThroughputMibps: 64,
				Iops:            1024,
			},
		}

		dbAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-uuid",
			},
			Name: "test_account",
		}
		originalnodePassword := common.NodePassword
		common.NodePassword = "password"
		defer func() {
			common.NodePassword = originalnodePassword
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
			CustomPerformanceParams: &common.CustomPerformanceParams{
				Enabled:         true,
				ThroughputMibps: 64,
				Iops:            1024,
			},
		}

		_, account := createDBPools(t, store)

		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		ValidateCreatePoolParams = func(params *common.CreatePoolParams) error {
			return nil
		}

		_, _, err := createPool(ctx, store, temporal, params)
		assert.EqualError(tt, err, "pool already exists")
	})
	t.Run("WhenCreatePoolSucceeds", func(tt *testing.T) {
		ctx, _, orch, temporal := setup(tt)

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
			CustomPerformanceParams: &common.CustomPerformanceParams{
				Enabled:         true,
				ThroughputMibps: 64,
				Iops:            1024,
			},
		}

		dbAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-uuid",
			},
			Name: "test_account",
		}
		originalnodePassword := common.NodePassword
		common.NodePassword = "password"

		authType := common.AuthType
		common.AuthType = common.USERNAME_PWD
		defer func() {
			common.AuthType = authType // Reset to original value after test
			common.NodePassword = originalnodePassword
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
			CustomPerformanceParams: &common.CustomPerformanceParams{
				Enabled:         true,
				ThroughputMibps: 64,
				Iops:            1024,
			},
		}

		dbAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-uuid",
			},
			Name: "test_account",
		}
		authType := common.AuthType
		common.AuthType = common.USER_CERTIFICATE
		defer func() {
			common.AuthType = authType // Reset to original value after test
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
			CustomPerformanceParams: &common.CustomPerformanceParams{
				Enabled:         true,
				ThroughputMibps: 64,
				Iops:            1024,
			},
		}

		dbAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-uuid",
			},
			Name: "test_account",
		}
		authType := common.AuthType
		common.AuthType = common.USERNAME_PWD_SEC_MGR
		defer func() {
			common.AuthType = authType // Reset to original value after test
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
		if !strings.Contains(err.Error(), "pool not found") {
			tt.Errorf("Expected not found error, got %s", err.Error())
		}
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

		_, err := orch.GetPoolByVendorID(ctx, "non-existent-vendor-id")
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

		result, err := orch.GetPoolByVendorID(ctx, "test-vendor-id")
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
		}
		err := _validateCreatePoolParams(params)
		assert.EqualError(t, err, "Given service level not supported. Supported service level is "+ServiceLevelNameFLEX)
	})
	t.Run("ValidateCreatePoolParams_WithInvalidSize_ReturnsError", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:  1024 * 1024 * 1024, // 1 GiB, which is below the minimum quota
			ServiceLevel: ServiceLevelNameFLEX,
			QosType:      QosTypeAuto,
		}
		err := _validateCreatePoolParams(params)
		assert.EqualError(t, err, "Given pool size not supported. Pool size must be greater than 2TiB and a multiple of 1GiB")
	})
	t.Run("ValidateCreatePoolParams_WithInvalidGiBSize_ReturnsError", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:  2 * 1099511627777, // Exactly the minimum quota+1
			ServiceLevel: ServiceLevelNameFLEX,
			QosType:      QosTypeAuto,
		}
		err := _validateCreatePoolParams(params)
		assert.EqualError(t, err, "Given pool size must be a multiple of 1GiB")
	})
	t.Run("ValidateCreatePoolParams_WithValidSize_WrongQosType", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:  2199023255552,
			ServiceLevel: ServiceLevelNameFLEX,
			QosType:      "Manual",
		}
		err := _validateCreatePoolParams(params)
		assert.EqualError(t, err, "Given QoS type not supported for Unified Flex Storage Pool. Supported QoS type is "+QosTypeAuto)
	})
	t.Run("ValidateCreatePoolParams_WithNoCustomPerformanceSet", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:             2 * 1099511627776,
			ServiceLevel:            ServiceLevelNameFLEX,
			QosType:                 QosTypeAuto,
			CustomPerformanceParams: &common.CustomPerformanceParams{Enabled: false, ThroughputMibps: 0, Iops: 0},
		}
		err := _validateCreatePoolParams(params)
		assert.EqualError(t, err, "CustomPerformanceEnabled must be true for Unified Flex Storage Pool")
	})
	t.Run("ValidateCreatePoolParams_WithInvalidThroughputSetWithCustomPerformance", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:             2 * 1099511627776,
			ServiceLevel:            ServiceLevelNameFLEX,
			QosType:                 QosTypeAuto,
			CustomPerformanceParams: &common.CustomPerformanceParams{Enabled: true, ThroughputMibps: 0, Iops: 1000},
		}
		err := _validateCreatePoolParams(params)
		assert.EqualError(t, err, "TotalThroughputMibps must be set and must be greater than 64 MiBps for Unified Flex Storage Pool")
	})
	t.Run("ValidateCreatePoolParams_WithInvalidIOPSSetWithCustomPerformance", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:             2 * 1099511627776,
			ServiceLevel:            ServiceLevelNameFLEX,
			QosType:                 QosTypeAuto,
			CustomPerformanceParams: &common.CustomPerformanceParams{Enabled: true, ThroughputMibps: 128, Iops: 100},
		}
		err := _validateCreatePoolParams(params)
		assert.EqualError(t, err, "TotalIops must be greater than 1024 for Unified Flex Storage Pool")
	})
}

func TestValidateUpdatePoolParams(t *testing.T) {
	t.Run("Rejects changing qos type from manual to auto", func(tt *testing.T) {
		pool := &datamodel.Pool{QosType: QosTypeAuto}
		params := &common.UpdatePoolParams{
			QosType:                  "Manual",
			SizeInBytes:              minQuotaInBytesPool * 2,
			CustomPerformanceEnabled: true,
			TotalThroughputMibps:     float64(minCustomThroughput + 10),
			TotalIops:                float64(minCustomIops + 100),
		}
		err := _validateUpdatePoolParams(params, pool)
		assert.EqualError(tt, err, "Cannot change qos type from manual to auto")
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
		// AddActivity 1 to minimum quota to simulate a value that's not divisible by minSizeGranularity.
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
	t.Run("Returns error when custom performance is disabled", func(tt *testing.T) {
		pool := &datamodel.Pool{QosType: "Manual"}
		params := &common.UpdatePoolParams{
			QosType:                  "Manual",
			SizeInBytes:              minQuotaInBytesPool * 2,
			CustomPerformanceEnabled: false,
		}
		err := _validateUpdatePoolParams(params, pool)
		assert.EqualError(tt, err, "CustomPerformanceEnabled must be true for Unified Flex Storage Pool")
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
		expectedErr := fmt.Sprintf("TotalThroughputMibps must be set and must be greater than %d MiBps for Unified Flex Storage Pool", minCustomThroughput)
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
		expectedErr := fmt.Sprintf("TotalIops must be greater than %d for Unified Flex Storage Pool", minCustomIops)
		err := _validateUpdatePoolParams(params, pool)
		assert.EqualError(tt, err, expectedErr)
	})
	t.Run("Succeeds with valid update parameters", func(tt *testing.T) {
		pool := &datamodel.Pool{QosType: "Manual"}
		// Use a valid size that is a multiple of minSizeGranularity. For this test, we assume that minQuotaInBytesPool*2 is valid.
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
			QosType:            QosTypeAuto,
			AllowAutoTiering:   true,
			HotTierSizeInBytes: int64(minQuotaInBytesPool * 2),
			SizeInBytes:        int64(minQuotaInBytesPool * 3),
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
			QosType:            QosTypeAuto,
			AllowAutoTiering:   true,
			HotTierSizeInBytes: int64(minQuotaInBytesPool),
			SizeInBytes:        int64(minQuotaInBytesPool * 2),
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
		if err == nil {
			t.Errorf("Expected error, got nil")
		}
		if !errors.IsNotFoundErr(err) {
			t.Errorf("Expected not found error, got %v", err)
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
	mockStorage.EXPECT().ListPools(ctx, (*utils.Filter)(nil)).Return(nil, errors.New("db error"))

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
	t.Run("WhenGetNodesByPoolIDFails", func(tt *testing.T) {
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
		}
		poolView := repository.ConvertPoolToPoolView(poolResp)
		mockStorage.On("GetPoolByName", ctx, mock.Anything).Return(poolView, nil)
		mockStorage.On("GetNodesByPoolID", ctx, mock.Anything).Return(nil, errors.New("node not found"))
		_, err := GetPoolByName(ctx, mockStorage, "test-pool", "test-account", queryDepthOne)
		assert.EqualError(tt, err, "node not found")
	})
	t.Run("WhenGetNodeReturnsEmpty", func(tt *testing.T) {
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
		}
		poolView := repository.ConvertPoolToPoolView(poolResp)
		mockStorage.On("GetPoolByName", ctx, mock.Anything).Return(poolView, nil)
		mockStorage.On("GetNodesByPoolID", ctx, mock.Anything).Return(nil, nil)
		_, err := GetPoolByName(ctx, mockStorage, "test-pool", "test-account", queryDepthZero)
		assert.EqualError(tt, err, "node not found")
	})
	t.Run("WhenGetInterclusterLifsError", func(tt *testing.T) {
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
		}
		nodeResp := []*datamodel.Node{{
			BaseModel: datamodel.BaseModel{UUID: "test-node-uuid", ID: 1},
			Name:      "test-node",
		},
		}
		poolView := repository.ConvertPoolToPoolView(poolResp)
		mockStorage.On("GetPoolByName", ctx, mock.Anything).Return(poolView, nil)
		mockStorage.On("GetNodesByPoolID", ctx, mock.Anything).Return(nodeResp, nil)

		getInterClusterLifsFromONTAP = func(ctx context.Context, node []*datamodel.Node, pools *datamodel.PoolView) ([]*vsa.InterclusterLif, error) {
			return nil, errors.New("lif not found")
		}

		_, err := GetPoolByName(ctx, mockStorage, "test-pool", "test-account", queryDepthOne)
		assert.EqualError(tt, err, "lif not found")
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
				Iops:            1024,
				PrimaryZone:     "us-central1-a",
			},
		}
		nodeResp := []*datamodel.Node{{
			BaseModel: datamodel.BaseModel{UUID: "test-node-uuid", ID: 1},
			Name:      "test-node",
		},
		}
		poolView := repository.ConvertPoolToPoolView(poolResp)
		mockStorage.On("GetPoolByName", ctx, mock.Anything).Return(poolView, nil)
		mockStorage.On("GetNodesByPoolID", ctx, mock.Anything).Return(nodeResp, nil)
		interClusterLifResp := []*vsa.InterclusterLif{
			{
				Name:    "test-intercluster-lif",
				Address: ontaprestmodel.IPAddress(net.ParseIP("10.0.0.1")),
			},
			{
				Name:    "test-intercluster-lif-2",
				Address: ontaprestmodel.IPAddress(net.ParseIP("10.0.0.2")),
			},
		}

		getInterClusterLifsFromONTAP = func(ctx context.Context, node []*datamodel.Node, pools *datamodel.PoolView) ([]*vsa.InterclusterLif, error) {
			return interClusterLifResp, nil
		}

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
				Iops:            1024,
				PrimaryZone:     "us-central1-a",
			},
		}
		nodeResp := []*datamodel.Node{
			{
				BaseModel: datamodel.BaseModel{UUID: "test-node-uuid", ID: 1},
				Name:      "test-node",
			},
		}
		poolView := repository.ConvertPoolToPoolView(poolResp)
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
			Name:                    "mock-pool",
			Description:             "Mock pool description",
			State:                   "ACTIVE",
			StateDetails:            "Mock state details",
			VendorID:                "mock-vendor-id",
			ServiceLevel:            "mock-service-level",
			SizeInBytes:             1024 * 1024 * 1024, // 1 GiB
			UsedBytes:               512 * 1024 * 1024,  // 512 MiB
			Network:                 "mock-network",
			AllowAutoTiering:        true,
			HotTierSizeInBytes:      256 * 1024 * 1024, // 256 MiB
			EnableHotTierAutoResize: false,
			AccountID:               1,
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
			QosType:            "mock-qos-type",
			AutoTierBucketName: "mock-bucket-name",
			ServiceAccountId:   "mock-service-account-id",
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

func Test_getInterClusterLifsFromONTAP(t *testing.T) {
	// Prepare test data
	nodes := []*datamodel.Node{
		{
			Name:           "node1",
			NodeAttributes: &datamodel.NodeDetails{},
		},
	}
	poolView := &datamodel.PoolView{
		Pool: datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "pass",
				SecretID:      "secret",
				CertificateID: "cert",
			},
		},
	}

	t.Run("success", func(t *testing.T) {
		origGetProviderByNode := activities.GetProviderByNode
		defer func() { activities.GetProviderByNode = origGetProviderByNode }()

		mockProvider := new(vsa.MockProvider)
		expectedLifs := []*vsa.InterclusterLif{{Address: "1.2.3.4"}}
		activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		mockProvider.On("GetInterclusterLIFs", "default-intercluster").Return(expectedLifs, nil)

		lifs, err := _getInterClusterLifsFromONTAP(context.Background(), nodes, poolView)
		assert.NoError(t, err)
		assert.Equal(t, expectedLifs, lifs)
	})

	t.Run("provider error", func(t *testing.T) {
		origGetProviderByNode := activities.GetProviderByNode
		defer func() { activities.GetProviderByNode = origGetProviderByNode }()

		activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, fmt.Errorf("provider error")
		}
		lifs, err := _getInterClusterLifsFromONTAP(context.Background(), nodes, poolView)
		assert.Error(t, err)
		assert.Nil(t, lifs)
	})

	t.Run("GetInterclusterLIFs error", func(t *testing.T) {
		origGetProviderByNode := activities.GetProviderByNode
		defer func() { activities.GetProviderByNode = origGetProviderByNode }()

		mockProvider := new(vsa.MockProvider)
		activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		mockProvider.On("GetInterclusterLIFs", "default-intercluster").Return(nil, fmt.Errorf("lif error"))

		lifs, err := _getInterClusterLifsFromONTAP(context.Background(), nodes, poolView)
		assert.Error(t, err)
		assert.Nil(t, lifs)
	})
}

func TestValidateUpdatePoolParams_AutoTieringDisabled(t *testing.T) {
	autoTieringEnabled = false // Simulate auto-tiering feature being disabled

	t.Run("ReturnsError_WhenAllowAutoTieringIsTrue", func(tt *testing.T) {
		params := &common.UpdatePoolParams{
			AllowAutoTiering: true,
			SizeInBytes:      minQuotaInBytesPool,
		}
		pool := &datamodel.Pool{
			HotTierSizeInBytes: 0,
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
			HotTierSizeInBytes: 0, // Non-zero value
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
			HotTierSizeInBytes: 0,
		}

		err := _validateUpdatePoolParams(params, pool)
		assert.NoError(tt, err)
	})
}

func TestValidateCreatePoolParams_AutoTieringDisabled(t *testing.T) {
	autoTieringEnabled = false // Simulate auto-tiering feature being disabled

	t.Run("ReturnsError_WhenAllowAutoTieringIsTrue", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			QosType:          QosTypeAuto,
			ServiceLevel:     ServiceLevelNameFLEX,
			AllowAutoTiering: true,
			SizeInBytes:      minQuotaInBytesPool,
			CustomPerformanceParams: &common.CustomPerformanceParams{
				Enabled:         true,
				ThroughputMibps: 128,
				Iops:            1024,
			},
		}

		err := _validateCreatePoolParams(params)
		assert.EqualError(tt, err, "Auto-Tiering feature is currently not enabled.")
	})

	t.Run("ReturnsError_WhenHotTierSizeInBytesIsNonZero", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			QosType:          QosTypeAuto,
			ServiceLevel:     ServiceLevelNameFLEX,
			AllowAutoTiering: false,
			SizeInBytes:      minQuotaInBytesPool,
			CustomPerformanceParams: &common.CustomPerformanceParams{
				Enabled:         true,
				ThroughputMibps: 128,
				Iops:            1024,
			},
			HotTierSizeInBytes: minQuotaInBytesPool,
		}

		err := _validateCreatePoolParams(params)
		assert.EqualError(tt, err, "Auto-Tiering feature is currently not enabled.")
	})

	t.Run("DoesNotReturnError_WhenAllowAutoTieringParametersAreNotPassed", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			QosType:     QosTypeAuto,
			SizeInBytes: minQuotaInBytesPool,
			CustomPerformanceParams: &common.CustomPerformanceParams{
				Enabled:         true,
				ThroughputMibps: 128,
				Iops:            1024,
			},
			ServiceLevel: ServiceLevelNameFLEX,
		}

		err := _validateCreatePoolParams(params)
		assert.NoError(tt, err)
	})
}
