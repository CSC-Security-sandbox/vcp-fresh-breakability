package orchestrator

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	ontaprestmodel "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	workflow_engine_mock "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine"
	"go.temporal.io/sdk/client"
	"gorm.io/gorm"
)

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
		ServiceLevel:     "premium",
	}
	accountName := "test-account"

	result := convertDatastorePoolToModel(datastorePool, accountName)

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
		ServiceLevel:     "premium",
	}
	accountName := "test-account"

	result := convertDatastorePoolToModel(datastorePool, accountName)

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
		ServiceLevel:     "premium",
	}
	accountName := "test-account"

	result := convertDatastorePoolToModel(datastorePool, accountName)

	assert.Nil(t, result.DeletedAt)
}

func TestCreatePool(t *testing.T) {
	temporal := workflow_engine_mock.NewMockTemporalTestClient(t)
	t.Run("WhenGetOrCreateAccountFails", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		// Create a PersistenceStore instance with the in-memory database
		se, err := database.NewTestStorage(mockLogger)
		if err != nil {
			t.Fatalf("Failed to create test storage: %v", err)
		}
		params := &common.CreatePoolParams{
			AccountName:      "test_account",
			Region:           "test_region",
			Name:             "test_pool",
			VendorID:         "test_vendor",
			SizeInBytes:      1024,
			AllowAutoTiering: true,
			VendorSubNetID:   "test_network",
			CustomPerformanceParams: &common.CustomPerformanceParams{
				Enabled:    true,
				Throughput: 64,
				Iops:       1024,
			},
		}

		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.New("account not found")
		}

		_, _, err = createPool(ctx, se, temporal, params)
		assert.EqualError(tt, err, "account not found")
	})
	t.Run("WhenValidatePoolParamFails", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		// Create a PersistenceStore instance with the in-memory database
		se, err := database.NewTestStorage(mockLogger)
		assert.NoError(tt, err, "Failed to create test storage")
		params := &common.CreatePoolParams{
			AccountName:      "test_account",
			Region:           "test_region",
			Name:             "test_pool",
			VendorID:         "test_vendor",
			SizeInBytes:      1024,
			AllowAutoTiering: true,
			VendorSubNetID:   "test_network",
			CustomPerformanceParams: &common.CustomPerformanceParams{
				Enabled:    true,
				Throughput: 64,
				Iops:       1024,
			},
		}

		dbAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-uuid",
			},
			Name: "test_account",
		}
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
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		// Create a PersistenceStore instance with the in-memory database
		store, err := database.NewTestStorage(mockLogger)
		assert.NoError(tt, err, "Failed to create test storage")
		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		assert.NoError(tt, err, "Failed to clean up test storage")

		params := &common.CreatePoolParams{
			AccountName:      "test_account",
			Region:           "test_region",
			Name:             "test_pool",
			VendorID:         "test_vendor",
			SizeInBytes:      1024,
			AllowAutoTiering: true,
			VendorSubNetID:   "test_network",
			CustomPerformanceParams: &common.CustomPerformanceParams{
				Enabled:    true,
				Throughput: 64,
				Iops:       1024,
			},
		}

		dbAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-uuid",
			},
			Name: "test_account",
		}
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return dbAccount, nil
		}
		ValidateCreatePoolParams = func(params *common.CreatePoolParams) error {
			return nil
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: dbAccount.ID,
		}
		err = store.DB().Create(pool).Error
		assert.NoError(tt, err, "Failed to create pool")

		_, _, err = createPool(ctx, store, temporal, params)
		assert.EqualError(tt, err, "pool already exists")
	})
	t.Run("WhenCreatePoolSucceeds", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		// Create a PersistenceStore instance with the in-memory database
		store, err := database.NewTestStorage(mockLogger)
		if err != nil {
			t.Fatalf("Failed to create test storage: %v", err)
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
		}

		orch := Orchestrator{
			storage:  store,
			temporal: temporal,
		}

		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		params := &common.CreatePoolParams{
			AccountName:      "test_account",
			Region:           "test_region",
			Name:             "test_pool",
			VendorID:         "test_vendor",
			SizeInBytes:      1024,
			AllowAutoTiering: true,
			VendorSubNetID:   "test_network",
			CustomPerformanceParams: &common.CustomPerformanceParams{
				Enabled:    true,
				Throughput: 64,
				Iops:       1024,
			},
		}

		dbAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-uuid",
			},
			Name: "test_account",
		}
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

func TestGetPool(t *testing.T) {
	t.Run("WhenPoolDoesNotExist", func(tt *testing.T) {
		ctx := context.Background()

		mockLogger := log.NewLogger()
		store, err := database.NewTestStorage(mockLogger)
		assert.NoError(tt, err, "Failed to create test storage")

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		assert.NoError(tt, err, "Failed to ClearInMemoryDB")

		orch := Orchestrator{
			storage: store,
		}

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{Name: "test_account"}, nil
		}

		_, err = orch.GetPool(ctx, "non-existent-uuid", "")
		var customErr *vsaerrors.CustomError
		if vsaerrors.As(err, &customErr) {
			assert.EqualError(tt, customErr.Unwrap(), "pool not found")
		} else {
			tt.Errorf("Expected custom error, got %v", err)
		}
	})

	t.Run("WhenPoolExists", func(tt *testing.T) {
		ctx := context.Background()

		mockLogger := log.NewLogger()
		store, err := database.NewTestStorage(mockLogger)
		assert.NoError(tt, err, "Failed to create test storage")

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		assert.NoError(tt, err, "Failed to ClearInMemoryDB")

		orch := Orchestrator{
			storage: store,
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		assert.NoError(tt, err, "Failed to create account")

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
		}
		err = store.DB().Create(pool).Error
		assert.NoError(tt, err, "Failed to create pool")

		result, err := orch.GetPool(ctx, "test-pool-uuid", "")
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, pool.Name, result.Name)
		assert.Equal(tt, account.Name, result.AccountName)
	})
}

func TestGetPoolByVendorID(t *testing.T) {
	t.Run("WhenPoolDoesNotExist", func(tt *testing.T) {
		ctx := context.Background()

		mockLogger := log.NewLogger()
		store, err := database.NewTestStorage(mockLogger)
		assert.NoError(tt, err, "Failed to create test storage")

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		assert.NoError(tt, err, "Failed to ClearInMemoryDB")

		orch := Orchestrator{
			storage: store,
		}

		_, err = orch.GetPoolByVendorID(ctx, "non-existent-vendor-id")
		var customErr *vsaerrors.CustomError
		if vsaerrors.As(err, &customErr) {
			assert.EqualError(tt, customErr.Unwrap(), "pool not found")
		} else {
			tt.Errorf("Expected custom error, got %v", err)
		}
	})

	t.Run("WhenPoolExists", func(tt *testing.T) {
		ctx := context.Background()

		mockLogger := log.NewLogger()
		store, err := database.NewTestStorage(mockLogger)
		assert.NoError(tt, err, "Failed to create test storage")

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			tt.Fatalf("Failed to clean up test storage: %v", err)
		}

		orch := Orchestrator{
			storage: store,
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
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			VendorID:  "test-vendor-id",
		}
		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		result, err := orch.GetPoolByVendorID(ctx, "test-vendor-id")
		if err != nil {
			tt.Errorf("Expected no error, got %v", err)
		}
		assert.Equal(tt, pool.Name, result.Name)
		assert.Equal(tt, account.Name, result.AccountName)
	})
}

func TestValidateCreatePoolParams(t *testing.T) {
	t.Run("ValidateCreatePoolParams_WithValidSize_ReturnsNil", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:  2199023255552, // Exactly the minimum quota
			ServiceLevel: ServiceLevelNameFLEX,
			QosType:      QosTypeAuto,
		}

		err := _validateCreatePoolParams(params)
		assert.NoError(t, err, "Expected no error, got %v", err)
	})
	t.Run("ValidateCreatePoolParams_WithSizeBelowMinimum_ReturnsError", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes: 1024, // Below the minimum quota
		}

		err := _validateCreatePoolParams(params)
		assert.EqualError(t, err, "Given pool size not supported. Pool size can't be less than 2TiB")
	})
	t.Run("ValidateCreatePoolParams_WithWrongServiceLevel", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:  2199023255552, // Exactly the minimum quota
			ServiceLevel: "Premium",
			QosType:      QosTypeAuto,
		}

		err := _validateCreatePoolParams(params)
		assert.EqualError(t, err, "Given service level not supported. Supported service level is "+ServiceLevelNameFLEX)
	})
	t.Run("ValidateCreatePoolParams_WithValidSize_WrongQosType", func(tt *testing.T) {
		params := &common.CreatePoolParams{
			SizeInBytes:  2199023255552, // Exactly the minimum quota
			ServiceLevel: ServiceLevelNameFLEX,
			QosType:      "Manual",
		}

		err := _validateCreatePoolParams(params)
		assert.EqualError(t, err, "Given QoS type not supported. Supported QoS type is "+QosTypeAuto)
	})
}

func TestDeletePool(t *testing.T) {
	t.Run("WhenGetOrCreateAccountFails", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		se := database.Storage(nil)
		temporal := client.Client(nil)

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
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		// Create a PersistenceStore instance with the in-memory database
		store, err := database.NewTestStorage(mockLogger)
		assert.NoError(tt, err, "Failed to create test storage")
		temporal := client.Client(nil)
		assert.NoError(tt, err, "Failed to create temporal client")

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
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
	t.Run("WhenDeletePoolSucceeds1", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		store, err := database.NewTestStorage(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}

		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			tt.Fatalf("Failed to clean up test storage: %v", err)
		}
		temporal := workflow_engine_mock.NewMockTemporalTestClient(t)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			t.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
		}
		err = store.DB().Create(pool).Error
		if err != nil {
			t.Fatalf("Failed to create pool: %v", err)
		}

		params := &common.DeletePoolParams{
			AccountName: account.Name,
			PoolID:      pool.UUID,
		}

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}

		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		result, jobID, err := _deletePool(ctx, temporal, store, params)
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
		if result.Name != pool.Name {
			t.Errorf("Expected pool name %v, got %v", pool.Name, result.Name)
		}
		if jobID == "" {
			t.Errorf("Expected job ID, got empty string")
		}
	})
}

func TestMultiplePools(t *testing.T) {
	t.Run("ReturnsErrorWhenAccountDoesNotExist", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		store, err := database.NewTestStorage(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}

		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			tt.Fatalf("Failed to clean up test storage: %v", err)
		}

		orch := Orchestrator{storage: store}

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.NewNotFoundErr("account not found", nil)
		}
		_, err = orch.GetMultiplePools(ctx, "non-existent-account", []string{"uuid1", "uuid2"})
		if err == nil {
			t.Errorf("Expected error, got nil")
		}
		if !errors.IsNotFoundErr(err) {
			t.Errorf("Expected not found error, got %v", err)
		}
	})
	t.Run("ReturnsErrorWhenNoPoolsMatchUUIDs", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		store, err := database.NewTestStorage(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}

		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			tt.Fatalf("Failed to clean up test storage: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			t.Fatalf("Failed to create account: %v", err)
		}

		orch := Orchestrator{storage: store}

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}

		pools, err := orch.GetMultiplePools(ctx, account.Name, []string{"non-existent-uuid"})
		assert.NoError(tt, err)
		if len(pools) != 0 {
			tt.Fatalf("Expected 0 pools, got %v", len(pools))
		}
	})
	t.Run("ReturnsPoolsSuccessfullyWhenUUIDsMatch", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		store, err := database.NewTestStorage(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}

		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			tt.Fatalf("Failed to clean up test storage: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			t.Fatalf("Failed to create account: %v", err)
		}

		pool1 := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid1"},
			Name:      "test_pool_1",
			AccountID: account.ID,
		}
		pool2 := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid2"},
			Name:      "test_pool_2",
			AccountID: account.ID,
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

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}

		result, err := orch.GetMultiplePools(ctx, account.Name, []string{"test-pool-uuid1", "test-pool-uuid2"})
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
		if len(result) != 2 {
			t.Errorf("Expected 2 pools, got %d", len(result))
		}
		if result[0].Name != pool1.Name || result[1].Name != pool2.Name {
			t.Errorf("Returned pools do not match expected pools")
		}
	})
}

func TestListPools(t *testing.T) {
	t.Run("ReturnsErrorWhenAccountDoesNotExist", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		store, err := database.NewTestStorage(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}

		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			tt.Fatalf("Failed to clean up test storage: %v", err)
		}

		orch := Orchestrator{storage: store}

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.NewNotFoundErr("account not found", nil)
		}

		_, err = orch.ListPools(ctx, "non-existent-account")
		if err == nil {
			tt.Errorf("Expected error, got nil")
		}
		if !errors.IsNotFoundErr(err) {
			tt.Errorf("Expected not found error, got %v", err)
		}
	})

	t.Run("ReturnsEmptyListWhenNoPoolsExist", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		store, err := database.NewTestStorage(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}

		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			tt.Fatalf("Failed to clean up test storage: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		orch := Orchestrator{storage: store}

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}

		pools, err := orch.ListPools(ctx, account.Name)
		if err != nil {
			tt.Errorf("Expected no error, got %v", err)
		}
		if len(pools) != 0 {
			tt.Errorf("Expected 0 pools, got %d", len(pools))
		}
	})

	t.Run("ReturnsPoolsSuccessfully", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		store, err := database.NewTestStorage(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}

		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			tt.Fatalf("Failed to clean up test storage: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool1 := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid1"},
			Name:      "test_pool_1",
			AccountID: account.ID,
		}
		pool2 := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid2"},
			Name:      "test_pool_2",
			AccountID: account.ID,
		}
		err = store.DB().Create(pool1).Error
		if err != nil {
			tt.Fatalf("Failed to create pool1: %v", err)
		}
		err = store.DB().Create(pool2).Error
		if err != nil {
			tt.Fatalf("Failed to create pool2: %v", err)
		}

		orch := Orchestrator{storage: store}

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}

		pools, err := orch.ListPools(ctx, account.Name)
		if err != nil {
			tt.Errorf("Expected no error, got %v", err)
		}
		if len(pools) != 2 {
			tt.Errorf("Expected 2 pools, got %d", len(pools))
		}
		if pools[0].Name != pool1.Name || pools[1].Name != pool2.Name {
			tt.Errorf("Returned pools do not match expected pools")
		}
	})
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
		mockStorage.On("GetPoolByName", ctx, mock.Anything).Return(poolResp, nil)
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

		mockStorage.On("GetPoolByName", ctx, mock.Anything).Return(poolResp, nil)
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

		mockStorage.On("GetPoolByName", ctx, mock.Anything).Return(poolResp, nil)
		mockStorage.On("GetNodesByPoolID", ctx, mock.Anything).Return(nodeResp, nil)

		getInterClusterLifsFromONTAP = func(ctx context.Context, node []*datamodel.Node, pools *datamodel.Pool) ([]*vsa.InterclusterLif, error) {
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
		}
		nodeResp := []*datamodel.Node{{
			BaseModel: datamodel.BaseModel{UUID: "test-node-uuid", ID: 1},
			Name:      "test-node",
		},
		}
		mockStorage.On("GetPoolByName", ctx, mock.Anything).Return(poolResp, nil)
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

		getInterClusterLifsFromONTAP = func(ctx context.Context, node []*datamodel.Node, pools *datamodel.Pool) ([]*vsa.InterclusterLif, error) {
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
		}
		nodeResp := []*datamodel.Node{
			{
				BaseModel: datamodel.BaseModel{UUID: "test-node-uuid", ID: 1},
				Name:      "test-node",
			},
		}
		mockStorage.On("GetPoolByName", ctx, mock.Anything).Return(poolResp, nil)
		mockStorage.On("GetNodesByPoolID", ctx, mock.Anything).Return(nodeResp, nil)

		_, err := GetPoolByName(ctx, mockStorage, "test-pool", "test-account", queryDepthZero)
		assert.NoError(tt, err)
	})
}
