package orchestrator

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
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
	// t.Run("WhenCreatePoolSucceeds", func(tt *testing.T) {
	//	ctx := context.Background()
	//	mockLogger := log.NewLogger()
	//	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
	//	// Create a PersistenceStore instance with the in-memory database
	//	store, err := database.NewTestStorage(mockLogger)
	//	if err != nil {
	//		t.Fatalf("Failed to create test storage: %v", err)
	//	}
	//
	//	// Clear the in-memory database
	//	err = database.ClearInMemoryDB(store.DB())
	//	if err != nil {
	//		t.Fatalf("Failed to clean up test storage: %v", err)
	//	}
	//
	//	orch := Orchestrator{
	//		storage:  store,
	//		temporal: temporal,
	//	}
	//
	//	temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	//	params := &common.CreatePoolParams{
	//		AccountName:      "test_account",
	//		Region:           "test_region",
	//		Name:             "test_pool",
	//		VendorID:         "test_vendor",
	//		SizeInBytes:      1024,
	//		AllowAutoTiering: true,
	//		VendorSubNetID:   "test_network",
	//		CustomPerformanceParams: &common.CustomPerformanceParams{
	//			Enabled:    true,
	//			Throughput: 64,
	//			Iops:       1024,
	//		},
	//	}
	//
	//	dbAccount := &datamodel.Account{
	//		BaseModel: datamodel.BaseModel{
	//			UUID: "test-uuid",
	//		},
	//		Name: "test_account",
	//	}
	//	getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
	//		return dbAccount, nil
	//	}
	//	ValidateCreatePoolParams = func(params *common.CreatePoolParams) error {
	//		return nil
	//	}
	//
	//	pool, _, err := orch.CreatePool(ctx, params)
	//	if err != nil {
	//		t.Errorf("Expected nil, got error")
	//	}
	//	assert.Equal(t, pool.Name, params.Name)
	//	assert.Equal(t, pool.VendorSubNetID, params.VendorSubNetID)
	//	assert.Equal(t, pool.AccountName, params.AccountName)
	// })
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
		assert.EqualError(tt, err, "pool not found")
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
		assert.EqualError(tt, err, "pool not found")
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

func TestValidateCreatePoolParams_WithSizeBelowMinimum_ReturnsError(t *testing.T) {
	params := &common.CreatePoolParams{
		SizeInBytes: 1024, // Below the minimum quota
	}

	err := _validateCreatePoolParams(params)
	assert.EqualError(t, err, "Given pool size not supported. Pool size can't be less than 2TiB")
}

func TestValidateCreatePoolParams_WithValidSize_ReturnsNil(t *testing.T) {
	params := &common.CreatePoolParams{
		SizeInBytes:  2199023255552, // Exactly the minimum quota
		ServiceLevel: ServiceLevelNameFLEX,
		QosType:      QosTypeAuto,
	}

	err := _validateCreatePoolParams(params)
	assert.NoError(t, err, "Expected no error, got %v", err)
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
		assert.EqualError(tt, err, "pool not found")
	})
	// t.Run("WhenDeletePoolSucceeds", func(tt *testing.T) {
	//	ctx := context.Background()
	//	mockLogger := log.NewLogger()
	//	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
	//	store, err := database.NewTestStorage(mockLogger)
	//	if err != nil {
	//		tt.Fatalf("Failed to create test storage: %v", err)
	//	}
	//
	//	err = database.ClearInMemoryDB(store.DB())
	//	if err != nil {
	//		tt.Fatalf("Failed to clean up test storage: %v", err)
	//	}
	//
	//	workflowClient := workflow_engine.TemporalWorkflowEngine{}
	//	workflowCfg := workflowClient.LoadConfig()
	//	err = workflowClient.InitializeClient(workflowCfg, mockLogger)
	//	if err != nil {
	//		t.Fatalf("Failed to initialize workflow client: %v", err)
	//	}
	//
	//	orch := Orchestrator{
	//		storage:  store,
	//		temporal: temporal,
	//	}
	//	temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	//
	//	// Create account
	//	account := &datamodel.Account{
	//		BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
	//		Name:      "test_account",
	//	}
	//	err = store.DB().Create(account).Error
	//	if err != nil {
	//		tt.Fatalf("Failed to create account: %v", err)
	//	}
	//
	//	// Create pool
	//	pool := &datamodel.Pool{
	//		BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
	//		Name:      "test_pool",
	//		AccountID: account.ID,
	//	}
	//	err = store.DB().Create(pool).Error
	//	if err != nil {
	//		tt.Fatalf("Failed to create pool: %v", err)
	//	}
	//
	//	// Create SVM
	//	svm := &datamodel.Svm{
	//		BaseModel: datamodel.BaseModel{UUID: "test-svm-uuid"},
	//		Name:      "test_svm",
	//		PoolID:    pool.ID,
	//		AccountID: account.ID,
	//	}
	//	err = store.DB().Create(svm).Error
	//	if err != nil {
	//		tt.Fatalf("Failed to create SVM: %v", err)
	//	}
	//
	//	// Create nodes
	//	node1 := &datamodel.Node{
	//		BaseModel: datamodel.BaseModel{UUID: "test-node-uuid1"},
	//		Name:      "test_node1",
	//		PoolID:    pool.ID,
	//		AccountID: account.ID,
	//	}
	//	err = store.DB().Create(node1).Error
	//	if err != nil {
	//		tt.Fatalf("Failed to create node: %v", err)
	//	}
	//
	//	node2 := &datamodel.Node{
	//		BaseModel: datamodel.BaseModel{UUID: "test-node-uuid2"},
	//		Name:      "test_node2",
	//		PoolID:    pool.ID,
	//		AccountID: account.ID,
	//	}
	//	err = store.DB().Create(node2).Error
	//	if err != nil {
	//		tt.Fatalf("Failed to create node: %v", err)
	//	}
	//
	//	// Create LIF
	//	lif1 := &datamodel.Lif{
	//		BaseModel: datamodel.BaseModel{UUID: "test-lif-uuid1"},
	//		Name:      "test_lif1",
	//		NodeID:    node1.ID,
	//		AccountID: account.ID,
	//	}
	//	err = store.DB().Create(lif1).Error
	//	if err != nil {
	//		tt.Fatalf("Failed to create LIF: %v", err)
	//	}
	//
	//	lif2 := &datamodel.Lif{
	//		BaseModel: datamodel.BaseModel{UUID: "test-lif-uuid2"},
	//		Name:      "test_lif2",
	//		NodeID:    node2.ID,
	//		AccountID: account.ID,
	//	}
	//	err = store.DB().Create(lif2).Error
	//	if err != nil {
	//		tt.Fatalf("Failed to create LIF: %v", err)
	//	}
	//
	//	// Delete pool
	//	params := &common.DeletePoolParams{
	//		AccountName: "test_account",
	//		PoolID:      "test-pool-uuid",
	//	}
	//
	//	result, _, err := orch.DeletePool(ctx, params)
	//	if err != nil {
	//		tt.Errorf("Expected nil, got error: %v", err)
	//	}
	//	assert.Equal(tt, pool.Name, result.Name)
	//	assert.Equal(tt, account.Name, result.AccountName)
	// })
}
