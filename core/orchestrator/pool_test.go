package orchestrator

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
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
		Name:         "Test Pool",
		Description:  "Test Description",
		SizeInBytes:  1024,
		State:        "active",
		StateDetails: "running",
		CoolAccess:   true,
		Network:      "test-network",
		ServiceLevel: "premium",
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
	assert.Equal(t, datastorePool.CoolAccess, result.CoolAccess)
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
		Name:         "Test Pool",
		Description:  "Test Description",
		SizeInBytes:  1024,
		State:        "active",
		StateDetails: "running",
		CoolAccess:   true,
		Network:      "test-network",
		ServiceLevel: "premium",
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
		Name:         "Test Pool",
		Description:  "Test Description",
		SizeInBytes:  1024,
		State:        "active",
		StateDetails: "running",
		CoolAccess:   true,
		Network:      "test-network",
		ServiceLevel: "premium",
	}
	accountName := "test-account"

	result := convertDatastorePoolToModel(datastorePool, accountName)

	assert.Nil(t, result.DeletedAt)
}

func TestCreatePool(t *testing.T) {
	t.Run("WhenGetOrCreateAccountFails", func(tt *testing.T) {
		ctx := context.Background()
		se := database.Storage(nil)

		params := &CreatePoolParams{
			AccountName:    "test_account",
			Region:         "test_region",
			Name:           "test_pool",
			VendorID:       "test_vendor",
			SizeInBytes:    1024,
			CoolAccess:     true,
			VendorSubNetID: "test_network",
			CustomPerformanceParams: &CustomPerformanceParams{
				Enabled:    true,
				Throughput: 64,
				Iops:       1024,
			},
		}

		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.New("account not found")
		}

		_, err := createPool(ctx, se, params)
		if err == nil {
			t.Errorf("Expected error, got nil")
		}
		if err.Error() != "account not found" {
			t.Errorf("Expected error 'account not found', got %v", err)
		}
	})
	t.Run("WhenValidatePoolParamFails", func(tt *testing.T) {
		ctx := context.Background()
		se := database.Storage(nil)

		params := &CreatePoolParams{
			AccountName:    "test_account",
			Region:         "test_region",
			Name:           "test_pool",
			VendorID:       "test_vendor",
			SizeInBytes:    1024,
			CoolAccess:     true,
			VendorSubNetID: "test_network",
			CustomPerformanceParams: &CustomPerformanceParams{
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
		validateCreatePoolParams = func(se database.Storage, params *CreatePoolParams) error {
			return errors.New("invalid pool params")
		}

		_, err := createPool(ctx, se, params)
		if err == nil {
			t.Errorf("Expected error, got nil")
		}
		if err.Error() != "invalid pool params" {
			t.Errorf("Expected error 'invalid pool params', got %v", err)
		}
	})
	t.Run("WhenCreatePoolFails", func(tt *testing.T) {
		ctx := context.Background()

		mockLogger := log.NewLogger()
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
			storage: store,
		}

		params := &CreatePoolParams{
			AccountName:    "test_account",
			Region:         "test_region",
			Name:           "test_pool",
			VendorID:       "test_vendor",
			SizeInBytes:    1024,
			CoolAccess:     true,
			VendorSubNetID: "test_network",
			CustomPerformanceParams: &CustomPerformanceParams{
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
		validateCreatePoolParams = func(se database.Storage, params *CreatePoolParams) error {
			return nil
		}

		_, err = createPool(ctx, store, params)
		if err != nil {
			t.Errorf("Expected nil, got error")
		}
		_, err = orch.CreatePool(ctx, params)
		if err == nil {
			t.Errorf("Expected error, got nil")
		}
		if err.Error() != "pool already exists" {
			t.Errorf("Expected error 'pool already exists', got %v", err)
		}
	})
	t.Run("WhenCreatePoolSucceeds", func(tt *testing.T) {
		ctx := context.Background()

		mockLogger := log.NewLogger()
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
			storage: store,
		}

		params := &CreatePoolParams{
			AccountName:    "test_account",
			Region:         "test_region",
			Name:           "test_pool",
			VendorID:       "test_vendor",
			SizeInBytes:    1024,
			CoolAccess:     true,
			VendorSubNetID: "test_network",
			CustomPerformanceParams: &CustomPerformanceParams{
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
		validateCreatePoolParams = func(se database.Storage, params *CreatePoolParams) error {
			return nil
		}

		pool, err := orch.CreatePool(ctx, params)
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
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
		}

		orch := Orchestrator{
			storage: store,
		}

		_, err = orch.GetPool(ctx, "non-existent-uuid")
		if err == nil {
			tt.Errorf("Expected error, got nil")
		}
		if err != gorm.ErrRecordNotFound {
			tt.Errorf("Expected error %v, got %v", gorm.ErrRecordNotFound, err)
		}
	})

	t.Run("WhenPoolExists", func(tt *testing.T) {
		ctx := context.Background()

		mockLogger := log.NewLogger()
		store, err := database.NewTestStorage(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
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
		}
		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		result, err := orch.GetPool(ctx, "test-pool-uuid")
		if err != nil {
			tt.Errorf("Expected no error, got %v", err)
		}
		assert.Equal(tt, pool.Name, result.Name)
		assert.Equal(tt, account.Name, result.AccountName)
	})
}

func TestGetPoolByVendorID(t *testing.T) {
	t.Run("WhenPoolDoesNotExist", func(tt *testing.T) {
		ctx := context.Background()

		mockLogger := log.NewLogger()
		store, err := database.NewTestStorage(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			tt.Fatalf("Failed to clean up test storage: %v", err)
		}

		orch := Orchestrator{
			storage: store,
		}

		_, err = orch.GetPoolByVendorID(ctx, "non-existent-vendor-id")
		if err == nil {
			tt.Errorf("Expected error, got nil")
		}
		if err.Error() != "pool not found" {
			tt.Errorf("Expected error %v, got %v", "pool not found", err)
		}
	})

	t.Run("WhenPoolExists", func(tt *testing.T) {
		ctx := context.Background()

		mockLogger := log.NewLogger()
		store, err := database.NewTestStorage(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}

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
	se := database.Storage(nil)

	params := &CreatePoolParams{
		SizeInBytes: 1024, // Below the minimum quota
	}

	err := _validateCreatePoolParams(se, params)
	if err == nil {
		t.Errorf("Expected error, got nil")
	}
	if err.Error() != "Given pool size not supported. Pool size can't be less than 2TiB" {
		t.Errorf("Expected error 'Given pool size not supported. Pool size can't be less than 2TiB', got %v", err)
	}
}

func TestValidateCreatePoolParams_WithValidSize_ReturnsNil(t *testing.T) {
	se := database.Storage(nil)

	params := &CreatePoolParams{
		SizeInBytes: 2199023255552, // Exactly the minimum quota
	}

	err := _validateCreatePoolParams(se, params)
	if err != nil {
		t.Errorf("Expected nil, got error: %v", err)
	}
}
