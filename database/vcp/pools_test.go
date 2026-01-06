package database

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils/gorm"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func setup(t *testing.T) *DataStoreRepository {
	db, err := SetupTestDB()
	if err != nil {
		t.Fatalf("Failed to set up test database: %v", err)
	}
	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	if err != nil {
		t.Fatalf("Failed to clean up test database: %v", err)
	}
	return store
}

func TestDataStoreRepository_ListPoolUUIDsPaginated_WithPagination(t *testing.T) {
	store := setup(t)
	ctx := context.Background()

	// Create test data
	pools, account := createDBPools(t, store)
	defer store.db.Delete(account)

	// Test with pagination
	filter := &utils.Filter{}
	offset := 0
	limit := 1

	results, err := store.ListPoolUUIDsPaginated(ctx, filter, offset, limit)
	assert.NoError(t, err)
	assert.Len(t, results, 1)
	assert.Equal(t, pools[0].UUID, results[0].UUID)
}

func TestDataStoreRepository_ListPoolUUIDsPaginated_WithOffset(t *testing.T) {
	store := setup(t)
	ctx := context.Background()

	// Create test data
	pools, account := createDBPools(t, store)
	defer store.db.Delete(account)

	// Test with offset
	filter := &utils.Filter{}
	offset := 1
	limit := 1

	results, err := store.ListPoolUUIDsPaginated(ctx, filter, offset, limit)
	assert.NoError(t, err)
	assert.Len(t, results, 1)
	assert.Equal(t, pools[2].UUID, results[0].UUID)
}

func TestDataStoreRepository_GetPoolsCount_WithFilter(t *testing.T) {
	store := setup(t)
	ctx := context.Background()

	// Create test data
	_, account := createDBPools(t, store)
	defer store.db.Delete(account)

	// Test with filter
	filter := &utils.Filter{}
	filter.SetIncludeDeleted(false)

	count, err := store.GetPoolsCount(ctx, filter)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)
}

func TestDataStoreRepository_GetPoolsCount_WithDeletedFilter(t *testing.T) {
	store := setup(t)
	ctx := context.Background()

	// Create test data
	pools, account := createDBPools(t, store)
	defer store.db.Delete(account)

	// Soft delete a pool
	err := store.db.Delete(pools[0]).Error()
	assert.NoError(t, err)

	// Test with deleted filter
	filter := &utils.Filter{}
	filter.IncludeDeleted = true

	count, err := store.GetPoolsCount(ctx, filter)
	assert.NoError(t, err)
	assert.Equal(t, int64(3), count)
}

func TestDataStoreRepository_GetPoolsCount_WithoutFilter(t *testing.T) {
	store := setup(t)
	ctx := context.Background()

	// Create test data
	_, account := createDBPools(t, store)
	defer store.db.Delete(account)

	// Test without filter
	count, err := store.GetPoolsCount(ctx, nil)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)
}

func createDBPools(t *testing.T, store *DataStoreRepository) ([]*datamodel.Pool, *datamodel.Account) {
	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
		Name:      "test_account",
	}
	err := store.db.Create(account).Error()
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

	err = store.db.Create(pool1).Error()
	assert.NoError(t, err)
	err = store.db.Create(deletedPool).Error()
	assert.NoError(t, err)
	err = store.db.Create(pool2).Error()
	assert.NoError(t, err)

	var pools []*datamodel.Pool
	store.db.GORM().Find(&pools)

	return []*datamodel.Pool{pool1, deletedPool, pool2}, account
}

func TestGetPool(t *testing.T) {
	t.Run("WhenPoolExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			tt.Fatalf("Failed to clean up test database: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test_account",
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			Account:   account,
		}
		err = store.db.Create(account).Error()
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}
		err = store.db.Create(pool).Error()
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		result, err := store.GetPool(context.Background(), "test-pool-uuid", 0)
		if err != nil {
			tt.Errorf("Expected no error, got %v", err)
		}
		if result.Name != pool.Name {
			tt.Errorf("Expected pool name %v, got %v", pool.Name, result.Name)
		}
		if result.Account.Name != account.Name {
			tt.Errorf("Expected account name %v, got %v", account.Name, result.Account.Name)
		}
	})

	t.Run("WhenPoolDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			tt.Fatalf("Failed to clean up test database: %v", err)
		}

		_, err = store.GetPool(context.Background(), "test-pool-uuid", 0)
		if err == nil {
			tt.Errorf("Expected error, got nil")
		}
		if !customerrors.IsNotFoundErr(err) {
			tt.Errorf("Expected error %v, got %v", gorm.ErrRecordNotFound, err)
		}
	})
}

func TestDescribePool(t *testing.T) {
	t.Run("WhenPoolExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			tt.Fatalf("Failed to clean up test database: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test_account",
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			Account:   account,
		}
		err = store.db.Create(account).Error()
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}
		err = store.db.Create(pool).Error()
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		result, err := store.DescribePool(context.Background(), "test-pool-uuid", 0)
		if err != nil {
			tt.Errorf("Expected no error, got %v", err)
		}
		if result.Name != pool.Name {
			tt.Errorf("Expected pool name %v, got %v", pool.Name, result.Name)
		}
		if result.Account.Name != account.Name {
			tt.Errorf("Expected account name %v, got %v", account.Name, result.Account.Name)
		}
	})

	t.Run("WhenPoolDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			tt.Fatalf("Failed to clean up test database: %v", err)
		}

		_, err = store.DescribePool(context.Background(), "test-pool-uuid", 0)
		if err == nil {
			tt.Errorf("Expected error, got nil")
		}
		if !customerrors.IsNotFoundErr(err) {
			tt.Errorf("Expected error %v, got %v", gorm.ErrRecordNotFound, err)
		}
	})
	t.Run("WhenPoolExistsButDeleted", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			tt.Fatalf("Failed to clean up test database: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test_account",
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid", DeletedAt: &gorm.DeletedAt{Time: time.Now(), Valid: true}},
			Name:      "test_pool",
			AccountID: account.ID,
			Account:   account,
		}
		err = store.db.Create(account).Error()
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}
		err = store.db.Create(pool).Error()
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		result, err := store.DescribePool(context.Background(), "test-pool-uuid", 0)
		if err != nil {
			tt.Errorf("Expected no error, got %v", err)
		}
		if result.Name != pool.Name {
			tt.Errorf("Expected pool name %v, got %v", pool.Name, result.Name)
		}
		if result.Account.Name != account.Name {
			tt.Errorf("Expected account name %v, got %v", account.Name, result.Account.Name)
		}
	})
}

func TestListPools(t *testing.T) {
	t.Run("WhenPoolsExist", func(tt *testing.T) {
		store := setup(tt)

		pools, _ := createDBPools(tt, store)

		result, err := store.ListPools(context.Background(), nil)
		assert.NoError(tt, err)
		assert.Len(tt, result, len(pools)-1) // Exclude deleted pool
	})

	t.Run("WhenPoolsExistIncludeDeleted", func(tt *testing.T) {
		store := setup(tt)

		pools, _ := createDBPools(tt, store)

		filter := &utils.Filter{}
		filter.SetIncludeDeleted(true)
		result, err := store.ListPools(context.Background(), filter)
		assert.NoError(tt, err)
		assert.Len(tt, result, len(pools)) // Exclude deleted pool
	})

	t.Run("WhenPoolDoesNotExist", func(tt *testing.T) {
		store := setup(tt)

		pools, err := store.ListPools(context.Background(), nil)
		assert.NoError(tt, err)
		assert.Len(tt, pools, 0)
	})
}

func TestGetPoolWithVendorID(t *testing.T) {
	t.Run("WhenPoolExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			tt.Fatalf("Failed to clean up test database: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test_account",
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			VendorID:  "test-pool-vendor-id",
			AccountID: account.ID,
			Account:   account,
		}
		err = store.db.Create(account).Error()
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}
		err = store.db.Create(pool).Error()
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		result, err := store.GetPoolByVendorID(context.Background(), "test-pool-vendor-id", account.ID)
		if err != nil {
			tt.Errorf("Expected no error, got %v", err)
		}
		if result.Name != pool.Name {
			tt.Errorf("Expected pool name %v, got %v", pool.Name, result.Name)
		}
		if result.Account.Name != account.Name {
			tt.Errorf("Expected account name %v, got %v", account.Name, result.Account.Name)
		}
	})

	t.Run("WhenPoolDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			tt.Fatalf("Failed to clean up test database: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test_account",
		}
		err = store.db.Create(account).Error()
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		_, err = store.GetPoolByVendorID(context.Background(), "test-pool-vendor-id", account.ID)
		if err == nil {
			tt.Errorf("Expected error, got nil")
		}
		if !customerrors.IsNotFoundErr(err) {
			tt.Errorf("Expected error %v, got %v", gorm.ErrRecordNotFound, err)
		}
	})
}

func TestCreatePool(t *testing.T) {
	t.Run("WhenPoolIsCreatedSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			tt.Fatalf("Failed to clean up test database: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test_account",
		}
		err = store.db.Create(account).Error()
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			Name:    "test_pool",
			Account: account,
		}

		createdPool, err := store.CreatingPool(context.Background(), pool)
		if err != nil {
			tt.Errorf("Expected no error, got %v", err)
		}
		if createdPool.Name != pool.Name {
			tt.Errorf("Expected pool name %v, got %v", pool.Name, createdPool.Name)
		}
		if createdPool.State != models.LifeCycleStateCreating {
			tt.Errorf("Expected pool state %v, got %v", models.LifeCycleStateCreating, createdPool.State)
		}
	})
	t.Run("WhenPoolAlreadyExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			tt.Fatalf("Failed to clean up test database: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test_account",
		}
		err = store.db.Create(account).Error()
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			Name:    "test_pool",
			Account: account,
		}
		err = store.db.Create(pool).Error()
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		_, err = store.CreatingPool(context.Background(), pool)
		if err == nil {
			tt.Errorf("Expected error, got nil")
		}
		if err.Error() != "Invalid input parameters provided" {
			tt.Errorf("Expected error 'Invalid input parameters provided', got %v", err)
		}
		assert.Contains(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "pool already exists")
	})
}

func TestSavePoolWithVsaClusterDetails(t *testing.T) {
	t.Run("WhenPoolAndAccountExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			tt.Fatalf("Failed to clean up test database: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test_account",
		}
		err = store.db.Create(account).Error()
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			Account:   account,
		}
		err = store.db.Create(pool).Error()
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		clusterDetails := &datamodel.ClusterDetails{
			ExternalName: "test-cluster",
			OntapVersion: "9.10.1",
		}

		err = store.SavePoolWithVsaDetails(context.Background(), pool, clusterDetails)
		if err != nil {
			tt.Errorf("Expected no error, got %v", err)
		}

		updatedPool := &datamodel.Pool{}
		err = store.db.GORM().First(updatedPool, "uuid = ?", pool.UUID).Error
		if err != nil {
			tt.Fatalf("Failed to fetch updated pool: %v", err)
		}
		if updatedPool.ClusterDetails.ExternalName != clusterDetails.ExternalName {
			tt.Errorf("Expected external name %v, got %v", clusterDetails.ExternalName, updatedPool.ClusterDetails.ExternalName)
		}
		if updatedPool.ClusterDetails.OntapVersion != clusterDetails.OntapVersion {
			tt.Errorf("Expected ONTAP version %v, got %v", clusterDetails.OntapVersion, updatedPool.ClusterDetails.OntapVersion)
		}
	})
}

func TestDeletePool(t *testing.T) {
	t.Run("ReturnsErrorWhenPoolDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			t.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			t.Fatalf("Failed to clean up test database: %v", err)
		}

		pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "non-existent-uuid"}}

		_, err = store.GetPool(context.Background(), pool.UUID, 0)
		if err == nil {
			t.Errorf("Expected error, got nil")
		}
		if !customerrors.IsNotFoundErr(err) {
			t.Errorf("Expected not found error, got %v", err)
		}
	})

	t.Run("UpdatesPoolStateSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			t.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			t.Fatalf("Failed to clean up test database: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test_account",
		}
		err = store.db.Create(account).Error()
		if err != nil {
			t.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			Account:   account,
			State:     models.LifeCycleStateCreating,
		}
		err = store.db.Create(pool).Error()
		if err != nil {
			t.Fatalf("Failed to create pool: %v", err)
		}

		pool.State = models.LifeCycleStateREADY
		pool.StateDetails = models.LifeCycleStateAvailableDetails

		_, err = store.UpdatedPool(context.Background(), pool)
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		updatedPool := &datamodel.Pool{}
		err = store.db.GORM().First(updatedPool, "uuid = ?", pool.UUID).Error
		if err != nil {
			t.Fatalf("Failed to fetch updated pool: %v", err)
		}
		if updatedPool.State != models.LifeCycleStateREADY {
			t.Errorf("Expected state %v, got %v", models.LifeCycleStateREADY, updatedPool.State)
		}
		if updatedPool.StateDetails != models.LifeCycleStateAvailableDetails {
			t.Errorf("Expected state details %v, got %v", models.LifeCycleStateAvailableDetails, updatedPool.StateDetails)
		}
	})
	t.Run("DeletesPoolSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			t.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			t.Fatalf("Failed to clean up test database: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test_account",
		}
		err = store.db.Create(account).Error()
		if err != nil {
			t.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			Account:   account,
		}
		err = store.db.Create(pool).Error()
		if err != nil {
			t.Fatalf("Failed to create pool: %v", err)
		}

		err = store.DeletePool(context.Background(), pool)
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		deletedPool := &datamodel.Pool{}
		err = store.db.GORM().First(deletedPool, "uuid = ?", pool.UUID).Error
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			t.Errorf("Expected record not found error, got %v", err)
		}
	})
}

func TestDeletingPool(t *testing.T) {
	t.Run("ReturnsErrorWhenPoolDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			tt.Fatalf("Failed to clean up test database: %v", err)
		}

		pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "non-existent-uuid"}}

		err = store.DeletingPool(context.Background(), pool)
		if err == nil {
			tt.Errorf("Expected error, got nil")
		}
		assert.Error(tt, err)
	})

	t.Run("UpdatesPoolStateToDeletingSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			tt.Fatalf("Failed to clean up test database: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test_account",
		}
		err = store.db.Create(account).Error()
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			Account:   account,
			State:     models.LifeCycleStateREADY,
		}
		err = store.db.Create(pool).Error()
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		err = store.DeletingPool(context.Background(), pool)
		if err != nil {
			tt.Errorf("Expected no error, got %v", err)
		}

		updatedPool := &datamodel.Pool{}
		err = store.db.GORM().First(updatedPool, "uuid = ?", pool.UUID).Error
		if err != nil {
			tt.Fatalf("Failed to fetch updated pool: %v", err)
		}
		if updatedPool.State != models.LifeCycleStateDeleting {
			tt.Errorf("Expected state %v, got %v", models.LifeCycleStateDeleting, updatedPool.State)
		}
		if updatedPool.StateDetails != models.LifeCycleStateDeletingDetails {
			tt.Errorf("Expected state details %v, got %v", models.LifeCycleStateDeletingDetails, updatedPool.StateDetails)
		}
	})
}

func TestUpdatedPool(t *testing.T) {
	t.Run("ReturnsErrorWhenPoolDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			tt.Fatalf("Failed to clean up test database: %v", err)
		}

		// Create a pool instance with a UUID that does not exist in DB.
		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "non-existent-uuid"},
		}

		_, err = store.UpdatedPool(context.Background(), pool)
		if err == nil {
			tt.Error("Expected error, got nil")
		}
		// Check that the error is a not found error.
		if !errors.Is(err, gorm.ErrRecordNotFound) &&
			!customerrors.IsNotFoundErr(err) {
			tt.Errorf("Expected not found error, got %v", err)
		}
	})
	t.Run("UpdatesPoolStateSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			tt.Fatalf("Failed to clean up test database: %v", err)
		}

		// Create account for the pool.
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test_account",
		}
		err = store.db.Create(account).Error()
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		// Create a pool with an initial state.
		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			Account:   account,
			State:     models.LifeCycleStateCreating,
			PoolAttributes: &datamodel.PoolAttributes{
				Iops:            100,
				ThroughputMibps: 100,
			},
		}
		err = store.db.Create(pool).Error()
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		// Sleep to ensure UpdatedAt will change.
		time.Sleep(10 * time.Millisecond)

		label := "label"
		labels := make(datamodel.JSONB)
		labels["test"] = label
		// Setting new state values.
		pool.State = models.LifeCycleStateREADY
		pool.StateDetails = models.LifeCycleStateAvailableDetails
		pool.PoolAttributes.Labels = &labels

		updatedPool, err := store.UpdatedPool(context.Background(), pool)
		if err != nil {
			tt.Errorf("Expected no error, got %v", err)
		}

		// Fetch the pool from the database.
		dbPool := &datamodel.Pool{}
		err = store.db.GORM().First(dbPool, "uuid = ?", pool.UUID).Error
		if err != nil {
			tt.Fatalf("Failed to fetch updated pool: %v", err)
		}

		if dbPool.State != models.LifeCycleStateREADY {
			tt.Errorf("Expected state %v, got %v", models.LifeCycleStateREADY, dbPool.State)
		}
		if dbPool.StateDetails != models.LifeCycleStateAvailableDetails {
			tt.Errorf("Expected state details %v, got %v", models.LifeCycleStateAvailableDetails, dbPool.StateDetails)
		}

		// Verify that UpdatedPool returns the correct updated pool.
		if updatedPool.UUID != pool.UUID {
			tt.Errorf("Expected pool UUID %v, got %v", pool.UUID, updatedPool.UUID)
		}
		assert.Equal(tt, updatedPool.PoolAttributes.Labels, &labels)
	})
}

func TestUpdatingPool(t *testing.T) {
	t.Run("ReturnsErrorWhenPoolIsTransitioning", func(tt *testing.T) {
		// Setup test database
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		if err = ClearInMemoryDB(store.db.GORM()); err != nil {
			tt.Fatalf("Failed to clean test database: %v", err)
		}

		// Create an account and a pool with a transitional state (e.g., LifeCycleStateCreating)
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test_account",
		}
		if err = store.db.Create(account).Error(); err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		// Create pool record with a transitioning state
		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			Account:   account,
			State:     models.LifeCycleStateCreating,
		}
		if err = store.db.Create(pool).Error(); err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		// Change fields to be updated
		pool.SizeInBytes = 2048
		pool.Description = "Updated description"

		// Call UpdatingPool. Since the pool is in a transitional state, conflict error is expected.
		_, err = store.UpdatingPool(context.Background(), pool)
		if err == nil {
			tt.Error("Expected conflict error, got nil")
		}
		if !customerrors.IsConflictErr(err) {
			tt.Errorf("Expected conflict error, got %v", err)
		}
	})
	t.Run("UpdatesPoolSuccessfully", func(tt *testing.T) {
		// Setup test database
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		if err = ClearInMemoryDB(store.db.GORM()); err != nil {
			tt.Fatalf("Failed to clean test database: %v", err)
		}

		// Create account and a pool in non-transitional state (READY)
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test_account",
		}
		if err = store.db.Create(account).Error(); err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		// Pool initially in READY state
		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			Account:   account,
			State:     models.LifeCycleStateREADY,
		}
		if err = store.db.Create(pool).Error(); err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		// Wait to ensure UpdatedAt will be changed
		time.Sleep(10 * time.Millisecond)
		// Update values
		pool.SizeInBytes = 4096
		pool.Description = "New Description"

		updatedPool, err := store.UpdatingPool(context.Background(), pool)
		if err != nil {
			tt.Errorf("Expected no error, got %v", err)
		}

		// Fetch updated pool from DB
		dbPool := &datamodel.Pool{}
		if err = store.db.GORM().First(dbPool, "uuid = ?", pool.UUID).Error; err != nil {
			tt.Fatalf("Failed to fetch updated pool: %v", err)
		}

		// Verify updated fields
		if dbPool.State != models.LifeCycleStateUpdating {
			tt.Errorf("Expected state %v, got %v", models.LifeCycleStateUpdating, dbPool.State)
		}
		if dbPool.SizeInBytes != 4096 {
			tt.Errorf("Expected SizeInBytes 4096, got %v", dbPool.SizeInBytes)
		}
		if dbPool.Description != "New Description" {
			tt.Errorf("Expected Description 'New Description', got %v", dbPool.Description)
		}

		// Verify the returned pool reflects the updated values
		if updatedPool.UUID != pool.UUID {
			tt.Errorf("Expected pool UUID %v, got %v", pool.UUID, updatedPool.UUID)
		}
		if updatedPool.State != models.LifeCycleStateUpdating {
			tt.Errorf("Expected state %v, got %v", models.LifeCycleStateUpdating, updatedPool.State)
		}
		if updatedPool.SizeInBytes != 4096 {
			tt.Errorf("Expected SizeInBytes 4096, got %v", updatedPool.SizeInBytes)
		}
		if updatedPool.Description != "New Description" {
			tt.Errorf("Expected Description 'New Description', got %v", updatedPool.Description)
		}
	})
}

func TestCreatedPool(t *testing.T) {
	t.Run("ReturnsErrorWhenPoolDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			tt.Fatalf("Failed to clean up test database: %v", err)
		}

		pool := &datamodel.Pool{
			Name:      "non-existent-pool",
			VendorID:  "non-existent-vendor-id",
			AccountID: 1,
		}

		_, err = store.CreatedPool(context.Background(), pool)
		if err == nil {
			tt.Errorf("Expected error, got nil")
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			tt.Errorf("Expected not found error, got %v", err)
		}
	})

	t.Run("UpdatesPoolStateToReadySuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			tt.Fatalf("Failed to clean up test database: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test_account",
		}
		err = store.db.Create(account).Error()
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			VendorID:  "test-vendor-id",
			AccountID: account.ID,
			Account:   account,
			State:     models.LifeCycleStateCreating,
		}
		err = store.db.Create(pool).Error()
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		updatedPool, err := store.CreatedPool(context.Background(), pool)
		if err != nil {
			tt.Errorf("Expected no error, got %v", err)
		}

		if updatedPool.State != models.LifeCycleStateREADY {
			tt.Errorf("Expected state %v, got %v", models.LifeCycleStateREADY, updatedPool.State)
		}
		if updatedPool.StateDetails != models.LifeCycleStateAvailableDetails {
			tt.Errorf("Expected state details %v, got %v", models.LifeCycleStateAvailableDetails, updatedPool.StateDetails)
		}
	})
}

func TestGetPoolByName(t *testing.T) {
	t.Run("WhenPoolExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			tt.Fatalf("Failed to clean up test database: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test_account",
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			VendorID:  "test-pool",
			AccountID: account.ID,
			Account:   account,
		}
		err = store.db.Create(account).Error()
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}
		err = store.db.Create(pool).Error()
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}
		conditions := [][]interface{}{{"name = ?", pool.Name}}
		conditions = append(conditions, []interface{}{"account_id = ?", account.ID})
		result, err := store.GetPoolByName(context.Background(), conditions)
		if err != nil {
			tt.Errorf("Expected no error, got %v", err)
		}
		if result.Name != pool.Name {
			tt.Errorf("Expected pool name %v, got %v", pool.Name, result.Name)
		}
		if result.Account.Name != account.Name {
			tt.Errorf("Expected account name %v, got %v", account.Name, result.Account.Name)
		}
	})
	t.Run("WhenPoolDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test_account",
		}

		if err != nil {
			tt.Fatalf("Failed to clean up test database: %v", err)
		}
		conditions := [][]interface{}{{"name = ?", "pool_name"}}
		conditions = append(conditions, []interface{}{"account_id = ?", account.ID})
		_, err = store.GetPoolByName(context.Background(), conditions)
		if err == nil {
			tt.Errorf("Expected error, got nil")
		}
		if !customerrors.IsNotFoundErr(err) {
			tt.Errorf("Expected error %v, got %v", gorm.ErrRecordNotFound, err)
		}
	})
}

func TestGetPoolsByAccountName(t *testing.T) {
	db, err := SetupTestDB()
	if err != nil {
		t.Fatalf("Failed to set up test database: %v", err)
	}
	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	if err != nil {
		t.Fatalf("Failed to clean up test database: %v", err)
	}

	account1 := &datamodel.Account{BaseModel: datamodel.BaseModel{
		ID: 1, UUID: "test-account-uuid-1"}, Name: "test-account-1"}
	account2 := &datamodel.Account{BaseModel: datamodel.BaseModel{
		ID: 2, UUID: "test-account-uuid-2"}, Name: "test-account-2"}
	account3 := &datamodel.Account{BaseModel: datamodel.BaseModel{
		ID: 3, UUID: "test-account-uuid-3"}, Name: "test-account-3"}
	var accounts []*datamodel.Account
	accounts = append(accounts, account1, account2, account3)

	pool1 := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid-1"},
		Name: "test-pool-1", AccountID: account1.ID, Account: account1, DeploymentName: "pool1"}
	pool2 := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid-2"},
		Name: "test-pool-2", AccountID: account2.ID, Account: account2, DeploymentName: "pool2"}
	pool3 := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid-3"},
		Name: "test-pool-3", AccountID: account1.ID, Account: account1, DeploymentName: "pool3"}

	var pools []*datamodel.Pool
	pools = append(pools, pool1, pool2, pool3)

	err = store.db.Create(accounts).Error()
	if err != nil {
		t.Fatalf("Failed to create account: %v", err)
	}
	err = store.db.Create(pools).Error()
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}

	t.Run("WhenPoolsExist", func(tt *testing.T) {
		result, errDB := store.GetPoolsByAccountName(context.Background(), "test-account-1")
		assert.NoError(tt, errDB)
		assert.NotNil(tt, result)
		assert.Equal(tt, 2, len(result))
		assert.Equal(tt, "test-pool-uuid-1", result[0].UUID)
		assert.Equal(tt, "test-pool-uuid-3", result[1].UUID)
	})
	t.Run("WhenPoolsDontExist", func(tt *testing.T) {
		result, errDB := store.GetPoolsByAccountName(context.Background(), "test-account-3")
		assert.NoError(tt, errDB)
		assert.NotNil(tt, result)
		assert.Equal(tt, 0, len(result))
	})
}

func TestUpdatePoolWithKmsConfigIDUTs(t *testing.T) {
	db, err := SetupTestDB()
	if err != nil {
		t.Fatalf("Failed to set up test database: %v", err)
	}
	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	if err != nil {
		t.Fatalf("Failed to clean up test database: %v", err)
	}

	account1 := &datamodel.Account{BaseModel: datamodel.BaseModel{
		ID: 1, UUID: "test-account-uuid-1"}, Name: "test-account-1"}
	account2 := &datamodel.Account{BaseModel: datamodel.BaseModel{
		ID: 2, UUID: "test-account-uuid-2"}, Name: "test-account-2"}
	var accounts []*datamodel.Account
	accounts = append(accounts, account1, account2)

	pool1 := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid-1"},
		Name: "test-pool-1", VendorID: "test-vendor-id-1", AccountID: account1.ID, Account: account1, DeploymentName: "deployment-name1"}
	pool2 := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid-2"},
		Name: "test-pool-2", VendorID: "test-vendor-id-2", AccountID: account2.ID, Account: account2, DeploymentName: "deployment-name2"}
	pool3 := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid-3"},
		Name: "test-pool-3", VendorID: "test-vendor-id-3", AccountID: account1.ID, Account: account1, DeploymentName: "deployment-name3"}

	var pools []*datamodel.Pool
	pools = append(pools, pool1, pool2)

	kmsConfigs := []*datamodel.KmsConfig{
		{BaseModel: datamodel.BaseModel{ID: int64(1), UUID: "kmsConfig-uuid1", DeletedAt: nil}, Name: "kmsConfig1"},
		{BaseModel: datamodel.BaseModel{ID: int64(2), UUID: "kmsConfig-uuid2", DeletedAt: nil}, Name: "kmsConfig2"},
	}

	err = store.db.Create(accounts).Error()
	if err != nil {
		t.Fatalf("Failed to create account: %v", err)
	}
	err = store.db.Create(pools).Error()
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	err = store.db.Create(kmsConfigs).Error()
	if err != nil {
		t.Fatalf("Failed to create kmsConfig: %v", err)
	}
	t.Run("WhenDbQuerySucceeds", func(tt *testing.T) {
		result, errDB := store.UpdatePoolWithKmsConfigID(context.Background(), pool1, "kmsConfig-uuid1")
		assert.NoError(tt, errDB)
		assert.NotNil(tt, result)
		assert.Equal(tt, result.KmsConfigID, sql.NullInt64{Int64: 1, Valid: true})
	})
	t.Run("WhenKmsConfigDoesNotExist", func(tt *testing.T) {
		result, errDB := store.UpdatePoolWithKmsConfigID(context.Background(), pool1, "kmsConfig-uuid3")
		assert.Error(tt, errDB)
		assert.Nil(tt, result)
		assert.Errorf(tt, errDB, "record not found not found")
	})
	t.Run("WhenPoolDoesNotExist", func(tt *testing.T) {
		result, errDB := store.UpdatePoolWithKmsConfigID(context.Background(), pool3, "kmsConfig-uuid1")
		assert.Error(tt, errDB)
		assert.Nil(tt, result)
		assert.Errorf(tt, errDB, "record not found not found")
	})
}

// Unit test for ConvertPoolViewToPool
func TestConvertPoolViewToPool(t *testing.T) {
	autoTiering := &datamodel.AutoTieringConfig{
		HotTierSizeInBytes:      200,
		EnableHotTierAutoResize: true,
	}
	view := &datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel:         datamodel.BaseModel{UUID: "uuid-1"},
			Name:              "test-pool",
			Description:       "desc",
			State:             "READY",
			StateDetails:      "Available",
			VendorID:          "vendor-1",
			ServiceLevel:      "premium",
			SizeInBytes:       1000,
			UsedBytes:         500,
			Network:           "net-1",
			AllowAutoTiering:  true,
			AutoTieringConfig: autoTiering,
			AccountID:         1,
			Account:           &datamodel.Account{Name: "acc"},
			ClusterDetails:    datamodel.ClusterDetails{ExternalName: "cluster"},
			QosType:           "qos",
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "pass",
				SecretID:      "",
				CertificateID: "",
			},
			VLMConfig: "dummy-vlm-config",
			BuildInfo: &datamodel.PoolBuildInfo{
				OntapVersion: "1.0",
			},
		},
	}

	pool := ConvertPoolViewToPool(view)
	if pool == nil {
		t.Fatal("expected non-nil pool")
	}
	if pool.Name != "test-pool" {
		t.Errorf("expected Name 'test-pool', got %v", pool.Name)
	}
	if pool.Account.Name != "acc" {
		t.Errorf("expected Account.Name 'acc', got %v", pool.Account.Name)
	}
	if pool.ClusterDetails.ExternalName != "cluster" {
		t.Errorf("expected ClusterDetails.ExternalName 'cluster', got %v", pool.ClusterDetails.ExternalName)
	}
	if pool.BuildInfo.OntapVersion != "1.0" {
		t.Errorf("expected BuildInfo.OntapVersion '1.0', got %v", pool.BuildInfo.OntapVersion)
	}

	// Test nil input
	if ConvertPoolViewToPool(nil) != nil {
		t.Error("expected nil for nil input")
	}
}

// Unit test for ConvertPoolToPoolView
func TestConvertPoolToPoolView(t *testing.T) {
	pool := &datamodel.Pool{
		BaseModel:    datamodel.BaseModel{UUID: "uuid-1"},
		Name:         "test-pool",
		AccountID:    1,
		Account:      &datamodel.Account{Name: "acc"},
		State:        "READY",
		StateDetails: "Available",
		VLMConfig:    "test-vlm-config",
	}
	view := ConvertPoolToPoolView(pool)
	if view == nil {
		t.Fatal("expected non-nil PoolView")
	}
	if view.Name != "test-pool" {
		t.Errorf("expected Pool.Name 'test-pool', got %v", view.Name)
	}
	if view.Account.Name != "acc" {
		t.Errorf("expected Account.Name 'acc', got %v", view.Account.Name)
	}
	if view.Throughput != 0 {
		t.Errorf("expected Throughput 0, got %v", view.Throughput)
	}
	if view.QuotaInBytes != 0 {
		t.Errorf("expected QuotaInBytes 0, got %v", view.QuotaInBytes)
	}
	if view.VolumeCount != 0 {
		t.Errorf("expected VolumeCount 0, got %v", view.VolumeCount)
	}
	if view.VLMConfig != "test-vlm-config" {
		t.Errorf("expected VLMConfig test-vlm-config, but instead got %v", view.VLMConfig)
	}

	// Test nil input
	if ConvertPoolToPoolView(nil) != nil {
		t.Error("expected nil for nil input")
	}
}

func TestUpdatePoolWithKmsConfigID(t *testing.T) {
	t.Run("ReturnsUpdatedPoolWhenKmsConfigAndPoolExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			tt.Fatalf("Failed to clean up test database: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.db.Create(account).Error()
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		kmsConfig := &datamodel.KmsConfig{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "kms-uuid"},
			KeyName:   "key",
		}
		err = store.db.Create(kmsConfig).Error()
		if err != nil {
			tt.Fatalf("Failed to create kms config: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			VendorID:  "test-vendor-id",
			AccountID: account.ID,
			Account:   account,
		}
		err = store.db.Create(pool).Error()
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		updatedPool, err := store.UpdatePoolWithKmsConfigID(context.Background(), pool, "kms-uuid")
		assert.NoError(tt, err)
		assert.NotNil(tt, updatedPool)
		assert.Equal(tt, int64(1), updatedPool.KmsConfigID.Int64)
		assert.True(tt, updatedPool.KmsConfigID.Valid)
	})

	t.Run("ReturnsErrorWhenPoolDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			tt.Fatalf("Failed to clean up test database: %v", err)
		}

		kmsConfig := &datamodel.KmsConfig{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "kms-uuid"},
			KeyName:   "key",
		}
		err = store.db.Create(kmsConfig).Error()
		if err != nil {
			tt.Fatalf("Failed to create kms config: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "non-existent-pool-uuid"},
			Name:      "non-existent-pool",
			VendorID:  "non-existent-vendor-id",
			AccountID: 1,
		}

		updatedPool, err := store.UpdatePoolWithKmsConfigID(context.Background(), pool, "kms-uuid")
		assert.Error(tt, err)
		assert.Nil(tt, updatedPool)
	})
	t.Run("ReturnsErrorWhenKmsConfigDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			tt.Fatalf("Failed to clean up test database: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.db.Create(account).Error()
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			Account:   account,
		}
		err = store.db.Create(pool).Error()
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		updatedPool, err := store.UpdatePoolWithKmsConfigID(context.Background(), pool, "non-existent-kms-uuid")
		assert.Error(tt, err)
		assert.Nil(tt, updatedPool)
	})
	t.Run("ReturnsErrorWhenTransactionCannotStart", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		pool := &datamodel.Pool{Name: "test_pool", AccountID: 1}
		orgStartTransaction := startTransaction
		defer func() {
			startTransaction = orgStartTransaction
		}()
		startTransaction = func(db *gorm.DB) (*gorm.DB, error) {
			return nil, errors.New("failed to start transaction")
		}
		updatedPool, err := store.UpdatePoolWithKmsConfigID(context.Background(), pool, "kms-uuid")
		assert.Error(tt, err)
		assert.Nil(tt, updatedPool)
	})
}

func TestUpdatePoolState(t *testing.T) {
	db, err := SetupTestDB()
	if err != nil {
		t.Fatalf("Failed to set up test database: %v", err)
	}
	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	if err != nil {
		t.Fatalf("Failed to clean up test database: %v", err)
	}

	// Re-using test data from other units tests...
	account1 := &datamodel.Account{BaseModel: datamodel.BaseModel{
		ID: 1, UUID: "test-account-uuid-1"}, Name: "test-account-1"}
	account2 := &datamodel.Account{BaseModel: datamodel.BaseModel{
		ID: 2, UUID: "test-account-uuid-2"}, Name: "test-account-2"}
	var accounts []*datamodel.Account
	accounts = append(accounts, account1, account2)

	pool1 := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid-1"}, DeploymentName: "deployment-name1",
		Name: "test-pool-1", AccountID: account1.ID, Account: account1, State: models.LifeCycleStateCreated, StateDetails: models.LifeCycleStateCreatingDetails}
	pool2 := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid-2"}, DeploymentName: "deployment-name2",
		Name: "test-pool-2", AccountID: account2.ID, Account: account2}
	pool3 := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid-3"}, DeploymentName: "deployment-name3",
		Name: "test-pool-3", AccountID: account1.ID, Account: account1}

	var pools []*datamodel.Pool
	pools = append(pools, pool1, pool2)

	kmsConfigs := []*datamodel.KmsConfig{
		{BaseModel: datamodel.BaseModel{ID: int64(1), UUID: "kmsConfig-uuid1", DeletedAt: nil}, Name: "kmsConfig1"},
		{BaseModel: datamodel.BaseModel{ID: int64(2), UUID: "kmsConfig-uuid2", DeletedAt: nil}, Name: "kmsConfig2"},
	}

	err = store.db.Create(accounts).Error()
	if err != nil {
		t.Fatalf("Failed to create account: %v", err)
	}
	err = store.db.Create(pools).Error()
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	err = store.db.Create(kmsConfigs).Error()
	if err != nil {
		t.Fatalf("Failed to create kmsConfig: %v", err)
	}
	t.Run("WhenDbUpdateSucceedsWithStateAlreadyDefined", func(tt *testing.T) {
		result, errDB := store.UpdatePoolState(context.Background(), pool1, models.LifeCycleStateMigrating, models.LifeCycleStateMigratingDetails)
		assert.NoError(tt, errDB)
		assert.NotNil(tt, result)
		assert.Equal(tt, result.State, models.LifeCycleStateMigrating)
		assert.Equal(tt, result.StateDetails, models.LifeCycleStateMigratingDetails)
	})
	t.Run("WhenDbUpdateSucceedsWithStateNotDefined", func(tt *testing.T) {
		result, errDB := store.UpdatePoolState(context.Background(), pool2, models.LifeCycleStateUpdating, models.LifeCycleStateUpdatingDetails)
		assert.NoError(tt, errDB)
		assert.NotNil(tt, result)
		assert.Equal(tt, result.State, models.LifeCycleStateUpdating)
		assert.Equal(tt, result.StateDetails, models.LifeCycleStateUpdatingDetails)
	})
	t.Run("WhenDbUpdateIsUnableToFindRecord", func(tt *testing.T) {
		result, errDB := store.UpdatePoolState(context.Background(), pool3, models.LifeCycleStateUpdating, models.LifeCycleStateUpdatingDetails)
		assert.Nil(tt, result)
		assert.Error(tt, errDB)
		assert.EqualError(tt, errDB, "Pool not found")
		assert.Contains(tt, errDB.(*vsaerrors.CustomError).OriginalErr.Error(), "pool not found")
	})
}

func TestUpdatePoolSubnetNames_UpdatesSnHostProject(t *testing.T) {
	db, err := SetupTestDB()
	if err != nil {
		t.Fatalf("Failed to set up test database: %v", err)
	}
	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)
	err = ClearInMemoryDB(store.db.GORM())
	if err != nil {
		t.Fatalf("Failed to clean up test database: %v", err)
	}

	// Create account and pool
	account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"}, Name: "test_account"}
	err = store.db.Create(account).Error()
	if err != nil {
		t.Fatalf("Failed to create account: %v", err)
	}
	pool := &datamodel.Pool{
		BaseModel:     datamodel.BaseModel{UUID: "test-pool-uuid"},
		Name:          "test_pool",
		AccountID:     account.ID,
		Account:       account,
		SnHostProject: "",
	}
	err = store.db.Create(pool).Error()
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}

	// Update SnHostProject
	expectedSnHostProject := "new-sn-host-project"
	subnetNames := []string{"subnet-1", "subnet-2"}
	err = store.UpdatePoolSubnetNames(context.Background(), pool.UUID, expectedSnHostProject, subnetNames)
	assert.NoError(t, err)

	// Verify update
	updatedPool := &datamodel.Pool{}
	err = store.db.GORM().First(updatedPool, "uuid = ?", pool.UUID).Error
	assert.NoError(t, err)
	assert.Equal(t, expectedSnHostProject, updatedPool.SnHostProject)
}

func TestGetNextSerialNumberInRegion(t *testing.T) {
	// Could not cover success case due to the use of a sequence, and sqlite in-memory database does not support sequences.
	t.Run("ReturnsError", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			t.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			t.Fatalf("Failed to clean up test database: %v", err)
		}

		_, err = store.GetNextSerialNumberInRegion(context.Background(), "93501")
		if err == nil {
			tt.Errorf("Expected error, got nil")
		}
	})
}

func TestListTpProjects_ReturnsDistinctNonEmptyProjects(t *testing.T) {
	db, err := SetupTestDB()
	if err != nil {
		t.Fatalf("Failed to set up test database: %v", err)
	}
	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)
	err = ClearInMemoryDB(store.db.GORM())
	if err != nil {
		t.Fatalf("Failed to clean up test database: %v", err)
	}

	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
		Name:      "test_account",
	}
	if err = store.db.Create(account).Error(); err != nil {
		t.Fatalf("Failed to create account: %v", err)
	}

	pool1 := &datamodel.Pool{
		BaseModel:      datamodel.BaseModel{UUID: "uuid-1"},
		Name:           "pool1",
		AccountID:      account.ID,
		Account:        account,
		SnHostProject:  "project-1",
		DeploymentName: "deployment-1",
		ClusterDetails: datamodel.ClusterDetails{
			RegionalTenantProject: "project-1",
		},
	}
	pool2 := &datamodel.Pool{
		BaseModel:      datamodel.BaseModel{UUID: "uuid-2"},
		Name:           "pool2",
		AccountID:      account.ID,
		Account:        account,
		SnHostProject:  "project-2",
		DeploymentName: "deployment-2",
		ClusterDetails: datamodel.ClusterDetails{
			RegionalTenantProject: "project-2",
		},
	}
	pool3 := &datamodel.Pool{
		BaseModel:      datamodel.BaseModel{UUID: "uuid-3"},
		Name:           "pool3",
		AccountID:      account.ID,
		Account:        account,
		SnHostProject:  "project-1",
		DeploymentName: "deployment-3",
		ClusterDetails: datamodel.ClusterDetails{
			RegionalTenantProject: "project-1",
		},
	}
	if err = store.db.Create(pool1).Error(); err != nil {
		t.Fatalf("Failed to create pool1: %v", err)
	}
	if err = store.db.Create(pool2).Error(); err != nil {
		t.Fatalf("Failed to create pool2: %v", err)
	}
	if err = store.db.Create(pool3).Error(); err != nil {
		t.Fatalf("Failed to create pool3: %v", err)
	}

	projects, err := store.ListTpProjects(context.Background())
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"project-1", "project-2"}, projects)
}

func TestListTpProjects_ExcludesEmptyAndNullProjects(t *testing.T) {
	db, err := SetupTestDB()
	if err != nil {
		t.Fatalf("Failed to set up test database: %v", err)
	}
	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)
	err = ClearInMemoryDB(store.db.GORM())
	if err != nil {
		t.Fatalf("Failed to clean up test database: %v", err)
	}

	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
		Name:      "test_account",
	}
	if err = store.db.Create(account).Error(); err != nil {
		t.Fatalf("Failed to create account: %v", err)
	}

	poolWithProject := &datamodel.Pool{
		BaseModel:      datamodel.BaseModel{UUID: "uuid-1"},
		Name:           "pool1",
		AccountID:      account.ID,
		Account:        account,
		SnHostProject:  "project-1",
		DeploymentName: "deployment-1",
		ClusterDetails: datamodel.ClusterDetails{
			RegionalTenantProject: "project-1",
		},
	}
	poolWithEmpty := &datamodel.Pool{
		BaseModel:      datamodel.BaseModel{UUID: "uuid-2"},
		Name:           "pool2",
		AccountID:      account.ID,
		Account:        account,
		SnHostProject:  "",
		DeploymentName: "deployment-2",
		ClusterDetails: datamodel.ClusterDetails{
			RegionalTenantProject: "",
		},
	}
	poolWithNull := &datamodel.Pool{
		BaseModel:      datamodel.BaseModel{UUID: "uuid-3"},
		Name:           "pool1",
		AccountID:      account.ID,
		Account:        account,
		SnHostProject:  "project-1",
		DeploymentName: "deployment-3",
		ClusterDetails: datamodel.ClusterDetails{
			RegionalTenantProject: "project-1",
		},
	}
	if err = store.db.Create(poolWithProject).Error(); err != nil {
		t.Fatalf("Failed to create poolWithProject: %v", err)
	}
	if err = store.db.Create(poolWithEmpty).Error(); err != nil {
		t.Fatalf("Failed to create poolWithEmpty: %v", err)
	}
	if err = store.db.Create(poolWithNull).Error(); err != nil {
		t.Fatalf("Failed to create poolWithNull: %v", err)
	}

	projects, err := store.ListTpProjects(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, []string{"project-1"}, projects)
}

func TestListTpProjects_ReturnsEmptySliceWhenNoProjects(t *testing.T) {
	db, err := SetupTestDB()
	if err != nil {
		t.Fatalf("Failed to set up test database: %v", err)
	}
	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)
	err = ClearInMemoryDB(store.db.GORM())
	if err != nil {
		t.Fatalf("Failed to clean up test database: %v", err)
	}

	projects, err := store.ListTpProjects(context.Background())
	assert.NoError(t, err)
	assert.Empty(t, projects)
}

func TestListTpProjects_ReturnsErrorOnDBFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Failed to create sqlmock: %v", err)
	}
	gdb, err := gorm.Open(postgres.New(postgres.Config{Conn: db}), &gorm.Config{})
	if err != nil {
		t.Fatalf("Failed to open gorm db: %v", err)
	}
	wrapper := gormwrapper.New(gdb)
	store := NewDataStoreRepository(wrapper)

	mock.ExpectClose() // Add this line

	sqlDB, _ := gdb.DB()
	if err := sqlDB.Close(); err != nil {
		t.Errorf("failed to close sqlDB: %v", err)
	}

	projects, err := store.ListTpProjects(context.Background())
	assert.Error(t, err)
	assert.Nil(t, projects)
}

func TestListPoolUUIDs(t *testing.T) {
	t.Run("ReturnsEmptySliceWhenNoPools", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			tt.Fatalf("Failed to clean up test database: %v", err)
		}

		poolIds, err := store.ListPoolUUIDs(context.Background(), nil)
		assert.NoError(tt, err)
		assert.Empty(tt, poolIds)
	})

	t.Run("ReturnsPoolsWhenPresent", func(tt *testing.T) {
		store := setup(tt)

		// Create test account and pools
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err := store.db.Create(account).Error()
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		// Create multiple pools
		pools := []*datamodel.Pool{
			{
				BaseModel:      datamodel.BaseModel{UUID: "test-pool-uuid-1"},
				Name:           "test_pool_1",
				AccountID:      account.ID,
				VendorID:       "test-vendor-id-1",
				DeploymentName: "deployment-1",
			},
			{
				BaseModel:      datamodel.BaseModel{UUID: "test-pool-uuid-2"},
				Name:           "test_pool_2",
				AccountID:      account.ID,
				VendorID:       "test-vendor-id-2",
				DeploymentName: "deployment-2",
			},
		}

		for _, pool := range pools {
			err := store.db.Create(pool).Error()
			if err != nil {
				tt.Fatalf("Failed to create pool: %v", err)
			}
		}

		// Test ListPoolUUIDs
		poolIds, err := store.ListPoolUUIDs(context.Background(), nil)
		assert.NoError(tt, err)
		assert.Len(tt, poolIds, 2)

		// Verify pool identifiers
		// Create a map for easier lookup
		poolMap := make(map[string]*PoolIdentifier)
		for _, p := range poolIds {
			poolMap[p.UUID] = p
		}

		// Check that both pools are returned with correct data
		for _, expected := range pools {
			actual, exists := poolMap[expected.UUID]
			assert.True(tt, exists, "Pool with UUID %s was not returned", expected.UUID)
			if exists {
				assert.Equal(tt, expected.UUID, actual.UUID)
				assert.Equal(tt, expected.VendorID, actual.VendorID)
				assert.Equal(tt, expected.Name, actual.Name)
				assert.Equal(tt, expected.AccountID, actual.AccountID)
			}
		}
	})

	t.Run("WithFilter", func(tt *testing.T) {
		store := setup(tt)

		// Create test account and pools
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err := store.db.Create(account).Error()
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		// Create multiple pools with different names
		pools := []*datamodel.Pool{
			{
				BaseModel:      datamodel.BaseModel{UUID: "test-pool-uuid-1"},
				Name:           "filter_pool",
				AccountID:      account.ID,
				VendorID:       "test-vendor-id-1",
				DeploymentName: "deployment-filter-1",
			},
			{
				BaseModel:      datamodel.BaseModel{UUID: "test-pool-uuid-2"},
				Name:           "other_pool",
				AccountID:      account.ID,
				VendorID:       "test-vendor-id-2",
				DeploymentName: "deployment-filter-2",
			},
		}

		for _, pool := range pools {
			err := store.db.Create(pool).Error()
			if err != nil {
				tt.Fatalf("Failed to create pool: %v", err)
			}
		}

		// Create a filter for pools with name = 'filter_pool'
		filter := utils.NewFilterCondition("name", "=", "filter_pool")

		// Test ListPoolUUIDs with filter
		poolIds, err := store.ListPoolUUIDs(context.Background(), utils.CreateFilterWithConditions(filter))
		assert.NoError(tt, err)
		assert.Len(tt, poolIds, 1)
		assert.Equal(tt, "test-pool-uuid-1", poolIds[0].UUID)
		assert.Equal(tt, "filter_pool", poolIds[0].Name)
		assert.Equal(tt, "test-vendor-id-1", poolIds[0].VendorID)
	})

	t.Run("WithFilterIncludeDeleted", func(tt *testing.T) {
		store := setup(tt)

		// Create test account and pools
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err := store.db.Create(account).Error()
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		// Create pools - one normal, one soft-deleted
		pools := []*datamodel.Pool{
			{
				BaseModel:      datamodel.BaseModel{UUID: "test-pool-uuid-1"},
				Name:           "active_pool",
				AccountID:      account.ID,
				VendorID:       "test-vendor-id-1",
				DeploymentName: "deployment-active",
			},
			{
				BaseModel:      datamodel.BaseModel{UUID: "test-pool-uuid-2"},
				Name:           "deleted_pool",
				AccountID:      account.ID,
				VendorID:       "test-vendor-id-2",
				DeploymentName: "deployment-deleted",
			},
		}

		for _, pool := range pools {
			err := store.db.Create(pool).Error()
			if err != nil {
				tt.Fatalf("Failed to create pool: %v", err)
			}
		}

		// Soft delete the second pool
		err = store.db.GORM().Delete(&pools[1]).Error
		if err != nil {
			tt.Fatalf("Failed to delete pool: %v", err)
		}

		// Test without including deleted
		poolIds, err := store.ListPoolUUIDs(context.Background(), nil)
		assert.NoError(tt, err)
		assert.Len(tt, poolIds, 1)
		assert.Equal(tt, "test-pool-uuid-1", poolIds[0].UUID)

		// Test with filter that includes deleted
		poolIds, err = store.ListPoolUUIDs(context.Background(), &utils.Filter{IncludeDeleted: true})
		assert.NoError(tt, err)
		assert.Len(tt, poolIds, 2)

		// Create a map for easier lookup
		poolMap := make(map[string]*PoolIdentifier)
		for _, p := range poolIds {
			poolMap[p.UUID] = p
		}

		// Verify both pools are present
		assert.Contains(tt, poolMap, "test-pool-uuid-1")
		assert.Contains(tt, poolMap, "test-pool-uuid-2")
	})

	t.Run("ReturnsErrorOnDBFailure", func(tt *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			tt.Fatalf("Failed to create sqlmock: %v", err)
		}
		gdb, err := gorm.Open(postgres.New(postgres.Config{Conn: db}), &gorm.Config{})
		if err != nil {
			tt.Fatalf("Failed to open gorm db: %v", err)
		}
		wrapper := gormwrapper.New(gdb)
		store := NewDataStoreRepository(wrapper)

		// Mock the query to return an error
		mock.ExpectQuery("SELECT uuid, vendor_id, name, account_id FROM \"pools\"").
			WillReturnError(sql.ErrConnDone)

		poolIds, err := store.ListPoolUUIDs(context.Background(), nil)
		assert.Error(tt, err)
		assert.Nil(tt, poolIds)

		// Verify all expectations were met
		if err := mock.ExpectationsWereMet(); err != nil {
			tt.Errorf("there were unfulfilled expectations: %s", err)
		}
	})
}

func TestUpdatePoolFields(t *testing.T) {
	db, err := SetupTestDB()
	if err != nil {
		t.Fatalf("Failed to set up test database: %v", err)
	}
	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)
	err = ClearInMemoryDB(store.db.GORM())
	if err != nil {
		t.Fatalf("Failed to clean up test database: %v", err)
	}

	// Create account and pool
	account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"}, Name: "test_account"}
	err = store.db.Create(account).Error()
	if err != nil {
		t.Fatalf("Failed to create account: %v", err)
	}
	pool := &datamodel.Pool{
		BaseModel:   datamodel.BaseModel{UUID: "test-pool-uuid"},
		Name:        "test_pool",
		AccountID:   account.ID,
		Account:     account,
		Description: "old description",
	}
	err = store.db.Create(pool).Error()
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}

	// Update description field
	newDescription := "new description"
	updates := map[string]interface{}{
		"description": newDescription,
	}
	err = store.UpdatePoolFields(context.Background(), pool.UUID, updates)
	assert.NoError(t, err)

	// Verify update
	updatedPool := &datamodel.Pool{}
	err = store.db.GORM().First(updatedPool, "uuid = ?", pool.UUID).Error
	assert.NoError(t, err)
	assert.Equal(t, newDescription, updatedPool.Description)
	assert.False(t, updatedPool.UpdatedAt.IsZero())

	// Test error when pool does not exist
	err = store.UpdatePoolFields(context.Background(), "non-existent-uuid", updates)
	assert.Error(t, err)
}

func Test_getPoolsByKmsConfigID(t *testing.T) {
	tests := []struct {
		name      string
		setup     func(db *gorm.DB) int64
		expectErr bool
		expectLen int
	}{
		{
			name: "ReturnsPoolsWhenPresent",
			setup: func(db *gorm.DB) int64 {
				kmsConfig := &datamodel.KmsConfig{BaseModel: datamodel.BaseModel{ID: 1, UUID: "kms-uuid"}}
				db.Create(kmsConfig)
				pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-uuid"}, KmsConfigID: sql.NullInt64{Int64: kmsConfig.ID, Valid: true}}
				db.Create(pool)
				return kmsConfig.ID
			},
			expectErr: false,
			expectLen: 1,
		},
		{
			name: "ReturnsEmptySliceWhenNoPools",
			setup: func(db *gorm.DB) int64 {
				return 999
			},
			expectErr: false,
			expectLen: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := SetupTestDB()
			assert.NoError(t, err)
			wrapper := gormwrapper.New(db)
			store := NewDataStoreRepository(wrapper)
			err = ClearInMemoryDB(store.db.GORM())
			assert.NoError(t, err)

			id := tt.setup(store.db.GORM())
			pools, err := _getPoolsByKmsConfigID(store.db.GORM(), id)
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Len(t, pools, tt.expectLen)
			}
		})
	}

	t.Run("ReturnsErrorOnDBFailure", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		assert.NoError(t, err)
		gdb, err := gorm.Open(postgres.New(postgres.Config{Conn: db}), &gorm.Config{})
		assert.NoError(t, err)
		wrapper := gormwrapper.New(gdb)

		mock.ExpectQuery("SELECT .* FROM \"pools\" WHERE kms_config_id = .*").WillReturnError(errors.New("db error"))

		_, err = _getPoolsByKmsConfigID(wrapper.GORM(), 1)
		assert.Error(t, err)
	})
}

func TestDataStoreRepository_GetPoolsCount_ErrorHandling(t *testing.T) {
	// This test covers line 545 in pools.go - error handling in GetPoolsCount
	logger := log.NewLogger()
	store, err := SetupStorageForTest(logger)
	require.NoError(t, err)
	defer func() {
		if err := store.Close(); err != nil {
			t.Logf("Error closing store: %v", err)
		}
	}()

	ctx := context.Background()
	filter := &utils.Filter{}

	// Close the database to simulate error
	sqlDB, _ := store.DB().DB()
	_ = sqlDB.Close()

	// Test GetPoolsCount with closed database
	count, err := store.GetPoolsCount(ctx, filter)
	assert.Error(t, err)
	assert.Equal(t, int64(0), count)
}

func TestListPoolsWithPagination_Cases(t *testing.T) {
	t.Run("WhenNoPoolsExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		conditions := [][]interface{}{
			{"account_id", "=", 999}, // Non-existent account ID
		}
		pagination := &utils.Pagination{Limit: 10, Offset: 0}
		pools, err := store.ListPoolsWithPagination(context.Background(), conditions, pagination)
		assert.NoError(tt, err)
		assert.Equal(tt, 0, len(pools))
	})

	t.Run("WhenPaginationIsNil", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		assert.NoError(tt, store.db.Create(account).Error())

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			Account:   account,
		}
		assert.NoError(tt, store.db.Create(pool).Error())

		conditions := [][]interface{}{
			{"account_id", "=", account.ID},
		}

		result, err := store.ListPoolsWithPagination(context.Background(), conditions, nil)
		assert.NoError(tt, err)
		assert.Equal(tt, 1, len(result))
	})

	t.Run("WhenDeletedPoolsAreIncluded", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		assert.NoError(tt, store.db.Create(account).Error())

		// Create 2 pools, one deleted
		pool1 := &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{UUID: "test-pool-uuid-1"},
			Name:           "test_pool_1",
			AccountID:      account.ID,
			Account:        account,
			DeploymentName: "deployment-1",
		}
		assert.NoError(tt, store.db.Create(pool1).Error())

		pool2 := &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{UUID: "test-pool-uuid-2", DeletedAt: &gorm.DeletedAt{Time: time.Now(), Valid: true}},
			Name:           "test_pool_2",
			AccountID:      account.ID,
			Account:        account,
			DeploymentName: "deployment-2",
		}
		assert.NoError(tt, store.db.Create(pool2).Error())

		// Use Unscoped filter to include deleted pools
		conditions := [][]interface{}{
			{"account_id = ?", account.ID},
		}
		pagination := &utils.Pagination{Limit: 10, Offset: 0}
		result, err := store.ListPoolsWithPagination(context.Background(), conditions, pagination)
		assert.NoError(tt, err)
		assert.Equal(tt, 2, len(result), "Expected 2 pools including deleted, got %v", len(result))
		var foundDeleted bool
		for _, p := range result {
			if p.Name == "test_pool_2" && p.DeletedAt.Valid {
				foundDeleted = true
			}
		}
		assert.True(tt, foundDeleted, "Expected deleted pool to be present in result set")
	})
}

func TestCreatingPool_VendorIDUniqueness(t *testing.T) {
	t.Run("AllowsSamePoolNameInDifferentZonesWithDifferentVendorIDs", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			tt.Fatalf("Failed to clean up test database: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test_account",
		}
		err = store.db.Create(account).Error()
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		// Create first pool in region (australia-southeast1)
		// VendorID includes location in the path
		poolRegion := &datamodel.Pool{
			Name:           "nitin-pool-1754107056",
			VendorID:       "/projects/29632252492/locations/australia-southeast1/pools/nitin-pool-1754107056",
			AccountID:      account.ID,
			Account:        account,
			DeploymentName: "deployment-region",
			PoolAttributes: &datamodel.PoolAttributes{
				PrimaryZone:     "australia-southeast1",
				ThroughputMibps: 64,
				Iops:            1024,
			},
		}

		createdPoolRegion, err := store.CreatingPool(context.Background(), poolRegion)
		assert.NoError(tt, err)
		assert.NotNil(tt, createdPoolRegion)
		assert.Equal(tt, "/projects/29632252492/locations/australia-southeast1/pools/nitin-pool-1754107056", createdPoolRegion.VendorID)
		assert.Equal(tt, "australia-southeast1", createdPoolRegion.PoolAttributes.PrimaryZone)
		assert.Equal(tt, models.LifeCycleStateCreating, createdPoolRegion.State)

		// Create second pool in zone-a (australia-southeast1-a) with same pool name but different vendor_id
		poolZoneA := &datamodel.Pool{
			Name:           "nitin-pool-1754107056",
			VendorID:       "/projects/29632252492/locations/australia-southeast1-a/pools/nitin-pool-1754107056",
			AccountID:      account.ID,
			Account:        account,
			DeploymentName: "deployment-zone-a",
			PoolAttributes: &datamodel.PoolAttributes{
				PrimaryZone:     "australia-southeast1-a",
				ThroughputMibps: 64,
				Iops:            1024,
			},
		}

		createdPoolZoneA, err := store.CreatingPool(context.Background(), poolZoneA)
		assert.NoError(tt, err)
		assert.NotNil(tt, createdPoolZoneA)
		assert.Equal(tt, "/projects/29632252492/locations/australia-southeast1-a/pools/nitin-pool-1754107056", createdPoolZoneA.VendorID)
		assert.Equal(tt, "australia-southeast1-a", createdPoolZoneA.PoolAttributes.PrimaryZone)
		assert.Equal(tt, models.LifeCycleStateCreating, createdPoolZoneA.State)

		// Create third pool in zone-b (australia-southeast1-b) with same pool name but different vendor_id
		poolZoneB := &datamodel.Pool{
			Name:           "nitin-pool-1754107056",
			VendorID:       "/projects/29632252492/locations/australia-southeast1-b/pools/nitin-pool-1754107056",
			AccountID:      account.ID,
			Account:        account,
			DeploymentName: "deployment-zone-b",
			PoolAttributes: &datamodel.PoolAttributes{
				PrimaryZone:     "australia-southeast1-b",
				ThroughputMibps: 64,
				Iops:            1024,
			},
		}

		createdPoolZoneB, err := store.CreatingPool(context.Background(), poolZoneB)
		assert.NoError(tt, err)
		assert.NotNil(tt, createdPoolZoneB)
		assert.Equal(tt, "/projects/29632252492/locations/australia-southeast1-b/pools/nitin-pool-1754107056", createdPoolZoneB.VendorID)
		assert.Equal(tt, "australia-southeast1-b", createdPoolZoneB.PoolAttributes.PrimaryZone)
		assert.Equal(tt, models.LifeCycleStateCreating, createdPoolZoneB.State)

		// Verify all three pools exist with same name but different VendorIDs and UUIDs
		assert.Equal(tt, "nitin-pool-1754107056", createdPoolRegion.Name)
		assert.Equal(tt, "nitin-pool-1754107056", createdPoolZoneA.Name)
		assert.Equal(tt, "nitin-pool-1754107056", createdPoolZoneB.Name)
		assert.NotEqual(tt, createdPoolRegion.VendorID, createdPoolZoneA.VendorID)
		assert.NotEqual(tt, createdPoolRegion.VendorID, createdPoolZoneB.VendorID)
		assert.NotEqual(tt, createdPoolZoneA.VendorID, createdPoolZoneB.VendorID)
		assert.NotEqual(tt, createdPoolRegion.UUID, createdPoolZoneA.UUID)
		assert.NotEqual(tt, createdPoolRegion.UUID, createdPoolZoneB.UUID)
		assert.NotEqual(tt, createdPoolZoneA.UUID, createdPoolZoneB.UUID)
	})

	t.Run("PreventsDuplicateVendorIDInSameAccount", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			tt.Fatalf("Failed to clean up test database: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test_account",
		}
		err = store.db.Create(account).Error()
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		// Create first pool with a vendor_id
		pool1 := &datamodel.Pool{
			Name:           "pool_1",
			VendorID:       "vendor-pool-duplicate",
			AccountID:      account.ID,
			Account:        account,
			DeploymentName: "deployment-1",
		}

		createdPool1, err := store.CreatingPool(context.Background(), pool1)
		assert.NoError(tt, err)
		assert.NotNil(tt, createdPool1)

		// Try to create another pool with the same vendor_id in the same account
		pool2 := &datamodel.Pool{
			Name:           "pool_2",
			VendorID:       "vendor-pool-duplicate",
			AccountID:      account.ID,
			Account:        account,
			DeploymentName: "deployment-2",
		}

		_, err = store.CreatingPool(context.Background(), pool2)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Invalid input parameters provided")
		assert.Contains(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "pool already exists")
	})

	t.Run("AllowsSameVendorIDInDifferentAccounts", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			tt.Fatalf("Failed to clean up test database: %v", err)
		}

		// Create two accounts
		account1 := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid-1",
			},
			Name: "test_account_1",
		}
		account2 := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   2,
				UUID: "test-account-uuid-2",
			},
			Name: "test_account_2",
		}
		err = store.db.Create(account1).Error()
		if err != nil {
			tt.Fatalf("Failed to create account1: %v", err)
		}
		err = store.db.Create(account2).Error()
		if err != nil {
			tt.Fatalf("Failed to create account2: %v", err)
		}

		// Create pool in account1 with a vendor_id
		pool1 := &datamodel.Pool{
			Name:           "pool_account1",
			VendorID:       "vendor-pool-shared",
			AccountID:      account1.ID,
			Account:        account1,
			DeploymentName: "deployment-account1",
		}

		createdPool1, err := store.CreatingPool(context.Background(), pool1)
		assert.NoError(tt, err)
		assert.NotNil(tt, createdPool1)
		assert.Equal(tt, account1.ID, createdPool1.AccountID)

		// Create pool in account2 with the same vendor_id (should succeed)
		pool2 := &datamodel.Pool{
			Name:           "pool_account2",
			VendorID:       "vendor-pool-shared",
			AccountID:      account2.ID,
			Account:        account2,
			DeploymentName: "deployment-account2",
		}

		createdPool2, err := store.CreatingPool(context.Background(), pool2)
		assert.NoError(tt, err)
		assert.NotNil(tt, createdPool2)
		assert.Equal(tt, account2.ID, createdPool2.AccountID)

		// Verify both pools exist with the same VendorID but different accounts
		assert.Equal(tt, createdPool1.VendorID, createdPool2.VendorID)
		assert.NotEqual(tt, createdPool1.AccountID, createdPool2.AccountID)
		assert.NotEqual(tt, createdPool1.UUID, createdPool2.UUID)
	})
}

func TestUpdatePoolTieringConfig(t *testing.T) {
	t.Run("UpdatesConsumptionSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			tt.Fatalf("Failed to clean up test database: %v", err)
		}

		// Create account and pool with auto_tiering_config
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.db.Create(account).Error()
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:           "test_pool",
			AccountID:      account.ID,
			Account:        account,
			DeploymentName: "test-deployment",
			AutoTieringConfig: &datamodel.AutoTieringConfig{
				HotTierSizeInBytes:      500000000000,
				EnableHotTierAutoResize: true,
				BucketName:              "test-bucket",
				TieringStatus:           datamodel.TieringStatusResumed,
				HotTierConsumption:      0,
				ColdTierConsumption:     0,
			},
		}
		err = store.db.Create(pool).Error()
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		// Update consumption values
		hotTierConsumption := int64(250000000000)  // 250GB
		coldTierConsumption := int64(150000000000) // 150GB

		err = store.UpdatePoolTieringConfig(context.Background(), pool.UUID, &hotTierConsumption, &coldTierConsumption, nil, nil)
		assert.NoError(tt, err)

		// Verify the update
		updatedPool := &datamodel.Pool{}
		err = store.db.GORM().First(updatedPool, "uuid = ?", pool.UUID).Error
		assert.NoError(tt, err)
		assert.NotNil(tt, updatedPool.AutoTieringConfig)
		assert.Equal(tt, hotTierConsumption, updatedPool.AutoTieringConfig.HotTierConsumption)
		assert.Equal(tt, coldTierConsumption, updatedPool.AutoTieringConfig.ColdTierConsumption)

		// Verify other fields remain unchanged
		assert.Equal(tt, pool.AutoTieringConfig.HotTierSizeInBytes, updatedPool.AutoTieringConfig.HotTierSizeInBytes)
		assert.Equal(tt, pool.AutoTieringConfig.EnableHotTierAutoResize, updatedPool.AutoTieringConfig.EnableHotTierAutoResize)
		assert.Equal(tt, pool.AutoTieringConfig.BucketName, updatedPool.AutoTieringConfig.BucketName)
		assert.Equal(tt, pool.AutoTieringConfig.TieringStatus, updatedPool.AutoTieringConfig.TieringStatus)
	})

	t.Run("UpdatesConsumptionToZero", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			tt.Fatalf("Failed to clean up test database: %v", err)
		}

		// Create account and pool with auto_tiering_config
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.db.Create(account).Error()
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:           "test_pool",
			AccountID:      account.ID,
			Account:        account,
			DeploymentName: "test-deployment",
			AutoTieringConfig: &datamodel.AutoTieringConfig{
				HotTierSizeInBytes:      500000000000,
				EnableHotTierAutoResize: true,
				BucketName:              "test-bucket",
				TieringStatus:           datamodel.TieringStatusResumed,
				HotTierConsumption:      100000000000,
				ColdTierConsumption:     50000000000,
			},
		}
		err = store.db.Create(pool).Error()
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		// Update consumption values to zero
		zero := int64(0)
		err = store.UpdatePoolTieringConfig(context.Background(), pool.UUID, &zero, &zero, nil, nil)
		assert.NoError(tt, err)

		// Verify the update
		updatedPool := &datamodel.Pool{}
		err = store.db.GORM().First(updatedPool, "uuid = ?", pool.UUID).Error
		assert.NoError(tt, err)
		assert.NotNil(tt, updatedPool.AutoTieringConfig)
		assert.Equal(tt, int64(0), updatedPool.AutoTieringConfig.HotTierConsumption)
		assert.Equal(tt, int64(0), updatedPool.AutoTieringConfig.ColdTierConsumption)
	})

	t.Run("ReturnsErrorWhenPoolDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			tt.Fatalf("Failed to clean up test database: %v", err)
		}

		// Try to update consumption for non-existent pool
		hot := int64(100000000000)
		cold := int64(50000000000)
		err = store.UpdatePoolTieringConfig(context.Background(), "non-existent-uuid", &hot, &cold, nil, nil)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Resource not found")
	})

	t.Run("ReturnsErrorWhenAutoTieringConfigIsNull", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			tt.Fatalf("Failed to clean up test database: %v", err)
		}

		// Create account and pool without auto_tiering_config
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.db.Create(account).Error()
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel:         datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:              "test_pool",
			AccountID:         account.ID,
			Account:           account,
			DeploymentName:    "test-deployment",
			AutoTieringConfig: nil, // No auto-tiering config
		}
		err = store.db.Create(pool).Error()
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		// Try to update consumption
		hot := int64(100000000000)
		cold := int64(50000000000)
		err = store.UpdatePoolTieringConfig(context.Background(), pool.UUID, &hot, &cold, nil, nil)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Resource not found")
	})

	t.Run("UpdatesMultipleTimesSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			tt.Fatalf("Failed to clean up test database: %v", err)
		}

		// Create account and pool
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.db.Create(account).Error()
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:           "test_pool",
			AccountID:      account.ID,
			Account:        account,
			DeploymentName: "test-deployment",
			AutoTieringConfig: &datamodel.AutoTieringConfig{
				HotTierSizeInBytes:      500000000000,
				EnableHotTierAutoResize: true,
				BucketName:              "test-bucket",
				TieringStatus:           datamodel.TieringStatusResumed,
				HotTierConsumption:      0,
				ColdTierConsumption:     0,
			},
		}
		err = store.db.Create(pool).Error()
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		// First update
		hot1 := int64(100000000000)
		cold1 := int64(50000000000)
		err = store.UpdatePoolTieringConfig(context.Background(), pool.UUID, &hot1, &cold1, nil, nil)
		assert.NoError(tt, err)

		// Verify first update
		updatedPool := &datamodel.Pool{}
		err = store.db.GORM().First(updatedPool, "uuid = ?", pool.UUID).Error
		assert.NoError(tt, err)
		assert.Equal(tt, int64(100000000000), updatedPool.AutoTieringConfig.HotTierConsumption)
		assert.Equal(tt, int64(50000000000), updatedPool.AutoTieringConfig.ColdTierConsumption)

		// Second update with different values
		hot2 := int64(200000000000)
		cold2 := int64(100000000000)
		err = store.UpdatePoolTieringConfig(context.Background(), pool.UUID, &hot2, &cold2, nil, nil)
		assert.NoError(tt, err)

		// Verify second update
		err = store.db.GORM().First(updatedPool, "uuid = ?", pool.UUID).Error
		assert.NoError(tt, err)
		assert.Equal(tt, int64(200000000000), updatedPool.AutoTieringConfig.HotTierConsumption)
		assert.Equal(tt, int64(100000000000), updatedPool.AutoTieringConfig.ColdTierConsumption)

		// Verify other fields remain unchanged
		assert.Equal(tt, int64(500000000000), updatedPool.AutoTieringConfig.HotTierSizeInBytes)
		assert.Equal(tt, true, updatedPool.AutoTieringConfig.EnableHotTierAutoResize)
		assert.Equal(tt, "test-bucket", updatedPool.AutoTieringConfig.BucketName)
		assert.Equal(tt, datamodel.TieringStatusResumed, updatedPool.AutoTieringConfig.TieringStatus)
	})

	t.Run("UpdatesUpdatedAtTimestamp", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			tt.Fatalf("Failed to clean up test database: %v", err)
		}

		// Create account and pool
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.db.Create(account).Error()
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:           "test_pool",
			AccountID:      account.ID,
			Account:        account,
			DeploymentName: "test-deployment",
			AutoTieringConfig: &datamodel.AutoTieringConfig{
				HotTierSizeInBytes:      500000000000,
				EnableHotTierAutoResize: true,
				HotTierConsumption:      0,
				ColdTierConsumption:     0,
			},
		}
		err = store.db.Create(pool).Error()
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		// Get original updated_at timestamp
		originalPool := &datamodel.Pool{}
		err = store.db.GORM().First(originalPool, "uuid = ?", pool.UUID).Error
		assert.NoError(tt, err)
		originalUpdatedAt := originalPool.UpdatedAt

		// Wait to ensure timestamp will be different
		time.Sleep(10 * time.Millisecond)

		// Update consumption
		hot := int64(100000000000)
		cold := int64(50000000000)
		err = store.UpdatePoolTieringConfig(context.Background(), pool.UUID, &hot, &cold, nil, nil)
		assert.NoError(tt, err)

		// Verify updated_at has changed
		updatedPool := &datamodel.Pool{}
		err = store.db.GORM().First(updatedPool, "uuid = ?", pool.UUID).Error
		assert.NoError(tt, err)
		assert.True(tt, updatedPool.UpdatedAt.After(originalUpdatedAt), "UpdatedAt should be updated")
	})

	t.Run("HandlesLargeConsumptionValues", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			tt.Fatalf("Failed to clean up test database: %v", err)
		}

		// Create account and pool
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.db.Create(account).Error()
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:           "test_pool",
			AccountID:      account.ID,
			Account:        account,
			DeploymentName: "test-deployment",
			AutoTieringConfig: &datamodel.AutoTieringConfig{
				HotTierSizeInBytes:  1000000000000, // 1TB
				HotTierConsumption:  0,
				ColdTierConsumption: 0,
			},
		}
		err = store.db.Create(pool).Error()
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		// Update with large values (in the TB range)
		hotTierConsumption := int64(5000000000000)   // 5TB
		coldTierConsumption := int64(10000000000000) // 10TB

		err = store.UpdatePoolTieringConfig(context.Background(), pool.UUID, &hotTierConsumption, &coldTierConsumption, nil, nil)
		assert.NoError(tt, err)

		// Verify the update
		updatedPool := &datamodel.Pool{}
		err = store.db.GORM().First(updatedPool, "uuid = ?", pool.UUID).Error
		assert.NoError(tt, err)
		assert.Equal(tt, hotTierConsumption, updatedPool.AutoTieringConfig.HotTierConsumption)
		assert.Equal(tt, coldTierConsumption, updatedPool.AutoTieringConfig.ColdTierConsumption)
	})

	t.Run("PreservesOtherAutoTieringConfigFields", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			tt.Fatalf("Failed to clean up test database: %v", err)
		}

		// Create account and pool with all auto_tiering_config fields populated
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.db.Create(account).Error()
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:           "test_pool",
			AccountID:      account.ID,
			Account:        account,
			DeploymentName: "test-deployment",
			AutoTieringConfig: &datamodel.AutoTieringConfig{
				HotTierSizeInBytes:      500000000000,
				EnableHotTierAutoResize: true,
				BucketName:              "my-test-bucket",
				TieringStatus:           datamodel.TieringStatusPaused,
				HotTierConsumption:      100000000,
				ColdTierConsumption:     200000000,
			},
		}
		err = store.db.Create(pool).Error()
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		// Update only consumption fields
		newHotTier := int64(300000000000)
		newColdTier := int64(150000000000)
		err = store.UpdatePoolTieringConfig(context.Background(), pool.UUID, &newHotTier, &newColdTier, nil, nil)
		assert.NoError(tt, err)

		// Verify only consumption fields changed
		updatedPool := &datamodel.Pool{}
		err = store.db.GORM().First(updatedPool, "uuid = ?", pool.UUID).Error
		assert.NoError(tt, err)
		assert.NotNil(tt, updatedPool.AutoTieringConfig)

		// Check updated fields
		assert.Equal(tt, newHotTier, updatedPool.AutoTieringConfig.HotTierConsumption)
		assert.Equal(tt, newColdTier, updatedPool.AutoTieringConfig.ColdTierConsumption)

		// Check preserved fields
		assert.Equal(tt, int64(500000000000), updatedPool.AutoTieringConfig.HotTierSizeInBytes)
		assert.Equal(tt, true, updatedPool.AutoTieringConfig.EnableHotTierAutoResize)
		assert.Equal(tt, "my-test-bucket", updatedPool.AutoTieringConfig.BucketName)
		assert.Equal(tt, datamodel.TieringStatusPaused, updatedPool.AutoTieringConfig.TieringStatus)
	})
}

// Unit tests for GetPoolByID
func TestDataStoreRepository_GetPoolByID(t *testing.T) {
	db, err := SetupTestDB()
	require.NoError(t, err)
	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)
	err = ClearInMemoryDB(store.db.GORM())
	require.NoError(t, err)

	// Create test account and pool
	account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"}, Name: "test_account"}
	require.NoError(t, store.db.Create(account).Error())
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 42, UUID: "test-pool-uuid"}, Name: "test_pool", AccountID: account.ID, Account: account}
	require.NoError(t, store.db.Create(pool).Error())

	t.Run("ReturnsPoolWhenExists", func(tt *testing.T) {
		result, err := store.GetPoolByID(context.Background(), pool.ID)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, pool.ID, result.ID)
		assert.Equal(tt, pool.UUID, result.UUID)
	})

	t.Run("ReturnsNotFoundErrorWhenPoolDoesNotExist", func(tt *testing.T) {
		result, err := store.GetPoolByID(context.Background(), 9999)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "not found")
	})

	t.Run("ReturnsErrorOnDBFailure", func(tt *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(tt, err)
		gdb, err := gorm.Open(postgres.New(postgres.Config{Conn: db}), &gorm.Config{})
		require.NoError(tt, err)
		wrapper := gormwrapper.New(gdb)
		store := NewDataStoreRepository(wrapper)

		mock.ExpectQuery("SELECT .* FROM \"pools\" WHERE id = .*").WillReturnError(errors.New("db error"))
		result, err := store.GetPoolByID(context.Background(), 123)
		assert.Error(tt, err)
		assert.Nil(tt, result)
	})
}

// Unit tests for GetPoolByUUID
func TestDataStoreRepository_GetPoolByUUID(t *testing.T) {
	db, err := SetupTestDB()
	require.NoError(t, err)
	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)
	err = ClearInMemoryDB(store.db.GORM())
	require.NoError(t, err)

	// Create test account and pool
	account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"}, Name: "test_account"}
	require.NoError(t, store.db.Create(account).Error())
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"}, Name: "test_pool", AccountID: account.ID, Account: account}
	require.NoError(t, store.db.Create(pool).Error())

	t.Run("ReturnsPoolWhenExists", func(tt *testing.T) {
		result, err := store.GetPoolByUUID(context.Background(), pool.UUID)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, pool.UUID, result.UUID)
		assert.Equal(tt, pool.Name, result.Name)
	})

	t.Run("ReturnsNotFoundErrorWhenPoolDoesNotExist", func(tt *testing.T) {
		// This test case covers line 113 in pools.go
		// When Find doesn't find a record, it returns no error but pool.UUID is empty
		result, err := store.GetPoolByUUID(context.Background(), "non-existent-uuid")
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.True(tt, customerrors.IsNotFoundErr(err))
		assert.Contains(tt, err.Error(), "not found")
	})

	t.Run("ReturnsErrorOnDBFailure", func(tt *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(tt, err)
		gdb, err := gorm.Open(postgres.New(postgres.Config{Conn: db}), &gorm.Config{})
		require.NoError(tt, err)
		wrapper := gormwrapper.New(gdb)
		store := NewDataStoreRepository(wrapper)

		mock.ExpectQuery("SELECT .* FROM \"pools\" WHERE uuid = .*").WillReturnError(errors.New("db error"))
		result, err := store.GetPoolByUUID(context.Background(), "test-uuid")
		assert.Error(tt, err)
		assert.Nil(tt, result)
	})
}

func TestListExpertModePool(t *testing.T) {
	t.Run("ReturnsAllOntapPools", func(tt *testing.T) {
		store := setup(tt)
		ctx := context.Background()

		// Create test account
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err := store.db.Create(account).Error()
		require.NoError(tt, err)
		defer store.db.Delete(account)

		// Create pools with different states and api_access_mode
		pools := []*datamodel.Pool{
			{
				BaseModel:      datamodel.BaseModel{UUID: "pool-ready-ontap"},
				Name:           "pool_ready_ontap",
				AccountID:      account.ID,
				State:          models.LifeCycleStateREADY,
				APIAccessMode:  "ONTAP",
				DeploymentName: "dep-ready-ontap",
			},
			{
				BaseModel:      datamodel.BaseModel{UUID: "pool-available-ontap"},
				Name:           "pool_available_ontap",
				AccountID:      account.ID,
				State:          models.LifeCycleStateAvailable,
				APIAccessMode:  "ONTAP",
				DeploymentName: "dep-available-ontap",
			},
			{
				BaseModel:      datamodel.BaseModel{UUID: "pool-ready-nonontap"},
				Name:           "pool_ready_nonontap",
				AccountID:      account.ID,
				State:          models.LifeCycleStateREADY,
				APIAccessMode:  "NFS",
				DeploymentName: "dep-ready-nonontap",
			},
			{
				BaseModel:      datamodel.BaseModel{UUID: "pool-creating-ontap"},
				Name:           "pool_creating_ontap",
				AccountID:      account.ID,
				State:          models.LifeCycleStateCreating,
				APIAccessMode:  "ONTAP",
				DeploymentName: "dep-creating-ontap",
			},
			{
				BaseModel:      datamodel.BaseModel{UUID: "pool-deleting-ontap"},
				Name:           "pool_deleting_ontap",
				AccountID:      account.ID,
				State:          models.LifeCycleStateDeleting,
				APIAccessMode:  "ONTAP",
				DeploymentName: "dep-deleting-ontap",
			},
		}

		for _, pool := range pools {
			err := store.db.Create(pool).Error()
			require.NoError(tt, err)
		}

		// Call ListExpertModePools
		result, err := store.ListExpertModePools(ctx)

		// Should return all pools with api_access_mode = ONTAP (regardless of state)
		assert.NoError(tt, err)
		assert.Len(tt, result, 4) // All 4 ONTAP pools: ready, available, creating, deleting

		// Verify the returned pools
		poolUUIDs := make(map[string]bool)
		for _, pool := range result {
			poolUUIDs[pool.UUID] = true
			assert.Equal(tt, "ONTAP", pool.APIAccessMode)
		}

		assert.True(tt, poolUUIDs["pool-ready-ontap"], "pool-ready-ontap should be in results")
		assert.True(tt, poolUUIDs["pool-available-ontap"], "pool-available-ontap should be in results")
		assert.True(tt, poolUUIDs["pool-creating-ontap"], "pool-creating-ontap should be in results")
		assert.True(tt, poolUUIDs["pool-deleting-ontap"], "pool-deleting-ontap should be in results")
		assert.False(tt, poolUUIDs["pool-ready-nonontap"], "pool-ready-nonontap should not be in results (wrong api_access_mode)")
	})

	t.Run("ReturnsEmptySliceWhenNoMatchingPools", func(tt *testing.T) {
		store := setup(tt)
		ctx := context.Background()

		// Create test account
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err := store.db.Create(account).Error()
		require.NoError(tt, err)
		defer store.db.Delete(account)

		// Create pools that don't match the criteria (only non-ONTAP pools)
		pools := []*datamodel.Pool{
			{
				BaseModel:      datamodel.BaseModel{UUID: "pool-ready-nfs"},
				Name:           "pool_ready_nfs",
				AccountID:      account.ID,
				State:          models.LifeCycleStateREADY,
				APIAccessMode:  "NFS",
				DeploymentName: "dep-ready-nfs",
			},
		}

		for _, pool := range pools {
			err := store.db.Create(pool).Error()
			require.NoError(tt, err)
		}

		// Call ListExpertModePools
		result, err := store.ListExpertModePools(ctx)

		assert.NoError(tt, err)
		assert.Empty(tt, result) // No ONTAP pools, so should be empty
	})

	t.Run("ReturnsEmptySliceWhenNoPoolsExist", func(tt *testing.T) {
		store := setup(tt)
		ctx := context.Background()

		result, err := store.ListExpertModePools(ctx)

		assert.NoError(tt, err)
		assert.Empty(tt, result)
	})

	t.Run("ExcludesDeletedPools", func(tt *testing.T) {
		store := setup(tt)
		ctx := context.Background()

		// Create test account
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err := store.db.Create(account).Error()
		require.NoError(tt, err)
		defer store.db.Delete(account)

		// Create active pool
		activePool := &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{UUID: "pool-active-ontap"},
			Name:           "pool_active_ontap",
			AccountID:      account.ID,
			State:          models.LifeCycleStateREADY,
			APIAccessMode:  "ONTAP",
			DeploymentName: "dep-active-ontap",
		}
		err = store.db.Create(activePool).Error()
		require.NoError(tt, err)

		// Create deleted pool (soft delete)
		deletedPool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				UUID:      "pool-deleted-ontap",
				DeletedAt: &gorm.DeletedAt{Time: time.Now(), Valid: true},
			},
			Name:           "pool_deleted_ontap",
			AccountID:      account.ID,
			State:          models.LifeCycleStateREADY,
			APIAccessMode:  "ONTAP",
			DeploymentName: "dep-deleted-ontap",
		}
		err = store.db.Create(deletedPool).Error()
		require.NoError(tt, err)

		// Call ListExpertModePools
		result, err := store.ListExpertModePools(ctx)

		assert.NoError(tt, err)
		assert.Len(tt, result, 1)
		assert.Equal(tt, "pool-active-ontap", result[0].UUID)
		assert.NotEqual(tt, "pool-deleted-ontap", result[0].UUID)
	})

	t.Run("ReturnsMultipleOntapPools", func(tt *testing.T) {
		store := setup(tt)
		ctx := context.Background()

		// Create test account
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err := store.db.Create(account).Error()
		require.NoError(tt, err)
		defer store.db.Delete(account)

		// Create pools with ONTAP api_access_mode
		pools := []*datamodel.Pool{
			{
				BaseModel:      datamodel.BaseModel{UUID: "pool-ready-1"},
				Name:           "pool_ready_1",
				AccountID:      account.ID,
				State:          models.LifeCycleStateREADY,
				APIAccessMode:  "ONTAP",
				DeploymentName: "dep-ready-1",
			},
			{
				BaseModel:      datamodel.BaseModel{UUID: "pool-ready-2"},
				Name:           "pool_ready_2",
				AccountID:      account.ID,
				State:          models.LifeCycleStateREADY,
				APIAccessMode:  "ONTAP",
				DeploymentName: "dep-ready-2",
			},
			{
				BaseModel:      datamodel.BaseModel{UUID: "pool-available-1"},
				Name:           "pool_available_1",
				AccountID:      account.ID,
				State:          models.LifeCycleStateAvailable,
				APIAccessMode:  "ONTAP",
				DeploymentName: "dep-available-1",
			},
		}

		for _, pool := range pools {
			err := store.db.Create(pool).Error()
			require.NoError(tt, err)
		}

		// Call ListExpertModePools
		result, err := store.ListExpertModePools(ctx)

		assert.NoError(tt, err)
		assert.Len(tt, result, 3)

		// Verify all returned pools have ONTAP api_access_mode (regardless of state)
		for _, pool := range result {
			assert.Equal(tt, "ONTAP", pool.APIAccessMode)
		}
	})
}
