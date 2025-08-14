package database

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils/gorm"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
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
		Name: "test-pool-1", AccountID: account1.ID, Account: account1, DeploymentName: "deployment-name1"}
	pool2 := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid-2"},
		Name: "test-pool-2", AccountID: account2.ID, Account: account2, DeploymentName: "deployment-name2"}
	pool3 := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid-3"},
		Name: "test-pool-3", AccountID: account1.ID, Account: account1, DeploymentName: "deployment-name3"}

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
		assert.EqualError(tt, errDB, "An internal error occurred.")
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

func TestListSnHosts_ReturnsDistinctNonEmptyProjects(t *testing.T) {
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
	}
	pool2 := &datamodel.Pool{
		BaseModel:      datamodel.BaseModel{UUID: "uuid-2"},
		Name:           "pool2",
		AccountID:      account.ID,
		Account:        account,
		SnHostProject:  "project-2",
		DeploymentName: "deployment-2",
	}
	pool3 := &datamodel.Pool{
		BaseModel:      datamodel.BaseModel{UUID: "uuid-3"},
		Name:           "pool3",
		AccountID:      account.ID,
		Account:        account,
		SnHostProject:  "project-1",
		DeploymentName: "deployment-3",
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

	projects, err := store.ListSnHosts(context.Background())
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"project-1", "project-2"}, projects)
}

func TestListSnHosts_ExcludesEmptyAndNullProjects(t *testing.T) {
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
	}
	poolWithEmpty := &datamodel.Pool{
		BaseModel:      datamodel.BaseModel{UUID: "uuid-2"},
		Name:           "pool2",
		AccountID:      account.ID,
		Account:        account,
		SnHostProject:  "",
		DeploymentName: "deployment-2",
	}
	poolWithNull := &datamodel.Pool{
		BaseModel:      datamodel.BaseModel{UUID: "uuid-3"},
		Name:           "pool1",
		AccountID:      account.ID,
		Account:        account,
		SnHostProject:  "project-1",
		DeploymentName: "deployment-3",
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

	projects, err := store.ListSnHosts(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, []string{"project-1"}, projects)
}

func TestListSnHosts_ReturnsEmptySliceWhenNoProjects(t *testing.T) {
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

	projects, err := store.ListSnHosts(context.Background())
	assert.NoError(t, err)
	assert.Empty(t, projects)
}

func TestListSnHosts_ReturnsErrorOnDBFailure(t *testing.T) {
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

	projects, err := store.ListSnHosts(context.Background())
	assert.Error(t, err)
	assert.Nil(t, projects)
}
