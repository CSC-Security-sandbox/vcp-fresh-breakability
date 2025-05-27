package repository

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/gorm"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"gorm.io/gorm"
)

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

		result, err := store.GetPoolByVendorID(context.Background(), "test-pool-vendor-id")
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

		_, err = store.GetPoolByVendorID(context.Background(), "test-pool-vendor-id")
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
		if err.Error() != "pool already exists" {
			tt.Errorf("Expected error 'pool already exists', got %v", err)
		}
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

		err = store.SavePoolWithVsaClusterDetails(context.Background(), pool, clusterDetails)
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

		err = store.UpdatePool(context.Background(), pool)
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
