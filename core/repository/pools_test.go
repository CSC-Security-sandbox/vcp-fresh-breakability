package repository

import (
	"context"
	"fmt"
	"testing"

	"gorm.io/gorm"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/gorm"
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

		result, err := store.GetPool(context.Background(), "test-pool-uuid")
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

		_, err = store.GetPool(context.Background(), "test-pool-uuid")
		if err == nil {
			tt.Errorf("Expected error, got nil")
		}
		if err != gorm.ErrRecordNotFound {
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
		if err != gorm.ErrRecordNotFound {
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

		createdPool, err := store.CreatePool(context.Background(), pool)
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

		_, err = store.CreatePool(context.Background(), pool)
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
			Nodes: []datamodel.Node{
				{
					InstanceType:      "c5.large",
					ExternalIpAddress: "192.168.1.1",
					InternalIpAddress: "10.0.0.1",
				},
				{
					InstanceType:      "c5.xlarge",
					ExternalIpAddress: "192.168.1.2",
					InternalIpAddress: "10.0.0.2",
				},
			},
		}

		err = store.SavePoolWithVsaClusterDetails(context.Background(), pool.Name, account.Name, clusterDetails)
		if err != nil {
			tt.Errorf("Expected no error, got %v", err)
		}

		updatedPool := &datamodel.Pool{}
		err = store.db.GORM().First(updatedPool, "uuid = ?", pool.UUID).Error
		fmt.Println(updatedPool)
		if err != nil {
			tt.Fatalf("Failed to fetch updated pool: %v", err)
		}
		if updatedPool.ClusterDetails.ExternalName != clusterDetails.ExternalName {
			tt.Errorf("Expected external name %v, got %v", clusterDetails.ExternalName, updatedPool.ClusterDetails.ExternalName)
		}
		if updatedPool.ClusterDetails.OntapVersion != clusterDetails.OntapVersion {
			tt.Errorf("Expected ONTAP version %v, got %v", clusterDetails.OntapVersion, updatedPool.ClusterDetails.OntapVersion)
		}
		if len(updatedPool.ClusterDetails.Nodes) != len(clusterDetails.Nodes) {
			tt.Errorf("Expected %d nodes, got %d", len(clusterDetails.Nodes), len(updatedPool.ClusterDetails.Nodes))
		}
		for i, node := range updatedPool.ClusterDetails.Nodes {
			if node.InstanceType != clusterDetails.Nodes[i].InstanceType {
				tt.Errorf("Expected node instance type %v, got %v", clusterDetails.Nodes[i].InstanceType, node.InstanceType)
			}
			if node.ExternalIpAddress != clusterDetails.Nodes[i].ExternalIpAddress {
				tt.Errorf("Expected external IP %v, got %v", clusterDetails.Nodes[i].ExternalIpAddress, node.ExternalIpAddress)
			}
			if node.InternalIpAddress != clusterDetails.Nodes[i].InternalIpAddress {
				tt.Errorf("Expected internal IP %v, got %v", clusterDetails.Nodes[i].InternalIpAddress, node.InternalIpAddress)
			}
		}
	})

	t.Run("WhenAccountDoesNotExist", func(tt *testing.T) {
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

		clusterDetails := &datamodel.ClusterDetails{
			ExternalName: "test-cluster",
			OntapVersion: "9.10.1",
			Nodes: []datamodel.Node{
				{
					InstanceType:      "c5.large",
					ExternalIpAddress: "192.168.1.1",
					InternalIpAddress: "10.0.0.1",
				},
			},
		}

		err = store.SavePoolWithVsaClusterDetails(context.Background(), "test_pool", "non-existent-account", clusterDetails)
		if err == nil {
			tt.Errorf("Expected error, got nil")
		}
		if err.Error() != "pool not found" {
			tt.Errorf("Expected error 'pool not found', got %v", err)
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

		clusterDetails := &datamodel.ClusterDetails{
			ExternalName: "test-cluster",
			OntapVersion: "9.10.1",
			Nodes: []datamodel.Node{
				{
					InstanceType:      "c5.large",
					ExternalIpAddress: "192.168.1.1",
					InternalIpAddress: "10.0.0.1",
				},
			},
		}

		err = store.SavePoolWithVsaClusterDetails(context.Background(), "non-existent-pool", account.Name, clusterDetails)
		if err == nil {
			tt.Errorf("Expected error, got nil")
		}
		if err.Error() != "pool not found" {
			tt.Errorf("Expected error 'pool not found', got %v", err)
		}
	})
}
