package repository

import (
	"context"
	"errors"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
	"testing"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/gorm"
)

func TestGetLifByNodeID(t *testing.T) {
	t.Run("WhenLifExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		node := &datamodel.Node{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-node-uuid",
			},
			Name:      "test_node",
			AccountID: int64(1234),
			PoolID:    1234,
		}
		err = store.db.Create(node).Error()
		if err != nil {
			tt.Fatalf("Failed to create node: %v", err)
		}

		lif := &datamodel.Lif{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-svm-uuid",
			},
			Name:      "test_lif",
			NodeID:    node.ID,
			AccountID: node.AccountID,
		}
		err = store.db.Create(lif).Error()
		if err != nil {
			tt.Fatalf("Failed to create lif: %v", err)
		}

		result, err := store.GetLifByNodeID(context.Background(), lif.NodeID, lif.AccountID)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, lif.Name, result.Name, "Expected lif name %v, got %v", lif.Name, result.Name)
		assert.Equal(tt, lif.NodeID, result.NodeID, "Expected lif node id %v, got %v", lif.NodeID, result.NodeID)
		assert.Equal(tt, lif.AccountID, result.AccountID, "Expected lif account id %v, got %v", lif.AccountID, result.AccountID)
	})
	t.Run("WhenLifDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		_, err1 := store.GetLifByNodeID(context.Background(), 1, 1234)
		assert.EqualError(tt, err1, "lif not found")
	})
}

func TestCreateLif(t *testing.T) {
	t.Run("WhenLifIsCreatedSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		node := &datamodel.Node{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-node-uuid",
			},
			Name:      "test_node",
			AccountID: int64(1234),
			PoolID:    1234,
		}
		err = store.db.Create(node).Error()
		if err != nil {
			tt.Fatalf("Failed to create node: %v", err)
		}

		lif := &datamodel.Lif{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-svm-uuid",
			},
			Name:      "test_lif",
			NodeID:    node.ID,
			AccountID: node.AccountID,
		}

		createdLif, err := store.CreateLif(context.Background(), lif)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, lif.Name, createdLif.Name, "Expected lif name %v, got %v", lif.Name, createdLif.Name)
		assert.Equal(tt, lif.NodeID, createdLif.NodeID, "Expected lif node id %v, got %v", lif.NodeID, createdLif.NodeID)
		assert.Equal(tt, lif.AccountID, createdLif.AccountID, "Expected lif account id %v, got %v", lif.AccountID, createdLif.AccountID)
	})
	t.Run("WhenLifAlreadyExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		node := &datamodel.Node{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-node-uuid",
			},
			Name:      "test_node",
			AccountID: int64(1234),
			PoolID:    1234,
		}
		err = store.db.Create(node).Error()
		if err != nil {
			tt.Fatalf("Failed to create node: %v", err)
		}

		lif := &datamodel.Lif{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-svm-uuid",
			},
			Name:      "test_lif",
			NodeID:    node.ID,
			AccountID: node.AccountID,
		}
		err = store.db.Create(lif).Error()
		if err != nil {
			tt.Fatalf("Failed to create lif: %v", err)
		}

		_, err = store.CreateLif(context.Background(), lif)
		assert.EqualError(tt, err, "lif already exists")
	})
}

func TestDeleteLif(t *testing.T) {
	t.Run("WhenLifIsDeletedSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		node := &datamodel.Node{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-node-uuid",
			},
			Name:      "test_node",
			AccountID: int64(1234),
			PoolID:    1234,
		}
		err = store.db.Create(node).Error()
		if err != nil {
			tt.Fatalf("Failed to create node: %v", err)
		}

		lif := &datamodel.Lif{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-svm-uuid",
			},
			Name:      "test_lif",
			NodeID:    node.ID,
			AccountID: node.AccountID,
		}
		err = store.db.Create(lif).Error()
		if err != nil {
			tt.Fatalf("Failed to create lif: %v", err)
		}

		err = store.DeleteLif(context.Background(), lif)
		if err != nil {
			tt.Errorf("Expected no error, got %v", err)
		}

		deletedLif := &datamodel.Lif{}
		err = store.db.GORM().First(deletedLif, "uuid = ?", lif.UUID).Error
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			tt.Errorf("Expected record not found error, got %v", err)
		}
	})
}

func TestGetLifForNode(t *testing.T) {
	t.Run("WhenLifExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		node := &datamodel.Node{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-node-uuid",
			},
			Name:      "test_node",
			AccountID: int64(1234),
			PoolID:    1234,
		}
		err = store.db.Create(node).Error()
		if err != nil {
			tt.Fatalf("Failed to create node: %v", err)
		}

		lif := &datamodel.Lif{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-svm-uuid",
			},
			Name:      "test_lif",
			NodeID:    node.ID,
			AccountID: node.AccountID,
		}
		err = store.db.Create(lif).Error()
		if err != nil {
			tt.Fatalf("Failed to create lif: %v", err)
		}

		result, err := store.GetLifForNode(context.Background(), lif.NodeID, lif.AccountID)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, lif.Name, result.Name, "Expected lif name %v, got %v", lif.Name, result.Name)
		assert.Equal(tt, lif.NodeID, result.NodeID, "Expected lif node id %v, got %v", lif.NodeID, result.NodeID)
		assert.Equal(tt, lif.AccountID, result.AccountID, "Expected lif account id %v, got %v", lif.AccountID, result.AccountID)
	})
}
