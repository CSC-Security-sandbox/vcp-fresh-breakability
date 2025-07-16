package database

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils/gorm"
	"gorm.io/gorm"
)

func TestGetNodesByPoolID(t *testing.T) {
	t.Run("WhenNodeExists", func(tt *testing.T) {
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
			Name:   "test_node",
			PoolID: 1234,
		}
		err = store.db.Create(node).Error()
		if err != nil {
			tt.Fatalf("Failed to create node: %v", err)
		}

		result, err := store.GetNodesByPoolID(context.Background(), node.PoolID)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Len(tt, result, 1)
		assert.Equal(tt, node.Name, result[0].Name, "Expected node name %v, got %v", node.Name, result[0].Name)
	})
	t.Run("WhenNodeDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		result, err := store.GetNodesByPoolID(context.Background(), 12)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, 0, len(result))
	})
}

func TestCreateNode(t *testing.T) {
	t.Run("WhenNodeIsCreatedSuccessfully", func(tt *testing.T) {
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
			AccountID: int64(12),
			PoolID:    1234,
		}

		createdNode, err := store.CreateNode(context.Background(), node)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, node.Name, createdNode.Name, "Expected node name %v, got %v", node.Name, createdNode.Name)
	})
	t.Run("WhenNodeAlreadyExists", func(tt *testing.T) {
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
			AccountID: int64(12),
			PoolID:    1234,
		}
		err = store.db.Create(node).Error()
		if err != nil {
			tt.Fatalf("Failed to create node: %v", err)
		}

		_, err1 := store.CreateNode(context.Background(), node)
		var customErr *vsaerrors.CustomError
		if errors.As(err1, &customErr) {
			assert.EqualError(tt, customErr.Unwrap(), "node already exists")
		} else {
			tt.Fatalf("Expected a CustomError, got %v", err)
		}
	})
}

func TestDeleteNode(t *testing.T) {
	t.Run("WhenNodeIsDeletedSuccessfully", func(tt *testing.T) {
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
			AccountID: int64(12),
			PoolID:    1234,
		}
		err = store.db.Create(node).Error()
		if err != nil {
			tt.Fatalf("Failed to create node: %v", err)
		}

		err = store.DeleteNode(context.Background(), node)
		if err != nil {
			tt.Errorf("Expected no error, got %v", err)
		}

		deletedNode := &datamodel.Node{}
		err = store.db.GORM().First(deletedNode, "uuid = ?", node.UUID).Error
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			tt.Errorf("Expected record not found error, got %v", err)
		}
	})
}

func TestErroredNode(t *testing.T) {
	t.Run("WhenNodeIsMarkedErroredSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		node := &datamodel.Node{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-node-uuid",
			},
			Name:      "test_node",
			AccountID: int64(12),
			PoolID:    1234,
			State:     models.LifeCycleStateREADY,
		}
		err = store.db.Create(node).Error()
		if err != nil {
			tt.Fatalf("Failed to create node: %v", err)
		}

		errMsg := "error during node update"
		err = store.ErroredNode(context.Background(), node, errMsg)
		assert.NoError(tt, err)

		updatedNode := &datamodel.Node{}
		err = store.db.GORM().First(updatedNode, "uuid = ?", node.UUID).Error
		assert.NoError(tt, err)
		assert.Equal(tt, models.LifeCycleStateError, updatedNode.State)
		assert.Equal(tt, errMsg, updatedNode.StateDetails)
		assert.WithinDuration(tt, time.Now(), updatedNode.UpdatedAt, 2*time.Second)
	})

	t.Run("WhenUpdatingNodeFails", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		node := &datamodel.Node{
			BaseModel: datamodel.BaseModel{
				ID:   2,
				UUID: "test-node-uuid-2",
			},
			Name:      "failing_node",
			AccountID: int64(34),
			PoolID:    5678,
			State:     models.LifeCycleStateREADY,
		}
		err = store.db.Create(node).Error()
		if err != nil {
			tt.Fatalf("Failed to create node: %v", err)
		}

		// Force failure by dropping the underlying table
		err = store.db.GORM().Exec("DROP TABLE nodes").Error
		assert.NoError(tt, err)

		errMsg := "simulated update error"
		err = store.ErroredNode(context.Background(), node, errMsg)
		assert.Error(tt, err)
		var vcpErr *vsaerrors.CustomError
		assert.True(tt, errors.As(err, &vcpErr))
		assert.Contains(tt, err.Error(), "no such table")
	})
}

func TestDeletingNode(t *testing.T) {
	t.Run("UpdatesNodeStateToDeletingSuccessfully", func(tt *testing.T) {
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
			AccountID: int64(12),
			PoolID:    1234,
		}
		err = store.db.Create(node).Error()
		if err != nil {
			tt.Fatalf("Failed to create node: %v", err)
		}

		err = store.DeletingNode(context.Background(), node)
		if err != nil {
			tt.Errorf("Expected no error, got %v", err)
		}

		updatedNode := &datamodel.Node{}
		err = store.db.GORM().First(updatedNode, "uuid = ?", node.UUID).Error
		if err != nil {
			tt.Fatalf("Failed to fetch updated node: %v", err)
		}
		if updatedNode.State != models.LifeCycleStateDeleting {
			tt.Errorf("Expected state %v, got %v", models.LifeCycleStateDeleting, updatedNode.State)
		}
		if updatedNode.StateDetails != models.LifeCycleStateDeletingDetails {
			tt.Errorf("Expected state details %v, got %v", models.LifeCycleStateDeletingDetails, updatedNode.StateDetails)
		}
	})
}

func TestCreateNode_Parallel_FileBased(t *testing.T) {
	db, fileName, err := SetupTestFileDB()
	assert.NoError(t, err, "Failed to set up file-based test database")
	defer cleanupTestDBFile(db, fileName)
	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)
	ctx := context.Background()
	numParallel := 5
	names := []string{"nodeA", "nodeB", "nodeC", "nodeD", "nodeE"}
	var wg sync.WaitGroup
	errs := make([]error, numParallel)
	results := make([]*datamodel.Node, numParallel)
	for i := 0; i < numParallel; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			node := &datamodel.Node{
				Name:      names[idx],
				AccountID: int64(idx + 1),
			}
			created, err := store.CreateNode(ctx, node)
			errs[idx] = err
			results[idx] = created
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		assert.NoError(t, err, "goroutine %d failed", i)
		assert.NotNil(t, results[i], "goroutine %d result nil", i)
		if results[i] != nil {
			t.Logf("Node %s created with ID %d, UUID %s", results[i].Name, results[i].ID, results[i].UUID)
		}
	}
	// Validate all nodes exist in DB
	var count int64
	db.Model(&datamodel.Node{}).Count(&count)
	assert.Equal(t, int64(numParallel), count)
}
