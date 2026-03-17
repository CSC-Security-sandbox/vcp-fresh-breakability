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
	t.Run("WhenDBError", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		dbConn, _ := db.DB()
		_ = dbConn.Close()

		result, err := store.GetNodesByPoolID(context.Background(), 1)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		var ce *vsaerrors.CustomError
		if assert.True(tt, errors.As(err, &ce)) {
			assert.Equal(tt, vsaerrors.ErrDatabaseDataReadError, ce.TrackingID)
		}
	})
}

func TestGetNodeByID(t *testing.T) {
	t.Run("WhenNodeExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		node := &datamodel.Node{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-node-uuid"},
			Name:      "test_node",
			PoolID:    1234,
		}
		err = store.db.Create(node).Error()
		assert.NoError(tt, err)

		got, err := store.GetNodeByID(context.Background(), node.ID)
		assert.NoError(tt, err)
		assert.NotNil(tt, got)
		assert.Equal(tt, node.ID, got.ID)
		assert.Equal(tt, node.Name, got.Name)
	})
	t.Run("WhenNodeNotFound", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		got, err := store.GetNodeByID(context.Background(), 99999)
		assert.Error(tt, err)
		assert.Nil(tt, got)
		var ce *vsaerrors.CustomError
		if assert.True(tt, errors.As(err, &ce)) {
			assert.Equal(tt, vsaerrors.ErrDatabaseDataNotFoundError, ce.TrackingID)
		}
	})
	t.Run("WhenDBReadError", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		dbConn, _ := db.DB()
		_ = dbConn.Close()
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		got, err := store.GetNodeByID(context.Background(), 1)
		assert.Error(tt, err)
		assert.Nil(tt, got)
		var ce *vsaerrors.CustomError
		if assert.True(tt, errors.As(err, &ce)) {
			assert.Equal(tt, vsaerrors.ErrDatabaseDataReadError, ce.TrackingID)
		}
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
		assert.Contains(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "no such table")
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

func TestUpdateNodesInstanceType(t *testing.T) {
	t.Run("Success_UpdatesAllNodesInPool", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create nodes with initial instance type
		poolID := int64(100)
		nodes := []*datamodel.Node{
			{
				BaseModel:      datamodel.BaseModel{UUID: "node-1"},
				Name:           "node-1",
				PoolID:         poolID,
				AccountID:      1,
				NodeAttributes: &datamodel.NodeDetails{InstanceType: "c3-standard-4-lssd"},
			},
			{
				BaseModel:      datamodel.BaseModel{UUID: "node-2"},
				Name:           "node-2",
				PoolID:         poolID,
				AccountID:      1,
				NodeAttributes: &datamodel.NodeDetails{InstanceType: "c3-standard-4-lssd"},
			},
		}

		for _, node := range nodes {
			err = store.db.Create(node).Error()
			assert.NoError(tt, err, "Failed to create node")
		}

		// Update instance type
		newInstanceType := "c3-standard-8-lssd"
		err = store.UpdateNodesInstanceType(context.Background(), poolID, newInstanceType)
		assert.NoError(tt, err, "Expected no error updating instance type")

		// Verify all nodes were updated
		updatedNodes, err := store.GetNodesByPoolID(context.Background(), poolID)
		assert.NoError(tt, err)
		assert.Len(tt, updatedNodes, 2)
		for _, node := range updatedNodes {
			assert.NotNil(tt, node.NodeAttributes)
			assert.Equal(tt, newInstanceType, node.NodeAttributes.InstanceType)
		}
	})

	t.Run("Success_HandlesNilNodeAttributes", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		poolID := int64(200)
		node := &datamodel.Node{
			BaseModel:      datamodel.BaseModel{UUID: "node-nil-attrs"},
			Name:           "node-nil",
			PoolID:         poolID,
			AccountID:      1,
			NodeAttributes: nil, // Nil attributes
		}

		err = store.db.Create(node).Error()
		assert.NoError(tt, err, "Failed to create node")

		// Update instance type
		newInstanceType := "c3-standard-16-lssd"
		err = store.UpdateNodesInstanceType(context.Background(), poolID, newInstanceType)
		assert.NoError(tt, err, "Expected no error updating instance type")

		// Verify node was updated with new attributes
		updatedNodes, err := store.GetNodesByPoolID(context.Background(), poolID)
		assert.NoError(tt, err)
		assert.Len(tt, updatedNodes, 1)
		assert.NotNil(tt, updatedNodes[0].NodeAttributes)
		assert.Equal(tt, newInstanceType, updatedNodes[0].NodeAttributes.InstanceType)
	})

	t.Run("Success_NoNodesInPool", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Try to update nodes for a pool with no nodes
		err = store.UpdateNodesInstanceType(context.Background(), 999, "c3-standard-4-lssd")
		assert.NoError(tt, err, "Expected no error when no nodes exist")
	})

	t.Run("Error_DatabaseClosed", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create a node
		poolID := int64(300)
		node := &datamodel.Node{
			BaseModel:      datamodel.BaseModel{UUID: "node-error"},
			Name:           "node-error",
			PoolID:         poolID,
			AccountID:      1,
			NodeAttributes: &datamodel.NodeDetails{InstanceType: "c3-standard-4-lssd"},
		}
		err = store.db.Create(node).Error()
		assert.NoError(tt, err, "Failed to create node")

		// Close the database
		sqlDB, err := db.DB()
		assert.NoError(tt, err)
		err = sqlDB.Close()
		assert.NoError(tt, err)

		// Try to update - should fail
		err = store.UpdateNodesInstanceType(context.Background(), poolID, "c3-standard-8-lssd")
		assert.Error(tt, err, "Expected error when database is closed")
	})
}
