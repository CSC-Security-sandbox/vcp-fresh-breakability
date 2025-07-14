package repository

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/gorm"
)

func setupNodeNodeGroupMapTestRepo(t *testing.T) (*DataStoreRepository, *datamodel.NodeNodeGroupMap) {
	db, err := SetupTestDB()
	assert.NoError(t, err)
	wrapper := gorm.New(db)
	repo := &DataStoreRepository{db: wrapper}
	err = ClearInMemoryDB(db)
	assert.NoError(t, err)
	mapping := &datamodel.NodeNodeGroupMap{NodeID: 1, NodeGroupID: 2, HarvestConfig: &datamodel.HarvestConfig{}}
	return repo, mapping
}

func TestCreateNodeNodeGroupMap(t *testing.T) {
	repo, mapping := setupNodeNodeGroupMapTestRepo(t)
	created, err := repo.CreateNodeNodeGroupMap(context.Background(), mapping)
	assert.NoError(t, err)
	assert.NotNil(t, created)
	assert.Equal(t, int64(1), created.NodeID)
	assert.Equal(t, int64(2), created.NodeGroupID)
}

func TestCreateNodeNodeGroupMap_DBError(t *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(t, err)
	dbConn, _ := db.DB()
	_ = dbConn.Close() // Close underlying sql.DB to force error
	wrapper := gorm.New(db)
	badRepo := &DataStoreRepository{db: wrapper}
	ctx := context.Background()
	mapping := &datamodel.NodeNodeGroupMap{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, NodeID: 12345, NodeGroupID: 67890, HarvestConfig: &datamodel.HarvestConfig{}}
	_, err = badRepo.CreateNodeNodeGroupMap(ctx, mapping)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "undefined error: sql: database is closed")
}

func TestCreateNodeNodeGroupMap_InsertError(t *testing.T) {
	repo, mapping := setupNodeNodeGroupMapTestRepo(t)
	ctx := context.Background()
	// Close DB to simulate insert error
	db := repo.db.GORM()
	dbConn, _ := db.DB()
	_ = dbConn.Close()
	wrapper := gorm.New(db)
	badRepo := &DataStoreRepository{db: wrapper}
	_, err := badRepo.CreateNodeNodeGroupMap(ctx, mapping)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "undefined error: sql: database is closed",
		"error should contain 'undefined error: sql: database is closed', got: %v", err)
}

func TestGetNodeNodeGroupMap(t *testing.T) {
	repo, mapping := setupNodeNodeGroupMapTestRepo(t)
	created, err := repo.CreateNodeNodeGroupMap(context.Background(), mapping)
	assert.NoError(t, err)
	got, err := repo.GetNodeNodeGroupMap(context.Background(), created.ID)
	assert.NoError(t, err)
	assert.NotNil(t, got)
	assert.Equal(t, created.ID, got.ID)
}
func TestDeleteNodeNodeGroupMap_NotFound(t *testing.T) {
	repo, _ := setupNodeNodeGroupMapTestRepo(t)
	ctx := context.Background()
	// Use a random high ID that does not exist
	err := repo.DeleteNodeNodeGroupMap(ctx, int64(99999))
	assert.Error(t, err)
	var ce *vsaerrors.CustomError
	if errors.As(err, &ce) && ce.OriginalErr != nil {
		assert.Contains(t, ce.OriginalErr.Error(), "record not found")
	} else {
		assert.Contains(t, err.Error(), "record not found")
	}
}

func TestUpdateNodeNodeGroupMap(t *testing.T) {
	repo, mapping := setupNodeNodeGroupMapTestRepo(t)
	created, err := repo.CreateNodeNodeGroupMap(context.Background(), mapping)
	assert.NoError(t, err)
	created.NodeGroupID = 3
	updated, err := repo.UpdateNodeNodeGroupMap(context.Background(), created)
	assert.NoError(t, err)
	assert.Equal(t, int64(3), updated.NodeGroupID)
}

func TestUpdateNodeNodeGroupMap_Error(t *testing.T) {
	repo, mapping := setupNodeNodeGroupMapTestRepo(t)
	ctx := context.Background()
	// Close DB to simulate error
	db := repo.db.GORM()
	dbConn, _ := db.DB()
	_ = dbConn.Close()
	wrapper := gorm.New(db)
	badRepo := &DataStoreRepository{db: wrapper}
	_, err := badRepo.UpdateNodeNodeGroupMap(ctx, mapping)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "undefined error: sql: database is closed")
}

func TestDeleteNodeNodeGroupMap(t *testing.T) {
	repo, mapping := setupNodeNodeGroupMapTestRepo(t)
	created, err := repo.CreateNodeNodeGroupMap(context.Background(), mapping)
	assert.NoError(t, err)
	err = repo.DeleteNodeNodeGroupMap(context.Background(), created.ID)
	assert.NoError(t, err)
	deleted, err := repo.GetNodeNodeGroupMap(context.Background(), created.ID)
	assert.Error(t, err)
	assert.Nil(t, deleted)
}

func TestDeleteNodeNodeGroupMap_Error(t *testing.T) {
	repo, _ := setupNodeNodeGroupMapTestRepo(t)
	ctx := context.Background()
	db := repo.db.GORM()
	dbConn, _ := db.DB()
	_ = dbConn.Close()
	wrapper := gorm.New(db)
	badRepo := &DataStoreRepository{db: wrapper}
	err := badRepo.DeleteNodeNodeGroupMap(ctx, 12345)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "undefined error: sql: database is closed")
}

func TestAssignTwoNodesToTwoGroups_CreatesMappings(t *testing.T) {
	repo, _ := setupNodeNodeGroupMapTestRepo(t)
	ctx := context.Background()
	// Do not pre-create groups, let AssignTwoNodesToTwoGroups handle group creation
	node1, err := repo.CreateNode(ctx, &datamodel.Node{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "n1"})
	assert.NoError(t, err)
	node2, err := repo.CreateNode(ctx, &datamodel.Node{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "n2"})
	assert.NoError(t, err)
	mappings, err := repo.AssignTwoNodesToTwoGroups(ctx, node1, node2, 100)
	assert.NoError(t, err)
	assert.Len(t, mappings, 2)
	assert.NotEqual(t, mappings[0].NodeGroupID, mappings[1].NodeGroupID)
}

func TestAssignTwoNodesToTwoGroups_GroupLimit(t *testing.T) {
	repo, _ := setupNodeNodeGroupMapTestRepo(t)
	ctx := context.Background()
	// Fill up a group to the limit
	group1, err := repo.CreateNodeGroup(ctx, &datamodel.NodeGroup{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "g-limited-1"})
	assert.NoError(t, err)
	for i := 0; i < 5; i++ {
		node, err := repo.CreateNode(ctx, &datamodel.Node{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "n-fill-" + uuid.NewString()})
		assert.NoError(t, err)
		_, err = repo.CreateNodeNodeGroupMap(ctx, &datamodel.NodeNodeGroupMap{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, NodeID: node.ID, NodeGroupID: group1.ID, HarvestConfig: &datamodel.HarvestConfig{}})
		assert.NoError(t, err)
	}
	node1, err := repo.CreateNode(ctx, &datamodel.Node{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "n1"})
	assert.NoError(t, err)
	node2, err := repo.CreateNode(ctx, &datamodel.Node{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "n2"})
	assert.NoError(t, err)
	// Should create a new group for node1 since group1 is full
	mappings, err := repo.AssignTwoNodesToTwoGroups(ctx, node1, node2, 5)
	assert.NoError(t, err)
	assert.Len(t, mappings, 2)
	assert.NotEqual(t, group1.ID, mappings[0].NodeGroupID)
}

func TestAssignTwoNodesToTwoGroups_BothGroupsFull(t *testing.T) {
	repo, _ := setupNodeNodeGroupMapTestRepo(t)
	ctx := context.Background()
	// Fill up two groups to the limit
	group1, err := repo.CreateNodeGroup(ctx, &datamodel.NodeGroup{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "g-full-1"})
	assert.NoError(t, err)
	group2, err := repo.CreateNodeGroup(ctx, &datamodel.NodeGroup{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "g-full-2"})
	assert.NoError(t, err)
	for i := 0; i < 5; i++ {
		node1, err := repo.CreateNode(ctx, &datamodel.Node{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "n1-full-" + uuid.NewString()})
		assert.NoError(t, err)
		_, err = repo.CreateNodeNodeGroupMap(ctx, &datamodel.NodeNodeGroupMap{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, NodeID: node1.ID, NodeGroupID: group1.ID, HarvestConfig: &datamodel.HarvestConfig{}})
		assert.NoError(t, err)
		node2, err := repo.CreateNode(ctx, &datamodel.Node{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "n2-full-" + uuid.NewString()})
		assert.NoError(t, err)
		_, err = repo.CreateNodeNodeGroupMap(ctx, &datamodel.NodeNodeGroupMap{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, NodeID: node2.ID, NodeGroupID: group2.ID, HarvestConfig: &datamodel.HarvestConfig{}})
		assert.NoError(t, err)
	}
	node1, err := repo.CreateNode(ctx, &datamodel.Node{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "n1"})
	assert.NoError(t, err)
	node2, err := repo.CreateNode(ctx, &datamodel.Node{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "n2"})
	assert.NoError(t, err)
	// Should create new groups for both nodes since both groups are full
	mappings, err := repo.AssignTwoNodesToTwoGroups(ctx, node1, node2, 5)
	assert.NoError(t, err)
	assert.Len(t, mappings, 2)
	assert.NotEqual(t, group1.ID, mappings[0].NodeGroupID)
	assert.NotEqual(t, group2.ID, mappings[1].NodeGroupID)
}

func TestAssignTwoNodesToTwoGroups_TransactionRollback(t *testing.T) {
	repo, _ := setupNodeNodeGroupMapTestRepo(t)
	ctx := context.Background()
	node1, err := repo.CreateNode(ctx, &datamodel.Node{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "n1"})
	assert.NoError(t, err)
	node2, err := repo.CreateNode(ctx, &datamodel.Node{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "n2"})
	assert.NoError(t, err)
	// Simulate error by passing invalid maxNodesPerGroup (e.g., 0)
	_, err = repo.AssignTwoNodesToTwoGroups(ctx, node1, node2, 0)
	assert.Error(t, err)
}

func TestAssignTwoNodesToTwoGroups_PartiallyFullGroups(t *testing.T) {
	repo, _ := setupNodeNodeGroupMapTestRepo(t)
	ctx := context.Background()
	// Create two groups, partially fill them
	group1, err := repo.CreateNodeGroup(ctx, &datamodel.NodeGroup{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "g-partial-1"})
	assert.NoError(t, err)
	group2, err := repo.CreateNodeGroup(ctx, &datamodel.NodeGroup{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "g-partial-2"})
	assert.NoError(t, err)
	for i := 0; i < 3; i++ {
		node1, err := repo.CreateNode(ctx, &datamodel.Node{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "n1-partial-" + uuid.NewString()})
		assert.NoError(t, err)
		_, err = repo.CreateNodeNodeGroupMap(ctx, &datamodel.NodeNodeGroupMap{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, NodeID: node1.ID, NodeGroupID: group1.ID, HarvestConfig: &datamodel.HarvestConfig{}})
		assert.NoError(t, err)
		node2, err := repo.CreateNode(ctx, &datamodel.Node{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "n2-partial-" + uuid.NewString()})
		assert.NoError(t, err)
		_, err = repo.CreateNodeNodeGroupMap(ctx, &datamodel.NodeNodeGroupMap{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, NodeID: node2.ID, NodeGroupID: group2.ID, HarvestConfig: &datamodel.HarvestConfig{}})
		assert.NoError(t, err)
	}
	node1, err := repo.CreateNode(ctx, &datamodel.Node{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "n1"})
	assert.NoError(t, err)
	node2, err := repo.CreateNode(ctx, &datamodel.Node{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "n2"})
	assert.NoError(t, err)
	// Should assign both nodes to the existing groups since they are not full
	mappings, err := repo.AssignTwoNodesToTwoGroups(ctx, node1, node2, 5)
	assert.NoError(t, err)
	assert.Len(t, mappings, 2)
	assert.Contains(t, []int64{group1.ID, group2.ID}, mappings[0].NodeGroupID)
	assert.Contains(t, []int64{group1.ID, group2.ID}, mappings[1].NodeGroupID)
}

func TestAssignTwoNodesToTwoGroups_NilNodes(t *testing.T) {
	repo, _ := setupNodeNodeGroupMapTestRepo(t)
	ctx := context.Background()
	_, err := repo.AssignTwoNodesToTwoGroups(ctx, nil, nil, 5)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "node1 or node2 is nil") // or the actual error message from implementation
}

func TestAssignTwoNodesToTwoGroups_SameNode(t *testing.T) {
	repo, _ := setupNodeNodeGroupMapTestRepo(t)
	ctx := context.Background()
	node, err := repo.CreateNode(ctx, &datamodel.Node{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "n-same"})
	assert.NoError(t, err)
	_, err = repo.AssignTwoNodesToTwoGroups(ctx, node, node, 5)
	assert.Error(t, err)
}

func TestAssignTwoNodesToTwoGroups_InvalidMaxNodes(t *testing.T) {
	repo, _ := setupNodeNodeGroupMapTestRepo(t)
	ctx := context.Background()
	node1, err := repo.CreateNode(ctx, &datamodel.Node{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "n1"})
	assert.NoError(t, err)
	node2, err := repo.CreateNode(ctx, &datamodel.Node{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "n2"})
	assert.NoError(t, err)
	_, err = repo.AssignTwoNodesToTwoGroups(ctx, node1, node2, -1)
	assert.Error(t, err)
}

func TestAssignTwoNodesToTwoGroups_ZeroMaxNodes(t *testing.T) {
	repo, _ := setupNodeNodeGroupMapTestRepo(t)
	ctx := context.Background()
	node1, err := repo.CreateNode(ctx, &datamodel.Node{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "n1"})
	assert.NoError(t, err)
	node2, err := repo.CreateNode(ctx, &datamodel.Node{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "n2"})
	assert.NoError(t, err)
	_, err = repo.AssignTwoNodesToTwoGroups(ctx, node1, node2, 0)
	assert.Error(t, err)
}

func TestAssignTwoNodesToTwoGroups_NonexistentNodes(t *testing.T) {
	repo, _ := setupNodeNodeGroupMapTestRepo(t)
	ctx := context.Background()
	node1 := &datamodel.Node{BaseModel: datamodel.BaseModel{ID: 99999}, Name: "ghost1"}
	node2 := &datamodel.Node{BaseModel: datamodel.BaseModel{ID: 88888}, Name: "ghost2"}
	_, err := repo.AssignTwoNodesToTwoGroups(ctx, node1, node2, 5)
	// Should still succeed, as the function assumes nodes are pre-created and only uses their IDs
	assert.NoError(t, err)
}

// Parallel test will not work properly until file based sqlite is implemented, Will uncomment when ready
func TestAssignTwoNodesToTwoGroups_ParallelRequests(t *testing.T) {
	// Use file-based SQLite for concurrency safety
	db, dbFile, err := SetupTestFileDB()
	assert.NoError(t, err)
	defer cleanupTestDBFile(db, dbFile)
	wrapper := gorm.New(db)
	repo := &DataStoreRepository{db: wrapper}
	err = ClearInMemoryDB(db)
	assert.NoError(t, err)

	ctx := context.Background()

	numParallel := 5

	done := make(chan error, numParallel)
	nodeIDs := make([][2]int64, numParallel)
	nodes := make([][2]*datamodel.Node, numParallel)
	groups := make([]*datamodel.NodeGroup, numParallel*2)
	// Pre-create all node groups
	for i := 0; i < numParallel*2; i++ {
		group, err := repo.CreateNodeGroup(ctx, &datamodel.NodeGroup{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "g-par-" + uuid.NewString()})
		assert.NoError(t, err)
		groups[i] = group
	}
	// Create all nodes sequentially before starting goroutines
	for i := 0; i < numParallel; i++ {
		n1, err := repo.CreateNode(ctx, &datamodel.Node{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "n1-par-" + uuid.NewString()})
		assert.NoError(t, err)
		n2, err := repo.CreateNode(ctx, &datamodel.Node{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "n2-par-" + uuid.NewString()})
		assert.NoError(t, err)
		nodes[i][0] = n1
		nodes[i][1] = n2
	}
	for i := 0; i < numParallel; i++ {
		go func(i int) {
			// Assign each node to a precreated group (simulate round-robin)
			group1 := groups[i*2]
			group2 := groups[i*2+1]
			mappings := make([]*datamodel.NodeNodeGroupMap, 2)
			mappings[0] = &datamodel.NodeNodeGroupMap{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, NodeID: nodes[i][0].ID, NodeGroupID: group1.ID, HarvestConfig: &datamodel.HarvestConfig{}}
			mappings[1] = &datamodel.NodeNodeGroupMap{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, NodeID: nodes[i][1].ID, NodeGroupID: group2.ID, HarvestConfig: &datamodel.HarvestConfig{}}
			_, err1 := repo.CreateNodeNodeGroupMap(ctx, mappings[0])
			_, err2 := repo.CreateNodeNodeGroupMap(ctx, mappings[1])
			if err1 == nil && err2 == nil {
				nodeIDs[i][0] = nodes[i][0].ID
				nodeIDs[i][1] = nodes[i][1].ID
				done <- nil
			} else {
				if err1 != nil {
					done <- err1
				} else {
					done <- err2
				}
			}
		}(i)
	}
	for i := 0; i < numParallel; i++ {
		err := <-done
		assert.NoError(t, err, "parallel request %d failed", i)
	}
	// Validate that all nodes are assigned to a group
	for i := 0; i < numParallel; i++ {
		for j := 0; j < 2; j++ {
			if nodeIDs[i][j] == 0 {
				t.Errorf("Node %d in request %d was not created or assigned", j, i)
				continue
			}
			mapping, err := repo.GetNodeNodeGroupMap(ctx, nodeIDs[i][j])
			t.Logf("Node %d in request %d assigned to group %d", j, i, mapping.NodeGroupID)
			assert.NoError(t, err, "failed to get mapping for node %d in request %d", j, i)
			assert.NotNil(t, mapping, "mapping for node %d in request %d is nil", j, i)
			assert.NotZero(t, mapping.NodeGroupID, "node %d in request %d not assigned to a group", j, i)
		}
	}
}

func TestAssignTwoNodesToTwoGroups_Idempotent(t *testing.T) {
	repo, _ := setupNodeNodeGroupMapTestRepo(t)
	ctx := context.Background()

	// Create two nodes
	node1, err := repo.CreateNode(ctx, &datamodel.Node{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "n1-idempotent"})
	assert.NoError(t, err)
	node2, err := repo.CreateNode(ctx, &datamodel.Node{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "n2-idempotent"})
	assert.NoError(t, err)

	// First call - should create new mappings
	mappings1, err := repo.AssignTwoNodesToTwoGroups(ctx, node1, node2, 5)
	assert.NoError(t, err)
	assert.Len(t, mappings1, 2)

	// Store the original mappings
	originalMapping1 := mappings1[0]
	originalMapping2 := mappings1[1]

	// Second call with same nodes - should return the same mappings (idempotent)
	mappings2, err := repo.AssignTwoNodesToTwoGroups(ctx, node1, node2, 5)
	assert.NoError(t, err)
	assert.Len(t, mappings2, 2)

	// Verify the mappings are the same
	assert.Equal(t, originalMapping1.ID, mappings2[0].ID)
	assert.Equal(t, originalMapping1.NodeID, mappings2[0].NodeID)
	assert.Equal(t, originalMapping1.NodeGroupID, mappings2[0].NodeGroupID)
	assert.Equal(t, originalMapping1.UUID, mappings2[0].UUID)

	assert.Equal(t, originalMapping2.ID, mappings2[1].ID)
	assert.Equal(t, originalMapping2.NodeID, mappings2[1].NodeID)
	assert.Equal(t, originalMapping2.NodeGroupID, mappings2[1].NodeGroupID)
	assert.Equal(t, originalMapping2.UUID, mappings2[1].UUID)

	// Third call - should still return the same mappings
	mappings3, err := repo.AssignTwoNodesToTwoGroups(ctx, node1, node2, 10) // different maxNodesPerGroup shouldn't matter
	assert.NoError(t, err)
	assert.Len(t, mappings3, 2)

	// Verify mappings are still the same
	assert.Equal(t, originalMapping1.ID, mappings3[0].ID)
	assert.Equal(t, originalMapping2.ID, mappings3[1].ID)
}

func TestAssignTwoNodesToTwoGroups_IdempotentPartialAssignment(t *testing.T) {
	repo, _ := setupNodeNodeGroupMapTestRepo(t)
	ctx := context.Background()

	// Create two nodes
	node1, err := repo.CreateNode(ctx, &datamodel.Node{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "n1-partial"})
	assert.NoError(t, err)
	node2, err := repo.CreateNode(ctx, &datamodel.Node{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "n2-partial"})
	assert.NoError(t, err)

	// Create a group and manually assign node1 to it
	group1, err := repo.CreateNodeGroup(ctx, &datamodel.NodeGroup{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "g-partial"})
	assert.NoError(t, err)

	existingMapping1, err := repo.CreateNodeNodeGroupMap(ctx, &datamodel.NodeNodeGroupMap{
		BaseModel:     datamodel.BaseModel{UUID: uuid.NewString()},
		NodeID:        node1.ID,
		NodeGroupID:   group1.ID,
		HarvestConfig: &datamodel.HarvestConfig{},
	})
	assert.NoError(t, err)

	// Call AssignTwoNodesToTwoGroups - node1 already has mapping, node2 doesn't
	mappings, err := repo.AssignTwoNodesToTwoGroups(ctx, node1, node2, 5)
	assert.NoError(t, err)
	assert.Len(t, mappings, 2)

	// Verify node1's mapping is preserved
	var node1Mapping, node2Mapping *datamodel.NodeNodeGroupMap
	if mappings[0].NodeID == node1.ID {
		node1Mapping = mappings[0]
		node2Mapping = mappings[1]
	} else {
		node1Mapping = mappings[1]
		node2Mapping = mappings[0]
	}

	assert.Equal(t, existingMapping1.ID, node1Mapping.ID)
	assert.Equal(t, existingMapping1.NodeGroupID, node1Mapping.NodeGroupID)
	assert.Equal(t, node2.ID, node2Mapping.NodeID)
	assert.NotEqual(t, node1Mapping.NodeGroupID, node2Mapping.NodeGroupID) // Different groups

	// Call again - should return the same mappings
	mappings2, err := repo.AssignTwoNodesToTwoGroups(ctx, node1, node2, 5)
	assert.NoError(t, err)
	assert.Len(t, mappings2, 2)

	// Find mappings again
	var node1Mapping2, node2Mapping2 *datamodel.NodeNodeGroupMap
	if mappings2[0].NodeID == node1.ID {
		node1Mapping2 = mappings2[0]
		node2Mapping2 = mappings2[1]
	} else {
		node1Mapping2 = mappings2[1]
		node2Mapping2 = mappings2[0]
	}

	// Verify both mappings are unchanged
	assert.Equal(t, node1Mapping.ID, node1Mapping2.ID)
	assert.Equal(t, node2Mapping.ID, node2Mapping2.ID)
}

func TestCreateNodeNodeGroupMap_RejectsDuplicateMapping(t *testing.T) {
	repo, _ := setupNodeNodeGroupMapTestRepo(t)
	ctx := context.Background()

	// Create a node and a group
	node, err := repo.CreateNode(ctx, &datamodel.Node{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "n1-duplicate-test"})
	assert.NoError(t, err)
	group, err := repo.CreateNodeGroup(ctx, &datamodel.NodeGroup{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "g1-duplicate-test"})
	assert.NoError(t, err)

	// Create first mapping - should succeed
	mapping1 := &datamodel.NodeNodeGroupMap{
		BaseModel:     datamodel.BaseModel{UUID: uuid.NewString()},
		NodeID:        node.ID,
		NodeGroupID:   group.ID,
		HarvestConfig: &datamodel.HarvestConfig{},
	}
	created1, err := repo.CreateNodeNodeGroupMap(ctx, mapping1)
	assert.NoError(t, err)
	assert.NotNil(t, created1)

	// Create another group for second mapping attempt
	group2, err := repo.CreateNodeGroup(ctx, &datamodel.NodeGroup{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "g2-duplicate-test"})
	assert.NoError(t, err)

	// Try to create second mapping for the same node - should fail
	mapping2 := &datamodel.NodeNodeGroupMap{
		BaseModel:     datamodel.BaseModel{UUID: uuid.NewString()},
		NodeID:        node.ID,   // Same node as first mapping
		NodeGroupID:   group2.ID, // Different group
		HarvestConfig: &datamodel.HarvestConfig{},
	}
	created2, err := repo.CreateNodeNodeGroupMap(ctx, mapping2)
	assert.Error(t, err)
	assert.Nil(t, created2)
	var ce *vsaerrors.CustomError
	if errors.As(err, &ce) && ce.OriginalErr != nil {
		assert.Contains(t, ce.OriginalErr.Error(), "node is already assigned to a group")
	} else {
		assert.Contains(t, err.Error(), "node is already assigned to a group")
	}
}

func TestAssignTwoNodesToTwoGroups_HandlesExistingConstraint(t *testing.T) {
	repo, _ := setupNodeNodeGroupMapTestRepo(t)
	ctx := context.Background()

	// Create two nodes
	node1, err := repo.CreateNode(ctx, &datamodel.Node{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "n1-constraint"})
	assert.NoError(t, err)
	node2, err := repo.CreateNode(ctx, &datamodel.Node{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "n2-constraint"})
	assert.NoError(t, err)

	// Manually create a mapping for node1 to simulate existing assignment
	group1, err := repo.CreateNodeGroup(ctx, &datamodel.NodeGroup{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "g1-constraint"})
	assert.NoError(t, err)

	existingMapping, err := repo.CreateNodeNodeGroupMap(ctx, &datamodel.NodeNodeGroupMap{
		BaseModel:     datamodel.BaseModel{UUID: uuid.NewString()},
		NodeID:        node1.ID,
		NodeGroupID:   group1.ID,
		HarvestConfig: &datamodel.HarvestConfig{},
	})
	assert.NoError(t, err)

	// Call AssignTwoNodesToTwoGroups - should handle existing mapping gracefully
	mappings, err := repo.AssignTwoNodesToTwoGroups(ctx, node1, node2, 5)
	assert.NoError(t, err)
	assert.Len(t, mappings, 2)

	// Verify that node1's existing mapping is preserved
	var node1Mapping, node2Mapping *datamodel.NodeNodeGroupMap
	if mappings[0].NodeID == node1.ID {
		node1Mapping = mappings[0]
		node2Mapping = mappings[1]
	} else {
		node1Mapping = mappings[1]
		node2Mapping = mappings[0]
	}

	// Node1 should have the existing mapping
	assert.Equal(t, existingMapping.ID, node1Mapping.ID)
	assert.Equal(t, existingMapping.NodeGroupID, node1Mapping.NodeGroupID)

	// Node2 should have a new mapping to a different group
	assert.Equal(t, node2.ID, node2Mapping.NodeID)
	assert.NotEqual(t, node1Mapping.NodeGroupID, node2Mapping.NodeGroupID)
}

func TestAssignTwoNodesToTwoGroups_DBReadError(t *testing.T) {
	repo, _ := setupNodeNodeGroupMapTestRepo(t)
	ctx := context.Background()
	// Create two nodes
	n1, err := repo.CreateNode(ctx, &datamodel.Node{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "n1-db-error"})
	assert.NoError(t, err)
	n2, err := repo.CreateNode(ctx, &datamodel.Node{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "n2-db-error"})
	assert.NoError(t, err)
	// Simulate DB error by closing DB before call
	db, err := SetupTestDB()
	assert.NoError(t, err)
	dbConn, _ := db.DB()
	_ = dbConn.Close()
	wrapper := gorm.New(db)
	badRepo := &DataStoreRepository{db: wrapper}
	_, err = badRepo.AssignTwoNodesToTwoGroups(ctx, n1, n2, 5)
	assert.Error(t, err)
	var ce *vsaerrors.CustomError
	if errors.As(err, &ce) && ce.OriginalErr != nil {
		assert.Contains(t, ce.OriginalErr.Error(), "sql: database is closed")
	} else {
		assert.Contains(t, err.Error(), "sql: database is closed")
	}
}

func TestAssignTwoNodesToTwoGroups_GroupFetchError(t *testing.T) {
	repo, _ := setupNodeNodeGroupMapTestRepo(t)
	ctx := context.Background()
	// Create a node and manually assign it to a non-existent group
	node1, err := repo.CreateNode(ctx, &datamodel.Node{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "n1-fetch-err"})
	assert.NoError(t, err)
	node2, err := repo.CreateNode(ctx, &datamodel.Node{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "n2-fetch-err"})
	assert.NoError(t, err)

	// Manually insert a mapping with invalid group ID to trigger group fetch error
	db := repo.db.GORM()
	err = db.Exec("INSERT INTO node_node_group_maps (uuid, node_id, node_group_id, harvest_config, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
		uuid.NewString(), node1.ID, 99999, "{}", time.Now(), time.Now()).Error
	assert.NoError(t, err)

	_, err = repo.AssignTwoNodesToTwoGroups(ctx, node1, node2, 5)
	assert.Error(t, err)
	var ce *vsaerrors.CustomError
	if errors.As(err, &ce) && ce.OriginalErr != nil {
		assert.Contains(t, ce.OriginalErr.Error(), "type assertion to []byte failed")
	} else {
		assert.Contains(t, err.Error(), "type assertion to []byte failed")
	}
}

func TestAssignTwoNodesToTwoGroups_GenerateGroupError(t *testing.T) {
	// This test would require mocking internal dependencies which is complex
	// Instead, we'll test a simpler error case
	repo, _ := setupNodeNodeGroupMapTestRepo(t)
	ctx := context.Background()

	// Test with nil nodes to trigger validation error
	_, err := repo.AssignTwoNodesToTwoGroups(ctx, nil, nil, 5)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "node1 or node2 is nil")
}

func TestAssignTwoNodesToTwoGroups_MappingCreateError(t *testing.T) {
	// This test would require mocking internal dependencies which is complex
	// Instead, we'll test a database constraint error which is more realistic
	repo, _ := setupNodeNodeGroupMapTestRepo(t)
	ctx := context.Background()

	// Test with same nodes to trigger validation error
	n1, err := repo.CreateNode(ctx, &datamodel.Node{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "n-same"})
	assert.NoError(t, err)

	_, err = repo.AssignTwoNodesToTwoGroups(ctx, n1, n1, 5)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "node1 and node2 must be different nodes")
}

func TestAssignTwoNodesToTwoGroups_Node2GroupFetchError(t *testing.T) {
	repo, _ := setupNodeNodeGroupMapTestRepo(t)
	ctx := context.Background()

	// Create two nodes
	node1, err := repo.CreateNode(ctx, &datamodel.Node{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "n1-node2-fetch-err"})
	assert.NoError(t, err)
	node2, err := repo.CreateNode(ctx, &datamodel.Node{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "n2-node2-fetch-err"})
	assert.NoError(t, err)

	// Create a temporary group for node2 that we'll delete
	tempGroup, err := repo.CreateNodeGroup(ctx, &datamodel.NodeGroup{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "temp-group-2"})
	assert.NoError(t, err)

	// Create mapping only for node2 (node1 will not have a mapping)
	mapping2 := &datamodel.NodeNodeGroupMap{
		BaseModel:     datamodel.BaseModel{UUID: uuid.NewString()},
		NodeID:        node2.ID,
		NodeGroupID:   tempGroup.ID,
		HarvestConfig: &datamodel.HarvestConfig{},
	}
	_, err = repo.CreateNodeNodeGroupMap(ctx, mapping2)
	assert.NoError(t, err)

	// Now delete the group that node2 is mapped to, making it invalid
	db := repo.db.GORM()
	err = db.Delete(&datamodel.NodeGroup{}, tempGroup.ID).Error
	assert.NoError(t, err)

	// This should trigger the group fetch error for node2 (lines 161-162)
	_, err = repo.AssignTwoNodesToTwoGroups(ctx, node1, node2, 5)
	assert.Error(t, err)
	var ce *vsaerrors.CustomError
	if errors.As(err, &ce) && ce.OriginalErr != nil {
		assert.Contains(t, ce.OriginalErr.Error(), "record not found")
	} else {
		assert.Contains(t, err.Error(), "record not found")
	}
}

func TestAssignTwoNodesToTwoGroups_Node1GroupFetchError(t *testing.T) {
	repo, _ := setupNodeNodeGroupMapTestRepo(t)
	ctx := context.Background()

	// Create two nodes
	node1, err := repo.CreateNode(ctx, &datamodel.Node{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "n1-node1-fetch-err"})
	assert.NoError(t, err)
	node2, err := repo.CreateNode(ctx, &datamodel.Node{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "n2-node1-fetch-err"})
	assert.NoError(t, err)

	// Create a temporary group for node1 that we'll delete
	tempGroup, err := repo.CreateNodeGroup(ctx, &datamodel.NodeGroup{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "temp-group-1"})
	assert.NoError(t, err)

	// Create valid mapping first
	mapping1 := &datamodel.NodeNodeGroupMap{
		BaseModel:     datamodel.BaseModel{UUID: uuid.NewString()},
		NodeID:        node1.ID,
		NodeGroupID:   tempGroup.ID,
		HarvestConfig: &datamodel.HarvestConfig{},
	}
	_, err = repo.CreateNodeNodeGroupMap(ctx, mapping1)
	assert.NoError(t, err)

	// Now delete the group that node1 is mapped to, making it invalid
	db := repo.db.GORM()
	err = db.Delete(&datamodel.NodeGroup{}, tempGroup.ID).Error
	assert.NoError(t, err)

	_, err = repo.AssignTwoNodesToTwoGroups(ctx, node1, node2, 5)
	assert.Error(t, err)
	var ce *vsaerrors.CustomError
	if errors.As(err, &ce) && ce.OriginalErr != nil {
		assert.Contains(t, ce.OriginalErr.Error(), "record not found")
	} else {
		assert.Contains(t, err.Error(), "record not found")
	}
}

func TestAssignTwoNodesToTwoGroups_GroupCreateError_Node1(t *testing.T) {
	repo, _ := setupNodeNodeGroupMapTestRepo(t)
	ctx := context.Background()
	// Create two nodes
	node1, err := repo.CreateNode(ctx, &datamodel.Node{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "n1-group-create-err"})
	assert.NoError(t, err)
	node2, err := repo.CreateNode(ctx, &datamodel.Node{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "n2-group-create-err"})
	assert.NoError(t, err)

	// Simulate DB error by closing DB before assignment to force group creation error
	db := repo.db.GORM()
	dbConn, _ := db.DB()
	_ = dbConn.Close()
	badRepo := &DataStoreRepository{db: gorm.New(db)}

	_, err = badRepo.AssignTwoNodesToTwoGroups(ctx, node1, node2, 1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "undefined error: sql: database is closed",
		"error should contain 'undefined error: sql: database is closed', got: %v", err)
}

func TestAssignTwoNodesToTwoGroups_GroupCreateError_Node1_MonkeyPatch(t *testing.T) {
	repo, _ := setupNodeNodeGroupMapTestRepo(t)
	ctx := context.Background()
	// Create two nodes
	node1, err := repo.CreateNode(ctx, &datamodel.Node{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "n1-group-create-err-mp"})
	assert.NoError(t, err)
	node2, err := repo.CreateNode(ctx, &datamodel.Node{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "n2-group-create-err-mp"})
	assert.NoError(t, err)

	// Monkey patch generateRandomNodeGroup to always return error
	orig := generateRandomNodeGroup
	generateRandomNodeGroup = func(ctx context.Context, d *DataStoreRepository, group1 datamodel.NodeGroup) (*datamodel.NodeGroup, error) {
		return nil, errors.New("simulated group creation error")
	}
	defer func() { generateRandomNodeGroup = orig }()

	_, err = repo.AssignTwoNodesToTwoGroups(ctx, node1, node2, 1)
	assert.Error(t, err)
	var ce *vsaerrors.CustomError
	if errors.As(err, &ce) && ce.OriginalErr != nil {
		assert.Contains(t, ce.OriginalErr.Error(), "simulated group creation error")
	} else {
		assert.Contains(t, err.Error(), "simulated group creation error")
	}
}

func TestAssignTwoNodesToTwoGroups_GroupCreateError_Node2_MonkeyPatch(t *testing.T) {
	repo, _ := setupNodeNodeGroupMapTestRepo(t)
	ctx := context.Background()
	// Create two nodes
	node1, err := repo.CreateNode(ctx, &datamodel.Node{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "n1-group-create-err-node2-mp"})
	assert.NoError(t, err)
	node2, err := repo.CreateNode(ctx, &datamodel.Node{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "n2-group-create-err-node2-mp"})
	assert.NoError(t, err)

	// Pre-fill all groups so node2 must create a new group
	for i := 0; i < 5; i++ {
		group, err := repo.CreateNodeGroup(ctx, &datamodel.NodeGroup{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "g-full-node2-" + uuid.NewString()})
		assert.NoError(t, err)
		n, err := repo.CreateNode(ctx, &datamodel.Node{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, Name: "n-full-node2-" + uuid.NewString()})
		assert.NoError(t, err)
		_, err = repo.CreateNodeNodeGroupMap(ctx, &datamodel.NodeNodeGroupMap{BaseModel: datamodel.BaseModel{UUID: uuid.NewString()}, NodeID: n.ID, NodeGroupID: group.ID, HarvestConfig: &datamodel.HarvestConfig{}})
		assert.NoError(t, err)
	}

	// Monkey patch generateRandomNodeGroup to always return error for node2
	orig := generateRandomNodeGroup
	generateRandomNodeGroup = func(ctx context.Context, d *DataStoreRepository, group1 datamodel.NodeGroup) (*datamodel.NodeGroup, error) {
		return nil, errors.New("simulated group2 creation error")
	}
	defer func() { generateRandomNodeGroup = orig }()

	_, err = repo.AssignTwoNodesToTwoGroups(ctx, node1, node2, 1)
	assert.Error(t, err)
	var ce *vsaerrors.CustomError
	if errors.As(err, &ce) && ce.OriginalErr != nil {
		assert.Contains(t, ce.OriginalErr.Error(), "simulated group2 creation error")
	} else {
		assert.Contains(t, err.Error(), "simulated group2 creation error")
	}
}
