package activities

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"go.temporal.io/sdk/testsuite"
	"gorm.io/gorm"
)

const (
	nodeCount = 2
)

func getUnRegisterNodes() []*datamodel.Node {
	var nodes []*datamodel.Node
	createdAt := time.Now()
	for i := 0; i < nodeCount; i++ {
		node := &datamodel.Node{
			BaseModel: datamodel.BaseModel{
				ID:        int64(i),
				CreatedAt: createdAt,
				UpdatedAt: createdAt,
				DeletedAt: &gorm.DeletedAt{Time: time.Now(), Valid: true},
				UUID:      "test-node-uuid-" + strconv.Itoa(i)},
			State: models.LifeCycleStateDeleted,
		}
		nodes = append(nodes, node)
	}
	return nodes
}

func getNodeGroupMap(isDelete, updateLeaseName bool) []*datamodel.NodeNodeGroupMap {
	var nodeGroupMap []*datamodel.NodeNodeGroupMap
	createdAt := time.Now()
	for i := 0; i < nodeCount; i++ {
		groupMap := &datamodel.NodeNodeGroupMap{
			BaseModel: datamodel.BaseModel{
				ID:        int64(i),
				CreatedAt: createdAt,
				UpdatedAt: createdAt,
				UUID:      "test-nodegroup-map-uuid-" + strconv.Itoa(i)},
			NodeID:      int64(i),
			NodeGroupID: int64(i),
			NodeGroup: &datamodel.NodeGroup{
				BaseModel: datamodel.BaseModel{ID: int64(i), CreatedAt: createdAt, UpdatedAt: createdAt},
				Name:      "test-harvest-name-" + strconv.Itoa(i),
			},
		}
		if isDelete {
			groupMap.DeletedAt = &gorm.DeletedAt{Time: createdAt, Valid: true}
		}
		if updateLeaseName {
			groupMap.NodeGroup.LeaseName = "test-harvest-lease-" + strconv.Itoa(i)
		}
		nodeGroupMap = append(nodeGroupMap, groupMap)
	}
	return nodeGroupMap
}

func TestValidateAndGetNodes_Success(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}

	testPoolID := int64(1)
	nodesInfo := getUnRegisterNodes()

	mockStorage.On("GetNodesByPoolID", mock.Anything, testPoolID).Return(nodesInfo, nil)
	testActParams := &UnRegisterNodeFromHarvestActivityParams{
		PoolID: testPoolID,
	}

	// Use Temporal test suite to provide proper activity context for heartbeat
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	env.RegisterActivity(activity.ValidateAndGetNodes)

	result, err := env.ExecuteActivity(activity.ValidateAndGetNodes, testActParams)
	assert.NoError(t, err)

	var dbNodesInfo []*datamodel.Node
	err = result.Get(&dbNodesInfo)
	assert.NoError(t, err)
	assert.True(t, len(dbNodesInfo) == nodeCount)
	mockStorage.AssertExpectations(t)
}

func TestValidateAndGetNodes_FailWithNoNodes(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}

	testPoolID := int64(1)

	mockStorage.On("GetNodesByPoolID", mock.Anything, testPoolID).Return(nil,
		vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, errors.NewNotFoundErr("node", nil)))
	testActParams := &UnRegisterNodeFromHarvestActivityParams{
		PoolID: testPoolID,
	}

	// Use Temporal test suite to provide proper activity context for heartbeat
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	env.RegisterActivity(activity.ValidateAndGetNodes)

	_, err := env.ExecuteActivity(activity.ValidateAndGetNodes, testActParams)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), UnRegisterNodesInfoNotAvailable)
	mockStorage.AssertExpectations(t)
}

func TestValidateAndGetNodes_Error(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}

	testPoolID := int64(1)

	mockStorage.On("GetNodesByPoolID", mock.Anything, testPoolID).Return(nil, errors.New("db-error"))
	testActParams := &UnRegisterNodeFromHarvestActivityParams{
		PoolID: testPoolID,
	}

	// Use Temporal test suite to provide proper activity context for heartbeat
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	env.RegisterActivity(activity.ValidateAndGetNodes)

	_, err := env.ExecuteActivity(activity.ValidateAndGetNodes, testActParams)
	assert.Error(t, err)
	mockStorage.AssertExpectations(t)
}

func TestGetNodeGroupMapping_Success(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}

	nodesInfo := getUnRegisterNodes()
	nodeGroupMapInfo := getNodeGroupMap(false, true)
	for index, nodeInfo := range nodesInfo {
		mockStorage.On("GetNodeNodeGroupMapByNodeID", mock.Anything, nodeInfo.ID).Return(nodeGroupMapInfo[index], nil)
	}
	testActParams := &UnRegisterNodeFromHarvestActivityParams{
		Nodes: nodesInfo,
	}

	// Use Temporal test suite to provide proper activity context for heartbeat
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	env.RegisterActivity(activity.GetNodeGroupMapping)

	result, err := env.ExecuteActivity(activity.GetNodeGroupMapping, testActParams)
	assert.NoError(t, err)

	var mappings []*datamodel.NodeNodeGroupMap
	err = result.Get(&mappings)
	assert.NoError(t, err)
	assert.True(t, len(mappings) == nodeCount)
	mockStorage.AssertExpectations(t)
}

func TestValidateAndGetNodes_FailWithNoNodeGroupMap(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}

	nodesInfo := getUnRegisterNodes()

	mockStorage.On("GetNodeNodeGroupMapByNodeID", mock.Anything, nodesInfo[0].ID).Return(nil,
		vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, errors.NewNotFoundErr("nodeGroupMap", nil)))

	testActParams := &UnRegisterNodeFromHarvestActivityParams{
		Nodes: nodesInfo,
	}

	// Use Temporal test suite to provide proper activity context for heartbeat
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	env.RegisterActivity(activity.GetNodeGroupMapping)

	_, err := env.ExecuteActivity(activity.GetNodeGroupMapping, testActParams)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), UnRegisterNodeGroupMapNotAvailable)
	mockStorage.AssertExpectations(t)
}

// TestGetNodeGroupMapping_LargePoolMultiHADisabled_SkipsNodeWithNoMap covers the scenario where
// we have more than 2 nodes and multi-HA pair registration is disabled (large pool). When
// GetNodeNodeGroupMapByNodeID returns NotFound for a node, the activity continues to the next
// node instead of failing, since only 2 nodes are registered at a time.
func TestGetNodeGroupMapping_LargePoolMultiHADisabled_SkipsNodeWithNoMap(t *testing.T) {
	oldValue := enableMultiHaPairRegistration
	enableMultiHaPairRegistration = false
	defer func() { enableMultiHaPairRegistration = oldValue }()

	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}

	// Create 3 nodes (large pool: len > 2)
	createdAt := time.Now()
	nodesInfo := []*datamodel.Node{
		{
			BaseModel: datamodel.BaseModel{ID: 0, CreatedAt: createdAt, UpdatedAt: createdAt, UUID: "node-0"},
			State:     models.LifeCycleStateDeleted,
		},
		{
			BaseModel: datamodel.BaseModel{ID: 1, CreatedAt: createdAt, UpdatedAt: createdAt, UUID: "node-1"},
			State:     models.LifeCycleStateDeleted,
		},
		{
			BaseModel: datamodel.BaseModel{ID: 2, CreatedAt: createdAt, UpdatedAt: createdAt, UUID: "node-2"},
			State:     models.LifeCycleStateDeleted,
		},
	}

	// Node 0 and 2 have mappings; node 1 has no mapping (NotFound) and should be skipped
	nodeGroupMap0 := &datamodel.NodeNodeGroupMap{
		BaseModel:   datamodel.BaseModel{ID: 10, CreatedAt: createdAt, UpdatedAt: createdAt, UUID: "ngm-0"},
		NodeID:      0,
		NodeGroupID: 100,
		NodeGroup:   &datamodel.NodeGroup{BaseModel: datamodel.BaseModel{ID: 100}, Name: "group-0", LeaseName: "lease-0"},
	}
	nodeGroupMap2 := &datamodel.NodeNodeGroupMap{
		BaseModel:   datamodel.BaseModel{ID: 12, CreatedAt: createdAt, UpdatedAt: createdAt, UUID: "ngm-2"},
		NodeID:      2,
		NodeGroupID: 102,
		NodeGroup:   &datamodel.NodeGroup{BaseModel: datamodel.BaseModel{ID: 102}, Name: "group-2", LeaseName: "lease-2"},
	}

	notFoundErr := vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, errors.NewNotFoundErr("nodeGroupMap", nil))

	mockStorage.On("GetNodeNodeGroupMapByNodeID", mock.Anything, int64(0)).Return(nodeGroupMap0, nil)
	mockStorage.On("GetNodeNodeGroupMapByNodeID", mock.Anything, int64(1)).Return(nil, notFoundErr)
	mockStorage.On("GetNodeNodeGroupMapByNodeID", mock.Anything, int64(2)).Return(nodeGroupMap2, nil)

	testActParams := &UnRegisterNodeFromHarvestActivityParams{
		Nodes: nodesInfo,
	}

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	env.RegisterActivity(activity.GetNodeGroupMapping)

	result, err := env.ExecuteActivity(activity.GetNodeGroupMapping, testActParams)
	assert.NoError(t, err)

	var mappings []*datamodel.NodeNodeGroupMap
	err = result.Get(&mappings)
	assert.NoError(t, err)
	assert.Len(t, mappings, 2, "expected 2 mappings (node 1 skipped due to no nodeGroupMap)")
	assert.Equal(t, int64(0), mappings[0].NodeID)
	assert.Equal(t, int64(2), mappings[1].NodeID)
	mockStorage.AssertExpectations(t)
}

func TestGetNodeGroupMapping_DeletedNodeGroup(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}

	nodesInfo := getUnRegisterNodes()
	nodeGroupMapInfo := getNodeGroupMap(true, true)
	for index, nodeInfo := range nodesInfo {
		mockStorage.On("GetNodeNodeGroupMapByNodeID", mock.Anything, nodeInfo.ID).Return(nodeGroupMapInfo[index], nil)
	}

	testActParams := &UnRegisterNodeFromHarvestActivityParams{
		Nodes: nodesInfo,
	}

	// Use Temporal test suite to provide proper activity context for heartbeat
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	env.RegisterActivity(activity.GetNodeGroupMapping)

	result, err := env.ExecuteActivity(activity.GetNodeGroupMapping, testActParams)
	assert.NoError(t, err)

	var mappings []*datamodel.NodeNodeGroupMap
	err = result.Get(&mappings)
	assert.NoError(t, err)
	assert.True(t, len(mappings) == 2)
	mockStorage.AssertExpectations(t)
}

func TestGetNodeGroupMapping_Error(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}

	nodesInfo := getUnRegisterNodes()

	mockStorage.On("GetNodeNodeGroupMapByNodeID", mock.Anything, nodesInfo[0].ID).Return(nil, errors.New("db-error"))

	testActParams := &UnRegisterNodeFromHarvestActivityParams{
		Nodes: nodesInfo,
	}

	// Use Temporal test suite to provide proper activity context for heartbeat
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	env.RegisterActivity(activity.GetNodeGroupMapping)

	_, err := env.ExecuteActivity(activity.GetNodeGroupMapping, testActParams)
	assert.Error(t, err)
	mockStorage.AssertExpectations(t)
}

func TestDeleteNodeGroupMapping_Success(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}

	nodeGroupMapInfo := getNodeGroupMap(false, true)

	for _, nodeGroupMap := range nodeGroupMapInfo {
		mockStorage.On("DeleteNodeNodeGroupMap", mock.Anything, nodeGroupMap.ID).Return(nil)
	}
	testActParams := &UnRegisterNodeFromHarvestActivityParams{
		NodeGroupsMap: nodeGroupMapInfo,
	}

	// Use Temporal test suite to provide proper activity context for heartbeat
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	env.RegisterActivity(activity.DeleteNodeGroupMapping)

	_, err := env.ExecuteActivity(activity.DeleteNodeGroupMapping, testActParams)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestDeleteNodeGroupMapping_Error(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}

	nodeGroupMapInfo := getNodeGroupMap(false, true)

	mockStorage.On("DeleteNodeNodeGroupMap", mock.Anything, nodeGroupMapInfo[0].ID).Return(gorm.ErrRecordNotFound)

	testActParams := &UnRegisterNodeFromHarvestActivityParams{
		NodeGroupsMap: nodeGroupMapInfo,
	}

	// Use Temporal test suite to provide proper activity context for heartbeat
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	env.RegisterActivity(activity.DeleteNodeGroupMapping)

	_, err := env.ExecuteActivity(activity.DeleteNodeGroupMapping, testActParams)
	assert.Error(t, err)
	mockStorage.AssertExpectations(t)
}

func TestDeletePollersFromHarvestFarm_Success(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}

	nodeGroupMapInfo := getNodeGroupMap(true, true)
	oldDeletePollerRestResponse := deletePollerRestResponse
	deletePollerRestResponse = func(ctx context.Context, url string) (*http.Response, error) {
		for _, nodeGroupMapInfo := range nodeGroupMapInfo {
			expectedDeleteUrl := fmt.Sprintf(harvestRestProtocol+"://"+harvestEndPoint+"/config/%s/%s%d",
				nodeGroupMapInfo.NodeGroup.LeaseName, leasePrefix, nodeGroupMapInfo.NodeID)
			if expectedDeleteUrl == url {
				return &http.Response{StatusCode: 200,
					Status: "Deleted poller",
				}, nil
			}
		}
		return nil, errors.New("delete url mismatch error")
	}
	defer func() { deletePollerRestResponse = oldDeletePollerRestResponse }()

	testActParams := &UnRegisterNodeFromHarvestActivityParams{
		NodeGroupsMap: nodeGroupMapInfo,
	}

	// Use Temporal test suite to provide proper activity context for heartbeat
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	env.RegisterActivity(activity.DeletePollersFromHarvestFarm)

	_, err := env.ExecuteActivity(activity.DeletePollersFromHarvestFarm, testActParams)
	assert.NoError(t, err)
}

func TestDeletePollersFromHarvestFarm_StatusNotFound(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}

	nodeGroupMapInfo := getNodeGroupMap(true, true)
	oldDeletePollerRestResponse := deletePollerRestResponse
	deletePollerRestResponse = func(ctx context.Context, url string) (*http.Response, error) {
		return &http.Response{StatusCode: 404,
			Status: "Poller file not found for given lease and poller name",
		}, nil
	}
	defer func() { deletePollerRestResponse = oldDeletePollerRestResponse }()

	testActParams := &UnRegisterNodeFromHarvestActivityParams{
		NodeGroupsMap: nodeGroupMapInfo,
	}

	// Use Temporal test suite to provide proper activity context for heartbeat
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	env.RegisterActivity(activity.DeletePollersFromHarvestFarm)

	_, err := env.ExecuteActivity(activity.DeletePollersFromHarvestFarm, testActParams)
	assert.NoError(t, err)
}

func TestDeletePollersFromHarvestFarm_Error(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}

	nodeGroupMapInfo := getNodeGroupMap(true, true)
	oldDeletePollerRestResponse := deletePollerRestResponse
	deletePollerRestResponse = func(ctx context.Context, url string) (*http.Response, error) {
		return nil, errors.New("rest-client failed")
	}
	defer func() { deletePollerRestResponse = oldDeletePollerRestResponse }()

	testActParams := &UnRegisterNodeFromHarvestActivityParams{
		NodeGroupsMap: nodeGroupMapInfo,
	}

	// Use Temporal test suite to provide proper activity context for heartbeat
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	env.RegisterActivity(activity.DeletePollersFromHarvestFarm)

	_, err := env.ExecuteActivity(activity.DeletePollersFromHarvestFarm, testActParams)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "rest-client failed")
}

// Below  test case will validate and issue lease client delete of k8's leases
func TestValidateAndReleaseLease(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	nodeGroupMapInfo := getNodeGroupMap(true, true)

	for _, nodeGroupMap := range nodeGroupMapInfo {
		mockStorage.On("GetNodeGroupMapNodeCount", mock.Anything, nodeGroupMap.NodeGroupID).Return(int64(0), nil)
		mockStorage.On("DeleteNodeGroup", mock.Anything, nodeGroupMap.NodeGroupID).Return(nil)
	}

	oldDeleteKubernetesLease := deleteKubernetesLease
	// Mock delete lease which is in utils
	deleteKubernetesLease = func(ctx context.Context, leaseNameSpace, leaseName string) error {
		return nil
	}
	defer func() { deleteKubernetesLease = oldDeleteKubernetesLease }()

	testActParams := &UnRegisterNodeFromHarvestActivityParams{
		NodeGroupsMap: nodeGroupMapInfo,
	}

	// Use Temporal test suite to provide proper activity context for heartbeat
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	env.RegisterActivity(activity.ValidateAndReleaseLease)

	_, err := env.ExecuteActivity(activity.ValidateAndReleaseLease, testActParams)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestValidateAndReleaseLease_NoLeaseToDelete(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	nodeGroupMapInfo := getNodeGroupMap(true, true)

	for _, nodeGroupMap := range nodeGroupMapInfo {
		mockStorage.On("GetNodeGroupMapNodeCount", mock.Anything, nodeGroupMap.NodeGroupID).Return(int64(1), nil)
	}

	testActParams := &UnRegisterNodeFromHarvestActivityParams{
		NodeGroupsMap: nodeGroupMapInfo,
	}

	// Use Temporal test suite to provide proper activity context for heartbeat
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	env.RegisterActivity(activity.ValidateAndReleaseLease)

	_, err := env.ExecuteActivity(activity.ValidateAndReleaseLease, testActParams)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestValidateAndReleaseLease_SingleLeaseToDelete(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	nodeGroupMapInfo := getNodeGroupMap(true, true)

	mockStorage.On("GetNodeGroupMapNodeCount", mock.Anything, nodeGroupMapInfo[0].NodeGroupID).Return(int64(1), nil)
	mockStorage.On("GetNodeGroupMapNodeCount", mock.Anything, nodeGroupMapInfo[1].NodeGroupID).Return(int64(0), nil)
	mockStorage.On("DeleteNodeGroup", mock.Anything, nodeGroupMapInfo[1].NodeGroupID).Return(nil)

	oldDeleteKubernetesLease := deleteKubernetesLease
	// Mock delete lease which is in utils
	deleteKubernetesLease = func(ctx context.Context, leaseNameSpace, leaseName string) error {
		return nil
	}
	defer func() { deleteKubernetesLease = oldDeleteKubernetesLease }()

	testActParams := &UnRegisterNodeFromHarvestActivityParams{
		NodeGroupsMap: nodeGroupMapInfo,
	}

	// Use Temporal test suite to provide proper activity context for heartbeat
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	env.RegisterActivity(activity.ValidateAndReleaseLease)

	_, err := env.ExecuteActivity(activity.ValidateAndReleaseLease, testActParams)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestValidateAndReleaseLease_LeaseClientError(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	nodeGroupMapInfo := getNodeGroupMap(true, true)

	mockStorage.On("GetNodeGroupMapNodeCount", mock.Anything, nodeGroupMapInfo[0].NodeGroupID).Return(int64(1), nil)
	mockStorage.On("GetNodeGroupMapNodeCount", mock.Anything, nodeGroupMapInfo[1].NodeGroupID).Return(int64(0), nil)

	oldDeleteKubernetesLease := deleteKubernetesLease
	// Mock delete lease which is in utils
	deleteKubernetesLease = func(ctx context.Context, leaseNameSpace, leaseName string) error {
		return errors.New("lease-client failed")
	}
	defer func() { deleteKubernetesLease = oldDeleteKubernetesLease }()

	testActParams := &UnRegisterNodeFromHarvestActivityParams{
		NodeGroupsMap: nodeGroupMapInfo,
	}

	// Use Temporal test suite to provide proper activity context for heartbeat
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	env.RegisterActivity(activity.ValidateAndReleaseLease)

	_, err := env.ExecuteActivity(activity.ValidateAndReleaseLease, testActParams)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "lease-client failed")
	mockStorage.AssertExpectations(t)
}

func TestValidateAndReleaseLease_DBError(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	nodeGroupMapInfo := getNodeGroupMap(true, true)

	mockStorage.On("GetNodeGroupMapNodeCount", mock.Anything, nodeGroupMapInfo[0].NodeGroupID).Return(int64(0), errors.New("db-error"))

	testActParams := &UnRegisterNodeFromHarvestActivityParams{
		NodeGroupsMap: nodeGroupMapInfo,
	}

	// Use Temporal test suite to provide proper activity context for heartbeat
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	env.RegisterActivity(activity.ValidateAndReleaseLease)

	_, err := env.ExecuteActivity(activity.ValidateAndReleaseLease, testActParams)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "db-error")
	mockStorage.AssertExpectations(t)
}

func TestUnRegisterAlertHarvestRegisterFailure(t *testing.T) {
	mockSE := new(database.MockStorage)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockSE}
	ctx := context.Background()
	err := activity.AlertHarvestUnRegisterFailure(ctx, "test-error-details")
	assert.NoError(t, err)
}

func TestReconcileNodeGroupMapsBatch_Empty(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	env.RegisterActivity(activity.ReconcileNodeGroupMapsBatch)

	result, err := env.ExecuteActivity(activity.ReconcileNodeGroupMapsBatch, &ReconcileNodeGroupMapsBatchParams{Maps: nil})
	assert.NoError(t, err)
	var batchResult ReconcileNodeGroupMapsBatchResult
	_ = result.Get(&batchResult)
	assert.Equal(t, 0, batchResult.Reconciled)
	mockStorage.AssertExpectations(t)
}

func TestReconcileNodeGroupMapsBatch_OneMap_Reconciles(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	nodeGroupMap := &datamodel.NodeNodeGroupMap{
		BaseModel:   datamodel.BaseModel{ID: 1},
		NodeID:      10,
		NodeGroupID: 100,
		NodeGroup:   &datamodel.NodeGroup{BaseModel: datamodel.BaseModel{ID: 100}, Name: "g1", LeaseName: "lease-1"},
	}
	mockStorage.On("DeleteNodeNodeGroupMap", mock.Anything, int64(1)).Return(nil)

	oldDelete := deletePollerRestResponse
	deletePollerRestResponse = func(ctx context.Context, url string) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK}, nil
	}
	defer func() { deletePollerRestResponse = oldDelete }()

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	env.RegisterActivity(activity.ReconcileNodeGroupMapsBatch)

	result, err := env.ExecuteActivity(activity.ReconcileNodeGroupMapsBatch, &ReconcileNodeGroupMapsBatchParams{Maps: []*datamodel.NodeNodeGroupMap{nodeGroupMap}})
	assert.NoError(t, err)
	var batchResult ReconcileNodeGroupMapsBatchResult
	_ = result.Get(&batchResult)
	assert.Equal(t, 1, batchResult.Reconciled)
	mockStorage.AssertExpectations(t)
}

func TestListAllMapsWithDeletedNodes_Empty(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	// First page is empty -> loop exits immediately
	mockStorage.On("ListNodeNodeGroupMapAfterID", mock.Anything, false, int64(0), 100).Return([]*datamodel.NodeNodeGroupMap{}, nil)

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	env.RegisterActivity(activity.ListAllMapsWithDeletedNodes)

	result, err := env.ExecuteActivity(activity.ListAllMapsWithDeletedNodes, &ListAllMapsWithDeletedNodesParams{})
	assert.NoError(t, err)
	var listResult ListAllMapsWithDeletedNodesResult
	_ = result.Get(&listResult)
	assert.Empty(t, listResult.MapsToReconcile)
	mockStorage.AssertExpectations(t)
}

func TestListAllMapsWithDeletedNodes_OnePage_OneDeleted(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	createdAt := time.Now()
	mapDeleted := &datamodel.NodeNodeGroupMap{
		BaseModel: datamodel.BaseModel{ID: 1, CreatedAt: createdAt, UpdatedAt: createdAt},
		NodeID:    10, NodeGroupID: 100,
		NodeGroup: &datamodel.NodeGroup{BaseModel: datamodel.BaseModel{ID: 100}, Name: "g1", LeaseName: "lease-1"},
	}
	nodeDeleted := &datamodel.Node{BaseModel: datamodel.BaseModel{ID: 10}, State: models.LifeCycleStateDeleted}
	// Single page with one map (deleted node) -> len(maps) < pageSize so loop exits after one page
	mockStorage.On("ListNodeNodeGroupMapAfterID", mock.Anything, false, int64(0), 100).Return([]*datamodel.NodeNodeGroupMap{mapDeleted}, nil)
	mockStorage.On("GetNodeByID", mock.Anything, int64(10)).Return(nodeDeleted, nil)

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	env.RegisterActivity(activity.ListAllMapsWithDeletedNodes)

	result, err := env.ExecuteActivity(activity.ListAllMapsWithDeletedNodes, &ListAllMapsWithDeletedNodesParams{})
	assert.NoError(t, err)
	var listResult ListAllMapsWithDeletedNodesResult
	_ = result.Get(&listResult)
	assert.Len(t, listResult.MapsToReconcile, 1)
	assert.Equal(t, int64(1), listResult.MapsToReconcile[0].ID)
	mockStorage.AssertExpectations(t)
}

// --- ListAllMapsWithDeletedNodes: additional scenarios ---

func TestListAllMapsWithDeletedNodes_ListNodeError_ReturnsError(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	mockStorage.On("ListNodeNodeGroupMapAfterID", mock.Anything, false, int64(0), 100).Return(([]*datamodel.NodeNodeGroupMap)(nil), errors.New("list failed"))

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	env.RegisterActivity(activity.ListAllMapsWithDeletedNodes)

	_, err := env.ExecuteActivity(activity.ListAllMapsWithDeletedNodes, &ListAllMapsWithDeletedNodesParams{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "list failed")
	mockStorage.AssertExpectations(t)
}

// TestListAllMapsWithDeletedNodes_GetNodeByIDReturnsNonNotFoundError_ReturnsError covers line 351: return nil, err when GetNodeByID fails with non-NotFound error.
func TestListAllMapsWithDeletedNodes_GetNodeByIDReturnsNonNotFoundError_ReturnsError(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	m := &datamodel.NodeNodeGroupMap{BaseModel: datamodel.BaseModel{ID: 1}, NodeID: 10, NodeGroupID: 100}
	readErr := vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, errors.New("db read error"))
	mockStorage.On("ListNodeNodeGroupMapAfterID", mock.Anything, false, int64(0), 100).Return([]*datamodel.NodeNodeGroupMap{m}, nil)
	mockStorage.On("GetNodeByID", mock.Anything, int64(10)).Return((*datamodel.Node)(nil), readErr)

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	env.RegisterActivity(activity.ListAllMapsWithDeletedNodes)

	_, err := env.ExecuteActivity(activity.ListAllMapsWithDeletedNodes, &ListAllMapsWithDeletedNodesParams{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "db read error")
	mockStorage.AssertExpectations(t)
}

func TestListAllMapsWithDeletedNodes_NodeNotFound_SkipsMap(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	m1 := &datamodel.NodeNodeGroupMap{BaseModel: datamodel.BaseModel{ID: 1}, NodeID: 10, NodeGroupID: 100}
	m2 := &datamodel.NodeNodeGroupMap{BaseModel: datamodel.BaseModel{ID: 2}, NodeID: 20, NodeGroupID: 100}
	notFoundErr := vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataNotFoundError, errors.NewNotFoundErr("node", nil))
	nodeDeleted := &datamodel.Node{BaseModel: datamodel.BaseModel{ID: 20}, State: models.LifeCycleStateDeleted}
	mockStorage.On("ListNodeNodeGroupMapAfterID", mock.Anything, false, int64(0), 50).Return([]*datamodel.NodeNodeGroupMap{m1, m2}, nil)
	mockStorage.On("GetNodeByID", mock.Anything, int64(10)).Return((*datamodel.Node)(nil), notFoundErr)
	mockStorage.On("GetNodeByID", mock.Anything, int64(20)).Return(nodeDeleted, nil)

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	env.RegisterActivity(activity.ListAllMapsWithDeletedNodes)

	result, err := env.ExecuteActivity(activity.ListAllMapsWithDeletedNodes, &ListAllMapsWithDeletedNodesParams{PageSize: 50})
	assert.NoError(t, err)
	var listResult ListAllMapsWithDeletedNodesResult
	_ = result.Get(&listResult)
	assert.Len(t, listResult.MapsToReconcile, 1)
	assert.Equal(t, int64(2), listResult.MapsToReconcile[0].ID)
	mockStorage.AssertExpectations(t)
}

func TestListAllMapsWithDeletedNodes_TwoPages_CollectsAllDeleted(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	page1Map := &datamodel.NodeNodeGroupMap{
		BaseModel: datamodel.BaseModel{ID: 1}, NodeID: 10, NodeGroupID: 100,
		NodeGroup: &datamodel.NodeGroup{BaseModel: datamodel.BaseModel{ID: 100}, Name: "g1", LeaseName: "lease-1"},
	}
	page2Map := &datamodel.NodeNodeGroupMap{
		BaseModel: datamodel.BaseModel{ID: 2}, NodeID: 20, NodeGroupID: 100,
		NodeGroup: &datamodel.NodeGroup{BaseModel: datamodel.BaseModel{ID: 100}, Name: "g1", LeaseName: "lease-1"},
	}
	node10 := &datamodel.Node{BaseModel: datamodel.BaseModel{ID: 10}, State: models.LifeCycleStateDeleted}
	node20 := &datamodel.Node{BaseModel: datamodel.BaseModel{ID: 20}, State: models.LifeCycleStateDeleted}
	// Page size 1: first page returns 1 map, second page returns 1 map, third returns 0
	mockStorage.On("ListNodeNodeGroupMapAfterID", mock.Anything, false, int64(0), 1).Return([]*datamodel.NodeNodeGroupMap{page1Map}, nil)
	mockStorage.On("GetNodeByID", mock.Anything, int64(10)).Return(node10, nil)
	mockStorage.On("ListNodeNodeGroupMapAfterID", mock.Anything, false, int64(1), 1).Return([]*datamodel.NodeNodeGroupMap{page2Map}, nil)
	mockStorage.On("GetNodeByID", mock.Anything, int64(20)).Return(node20, nil)
	mockStorage.On("ListNodeNodeGroupMapAfterID", mock.Anything, false, int64(2), 1).Return([]*datamodel.NodeNodeGroupMap{}, nil)

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	env.RegisterActivity(activity.ListAllMapsWithDeletedNodes)

	result, err := env.ExecuteActivity(activity.ListAllMapsWithDeletedNodes, &ListAllMapsWithDeletedNodesParams{PageSize: 1})
	assert.NoError(t, err)
	var listResult ListAllMapsWithDeletedNodesResult
	_ = result.Get(&listResult)
	assert.Len(t, listResult.MapsToReconcile, 2)
	assert.Equal(t, int64(1), listResult.MapsToReconcile[0].ID)
	assert.Equal(t, int64(2), listResult.MapsToReconcile[1].ID)
	mockStorage.AssertExpectations(t)
}

// TestListAllMapsWithDeletedNodes_NodeNotDeleted_SkipsMap covers line 351: continue when node.State != Deleted.
func TestListAllMapsWithDeletedNodes_NodeNotDeleted_SkipsMap(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	m1 := &datamodel.NodeNodeGroupMap{BaseModel: datamodel.BaseModel{ID: 1}, NodeID: 10, NodeGroupID: 100}
	m2 := &datamodel.NodeNodeGroupMap{BaseModel: datamodel.BaseModel{ID: 2}, NodeID: 20, NodeGroupID: 100}
	node10Active := &datamodel.Node{BaseModel: datamodel.BaseModel{ID: 10}, State: models.LifeCycleStateAvailable}
	node20Deleted := &datamodel.Node{BaseModel: datamodel.BaseModel{ID: 20}, State: models.LifeCycleStateDeleted}
	mockStorage.On("ListNodeNodeGroupMapAfterID", mock.Anything, false, int64(0), 100).Return([]*datamodel.NodeNodeGroupMap{m1, m2}, nil)
	mockStorage.On("GetNodeByID", mock.Anything, int64(10)).Return(node10Active, nil)
	mockStorage.On("GetNodeByID", mock.Anything, int64(20)).Return(node20Deleted, nil)

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	env.RegisterActivity(activity.ListAllMapsWithDeletedNodes)

	result, err := env.ExecuteActivity(activity.ListAllMapsWithDeletedNodes, &ListAllMapsWithDeletedNodesParams{})
	assert.NoError(t, err)
	var listResult ListAllMapsWithDeletedNodesResult
	_ = result.Get(&listResult)
	assert.Len(t, listResult.MapsToReconcile, 1)
	assert.Equal(t, int64(2), listResult.MapsToReconcile[0].ID)
	mockStorage.AssertExpectations(t)
}

// --- ReconcileNodeGroupMapsBatch: additional scenarios ---

func TestReconcileNodeGroupMapsBatch_EmptySlice_ReturnsZero(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	env.RegisterActivity(activity.ReconcileNodeGroupMapsBatch)

	result, err := env.ExecuteActivity(activity.ReconcileNodeGroupMapsBatch, &ReconcileNodeGroupMapsBatchParams{Maps: []*datamodel.NodeNodeGroupMap{}})
	assert.NoError(t, err)
	var batchResult ReconcileNodeGroupMapsBatchResult
	_ = result.Get(&batchResult)
	assert.Equal(t, 0, batchResult.Reconciled)
	mockStorage.AssertExpectations(t)
}

func TestReconcileNodeGroupMapsBatch_NoLeaseName_StillDeletesFromDB(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	m := &datamodel.NodeNodeGroupMap{
		BaseModel: datamodel.BaseModel{ID: 1}, NodeID: 10, NodeGroupID: 100,
		NodeGroup: &datamodel.NodeGroup{BaseModel: datamodel.BaseModel{ID: 100}, Name: "g1", LeaseName: ""},
	}
	mockStorage.On("DeleteNodeNodeGroupMap", mock.Anything, int64(1)).Return(nil)

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	env.RegisterActivity(activity.ReconcileNodeGroupMapsBatch)

	result, err := env.ExecuteActivity(activity.ReconcileNodeGroupMapsBatch, &ReconcileNodeGroupMapsBatchParams{Maps: []*datamodel.NodeNodeGroupMap{m}})
	assert.NoError(t, err)
	var batchResult ReconcileNodeGroupMapsBatchResult
	_ = result.Get(&batchResult)
	assert.Equal(t, 1, batchResult.Reconciled)
	mockStorage.AssertExpectations(t)
}

func TestReconcileNodeGroupMapsBatch_NodeGroupNil_StillDeletesFromDB(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	m := &datamodel.NodeNodeGroupMap{
		BaseModel: datamodel.BaseModel{ID: 1}, NodeID: 10, NodeGroupID: 100,
		NodeGroup: nil,
	}
	mockStorage.On("DeleteNodeNodeGroupMap", mock.Anything, int64(1)).Return(nil)

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	env.RegisterActivity(activity.ReconcileNodeGroupMapsBatch)

	result, err := env.ExecuteActivity(activity.ReconcileNodeGroupMapsBatch, &ReconcileNodeGroupMapsBatchParams{Maps: []*datamodel.NodeNodeGroupMap{m}})
	assert.NoError(t, err)
	var batchResult ReconcileNodeGroupMapsBatchResult
	_ = result.Get(&batchResult)
	assert.Equal(t, 1, batchResult.Reconciled)
	mockStorage.AssertExpectations(t)
}

// TestReconcileNodeGroupMapsBatch_SingleMap_HarvestFails_Continues covers line 466: continue when deletePollerRestResponse returns error.
func TestReconcileNodeGroupMapsBatch_SingleMap_HarvestFails_Continues(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	m := &datamodel.NodeNodeGroupMap{
		BaseModel: datamodel.BaseModel{ID: 1}, NodeID: 10, NodeGroupID: 100,
		NodeGroup: &datamodel.NodeGroup{BaseModel: datamodel.BaseModel{ID: 100}, Name: "g1", LeaseName: "lease-1"},
	}
	oldDelete := deletePollerRestResponse
	deletePollerRestResponse = func(ctx context.Context, url string) (*http.Response, error) {
		return nil, errors.New("harvest down")
	}
	defer func() { deletePollerRestResponse = oldDelete }()

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	env.RegisterActivity(activity.ReconcileNodeGroupMapsBatch)

	result, err := env.ExecuteActivity(activity.ReconcileNodeGroupMapsBatch, &ReconcileNodeGroupMapsBatchParams{Maps: []*datamodel.NodeNodeGroupMap{m}})
	assert.NoError(t, err)
	var batchResult ReconcileNodeGroupMapsBatchResult
	_ = result.Get(&batchResult)
	assert.Equal(t, 0, batchResult.Reconciled)
	mockStorage.AssertExpectations(t)
}

func TestReconcileNodeGroupMapsBatch_HarvestFails_SkipsThatMap(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	m1 := &datamodel.NodeNodeGroupMap{
		BaseModel: datamodel.BaseModel{ID: 1}, NodeID: 10, NodeGroupID: 100,
		NodeGroup: &datamodel.NodeGroup{BaseModel: datamodel.BaseModel{ID: 100}, Name: "g1", LeaseName: "lease-1"},
	}
	m2 := &datamodel.NodeNodeGroupMap{
		BaseModel: datamodel.BaseModel{ID: 2}, NodeID: 20, NodeGroupID: 100,
		NodeGroup: &datamodel.NodeGroup{BaseModel: datamodel.BaseModel{ID: 100}, Name: "g1", LeaseName: "lease-1"},
	}
	mockStorage.On("DeleteNodeNodeGroupMap", mock.Anything, int64(2)).Return(nil)

	callCount := 0
	oldDelete := deletePollerRestResponse
	deletePollerRestResponse = func(ctx context.Context, url string) (*http.Response, error) {
		callCount++
		if callCount == 1 {
			return nil, errors.New("harvest down")
		}
		return &http.Response{StatusCode: http.StatusOK}, nil
	}
	defer func() { deletePollerRestResponse = oldDelete }()

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	env.RegisterActivity(activity.ReconcileNodeGroupMapsBatch)

	result, err := env.ExecuteActivity(activity.ReconcileNodeGroupMapsBatch, &ReconcileNodeGroupMapsBatchParams{Maps: []*datamodel.NodeNodeGroupMap{m1, m2}})
	assert.NoError(t, err)
	var batchResult ReconcileNodeGroupMapsBatchResult
	_ = result.Get(&batchResult)
	assert.Equal(t, 1, batchResult.Reconciled)
	mockStorage.AssertExpectations(t)
}

func TestReconcileNodeGroupMapsBatch_HarvestReturns500_SkipsThatMap(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	m1 := &datamodel.NodeNodeGroupMap{
		BaseModel: datamodel.BaseModel{ID: 1}, NodeID: 10, NodeGroupID: 100,
		NodeGroup: &datamodel.NodeGroup{BaseModel: datamodel.BaseModel{ID: 100}, Name: "g1", LeaseName: "lease-1"},
	}
	m2 := &datamodel.NodeNodeGroupMap{
		BaseModel: datamodel.BaseModel{ID: 2}, NodeID: 20, NodeGroupID: 100,
		NodeGroup: &datamodel.NodeGroup{BaseModel: datamodel.BaseModel{ID: 100}, Name: "g1", LeaseName: "lease-1"},
	}
	mockStorage.On("DeleteNodeNodeGroupMap", mock.Anything, int64(2)).Return(nil)

	callCount := 0
	oldDelete := deletePollerRestResponse
	deletePollerRestResponse = func(ctx context.Context, url string) (*http.Response, error) {
		callCount++
		if callCount == 1 {
			return &http.Response{StatusCode: http.StatusInternalServerError}, nil
		}
		return &http.Response{StatusCode: http.StatusOK}, nil
	}
	defer func() { deletePollerRestResponse = oldDelete }()

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	env.RegisterActivity(activity.ReconcileNodeGroupMapsBatch)

	result, err := env.ExecuteActivity(activity.ReconcileNodeGroupMapsBatch, &ReconcileNodeGroupMapsBatchParams{Maps: []*datamodel.NodeNodeGroupMap{m1, m2}})
	assert.NoError(t, err)
	var batchResult ReconcileNodeGroupMapsBatchResult
	_ = result.Get(&batchResult)
	assert.Equal(t, 1, batchResult.Reconciled)
	mockStorage.AssertExpectations(t)
}

func TestReconcileNodeGroupMapsBatch_DBDeleteFails_SkipsThatMap(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	m1 := &datamodel.NodeNodeGroupMap{
		BaseModel: datamodel.BaseModel{ID: 1}, NodeID: 10, NodeGroupID: 100,
		NodeGroup: &datamodel.NodeGroup{BaseModel: datamodel.BaseModel{ID: 100}, Name: "g1", LeaseName: "lease-1"},
	}
	m2 := &datamodel.NodeNodeGroupMap{
		BaseModel: datamodel.BaseModel{ID: 2}, NodeID: 20, NodeGroupID: 100,
		NodeGroup: &datamodel.NodeGroup{BaseModel: datamodel.BaseModel{ID: 100}, Name: "g1", LeaseName: "lease-1"},
	}
	mockStorage.On("DeleteNodeNodeGroupMap", mock.Anything, int64(1)).Return(errors.New("db fail"))
	mockStorage.On("DeleteNodeNodeGroupMap", mock.Anything, int64(2)).Return(nil)

	oldDelete := deletePollerRestResponse
	deletePollerRestResponse = func(ctx context.Context, url string) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK}, nil
	}
	defer func() { deletePollerRestResponse = oldDelete }()

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	env.RegisterActivity(activity.ReconcileNodeGroupMapsBatch)

	result, err := env.ExecuteActivity(activity.ReconcileNodeGroupMapsBatch, &ReconcileNodeGroupMapsBatchParams{Maps: []*datamodel.NodeNodeGroupMap{m1, m2}})
	assert.NoError(t, err)
	var batchResult ReconcileNodeGroupMapsBatchResult
	_ = result.Get(&batchResult)
	assert.Equal(t, 1, batchResult.Reconciled)
	mockStorage.AssertExpectations(t)
}

func TestReconcileNodeGroupMapsBatch_MultipleMaps_AllSucceed(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	m1 := &datamodel.NodeNodeGroupMap{
		BaseModel: datamodel.BaseModel{ID: 1}, NodeID: 10, NodeGroupID: 100,
		NodeGroup: &datamodel.NodeGroup{BaseModel: datamodel.BaseModel{ID: 100}, Name: "g1", LeaseName: "lease-1"},
	}
	m2 := &datamodel.NodeNodeGroupMap{
		BaseModel: datamodel.BaseModel{ID: 2}, NodeID: 20, NodeGroupID: 100,
		NodeGroup: &datamodel.NodeGroup{BaseModel: datamodel.BaseModel{ID: 100}, Name: "g1", LeaseName: "lease-1"},
	}
	mockStorage.On("DeleteNodeNodeGroupMap", mock.Anything, int64(1)).Return(nil)
	mockStorage.On("DeleteNodeNodeGroupMap", mock.Anything, int64(2)).Return(nil)

	oldDelete := deletePollerRestResponse
	deletePollerRestResponse = func(ctx context.Context, url string) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK}, nil
	}
	defer func() { deletePollerRestResponse = oldDelete }()

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	env.RegisterActivity(activity.ReconcileNodeGroupMapsBatch)

	result, err := env.ExecuteActivity(activity.ReconcileNodeGroupMapsBatch, &ReconcileNodeGroupMapsBatchParams{Maps: []*datamodel.NodeNodeGroupMap{m1, m2}})
	assert.NoError(t, err)
	var batchResult ReconcileNodeGroupMapsBatchResult
	_ = result.Get(&batchResult)
	assert.Equal(t, 2, batchResult.Reconciled)
	mockStorage.AssertExpectations(t)
}

// TestReconcileNodeGroupMapsBatch_HarvestOK_WithBody_ClosesBody covers line 466 (resp.Body.Close).
func TestReconcileNodeGroupMapsBatch_HarvestOK_WithBody_ClosesBody(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	m := &datamodel.NodeNodeGroupMap{
		BaseModel: datamodel.BaseModel{ID: 1}, NodeID: 10, NodeGroupID: 100,
		NodeGroup: &datamodel.NodeGroup{BaseModel: datamodel.BaseModel{ID: 100}, Name: "g1", LeaseName: "lease-1"},
	}
	mockStorage.On("DeleteNodeNodeGroupMap", mock.Anything, int64(1)).Return(nil)

	oldDelete := deletePollerRestResponse
	deletePollerRestResponse = func(ctx context.Context, url string) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(""))}, nil
	}
	defer func() { deletePollerRestResponse = oldDelete }()

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	env.RegisterActivity(activity.ReconcileNodeGroupMapsBatch)

	result, err := env.ExecuteActivity(activity.ReconcileNodeGroupMapsBatch, &ReconcileNodeGroupMapsBatchParams{Maps: []*datamodel.NodeNodeGroupMap{m}})
	assert.NoError(t, err)
	var batchResult ReconcileNodeGroupMapsBatchResult
	_ = result.Get(&batchResult)
	assert.Equal(t, 1, batchResult.Reconciled)
	mockStorage.AssertExpectations(t)
}
