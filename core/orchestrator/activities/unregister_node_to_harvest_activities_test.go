package activities

import (
	"context"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
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
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	testPoolID := int64(1)
	nodesInfo := getUnRegisterNodes()

	mockStorage.On("GetNodesByPoolID", ctx, testPoolID).Return(nodesInfo, nil)
	testActParams := &UnRegisterNodeFromHarvestActivityParams{
		PoolID: testPoolID,
	}
	dbNodesInfo, err := activity.ValidateAndGetNodes(ctx, testActParams)

	assert.NoError(t, err)
	assert.True(t, len(dbNodesInfo) == nodeCount)
	mockStorage.AssertExpectations(t)
}

func TestValidateAndGetNodes_FailWithNoNodes(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	testPoolID := int64(1)

	mockStorage.On("GetNodesByPoolID", ctx, testPoolID).Return(nil,
		vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, errors.NewNotFoundErr("node", nil)))
	testActParams := &UnRegisterNodeFromHarvestActivityParams{
		PoolID: testPoolID,
	}
	dbNodesInfo, err := activity.ValidateAndGetNodes(ctx, testActParams)

	assert.Error(t, err)
	assert.Nil(t, dbNodesInfo)
	assert.Contains(t, err.Error(), UnRegisterNodesInfoNotAvailable)
	mockStorage.AssertExpectations(t)
}

func TestValidateAndGetNodes_Error(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	testPoolID := int64(1)

	mockStorage.On("GetNodesByPoolID", ctx, testPoolID).Return(nil, errors.New("db-error"))
	testActParams := &UnRegisterNodeFromHarvestActivityParams{
		PoolID: testPoolID,
	}
	dbNodesInfo, err := activity.ValidateAndGetNodes(ctx, testActParams)

	assert.Error(t, err)
	assert.Nil(t, dbNodesInfo)
	mockStorage.AssertExpectations(t)
}

func TestGetNodeGroupMapping_Success(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	nodesInfo := getUnRegisterNodes()
	nodeGroupMapInfo := getNodeGroupMap(false, true)
	for index, nodeInfo := range nodesInfo {
		mockStorage.On("GetNodeNodeGroupMapByNodeID", ctx, nodeInfo.ID).Return(nodeGroupMapInfo[index], nil)
	}
	testActParams := &UnRegisterNodeFromHarvestActivityParams{
		Nodes: nodesInfo,
	}

	result, err := activity.GetNodeGroupMapping(ctx, testActParams)

	assert.NoError(t, err)
	assert.True(t, len(result) == nodeCount)
	mockStorage.AssertExpectations(t)
}

func TestValidateAndGetNodes_FailWithNoNodeGroupMap(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	nodesInfo := getUnRegisterNodes()

	mockStorage.On("GetNodeNodeGroupMapByNodeID", ctx, nodesInfo[0].ID).Return(nil,
		vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, errors.NewNotFoundErr("nodeGroupMap", nil)))

	testActParams := &UnRegisterNodeFromHarvestActivityParams{
		Nodes: nodesInfo,
	}

	result, err := activity.GetNodeGroupMapping(ctx, testActParams)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), UnRegisterNodeGroupMapNotAvailable)
	mockStorage.AssertExpectations(t)
}

func TestGetNodeGroupMapping_DeletedNodeGroup(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	nodesInfo := getUnRegisterNodes()
	nodeGroupMapInfo := getNodeGroupMap(true, true)
	for index, nodeInfo := range nodesInfo {
		mockStorage.On("GetNodeNodeGroupMapByNodeID", ctx, nodeInfo.ID).Return(nodeGroupMapInfo[index], nil)
	}

	testActParams := &UnRegisterNodeFromHarvestActivityParams{
		Nodes: nodesInfo,
	}

	result, err := activity.GetNodeGroupMapping(ctx, testActParams)

	assert.NoError(t, err)
	assert.True(t, len(result) == 2)
	mockStorage.AssertExpectations(t)
}

func TestGetNodeGroupMapping_Error(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	nodesInfo := getUnRegisterNodes()

	mockStorage.On("GetNodeNodeGroupMapByNodeID", ctx, nodesInfo[0].ID).Return(nil, errors.New("db-error"))

	testActParams := &UnRegisterNodeFromHarvestActivityParams{
		Nodes: nodesInfo,
	}
	result, err := activity.GetNodeGroupMapping(ctx, testActParams)

	assert.Error(t, err)
	assert.Nil(t, result)
	mockStorage.AssertExpectations(t)
}

func TestDeleteNodeGroupMapping_Success(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	nodeGroupMapInfo := getNodeGroupMap(false, true)

	for _, nodeGroupMap := range nodeGroupMapInfo {
		mockStorage.On("DeleteNodeNodeGroupMap", ctx, nodeGroupMap.ID).Return(nil)
	}
	testActParams := &UnRegisterNodeFromHarvestActivityParams{
		NodeGroupsMap: nodeGroupMapInfo,
	}

	err := activity.DeleteNodeGroupMapping(ctx, testActParams)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestDeleteNodeGroupMapping_Error(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	nodeGroupMapInfo := getNodeGroupMap(false, true)

	mockStorage.On("DeleteNodeNodeGroupMap", ctx, nodeGroupMapInfo[0].ID).Return(gorm.ErrRecordNotFound)

	testActParams := &UnRegisterNodeFromHarvestActivityParams{
		NodeGroupsMap: nodeGroupMapInfo,
	}

	err := activity.DeleteNodeGroupMapping(ctx, testActParams)
	assert.Error(t, err)
	mockStorage.AssertExpectations(t)
}

func TestDeletePollersFromHarvestFarm_Success(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	nodeGroupMapInfo := getNodeGroupMap(true, true)
	oldDeletePollerRestResponse := deletePollerRestResponse
	deletePollerRestResponse = func(ctx context.Context, url string) (*http.Response, error) {
		return &http.Response{StatusCode: 200,
			Status: "Deleted poller",
		}, nil
	}
	defer func() { deletePollerRestResponse = oldDeletePollerRestResponse }()

	testActParams := &UnRegisterNodeFromHarvestActivityParams{
		NodeGroupsMap: nodeGroupMapInfo,
	}

	err := activity.DeletePollersFromHarvestFarm(ctx, testActParams)
	assert.NoError(t, err)
}

func TestDeletePollersFromHarvestFarm_StatusNotFound(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

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
	err := activity.DeletePollersFromHarvestFarm(ctx, testActParams)
	assert.NoError(t, err)
}

func TestDeletePollersFromHarvestFarm_Error(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	nodeGroupMapInfo := getNodeGroupMap(true, true)
	oldDeletePollerRestResponse := deletePollerRestResponse
	deletePollerRestResponse = func(ctx context.Context, url string) (*http.Response, error) {
		return nil, errors.New("rest-client failed")
	}
	defer func() { deletePollerRestResponse = oldDeletePollerRestResponse }()

	testActParams := &UnRegisterNodeFromHarvestActivityParams{
		NodeGroupsMap: nodeGroupMapInfo,
	}

	err := activity.DeletePollersFromHarvestFarm(ctx, testActParams)
	assert.Error(t, err)
	assert.Equal(t, "rest-client failed", err.Error())
}

// Below  test case will validate and issue lease client delete of k8's leases
func TestValidateAndReleaseLease(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	nodeGroupMapInfo := getNodeGroupMap(true, true)

	for _, nodeGroupMap := range nodeGroupMapInfo {
		mockStorage.On("GetNodeGroupMapNodeCount", ctx, nodeGroupMap.NodeGroupID).Return(int64(0), nil)
		mockStorage.On("DeleteNodeGroup", ctx, nodeGroupMap.NodeGroupID).Return(nil)
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

	err := activity.ValidateAndReleaseLease(ctx, testActParams)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestValidateAndReleaseLease_NoLeaseToDelete(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	nodeGroupMapInfo := getNodeGroupMap(true, true)

	for _, nodeGroupMap := range nodeGroupMapInfo {
		mockStorage.On("GetNodeGroupMapNodeCount", ctx, nodeGroupMap.NodeGroupID).Return(int64(1), nil)
	}

	testActParams := &UnRegisterNodeFromHarvestActivityParams{
		NodeGroupsMap: nodeGroupMapInfo,
	}

	err := activity.ValidateAndReleaseLease(ctx, testActParams)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestValidateAndReleaseLease_SingleLeaseToDelete(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	nodeGroupMapInfo := getNodeGroupMap(true, true)

	mockStorage.On("GetNodeGroupMapNodeCount", ctx, nodeGroupMapInfo[0].NodeGroupID).Return(int64(1), nil)
	mockStorage.On("GetNodeGroupMapNodeCount", ctx, nodeGroupMapInfo[1].NodeGroupID).Return(int64(0), nil)
	mockStorage.On("DeleteNodeGroup", ctx, nodeGroupMapInfo[1].NodeGroupID).Return(nil)

	oldDeleteKubernetesLease := deleteKubernetesLease
	// Mock delete lease which is in utils
	deleteKubernetesLease = func(ctx context.Context, leaseNameSpace, leaseName string) error {
		return nil
	}
	defer func() { deleteKubernetesLease = oldDeleteKubernetesLease }()

	testActParams := &UnRegisterNodeFromHarvestActivityParams{
		NodeGroupsMap: nodeGroupMapInfo,
	}

	err := activity.ValidateAndReleaseLease(ctx, testActParams)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestValidateAndReleaseLease_LeaseClientError(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	nodeGroupMapInfo := getNodeGroupMap(true, true)

	mockStorage.On("GetNodeGroupMapNodeCount", ctx, nodeGroupMapInfo[0].NodeGroupID).Return(int64(1), nil)
	mockStorage.On("GetNodeGroupMapNodeCount", ctx, nodeGroupMapInfo[1].NodeGroupID).Return(int64(0), nil)

	oldDeleteKubernetesLease := deleteKubernetesLease
	// Mock delete lease which is in utils
	deleteKubernetesLease = func(ctx context.Context, leaseNameSpace, leaseName string) error {
		return errors.New("lease-client failed")
	}
	defer func() { deleteKubernetesLease = oldDeleteKubernetesLease }()

	testActParams := &UnRegisterNodeFromHarvestActivityParams{
		NodeGroupsMap: nodeGroupMapInfo,
	}

	err := activity.ValidateAndReleaseLease(ctx, testActParams)
	assert.Error(t, err)
	assert.Equal(t, "lease-client failed", err.Error())
	mockStorage.AssertExpectations(t)
}

func TestValidateAndReleaseLease_DBError(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	nodeGroupMapInfo := getNodeGroupMap(true, true)

	mockStorage.On("GetNodeGroupMapNodeCount", ctx, nodeGroupMapInfo[0].NodeGroupID).Return(int64(0), errors.New("db-error"))

	testActParams := &UnRegisterNodeFromHarvestActivityParams{
		NodeGroupsMap: nodeGroupMapInfo,
	}

	err := activity.ValidateAndReleaseLease(ctx, testActParams)
	assert.Error(t, err)
	assert.Equal(t, "db-error", err.Error())
	mockStorage.AssertExpectations(t)
}
