package activities

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
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
	dbNodesInfo, err := activity.ValidateAndGetNodes(ctx, testPoolID)

	assert.NoError(t, err)
	assert.True(t, len(dbNodesInfo) == nodeCount)
	mockStorage.AssertExpectations(t)
}

func TestValidateAndGetNodes_EmptyNodes(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	testPoolID := int64(1)
	nodesInfo := []*datamodel.Node{}

	mockStorage.On("GetNodesByPoolID", ctx, testPoolID).Return(nodesInfo, nil)
	dbNodesInfo, err := activity.ValidateAndGetNodes(ctx, testPoolID)

	assert.NoError(t, err)
	assert.True(t, len(dbNodesInfo) == 0)
	mockStorage.AssertExpectations(t)
}

func TestValidateAndGetNodes_Error(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	testPoolID := int64(1)

	mockStorage.On("GetNodesByPoolID", ctx, testPoolID).Return(nil, gorm.ErrRecordNotFound)
	dbNodesInfo, err := activity.ValidateAndGetNodes(ctx, testPoolID)

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

	result, err := activity.GetNodeGroupMapping(ctx, nodesInfo)

	assert.NoError(t, err)
	assert.True(t, len(result) == nodeCount)
	mockStorage.AssertExpectations(t)
}

func TestGetNodeGroupMapping_EmptyNodeGroup(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	nodesInfo := getUnRegisterNodes()
	nodeGroupMapInfo := getNodeGroupMap(true, true)
	for index, nodeInfo := range nodesInfo {
		mockStorage.On("GetNodeNodeGroupMapByNodeID", ctx, nodeInfo.ID).Return(nodeGroupMapInfo[index], nil)
	}

	result, err := activity.GetNodeGroupMapping(ctx, nodesInfo)

	assert.NoError(t, err)
	assert.True(t, len(result) == 0)
	mockStorage.AssertExpectations(t)
}

func TestGetNodeGroupMapping_Error(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	nodesInfo := getUnRegisterNodes()

	mockStorage.On("GetNodeNodeGroupMapByNodeID", ctx, nodesInfo[0].ID).Return(nil, gorm.ErrRecordNotFound)

	result, err := activity.GetNodeGroupMapping(ctx, nodesInfo)

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

	err := activity.DeleteNodeGroupMapping(ctx, nodeGroupMapInfo)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestDeleteNodeGroupMapping_Error(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	nodeGroupMapInfo := getNodeGroupMap(false, true)

	mockStorage.On("DeleteNodeNodeGroupMap", ctx, nodeGroupMapInfo[0].ID).Return(gorm.ErrRecordNotFound)

	err := activity.DeleteNodeGroupMapping(ctx, nodeGroupMapInfo)
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

	err := activity.DeletePollersFromHarvestFarm(ctx, nodeGroupMapInfo)
	assert.NoError(t, err)
}

func TestDeletePollersFromHarvestFarm_StatusFailure(t *testing.T) {
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

	err := activity.DeletePollersFromHarvestFarm(ctx, nodeGroupMapInfo)
	assert.Error(t, err)
	assert.Equal(t, "delete yaml failed: Poller file not found for given lease and poller name", err.Error())
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

	err := activity.DeletePollersFromHarvestFarm(ctx, nodeGroupMapInfo)
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

	err := activity.ValidateAndReleaseLease(ctx, nodeGroupMapInfo)
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

	err := activity.ValidateAndReleaseLease(ctx, nodeGroupMapInfo)
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
	err := activity.ValidateAndReleaseLease(ctx, nodeGroupMapInfo)
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

	err := activity.ValidateAndReleaseLease(ctx, nodeGroupMapInfo)
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

	err := activity.ValidateAndReleaseLease(ctx, nodeGroupMapInfo)
	assert.Error(t, err)
	assert.Equal(t, "db-error", err.Error())
	mockStorage.AssertExpectations(t)
}
