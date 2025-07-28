package activities

import (
	"context"
	"fmt"
	"go.temporal.io/sdk/temporal"
	"net/http"
	"strings"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

const (
	UnRegisterNodesInfoNotAvailable    = "no Available Nodes to unregister"
	UnRegisterNodeGroupMapNotAvailable = "node group map not available"
)

var (
	harvestEndPoint     = env.GetString("HARVEST_HOST", "localhost:8080")
	harvestRestProtocol = env.GetString("HARVEST_REST_PROTOCOL", "http")

	deletePollerRestResponse = func(ctx context.Context, url string) (*http.Response, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
		if err != nil {
			return nil, err
		}
		client := &http.Client{}
		return client.Do(req)
	}

	deleteKubernetesLease                           = utils.DeleteKubernetesLease
	UnRegisterNodeFromHarvestFarmNonRetryableErrors = []string{
		UnRegisterNodesInfoNotAvailable,
		UnRegisterNodeGroupMapNotAvailable,
	}
)

type UnRegisterNodeFromHarvestActivityParams struct {
	PoolID            int64
	CustomerProjectID string
	TenantProjectID   string
	Nodes             []*datamodel.Node
	NodeGroupsMap     []*datamodel.NodeNodeGroupMap
}

type UnRegisterNodeFromHarvestActivity struct {
	SE database.Storage
}

func (unRegisterNodeToHarvest *UnRegisterNodeFromHarvestActivity) ValidateAndGetNodes(
	ctx context.Context,
	activityParams *UnRegisterNodeFromHarvestActivityParams,
) ([]*datamodel.Node, error) {
	logger := util.GetLogger(ctx)
	se := unRegisterNodeToHarvest.SE
	poolID := activityParams.PoolID
	nodes, err := se.GetNodesByPoolID(ctx, poolID)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			logger.Errorf("no nodes found for pool ID %d", poolID)
			return nil, temporal.NewNonRetryableApplicationError(err.Error(), UnRegisterNodesInfoNotAvailable, err)
		}
		logger.Warnf("failed to retrieve nodes information from pool ID %d: %w", poolID, err)
		return nil, err
	}
	return nodes, nil
}

func (unRegisterNodeToHarvest *UnRegisterNodeFromHarvestActivity) GetNodeGroupMapping(
	ctx context.Context,
	activityParams *UnRegisterNodeFromHarvestActivityParams,
) ([]*datamodel.NodeNodeGroupMap, error) {
	se := unRegisterNodeToHarvest.SE
	logger := util.GetLogger(ctx)
	var nodeGroupMappings []*datamodel.NodeNodeGroupMap
	for _, node := range activityParams.Nodes {
		nodeGroupMap, err := se.GetNodeNodeGroupMapByNodeID(ctx, node.ID)
		if err != nil {
			if errors.IsNotFoundErr(err) {
				logger.Errorf("no nodegroupmap info found for node ID %d", node.ID)
				return nil, temporal.NewNonRetryableApplicationError(err.Error(), UnRegisterNodeGroupMapNotAvailable, err)
			}
			logger.Warnf("failed to retrieve nodegroupmap information from nodeId %s: %w", node.ID, err)
			return nil, err
		}
		nodeGroupMappings = append(nodeGroupMappings, nodeGroupMap)
	}
	return nodeGroupMappings, nil
}

func (unRegisterNodeToHarvest *UnRegisterNodeFromHarvestActivity) DeleteNodeGroupMapping(
	ctx context.Context, activityParams *UnRegisterNodeFromHarvestActivityParams,
) error {
	logger := util.GetLogger(ctx)
	se := unRegisterNodeToHarvest.SE
	for _, nodeGroupMapping := range activityParams.NodeGroupsMap {
		err := se.DeleteNodeNodeGroupMap(ctx, nodeGroupMapping.ID)
		if err != nil {
			logger.Warnf("failed to delete nodeGroupMap for node %s: %w", nodeGroupMapping.NodeID, err)
			return err
		}
	}
	return nil
}

func (unRegisterNodeToHarvest *UnRegisterNodeFromHarvestActivity) DeletePollersFromHarvestFarm(
	ctx context.Context, activityParams *UnRegisterNodeFromHarvestActivityParams) error {
	logger := util.GetLogger(ctx)
	for _, nodeMap := range activityParams.NodeGroupsMap {
		leaseName := nodeMap.NodeGroup.LeaseName
		if len(leaseName) == 0 {
			logger.Warnf("no leaseName exists for nodeGroupMap Name:%s", nodeMap.NodeGroup.Name)
			continue
		}
		deleteURL := fmt.Sprintf(harvestRestProtocol+"://"+harvestEndPoint+"/config/%s/%d", leaseName, nodeMap.NodeID)
		resp, err := deletePollerRestResponse(ctx, deleteURL)
		if err != nil {
			logger.Warnf("Failed to delete YAML template for node mapping %d: %v", nodeMap.NodeID, err)
			return err
		}
		if resp.Body != nil {
			defer func() { _ = resp.Body.Close() }()
		}
		if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNotFound {
			logger.Infof("Successfully deleted poller: %s from harvest farm lease :%s, with status code:%d and status:%s", nodeMap.UUID, leaseName,
				resp.StatusCode, resp.Status)
		} else {
			logger.Warnf("delete failed for node mapping %d with status: %s", nodeMap.NodeID, resp.Status)
			return fmt.Errorf("delete yaml failed for node mapping %d: %s", nodeMap.NodeID, resp.Status)
		}
	}
	return nil
}

func (unRegisterNodeToHarvest *UnRegisterNodeFromHarvestActivity) ValidateAndReleaseLease(
	ctx context.Context,
	activityParams *UnRegisterNodeFromHarvestActivityParams) error {
	logger := util.GetLogger(ctx)
	se := unRegisterNodeToHarvest.SE
	for _, nodeGroupMap := range activityParams.NodeGroupsMap {
		nodesCount, err := se.GetNodeGroupMapNodeCount(ctx, nodeGroupMap.NodeGroupID)
		if err != nil {
			return err
		}
		if nodesCount == 0 {
			if nodeGroupMap.NodeGroup != nil {
				logger.Infof("Releasing kubernetes lease:%s for node group %d", nodeGroupMap.NodeGroup.LeaseName, nodeGroupMap.NodeGroupID)
				err := deleteKubernetesLease(ctx, vcpLeaseNameSpace, nodeGroupMap.NodeGroup.LeaseName)
				if err != nil {
					if strings.Contains(err.Error(), "not found") {
						logger.Warnf("kubernetes lease not found for node group %d: %v", nodeGroupMap.NodeGroupID, err)
						continue
					}
					logger.Errorf("Failed to delete kubernetes lease for node group %d: %v", nodeGroupMap.NodeGroupID, err)
					return err
				}
				// Delete lease table
				err = se.DeleteNodeGroup(ctx, nodeGroupMap.NodeGroupID)
				if err != nil {
					logger.Errorf("Failed to delete node group %d: %v", nodeGroupMap.NodeGroupID, err)
					return err
				}
			} else {
				logger.Warnf("NodeGroup is nil for NodeGroupID: %d", nodeGroupMap.NodeGroupID)
			}
		}
	}
	return nil
}
