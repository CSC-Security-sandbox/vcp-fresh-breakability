package activities

import (
	"context"
	"errors"
	"fmt"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"net/http"
	"strings"
)

var (
	vcpLeaseNameSpace   = env.GetString("LEASE_NAMESPACE", "vcp")
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

	deleteKubernetesLease = utils.DeleteKubernetesLease
)

type UnRegisterNodeFromHarvestActivity struct {
	SE database.Storage
}

func (unRegisterNodeToHarvest *UnRegisterNodeFromHarvestActivity) ValidateAndGetNodes(
	ctx context.Context, poolId int64,
) ([]*datamodel.Node, error) {
	logger := util.GetLogger(ctx)
	se := unRegisterNodeToHarvest.SE
	nodes, err := se.GetNodesByPoolID(ctx, poolId)
	if err != nil {
		logger.Warnf("failed to retrieve nodes information from poodId %s: %w", poolId, err)
		return nil,
			vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, fmt.Errorf("failed to retrieve nodes for pool %d: %w", poolId, err))
	}
	var deletedPoolNodes []*datamodel.Node
	for _, node := range nodes {
		if node.State == models.LifeCycleStateDeleted {
			deletedPoolNodes = append(deletedPoolNodes, node)
		}
	}
	return deletedPoolNodes, nil
}

func (unRegisterNodeToHarvest *UnRegisterNodeFromHarvestActivity) GetNodeGroupMapping(
	ctx context.Context, nodes []*datamodel.Node) ([]*datamodel.NodeNodeGroupMap, error) {
	se := unRegisterNodeToHarvest.SE
	logger := util.GetLogger(ctx)
	var nodeGroupMappings []*datamodel.NodeNodeGroupMap
	for _, node := range nodes {
		nodeGroupMap, err := se.GetNodeNodeGroupMapByNodeID(ctx, node.ID)
		if err != nil {
			logger.Warnf("failed to retrieve node group mappings for node %s: %w", node.ID, err)
			return nil, err
		}
		if nodeGroupMap.DeletedAt == nil {
			nodeGroupMappings = append(nodeGroupMappings, nodeGroupMap)
		}
	}
	return nodeGroupMappings, nil
}

func (unRegisterNodeToHarvest *UnRegisterNodeFromHarvestActivity) DeleteNodeGroupMapping(
	ctx context.Context, nodeGroupMappings []*datamodel.NodeNodeGroupMap,
) error {
	logger := util.GetLogger(ctx)
	se := unRegisterNodeToHarvest.SE
	for _, nodeGroupMapping := range nodeGroupMappings {
		err := se.DeleteNodeNodeGroupMap(ctx, nodeGroupMapping.ID)
		if err != nil {
			logger.Warnf("failed to delete nodeGroupMap for node %s: %w", nodeGroupMapping.NodeID, err)
			return err
		}
	}
	return nil
}

func (unRegisterNodeToHarvest *UnRegisterNodeFromHarvestActivity) DeletePollersFromHarvestFarm(
	ctx context.Context, nodeGroupMap []*datamodel.NodeNodeGroupMap) error {
	logger := util.GetLogger(ctx)
	for _, nodeMap := range nodeGroupMap {
		leaseName := nodeMap.NodeGroup.LeaseName
		deleteURL := fmt.Sprintf(harvestRestProtocol+"://"+harvestEndPoint+"/config/%s/%d", leaseName, nodeMap.NodeID)
		resp, err := deletePollerRestResponse(ctx, deleteURL)
		if err != nil {
			logger.Warnf("Failed to delete YAML template for node mapping %d: %v", nodeMap.NodeID, err)
			return err
		}
		if resp.Body != nil {
			defer func() { _ = resp.Body.Close() }()
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			logger.Warnf("delete failed for node mapping %d with status: %s", nodeMap.NodeID, resp.Status)
			return errors.New("delete yaml failed: " + resp.Status)
		}
		logger.Infof("Successfully deleted poller: %s from harvest farm lease :%s", nodeMap.UUID, leaseName)
	}
	return nil
}

func (unRegisterNodeToHarvest *UnRegisterNodeFromHarvestActivity) ValidateAndReleaseLease(
	ctx context.Context,
	nodeGroupsMap []*datamodel.NodeNodeGroupMap) error {
	logger := util.GetLogger(ctx)
	se := unRegisterNodeToHarvest.SE
	for _, nodeGroupMap := range nodeGroupsMap {
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
