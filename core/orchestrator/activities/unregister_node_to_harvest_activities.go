package activities

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/worker/metrics"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
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

// ValidateAndGetNodes retrieves nodes for the given pool ID.
// Note: dbHbCtx is the context with DB heartbeat timeout configured from the workflow
func (unRegisterNodeToHarvest *UnRegisterNodeFromHarvestActivity) ValidateAndGetNodes(
	dbHbCtx context.Context,
	activityParams *UnRegisterNodeFromHarvestActivityParams,
) ([]*datamodel.Node, error) {
	logger := util.GetLogger(dbHbCtx)
	activity.RecordHeartbeat(dbHbCtx, "ValidateAndGetNodes started")

	se := unRegisterNodeToHarvest.SE
	poolID := activityParams.PoolID
	nodes, err := se.GetNodesByPoolID(dbHbCtx, poolID)
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

// GetNodeGroupMapping retrieves node group mappings for the given nodes.
// Note: dbHbCtx is the context with DB heartbeat timeout configured from the workflow
func (unRegisterNodeToHarvest *UnRegisterNodeFromHarvestActivity) GetNodeGroupMapping(
	dbHbCtx context.Context,
	activityParams *UnRegisterNodeFromHarvestActivityParams,
) ([]*datamodel.NodeNodeGroupMap, error) {
	se := unRegisterNodeToHarvest.SE
	logger := util.GetLogger(dbHbCtx)
	activity.RecordHeartbeat(dbHbCtx, "GetNodeGroupMapping started")

	var nodeGroupMappings []*datamodel.NodeNodeGroupMap
	for i, node := range activityParams.Nodes {
		activity.RecordHeartbeat(dbHbCtx, fmt.Sprintf("Getting node group mapping %d/%d", i+1, len(activityParams.Nodes)))
		nodeGroupMap, err := se.GetNodeNodeGroupMapByNodeID(dbHbCtx, node.ID)
		if err != nil {
			if errors.IsNotFoundErr(err) {
				// If we have Large Pool and multi-HA pair registration is disabled, then only 2 nodes are
				// registered at a time, so if no nodeGroupMap is found for a node, then continue to the next node
				if len(activityParams.Nodes) > 2 && !enableMultiHaPairRegistration {
					continue
				}
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

// DeleteNodeGroupMapping deletes node group mappings from the database.
// Note: dbHbCtx is the context with DB heartbeat timeout configured from the workflow
func (unRegisterNodeToHarvest *UnRegisterNodeFromHarvestActivity) DeleteNodeGroupMapping(
	dbHbCtx context.Context, activityParams *UnRegisterNodeFromHarvestActivityParams,
) error {
	logger := util.GetLogger(dbHbCtx)
	activity.RecordHeartbeat(dbHbCtx, "DeleteNodeGroupMapping started")

	se := unRegisterNodeToHarvest.SE
	for i, nodeGroupMapping := range activityParams.NodeGroupsMap {
		activity.RecordHeartbeat(dbHbCtx, fmt.Sprintf("Deleting node group mapping %d/%d", i+1, len(activityParams.NodeGroupsMap)))
		err := se.DeleteNodeNodeGroupMap(dbHbCtx, nodeGroupMapping.ID)
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
	activity.RecordHeartbeat(ctx, "DeletePollersFromHarvestFarm started")

	for i, nodeMap := range activityParams.NodeGroupsMap {
		activity.RecordHeartbeat(ctx, fmt.Sprintf("Deleting poller %d/%d", i+1, len(activityParams.NodeGroupsMap)))
		leaseName := nodeMap.NodeGroup.LeaseName
		if len(leaseName) == 0 {
			logger.Warnf("no leaseName exists for nodeGroupMap Name:%s", nodeMap.NodeGroup.Name)
			continue
		}
		deleteURL := fmt.Sprintf(harvestRestProtocol+"://"+harvestEndPoint+"/config/%s/%s%d", leaseName, leasePrefix, nodeMap.NodeID)
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

// ValidateAndReleaseLease validates and releases Kubernetes leases for node groups.
// Note: dbHbCtx is the context with DB heartbeat timeout configured from the workflow
func (unRegisterNodeToHarvest *UnRegisterNodeFromHarvestActivity) ValidateAndReleaseLease(
	dbHbCtx context.Context,
	activityParams *UnRegisterNodeFromHarvestActivityParams) error {
	logger := util.GetLogger(dbHbCtx)
	activity.RecordHeartbeat(dbHbCtx, "ValidateAndReleaseLease started")

	se := unRegisterNodeToHarvest.SE
	for i, nodeGroupMap := range activityParams.NodeGroupsMap {
		activity.RecordHeartbeat(dbHbCtx, fmt.Sprintf("Validating and releasing lease %d/%d", i+1, len(activityParams.NodeGroupsMap)))
		nodesCount, err := se.GetNodeGroupMapNodeCount(dbHbCtx, nodeGroupMap.NodeGroupID)
		if err != nil {
			return err
		}
		if nodesCount == 0 {
			if nodeGroupMap.NodeGroup != nil {
				logger.Infof("Releasing kubernetes lease:%s for node group %d", nodeGroupMap.NodeGroup.LeaseName, nodeGroupMap.NodeGroupID)
				err := deleteKubernetesLease(dbHbCtx, vcpLeaseNameSpace, nodeGroupMap.NodeGroup.LeaseName)
				if err != nil {
					if strings.Contains(err.Error(), "not found") {
						logger.Warnf("kubernetes lease not found for node group %d: %v", nodeGroupMap.NodeGroupID, err)
						continue
					}
					logger.Errorf("Failed to delete kubernetes lease for node group %d: %v", nodeGroupMap.NodeGroupID, err)
					return err
				}
				// Delete lease table
				err = se.DeleteNodeGroup(dbHbCtx, nodeGroupMap.NodeGroupID)
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

func (unRegisterNodeToHarvest *UnRegisterNodeFromHarvestActivity) AlertHarvestUnRegisterFailure(ctx context.Context, errorDetails string) error {
	metrics.IncJobStatusCounter(ctx, errorDetails, "failed to un-register nodes from harvest farm")
	return nil
}

// ListAllMapsWithDeletedNodesParams optionally configures page size for the internal keyset loop. Zero = default.
type ListAllMapsWithDeletedNodesParams struct {
	PageSize int // limit per internal page; default 100
}

// ListAllMapsWithDeletedNodesResult returns the full list of node group maps whose node is DELETED.
type ListAllMapsWithDeletedNodesResult struct {
	MapsToReconcile []*datamodel.NodeNodeGroupMap
}

const defaultListAllPageSize = 100
const maxListAllPageIterations = 10000

// ListAllMapsWithDeletedNodes runs the keyset pagination loop inside the activity: fetches all pages
// of non-deleted node group maps, filters to those whose node is in DELETED state, and returns
// the entire list. Use with ReconcileNodeGroupMapsBatch so the workflow has no loop.
func (a *UnRegisterNodeFromHarvestActivity) ListAllMapsWithDeletedNodes(
	ctx context.Context,
	params *ListAllMapsWithDeletedNodesParams,
) (*ListAllMapsWithDeletedNodesResult, error) {
	logger := util.GetLogger(ctx)
	activity.RecordHeartbeat(ctx, "ListAllMapsWithDeletedNodes started")

	pageSize := defaultListAllPageSize
	if params != nil && params.PageSize > 0 {
		pageSize = params.PageSize
	}

	se := a.SE
	var all []*datamodel.NodeNodeGroupMap
	afterID := int64(0)
	iterations := 0

	for iterations < maxListAllPageIterations {
		iterations++
		activity.RecordHeartbeat(ctx, fmt.Sprintf("Listing page after ID %d", afterID))

		maps, err := se.ListNodeNodeGroupMapAfterID(ctx, false, afterID, pageSize)
		if err != nil {
			logger.Warnf("failed to list node group maps: %v", err)
			return nil, err
		}
		if len(maps) == 0 {
			break
		}
		lastID := maps[len(maps)-1].ID

		for i, m := range maps {
			activity.RecordHeartbeat(ctx, fmt.Sprintf("Checking node %d/%d (page)", i+1, len(maps)))

			node, err := se.GetNodeByID(ctx, m.NodeID)
			if err != nil {
				if errors.IsNotFoundErr(err) {
					logger.Warnf("node ID %d for node group map %d not found, skipping", m.NodeID, m.ID)
					continue
				}
				logger.Warnf("GetNodeByID failed for node %d (node group map %d): %v", m.NodeID, m.ID, err)
				return nil, err
			}
			if node.State != models.LifeCycleStateDeleted {
				continue
			}
			all = append(all, m)
		}

		if len(maps) < pageSize {
			break
		}
		afterID = lastID
	}

	logger.Infof("ListAllMapsWithDeletedNodes collected %d maps to reconcile", len(all))
	return &ListAllMapsWithDeletedNodesResult{MapsToReconcile: all}, nil
}

// ReconcileNodeGroupMapsBatchParams holds the list of node group maps to reconcile (Harvest delete + DB soft-delete).
type ReconcileNodeGroupMapsBatchParams struct {
	Maps []*datamodel.NodeNodeGroupMap
}

// ReconcileNodeGroupMapsBatchResult holds the number of records successfully reconciled.
type ReconcileNodeGroupMapsBatchResult struct {
	Reconciled int
}

// ReconcileNodeGroupMapsBatch issues Harvest delete and DB soft-delete for each given node group map.
// Call after ListAllMapsWithDeletedNodes; input maps are expected to have NodeGroup preloaded.
func (a *UnRegisterNodeFromHarvestActivity) ReconcileNodeGroupMapsBatch(
	ctx context.Context,
	params *ReconcileNodeGroupMapsBatchParams,
) (*ReconcileNodeGroupMapsBatchResult, error) {
	logger := util.GetLogger(ctx)
	activity.RecordHeartbeat(ctx, "ReconcileNodeGroupMapsBatch started")

	if params == nil || len(params.Maps) == 0 {
		return &ReconcileNodeGroupMapsBatchResult{}, nil
	}

	se := a.SE
	var reconciled int
	for i, m := range params.Maps {
		activity.RecordHeartbeat(ctx, fmt.Sprintf("Reconciling %d/%d", i+1, len(params.Maps)))

		if m.NodeGroup != nil && len(m.NodeGroup.LeaseName) > 0 {
			deleteURL := fmt.Sprintf(harvestRestProtocol+"://"+harvestEndPoint+"/config/%s/%s%d", m.NodeGroup.LeaseName, leasePrefix, m.NodeID)
			resp, err := deletePollerRestResponse(ctx, deleteURL)
			if err != nil {
				logger.Warnf("failed to delete poller from harvest for node group map %d (node %d): %v", m.ID, m.NodeID, err)
				continue
			}
			if resp != nil && resp.Body != nil {
				_ = resp.Body.Close()
			}
			if resp != nil && resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
				logger.Warnf("harvest delete returned status %d for node group map %d", resp.StatusCode, m.ID)
				continue
			}
		}

		if err := se.DeleteNodeNodeGroupMap(ctx, m.ID); err != nil {
			logger.Warnf("failed to mark node group map %d as deleted: %v", m.ID, err)
			continue
		}
		reconciled++
		logger.Infof("reconciled node group map %d (node %d deleted): removed from Harvest and marked deleted", m.ID, m.NodeID)
	}

	return &ReconcileNodeGroupMapsBatchResult{Reconciled: reconciled}, nil
}
