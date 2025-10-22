package activities

import (
	"context"
	"fmt"
	"io"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/worker/metrics"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"golang.org/x/sync/errgroup"
)

const (
	harvestNodeRefreshMaxConcurrency = 10
	paginationLimit                  = 1000
)

var (
	renderTemplateForharvest = utils.RenderHarvestTemplate
)

type HarvestNodesRefreshActivityParams struct {
	NodeGroupMaps []*datamodel.NodeNodeGroupMap
	RefreshURL    string
}

type HarvestNodesRefreshActivity struct {
	SE database.Storage
}

func (harvestNodesRefresh *HarvestNodesRefreshActivity) GetNodeGroupMaps(
	ctx context.Context,
	_ *HarvestNodesRefreshActivityParams,
) ([]*datamodel.NodeNodeGroupMap, error) {
	var nodeGroupMapsInfo []*datamodel.NodeNodeGroupMap
	logger := util.GetLogger(ctx)
	se := harvestNodesRefresh.SE
	totalFetched := 0
	for {
		pagination := &dbutils.Pagination{
			Limit:  paginationLimit,
			Offset: totalFetched,
		}
		nodeGroupMaps, err := se.ListNodeNodeGroupMap(ctx, false, pagination)
		if err != nil {
			logger.Errorf("Failed to list node group maps: %v", err)
			return nodeGroupMapsInfo, err
		}
		// Break if no more records
		if len(nodeGroupMaps) == 0 {
			break
		}
		totalFetched += len(nodeGroupMaps)
		nodeGroupMapsInfo = append(nodeGroupMapsInfo, nodeGroupMaps...)
		// If fetched less than limit, we are done
		if len(nodeGroupMaps) < paginationLimit {
			break
		}
	}
	return nodeGroupMapsInfo, nil
}

func (harvestNodesRefresh *HarvestNodesRefreshActivity) RefreshHarvestNodes(
	ctx context.Context,
	activityParams *HarvestNodesRefreshActivityParams,
) error {
	logger := util.GetLogger(ctx)
	nodeGroupsMaps := activityParams.NodeGroupMaps

	// Validate if no maps to process
	if len(nodeGroupsMaps) == 0 {
		logger.Warn("No node group maps to process")
		return nil
	}

	// Filter valid node group maps
	var validMaps []*datamodel.NodeNodeGroupMap
	for _, nodeGroupMap := range nodeGroupsMaps {
		if validateNodeGroupMap(logger, nodeGroupMap) {
			if nodeGroupMap.HarvestConfig.SECRET_PROJECT == "" {
				nodeGroupMap.HarvestConfig.SECRET_PROJECT = env.SecretManagerProjectID
			}
			if nodeGroupMap.HarvestConfig.SECRET_ID == "" {
				// Skip if no secret ID as it will break the refresh process
				logger.Warnf("Skipping node group map %v due to missing secret ID", nodeGroupMap.ID)
				continue
			}
			validMaps = append(validMaps, nodeGroupMap)
		}
	}

	if len(validMaps) == 0 {
		logger.Warn("No valid node group maps to process after validation")
		return nil
	}

	// Create error group with context and concurrency limit
	errGroup, ctx := errgroup.WithContext(ctx)
	errGroup.SetLimit(harvestNodeRefreshMaxConcurrency)

	// Process each valid node group map
	for _, nodeGroupMap := range validMaps {
		nodeGroupMap := nodeGroupMap // capture loop variable
		errGroup.Go(func() error {
			// Check context before processing
			if ctx.Err() != nil {
				return nil // Context cancelled, skip processing
			}

			if err := processHarvestNodeRefresh(ctx, logger, nodeGroupMap, activityParams.RefreshURL); err != nil {
				// Log the error but don't fail the entire operation
				logger.Errorf("Failed to refresh node %v: %v", nodeGroupMap, err)
				// Return nil to continue processing other nodes
				// If you want to fail fast, return the error instead
				return nil
			}
			return nil
		})
	}

	// Wait for all goroutines to complete
	if err := errGroup.Wait(); err != nil {
		// This will only happen if you return errors from the goroutines above
		logger.Warnf("Some nodes failed to refresh: %v", err)
		return err
	}

	return nil
}

func (harvestNodesRefresh *HarvestNodesRefreshActivity) AlertHarvestRefreshFailure(ctx context.Context, errorDetails string) error {
	metrics.IncJobStatusCounter(ctx, errorDetails, "failed to refresh nodes info harvest farm")
	return nil
}

func validateNodeGroupMap(logger log.Logger, nodeGroupMap *datamodel.NodeNodeGroupMap) bool {
	switch {
	case nodeGroupMap == nil:
		logger.Warnf("nil node group map")
	case nodeGroupMap.NodeID == 0:
		logger.Warnf("empty node ID for node group map: %v", nodeGroupMap.ID)
	case nodeGroupMap.NodeGroup == nil:
		logger.Warnf("empty node group for node group map: %v", nodeGroupMap.ID)
	default:
		return true // returns true only if all validations pass
	}
	return false
}

// processHarvestNodeRefresh handles a single node group map processing
func processHarvestNodeRefresh(
	ctx context.Context,
	logger log.Logger,
	nodeGroupMap *datamodel.NodeNodeGroupMap,
	refreshURL string,
) error {
	nodeGroup := nodeGroupMap.NodeGroup
	leaseName := nodeGroup.LeaseName

	tmplStr, err := renderTemplateForharvest(nodeGroupMap.HarvestConfig)
	if err != nil {
		logger.Warnf("failed to render template for lease: %s with node group map: %v", leaseName, nodeGroupMap.ID)
		return fmt.Errorf("template render failed: %w", err)
	}

	resp, err := uploadYAMLFile(ctx, UploadYAMLFileInput{
		URL:       refreshURL,
		YAML:      tmplStr,
		LeaseName: leaseName,
		NodeID:    nodeGroupMap.NodeID,
	})
	if err != nil {
		logger.Errorf("Failed to upload YAML template for node mapping %d: %v", nodeGroupMap.ID, err)
		return fmt.Errorf("upload failed: %w", err)
	}

	if resp == nil {
		logger.Errorf("No response received for node mapping %d", nodeGroupMap.ID)
		return fmt.Errorf("nil response for node mapping %d", nodeGroupMap.ID)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			logger.Debugf("Failed to close response body: %v", err)
		}
	}()

	// Read body with size limit to prevent memory issues
	limitedReader := io.LimitReader(resp.Body, 1<<20) // 1MB limit
	body, err := io.ReadAll(limitedReader)
	if err != nil {
		logger.Errorf("Failed to read response body for node mapping %d: %v", nodeGroupMap.ID, err)
		return fmt.Errorf("response read failed: %w", err)
	}

	// Check status code and handle non-success cases
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		logger.Errorf("Upload failed for node mapping %d with status: %s, response: %s",
			nodeGroupMap.ID, resp.Status, string(body))
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, resp.Status)
	}

	logger.Infof("Successfully uploaded rendered template for node mapping %d as YAML file", nodeGroupMap.ID)
	return nil
}
