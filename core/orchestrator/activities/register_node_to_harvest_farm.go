package activities

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// RegisterNodeToHarvestFarmInput holds input parameters for the activity
type RegisterNodeToHarvestFarmInput struct {
	PoolID           int64
	MaxNodesPerGroup int

	CustomerProjectID string
	TenantProjectID   string
}

// RegisterNodeToHarvestFarmOutput holds the output for the next activity
// Contains the rendered template and upload URL
// You can add more fields as needed
type RegisterNodeToHarvestFarmOutput struct {
	RenderedTemplate string
	UploadURL        string
}

// RegisterNodeToHarvestFarmActivity struct for dependency injection
// SE is the Storage Engine (database.Storage)
type RegisterNodeToHarvestFarmActivity struct {
	SE database.Storage
	// Injected function for loading the harvest template (for testability)
	LoadHarvestTemplateFunc func() (string, error)
}

// RegisterNodeToHarvestFarm registers two nodes to the Harvest farm and assigns them to node groups in the database
// Now returns output for the upload activity
func (a *RegisterNodeToHarvestFarmActivity) RegisterNodeToHarvestFarm(ctx context.Context, input RegisterNodeToHarvestFarmInput) ([]*datamodel.NodeNodeGroupMap, error) {
	logger := util.GetLogger(ctx)
	logger.Infof("Registering nodes to Harvest farm: %d", input.PoolID)

	nodes, err := a.SE.GetNodesByPoolID(ctx, input.PoolID)
	if err != nil {
		logger.Errorf("Failed to fetch node for pool ID %d: %v", input.PoolID, err)
		return nil, fmt.Errorf("error fetching nodes for pool ID %d: %w", input.PoolID, err)
	}

	if len(nodes) < 2 {
		logger.Errorf("Not enough nodes found for pool ID %d: got %d", input.PoolID, len(nodes))
		return nil, fmt.Errorf("not enough nodes found for pool")
	}

	nodeMappings, err := a.SE.AssignTwoNodesToTwoGroups(ctx, nodes[0], nodes[1], input.MaxNodesPerGroup)
	if err != nil {
		logger.Errorf("Failed to assign nodes to groups for pool ID %d: %v", input.PoolID, err)
		return nil, fmt.Errorf("error assigning nodes to groups for pool ID %d: %w", input.PoolID, err)
	}

	if len(nodeMappings) < 2 {
		logger.Errorf("Node group assignment returned insufficient mappings for pool ID %d", input.PoolID)
		return nil, fmt.Errorf("node group assignment returned insufficient mappings for pool")
	}

	logger.Infof("Successfully registered and assigned nodes %d and %d to groups", nodes[0].ID, nodes[1].ID)
	return nodeMappings, nil
}

// UploadHarvestTemplateActivity handles uploading the rendered template as YAML via REST
// Now supports dependency injection for template functions
type UploadHarvestTemplateActivity struct {
	LoadHarvestTemplateFunc   func() (string, error)
	RenderHarvestTemplateFunc func(*datamodel.HarvestConfig) (string, error)
}

// UploadHarvestTemplateInput holds the input for the upload activity
type UploadHarvestTemplateInput struct {
	NodeMappings []*datamodel.NodeNodeGroupMap
	UploadURL    string
}

// uploadYAMLFile uploads the given YAML content as a file via HTTP POST multipart/form-data
func uploadYAMLFile(ctx context.Context, url string, yamlContent string) (*http.Response, error) {
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)
	part, err := writer.CreateFormFile("file", "harvest.yaml")
	if err != nil {
		return nil, errors.New("failed to create form file: " + err.Error())
	}
	_, err = part.Write([]byte(yamlContent))
	if err != nil {
		return nil, errors.New("failed to write YAML content: " + err.Error())
	}
	if err := writer.Close(); err != nil {
		return nil, errors.New("failed to close multipart writer: " + err.Error())
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, &buf)
	if err != nil {
		return nil, errors.New("failed to create HTTP request: " + err.Error())
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, errors.New("failed to execute HTTP request: " + err.Error())
	}
	return resp, nil
}

// UploadHarvestTemplate uploads the rendered template as a YAML file via REST call for each node mapping
func (a *UploadHarvestTemplateActivity) UploadHarvestTemplate(ctx context.Context, input UploadHarvestTemplateInput) error {
	logger := util.GetLogger(ctx)

	renderFunc := a.RenderHarvestTemplateFunc
	if renderFunc == nil {
		renderFunc = utils.RenderHarvestTemplate
	}

	for i, mapping := range input.NodeMappings {
		if mapping == nil {
			logger.Errorf("NodeNodeGroupMap is nil at index %d", i)
			return errors.New("invalid node mapping: nil mapping")
		}
		if mapping.HarvestConfig == nil {
			logger.Errorf("HarvestConfig is nil for node mapping at index %d", i)
			return errors.New("invalid node mapping: nil HarvestConfig")
		}
		tmplStr, err := renderFunc(mapping.HarvestConfig)
		if err != nil {
			logger.Errorf("Failed to render template for node mapping %d: %v", i, err)
			return errors.New("template render failed for node mapping: " + err.Error())
		}
		resp, err := uploadYAMLFile(ctx, input.UploadURL, tmplStr)
		if err != nil {
			logger.Errorf("Failed to upload YAML template for node mapping %d: %v", i, err)
			return errors.New("upload failed for node mapping: " + err.Error())
		}
		if resp == nil {
			logger.Errorf("No response received for node mapping %d", i)
			return errors.New("no response received from upload endpoint")
		}

		// Always read and close the response body to avoid resource leaks
		_, readErr := io.ReadAll(resp.Body)
		closeErr := resp.Body.Close()
		if readErr != nil {
			logger.Warnf("Failed to read response body for node mapping %d: %v", i, readErr)
		}
		if closeErr != nil {
			logger.Errorf("Failed to close response body for node mapping %d: %v", i, closeErr)
		} else {
			logger.Infof("Closed response body for node mapping %d", i)
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			logger.Errorf("Upload failed for node mapping %d with status: %s", i, resp.Status)
			return errors.New("upload failed: " + resp.Status)
		}
		logger.Infof("Successfully uploaded rendered template for node mapping %d as YAML file", i)
	}
	return nil
}
