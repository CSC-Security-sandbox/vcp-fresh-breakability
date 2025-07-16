package activities

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"io"
	"mime/multipart"
	"net/http"
)

const (
	leasePrefix = "harvest-"
)

var (
	createKubernetesLease = utils.CreateKubernetesLease
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

	nodeMappings, err := a.SE.AssignTwoNodesToTwoGroups(ctx, datamodel.NodeGroupAssignmentParams{
		Node1:            nodes[0],
		Node2:            nodes[1],
		MaxNodesPerGroup: input.MaxNodesPerGroup,
		CustomerProject:  input.CustomerProjectID,
		TenantProject:    input.TenantProjectID,
	})
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

func (a *RegisterNodeToHarvestFarmActivity) ValidateAndCreateKubernetesLease(ctx context.Context, nodeGroupsMap []*datamodel.NodeNodeGroupMap) error {
	logger := util.GetLogger(ctx)
	se := a.SE
	for _, nodeGroupMap := range nodeGroupsMap {
		nodeGroup := nodeGroupMap.NodeGroup
		if nodeGroup == nil {
			logger.Errorf("Failed to fetch node group details of node groupID:%s", nodeGroupMap.NodeGroupID)
			return errors.New("failed to fetch node group details from nodeGroup Map table")
		}
		if nodeGroup.LeaseName == "" {
			logger.Infof("Creating new k8's lease for node group: %d", nodeGroup.ID)
			leaseName := leasePrefix + nodeGroup.UUID
			err := createKubernetesLease(ctx, vcpLeaseNameSpace, leaseName)
			if err != nil {
				logger.Errorf("Failed to create k8s lease for node group %d: %v", nodeGroup.ID, err)
				return err
			}
			nodeGroup.LeaseName = leaseName
			if _, err := se.UpdateNodeGroup(ctx, nodeGroup); err != nil {
				logger.Errorf("Failed to update k8s lease info in DB for node group %d: %v", nodeGroup.ID, err)
				return err
			}
		}
	}
	return nil
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

// UploadYAMLFileInput is the input struct for uploadYAMLFile
type UploadYAMLFileInput struct {
	URL       string
	YAML      string
	LeaseName string
	NodeID    int64
}

// uploadYAMLFile uploads the given YAML content as a file via HTTP POST multipart/form-data
func uploadYAMLFile(ctx context.Context, input UploadYAMLFileInput) (*http.Response, error) {
	var buf bytes.Buffer
	logger := util.GetLogger(ctx)
	writer := multipart.NewWriter(&buf)
	fileName := fmt.Sprintf("%s%d.yaml", leasePrefix, input.NodeID) // Append nodeID to the file name
	part, err := writer.CreateFormFile("file", fileName)
	if err != nil {
		return nil, errors.New("failed to create form file: " + err.Error())
	}
	_, err = part.Write([]byte(input.YAML))
	logger.Debugf("Uploading YAML content %s", input.YAML)
	if err != nil {
		return nil, errors.New("failed to write YAML content: " + err.Error())
	}
	// Add leaseName as a form field
	if input.LeaseName != "" {
		err = writer.WriteField("leaseName", input.LeaseName)
		if err != nil {
			return nil, errors.New("failed to write leaseName field: " + err.Error())
		}
	}
	if err := writer.Close(); err != nil {
		return nil, errors.New("failed to close multipart writer: " + err.Error())
	}

	req, err := http.NewRequestWithContext(ctx, "POST", input.URL, &buf)
	if err != nil {
		logger.Errorf("Failed to create HTTP request for uploading YAML file: %v", err)
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
		// Fail if NodeGroup is nil or LeaseName is empty
		if mapping.NodeGroup == nil || mapping.NodeGroup.LeaseName == "" {
			logger.Errorf("NodeGroup is nil or LeaseName is empty for node mapping at index %d. Upload cannot proceed.", i)
			return errors.New("invalid node mapping: NodeGroup is nil or LeaseName is empty; cannot upload YAML")
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
		// Only access LeaseName if NodeGroup is not nil
		leaseName := ""
		if mapping.NodeGroup != nil {
			leaseName = mapping.NodeGroup.LeaseName
		}
		resp, err := uploadYAMLFile(ctx, UploadYAMLFileInput{
			URL:       input.UploadURL,
			YAML:      tmplStr,
			LeaseName: leaseName,
			NodeID:    mapping.NodeID,
		})
		if err != nil {
			logger.Errorf("Failed to upload YAML template for node mapping %d: %v", i, err)
			return errors.New("upload failed for node mapping: " + err.Error())
		}
		if resp == nil {
			logger.Errorf("No response received for node mapping %d", i)
			return errors.New("no response received from upload endpoint")
		}

		// Always read and close the response body to avoid resource leaks
		body, readErr := io.ReadAll(resp.Body)
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
			logger.Errorf("Upload failed for node mapping %d with status: %s, response: %s", i, resp.Status, string(body))
			return fmt.Errorf("upload failed for node mapping %d: %s", i, resp.Status)
		}
		logger.Infof("Successfully uploaded rendered template for node mapping %d as YAML file", i)
	}
	return nil
}
