package activities

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strconv"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/vlm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/worker/metrics"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"gorm.io/gorm"
)

const (
	leasePrefix = "harvest-"
)

var (
	createKubernetesLease = utils.CreateKubernetesLease
	leaseExists           = utils.LeaseExists
	vcpLeaseNameSpace     = env.GetString("LEASE_NAMESPACE", "vcp")
	smHarvestAuthEnabled  = env.GetBool("HARVEST_SECRET_MANAGER_AUTH_ENABLED", false)
)

// RegisterNodeToHarvestFarmInput holds input parameters for the activity
type RegisterNodeToHarvestFarmInput struct {
	PoolID           int64
	MaxNodesPerGroup int

	CustomerProjectID string
	TenantProjectID   string
	DeploymentName    string
	PoolName          string
	IsRegionalHA      bool
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
		return nil, vsaerrors.ExtractCustomError(fmt.Errorf("error fetching nodes for pool ID %d: %w", input.PoolID, err))
	}

	if len(nodes) < 2 {
		logger.Errorf("Not enough nodes found for pool ID %d: got %d", input.PoolID, len(nodes))
		return nil, vsaerrors.ExtractCustomError(fmt.Errorf("not enough nodes found for pool"))
	}

	nodeMappings, err := a.SE.AssignTwoNodesToTwoGroups(ctx, datamodel.NodeGroupAssignmentParams{
		Node1:            nodes[0],
		Node2:            nodes[1],
		MaxNodesPerGroup: input.MaxNodesPerGroup,
		CustomerProject:  input.CustomerProjectID,
		TenantProject:    input.TenantProjectID,
		DeploymentName:   input.DeploymentName,
		PoolName:         input.PoolName,
		IsRegionalHA:     input.IsRegionalHA,
	})
	if err != nil {
		logger.Errorf("Failed to assign nodes to groups for pool ID %d: %v", input.PoolID, err)
		return nil, vsaerrors.ExtractCustomError(fmt.Errorf("error assigning nodes to groups for pool ID %d: %w", input.PoolID, err))
	}

	if len(nodeMappings) < 2 {
		logger.Errorf("Node group assignment returned insufficient mappings for pool ID %d", input.PoolID)
		return nil, vsaerrors.ExtractCustomError(fmt.Errorf("node group assignment returned insufficient mappings for pool"))
	}

	logger.Infof("Successfully registered and assigned nodes %d and %d to groups", nodes[0].ID, nodes[1].ID)
	return nodeMappings, nil
}

func (a *RegisterNodeToHarvestFarmActivity) ValidateAndCreateKubernetesLease(ctx context.Context, nodeGroupsMap []*datamodel.NodeNodeGroupMap) ([]*datamodel.NodeNodeGroupMap, error) {
	logger := util.GetLogger(ctx)

	for _, nodeGroupMap := range nodeGroupsMap {
		if err := a.validateNodeGroupMap(nodeGroupMap, logger); err != nil {
			return nil, err
		}

		nodeGroup := nodeGroupMap.NodeGroup
		wasNewLease := nodeGroup.LeaseName == ""

		if err := a.ensureLeaseExists(ctx, nodeGroup, vcpLeaseNameSpace, logger); err != nil {
			return nil, err
		}

		// Ensure HarvestConfig has the lease name set
		nodeGroupMap.HarvestConfig.LEASE_NAME = nodeGroup.LeaseName

		// Update database records only if lease was newly created
		if wasNewLease {
			if err := a.updateDatabaseRecords(ctx, nodeGroup, nodeGroupMap, logger); err != nil {
				return nil, err
			}
		}
	}
	return nodeGroupsMap, nil
}

// validateNodeGroupMap validates the node group mapping structure
func (a *RegisterNodeToHarvestFarmActivity) validateNodeGroupMap(nodeGroupMap *datamodel.NodeNodeGroupMap, logger log.Logger) error {
	if nodeGroupMap.NodeGroup == nil {
		logger.Errorf("Failed to fetch node group details of node groupID:%s", nodeGroupMap.NodeGroupID)
		return errors.New("failed to fetch node group details from nodeGroup Map table")
	}
	return nil
}

// ensureLeaseExists ensures the Kubernetes lease exists, creating it if necessary
func (a *RegisterNodeToHarvestFarmActivity) ensureLeaseExists(ctx context.Context, nodeGroup *datamodel.NodeGroup, vcpLeaseNameSpace string, logger log.Logger) error {
	if nodeGroup.LeaseName == "" {
		return a.createNewLease(ctx, nodeGroup, vcpLeaseNameSpace, logger)
	}
	return a.validateExistingLease(ctx, nodeGroup, vcpLeaseNameSpace, logger)
}

// createNewLease creates a new lease for a node group that doesn't have one
func (a *RegisterNodeToHarvestFarmActivity) createNewLease(ctx context.Context, nodeGroup *datamodel.NodeGroup, vcpLeaseNameSpace string, logger log.Logger) error {
	logger.Infof("Creating new k8's lease for node group: %d", nodeGroup.ID)

	leaseName := leasePrefix + nodeGroup.UUID
	if err := createKubernetesLease(ctx, vcpLeaseNameSpace, leaseName); err != nil {
		logger.Errorf("Failed to create k8s lease for node group %d: %v", nodeGroup.ID, err)
		return err
	}

	nodeGroup.LeaseName = leaseName
	return nil
}

// validateExistingLease validates that an existing lease exists in Kubernetes, creating it if missing
func (a *RegisterNodeToHarvestFarmActivity) validateExistingLease(ctx context.Context, nodeGroup *datamodel.NodeGroup, vcpLeaseNameSpace string, logger log.Logger) error {
	logger.Infof("Checking if k8's lease %s exists for node group: %d", nodeGroup.LeaseName, nodeGroup.ID)

	exists, err := leaseExists(ctx, vcpLeaseNameSpace, nodeGroup.LeaseName)
	if err != nil {
		logger.Errorf("Failed to check k8s lease existence for node group %d: %v", nodeGroup.ID, err)
		return err
	}

	if !exists {
		logger.Infof("k8's lease %s doesn't exist in Kubernetes, creating it for node group: %d", nodeGroup.LeaseName, nodeGroup.ID)
		if err := createKubernetesLease(ctx, vcpLeaseNameSpace, nodeGroup.LeaseName); err != nil {
			logger.Errorf("Failed to create k8s lease %s for node group %d: %v", nodeGroup.LeaseName, nodeGroup.ID, err)
			return err
		}
		logger.Infof("Successfully created k8's lease %s for node group: %d", nodeGroup.LeaseName, nodeGroup.ID)
	} else {
		logger.Infof("k8's lease %s already exists for node group: %d", nodeGroup.LeaseName, nodeGroup.ID)
	}

	return nil
}

// updateDatabaseRecords updates the database records for node group and mapping
func (a *RegisterNodeToHarvestFarmActivity) updateDatabaseRecords(ctx context.Context, nodeGroup *datamodel.NodeGroup, nodeGroupMap *datamodel.NodeNodeGroupMap, logger log.Logger) error {
	if _, err := a.SE.UpdateNodeGroup(ctx, nodeGroup); err != nil {
		logger.Errorf("Failed to update k8s lease info in DB for node group %d: %v", nodeGroup.ID, err)
		return err
	}

	if _, err := a.SE.UpdateNodeNodeGroupMap(ctx, nodeGroupMap); err != nil {
		logger.Errorf("Failed to update k8s lease info in DB for node id %d: %v", nodeGroupMap.NodeID, err)
		return err
	}
	return nil
}

// AlertHarvestRegisterFailure logs error details to metrics for monitoring purposes
func (a *RegisterNodeToHarvestFarmActivity) AlertHarvestRegisterFailure(ctx context.Context, errorDetails string) error {
	metrics.IncJobStatusCounter(ctx, errorDetails, "failed to register nodes to harvest farm")
	return nil
}

// UploadHarvestTemplateActivity handles uploading the rendered template as YAML via REST
// Now supports dependency injection for template functions
type UploadHarvestTemplateActivity struct {
	SE                        database.Storage
	LoadHarvestTemplateFunc   func() (string, error)
	RenderHarvestTemplateFunc func(*datamodel.HarvestConfig) (string, error)
}

// UploadHarvestTemplateInput holds the input for the upload activity
type UploadHarvestTemplateInput struct {
	NodeMappings []*datamodel.NodeNodeGroupMap
	UploadURL    string
	PoolUUID     string
	AccountID    int64
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
	poolView, err := a.SE.GetPool(ctx, input.PoolUUID, input.AccountID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			logger.Errorf("Pool not found for UUID %s and AccountID %d", input.PoolUUID, input.AccountID)
			return temporal.NewNonRetryableApplicationError(err.Error(), "Pool Record not found", err)
		}
		logger.Errorf("Failed to fetch pool for UUID %s and AccountID %d: %v", input.PoolUUID, input.AccountID, err)
		return err
	}
	pool := database.ConvertPoolViewToPool(poolView)
	var credentials *vlm.OntapCredentials
	if !smHarvestAuthEnabled {
		credentials, err = fetchOnTapCredentials(ctx, pool)
		if err != nil {
			return err
		}
		if credentials == nil {
			logger.Errorf("Failed to get credentials for pool %d", pool.ID)
			return fmt.Errorf("failed to get credentials for pool %d", pool.ID)
		}
	}
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

		if !smHarvestAuthEnabled && credentials != nil {
			// Set password if credentials are provided
			mapping.HarvestConfig.PASSWORD = strconv.Quote(credentials.AdminPassword)
		} else {
			// Set Auth Type and SecretID info if smHarvestAuthEnabled env flag is set
			mapping.HarvestConfig.PASSWORD = ""
			mapping.HarvestConfig.AUTH_TYPE = pool.PoolCredentials.AuthType
			mapping.HarvestConfig.SECRET_ID = pool.PoolCredentials.SecretID
			mapping.HarvestConfig.SECRET_PROJECT = env.SecretManagerProjectID
		}

		// Update the database record with the possibly modified HarvestConfig
		// This ensures that any changes (like setting the password) are persisted
		// before we render and upload the template
		if _, err := a.SE.UpdateNodeNodeGroupMap(ctx, mapping); err != nil {
			logger.Errorf("Failed to update harvest config info in DB for node id %d: %v", mapping.NodeID, err)
			return err
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
