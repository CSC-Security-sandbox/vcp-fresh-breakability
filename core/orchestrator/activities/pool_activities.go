package activities

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/hyperscaler/google"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/vlm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"gorm.io/gorm"
	"netapp.com/vsa/lifecycle-manager/pkg/vlmconfig"
)

var (
	GetProviderByNode           = _getProviderByNode
	FindTenancyAndGetSubnetwork = _findTenancyAndGetSubnetwork
	SetupNetwork                = common.SetupNetwork
	DeploymentsInsert           = common.DeploymentsInsert
	PrepareVlmConfig            = _prepareVlmConfig
	ReadFile                    = os.ReadFile
	GetVLMClient                = _getVLMClient
	SaveNodeDetails             = _saveNodeDetails
	DeleteLIFs                  = _deleteLIFs
	DeleteSVMs                  = _deleteSVMs
	DeleteNodes                 = _deleteNodes
	DeletingNodes               = _deletingNodes
	DeletingSVMs                = _deletingSVMs
)

const defaultServiceAccountPattern = "-compute@developer.gserviceaccount.com"

type PoolActivity struct {
	SE database.Storage
}

const (
	aggregateName  = "dataaggr_01"
	defaultSvmName = "gcnv-default-svm"
	lifNameFormat  = "san_lif_%s"
	enableIscsi    = true
)

var (
	homePort = env.GetString("VSA_NODE_HOME_PORT", "e0e")
	region   = env.GetString("REGION", "")
)

func (j *PoolActivity) CreatingPool(ctx context.Context, pool *datamodel.Pool) (*datamodel.Pool, error) {
	se := j.SE
	return se.CreatingPool(ctx, pool)
}

func (j *PoolActivity) FailedPool(ctx context.Context, pool *datamodel.Pool, errMessage string) error {
	se := j.SE
	pool.State = models.LifeCycleStateError
	pool.StateDetails = errMessage
	return se.UpdatePool(ctx, pool)
}

func (j *PoolActivity) CreatedPool(ctx context.Context, pool *datamodel.Pool) (*datamodel.Pool, error) {
	se := j.SE
	return se.CreatedPool(ctx, pool)
}

func (j *PoolActivity) CreateTenancy(ctx context.Context, params commonparams.CreatePoolParams) (*commonparams.TenancyInfo, error) {
	tenancy, err := FindTenancyAndGetSubnetwork(ctx, params.VendorSubNetID, params.AccountName, &params.Region)
	if err != nil {
		return nil, err
	}
	return tenancy, nil
}

// _findTenancyAndGetSubnetwork finds the tenancy unit and creates a subnetwork for the tenant project
func _findTenancyAndGetSubnetwork(ctx context.Context, consumerVPC string, customerProjectNumber string, tenantProjectRegion *string) (*commonparams.TenancyInfo, error) {
	logger := util.GetLogger(ctx)
	// need to pass tenantProjectRegion only in case of CBR where region != the regional region as set from env variable
	var gService hyperscaler.GoogleServices
	gcpService := &google.GcpServices{
		Ctx:    ctx,
		Logger: logger,
	}
	gService = gcpService

	gcpService.Logger.Debug("gcpService initialized")

	if tenantProjectRegion == nil {
		tenantProjectRegion = &region
	}
	gcpService.Logger.Debug("Calling InitializeClients")
	err := gService.InitializeClients()
	if err != nil || !gService.IsAdminClientInitialized() {
		gcpService.Logger.Debug("Initialisation of service failed")
		return nil, errors.New("initialisation of service failed")
	}

	tenantProjectNumber, err := gService.GetTenantProject(consumerVPC, customerProjectNumber, *tenantProjectRegion)
	if err != nil {
		gcpService.Logger.Errorf("Error finding tenancy unit: %v", err)
		return nil, err
	}
	subnet, err := gService.CreateSubnetwork(consumerVPC, *tenantProjectRegion, tenantProjectNumber)
	if err != nil {
		gcpService.Logger.Errorf("Error adding subnetwork: %v", err)
		return nil, err
	}
	snHostProject, network, err := utils.ParseProjectId(subnet.Network)
	if err != nil {
		return nil, err
	}
	subnetwork, err := gcpService.GetSubnetwork(snHostProject, *tenantProjectRegion, subnet.Name)
	if err != nil {
		return nil, err
	}
	gcpService.Logger.Errorf("FindTenancyAndGetSubnetwork: tenantProjectNumber :  %s subnet  :  %s   ", &tenantProjectNumber, subnet)

	return &commonparams.TenancyInfo{
		RegionalTenantProject: tenantProjectNumber,
		Network:               network,
		SubnetworkName:        subnet.Name,
		SnHostProject:         snHostProject,
		Gateway:               subnetwork.GatewayAddress,
	}, nil
}

func (j *PoolActivity) DeployDeploymentManager(ctx context.Context, deploymentName, region, zone, network, subnet, projectId, snHostProject string, size int) (*[]map[string]string, error) {
	return DeploymentsInsert(ctx, deploymentName, region, zone, network, subnet, projectId, snHostProject, size)
}

func (j *PoolActivity) SetupNetwork(ctx context.Context, region, network, projectId, snHostProject string) error {
	return SetupNetwork(ctx, projectId, snHostProject, network, region)
}

func (j *PoolActivity) SavePoolWithClusterDetails(ctx context.Context, dbPool *datamodel.Pool, cluster *datamodel.ClusterDetails) error {
	se := j.SE
	return se.SavePoolWithVsaClusterDetails(ctx, dbPool, cluster)
}

func _getProviderByNode(node *models.Node) vsa.Provider {
	// as we don't have any other provider, we can directly return the ontap_rest provider
	return vsa.NewProvider(vsa.ProviderDetails{
		IPAddress: node.EndpointAddress,
		UserName:  node.Username,
		Password:  node.Password,
		// TODO : need to fix once we have certs
		InsecureSkipVerify: true,
	})
}

func _getVLMClient(ctx context.Context, logger log.Logger, vlmConfig *vlmconfig.VLMConfig) vlm.ClientFactory {
	return vlm.NewClient(ctx, logger, vlmConfig)
}

func (j *PoolActivity) GetOntapVersion(ctx context.Context, node *models.Node) (*string, error) {
	provider := GetProviderByNode(node)
	return provider.GetONTAPVersion()
}

func (j *PoolActivity) CreateVSASVM(ctx context.Context, pool *datamodel.Pool, vlmConfig *vlmconfig.VLMConfig) error {
	logger := util.GetLogger(ctx)
	vlmClient := GetVLMClient(ctx, logger, vlmConfig)
	se := j.SE
	svmParam := &vlmconfig.SVMConfigParams{
		Name:      defaultSvmName,
		VlmConfig: vlmConfig,
	}
	err := vlmClient.VSASVMCreate(ctx, svmParam)
	if err != nil {
		return err
	}
	name := vlmConfig.Deployment.DeploymentID + "-datasvm-" + defaultSvmName
	svm := vlmConfig.Svm[name]

	svmRec := &datamodel.Svm{
		Name:      svm.Svmname,
		AccountID: pool.AccountID,
		PoolID:    pool.ID,
		SvmDetails: &datamodel.SvmDetails{
			ExternalUUID: svm.Svmuuid,
			IPSpace:      "Default",
		},
	}
	if _, err = se.CreateSVM(ctx, svmRec); err != nil {
		return err
	}

	nodes, err := se.GetNodesByPoolID(ctx, pool.ID)
	if err != nil {
		return err
	}
	if len(nodes) < 2 {
		return errors.New("not enough nodes in the cluster to create LIFs for SVM " + svm.Svmname)
	}
	lifs := svm.SVMLIFs[vlmconfig.LIFTypeIscsi]

	for i, lif := range lifs {
		dataLif := lif.IP
		ip := strings.Split(dataLif, "/")[0]
		lifRec := &datamodel.Lif{
			Name:       lif.Name,
			AccountID:  pool.AccountID,
			NodeID:     nodes[i].ID,                             // FIXME : need to get the node name from the lif object - VLM changes
			LifDetails: &datamodel.LifDetails{ExternalUUID: ""}, // FIXME : = need to get the external UUID from the lif object - VLM changes
			IPAddress:  ip,
			SubnetMask: vsa.DefaultNetmask,
		}
		if _, err = se.CreateLif(ctx, lifRec); err != nil {
			return err
		}
	}

	return nil
}

func (j *PoolActivity) CreateVSACluster(ctx context.Context, deploymentName, region, zone, network, subnet, projectId, snHostProject string, size int) (*vlmconfig.VLMConfig, error) {
	logger := util.GetLogger(ctx)
	cfg := &vlmconfig.VLMConfig{}
	err := PrepareVlmConfig(cfg, deploymentName, region, zone, network, subnet, projectId, snHostProject)
	if err != nil {
		return nil, err
	}
	vlmClient := GetVLMClient(ctx, logger, cfg)

	err = vlmClient.VSAClusterDeployCreate(ctx, cfg)
	if err != nil {
		logger.Error("Failed to create VSA cluster", "error", err)
		return nil, err
	}
	return cfg, nil
}

func assignNetworkConfig(cfg *vlmconfig.VLMConfig, lifType vlmconfig.VSALIFType, vpc, subnet, subnetProjectID string) {
	cfg.Deployment.NetConfig[lifType] = vlmconfig.NetworkConfig{
		VPC:              vpc,
		Subnet:           subnet,
		GCPNetworkConfig: vlmconfig.GCPNetworkConfig{SubnetProjectID: subnetProjectID},
	}
}

func _prepareVlmConfig(cfg *vlmconfig.VLMConfig, deploymentName, region, zone, network, subnet, projectId, snHostProject string) error {
	vlmContent, err := ReadFile("common/vsa_config/vlm-config.json")
	if err != nil {
		return err
	}
	err = json.Unmarshal(vlmContent, cfg)
	if err != nil {
		return err
	}
	cfg.Deployment.DeploymentID = deploymentName
	cfg.Deployment.DeploymentName = deploymentName
	cfg.Deployment.Zone = vlmconfig.ZoneInfo{
		Zone1: zone,
		Zone2: zone,
	}
	cfg.Deployment.Region = region

	networkConfigs := map[vlmconfig.VSALIFType]struct {
		VPC             string
		Subnet          string
		SubnetProjectID string
	}{
		vlmconfig.LIFTypeNodeMgmt: {"mgmt-vpc", "mgmt-subnet", projectId},
		vlmconfig.LIFTypeIC:       {"cluster-ic-vpc", "cluster-ic-subnet", projectId},
		vlmconfig.LIFTypeRSM:      {"rsm-vpc", "rsm-subnet", projectId},
	}

	// assign network configurations for each LIF type
	for lifType, config := range networkConfigs {
		assignNetworkConfig(cfg, lifType, config.VPC, config.Subnet, config.SubnetProjectID)
	}

	// assign network configuration for data LIF from snHostProject
	assignNetworkConfig(cfg, vlmconfig.LIFTypeInterCluster, network, subnet, snHostProject)

	svcAccount := projectId + defaultServiceAccountPattern // FIXME : need to to discuss on what service account to be passed

	cfg.Deployment.GCPConfig = vlmconfig.GCPConfig{
		ProjectID:           projectId,
		ImageProjectID:      projectId,
		ServiceAccountEmail: svcAccount,
	}

	cfg.Deployment.OntapCredentials.Username = env.GetString("VSA_NODE_USERNAME", "")
	cfg.Deployment.OntapCredentials.Password = env.GetString("VSA_NODE_PASSWORD", "")

	return nil
}

func (j *PoolActivity) DeleteVSADeployment(ctx context.Context, pool *datamodel.Pool) (*datamodel.Pool, error) {
	if pool.ClusterDetails.ExternalName == "" {
		return nil, errors.New("pool cannot be deleted with active clusters")
	}
	deploymentName := pool.ClusterDetails.ExternalName
	se := j.SE
	node, err := se.GetNodesByPoolID(ctx, pool.ID)
	if err != nil {
		return nil, err
	}

	logger := util.GetLogger(ctx)
	cfg := &vlmconfig.VLMConfig{}
	err = PrepareVlmConfig(cfg, deploymentName, region, node[0].ZoneName, pool.ClusterDetails.Network, "vsa-"+region, pool.ClusterDetails.RegionalTenantProject, pool.ClusterDetails.SnHostProject)
	if err != nil {
		return nil, err
	}
	vlmClient := GetVLMClient(ctx, logger, cfg)
	err = vlmClient.VSAClusterDeploymentDelete(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return pool, nil
}

func (j *PoolActivity) SaveVSANodeDetails(ctx context.Context, pool *datamodel.Pool, vlmConfig *vlmconfig.VLMConfig) (node1 *datamodel.Node, err error) {
	if len(vlmConfig.Cloud.HAPairs) == 0 {
		return nil, errors.New("no cluster details provided")
	}
	for _, details := range vlmConfig.Cloud.HAPairs {
		node1, err = SaveNodeDetails(ctx, j.SE, details.VM1, vlmConfig.Deployment, pool)
		if err != nil {
			return nil, err
		}
		_, err = SaveNodeDetails(ctx, j.SE, details.VM2, vlmConfig.Deployment, pool)
		if err != nil {
			return nil, err
		}
	}
	return node1, nil
}

func _saveNodeDetails(ctx context.Context, se database.Storage, vmConfig vlmconfig.VMConfig, deploymentConfig vlmconfig.DeploymentConfig, pool *datamodel.Pool) (*datamodel.Node, error) {
	node := &models.Node{
		Name:            vmConfig.HostName,
		EndpointAddress: vmConfig.SystemLIFs[vlmconfig.LIFTypeNodeMgmt].IP,
		Username:        deploymentConfig.OntapCredentials.Username,
		Password:        deploymentConfig.OntapCredentials.Password,
		Zone:            vmConfig.Zone,
		InstanceType:    deploymentConfig.VSAInstanceType,
	}
	provider := GetProviderByNode(node)

	vsaNode, err := provider.GetNodeByName(node.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to get node %s: %w", node.Name, err)
	}
	rec := &datamodel.Node{
		Name:            node.Name,
		EndpointAddress: node.EndpointAddress,
		PoolID:          pool.ID,
		State:           models.LifeCycleStateAvailable,
		StateDetails:    models.LifeCycleStateAvailableDetails,
		NodeAttributes:  &datamodel.NodeDetails{ExternalUUID: vsaNode.ExternalUUID, InstanceType: node.InstanceType},
		ZoneName:        node.Zone,
	}
	if _, err = se.CreateNode(ctx, rec); err != nil {
		return nil, err
	}
	return rec, nil
}

func (j *PoolActivity) CreateLifForSvm(ctx context.Context, node *models.Node, cluster []map[string]string, pool *datamodel.Pool, svm *datamodel.Svm) error {
	provider := GetProviderByNode(node)
	se := j.SE
	nodes, err := se.GetNodesByPoolID(ctx, pool.ID)
	if err != nil {
		return err
	}
	if len(nodes) < 2 {
		return errors.New("not enough nodes in the cluster to create LIFs for SVM " + svm.Name)
	}

	for i, node := range nodes {
		dataLif, ok := cluster[i]["dataLif"]
		if !ok {
			return fmt.Errorf("missing dataLif in cluster details for node index %d", i)
		}
		ip := strings.Split(dataLif, "/")[0]
		lifName := fmt.Sprintf(lifNameFormat, node.Name)
		lifResponse, err := provider.CreateDataLIF(vsa.CreateLifParams{Name: lifName, SvmName: svm.Name, IpAddress: ip, NodeName: node.Name, HomePort: homePort})
		if err != nil {
			return err
		}
		lifRec := &datamodel.Lif{
			Name:       lifResponse.Name,
			AccountID:  pool.AccountID,
			NodeID:     node.ID,
			LifDetails: &datamodel.LifDetails{ExternalUUID: lifResponse.ExternalUUID},
			IPAddress:  lifResponse.IPAddress,
			SubnetMask: lifResponse.SubnetMask,
		}
		if _, err = se.CreateLif(ctx, lifRec); err != nil {
			return err
		}
	}
	return nil
}

func (j *PoolActivity) GetPool(ctx context.Context, pool *datamodel.Pool) (*datamodel.Pool, error) {
	se := j.SE
	return se.GetPool(ctx, pool.UUID, 0)
}

func (j *PoolActivity) DeletingPoolResources(ctx context.Context, pool *datamodel.Pool) (*datamodel.Pool, error) {
	se := j.SE
	// Update SVM, and Pool States to Deleting
	if err := DeletingSVMs(ctx, se, pool); err != nil {
		return nil, err
	}

	if err := DeletingNodes(ctx, se, pool); err != nil {
		return nil, err
	}
	return pool, nil
}

func (j *PoolActivity) ReleaseSubnet(ctx context.Context, pool *datamodel.Pool) error {
	se := j.SE
	logger := util.GetLogger(ctx)
	conditions := [][]interface{}{{"account_id = ?", pool.AccountID}}
	conditions = append(conditions, []interface{}{"network = ?", pool.Network})
	pools, err := se.ListPools(ctx, conditions)
	if err != nil {
		return err
	}
	if len(pools) > 1 {
		logger.Info("Skipping release subnetwork as there are other pools in the same region for the account")
		return nil
	}
	var gService hyperscaler.GoogleServices
	gcpService := &google.GcpServices{
		Ctx:    ctx,
		Logger: logger,
	}
	gService = gcpService

	gcpService.Logger.Debug("gcpService initialized")

	gcpService.Logger.Debug("Calling InitializeClients")
	err = gService.InitializeClients()
	if err != nil || !gService.IsAdminClientInitialized() {
		gcpService.Logger.Debug("Initialisation of service failed")
		return errors.New("initialisation of service failed")
	}

	consumerVpc := pool.Network
	accountName := pool.Account.Name
	subnetwork := "vsa-" + region

	tenantProjectNumber, err := gService.GetTenantProject(consumerVpc, accountName, region)
	if err != nil {
		gcpService.Logger.Errorf("Error finding tenancy unit: %v", err)
		return err
	}

	err = gService.ReleaseSubnetwork(region, tenantProjectNumber, subnetwork)
	if err != nil {
		gcpService.Logger.Errorf("Error Releasing subnetwork: %v", err)
		return err
	}
	return nil
}

func (j *PoolActivity) DeletePoolResources(ctx context.Context, pool *datamodel.Pool) (*datamodel.Pool, error) {
	se := j.SE

	// Delete LIFs
	if err := DeleteLIFs(ctx, se, pool); err != nil {
		return nil, err
	}

	// Delete SVMs
	if err := DeleteSVMs(ctx, se, pool); err != nil {
		return nil, err
	}

	// Delete nodes
	if err := DeleteNodes(ctx, se, pool); err != nil {
		return nil, err
	}

	// Delete the pool itself from a database
	if err := se.DeletePool(ctx, pool); err != nil {
		return nil, err
	}
	return pool, nil
}

// deletingSVMs updates svm status to deleting.
func _deletingSVMs(ctx context.Context, se database.Storage, pool *datamodel.Pool) error {
	// Retrieve the svms associated with the pool
	svms, err := se.GetSvmsByPoolID(ctx, pool.ID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return errors.New("SVM not found")
		}
		return err
	}
	for _, svm := range svms {
		if err = se.DeletingSVM(ctx, svm); err != nil {
			return fmt.Errorf("failed to update SVM record to deleting %s: %w", svm.Name, err)
		}
	}

	return nil
}

// deletingNodes updates nodes status to deleting.
func _deletingNodes(ctx context.Context, se database.Storage, pool *datamodel.Pool) error {
	// Retrieve the nodes associated with the pool
	nodes, err := se.GetNodesByPoolID(ctx, pool.ID)
	if err != nil {
		return fmt.Errorf("failed to retrieve nodes for pool %d: %w", pool.ID, err)
	}

	// Delete each node
	for _, node := range nodes {
		// Delete the node record from the database
		if err := se.DeletingNode(ctx, node); err != nil {
			return fmt.Errorf("failed to delete node record %s: %w", node.Name, err)
		}
	}
	return nil
}

// deleteSVMs deletes all SVMs and their associated database records.
func _deleteSVMs(ctx context.Context, se database.Storage, pool *datamodel.Pool) error {
	// Get SVMs by pool ID
	svms, err := se.GetSvmsByPoolID(ctx, pool.ID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return errors.New("SVM not found")
		}
		return err
	}

	for _, svm := range svms {
		// Delete the SVM record from the database
		if svm.DeletedAt != nil && svm.DeletedAt.Valid {
			continue
		}
		if err := se.DeleteSVM(ctx, svm); err != nil {
			return fmt.Errorf("failed to delete SVM record %s: %w", pool.Name, err)
		}
	}
	return nil
}

// _deleteLIFs deletes LIFs database records associated with the given Nodes.
func _deleteLIFs(ctx context.Context, se database.Storage, pool *datamodel.Pool) error {
	// Retrieve the nodes associated with the pool
	nodes, err := se.GetNodesByPoolID(ctx, pool.ID)
	if err != nil {
		return fmt.Errorf("failed to retrieve nodes for pool %d: %w", pool.ID, err)
	}

	// Delete each LIF
	for _, node := range nodes {
		// Retrieve the LIFs associated with the Node
		lif, err := se.GetLifByNodeID(ctx, node.ID, node.AccountID)
		if err != nil {
			return fmt.Errorf("failed to retrieve LIFs for Node %s: %w", node.Name, err)
		}

		if lif.DeletedAt != nil && lif.DeletedAt.Valid {
			continue
		}

		// Delete the LIF record from the database
		if err := se.DeleteLif(ctx, lif); err != nil {
			return fmt.Errorf("failed to delete LIF record %s: %w", lif.Name, err)
		}
	}

	return nil
}

// deleteNodes deletes node database records associated with the given pool.
func _deleteNodes(ctx context.Context, se database.Storage, pool *datamodel.Pool) error {
	// Retrieve the nodes associated with the pool
	nodes, err := se.GetNodesByPoolID(ctx, pool.ID)
	if err != nil {
		return fmt.Errorf("failed to retrieve nodes for pool %d: %w", pool.ID, err)
	}

	// Delete each node
	for _, node := range nodes {
		// Delete the node record from the database
		if err := se.DeleteNode(ctx, node); err != nil {
			return fmt.Errorf("failed to update node record to deleting %s: %w", node.Name, err)
		}
	}

	return nil
}
