package activities

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/hyperscaler/google"
	hyperscaler_models "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/hyperscaler/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/vlm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/repository"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	utilErrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/activity"
	"google.golang.org/api/iam/v1"
	"google.golang.org/api/servicenetworking/v1"
	"gorm.io/gorm"
	"netapp.com/vsa/lifecycle-manager/pkg/vlmconfig"
)

var (
	GetProviderByNode                 = _getProviderByNode
	FindTenancyAndGetSubnetwork       = _findTenancyAndGetSubnetwork
	DeploymentsInsert                 = common.DeploymentsInsert
	PrepareVlmConfig                  = _prepareVlmConfig
	ReadFile                          = os.ReadFile
	GetVLMClient                      = _getVLMClient
	SaveNodeDetails                   = _saveNodeDetails
	DeleteLIFs                        = _deleteLIFs
	DeleteSVMs                        = _deleteSVMs
	DeleteNodes                       = _deleteNodes
	DeletingNodes                     = _deletingNodes
	DeletingSVMs                      = _deletingSVMs
	CreateVPC                         = _createVPC
	InsertSubnet                      = _insertSubnet
	InsertFirewall                    = _insertFirewall
	GetGCPService                     = _getGCPService
	NewGcpServices                    = _newGcpServices
	SetupNetworkWithFirewall          = setupNetworkWithFirewall
	SetupNetworkFirewallsForIscsi     = setupNetworkFirewallsForIscsi
	CreateGCPBucket                   = _createGCPBucket
	CreateServiceAccountAndAttachRole = _createServiceAccountAndAttachRole
	DeleteSrvcAccount                 = _deleteServiceAccount
	DeleteGCPBucket                   = _deleteGCPBucket
)

type PoolActivity struct {
	SE database.Storage
}

const (
	aggregateName  = "aggr1"
	defaultSvmName = "gcnv-default-svm"
	lifNameFormat  = "san_lif_%s"
	enableIscsi    = true

	firewallPriority        = 1000
	ingressTrafficDirection = "INGRESS"
)

var (
	maxRetries          = env.GetInt("GOOGLE_API_MAX_RETRIES", 6)
	localRegion         = env.GetString("REGION", "")
	firewallSourceRange = env.GetString("FIREWALL_SOURCE_RANGE", "10.0.0.0/8,172.16.0.0/12,192.168.0.0/16,34.0.0.0/8,46.149.16.0/20,52.94.203.152/29,52.94.203.160/29,185.35.244.0/22,202.3.112.0/20,216.240.16.0/20,217.70.208.0/20,198.18.0.0/15")
	homePort            = env.GetString("VSA_NODE_HOME_PORT", "e0e")
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
	pool, err := se.CreatedPool(ctx, pool)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	return pool, nil
}

func (j *PoolActivity) ErroredPool(ctx context.Context, pool *datamodel.Pool, errMessage string) (*datamodel.Pool, error) {
	se := j.SE

	// Delete LIFs
	if err := DeleteLIFs(ctx, se, pool); err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Delete SVMs
	if err := DeleteSVMs(ctx, se, pool); err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Delete nodes
	if err := DeleteNodes(ctx, se, pool); err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	pool.State = models.LifeCycleStateError
	pool.StateDetails = errMessage
	err := se.UpdatePool(ctx, pool)
	return pool, err
}

func (j *PoolActivity) CreateTenancy(ctx context.Context, params commonparams.CreatePoolParams) (*commonparams.TenancyInfo, error) {
	gcpService, err := GetGCPService(ctx)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	gcpService.Logger.Debug("gcpService initialized")

	tenancy, err := FindTenancyAndGetSubnetwork(ctx, gcpService, params.VendorSubNetID, params.AccountName, &params.Region)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return tenancy, nil
}

// _findTenancyAndGetSubnetwork finds the tenancy unit and creates a subnetwork for the tenant project
func _findTenancyAndGetSubnetwork(ctx context.Context, gcpService hyperscaler.GoogleServices, consumerVPC string, customerProjectNumber string, tenantProjectRegion *string) (*commonparams.TenancyInfo, error) {
	// need to pass tenantProjectRegion only in case of CBR where region != the regional region as set from env variable
	if tenantProjectRegion == nil {
		tenantProjectRegion = &localRegion
	}

	tenantProjectNumber, err := gcpService.GetTenantProject(consumerVPC, customerProjectNumber, *tenantProjectRegion)
	if err != nil {
		gcpService.GetLogger().Errorf("Error finding tenancy unit: %v", err)
		return nil, err
	}

	// check if subnet already exists
	subnetName := "vsa-" + *tenantProjectRegion
	subnetReceived, err := gcpService.GetSubnetwork(tenantProjectNumber, *tenantProjectRegion, subnetName)
	if err != nil {
		resourceNotFound, errReceived := resourceNotFoundCheck(err.Error(), tenantProjectNumber, "", subnetName, "")
		if !resourceNotFound {
			return nil, errReceived
		}
	}
	if subnetReceived != nil && subnetReceived.Name == subnetName {
		gcpService.GetLogger().Debug(fmt.Sprintf("Subnetwork %s already exists in tenant project %s and region %s. Won't create another subnet for this region", subnetName, tenantProjectNumber, *tenantProjectRegion))
		return nil, nil
	}
	subnetInBytes, err := gcpService.CreateSubnetworkForTenantProject(tenantProjectNumber, consumerVPC, *tenantProjectRegion)
	if err != nil {
		gcpService.GetLogger().Errorf("Error adding subnetwork: %v", err)
		return nil, err
	}
	subnet := &servicenetworking.Subnetwork{}
	gcpService.GetLogger().Debug(fmt.Sprintf("subnetInBytes %s", string(subnetInBytes)))

	if err := json.Unmarshal(subnetInBytes, subnet); err != nil {
		gcpService.GetLogger().Debug(fmt.Sprintf("subnetInBytes json unmarshal error %s", err.Error()))
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrJSONParsingError, err)
	}
	snHostProject, network, err := utils.ParseProjectId(subnet.Network)
	if err != nil {
		return nil, err
	}
	subnetwork, err := gcpService.GetSubnetwork(snHostProject, *tenantProjectRegion, subnet.Name)
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceFetchError, err)
	}
	gcpService.GetLogger().Debug(fmt.Sprintf("Subnet IpCidrRange %s, consumerPeeringNetwork %s, subnet %s", subnet.IpCidrRange, consumerVPC, subnet.Name))
	gcpService.GetLogger().Debug(fmt.Sprintf("FindTenancyAndGetSubnetwork: tenantProjectNumber :  %s subnet  :  %s IpCidrRange : %s, consumerPeeringNetwork : %s", tenantProjectNumber, subnet.Name, subnet.IpCidrRange, consumerVPC))

	return &commonparams.TenancyInfo{
		RegionalTenantProject: tenantProjectNumber,
		Network:               network,
		SubnetworkName:        subnet.Name,
		SnHostProject:         snHostProject,
		Gateway:               subnetwork.GatewayAddress,
	}, nil
}

// SetupNetwork TODO : need to add all network setup as part of network activity
// SetupNetwork sets up a VPC network, subnet, and firewall rules for 1st pool in GCP
func (j *PoolActivity) SetupNetwork(ctx context.Context, region, project, snHostProject, network string) error {
	mgmtVpcName := "mgmt-vpc"
	vpcSubnetMap := map[string]string{
		mgmtVpcName:        "mgmt-subnet",
		"cluster-ic-vpc":   "cluster-ic-subnet",
		"interconnect-vpc": "interconnect-subnet",
		"rsm-vpc":          "rsm-subnet",
	}

	serviceStruct, err := GetGCPService(ctx)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	service := hyperscaler.GoogleServices(serviceStruct)

	// Record heartbeat to indicate progress to temporal server
	activity.RecordHeartbeat(ctx, "Setting up network for VSA pool")

	i := 1
	for vpcName, subnetName := range vpcSubnetMap {
		firewallPortRules := []string{"tcp", "udp"}
		if vpcName == mgmtVpcName {
			firewallPortRules = []string{"tcp", "udp", "icmp"}
		}
		err = SetupNetworkWithFirewall(ctx, project, vpcName, &region, subnetName, fmt.Sprintf("198.18.%d.0/27", i*3), firewallPriority, ingressTrafficDirection, strings.Split(firewallSourceRange, ","), firewallPortRules)
		if err != nil {
			return vsaerrors.WrapAsTemporalApplicationError(err)
		}
		i++
	}

	// Record heartbeat to indicate progress to temporal server
	activity.RecordHeartbeat(ctx, "Setting up network firewalls for iSCSI")

	err = SetupNetworkFirewallsForIscsi(service, snHostProject, network)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return nil
}

// setupNetworkFirewallsForIscsi sets up a firewall for iSCSI traffic in GCP
func setupNetworkFirewallsForIscsi(service hyperscaler.GoogleServices, snHostProject, network string) error {
	err := InsertFirewall(service, snHostProject, "data-iscsi-ingress", network, firewallPriority, ingressTrafficDirection, []string{"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"}, []string{"tcp", "3260"})
	if err != nil {
		service.GetLogger().Error(fmt.Sprintf("Failed to setup network firewalls for iSCSI with error: %s", err.Error()))
		return err
	}
	return nil
}

func (j *PoolActivity) DeployDeploymentManager(ctx context.Context, deploymentName, region, zone, network, subnet, projectId, snHostProject string, size int) (*[]map[string]string, error) {
	return DeploymentsInsert(ctx, deploymentName, region, zone, network, subnet, projectId, snHostProject, size)
}

func (j *PoolActivity) SavePoolWithClusterDetails(ctx context.Context, dbPool *datamodel.Pool, cluster *datamodel.ClusterDetails) error {
	se := j.SE
	err := se.SavePoolWithVsaClusterDetails(ctx, dbPool, cluster)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	return nil
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
	version, err := provider.GetONTAPVersion()
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	return version, nil
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
	// If the SVM already exists, we can ignore the error and move forward
	if err != nil && !strings.Contains(err.Error(), "already exists and is in use by a different VM") {
		return vsaerrors.WrapAsTemporalApplicationError(err)
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
	if _, err = se.CreateSVM(ctx, svmRec); err != nil && !utilErrors.IsConflictErr(err) {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	nodes, err := se.GetNodesByPoolID(ctx, pool.ID)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	if len(nodes) < 2 {
		return vsaerrors.NewVCPError(vsaerrors.ErrIncorrectVSAClusterState, errors.New("not enough nodes in the cluster to create LIFs for SVM "+svm.Svmname))
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
		if _, err = se.CreateLif(ctx, lifRec); err != nil && !utilErrors.IsConflictErr(err) {
			return vsaerrors.WrapAsTemporalApplicationError(err)
		}
	}

	return nil
}

func (j *PoolActivity) CreateVSACluster(ctx context.Context, deploymentName, region, primaryZone, secondaryZone, network, subnet, projectId, snHostProject string, sizeInGiB int, throughputMibps, iops int64, saEmail string, autoTierBucket string) (*vlmconfig.VLMConfig, error) {
	logger := util.GetLogger(ctx)
	cfg := &vlmconfig.VLMConfig{}
	err := PrepareVlmConfig(cfg, deploymentName, region, primaryZone, secondaryZone, network, subnet, projectId, snHostProject, sizeInGiB, throughputMibps, iops, saEmail, autoTierBucket)

	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	vlmClient := GetVLMClient(ctx, logger, cfg)

	err = vlmClient.VSAClusterDeployCreate(ctx, cfg)
	if err != nil {
		logger.Error("Failed to create VSA cluster", "error", err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
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

func _prepareVlmConfig(cfg *vlmconfig.VLMConfig, deploymentName, region, primaryZone, secondaryZone, network, subnet, projectId, snHostProject string, sizeInGib int, throughputMibps, iops int64, saEmail string, autoTierBucket string) error {
	vlmContent, err := ReadFile("common/vsa_config/vlm-config.json")
	if err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrFileReadError, err)
	}
	err = json.Unmarshal(vlmContent, cfg)
	if err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrJSONParsingError, err)
	}
	cfg.Deployment.DeploymentID = deploymentName
	cfg.Deployment.DeploymentName = deploymentName

	cfg.Deployment.Zone.Zone1 = primaryZone
	cfg.Deployment.Zone.Zone2 = secondaryZone
	if secondaryZone == "" {
		cfg.Deployment.Zone.Zone2 = primaryZone
	}
	cfg.Deployment.Region = region

	cfg.Deployment.SPConfig.Throughput = throughputMibps
	cfg.Deployment.SPConfig.IOps = iops
	cfg.Deployment.SPConfig.Size = fmt.Sprintf("%dGi", sizeInGib)

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

	cfg.Deployment.GCPConfig = vlmconfig.GCPConfig{
		ProjectID:           projectId,
		ImageProjectID:      projectId,
		ServiceAccountEmail: saEmail,
		BucketName:          autoTierBucket,
	}

	cfg.Deployment.OntapCredentials.Username = env.GetString("VSA_NODE_USERNAME", "")
	cfg.Deployment.OntapCredentials.Password = env.GetString("VSA_NODE_PASSWORD", "")

	return nil
}

func (j *PoolActivity) DeleteVSADeployment(ctx context.Context, pool *datamodel.Pool) (*datamodel.Pool, error) {
	if pool.ClusterDetails.ExternalName == "" {
		return nil, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrIncorrectVSAClusterState, errors.New("pool cannot be deleted with active clusters")))
	}
	deploymentName := pool.ClusterDetails.ExternalName

	logger := util.GetLogger(ctx)
	cfg := &vlmconfig.VLMConfig{}
	saEmail := fmt.Sprintf("%s@%s.iam.gserviceaccount.com", pool.ServiceAccountId, pool.ClusterDetails.RegionalTenantProject)

	err := PrepareVlmConfig(cfg, deploymentName, localRegion, pool.PoolAttributes.PrimaryZone, pool.PoolAttributes.SecondaryZone, pool.ClusterDetails.Network, "vsa-"+localRegion, pool.ClusterDetails.RegionalTenantProject, pool.ClusterDetails.SnHostProject,
		int(pool.SizeInBytes), pool.PoolAttributes.ThroughputMibps, pool.PoolAttributes.Iops, saEmail, pool.AutoTierBucketName)

	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	vlmClient := GetVLMClient(ctx, logger, cfg)

	err = vlmClient.VSAClusterDeploymentDelete(ctx, cfg)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return pool, nil
}

func (j *PoolActivity) SaveVSANodeDetails(ctx context.Context, pool *datamodel.Pool, vlmConfig *vlmconfig.VLMConfig) (node1 *datamodel.Node, err error) {
	if len(vlmConfig.Cloud.HAPairs) == 0 {
		return nil, vsaerrors.WrapAsTemporalApplicationError(
			vsaerrors.NewVCPError(vsaerrors.ErrIncorrectVSAClusterState, errors.New("no cluster details provided")))
	}
	for _, details := range vlmConfig.Cloud.HAPairs {
		node1, err = SaveNodeDetails(ctx, j.SE, details.VM1, vlmConfig.Deployment, pool)
		if err != nil {
			return nil, vsaerrors.WrapAsTemporalApplicationError(err)
		}
		_, err = SaveNodeDetails(ctx, j.SE, details.VM2, vlmConfig.Deployment, pool)
		if err != nil {
			return nil, vsaerrors.WrapAsTemporalApplicationError(err)
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
		return nil, err
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

	if _, err = se.CreateNode(ctx, rec); err != nil && !utilErrors.IsConflictErr(err) {
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
	poolView, err := se.GetPool(ctx, pool.UUID, pool.AccountID)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	dbPool := repository.ConvertPoolViewToPool(poolView)
	return dbPool, nil
}

func (j *PoolActivity) DeletingPoolResources(ctx context.Context, pool *datamodel.Pool) (*datamodel.Pool, error) {
	se := j.SE
	// Update SVM, and Pool States to Deleting
	if err := DeletingSVMs(ctx, se, pool); err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	if err := DeletingNodes(ctx, se, pool); err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
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
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	if len(pools) > 1 {
		logger.Info("Skipping release subnetwork as there are other pools in the same region for the account")
		return nil
	}
	var gService hyperscaler.GoogleServices
	gcpService := NewGcpServices(ctx)

	consumerVpc := pool.Network
	accountName := pool.Account.Name
	subnetwork := "vsa-" + localRegion

	tenantProjectNumber, err := gService.GetTenantProject(consumerVpc, accountName, localRegion)
	if err != nil {
		gcpService.Logger.Errorf("Error finding tenancy unit: %v", err)
		return err
	}

	err = gService.ReleaseSubnetwork(localRegion, tenantProjectNumber, subnetwork)
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
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Delete SVMs
	if err := DeleteSVMs(ctx, se, pool); err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Delete nodes
	if err := DeleteNodes(ctx, se, pool); err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Delete the pool itself from a database
	if err := se.DeletePool(ctx, pool); err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return pool, nil
}

// CreateAutoTierBucket creates a GCP bucket for auto-tiering in the specified project and region.
// Parameters:
// - ctx: The context for managing request-scoped values, deadlines, and cancellation signals.
// - params: Contains the pool parameters, including the name and region of the pool.
// - projectId: The ID of the GCP project where the bucket will be created.
// Returns:
// - An error if the bucket creation fails or if there is an issue initializing GCP services.
func (j *PoolActivity) CreateAutoTierBucket(ctx context.Context, autoTierBucketName string, region string, projectId string) error {
	gcpService := &google.GcpServices{
		Ctx:    ctx,
		Logger: util.GetLogger(ctx),
	}

	err := CreateGCPBucket(ctx, projectId, autoTierBucketName, region, gcpService)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	return nil
}

// DeleteAutoTierBucket deletes the specified GCP bucket used for auto-tiering.
// It initializes a GCP service client and attempts to delete the bucket.
// Returns an error if the deletion fails or if GCP service initialization fails.
func (j *PoolActivity) DeleteAutoTierBucket(ctx context.Context, autoTierBucketName string) error {
	logger := util.GetLogger(ctx)

	gcpService := &google.GcpServices{
		Ctx:    ctx,
		Logger: util.GetLogger(ctx),
	}

	logger.Debugf("Deleting autoTiering bucket %v", autoTierBucketName)
	err := DeleteGCPBucket(ctx, autoTierBucketName, gcpService)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	return nil
}

func _createGCPBucket(ctx context.Context, projectId, bucketName, region string, gcpService hyperscaler.GoogleServices) error {
	logger := util.GetLogger(ctx)

	err := gcpService.InitializeClients()
	if err != nil {
		logger.Errorf("Error initializing GCP services: %v", err)
		return err
	}

	err = gcpService.CreateBucketIfNotExists(ctx, projectId, bucketName, region)
	if err != nil {
		logger.Errorf("error creating bucket: %v", err)
		return vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceAlreadyExistsError, err)
	}
	logger.Infof("Bucket created successfully %s", bucketName)

	return nil
}

func _deleteGCPBucket(ctx context.Context, bucketName string, gcpService hyperscaler.GoogleServices) error {
	logger := util.GetLogger(ctx)

	err := gcpService.InitializeClients()
	if err != nil {
		logger.Errorf("Error initializing GCP services: %v", err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	err = gcpService.DeleteBucket(ctx, bucketName)
	if err != nil {
		logger.Errorf("error deleting bucket: %v", err)
		return vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceDeprovisionError, err)
	}
	logger.Infof("Bucket deleted successfully %s", bucketName)

	return nil
}

// CreateServiceAccountWithStorageRole creates a GCP service account with the specified ID and display name,
// and attaches the "roles/storage.objectUser" role to it in the given project.
// Parameters:
// - ctx: Context for request-scoped values and cancellation.
// - projectID: The GCP project ID where the service account will be created.
// - saAccountID: The unique ID for the new service account.
// - saDisplayName: The display name for the new service account.
// Returns:
// - The created *iam.ServiceAccount, or an error if creation or role attachment fails.
func (j *PoolActivity) CreateServiceAccountWithStorageRole(ctx context.Context, projectID string, saAccountID string, saDisplayName string) (*iam.ServiceAccount, error) {
	gcpService := &google.GcpServices{
		Ctx:    ctx,
		Logger: util.GetLogger(ctx),
	}

	sa, err := CreateServiceAccountAndAttachRole(ctx, projectID, saAccountID, saDisplayName, gcpService)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	return sa, nil
}

func (j *PoolActivity) DeleteServiceAccount(ctx context.Context, projectID string, saAccountID string) error {
	gcpService := &google.GcpServices{
		Ctx:    ctx,
		Logger: util.GetLogger(ctx),
	}

	err := DeleteSrvcAccount(ctx, projectID, saAccountID, gcpService)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	return nil
}

func _deleteServiceAccount(ctx context.Context, projectID string, saAccountID string, gcpService hyperscaler.GoogleServices) error {
	logger := util.GetLogger(ctx)
	err := gcpService.InitializeClients()
	if err != nil {
		logger.Errorf("Error initializing GCP services: %v", err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	saEmail := fmt.Sprintf("%s@%s.iam.gserviceaccount.com", saAccountID, projectID)
	logger.Infof("Deleting service account %s", saEmail)
	err = gcpService.DeleteServiceAccount(saEmail)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return nil
}

func _createServiceAccountAndAttachRole(ctx context.Context, projectID string, saAccountID string, saDisplayName string, gcpService hyperscaler.GoogleServices) (*iam.ServiceAccount, error) {
	logger := util.GetLogger(ctx)

	err := gcpService.InitializeClients()
	if err != nil {
		return nil, err
	}

	createReq := &iam.CreateServiceAccountRequest{
		AccountId: saAccountID,
		ServiceAccount: &iam.ServiceAccount{
			DisplayName: saDisplayName,
		},
	}
	saEmail := fmt.Sprintf("%s@%s.iam.gserviceaccount.com", saAccountID, projectID)

	logger.Infof("Creating service account with object user role %s", saEmail)
	sa, err := gcpService.CreateServiceAccount(createReq, projectID, saEmail)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	logger.Infof("Created service account %s", saAccountID)
	roles := []string{"roles/storage.objectUser"}

	err = gcpService.AttachOrUpdateRolesForServiceAccounts(roles, saEmail, projectID)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return sa, nil
}

// deletingSVMs updates svm status to deleting.
func _deletingSVMs(ctx context.Context, se database.Storage, pool *datamodel.Pool) error {
	// Retrieve the svms associated with the pool
	svms, err := se.GetSvmsByPoolID(ctx, pool.ID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return vsaerrors.NewVCPError(vsaerrors.ErrSVMNotFound, errors.New("SVM not found"))
		}
		return err
	}
	for _, svm := range svms {
		// Check if the SVM is already marked for deletion
		if svm.State == models.LifeCycleStateDeleting {
			continue
		}
		if err = se.DeletingSVM(ctx, svm); err != nil {
			return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, fmt.Errorf("failed to update SVM record to deleting %s: %w", svm.Name, err))
		}
	}

	return nil
}

// deletingNodes updates nodes status to deleting.
func _deletingNodes(ctx context.Context, se database.Storage, pool *datamodel.Pool) error {
	// Retrieve the nodes associated with the pool
	nodes, err := se.GetNodesByPoolID(ctx, pool.ID)
	if err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, fmt.Errorf("failed to retrieve nodes for pool %d: %w", pool.ID, err))
	}

	// Delete each node
	for _, node := range nodes {
		// Check if the node is already marked for deletion
		if node.State == models.LifeCycleStateDeleting {
			continue
		}
		// Delete the node record from the database
		if err := se.DeletingNode(ctx, node); err != nil {
			return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, fmt.Errorf("failed to delete node record %s: %w", node.Name, err))
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
			return vsaerrors.NewVCPError(vsaerrors.ErrSVMNotFound, errors.New("SVM not found"))
		}
		return err
	}

	for _, svm := range svms {
		// Delete the SVM record from the database
		if svm.DeletedAt != nil && svm.DeletedAt.Valid {
			continue
		}
		if err := se.DeleteSVM(ctx, svm); err != nil {
			return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, fmt.Errorf("failed to delete SVM record %s: %w", pool.Name, err))
		}
	}
	return nil
}

// _deleteLIFs deletes LIFs database records associated with the given Nodes.
func _deleteLIFs(ctx context.Context, se database.Storage, pool *datamodel.Pool) error {
	// Retrieve the nodes associated with the pool
	nodes, err := se.GetNodesByPoolID(ctx, pool.ID)
	if err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, fmt.Errorf("failed to retrieve nodes for pool %d: %w", pool.ID, err))
	}

	// Delete each LIF
	for _, node := range nodes {
		// Retrieve the LIFs associated with the Node
		lif, err := se.GetLifByNodeID(ctx, node.ID, node.AccountID)
		if err != nil {
			return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, fmt.Errorf("failed to retrieve LIFs for Node %s: %w", node.Name, err))
		}

		if lif.DeletedAt != nil && lif.DeletedAt.Valid {
			continue
		}

		// Delete the LIF record from the database
		if err := se.DeleteLif(ctx, lif); err != nil {
			return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, fmt.Errorf("failed to delete LIF record %s: %w", lif.Name, err))
		}
	}

	return nil
}

// deleteNodes deletes node database records associated with the given pool.
func _deleteNodes(ctx context.Context, se database.Storage, pool *datamodel.Pool) error {
	// Retrieve the nodes associated with the pool
	nodes, err := se.GetNodesByPoolID(ctx, pool.ID)
	if err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, fmt.Errorf("failed to retrieve nodes for pool %d: %w", pool.ID, err))
	}

	// Delete each node
	for _, node := range nodes {
		// Check if the node is already deleted
		if node.DeletedAt != nil && node.DeletedAt.Valid {
			continue
		}
		// Delete the node record from the database
		if err := se.DeleteNode(ctx, node); err != nil {
			return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, fmt.Errorf("failed to update node record to deleting %s: %w", node.Name, err))
		}
	}

	return nil
}

func _newGcpServices(ctx context.Context) *google.GcpServices {
	return &google.GcpServices{
		Ctx:    ctx,
		Logger: util.GetLogger(ctx),
		Retry:  google.NewExponentialRetryStrategy(time.Second, uint(maxRetries)),
	}
}

func _getGCPService(ctx context.Context) (*google.GcpServices, error) {
	gcpService := NewGcpServices(ctx)

	gcpService.Logger.Debug("gcpService initialized")

	gcpService.Logger.Debug("Calling InitializeClients")
	err := gcpService.InitializeClients()
	if err != nil || !gcpService.IsAdminClientInitialized() {
		gcpService.Logger.Debug("Initialisation of service failed")
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPClientInitializationError, errors.New("initialisation of Google GCP service failed"))
	}
	return gcpService, nil
}

// setupNetworkWithFirewall sets up a VPC network, subnet, and firewall rules in GCP
func setupNetworkWithFirewall(ctx context.Context, projectName string, vpcName string, region *string, subnetName, subnetIpCidrRange string, firewallPriority int64, trafficDirection string, firewallSourceRanges []string, firewallAllowedPortRules []string) error {
	var service hyperscaler.GoogleServices
	service, err := GetGCPService(ctx)
	if err != nil {
		return err
	}
	err = CreateVPC(service, projectName, vpcName)
	if err != nil {
		return err
	}

	// Record heartbeat to indicate progress to temporal server
	activity.RecordHeartbeat(ctx, "VPC created, name: "+vpcName)

	err = InsertSubnet(service, projectName, region, subnetName, vpcName, subnetIpCidrRange)
	if err != nil {
		return err
	}

	// Record heartbeat to indicate progress to temporal server
	activity.RecordHeartbeat(ctx, "Subnet inserted, name: "+subnetName)

	err = InsertFirewall(service, projectName, fmt.Sprintf("ingress-%s", vpcName), vpcName, firewallPriority, trafficDirection, firewallSourceRanges, firewallAllowedPortRules)
	if err != nil {
		return err
	}

	// Record heartbeat to indicate progress to temporal server
	activity.RecordHeartbeat(ctx, "Firewall inserted, name: "+fmt.Sprintf("ingress-%s", vpcName))
	return nil
}

func resourceNotFoundCheck(errorString string, projectName, vpcName, subnetName, firewall string) (bool, error) {
	if !strings.Contains(errorString, "not found") {
		errorMessage := fmt.Sprintf("Error getting vpc for project: %s and vpc name: %s. Error : %s", projectName, vpcName, errorString)
		if subnetName != "" {
			errorMessage = fmt.Sprintf("Error getting subnet for project: %s, vpc name: %s, subnet name: %s. Error : %s", projectName, vpcName, subnetName, errorString)
		}
		if firewall != "" {
			errorMessage = fmt.Sprintf("Error getting subnet for project: %s, vpc name: %s, firewall name: %s. Error : %s", projectName, vpcName, firewall, errorString)
		}
		return false, vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceFetchError, errors.New(errorMessage))
	}
	return true, nil
}

// _createVPC invokes create VPC call from orchestrator. It is used for creating a VPC network in GCP for a project with the specified vpc name
func _createVPC(gService hyperscaler.GoogleServices, projectName, vpcName string) error {
	logger := gService.GetLogger()
	logger.Info(fmt.Sprintf("Checking if VPC already exists before creating VPC for project : %s network name : %s", projectName, vpcName))
	vpcNetworkReceived, err := gService.GetVPCNetwork(projectName, vpcName)
	if err != nil {
		resourceNotFound, errReceived := resourceNotFoundCheck(err.Error(), projectName, vpcName, "", "")
		if !resourceNotFound {
			return errReceived
		}
	}
	if vpcNetworkReceived != nil {
		logger.Debug(fmt.Sprintf("VPC already exists. Skipping creation. project name : %s , vpc name : %s", projectName, vpcName))
		return nil
	}
	vpcNetwork := &hyperscaler_models.VPCNetwork{Name: vpcName, ProjectName: projectName}
	err = gService.CreateVPC(vpcNetwork)
	if err != nil {
		errorString := fmt.Sprintf("Error creating vpc for project: %s and vpc name: %s. Error : %v", projectName, vpcName, err)
		logger.Errorf(errorString)
		return vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceProvisionError, errors.New(errorString))
	}
	logger.Info(fmt.Sprintf("vpc creation successful for project name : %s , vpc name : %s", projectName, vpcName))
	return nil
}

// _insertSubnet invokes create subnetwork call from orchestrator. It is used for creating a subnetwork in GCP for a project with the specified subnet name
func _insertSubnet(gService hyperscaler.GoogleServices, projectName string, region *string, subnetName string, vpcName string, ipCidrRange string) error {
	if region == nil {
		region = &localRegion
	}
	logger := gService.GetLogger()
	logger.Info(fmt.Sprintf("Checking if subnet already exists before creating subnet for project : %s  network name : %s firewall name : %s", projectName, vpcName, subnetName))
	subnetReceived, err := gService.GetSubnetwork(projectName, *region, subnetName)
	if err != nil {
		resourceNotFound, errReceived := resourceNotFoundCheck(err.Error(), projectName, vpcName, subnetName, "")
		if !resourceNotFound {
			return errReceived
		}
	}
	if subnetReceived != nil {
		logger.Debug(fmt.Sprintf("Subnet already exists. Skipping creation. project name : %s , vpc name : %s, subnet name : %s", projectName, vpcName, subnetName))
		return nil
	}
	subnetRequest := &hyperscaler_models.Subnet{
		Name:        subnetName,
		Network:     fmt.Sprintf("projects/%s/global/networks/%s", projectName, vpcName),
		IpCidrRange: ipCidrRange,
		Region:      region,
		ProjectName: projectName,
	}
	err = gService.CreateSubnetwork(subnetRequest)
	if err != nil {
		logger.Errorf("Error adding subnetwork: %v", err)
		return err
	}
	logger.Info(fmt.Sprintf("Successfully created subnet name : %s, vpc: %s, project name : %s", subnetName, vpcName, projectName))
	return nil
}

// _insertFirewall invokes create firewall call from orchestrator. It is used for creating a firewall in GCP for a project with the specified firewall name
func _insertFirewall(gService hyperscaler.GoogleServices, projectName, firewallName, vpcName string, priority int64, direction string, firewallSourceRanges, firewallAllowedPortRules []string) error {
	firewallRequest := &hyperscaler_models.Firewall{
		Name:             firewallName,
		AllowedPortRules: firewallAllowedPortRules,
		SourceRanges:     firewallSourceRanges,
		VPCNetworkName:   vpcName,
		ProjectName:      projectName,
		Priority:         priority,
		Direction:        direction, // can be INGRESS or EGRESS
	}
	logger := gService.GetLogger()
	logger.Info(fmt.Sprintf("Checking if firewall already exists before creating firewall for project : %s  network name : %s firewall name : %s", projectName, vpcName, firewallName))
	firewallReceived, err := gService.GetFirewall(projectName, firewallName)
	if err != nil {
		logger.Debug(fmt.Sprintf("Error getting firewall for project : %s and network name : %s firewall name : %s . Error : %v", projectName, vpcName, firewallName, err))
		resourceNotFound, errReceived := resourceNotFoundCheck(err.Error(), projectName, vpcName, "", firewallName)
		logger.Debug(fmt.Sprintf("Error getting firewall for project : %s and network name : %s firewall name : %s . Error : %v resourceNotFound : %t", projectName, vpcName, firewallName, err, resourceNotFound))
		if !resourceNotFound {
			return errReceived
		}
	}
	if firewallReceived != nil {
		logger.Debug(fmt.Sprintf("Firewall already exists. Skipping creation. project name : %s , vpc name : %s, firewall name : %s", projectName, vpcName, firewallName))
		return nil
	}
	logger.Info(fmt.Sprintf("Creating firewall for project : %s and network name : %s ", projectName, vpcName))

	err = gService.InsertFirewall(firewallRequest)
	if err != nil {
		logger.Errorf("Error adding firewall for project : %s and network name : %s. Error : %v ", projectName, vpcName, err)
		return err
	}
	logger.Info(fmt.Sprintf("Successfully created firewall for  project : %s and  VPC : %s", projectName, vpcName))
	return nil
}
