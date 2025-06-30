package activities

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	digitalCert "crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/json"
	"encoding/pem"
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
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vmrs"
	vmrs_config "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vmrs/config"
	vmrs_decision "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vmrs/decision"
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
	FindTenancyAndGetSubnetwork               = _findTenancyAndGetSubnetwork
	DeploymentsInsert                         = common.DeploymentsInsert
	PrepareVlmConfig                          = _prepareVlmConfig
	ReadFile                                  = os.ReadFile
	GetVLMClient                              = _getVLMClient
	SaveNodeDetails                           = _saveNodeDetails
	DeleteLIFs                                = _deleteLIFs
	DeleteSVMs                                = _deleteSVMs
	DeleteNodes                               = _deleteNodes
	DeletingNodes                             = _deletingNodes
	DeletingSVMs                              = _deletingSVMs
	CreateVPC                                 = _createVPC
	InsertSubnet                              = _insertSubnet
	InsertFirewall                            = _insertFirewall
	GetGCPService                             = _getGCPService
	NewGcpServices                            = _newGcpServices
	SetupNetworkWithFirewall                  = setupNetworkWithFirewall
	SetupNetworkFirewallsForIscsi             = setupNetworkFirewallsForIscsi
	CreateGCPBucket                           = _createGCPBucket
	CreateServiceAccountAndAttachRole         = _createServiceAccountAndAttachRole
	DeleteSrvcAccount                         = _deleteServiceAccount
	DeleteGCPBucket                           = _deleteGCPBucket
	GetSubnetForConsumerProjectAndRelease     = _getSubnetForConsumerProjectAndRelease
	GenerateAndCreateCertificateForVSACluster = _generateAndCreateCertificateForVSACluster
	GeneratePasswordForVSACluster             = _generatePasswordForVSACluster
	GetPasswordForVSACluster                  = _getPasswordForVSACluster
	GetPasswordFromCacheOrSecretManager       = _getPasswordFromCacheOrSecretManager
	GenerateCSR                               = _generateCSR
	LoadVMRSConfig                            = vmrs_config.LoadConfig
	CreateDecisionMaker                       = vmrs_decision.NewDecisionMaker
)

type PoolActivity struct {
	SE database.Storage
}

const (
	aggregateName  = "aggr1"
	DefaultSvmName = "gcnv"
	lifNameFormat  = "san_lif_%s"
	enableIscsi    = true

	firewallPriority        = 1000
	ingressTrafficDirection = "INGRESS"

	CsrType          = "CERTIFICATE REQUEST"
	RsaKeyType       = "RSA PRIVATE KEY"
	digitalSignature = 0x80 // 10000000 in binary (bit 0)
	keyEncipherment  = 0x20
)

var (
	maxRetries              = env.GetInt("GOOGLE_API_MAX_RETRIES", 6)
	localRegion             = env.GetString("REGION", "")
	firewallSourceRange     = env.GetString("FIREWALL_SOURCE_RANGE", "10.0.0.0/8,172.16.0.0/12,192.168.0.0/16,34.0.0.0/8,46.149.16.0/20,52.94.203.152/29,52.94.203.160/29,185.35.244.0/22,202.3.112.0/20,216.240.16.0/20,217.70.208.0/20,198.18.0.0/15")
	caName                  = env.GetString("CA_NAME", "")
	caPoolName              = env.GetString("CA_POOL_NAME", "")
	caPoolDeployedProjectID = env.GetString("CA_POOL_DEPLOYED_PROJECT_ID", "")
	vsaDeployedDnsName      = env.GetString("VSA_DEPLOYED_DNS_NAME", "")
	nodePassword            = env.GetString("VSA_NODE_PASSWORD", "")
)

func (j *PoolActivity) CreatingPool(ctx context.Context, pool *datamodel.Pool) (*datamodel.Pool, error) {
	se := j.SE
	return se.CreatingPool(ctx, pool)
}

func (j *PoolActivity) FailedPool(ctx context.Context, pool *datamodel.Pool, errMessage string) error {
	se := j.SE
	_, err := se.ErroredResource(ctx, pool, errMessage)
	if err != nil {
		return err
	}
	return nil
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

	res, err := se.ErroredResource(ctx, pool, errMessage)
	dbPool := res.(*datamodel.Pool)
	return dbPool, err
}

func (j *PoolActivity) DeletePoolResourcesOnRollback(ctx context.Context, pool *datamodel.Pool) error {
	se := j.SE

	// Delete LIFs
	if err := DeleteLIFs(ctx, se, pool); err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Delete SVMs
	if err := DeleteSVMs(ctx, se, pool); err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Delete nodes
	if err := DeleteNodes(ctx, se, pool); err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return nil
}

func (j *PoolActivity) UpdatedPool(ctx context.Context, pool *datamodel.Pool) (*datamodel.Pool, error) {
	se := j.SE
	return se.UpdatedPool(ctx, pool)
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

	snHost, err := gcpService.GetSnHost(tenantProjectNumber)
	if err != nil {
		if !strings.Contains(err.Error(), "not found") {
			gcpService.GetLogger().Errorf("Error getting service networking host project: %v", err)
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceFetchError, err)
		}
	} else {
		// if snHost is found, check if the subnetwork already exists in the tenant project
		subnetName := "vsa-" + *tenantProjectRegion
		subnetReceived, err := gcpService.GetSubnetwork(snHost, *tenantProjectRegion, subnetName)
		if err != nil {
			resourceNotFound, errReceived := resourceNotFoundCheck(err.Error(), tenantProjectNumber, "", subnetName, "")
			if !resourceNotFound {
				gcpService.GetLogger().Errorf("Error getting subnetwork for tenant project: %s, SN host : %s, region %s. Error : %s", tenantProjectNumber, snHost, *tenantProjectRegion, err.Error())
				return nil, errReceived
			}
		}
		if subnetReceived != nil && subnetReceived.Name == subnetName {
			gcpService.GetLogger().Debug(fmt.Sprintf("Subnetwork %s already exists in tenant project %s and region %s. Won't create another subnet for this region", subnetName, tenantProjectNumber, *tenantProjectRegion))
			return &commonparams.TenancyInfo{
				RegionalTenantProject: tenantProjectNumber,
				Network:               strings.Split(subnetReceived.Network, "/")[len(strings.Split(subnetReceived.Network, "/"))-1],
				SubnetworkName:        subnetReceived.Name,
				SnHostProject:         snHost,
				Gateway:               subnetReceived.GatewayAddress,
			}, nil
		}
	}
	// if snHost is not found, create a new subnetwork for the tenant project. If subnet is not found, create a new subnetwork
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

func (j *PoolActivity) CreateCertificate(ctx context.Context, region, clusterName string) error {
	gcpService, err := GetGCPService(ctx)
	if err != nil {
		return err
	}

	// Generate a unique certificate ID and common name
	uuid := utils.RandomUUID()
	commonName := fmt.Sprintf("%s-cn", uuid)
	domains := fmt.Sprintf("*.%s.%s", clusterName, vsaDeployedDnsName)
	params := &hyperscaler_models.CustomCertificateParam{
		Region:        region,
		CaPoolName:    caPoolName,
		CaName:        caName,
		CertificateId: uuid,
		CommonName:    commonName,
		Domains:       []string{domains},
		AccountId:     caPoolDeployedProjectID,
	}
	_, _, err = GenerateAndCreateCertificateForVSACluster(gcpService, params)
	if err != nil {
		return err
	}
	return nil
}

// CreateSecret creates a secret in GCP Secret Manager for the VSA cluster
func (j *PoolActivity) CreateSecret(ctx context.Context, region, secretID string) (*hyperscaler_models.CustomSecret, error) {
	gcpService, err := GetGCPService(ctx)
	if err != nil {
		return nil, err
	}

	secret, err := GeneratePasswordForVSACluster(gcpService, caPoolDeployedProjectID, region, secretID)
	if err != nil {
		return nil, err
	}
	return secret, nil
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

func _getVLMClient(ctx context.Context, logger log.Logger, vlmConfig *vlmconfig.VLMConfig) vlm.ClientFactory {
	return vlm.NewClient(ctx, logger, vlmConfig)
}

func (j *PoolActivity) GetOntapVersion(ctx context.Context, node *models.Node) (*string, error) {
	provider := GetProviderByNode(ctx, node)
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
		Name:      DefaultSvmName,
		VlmConfig: vlmConfig,
	}

	err := vlmClient.VSASVMCreate(ctx, svmParam)
	// If the SVM already exists, we can ignore the error and move forward
	if err != nil && !strings.Contains(err.Error(), "already exists and is in use by a different VM") {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	name := vlmConfig.Deployment.DeploymentID + "-datasvm-" + DefaultSvmName
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

// The IdentifyVMs takes as input the VMRS configuration, the customer requested performance parameters, and the current VLM configuration to identify the optimal VMs to use for the VSA cluster.
func (j *PoolActivity) IdentifyVMs(ctx context.Context, vmrsConfigPath string, customerRequest vmrs.CustomerRequestedPerformance, deploymentName, region, primaryZone, secondaryZone, network, subnet, projectId, snHostProject string, vsaClusterPassword string, saEmail string, autoTierBucket string) (*vlmconfig.VLMConfig, error) {
	logger := util.GetLogger(ctx)
	logger.Debug("Identifying VMs to use for VSA cluster")

	// Parse VMRS config.
	vmrsConfig, err := LoadVMRSConfig(vmrsConfigPath)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Identify the right VMs to use using the selection strategy defined in the VMRS config.
	decisionMaker, err := CreateDecisionMaker(vmrsConfig)
	if err != nil {
		logger.Error("Failed to create decision maker", "error", err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	vlmConfig := &vlmconfig.VLMConfig{}
	decision, err := decisionMaker.FindOptimalVMs(vmrsConfig, customerRequest, vlmConfig)
	if err != nil {
		logger.Error("Failed to identify optimal VMs", "error", err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Convert the decision to a VLMConfig.
	err = PrepareVlmConfig(vlmConfig, deploymentName, region, primaryZone, secondaryZone, network, subnet, projectId, snHostProject, decision, vsaClusterPassword, saEmail, autoTierBucket)
	if err != nil {
		logger.Error("Failed to prepare VLM config", "error", err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	return vlmConfig, nil
}

func (j *PoolActivity) CreateVSACluster(ctx context.Context, cfg *vlmconfig.VLMConfig) (*vlmconfig.VLMConfig, error) {
	logger := util.GetLogger(ctx)
	vlmClient := GetVLMClient(ctx, logger, cfg)

	err := vlmClient.VSAClusterDeployCreate(ctx, cfg)
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

func _prepareVlmConfig(cfg *vlmconfig.VLMConfig, deploymentName, region, primaryZone, secondaryZone, network, subnet, projectId, snHostProject string, decision *vmrs.Decision, password string, saEmail string, autoTierBucket string) error {
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

	cfg.Deployment.SPConfig.Throughput = decision.StoragePoolRequirements.DesiredThroughputInMiBs
	cfg.Deployment.SPConfig.IOps = decision.StoragePoolRequirements.DesiredIOPS
	cfg.Deployment.SPConfig.Size = fmt.Sprintf("%dGi", decision.StoragePoolRequirements.DesiredCapacityInGiB)
	cfg.Deployment.VSAInstanceType = decision.ChosenVMs[0] // VLM currently only supports a single VM type for VSA clusters (homogeneous clusters).

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
	cfg.Deployment.OntapCredentials.Password = password

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
	var password string
	if pool.SecretID != "" {
		secret, err := GetPasswordForVSACluster(ctx, caPoolDeployedProjectID, pool.SecretID)
		if err != nil {
			logger.Error("Failed to get password for VSA cluster", "error", err)
			return nil, err
		}
		password = secret.SecretVersion.Value
	} else {
		password = nodePassword
	}

	decision := &vmrs.Decision{
		ChosenVMs: []string{""}, // The value of this field doesn't matter for deletion.
		StoragePoolRequirements: vmrs.CustomerRequestedPerformance{
			DesiredIOPS:             pool.PoolAttributes.Iops,
			DesiredThroughputInMiBs: pool.PoolAttributes.ThroughputMibps,
			DesiredCapacityInGiB:    pool.SizeInBytes,
		},
	}

	err := PrepareVlmConfig(cfg, deploymentName, localRegion, pool.PoolAttributes.PrimaryZone, pool.PoolAttributes.SecondaryZone, pool.ClusterDetails.Network, "vsa-"+localRegion, pool.ClusterDetails.RegionalTenantProject, pool.ClusterDetails.SnHostProject, decision, password, saEmail, pool.AutoTierBucketName)

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
		Zone:            vmConfig.Zone,
		InstanceType:    deploymentConfig.VSAInstanceType,
	}
	if pool.SecretID != "" {
		node.SecretID = pool.SecretID
	} else {
		node.Password = deploymentConfig.OntapCredentials.Password
	}
	provider := GetProviderByNode(ctx, node)

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
	logger := util.GetLogger(ctx)
	se := j.SE
	conditions := [][]interface{}{{"account_id = ?", pool.AccountID}}
	conditions = append(conditions, []interface{}{"network = ?", pool.Network})
	pools, err := se.ListPools(ctx, conditions)
	if err != nil {
		logger.Errorf("Failed to get pools for account: %s, network: %s, error: %s", pool.AccountID, pool.Network, err.Error())
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	if len(pools) > 1 {
		logger.Info("Skipping release subnetwork as there are other pools in the same region for the account")
		return nil
	}

	consumerVpc := pool.Network
	accountName := pool.Account.Name
	subnetworkName := "vsa-" + localRegion

	gcpService, err := GetGCPService(ctx)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	err = GetSubnetForConsumerProjectAndRelease(gcpService, consumerVpc, accountName, localRegion, subnetworkName, pool.ClusterDetails)
	if err != nil {
		logger.Errorf("Error releasing subnetwork: %v", err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return nil
}

func _getSubnetForConsumerProjectAndRelease(service hyperscaler.GoogleServices, consumerVpc, accountName, localRegion, subnetworkName string, clusterDetails datamodel.ClusterDetails) error {
	logger := service.GetLogger()
	snHostProject := ""
	var err error
	if clusterDetails.RegionalTenantProject != "" && clusterDetails.SnHostProject != "" {
		snHostProject = clusterDetails.SnHostProject
	} else {
		tenantProjectNumber, err := service.GetTenantProject(consumerVpc, accountName, localRegion)
		if err != nil {
			logger.Errorf("Error finding tenancy unit: %v", err)
			return err
		}

		snHostProject, err = service.GetSnHost(tenantProjectNumber)
		if err != nil {
			logger.Errorf("Error getting SN host for subnet: %s with tenant project %s, skipping release", subnetworkName, tenantProjectNumber)
			return err
		}
	}
	// Check if the subnetwork exists
	_, err = service.GetSubnetwork(snHostProject, localRegion, subnetworkName)
	if err != nil {
		logger.Errorf("Error getting Subnetwork %s project %s, skipping release", subnetworkName, accountName)
		return err
	}
	err = service.ReleaseSubnetwork(localRegion, snHostProject, subnetworkName)
	if err != nil {
		logger.Errorf("Error Releasing subnetwork: %v", err)
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

// _generateAndCreateCertificateForVSACluster generates a CSR and creates a certificate in GCP Certificate Authority Service.
func _generateAndCreateCertificateForVSACluster(gcpService hyperscaler.GoogleServices, param *hyperscaler_models.CustomCertificateParam) (*hyperscaler_models.CustomCertificate, *hyperscaler_models.CustomSecret, error) {
	logger := gcpService.GetLogger()
	// Generate CSR
	csrDER, key, err := GenerateCSR(param.CommonName, param.Domains)
	if err != nil {
		logger.Errorf("failed to generate CSR for commonName: %s, certificateId : %s, err : %v", param.CommonName, param.CertificateId, err)
		return nil, nil, err
	}
	pemBlock := pem.Block{
		Type:  CsrType,
		Bytes: csrDER,
	}
	logger.Debug("Generate CSR for commonName: %s, certificateId : %s", param.CommonName, param.CertificateId)

	certificate, err := google.ValidateAndConvertCertificateParamsToCustomCertificate(param, pemBlock)
	if err != nil {
		return nil, nil, err
	}
	// Create the Certificate
	certificate, err = gcpService.CreateCertificate(certificate)
	if err != nil {
		return nil, nil, err
	}

	// Store the private key in Secret Manager
	secretName := fmt.Sprintf("%s-%s-%s-%s", param.AccountId, param.Region, param.CaName, param.CertificateId)
	secretValue := google.ConvertPrivateKeyToString(key, RsaKeyType)
	secret, err := gcpService.CreateSecret(param.AccountId, param.Region, secretName, secretValue)
	if err != nil {
		// Revoke the certificate if the secret creation fails
		_, revokeError := gcpService.RevokeCertificate(certificate)
		if revokeError != nil {
			return nil, nil, revokeError
		}
		return nil, nil, err
	}
	return certificate, secret, nil
}

// _generatePasswordForVSACluster generates a strong password and creates a secret in GCP Secret Manager.
func _generatePasswordForVSACluster(gcpService hyperscaler.GoogleServices, projectID, region, secretID string) (*hyperscaler_models.CustomSecret, error) {
	logger := gcpService.GetLogger()
	password, err := utils.GenerateStrongPassword(12)
	if err != nil {
		logger.Errorf("failed to generate password for secretID: %s, err : %v", secretID, err)
		return nil, err
	}
	var secret *hyperscaler_models.CustomSecret
	secret, getSecretError := gcpService.GetSecretWithLatestVersion(projectID, secretID)
	if getSecretError != nil {
		secret, err = gcpService.CreateSecret(projectID, region, secretID, password)
		if err != nil {
			return nil, err
		}
	}
	return secret, nil
}

// _getPasswordForVSACluster retrieves the password for a VSA cluster from GCP Secret Manager.
func _getPasswordForVSACluster(ctx context.Context, projectID, secretID string) (*hyperscaler_models.CustomSecret, error) {
	var gcpService hyperscaler.GoogleServices
	gcpService, err := GetGCPService(ctx)
	if err != nil {
		return nil, err
	}
	secret, err := gcpService.GetSecretWithLatestVersion(caPoolDeployedProjectID, secretID)
	if err != nil || secret == nil || secret.SecretVersion == nil {
		return nil, fmt.Errorf("failed to get secret for project: %s, userName: %s, err: %s", projectID, secretID, err)
	}
	return secret, nil
}

// _getPasswordFromCacheOrSecretManager retrieves the password for a VSA cluster from cache or GCP Secret Manager if not found in cache.
func _getPasswordFromCacheOrSecretManager(ctx context.Context, secretID string) string {
	password := ""
	userCache, exist := commonparams.GetAuthCache(secretID)
	if !exist || userCache.Password == "" {
		secret, err := GetPasswordForVSACluster(ctx, caPoolDeployedProjectID, secretID)
		if err != nil {
			return ""
		}
		password = secret.SecretVersion.Value
		commonparams.AddToAuthCache(secretID, password)
		return password
	}
	return userCache.Password
}

// _generateCSR generates a Certificate Signing Request (CSR) with the specified common name and domains.
func _generateCSR(commonName string, domains []string) ([]byte, *rsa.PrivateKey, error) {
	// Generate an RSA private key.
	key, err := rsa.GenerateKey(rand.Reader, 3072)
	if err != nil {
		return nil, nil, err
	}

	// Build Key Usage extension. We want DigitalSignature and KeyEncipherment set.
	keyUsageVal := digitalSignature | keyEncipherment // Should be 0x80 | 0x20 = 0xA0 (10100000)

	// Create the ASN.1 BIT STRING for key usage.
	bitString := asn1.BitString{
		Bytes:     []byte{byte(keyUsageVal)},
		BitLength: 8, // We are encoding one full byte.
	}
	rawKeyUsage, err := asn1.Marshal(bitString)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal key usage: %s", err.Error())
	}

	// --- Build Extended Key Usage extension ---
	// We want both serverAuth and clientAuth.
	ekuOIDs := []asn1.ObjectIdentifier{
		{1, 3, 6, 1, 5, 5, 7, 3, 1},
		{1, 3, 6, 1, 5, 5, 7, 3, 2},
	}
	rawEKU, err := asn1.Marshal(ekuOIDs)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal extended key usage: %v", err)
	}

	// Prepare the extensions.
	extensions := []pkix.Extension{
		{
			Id:       asn1.ObjectIdentifier{2, 5, 29, 15}, // Key Usage
			Critical: true,
			Value:    rawKeyUsage,
		},
		{
			Id:       asn1.ObjectIdentifier{2, 5, 29, 37}, // Extended Key Usage
			Critical: false,
			Value:    rawEKU,
		},
	}

	// Build the certificate request template.
	template := digitalCert.CertificateRequest{
		Subject: pkix.Name{
			CommonName:   commonName,
			Organization: []string{"Netapp"},
		},
		SignatureAlgorithm: digitalCert.SHA256WithRSA,
		ExtraExtensions:    extensions,
		DNSNames:           domains,
	}

	// Create the CSR in DER format.
	csrDER, err := digitalCert.CreateCertificateRequest(rand.Reader, &template, key)
	if err != nil {
		return nil, nil, err
	}

	return csrDER, key, nil
}
