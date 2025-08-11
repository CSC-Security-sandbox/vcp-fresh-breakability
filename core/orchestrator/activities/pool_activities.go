package activities

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"

	"dario.cat/mergo"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/vlm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vmrs"
	vmrs_config "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vmrs/config"
	vmrs_decision "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vmrs/decision"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	hyperscaler2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	hyperscaler_models "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	utilErrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/activity"
	"google.golang.org/api/servicenetworking/v1"
	"gorm.io/gorm"
)

var (
	DeploymentsInsert                 = common.DeploymentsInsert
	PrepareVlmConfig                  = _prepareVlmConfig
	ReadFile                          = os.ReadFile
	SaveNodeDetails                   = _saveNodeDetails
	DeleteLIFs                        = _deleteLIFs
	DeleteSVMs                        = _deleteSVMs
	FailedSVMs                        = _failedSVMs
	DeleteNodes                       = _deleteNodes
	FailedNodes                       = _failedNodes
	DeletingNodes                     = _deletingNodes
	DeletingSVMs                      = _deletingSVMs
	CreateVPC                         = _createVPC
	InsertSubnet                      = _insertSubnet
	InsertFirewall                    = _insertFirewall
	GetTenantProject                  = _getTenantProject
	GetCreateDataSubnetworkOp         = _getCreateDataSubnetworkOp
	GetSubnetToBeUsed                 = getSubnetToBeUsed
	SetupNetworkFirewallsForIscsi     = setupNetworkFirewallsForIscsi
	CreateGCPBucket                   = _createGCPBucket
	CheckReusableSubnet               = _checkReusableSubnet
	CreateServiceAccountAndAttachRole = _createServiceAccountAndAttachRole
	DeleteSrvcAccount                 = _deleteServiceAccount
	DeleteGCPBucket                   = _deleteGCPBucket
	LoadVMRSConfig                    = vmrs_config.LoadConfig
	CreateDecisionMaker               = vmrs_decision.NewDecisionMaker
	VlmConfigFilePath                 = env.GetString("VLM_CONFIG_FILE_PATH", "common/vsa_config/vlm-config.json")
	ValidateVlmConfigInputs           = _validateVlmConfigInputs
	GetCreateSubnetworkOperation      = _getCreateSubnetworkOperation
	ReleaseSubnet                     = _releaseSubnet
	CheckAndUpdateFirewall            = _checkAndUpdateFirewall
	LoadVlmConfigFromFile             = loadVlmConfigFromFile
	GetServiceNetOpStatus             = _getServiceNetOpStatus
	GetComputeOpStatus                = _getComputeOpStatus
	GetSubnetFromOperation            = _getSubnetFromOperation
	GetGatewayFromIpCidrRange         = _getGatewayFromIpCidrRange
	ResolveZonesForCluster            = _resolveZonesForCluster

	// Feature flag to enforce minimum values for SPConfig throughput and IOPS.
	// Set ENFORCE_MIN_SP_CONFIG=true in the environment to enable.
	enforceMinSPConfig   = env.GetBool("ENFORCE_MIN_SP_CONFIG", false)
	vsaImageProject      = env.GetString("VSA_IMAGE_PROJECT", "")
	mediatorImageProject = env.GetString("VSA_MEDIATOR_IMAGE_PROJECT", "")
)

type PoolActivity struct {
	SE database.Storage
}

type controlPlaneNetworks struct {
	vpcName      string
	subnetName   string
	ipCidrRange  string
	sourceRanges string
	portRules    string
	firewallName string
}

var internalVSANetworks = map[string]controlPlaneNetworks{
	mgmtVpcName: controlPlaneNetworks{vpcName: mgmtVpcName, subnetName: mgmtSubnetName, ipCidrRange: MgmtNetworkIpRange, firewallName: mgmtFirewallName, sourceRanges: getFirewallSourceRangesForMgmt(), portRules: MgmtFirewallPortRules},
	icVpcName:   controlPlaneNetworks{vpcName: icVpcName, subnetName: icSubnet, ipCidrRange: IcNetworkIpRange, firewallName: icFirewallName, sourceRanges: IcFirewallSourceRanges, portRules: IcFirewallPortRules},
	rsmVpcName:  controlPlaneNetworks{vpcName: rsmVpcName, subnetName: rsmSubnetName, ipCidrRange: RsmNetworkIpRange, firewallName: rsmFirewallName, sourceRanges: RsmFirewallSourceRanges, portRules: RSMFirewallPortRules},
}

const (
	aggregateName  = "aggr1"
	DefaultSvmName = "gcnv"

	firewallPriority        = 1000
	ingressTrafficDirection = "INGRESS"

	keyManagerBootarg = "bootarg.keymanager.ekmip.svm_context=false"

	mgmtVpcName      = "mgmt-e0a-vpc-01"
	mgmtSubnetName   = "mgmt-e0a-subnet-01"
	mgmtFirewallName = "ingress-" + mgmtVpcName

	icVpcName      = "ic-e0b-vpc-01"
	icSubnet       = "ic-e0b-subnet-01"
	icFirewallName = "ingress-" + icVpcName

	rsmVpcName      = "rsm-e0c-vpc-01"
	rsmSubnetName   = "rsm-e0c-subnet-01"
	rsmFirewallName = "ingress-" + rsmVpcName

	iscsiDataFirewallName = "ingress-data-iscsi"

	DefaultDataFiles = "default-data-files"
)

// Minimum allowed values for SPConfig throughput (in MiBs) and IOPS.
// These are enforced only if the feature flag above is enabled.
const (
	minSPConfigThroughput = 1120
	minSPConfigIOps       = 24000
)

var (
	totalIPPerHAPair          = env.GetInt("TOTAL_IP_PER_HA_PAIR", 6)
	mediatorVmInstanceType    = env.GetString("VSA_MEDIATOR_INSTANCE_TYPE", "n2-standard-4")
	mediatorVmDiskType        = env.GetString("VSA_MEDIATOR_DISK_TYPE", "pd-ssd")
	clusterSerialNumberPrefix = env.GetString("CLUSTER_SERIAL_NUMBER_PREFIX", "935")
	Region                    = env.GetString("LOCAL_REGION", "")
	regionMapJson             = env.GetString("REGION_NUMBER_MAP", `{"africa-south1": "01","asia-east1": "02","asia-east2": "03","asia-northeast1": "04","asia-northeast2": "05","asia-northeast3": "06","asia-south1": "07","asia-south2": "08","asia-southeast1": "09","asia-southeast2": "10","australia-southeast1": "11","australia-southeast2": "12","europe-central2": "13","europe-north1": "14","europe-north2": "15","europe-southwest1": "16","europe-west1": "17","europe-west10": "18","europe-west12": "19","europe-west2": "20","europe-west3": "21","europe-west4": "22","europe-west6": "23","europe-west8": "24","europe-west9": "25","me-central1": "26","me-central2": "27","me-west1": "28","northamerica-northeast1": "29","northamerica-northeast2": "30","northamerica-south1": "31","southamerica-east1": "32","southamerica-west1": "33","us-central1": "34","us-east1": "35","us-east4": "36","us-east5": "37","us-south1": "38","us-west1": "39","us-west2": "40","us-west3": "41","us-west4": "42"}`)

	MgmtFirewallSourceRanges  = env.GetString("MGMT_FIREWALL_SOURCE_RANGES", "")
	RsmFirewallSourceRanges   = env.GetString("RSM_FIREWALL_SOURCE_RANGES", "")
	IcFirewallSourceRanges    = env.GetString("IC_FIREWALL_SOURCE_RANGES", "")
	IscsiFirewallSourceRanges = env.GetString("ISCSI_FIREWALL_SOURCE_RANGES", "")

	MgmtRegionalNatIP = env.GetString("MGMT_REGIONAL_NAT_IP", "")

	MgmtNetworkIpRange = env.GetString("MGMT_NETWORK_IP_RANGE", "198.18.0.0/20")
	RsmNetworkIpRange  = env.GetString("RSM_NETWORK_IP_RANGE", "198.18.16.0/20")
	IcNetworkIpRange   = env.GetString("IC_NETWORK_IP_RANGE", "198.18.32.0/20")

	MgmtFirewallPortRules = env.GetString("MGMT_FIREWALL_PORT_RULES", "tcp,udp,icmp")
	RSMFirewallPortRules  = env.GetString("RSM_FIREWALL_PORT_RULES", "tcp,udp")
	IcFirewallPortRules   = env.GetString("IC_FIREWALL_PORT_RULES", "tcp,udp")

	IscsiFirewallPortRules = env.GetString("ISCSI_FIREWALL_PORT_RULES", "tcp,3260")
	RegionNumber           = getRegionNumber()
)

func (j *PoolActivity) CreatingPool(ctx context.Context, pool *datamodel.Pool) (*datamodel.Pool, error) {
	se := j.SE
	pool, err := se.CreatingPool(ctx, pool)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return pool, nil
}

func (j *PoolActivity) FailedPool(ctx context.Context, pool *datamodel.Pool, errMsg string) error {
	se := j.SE
	pool.State = models.LifeCycleStateError
	_, err := se.ErroredResource(ctx, pool, errMsg)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// mark SVMs as failed SVMs
	if err := FailedSVMs(ctx, se, pool); err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// mark nodes as failed nodes
	if err := FailedNodes(ctx, se, pool); err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return nil
}

func (j *PoolActivity) FailedPoolActivity(ctx context.Context, pool *datamodel.Pool, errMsg string) error {
	se := j.SE
	pool.State = models.LifeCycleStateError
	_, err := se.ErroredResource(ctx, pool, errMsg)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return nil
}

func (j *PoolActivity) CreatedPool(ctx context.Context, pool *datamodel.Pool, vlmConfig *vlm.VLMConfig) (*datamodel.Pool, error) {
	se := j.SE

	pool, err := se.CreatedPool(ctx, pool)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	if vlmConfig != nil {
		// Save VLMConfig here, so that it can be reused.
		marshalledVlmConfig, err := json.Marshal(*vlmConfig)
		if err != nil {
			return nil, vsaerrors.WrapAsTemporalApplicationError(err)
		}
		pool.VLMConfig = string(marshalledVlmConfig)
		pool, err = se.UpdatedPool(ctx, pool)
		if err != nil {
			return nil, vsaerrors.WrapAsTemporalApplicationError(err)
		}
	}

	return pool, nil
}

func (j *PoolActivity) ErroredPool(ctx context.Context, pool *datamodel.Pool, errMessage string) (*datamodel.Pool, error) {
	se := j.SE

	res, err := se.ErroredResource(ctx, pool, errMessage)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	dbPool := res.(*datamodel.Pool)
	return dbPool, nil
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
	pool, err := se.UpdatedPool(ctx, pool)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return pool, nil
}

func (j *PoolActivity) UpdatedPoolWithVLMConfig(ctx context.Context, pool *datamodel.Pool, vlmConfig vlm.VLMConfig, updatePoolParams *commonparams.UpdatePoolParams) (*datamodel.Pool, error) {
	se := j.SE
	marshalledVlmConfig, err := json.Marshal(vlmConfig)
	if err != nil {
		return nil, err
	}

	// modifying only the required fields
	pool.VLMConfig = string(marshalledVlmConfig)
	pool.SizeInBytes = int64(updatePoolParams.SizeInBytes)
	pool.Description = updatePoolParams.Description
	pool.PoolAttributes.ThroughputMibps = int64(updatePoolParams.TotalThroughputMibps)
	pool.PoolAttributes.Iops = int64(updatePoolParams.TotalIops)
	if updatePoolParams.Labels != nil {
		pool.PoolAttributes.Labels = updatePoolParams.Labels
	}

	return se.UpdatedPool(ctx, pool)
}

func (j *PoolActivity) UpdatingPool(ctx context.Context, pool *datamodel.Pool) (*datamodel.Pool, error) {
	se := j.SE
	pool, err := se.UpdatingPool(ctx, pool)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	return pool, nil
}

func (j *PoolActivity) UpdatePoolState(ctx context.Context, pool *datamodel.Pool, state string, stateDetails string) (*datamodel.Pool, error) {
	se := j.SE
	pool, err := se.UpdatePoolState(ctx, pool, state, stateDetails)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	return pool, nil
}

// FindTenancy finds the tenancy unit for a customer
func (j *PoolActivity) FindTenancyProject(ctx context.Context, params commonparams.CreatePoolParams) (string, error) {
	// need to pass tenantProjectRegion only in case of CBR where region != the regional region as set from env variable
	service, err := hyperscaler2.GetGCPService(ctx)
	if err != nil {
		return "", vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return GetTenantProject(service, params)
}

func _getTenantProject(service hyperscaler2.GoogleServices, params commonparams.CreatePoolParams) (string, error) {
	tenantProjectNumber, err := service.GetTenantProject(params.VendorSubNetID, params.AccountName, params.Region)
	if err != nil {
		service.GetLogger().Errorf("Error finding tenancy unit. Project: %s vpc: %s Error: %v", params.AccountName, params.VendorSubNetID, err)
		return "", err
	}
	service.GetLogger().Debugf("Found tenancy: tenantProjectNumber: %s for consumer project: %s", tenantProjectNumber, params.AccountName)
	return tenantProjectNumber, nil
}

// GetAvailableSubnet identifies current available subnets and re-uses subnet if IP CIDR range is available
func (j *PoolActivity) GetAvailableSubnet(ctx context.Context, params commonparams.CreatePoolParams, tenantProjectNumber string) (*hyperscaler_models.Subnet, error) {
	service, err := hyperscaler2.GetGCPService(ctx)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return CheckReusableSubnet(j.SE, service, params, tenantProjectNumber)
}

func _checkReusableSubnet(se database.Storage, service hyperscaler2.GoogleServices, params commonparams.CreatePoolParams, tenantProjectNumber string) (*hyperscaler_models.Subnet, error) {
	var subnet *hyperscaler_models.Subnet
	logger := service.GetLogger()
	snHostProject, err := service.GetSnHost(tenantProjectNumber)
	if err != nil {
		if !strings.Contains(err.Error(), "not found") {
			service.GetLogger().Errorf("Error getting service networking host project for tenant project: %s Error: %v", tenantProjectNumber, err)
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceFetchError, err)
		}
	}
	customerProjectNumber := params.AccountName
	tenantProjectRegion := params.Region
	if snHostProject != "" {
		// if snHost is found, check if the subnetwork already exists in the SN host project and reuse it if applicable
		subnet, err = GetSubnetToBeUsed(service, se, customerProjectNumber, tenantProjectNumber, snHostProject, tenantProjectRegion)
		if err != nil {
			logger.Errorf("Error getting data subnet for tenant project: %s, SN host : %s, Region %s. Error : %s", tenantProjectNumber, snHostProject, tenantProjectRegion, err.Error())
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceFetchError, err)
		}
	}
	return subnet, nil
}

// GetCreateDataSubnetOp creates a subnetwork for the tenant project
func (j *PoolActivity) GetCreateDataSubnetOp(ctx context.Context, params commonparams.CreatePoolParams, tenantProjectNumber string) (*string, error) {
	service, err := hyperscaler2.GetGCPService(ctx)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return GetCreateDataSubnetworkOp(service, params, tenantProjectNumber)
}

func _getCreateDataSubnetworkOp(service hyperscaler2.GoogleServices, params commonparams.CreatePoolParams, tenantProjectNumber string) (*string, error) {
	tenantProjectRegion := params.Region
	consumerVPC := params.VendorSubNetID
	logger := service.GetLogger()
	// if snHost is not found or subnet found cannot be used, create a new subnetwork for the tenant project
	operationName, err := GetCreateSubnetworkOperation(service, tenantProjectNumber, consumerVPC, &tenantProjectRegion)
	if err != nil {
		logger.Errorf("Error creating subnetwork for tenant project: %s, Region %s. Error : %s", tenantProjectNumber, tenantProjectRegion, err.Error())
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceFetchError, err)
	}
	return operationName, err
}

// GetTenancyInfo creates a subnetwork for the tenant project
func (j *PoolActivity) GetTenancyInfo(ctx context.Context, tenantProjectNumber string, subnet *hyperscaler_models.Subnet) (*commonparams.TenancyInfo, error) {
	snHostProject, network, err := utils.ParseProjectId(subnet.Network)
	if err != nil {
		return nil, err
	}
	logger := util.GetLogger(ctx)
	logger.Infof("Subnet used for tenant project: tenantProjectNumber: %s SN host project : %s IpCidrRange: %s, consumerPeeringNetwork: %s", tenantProjectNumber, snHostProject, subnet.IpCidrRange, subnet.Name)
	return &commonparams.TenancyInfo{
		RegionalTenantProject: tenantProjectNumber,
		Network:               network,
		SubnetworkNames:       []string{subnet.Name},
		SnHostProject:         snHostProject,
		Gateway:               subnet.GatewayAddress,
	}, nil
}

// UpdatePoolSubnet updates the subnet name for the pool in the database
func (j *PoolActivity) UpdatePoolSubnet(ctx context.Context, poolUUID string, tenancyDetails commonparams.TenancyInfo) error {
	err := j.SE.UpdatePoolSubnetNames(ctx, poolUUID, tenancyDetails.SnHostProject, tenancyDetails.SubnetworkNames)
	if err != nil {
		return err
	}
	return nil
}

// createSubnetwork generates a subnetwork name based on the tenant project number and region and triggers creation the subnet in SN host project. returns operation name
func _getCreateSubnetworkOperation(service hyperscaler2.GoogleServices, tenantProjectNumber, consumerVPC string, tenantProjectRegion *string) (*string, error) {
	subnetName := MakeSubnetName(tenantProjectNumber)
	operationName, err := service.CreateTPSubnetOp(tenantProjectNumber, consumerVPC, *tenantProjectRegion, subnetName)
	if err != nil {
		service.GetLogger().Errorf("Error adding subnetwork: %v", err)
		return nil, err
	}
	return operationName, err
}

func (j *PoolActivity) CreateVPCs(ctx context.Context, project string) (*[]commonparams.Operations, error) {
	serviceStruct, err := hyperscaler2.GetGCPService(ctx)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	service := hyperscaler2.GoogleServices(serviceStruct)

	// Record heartbeat to indicate progress to temporal server
	activity.RecordHeartbeat(ctx, "Setting up VPC's for VSA pool")
	operations := make([]commonparams.Operations, 0)
	op := ""
	for _, values := range internalVSANetworks {
		// Create VPCs for management, cluster interconnect, and RSM
		op, err = CreateVPC(service, project, values.vpcName)
		if err != nil {
			return nil, vsaerrors.WrapAsTemporalApplicationError(err)
		}
		if op != "" {
			operations = append(operations, commonparams.Operations{
				OperationName:      op,
				OperationType:      "vpc",
				IsDone:             false,
				IsRegionalResource: false,
				Project:            project,
			})
		}
	}
	return &operations, nil
}

func (j *PoolActivity) CreateSubnets(ctx context.Context, project string) (*[]commonparams.Operations, error) {
	serviceStruct, err := hyperscaler2.GetGCPService(ctx)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	service := hyperscaler2.GoogleServices(serviceStruct)

	// Record heartbeat to indicate progress to temporal server
	activity.RecordHeartbeat(ctx, "Setting up Subnets for VSA pool")
	operations := make([]commonparams.Operations, 0)
	op := ""
	for _, values := range internalVSANetworks {
		op, err = InsertSubnet(service, project, &Region, values.subnetName, values.vpcName, values.ipCidrRange)
		if err != nil {
			return nil, vsaerrors.WrapAsTemporalApplicationError(err)
		}
		if op != "" {
			operations = append(operations, commonparams.Operations{
				OperationName:      op,
				OperationType:      "subnet",
				IsDone:             false,
				IsRegionalResource: true,
				Project:            project,
			})
		}
	}
	return &operations, nil
}

func (j *PoolActivity) CreateFirewalls(ctx context.Context, project, snHostProject, network string) (*[]commonparams.Operations, error) {
	serviceStruct, err := hyperscaler2.GetGCPService(ctx)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	service := hyperscaler2.GoogleServices(serviceStruct)
	// Record heartbeat to indicate progress to temporal server
	activity.RecordHeartbeat(ctx, "Setting up Firewall for VSA pool")
	operations := make([]commonparams.Operations, 0)
	op := ""
	for _, values := range internalVSANetworks {
		op, err = InsertFirewall(service, project, values.firewallName, values.vpcName, firewallPriority, ingressTrafficDirection, strings.Split(values.sourceRanges, ","), strings.Split(values.portRules, ","))
		if err != nil {
			return nil, vsaerrors.WrapAsTemporalApplicationError(err)
		}
		if op != "" {
			operations = append(operations, commonparams.Operations{
				OperationName:      op,
				OperationType:      "firewall",
				IsDone:             false,
				IsRegionalResource: false,
				Project:            project,
			})
		}
	}

	// Record heartbeat to indicate progress to temporal server
	activity.RecordHeartbeat(ctx, "Setting up network firewalls for iSCSI")

	op, err = SetupNetworkFirewallsForIscsi(service, snHostProject, network)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	if op != "" {
		operations = append(operations, commonparams.Operations{
			OperationName:      op,
			OperationType:      "firewall",
			IsDone:             false,
			IsRegionalResource: false,
			Project:            snHostProject,
		})
	}
	return &operations, nil
}

func (j *PoolActivity) CreateOnTapCredentials(ctx context.Context, pool *datamodel.Pool, region, clusterName string) (*vlm.OntapCredentials, error) {
	credentials := &vlm.OntapCredentials{}
	gcpService, getGcpServiceErr := hyperscaler2.GetGCPService(ctx)
	if getGcpServiceErr != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrGCPClientInitializationError, getGcpServiceErr))
	}

	switch pool.PoolCredentials.AuthType {
	case env.USER_CERTIFICATE:
		// Generate and create a certificate for the VSA cluster in CAS and fallthrough to generate and create the password for VSA cluster in Secret Manager as well
		certificate, err := hyperscaler2.GenerateAndCreateCertificateForVSACluster(gcpService, region, pool.PoolCredentials.CertificateID, clusterName)
		if err != nil {
			return nil, vsaerrors.WrapAsTemporalApplicationError(err)
		}
		credentials.Certificate.CommonName = certificate.Certificate.SubjectCommonName
		credentials.Certificate.Certificate = certificate.Certificate.PemCertificate
		credentials.Certificate.PrivateKey = certificate.Secret.SecretVersion.Value
		credentials.Certificate.InterMediateCertificate = certificate.Certificate.PemCertificateChain
		fallthrough
	case env.USERNAME_PWD_SEC_MGR:
		secret, err := hyperscaler2.GeneratePasswordForVSACluster(gcpService, env.SecretManagerProjectID, region, pool.PoolCredentials.SecretID)
		if err != nil {
			return nil, vsaerrors.WrapAsTemporalApplicationError(err)
		}
		credentials.AdminPassword = secret.SecretVersion.Value
	default:
		credentials.AdminPassword = pool.PoolCredentials.Password
	}
	return credentials, nil
}

func (j *PoolActivity) DeleteOnTapCredentials(ctx context.Context, pool *datamodel.Pool) error {
	gcpService, err := hyperscaler2.GetGCPService(ctx)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrGCPClientInitializationError, err))
	}
	switch pool.PoolCredentials.AuthType {
	case env.USER_CERTIFICATE:
		// Revoke the certificates and delete the private key from secret manager and cache then fallthrough to delete the password from secret manager and cache
		err = hyperscaler2.RevokeCertificateAndDeleteFromCacheAndSecretManager(gcpService, pool.PoolCredentials.CertificateID)
		if err != nil {
			return vsaerrors.WrapAsTemporalApplicationError(err)
		}
		fallthrough
	case env.USERNAME_PWD_SEC_MGR:
		err = hyperscaler2.DeletePasswordFromCacheAndSecretManager(gcpService, pool.PoolCredentials.SecretID)
		if err != nil {
			return vsaerrors.WrapAsTemporalApplicationError(err)
		}
	default:
		return nil
	}
	return nil
}

func (j *PoolActivity) GetOnTapCredentials(ctx context.Context, pool *datamodel.Pool) (*vlm.OntapCredentials, error) {
	credentials, err := fetchOnTapCredentials(ctx, pool)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return credentials, nil
}

// setupNetworkFirewallsForIscsi sets up a firewall for iSCSI traffic in GCP
func setupNetworkFirewallsForIscsi(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
	return InsertFirewall(service, snHostProject, iscsiDataFirewallName, network, firewallPriority, ingressTrafficDirection, strings.Split(IscsiFirewallSourceRanges, ","), strings.Split(IscsiFirewallPortRules, ","))
}

func (j *PoolActivity) DeployDeploymentManager(ctx context.Context, deploymentName, region, zone, network, subnet, projectId, snHostProject string, size int) (*[]map[string]string, error) {
	return DeploymentsInsert(ctx, deploymentName, region, zone, network, subnet, projectId, snHostProject, size)
}

func (j *PoolActivity) SavePoolWithClusterDetails(ctx context.Context, dbPool *datamodel.Pool, cluster *datamodel.ClusterDetails) error {
	se := j.SE
	err := se.SavePoolWithVsaDetails(ctx, dbPool, cluster)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	return nil
}

func (j *PoolActivity) GetOntapVersion(ctx context.Context, node *models.Node) (*string, error) {
	provider, err := hyperscaler2.GetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	version, err := provider.GetONTAPVersion()
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	return version, nil
}

func (j *PoolActivity) SaveSVMAndLifData(ctx context.Context, pool *datamodel.Pool, vlmConfig *vlm.VLMConfig, svmName string) (*datamodel.Svm, error) {
	se := j.SE

	svm := vlmConfig.Svm[svmName]
	svmRec := &datamodel.Svm{
		Name:      svm.Svmname,
		AccountID: pool.AccountID,
		PoolID:    pool.ID,
		SvmDetails: &datamodel.SvmDetails{
			ExternalUUID: svm.Svmuuid,
			IPSpace:      "Default",
		},
	}

	createdSvm, err := se.CreateSVM(ctx, svmRec)
	if err != nil && !utilErrors.IsConflictErr(err) {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	nodes, err := se.GetNodesByPoolID(ctx, pool.ID)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	if len(nodes) < 2 {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrIncorrectVSAClusterState, errors.New("not enough nodes in the cluster to create LIFs for SVM "+svm.Svmname))
	}
	// create map of nodes with node name as key and node ID as value
	nodeMap := make(map[string]int64)
	for _, node := range nodes {
		if node.Name == "" {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrIncorrectVSAClusterState, errors.New("node name is empty for node ID "+strconv.FormatInt(node.ID, 10)))
		}
		nodeMap[node.Name] = node.ID
	}

	lifs := svm.SVMLIFs[vlm.LIFTypeSan]

	for _, lif := range lifs {
		dataLif := lif.IP
		ip := strings.Split(dataLif, "/")[0]

		// Validate that the HomeNode exists in the nodeMap
		nodeID, exists := nodeMap[lif.HomeNode]
		if !exists {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrIncorrectVSAClusterState, fmt.Errorf("LIF %s references non-existent home node %s", lif.Name, lif.HomeNode))
		}

		lifRec := &datamodel.Lif{
			Name:      lif.Name,
			AccountID: pool.AccountID,
			NodeID:    nodeID,
			LifDetails: &datamodel.LifDetails{
				ExternalUUID: lif.Uuid,
				ProtocolType: string(vlm.LIFTypeSan),
			},
			IPAddress:  ip,
			SubnetMask: vsa.DefaultNetmask,
		}
		if _, err = se.CreateLif(ctx, lifRec); err != nil && !utilErrors.IsConflictErr(err) {
			return nil, vsaerrors.WrapAsTemporalApplicationError(err)
		}
	}

	lifs = svm.SVMLIFs[vlm.LIFTypeNas]
	for _, lif := range lifs {
		dataLif := lif.IP
		ip := strings.Split(dataLif, "/")[0]

		// Validate that the HomeNode exists in the nodeMap
		nodeID, exists := nodeMap[lif.HomeNode]
		if !exists {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrIncorrectVSAClusterState, fmt.Errorf("LIF %s references non-existent home node %s", lif.Name, lif.HomeNode))
		}

		lifRec := &datamodel.Lif{
			Name:      lif.Name,
			AccountID: pool.AccountID,
			NodeID:    nodeID,
			LifDetails: &datamodel.LifDetails{
				ExternalUUID: lif.Uuid,
				ProtocolType: string(vlm.LIFTypeNas),
			},
			IPAddress:  ip,
			SubnetMask: vsa.DefaultNetmask}
		if _, err = se.CreateLif(ctx, lifRec); err != nil && !utilErrors.IsConflictErr(err) {
			return nil, vsaerrors.WrapAsTemporalApplicationError(err)
		}
	}

	return createdSvm, nil
}

// CreateQoSPolicyAndApplyToSVM creates a QoS policy group and applies it to the SVM
func (j *PoolActivity) CreateQoSPolicyAndApplyToSVM(ctx context.Context, pool *datamodel.Pool, svm *datamodel.Svm, node *models.Node) error {
	logger := util.GetLogger(ctx)
	logger.Info("Creating QoS policy and applying to SVM", "svmName", svm.Name, "poolName", pool.Name)

	// Get the provider for the node
	provider, err := hyperscaler2.GetProviderByNode(ctx, node)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Create QoS policy group with default values
	// These values can be made configurable in the future
	qosPolicyName := fmt.Sprintf("%s-qos-policy", svm.Name)
	maxThroughput := pool.PoolAttributes.ThroughputMibps
	maxIOPS := pool.PoolAttributes.Iops

	// Create the QoS policy group
	qosPolicyParams := vsa.CreateQoSGroupPolicyParams{
		Name:          qosPolicyName,
		SvmName:       svm.Name,
		MaxThroughput: maxThroughput,
		MaxIOPS:       maxIOPS,
	}

	qosPolicyResponse, err := provider.CreateQoSGroupPolicy(qosPolicyParams)
	if err != nil {
		logger.Error("Failed to create QoS policy group", "error", err, "policyName", qosPolicyName)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	logger.Info("QoS policy group created successfully", "policyName", qosPolicyResponse.Name, "policyUUID", qosPolicyResponse.UUID)

	// Apply the QoS policy to the SVM
	modifySvmParams := vsa.ModifySVMWithQoSPolicyParams{
		SvmUUID:       svm.SvmDetails.ExternalUUID,
		QoSPolicyName: qosPolicyResponse.Name,
	}

	err = provider.ModifySVMWithQoSPolicy(modifySvmParams)
	if err != nil {
		logger.Error("Failed to apply QoS policy to SVM", "error", err, "svmName", svm.Name, "policyName", qosPolicyResponse.Name)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	logger.Info("QoS policy applied to SVM successfully", "svmName", svm.Name, "policyName", qosPolicyResponse.Name)
	return nil
}

// The IdentifyVMs takes as input the VMRS configuration, the customer requested performance parameters, and the current VLM configuration to identify the optimal VMs to use for the VSA cluster.
func (j *PoolActivity) IdentifyVMs(ctx context.Context, vmrsConfigPath string, customerRequest vmrs.CustomerRequestedPerformance, deploymentName string, locationInfo *commonparams.LocationInfo, tenancyInfo *commonparams.TenancyInfo, saEmail string, autoTierBucket string) (*vlm.VLMConfig, error) {
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

	vlmConfig := &vlm.VLMConfig{}
	decision, err := decisionMaker.FindOptimalVMs(vmrsConfig, customerRequest, vlmConfig)
	if err != nil {
		logger.Error("Failed to identify optimal VMs", "error", err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	subnet := ""
	if len(tenancyInfo.SubnetworkNames) > 0 {
		subnet = tenancyInfo.SubnetworkNames[len(tenancyInfo.SubnetworkNames)-1]
	}

	// Convert the decision to a VLMConfig.
	err = PrepareVlmConfig(vlmConfig, deploymentName, locationInfo.Region, locationInfo.PrimaryZone, locationInfo.SecondaryZone, tenancyInfo.Network, subnet, tenancyInfo.RegionalTenantProject, tenancyInfo.SnHostProject, decision, saEmail, autoTierBucket)
	if err != nil {
		logger.Error("Failed to prepare VLM config", "error", err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	return vlmConfig, nil
}

func _resolveZonesForCluster(gcpService hyperscaler2.GoogleServices, projectNumber, region, primaryZone, secondaryZone, mediatorZone, instanceType string) (string, string, error) {
	if primaryZone == "" || projectNumber == "" || region == "" {
		return "", "", vsaerrors.WrapAsTemporalApplicationError(errors.New("primary zone is not set or project number is empty or region is empty"))
	}
	zones, err := gcpService.GetZones(projectNumber, region)
	if err != nil {
		return "", "", vsaerrors.WrapAsTemporalApplicationError(err)
	}
	// Remove primaryZone from the list
	var availableZones []string
	for _, zone := range zones {
		if zone != primaryZone {
			availableZones = append(availableZones, zone)
		}
	}
	if len(availableZones) < 1 {
		return "", "", vsaerrors.WrapAsTemporalApplicationError(errors.New("no zones available besides primary"))
	}

	// Helper function to validate machine type in a zone
	validateMachineTypeInZone := func(zone string, machineType string) bool {
		isAvailable, err := gcpService.IsMachineTypeAvailable(projectNumber, zone, machineType)
		if err != nil {
			return false
		}
		return isAvailable
	}

	// If secondaryZone is not set, pick the first available zone that supports the instance type as secondary
	if secondaryZone == "" {
		// Find a secondary zone that supports the instance type
		var validSecondaryZone string
		for _, zone := range availableZones {
			if validateMachineTypeInZone(zone, instanceType) {
				validSecondaryZone = zone
				break
			}
		}
		if validSecondaryZone == "" {
			return "", "", vsaerrors.WrapAsTemporalApplicationError(errors.New("no secondary zone found that supports the instance type"))
		}
		secondaryZone = validSecondaryZone
	}

	// If mediatorZone is not set, find one that supports the instance type and is different from secondary
	if mediatorZone == "" {
		for _, zone := range availableZones {
			if zone != secondaryZone && validateMachineTypeInZone(zone, mediatorVmInstanceType) {
				mediatorZone = zone
				break
			}
		}
		if mediatorZone == "" {
			return "", "", vsaerrors.WrapAsTemporalApplicationError(errors.New("no mediator zone found that supports the instance type"))
		}
	} else {
		// If mediatorZone is set, validate it supports the instance type and is different from secondary
		if mediatorZone == secondaryZone {
			return "", "", vsaerrors.WrapAsTemporalApplicationError(errors.New("mediator zone cannot be the same as secondary zone"))
		}
	}

	return secondaryZone, mediatorZone, nil
}

func _prepareVlmConfig(vlmConfig *vlm.VLMConfig, deploymentID, region, primaryZone, secondaryZone, network, subnet, regionalTenantProjectID, snHostProject string, decision *vmrs.Decision, vsaClusterSaEmail string, autoTierBucket string) error {
	if err := ValidateVlmConfigInputs(vlmConfig, decision, deploymentID, region, primaryZone, network, subnet, regionalTenantProjectID, snHostProject, vsaClusterSaEmail); err != nil {
		return err
	}

	// Load the base VLM config from file
	baseConfig, err := LoadVlmConfigFromFile()
	if err != nil {
		return err
	}

	// Merge in base/loaded VLM config to fill out any missing zero fields.
	if err := mergo.Merge(vlmConfig, *baseConfig); err != nil {
		return err
	}

	vsaImageProjectID := vsaImageProject
	if vsaImageProjectID == "" {
		vsaImageProjectID = regionalTenantProjectID
	}

	mediatorImageProjectID := mediatorImageProject
	if mediatorImageProjectID == "" {
		mediatorImageProjectID = regionalTenantProjectID
	}

	vlmConfig.Deployment.GCPConfig = vlm.GCPConfig{
		ProjectID:              regionalTenantProjectID,
		ImageProjectID:         vsaImageProjectID,
		MediatorImageProjectID: mediatorImageProjectID,
		ServiceAccountEmail:    vsaClusterSaEmail,
		BucketName:             autoTierBucket,
	}

	vlmConfig.Deployment.Region = region

	// Enforce minimum values for SPConfig throughput and IOPS if the feature flag is enabled.
	// This ensures that the values do not fall below the required thresholds for VLM worker compatibility.
	if enforceMinSPConfig {
		if decision.StoragePoolRequirements.DesiredThroughputInMiBs < minSPConfigThroughput {
			decision.StoragePoolRequirements.DesiredThroughputInMiBs = minSPConfigThroughput
		}
		if decision.StoragePoolRequirements.DesiredIOPS < minSPConfigIOps {
			decision.StoragePoolRequirements.DesiredIOPS = minSPConfigIOps
		}
	}
	vlmConfig.Deployment.SPConfig.Throughput = decision.StoragePoolRequirements.DesiredThroughputInMiBs
	vlmConfig.Deployment.SPConfig.IOps = decision.StoragePoolRequirements.DesiredIOPS

	vlmConfig.Deployment.SPConfig.Size = fmt.Sprintf("%dGi", decision.StoragePoolRequirements.DesiredCapacityInGiB)
	vlmConfig.Deployment.VSAInstanceType = decision.ChosenVMs[0] // VLM currently only supports a single VM type for VSA clusters (homogeneous clusters).

	vlmConfig.Deployment.DeploymentID = deploymentID
	vlmConfig.Deployment.Zone.Zone1 = primaryZone
	vlmConfig.Deployment.Zone.Zone2 = secondaryZone

	networkConfigs := map[vlm.VSALIFType]struct {
		VPC             string
		Subnet          string
		SubnetProjectID string
	}{
		vlm.LIFTypeNodeMgmt: {mgmtVpcName, mgmtSubnetName, regionalTenantProjectID},
		vlm.LIFTypeIC:       {icVpcName, icSubnet, regionalTenantProjectID},
		vlm.LIFTypeRSM:      {rsmVpcName, rsmSubnetName, regionalTenantProjectID},
	}

	// assign network configurations for each LIF type
	for lifType, config := range networkConfigs {
		assignNetworkConfig(vlmConfig, lifType, config.VPC, config.Subnet, config.SubnetProjectID)
	}

	// assign network configuration for data LIF from snHostProject
	assignNetworkConfig(vlmConfig, vlm.LIFTypeInterCluster, network, subnet, snHostProject)

	// Bootargs for key manager
	vlmConfig.Deployment.UserBootargs = keyManagerBootarg

	vlmConfig.Deployment.MediatorInstanceType = mediatorVmInstanceType
	vlmConfig.Deployment.MediatorDiskType = mediatorVmDiskType

	return nil
}

func assignNetworkConfig(vlmConfig *vlm.VLMConfig, lifType vlm.VSALIFType, vpc, subnet, subnetProjectID string) {
	if vlmConfig.Deployment.NetConfig == nil {
		vlmConfig.Deployment.NetConfig = make(map[vlm.VSALIFType]vlm.NetworkConfig)
	}

	vlmConfig.Deployment.NetConfig[lifType] = vlm.NetworkConfig{
		VPC:              vpc,
		Subnet:           subnet,
		GCPNetworkConfig: vlm.GCPNetworkConfig{SubnetProjectID: subnetProjectID},
	}
}

func _validateVlmConfigInputs(vlmConfig *vlm.VLMConfig, decision *vmrs.Decision, deploymentID, region, primaryZone, network, subnet, regionalTenantProjectID, snHostProject, vsaClusterSaEmail string) error {
	if vlmConfig == nil {
		return errors.New("vlmConfig is nil")
	}

	if decision == nil {
		return errors.New("decision is nil")
	}

	if deploymentID == "" || region == "" || primaryZone == "" || network == "" || subnet == "" || regionalTenantProjectID == "" || snHostProject == "" || vsaClusterSaEmail == "" {
		return errors.New("one or more required string parameters are empty")
	}

	return nil
}

func loadVlmConfigFromFile() (*vlm.VLMConfig, error) {
	vlmConfig := &vlm.VLMConfig{}

	vlmContent, err := ReadFile(VlmConfigFilePath)
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrFileReadError, err)
	}

	err = json.Unmarshal(vlmContent, vlmConfig)
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrFileReadError, err)
	}

	return vlmConfig, nil
}

// AllocateClusterSerialNumber generates and assigns a unique serial number for the VSA cluster.
// It retrieves the next serial number from the database and sets it in the VLMConfig.
// The requirement for node serial number is 20 digits, we would be generating an 18-digit prefix and send it to VLM.
// The last two digits will be generated by VLM, which will be the node index in the cluster.
func (j *PoolActivity) AllocateClusterSerialNumber(ctx context.Context, cfg *vlm.VLMConfig) (*vlm.VLMConfig, error) {
	logger := util.GetLogger(ctx)
	se := j.SE

	// generate unique serial number for the cluster
	err := assignUniqueSerialNumber(ctx, se, cfg)
	if err != nil {
		logger.Error("Failed to assign unique serial number for VSA cluster", "error", err)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGeneratingUniqueSerialNumber, err)
	}
	return cfg, nil
}

// CreateCloudDNSRecords creates DNS records for the VSA cluster's nodes in the cloud DNS service
func (j *PoolActivity) CreateCloudDNSRecords(ctx context.Context, vlmConfig *vlm.VLMConfig, clusterName string, authType int) (*map[string]string, error) {
	hostMap := make(map[string]string)
	if authType == env.USER_CERTIFICATE {
		if len(vlmConfig.Cloud.HAPairs) == 0 {
			return nil, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrIncorrectVSAClusterState, errors.New("no cluster details provided")))
		}
		for i, details := range vlmConfig.Cloud.HAPairs {
			if len(details.VM1.SystemLIFs) == 0 || len(details.VM2.SystemLIFs) == 0 {
				return nil, vsaerrors.WrapAsTemporalApplicationError(
					vsaerrors.NewVCPError(vsaerrors.ErrIncorrectVSAClusterState, errors.New("no system LIFs provided for VMs")))
			}
			gcpService, err := hyperscaler2.GetGCPService(ctx)
			if err != nil {
				return nil, err
			}

			IpaddressVm1 := details.VM1.SystemLIFs[vlm.LIFTypeNodeMgmt].IP
			haPairNode1 := fmt.Sprintf("%s-%d.%s.%s.", "dns", (2*i)+1, clusterName, env.VsaDeployedDnsName)
			record1, err := hyperscaler2.GetOrCreateCloudDNSRecord(gcpService, haPairNode1, IpaddressVm1)
			if err != nil {
				return nil, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceProvisionError, err))
			}
			hostMap[IpaddressVm1] = record1.RecordName

			IpaddressVm2 := details.VM2.SystemLIFs[vlm.LIFTypeNodeMgmt].IP
			haPairNode2 := fmt.Sprintf("%s-%d.%s.%s.", "dns", (2*i)+2, clusterName, env.VsaDeployedDnsName)
			record2, err := hyperscaler2.GetOrCreateCloudDNSRecord(gcpService, haPairNode2, IpaddressVm2)
			if err != nil {
				return nil, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceProvisionError, err))
			}
			hostMap[IpaddressVm2] = record2.RecordName
		}
		return &hostMap, nil
	}
	return &hostMap, nil
}

func (j *PoolActivity) DeleteCloudDNSRecords(ctx context.Context, hostMap map[string]string, authType int) error {
	if authType == env.USER_CERTIFICATE {
		gcpService, err := hyperscaler2.GetGCPService(ctx)
		if err != nil {
			return vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrGCPClientInitializationError, err))
		}
		// Delete entries for each node
		for _, host := range hostMap {
			// Check if the node is already deleted
			err = hyperscaler2.DeleteCloudDNSRecord(gcpService, host)
			if err != nil {
				util.GetLogger(ctx).Errorf("Failed to delete DNS record for host %s: %v", host, err)
				return vsaerrors.WrapAsTemporalApplicationError(err)
			}
		}
	}
	return nil
}

func (j *PoolActivity) GetCloudDNSRecords(ctx context.Context, poolId int64, authType int) (*map[string]string, error) {
	hostMap := make(map[string]string)
	if authType == env.USER_CERTIFICATE {
		se := j.SE
		nodes, err := se.GetNodesByPoolID(ctx, poolId)
		if err != nil {
			return &hostMap, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err))
		}
		if len(nodes) == 0 {
			return &hostMap, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrResourceEmptyError, errors.New("no node found for the pool")))
		}
		for _, node := range nodes {
			hostMap[node.EndpointAddress] = node.HostDNSName
		}
	}
	return &hostMap, nil
}

func (j *PoolActivity) SaveVSANodeDetails(ctx context.Context, pool *datamodel.Pool, vlmConfig *vlm.VLMConfig, deploymentName string, hostMap map[string]string) (node1 *datamodel.Node, err error) {
	if len(vlmConfig.Cloud.HAPairs) == 0 {
		return nil, vsaerrors.WrapAsTemporalApplicationError(
			vsaerrors.NewVCPError(vsaerrors.ErrIncorrectVSAClusterState, errors.New("no cluster details provided")))
	}
	for _, details := range vlmConfig.Cloud.HAPairs {
		node1, err = SaveNodeDetails(ctx, j.SE, details.VM1, vlmConfig.Deployment, pool, deploymentName, hostMap)
		if err != nil {
			return nil, vsaerrors.WrapAsTemporalApplicationError(err)
		}
		_, err = SaveNodeDetails(ctx, j.SE, details.VM2, vlmConfig.Deployment, pool, deploymentName, hostMap)
		if err != nil {
			return nil, vsaerrors.WrapAsTemporalApplicationError(err)
		}
	}
	return node1, nil
}

func _saveNodeDetails(ctx context.Context, se database.Storage, vmConfig vlm.VMConfig, deploymentConfig vlm.DeploymentConfig, pool *datamodel.Pool, deploymentName string, hostMap map[string]string) (*datamodel.Node, error) {
	node := &models.Node{
		Name:                           vmConfig.HostName,
		EndpointAddress:                vmConfig.SystemLIFs[vlm.LIFTypeNodeMgmt].IP,
		Zone:                           vmConfig.Zone,
		InstanceType:                   deploymentConfig.VSAInstanceType,
		DeploymentName:                 deploymentName,
		EndpointAddressesToHostNameMap: hostMap,
		CertificateID:                  pool.PoolCredentials.CertificateID,
		SecretID:                       pool.PoolCredentials.SecretID,
		Password:                       pool.PoolCredentials.Password,
		AuthType:                       pool.PoolCredentials.AuthType,
	}

	provider, err := hyperscaler2.GetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

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
	if pool.PoolCredentials.AuthType == env.USER_CERTIFICATE {
		rec.HostDNSName = hostMap[node.EndpointAddress]
	} else {
		rec.HostDNSName = node.EndpointAddress
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
	dbPool := database.ConvertPoolViewToPool(poolView)
	return dbPool, nil
}

func (j *PoolActivity) GetPoolsByAccountName(ctx context.Context, accountName string) ([]*datamodel.Pool, error) {
	se := j.SE
	pools, err := se.GetPoolsByAccountName(ctx, accountName)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	return pools, err
}

func (j *PoolActivity) GetSvmForPoolID(ctx context.Context, poolID int64) (*datamodel.Svm, error) {
	se := j.SE
	svm, err := se.GetSvmForPoolID(ctx, poolID)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	return svm, nil
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
	// identify the subnet having totalIPPerHAPair IPs and release it
	if len(pool.ClusterDetails.SubnetNames) == 0 {
		logger.Infof("Subnet is not associated with the pool. Skipping release for network: Account : %s Network : %s", pool.Account.Name, pool.Network)
		return vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("subnet is not associated with the pool: %s account : %s", pool.UUID, pool.Account.Name))
	}
	se := j.SE
	subnetName := pool.ClusterDetails.SubnetNames[len(pool.ClusterDetails.SubnetNames)-1]
	poolsUsingSubnet, err := _getPoolsBySubnetwork(ctx, se, strconv.Itoa(int(pool.Account.ID)), subnetName, pool.Network)
	if err != nil {
		logger.Errorf("Failed to list pools for subnetwork: %s for account: %s, network: %s, error: %s", subnetName, pool.Account.Name, pool.Network, err.Error())
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	if len(poolsUsingSubnet) > 1 {
		logger.Infof("Skipping release subnetwork as there are other pools using the same subnetwork: %s for account: %s, network: %s", subnetName, pool.Account.Name, pool.Network)
		return nil
	}
	service, err := hyperscaler2.GetGCPService(ctx)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	err = ReleaseSubnet(service, poolsUsingSubnet[0].ClusterDetails.SnHostProject, subnetName)
	if err != nil {
		logger.Errorf("Failed to release subnetwork: %s for account: %s, network: %s, error: %s", subnetName, pool.Account.Name, pool.Network, err.Error())
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return nil
}

func _releaseSubnet(service hyperscaler2.GoogleServices, snHost, subnetName string) error {
	err := service.ReleaseSubnetwork(Region, snHost, subnetName)
	return err
}

// DeletePoolResources deletes all pool resources and the pool record from the database.
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
	gcpService, err := hyperscaler2.GetGCPService(ctx)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	err = CreateGCPBucket(ctx, projectId, autoTierBucketName, region, gcpService)
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
	if autoTierBucketName == "" {
		// If the bucket name is empty, log a warning and return nil
		logger.Warnf("Skipping autoTiering bucket deletion,cannot delete autoTiering bucket: bucket name is empty")
		return nil
	}
	gcpService, err := hyperscaler2.GetGCPService(ctx)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	logger.Debugf("Deleting autoTiering bucket %v", autoTierBucketName)
	err = DeleteGCPBucket(ctx, autoTierBucketName, gcpService)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	return nil
}

func _createGCPBucket(ctx context.Context, projectId, bucketName, region string, gcpService hyperscaler2.GoogleServices) error {
	logger := gcpService.GetLogger()
	err := gcpService.CreateBucketIfNotExists(ctx, projectId, bucketName, region)
	if err != nil {
		logger.Errorf("error creating bucket: %v", err)
		return vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceAlreadyExistsError, err)
	}
	logger.Infof("Bucket created successfully %s", bucketName)

	return nil
}

func _deleteGCPBucket(ctx context.Context, bucketName string, gcpService hyperscaler2.GoogleServices) error {
	logger := gcpService.GetLogger()
	err := gcpService.DeleteBucket(ctx, bucketName)
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
func (j *PoolActivity) CreateServiceAccountWithStorageRole(ctx context.Context, projectID string, saAccountID string, saDisplayName string) (*hyperscaler_models.ServiceAccount, error) {
	gcpService, err := hyperscaler2.GetGCPService(ctx)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	sa, err := CreateServiceAccountAndAttachRole(ctx, projectID, saAccountID, saDisplayName, gcpService)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	return sa, nil
}

func (j *PoolActivity) DeleteServiceAccount(ctx context.Context, projectID string, saAccountID string) error {
	logger := util.GetLogger(ctx)
	if saAccountID == "" || projectID == "" {
		// If the service account ID or project ID is empty, log a warning and return nil
		logger.Warnf("Skipping service account deletion,cannot delete service account without service account ID or project ID: saAccountID=%s, projectID=%s", saAccountID, projectID)
		return nil
	}
	gcpService, err := hyperscaler2.GetGCPService(ctx)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	err = DeleteSrvcAccount(ctx, projectID, saAccountID, gcpService)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	return nil
}

// getRegionNumber retrieves the region number from the region map JSON string.
func getRegionNumber() string {
	var regionMap map[string]string
	_ = json.Unmarshal([]byte(regionMapJson), &regionMap)
	return regionMap[Region]
}

// assignUniqueSerialNumber assigns a unique serial number to the VLMConfig based on the region.
func assignUniqueSerialNumber(ctx context.Context, se database.Storage, cfg *vlm.VLMConfig) error {
	if RegionNumber == "" {
		return errors.New("region number is not set")
	}

	serialNumber, err := se.GetNextSerialNumberInRegion(ctx, clusterSerialNumberPrefix+RegionNumber)
	if err != nil {
		util.GetLogger(ctx).Error("Failed to get next regional cluster serial number", "error", err)
		return err
	}
	cfg.Deployment.SerialNumberPrefix = serialNumber
	return nil
}

func _deleteServiceAccount(ctx context.Context, projectID string, saAccountID string, gcpService hyperscaler2.GoogleServices) error {
	logger := gcpService.GetLogger()

	saEmail := utils.ConstructServiceAccountEmail(saAccountID, projectID)
	logger.Infof("Deleting service account %s", saEmail)
	err := gcpService.DeleteServiceAccount(saEmail)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return nil
}

func _createServiceAccountAndAttachRole(ctx context.Context, projectID string, saAccountID string, saDisplayName string, gcpService hyperscaler2.GoogleServices) (*hyperscaler_models.ServiceAccount, error) {
	logger := gcpService.GetLogger()
	createReq := &hyperscaler_models.CreateServiceAccountRequest{
		AccountId: saAccountID,
		ServiceAccount: &hyperscaler_models.ServiceAccount{
			DisplayName: saDisplayName,
		},
	}
	saEmail := utils.ConstructServiceAccountEmail(saAccountID, projectID)

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

// _failedSVMs updates svm status to error.
func _failedSVMs(ctx context.Context, se database.Storage, pool *datamodel.Pool) error {
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
			svm.State = models.LifeCycleStateError
			svm.StateDetails = models.LifeCycleStateDeletionErrorDetails
			err = se.ErroredSVM(ctx, svm, models.LifeCycleStateDeletionErrorDetails)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// _failedNodes updates nodes status to error.
func _failedNodes(ctx context.Context, se database.Storage, pool *datamodel.Pool) error {
	// Retrieve the nodes associated with the pool
	nodes, err := se.GetNodesByPoolID(ctx, pool.ID)
	if err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, fmt.Errorf("failed to retrieve nodes for pool %d: %w", pool.ID, err))
	}

	// Delete each node
	for _, node := range nodes {
		// Check if the node is already marked for deletion
		if node.State == models.LifeCycleStateDeleting {
			node.State = models.LifeCycleStateError
			node.StateDetails = models.LifeCycleStateDeletionErrorDetails
			err = se.ErroredNode(ctx, node, models.LifeCycleStateDeletionErrorDetails)
			if err != nil {
				return err
			}
		}
	}
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
func _createVPC(gService hyperscaler2.GoogleServices, projectName, vpcName string) (string, error) {
	logger := gService.GetLogger()
	logger.Debugf("Checking if VPC already exists before creating VPC for project : %s network name : %s", projectName, vpcName)
	vpcNetworkReceived, err := gService.GetVPCNetwork(projectName, vpcName)
	if err != nil {
		resourceNotFound, errReceived := resourceNotFoundCheck(err.Error(), projectName, vpcName, "", "")
		if !resourceNotFound {
			return "", vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceFetchError, errReceived)
		}
	}
	if vpcNetworkReceived != nil {
		logger.Infof("VPC already exists. Skipping creation. project name : %s , vpc name : %s", projectName, vpcName)
		return "", nil
	}
	vpcNetwork := &hyperscaler_models.VPCNetwork{Name: vpcName, ProjectName: projectName}

	logger.Infof("Creating VPC for project name : %s , vpc name : %s", projectName, vpcName)
	return gService.CreateVPC(vpcNetwork)
}

// _insertSubnet invokes create subnetwork call from orchestrator. It is used for creating a subnetwork in GCP for a project with the specified subnet name
func _insertSubnet(gService hyperscaler2.GoogleServices, projectName string, Region *string, subnetName string, vpcName string, ipCidrRange string) (string, error) {
	logger := gService.GetLogger()
	logger.Debugf("Checking if subnet already exists before creating subnet for project : %s  network name : %s firewall name : %s", projectName, vpcName, subnetName)
	subnetReceived, err := gService.GetSubnetwork(projectName, *Region, subnetName)
	if err != nil {
		resourceNotFound, errReceived := resourceNotFoundCheck(err.Error(), projectName, vpcName, subnetName, "")
		if !resourceNotFound {
			return "", vsaerrors.NewVCPError(vsaerrors.ErrResourceNotFound, errReceived)
		}
	}
	if subnetReceived != nil {
		logger.Infof("Subnet already exists. Skipping creation. project name : %s , vpc name : %s, subnet name : %s", projectName, vpcName, subnetName)
		return "", nil
	}
	subnetRequest := &hyperscaler_models.Subnet{
		Name:        subnetName,
		Network:     fmt.Sprintf("projects/%s/global/networks/%s", projectName, vpcName),
		IpCidrRange: ipCidrRange,
		Region:      Region,
		ProjectName: projectName,
	}
	logger.Infof("Creating Subnet for project name : %s , vpc name : %s, subnet name : %s", projectName, vpcName, subnetName)
	return gService.CreateSubnetwork(subnetRequest)
}

// _insertFirewall invokes create firewall call from orchestrator. It is used for creating a firewall in GCP for a project with the specified firewall name
func _insertFirewall(gService hyperscaler2.GoogleServices, projectName, firewallName, vpcName string, priority int64, direction string, firewallSourceRanges, firewallAllowedPortRules []string) (string, error) {
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
	logger.Debugf("Checking if firewall already exists before creating firewall for project : %s  network name : %s firewall name : %s", projectName, vpcName, firewallName)
	existingFirewall, err := gService.GetFirewall(projectName, firewallName)
	if err != nil {
		logger.Debugf("Error getting firewall for project : %s and network name : %s firewall name : %s . Error : %v", projectName, vpcName, firewallName, err)
		resourceNotFound, errReceived := resourceNotFoundCheck(err.Error(), projectName, vpcName, "", firewallName)
		logger.Debugf("Error getting firewall for project : %s and network name : %s firewall name : %s . Error : %v resourceNotFound : %t", projectName, vpcName, firewallName, err, resourceNotFound)
		if !resourceNotFound {
			return "", vsaerrors.NewVCPError(vsaerrors.ErrResourceNotFound, errReceived)
		}
	}
	if existingFirewall != nil {
		return CheckAndUpdateFirewall(gService, existingFirewall, firewallRequest)
	}

	logger.Infof("Creating firewall for project : %s and network name : %s ", projectName, vpcName)
	return gService.InsertFirewall(firewallRequest)
}

// _checkAndUpdateFirewall check if firewall has been updated by checking if all SourceRanges in firewallReceived exist in firewallRequest.SourceRanges
func _checkAndUpdateFirewall(gService hyperscaler2.GoogleServices, existingFirewall, firewallRequest *hyperscaler_models.Firewall) (string, error) {
	needsUpdate := false

	needsUpdate = !utils.IsSliceEqual(firewallRequest.SourceRanges, existingFirewall.SourceRanges)
	if needsUpdate {
		gService.GetLogger().Infof("Updating firewall for project : %s and network name : %s, firewall name : %s ", firewallRequest.ProjectName, firewallRequest.VPCNetworkName, firewallRequest.Name)
		op, err := gService.UpdateFirewall(firewallRequest)
		if err != nil {
			gService.GetLogger().Errorf("Error updating firewall for project : %s and network name : %s firewall name : %s. Error : %v ", firewallRequest.ProjectName, firewallRequest.VPCNetworkName, firewallRequest.Name, err)
			return "", vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceProvisionError, fmt.Errorf("error updating firewall for project: %s and network name: %s firewall name: %s. Error : %v", firewallRequest.ProjectName, firewallRequest.VPCNetworkName, firewallRequest.Name, err))
		}
		return op, err
	}
	gService.GetLogger().Infof("Firewall already exists. Skipping creation. project name : %s , vpc name : %s, firewall name : %s", firewallRequest.ProjectName, firewallRequest.VPCNetworkName, firewallRequest.Name)
	return "", nil
}

// GetServiceNetOpStatus returns the status (and result) of a Google's service networking operation
func (j *PoolActivity) GetServiceNetOpStatus(ctx context.Context, operation string) (*hyperscaler_models.ComputeOperation, error) {
	gcpService, err := hyperscaler2.GetGCPService(ctx)
	if err != nil {
		return nil, err
	}
	return GetServiceNetOpStatus(gcpService, operation)
}

func _getServiceNetOpStatus(gcpService hyperscaler2.GoogleServices, operation string) (*hyperscaler_models.ComputeOperation, error) {
	return gcpService.GetServiceNetOpStatus(operation)
}

// GetSubnetFromOperation returns the status (and result) of a Google's service networking operation
func (j *PoolActivity) GetSubnetFromOperation(ctx context.Context, subnetInBytes []byte) (*hyperscaler_models.Subnet, error) {
	return GetSubnetFromOperation(ctx, subnetInBytes)
}

func _getSubnetFromOperation(ctx context.Context, subnetInBytes []byte) (*hyperscaler_models.Subnet, error) {
	logger := util.GetLogger(ctx)
	if subnetInBytes == nil {
		logger.Error("Operation response is nil, cannot extract subnet")
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceFetchError, vsaerrors.New("operation response is nil"))
	}
	logger.Debugf("subnetInBytes %s", string(subnetInBytes))

	subnetCreated := &servicenetworking.Subnetwork{}
	if err := json.Unmarshal(subnetInBytes, subnetCreated); err != nil {
		logger.Debugf("subnetInBytes json unmarshal error %s", err.Error())
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrJSONParsingError, err)
	}
	gateway, err := GetGatewayFromIpCidrRange(subnetCreated.IpCidrRange)
	if err != nil {
		logger.Errorf("Failed to get gateway from IP CIDR range %s: %v", subnetCreated.IpCidrRange, err)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceFetchError, err)
	}
	return &hyperscaler_models.Subnet{Name: subnetCreated.Name, Network: subnetCreated.Network, GatewayAddress: gateway}, nil
}

func _getGatewayFromIpCidrRange(ipCidrRange string) (string, error) {
	ip, _, err := net.ParseCIDR(ipCidrRange)
	if err != nil {
		return "", err
	}
	ip4 := ip.To4()
	if ip4 == nil {
		return "", fmt.Errorf("IP CIDR range is not an IPv4 address")
	}
	ip4[3] += 1
	return ip4.String(), nil
}

// IdentifySecondaryAndMediatorZone identifies the secondary and mediator zones for a cluster
// and returns the resolved zones.
func (j *PoolActivity) IdentifySecondaryAndMediatorZone(ctx context.Context, projectNumber string, locationInfo *commonparams.LocationInfo, instanceType string) (*commonparams.LocationInfo, error) {
	logger := util.GetLogger(ctx)
	logger.Debug("Identifying secondary and mediator zones for cluster")

	// Get GCP service
	gcpService, err := hyperscaler2.GetGCPService(ctx)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Use ResolveZonesForCluster to get the secondary and mediator zones
	resolvedSecondaryZone, resolvedMediatorZone, err := ResolveZonesForCluster(gcpService, projectNumber, locationInfo.Region, locationInfo.PrimaryZone, locationInfo.SecondaryZone, locationInfo.MediatorZone, instanceType)
	if err != nil {
		logger.Error("Failed to resolve zones for cluster", "error", err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Create and return the updated location info
	updatedLocationInfo := &commonparams.LocationInfo{
		PrimaryZone:   locationInfo.PrimaryZone,
		SecondaryZone: resolvedSecondaryZone,
		Region:        locationInfo.Region,
		MediatorZone:  resolvedMediatorZone,
	}

	logger.Debug("Successfully identified secondary and mediator zones",
		"secondaryZone", resolvedSecondaryZone,
		"mediatorZone", resolvedMediatorZone)

	return updatedLocationInfo, nil
}

func (j *PoolActivity) AllocateSVMName(ctx context.Context, pool *datamodel.Pool) (string, error) {
	// TODO: This function currently just adds a sequence to the SVM name.
	// It will be enhanced later when multiple SVM support is added to handle
	// more sophisticated naming strategies and SVM allocation logic.
	se := j.SE

	// Get the next SVM index directly from the database
	nextSequence, err := se.GetNextSVMIndexByPoolID(ctx, pool.ID)
	if err != nil {
		return "", vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Format the sequence with leading zeros (01, 02, 03, etc.)
	sequenceStr := fmt.Sprintf("%02d", nextSequence)

	// Return SVM name with sequence
	return fmt.Sprintf("%s-svm-%s", pool.DeploymentName, sequenceStr), nil
}

func getFirewallSourceRangesForMgmt() string {
	if MgmtRegionalNatIP != "" {
		return MgmtFirewallSourceRanges + "," + MgmtRegionalNatIP
	}
	return MgmtFirewallSourceRanges
}

// GetComputeOpStatus returns the status (and result) of a Google's compute networking operation for global and regional operations
func (j *PoolActivity) GetComputeOpStatus(ctx context.Context, project string, isRegionalResource bool, operation string) (*hyperscaler_models.ComputeOperation, error) {
	gcpService, err := hyperscaler2.GetGCPService(ctx)
	if err != nil {
		return nil, err
	}
	return GetComputeOpStatus(gcpService, project, isRegionalResource, operation)
}

func _getComputeOpStatus(gcpService hyperscaler2.GoogleServices, project string, isRegionalResource bool, operation string) (*hyperscaler_models.ComputeOperation, error) {
	if !isRegionalResource {
		return gcpService.GetComputeGlobalOpStatus(project, operation)
	}
	return gcpService.GetComputeRegionalOpStatus(project, Region, operation)
}

func fetchOnTapCredentials(ctx context.Context, pool *datamodel.Pool) (*vlm.OntapCredentials, error) {
	credentials := &vlm.OntapCredentials{}
	switch pool.PoolCredentials.AuthType {
	case env.USER_CERTIFICATE:
		certificate, err := hyperscaler2.GetCertificateFromCacheOrSecretManager(ctx, pool.PoolCredentials.CertificateID)
		if err != nil {
			return nil, err
		}
		credentials.Certificate.CommonName = certificate.CommonName
		credentials.Certificate.Certificate = certificate.SignedCertificate
		credentials.Certificate.PrivateKey = certificate.PrivateKey
		credentials.Certificate.InterMediateCertificate = certificate.InterMediateCertificates
		fallthrough
	case env.USERNAME_PWD_SEC_MGR:
		secret, err := hyperscaler2.GetPasswordFromCacheOrSecretManager(ctx, pool.PoolCredentials.SecretID)
		if err != nil {
			return nil, err
		}
		credentials.AdminPassword = secret
	default:
		credentials.AdminPassword = pool.PoolCredentials.Password
	}
	return credentials, nil
}

// GetInterClusterLifsFromVLMConfig retrieves intercluster LIF IP addresses from VLM config
func (j *PoolActivity) GetInterClusterLifsFromVLMConfig(ctx context.Context, vlmConfig *vlm.VLMConfig) ([]string, error) {
	logger := util.GetLogger(ctx)

	logger.Info("Getting intercluster LIFs from VLM config")

	// Extract intercluster LIF IP addresses from VLM config's systemLifs
	var lifIPs []string

	// Iterate through all HA pairs to find intercluster LIFs
	if vlmConfig != nil && len(vlmConfig.Cloud.HAPairs) > 0 {
		for _, haPair := range vlmConfig.Cloud.HAPairs {
			// Check VM1 for intercluster LIFs
			if vm1Lif, exists := haPair.VM1.SystemLIFs[vlm.LIFTypeInterCluster]; exists {
				lifIPs = append(lifIPs, vm1Lif.IP)
				logger.Debug("Found intercluster LIF on VM1", "vmName", haPair.VM1.Name, "ipAddress", vm1Lif.IP)
			}

			// Check VM2 for intercluster LIFs
			if vm2Lif, exists := haPair.VM2.SystemLIFs[vlm.LIFTypeInterCluster]; exists {
				lifIPs = append(lifIPs, vm2Lif.IP)
				logger.Debug("Found intercluster LIF on VM2", "vmName", haPair.VM2.Name, "ipAddress", vm2Lif.IP)
			}
		}
	}

	logger.Info("Extracted intercluster LIF IPs from VLM config", "lifCount", len(lifIPs))
	return lifIPs, nil
}
