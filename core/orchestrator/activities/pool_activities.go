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
	"time"

	"dario.cat/mergo"
	networkpriv "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/priv/client/operations"
	privmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/priv/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/vlm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/hydrationActivities"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vmrs"
	vmrs_config "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vmrs/config"
	vmrs_decision "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vmrs/decision"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	hyperscaler2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/google"
	hyperscaler_models "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	utilErrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/activity"
	"google.golang.org/api/servicenetworking/v1"
	"gorm.io/gorm"
)

const (
	VMsPerHAPair                    = 2
	EnableServerAuthInCSR           = true  // server auth will be enabled in the CSR(Certificate Signing Request), by default client is enabled
	EnableServerAuthInCSRForExpMode = false // only client auth will be enabled in the CSR(Certificate Signing Request)
	certificate                     = "certificate"
	password                        = "password"
)

var (
	DeploymentsInsert                        = common.DeploymentsInsert
	PrepareVlmConfig                         = _prepareVlmConfig
	ReadFile                                 = os.ReadFile
	SaveNodeDetails                          = _saveNodeDetails
	DeleteLIFs                               = _deleteLIFs
	DeleteSVMs                               = _deleteSVMs
	FailedSVMs                               = _failedSVMs
	DeleteNodes                              = _deleteNodes
	FailedNodes                              = _failedNodes
	DeletingNodes                            = _deletingNodes
	DeletingSVMs                             = _deletingSVMs
	CreateVPC                                = _createVPC
	InsertSubnet                             = _insertSubnet
	InsertFirewall                           = _insertFirewall
	GetTenantProject                         = _getTenantProject
	GetCreateDataSubnetworkOp                = _getCreateDataSubnetworkOp
	GetSubnetToBeUsed                        = getSubnetToBeUsed
	SetupNetworkFirewallsForIscsi            = setupNetworkFirewallsForIscsi
	SetupNetworkFirewallsForNFS              = setupNetworkFirewallsForNFS
	SetupNetworkFirewallsForIntercluster     = setupNetworkFirewallsForIntercluster
	SetupNetworkFirewallsForSMB              = setupNetworkFirewallsForSMB
	SetupNetworkFirewallsForIlbHealthCheck   = setupNetworkFirewallsForIlbHealthCheck
	CreateGCPBucket                          = _createGCPBucket
	CheckReusableSubnet                      = _checkReusableSubnet
	CreateServiceAccountAndAttachRole        = _createServiceAccountAndAttachRole
	DeleteServiceAccountAndRemoveStorageRole = _deleteServiceAccountAndRemoveStorageRole
	DeleteGCPBucket                          = _deleteGCPBucket
	LoadVMRSConfig                           = vmrs_config.LoadConfig
	CreateDecisionMaker                      = vmrs_decision.NewDecisionMaker
	CreateLargeVolumeVMRSConfig              = _createLargeVolumeVMRSConfig
	VlmConfigFilePath                        = env.GetString("VLM_CONFIG_FILE_PATH", "/common/vsa_config/vlm-config.json")
	ValidateVlmConfigInputs                  = _validateVlmConfigInputs
	GetCreateSubnetworkOperation             = _getCreateSubnetworkOperation
	ReleaseSubnetOp                          = _releaseSubnetOp
	CheckAndUpdateFirewall                   = _checkAndUpdateFirewall
	LoadVlmConfigFromFile                    = loadVlmConfigFromFile
	GetServiceNetOpStatus                    = _getServiceNetOpStatus
	GetComputeOpStatus                       = _getComputeOpStatus
	GetSubnetFromOperation                   = _getSubnetFromOperation
	GetGatewayFromIpCidrRange                = _getGatewayFromIpCidrRange
	ResolveZonesForCluster                   = _resolveZonesForCluster
	GetInternalVSANetworkForFirewalls        = _getInternalVSANetworkForFirewalls
	ListAddressesByDeployment                = _listAddressesByDeployment
	GetBucketFile                            = _getBucketFile

	// Feature flag to enforce minimum values for SPConfig throughput and IOPS.
	// Set ENFORCE_MIN_SP_CONFIG=true in the environment to enable.
	enforceMinSPConfig       = env.GetBool("ENFORCE_MIN_SP_CONFIG", false)
	VsaImageProject          = env.GetString("VSA_IMAGE_PROJECT", "")
	MediatorImageProject     = env.GetString("VSA_MEDIATOR_IMAGE_PROJECT", "")
	VsaInstanceTypeOverride  = env.GetBool("VSA_INSTANCE_TYPE_OVERRIDE_LSSD", false)
	IsIntegrationTest        = env.GetBool("INTEGRATION_TEST", false)
	maxNestedCloneLimit      = env.GetInt("MAX_NESTED_CLONE_LIMIT", 499)
	ExpertModeRbacBucketName = env.GetString("EXPERT_MODE_RBAC_BUCKET_NAME", "gcnv-autopush-images-bucket")
	ExpertModeRbacFilePath   = env.GetString("EXPERT_MODE_RBAC_FILE_PATH", "GCNV/%s/RBAC/gcnvadmin_create_cli")
	OntapModeRBACChecksums   = env.GetString("ONTAP_MODE_RBAC_CHECKSUMS", "{}")
	ValidateRbacHashFlag     = env.GetBool("VALIDATE_RBAC_HASH", false)

	ValidateImageDigestFlag = env.GetBool("VALIDATE_IMAGE_DIGEST", false)
	VsaImageChecksums       = env.GetString("VSA_IMAGE_CHECKSUMS", "")
	VsaImageName            = env.GetString("VSA_IMAGE_NAME", "")
	MediatorImageName       = env.GetString("VSA_MEDIATOR_IMAGE_NAME", "")
)

const (
	imageVerifiedLabel = "image_digest_verified"
	checksumLabel1     = "checksum1"
	checksumLabel2     = "checksum2"
)

// ValidateVSAZonesForMachineType validates that primary and secondary zones support the VSA instance type
func ValidateVSAZonesForMachineType(gcpService hyperscaler2.GoogleServices, projectNumber, primaryZone, secondaryZone, instanceType string) error {
	// Validate primary zone supports the instance type
	isAvailable, err := gcpService.IsMachineTypeAvailable(projectNumber, primaryZone, instanceType)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("failed to validate machine type availability in primary zone %s: %w", primaryZone, err))
	}
	if !isAvailable {
		return vsaerrors.WrapAsNonRetryableTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrZoneMachineTypeValidation, fmt.Errorf("primary zone %s does not support machine type %s", primaryZone, instanceType)))
	}

	// Validate secondary zone supports the instance type
	isAvailable, err = gcpService.IsMachineTypeAvailable(projectNumber, secondaryZone, instanceType)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("failed to validate machine type availability in secondary zone %s: %w", secondaryZone, err))
	}
	if !isAvailable {
		return vsaerrors.WrapAsNonRetryableTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrZoneMachineTypeValidation, fmt.Errorf("secondary zone %s does not support machine type %s", secondaryZone, instanceType)))
	}

	return nil
}

// ValidateZonesForMachineTypes is an activity method that validates VSA zones support the machine type
func (j *PoolActivity) ValidateZonesForMachineTypes(ctx context.Context, projectNumber, primaryZone, secondaryZone, instanceType string) error {
	activity.RecordHeartbeat(ctx, "Starting ValidateZonesForMachineTypes activity and Getting GCP service")
	gcpService, err := hyperscaler2.GetGCPService(ctx)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("failed to initialize GCP service: %w", err))
	}
	activity.RecordHeartbeat(ctx, "Validating zones %s and %s for machine type %s", primaryZone, secondaryZone, instanceType)
	err = ValidateVSAZonesForMachineType(gcpService, projectNumber, primaryZone, secondaryZone, instanceType)
	if err != nil {
		return err
	}
	activity.RecordHeartbeat(ctx, "Finished ValidateZonesForMachineTypes activity")
	return nil
}

type PoolActivity struct {
	SE database.Storage
}

type InternalVSANetwork struct {
	VpcName     string
	SubnetName  string
	IpCidrRange string
	Firewall    hyperscaler_models.Firewall
}

const (
	FirewallPriority        = 1000
	IngressTrafficDirection = "INGRESS"

	volStyleFlexGroup = "flexgroup"
	volStyleFlexVol   = "flexvol"

	keyManagerBootarg = "bootarg.keymanager.ekmip.svm_context=false"

	MgmtVpcName      = "mgmt-e0a-vpc-01"
	MgmtSubnetName   = "mgmt-e0a-subnet-01"
	MgmtFirewallName = "ingress-" + MgmtVpcName

	IcVpcName      = "ic-e0b-vpc-01"
	IcSubnet       = "ic-e0b-subnet-01"
	IcFirewallName = "ingress-" + IcVpcName

	RsmVpcName      = "rsm-e0c-vpc-01"
	RsmSubnetName   = "rsm-e0c-subnet-01"
	RsmFirewallName = "ingress-" + RsmVpcName

	iscsiDataFirewallName    = "ingress-data-iscsi"
	nfsDataFirewallName      = "ingress-data-nfs"
	interclusterFirewallName = "ingress-intercluster"

	AllowAllPorts = "all"
)

// Minimum allowed values for SPConfig throughput (in MiBs) and IOPS.
// These are enforced only if the feature flag above is enabled.
const (
	minSPConfigThroughput = 1120
	minSPConfigIOps       = 24000
)

var (
	totalIPPerHAPair          = env.GetInt("TOTAL_IP_PER_HA_PAIR", 6)
	mediatorVmInstanceType    = env.GetString("VSA_MEDIATOR_INSTANCE_TYPE", "e2-micro")
	mediatorVmDiskType        = env.GetString("VSA_MEDIATOR_DISK_TYPE", "pd-ssd")
	clusterSerialNumberPrefix = env.GetString("CLUSTER_SERIAL_NUMBER_PREFIX", "935")
	Region                    = env.GetString("LOCAL_REGION", "")
	regionMapJson             = env.GetString("REGION_NUMBER_MAP", `{"africa-south1": "01","asia-east1": "02","asia-east2": "03","asia-northeast1": "04","asia-northeast2": "05","asia-northeast3": "06","asia-south1": "07","asia-south2": "08","asia-southeast1": "09","asia-southeast2": "10","australia-southeast1": "11","australia-southeast2": "12","europe-central2": "13","europe-north1": "14","europe-north2": "15","europe-southwest1": "16","europe-west1": "17","europe-west10": "18","europe-west12": "19","europe-west2": "20","europe-west3": "21","europe-west4": "22","europe-west6": "23","europe-west8": "24","europe-west9": "25","me-central1": "26","me-central2": "27","me-west1": "28","northamerica-northeast1": "29","northamerica-northeast2": "30","northamerica-south1": "31","southamerica-east1": "32","southamerica-west1": "33","us-central1": "34","us-east1": "35","us-east4": "36","us-east5": "37","us-south1": "38","us-west1": "39","us-west2": "40","us-west3": "41","us-west4": "42"}`)
	AggregateName             = env.GetString("AGGREGATE_NAME", "aggr1")

	MgmtFirewallSourceRanges = env.GetString("MGMT_FIREWALL_SOURCE_RANGES", "")
	RsmFirewallSourceRanges  = env.GetString("RSM_FIREWALL_SOURCE_RANGES", "")
	IcFirewallSourceRanges   = env.GetString("IC_FIREWALL_SOURCE_RANGES", "")
	DataFirewallSourceRanges = env.GetString("DATA_FIREWALL_SOURCE_RANGES", "")

	MgmtRegionalNatIP = env.GetString("MGMT_REGIONAL_NAT_IP", "")

	MgmtNetworkIpRange = env.GetString("MGMT_NETWORK_IP_RANGE", "198.18.0.0/20")
	RsmNetworkIpRange  = env.GetString("RSM_NETWORK_IP_RANGE", "198.18.16.0/20")
	IcNetworkIpRange   = env.GetString("IC_NETWORK_IP_RANGE", "198.18.32.0/20")

	MgmtFirewallPortRules         = env.GetString("MGMT_FIREWALL_PORT_RULES", "tcp,22,443")
	RSMFirewallPortRules          = env.GetString("RSM_FIREWALL_PORT_RULES", "tcp,udp")
	IcFirewallPortRules           = env.GetString("IC_FIREWALL_PORT_RULES", "tcp,udp")
	InterclusterFirewallPortRules = env.GetString("INTERCLUSTER_FIREWALL_PORT_RULES", "tcp,10566,11104,11105")

	IscsiFirewallPortRules                       = env.GetString("ISCSI_FIREWALL_PORT_RULES", "tcp,3260")
	NFSFirewallPortRules                         = env.GetString("NFS_FIREWALL_PORT_RULES", "tcp,111,635,2049,4045,63001-65000,udp,111,4046")
	SmbFirewallAllowedPortRulesConfig            = env.GetString("SMB_FIREWALL_ALLOWED_PORT_RULES", "tcp,88,135,139,389,445,464,636,udp,53,88,389,464")
	IlbHealthCheckFirewallSourceRangesConfig     = env.GetString("ILB_HEALTH_CHECK_FIREWALL_SOURCE_RANGES", "130.211.0.0/22,35.191.0.0/16")
	IlbHealthCheckFirewallAllowedPortRulesConfig = env.GetString("ILB_HEALTH_CHECK_FIREWALL_ALLOWED_PORT_RULES", "tcp")
	RegionNumber                                 = getRegionNumber()
)

var InternalVSANetworks = map[string]InternalVSANetwork{
	MgmtVpcName: {VpcName: MgmtVpcName, SubnetName: MgmtSubnetName, IpCidrRange: MgmtNetworkIpRange,
		Firewall: hyperscaler_models.Firewall{Name: MgmtFirewallName, SourceRanges: []string{}, AllowedPortRules: strings.Split(MgmtFirewallPortRules, ",")}},
	IcVpcName: {VpcName: IcVpcName, SubnetName: IcSubnet, IpCidrRange: IcNetworkIpRange,
		Firewall: hyperscaler_models.Firewall{Name: IcFirewallName, SourceRanges: strings.Split(IcFirewallSourceRanges, ","), AllowedPortRules: strings.Split(IcFirewallPortRules, ",")}},
	RsmVpcName: {VpcName: RsmVpcName, SubnetName: RsmSubnetName, IpCidrRange: RsmNetworkIpRange,
		Firewall: hyperscaler_models.Firewall{Name: RsmFirewallName, SourceRanges: strings.Split(RsmFirewallSourceRanges, ","), AllowedPortRules: strings.Split(RSMFirewallPortRules, ",")}},
}

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
	activity.RecordHeartbeat(ctx, "Starting CreatedPool activity")
	se := j.SE
	activity.RecordHeartbeat(ctx, "Marking Pool as ready in the database")
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

	activity.RecordHeartbeat(ctx, "Finished CreatedPool activity")
	return pool, nil
}

// SetWaflMaxVolCloneHier sets the wafl.maxvolclonehier option on the ONTAP cluster
func (j *PoolActivity) SetWaflMaxVolCloneHier(ctx context.Context, node *models.Node) error {
	activity.RecordHeartbeat(ctx, "Initializing WAFL maxvolclonehier configuration")
	logger := util.GetLogger(ctx)
	if node == nil {
		logger.Warnf("SetWaflMaxVolCloneHier: node is nil, skipping")
		return nil
	}

	activity.RecordHeartbeat(ctx, "Getting ONTAP provider")
	provider, err := hyperscaler2.GetProviderByNode(ctx, node)
	if err != nil {
		logger.Errorf("SetWaflMaxVolCloneHier failed to get provider: %v", err)
		return nil
	}

	activity.RecordHeartbeat(ctx, "Creating REST client")
	restClient, err := provider.CreateRESTClient()
	if err != nil {
		logger.Errorf("SetWaflMaxVolCloneHier failed to create REST client: %v", err)
		return nil
	}
	if restClient == nil {
		logger.Warnf("SetWaflMaxVolCloneHier: REST client is nil")
		return nil
	}

	activity.RecordHeartbeat(ctx, "Getting networking client")
	networkingClient := restClient.Networking()
	if networkingClient == nil {
		logger.Warnf("SetWaflMaxVolCloneHier: networking client is nil")
		return nil
	}

	activity.RecordHeartbeat(ctx, "Executing CLI command to set WAFL maxvolclonehier")
	nodeName := "*" // Applying maxvolclonehier to all the available nodes
	cliInput := fmt.Sprintf("system node run -node %s -command options wafl.maxvolclonehier %d", nodeName, maxNestedCloneLimit)
	cliPrivilege := "admin"
	cliExecuteBody := &privmodels.CliExecute{
		Input:     &cliInput,
		Privilege: &cliPrivilege,
	}

	cliParams := networkpriv.NewCliExecuteParamsWithContext(ctx).
		WithBody(cliExecuteBody).
		WithTimeout(30 * time.Second)

	response, err := networkingClient.CliExecute(cliParams)
	if err != nil {
		logger.Errorf("SetWaflMaxVolCloneHier failed to execute CLI command: %v", err)
		return nil
	}
	if response == nil || response.Payload == nil {
		logger.Warnf("SetWaflMaxVolCloneHier received empty response")
		return nil
	}
	activity.RecordHeartbeat(ctx, "WAFL maxvolclonehier configured successfully")

	logger.Infof("wafl.maxvolclonehier updated successfully for node %s to %d, response: %s", nodeName, maxNestedCloneLimit, response.Payload.Output)
	return nil
}

func (j *PoolActivity) ErroredPool(ctx context.Context, pool *datamodel.Pool, errMessage string) (*datamodel.Pool, error) {
	activity.RecordHeartbeat(ctx, "Starting ErroredPool activity")
	se := j.SE
	activity.RecordHeartbeat(ctx, "Marking Pool as error in the database")
	res, err := se.ErroredResource(ctx, pool, errMessage)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	dbPool := res.(*datamodel.Pool)
	activity.RecordHeartbeat(ctx, "Finished ErroredPool activity")
	return dbPool, nil
}

func (j *PoolActivity) DeletePoolResourcesOnRollback(ctx context.Context, pool *datamodel.Pool) error {
	activity.RecordHeartbeat(ctx, "Starting DeletePoolResourcesOnRollback activity")
	se := j.SE

	// Delete LIFs
	activity.RecordHeartbeat(ctx, "Deleting LIFs")
	if err := DeleteLIFs(ctx, se, pool); err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Delete SVMs
	activity.RecordHeartbeat(ctx, "Deleting SVMs")
	if err := DeleteSVMs(ctx, se, pool); err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Delete nodes
	activity.RecordHeartbeat(ctx, "Deleting Nodes")
	if err := DeleteNodes(ctx, se, pool); err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return nil
}

func (j *PoolActivity) UpdatedPool(ctx context.Context, pool *datamodel.Pool) (*datamodel.Pool, error) {
	activity.RecordHeartbeat(ctx, "Starting UpdatedPool activity")
	se := j.SE
	activity.RecordHeartbeat(ctx, "Updating Pool in the database")
	pool, err := se.UpdatedPool(ctx, pool)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	activity.RecordHeartbeat(ctx, "Finished UpdatedPool activity")
	return pool, nil
}

func (j *PoolActivity) ParseVlmConfig(ctx context.Context, pool *datamodel.Pool) (*vlm.VLMConfig, error) {
	activity.RecordHeartbeat(ctx, "Starting ParseVlmConfig activity")
	log := util.GetLogger(ctx)

	currentVlmConfig := &vlm.VLMConfig{}

	activity.RecordHeartbeat(ctx, "Unmarshalling VLM config from pool")
	// First attempt: unmarshal as-is
	if err := json.Unmarshal([]byte(pool.VLMConfig), currentVlmConfig); err != nil {
		log.Errorf("VLM config unmarshal failed after patching for pool %s: %v", pool.Name, err)
		return nil, vsaerrors.WrapAsNonRetryableTemporalApplicationError(
			vsaerrors.NewVCPError(vsaerrors.ErrVLMConfigParseError, err))
	}

	activity.RecordHeartbeat(ctx, "Finished ParseVlmConfig activity")
	return currentVlmConfig, nil
}

func (j *PoolActivity) UpdatedPoolWithVLMConfig(ctx context.Context, pool *datamodel.Pool, vlmConfig vlm.VLMConfig, updatePoolParams *commonparams.UpdatePoolParams) (*datamodel.Pool, error) {
	se := j.SE
	activity.RecordHeartbeat(ctx, "Starting UpdatedPoolWithVLMConfig activity")
	marshalledVlmConfig, err := json.Marshal(vlmConfig)
	if err != nil {
		return nil, err
	}

	// modifying only the required fields
	pool.VLMConfig = string(marshalledVlmConfig)
	pool.SizeInBytes = int64(updatePoolParams.SizeInBytes)
	pool.Description = updatePoolParams.Description
	pool.PoolAttributes.ThroughputMibps = updatePoolParams.TotalThroughputMibps
	if updatePoolParams.TotalIops != nil {
		pool.PoolAttributes.Iops = *updatePoolParams.TotalIops
	}
	if updatePoolParams.Labels != nil {
		pool.PoolAttributes.Labels = updatePoolParams.Labels
	}

	if updatePoolParams.AllowAutoTiering {
		pool.AllowAutoTiering = true
		pool.AutoTieringConfig.HotTierSizeInBytes = int64(updatePoolParams.HotTierSizeInBytes)
		pool.AutoTieringConfig.EnableHotTierAutoResize = updatePoolParams.EnableHotTierAutoResize
	} else {
		// Keep HotTierSizeInBytes in sync with SizeInBytes when AutoTiering is disabled
		pool.AutoTieringConfig.HotTierSizeInBytes = int64(updatePoolParams.SizeInBytes)
	}

	activity.RecordHeartbeat(ctx, "Starting pool update with new VLM config")
	updatedPool, err := se.UpdatedPool(ctx, pool)
	if err != nil {
		return nil, err
	}

	activity.RecordHeartbeat(ctx, "Finished UpdatedPoolWithVLMConfig activity")
	return updatedPool, nil
}

func (j *PoolActivity) UpdateNodesInstanceTypeActivity(ctx context.Context, poolID int64, newInstanceType string) error {
	se := j.SE
	logger := util.GetLogger(ctx)

	logger.Debugf("Updating nodes instance type for pool ID %d to %s", poolID, newInstanceType)
	activity.RecordHeartbeat(ctx, "Starting UpdateNodesInstanceTypeActivity activity")

	err := se.UpdateNodesInstanceType(ctx, poolID, newInstanceType)
	if err != nil {
		logger.Errorf("Failed to update nodes instance type: %v", err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	logger.Infof("Successfully updated nodes instance type for pool ID %d", poolID)
	activity.RecordHeartbeat(ctx, "Finished UpdateNodesInstanceTypeActivity activity")
	return nil
}

func (j *PoolActivity) UpdatingPool(ctx context.Context, pool *datamodel.Pool) (*datamodel.Pool, error) {
	activity.RecordHeartbeat(ctx, "Initializing pool update operation")
	se := j.SE
	pool, err := se.UpdatingPool(ctx, pool)
	activity.RecordHeartbeat(ctx, "Updated pool state in database")
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
			return nil, err
		}
	}
	customerProjectNumber := params.AccountName
	tenantProjectRegion := params.Region
	isLargeCapacity := params.LargeCapacity
	if snHostProject != "" {
		// if snHost is found, check if the subnetwork already exists in the SN host project and reuse it if applicable
		subnet, err = GetSubnetToBeUsed(service, se, customerProjectNumber, tenantProjectNumber, snHostProject, tenantProjectRegion, isLargeCapacity)
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
	logger.Debugf("Handling creation of new subnetwork for pool : %s, tenant project: %s ", params.Name, tenantProjectNumber)
	operationName, err := GetCreateSubnetworkOperation(service, tenantProjectNumber, consumerVPC, &tenantProjectRegion, params.LargeCapacity)
	if err != nil {
		logger.Errorf("Error creating subnetwork for pool: %s tenant project: %s, Region %s. Error : %s", params.Name, tenantProjectNumber, tenantProjectRegion, err.Error())
		return nil, err
	}
	return operationName, err
}

// GetTenancyInfo gets the SN host and populates values in TenancyInfo struct
func (j *PoolActivity) GetTenancyInfo(ctx context.Context, tenantProjectNumber string, subnet *hyperscaler_models.Subnet) (*commonparams.TenancyInfo, error) {
	_, network, err := utils.ParseProjectId(subnet.Network)
	if err != nil {
		return nil, err
	}
	service, err := hyperscaler2.GetGCPService(ctx)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrGCPClientInitializationError, err))
	}
	logger := service.GetLogger()
	snHostProjectID, err := service.GetSnHost(tenantProjectNumber)
	if err != nil {
		return nil, err
	}
	if snHostProjectID == "" {
		logger.Errorf("Failed to find SN host project for tenant project: %s. IpCidrRange: %s, consumerPeeringNetwork: %s", tenantProjectNumber, subnet.IpCidrRange, subnet.Name)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceFetchError, errors.New(fmt.Sprintf("SN host project not found for tenant project : %s ", tenantProjectNumber)))
	}
	logger.Infof("Subnet used for tenant project: tenantProjectNumber: %s SN host project : %s IpCidrRange: %s, consumerPeeringNetwork: %s", tenantProjectNumber, snHostProjectID, subnet.IpCidrRange, subnet.Name)
	return &commonparams.TenancyInfo{
		RegionalTenantProject: tenantProjectNumber,
		Network:               network,
		SubnetworkNames:       []string{subnet.Name},
		SnHostProject:         snHostProjectID,
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
func _getCreateSubnetworkOperation(service hyperscaler2.GoogleServices, tenantProjectNumber, consumerVPC string, tenantProjectRegion *string, isLargeCapacity bool) (*string, error) {
	subnetName := MakeSubnetName(tenantProjectNumber, isLargeCapacity)
	operationName, err := service.CreateTPSubnetOp(tenantProjectNumber, consumerVPC, *tenantProjectRegion, subnetName, isLargeCapacity)
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
	for _, values := range InternalVSANetworks {
		// Create VPCs for management, cluster interconnect, and RSM
		op, err = CreateVPC(service, project, values.VpcName)
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
	for _, values := range InternalVSANetworks {
		op, err = InsertSubnet(service, project, &Region, values.SubnetName, values.VpcName, values.IpCidrRange)
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
	internalVSANetworksLocal := PrepareInternalVSANetworksForFirewall()

	for _, values := range internalVSANetworksLocal {
		op, err = InsertFirewall(service, project, values.Firewall.Name, values.VpcName, values.Firewall.Priority, values.Firewall.Direction, values.Firewall.SourceRanges, values.Firewall.AllowedPortRules)
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

	op, err = SetupNetworkFirewallsForIntercluster(service, snHostProject, network)
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

	if utils.FileProtocolSupported {
		op, err = SetupNetworkFirewallsForNFS(service, snHostProject, network)
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

		op, err = SetupNetworkFirewallsForSMB(service, snHostProject, network)
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

		op, err = SetupNetworkFirewallsForIlbHealthCheck(service, snHostProject, network)
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
	}

	return &operations, nil
}

// PrepareInternalVSANetworksForFirewall adds private and public IPs for management VPC on top of the existing InternalVSANetworks
func PrepareInternalVSANetworksForFirewall() map[string]InternalVSANetwork {
	internalVSANetworksLocal := map[string]InternalVSANetwork{}
	mgmtValues := InternalVSANetworks[MgmtVpcName]

	// private firewall ned no restriction for port rules
	internalVSANetworksLocal[MgmtVpcName+"-1"] = GetInternalVSANetworkForFirewalls(mgmtValues.VpcName, mgmtValues.Firewall.Name+"-1", strings.Split(MgmtFirewallSourceRanges, ","), []string{AllowAllPorts}, FirewallPriority, IngressTrafficDirection)
	// public firewall needs to have restrictions using port rules
	internalVSANetworksLocal[MgmtVpcName+"-2"] = GetInternalVSANetworkForFirewalls(mgmtValues.VpcName, mgmtValues.Firewall.Name+"-2", strings.Split(MgmtRegionalNatIP, ","), mgmtValues.Firewall.AllowedPortRules, FirewallPriority, IngressTrafficDirection)
	internalVSANetworksLocal[IcVpcName] = InternalVSANetworks[IcVpcName]
	internalVSANetworksLocal[RsmVpcName] = InternalVSANetworks[RsmVpcName]
	return internalVSANetworksLocal
}

func _getInternalVSANetworkForFirewalls(vpcName, firewallName string, sourceRanges, portRules []string, priority int64, trafficDirection string) InternalVSANetwork {
	network := InternalVSANetworks[vpcName]
	return InternalVSANetwork{
		VpcName:     network.VpcName,
		SubnetName:  network.SubnetName,
		IpCidrRange: network.IpCidrRange,
		Firewall: hyperscaler_models.Firewall{
			Name:             firewallName,
			SourceRanges:     sourceRanges,
			AllowedPortRules: portRules,
			Priority:         priority,
			Direction:        trafficDirection,
		},
	}
}

// CreateOnTapCredentials creates ONTAP admin credentials for the pool based on the authentication type
func (j *PoolActivity) CreateOnTapCredentials(ctx context.Context, pool *datamodel.Pool) (*vlm.OntapCredentials, error) {
	activity.RecordHeartbeat(ctx, "Starting CreateOnTapCredentials activity")
	credentials := &vlm.OntapCredentials{}
	gcpService, getGcpServiceErr := hyperscaler2.GetGCPService(ctx)
	if getGcpServiceErr != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrGCPClientInitializationError, getGcpServiceErr))
	}

	switch pool.PoolCredentials.AuthType {
	case env.USER_CERTIFICATE:
		activity.RecordHeartbeat(ctx, "Generating and creating certificate for ONTAP credentials")
		// Generate and create a certificate for the VSA cluster in CAS and fallthrough to generate and create the password for VSA cluster in Secret Manager as well
		certificate, err := hyperscaler2.GenerateAndCreateCertificateForVSACluster(gcpService, pool.DeploymentName, pool.PoolCredentials.Username, pool.PoolCredentials, EnableServerAuthInCSR)
		if err != nil {
			return nil, vsaerrors.WrapAsTemporalApplicationError(err)
		}
		credentials = setPoolCredentials(certificate)
		activity.RecordHeartbeat(ctx, "Certificate generated and created successfully")
		fallthrough
	case env.USERNAME_PWD_SEC_MGR:
		activity.RecordHeartbeat(ctx, "Generating password for ONTAP credentials in Secret Manager")
		secret, err := hyperscaler2.GeneratePasswordForVSACluster(gcpService, pool.PoolCredentials.SecretID)
		if err != nil {
			return nil, vsaerrors.WrapAsTemporalApplicationError(err)
		}
		credentials.AdminPassword = secret.SecretVersion.Value
		activity.RecordHeartbeat(ctx, "password generated successfully")
	default:
		activity.RecordHeartbeat(ctx, "Using default password for ONTAP credentials")
		credentials.AdminPassword = pool.PoolCredentials.Password
	}
	activity.RecordHeartbeat(ctx, "Finished CreateOnTapCredentials activity")
	return credentials, nil
}

// CreateExpertModeCredentials creates ONTAP expert mode credentials based on the authentication type
func (j *PoolActivity) CreateExpertModeCredentials(ctx context.Context, pool *datamodel.Pool, clusterName, username string) (*vlm.OntapCredentials, error) {
	activity.RecordHeartbeat(ctx, "Starting CreateExpertModeCredentials activity")
	credentials := &vlm.OntapCredentials{}
	gcpService, getGcpServiceErr := hyperscaler2.GetGCPService(ctx)
	if getGcpServiceErr != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrGCPClientInitializationError, getGcpServiceErr))
	}

	if pool.ExpertModeCredentials == nil || pool.ExpertModeCredentials.ExpertModeCredential == nil || len(pool.ExpertModeCredentials.ExpertModeCredential) == 0 {
		return nil, vsaerrors.WrapAsNonRetryableTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrResourceEmptyError, fmt.Errorf("expert mode credentials are not provided")))
	}
	switch pool.ExpertModeCredentials.ExpertModeCredential[0].AuthType {
	case env.USER_CERTIFICATE:
		activity.RecordHeartbeat(ctx, "Generating and creating certificate for expert mode credentials")
		// Generate and create a certificate for the VSA cluster in CAS and fallthrough to generate and create the password for VSA cluster in Secret Manager as well
		expertPoolCredentials := &datamodel.PoolCredentials{
			CertificateID: pool.ExpertModeCredentials.ExpertModeCredential[0].CertificateID,
		}
		// Use pool's CaURI if available, otherwise it will fallback to env vars
		if pool.PoolCredentials != nil {
			expertPoolCredentials.CaURI = pool.PoolCredentials.CaURI
		}
		// Generate and create certificate for expert mode, that has only client auth in CSR - server auth is not needed for expert mode. Hence, passing EnableServerAuthInCSRForExpMode as false
		certificate, err := hyperscaler2.GenerateAndCreateCertificateForVSACluster(gcpService, clusterName, username, expertPoolCredentials, EnableServerAuthInCSRForExpMode)
		if err != nil {
			return nil, vsaerrors.WrapAsTemporalApplicationError(err)
		}
		credentials = setPoolCredentials(certificate)
		credentials.AdminPassword = "" // Setting empty password as certificate is used for authentication
		activity.RecordHeartbeat(ctx, "Certificate generated and created successfully")
	case env.USERNAME_PWD_SEC_MGR:
		activity.RecordHeartbeat(ctx, "Generating password for expert mode credentials in Secret Manager")
		secret, err := hyperscaler2.GeneratePasswordForVSACluster(gcpService, pool.ExpertModeCredentials.ExpertModeCredential[0].SecretID)
		if err != nil {
			return nil, vsaerrors.WrapAsTemporalApplicationError(err)
		}
		credentials.AdminPassword = secret.SecretVersion.Value
		activity.RecordHeartbeat(ctx, "Password generated successfully")
	default:
		activity.RecordHeartbeat(ctx, "Using default password for expert mode credentials")
		credentials.AdminPassword = pool.ExpertModeCredentials.ExpertModeCredential[0].Password
	}
	activity.RecordHeartbeat(ctx, "Finished CreateExpertModeCredentials activity")
	return credentials, nil
}

func setPoolCredentials(certificate *hyperscaler_models.CustomCertificateResponse) *vlm.OntapCredentials {
	credentials := &vlm.OntapCredentials{}
	credentials.Certificate.CommonName = certificate.Certificate.SubjectCommonName
	credentials.Certificate.Certificate = certificate.Certificate.PemCertificate
	credentials.Certificate.PrivateKey = certificate.Secret.SecretVersion.Value
	credentials.Certificate.InterMediateCertificate = certificate.Certificate.PemCertificateChain
	return credentials
}

// DeleteOnTapCredentials deletes ONTAP admin credentials for the pool based on the authentication type
func (j *PoolActivity) DeleteOnTapCredentials(ctx context.Context, pool *datamodel.Pool) error {
	activity.RecordHeartbeat(ctx, "Starting DeleteOnTapCredentials activity")
	gcpService, err := hyperscaler2.GetGCPService(ctx)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrGCPClientInitializationError, err))
	}
	switch pool.PoolCredentials.AuthType {
	case env.USER_CERTIFICATE:
		activity.RecordHeartbeat(ctx, "Revoking certificate and deleting from Secret Manager")
		// Revoke the certificates and delete the private key from secret manager and cache then fallthrough to delete the password from secret manager and cache
		err = hyperscaler2.RevokeCertificateAndDeleteFromCacheAndSecretManager(gcpService, pool.PoolCredentials)
		if err != nil {
			return vsaerrors.WrapAsTemporalApplicationError(err)
		}
		activity.RecordHeartbeat(ctx, "certificate revoked and deleted successfully")
		fallthrough
	case env.USERNAME_PWD_SEC_MGR:
		activity.RecordHeartbeat(ctx, "Deleting password from Secret Manager")
		err = hyperscaler2.DeletePasswordFromCacheAndSecretManager(gcpService, pool.PoolCredentials.SecretID)
		if err != nil {
			return vsaerrors.WrapAsTemporalApplicationError(err)
		}
		activity.RecordHeartbeat(ctx, "Password deleted successfully")
	default:
		activity.RecordHeartbeat(ctx, "No deletion needed for default password type")
		return nil
	}
	activity.RecordHeartbeat(ctx, "Finished DeleteOnTapCredentials activity")
	return nil
}

// DeleteExpertModeCredentials DeleteOnTapCredentials deletes ONTAP expert mode credentials for the pool based on the authentication type
func (j *PoolActivity) DeleteExpertModeCredentials(ctx context.Context, pool *datamodel.Pool) error {
	activity.RecordHeartbeat(ctx, "Starting DeleteExpertModeCredentials activity")
	gcpService, err := hyperscaler2.GetGCPService(ctx)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrGCPClientInitializationError, err))
	}
	if pool.ExpertModeCredentials == nil || pool.ExpertModeCredentials.ExpertModeCredential == nil || len(pool.ExpertModeCredentials.ExpertModeCredential) == 0 {
		return vsaerrors.WrapAsNonRetryableTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrResourceEmptyError, fmt.Errorf("expert mode credentials are not provided")))
	}
	switch pool.ExpertModeCredentials.ExpertModeCredential[0].AuthType {
	case env.USER_CERTIFICATE:
		activity.RecordHeartbeat(ctx, "Revoking certificate and deleting from Secret Manager for expert mode")
		// Revoke the certificates and delete the private key from secret manager and cache then fallthrough to delete the password from secret manager and cache
		// Create PoolCredentials from ExpertModeCredential, using pool's PoolCredentials for CaURI if available
		expertPoolCredentials := &datamodel.PoolCredentials{
			CertificateID: pool.ExpertModeCredentials.ExpertModeCredential[0].CertificateID,
		}
		if pool.PoolCredentials != nil {
			expertPoolCredentials.CaURI = pool.PoolCredentials.CaURI
		}
		err = hyperscaler2.RevokeCertificateAndDeleteFromCacheAndSecretManager(gcpService, expertPoolCredentials)
		if err != nil {
			return vsaerrors.WrapAsTemporalApplicationError(err)
		}
		activity.RecordHeartbeat(ctx, "certificate revoked and deleted successfully")
	case env.USERNAME_PWD_SEC_MGR:
		activity.RecordHeartbeat(ctx, "Deleting password from Secret Manager for expert mode")
		err = hyperscaler2.DeletePasswordFromCacheAndSecretManager(gcpService, pool.ExpertModeCredentials.ExpertModeCredential[0].SecretID)
		if err != nil {
			return vsaerrors.WrapAsTemporalApplicationError(err)
		}
		activity.RecordHeartbeat(ctx, "Password deleted successfully")
	default:
		activity.RecordHeartbeat(ctx, "No deletion needed for default password type")
		return nil
	}
	activity.RecordHeartbeat(ctx, "Finished DeleteExpertModeCredentials activity")
	return nil
}

// GetOnTapCredentials fetches ONTAP admin credentials for the pool based on the authentication type
func (j *PoolActivity) GetOnTapCredentials(ctx context.Context, pool *datamodel.Pool) (*vlm.OntapCredentials, error) {
	activity.RecordHeartbeat(ctx, "Starting GetOnTapCredentials activity")
	credentials, err := fetchOnTapCredentials(ctx, pool)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	activity.RecordHeartbeat(ctx, "Finished GetOnTapCredentials activity")
	return credentials, nil
}

// GetExpertModeCredentials fetches ONTAP expert mode credentials based on the authentication type
func (j *PoolActivity) GetExpertModeCredentials(ctx context.Context, pool *datamodel.Pool) (*vlm.OntapCredentials, error) {
	activity.RecordHeartbeat(ctx, "Starting GetExpertModeCredentials activity")
	credentials, err := fetchExpertModeCredentials(ctx, pool)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	activity.RecordHeartbeat(ctx, "Finished GetExpertModeCredentials activity")
	return credentials, nil
}

func (j *PoolActivity) PrepareCreateVSAExpertModeReq(vlmConfig vlm.VLMConfig, ontapCredentials vlm.OntapCredentials, expertModeCredentials vlm.OntapCredentials, pool *datamodel.Pool, bucketFileDetails *hyperscaler_models.BucketFileDetails) (*vlm.OntapExpertModeUserConfig, error) {
	createVSAExpertModeRequest := &vlm.OntapExpertModeUserConfig{}
	createVSAExpertModeRequest.VLMConfig = vlmConfig
	createVSAExpertModeRequest.OntapCredentials = ontapCredentials
	createVSAExpertModeRequest.ExpertModeUserCredentials = expertModeCredentials
	if pool.PoolCredentials.AuthType == env.USER_CERTIFICATE {
		createVSAExpertModeRequest.AuthenticationType = certificate
	} else {
		createVSAExpertModeRequest.AuthenticationType = password
	}
	if pool.ExpertModeCredentials == nil || pool.ExpertModeCredentials.ExpertModeCredential == nil || len(pool.ExpertModeCredentials.ExpertModeCredential) == 0 {
		return nil, vsaerrors.WrapAsNonRetryableTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrResourceEmptyError, fmt.Errorf("expert mode credentials are not provided")))
	}
	createVSAExpertModeRequest.Username = pool.ExpertModeCredentials.ExpertModeCredential[0].Username

	if bucketFileDetails == nil || bucketFileDetails.FileHashSHA256 == "" || bucketFileDetails.FileUrl == "" || bucketFileDetails.BucketName == "" {
		return nil, vsaerrors.WrapAsNonRetryableTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrResourceEmptyError, fmt.Errorf("exp mode rbac file details are missing")))
	}
	createVSAExpertModeRequest.RbacFileURL = fmt.Sprintf("gs://%s/%s", bucketFileDetails.BucketName, bucketFileDetails.FileUrl)
	createVSAExpertModeRequest.RbacFileChecksum = bucketFileDetails.FileHashSHA256
	return createVSAExpertModeRequest, nil
}

func (j *PoolActivity) GetRbacHash(ctx context.Context, ontapVersion string) (*hyperscaler_models.BucketFileDetails, error) {
	rbacFileurl := utils.GenerateRbacFilePath(ExpertModeRbacFilePath, ontapVersion)
	gcpService, err := hyperscaler2.GetGCPService(ctx)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrGCPClientInitializationError, err))
	}
	bucketFileDetails, err := GetBucketFile(gcpService, ctx, ExpertModeRbacBucketName, rbacFileurl)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return bucketFileDetails, nil
}

// ValidateRbacHash validates if the hash from GetRbacHash matches the configured checksum in ConfigMap."
func (j *PoolActivity) ValidateRbacHash(ctx context.Context, ontapVersion string, bucketFileDetails *hyperscaler_models.BucketFileDetails) error {
	logger := util.GetLogger(ctx)

	// Skip validation if flag is disabled
	if !ValidateRbacHashFlag {
		logger.Infof("RBAC hash validation is disabled, ontapVersion : %s", ontapVersion)
		return nil
	}

	// If bucketFileDetails is nil or hash is empty, return error
	if bucketFileDetails == nil || bucketFileDetails.FileHashSHA256 == "" {
		return vsaerrors.WrapAsNonRetryableTemporalApplicationError(
			vsaerrors.NewVCPError(vsaerrors.ErrResourceEmptyError, fmt.Errorf("bucket file details or hash is empty")))
	}

	// Read configured checksums from environment
	checksumsConfig := OntapModeRBACChecksums
	if checksumsConfig == "" || checksumsConfig == "{}" {
		errMsg := "ONTAP_MODE_RBAC_CHECKSUMS not configured"
		logger.Error(errMsg, "for ontapVersion", ontapVersion)
		return vsaerrors.WrapAsNonRetryableTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrInputValidationError, errors.New(errMsg)))
	}

	// Parse JSON configuration
	var checksumsMap map[string]string
	if err := json.Unmarshal([]byte(checksumsConfig), &checksumsMap); err != nil {
		logger.Errorf("Failed to parse ONTAP_MODE_RBAC_CHECKSUMS configuration, error : %v", err)
		return vsaerrors.WrapAsNonRetryableTemporalApplicationError(
			vsaerrors.NewVCPError(vsaerrors.ErrJSONParsingError, fmt.Errorf("failed to parse ONTAP_MODE_RBAC_CHECKSUMS configuration: %w", err)))
	}

	// Check if ONTAP version is configured
	configuredChecksum, exists := checksumsMap[ontapVersion]
	if !exists {
		errMsg := fmt.Sprintf("ONTAP version %s not found in ONTAP_MODE_RBAC_CHECKSUMS configuration", ontapVersion)
		logger.Error(errMsg)
		return vsaerrors.WrapAsNonRetryableTemporalApplicationError(
			vsaerrors.NewVCPError(vsaerrors.ErrInputValidationError, errors.New(errMsg)))
	}

	// Compare checksums
	if configuredChecksum != bucketFileDetails.FileHashSHA256 {
		errMsg := fmt.Sprintf("RBAC hash mismatch for ONTAP version %s: expected %s, got %s", ontapVersion, configuredChecksum, bucketFileDetails.FileHashSHA256)
		logger.Error(errMsg)
		return vsaerrors.WrapAsNonRetryableTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrInputValidationError, errors.New(errMsg)))
	}

	logger.Info("RBAC hash validation passed", "ontapVersion", ontapVersion, "hash", bucketFileDetails.FileHashSHA256)
	return nil
}

func _getBucketFile(service hyperscaler2.GoogleServices, ctx context.Context, bucketName string, fileUrl string) (*hyperscaler_models.BucketFileDetails, error) {
	bucketFileDetails, err := service.GetFileFromBucket(ctx, bucketName, fileUrl)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return bucketFileDetails, nil
}

func (j *PoolActivity) UpdateRbacCheckSumInPool(ctx context.Context, pool *datamodel.Pool, bucketFileDetails *hyperscaler_models.BucketFileDetails) error {
	se := j.SE
	// Fetch the latest pool data to avoid overwriting concurrent changes to BuildInfo
	// (e.g., from upgrade workflows that may update VSABuildImage, MediatorBuildImage, etc.)
	latestPool, err := se.GetPoolByUUID(ctx, pool.UUID)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Use the latest BuildInfo to preserve any concurrent updates
	vsaBuildInfo := latestPool.BuildInfo
	if vsaBuildInfo == nil {
		return vsaerrors.WrapAsTemporalApplicationError(errors.New("vsaBuildInfo is nil"))
	}
	vsaBuildInfo.RbacFileHash = bucketFileDetails.FileHashSHA256
	vsaBuildInfo.RbacFileUrl = fmt.Sprintf("gs://%s/%s", bucketFileDetails.BucketName, bucketFileDetails.FileUrl)

	updates := map[string]interface{}{
		"build_info": vsaBuildInfo,
	}
	err = se.UpdatePoolFields(ctx, pool.UUID, updates)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return nil
}

func setupNetworkFirewallsForIntercluster(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
	return InsertFirewall(service, snHostProject, interclusterFirewallName, network, FirewallPriority, IngressTrafficDirection, strings.Split(DataFirewallSourceRanges, ","), strings.Split(InterclusterFirewallPortRules, ","))
}

// setupNetworkFirewallsForIscsi sets up a firewall for iSCSI traffic in GCP
func setupNetworkFirewallsForIscsi(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
	return InsertFirewall(service, snHostProject, iscsiDataFirewallName, network, FirewallPriority, IngressTrafficDirection, strings.Split(DataFirewallSourceRanges, ","), strings.Split(IscsiFirewallPortRules, ","))
}

// setupNetworkFirewallsForNFS sets up a firewall for NFS traffic in GCP
func setupNetworkFirewallsForNFS(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
	return InsertFirewall(service, snHostProject, nfsDataFirewallName, network, FirewallPriority, IngressTrafficDirection, strings.Split(DataFirewallSourceRanges, ","), strings.Split(NFSFirewallPortRules, ","))
}

// setupNetworkFirewallsForSMB sets up a firewall for SMB traffic in GCP
func setupNetworkFirewallsForSMB(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
	return InsertFirewall(service, snHostProject, SmbFirewallName, network, FirewallPriority, IngressTrafficDirection, strings.Split(DataFirewallSourceRanges, ","), strings.Split(SmbFirewallAllowedPortRulesConfig, ","))
}

// setupNetworkFirewallsForIlbHealthCheck sets up a firewall for ILB health check traffic in GCP
func setupNetworkFirewallsForIlbHealthCheck(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
	return InsertFirewall(service, snHostProject, ILBHealthCheckFirewallName, network, FirewallPriority, IngressTrafficDirection, strings.Split(IlbHealthCheckFirewallSourceRangesConfig, ","), strings.Split(IlbHealthCheckFirewallAllowedPortRulesConfig, ","))
}

func (j *PoolActivity) DeployDeploymentManager(ctx context.Context, deploymentName, region, zone, network, subnet, projectId, snHostProject string, size int) (*[]map[string]string, error) {
	return DeploymentsInsert(ctx, deploymentName, region, zone, network, subnet, projectId, snHostProject, size)
}

func (j *PoolActivity) SavePoolWithClusterDetails(ctx context.Context, dbPool *datamodel.Pool, cluster *datamodel.ClusterDetails) error {
	activity.RecordHeartbeat(ctx, "Starting SavePoolWithClusterDetails activity")
	se := j.SE

	activity.RecordHeartbeat(ctx, "Saving pool with VSA details to database")
	err := se.SavePoolWithVsaDetails(ctx, dbPool, cluster)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	activity.RecordHeartbeat(ctx, "Finished SavePoolWithClusterDetails activity")
	return nil
}

func (j *PoolActivity) GetIPsConsumedForSubnet(ctx context.Context, pool datamodel.Pool, tenancyDetails *commonparams.TenancyInfo, region string) (*[]datamodel.SubnetToIPs, error) {
	activity.RecordHeartbeat(ctx, "Starting GetIPsConsumedForSubnet activity")
	logger := util.GetLogger(ctx)
	gcpService, err := hyperscaler2.GetGCPService(ctx)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrGCPClientInitializationError, err))
	}

	// Fetch all addresses with deployment ID filter only (no subnet filter)
	// This avoids the issue with incomplete subnet information in the API response
	activity.RecordHeartbeat(ctx, fmt.Sprintf("Fetching addresses for deployment: %s", pool.DeploymentName))
	addresses, err := ListAddressesByDeployment(gcpService, tenancyDetails.RegionalTenantProject, region, pool.DeploymentName)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	addressCount := 0
	if addresses != nil {
		addressCount = len(*addresses)
	}
	activity.RecordHeartbeat(ctx, fmt.Sprintf("Fetched %d addresses, filtering by subnet", addressCount))

	// If no subnetworkNAme
	if len(tenancyDetails.SubnetworkNames) == 0 {
		logger.Debugf("No subnetwork found for the pool: %s", pool.Name)
		activity.RecordHeartbeat(ctx, "No subnetwork names provided, returning nil")
		return nil, nil
	}

	// Build result with only the target subnet
	subnetToIps := make([]datamodel.SubnetToIPs, 0)
	// Iterate through addresses and filter by the specific subnetwork
	if addresses != nil {
		for _, targetSubnetName := range tenancyDetails.SubnetworkNames {
			activity.RecordHeartbeat(ctx, fmt.Sprintf("Filtering addresses for subnet: %s", targetSubnetName))
			logger.Debugf("Filtering addresses for target subnet: %s", targetSubnetName)
			totalIPs := int64(0)
			for _, address := range *addresses {
				logger.Debugf("Address: %s, SubnetURI: %s, SelfLink: %s", address.AddressName, address.SubnetURI, address.SelfLink)

				// Check if this address belongs to our target subnet
				// Match by subnet name in the SubnetURI or SelfLink
				if strings.HasSuffix(address.SubnetURI, "/"+targetSubnetName) {
					totalIPs++
					logger.Debugf("Address %s matched target subnet %s", address.AddressName, targetSubnetName)
				}
			}
			subnetToIps = append(subnetToIps, datamodel.SubnetToIPs{
				SubnetName:  targetSubnetName,
				IPsReserved: totalIPs,
			})
			logger.Infof("Target subnet %s has %d reserved IPs", targetSubnetName, totalIPs)
		}
	}
	activity.RecordHeartbeat(ctx, "Finished GetIPsConsumedForSubnet activity")
	return &subnetToIps, nil
}

func (j *PoolActivity) GetOntapVersion(ctx context.Context, node *models.Node) (*string, error) {
	activity.RecordHeartbeat(ctx, "Starting GetOntapVersion activity")
	provider, err := hyperscaler2.GetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	activity.RecordHeartbeat(ctx, "Fetching ONTAP version from provider")
	version, err := provider.GetONTAPVersion()
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	activity.RecordHeartbeat(ctx, "Finished GetOntapVersion activity")
	return version, nil
}

func (j *PoolActivity) SaveSVMAndLifData(ctx context.Context, pool *datamodel.Pool, vlmConfig *vlm.VLMConfig, svmName string) (*datamodel.Svm, error) {
	activity.RecordHeartbeat(ctx, "Starting SaveSVMAndLifData activity")
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

	activity.RecordHeartbeat(ctx, "Creating SVM record in database")
	createdSvm, err := se.CreateSVM(ctx, svmRec)
	if err != nil && !utilErrors.IsConflictErr(err) {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	activity.RecordHeartbeat(ctx, "Getting nodes for pool to create LIF records")
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

	createLifs := func(lifType vlm.VSALIFType, protocolType string) error {
		for _, lif := range svm.SVMLIFs[lifType] {
			ip := strings.Split(lif.IP, "/")[0]

			nodeID, exists := nodeMap[lif.HomeNode]
			if !exists {
				return vsaerrors.NewVCPError(vsaerrors.ErrIncorrectVSAClusterState, fmt.Errorf("LIF %s references non-existent home node %s", lif.Name, lif.HomeNode))
			}

			lifRec := &datamodel.Lif{
				Name:      lif.Name,
				AccountID: pool.AccountID,
				NodeID:    nodeID,
				LifDetails: &datamodel.LifDetails{
					ExternalUUID: lif.Uuid,
					ProtocolType: protocolType,
				},
				IPAddress:  ip,
				SubnetMask: vsa.DefaultNetmask,
			}

			activity.RecordHeartbeat(ctx, "Creating LIF record in database")
			if _, err := se.CreateLif(ctx, lifRec); err != nil && !utilErrors.IsConflictErr(err) {
				return vsaerrors.WrapAsTemporalApplicationError(err)
			}
		}
		return nil
	}

	if err := createLifs(vlm.LIFTypeSan, string(vlm.LIFTypeSan)); err != nil {
		return nil, err
	}

	if err := createLifs(vlm.LIFTypeNas, string(vlm.LIFTypeNas)); err != nil {
		return nil, err
	}

	if err := createLifs(vlm.LIFTypeIlbNas, string(vlm.LIFTypeNas)); err != nil {
		return nil, err
	}

	activity.RecordHeartbeat(ctx, "Finished SaveSVMAndLifData activity")
	return createdSvm, nil
}

// applyQoSPolicyToSVM is a utility function that applies a QoS policy to an SVM
// It handles the common logic of getting the provider and applying the policy
func applyQoSPolicyToSVM(ctx context.Context, svm *datamodel.Svm, node *models.Node, qosPolicyName string) error {
	logger := util.GetLogger(ctx)

	// Get the provider for the node
	provider, err := hyperscaler2.GetProviderByNode(ctx, node)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Apply the QoS policy to the SVM
	modifySvmParams := vsa.ModifySVMWithQoSPolicyParams{
		SvmUUID:       svm.SvmDetails.ExternalUUID,
		QoSPolicyName: qosPolicyName,
	}

	err = provider.ModifySVMWithQoSPolicy(modifySvmParams)
	if err != nil {
		logger.Error("Failed to apply QoS policy to SVM", "error", err, "svmName", svm.Name, "policyName", qosPolicyName)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	logger.Info("QoS policy applied to SVM successfully", "svmName", svm.Name, "policyName", qosPolicyName)
	return nil
}

// generateQoSPolicyName generates a consistent QoS policy name for an SVM
func generateQoSPolicyName(svmName string) string {
	return fmt.Sprintf("%s-qos-policy", svmName)
}

// CreateQoSPolicyAndApplyToSVM creates a QoS policy group and applies it to the SVM
// This activity is idempotent - it will check if the QoS policy already exists before creating
func (j *PoolActivity) CreateQoSPolicyAndApplyToSVM(ctx context.Context, pool *datamodel.Pool, svm *datamodel.Svm, node *models.Node) error {
	logger := util.GetLogger(ctx)
	logger.Info("Creating QoS policy and applying to SVM", "svmName", svm.Name, "poolName", pool.Name)

	activity.RecordHeartbeat(ctx, "Starting CreateQoSPolicyAndApplyToSVM activity & Getting ONTAP provider")
	// Get the provider for the node - CA fields are already in the node struct from CreateNodeForProvider()
	provider, err := hyperscaler2.GetProviderByNode(ctx, node)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Create QoS policy group with default values
	// These values can be made configurable in the future
	qosPolicyName := generateQoSPolicyName(svm.Name)
	maxThroughput := pool.PoolAttributes.ThroughputMibps
	maxIOPS := pool.PoolAttributes.Iops

	// Check if the QoS policy already exists (idempotent behavior)
	findQosPolicyParams := vsa.FindQoSGroupPolicyParams{
		Name:    qosPolicyName,
		SvmName: svm.Name,
	}

	activity.RecordHeartbeat(ctx, "Checking for existing QoS policy group")
	existingQosPolicy, err := provider.FindQoSGroupPolicy(findQosPolicyParams)
	if err == nil {
		// QoS policy already exists, check if it matches our requirements
		if existingQosPolicy.MaxThroughput == maxThroughput && existingQosPolicy.MaxIOPS == maxIOPS {
			logger.Info("QoS policy already exists and matches requirements, skipping creation",
				"policyName", qosPolicyName,
				"throughput", existingQosPolicy.MaxThroughput,
				"iops", existingQosPolicy.MaxIOPS)

			activity.RecordHeartbeat(ctx, "Applying QoS policy to SVM")
			// Apply the existing QoS policy to the SVM using the utility function
			return applyQoSPolicyToSVM(ctx, svm, node, existingQosPolicy.Name)
		} else {
			logger.Info("QoS policy already exists but with different values, updating instead",
				"policyName", qosPolicyName,
				"existingThroughput", existingQosPolicy.MaxThroughput,
				"newThroughput", maxThroughput,
				"existingIOPS", existingQosPolicy.MaxIOPS,
				"newIOPS", maxIOPS)

			// Update the existing QoS policy with new values
			updateQosPolicyParams := vsa.UpdateQoSGroupPolicyParams{
				UUID:          existingQosPolicy.UUID,
				Name:          existingQosPolicy.Name,
				SvmName:       existingQosPolicy.SvmName,
				MaxThroughput: maxThroughput,
				MaxIOPS:       maxIOPS,
			}

			activity.RecordHeartbeat(ctx, "Updating existing QoS policy group")
			err = provider.UpdateQoSGroupPolicy(updateQosPolicyParams)
			if err != nil {
				logger.Error("Failed to update existing QoS policy group", "error", err, "policyName", qosPolicyName)
				return vsaerrors.WrapAsTemporalApplicationError(err)
			}

			logger.Info("QoS policy group updated successfully", "policyName", existingQosPolicy.Name, "policyUUID", existingQosPolicy.UUID)

			activity.RecordHeartbeat(ctx, "Applying QoS policy to SVM")
			// Apply the updated QoS policy to the SVM using the utility function
			return applyQoSPolicyToSVM(ctx, svm, node, existingQosPolicy.Name)
		}
	}

	// QoS policy doesn't exist, create it
	logger.Info("QoS policy does not exist, creating new one", "policyName", qosPolicyName)

	// Create the QoS policy group
	// Default to IsShared=true for backward compatibility (shared capacity policy)
	isShared := true
	qosPolicyParams := vsa.CreateQoSGroupPolicyParams{
		Name:          qosPolicyName,
		SvmName:       svm.Name,
		MaxThroughput: maxThroughput,
		MaxIOPS:       maxIOPS,
		IsShared:      &isShared,
	}

	activity.RecordHeartbeat(ctx, "Creating QoS policy group")
	qosPolicyResponse, err := provider.CreateQoSGroupPolicy(qosPolicyParams)
	if err != nil {
		logger.Error("Failed to create QoS policy group", "error", err, "policyName", qosPolicyName)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	logger.Info("QoS policy group created successfully", "policyName", qosPolicyResponse.Name, "policyUUID", qosPolicyResponse.UUID)

	activity.RecordHeartbeat(ctx, "Applying QoS policy to SVM")
	// Apply the QoS policy to the SVM using the utility function
	return applyQoSPolicyToSVM(ctx, svm, node, qosPolicyResponse.Name)
}

// ModifyQoSPolicyAndApplyToSVM modifies an existing QoS policy group and applies it to the SVM if changes are needed
// This activity is idempotent - it will only update the QoS policy if the new requirements differ from the current ones
func (j *PoolActivity) ModifyQoSPolicyAndApplyToSVM(ctx context.Context, pool *datamodel.Pool, node *models.Node, updateParams *commonparams.UpdatePoolParams) error {
	logger := util.GetLogger(ctx)
	logger.Info("Modifying QoS policy and applying to SVM", "poolName", pool.Name)

	activity.RecordHeartbeat(ctx, "Starting ModifyQoSPolicyAndApplyToSVM activity & Getting ONTAP provider")
	// Get the provider for the node - CA fields are already in the node struct from CreateNodeForProvider()
	provider, err := hyperscaler2.GetProviderByNode(ctx, node)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	activity.RecordHeartbeat(ctx, "Finding SVM for pool")
	// Find the SVM related to the pool
	svm, err := j.GetSvmForPoolID(ctx, pool.ID)
	if err != nil {
		logger.Error("Failed to get SVM for pool", "error", err, "poolID", pool.ID)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Construct the QoS policy name (same format as CreateQoSPolicyAndApplyToSVM)
	qosPolicyName := generateQoSPolicyName(svm.Name)

	// Get the new requirements from the update parameters
	newMaxThroughput := updateParams.TotalThroughputMibps
	newMaxIOPS := updateParams.TotalIops

	// Find the existing QoS policy
	findQosPolicyParams := vsa.FindQoSGroupPolicyParams{
		Name:    qosPolicyName,
		SvmName: svm.Name,
	}

	activity.RecordHeartbeat(ctx, "Finding existing QoS policy group")
	existingQosPolicy, err := provider.FindQoSGroupPolicy(findQosPolicyParams)
	if err != nil {
		logger.Error("Failed to find existing QoS policy", "error", err, "policyName", qosPolicyName)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Check if the QoS policy needs to be updated
	if existingQosPolicy.MaxThroughput == newMaxThroughput && existingQosPolicy.MaxIOPS == *newMaxIOPS {
		logger.Info("QoS policy already matches the new requirements, no update needed",
			"policyName", qosPolicyName,
			"currentThroughput", existingQosPolicy.MaxThroughput,
			"newThroughput", newMaxThroughput,
			"currentIOPS", existingQosPolicy.MaxIOPS,
			"newIOPS", newMaxIOPS)
		return nil
	}

	logger.Info("QoS policy needs to be updated",
		"policyName", qosPolicyName,
		"currentThroughput", existingQosPolicy.MaxThroughput,
		"newThroughput", newMaxThroughput,
		"currentIOPS", existingQosPolicy.MaxIOPS,
		"newIOPS", newMaxIOPS)

	// Update the QoS policy with new values
	updateQosPolicyParams := vsa.UpdateQoSGroupPolicyParams{
		UUID:          existingQosPolicy.UUID,
		Name:          existingQosPolicy.Name,
		SvmName:       existingQosPolicy.SvmName,
		MaxThroughput: newMaxThroughput,
		MaxIOPS:       *newMaxIOPS,
	}

	activity.RecordHeartbeat(ctx, "Updating QoS policy group")
	err = provider.UpdateQoSGroupPolicy(updateQosPolicyParams)
	if err != nil {
		logger.Error("Failed to update QoS policy group", "error", err, "policyName", qosPolicyName)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	logger.Info("QoS policy group updated successfully", "policyName", existingQosPolicy.Name, "policyUUID", existingQosPolicy.UUID)

	// Apply the updated QoS policy to the SVM using the utility function
	res := applyQoSPolicyToSVM(ctx, svm, node, existingQosPolicy.Name)
	activity.RecordHeartbeat(ctx, "Finished ModifyQoSPolicyAndApplyToSVM activity")
	return res
}

// ValidateImageDigest validates that configured VSA and mediator image checksums match the ones in the image repository.
func (j *PoolActivity) ValidateImageDigest(ctx context.Context) (bool, error) {
	logger := util.GetLogger(ctx)
	logger.Info("Validating VSA and mediator image checksums")
	activity.RecordHeartbeat(ctx, "Validating VSA and mediator image checksums")

	vsaCfg, medCfg, err := GetImageConfigChecksums()
	if err != nil {
		logger.Error("Failed to get image config checksums", "error", err)
		return false, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	activity.RecordHeartbeat(ctx, "Fetching image checksums from repository")
	gcpService, err := hyperscaler2.GetGCPService(ctx)
	if err != nil {
		return false, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	vsaRepo, medRepo, err := GetImageRepoChecksums(ctx, gcpService)
	if err != nil {
		logger.Error("Failed to get image repo checksums", "error", err)
		return false, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	if vsaCfg != vsaRepo || medCfg != medRepo {
		logger.Error("VSA and mediator image checksums do not match the ones in the image repository")
		return false, vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("VSA image verification failed"))
	}
	logger.Info("Successfully verified VSA and mediator images")
	activity.RecordHeartbeat(ctx, "VSA and mediator image checksums verified")

	return true, nil
}

// GetImageConfigChecksums reads configured checksums from env
func GetImageConfigChecksums() (vsaChecksum string, mediatorChecksum string, err error) {
	if strings.TrimSpace(VsaImageChecksums) != "" && strings.TrimSpace(VsaImageChecksums) != "{}" {
		var payload struct {
			VSAImageChecksum         string `json:"VSA_IMAGE_CHECKSUM"`
			VSAMediatorImageChecksum string `json:"VSA_MEDIATOR_IMAGE_CHECKSUM"`
		}
		if err := json.Unmarshal([]byte(VsaImageChecksums), &payload); err != nil {
			return "", "", fmt.Errorf("failed to unmarshal configured VSA image checksums: %w", err)
		}
		vsaChecksum = strings.TrimSpace(payload.VSAImageChecksum)
		mediatorChecksum = strings.TrimSpace(payload.VSAMediatorImageChecksum)
	}

	if vsaChecksum == "" || mediatorChecksum == "" {
		return "", "", fmt.Errorf("VSA or mediator image checksums are not configured")
	}

	return vsaChecksum, mediatorChecksum, nil
}

// GetImageRepoChecksums fetches md5sum labels for VSA and mediator images from GCE Images API.
func GetImageRepoChecksums(ctx context.Context, gcpService *google.GcpServices) (vsaChecksum string, mediatorChecksum string, err error) {
	if VsaImageProject == "" || VsaImageName == "" {
		return "", "", fmt.Errorf("vsa image details are not configured")
	}

	if MediatorImageProject == "" || MediatorImageName == "" {
		return "", "", fmt.Errorf("mediator image details are not configured")
	}

	vsaCtx, vsaCancel := context.WithTimeout(ctx, 60*time.Second)
	defer vsaCancel()
	vsaLabels, err := gcpService.GetImageLabels(vsaCtx, VsaImageProject, VsaImageName)
	if err != nil {
		return "", "", fmt.Errorf("failed to get VSA image details from repo: %w", err)
	}
	vsaChecksum, err = GetImageChecksum(vsaLabels)
	if err != nil {
		return "", "", fmt.Errorf("failed to get VSA image checksum from repo: %w", err)
	}

	mediatorCtx, mediatorCancel := context.WithTimeout(ctx, 60*time.Second)
	defer mediatorCancel()
	mediatorLabels, err := gcpService.GetImageLabels(mediatorCtx, MediatorImageProject, MediatorImageName)
	if err != nil {
		return "", "", fmt.Errorf("failed to get mediator image details from repo: %w", err)
	}
	mediatorChecksum, err = GetImageChecksum(mediatorLabels)
	if err != nil {
		return "", "", fmt.Errorf("failed to get mediator image checksum from repo: %w", err)
	}

	return vsaChecksum, mediatorChecksum, nil
}

// GetImageChecksum extracts and validates the checksum from image labels.
func GetImageChecksum(labels map[string]string) (string, error) {
	if len(labels) == 0 {
		return "", fmt.Errorf("image labels are empty")
	}

	if v, ok := labels[imageVerifiedLabel]; !ok || strings.ToLower(v) != "true" {
		return "", fmt.Errorf("image digest is not verified in repo")
	}

	checksum1 := labels[checksumLabel1]
	if checksum1 == "" || len(checksum1) != 32 {
		return "", fmt.Errorf("appropriate checksumLabel1 not found in image labels")
	}
	checksum2 := labels[checksumLabel2]
	if checksum2 == "" || len(checksum2) != 32 {
		return "", fmt.Errorf("appropriate checksumLabel2 not found in image labels")
	}
	return checksum1 + checksum2, nil
}

// The IdentifyVMs takes as input the VMRS configuration, the customer requested performance parameters, and the current VLM configuration to identify the optimal VMs to use for the VSA cluster.
func (j *PoolActivity) IdentifyVMs(ctx context.Context, vmrsConfigPath string, customerRequest vmrs.CustomerRequestedPerformance, deploymentName string, locationInfo *commonparams.LocationInfo, tenancyInfo *commonparams.TenancyInfo, saEmail string, autoTierBucket string, isLargeCapacityPool bool) (*vlm.VLMConfig, error) {
	activity.RecordHeartbeat(ctx, "Starting IdentifyVMs activity")
	logger := util.GetLogger(ctx)
	logger.Debug("Identifying VMs to use for VSA cluster")

	activity.RecordHeartbeat(ctx, "Loading VMRS Config")
	// Parse VMRS config.
	vmrsConfig, err := LoadVMRSConfig(vmrsConfigPath)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Identify the right VMs to use using the selection strategy defined in the VMRS config.
	// For large capacity pools, force the use of the large volume cluster strategy.
	var decisionMaker vmrs.DecisionMaker
	if isLargeCapacityPool {
		// Force large volume cluster strategy for large capacity pools
		largeVolumeConfig := CreateLargeVolumeVMRSConfig(vmrsConfig)
		decisionMaker, err = CreateDecisionMaker(largeVolumeConfig)
	} else {
		decisionMaker, err = CreateDecisionMaker(vmrsConfig)
	}
	if err != nil {
		logger.Error("Failed to create decision maker", "error", err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	activity.RecordHeartbeat(ctx, "Finding optimal VMs")
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

	activity.RecordHeartbeat(ctx, "Preparing VLM config")
	// Convert the decision to a VLMConfig.
	err = PrepareVlmConfig(vlmConfig, deploymentName, locationInfo.Region, locationInfo.PrimaryZone, locationInfo.SecondaryZone, tenancyInfo.Network, subnet, tenancyInfo.RegionalTenantProject, tenancyInfo.SnHostProject, decision, saEmail, autoTierBucket)
	if err != nil {
		logger.Error("Failed to prepare VLM config", "error", err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	if isLargeCapacityPool {
		vlmConfig.Deployment.NumHAPair = decision.ClusterMetadata.NumHAPairs
	}

	activity.RecordHeartbeat(ctx, "Finished IdentifyVMs activity")
	return vlmConfig, nil
}

func _resolveZonesForCluster(gcpService hyperscaler2.GoogleServices, projectNumber, region, primaryZone, secondaryZone, mediatorZone, instanceType string, isRegionalHA bool) (string, string, error) {
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

	// Validate that primary zone supports the instance type
	isAvailable, err := gcpService.IsMachineTypeAvailable(projectNumber, primaryZone, instanceType)
	if err != nil {
		return "", "", vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("failed to validate machine type availability in primary zone %s: %w", primaryZone, err))
	}
	if !isAvailable {
		return "", "", vsaerrors.WrapAsNonRetryableTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrZoneMachineTypeValidation, fmt.Errorf("primary zone %s does not support machine type %s", primaryZone, instanceType)))
	}

	// If secondaryZone is not set, pick the first available zone that supports the instance type as secondary
	if secondaryZone == "" {
		// Find a secondary zone that supports the instance type
		var validSecondaryZone string
		for _, zone := range availableZones {
			isAvailable, err := gcpService.IsMachineTypeAvailable(projectNumber, zone, instanceType)
			if err != nil {
				return "", "", vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("failed to validate machine type availability in zone %s: %w", zone, err))
			}
			if isAvailable {
				validSecondaryZone = zone
				break
			}
		}
		if validSecondaryZone == "" {
			return "", "", vsaerrors.WrapAsTemporalApplicationError(errors.New("no secondary zone found that supports the instance type"))
		}
		secondaryZone = validSecondaryZone
	} else {
		// If secondaryZone is set, validate it supports the instance type
		isAvailable, err := gcpService.IsMachineTypeAvailable(projectNumber, secondaryZone, instanceType)
		if err != nil {
			return "", "", vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("failed to validate machine type availability in secondary zone %s: %w", secondaryZone, err))
		}
		if !isAvailable {
			return "", "", vsaerrors.WrapAsNonRetryableTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrZoneMachineTypeValidation, fmt.Errorf("secondary zone %s does not support machine type %s", secondaryZone, instanceType)))
		}
	}

	if !isRegionalHA {
		mediatorZone = primaryZone
		// Validate that primary zone supports the mediator instance type when used as mediator
		isAvailable, err := gcpService.IsMachineTypeAvailable(projectNumber, primaryZone, mediatorVmInstanceType)
		if err != nil {
			return "", "", vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("failed to validate mediator machine type availability in primary zone %s: %w", primaryZone, err))
		}
		if !isAvailable {
			return "", "", vsaerrors.WrapAsNonRetryableTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrZoneMachineTypeValidation, fmt.Errorf("primary zone %s does not support mediator machine type %s", primaryZone, mediatorVmInstanceType)))
		}
	}
	// If mediatorZone is not set, find one that supports the instance type and is different from secondary
	if mediatorZone == "" {
		for _, zone := range availableZones {
			if zone != secondaryZone {
				isAvailable, err := gcpService.IsMachineTypeAvailable(projectNumber, zone, mediatorVmInstanceType)
				if err != nil {
					return "", "", vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("failed to validate mediator machine type availability in zone %s: %w", zone, err))
				}
				if isAvailable {
					mediatorZone = zone
					break
				}
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
		// Validate that the set mediator zone supports the mediator instance type
		isAvailable, err := gcpService.IsMachineTypeAvailable(projectNumber, mediatorZone, mediatorVmInstanceType)
		if err != nil {
			return "", "", vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("failed to validate mediator machine type availability in mediator zone %s: %w", mediatorZone, err))
		}
		if !isAvailable {
			return "", "", vsaerrors.WrapAsNonRetryableTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrZoneMachineTypeValidation, fmt.Errorf("mediator zone %s does not support machine type %s", mediatorZone, mediatorVmInstanceType)))
		}
	}

	return secondaryZone, mediatorZone, nil
}

func _mockVlmConfig(vlmConfig *vlm.VLMConfig) (*vlm.VLMConfig, error) {
	mockOntapIP := env.GetString("MOCK_ONTAP_IP", "")
	if mockOntapIP == "" {
		return vlmConfig, errors.New("MOCK_ONTAP_IP environment variable is not set for integration tests")
	}
	ogConfig := vlmConfig.Cloud.HAPairs[0].VM1.SystemLIFs[vlm.LIFTypeNodeMgmt]
	ogConfig.IP = mockOntapIP
	vlmConfig.Cloud.HAPairs[0].VM1.SystemLIFs[vlm.LIFTypeNodeMgmt] = ogConfig
	ogConfig = vlmConfig.Cloud.HAPairs[0].VM2.SystemLIFs[vlm.LIFTypeNodeMgmt]
	ogConfig.IP = mockOntapIP
	vlmConfig.Cloud.HAPairs[0].VM2.SystemLIFs[vlm.LIFTypeNodeMgmt] = ogConfig
	return vlmConfig, nil
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

	vsaImageProjectID := VsaImageProject
	if vsaImageProjectID == "" {
		vsaImageProjectID = regionalTenantProjectID
	}

	mediatorImageProjectID := MediatorImageProject
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

	// Mock ONTAP server if integration tests
	if IsIntegrationTest {
		vlmConfig, err = _mockVlmConfig(vlmConfig)
		if err != nil {
			return err
		}
	}

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
	if VsaInstanceTypeOverride {
		vlmConfig.Deployment.VSAInstanceType = strings.TrimSuffix(decision.ChosenVMs[0], "-lssd") // Remove the "-lssd" suffix if it exists, as the region does not support SSDs.
	}

	vlmConfig.Deployment.DeploymentID = deploymentID
	vlmConfig.Deployment.Zone.Zone1 = primaryZone
	vlmConfig.Deployment.Zone.Zone2 = secondaryZone

	networkConfigs := map[vlm.VSALIFType]struct {
		VPC             string
		Subnet          string
		SubnetProjectID string
	}{
		vlm.LIFTypeNodeMgmt: {MgmtVpcName, MgmtSubnetName, regionalTenantProjectID},
		vlm.LIFTypeIC:       {IcVpcName, IcSubnet, regionalTenantProjectID},
		vlm.LIFTypeRSM:      {RsmVpcName, RsmSubnetName, regionalTenantProjectID},
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

// AllocateClusterSerialNumber generates and assigns a unique 20-digit serial number for the VSA cluster.
// It retrieves the next serial number from the database and sets it in the VLMConfig.
// The serial number is 20 digits: the first 3 digits are a fixed prefix (935), the next 2 digits are the region code (up to 99 regions, currently 42 in use),
// and the remaining 15 digits are a per-region counter. All 20 digits are generated and assigned by the control plane; there is no reservation for VLM.
func (j *PoolActivity) AllocateClusterSerialNumber(ctx context.Context, cfg *vlm.CreateVSAClusterDeploymentRequest) (*vlm.CreateVSAClusterDeploymentRequest, error) {
	activity.RecordHeartbeat(ctx, "Starting AllocateClusterSerialNumber activity")
	logger := util.GetLogger(ctx)
	se := j.SE

	activity.RecordHeartbeat(ctx, "Generating unique serial number for VSA cluster")
	// generate unique serial number for the cluster
	err := assignUniqueSerialNumber(ctx, se, cfg)
	if err != nil {
		logger.Error("Failed to assign unique serial number for VSA cluster", "error", err)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGeneratingUniqueSerialNumber, err)
	}

	activity.RecordHeartbeat(ctx, "Finished AllocateClusterSerialNumber activity")
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
				return nil, vsaerrors.WrapAsTemporalApplicationError(err)
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
			return vsaerrors.WrapAsTemporalApplicationError(err)
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
		for _, node := range nodes {
			hostMap[node.EndpointAddress] = node.HostDNSName
		}
	}
	return &hostMap, nil
}

func (j *PoolActivity) SaveVSANodeDetails(ctx context.Context, pool *datamodel.Pool, vlmConfig *vlm.VLMConfig, deploymentName string, hostMap map[string]string) (node1 *datamodel.Node, err error) {
	activity.RecordHeartbeat(ctx, "Starting SaveVSANodeDetails activity")
	if len(vlmConfig.Cloud.HAPairs) == 0 {
		return nil, vsaerrors.WrapAsTemporalApplicationError(
			vsaerrors.NewVCPError(vsaerrors.ErrIncorrectVSAClusterState, errors.New("no cluster details provided")))
	}
	for _, details := range vlmConfig.Cloud.HAPairs {
		activity.RecordHeartbeat(ctx, "Saving node details for VM1")
		node1, err = SaveNodeDetails(ctx, j.SE, details.VM1, vlmConfig.Deployment, pool, deploymentName, hostMap)
		if err != nil {
			return nil, vsaerrors.WrapAsTemporalApplicationError(err)
		}
		activity.RecordHeartbeat(ctx, "Saving node details for VM2")
		_, err = SaveNodeDetails(ctx, j.SE, details.VM2, vlmConfig.Deployment, pool, deploymentName, hostMap)
		if err != nil {
			return nil, vsaerrors.WrapAsTemporalApplicationError(err)
		}
	}
	activity.RecordHeartbeat(ctx, "Finished SaveVSANodeDetails activity")
	return node1, nil
}

func _saveNodeDetails(ctx context.Context, se database.Storage, vmConfig vlm.VMConfig, deploymentConfig vlm.DeploymentConfig, pool *datamodel.Pool, deploymentName string, hostMap map[string]string) (*datamodel.Node, error) {
	// Build CA URI from pool credentials
	caURI := env.BuildCaURI("", "", "")
	if pool.PoolCredentials != nil {
		caURI = pool.PoolCredentials.GetCaURIWithFallback()
	}

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
		CaURI:                          caURI,
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
		AccountID:       pool.AccountID,
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
	activity.RecordHeartbeat(ctx, "Starting GetPool activity")
	se := j.SE
	poolView, err := se.GetPool(ctx, pool.UUID, pool.AccountID)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	dbPool := database.ConvertPoolViewToPool(poolView)
	activity.RecordHeartbeat(ctx, "Finished GetPool activity")
	return dbPool, nil
}

func (j *PoolActivity) GetPoolView(ctx context.Context, pool *datamodel.Pool) (*datamodel.PoolView, error) {
	se := j.SE
	poolView, err := se.GetPool(ctx, pool.UUID, pool.AccountID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrPoolNotFound, errors.New("pool not found"))
		}
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return poolView, nil
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
	activity.RecordHeartbeat(ctx, "Starting DeletingPoolResources activity")
	se := j.SE

	activity.RecordHeartbeat(ctx, "Deleting SVMs")
	// Update SVM, and Pool States to Deleting
	if err := DeletingSVMs(ctx, se, pool); err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	activity.RecordHeartbeat(ctx, "Deleting Nodes")
	if err := DeletingNodes(ctx, se, pool); err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	activity.RecordHeartbeat(ctx, "Finished DeletingPoolResources activity")
	return pool, nil
}

func (j *PoolActivity) ReleaseDataSubnetOp(ctx context.Context, pool *datamodel.Pool) (*[]commonparams.Operations, error) {
	logger := util.GetLogger(ctx)
	logger.Infof("Handling conditions for releasing data subnet for pool: %s Account : %s Network : %s", pool.Name, pool.Account.Name, pool.Network)
	// identify the subnet having totalIPPerHAPair IPs and release it
	if len(pool.ClusterDetails.SubnetNames) == 0 {
		logger.Infof("Subnet is not associated with the pool: %s. Skipping release for network: Account : %s Network : %s", pool.Name, pool.Account.Name, pool.Network)
		return nil, nil
	}
	se := j.SE
	subnetName := pool.ClusterDetails.SubnetNames[len(pool.ClusterDetails.SubnetNames)-1]
	poolsUsingSubnet, err := getPoolsBySubnetwork(ctx, se, strconv.Itoa(int(pool.Account.ID)), subnetName, pool.Network)
	if err != nil {
		logger.Errorf("Failed to list pools for pool: %s subnetwork: %s for account: %s, network: %s, error: %s", pool.Name, subnetName, pool.Account.Name, pool.Network, err.Error())
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	logger.Infof("Found %d pools using the same subnetwork: %s for account: %s, network: %s", len(poolsUsingSubnet), subnetName, pool.Account.Name, pool.Network)
	if len(poolsUsingSubnet) > 1 {
		logger.Infof("Skipping release subnetwork as there are other pools using the same subnetwork: %s for account: %s, network: %s, pool : %s", subnetName, pool.Account.Name, pool.Network, pool.Name)
		return nil, nil
	}
	service, err := hyperscaler2.GetGCPService(ctx)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	operations := make([]commonparams.Operations, 0)
	operationName, err := ReleaseSubnetOp(service, poolsUsingSubnet[0].ClusterDetails.SnHostProject, subnetName)
	if err != nil {
		logger.Errorf("Failed to create operation for release subnetwork: %s for account: %s, pool: %s, network: %s, error: %s", subnetName, pool.Account.Name, pool.Name, pool.Network, err.Error())
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	if operationName != "" {
		operations = append(operations, commonparams.Operations{
			OperationName:      operationName,
			Project:            poolsUsingSubnet[0].ClusterDetails.SnHostProject,
			IsDone:             false,
			IsRegionalResource: true,
		})
	}
	return &operations, nil
}

func _releaseSubnetOp(service hyperscaler2.GoogleServices, projectId, subnetName string) (string, error) {
	return service.ReleaseSubnetworkOp(Region, projectId, subnetName)
}

// DeletePoolResources deletes all pool resources and the pool record from the database.
func (j *PoolActivity) DeletePoolResources(ctx context.Context, pool *datamodel.Pool) (*datamodel.Pool, error) {
	se := j.SE
	activity.RecordHeartbeat(ctx, "Starting DeletePoolResources activity")

	activity.RecordHeartbeat(ctx, "Deleting LIFs")
	// Delete LIFs
	if err := DeleteLIFs(ctx, se, pool); err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	activity.RecordHeartbeat(ctx, "Deleting SVMs")
	// Delete SVMs
	if err := DeleteSVMs(ctx, se, pool); err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	activity.RecordHeartbeat(ctx, "Deleting Nodes")
	// Delete nodes
	if err := DeleteNodes(ctx, se, pool); err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	activity.RecordHeartbeat(ctx, "Deleting Pool")
	// Delete the pool itself from a database
	if err := se.DeletePool(ctx, pool); err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	activity.RecordHeartbeat(ctx, "Finished DeletePoolResources activity")
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
	activity.RecordHeartbeat(ctx, "Initializing auto-tier bucket creation")
	gcpService, err := hyperscaler2.GetGCPService(ctx)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	activity.RecordHeartbeat(ctx, "Creating auto-tier bucket in GCP")
	err = CreateGCPBucket(ctx, projectId, autoTierBucketName, region, gcpService)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	activity.RecordHeartbeat(ctx, "Auto-tier bucket created successfully")

	return nil
}

// DeleteAutoTierBucket deletes the specified GCP bucket used for auto-tiering.
// It initializes a GCP service client and attempts to delete the bucket.
// Returns an error if the deletion fails or if GCP service initialization fails.
func (j *PoolActivity) DeleteAutoTierBucket(ctx context.Context, autoTierBucketName string, accountName string, poolID int64) error {
	activity.RecordHeartbeat(ctx, "Initializing auto-tier bucket deletion")
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

	activity.RecordHeartbeat(ctx, "Deleting auto-tier bucket from GCP")
	logger.Debugf("Deleting autoTiering bucket %v", autoTierBucketName)
	isDeleted, err := DeleteGCPBucket(ctx, autoTierBucketName, gcpService)
	if !isDeleted {
		activity.RecordHeartbeat(ctx, "Bucket deletion pending, creating pending resource deletion entry")
		var errorMessage string
		if err != nil {
			errorMessage = err.Error()
		} else {
			errorMessage = ""
		}
		_, err := j.SE.CreatePendingResourceDeletion(ctx, models.ResourceTypeStringBucket, autoTierBucketName, errorMessage, accountName, poolID)
		if err != nil {
			logger.Errorf("Failed to insert the bucket entry which needs to be cleaned up for bucket %s: %v",
				autoTierBucketName, err)
			// TODO: Alert about persistent failure to insert pending resource deletion for auto-tiering bucket.
		}
	}
	activity.RecordHeartbeat(ctx, "DeleteAutoTierBucket activity completed successfully")

	return nil
}

func _createGCPBucket(ctx context.Context, projectId, bucketName, region string, gcpService hyperscaler2.GoogleServices) error {
	logger := gcpService.GetLogger()
	err := gcpService.CreateBucketIfNotExists(ctx, projectId, bucketName, region, nil)
	if err != nil {
		logger.Errorf("error creating bucket: %v", err)
		return vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceAlreadyExistsError, err)
	}
	logger.Infof("Bucket created successfully %s", bucketName)

	return nil
}

func _deleteGCPBucket(ctx context.Context, bucketName string, gcpService hyperscaler2.GoogleServices) (bool, error) {
	logger := gcpService.GetLogger()
	isDeleted, err := gcpService.DeleteBucketWithLifecyclePolicy(ctx, bucketName)
	if err != nil {
		logger.Errorf("error deleting bucket: %v", err)
		return isDeleted, vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceDeprovisionError, err)
	}
	logger.Infof("Bucket deleted successfully %s", bucketName)

	return isDeleted, nil
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
	activity.RecordHeartbeat(ctx, "Initializing service account creation with storage role")
	gcpService, err := hyperscaler2.GetGCPService(ctx)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	activity.RecordHeartbeat(ctx, "Creating service account and attaching storage role")
	sa, err := CreateServiceAccountAndAttachRole(ctx, projectID, saAccountID, saDisplayName, gcpService)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	activity.RecordHeartbeat(ctx, "Service account created successfully with storage role")
	return sa, nil
}

func (j *PoolActivity) DeleteServiceAccount(ctx context.Context, projectID string, saAccountID string) error {
	activity.RecordHeartbeat(ctx, "Initializing service account deletion")
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

	activity.RecordHeartbeat(ctx, "Removing storage role and deleting service account")
	err = DeleteServiceAccountAndRemoveStorageRole(ctx, projectID, saAccountID, gcpService)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	activity.RecordHeartbeat(ctx, "Service account deleted successfully")
	return nil
}

// getRegionNumber retrieves the region number from the region map JSON string.
func getRegionNumber() string {
	var regionMap map[string]string
	_ = json.Unmarshal([]byte(regionMapJson), &regionMap)
	return regionMap[Region]
}

// assignUniqueSerialNumber assigns a unique serial number to the VLMConfig based on the region.
func assignUniqueSerialNumber(ctx context.Context, se database.Storage, cfg *vlm.CreateVSAClusterDeploymentRequest) error {
	if RegionNumber == "" {
		return errors.New("region number is not set")
	}

	if cfg.VLMConfig.Deployment.NumHAPair < 1 {
		return errors.New("HA pairs count must be at least 1")
	}

	// Generate serial number prefix for number of ha pairs * VMsPerHAPair (for each VM in the HA pair).
	var serials []string
	for range cfg.VLMConfig.Deployment.NumHAPair * VMsPerHAPair {
		serialNumber, err := se.GetNextSerialNumberInRegion(ctx, clusterSerialNumberPrefix+RegionNumber)
		if err != nil {
			util.GetLogger(ctx).Error("Failed to get next regional cluster serial number", "error", err)
			return err
		}
		serials = append(serials, serialNumber)
	}

	// Need to set the SerialNumberPrefix to empty otherwise VMSerialNumbers will be ignored by VLM.
	cfg.VLMConfig.Deployment.SerialNumberPrefix = ""
	cfg.VLMConfig.Deployment.VMSerialNumbers = serials

	return nil
}

func _deleteServiceAccountAndRemoveStorageRole(ctx context.Context, projectNumber string, saAccountID string, gcpService hyperscaler2.GoogleServices) error {
	logger := gcpService.GetLogger()

	saEmail := utils.ConstructServiceAccountEmail(saAccountID, projectNumber)
	logger.Infof("Deleting service account %s in project %s", saEmail, projectNumber)

	roles := []string{"roles/storage.objectUser"}
	err := gcpService.RemoveRolesFromServiceAccounts(roles, saEmail, projectNumber)
	if err != nil {
		logger.Errorf("Failed to remove roles from service account %s: %v.", saEmail, err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	logger.Infof("Successfully removed roles from service account %s", saEmail)

	err = gcpService.DeleteServiceAccount(projectNumber, saEmail)
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
	logger.Infof("Successfully created service account %s with roles %v", saEmail, roles)

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

	// return if there are no nodes(that means no lifs are there
	if len(nodes) == 0 {
		return nil
	}

	nodeIds := make([]int64, 0, len(nodes))
	for _, node := range nodes {
		nodeIds = append(nodeIds, node.ID)
	}
	// Retrieve the LIFs associated with the Node
	lifs, err := se.GetLifsForNodesWithProtocol(ctx, nodeIds, pool.AccountID, "")
	if err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, fmt.Errorf("failed to retrieve LIFs for pool %d: %w", pool.ID, err))
	}
	// Loop over to delete each LIF
	for _, lif := range lifs {
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

func resourceNotFoundCheck(errorString string, projectName, vpcName, subnetName, addressName, firewall string) (bool, error) {
	if !strings.Contains(errorString, "not found") {
		errorMessage := fmt.Sprintf("Error getting vpc for project: %s and vpc name: %s. Error : %s", projectName, vpcName, errorString)
		if subnetName != "" {
			errorMessage = fmt.Sprintf("Error getting subnet for project: %s, vpc name: %s, subnet name: %s. Error : %s", projectName, vpcName, subnetName, errorString)
		}
		if firewall != "" {
			errorMessage = fmt.Sprintf("Error getting subnet for project: %s, vpc name: %s, firewall name: %s. Error : %s", projectName, vpcName, firewall, errorString)
		}
		if addressName != "" {
			errorMessage = fmt.Sprintf("Error getting address/forwarding rule for project: %s, address name: %s. Error : %s", projectName, addressName, errorString)
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
		resourceNotFound, errReceived := resourceNotFoundCheck(err.Error(), projectName, vpcName, "", "", "")
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
		resourceNotFound, errReceived := resourceNotFoundCheck(err.Error(), projectName, vpcName, subnetName, "", "")
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
		resourceNotFound, errReceived := resourceNotFoundCheck(err.Error(), projectName, vpcName, "", "", firewallName)
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
	activity.RecordHeartbeat(ctx, fmt.Sprintf("Data Subnet creation in Hyperscaler: %s", operation))
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
func (j *PoolActivity) IdentifySecondaryAndMediatorZone(ctx context.Context, projectNumber string, locationInfo *commonparams.LocationInfo, instanceType string, isRegionalHA bool) (*commonparams.LocationInfo, error) {
	activity.RecordHeartbeat(ctx, "Starting IdentifySecondaryAndMediatorZone activity")
	logger := util.GetLogger(ctx)
	logger.Debug("Identifying secondary and mediator zones for cluster")

	activity.RecordHeartbeat(ctx, "Getting GCP service")
	// Get GCP service
	gcpService, err := hyperscaler2.GetGCPService(ctx)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	activity.RecordHeartbeat(ctx, "Getting secondary and mediator zones")
	// Use ResolveZonesForCluster to get the secondary and mediator zones
	resolvedSecondaryZone, resolvedMediatorZone, err := ResolveZonesForCluster(gcpService, projectNumber, locationInfo.Region, locationInfo.PrimaryZone, locationInfo.SecondaryZone, locationInfo.MediatorZone, instanceType, isRegionalHA)
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

	activity.RecordHeartbeat(ctx, "Finished IdentifySecondaryAndMediatorZone activity")
	return updatedLocationInfo, nil
}

func (j *PoolActivity) AllocateSVMName(ctx context.Context, pool *datamodel.Pool) (string, error) {
	// TODO: This function currently just adds a sequence to the SVM name.
	// It will be enhanced later when multiple SVM support is added to handle
	// more sophisticated naming strategies and SVM allocation logic.
	activity.RecordHeartbeat(ctx, "Starting AllocateSVMName activity")
	se := j.SE

	activity.RecordHeartbeat(ctx, "Getting next SVM index for pool")
	// Get the next SVM index directly from the database
	nextSequence, err := se.GetNextSVMIndexByPoolID(ctx, pool.ID)
	if err != nil {
		return "", vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Format the sequence with leading zeros (01, 02, 03, etc.)
	sequenceStr := fmt.Sprintf("%02d", nextSequence)

	// SVM name with sequence
	svmName := fmt.Sprintf("%s-svm-%s", pool.DeploymentName, sequenceStr)

	activity.RecordHeartbeat(ctx, "Finished AllocateSVMName activity")
	return svmName, nil
}

// GetComputeOpStatus returns the status (and result) of a Google's compute networking operation for global and regional operations
func (j *PoolActivity) GetComputeOpStatus(ctx context.Context, project string, isRegionalResource bool, operation string) (*hyperscaler_models.ComputeOperation, error) {
	// Record heartbeat to indicate progress during polling
	activity.RecordHeartbeat(ctx, fmt.Sprintf("Hyperscaler operation status for operation name: %s in project: %s", operation, project))
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
		certificate, err := hyperscaler2.GetCertificateFromCacheOrSecretManager(ctx, pool.PoolCredentials)
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

func fetchExpertModeCredentials(ctx context.Context, pool *datamodel.Pool) (*vlm.OntapCredentials, error) {
	credentials := &vlm.OntapCredentials{}
	if pool.ExpertModeCredentials == nil || pool.ExpertModeCredentials.ExpertModeCredential == nil || len(pool.ExpertModeCredentials.ExpertModeCredential) == 0 {
		return nil, vsaerrors.WrapAsNonRetryableTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrResourceEmptyError, fmt.Errorf("expert mode credentials are not provided")))
	}
	switch pool.ExpertModeCredentials.ExpertModeCredential[0].AuthType {
	case env.USER_CERTIFICATE:
		// Create PoolCredentials from ExpertModeCredential, using pool's PoolCredentials for CaURI if available
		expertPoolCredentials := &datamodel.PoolCredentials{
			CertificateID: pool.ExpertModeCredentials.ExpertModeCredential[0].CertificateID,
		}
		if pool.PoolCredentials != nil {
			expertPoolCredentials.CaURI = pool.PoolCredentials.CaURI
		}
		certificate, err := hyperscaler2.GetCertificateFromCacheOrSecretManager(ctx, expertPoolCredentials)
		if err != nil {
			return nil, err
		}
		credentials.Certificate.CommonName = certificate.CommonName
		credentials.Certificate.Certificate = certificate.SignedCertificate
		credentials.Certificate.PrivateKey = certificate.PrivateKey
		credentials.Certificate.InterMediateCertificate = certificate.InterMediateCertificates
	case env.USERNAME_PWD_SEC_MGR:
		secret, err := hyperscaler2.GetPasswordFromCacheOrSecretManager(ctx, pool.ExpertModeCredentials.ExpertModeCredential[0].SecretID)
		if err != nil {
			return nil, err
		}
		credentials.AdminPassword = secret
	default:
		credentials.AdminPassword = pool.ExpertModeCredentials.ExpertModeCredential[0].Password
	}
	return credentials, nil
}

// GetInterClusterLifsFromVLMConfig retrieves intercluster LIF IP addresses from VLM config
func (j *PoolActivity) GetInterClusterLifsFromVLMConfig(ctx context.Context, vlmConfig *vlm.VLMConfig) ([]string, error) {
	activity.RecordHeartbeat(ctx, "Starting GetInterClusterLifsFromVLMConfig activity")
	logger := util.GetLogger(ctx)

	logger.Info("Getting intercluster LIFs from VLM config")

	// Extract intercluster LIF IP addresses from VLM config's systemLifs
	var lifIPs []string

	// Iterate through all HA pairs to find intercluster LIFs
	if vlmConfig != nil && len(vlmConfig.Cloud.HAPairs) > 0 {
		activity.RecordHeartbeat(ctx, fmt.Sprintf("Processing %d HA pairs to extract intercluster LIFs", len(vlmConfig.Cloud.HAPairs)))
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
	activity.RecordHeartbeat(ctx, "Finished GetInterClusterLifsFromVLMConfig activity")
	return lifIPs, nil
}

// DetermineVMScalingDirection determines whether the new VM decision represents scaling up or down
// by using the decision maker's comparison method.
// Returns true if scaling up (new VM is more expensive), false if scaling down (new VM is cheaper).
func (j *PoolActivity) DetermineVMScalingDirection(ctx context.Context, vmrsConfigPath string, currentInstanceType string, newInstanceType string) (bool, error) {
	activity.RecordHeartbeat(ctx, "Starting DetermineVMScalingDirection activity")
	logger := util.GetLogger(ctx)
	logger.Debug("Determining VM scaling direction", "currentType", currentInstanceType, "newType", newInstanceType)

	activity.RecordHeartbeat(ctx, "Load VMRS config")
	// Parse VMRS config to get access to decision maker
	vmrsConfig, err := LoadVMRSConfig(vmrsConfigPath)
	if err != nil {
		return false, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	activity.RecordHeartbeat(ctx, "Create decision maker")
	// Create decision maker to access the comparison method
	decisionMaker, err := CreateDecisionMaker(vmrsConfig)
	if err != nil {
		logger.Error("Failed to create decision maker", "error", err)
		return false, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	activity.RecordHeartbeat(ctx, "Comparing VM scaling direction")
	// Use the decision maker's comparison method directly
	// This eliminates the need for type casting and makes the code more maintainable
	isScalingUp, err := decisionMaker.CompareVMScalingDirection(currentInstanceType, newInstanceType)
	if err != nil {
		logger.Error("Failed to compare VM scaling direction", "error", err)
		return false, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	logger.Info("VM scaling direction determined",
		"currentType", currentInstanceType,
		"newType", newInstanceType,
		"isScalingUp", isScalingUp)

	activity.RecordHeartbeat(ctx, "Finished DetermineVMScalingDirection activity")
	return isScalingUp, nil
}

// UpdatePoolFields updates specific fields of a pool without changing its state
// This is a generic method that can be used to update any combination of pool fields
func (j *PoolActivity) UpdatePoolFields(ctx context.Context, poolUUID string, updates map[string]interface{}) error {
	activity.RecordHeartbeat(ctx, "Starting UpdatePoolFields activity")
	se := j.SE

	activity.RecordHeartbeat(ctx, "Updating pool fields")
	err := se.UpdatePoolFields(ctx, poolUUID, updates)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	activity.RecordHeartbeat(ctx, "Finished UpdatePoolFields activity")
	return nil
}

// _createLargeVolumeVMRSConfig creates a copy of the provided VMRS configuration and
// modifies it to use the large volume cluster selection strategy.
// This ensures that large capacity pools always use the appropriate decision maker
// regardless of the original configuration strategy.
func _createLargeVolumeVMRSConfig(originalConfig *vmrs.VMRSConfig) *vmrs.VMRSConfig {
	// Create a copy of the configuration to avoid modifying the original
	configCopy := *originalConfig

	// Override the VM selection strategy for large volume deployments
	configCopy.HyperscalerPerfLimits.VMSelectionStrategy = vmrs.LeastCostLargeVolumeCluster

	return &configCopy
}

func (j *PoolActivity) HydrateUpdatedPoolToCCFE(ctx context.Context, dbPool datamodel.Pool) error {
	activity.RecordHeartbeat(ctx, "Initializing pool hydration to CCFE")
	logger := util.GetLogger(ctx)

	if !hydrationEnabled {
		logger.Warn("Hydration is disabled, skipping pool hydration to CCFE")
		return nil
	}

	activity.RecordHeartbeat(ctx, "Hydrating updated pool to CCFE")
	err := hydrationActivities.HydrateUpdatedPoolToCCFE(ctx, dbPool)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	activity.RecordHeartbeat(ctx, "Pool hydrated to CCFE successfully")
	return nil
}

// Add this function
func _listAddressesByDeployment(gcpService hyperscaler2.GoogleServices, projectName, region, deploymentID string) (*[]hyperscaler_models.Address, error) {
	return gcpService.ListAddressesByDeployment(projectName, region, deploymentID)
}

// FetchPoolData fetches pool data from database and parses VLM config
func (a *PoolActivity) FetchPoolData(ctx context.Context, input FetchPoolDataActivityInput) (*FetchPoolDataActivityOutput, error) {
	logger := util.GetLogger(ctx)
	logger.Info("Starting pool data fetch", "poolUUID", input.PoolUUID, "accountID", input.AccountID)

	// Record activity heartbeat
	activity.RecordHeartbeat(ctx, "Starting pool data fetch")

	// Fetch the pool from database
	poolView, err := a.SE.GetPool(ctx, input.PoolUUID, input.AccountID)
	if err != nil {
		logger.Error("Failed to fetch pool", "poolUUID", input.PoolUUID, "error", err)
		return &FetchPoolDataActivityOutput{
			PoolUUID: input.PoolUUID,
			Success:  false,
			Error:    fmt.Sprintf("Failed to fetch pool: %v", err),
		}, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}

	// Record activity heartbeat
	activity.RecordHeartbeat(ctx, "Pool fetched, parsing VLM config")

	// Parse VLM config from pool
	var vlmConfig vlm.VLMConfig
	if poolView.VLMConfig != "" {
		err = json.Unmarshal([]byte(poolView.VLMConfig), &vlmConfig)
		if err != nil {
			logger.Error("Failed to parse VLM config", "poolUUID", input.PoolUUID, "error", err)
			return &FetchPoolDataActivityOutput{
				PoolUUID: input.PoolUUID,
				Success:  false,
				Error:    fmt.Sprintf("Failed to parse VLM config: %v", err),
			}, vsaerrors.NewVCPError(vsaerrors.ErrInputValidationError, err)
		}
	} else {
		logger.Error("Failed to get VLM config", "poolUUID", input.PoolUUID, "error", err)
		return &FetchPoolDataActivityOutput{
			PoolUUID: input.PoolUUID,
			Success:  false,
			Error:    fmt.Sprintf("Failed to get VLM config: %v", err),
		}, vsaerrors.NewVCPError(vsaerrors.ErrInputValidationError, err)
	}

	logger.Info("Pool data fetch completed successfully", "poolUUID", input.PoolUUID)

	var accountName string
	if poolView.Account != nil {
		accountName = poolView.Account.Name
	}
	return &FetchPoolDataActivityOutput{
		PoolUUID:              input.PoolUUID,
		VLMConfig:             vlmConfig,
		Success:               true,
		AccountName:           accountName,
		AutoTieringEnabled:    poolView.AllowAutoTiering,
		AutoTieringBucketName: poolView.AutoTieringConfig.BucketName,
	}, nil
}

// UpdatePoolCompliance updates the pool with compliance data
func (a *PoolActivity) UpdatePoolCompliance(ctx context.Context, input UpdatePoolComplianceActivityInput) (*UpdatePoolComplianceActivityOutput, error) {
	logger := util.GetLogger(ctx)
	logger.Info("Starting pool compliance update", "poolUUID", input.PoolUUID)

	// Record activity heartbeat
	activity.RecordHeartbeat(ctx, "Updating pool compliance fields")

	// Update the pool with compliance data
	updates := map[string]interface{}{
		"satisfy_zi": input.SatisfyZI,
		"satisfy_zs": input.SatisfyZS,
	}

	// Add asset metadata if provided
	if input.AssetMetadata != nil {
		updates["asset_metadata"] = input.AssetMetadata
	}

	err := a.SE.UpdatePoolFields(ctx, input.PoolUUID, updates)
	// Record heartbeat before the update call to signal we've started persisting
	activity.RecordHeartbeat(ctx, "Persisting asset metadata")
	logger.Info("Committing pool updates", "poolUUID", input.PoolUUID, "satisfyZI", input.SatisfyZI, "satisfyZS", input.SatisfyZS)
	if err != nil {
		logger.Error("Failed to update pool compliance fields", "poolUUID", input.PoolUUID, "error", err)
		return &UpdatePoolComplianceActivityOutput{
			PoolUUID: input.PoolUUID,
			Success:  false,
			Error:    fmt.Sprintf("Failed to update pool compliance fields: %v", err),
		}, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
	}

	logger.Info("Pool compliance update completed successfully",
		"poolUUID", input.PoolUUID,
		"satisfyZI", input.SatisfyZI,
		"satisfyZS", input.SatisfyZS)

	return &UpdatePoolComplianceActivityOutput{
		PoolUUID: input.PoolUUID,
		Success:  true,
	}, nil
}

func (a *PoolActivity) GetBucketCompliance(ctx context.Context, bucketName string) (*datamodel.BucketDetails, error) {
	activity.RecordHeartbeat(ctx, "Initializing bucket compliance check")
	logger := util.GetLogger(ctx)

	if bucketName == "" {
		logger.Errorf("Bucket name parameter is empty, required to fetch zi/zs compliance")
		return nil, fmt.Errorf("bucket name parameter is required to fetch zi/zs compliance")
	}

	activity.RecordHeartbeat(ctx, "Getting cloud service")
	// Get cloud service
	cloudService, err := GetCloudService(ctx)
	if err != nil {
		logger.Errorf("Failed to get cloud service during AT bucket compliance check: %v", err)
		return nil, err
	}

	activity.RecordHeartbeat(ctx, "Retrieving bucket compliance details from GCP")
	// Get bucket details from GCP API
	cloudBucketDetails, err := cloudService.GetBucket(ctx, bucketName)
	if err != nil {
		logger.Errorf("Failed to get bucket details from GCP for fetching zi/zs compliance, error: %v", err)
		return nil, err
	}

	activity.RecordHeartbeat(ctx, "Bucket compliance check completed successfully")
	logger.Infof("Successfully retrieved bucket details from GCP for fetching zi/zs compliance, bucketName: %s", bucketName)
	logger.Infof("Received bucket compliance details from GCP - satisfiesPzi: %t, satisfiesPzs: %t", cloudBucketDetails.SatisfiesPzi, cloudBucketDetails.SatisfiesPzs)

	return &datamodel.BucketDetails{
		BucketName:   bucketName,
		SatisfiesPzi: cloudBucketDetails.SatisfiesPzi,
		SatisfiesPzs: cloudBucketDetails.SatisfiesPzs,
	}, nil
}

// FetchPoolDataActivityInput represents the input for fetching pool data
type FetchPoolDataActivityInput struct {
	PoolUUID  string `json:"pool_uuid"`
	AccountID int64  `json:"account_id"`
}

// FetchPoolDataActivityOutput represents the output for fetching pool data
type FetchPoolDataActivityOutput struct {
	PoolUUID              string        `json:"pool_uuid"`
	VLMConfig             vlm.VLMConfig `json:"vlm_config"`
	Success               bool          `json:"success"`
	Error                 string        `json:"error,omitempty"`
	AccountName           string        `json:"account_name"`
	AutoTieringEnabled    bool          `json:"auto_tiering_enabled"`
	AutoTieringBucketName string        `json:"auto_tiering_bucket_name"`
}

// UpdatePoolComplianceActivityInput represents the input for updating pool compliance
type UpdatePoolComplianceActivityInput struct {
	PoolUUID      string                   `json:"pool_uuid"`
	SatisfyZI     bool                     `json:"satisfy_zi"`
	SatisfyZS     bool                     `json:"satisfy_zs"`
	AssetMetadata *datamodel.AssetMetadata `json:"asset_metadata,omitempty"`
}

// UpdatePoolComplianceActivityOutput represents the output for updating pool compliance
type UpdatePoolComplianceActivityOutput struct {
	PoolUUID string `json:"pool_uuid"`
	Success  bool   `json:"success"`
	Error    string `json:"error,omitempty"`
}

// CalculateBatchPlanActivityInput represents input for calculating batch plan
type CalculateBatchPlanActivityInput struct {
	NumHAPairs                  int `json:"num_ha_pairs"`
	ParallelNumberOfNodesForITC int `json:"parallel_number_of_nodes_for_itc"`
}

// CalculateBatchPlanActivityOutput represents output for batch plan calculation
type CalculateBatchPlanActivityOutput struct {
	NumHAPairs       int     `json:"num_ha_pairs"`
	BatchSize        int     `json:"batch_size"`
	NumWorkflowCalls int     `json:"num_workflow_calls"`
	BatchIndices     [][]int `json:"batch_indices"` // Each inner slice contains HA pair indices for that batch
}

// CalculateBatchPlanForUpdate calculates the batch plan for HA pair updates
func (a *PoolActivity) CalculateBatchPlanForUpdate(ctx context.Context, input CalculateBatchPlanActivityInput) (*CalculateBatchPlanActivityOutput, error) {
	activity.RecordHeartbeat(ctx, "Starting CalculateBatchPlanForUpdate activity")
	logger := util.GetLogger(ctx)
	logger.Info("Calculating update batch plan as per the parallel number of nodes for ITC", "numHAPairs", input.NumHAPairs, "parallelNumberOfNodesForITC", input.ParallelNumberOfNodesForITC)

	if input.NumHAPairs <= 0 {
		return nil, vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("invalid number of HA pairs: %d", input.NumHAPairs))
	}

	numHAPairs := input.NumHAPairs
	parallelNumberOfNodesForITC := input.ParallelNumberOfNodesForITC

	if parallelNumberOfNodesForITC <= 0 {
		return nil, vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("invalid parallel number of nodes for ITC: %d", parallelNumberOfNodesForITC))
	}

	// Calculate batch size based on the batching strategy:
	// - For 1-3 HA pairs: batch size = 1
	// - For 4+ HA pairs: batch size = floor(numHAPairs / 2)

	// floor((numHAPairs * 2) / parallelNumberOfNodesForITC) for integer division
	activity.RecordHeartbeat(ctx, fmt.Sprintf("Calculating batch size for %d HA pairs with %d parallel nodes", numHAPairs, parallelNumberOfNodesForITC))
	batchSize := max(1, (numHAPairs*2)/parallelNumberOfNodesForITC)

	// Calculate number of workflow calls needed: ceil(numHAPairs / batchSize)
	numWorkflowCalls := (numHAPairs + batchSize - 1) / batchSize

	// Generate batch indices for all batches
	activity.RecordHeartbeat(ctx, fmt.Sprintf("Generating batch indices for %d workflow calls with batch size %d", numWorkflowCalls, batchSize))
	batchIndices := make([][]int, 0, numWorkflowCalls)
	for batchNum := 0; batchNum < numWorkflowCalls; batchNum++ {
		startIdx := batchNum * batchSize
		endIdx := startIdx + batchSize
		if endIdx > numHAPairs {
			endIdx = numHAPairs
		}

		// Generate HAPairIndices for this batch (1-indexed)
		indices := make([]int, endIdx-startIdx)
		for i := 0; i < endIdx-startIdx; i++ {
			indices[i] = startIdx + i + 1
		}
		batchIndices = append(batchIndices, indices)
	}

	logger.Info("Batch plan calculated", "numHAPairs", numHAPairs, "batchSize", batchSize, "numWorkflowCalls", numWorkflowCalls)
	activity.RecordHeartbeat(ctx, "Finished CalculateBatchPlanForUpdate activity")

	return &CalculateBatchPlanActivityOutput{
		NumHAPairs:       numHAPairs,
		BatchSize:        batchSize,
		NumWorkflowCalls: numWorkflowCalls,
		BatchIndices:     batchIndices,
	}, nil
}

// GetCreateJobByResourceUUID retrieves the create job for a resource by resource UUID and validates correlation ID
// Returns CreateJobResult with job UUID and workflow ID if found and correlation ID matches
func (j *PoolActivity) GetCreateJobByResourceUUID(ctx context.Context, resourceUUID string, correlationID string, jobType string) (*commonparams.CreateJobResult, error) {
	logger := util.GetLogger(ctx)
	se := j.SE

	// Get the create job for this resource by resource UUID
	createJob, err := se.GetJobByResourceUUID(ctx, resourceUUID, jobType)
	if err != nil {
		logger.Warnf("Could not find create job for resource %s with job type %s: %v", resourceUUID, jobType, err)
		return nil, err
	}

	// Validate correlation ID matches
	if correlationID != "" && createJob.CorrelationID != correlationID {
		logger.Warnf("Correlation ID mismatch: create job correlation ID %s does not match delete request correlation ID %s",
			createJob.CorrelationID, correlationID)
		return nil, fmt.Errorf("correlation ID mismatch")
	}

	logger.Infof("Found matching create job %s with workflow ID %s for job type %s", createJob.UUID, createJob.WorkflowID, jobType)

	return &commonparams.CreateJobResult{
		JobUUID:    createJob.UUID,
		WorkflowID: createJob.WorkflowID,
	}, nil
}
