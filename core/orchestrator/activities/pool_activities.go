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
	"strconv"
	"strings"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/hyperscaler"
	hyperscaler_models "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/hyperscaler/models"
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
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
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
	DeploymentsInsert                 = common.DeploymentsInsert
	PrepareVlmConfig                  = _prepareVlmConfig
	PrepareVlmConfigForVLMClient      = _prepareVlmConfigForVLMClient
	ReadFile                          = os.ReadFile
	GetVLMClient                      = _getVLMClient
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
	SetupNetworkWithFirewall          = setupNetworkWithFirewall
	GetTenantProject                  = _getTenantProject
	GetOrCreateSubnetwork             = _getOrCreateSubnetwork
	GetSubnetToBeUsed                 = getSubnetToBeUsed
	SetupNetworkFirewallsForIscsi     = setupNetworkFirewallsForIscsi
	CreateGCPBucket                   = _createGCPBucket
	CreateServiceAccountAndAttachRole = _createServiceAccountAndAttachRole
	DeleteSrvcAccount                 = _deleteServiceAccount
	DeleteGCPBucket                   = _deleteGCPBucket
	LoadVMRSConfig                    = vmrs_config.LoadConfig
	CreateDecisionMaker               = vmrs_decision.NewDecisionMaker
	vlmConfigFilePath                 = env.GetString("VLM_CONFIG_FILE_PATH", "common/vsa_config/vlm-config.json")
	ValidateVlmConfigInputs           = _validateVlmConfigInputs
	CreateSubnetwork                  = _createSubnetwork
	ReleaseSubnet                     = _releaseSubnet
	CheckAndUpdateFirewall            = _checkAndUpdateFirewall

	// Feature flag to enforce minimum values for SPConfig throughput and IOPS.
	// Set ENFORCE_MIN_SP_CONFIG=true in the environment to enable.
	enforceMinSPConfig                                  = env.GetBool("ENFORCE_MIN_SP_CONFIG", false)
	GenerateAndCreateCertificateForVSACluster           = _generateAndCreateCertificateForVSACluster
	GeneratePasswordForVSACluster                       = _generatePasswordForVSACluster
	GetPasswordForVSACluster                            = _getPasswordForVSACluster
	GetPasswordFromCacheOrSecretManager                 = _getPasswordFromCacheOrSecretManager
	GenerateCSR                                         = _generateCSR
	DeletePasswordFromCacheAndSecretManager             = _deletePasswordFromSecretManagerAndCache
	DeleteCloudDNSRecord                                = _deleteCloudDNSRecord
	GetOrCreateCloudDNSRecord                           = _getOrCreateCloudDNSRecord
	GetCertificateAndPrivateKeyByID                     = _getCertificateAndPrivateKeyByID
	GetOrCreatePrivateKeyInSecretManagerAndCache        = _getOrCreatePrivateKeyInSecretManagerAndCache
	GetOrCreateCertificateInCASAndPrivateKeyInSM        = _getOrCreateCertificateInCASAndPrivateKeyInSM
	GetCertificateFromCacheOrSecretManager              = _getCertificateFromCacheOrSecretManager
	RevokeCertificateAndDeleteFromCacheAndSecretManager = _revokeCertificateAndDeleteFromCacheAndSecretManager
)

type PoolActivity struct {
	SE database.Storage
}

const (
	aggregateName  = "aggr1"
	DefaultSvmName = "gcnv"

	firewallPriority        = 1000
	ingressTrafficDirection = "INGRESS"

	CsrType           = "CERTIFICATE REQUEST"
	RsaKeyType        = "RSA PRIVATE KEY"
	digitalSignature  = 0x80 // 10000000 in binary (bit 0)
	keyEncipherment   = 0x20
	keyManagerBootarg = "bootarg.keymanager.ekmip.svm_context=false"

	mgmtVpcName      = "mgmt-vpc"
	mgmtSubnetName   = "mgmt-subnet"
	clusterICVpcName = "cluster-ic-vpc"
	clusterICSubnet  = "cluster-ic-subnet"
	rsmVpcName       = "rsm-vpc"
	rsmSubnetName    = "rsm-subnet"
)

// Minimum allowed values for SPConfig throughput (in MiBs) and IOPS.
// These are enforced only if the feature flag above is enabled.
const (
	minSPConfigThroughput = 1120
	minSPConfigIOps       = 24000
)

var (
	maxRetries           = env.GetInt("GOOGLE_API_MAX_RETRIES", 6)
	localRegion          = env.GetString("LOCAL_REGION", "")
	firewallSourceRanges = env.GetString("FIREWALL_SOURCE_RANGES", "")
	totalIPPerHAPair     = env.GetInt("TOTAL_IP_PER_HA_PAIR", 6)
)

func (j *PoolActivity) CreatingPool(ctx context.Context, pool *datamodel.Pool) (*datamodel.Pool, error) {
	se := j.SE
	return se.CreatingPool(ctx, pool)
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
	service, err := GetGCPService(ctx)
	if err != nil {
		return "", vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return GetTenantProject(service, params)
}

func _getTenantProject(service hyperscaler.GoogleServices, params commonparams.CreatePoolParams) (string, error) {
	tenantProjectNumber, err := service.GetTenantProject(params.VendorSubNetID, params.AccountName, params.Region)
	if err != nil {
		service.GetLogger().Errorf("Error finding tenancy unit. Project: %s vpc: %s Error: %v", params.AccountName, params.VendorSubNetID, err)
		return "", err
	}
	service.GetLogger().Debug(fmt.Sprintf("Found tenancy: tenantProjectNumber :  %s for consumer project : %s", tenantProjectNumber, params.AccountName))
	return tenantProjectNumber, nil
}

// CreateOrGetSubnetwork re-uses subnet if IP CIDR range is available; else creates a subnetwork for the tenant project
func (j *PoolActivity) CreateOrGetSubnetwork(ctx context.Context, params commonparams.CreatePoolParams, tenantProjectNumber string) (*commonparams.TenancyInfo, error) {
	service, err := GetGCPService(ctx)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return GetOrCreateSubnetwork(j.SE, service, params, tenantProjectNumber)
}

func _getOrCreateSubnetwork(se database.Storage, service hyperscaler.GoogleServices, params commonparams.CreatePoolParams, tenantProjectNumber string) (*commonparams.TenancyInfo, error) {
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
	consumerVPC := params.VendorSubNetID
	if snHostProject != "" {
		// if snHost is found, check if the subnetwork already exists in the SN host project and reuse it if applicable
		subnet, err = GetSubnetToBeUsed(service, se, customerProjectNumber, tenantProjectNumber, snHostProject, tenantProjectRegion)
		if err != nil {
			logger.Errorf("Error getting subnet for tenant project: %s, SN host : %s, region %s. Error : %s", tenantProjectNumber, snHostProject, tenantProjectRegion, err.Error())
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceFetchError, err)
		}
	}
	if subnet == nil {
		// if snHost is not found or subnet found cannot be used, create a new subnetwork for the tenant project
		subnet, err = CreateSubnetwork(service, tenantProjectNumber, consumerVPC, &tenantProjectRegion)
		if err != nil {
			logger.Errorf("Error creating subnetwork for tenant project: %s, SN host : %s, region %s. Error : %s", tenantProjectNumber, snHostProject, tenantProjectRegion, err.Error())
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceFetchError, err)
		}
	}
	logger.Infof("Subnet used for tenant project: tenantProjectNumber: %s SN host project : %s subnet: %s IpCidrRange: %s, consumerPeeringNetwork: %s", tenantProjectNumber, snHostProject, subnet.IpCidrRange, consumerVPC, subnet.Name)

	snHostProject, network, err := utils.ParseProjectId(subnet.Network)
	if err != nil {
		return nil, err
	}
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

// createSubnetwork generates a subnetwork name based on the tenant project number and region and triggers creation the subnet in SN host project
func _createSubnetwork(service hyperscaler.GoogleServices, tenantProjectNumber, consumerVPC string, tenantProjectRegion *string) (*hyperscaler_models.Subnet, error) {
	subnetName := MakeSubnetName(tenantProjectNumber)
	subnetInBytes, err := service.CreateSubnetworkForTenantProject(tenantProjectNumber, consumerVPC, *tenantProjectRegion, subnetName)
	if err != nil {
		service.GetLogger().Errorf("Error adding subnetwork: %v", err)
		return nil, err
	}
	subnetCreated := &servicenetworking.Subnetwork{}
	var subnet *hyperscaler_models.Subnet
	service.GetLogger().Debug(fmt.Sprintf("subnetInBytes %s", string(subnetInBytes)))

	if err := json.Unmarshal(subnetInBytes, subnetCreated); err != nil {
		service.GetLogger().Debug(fmt.Sprintf("subnetInBytes json unmarshal error %s", err.Error()))
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrJSONParsingError, err)
	}

	snHostProject, _, err := utils.ParseProjectId(subnetCreated.Network)
	if err != nil {
		return nil, err
	}
	subnet, err = service.GetSubnetwork(snHostProject, *tenantProjectRegion, subnetCreated.Name)
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceFetchError, err)
	}
	service.GetLogger().Infof("created subnetwork for tenant project: %s, SN host : %s, region %s. Subnet name : %s", tenantProjectNumber, snHostProject, tenantProjectRegion, subnetCreated.Name)
	return subnet, nil
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
		err = SetupNetworkWithFirewall(ctx, project, vpcName, &region, subnetName, fmt.Sprintf("198.18.%d.0/27", i*3), firewallPriority, ingressTrafficDirection, strings.Split(firewallSourceRanges, ","), firewallPortRules)
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

func (j *PoolActivity) CreateOnTapCredentials(ctx context.Context, pool *datamodel.Pool, region, clusterName string) (*vlm.OntapCredentials, error) {
	credentials := &vlm.OntapCredentials{}
	gcpService, getGcpServiceErr := GetGCPService(ctx)
	if getGcpServiceErr != nil {
		return nil, getGcpServiceErr
	}

	switch pool.PoolCredentials.AuthType {
	case commonparams.USER_CERTIFICATE:
		// Generate and create a certificate for the VSA cluster in CAS and fallthrough to generate and create the password for VSA cluster in Secret Manager as well
		certificate, err := GenerateAndCreateCertificateForVSACluster(gcpService, region, pool.PoolCredentials.CertificateID, clusterName)
		if err != nil {
			return nil, vsaerrors.WrapAsTemporalApplicationError(err)
		}
		credentials.Certificate.CommonName = certificate.Certificate.SubjectCommonName
		credentials.Certificate.Certificate = certificate.Certificate.PemCertificate
		credentials.Certificate.PrivateKey = certificate.Secret.SecretVersion.Value
		credentials.Certificate.InterMediateCertificate = certificate.Certificate.PemCertificateChain
		credentials.Certificate.CaCertificate = certificate.Certificate.RootCACertificate
		fallthrough
	case commonparams.USERNAME_PWD_SEC_MGR:
		secret, err := GeneratePasswordForVSACluster(gcpService, commonparams.SecretManagerProjectID, region, pool.PoolCredentials.SecretID)
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
	gcpService, err := GetGCPService(ctx)
	if err != nil {
		return err
	}
	switch pool.PoolCredentials.AuthType {
	case commonparams.USER_CERTIFICATE:
		// Revoke the certificates and delete the private key from secret manager and cache then fallthrough to delete the password from secret manager and cache
		err = RevokeCertificateAndDeleteFromCacheAndSecretManager(gcpService, pool.PoolCredentials.CertificateID)
		if err != nil {
			return vsaerrors.WrapAsTemporalApplicationError(err)
		}
		fallthrough
	case commonparams.USERNAME_PWD_SEC_MGR:
		err = DeletePasswordFromCacheAndSecretManager(gcpService, pool.PoolCredentials.SecretID)
		if err != nil {
			return vsaerrors.WrapAsTemporalApplicationError(err)
		}
	default:
		return nil
	}
	return nil
}

func (j *PoolActivity) GetOnTapCredentials(ctx context.Context, pool *datamodel.Pool) (*vlm.OntapCredentials, error) {
	credentials := &vlm.OntapCredentials{}
	switch pool.PoolCredentials.AuthType {
	case commonparams.USER_CERTIFICATE:
		certificate, err := GetCertificateFromCacheOrSecretManager(ctx, pool.PoolCredentials.CertificateID)
		if err != nil {
			return nil, vsaerrors.WrapAsTemporalApplicationError(err)
		}
		credentials.Certificate.CommonName = certificate.CommonName
		credentials.Certificate.Certificate = certificate.SignedCertificate
		credentials.Certificate.PrivateKey = certificate.PrivateKey
		credentials.Certificate.InterMediateCertificate = certificate.InterMediateCertificates
		credentials.Certificate.CaCertificate = certificate.RootCaCertificate
		fallthrough
	case commonparams.USERNAME_PWD_SEC_MGR:
		secret, err := GetPasswordFromCacheOrSecretManager(ctx, pool.PoolCredentials.SecretID)
		if err != nil {
			return nil, vsaerrors.WrapAsTemporalApplicationError(err)
		}
		credentials.AdminPassword = secret
	default:
		credentials.AdminPassword = pool.PoolCredentials.Password
	}
	return credentials, nil
}

func _revokeCertificateAndDeleteFromCacheAndSecretManager(gcpService hyperscaler.GoogleServices, certificateID string) error {
	logger := gcpService.GetLogger()
	_, err := gcpService.GetCertificate(commonparams.CaPoolDeployedProjectID, commonparams.Region, commonparams.CaPoolName, certificateID)
	if err != nil {
		logger.Errorf("Failed to get certificate from cache for project %s and region %s", commonparams.CaPoolDeployedProjectID, commonparams.Region)
		return err
	}
	certObject := &hyperscaler_models.CustomCertificate{
		CertOwningEntity: commonparams.CaPoolDeployedProjectID,
		Region:           commonparams.Region,
		CaGroupName:      commonparams.CaPoolName,
		CertificateID:    certificateID,
	}

	// delete the certificate from CAS
	_, err = gcpService.RevokeCertificate(certObject)
	if err != nil {
		logger.Errorf("Failed to revoke certificate for project %s and region %s", commonparams.CaPoolDeployedProjectID, commonparams.Region)
		return err
	}

	_, err = gcpService.GetSecretWithLatestVersion(commonparams.SecretManagerProjectID, certificateID)
	if err != nil {
		logger.Errorf("Failed to get private key from secret manager for project %s and certificate %s", commonparams.SecretManagerProjectID, certificateID)
		return err
	}

	// delete the private key from secret manager
	err = gcpService.DeleteSecret(commonparams.SecretManagerProjectID, certificateID)
	if err != nil {
		logger.Errorf("Failed to delete private key from secret manager for project %s and certificate %s", commonparams.SecretManagerProjectID, certificateID)
		return err
	}

	// delete from cache if not expired
	done := commonparams.RemoveFromCertAuthCache(certificateID)
	if !done {
		logger.Errorf("Failed to remove certificate %s from cache", certificateID)
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
	err := se.SavePoolWithVsaDetails(ctx, dbPool, cluster)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	return nil
}

func _getVLMClient(ctx context.Context, logger log.Logger, vlmConfig *vlmconfig.VLMConfig) vlm.ClientFactory {
	return vlm.NewClient(ctx, logger, vlmConfig)
}

func (j *PoolActivity) GetOntapVersion(ctx context.Context, node *models.Node) (*string, error) {
	provider, err := GetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	version, err := provider.GetONTAPVersion()
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	return version, nil
}

func (j *PoolActivity) SaveSVMAndLifData(ctx context.Context, pool *datamodel.Pool, vlmConfig *vlm.VLMConfig) (*datamodel.Svm, error) {
	se := j.SE

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
	// TODO: Remove this workaround once the VLM worker image is updated to use the correct LIF type ("iscsi").
	// Currently, the received data uses "default-data-iscsi" instead of the expected "iscsi" as per the data model.
	lifs := svm.SVMLIFs[vlm.DefaultLIFTypeIscsi]

	for i, lif := range lifs {
		dataLif := lif.IP
		ip := strings.Split(dataLif, "/")[0]
		lifRec := &datamodel.Lif{
			Name:       lif.Name,
			AccountID:  pool.AccountID,
			NodeID:     nodes[i].ID, // FIXME : need to get the node name from the lif object - VLM changes
			LifDetails: &datamodel.LifDetails{ExternalUUID: lif.Uuid},
			IPAddress:  ip,
			SubnetMask: vsa.DefaultNetmask,
		}
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
	provider, err := GetProviderByNode(ctx, node)
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
func (j *PoolActivity) IdentifyVMs(ctx context.Context, vmrsConfigPath string, customerRequest vmrs.CustomerRequestedPerformance, deploymentName, region, primaryZone, secondaryZone, network string, subnets []string, projectId, snHostProject string, saEmail string, autoTierBucket string) (*vlm.VLMConfig, error) {
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
	if len(subnets) > 0 {
		subnet = subnets[len(subnets)-1]
	}

	// Convert the decision to a VLMConfig.
	err = PrepareVlmConfig(vlmConfig, deploymentName, region, primaryZone, secondaryZone, network, subnet, projectId, snHostProject, decision, saEmail, autoTierBucket)
	if err != nil {
		logger.Error("Failed to prepare VLM config", "error", err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	return vlmConfig, nil
}

func _prepareVlmConfig(vlmConfig *vlm.VLMConfig, deploymentID, region, primaryZone, secondaryZone, network, subnet, regionalTenantProjectID, snHostProject string, decision *vmrs.Decision, vsaClusterSaEmail string, autoTierBucket string) error {
	if err := ValidateVlmConfigInputs(vlmConfig, decision, deploymentID, region, primaryZone, network, subnet, regionalTenantProjectID, snHostProject, vsaClusterSaEmail); err != nil {
		return err
	}

	vlmContent, err := ReadFile(vlmConfigFilePath)
	if err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrFileReadError, err)
	}

	err = json.Unmarshal(vlmContent, &vlmConfig)
	if err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrFileReadError, err)
	}

	vlmConfig.Deployment.GCPConfig = vlm.GCPConfig{
		ProjectID:           regionalTenantProjectID,
		ImageProjectID:      regionalTenantProjectID,
		ServiceAccountEmail: vsaClusterSaEmail,
		BucketName:          autoTierBucket,
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
	if secondaryZone == "" {
		vlmConfig.Deployment.Zone.Zone2 = primaryZone
	}

	networkConfigs := map[vlm.VSALIFType]struct {
		VPC             string
		Subnet          string
		SubnetProjectID string
	}{
		vlm.LIFTypeNodeMgmt: {mgmtVpcName, mgmtSubnetName, regionalTenantProjectID},
		vlm.LIFTypeIC:       {clusterICVpcName, clusterICSubnet, regionalTenantProjectID},
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

// Update VSA cluster by invoking VLM.
func (j *PoolActivity) UpdateVSACluster(ctx context.Context, currentVlmConfig *vlmconfig.VLMConfig, newVlmConfig *vlmconfig.VLMConfig, credential vlmconfig.OntapCredentials) (*vlmconfig.VLMConfig, error) {
	logger := util.GetLogger(ctx)
	vlmClient := GetVLMClient(ctx, logger, currentVlmConfig)

	vlmConfig, err := vlmClient.VSAClusterDeployUpdate(ctx, credential, currentVlmConfig, newVlmConfig)
	if err != nil {
		logger.Error("Failed to update VSA cluster", "error", err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return vlmConfig, nil
}

func (j *PoolActivity) CreateVlmConfig(ctx context.Context, deploymentName, region, primaryZone, secondaryZone, network, subnet, projectId, snHostProject string, decision *vmrs.Decision, saEmail string, autoTierBucket string) (*vlmconfig.VLMConfig, error) {
	cfg := &vlmconfig.VLMConfig{}
	err := PrepareVlmConfigForVLMClient(cfg, deploymentName, region, primaryZone, secondaryZone, network, subnet, projectId, snHostProject, decision, saEmail, autoTierBucket)
	return cfg, vsaerrors.WrapAsTemporalApplicationError(err)
}

func _prepareVlmConfigForVLMClient(cfg *vlmconfig.VLMConfig, deploymentName, region, primaryZone, secondaryZone, network, subnet, projectId, snHostProject string, decision *vmrs.Decision, saEmail string, autoTierBucket string) error {
	vlmContent, err := ReadFile(vlmConfigFilePath)
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
		assignNetworkConfigForVLMClient(cfg, lifType, config.VPC, config.Subnet, config.SubnetProjectID)
	}

	// assign network configuration for data LIF from snHostProject
	assignNetworkConfigForVLMClient(cfg, vlmconfig.LIFTypeInterCluster, network, subnet, snHostProject)

	cfg.Deployment.GCPConfig = vlmconfig.GCPConfig{
		ProjectID:           projectId,
		ImageProjectID:      projectId,
		ServiceAccountEmail: saEmail,
		BucketName:          autoTierBucket,
	}
	// Bootargs for key manager
	cfg.Deployment.UserBootargs = keyManagerBootarg

	return nil
}

func assignNetworkConfigForVLMClient(cfg *vlmconfig.VLMConfig, lifType vlmconfig.VSALIFType, vpc, subnet, subnetProjectID string) {
	cfg.Deployment.NetConfig[lifType] = vlmconfig.NetworkConfig{
		VPC:              vpc,
		Subnet:           subnet,
		GCPNetworkConfig: vlmconfig.GCPNetworkConfig{SubnetProjectID: subnetProjectID},
	}
}

// CreateCloudDNSRecords creates DNS records for the VSA cluster's nodes in the cloud DNS service
func (j *PoolActivity) CreateCloudDNSRecords(ctx context.Context, vlmConfig *vlmconfig.VLMConfig, clusterName string, authType int) (*map[string]string, error) {
	hostMap := make(map[string]string)
	if authType == commonparams.USER_CERTIFICATE {
		if len(vlmConfig.Cloud.HAPairs) == 0 {
			return nil, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrIncorrectVSAClusterState, errors.New("no cluster details provided")))
		}
		for i, details := range vlmConfig.Cloud.HAPairs {
			if len(details.VM1.SystemLIFs) == 0 || len(details.VM2.SystemLIFs) == 0 {
				return nil, vsaerrors.WrapAsTemporalApplicationError(
					vsaerrors.NewVCPError(vsaerrors.ErrIncorrectVSAClusterState, errors.New("no system LIFs provided for VMs")))
			}
			gcpService, err := GetGCPService(ctx)
			if err != nil {
				return nil, err
			}

			IpaddressVm1 := details.VM1.SystemLIFs[vlmconfig.LIFTypeNodeMgmt].IP
			haPairNode1 := fmt.Sprintf("%s-%d.%s.%s.", "dns", (2*i)+1, clusterName, commonparams.VsaDeployedDnsName)
			record1, err := GetOrCreateCloudDNSRecord(gcpService, IpaddressVm1, haPairNode1)
			if err != nil {
				return nil, err
			}
			hostMap[IpaddressVm1] = record1.RecordName

			IpaddressVm2 := details.VM2.SystemLIFs[vlmconfig.LIFTypeNodeMgmt].IP
			haPairNode2 := fmt.Sprintf("%s-%d.%s.%s.", "dns", (2*i)+2, clusterName, commonparams.VsaDeployedDnsName)
			record2, err := GetOrCreateCloudDNSRecord(gcpService, IpaddressVm2, haPairNode2)
			if err != nil {
				return nil, err
			}
			hostMap[IpaddressVm2] = record2.RecordName
		}
		return &hostMap, nil
	}
	return &hostMap, nil
}

func (j *PoolActivity) DeleteCloudDNSRecords(ctx context.Context, hostMap map[string]string, authType int) error {
	if authType == commonparams.USER_CERTIFICATE {
		gcpService, err := GetGCPService(ctx)
		if err != nil {
			return err
		}
		// Delete entries for each node
		for _, host := range hostMap {
			// Check if the node is already deleted
			err = DeleteCloudDNSRecord(gcpService, host)
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
	if authType == commonparams.USER_CERTIFICATE {
		se := j.SE
		nodes, err := se.GetNodesByPoolID(ctx, poolId)
		if err != nil {
			return &hostMap, err
		}
		if len(nodes) == 0 {
			return &hostMap, errors.New("no node found for the pool")
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

	provider, err := GetProviderByNode(ctx, node)
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
	if pool.PoolCredentials.AuthType == commonparams.USER_CERTIFICATE {
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
	service, err := GetGCPService(ctx)
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

func _releaseSubnet(service hyperscaler.GoogleServices, snHost, subnetName string) error {
	err := service.ReleaseSubnetwork(localRegion, snHost, subnetName)
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
	gcpService, err := GetGCPService(ctx)
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
	gcpService, err := GetGCPService(ctx)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	logger := util.GetLogger(ctx)

	logger.Debugf("Deleting autoTiering bucket %v", autoTierBucketName)
	err = DeleteGCPBucket(ctx, autoTierBucketName, gcpService)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	return nil
}

func _createGCPBucket(ctx context.Context, projectId, bucketName, region string, gcpService hyperscaler.GoogleServices) error {
	logger := gcpService.GetLogger()
	err := gcpService.CreateBucketIfNotExists(ctx, projectId, bucketName, region)
	if err != nil {
		logger.Errorf("error creating bucket: %v", err)
		return vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceAlreadyExistsError, err)
	}
	logger.Infof("Bucket created successfully %s", bucketName)

	return nil
}

func _deleteGCPBucket(ctx context.Context, bucketName string, gcpService hyperscaler.GoogleServices) error {
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
func (j *PoolActivity) CreateServiceAccountWithStorageRole(ctx context.Context, projectID string, saAccountID string, saDisplayName string) (*iam.ServiceAccount, error) {
	gcpService, err := GetGCPService(ctx)
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
	gcpService, err := GetGCPService(ctx)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	err = DeleteSrvcAccount(ctx, projectID, saAccountID, gcpService)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	return nil
}

func _deleteServiceAccount(ctx context.Context, projectID string, saAccountID string, gcpService hyperscaler.GoogleServices) error {
	logger := gcpService.GetLogger()

	saEmail := fmt.Sprintf("%s@%s.iam.gserviceaccount.com", saAccountID, projectID)
	logger.Infof("Deleting service account %s", saEmail)
	err := gcpService.DeleteServiceAccount(saEmail)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return nil
}

func _createServiceAccountAndAttachRole(ctx context.Context, projectID string, saAccountID string, saDisplayName string, gcpService hyperscaler.GoogleServices) (*iam.ServiceAccount, error) {
	logger := gcpService.GetLogger()
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
	existingFirewall, err := gService.GetFirewall(projectName, firewallName)
	if err != nil {
		logger.Debug(fmt.Sprintf("Error getting firewall for project : %s and network name : %s firewall name : %s . Error : %v", projectName, vpcName, firewallName, err))
		resourceNotFound, errReceived := resourceNotFoundCheck(err.Error(), projectName, vpcName, "", firewallName)
		logger.Debug(fmt.Sprintf("Error getting firewall for project : %s and network name : %s firewall name : %s . Error : %v resourceNotFound : %t", projectName, vpcName, firewallName, err, resourceNotFound))
		if !resourceNotFound {
			return errReceived
		}
	}
	if existingFirewall != nil {
		return CheckAndUpdateFirewall(gService, existingFirewall, firewallRequest)
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

// _checkAndUpdateFirewall check if firewall has been updated by checking if all SourceRanges in firewallReceived exist in firewallRequest.SourceRanges
func _checkAndUpdateFirewall(gService hyperscaler.GoogleServices, existingFirewall, firewallRequest *hyperscaler_models.Firewall) error {
	var err error
	needsUpdate := false

	needsUpdate = !utils.IsSliceEqual(firewallRequest.SourceRanges, existingFirewall.SourceRanges)
	if needsUpdate {
		gService.GetLogger().Info(fmt.Sprintf("Updating firewall for project : %s and network name : %s, firewall name : %s ", firewallRequest.ProjectName, firewallRequest.VPCNetworkName, firewallRequest.Name))
		err = gService.UpdateFirewall(firewallRequest)
		if err != nil {
			gService.GetLogger().Errorf("Error updating firewall for project : %s and network name : %s firewall name : %s. Error : %v ", firewallRequest.ProjectName, firewallRequest.VPCNetworkName, firewallRequest.Name, err)
			return vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceProvisionError, fmt.Errorf("error updating firewall for project: %s and network name: %s firewall name: %s. Error : %v", firewallRequest.ProjectName, firewallRequest.VPCNetworkName, firewallRequest.Name, err))
		}
	}
	gService.GetLogger().Debug(fmt.Sprintf("Firewall already exists. Skipping creation. project name : %s , vpc name : %s, firewall name : %s", firewallRequest.ProjectName, firewallRequest.VPCNetworkName, firewallRequest.Name))
	return nil
}

// _generateAndCreateCertificateForVSACluster generates a CSR and creates a certificate in GCP Certificate Authority Service.
func _generateAndCreateCertificateForVSACluster(gcpService hyperscaler.GoogleServices, region, certificateID, clusterName string) (*hyperscaler_models.CustomCertificateResponse, error) {
	logger := gcpService.GetLogger()
	domains := fmt.Sprintf("*.%s.%s", clusterName, commonparams.VsaDeployedDnsName)
	param := &hyperscaler_models.CustomCertificateParam{
		Region:           region,
		CertificateID:    certificateID,
		CaPoolName:       commonparams.CaPoolName,
		CaName:           commonparams.CaName,
		CommonName:       commonparams.VCP_ADMIN,
		Domains:          []string{domains},
		CertOwningEntity: commonparams.CaPoolDeployedProjectID,
	}
	// Generate CSR
	csrDER, key, err := GenerateCSR(param.CommonName, param.Domains)
	if err != nil {
		logger.Errorf("failed to generate CSR for commonName: %s, certificateId : %s, err : %v", param.CommonName, param.CertificateID, err)
		return nil, err
	}

	pemBlock := pem.Block{
		Type:  CsrType,
		Bytes: csrDER,
	}
	logger.Debug("Generate CSR for commonName: %s, certificateId : %s", param.CommonName, param.CertificateID)

	customCertificate, err := commonparams.ValidateAndConvertCertificateParamsToCustomCertificate(param, pemBlock)
	if err != nil {
		return nil, err
	}

	// Create the Certificate
	cert, secret, err := GetOrCreateCertificateInCASAndPrivateKeyInSM(gcpService, customCertificate, key)
	if err != nil {
		logger.Errorf("failed to create customCertificate in CAS and private key in SM for commonName: %s, certificateId : %s, err : %v", param.CommonName, param.CertificateID, err)
		return nil, err
	}

	// Add the certificate to the cache
	commonparams.AddToCertAuthCache(certificateID, &models.Certificate{
		CommonName:               cert.SubjectCommonName,
		SignedCertificate:        cert.PemCertificate,
		PrivateKey:               secret.SecretVersion.Value,
		InterMediateCertificates: cert.PemCertificateChain,
		RootCaCertificate:        cert.RootCACertificate,
	})
	return &hyperscaler_models.CustomCertificateResponse{
		Certificate: cert,
		Secret:      secret,
	}, nil
}

// _getOrCreateCertificateInCASAndPrivateKeyInSM creates a certificate in GCP Certificate Authority Service and stores the private key in Secret Manager.
func _getOrCreateCertificateInCASAndPrivateKeyInSM(gcpService hyperscaler.GoogleServices, certificate *hyperscaler_models.CustomCertificate, key *rsa.PrivateKey) (*hyperscaler_models.CustomCertificate, *hyperscaler_models.CustomSecret, error) {
	// Create the certificate if Get the certificate fails
	logger := gcpService.GetLogger()
	var secret *hyperscaler_models.CustomSecret
	var cert *hyperscaler_models.CustomCertificate
	cert, err := gcpService.GetCertificate(commonparams.CaPoolDeployedProjectID, certificate.Region, commonparams.CaPoolName, certificate.CertificateID)
	if err != nil {
		// Create the Certificate
		cert, err = gcpService.CreateCertificate(certificate)
		if err != nil {
			logger.Errorf("failed to create certificate in CAS for commonName: %s, certificateId : %s, err : %v", certificate.SubjectCommonName, certificate.CertificateID, err)
			return nil, nil, err
		}
		logger.Debugf("created certificate in CAS for commonName: %s, certificateId : %s", certificate.SubjectCommonName, certificate.CertificateID)

		secret, err = GetOrCreatePrivateKeyInSecretManagerAndCache(gcpService, certificate, key)
		if err != nil {
			return nil, nil, err
		}
		return cert, secret, nil
	}

	secret, err = GetOrCreatePrivateKeyInSecretManagerAndCache(gcpService, certificate, key)
	if err != nil {
		return nil, nil, err
	}
	return cert, secret, nil
}

func _getOrCreatePrivateKeyInSecretManagerAndCache(gcpService hyperscaler.GoogleServices, certificate *hyperscaler_models.CustomCertificate, key *rsa.PrivateKey) (*hyperscaler_models.CustomSecret, error) {
	logger := gcpService.GetLogger()
	secret, err := gcpService.GetSecretWithLatestVersion(commonparams.SecretManagerProjectID, certificate.CertificateID)
	if err != nil {
		// Store the private key in Secret Manager
		secretValue := commonparams.ConvertPrivateKeyToString(key, RsaKeyType)
		secret, err = gcpService.CreateSecret(commonparams.SecretManagerProjectID, certificate.Region, certificate.CertificateID, secretValue)
		if err != nil {
			logger.Errorf("failed to create secret in SM for commonName: %s, certificateId : %s, err : %v", certificate.SubjectCommonName, certificate.CertificateID, err)
			// Revoke the certificate if the secret creation fails
			_, revokeError := gcpService.RevokeCertificate(certificate)
			if revokeError != nil {
				logger.Errorf("failed to revoke certificate in CAS for commonName: %s, certificateId : %s, err : %v", certificate.SubjectCommonName, certificate.CertificateID, revokeError)
				return nil, revokeError
			}
			return nil, err
		}
		logger.Debugf("created secret in SM for commonName: %s, certificateId : %s", certificate.SubjectCommonName, certificate.CertificateID)
	}
	return secret, nil
}

// _getCertificateForVSACluster retrieves the certificate for a VSA cluster from GCP Certificate Authority Service and Private key from Secret Manager.
func _getCertificateAndPrivateKeyByID(gcpService hyperscaler.GoogleServices, caDeployedProjectID, secretManagerProjectID, region, caPoolName, certificateID string) (*hyperscaler_models.CustomCertificateResponse, error) {
	certificate, err := gcpService.GetCertificate(caDeployedProjectID, region, caPoolName, certificateID)
	if err != nil || certificate == nil {
		return nil, fmt.Errorf("failed to get certficate for project: %s, region: %s, caPoolName : %s, certificateID : %s, err: %s", caDeployedProjectID, region, caPoolName, certificateID, err)
	}
	secret, err := gcpService.GetSecretWithLatestVersion(secretManagerProjectID, certificateID)
	if err != nil || secret == nil || secret.SecretVersion == nil {
		return nil, fmt.Errorf("failed to get secret for project: %s, certificateID: %s, err: %s", secretManagerProjectID, certificateID, err)
	}
	return &hyperscaler_models.CustomCertificateResponse{
		Certificate: certificate,
		Secret:      secret,
	}, nil
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
		commonparams.AddToUserAuthCache(secretID, secret.SecretVersion.Value)
	}
	return secret, nil
}

// _getPasswordForVSACluster retrieves the password for a VSA cluster from GCP Secret Manager.
func _getPasswordForVSACluster(gcpService hyperscaler.GoogleServices, secretID string) (*hyperscaler_models.CustomSecret, error) {
	secret, err := gcpService.GetSecretWithLatestVersion(commonparams.SecretManagerProjectID, secretID)
	if err != nil || secret == nil || secret.SecretVersion == nil {
		return nil, fmt.Errorf("failed to get secret for project: %s, userName: %s, err: %s", commonparams.SecretManagerProjectID, secretID, err)
	}
	return secret, nil
}

// _getPasswordFromCacheOrSecretManager retrieves the password for a VSA cluster from cache or GCP Secret Manager if not found in cache.
func _getPasswordFromCacheOrSecretManager(ctx context.Context, secretID string) (string, error) {
	password := ""
	userCache, exist := commonparams.GetFromUserAuthCache(secretID)
	if !exist || userCache.Password == "" {
		gcpService, err := GetGCPService(ctx)
		if err != nil {
			return "", err
		}
		secret, err := GetPasswordForVSACluster(gcpService, secretID)
		if err != nil {
			return "", err
		}
		password = secret.SecretVersion.Value
		commonparams.AddToUserAuthCache(secretID, password)
		return password, nil
	}
	password = userCache.Password
	return password, nil
}

// _deletePasswordFromSecretManagerAndCache generates a strong password and creates a secret in GCP Secret Manager.
func _deletePasswordFromSecretManagerAndCache(gcpService hyperscaler.GoogleServices, secretID string) error {
	logger := gcpService.GetLogger()
	_, err := gcpService.GetSecretWithLatestVersion(commonparams.SecretManagerProjectID, secretID)
	if err == nil {
		err = gcpService.DeleteSecret(commonparams.SecretManagerProjectID, secretID)
		if err != nil {
			logger.Errorf("failed to delete password for secretID: %s, err : %v", secretID, err)
			return err
		}

		done := commonparams.RemoveFromUserAuthCache(secretID)
		if !done {
			logger.Errorf("failed to remove password from cache for secretID: %s", secretID)
			return nil
		}
	}
	return nil
}

// _getCertificateFromCacheOrSecretManager retrieves the certificate from cache or GCP Certificate and Secret Manager.
func _getCertificateFromCacheOrSecretManager(ctx context.Context, certificateID string) (*models.Certificate, error) {
	certCache, exist := commonparams.GetCertAuthCache(certificateID)
	// If not found in cache, fetch from GCP Certificate and Secret Manager
	if !exist || certCache.Certificate == nil {
		gcpService, err := GetGCPService(ctx)
		if err != nil {
			return nil, err
		}
		certificateResponse, err := GetCertificateAndPrivateKeyByID(gcpService, commonparams.CaPoolDeployedProjectID, commonparams.SecretManagerProjectID, commonparams.Region, commonparams.CaPoolName, certificateID)
		if err != nil {
			return nil, err
		}
		cert := &models.Certificate{
			SignedCertificate:        certificateResponse.Certificate.CertificateID,
			PrivateKey:               certificateResponse.Secret.SecretVersion.Value,
			CommonName:               certificateResponse.Certificate.SubjectCommonName,
			InterMediateCertificates: certificateResponse.Certificate.PemCertificateChain,
			RootCaCertificate:        certificateResponse.Certificate.RootCACertificate,
		}
		commonparams.AddToCertAuthCache(certificateID, cert)
		return cert, nil
	}

	return certCache.Certificate, nil
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

// _getOrCreateCloudDNSRecord checks if a Cloud DNS record exists, and if not, creates it.
func _getOrCreateCloudDNSRecord(gcpService hyperscaler.GoogleServices, recordName, ipAddress string) (*hyperscaler_models.CustomCloudDNSRecord, error) {
	record, getErr := gcpService.GetResourceRecordSet(commonparams.CaPoolDeployedProjectID, commonparams.VsaManagedZone, recordName)
	if getErr != nil {
		gcpService.GetLogger().Debugf("Creating Cloud DNS record: %s.%s with type %s", recordName, commonparams.VsaManagedZone, recordName)
		record, err := gcpService.CreateResourceRecordSet(commonparams.CaPoolDeployedProjectID, commonparams.VsaManagedZone, ipAddress, recordName)
		if err != nil {
			gcpService.GetLogger().Errorf("Failed to create Cloud DNS record: %v", err)
			return nil, vsaerrors.WrapAsTemporalApplicationError(err)
		}
		return record, nil
	}
	// If the record already exists, return it
	return record, nil
}

func _deleteCloudDNSRecord(gcpService hyperscaler.GoogleServices, recordName string) error {
	logger := gcpService.GetLogger()
	_, err := gcpService.GetResourceRecordSet(commonparams.CaPoolDeployedProjectID, commonparams.VsaManagedZone, recordName)
	if err == nil {
		logger.Debugf("Deleting Cloud DNS record: %s.%s", recordName, commonparams.VsaManagedZone)
		err = gcpService.DeleteResourceRecordSet(commonparams.CaPoolDeployedProjectID, commonparams.VsaManagedZone, recordName)
		if err != nil {
			return err
		}
	}
	return nil
}
