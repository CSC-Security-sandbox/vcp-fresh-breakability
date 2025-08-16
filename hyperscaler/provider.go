package hyperscaler

import (
	"context"

	models "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

// Services describes a gcp interface which contains the required methods to create or reuse a data path in a tenant project
type Services interface {
	InitializeClients() error

	GetLogger() log.Logger
	GetContext() context.Context

	CreateVPC(vpcNetwork *models.VPCNetwork) (string, error)
	GetVPCNetwork(projectName, vpcNetworkName string) (*models.VPCNetwork, error)

	CreateSubnetwork(request *models.Subnet) (string, error)
	GetSubnetwork(projectName, region, subnetName string) (*models.Subnet, error)
	ReleaseSubnetwork(region, projectNumber, subnetwork string) error
	ListSubnetworks(projectName, region string) (*[]models.Subnet, error)

	InsertFirewall(firewallRule *models.Firewall) (string, error)
	UpdateFirewall(firewallRule *models.Firewall) (string, error)
	GetFirewall(projectName string, firewallName string) (*models.Firewall, error)

	GetAddress(projectName string, region string, address string) (*models.Address, error)
	CreateAddressOperation(address *models.Address) (string, error)
	ReleaseAddress(region, projectNumber, addressName string) (string, error)

	GetForwardingRule(projectName string, region string, endpointName string) (*models.ForwardingRule, error)
	CreateForwardingRuleOperation(forwardingRule *models.ForwardingRule) (string, error)
	DeleteForwardingRule(region, projectNumber, addressName string) (string, error)

	GetComputeRegionalOpStatus(projectNumber, region, operationName string) (*models.ComputeOperation, error)

	CreateBucketIfNotExists(ctx context.Context, projectID, bucketName, region string) error
	DeleteBucket(ctx context.Context, bucketName string) error

	GetServiceAccount(projectID, email string) (*models.ServiceAccount, error)
	CreateServiceAccount(createRequest *models.CreateServiceAccountRequest, projectNumber, email string) (account *models.ServiceAccount, err error)
	IsServiceAccountCreated(email string) (account *models.ServiceAccount, isSACreated bool, err error)
	AttachOrUpdateRolesForServiceAccounts(roles []string, serviceAccountEmail, projectID string) error
	RemoveRolesFromServiceAccounts(roles []string, serviceAccountEmail, projectID string) error
	DeleteServiceAccount(project string, email string) error
	GetServiceAccountByEmail(email string) (*models.ServiceAccount, error)

	CreateHmacKey(projectID string, serviceAccount string) (accessKey *string, secretKey *string, err error)
	DeleteHmacKey(projectID string, accessKey string, ServiceAccount string) error

	CreateCertificate(cert *models.CustomCertificate) (*models.CustomCertificate, error)
	RevokeCertificate(cert *models.CustomCertificate) (string, error)
	GetCertificate(projectID, region, poolName, certificateID string) (*models.CustomCertificate, error)

	CreateSecret(projectID, region, secretID, secretValue string) (*models.CustomSecret, error)
	GetSecretWithLatestVersion(projectID, secretID string) (*models.CustomSecret, error)
	DeleteSecret(projectID, secretID string) error
	CreateServiceAccountKey(ctx context.Context, email string) (*models.ServiceAccountKey, error)
	DeleteAllServiceAccountKeys(ctx context.Context, email string) error
	GetSecretWithCustomVersion(projectID, secretID string, versionID string) (*models.CustomSecret, error)
	DeleteServiceAccountKeysExcludingKey(ctx context.Context, email, keyToExclude string) error

	GetZones(projectName, region string) ([]string, error)
	IsMachineTypeAvailable(projectNumber, zone, machineType string) (bool, error)

	CreateResourceRecordSet(projectID, managedZone, ipAddress, recordName string) (*models.CustomCloudDNSRecord, error)
	GetResourceRecordSet(projectID, managedZone, recordName string) (*models.CustomCloudDNSRecord, error)
	DeleteResourceRecordSet(projectID, managedZone, recordName string) error

	CreateCloudRunService(ctx context.Context, config *models.CloudRunServiceConfig) (*models.CloudRunOperationResponse, error)
	CheckOperationStatus(ctx context.Context, operationName string) (bool, error)
	GetCloudRunServiceURL(ctx context.Context, projectID, locationID, serviceName string) (string, error)
	DeleteCloudRunService(ctx context.Context, projectID, locationID, serviceName string) (*models.CloudRunOperationResponse, error)
	GetIdentityToken() (string, error)
}

type GoogleServices interface {
	Services
	IsAdminClientInitialized() bool

	GetTenantProject(consumerNetwork, customerProjectNumber, tenantProjectRegion string) (string, error)
	GetSnHost(project string) (string, error)

	CreateTPSubnetOp(tenantProjectNumber, consumerNetwork, region, subnetName string) (*string, error)
	GetServiceNetOpStatus(operationName string) (*models.ComputeOperation, error)
	GetComputeGlobalOpStatus(tenantProject, operationName string) (*models.ComputeOperation, error)
	GetComputeRegionalOpStatus(projectNumber, region, operationName string) (*models.ComputeOperation, error)
}
