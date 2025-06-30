package hyperscaler

import (
	"context"

	models "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/hyperscaler/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"google.golang.org/api/iam/v1"
)

// Services describes a gcp interface which contains the required methods to create or reuse a data path in a tenant project
type Services interface {
	InitializeClients() error

	GetLogger() log.Logger

	CreateVPC(vpcNetwork *models.VPCNetwork) error
	GetVPCNetwork(projectName, vpcNetworkName string) (*models.VPCNetwork, error)

	CreateSubnetwork(request *models.Subnet) error
	GetSubnetwork(projectName, region, subnetName string) (*models.Subnet, error)
	ReleaseSubnetwork(region, projectNumber, subnetwork string) error

	InsertFirewall(firewallRule *models.Firewall) error
	GetFirewall(projectName string, firewallName string) (*models.Firewall, error)

	CreateBucketIfNotExists(ctx context.Context, projectID, bucketName, region string) error
	DeleteBucket(ctx context.Context, bucketName string) error

	GetServiceAccount(projectID, email string) (*iam.ServiceAccount, error)
	CreateServiceAccount(createRequest *iam.CreateServiceAccountRequest, projectNumber, email string) (account *iam.ServiceAccount, err error)
	IsServiceAccountCreated(email string) (account *iam.ServiceAccount, isSACreated bool, err error)
	AttachOrUpdateRolesForServiceAccounts(roles []string, serviceAccountEmail, projectID string) error
	DeleteServiceAccount(email string) error

	CreateHmacKey(projectID string, serviceAccount string) (accessKey *string, secretKey *string, err error)
	DeleteHmacKey(projectID string, accessKey string, ServiceAccount string) error

	CreateCertificate(cert *models.CustomCertificate) (*models.CustomCertificate, error)
	RevokeCertificate(cert *models.CustomCertificate) (string, error)
	GetCertificate(projectID, region, poolName, certificateID string) (*models.CustomCertificate, error)

	CreateSecret(projectID, region, secretID, secretValue string) (*models.CustomSecret, error)
	GetSecretWithLatestVersion(projectID, secretID string) (*models.CustomSecret, error)
	DeleteSecret(projectID, secretID string) error

	CreateServiceAccountKey(ctx context.Context, email string) (*iam.ServiceAccountKey, error)
	DeleteAllServiceAccountKeys(ctx context.Context, email string) error
}

type GoogleServices interface {
	Services
	IsAdminClientInitialized() bool

	GetTenantProject(consumerNetwork, customerProjectNumber, tenantProjectRegion string) (string, error)
	GetSnHost(project string) (string, error)

	CreateSubnetworkForTenantProject(tenantProjectNumber, consumerNetwork, region string) ([]byte, error)
}
