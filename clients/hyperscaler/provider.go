package hyperscaler

import (
	models "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/hyperscaler/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

// Services describes a gcp interface which contains the required methods to create or reuse a data path in a tenant project
type Services interface {
	InitializeClients() error

	GetLogger() log.Logger

	CreateVPC(vpcNetwork *models.VPCNetwork) error
	GetVPCNetwork(projectName, vpcNetworkName string) (*models.VPCNetwork, error)

	CreateSubnetwork(request *models.Subnet) error
	GetSubnetwork(projectName, region, subnetName string) (*models.Subnet, error)

	InsertFirewall(firewallRule *models.Firewall) error
	GetFirewall(projectName string, firewallName string) (*models.Firewall, error)
	ReleaseSubnetwork(region, tenantProjectNumber, subnetwork string) error
}

type GoogleServices interface {
	Services
	IsAdminClientInitialized() bool

	GetTenantProject(consumerNetwork, customerProjectNumber, tenantProjectRegion string) (string, error)

	CreateSubnetworkForTenantProject(tenantProjectNumber, consumerNetwork, region string) ([]byte, error)
}
