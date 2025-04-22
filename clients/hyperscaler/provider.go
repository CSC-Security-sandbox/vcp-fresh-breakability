package hyperscaler

import (
	"google.golang.org/api/compute/v1"
	"google.golang.org/api/servicenetworking/v1"
)

// Services describes a gcp interface which contains the required methods to create or reuse a data path in a tenant project
type Services interface {
	InitializeClients() error
	GetSubnetwork(tenantProject, region, subnetName string) (*compute.Subnetwork, error)
}

type GoogleServices interface {
	Services
	IsAdminClientInitialized() bool
	GetTenantProject(consumerVPC string, customerProjectNumber string, tenantProjectRegion string) (string, error)
	CreateSubnetwork(consumerVPC, region, tenantProjectNumber string) (*servicenetworking.Subnetwork, error)
	AddSubnetwork(request *servicenetworking.AddSubnetworkRequest, tenantProjectNumber string) (*servicenetworking.Operation, error)
}

type TenancyInfo struct {
	TenantProjectNumber string
	TenantProjectId     string
}

type HostProjectInfo struct {
	ProjectNumber     string
	ProjectId         string
	PeeredNetworkName string
}
