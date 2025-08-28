package google

import (
	"fmt"
	"strings"
	"time"

	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	models "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/servicenetworking/v1"
)

var (
	waitTimeoutMinutes         = time.Minute * time.Duration(env.GetInt("GCP_LRO_TIMEOUT_MINUTES", 20))
	minimumTenantNetworkSize   = env.GetInt64("DATA_SUBNET_CIDR_BLOCK", int64(28))
	minimumLVTenantNetworkSize = env.GetInt64("DATA_SUBNET_CIDR_BLOCK_LV", int64(26))
	defaultSleepTime           = time.Duration(env.GetInt64("GCP_NETWORK_SLEEP_SECONDS", int64(28))) * time.Second

	CreateTPSubnetOp       = _createTPSubnetOp
	getProjectIDFromNumber = _getProjectIDFromNumber
	createAddress          = _createAddress
	createForwardingRule   = _createForwardingRule
)

// GetTenantProject lists registered tenancy units for the customer project
func (gcpService *GcpServices) GetTenantProject(consumerNetwork, customerProjectNumber, tenantProjectRegion string) (string, error) {
	parent := fmt.Sprintf("services/%s/projects/%s", gcpService.GetServiceConsumerManagementEndpoint(), customerProjectNumber)
	gcpService.Logger.Debugf("Inside GetTenantProject. consumerNetwork: %s, customerProjectNumber: %s, tenantProjectRegion: %s , parent : %s ", consumerNetwork, customerProjectNumber, tenantProjectRegion, parent)

	tenantProjectsResp, err := gcpService.AdminGCPService.managementService.Services.TenancyUnits.List(parent).Context(gcpService.Ctx).Do()
	if err != nil {
		gcpService.Logger.Debugf("List TenancyUnits call failed : %s ", err.Error())
		return "", vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceFetchError, err)
	}

	for _, tenancy := range tenantProjectsResp.TenancyUnits {
		for _, tenantResource := range tenancy.TenantResources {
			tenantProjectNumber := strings.TrimPrefix(tenantResource.Resource, "projects/")
			if tenantResource.Tag == consumerNetwork+"-"+tenantProjectRegion {
				gcpService.Logger.Infof("Found tenancy for 1P for Tenant project: %s, consumer network : %s", tenantProjectNumber, consumerNetwork)
				return tenantProjectNumber, nil
			}
		}
	}
	gcpService.Logger.Debugf("Tenancy not found : consumerNetwork: %s, customerProjectNumber: %s, tenantProjectRegion: %s , parent : %s ", consumerNetwork, customerProjectNumber, tenantProjectRegion, parent)
	return "", vsaerrors.WrapAsNonRetryableTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrPSAPeeringNotFoundError, fmt.Errorf("vpc peering network for TenancyUnit '%s' not found. Use the correct vpc name and ensure VPC network peering with tenant project has already been established", consumerNetwork)))
}

// getNetworkSize returns the appropriate network size based on the isLargeCapacity flag
func getNetworkSize(isLargeCapacity bool) int64 {
	if isLargeCapacity {
		return minimumLVTenantNetworkSize
	}
	return minimumTenantNetworkSize
}

// CreateTPSubnetOp returns GCP operation for creating subnetwork for a tenant project
func (gcpService *GcpServices) CreateTPSubnetOp(tenantProjectNumber, consumerNetwork, region, subnetName string, isLargeCapacity bool) (*string, error) {
	consumerProjectNumber, consumerPeeringNetwork, err := utils.ParseProjectId(consumerNetwork)
	if err != nil {
		return nil, err
	}
	gcpService.Logger.Infof("Calling CreateTPSubnetOp consumerProjectNumber : %s consumerPeeringNetwork : %s tenantProjectNumber : %s region: %s subnet name : %s isLargeCapacity: %t", consumerProjectNumber, consumerPeeringNetwork, tenantProjectNumber, region, subnetName, isLargeCapacity)

	// Use the calculator to determine the appropriate network size
	networkSize := getNetworkSize(isLargeCapacity)

	request := servicenetworking.AddSubnetworkRequest{
		Consumer:        "projects/" + consumerProjectNumber,
		ConsumerNetwork: consumerNetwork,
		Description:     "vsa-network",
		IpPrefixLength:  networkSize,
		Region:          region,
		Subnetwork:      subnetName,
	}
	snProducerOperation, err := CreateTPSubnetOp(gcpService, &request, tenantProjectNumber)
	if err != nil {
		return nil, err
	}
	return &snProducerOperation.Name, nil
}

// ReleaseSubnetwork calls GCP releaseSubnetwork API and return a long-running operation.
func (gcpService *GcpServices) ReleaseSubnetwork(region, projectName, subnetwork string) error {
	op, err := gcpService.AdminGCPService.computeService.Subnetworks.Delete(projectName, region, subnetwork).Do()
	if err != nil {
		if strings.Contains(err.Error(), "notFound") {
			// If the subnetwork is not found, it means it has already been deleted or never existed.
			return nil
		}
		if strings.Contains(err.Error(), "resourceInUseByAnotherResource") {
			gcpService.Logger.Debugf("Failed to delete subnetwork because it is in use by another resource: %s, error : %s", subnetwork, err.Error())
			return nil
		}
		gcpService.Logger.Debug("Failed to delete subnetwork...")
		return vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceDeprovisionError, err)
	}

	err = waitForComputeOperation(*gcpService, projectName, region, fmt.Sprintf("(name=%s)", op.Name))
	if err != nil {
		// TODO: Add VCP Error for this
		gcpService.Logger.Error(fmt.Sprintf("Failed to delete subnetwork %s in project %s with error: %v", subnetwork, projectName, err))
		return vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceDeprovisionError, err)
	}

	return nil
}

// GetSnHost returns host project peered with the given service project
func (gcpService *GcpServices) GetSnHost(project string) (string, error) {
	snProject, err := gcpService.AdminGCPService.computeService.Projects.GetXpnHost(project).Do()
	if err != nil {
		gcpService.Logger.Errorf(fmt.Sprintf("error getting SN host for project : %s, Error : %v", project, err))
		if strings.Contains(err.Error(), "Please create Service Networking connection with service") {
			return "", vsaerrors.WrapAsNonRetryableTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrPSAPeeringNotFoundError, fmt.Errorf("SN Producer Host Project %s Error: %v", project, err)))
		}
		return "", err
	}
	// for a new VPC, snhost project will be empty. we need to return empty in this case to establish datalink
	if snProject != nil && snProject.Name == "" {
		return "", vsaerrors.WrapAsNonRetryableTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrPSAPeeringNotFoundError, fmt.Errorf("SN Producer Host Project %s Error: %v", project, err)))
	}
	return snProject.Name, nil
}

// _createTPSubnetOp returns GCP operation for subnetwork in a producer tenant project. This method will make producer's tenant project to be a shared VPC service project as needed. Reference : https://cloud.google.com/service-infrastructure/docs/service-networking/reference/rest/v1/services/addSubnetwork
func _createTPSubnetOp(gcpService *GcpServices, request *servicenetworking.AddSubnetworkRequest, tenantProjectNumber string) (*models.ComputeOperation, error) {
	parent := fmt.Sprintf("services/%s/projects/%s", gcpService.GetServiceNetworkingEndpoint(), tenantProjectNumber)
	tu, err := gcpService.AdminGCPService.networkingService.Services.AddSubnetwork(parent, request).Context(gcpService.Ctx).Do()
	if err != nil || (tu != nil && tu.Error != nil) {
		if err == nil {
			err = &googleapi.Error{Message: tu.Error.Message}
		}
		if err != nil {
			if strings.Contains(err.Error(), "are not successfully connected yet") || strings.Contains(err.Error(), "Please create Service Networking connection with service") {
				gcpService.Logger.Errorf("CreateTPSubnetOp failed : with error : %v", err.Error())
				return nil, vsaerrors.WrapAsNonRetryableTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrPSAPeeringNotFoundError, err))
			} else if strings.Contains(err.Error(), "Couldn't find free blocks in allocated IP ranges. Please allocate new ranges for this service provider") {
				gcpService.Logger.Errorf("CreateTPSubnetOp failed : with error : %v", err.Error())
				return nil, vsaerrors.WrapAsNonRetryableTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrGCPCustomerIPExhaustion, err))
			}
			gcpService.Logger.Errorf("CreateTPSubnetOp failed with error: %s", err.Error())
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceProvisionError, err)
		}
	}
	gcpService.Logger.Debugf("CreateTPSubnetOp for tenant project : %s successful", tenantProjectNumber)
	return convertServiceNetOpToComputeOp(tu), nil
}

// GetSubnetwork retrieves a subnetwork for a given project name, region and subnetwork name using compute API. Reference : https://cloud.google.com/compute/docs/reference/rest/v1/subnetworks/get
func (gcpService *GcpServices) GetSubnetwork(projectName, region, subnetName string) (*models.Subnet, error) {
	gcpService.Logger.Debugf("Calling GetSubnetwork for project name : %s, region : %s, subnet name : %s", projectName, region, subnetName)

	subnetwork, err := gcpService.AdminGCPService.computeService.Subnetworks.Get(projectName, region, subnetName).Context(gcpService.Ctx).Do()
	if err != nil {
		gcpService.Logger.Errorf("GetSubnetwork failed for project name : %s, region : %s, subnet name : %s with error : %v", projectName, region, subnetName, err.Error())
		return nil, err
	}
	gcpService.Logger.Debugf("GetSubnetwork success with response :  %s", subnetwork.Name)
	return convertGoogleSubnetToSubnet(subnetwork), nil
}

// ListSubnetworks retrieves a subnetwork for a given project name, region and subnetwork name using compute API. Reference : https://cloud.google.com/compute/docs/reference/rest/v1/subnetworks/get
func (gcpService *GcpServices) ListSubnetworks(projectName, region string) (*[]models.Subnet, error) {
	subnetworks, err := gcpService.AdminGCPService.computeService.Subnetworks.List(projectName, region).Context(gcpService.Ctx).Do()
	if err != nil {
		gcpService.Logger.Debugf("ListSubnetworks failed for project name : %s, region : %s", projectName, region)
		return nil, err
	}
	gcpService.Logger.Debugf("ListSubnetworks success with number of subnets = %d", len(subnetworks.Items))
	return convertGoogleSubnetsToSubnets(subnetworks), nil
}

// GetVPCNetwork retrieves a VPC network for given project name and VPC network name
func (gcpService *GcpServices) GetVPCNetwork(projectName, vpcNetworkName string) (*models.VPCNetwork, error) {
	gcpService.Logger.Debugf("calling GetVPCNetwork for project name : %s, VPC network name : %s", projectName, vpcNetworkName)

	vpcNetwork, err := gcpService.AdminGCPService.computeService.Networks.Get(projectName, vpcNetworkName).Context(gcpService.Ctx).Do()
	if err != nil {
		gcpService.Logger.Errorf("GetVPCNetwork failed projectName : %s, VPC network name : %s with error : %v", projectName, vpcNetworkName, err.Error())
		return nil, err
	}
	gcpService.Logger.Debugf("GetVPCNetwork success with response :  %s", vpcNetwork.Name)
	return convertGoogleVPCToVPC(vpcNetwork), nil
}

// CreateVPC creates a VPC network in a project using compute API. This function also waits until the operation concludes. Reference : https://cloud.google.com/compute/docs/reference/rest/v1/networks/insert
func (gcpService *GcpServices) CreateVPC(vpcNetwork *models.VPCNetwork) (string, error) {
	projectName := vpcNetwork.ProjectName
	vpcNetworkName := vpcNetwork.Name
	op, err := gcpService.AdminGCPService.computeService.Networks.Insert(projectName, convertVPCToGoogleVPC(vpcNetwork)).Context(gcpService.Ctx).Do()
	if err != nil {
		gcpService.Logger.Errorf("Failed to create VPC for project name : %s, VPC network name : %s with error : %v", projectName, vpcNetworkName, err.Error())
		return "", vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceProvisionError, err)
	}
	gcpService.Logger.Debugf("Operation for Create VPC for project name : %s, VPC network name : %s created successfully", projectName, vpcNetworkName)
	return op.Name, nil
}

// CreateSubnetwork allocates a subnetwork in a single region in VPC for a project using compute API. Reference : https://cloud.google.com/compute/docs/reference/rest/v1/subnetworks/insert
func (gcpService *GcpServices) CreateSubnetwork(request *models.Subnet) (string, error) {
	projectName := request.ProjectName
	gcpService.Logger.Debugf("Creating Subnet: %s for Project name: %s and VPC : %s ", request.Name, projectName, request.Network)

	op, err := gcpService.AdminGCPService.computeService.Subnetworks.Insert(projectName, *request.Region, convertSubnetToGoogleSubnet(request)).Context(gcpService.Ctx).Do()
	if err != nil {
		gcpService.Logger.Errorf("Failed to create subnetwork for project name : %s, VPC network name : %s, subnetwork name : %s with error : %v", projectName, request.Network, request.Name, err.Error())
		return "", vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceProvisionError, err)
	}
	gcpService.Logger.Debugf("Operation to create subnetwork successful for project name : %s, VPC network name : %s, subnetwork name : %s", projectName, request.Network, request.Name)
	return op.Name, nil
}

// GetFirewall retrieves a firewall rule for given project name and firewall name using compute API. Reference : https://cloud.google.com/compute/docs/reference/rest/v1/firewalls/get
func (gcpService *GcpServices) GetFirewall(projectName string, firewallName string) (*models.Firewall, error) {
	gcpService.Logger.Debugf("calling get firewall for project name : %s, firewall name : %s", projectName, firewallName)

	firewall, err := gcpService.AdminGCPService.computeService.Firewalls.Get(projectName, firewallName).Context(gcpService.Ctx).Do()
	if err != nil {
		gcpService.Logger.Errorf("Failed to get firewall for project name : %s, firewall name : %s with error : %v", projectName, firewallName, err.Error())
		return nil, err
	}
	gcpService.Logger.Debugf("Get firewall successful with response :  %s", firewall.Name)
	return convertGCPFirewallToFirewall(firewall), nil
}

// getAddress retrieves an address for given project name, region and address name using compute API. Reference : https://cloud.google.com/compute/docs/reference/rest/v1/addresses/get
func (gcpService *GcpServices) GetAddress(projectName string, region string, address string) (*models.Address, error) {
	gcpService.Logger.Debugf("calling get address for project name : %s, address name : %s", projectName, address)

	ipAddress, err := gcpService.AdminGCPService.computeService.Addresses.Get(projectName, region, address).Context(gcpService.Ctx).Do()
	if err != nil {
		gcpService.Logger.Errorf("Failed to get address for project name : %s, address name : %s with error : %v", projectName, address, err.Error())
		return nil, err
	}

	gcpService.Logger.Debugf("Get address successful with response :  %s", ipAddress.Name)
	return convertGCPAddressToAddress(ipAddress), nil
}

func (gcpService *GcpServices) CreateAddressOperation(address *models.Address) (string, error) {
	projectName := address.ProjectId
	addressName := address.AddressName

	gcpService.Logger.Infof("Creating address %s for project %s ", addressName, projectName)

	operation, err := createAddress(gcpService, address)
	if err != nil {
		gcpService.Logger.Errorf("Failed to create address %s for project %s. Error: %+v", addressName, projectName, err)
		return "", err
	}

	return operation.Name, nil
}

func (gcpService *GcpServices) CreateForwardingRuleOperation(address *models.ForwardingRule) (string, error) {
	projectName := address.ProjectId
	addressName := address.Name
	gcpService.Logger.Infof("Creating forwarding rule %s for project %s ", addressName, projectName)

	operation, err := createForwardingRule(gcpService, address)
	if err != nil {
		return "", err
	}

	return operation.Name, nil
}

func (gcpService *GcpServices) ReleaseAddress(region, projectNumber, address string) (string, error) {
	op, err := gcpService.AdminGCPService.computeService.Addresses.Delete(projectNumber, region, address).Do()
	if err != nil {
		if strings.Contains(err.Error(), "notFound") {
			return "", vsaerrors.NewVCPError(vsaerrors.ErrResourceNotFound, errors.NewNotFoundErr("compute.Subnetwork", &address))
		}
		if strings.Contains(err.Error(), "resourceInUseByAnotherResource") {
			gcpService.Logger.Warnf("Failed to delete address because it is in use by another resource: %s, error : %s", address, err.Error())
			return "", nil
		}
		gcpService.Logger.Error("Failed to delete address...")
		return "", vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceDeprovisionError, err)
	}

	return op.Name, nil
}

// create address
func _createAddress(gcpService *GcpServices, request *models.Address) (*models.ComputeOperation, error) {
	addressName := request.AddressName
	projectName := request.ProjectId
	gcpService.Logger.Debugf("Calling create address for project name : %s, address name : %s", projectName, addressName)

	op, err := gcpService.AdminGCPService.computeService.Addresses.Insert(projectName, request.Region, convertAddressToGoogleAddress(request)).Context(gcpService.Ctx).Do()

	if err != nil {
		gcpService.Logger.Errorf("Failed to create address for project name : %s, address name : %s with error : %v", projectName, addressName, err.Error())
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceDeprovisionError, err)
	}

	gcpService.Logger.Debugf("Operation to create address successful for project name : %s,address name : %s", projectName, addressName)
	return convertComputeOpToComputeOp(op), nil
}

// getForwardingRules retrieves a forwarding rule for given project name, region and endpoint name using compute API. Reference : https://cloud.google.com/compute/docs/reference/rest/v1/forwardingRules/get
func (gcpService *GcpServices) GetForwardingRule(projectName string, region string, endpointName string) (*models.ForwardingRule, error) {
	gcpService.Logger.Debugf("calling get forwarding rules for project name : %s, forwarding rule name : %s", projectName, endpointName)

	forwardingrule, err := gcpService.AdminGCPService.computeService.ForwardingRules.Get(projectName, region, endpointName).Context(gcpService.Ctx).Do()
	if err != nil {
		gcpService.Logger.Errorf("Failed to get forwarding rules for project name : %s, endpoint name : %s with error : %v", projectName, endpointName, err.Error())
		return nil, err
	}
	gcpService.Logger.Debugf("Get forwarding rules successful with response :  %s", forwardingrule.Name)
	return convertGCPForwardingRuleToForwardingRule(forwardingrule), nil
}

func _createForwardingRule(gcpService *GcpServices, request *models.ForwardingRule) (*models.ComputeOperation, error) {
	addressName := request.Name
	projectName := request.ProjectId
	gcpService.Logger.Debugf("Calling create forwarding rule for project name : %s, forwarding rule : %s", projectName, addressName)

	op, err := gcpService.AdminGCPService.computeService.ForwardingRules.Insert(projectName, request.Region, convertForwardingRuleToGoogleForwardingRule(request)).Context(gcpService.Ctx).Do()

	if err != nil {
		gcpService.Logger.Errorf("Failed to create forwarding rule for project name : %s, forwarding rule : %s with error : %v", projectName, addressName, err.Error())
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceProvisionError, err)
	}

	gcpService.Logger.Debugf("Operation to create subnetwork successful for project name : %s,address name : %s", projectName, addressName)
	return convertComputeOpToComputeOp(op), nil
}

func (gcpService *GcpServices) DeleteForwardingRule(region, projectNumber, forwardingRule string) (string, error) {
	op, err := gcpService.AdminGCPService.computeService.ForwardingRules.Delete(projectNumber, region, forwardingRule).Do()
	if err != nil {
		if strings.Contains(err.Error(), "notFound") {
			return "", vsaerrors.NewVCPError(vsaerrors.ErrResourceNotFound, errors.NewNotFoundErr("compute.Subnetwork", &forwardingRule))
		}
		if strings.Contains(err.Error(), "resourceInUseByAnotherResource") {
			gcpService.Logger.Errorf("Failed to delete forwarding rule because it is in use by another resource: %s, error : %s", forwardingRule, err.Error())
			return "", nil
		}
		gcpService.Logger.Error("Failed to delete forwardingRule...")
		return "", vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceDeprovisionError, err)
	}

	return op.Name, nil
}

// InsertFirewall creates a firewall rule in a project using compute API. This function also waits until the operation concludes
func (gcpService *GcpServices) InsertFirewall(firewallRule *models.Firewall) (string, error) {
	projectName := firewallRule.ProjectName
	firewallName := firewallRule.Name

	gcpService.Logger.Debugf("Inserting firewall rule %s for project %s ", firewallName, projectName)
	op, err := gcpService.AdminGCPService.computeService.Firewalls.Insert(projectName, convertToGoogleFirewallRule(firewallRule)).Context(gcpService.Ctx).Do()
	if err != nil {
		gcpService.Logger.Errorf("Insert firewall failed for project name : %s, firewall name : %s with error : %v", projectName, firewallName, err.Error())
		return "", vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceProvisionError, err)
	}
	gcpService.Logger.Debugf("Operation to insert firewall created successfully for project name : %s, firewall name : %s", projectName, firewallName)
	return op.Name, nil
}

// UpdateFirewall creates a firewall rule in a project using compute API. This function also waits until the operation concludes
func (gcpService *GcpServices) UpdateFirewall(firewallRule *models.Firewall) (string, error) {
	projectName := firewallRule.ProjectName
	firewallName := firewallRule.Name
	gcpService.Logger.Debugf("Updating firewall rule %s for project %s ", firewallName, projectName)

	op, err := gcpService.AdminGCPService.computeService.Firewalls.Update(projectName, firewallName, convertToGoogleFirewallRule(firewallRule)).Context(gcpService.Ctx).Do()
	if err != nil {
		gcpService.Logger.Debugf("failed to update firewall for project name : %s, firewall name : %s", projectName, firewallName)
		return "", vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceProvisionError, err)
	}
	gcpService.Logger.Debugf("Operation to update firewall created successfully for project name : %s, firewall name : %s", projectName, firewallName)
	return op.Name, nil
}

// GetServiceNetOpStatus get the status of the operation on basis of the operation name
func (gcpService *GcpServices) GetServiceNetOpStatus(operationName string) (*models.ComputeOperation, error) {
	op, err := gcpService.AdminGCPService.networkingService.Operations.Get(operationName).Do()
	if err != nil {
		gcpService.Logger.Errorf(fmt.Sprintf("GetServiceNetOpStatus failed with error : %s", err.Error()))
		return nil, err
	}
	if op != nil && op.Error != nil {
		gcpService.Logger.Debug(fmt.Sprintf("GetServiceNetOpStatus's operation failed with error : %s", op.Error.Message))
		err = &googleapi.Error{Message: op.Error.Message}
		return nil, err
	}
	gcpService.Logger.Debug(fmt.Sprintf("GetServiceNetOpStatus successful : %s", op.Name))
	return convertServiceNetOpToComputeOp(op), nil
}

// GetComputeGlobalOpStatus gets ComputeOperation object for the given tenantProject, operationName
func (gcpService *GcpServices) GetComputeGlobalOpStatus(tenantProject, operationName string) (*models.ComputeOperation, error) {
	op, err := gcpService.AdminGCPService.computeService.GlobalOperations.Get(tenantProject, operationName).Do()
	if err != nil {
		gcpService.Logger.Errorf("Failed to get compute global operation status for project %s with operation name %s: %v", tenantProject, operationName, err)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceFetchError, err)
	}

	if op != nil && op.Error != nil {
		gcpService.Logger.Errorf("Failed to get compute global operation status for project %s with operation name %s: %v", tenantProject, operationName, &googleapi.Error{Message: op.Error.Errors[0].Message})
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceFetchError, &googleapi.Error{Message: op.Error.Errors[0].Message})
	}
	gcpService.Logger.Debug(fmt.Sprintf("getComputeGlobalOpStatus successful : %s", op.Name))
	return convertComputeOpToComputeOp(op), nil
}

// GetComputeRegionalOpStatus gets ComputeOperation object for the given projectNumber, region, operationName
func (gcpService *GcpServices) GetComputeRegionalOpStatus(projectNumber, region, operationName string) (*models.ComputeOperation, error) {
	op, err := gcpService.AdminGCPService.computeService.RegionOperations.Get(projectNumber, region, operationName).Do()
	if err != nil {
		gcpService.Logger.Errorf("Failed to get compute regional operation status for project %s with region %s operation name %s: %v", projectNumber, region, operationName, err)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceFetchError, err)
	}
	if op != nil && op.Error != nil {
		gcpService.Logger.Debug(fmt.Sprintf("getComputeRegionalOpStatus's operation failed with error : %s", op.Error.Errors[0].Message))
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceFetchError, &googleapi.Error{Message: op.Error.Errors[0].Message})
	}
	return convertComputeOpToComputeOp(op), nil
}

// GetZones retrieves a list of zones for a given project name using compute API. Reference : https://cloud.google.com/compute/docs/reference/rest/v1/zones/list
func (gcpService *GcpServices) GetZones(projectNumber, region string) ([]string, error) {
	projectName, err := getProjectIDFromNumber(gcpService, projectNumber)
	if err != nil {
		gcpService.Logger.Errorf("GetZones failed for project number : %s with error : %v", projectNumber, err)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceFetchError, err)
	}

	gcpService.Logger.Debugf("Calling GetZones for project name : %s", projectName)

	regionUrl := "https://www.googleapis.com/compute/v1/projects/" + projectName + "/regions/" + region
	zoneList, err := gcpService.AdminGCPService.computeService.Zones.List(projectName).Filter("region= \"" + regionUrl + "\"").Context(gcpService.Ctx).Do()
	if err != nil {
		gcpService.Logger.Errorf("GetZones failed for project name : %s with error : %v", projectName, err.Error())
		return nil, err
	}
	var zones []string
	for _, zone := range zoneList.Items {
		zones = append(zones, zone.Name)
	}
	gcpService.Logger.Debugf("GetZones success with number of zones = %d", len(zones))
	return zones, nil
}

// IsMachineTypeAvailable checks if a specific machine type is available in a given project and zone using compute API. Reference : https://cloud.google.com/compute/docs/reference/rest/v1/machineTypes/get
func (gcpService *GcpServices) IsMachineTypeAvailable(projectNumber, zone, machineType string) (bool, error) {
	projectName, err := getProjectIDFromNumber(gcpService, projectNumber)
	if err != nil {
		gcpService.Logger.Errorf("IsMachineTypeAvailable failed for project number : %s with error : %v", projectNumber, err)
		return false, vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceFetchError, err)
	}
	gcpService.Logger.Debugf("Checking if machine type %s is available in project name : %s, zone : %s", machineType, projectName, zone)

	_, err = gcpService.AdminGCPService.computeService.MachineTypes.Get(projectName, zone, machineType).Context(gcpService.Ctx).Do()
	if err != nil {
		gcpService.Logger.Debugf("Machine type %s is not available in zone %s", machineType, zone)
		return false, nil
	}
	gcpService.Logger.Debugf("Machine type %s is available in zone %s", machineType, zone)
	return true, nil
}

// _getProjectIDFromNumber retrieves the project ID from the project number using compute API. Reference : https://cloud.google.com/compute/docs/reference/rest/v1/projects/get
func _getProjectIDFromNumber(gcpService *GcpServices, projectNumber string) (string, error) {
	project, err := gcpService.AdminGCPService.computeService.Projects.Get(projectNumber).Context(gcpService.Ctx).Do()
	if err != nil {
		return "", err
	}
	return project.Name, nil
}
