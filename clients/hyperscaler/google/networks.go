package google

import (
	"fmt"
	"strings"
	"time"

	models "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/hyperscaler/models"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/servicenetworking/v1"
)

var (
	waitTimeoutMinutes       = time.Minute * time.Duration(env.GetInt("GCP_LRO_TIMEOUT_MINUTES", 20))
	minimumTenantNetworkSize = env.GetInt64("DATA_SUBNET_CIDR_BLOCK", int64(28))
	defaultSleepTime         = time.Duration(env.GetInt64("GCP_NETWORK_SLEEP_SECONDS", int64(28))) * time.Second

	CreateTPSubnetOp       = _createTPSubnetOp
	createSubnetwork       = _createSubnetwork
	createVPC              = _createVPC
	insertFirewall         = _insertFirewall
	updateFirewall         = _updateFirewall
	getProjectIDFromNumber = _getProjectIDFromNumber
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
	return "", vsaerrors.NewVCPError(vsaerrors.ErrPSAPeeringNotFoundError, errors.New(fmt.Sprintf("VPC peering network for TenancyUnit '%s' not found. Use the correct vpc name and ensure VPC network peering with tenant project has already been established.", consumerNetwork)))
}

// CreateTPSubnetOp returns GCP operation for creating subnetwork for a tenant project
func (gcpService *GcpServices) CreateTPSubnetOp(tenantProjectNumber, consumerNetwork, region, subnetName string) (*string, error) {
	consumerProjectNumber, consumerPeeringNetwork, err := utils.ParseProjectId(consumerNetwork)
	if err != nil {
		return nil, err
	}
	gcpService.Logger.Infof("Calling CreateTPSubnetOp consumerProjectNumber : %s consumerPeeringNetwork : %s tenantProjectNumber : %s region: %s subnet name : %s", consumerProjectNumber, consumerPeeringNetwork, tenantProjectNumber, region, subnetName)

	request := servicenetworking.AddSubnetworkRequest{
		Consumer:        "projects/" + consumerProjectNumber,
		ConsumerNetwork: consumerNetwork,
		Description:     "vsa-network",
		IpPrefixLength:  minimumTenantNetworkSize,
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
		gcpService.Logger.Errorf(fmt.Sprintf("error getting SN host for project %s", project))
		return "", err
	}
	// for a new VPC, snhost project will be empty. we need to return empty in this case to establish datalink
	if snProject != nil && snProject.Name == "" {
		return "", errors.NewNotFoundErr("SN Producer Host Project", &project)
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
			if strings.Contains(err.Error(), "are not successfully connected yet") {
				gcpService.Logger.Errorf("AddSubnetwork failed : with error : %v", err.Error())
				return nil, vsaerrors.NewVCPError(vsaerrors.ErrPSAPeeringNotFoundError, err)
			}
			gcpService.Logger.Errorf("CreateTPSubnetOp failed with error: %s", err.Error())
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceProvisionError, err)
		}
	}
	gcpService.Logger.Debugf("AddSubnetwork for tenant project : %s successful", tenantProjectNumber)
	return convertServiceNetOpToComputeOp(tu), nil
}

// GetSubnetwork retrieves a subnetwork for a given project name, region and subnetwork name using compute API. Reference : https://cloud.google.com/compute/docs/reference/rest/v1/subnetworks/get
func (gcpService *GcpServices) GetSubnetwork(projectName, region, subnetName string) (*models.Subnet, error) {
	defer gcpService.Retry.Reset()
	gcpService.Logger.Debugf("Calling GetSubnetwork for project name : %s, region : %s, subnet name : %s", projectName, region, subnetName)

	subnetwork, err := gcpService.AdminGCPService.computeService.Subnetworks.Get(projectName, region, subnetName).Context(gcpService.Ctx).Do()
	if err != nil {
		err = gcpService.Retry.Sleep(err)
		if err != nil {
			gcpService.Logger.Errorf("GetSubnetwork failed for project name : %s, region : %s, subnet name : %s with error : %v", projectName, region, subnetName, err.Error())
			return nil, err
		}
		gcpService.Logger.Debugf("Retrying : GetSubnetwork for project name : %s, region : %s, subnet name : %s", projectName, region, subnetName)
		return gcpService.GetSubnetwork(projectName, region, subnetName)
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
	defer gcpService.Retry.Reset()
	gcpService.Logger.Debugf("calling GetVPCNetwork for project name : %s, VPC network name : %s", projectName, vpcNetworkName)

	vpcNetwork, err := gcpService.AdminGCPService.computeService.Networks.Get(projectName, vpcNetworkName).Context(gcpService.Ctx).Do()
	if err != nil {
		err = gcpService.Retry.Sleep(err)
		if err != nil {
			gcpService.Logger.Errorf("GetVPCNetwork failed projectName : %s, VPC network name : %s with error : %v", projectName, vpcNetworkName, err.Error())
			return nil, err
		}
		gcpService.Logger.Debugf("GetVPCNetwork retrying project name : %s, VPC network name : %s", projectName, vpcNetworkName)
		return gcpService.GetVPCNetwork(projectName, vpcNetworkName)
	}
	gcpService.Logger.Debugf("GetVPCNetwork success with response :  %s", vpcNetwork.Name)
	return convertGoogleVPCToVPC(vpcNetwork), nil
}

// CreateVPC creates a VPC network in a project using compute API. This function also waits until the operation concludes. Reference : https://cloud.google.com/compute/docs/reference/rest/v1/networks/insert
func (gcpService *GcpServices) CreateVPC(vpcNetwork *models.VPCNetwork) error {
	projectName := vpcNetwork.ProjectName
	vpcNetworkName := vpcNetwork.Name
	// Call the Networks.Insert method to create the VPC
	operation, err := createVPC(gcpService, vpcNetwork)
	if err != nil {
		gcpService.Logger.Errorf("Failed to create VPC %s: with error : %v", vpcNetwork.Name, err.Error())
		return vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceProvisionError, err)
	}
	// Wait for the network creation operation to complete
	_, err = waitForComputeNetGlobalOpStatus(gcpService, projectName, operation.Name)
	if err != nil {
		gcpService.Logger.Errorf("Failed to create project name : %s VPC name: %s with error : %v", projectName, vpcNetworkName, err.Error())
		return vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceProvisionError, err)
	}
	gcpService.Logger.Infof("Successfully created VPC for project name : %s VPC name : %s", projectName, vpcNetworkName)
	return nil
}

// _createVPC creates an operation to create VPC network in a project using compute API. Reference : https://cloud.google.com/compute/docs/reference/rest/v1/networks/insert
func _createVPC(gcpService *GcpServices, vpcNetwork *models.VPCNetwork) (*models.ComputeOperation, error) {
	projectName := vpcNetwork.ProjectName
	vpcNetworkName := vpcNetwork.Name
	gcpService.Logger.Debugf("calling CreateVPC for project name : %s, VPC network name : %s", projectName, vpcNetworkName)
	defer gcpService.Retry.Reset()

	op, err := gcpService.AdminGCPService.computeService.Networks.Insert(projectName, convertVPCToGoogleVPC(vpcNetwork)).Context(gcpService.Ctx).Do()
	if err != nil {
		err = gcpService.Retry.Sleep(err)
		if err != nil {
			gcpService.Logger.Errorf("Failed to create VPC for project name : %s, VPC network name : %s with error : %v", projectName, vpcNetworkName, err.Error())
			return nil, err
		}
		gcpService.Logger.Debugf("Retrying CreateVPC for project name : %s, VPC network name : %s", projectName, vpcNetworkName)
		return _createVPC(gcpService, vpcNetwork)
	}
	gcpService.Logger.Debugf("Operation for Create VPC for project name : %s, VPC network name : %s created successfully", projectName, vpcNetworkName)
	return convertComputeOpToComputeOp(op), nil
}

// CreateSubnetwork allocates a subnetwork in a single region in VPC for a project using compute API. Reference : https://cloud.google.com/compute/docs/reference/rest/v1/subnetworks/insert
func (gcpService *GcpServices) CreateSubnetwork(request *models.Subnet) error {
	projectName := request.ProjectName
	gcpService.Logger.Debugf("Creating Subnet: %s for Project name: %s and VPC : %s ", request.Name, projectName, request.Network)

	// Create the Google subnetwork request
	operation, err := createSubnetwork(gcpService, request)
	if err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceProvisionError, err)
	}
	gcpService.Logger.Debugf("Waiting for compute network operation status during subnet creation Subnet: %s for Project name: %s and VPC : %s ", request.Name, projectName, request.Network)

	_, err = waitForComputeRegionalOperation(gcpService, projectName, *request.Region, operation.Name)
	if err != nil {
		if strings.Contains(err.Error(), "Timeout while confirming service network google components") {
			_, err = waitForComputeRegionalOperation(gcpService, projectName, *request.Region, operation.Name)
			if err != nil {
				gcpService.Logger.Errorf("Failed to create subnet Project name : %s, subnet name : %s with error : %v", projectName, request.Name, err.Error())
				return vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceProvisionError, err)
			}
		}
		gcpService.Logger.Errorf("Failed to create subnet Project name : %s, subnet name : %s with error : %v", projectName, request.Name, err)
		return vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceProvisionError, err)
	}
	gcpService.Logger.Debugf("Subnet created successfully for Subnet: %s for Project name: %s and VPC : %s ", request.Name, projectName, request.Network)
	return nil
}

// _createSubnetwork allocates a subnetwork in a single region in VPC for a project using compute API. Reference : https://cloud.google.com/compute/docs/reference/rest/v1/subnetworks/insert
func _createSubnetwork(gcpService *GcpServices, request *models.Subnet) (*models.ComputeOperation, error) {
	subnetName := request.Name
	subnetNetwork := request.Network
	projectName := request.ProjectName
	gcpService.Logger.Debugf("Calling create subnetwork  for project name : %s, VPC network name : %s, subnetwork name : %s", projectName, subnetNetwork, subnetName)
	defer gcpService.Retry.Reset()

	op, err := gcpService.AdminGCPService.computeService.Subnetworks.Insert(projectName, *request.Region, convertSubnetToGoogleSubnet(request)).Context(gcpService.Ctx).Do()
	if err != nil {
		err = gcpService.Retry.Sleep(err)
		if err != nil {
			gcpService.Logger.Errorf("Failed to create subnetwork for project name : %s, VPC network name : %s, subnetwork name : %s with error : %v", projectName, subnetNetwork, subnetName, err.Error())
			return nil, err
		}
		gcpService.Logger.Debugf("Retrying to create subnetwork for project name : %s, VPC network name : %s, subnetwork name : %s", projectName, subnetNetwork, subnetName)
		return _createSubnetwork(gcpService, request)
	}
	gcpService.Logger.Debugf("Operation to create subnetwork successful for project name : %s, VPC network name : %s, subnetwork name : %s", projectName, subnetNetwork, subnetName)
	return convertComputeOpToComputeOp(op), nil
}

// GetFirewall retrieves a firewall rule for given project name and firewall name using compute API. Reference : https://cloud.google.com/compute/docs/reference/rest/v1/firewalls/get
func (gcpService *GcpServices) GetFirewall(projectName string, firewallName string) (*models.Firewall, error) {
	defer gcpService.Retry.Reset()
	gcpService.Logger.Debugf("calling get firewall for project name : %s, firewall name : %s", projectName, firewallName)

	firewall, err := gcpService.AdminGCPService.computeService.Firewalls.Get(projectName, firewallName).Context(gcpService.Ctx).Do()
	if err != nil {
		err = gcpService.Retry.Sleep(err)
		if err != nil {
			gcpService.Logger.Errorf("Failed to get firewall for project name : %s, firewall name : %s with error : %v", projectName, firewallName, err.Error())
			return nil, err
		}
		gcpService.Logger.Debugf("Retrying to get firewall project name : %s, firewall name : %s", projectName, firewallName)
		return gcpService.GetFirewall(projectName, firewallName)
	}
	gcpService.Logger.Debugf("Get firewall successful with response :  %s", firewall.Name)
	return convertGCPFirewallToFirewall(firewall), nil
}

// InsertFirewall creates a firewall rule in a project using compute API. This function also waits until the operation concludes
func (gcpService *GcpServices) InsertFirewall(firewallRule *models.Firewall) error {
	projectName := firewallRule.ProjectName
	firewallName := firewallRule.Name
	gcpService.Logger.Debugf("Creating firewall rule %s for project %s ", firewallName, projectName)

	operation, err := insertFirewall(gcpService, firewallRule)
	if err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceProvisionError, err)
	}
	gcpService.Logger.Debugf("Waiting for compute network operation status during firewall rule creation project name: %s firewall rule: %s", projectName, firewallName)
	_, err = waitForComputeNetGlobalOpStatus(gcpService, projectName, operation.Name)
	if err != nil {
		gcpService.Logger.Errorf("Failed to create firewall rule %s for project %s. Error : %v", firewallName, projectName, err)
		return vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceProvisionError, err)
	}
	gcpService.Logger.Debugf("Successfully created firewall for project name : %s, firewall name : %s", projectName, firewallName)
	return nil
}

// UpdateFirewall creates a firewall rule in a project using compute API. This function also waits until the operation concludes
func (gcpService *GcpServices) UpdateFirewall(firewallRule *models.Firewall) error {
	projectName := firewallRule.ProjectName
	firewallName := firewallRule.Name
	gcpService.Logger.Debugf("Updating firewall rule %s for project %s ", firewallName, projectName)

	operation, err := updateFirewall(gcpService, firewallRule)
	if err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceProvisionError, err)
	}
	gcpService.Logger.Debugf("Waiting for compute network operation status during firewall rule updation project name: %s firewall rule: %s", projectName, firewallName)
	_, err = waitForComputeNetGlobalOpStatus(gcpService, projectName, operation.Name)
	if err != nil {
		gcpService.Logger.Errorf("Failed to update firewall rule %s for project %s. Error : %v", firewallName, projectName, err)
		return vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceProvisionError, err)
	}
	gcpService.Logger.Debugf("Successfully updated firewall for project name : %s, firewall name : %s", projectName, firewallName)
	return nil
}

// _insertFirewall creates an operation to create a firewall rule in a project using compute API. Reference : https://cloud.google.com/compute/docs/reference/rest/v1/firewalls/insert
func _insertFirewall(gcpService *GcpServices, request *models.Firewall) (*models.ComputeOperation, error) {
	defer gcpService.Retry.Reset()
	firewallName := request.Name
	projectName := request.ProjectName
	gcpService.Logger.Debugf("Inserting firewall rule %s for project %s ", firewallName, request.ProjectName)

	op, err := gcpService.AdminGCPService.computeService.Firewalls.Insert(projectName, convertToGoogleFirewallRule(request)).Context(gcpService.Ctx).Do()
	if err != nil {
		err = gcpService.Retry.Sleep(err)
		if err != nil {
			gcpService.Logger.Errorf("Insert firewall failed for project name : %s, firewall name : %s with error : %v", projectName, firewallName, err.Error())
			return nil, err
		}
		gcpService.Logger.Debugf("Retrying to insert firewall for project name : %s, firewall name : %s", projectName, firewallName)
		return _insertFirewall(gcpService, request)
	}
	gcpService.Logger.Debugf("Operation to insert firewall created successfully for project name : %s, firewall name : %s", projectName, firewallName)
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

// _updateFirewall creates an operation to update an existing firewall rule in a project using the compute API. Reference: https://cloud.google.com/compute/docs/reference/rest/v1/firewalls/update
func _updateFirewall(gcpService *GcpServices, request *models.Firewall) (*models.ComputeOperation, error) {
	defer gcpService.Retry.Reset()
	firewallName := request.Name
	projectName := request.ProjectName
	gcpService.Logger.Debugf("Updating firewall rule %s for project %s ", firewallName, request.ProjectName)

	op, err := gcpService.AdminGCPService.computeService.Firewalls.Update(projectName, firewallName, convertToGoogleFirewallRule(request)).Context(gcpService.Ctx).Do()
	if err != nil {
		gcpService.Logger.Debugf("Retrying to update firewall for project name : %s, firewall name : %s", projectName, firewallName)
		return nil, err
	}
	gcpService.Logger.Debugf("Operation to update firewall created successfully for project name : %s, firewall name : %s", projectName, firewallName)
	return convertComputeOpToComputeOp(op), nil
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
