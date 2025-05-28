package google

import (
	"fmt"
	"strings"
	"time"

	models "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/hyperscaler/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/servicenetworking/v1"
)

var (
	waitTimeoutMinutes = time.Minute * time.Duration(env.GetInt("GCP_LRO_TIMEOUT_MINUTES", 20))

	minimumTenantNetworkSize = env.GetInt64("MIN_TENANT_NETWORK_SIZE", int64(24))
	defaultSleepTime         = time.Duration(env.GetInt64("GCP_NETWORK_SLEEP_SECONDS", int64(28))) * time.Second

	createSubnetworkForTenantProject = _createSubnetworkForTenantProject
	createSubnetwork                 = _createSubnetwork
	createVPC                        = _createVPC
	insertFirewall                   = _insertFirewall
)

// GetTenantProject lists registered tenancy units for the customer project
func (gcpService *GcpServices) GetTenantProject(consumerNetwork, customerProjectNumber, tenantProjectRegion string) (string, error) {
	parent := fmt.Sprintf("services/%s/projects/%s", gcpService.GetServiceConsumerManagementEndpoint(), customerProjectNumber)
	gcpService.Logger.Debug(fmt.Sprintf("Inside GetTenantProject. consumerNetwork: %s, customerProjectNumber: %s, tenantProjectRegion: %s , parent : %s ", consumerNetwork, customerProjectNumber, tenantProjectRegion, parent))

	tenantProjectsResp, err := gcpService.AdminGCPService.managementService.Services.TenancyUnits.List(parent).Context(gcpService.Ctx).Do()
	if err != nil {
		gcpService.Logger.Debug(fmt.Sprintf("List TenancyUnits call failed : %s ", err.Error()))
		return "", err
	}

	for _, tenancy := range tenantProjectsResp.TenancyUnits {
		for _, tenantResource := range tenancy.TenantResources {
			tenantProjectNumber := strings.TrimPrefix(tenantResource.Resource, "projects/")
			if tenantResource.Tag == consumerNetwork+"-"+tenantProjectRegion {
				gcpService.Logger.Info(fmt.Sprintf("Found tenancy for 1P for Tenant project: %s, consumer network : %s", tenantProjectNumber, consumerNetwork))
				return tenantProjectNumber, nil
			}
		}
	}
	gcpService.Logger.Debug(fmt.Sprintf("Tenancy not found : consumerNetwork: %s, customerProjectNumber: %s, tenantProjectRegion: %s , parent : %s ", consumerNetwork, customerProjectNumber, tenantProjectRegion, parent))
	return "", errors.New(fmt.Sprintf("VPC peering network for TenancyUnit '%s' not found. Use the correct vpc name and ensure VPC network peering with tenant project has already been established.", consumerNetwork))
}

// CreateSubnetworkForTenantProject creates GCP subnetwork
func (gcpService *GcpServices) CreateSubnetworkForTenantProject(tenantProjectNumber, consumerNetwork, region string) ([]byte, error) {
	consumerProjectNumber, consumerPeeringNetwork, err := utils.ParseProjectId(consumerNetwork)
	if err != nil {
		return nil, err
	}
	gcpService.Logger.Debug(fmt.Sprintf("Calling CreateSubnetworkForTenantProject consumerProjectNumber : %s consumerPeeringNetwork : %s Consumer : %s region: %s", consumerProjectNumber, consumerPeeringNetwork, consumerProjectNumber, region))

	request := servicenetworking.AddSubnetworkRequest{
		Consumer:        "projects/" + consumerProjectNumber,
		ConsumerNetwork: consumerNetwork,
		Description:     "vsa-network",
		IpPrefixLength:  minimumTenantNetworkSize,
		Region:          region,
		Subnetwork:      "vsa-" + region,
	}
	snProducerOperation, err := createSubnetworkForTenantProject(gcpService, &request, tenantProjectNumber)
	if err != nil {
		return nil, err
	}
	snProducerOperationName := snProducerOperation.Name
	gcpService.Logger.Info(fmt.Sprintf("Waiting for service network operation status for tenant project : %s consumer peering network : %s", tenantProjectNumber, consumerPeeringNetwork))
	snProducerOperation, err = waitForServiceNetworkOperationStatus(gcpService, snProducerOperationName)
	if err != nil {
		if strings.Contains(err.Error(), "Timeout while confirming service network google components") {
			snProducerOperation, err = waitForServiceNetworkOperationStatus(gcpService, snProducerOperationName)
			if err != nil {
				gcpService.Logger.Errorf(fmt.Sprintf("Failed to get service networking operation status for tenant project : %s consumer peering network : %s with error : %v", tenantProjectNumber, consumerPeeringNetwork, err.Error()))
				return nil, err
			}
			return snProducerOperation.Response, nil
		}
		gcpService.Logger.Errorf(fmt.Sprintf("Failed to get service networking operation status for tenant project : %s consumer peering network : %s with error : %v", tenantProjectNumber, consumerPeeringNetwork, err.Error()))
		return nil, err
	}
	return snProducerOperation.Response, nil
}

// ReleaseSubnetwork calls GCP releaseSubnetwork API and return a long-running operation.
func (gcpService *GcpServices) ReleaseSubnetwork(region, tenantProjectNumber, subnetwork string) error {
	op, err := gcpService.AdminGCPService.computeService.Subnetworks.Delete(tenantProjectNumber, region, subnetwork).Do()
	if err != nil {
		if strings.Contains(err.Error(), "notFound") {
			return errors.NewNotFoundErr("compute.Subnetwork", &subnetwork)
		}
		gcpService.Logger.Debug("Failed to delete subnetwork...")
		return err
	}

	err = waitForComputeOperation(*gcpService, tenantProjectNumber, region, op.Name)
	if err != nil {
		gcpService.Logger.Error(fmt.Sprintf("Failed to delete subnetwork %s in project %s with error: %v", subnetwork, tenantProjectNumber, err))
		return err
	}

	return nil
}

// _createSubnetworkForTenantProject creates GCP subnetwork in a producer tenant project. This method will make producer's tenant project to be a shared VPC service project as needed. Reference : https://cloud.google.com/service-infrastructure/docs/service-networking/reference/rest/v1/services/addSubnetwork
func _createSubnetworkForTenantProject(gcpService *GcpServices, request *servicenetworking.AddSubnetworkRequest, tenantProjectNumber string) (*models.ComputeOperation, error) {
	parent := fmt.Sprintf("services/%s/projects/%s", gcpService.GetServiceNetworkingEndpoint(), tenantProjectNumber)
	tu, err := gcpService.AdminGCPService.networkingService.Services.AddSubnetwork(parent, request).Context(gcpService.Ctx).Do()
	if err != nil || (tu != nil && tu.Error != nil) {
		if err == nil {
			err = &googleapi.Error{Message: tu.Error.Message}
		}
		if err != nil {
			if strings.Contains(err.Error(), "are not successfully connected yet") {
				gcpService.Logger.Error(fmt.Sprintf("AddSubnetwork failed : with error : %v", err.Error()))
				return nil, errors.New(err.Error())
			}
			gcpService.Logger.Error(fmt.Sprintf("createSubnetworkForTenantProject failed with error: %s", err.Error()))
			return nil, err
		}
	}
	gcpService.Logger.Debug(fmt.Sprintf("AddSubnetwork for tenant project : %s successful", tenantProjectNumber))
	return convertServiceNetOpToComputeOp(tu), nil
}

// GetSubnetwork retrieves a subnetwork for a given project name, region and subnetwork name using compute API. Reference : https://cloud.google.com/compute/docs/reference/rest/v1/subnetworks/get
func (gcpService *GcpServices) GetSubnetwork(projectName, region, subnetName string) (*models.Subnet, error) {
	defer gcpService.Retry.Reset()
	gcpService.Logger.Debug(fmt.Sprintf("Calling GetSubnetwork for project name : %s, region : %s, subnet name : %s", projectName, region, subnetName))

	subnetwork, err := gcpService.AdminGCPService.computeService.Subnetworks.Get(projectName, region, subnetName).Context(gcpService.Ctx).Do()
	if err != nil {
		err = gcpService.Retry.Sleep(err)
		if err != nil {
			gcpService.Logger.Error(fmt.Sprintf("GetSubnetwork failed for project name : %s, region : %s, subnet name : %s with error : %v", projectName, region, subnetName, err.Error()))
			return nil, err
		}
		gcpService.Logger.Debug(fmt.Sprintf("Retrying : GetSubnetwork for project name : %s, region : %s, subnet name : %s", projectName, region, subnetName))
		return gcpService.GetSubnetwork(projectName, region, subnetName)
	}

	gcpService.Logger.Debug(fmt.Sprintf("GetSubnetwork success with response :  %s", subnetwork.Name))
	return convertGoogleSubnetToSubnet(subnetwork), nil
}

// GetVPCNetwork retrieves a VPC network for given project name and VPC network name
func (gcpService *GcpServices) GetVPCNetwork(projectName, vpcNetworkName string) (*models.VPCNetwork, error) {
	defer gcpService.Retry.Reset()
	gcpService.Logger.Debug(fmt.Sprintf("calling GetVPCNetwork for project name : %s, VPC network name : %s", projectName, vpcNetworkName))

	vpcNetwork, err := gcpService.AdminGCPService.computeService.Networks.Get(projectName, vpcNetworkName).Context(gcpService.Ctx).Do()
	if err != nil {
		err = gcpService.Retry.Sleep(err)
		if err != nil {
			gcpService.Logger.Error(fmt.Sprintf("GetVPCNetwork failed projectName : %s, VPC network name : %s with error : %v", projectName, vpcNetworkName, err.Error()))
			return nil, err
		}
		gcpService.Logger.Debug(fmt.Sprintf("GetVPCNetwork retrying project name : %s, VPC network name : %s", projectName, vpcNetworkName))
		return gcpService.GetVPCNetwork(projectName, vpcNetworkName)
	}
	gcpService.Logger.Debug(fmt.Sprintf("GetVPCNetwork success with response :  %s", vpcNetwork.Name))
	return convertGoogleVPCToVPC(vpcNetwork), nil
}

// CreateVPC creates a VPC network in a project using compute API. This function also waits until the operation concludes. Reference : https://cloud.google.com/compute/docs/reference/rest/v1/networks/insert
func (gcpService *GcpServices) CreateVPC(vpcNetwork *models.VPCNetwork) error {
	projectName := vpcNetwork.ProjectName
	vpcNetworkName := vpcNetwork.Name
	// Call the Networks.Insert method to create the VPC
	operation, err := createVPC(gcpService, vpcNetwork)
	if err != nil {
		gcpService.Logger.Error(fmt.Sprintf("Failed to create VPC %s: with error : %v", vpcNetwork.Name, err.Error()))
		return err
	}
	// Wait for the network creation operation to complete
	_, err = waitForComputeNetGlobalOpStatus(gcpService, projectName, operation.Name)
	if err != nil {
		gcpService.Logger.Error(fmt.Sprintf("Failed to create project name : %s VPC name: %s with error : %v", projectName, vpcNetworkName, err.Error()))
		return err
	}
	gcpService.Logger.Info(fmt.Sprintf("Successfully created VPC for project name : %s VPC name : %s", projectName, vpcNetworkName))
	return nil
}

// _createVPC creates an operation to create VPC network in a project using compute API. Reference : https://cloud.google.com/compute/docs/reference/rest/v1/networks/insert
func _createVPC(gcpService *GcpServices, vpcNetwork *models.VPCNetwork) (*models.ComputeOperation, error) {
	projectName := vpcNetwork.ProjectName
	vpcNetworkName := vpcNetwork.Name
	gcpService.Logger.Debug(fmt.Sprintf("calling CreateVPC for project name : %s, VPC network name : %s", projectName, vpcNetworkName))
	defer gcpService.Retry.Reset()

	op, err := gcpService.AdminGCPService.computeService.Networks.Insert(projectName, convertVPCToGoogleVPC(vpcNetwork)).Context(gcpService.Ctx).Do()
	if err != nil {
		err = gcpService.Retry.Sleep(err)
		if err != nil {
			gcpService.Logger.Error(fmt.Sprintf("Failed to create VPC for project name : %s, VPC network name : %s with error : %v", projectName, vpcNetworkName, err.Error()))
			return nil, err
		}
		gcpService.Logger.Debug(fmt.Sprintf("Retrying CreateVPC for project name : %s, VPC network name : %s", projectName, vpcNetworkName))
		return _createVPC(gcpService, vpcNetwork)
	}
	gcpService.Logger.Debug(fmt.Sprintf("Operation for Create VPC for project name : %s, VPC network name : %s created successfully", projectName, vpcNetworkName))
	return convertComputeOpToComputeOp(op), nil
}

// CreateSubnetwork allocates a subnetwork in a single region in VPC for a project using compute API. Reference : https://cloud.google.com/compute/docs/reference/rest/v1/subnetworks/insert
func (gcpService *GcpServices) CreateSubnetwork(request *models.Subnet) error {
	projectName := request.ProjectName
	gcpService.Logger.Debug(fmt.Sprintf("Creating Subnet: %s for Project name: %s and VPC : %s ", request.Name, projectName, request.Network))

	// Create the Google subnetwork request
	operation, err := createSubnetwork(gcpService, request)
	if err != nil {
		return err
	}
	gcpService.Logger.Debug(fmt.Sprintf("Waiting for compute network operation status during subnet creation Subnet: %s for Project name: %s and VPC : %s ", request.Name, projectName, request.Network))

	_, err = waitForComputeRegionalOperation(gcpService, projectName, *request.Region, operation.Name)
	if err != nil {
		if strings.Contains(err.Error(), "Timeout while confirming service network google components") {
			_, err = waitForComputeRegionalOperation(gcpService, projectName, *request.Region, operation.Name)
			if err != nil {
				gcpService.Logger.Error(fmt.Sprintf("Failed to create subnet Project name : %s, subnet name : %s with error : %v", projectName, request.Name, err.Error()))
				return err
			}
		}
		gcpService.Logger.Error(fmt.Sprintf("Failed to create subnet Project name : %s, subnet name : %s with error : %v", projectName, request.Name, err.Error()))
		return err
	}
	gcpService.Logger.Debug(fmt.Sprintf("Subnet created successfully for Subnet: %s for Project name: %s and VPC : %s ", request.Name, projectName, request.Network))
	return nil
}

// _createSubnetwork allocates a subnetwork in a single region in VPC for a project using compute API. Reference : https://cloud.google.com/compute/docs/reference/rest/v1/subnetworks/insert
func _createSubnetwork(gcpService *GcpServices, request *models.Subnet) (*models.ComputeOperation, error) {
	subnetName := request.Name
	subnetNetwork := request.Network
	projectName := request.ProjectName
	gcpService.Logger.Debug(fmt.Sprintf("Calling create subnetwork  for project name : %s, VPC network name : %s, subnetwork name : %s", projectName, subnetNetwork, subnetName))
	defer gcpService.Retry.Reset()

	op, err := gcpService.AdminGCPService.computeService.Subnetworks.Insert(projectName, *request.Region, convertSubnetToGoogleSubnet(request)).Context(gcpService.Ctx).Do()
	if err != nil {
		err = gcpService.Retry.Sleep(err)
		if err != nil {
			gcpService.Logger.Error(fmt.Sprintf("Failed to create subnetwork for project name : %s, VPC network name : %s, subnetwork name : %s with error : %v", projectName, subnetNetwork, subnetName, err.Error()))
			return nil, err
		}
		gcpService.Logger.Debug(fmt.Sprintf("Retrying to create subnetwork for project name : %s, VPC network name : %s, subnetwork name : %s", projectName, subnetNetwork, subnetName))
		return _createSubnetwork(gcpService, request)
	}
	gcpService.Logger.Debug(fmt.Sprintf("Operation to create subnetwork successful for project name : %s, VPC network name : %s, subnetwork name : %s", projectName, subnetNetwork, subnetName))
	return convertComputeOpToComputeOp(op), nil
}

// GetFirewall retrieves a firewall rule for given project name and firewall name using compute API. Reference : https://cloud.google.com/compute/docs/reference/rest/v1/firewalls/get
func (gcpService *GcpServices) GetFirewall(projectName string, firewallName string) (*models.Firewall, error) {
	defer gcpService.Retry.Reset()
	gcpService.Logger.Debug(fmt.Sprintf("calling get firewall for project name : %s, firewall name : %s", projectName, firewallName))

	firewall, err := gcpService.AdminGCPService.computeService.Firewalls.Get(projectName, firewallName).Context(gcpService.Ctx).Do()
	if err != nil {
		err = gcpService.Retry.Sleep(err)
		if err != nil {
			gcpService.Logger.Error(fmt.Sprintf("Failed to get firewall for project name : %s, firewall name : %s with error : %v", projectName, firewallName, err.Error()))
			return nil, err
		}
		gcpService.Logger.Debug(fmt.Sprintf("Retrying to get firewall project name : %s, firewall name : %s", projectName, firewallName))
		return gcpService.GetFirewall(projectName, firewallName)
	}
	gcpService.Logger.Debug(fmt.Sprintf("Get firewall successful with response :  %s", firewall.Name))
	return convertGCPFirewallToFirewall(firewall), nil
}

// InsertFirewall creates a firewall rule in a project using compute API. This function also waits until the operation concludes
func (gcpService *GcpServices) InsertFirewall(firewallRule *models.Firewall) error {
	projectName := firewallRule.ProjectName
	firewallName := firewallRule.Name
	gcpService.Logger.Debug(fmt.Sprintf("Creating firewall rule %s for project %s ", firewallName, projectName))

	operation, err := insertFirewall(gcpService, firewallRule)
	if err != nil {
		return err
	}
	gcpService.Logger.Debug(fmt.Sprintf("Waiting for compute network operation status during firewall rule creation project name: %s firewall rule: %s", projectName, firewallName))
	_, err = waitForComputeNetGlobalOpStatus(gcpService, projectName, operation.Name)
	if err != nil {
		gcpService.Logger.Error(fmt.Sprintf("Failed to create firewall rule %s for project %s. Error : %v", firewallName, projectName, err))
		return err
	}
	gcpService.Logger.Debug(fmt.Sprintf("Successfully created firewall for project name : %s, firewall name : %s", projectName, firewallName))
	return nil
}

// _insertFirewall creates an operation to create a firewall rule in a project using compute API. Reference : https://cloud.google.com/compute/docs/reference/rest/v1/firewalls/insert
func _insertFirewall(gcpService *GcpServices, request *models.Firewall) (*models.ComputeOperation, error) {
	defer gcpService.Retry.Reset()
	firewallName := request.Name
	projectName := request.ProjectName
	gcpService.Logger.Debug(fmt.Sprintf("Inserting firewall rule %s for project %s ", firewallName, request.ProjectName))

	op, err := gcpService.AdminGCPService.computeService.Firewalls.Insert(projectName, convertToGoogleFirewallRule(request)).Context(gcpService.Ctx).Do()
	if err != nil {
		err = gcpService.Retry.Sleep(err)
		if err != nil {
			gcpService.Logger.Error(fmt.Sprintf("Insert firewall failed for project name : %s, firewall name : %s with error : %v", projectName, firewallName, err.Error()))
			return nil, err
		}
		gcpService.Logger.Debug(fmt.Sprintf("Retrying to insert firewall for project name : %s, firewall name : %s", projectName, firewallName))
		return _insertFirewall(gcpService, request)
	}
	gcpService.Logger.Debug(fmt.Sprintf("Operation to insert firewall created successfully for project name : %s, firewall name : %s", projectName, firewallName))
	return convertComputeOpToComputeOp(op), nil
}
