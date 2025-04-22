package google

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"google.golang.org/api/compute/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/servicenetworking/v1"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/util/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
)

var (
	waitTimeoutMinutes = time.Minute * time.Duration(env.GetInt("GCP_LRO_TIMEOUT_MINUTES", 20))

	minimumTenantNetworkSize = env.GetInt64("MIN_TENANT_NETWORK_SIZE", int64(28))
	defaultSleepTime         = time.Duration(env.GetInt64("GCP_NETWORK_SLEEP_SECONDS", int64(28))) * time.Second
)

// GetTenantProject lists registered tenancy units for the customer project
func (gcpService *GcpServices) GetTenantProject(consumerNetwork string, customerProjectNumber string, tenantProjectRegion string) (string, error) {
	parent := fmt.Sprintf("services/%s/projects/%s", gcpService.GetServiceConsumerManagementEndpoint(), customerProjectNumber)
	gcpService.Logger.Debug(fmt.Sprintf("Inside GetTenantProject. consumerNetwork: %s, customerProjectNumber: %s, tenantProjectRegion: %s , parent : %s ", consumerNetwork, customerProjectNumber, tenantProjectRegion, parent))

	tenantProjectsResp, err := gcpService.AdminGCPService.managementService.Services.TenancyUnits.List(parent).Do()
	if err != nil {
		gcpService.Logger.Debug(fmt.Sprintf("List TenancyUnits call failed : %s ", err.Error()))
		return "", err
	}

	for _, tenancy := range tenantProjectsResp.TenancyUnits {
		for _, tenantResource := range tenancy.TenantResources {
			tenantProjectNumber := strings.TrimPrefix(tenantResource.Resource, "projects/")
			if tenantResource.Tag == consumerNetwork+"-"+tenantProjectRegion {
				gcpService.Logger.Info("Found tenancy for 1P for remote account id: %s : %s", consumerNetwork, tenantProjectNumber)
				return tenantProjectNumber, nil
			}
		}
	}
	gcpService.Logger.Debug(fmt.Sprintf("Tenancy not found : consumerNetwork: %s, customerProjectNumber: %s, tenantProjectRegion: %s , parent : %s ", consumerNetwork, customerProjectNumber, tenantProjectRegion, parent))
	return "", errors.New(fmt.Sprintf("VPC peering network for TenancyUnit '%s' not found. Use the correct vpc name and ensure VPC network peering with tenant project has already been established.", consumerNetwork))
}

// AddSubnetwork calls GCP addSubnetwork API and return a long running operation.
func (gcpService *GcpServices) AddSubnetwork(request *servicenetworking.AddSubnetworkRequest, tenantProjectNumber string) (*servicenetworking.Operation, error) {
	parent := fmt.Sprintf("services/%s/projects/%s", gcpService.GetServiceNetworkingEndpoint(), tenantProjectNumber)
	tu, err := gcpService.AdminGCPService.networkingService.Services.AddSubnetwork(parent, request).Do()
	if err != nil || (tu != nil && tu.Error != nil) {
		if err == nil {
			err = &googleapi.Error{Message: tu.Error.Message}
		}
		if err != nil {
			if strings.Contains(err.Error(), "are not successfully connected yet") {
				gcpService.Logger.Debug(fmt.Sprintf("AddSubnetwork failed : err: %s", err.Error()))
				return nil, errors.New(err.Error())
			}
			gcpService.Logger.Debug(fmt.Sprintf("AddSubnetwork failed : err: %s", err.Error()))
			return nil, err
		}
	}
	gcpService.Logger.Debug(fmt.Sprintf("AddSubnetwork successful : Operation: %s", tu.Name))
	return tu, nil
}

// CreateSubnetwork creates GCP subnetwork
func (gcpService *GcpServices) CreateSubnetwork(consumerNetwork, region, tenantProjectNumber string) (*servicenetworking.Subnetwork, error) {
	consumerProjectNumber, consumerPeeringNetwork, err := parseProjectId(consumerNetwork)
	if err != nil {
		return nil, err
	}
	gcpService.Logger.Debug(fmt.Sprintf("consumerProjectNumber : %s consumerPeeringNetwork : %s", consumerProjectNumber, consumerPeeringNetwork))

	request := servicenetworking.AddSubnetworkRequest{
		Consumer:        "projects/" + consumerProjectNumber,
		ConsumerNetwork: consumerNetwork,
		Description:     "vsanetwork",
		IpPrefixLength:  minimumTenantNetworkSize,
		Region:          region,
		Subnetwork:      "vsa-" + region,
	}
	gcpService.Logger.Debug("AddSubnetworkRequest : ", request)

	snProducerOperation, err := gcpService.AddSubnetwork(&request, tenantProjectNumber)
	if err != nil {
		return nil, err
	}
	gcpService.Logger.Info("Waiting for service network operation status")
	snProducerOperation, err = waitForServiceNetworkOperationStatus(gcpService, snProducerOperation.Name)
	if err != nil {
		return nil, err
	}

	subnet := &servicenetworking.Subnetwork{}
	gcpService.Logger.Debug(fmt.Sprintf("snProducerOperation.Response %s", snProducerOperation.Response))

	if err := json.Unmarshal(snProducerOperation.Response, subnet); err != nil {
		gcpService.Logger.Debug(fmt.Sprintf("snProducerOperation.Response json unmarshal error %s", err.Error()))
		return nil, err
	}
	gcpService.Logger.Debug(fmt.Sprintf("Subnet IpCidrRange %s", subnet.IpCidrRange))
	gcpService.Logger.Debug(fmt.Sprintf("consumerPeeringNetwork %s", consumerPeeringNetwork))
	gcpService.Logger.Debug(fmt.Sprintf("subnet %s", subnet.Name))
	return subnet, nil
}

// GetSubnetwork retrieves a subnetwork
func (gcpService *GcpServices) GetSubnetwork(tenantProject, region, subnetName string) (*compute.Subnetwork, error) {
	gcpService.Logger.Debug(fmt.Sprintf("calling GetSubnetwork for tenantProject : %s, region : %s, subnetName : %s", tenantProject, region, subnetName))

	subnetwork, err := gcpService.AdminGCPService.computeService.Subnetworks.Get(tenantProject, region, subnetName).Do()
	if err != nil {
		if strings.Contains(err.Error(), "notFound") {
			return nil, errors.NewNotReadyErr(fmt.Sprintf("compute.Subnetwork %v", &subnetName))
		}
		gcpService.Logger.Debug(fmt.Sprintf("GetSubnetwork tenantProject : %s, region : %s, subnetName : %s", tenantProject, region, subnetName))
		return nil, err
		// TODO : gcpService.GetTrace().Info("Retrying to get subnetwork")
	}
	gcpService.retry.Reset()
	gcpService.Logger.Debug(fmt.Sprintf("GetSubnetwork success with response :  %s", subnetwork.Name))
	return subnetwork, nil
}
