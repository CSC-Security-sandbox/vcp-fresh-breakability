package common

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	logger "golang.org/x/exp/slog"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/cloudresourcemanager/v1"
	"google.golang.org/api/compute/v1"
	"google.golang.org/api/deploymentmanager/v2"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	scopesHttp "google.golang.org/api/transport/http"
	"gopkg.in/yaml.v2"
)

var (
	vsaDeploymentTimeout      = time.Duration(env.GetInt("VSA_DEPLOYMENT_TIMEOUT", 5)) * time.Minute
	vsaDeploymentPollInterval = time.Duration(env.GetInt("VSA_DEPLOYMENT_POLL_INTERVAL", 10)) * time.Second
	firewallSourceRange       = env.GetString("FIREWALL_SOURCE_RANGE", "10.0.0.0/8,172.16.0.0/12,192.168.0.0/16,34.0.0.0/8,46.149.16.0/20,52.94.203.152/29,52.94.203.160/29,185.35.244.0/22,202.3.112.0/20,216.240.16.0/20,217.70.208.0/20,198.18.0.0/15")
)

type DeploymentConfig struct {
	Imports   []Import   `yaml:"imports"`
	Resources []Resource `yaml:"resources"`
}

type Import struct {
	Path string `yaml:"path"`
	Name string `yaml:"name"`
}

type Resource struct {
	Name       string     `yaml:"name"`
	Type       string     `yaml:"type"`
	Properties Properties `yaml:"properties"`
}

type Properties struct {
	EnableFlashCache               bool              `yaml:"enableFlashCache"`
	HaDeployment                   HaDeployment      `yaml:"haDeployment"`
	Labels                         map[string]string `yaml:"labels"`
	MachineType                    string            `yaml:"machineType"`
	DataDisksStorageCapacityInGB   int               `yaml:"dataDisksStorageCapacityInGB"`
	PlatformSerialNumberNode1      string            `yaml:"platformSerialNumberNode1"`
	Region                         string            `yaml:"region"`
	SourceImage                    SourceImage       `yaml:"sourceImage"`
	VirtualNetworkInterface0       NetworkInterface  `yaml:"virtualNetworkInterface0"`
	VirtualNetworkInterfaceForData NetworkInterface  `yaml:"virtualNetworkInterfaceForData"`
	Zone                           string            `yaml:"zone"`
	NumberOfHaPairs                int               `yaml:"numberOfHaPairs"`
}

type HaDeployment struct {
	NonSharedHaDeployment     NonSharedHaDeployment `yaml:"nonSharedHaDeployment"`
	PlatformSerialNumberNode2 string                `yaml:"platformSerialNumberNode2"`
	VirtualNetworkInterface1  NetworkInterface      `yaml:"virtualNetworkInterface1"`
	VirtualNetworkInterface2  NetworkInterface      `yaml:"virtualNetworkInterface2"`
	Zone                      string                `yaml:"zone"`
}

type NonSharedHaDeployment struct {
	Mediator                 Mediator         `yaml:"mediator"`
	VirtualNetworkInterface3 NetworkInterface `yaml:"virtualNetworkInterface3"`
}

type Mediator struct {
	MediatorImage            MediatorImage    `yaml:"mediatorImage"`
	VirtualNetworkInterface0 NetworkInterface `yaml:"virtualNetworkInterface0"`
}

type MediatorImage struct {
	Name string `yaml:"name"`
}

type NetworkInterface struct {
	Network string `yaml:"network"`
	Subnet  string `yaml:"subnet"`
	Project string `yaml:"project"`
}

type SourceImage struct {
	Name string `yaml:"name"`
}

// DeploymentsInsert creates a new Deployment Manager deployment.
func DeploymentsInsert(ctx context.Context, name, region, zone, network, subnet, projectId, snHostProject string, size int) (*[]map[string]string, error) {
	// slog := ctx.Value(middleware.ContextSLoggerKey).(log.Logger)
	slog := log.NewLogger()
	err := SetupNetwork(slog, projectId, snHostProject, network, region)
	if err != nil {
		return nil, err
	}

	serviceAccountEmail := projectId + "@cloudservices.gserviceaccount.com"

	err = grantComputeSubnetworkUsePermission(slog, snHostProject, serviceAccountEmail)
	if err != nil {
		slog.Errorf("Error granting permission: %v", err)
	}
	deploymentmanagerService, err := deploymentmanager.NewService(ctx)
	if err != nil {
		return nil, err
	}
	content, err := os.ReadFile("common/vsa_config/sample.yaml")
	if err != nil {
		return nil, err
	}

	var config DeploymentConfig
	err = yaml.Unmarshal(content, &config)
	if err != nil {
		return nil, err
	}

	config.Resources[0].Properties.VirtualNetworkInterfaceForData.Network = network
	config.Resources[0].Properties.VirtualNetworkInterfaceForData.Subnet = subnet
	config.Resources[0].Properties.VirtualNetworkInterfaceForData.Project = snHostProject
	config.Resources[0].Properties.Region = region
	config.Resources[0].Properties.Zone = zone
	config.Resources[0].Properties.HaDeployment.Zone = zone
	config.Resources[0].Properties.DataDisksStorageCapacityInGB = size
	modifiedContent, err := yaml.Marshal(&config)
	if err != nil {
		return nil, err
	}

	configFile := deploymentmanager.ConfigFile{Content: string(modifiedContent)}

	f1Content, err := os.ReadFile("common/vsa_config/netapp-cvo-deployment.py")
	if err != nil {
		return nil, err
	}
	f2Content, err := os.ReadFile("common/vsa_config/netapp-cvo-deployment.py.schema")
	if err != nil {
		return nil, err
	}

	file1 := deploymentmanager.ImportFile{Name: "netapp-cvo-deployment.py", Content: string(f1Content)}
	file2 := deploymentmanager.ImportFile{Name: "netapp-cvo-deployment.py.schema", Content: string(f2Content)}
	imports := []*deploymentmanager.ImportFile{&file1, &file2}

	target := deploymentmanager.TargetConfiguration{Config: &configFile, Imports: imports}
	deployment := deploymentmanager.Deployment{Name: name, Target: &target}

	var resourcesList *deploymentmanager.ResourcesListResponse
	res, err := deploymentmanagerService.Deployments.Insert(projectId, &deployment).Do()
	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			resourcesList, err = deploymentmanagerService.Resources.List(projectId, name).Do()
			if err != nil {
				slog.Errorf("Error listing resources: %v", err)
				return nil, err
			}
		} else {
			return nil, err
		}
	} else {
		resourcesList, err = pollDeploymentStatus(slog, deploymentmanagerService, projectId, name, res.Name)
		if err != nil {
			slog.Errorf("Error creating deployment: %v", err)
			return nil, err
		}
	}

	slog.Infof("Instance created: %v\n", res)

	computeInstancesIPAddress, err := getIPAddressDetails(slog, projectId, resourcesList)
	if err != nil {
		slog.Errorf("Error getting IP address details : %v", err)
		return nil, err
	}

	return &computeInstancesIPAddress, nil
}

func pollDeploymentStatus(slog log.Logger, service *deploymentmanager.Service, projectId, deploymentName, operationName string) (*deploymentmanager.ResourcesListResponse, error) {
	startTime := time.Now()

	for time.Since(startTime) < vsaDeploymentTimeout {
		operation, err := service.Operations.Get(projectId, operationName).Do()
		if err != nil {
			slog.Errorf("Error getting operation(operation name : %s): %v", operationName, err)
			return nil, err
		}

		if operation.Status == "DONE" {
			if operation.Error != nil {
				slog.Errorf("Deployment failed")
				for _, e := range operation.Error.Errors {
					slog.Errorf("Error Code: %s, Message: %s\n", e.Code, e.Message)
				}
				return nil, fmt.Errorf("%v", operation.Error)
			}
			slog.Infof("Deployment completed successfully!")

			resources, err := service.Resources.List(projectId, deploymentName).Do()
			if err != nil {
				slog.Errorf("Error listing resources(deployment name : %s): %v", deploymentName, err)
				return nil, err
			}
			return resources, nil
		}
		// Log the current status with time elapsed
		slog.Infof("Deployment status: %s, Time elapsed: %v", operation.Status, time.Since(startTime))
		time.Sleep(vsaDeploymentPollInterval)
	}

	return nil, fmt.Errorf("deployment creation timed out")
}

func getIPAddressDetails(slog log.Logger, projectId string, resources *deploymentmanager.ResourcesListResponse) ([]map[string]string, error) {
	// Filter resources to fetch only compute instances and their IPs
	var computeInstancesIPAddress []map[string]string
	for _, resource := range resources.Resources {
		if resource.Type == "compute.v1.instance" && !strings.Contains(resource.Name, "mediator") {
			// Parse resource.Properties YAML
			var propertiesMap map[string]interface{}
			if err := yaml.Unmarshal([]byte(resource.Properties), &propertiesMap); err != nil {
				slog.Errorf("Error parsing properties YAML: %v", err)
				return nil, err
			}

			zone, ok := propertiesMap["zone"].(string)
			if !ok {
				return nil, fmt.Errorf("zone property is not a string")
			}

			instanceDetails, err := getInstanceDetails(projectId, resource.Name, zone)
			if err != nil {
				slog.Errorf("Error getting instance details(resource name : %s): %v", resource.Name, err)
				return nil, err
			}
			slog.Infof("Instance details: %v", instanceDetails)
			computeInstancesIPAddress = append(computeInstancesIPAddress, instanceDetails)
		}
	}
	// TODO : as only one once has external IP , using the same for both
	computeInstancesIPAddress[1]["NodeIp"] = computeInstancesIPAddress[0]["NodeIp"]
	return computeInstancesIPAddress, nil
}

func getInstanceDetails(projectId, instanceName, zone string) (map[string]string, error) {
	ctx := context.Background()
	computeService, err := compute.NewService(ctx)
	numNetworkInterfaces := 5
	if err != nil {
		return nil, err
	}

	instance, err := computeService.Instances.Get(projectId, zone, instanceName).Do()
	if err != nil {
		return nil, err
	}

	// Ensure there are enough network interfaces and access configs
	if len(instance.NetworkInterfaces) < numNetworkInterfaces {
		return nil, fmt.Errorf("instance does not have the expected network interfaces")
	}

	instanceDetails := map[string]string{
		"Name":        instance.Name,
		"InternalIP":  instance.NetworkInterfaces[4].NetworkIP,
		"Zone":        instance.Zone,
		"MachineType": instance.MachineType,
	}

	if len(instance.NetworkInterfaces) >= 1 && len(instance.NetworkInterfaces[0].AccessConfigs) >= 1 {
		instanceDetails["NodeIp"] = instance.NetworkInterfaces[0].AccessConfigs[0].NatIP
	}
	if len(instance.NetworkInterfaces) >= 5 && len(instance.NetworkInterfaces[4].AliasIpRanges) >= 1 {
		instanceDetails["dataLif"] = instance.NetworkInterfaces[4].AliasIpRanges[0].IpCidrRange
	}
	return instanceDetails, nil
}

func DeleteDeployment(ctx context.Context, projectId, deploymentName string) error {
	slog := log.NewLogger()
	deploymentmanagerService, err := deploymentmanager.NewService(ctx)
	if err != nil {
		return err
	}

	// Initiate the delete operation
	op, err := deploymentmanagerService.Deployments.Delete(projectId, deploymentName).Do()
	if err != nil {
		return err
	}

	// Wait for the delete operation to complete
	for {
		operation, err := deploymentmanagerService.Operations.Get(projectId, op.Name).Do()
		if err != nil {
			return err
		}

		if operation.Status == "DONE" {
			if operation.Error != nil {
				return err
			}
			slog.Infof("Deployment deleted successfully!")
			break
		}
		slog.Infof("Delete status: %s", operation.Status)
		time.Sleep(2 * time.Second)
	}

	return nil
}

func SetupNetwork(slog log.Logger, project, snHostProject, network, tpregion string) error {
	projectID := project
	region := tpregion
	sourceRanges := strings.Split(firewallSourceRange, ",")

	ctx := context.Background()
	computeService, err := compute.NewService(ctx)
	if err != nil {
		slog.Errorf("Failed to create compute service: %v", err)
		return err
	}

	vpcSubnetMap := map[string]string{
		"mgmt-vpc":         "mgmt-subnet",
		"cluster-ic-vpc":   "cluster-ic-subnet",
		"interconnect-vpc": "interconnect-subnet",
		"rsm-vpc":          "rsm-subnet",
	}

	i := 1
	for vpcName, subnetName := range vpcSubnetMap {
		firewallName := fmt.Sprintf("ingress-%s", vpcName)
		// Check if VPC exists
		_, err = computeService.Networks.Get(projectID, vpcName).Context(ctx).Do()
		if err != nil {
			if gErr, ok := err.(*googleapi.Error); ok && gErr.Code == 404 {
				slog.Infof("Creating VPC: %s in project %s...\n", vpcName, projectID)
				op, err1 := computeService.Networks.Insert(projectID, &compute.Network{
					Name:                  vpcName,
					AutoCreateSubnetworks: false,
					// make sure AutoCreateSubnetworks field is included in request
					ForceSendFields: []string{"AutoCreateSubnetworks"},
				}).Context(ctx).Do()
				if err1 != nil {
					slog.Errorf("Failed to create VPC %s: %v", vpcName, err)
					return err
				}
				// Wait for the network creation operation to complete
				err = waitForOperation(ctx, computeService, projectID, op)
				if err != nil {
					slog.Errorf("Failed to wait for VPC creation %s: %v", vpcName, err)
				}
			} else {
				slog.Errorf("Failed to check VPC %s: %v", vpcName, err)
				return err
			}
		} else {
			slog.Errorf("VPC %s already exists.\n", vpcName)
		}

		// Check if Subnet exists
		_, err = computeService.Subnetworks.Get(projectID, region, subnetName).Context(ctx).Do()
		if err != nil {
			if gErr, ok := err.(*googleapi.Error); ok && gErr.Code == 404 {
				slog.Infof("Creating subnet: %s in %s...\n", subnetName, vpcName)
				op, err := computeService.Subnetworks.Insert(projectID, region, &compute.Subnetwork{
					Name:                  subnetName,
					Network:               fmt.Sprintf("projects/%s/global/networks/%s", projectID, vpcName),
					IpCidrRange:           fmt.Sprintf("198.18.%d.0/27", i*3),
					PrivateIpGoogleAccess: true,
				}).Context(ctx).Do()
				if err != nil {
					slog.Errorf("Failed to create subnet %s: %v", subnetName, err)
					return err
				}
				// Wait for the subnet creation operation to complete
				err = waitForRegionalOperation(ctx, computeService, projectID, region, op.Name)
				if err != nil {
					slog.Errorf("Failed to wait for subnet creation %s: %v", subnetName, err)
				}
			} else {
				slog.Errorf("Failed to check subnet %s: %v", subnetName, err)
				return err
			}
		} else {
			slog.Infof("Subnet %s already exists.\n", subnetName)
		}

		// Check if Firewall rule exists
		_, err = computeService.Firewalls.Get(projectID, firewallName).Context(ctx).Do()
		if err != nil {
			if gErr, ok := err.(*googleapi.Error); ok && gErr.Code == 404 {
				slog.Infof("Creating firewall rule: %s for VPC: %s...\n", firewallName, vpcName)
				_, err = computeService.Firewalls.Insert(projectID, &compute.Firewall{
					Name:    firewallName,
					Network: fmt.Sprintf("projects/%s/global/networks/%s", projectID, vpcName),
					Allowed: []*compute.FirewallAllowed{
						{
							IPProtocol: "tcp",
						},
						{
							IPProtocol: "udp",
						},
						{
							IPProtocol: "icmp",
						},
					},
					SourceRanges: sourceRanges,
					Direction:    "INGRESS",
					Priority:     1000,
				}).Context(ctx).Do()
				if err != nil {
					slog.Errorf("Failed to create firewall rule %s: %v", firewallName, err)
					return err
				}
			} else {
				slog.Errorf("Failed to check firewall rule %s: %v", firewallName, err)
				return err
			}
		} else {
			slog.Infof("Firewall rule %s already exists.\n", firewallName)
		}
		i++
	}

	slog.Infof("All VPCs, subnets, and firewall rules created successfully in project %s!\n", projectID)

	slog.Infof("Checking if firewall rule: iscsi-ingress exists for VPC: %s...\n", network)
	_, err = computeService.Firewalls.Get(snHostProject, "iscsi-ingress").Context(ctx).Do()
	if err != nil {
		if gErr, ok := err.(*googleapi.Error); ok && gErr.Code == 404 {
			slog.Infof("Creating firewall rule: iscsi-ingress for VPC: %s...\n", network)
			_, err = computeService.Firewalls.Insert(snHostProject, &compute.Firewall{
				Name:    "iscsi-ingress",
				Network: fmt.Sprintf("projects/%s/global/networks/%s", snHostProject, network),
				Allowed: []*compute.FirewallAllowed{
					{
						IPProtocol: "tcp",
						Ports:      []string{"3260"}, // iSCSI port
					},
				},
				SourceRanges: []string{"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"},
				Direction:    "INGRESS",
				Priority:     1000,
			}).Context(ctx).Do()
			if err != nil {
				slog.Errorf("Failed to create firewall rule iscsi-ingress: %v", err)
				return err
			}
		} else {
			slog.Errorf("Failed to check firewall rule iscsi-ingress: %v", err)
			return err
		}
	} else {
		slog.Infof("Firewall rule iscsi-ingress already exists.\n")
	}

	return nil
}

func waitForOperation(ctx context.Context, computeService *compute.Service, projectID string, op *compute.Operation) error {
	for {
		result, err := computeService.GlobalOperations.Get(projectID, op.Name).Context(ctx).Do()
		if err != nil {
			return fmt.Errorf("failed to get operation: %v", err)
		}
		if result.Status == "DONE" {
			if result.Error != nil {
				return fmt.Errorf("operation error: %+v", result.Error.Errors)
			}
			break
		}
		time.Sleep(2 * time.Second)
	}
	return nil
}

func waitForRegionalOperation(ctx context.Context, computeService *compute.Service, projectID, region, opName string) error {
	for {
		result, err := computeService.RegionOperations.Get(projectID, region, opName).Context(ctx).Do()
		if err != nil {
			return fmt.Errorf("failed to get regional operation: %v", err)
		}
		if result.Status == "DONE" {
			if result.Error != nil {
				return fmt.Errorf("operation error: %+v", result.Error.Errors)
			}
			break
		}
		time.Sleep(2 * time.Second)
	}
	return nil
}

func grantComputeSubnetworkUsePermission(slog log.Logger, projectID, serviceAccountEmail string) error {
	ctx := context.Background()

	// Create a Cloud Resource Manager service

	scopesOption := option.WithScopes(cloudresourcemanager.CloudPlatformScope)
	opts := []option.ClientOption{scopesOption}
	logger.Debug(fmt.Sprintf("opts: %#v", opts))

	opts = append(opts, option.WithTokenSource(google.ComputeTokenSource("", cloudresourcemanager.CloudPlatformScope)))

	logger.Debug("creating newClient")
	client, _, err := scopesHttp.NewClient(ctx, opts...)
	if err != nil {
		logger.Error("error while creating new client for _initializeNetworkingService", err)
		return err
	}
	crmService, err := cloudresourcemanager.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		return fmt.Errorf("failed to create Cloud Resource Manager service: %v", err)
	}

	// Get the current IAM policy for the project
	policy, err := crmService.Projects.GetIamPolicy(projectID, &cloudresourcemanager.GetIamPolicyRequest{}).Do()
	if err != nil {
		return fmt.Errorf("failed to get IAM policy: %v", err)
	}

	// Define the role and member
	role := "roles/compute.networkUser"
	member := fmt.Sprintf("serviceAccount:%s", serviceAccountEmail)

	// Check if the binding already exists
	var bindingExists bool
	for _, binding := range policy.Bindings {
		if binding.Role == role {
			for _, m := range binding.Members {
				if m == member {
					bindingExists = true
					break
				}
			}
			if !bindingExists {
				binding.Members = append(binding.Members, member)
			}
			break
		}
	}

	// If the binding does not exist, add a new one
	if !bindingExists {
		policy.Bindings = append(policy.Bindings, &cloudresourcemanager.Binding{
			Role:    role,
			Members: []string{member},
		})
	}

	// Set the updated IAM policy
	_, err = crmService.Projects.SetIamPolicy(projectID, &cloudresourcemanager.SetIamPolicyRequest{
		Policy: policy,
	}).Do()
	if err != nil {
		return fmt.Errorf("failed to set IAM policy: %v", err)
	}

	slog.Infof("Granted %s to %s on project %s", role, serviceAccountEmail, projectID)
	return nil
}
