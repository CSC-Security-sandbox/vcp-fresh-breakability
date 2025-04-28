package common

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"google.golang.org/api/compute/v1"
	"google.golang.org/api/deploymentmanager/v2"
	"gopkg.in/yaml.v2"
)

var (
	vsaDeploymentTimeout      = time.Duration(env.GetInt("VSA_DEPLOYMENT_TIMEOUT", 5)) * time.Minute
	vsaDeploymentPollInterval = time.Duration(env.GetInt("VSA_DEPLOYMENT_POLL_INTERVAL", 10)) * time.Second
	vsaTenantProjectId        = env.GetString("VSA_TENANT_PROJECT_ID", "ua097ba39031f53a4-tp")
)

// DeploymentsInsert creates a new Deployment Manager deployment.
func DeploymentsInsert(ctx context.Context, name string) ([]map[string]string, error) {
	logger := ctx.Value(middleware.ContextSLoggerKey).(log.Logger)
	deploymentmanagerService, err := deploymentmanager.NewService(ctx)
	if err != nil {
		return nil, err
	}

	// TODO: will use getTenancy function once we have networking changes
	projectId := vsaTenantProjectId
	content, err := os.ReadFile("common/vsa_config/sample.yaml")
	if err != nil {
		return nil, err
	}
	configFile := deploymentmanager.ConfigFile{Content: string(content)}

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
				logger.Errorf("Error listing resources: %v", err)
				return nil, err
			}
		} else {
			return nil, err
		}
	} else {
		resourcesList, err = pollDeploymentStatus(ctx, deploymentmanagerService, projectId, name, res.Name)
		if err != nil {
			logger.Errorf("Error creating deployment: %v", err)
			return nil, err
		}
	}

	logger.Infof("Instance created: %v\n", res)

	computeInstancesIPAddress, err := getIPAddressDetails(ctx, projectId, resourcesList)
	if err != nil {
		logger.Errorf("Error getting IP address details : %v", err)
		return nil, err
	}

	return computeInstancesIPAddress, nil
}

func pollDeploymentStatus(ctx context.Context, service *deploymentmanager.Service, projectId, deploymentName, operationName string) (*deploymentmanager.ResourcesListResponse, error) {
	logger := ctx.Value(middleware.ContextSLoggerKey).(log.Logger)
	startTime := time.Now()

	for time.Since(startTime) < vsaDeploymentTimeout {
		operation, err := service.Operations.Get(projectId, operationName).Do()
		if err != nil {
			logger.Errorf("Error getting operation(operation name : %s): %v", operationName, err)
			return nil, err
		}

		if operation.Status == "DONE" {
			if operation.Error != nil {
				logger.Errorf("Deployment failed")
				for _, e := range operation.Error.Errors {
					logger.Errorf("Error Code: %s, Message: %s\n", e.Code, e.Message)
				}
				return nil, fmt.Errorf("%v", operation.Error)
			}
			logger.Infof("Deployment completed successfully!")

			resources, err := service.Resources.List(projectId, deploymentName).Do()
			if err != nil {
				logger.Errorf("Error listing resources(deployment name : %s): %v", deploymentName, err)
				return nil, err
			}
			return resources, nil
		}
		time.Sleep(vsaDeploymentPollInterval)
	}

	return nil, fmt.Errorf("deployment creation timed out")
}

func getIPAddressDetails(ctx context.Context, projectId string, resources *deploymentmanager.ResourcesListResponse) ([]map[string]string, error) {
	logger := ctx.Value(middleware.ContextSLoggerKey).(log.Logger)
	// Filter resources to fetch only compute instances and their IPs
	var computeInstancesIPAddress []map[string]string
	for _, resource := range resources.Resources {
		if resource.Type == "compute.v1.instance" && !strings.Contains(resource.Name, "mediator") {
			// Parse resource.Properties YAML
			var propertiesMap map[string]interface{}
			if err := yaml.Unmarshal([]byte(resource.Properties), &propertiesMap); err != nil {
				logger.Errorf("Error parsing properties YAML: %v", err)
				return nil, err
			}

			zone, ok := propertiesMap["zone"].(string)
			if !ok {
				return nil, fmt.Errorf("zone property is not a string")
			}

			instanceDetails, err := getInstanceDetails(projectId, resource.Name, zone)
			if err != nil {
				logger.Errorf("Error getting instance details(resource name : %s): %v", resource.Name, err)
				return nil, err
			}
			logger.Infof("Instance details: %v", instanceDetails)
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
