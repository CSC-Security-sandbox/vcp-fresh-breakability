package common

import (
	"context"
	"fmt"
	"gopkg.in/yaml.v2"
	"log"
	"os"
	"strings"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"google.golang.org/api/compute/v1"
	"google.golang.org/api/deploymentmanager/v2"
)

var (
	vsaDeploymentTimeout      = time.Duration(env.GetInt("VSA_DEPLOYMENT_TIMEOUT", 5)) * time.Minute
	vsaDeploymentPollInterval = time.Duration(env.GetInt("VSA_DEPLOYMENT_POLL_INTERVAL", 10)) * time.Second
	vsaTenantProjectId        = env.GetString("VSA_TENANT_PROJECT_ID", "ua097ba39031f53a4-tp")
)

// DeploymentsInsert creates a new Deployment Manager deployment.
func DeploymentsInsert(name string) ([]map[string]string, error) {
	ctx := context.Background()
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

	res, err := deploymentmanagerService.Deployments.Insert(projectId, &deployment).Do()
	if err != nil {
		return nil, err
	}

	log.Printf("Instance created: %v\n", res)

	// Polling for deployment status
	resourcesList, err := pollDeploymentStatus(deploymentmanagerService, projectId, name, res.Name)

	if err != nil {
		log.Printf("Error creating deployment: %v", err)
		return nil, err
	}

	computeInstancesIPAddress, err := getIPAddressDetails(projectId, resourcesList)
	if err != nil {
		log.Printf("Error getting IP address details : %v", err)
		return nil, err
	}

	return computeInstancesIPAddress, nil
}

func pollDeploymentStatus(service *deploymentmanager.Service, projectId, deploymentName, operationName string) (*deploymentmanager.ResourcesListResponse, error) {
	startTime := time.Now()

	for time.Since(startTime) < vsaDeploymentTimeout {
		operation, err := service.Operations.Get(projectId, operationName).Do()
		if err != nil {
			log.Printf("Error getting operation: %v\n", err)
			return nil, err
		}

		if operation.Status == "DONE" {
			if operation.Error != nil {
				log.Print("Deployment failed")
				for _, e := range operation.Error.Errors {
					log.Printf("Error Code: %s, Message: %s\n", e.Code, e.Message)
				}
				return nil, fmt.Errorf("%v", operation.Error)
			}
			log.Print("Deployment completed successfully!")

			resources, err := service.Resources.List(projectId, deploymentName).Do()
			if err != nil {
				log.Printf("Error listing resources: %v", err)
				return nil, err
			}
			return resources, nil
		}
		time.Sleep(vsaDeploymentPollInterval)
	}

	return nil, fmt.Errorf("deployment creation timed out")
}

func getIPAddressDetails(projectId string, resources *deploymentmanager.ResourcesListResponse) ([]map[string]string, error) {
	// Filter resources to fetch only compute instances and their IPs
	var computeInstancesIPAddress []map[string]string
	for _, resource := range resources.Resources {
		if resource.Type == "compute.v1.instance" && !strings.Contains(resource.Name, "mediator") {
			// Parse resource.Properties YAML
			var propertiesMap map[string]interface{}
			if err := yaml.Unmarshal([]byte(resource.Properties), &propertiesMap); err != nil {
				log.Printf("Error parsing properties YAML: %v", err)
				return nil, err
			}

			zone, ok := propertiesMap["zone"].(string)
			if !ok {
				return nil, fmt.Errorf("zone property is not a string")
			}

			instanceDetails, err := getInstanceDetails(projectId, resource.Name, zone)
			if err != nil {
				log.Printf("Error getting instance details: %v", err)
				return nil, err
			}
			computeInstancesIPAddress = append(computeInstancesIPAddress, instanceDetails)
		}
	}

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
		"Name":       instance.Name,
		"InternalIP": instance.NetworkInterfaces[4].NetworkIP,
	}

	if len(instance.NetworkInterfaces) >= 1 && len(instance.NetworkInterfaces[0].AccessConfigs) >= 1 {
		instanceDetails["ExternalIP"] = instance.NetworkInterfaces[0].AccessConfigs[0].NatIP
	}

	log.Printf("Instance details: %v\n", instanceDetails)
	return instanceDetails, nil
}
