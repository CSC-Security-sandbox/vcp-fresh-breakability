package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/goccy/go-yaml"
	"google.golang.org/api/compute/v1"
	deploymentmanager "google.golang.org/api/deploymentmanager/v2"
)

// DeploymentsInsert creates a new Deployment Manager deployment.
func DeploymentsInsert(name string) ([]map[string]string, error) {
	ctx := context.Background()
	deploymentmanagerService, err := deploymentmanager.NewService(ctx)
	if err != nil {
		return nil, err
	}

	projectId := "<replace project id>" // Replace with your project ID  // 1082706288987
	content, err := os.ReadFile("vsa_config/sample_yaml.yaml")
	if err != nil {
		return nil, err
	}
	configFile := deploymentmanager.ConfigFile{Content: string(content)}

	f1Content, err := os.ReadFile("vsa_config/netapp-cvo-deployment.py")
	if err != nil {
		return nil, err
	}
	f2Content, err := os.ReadFile("vsa_config/netapp-cvo-deployment.py.schema")
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
	interval := 10 * time.Second
	timeout := 5 * time.Minute
	startTime := time.Now()

	for time.Since(startTime) < timeout {
		operation, err := service.Operations.Get(projectId, operationName).Do()
		if err != nil {
			log.Printf("Error getting operation: %v\n", err)
			return nil, err
		}

		if operation.Status == "DONE" {
			if operation.Error != nil {
				log.Println("Deployment failed")
				for _, e := range operation.Error.Errors {
					log.Printf("Error Code: %s, Message: %s\n", e.Code, e.Message)
				}
				return nil, fmt.Errorf("%v", operation.Error)
			}
			log.Println("Deployment completed successfully!")

			resources, err := service.Resources.List(projectId, deploymentName).Do()
			if err != nil {
				log.Printf("Error listing resources: %v", err)
				return nil, err
			}
			return resources, nil
		}
		time.Sleep(interval)
	}

	return nil, fmt.Errorf("deployment creation timed out")
}

func getIPAddressDetails(projectId string, resources *deploymentmanager.ResourcesListResponse) ([]map[string]string, error) {
	// Filter resources to fetch only compute instances and their IPs
	var computeInstancesIPAddress []map[string]string
	for _, resource := range resources.Resources {
		if resource.Type == "compute.v1.instance" {
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

func main() {
	if len(os.Args) < 2 {
		log.Println("Usage: go run main.go <deployment-name>")
		os.Exit(1)
	}

	name := os.Args[1]
	res, err := DeploymentsInsert(name)
	if err != nil {
		log.Printf("Error creating deployment: %v\n", err)
		os.Exit(1)
	}

	log.Printf("Deployment created successfully: %v\n", res)
}
