package main

import (
	"context"
	"fmt"
	"os"

	deploymentmanager "google.golang.org/api/deploymentmanager/v2"
)

// DeploymentsInsert creates a new Deployment Manager deployment.
func DeploymentsInsert(name string) error {
	ctx := context.Background()
	deploymentmanagerService, err := deploymentmanager.NewService(ctx)
	if err != nil {
		return err
	}

	projectId := "478271815769" // Replace with your project ID
	content, err := os.ReadFile("vsa_config/sample_yaml.yaml")
	if err != nil {
		return err
	}
	configFile := deploymentmanager.ConfigFile{Content: string(content)}

	f1Content, err := os.ReadFile("vsa_config/netapp-cvo-deployment.py")
	if err != nil {
		return err
	}
	f2Content, err := os.ReadFile("vsa_config/netapp-cvo-deployment.py.schema")
	if err != nil {
		return err
	}

	file1 := deploymentmanager.ImportFile{Name: "netapp-cvo-deployment.py", Content: string(f1Content)}
	file2 := deploymentmanager.ImportFile{Name: "netapp-cvo-deployment.py.schema", Content: string(f2Content)}
	imports := []*deploymentmanager.ImportFile{&file1, &file2}

	target := deploymentmanager.TargetConfiguration{Config: &configFile, Imports: imports}
	deployment := deploymentmanager.Deployment{Name: name, Target: &target}

	res, err := deploymentmanagerService.Deployments.Insert(projectId, &deployment).Do()
	if err != nil {
		return err
	}

	fmt.Printf("Instance created: %v\n", res)
	return nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run main.go <deployment-name>")
		os.Exit(1)
	}

	name := os.Args[1]
	err := DeploymentsInsert(name)
	if err != nil {
		fmt.Printf("Error creating deployment: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Deployment created successfully.")
}
