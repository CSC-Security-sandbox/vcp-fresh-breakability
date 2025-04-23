package executor

import (
	"context"
	"log"
	"os"

	"google.golang.org/api/deploymentmanager/v2"
)

type Jobs struct {
}

func (j *Jobs) CreateVsaCluster() error {
	ctx := context.Background()
	deploymentmanagerService, err := deploymentmanager.NewService(ctx)
	if err != nil {
		return err
	}

	projectId := "478271815769" // ua097ba39031f53a4-tp
	content, err := os.ReadFile("vsa_config/sample_yaml.yaml")
	if err != nil {
		log.Println(err)
		return err
	}
	configFile := deploymentmanager.ConfigFile{Content: string(content)}
	f1Content, err := os.ReadFile("vsa_config/netapp-cvo-deployment.py")
	if err != nil {
		log.Println(err)
		return err
	}
	f2Content, err := os.ReadFile("vsa_config/netapp-cvo-deployment.py.schema")
	if err != nil {
		log.Println(err)
		return err
	}
	file1 := deploymentmanager.ImportFile{Name: "netapp-cvo-deployment.py", Content: string(f1Content)}
	file2 := deploymentmanager.ImportFile{Name: "netapp-cvo-deployment.py.schema", Content: string(f2Content)}
	imports := []*deploymentmanager.ImportFile{&file1, &file2}
	target := deploymentmanager.TargetConfiguration{Config: &configFile, Imports: imports}
	deployment := deploymentmanager.Deployment{Name: "software-ontap", Target: &target}

	res, err := deploymentmanagerService.Deployments.Insert(projectId, &deployment).Do()
	if err != nil {
		return err
	}
	log.Println(res)
	return nil
}
