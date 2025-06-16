package vsa

import (
	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

func (rc *OntapRestProvider) CloudTargetCreate(name, containerName string) (*ontapRest.CloudTarget, error) {
	client := getOntapClientFunc(rc.ClientParams)
	_, job, err := client.Cloud().CloudTargetCreate(&ontapRest.CloudTargetCreateParams{
		Name:      &name,
		Container: &containerName,
	})
	if err != nil {
		return nil, err
	}
	err = waitForJobIfNeeded(rc, job)
	if err != nil {
		return nil, err
	}
	cloudTarget, err := client.Cloud().CloudTargetGet(&name)
	if err != nil {
		return nil, err
	}
	return cloudTarget, nil
}

func (rc *OntapRestProvider) CloudTargetGet(name *string) (*ontapRest.CloudTarget, error) {
	client := getOntapClientFunc(rc.ClientParams)
	cloudTarget, err := client.Cloud().CloudTargetGet(name)
	if err != nil {
		return nil, err
	}
	if cloudTarget == nil {
		return nil, errors.New("invalid CloudTarget response from API")
	}
	// Return the created SVM
	return cloudTarget, nil
}
