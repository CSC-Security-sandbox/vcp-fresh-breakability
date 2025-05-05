package vsa

import (
	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

// CreateVolume creates a volume by calling the ONTAP REST Client
func (rc *OntapRestProvider) CreateVolume(params CreateVolumeParams) (*ProviderResponse, error) {
	client := getOntapClientFunc(rc.ClientParams)
	vol, job, err := client.Storage().VolumeCreate(&ontapRest.VolumeCreateParams{
		Name:       params.VolumeName,
		Type:       params.VolumeType,
		Size:       params.Size,
		Svm:        params.SvmName,
		Aggregates: []string{params.AggregateName},
	})
	if err != nil {
		return nil, err
	}

	// Poll the job if it exists
	if job != nil {
		if err = client.Poll(job.JobUUID); err != nil {
			return nil, err
		}
	}

	// Validate the Volume response to avoid nil pointer dereferences
	if vol == nil || vol.Name == nil || vol.UUID == nil {
		return nil, errors.New("invalid Volume response from API")
	}

	// Return the created SVM
	return &ProviderResponse{
		Name:         *vol.Name,
		ExternalUUID: *vol.UUID,
	}, nil
}

// DeleteVolume creates a volume by calling the ONTAP REST Client
func (rc *OntapRestProvider) DeleteVolume(volumeUUID, volumeName string) error {
	client := getOntapClientFunc(rc.ClientParams)
	err := client.Storage().VolumeDelete(&ontapRest.VolumeDeleteParams{
		UUID: volumeUUID,
		Name: volumeName,
	})

	if err != nil {
		return err
	}

	return nil
}
