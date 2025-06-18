package vsa

import (
	"strings"

	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

// CreateVolume creates a volume by calling the ONTAP REST Client
func (rc *OntapRestProvider) CreateVolume(params CreateVolumeParams) (*VolumeResponse, error) {
	client := getOntapClientFunc(rc.ClientParams)
	vol, job, err := client.Storage().VolumeCreate(&ontapRest.VolumeCreateParams{
		Name:                   params.VolumeName,
		Type:                   params.VolumeType,
		Size:                   params.Size,
		Svm:                    params.SvmName,
		Aggregates:             []string{params.AggregateName},
		SnapshotPolicy:         params.SnapshotPolicyName,
		SnapshotReservePercent: 0, // Setting it to 0, yields more available space
	})
	if err != nil {
		if strings.Contains(err.Error(), "Duplicate volume name") {
			return nil, errors.NewConflictErr(params.VolumeName + " already exists")
		}
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
	return &VolumeResponse{
		ProviderResponse: ProviderResponse{
			Name:         *vol.Name,
			ExternalUUID: *vol.UUID,
		},
		AvailableSpace: *vol.Space.Available,
		State:          *vol.State,
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

// GetVolume returns a volume by calling the ONTAP REST Client
func (rc *OntapRestProvider) GetVolume(params GetVolumeParams) (*VolumeResponse, error) {
	client := getOntapClientFunc(rc.ClientParams)
	vol, err := client.Storage().VolumeGet(&ontapRest.VolumeGetParams{
		UUID:    params.UUID,
		Name:    params.VolumeName,
		SvmName: &params.SvmName,
	})
	if err != nil {
		return nil, err
	}
	if vol == nil || vol.Name == nil || vol.UUID == nil {
		return nil, errors.NewNotFoundErr("volume", nil)
	}
	return &VolumeResponse{
		ProviderResponse: ProviderResponse{
			Name:         *vol.Name,
			ExternalUUID: *vol.UUID,
		},
		AvailableSpace: *vol.Space.Available,
		Size:           *vol.Space.Size,
		State:          *vol.State,
	}, nil
}

func (rc *OntapRestProvider) UpdateVolume(params UpdateVolumeParams) error {
	client := getOntapClientFunc(rc.ClientParams)
	success, job, err := client.Storage().VolumeModify(&ontapRest.VolumeModifyParams{
		UUID: params.UUID,
		Size: nillable.ToPointer(uint64(params.Size)),
	})
	if err != nil {
		return err
	}
	if success {
		return nil
	}
	return client.Poll(job.JobUUID)
}
