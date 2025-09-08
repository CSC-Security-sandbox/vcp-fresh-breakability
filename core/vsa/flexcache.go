package vsa

import (
	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

// CreateFlexCacheVolume creates a FlexCache volume by calling the ONTAP REST Client
func (rc *OntapRestProvider) CreateFlexCacheVolume(params CreateFlexCacheVolumeParams) (*VolumeResponse, error) {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return nil, err
	}

	flexCacheVolumeCreateParams := &ontapRest.FlexCacheVolumeCreateParams{
		Name:             params.Name,
		OriginSvmName:    params.OriginSVMName,
		OriginVolumeName: params.OriginVolumeName,
		Aggregates:       []string{params.AggregateName},
		SvmName:          params.SvmName,
		Path:             params.JunctionPath,
	}

	vol, job, err := client.Storage().FlexCacheVolumeCreate(flexCacheVolumeCreateParams)
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

	// Return the created FlexCache volume
	return &VolumeResponse{
		ProviderResponse: ProviderResponse{
			Name:         *vol.Name,
			ExternalUUID: *vol.UUID,
		},
	}, nil
}
