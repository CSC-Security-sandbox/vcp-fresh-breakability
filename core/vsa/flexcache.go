package vsa

import (
	"fmt"

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

	// Update the volume's export policy if provided
	if params.ExportPolicy != nil {
		updateParams := UpdateVolumeParams{
			UUID:         *vol.UUID,
			ExportPolicy: params.ExportPolicy,
		}
		if err = rc.UpdateVolume(updateParams); err != nil {
			return nil, err
		}
	}

	// Return the created FlexCache volume
	return &VolumeResponse{
		ProviderResponse: ProviderResponse{
			Name:         *vol.Name,
			ExternalUUID: *vol.UUID,
		},
	}, nil
}

// DeleteFlexCacheVolume delete a FlexCache volume by calling the ONTAP REST Client
func (rc *OntapRestProvider) DeleteFlexCacheVolume(volumeUUID, name string) (*OntapAsyncResponse, error) {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return nil, err
	}

	jobAccepted, err := client.Storage().FlexCacheVolumeDelete(&ontapRest.FlexCacheVolumeDeleteParams{
		UUID: volumeUUID,
		Name: name,
	})
	if err != nil {
		return nil, err
	}

	if jobAccepted != nil {
		return &OntapAsyncResponse{JobUUID: jobAccepted.JobUUID}, nil
	}

	return nil, nil
}

// UpdateFlexCacheVolume updates a FlexCache volume configuration
// Returns OntapAsyncResponse with job UUID if async, nil if completed synchronously
func (rc *OntapRestProvider) UpdateFlexCacheVolume(params UpdateFlexCacheVolumeParams) (*OntapAsyncResponse, error) {
	success, jobUUID, err := rc.updateFlexCacheVolume(params)
	if err != nil {
		return nil, err
	}

	// Return the job UUID if one was created (async operation)
	if jobUUID != nil {
		return &OntapAsyncResponse{JobUUID: *jobUUID}, nil
	}

	// Completed synchronously
	if !success {
		return nil, fmt.Errorf("FlexCache volume update failed")
	}

	return nil, nil
}

// updateFlexCacheVolume is the internal implementation
// Returns: (success, jobUUID, error)
func (rc *OntapRestProvider) updateFlexCacheVolume(params UpdateFlexCacheVolumeParams) (bool, *string, error) {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return false, nil, err
	}

	flexCacheVolumeUpdateParams := &ontapRest.FlexcacheModifyParams{
		BaseParams:                 ontapRest.BaseParams{},
		UUID:                       params.UUID,
		PrepopulateExcludeDirPaths: params.PrepopulateExcludeDirPaths,
		PrepopulateDirPaths:        params.PrepopulateDirPaths,
		PrepopulateRecurse:         params.IsRecursionEnabled,
		WritebackEnabled:           params.WritebackEnabled,
		RelativeSizeEnabled:        params.RelativeSizeEnabled,
		RelativeSizePercentage:     params.RelativeSizePercentage,
		AtimeScrubEnabled:          params.AtimeScrubEnabled,
		AtimeScrubPeriod:           params.AtimeScrubPeriod,
		CifsChangeNotifyEnabled:    params.CifsChangeNotifyEnabled,
	}

	success, job, err := client.Storage().FlexCacheVolumeModify(flexCacheVolumeUpdateParams)
	if err != nil {
		return false, nil, err
	}

	// Return job UUID if ONTAP created a background job
	if job != nil && job.JobUUID != "" {
		return success, &job.JobUUID, nil
	}

	return success, nil, nil
}
