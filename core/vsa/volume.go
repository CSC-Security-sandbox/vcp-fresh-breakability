package vsa

import (
	"strings"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

// CreateVolume creates a volume by calling the ONTAP REST Client
func (rc *OntapRestProvider) CreateVolume(params CreateVolumeParams) (*VolumeResponse, error) {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return nil, err
	}
	volumeCreateParams := &ontapRest.VolumeCreateParams{
		Name:                   params.VolumeName,
		Type:                   params.VolumeType,
		Size:                   params.Size,
		Svm:                    params.SvmName,
		Aggregates:             []string{params.AggregateName},
		SnapshotPolicy:         params.SnapshotPolicyName,
		SnapshotReservePercent: params.SnapReserve,
	}
	if params.RestoreFromSnapshot != nil && params.RestoreFromSnapshot.SnapshotUUID != "" {
		volumeCreateParams.RestoreFromSnapshot = &ontapRest.RestoreFromSnapshotParams{
			ParentVolumeExternalUUID: params.RestoreFromSnapshot.ParentVolumeExternalUUID,
			ParentVolumeName:         params.RestoreFromSnapshot.ParentVolumeName,
			SnapshotUUID:             params.RestoreFromSnapshot.SnapshotUUID,
			SnapshotName:             params.RestoreFromSnapshot.SnapshotName,
			ParentVolumeSvmName:      params.RestoreFromSnapshot.ParentVolumeSvmName,
		}
		volumeCreateParams.Size = 0
	}

	if params.TieringPolicy != nil {
		volumeCreateParams.TieringPolicy = &ontapRest.TieringPolicy{
			CoolAccessTieringPolicy: params.TieringPolicy.CoolAccessTieringPolicy,
			MinCoolingDays:          params.TieringPolicy.CoolnessPeriod,
			CloudRetrievalPolicy:    params.TieringPolicy.CoolAccessRetrievalPolicy,
		}
	}

	vol, job, err := client.Storage().VolumeCreate(volumeCreateParams)
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
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return err
	}
	err = client.Storage().VolumeDelete(&ontapRest.VolumeDeleteParams{
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
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return nil, err
	}
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

	res := &VolumeResponse{
		ProviderResponse: ProviderResponse{
			Name:         *vol.Name,
			ExternalUUID: *vol.UUID,
		},
		AvailableSpace: *vol.Space.Available,
		Size:           *vol.Space.Size,
		State:          *vol.State,
		// by default, volume will always have none as the snapshot policy
		SnapshotPolicyName: *vol.SnapshotPolicy.Name,
	}
	if vol.Space.SizeAvailableForSnapshots != nil {
		res.SnapReserve = *vol.Space.SizeAvailableForSnapshots
	}
	return res, nil
}

func (rc *OntapRestProvider) GetVolumes() ([]*Volume, error) {
	var resultVolumes []*Volume

	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return nil, err
	}
	ucbf := func(volumes []*ontapRest.Volume) error {
		for _, volume := range volumes {
			vol := &Volume{
				Volume: models.Volume{
					Name:      volume.Name,
					UUID:      volume.UUID,
					Svm:       volume.Svm,
					IsSvmRoot: volume.IsSvmRoot,
					Style:     volume.Style,
				},
				ExternalUUID: *volume.UUID,
			}
			resultVolumes = append(resultVolumes, vol)
		}
		return nil
	}

	err = client.Storage().VolumeCollectionGet(&ontapRest.VolumeCollectionGetParams{
		BaseParams: ontapRest.BaseParams{
			Fields: []string{"uuid", "name", "svm", "is_svm_root", "style"},
		},
	}, ucbf)

	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrOntapRestAPIError, err)
	}
	return resultVolumes, nil
}

func (rc *OntapRestProvider) UpdateVolume(params UpdateVolumeParams) error {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return err
	}
	volumeModifyParams := &ontapRest.VolumeModifyParams{
		UUID:        params.UUID,
		SnapReserve: params.SnapReserve,
	}

	if params.TieringPolicy != nil {
		volumeModifyParams.TieringPolicy = &ontapRest.TieringPolicy{
			CoolAccessTieringPolicy: params.TieringPolicy.CoolAccessTieringPolicy,
			MinCoolingDays:          params.TieringPolicy.CoolnessPeriod,
			CloudRetrievalPolicy:    params.TieringPolicy.CoolAccessRetrievalPolicy,
		}
	}

	if params.InitiateSplit {
		volumeModifyParams.SplitInitiated = &params.InitiateSplit
		volumeModifyParams.MatchParentStorageTier = false // TODO: add this `params.TieringPolicy == "auto"`, when autotier is supported
	} else {
		volumeModifyParams.Size = nillable.ToPointer(uint64(params.Size))
		volumeModifyParams.SnapshotPolicyName = nillable.GetStringPtr(params.SnapshotPolicyName)
	}
	success, job, err := client.Storage().VolumeModify(volumeModifyParams)
	if err != nil {
		return err
	}
	if success {
		return nil
	}
	return client.Poll(job.JobUUID)
}

func (rc *OntapRestProvider) UpdateVolumeEnableEncryption(params UpdateVolumeParams) error {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return err
	}
	success, job, err := client.Storage().VolumeModify(
		&ontapRest.VolumeModifyParams{
			UUID:             params.UUID,
			EncryptionEnable: &params.EncryptionEnable,
		})
	if err != nil {
		return err
	}
	if success {
		return nil
	}
	return client.Poll(job.JobUUID)
}

func (rc *OntapRestProvider) GetVolumeEncryptionStatus(params GetVolumeParams) (*VolumeResponse, error) {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return nil, err
	}
	volumeGetParams := &ontapRest.VolumeGetParams{
		BaseParams: ontapRest.BaseParams{Fields: []string{"encryption.*"}},
		UUID:       params.UUID,
		Name:       params.VolumeName,
		SvmName:    &params.SvmName,
	}
	vol, err := client.Storage().VolumeGet(volumeGetParams)
	if err != nil {
		return nil, err
	}
	if vol == nil || vol.Name == nil || vol.UUID == nil {
		return nil, errors.NewNotFoundErr("volume", nil)
	}
	if vol.Encryption == nil {
		return nil, errors.New("Encryption field is not populated in Get Volume from VSA")
	}

	return &VolumeResponse{
		ProviderResponse: ProviderResponse{
			Name:         *vol.Name,
			ExternalUUID: *vol.UUID,
		},
		Encryption: Encryption{
			Enabled: vol.Encryption.Enabled,
			State:   vol.Encryption.State,
			Type:    vol.Encryption.Type,
		},
	}, nil
}
