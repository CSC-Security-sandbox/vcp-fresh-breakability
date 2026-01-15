package vsa

import (
	"fmt"
	"strings"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

// Error message constants
const (
	// ErrMsgVolumeMaxSizeExceeded is the error message pattern returned by ONTAP when volume size exceeds maximum
	ErrMsgVolumeMaxSizeExceeded = "failed because the resulting volume size is greater than the maximum size"
)

var (
	enableCloneInfoRefresh = env.GetBool("ENABLE_CLONE_INFO_REFRESH", false)
)

// CreateVolume creates a volume by calling the ONTAP REST Client
func (rc *OntapRestProvider) CreateVolume(params CreateVolumeParams) (*VolumeResponse, error) {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return nil, err
	}
	volumeCreateParams := &ontapRest.VolumeCreateParams{
		Name:                           params.VolumeName,
		Type:                           params.VolumeType,
		Size:                           params.Size,
		Svm:                            params.SvmName,
		Aggregates:                     params.Aggregates,
		ConstituentsPerAggregate:       params.ConstituentsPerAggregate,
		Style:                          params.Style,
		SnapshotPolicy:                 params.SnapshotPolicyName,
		SnapshotReservePercent:         params.SnapReserve,
		SnapshotDirectoryAccessEnabled: params.SnapshotDirectory,
		ExportPolicy:                   params.ExportPolicy,
		JunctionPath:                   params.JunctionPath,
		TieringSupported:               params.TieringSupported,
	}
	if params.SecurityStyle != nil && *params.SecurityStyle != "" {
		volumeCreateParams.SecurityStyle = *params.SecurityStyle
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
			CloudWriteModeEnabled:   params.TieringPolicy.CloudWriteModeEnabled,
		}
	}

	vol, job, err := client.Storage().VolumeCreate(volumeCreateParams)
	if err != nil {
		if strings.Contains(err.Error(), "Duplicate volume name") {
			return nil, errors.NewConflictErr(params.VolumeName + " already exists")
		}
		if strings.Contains(err.Error(), "Maximum clone hierarchy") {
			return nil, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrNestedCloneLimitExceeded, err))
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

	// Return the created volume
	volRes := &VolumeResponse{
		ProviderResponse: ProviderResponse{
			Name:         *vol.Name,
			ExternalUUID: *vol.UUID,
		},
		State: *vol.State,
	}

	// adding nil pointer checks as in some cases it may not be populated like FlexGroup volumes with large number of constituents
	if vol.Space != nil {
		if vol.Space.Available != nil {
			volRes.AvailableSpace = *vol.Space.Available
		}
		if vol.Space.SizeAvailableForSnapshots != nil {
			volRes.SnapReserve = *vol.Space.SizeAvailableForSnapshots
		}
		if vol.Space.AfsTotal != nil {
			volRes.AFSSize = *vol.Space.AfsTotal
		}
		if vol.Space.Metadata != nil {
			volRes.MetadataSize = *vol.Space.Metadata
		}
	}
	if vol.Size != nil {
		volRes.Size = *vol.Size
	}

	// Extract constituent count from the created volume
	if vol.ConstituentCount != nil {
		count := int32(*vol.ConstituentCount)
		volRes.ConstituentCount = &count
	} else if vol.VolumeInlineConstituents != nil {
		// If constituent_count is not available but constituents array is available (even if empty), use the length
		count := int32(len(vol.VolumeInlineConstituents))
		volRes.ConstituentCount = &count
	}

	return volRes, nil
}

// DeleteVolume deletes a volume by calling the ONTAP REST Client
// It polls the job internally if the deletion is async, similar to CreateVolume
func (rc *OntapRestProvider) DeleteVolume(volumeUUID, volumeName string) error {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return err
	}

	// First, get the volume details to check if it's in use for splitting or has clones
	vol, err := client.Storage().VolumeGet(&ontapRest.VolumeGetParams{
		UUID: volumeUUID,
		BaseParams: ontapRest.BaseParams{
			Fields: []string{"uuid", "name", "clone.*", "snapmirror.*"},
		},
	})
	if err != nil {
		if strings.Contains(err.Error(), "entry doesn't exist") || strings.Contains(err.Error(), "entry not found") || strings.Contains(err.Error(), "UUID and Name parameters cannot be empty when querying for a volume") {
			return nil
		}
		return err
	}
	// Check if the volume is in use for splitting or has clones
	if vol != nil && vol.Clone != nil {
		// Check if volume has FlexClone volumes or a split is initiated
		if (vol.Clone.HasFlexclone != nil && *vol.Clone.HasFlexclone) ||
			(vol.Clone.SplitInitiated != nil && *vol.Clone.SplitInitiated) {
			return vsaerrors.NewVCPError(vsaerrors.ErrDeleteVolumeWhenInSplitState, errors.New("Cannot delete a volume that is being actively used in a Volume Replication relationship or a file clone split triggered by Snapshot RestoreFiles operation or used as a reference snapshot for a backup"))
		}
	}

	// If all checks pass, proceed with volume deletion
	job, err := client.Storage().VolumeDelete(&ontapRest.VolumeDeleteParams{
		UUID: volumeUUID,
		Name: volumeName,
	})

	if err != nil {
		return err
	}

	// Poll the job if it exists
	if job != nil {
		if err = client.Poll(job.JobUUID); err != nil {
			return err
		}
	}

	return nil
}

func (rc *OntapRestProvider) UnmountVolume(volumeUUID string) (*OntapAsyncResponse, error) {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return nil, err
	}

	jobAccepted, err := client.Storage().VolumeUnmount(&ontapRest.VolumeUnmountParams{
		UUID: volumeUUID,
	})
	if err != nil {
		return nil, err
	}

	if jobAccepted != nil {
		return &OntapAsyncResponse{JobUUID: jobAccepted.JobUUID}, nil
	}

	return nil, nil
}

func (rc *OntapRestProvider) MountVolume(params MountVolumeParams) (*OntapAsyncResponse, error) {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return nil, err
	}

	jobAccepted, err := client.Storage().VolumeMount(&ontapRest.VolumeMountParams{
		UUID:         params.UUID,
		JunctionPath: params.JunctionPath,
	})
	if err != nil {
		return nil, err
	}

	// Poll the job if it exists
	if jobAccepted != nil {
		if err = client.Poll(jobAccepted.JobUUID); err != nil {
			return nil, err
		}
	}

	return &OntapAsyncResponse{JobUUID: jobAccepted.JobUUID}, nil
}

// GetVolume returns a volume by calling the ONTAP REST Client
func (rc *OntapRestProvider) GetVolume(params GetVolumeParams) (*VolumeResponse, error) {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return nil, err
	}
	vol, err := client.Storage().VolumeGet(&ontapRest.VolumeGetParams{
		UUID:                           params.UUID,
		Name:                           params.VolumeName,
		SvmName:                        &params.SvmName,
		SnapshotDirectoryAccessEnabled: &params.SnapshotDirectory,
		BaseParams: ontapRest.BaseParams{
			Fields: []string{"uuid", "name", "space.*", "state", "snapshot_policy.name", "type", "snapshot_directory_access_enabled", "constituents"},
		},
	})
	if err != nil {
		return nil, err
	}
	if vol == nil || vol.Name == nil || vol.UUID == nil || vol.Space == nil {
		return nil, errors.NewNotFoundErr("volume", nil)
	}
	if !params.IsRestore && vol.Space.LogicalSpace == nil {
		return nil, errors.NewNotFoundErr("volume", nil)
	}
	usedBytes := int64(0)
	if vol.Space.LogicalSpace != nil && vol.Space.LogicalSpace.Used != nil {
		usedBytes = nillable.FromPointer(vol.Space.LogicalSpace.Used)
	}
	volType := ""
	if vol.Type != nil {
		volType = *vol.Type
	}
	res := &VolumeResponse{
		ProviderResponse: ProviderResponse{
			Name:         *vol.Name,
			ExternalUUID: *vol.UUID,
		},
		AvailableSpace: *vol.Space.Available,
		Size:           *vol.Space.Size,
		State:          *vol.State,
		UsedBytes:      usedBytes,
		// by default, volume will always have none as the snapshot policy
		SnapshotPolicyName:             *vol.SnapshotPolicy.Name,
		Type:                           volType,
		SnapshotDirectoryAccessEnabled: *vol.SnapshotDirectoryAccessEnabled,
	}
	if vol.Space.SizeAvailableForSnapshots != nil {
		res.SnapReserve = *vol.Space.SizeAvailableForSnapshots
	}
	if vol.Space.Metadata != nil {
		res.MetadataSize = *vol.Space.Metadata
	}
	if vol.Space.AfsTotal != nil {
		res.AFSSize = *vol.Space.AfsTotal
	}
	if vol.VolumeInlineConstituents != nil {
		if len(vol.VolumeInlineConstituents) > 0 {
			count := int32(len(vol.VolumeInlineConstituents))
			res.ConstituentCount = &count
		} else {
			zero := int32(0)
			res.ConstituentCount = &zero
		}
	}
	return res, nil
}

func (rc *OntapRestProvider) GetVolumeForExpertMode(params GetVolumeParams) (*VolumeResponse, error) {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return nil, err
	}
	vol, err := client.Storage().VolumeGet(&ontapRest.VolumeGetParams{
		UUID:    params.UUID,
		Name:    params.VolumeName,
		SvmName: &params.SvmName,
		BaseParams: ontapRest.BaseParams{
			Fields: []string{"uuid", "name", "state", "type"},
		},
	})
	if err != nil {
		return nil, err
	}
	volStyle := ""
	if vol.Style != nil {
		volStyle = *vol.Style
	}
	res := &VolumeResponse{
		ProviderResponse: ProviderResponse{
			Name:         *vol.Name,
			ExternalUUID: *vol.UUID,
		},
		Size:  *vol.Size,
		State: *vol.State,
		Style: volStyle,
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
					Space:     volume.Space,
					Type:      volume.Type,
					Clone:     volume.Clone,
				},
				ExternalUUID: *volume.UUID,
			}
			resultVolumes = append(resultVolumes, vol)
		}
		return nil
	}

	// Build base fields list
	fields := []string{"uuid", "name", "space.*", "svm", "is_svm_root", "style", "type"}

	// Conditionally add clone fields if feature flag is enabled
	if enableCloneInfoRefresh {
		fields = append(fields, "clone.parent_snapshot.name", "clone.parent_volume.name")
	}

	err = client.Storage().VolumeCollectionGet(&ontapRest.VolumeCollectionGetParams{
		BaseParams: ontapRest.BaseParams{
			Fields: fields,
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
			CloudWriteModeEnabled:   params.TieringPolicy.CloudWriteModeEnabled,
		}
	}

	if params.InitiateSplit {
		volumeModifyParams.SplitInitiated = &params.InitiateSplit
		volumeModifyParams.MatchParentStorageTier = false // TODO: add this `params.TieringPolicy == "auto"`, when autotier is supported
	} else {
		if params.Size != 0 {
			volumeModifyParams.Size = nillable.ToPointer(uint64(params.Size))
		}
		volumeModifyParams.SnapshotPolicyName = nillable.GetStringPtr(params.SnapshotPolicyName)
	}

	if params.ExportPolicy != nil {
		volumeModifyParams.ExportPolicy = params.ExportPolicy
	}
	if params.JunctionPath != nil {
		volumeModifyParams.Path = params.JunctionPath
	}
	if params.SnapshotDirectoryAccess != nil {
		volumeModifyParams.SnapshotDirectoryAccessEnabled = params.SnapshotDirectoryAccess
	}

	// Handle QoS policy assignment/unassignment
	// If QosPolicyName is provided (not nil), set it:
	// - Use "none" to unassign (no policy) per ONTAP API specification
	// - Use policy name to assign that policy
	// - Empty string will be rejected by ONTAP with an appropriate error
	// If QosPolicyName is nil, don't change the policy
	if params.QosPolicyName != nil {
		volumeModifyParams.QosPolicy = params.QosPolicyName
	}

	err = handleVolumeCloudWriteModeDisableIfProvided(client, volumeModifyParams)
	if err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrOntapRestAPIError, err)
	}

	success, job, err := client.Storage().VolumeModify(volumeModifyParams)
	if err != nil {
		// Check for maximum volume size error
		if strings.Contains(err.Error(), ErrMsgVolumeMaxSizeExceeded) {
			// Extract the maximum size from the error message
			maxSizeStart := strings.Index(err.Error(), "The maximum possible size is ")
			if maxSizeStart > 0 {
				maxSizeStr := err.Error()[maxSizeStart:]
				// Create a sanitized error message without exposing SVM name
				sanitizedError := fmt.Sprintf("Volume size exceeds the maximum allowed size. %s", maxSizeStr)
				return vsaerrors.NewVCPError(vsaerrors.ErrVolumeExceedsMaximumSize,
					errors.NewUserInputValidationErr(sanitizedError))
			}
		}
		return vsaerrors.NewVCPError(vsaerrors.ErrOntapRestAPIError, err)
	}
	if success || params.InitiateSplit { // Split operation can run in background without polling
		return nil
	}
	return client.Poll(job.JobUUID)
}

// UnassignQoSPolicyFromVolume unassigns the QoS policy from a volume by setting it to "none".
// This is a convenience function that wraps UpdateVolume with the correct "none" value
// as required by the ONTAP API specification.
func (rc *OntapRestProvider) UnassignQoSPolicyFromVolume(volumeUUID string) error {
	none := "none"
	params := UpdateVolumeParams{
		UUID:          volumeUUID,
		QosPolicyName: &none,
	}
	return rc.UpdateVolume(params)
}

func handleVolumeCloudWriteModeDisableIfProvided(client ontapRest.RESTClient, params *ontapRest.VolumeModifyParams) error {
	// For files volume, when changing ontap auto-tiering policy from all to auto/none/snapshot_only
	// policy along with disabling cloud write mode, ontap throws error. It is required to
	// disable the cloud write mode first and then change the policy.
	if params.TieringPolicy != nil &&
		params.TieringPolicy.CloudWriteModeEnabled != nil &&
		!*params.TieringPolicy.CloudWriteModeEnabled &&
		params.TieringPolicy.CoolAccessTieringPolicy != models.VolumeInlineTieringPolicyAll {
		success, job, err := client.Storage().VolumeModifyCloudWriteMode(params)
		if err != nil {
			return vsaerrors.NewVCPError(vsaerrors.ErrOntapRestAPIError, err)
		}
		if success {
			return nil
		}
		return client.Poll(job.JobUUID)
	}

	return nil
}

func (rc *OntapRestProvider) RevertVolume(params RevertVolumeParams) error {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrOntapRestAPIError, err)
	}

	revertParams := &ontapRest.VolumeModifyParams{
		UUID:                  params.VolumeID,
		RestoreToSnapshotUUID: nillable.GetStringPtr(params.SnapshotID),
	}

	done, jj, err := client.Storage().VolumeModify(revertParams)
	if err != nil {
		if strings.Contains(err.Error(), "Failed to restore snapshot") || strings.Contains(err.Error(), "Volume snap restore error") {
			return vsaerrors.NewVCPError(vsaerrors.ErrRevertVolumeWhenSnapshotInUse, errors.NewUserInputValidationErr("Cannot revert a Volume when snapshot is in use by the cloned volume"))
		}
		if errors.IsNotFoundErr(err) {
			return vsaerrors.NewVCPError(vsaerrors.ErrResourceNotFound, errors.NewNotFoundErr("Volume", nil))
		}
		if strings.Contains(err.Error(), "Only a Snapshot copy of a read/write volume can be promoted") {
			return vsaerrors.NewVCPError(vsaerrors.ErrRevertReplicationDestinationVolume, errors.NewUserInputValidationErr("Cannot revert a Volume Replication Destination Volume"))
		}
		if strings.Contains(err.Error(), "entry doesn't exist") || strings.Contains(err.Error(), "entry not found") {
			return vsaerrors.NewVCPError(vsaerrors.ErrResourceNotFound, errors.NewNotFoundErr("Snapshot", nil))
		}
		return vsaerrors.NewVCPError(vsaerrors.ErrOntapRestAPIError, err)
	}
	if !done {
		if err = client.Poll(jj.JobUUID); err != nil {
			return vsaerrors.NewVCPError(vsaerrors.ErrOntapRestAPIError, err)
		}
	}
	return nil
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
