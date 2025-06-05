package vsa

import (
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

// CreateSnapshot creates a snapshot by calling the ONTAP REST Client
func (rc *OntapRestProvider) CreateSnapshot(params CreateSnapshotParams) (*SnapshotProviderResponse, error) {
	client := getOntapClientFunc(rc.ClientParams)
	snapshot, job, err := client.Storage().SnapshotCreate(&ontapRest.SnapshotCreateParams{
		VolumeUUID: params.VolumeUUID,
		Name:       params.Name,
		Comment:    nillable.ToPointer(params.Comment),
	})
	if err != nil {
		if !errors.IsNotFoundErr(err) {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrOntapRestAPIError, err)
		}
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrResourceNotFound, errors.NewNotFoundErr("Volume", nil))
	}

	var uuid string
	if snapshot != nil && snapshot.UUID != nil {
		uuid = *snapshot.UUID
	} else {
		// Poll the job if it exists
		if job != nil {
			if err = client.Poll(job.JobUUID); err != nil {
				return nil, vsaerrors.NewVCPError(vsaerrors.ErrOntapRestAPIError, err)
			}
			uuid = job.ResourceUUID
		}
	}

	snapshot, err = client.Storage().SnapshotGet(&ontapRest.SnapshotGetParams{
		BaseParams: ontapRest.BaseParams{Fields: []string{
			"reclaimable_space",
			"size",
			"logical_size",
		}},
		UUID:       uuid,
		VolumeUUID: params.VolumeUUID,
	})
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrOntapRestAPIError, err)
	}
	// Validate the Snapshot response to avoid nil pointer dereferences
	if snapshot == nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrOntapInconsistentResourceError, errors.NewBadRequestErr("invalid Snapshot create response from API: snapshot is nil"))
	}
	if snapshot.Name == nil || snapshot.UUID == nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrOntapInconsistentResourceError, errors.NewBadRequestErr("invalid Snapshot create response from API: missing required fields"))
	}

	// Return the created SVM
	return &SnapshotProviderResponse{
		ProviderResponse: ProviderResponse{
			Name:         *snapshot.Name,
			ExternalUUID: *snapshot.UUID,
		},
		SizeInBytes:        *snapshot.Size,
		LogicalSizeInBytes: *snapshot.LogicalSize,
	}, nil
}

// DeleteSnapshot deletes a snapshot by calling the ONTAP REST Client
func (rc *OntapRestProvider) DeleteSnapshot(snapshotUUID string, volumeUUID string) error {
	client := getOntapClientFunc(rc.ClientParams)
	err := client.Storage().SnapshotDelete(&ontapRest.SnapshotDeleteParams{
		UUID:       snapshotUUID,
		VolumeUUID: volumeUUID,
	})

	if err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrOntapRestAPIError, err)
	}

	return nil
}
