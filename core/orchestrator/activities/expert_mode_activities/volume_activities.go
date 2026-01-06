package expertmodeactivities

import (
	"context"
	"fmt"
	"strings"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	utilErrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/activity"
)

type ExpertModeVolumeActivity struct {
	SE database.Storage
}

// FetchOntapVolumeByName fetches a volume from ONTAP by name and updates the volume with ONTAP data.
func (a *ExpertModeVolumeActivity) FetchOntapVolumeByName(ctx context.Context, volume *datamodel.ExpertModeVolumes, node *models.Node) (*datamodel.ExpertModeVolumes, error) {
	logger := util.GetLogger(ctx)
	activity.RecordHeartbeat(ctx, fmt.Sprintf("Fetching volume %s from ONTAP", volume.Name))

	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		logger.Errorf("Failed to get ONTAP provider from node: %v", err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	var svmName string
	if volume.Svm != nil {
		svmName = volume.Svm.Name
	}

	getVolumeParams := vsa.GetVolumeParams{
		VolumeName: volume.Name,
		SvmName:    svmName,
		IsRestore:  false,
	}

	volumeResponse, err := provider.GetVolumeForExpertMode(getVolumeParams)
	if err != nil {
		if utilErrors.IsNotFoundErr(err) || strings.Contains(strings.ToLower(err.Error()), "not found") {
			logger.Infof("Volume %s not found in ONTAP, will retry", volume.Name)
			return nil, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrResourceNotFound, err))
		}
		logger.Errorf("Failed to get volume %s from ONTAP: %v", volume.Name, err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	logger.Infof("Volume %s found in ONTAP", volume.Name)

	volume.Name = volumeResponse.Name
	volume.SizeInBytes = volumeResponse.Size
	volume.Style = volumeResponse.Style
	volume.State = models.LifeCycleStateAvailable
	volume.ExternalUUID = volumeResponse.ExternalUUID

	return volume, nil
}

// CheckVolumeDeletedInOntap checks if a volume is deleted in ONTAP.
// If the volume is found, it returns an error to trigger activity retry.
// If the volume is not found, it returns nil (success) indicating deletion is complete.
func (a *ExpertModeVolumeActivity) CheckVolumeDeletedInOntap(ctx context.Context, volume *datamodel.ExpertModeVolumes, node *models.Node) error {
	logger := util.GetLogger(ctx)
	activity.RecordHeartbeat(ctx, fmt.Sprintf("Checking if volume %s is deleted in ONTAP", volume.Name))

	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		logger.Errorf("Failed to get ONTAP provider from node: %v", err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	var svmName string
	if volume.Svm != nil {
		svmName = volume.Svm.Name
	}

	getVolumeParams := vsa.GetVolumeParams{
		VolumeName: volume.Name,
		SvmName:    svmName,
		IsRestore:  false,
	}

	volumeResponse, err := provider.GetVolumeForExpertMode(getVolumeParams)
	if err != nil {
		if utilErrors.IsNotFoundErr(err) || strings.Contains(strings.ToLower(err.Error()), "not found") {
			// Volume is not found - deletion is complete, return success
			logger.Infof("Volume %s not found in ONTAP, deletion is complete", volume.Name)
			return nil
		}
		// Other errors (network, etc.) should be retried
		logger.Errorf("Failed to get volume %s from ONTAP: %v", volume.Name, err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Volume is still found - return error to trigger activity retry
	logger.Infof("Volume %s still exists in ONTAP (UUID: %s), deletion may be in progress. Will retry.", volume.Name, volumeResponse.ExternalUUID)
	return vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrResourceStateConflictError, fmt.Errorf("volume %s still exists in ONTAP, deletion may be in progress", volume.Name)))
}

// UpdateExpertModeVolumeInDB updates the expert mode volume in the database using UpdateExpertModeVolume.
func (a *ExpertModeVolumeActivity) UpdateExpertModeVolumeInDB(ctx context.Context, volume *datamodel.ExpertModeVolumes) error {
	logger := util.GetLogger(ctx)
	activity.RecordHeartbeat(ctx, fmt.Sprintf("Updating volume %s state in DB", volume.Name))

	updatedVolume, err := a.SE.UpdateExpertModeVolume(ctx, volume)
	if err != nil {
		logger.Errorf("Failed to update expert mode volume %s: %v", volume.Name, err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	logger.Infof("Successfully updated expert mode volume %s (UUID: %s) with state=%s, size=%d, name=%s, style=%s",
		updatedVolume.Name, updatedVolume.UUID, updatedVolume.State, updatedVolume.SizeInBytes, updatedVolume.Name, updatedVolume.Style)

	return nil
}
