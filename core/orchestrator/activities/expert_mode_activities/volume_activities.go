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

var (
	fetchOntapVolumeByUUID = _fetchOntapVolumeByUUID
)

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

// UpdateExpertModeVolumeStateInDB updates the lifecycle state of an expert mode volume by UUID.
// Only state is updated; Style is not changed (Style is a volume type/display attribute, not state details).
func (a *ExpertModeVolumeActivity) UpdateExpertModeVolumeStateInDB(ctx context.Context, volumeUUID, state string) error {
	logger := util.GetLogger(ctx)

	volume, err := a.SE.GetExpertModeVolumeByUUID(ctx, volumeUUID)
	if err != nil {
		logger.Errorf("Failed to get expert mode volume by UUID %s: %v", volumeUUID, err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	volume.State = state

	_, err = a.SE.UpdateExpertModeVolume(ctx, volume)
	if err != nil {
		logger.Errorf("Failed to update expert mode volume state for UUID %s: %v", volumeUUID, err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	logger.Infof("Successfully updated expert mode volume state to %s for UUID %s", state, volumeUUID)
	return nil
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

	logger.Infof("Successfully updated expert mode volume %s (UUID: %s) with state=%s, size=%d, style=%s, state=%s",
		updatedVolume.Name, updatedVolume.UUID, updatedVolume.State, updatedVolume.SizeInBytes, updatedVolume.Style, updatedVolume.State)

	return nil
}

// DeleteExpertModeVolumeInDB soft deletes the expert mode volume in the database.
// It sets the DeletedAt timestamp and updates the state to deleted.
func (a *ExpertModeVolumeActivity) DeleteExpertModeVolumeInDB(ctx context.Context, volumeUUID string) error {
	logger := util.GetLogger(ctx)
	activity.RecordHeartbeat(ctx, fmt.Sprintf("Deleting volume %s in DB", volumeUUID))

	err := a.SE.DeleteExpertModeVolume(ctx, volumeUUID)
	if err != nil {
		logger.Errorf("Failed to delete expert mode volume %s: %v", volumeUUID, err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	logger.Infof("Successfully deleted expert mode volume (UUID: %s)", volumeUUID)

	return nil
}

// ValidateONTAPVolumeUpdate fetches a volume from ONTAP by UUID and checks if the update was successful
func (a *ExpertModeVolumeActivity) ValidateONTAPVolumeUpdate(ctx context.Context, volume *datamodel.ExpertModeVolumes, node *models.Node) (*datamodel.ExpertModeVolumes, error) {
	logger := util.GetLogger(ctx)
	activity.RecordHeartbeat(ctx, fmt.Sprintf("Validating ONTAP volume update for %s", volume.Name))

	ontapVolume, err := fetchOntapVolumeByUUID(ctx, volume, node)
	if err != nil {
		return nil, err
	}

	// validate state and name received from ontapVolume is same as volume.
	if ontapVolume.Size == volume.SizeInBytes && ontapVolume.Name == volume.Name {
		logger.Infof("ONTAP volume update validated successfully for volume %s (UUID: %s): SizeInBytes=%d, name=%s ontapVolume.State:%s",
			ontapVolume.Name, ontapVolume.ExternalUUID, ontapVolume.Size, ontapVolume.Name, ontapVolume.State)
		return volume, nil
	}

	logger.Infof("Volume %s still not updated in ONTAP (UUID: %s), update may be in progress. Will retry.", volume.Name, volume.ExternalUUID)
	return nil, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrResourceStateConflictError, fmt.Errorf("Volume %s still not updated in ONTAP (UUID: %s), update may be in progress. Will retry.", volume.Name, volume.ExternalUUID)))
}

// ValidateONTAPVolumeUpdate fetches a volume from ONTAP by UUID and checks if the update was successful
func (a *ExpertModeVolumeActivity) FetchOntapVolumeByUUID(ctx context.Context, volume *datamodel.ExpertModeVolumes, node *models.Node) (*datamodel.ExpertModeVolumes, error) {
	logger := util.GetLogger(ctx)
	activity.RecordHeartbeat(ctx, fmt.Sprintf("Fetching ONTAP volume by UUID for external UUID: %s and Name : %s", volume.ExternalUUID, volume.Name))

	logger.Debugf("Fetching volume from ONTAP Name: %s, external UUID: %s, UUID: %s", volume.Name, volume.ExternalUUID, volume.UUID)
	ontapVolume, err := fetchOntapVolumeByUUID(ctx, volume, node)
	if err != nil {
		logger.Errorf("Failed to fetch volume from ONTAP: %v", err)
		return nil, err
	}
	logger.Debugf("Successfully fetched volume from ONTAP Name: %s, external UUID: %s, size: %d", ontapVolume.Name, ontapVolume.ExternalUUID, ontapVolume.Size)
	return convertOntapToONTAPModeVol(ontapVolume, volume), nil
}

func convertOntapToONTAPModeVol(ontapVol *vsa.VolumeResponse, dbVolume *datamodel.ExpertModeVolumes) *datamodel.ExpertModeVolumes {
	var volume datamodel.ExpertModeVolumes

	volume.Name = ontapVol.Name
	volume.SizeInBytes = ontapVol.Size
	volume.ExternalUUID = dbVolume.ExternalUUID
	volume.UUID = dbVolume.UUID
	volume.Svm = dbVolume.Svm
	volume.Style = dbVolume.Style
	volume.State = models.LifeCycleStateAvailable
	volume.Description = dbVolume.Description
	volume.AccountID = dbVolume.AccountID
	volume.PoolID = dbVolume.PoolID
	volume.SvmID = dbVolume.SvmID
	volume.Account = dbVolume.Account
	volume.Pool = dbVolume.Pool
	volume.BackupConfig = dbVolume.BackupConfig
	volume.VolumeAttributes = dbVolume.VolumeAttributes

	return &volume
}

// UpdateExpertModeVolumeBackupConfigInDB persists the BackupConfig on an expert mode volume.
func (a *ExpertModeVolumeActivity) UpdateExpertModeVolumeBackupConfigInDB(ctx context.Context, volume *datamodel.ExpertModeVolumes) error {
	logger := util.GetLogger(ctx)
	if err := a.SE.UpdateExpertModeVolumeDataProtection(ctx, volume); err != nil {
		logger.Errorf("Failed to update backup config for expert mode volume %s: %v", volume.UUID, err)
		return err
	}
	logger.Infof("Successfully updated backup config for expert mode volume %s", volume.UUID)
	return nil
}

func _fetchOntapVolumeByUUID(ctx context.Context, volume *datamodel.ExpertModeVolumes, node *models.Node) (*vsa.VolumeResponse, error) {
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
		SvmName:   svmName,
		IsRestore: false,
		UUID:      volume.ExternalUUID,
	}

	volumeResponse, err := provider.GetVolumeForExpertMode(getVolumeParams)
	if err != nil {
		if utilErrors.IsNotFoundErr(err) || strings.Contains(strings.ToLower(err.Error()), "not found") {
			logger.Infof("Volume external UUID : %s not found in ONTAP, will retry", volume.UUID)
			return nil, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrResourceNotFound, err))
		}
		logger.Errorf("Failed to get volume external UUID : %s from ONTAP: %v", volume.UUID, err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	logger.Infof("Volume external UUID : %s found in ONTAP", volumeResponse.ExternalUUID)
	return volumeResponse, nil
}
