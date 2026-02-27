package activities

import (
	"context"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// OntapModeRestoreActivity holds activities for ONTAP mode (expert mode) restore workflows.
type OntapModeRestoreActivity struct {
	SE database.Storage // injected by worker when registering
}

// FetchConstituentCountForLargeVolume fetches the volume from ONTAP and returns its constituent count.
// Reuses the same logic as GetVolumesAndConstituentCountActivity in backup_activities.
func (a OntapModeRestoreActivity) FetchConstituentCountForLargeVolume(ctx context.Context, volume *datamodel.Volume, node *models.Node) (int32, error) {
	logger := util.GetLogger(ctx)
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return 0, vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("failed to get provider: %w", err))
	}

	volumeResponse, err := provider.GetVolume(vsa.GetVolumeParams{
		UUID:    volume.VolumeAttributes.ExternalUUID,
		SvmName: volume.Svm.Name,
	})
	if err != nil {
		logger.Errorf("Failed to get volume from ONTAP: %v", err)
		return 0, vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("failed to get volume from ONTAP: %w", err))
	}

	if volumeResponse == nil {
		logger.Warnf("Volume not found in ONTAP, volumeUUID: %s", volume.VolumeAttributes.ExternalUUID)
		return 0, nil
	}

	if volumeResponse.ConstituentCount != nil {
		count := *volumeResponse.ConstituentCount
		logger.Infof("Found constituent count for volume %s: %d", volume.Name, count)
		return count, nil
	}
	logger.Debugf("No constituent count found for volume %s (may not be a flexgroup volume)", volume.Name)
	return 0, nil
}

// VerifyCVCountForLargeVolume verifies that the restore target's constituent count matches the backup's
// ConstituentCountOfBackup when the backup is a flexgroup (large volume) backup.
func (a OntapModeRestoreActivity) VerifyCVCountForLargeVolume(ctx context.Context, backup *datamodel.Backup, restoreTargetConstituentCount int32) error {
	logger := util.GetLogger(ctx)
	backupConstituentCount := backup.Attributes.ConstituentCountOfBackup
	if restoreTargetConstituentCount != backupConstituentCount {
		return vsaerrors.WrapAsTemporalApplicationError(
			customerrors.NewUserInputValidationErr(
				fmt.Sprintf("restore target volume constituent count (%d) does not match backup constituent count (%d)",
					restoreTargetConstituentCount, backupConstituentCount)))
	}
	logger.Infof("Constituent count verified: restore target %d matches backup %d", restoreTargetConstituentCount, backupConstituentCount)
	return nil
}
