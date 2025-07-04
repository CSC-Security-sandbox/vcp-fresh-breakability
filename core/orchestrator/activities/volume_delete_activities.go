package activities

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

type VolumeDeleteActivity struct {
	SE database.Storage
}

func (va VolumeDeleteActivity) DeleteVolumeInONTAP(ctx context.Context, volumeExternalUUID, volumeName string, node *models.Node) error {
	logger := util.GetLogger(ctx)
	provider, err := GetProviderByNode(ctx, node)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	err = provider.DeleteVolume(volumeExternalUUID, volumeName)
	if err != nil {
		return err
	}
	logger.Debugf("Volume %s deleted successfully from the vsa cluster", volumeName)

	return nil
}

func (va VolumeDeleteActivity) DeleteVolume(ctx context.Context, volume *datamodel.Volume) error {
	logger := util.GetLogger(ctx)
	se := va.SE

	_, err := se.DeleteVolume(ctx, volume.UUID)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			return nil
		}
		return err
	}
	logger.Debugf("Volume:%s marked deleted successfully in the db", volume.Name)

	return nil
}

// DeleteSnapshotPolicyInONTAP deletes the snapshot policy associated with a volume in ONTAP.
func (va VolumeDeleteActivity) DeleteSnapshotPolicyInONTAP(ctx context.Context, SnapshotPolicyName string, node *models.Node) error {
	if node != nil && SnapshotPolicyName != "" {
		logger := util.GetLogger(ctx)
		provider, err := GetProviderByNode(ctx, node)
		if err != nil {
			return vsaerrors.WrapAsTemporalApplicationError(err)
		}
		op := func() error {
			return provider.DeleteSnapshotPolicy(SnapshotPolicyName)
		}
		err = vsa.RetryOnErrors(op, []string{"Policy is in use by at least one volume"})
		if err != nil {
			logger.Errorf("failed to delete snapshot policy: %v", err)
			return err
		}
	}
	return nil
}
