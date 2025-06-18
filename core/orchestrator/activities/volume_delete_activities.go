package activities

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

type VolumeDeleteActivity struct {
	SE database.Storage
}

func (va VolumeDeleteActivity) DeleteVolumeInONTAP(ctx context.Context, volumeExternalUUID, volumeName string, node *models.Node) error {
	logger := util.GetLogger(ctx)
	provider := GetProviderByNode(ctx,node)
	err := provider.DeleteVolume(volumeExternalUUID, volumeName)
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
