package activities

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

type VolumeDeleteActivity struct {
	SE database.Storage
}

func (a *VolumeDeleteActivity) DeleteVolumeInONTAP(ctx context.Context, volume *datamodel.Volume, node *models.Node) error {
	logger := util.GetLogger(ctx)
	provider := GetProviderByNode(node)
	err := provider.DeleteVolume(volume.VolumeAttributes.ExternalUUID, volume.Name)
	if err != nil {
		return err
	}
	logger.Debug("Volume %s deleted successfully from the vsa cluster", volume.Name)

	return nil
}

func (a *VolumeDeleteActivity) DeleteVolume(ctx context.Context, volume *datamodel.Volume) error {
	logger := util.GetLogger(ctx)
	se := a.SE

	_, err := se.DeleteVolume(ctx, volume.UUID)
	if err != nil {
		return err
	}
	logger.Debug("Volume:%s marked deleted successfully in the db", volume.Name)

	return nil
}
