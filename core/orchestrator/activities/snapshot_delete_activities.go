package activities

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

type SnapshotDeleteActivity struct {
	SE database.Storage
}

func (a *SnapshotDeleteActivity) DeleteSnapshotInONTAP(ctx context.Context, snapshot *datamodel.Snapshot, node *models.Node) error {
	logger := util.GetLogger(ctx)
	provider := GetProviderByNode(node)
	err := provider.DeleteSnapshot(snapshot.SnapshotAttributes.ExternalUUID, snapshot.Volume.VolumeAttributes.ExternalUUID)
	if err != nil {
		return err
	}
	logger.Infof("Snapshot %s deleted successfully from the vsa cluster", snapshot.Name)
	return nil
}

func (a *SnapshotDeleteActivity) DeleteSnapshot(ctx context.Context, snapshot *datamodel.Snapshot) error {
	logger := util.GetLogger(ctx)
	se := a.SE

	_, err := se.DeleteSnapshot(ctx, snapshot.UUID)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			logger.Infof("Snapshot %s not found, assuming it is already deleted", snapshot.Name)
			return nil
		}
		return err
	}
	logger.Debugf("Snapshot:%s marked deleted successfully in the db", snapshot.Name)

	return nil
}

func (a *SnapshotDeleteActivity) UpdateDeleteSnapshotDetails(ctx context.Context, snapshot *datamodel.Snapshot) error {
	se := a.SE
	err := se.UpdateSnapshot(ctx, snapshot)
	if err != nil {
		return err
	}
	return nil
}
