package activities

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

type SnapshotCreateActivity struct {
	SE database.Storage
}

func (a *SnapshotCreateActivity) CreateSnapshotInONTAP(ctx context.Context, snapshot *datamodel.Snapshot, node *models.Node) (*vsa.SnapshotProviderResponse, error) {
	logger := util.GetLogger(ctx)
	provider := GetProviderByNode(node)
	res, err := provider.CreateSnapshot(vsa.CreateSnapshotParams{
		VolumeUUID: snapshot.Volume.VolumeAttributes.ExternalUUID,
		Name:       snapshot.Name,
		Comment:    snapshot.Description,
	})
	if err != nil {
		return nil, err
	}
	logger.Debug("CreateSnapshotInONTAP: snapshot created successfully")

	return res, nil
}

func (a *SnapshotCreateActivity) UpdateSnapshotDetails(ctx context.Context, snapshot *datamodel.Snapshot) error {
	se := a.SE
	err := se.UpdateSnapshot(ctx, snapshot)
	if err != nil {
		return err
	}
	return nil
}
