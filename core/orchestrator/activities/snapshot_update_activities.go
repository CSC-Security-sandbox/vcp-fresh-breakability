package activities

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
)

type SnapshotUpdateActivity struct {
	SE database.Storage
}

func (a *SnapshotUpdateActivity) UpdateSnapshot(ctx context.Context, snapshot *datamodel.Snapshot) error {
	se := a.SE

	_, err := se.UpdateSnapshot(ctx, snapshot)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return nil
}
