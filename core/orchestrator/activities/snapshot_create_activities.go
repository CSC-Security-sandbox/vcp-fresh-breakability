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

type SnapshotCreateActivity struct {
	SE database.Storage
}

func (a *SnapshotCreateActivity) CreateSnapshotInONTAP(ctx context.Context, snapshot *datamodel.Snapshot, node *models.Node) (*vsa.SnapshotProviderResponse, error) {
	logger := util.GetLogger(ctx)
	provider, err := GetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	res, err := provider.CreateSnapshot(vsa.CreateSnapshotParams{
		VolumeUUID: snapshot.Volume.VolumeAttributes.ExternalUUID,
		Name:       snapshot.Name,
		Comment:    snapshot.Description,
	})
	if err != nil && !errors.IsConflictErr(err) {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	logger.Debug("CreateSnapshotInONTAP: snapshot created successfully")

	return res, nil
}

func (a *SnapshotCreateActivity) UpdateSnapshotDetails(ctx context.Context, dbSnapshot *datamodel.Snapshot, snapshotCreateResponse *vsa.SnapshotProviderResponse) error {
	se := a.SE
	if snapshotCreateResponse == nil {
		dbSnapshot.State = models.LifeCycleStateError
		dbSnapshot.StateDetails = models.LifeCycleStateCreationErrorDetails
	} else {
		dbSnapshot.State = models.LifeCycleStateREADY
		dbSnapshot.StateDetails = models.LifeCycleStateAvailableDetails
		dbSnapshot.SnapshotAttributes.SizeInBytes = snapshotCreateResponse.SizeInBytes
		dbSnapshot.SnapshotAttributes.ExternalUUID = snapshotCreateResponse.ExternalUUID
		dbSnapshot.SnapshotAttributes.LogicalSizeUsedInBytes = snapshotCreateResponse.LogicalSizeInBytes
	}
	_, err := se.UpdateSnapshot(ctx, dbSnapshot)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return nil
}
