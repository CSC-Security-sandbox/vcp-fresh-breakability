package activities

import (
	"context"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/activity"
	"gorm.io/gorm"
)

const SnapshotTypeBackup = "backup"

type SnapshotCreateActivity struct {
	SE database.Storage
}

func (a *SnapshotCreateActivity) CreateSnapshotInONTAP(ctx context.Context, snapshot *datamodel.Snapshot, node *models.Node) (*vsa.SnapshotProviderResponse, error) {
	logger := util.GetLogger(ctx)
	activity.RecordHeartbeat(ctx, "Initializing snapshot creation")
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	activity.RecordHeartbeat(ctx, "Creating snapshot in ONTAP")
	res, err := provider.CreateSnapshot(vsa.CreateSnapshotParams{
		VolumeUUID: snapshot.Volume.VolumeAttributes.ExternalUUID,
		Name:       snapshot.Name,
		Comment:    snapshot.Description,
	})
	if err != nil {
		return nil, vsaerrors.WrapAsNonRetryableTemporalApplicationError(err)
	}
	activity.RecordHeartbeat(ctx, "Snapshot created successfully")
	logger.Debug("CreateSnapshotInONTAP: snapshot created successfully")

	return res, nil
}

func (a *SnapshotCreateActivity) UpdateSnapshotDetails(ctx context.Context, dbSnapshot *datamodel.Snapshot, snapshotCreateResponse *vsa.SnapshotProviderResponse) error {
	se := a.SE
	activity.RecordHeartbeat(ctx, "Updating snapshot details")
	if snapshotCreateResponse == nil {
		dbSnapshot.DeletedAt = &gorm.DeletedAt{Time: time.Now(), Valid: true}
		dbSnapshot.State = models.LifeCycleStateError
		dbSnapshot.StateDetails = models.LifeCycleStateCreationErrorDetails
	} else {
		dbSnapshot.State = models.LifeCycleStateREADY
		dbSnapshot.StateDetails = models.LifeCycleStateAvailableDetails
		dbSnapshot.SnapshotAttributes.SizeInBytes = snapshotCreateResponse.SizeInBytes
		dbSnapshot.SnapshotAttributes.ExternalUUID = snapshotCreateResponse.ExternalUUID
		dbSnapshot.SnapshotAttributes.LogicalSizeUsedInBytes = snapshotCreateResponse.LogicalSizeInBytes
	}
	activity.RecordHeartbeat(ctx, "Persisting snapshot state to database")
	_, err := se.UpdateSnapshot(ctx, dbSnapshot)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	activity.RecordHeartbeat(ctx, "Snapshot details updated successfully")
	return nil
}
