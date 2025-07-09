package replicationActivities

import (
	"context"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/hydrationActivities"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	hydrateBatchSnapshotsToCCFE = hydrationActivities.HydrateBatchSnapshotstoCCFE
)

type InternalSnapshotsDeleteActivity struct {
	SE database.Storage
}

func (r *InternalSnapshotsDeleteActivity) GetNodeFromDB(ctx context.Context, params *commonparams.SnapshotsInternalDeleteParams) (*commonparams.SnapshotsInternalDeleteParams, error) {
	se := r.SE
	nodes, err := se.GetNodesByPoolID(ctx, params.Volume.PoolID)
	if err != nil {
		return nil, err
	}
	if len(nodes) == 0 {
		return nil, vsaerrors.New("no node found for the pool")
	}
	params.Nodes = nodes
	return params, nil
}

func (a *InternalSnapshotsDeleteActivity) ListSnapshotInONTAP(ctx context.Context, params *commonparams.SnapshotsInternalDeleteParams, node *models.Node) (*commonparams.SnapshotsInternalDeleteParams, error) {
	provider, err := activities.GetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	snapshots, err := provider.ListSnapmirrorSnapshots(params.Volume.VolumeAttributes.ExternalUUID)
	if err != nil {
		if customerrors.IsConflictErr(err) {
			return nil, customerrors.NewNonRetryableErr(err.Error())
		}
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	snapshotList := make([]*commonparams.SnapshotListResponse, 0, len(snapshots))
	for _, snapshot := range snapshots {
		snapshotList = append(snapshotList, &commonparams.SnapshotListResponse{
			Name:               snapshot.Name,
			ExternalUUID:       snapshot.ExternalUUID,
			VolumeExternalUUID: params.Volume.VolumeAttributes.ExternalUUID,
		})
	}

	params.SnapshotsFromOntap = snapshotList
	return params, nil
}

func (r *InternalSnapshotsDeleteActivity) ListSnapshotFromDB(ctx context.Context, params *commonparams.SnapshotsInternalDeleteParams) (*commonparams.SnapshotsInternalDeleteParams, error) {
	se := r.SE
	logger := util.GetLogger(ctx)

	snapshots, err := se.GetReplicationSnapshotsByVolumeID(ctx, params.Volume.ID)
	if err != nil {
		logger.Errorf("Failed to list snapshots for account %s: %v", params.AccountName, err)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	params.SnapshotsFromDB = snapshots
	return params, nil
}

func (r *InternalSnapshotsDeleteActivity) DeleteSnapshotsInONTAP(ctx context.Context, params *commonparams.SnapshotsInternalDeleteParams, node *models.Node) error {
	logger := util.GetLogger(ctx)
	provider, err := activities.GetProviderByNode(ctx, node)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	for _, snapshot := range params.SnapshotsFromOntap {
		if err := provider.DeleteSnapshot(snapshot.ExternalUUID, snapshot.VolumeExternalUUID); err != nil && !customerrors.IsNotFoundErr(err) {
			if customerrors.IsConflictErr(err) {
				return customerrors.NewNonRetryableErr(err.Error())
			}
			logger.Errorf("Failed to delete snapshots for account %s: %v", params.AccountName, err)
			return err
		}
		logger.Infof("Snapshot %s deleted successfully from the vsa cluster", snapshot.Name)
	}
	return nil
}

func (r *InternalSnapshotsDeleteActivity) UpdateSnapshotRecordInDB(ctx context.Context, params *commonparams.SnapshotsInternalDeleteParams) error {
	logger := util.GetLogger(ctx)
	se := r.SE
	for _, snapshot := range params.SnapshotsFromDB {
		_, err := se.DeleteSnapshot(ctx, snapshot.UUID)
		if err != nil {
			if customerrors.IsNotFoundErr(err) {
				logger.Infof("Snapshot %s not found, assuming it is already deleted", snapshot.Name)
			}
			logger.Infof("Snapshot %s error out, ", snapshot.Name)
		}
		logger.Infof("Snapshot:%s marked deleted successfully in the db", snapshot.Name)
	}
	return nil
}

func (r *InternalSnapshotsDeleteActivity) DehydrateSnapshots(ctx context.Context, params *commonparams.SnapshotsInternalDeleteParams) error {
	logger := util.GetLogger(ctx)
	if len(params.SnapshotsFromDB) > 0 {
		if hydrationEnabled {
			err := hydrateBatchSnapshotsToCCFE(ctx, nil, params.SnapshotsFromDB)
			if err != nil {
				return vsaerrors.NewVCPError(vsaerrors.ErrDeHydrateSnapshots, err)
			}
		}
	}
	logger.Infof("Snapshots dehydration completed successfully for volume %s in project %s", params.Volume.Name, params.AccountName)
	return nil
}
