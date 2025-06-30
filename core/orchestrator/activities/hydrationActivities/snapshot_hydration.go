package hydrationActivities

import (
	"context"
	"errors"
	"sort"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	HydrateBatchSnapshotstoCCFE  = _hydrateBatchSnapshotstoCCFE
	batchHydrateCreatedSnapshots = common.BatchHydrateCreatedSnapshots
	batchHydrateDeletedSnapshots = common.BatchHydrateDeletedSnapshots
)

// batchHydrateSnapshots hydrates snapshots in batches grouped by volume to CCFE.
func batchHydrateSnapshots(ctx context.Context, snapshots []*datamodel.Snapshot, batchHydrateFunc func(context.Context, log.Logger, []models.Request, string, string, string, string) error, callbackToken string) error {
	logger := util.GetLogger(ctx)

	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].Volume.Name < snapshots[j].Volume.Name
	})

	var requestArr []models.Request
	var newVolName, currVolumeName, region string
	currVolumeName = snapshots[0].Volume.Name

	for _, ss := range snapshots {
		var errValidate error
		region, errValidate = _validateSnapshot(*ss)
		if errValidate != nil {
			logger.Error("Error validating snapshot", "snapshotName", ss.Name, "error", errValidate, "volumeName", ss.Volume.Name)
			continue // Skip this snapshot if validation fails
		}
		newVolName = ss.Volume.Name
		if newVolName != currVolumeName {
			// if set of snapshots with new VolumeID are found, push the batch with current volumeID in CCFE
			err := batchHydrateFunc(ctx, logger, requestArr, currVolumeName, region, ss.Account.Name, callbackToken)
			if err != nil {
				logger.Error("Unsuccessful Batch Snapshot hydration, continuing with next volume", "requestArr", requestArr, "VolumeName", currVolumeName, "region", region, "accountName", ss.Account.Name, "error", err)
			}
			// clean the array so that snapshots for next volume are pushed clean in this. No redundacy of snapshots is done.
			requestArr = []models.Request{}
			currVolumeName = newVolName
		}
		convertedSS := _convertBulkSnapshotToGCPSnapshotObject(*ss)
		request := models.Request{Snapshot: &convertedSS}
		requestArr = append(requestArr, request)
	}

	// last batch won't be pushed so adding for it explicitly
	err := batchHydrateFunc(ctx, logger, requestArr, currVolumeName, region, snapshots[len(snapshots)-1].Account.Name, callbackToken)
	if err != nil {
		logger.Error("Unsuccessful Batch Snapshot hydration, continuing with next volume", "requestArr ", requestArr, "VolumeName", currVolumeName, "region", region, "accountName", snapshots[len(snapshots)-1].Account.Name, "error", err)
	}

	return nil
}

// _hydrateBatchSnapshotstoCCFE hydrates batches of created and deleted snapshots to CCFE.
func _hydrateBatchSnapshotstoCCFE(ctx context.Context, createdSnapshots []*datamodel.Snapshot, deletedSnapshots []*datamodel.Snapshot) error {
	logger := util.GetLogger(ctx)
	callbackToken, err := auth.GenerateCallbackToken(ctx)
	if err != nil {
		logger.Error("Error when getting callback token", err)
		return err
	}

	if len(createdSnapshots) > 0 {
		err := batchHydrateSnapshots(ctx, createdSnapshots, batchHydrateCreatedSnapshots, callbackToken)
		if err != nil {
			return err
		}
	}

	if len(deletedSnapshots) > 0 {
		err := batchHydrateSnapshots(ctx, deletedSnapshots, batchHydrateDeletedSnapshots, callbackToken)
		if err != nil {
			return err
		}
	}
	return nil
}

// _convertBulkSnapshotToGCPSnapshotObject converts a snapshot to a GCP-compatible snapshot object.
func _convertBulkSnapshotToGCPSnapshotObject(snapshot datamodel.Snapshot) models.HydrateSnapshot {
	gcpSnapshot := models.HydrateSnapshot{
		ResourceId:   utils.RenameSnapshotName(snapshot.Name),
		SnapshotId:   snapshot.UUID,
		State:        common.MapStateToGcpState(snapshot.State),
		StateDetails: snapshot.StateDetails,
		CreateTime:   snapshot.CreatedAt,
		VolumeName:   snapshot.Volume.Name,
		AccountName:  snapshot.Account.Name,
	}
	if snapshot.SnapshotAttributes != nil {
		gcpSnapshot.UsedBytes = snapshot.SnapshotAttributes.SizeInBytes
	}
	if snapshot.Description != "" {
		gcpSnapshot.Description = snapshot.Description
	}
	return gcpSnapshot
}

// _validateSnapshot validates the snapshot's UUID, volume name, account name, and description length.
func _validateSnapshot(ss datamodel.Snapshot) (string, error) {
	region := utils.GetRegion(ss)
	if region == "" {
		return region, errors.New("errorEmptyRegion")
	}
	if ss.UUID == "" {
		return region, errors.New("errorEmptySnapShotSnapshotUUID")
	}
	if ss.Volume != nil && ss.Volume.Name == "" {
		return region, errors.New("errorEmptySnapShotVolumeName")
	}
	if ss.Account != nil && ss.Account.Name == "" {
		return region, errors.New("errorEmptySnapshotAccountName")
	}
	if len(ss.Description) >= 1024 {
		return region, errors.New("errorTooLongDescription")
	}
	return region, nil
}
