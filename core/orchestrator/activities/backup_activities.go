package activities

import (
	"context"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	wait = time.Duration(env.GetUint("ONTAP_REST_ASYNC_POLL_WAIT_SECONDS", 3)) * time.Second
)

type BackupActivity struct {
	SE database.Storage
}

func (a *BackupActivity) CreateBackup(ctx context.Context, backup *datamodel.Backup) (*datamodel.Backup, error) {
	se := a.SE
	return se.CreateBackup(ctx, backup)
}
func (a *BackupActivity) GetBackup(ctx context.Context, backupUUID string) (*datamodel.Backup, error) {
	se := a.SE
	return se.GetBackup(ctx, backupUUID)
}
func (a *BackupActivity) DeleteBackup(ctx context.Context, backupUUID string) (*datamodel.Backup, error) {
	se := a.SE
	return se.DeleteBackup(ctx, backupUUID)
}
func (a *BackupActivity) FinishBackup(ctx context.Context, backup *datamodel.Backup) error {
	se := a.SE
	_, err := se.FinishBackup(ctx, backup)
	return err
}

func (a *BackupActivity) UpdateBackupError(ctx context.Context, backup *datamodel.Backup, errorString string) error {
	if backup == nil || errorString == "" {
		return errors.New("invalid input")
	}
	se := a.SE
	backup.State = models.LifeCycleStateError
	backup.StateDetails = errorString
	_, err := se.UpdateBackupState(ctx, backup)
	return err
}

func (a *BackupActivity) GetOrCreateObjectStore(ctx context.Context, node *models.Node, name, containerName string) (*commonparams.CloudTarget, error) {
	provider := GetProviderByNode(ctx, node)
	// Handle both return values from CloudTargetGet
	objectStore, err := provider.CloudTargetGet(&name)
	// Check if the error is nil, which means the object store already exists
	if err == nil {
		// If no error, return the existing object store
		return &commonparams.CloudTarget{Name: *objectStore.Name}, nil
	}
	objectStore, err = provider.CloudTargetCreate(name, containerName)
	if err == nil {
		// If no error, return the existing object store
		return &commonparams.CloudTarget{Name: *objectStore.Name}, nil
	}

	return nil, errors.New("failed to get or create object store")
}

func (a *BackupActivity) SnapmirrorGetorCreate(ctx context.Context, node *models.Node, sourcePath, destinationPath string) (*commonparams.SnapmirrorRelationship, error) {
	logger := util.GetLogger(ctx)
	provider := GetProviderByNode(ctx, node)
	snapmirror, err := provider.SnapmirrorRelationshipGet(destinationPath, sourcePath)
	if err != nil {
		// Log the error but continue to create a new snapmirror relationship
		logger.Info(err.Error())
	}
	if snapmirror != nil {
		resp := commonparams.SnapmirrorRelationship{UUID: snapmirror.UUID.String()}
		if snapmirror.Destination != nil && snapmirror.Destination.UUID != nil {
			resp.DestinationUUID = nillable.ToPointer(snapmirror.Destination.UUID.String())
		}
		return &resp, nil
	}
	snapmirror, err = provider.SnapmirrorRelationshipCreate(destinationPath, sourcePath)
	if snapmirror != nil {
		resp := commonparams.SnapmirrorRelationship{UUID: snapmirror.UUID.String()}
		if snapmirror.Destination != nil && snapmirror.Destination.UUID != nil {
			resp.DestinationUUID = nillable.ToPointer(snapmirror.Destination.UUID.String())
		}
		return &resp, nil
	}
	return nil, err
}

func (a *BackupActivity) SnapshotCreate(ctx context.Context, node *models.Node, volumeUUID, name, comment string) (*vsa.SnapshotProviderResponse, error) {
	provider := GetProviderByNode(ctx, node)
	return provider.CreateSnapshot(vsa.CreateSnapshotParams{
		VolumeUUID: volumeUUID,
		Name:       name,
		Comment:    comment,
	})
}

func (a *BackupActivity) SnapmirrorTransfer(ctx context.Context, node *models.Node, snapmirrorUUID, snapshotName string) error {
	provider := GetProviderByNode(ctx, node)
	return provider.SnapmirrorRelationshipTransferCreate(snapmirrorUUID, snapshotName)
}

func (a *BackupActivity) SnapmirrorTransferPoll(ctx context.Context, node *models.Node, snapmirrorUUID, snapshotName string) error {
	provider := GetProviderByNode(ctx, node)
	// Start polling for the snapmirror transfer status
	// Keep polling until the transfer is either successful or failed
	for {
		rsp, err := provider.SnapmirrorRelationshipTransferGet(snapmirrorUUID, snapshotName)
		if err != nil {
			return err
		}
		if rsp == nil {
			return nil
		}
		if rsp != nil && rsp.State != nil && *rsp.State == "failed" {
			return errors.New("Snapmirror transfer failed with state: " + *rsp.State)
		}
		if rsp != nil && rsp.State != nil && *rsp.State == "success" {
			return nil
		}
		// TODO: Use workflow.Sleep instead of time.Sleep
		time.Sleep(wait)
	}
}

func (a *BackupActivity) DeleteBackupSnapshot(ctx context.Context, node *models.Node, snapshotUUID, volumeUUID string) error {
	provider := GetProviderByNode(ctx, node)
	return provider.DeleteSnapshot(snapshotUUID, volumeUUID)
}
