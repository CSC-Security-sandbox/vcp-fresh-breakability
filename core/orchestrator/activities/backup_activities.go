package activities

import (
	"context"
	"fmt"
	"time"

	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"gorm.io/gorm"
)

const (
	BackupComment               = "VCP-Backup"
	SmStatusTransferring        = "transferring"
	SmStatusSuccess             = "success"
	SmStatusFailed              = "failed"
	BackupRestoreCountIncrement = "increment"
	BackupRestoreCountDecrement = "decrement"
)

var (
	hydrationEnabled = env.GetBool("GCP_HYDRATE_ENABLED", true)
)

// EventHistorySafetyThreshold is the threshold for triggering ContinueAsNew
// this is basically number of workflow event logs, every workflow has max 50100 events. so we made 45000 as a threshold for backup. and when we reach this limit, we create a new workflow and continue the execution from there.
const (
	EventHistorySafetyThreshold = 45000
)

type BackupActivity struct {
	SE database.Storage
}

type BackupWorkflowInput struct {
	Backup      *datamodel.Backup
	BackupVault *datamodel.BackupVault
	Volume      *datamodel.Volume
}

type ScheduledBackupParams struct {
	Backups       []*datamodel.Backup
	BackupPolicy  *datamodel.BackupPolicy
	OntapSnapshot *vsa.SnapshotProviderResponse
	Job           *datamodel.Job
}
type BackupActivitiesContext struct {
	// Initial inputs
	BackupWorkflowInit *BackupWorkflowInput
	// for Scheduled backup workflow
	ScheduledBackupParams *ScheduledBackupParams

	// Workflow state
	Node                   *models.Node
	ObjStoreName           string
	BucketDetails          *datamodel.BucketDetails
	BucketName             string
	ObjStore               *commonparams.CloudTarget
	SmSourcePath           string
	SmDestinationPath      string
	SnapmirrorRelationship *commonparams.SnapmirrorRelationship
	SnapshotName           string
	SnapshotResponse       *vsa.SnapshotProviderResponse
	TransferStatus         string
	DbSnapshot             *datamodel.Snapshot
	ObjStoreSnapshot       *vsa.SmObjectStoreEndpointSnapshot
}

// PollTransferStatusInput represents the input for the polling activity
type PollTransferStatusInput struct {
	BackupActivitiesContext *BackupActivitiesContext
	Node                    *models.Node
	SnapmirrorRelationship  *commonparams.SnapmirrorRelationship
	SnapshotName            string
	EventHistoryCount       int
	NextWaitTime            time.Duration
}

// PollTransferStatusOutput represents the output of the polling activity
type PollTransferStatusOutput struct {
	BackupActivitiesContext *BackupActivitiesContext
	TransferComplete        bool
	ShouldContinueAsNew     bool
	ContinueAsNewReason     string
	NextWaitTime            time.Duration
}

func (a BackupActivity) CreateBackup(ctx context.Context, backup *datamodel.Backup) (*datamodel.Backup, error) {
	se := a.SE
	return se.CreateBackup(ctx, backup)
}

func (a BackupActivity) IsSnapmirrorDeleted(ctx context.Context, node *models.Node, params *commonparams.SnapmirrorRelationshipParams) (bool, error) {
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return false, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	_, err = provider.SnapmirrorRelationshipGet(params.DestinationPath, params.SourcePath)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			return true, nil
		}
		return false, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return false, nil
}

func (a BackupActivity) GetBackup(ctx context.Context, backupVaultUUID, backupUUID, accountName string) (*datamodel.Backup, error) {
	se := a.SE
	return se.GetBackup(ctx, backupVaultUUID, backupUUID, accountName)
}

func (a BackupActivity) DeleteBackup(ctx context.Context, backupUUID string) (*datamodel.Backup, error) {
	se := a.SE
	return se.DeleteBackup(ctx, backupUUID)
}

func (a BackupActivity) FinishBackup(ctx context.Context, backup *datamodel.Backup) error {
	se := a.SE
	_, err := se.FinishBackup(ctx, backup)
	return err
}

func (a BackupActivity) UpdateBackupError(ctx context.Context, backup *datamodel.Backup, errorString string) error {
	if backup == nil || errorString == "" {
		return errors.New("invalid input")
	}
	se := a.SE
	backup.State = models.LifeCycleStateError
	backup.StateDetails = errorString
	_, err := se.UpdateBackupState(ctx, backup)
	return err
}

func (a BackupActivity) MarkBackupAvailable(ctx context.Context, backup *datamodel.Backup) error {
	if backup == nil {
		return errors.New("backup cannot be nil")
	}
	se := a.SE
	backup.State = models.LifeCycleStateAvailable
	backup.StateDetails = models.LifeCycleStateAvailableDetails
	_, err := se.UpdateBackupState(ctx, backup)
	return err
}

// PrepareObjectStoreActivity prepares object store details
func (b *BackupActivity) PrepareObjectStoreActivity(ctx context.Context, backupActivitiesContext *BackupActivitiesContext) (*BackupActivitiesContext, error) {
	objStoreName, err := getObjStoreName(backupActivitiesContext.BackupWorkflowInit.BackupVault, backupActivitiesContext.BackupWorkflowInit.Volume)
	if err != nil {
		return nil, err
	}
	backupActivitiesContext.ObjStoreName = objStoreName

	bucketDetails, err := getBucketDetails(backupActivitiesContext.BackupWorkflowInit.BackupVault, backupActivitiesContext.BackupWorkflowInit.Volume)
	if err != nil {
		return nil, err
	}
	backupActivitiesContext.BucketDetails = bucketDetails
	backupActivitiesContext.BucketName = bucketDetails.BucketName

	return backupActivitiesContext, nil
}

// GetOrCreateObjectStoreActivity gets or creates object store
func (b *BackupActivity) GetOrCreateObjectStoreActivity(ctx context.Context, backupActivitiesContext *BackupActivitiesContext) (*BackupActivitiesContext, error) {
	objStore, err := b.GetOrCreateObjectStore(ctx, backupActivitiesContext.Node, backupActivitiesContext.ObjStoreName, backupActivitiesContext.BucketName)
	if err != nil {
		return nil, err
	}
	backupActivitiesContext.ObjStore = objStore
	backupActivitiesContext.BackupWorkflowInit.Backup.Attributes.BucketName = backupActivitiesContext.BucketName
	backupActivitiesContext.BackupWorkflowInit.Backup.Attributes.ServiceAccountName = backupActivitiesContext.BucketDetails.ServiceAccountName
	backupActivitiesContext.BackupWorkflowInit.Backup.Attributes.ObjectStoreUUID = objStore.UUID
	return backupActivitiesContext, nil
}

// PrepareSnapmirrorActivity prepares snapmirror paths
func (b *BackupActivity) PrepareSnapmirrorActivity(ctx context.Context, backupActivitiesContext *BackupActivitiesContext) (*BackupActivitiesContext, error) {
	smDestinationPath, err := getSmDestinationPath(backupActivitiesContext.BackupWorkflowInit.BackupVault, backupActivitiesContext.BackupWorkflowInit.Volume)
	if err != nil {
		return nil, err
	}
	backupActivitiesContext.SmDestinationPath = smDestinationPath
	backupActivitiesContext.SmSourcePath = getSmSourcePath(backupActivitiesContext.BackupWorkflowInit.Volume)

	return backupActivitiesContext, nil
}

// CreateSnapmirrorRelationshipActivity creates snapmirror relationship
func (b *BackupActivity) CreateSnapmirrorRelationshipActivity(ctx context.Context, backupActivitiesContext *BackupActivitiesContext) (*BackupActivitiesContext, error) {
	snapmirrorParams := &commonparams.SnapmirrorRelationshipParams{
		SourcePath:      backupActivitiesContext.SmSourcePath,
		DestinationPath: backupActivitiesContext.SmDestinationPath,
		SourceUUID:      nil,
		IsRestore:       false,
	}

	snapmirrorRelationship, err := b.SnapmirrorGetOrCreate(ctx, backupActivitiesContext.Node, snapmirrorParams)
	if err != nil {
		return nil, err
	}

	backupActivitiesContext.SnapmirrorRelationship = snapmirrorRelationship
	if snapmirrorRelationship != nil && snapmirrorRelationship.DestinationUUID != nil {
		backupActivitiesContext.BackupWorkflowInit.Backup.Attributes.EndpointUUID = *snapmirrorRelationship.DestinationUUID
	} else {
		return backupActivitiesContext, vsaerrors.NewVCPError(vsaerrors.ErrResourceEmptyError, fmt.Errorf("DestinationUUID not found in snapmirror relationship"))
	}

	return backupActivitiesContext, nil
}

// CreatingSnapshotActivity creates snapshot in database
func (b *BackupActivity) CreatingSnapshotActivity(ctx context.Context, backupActivitiesContext *BackupActivitiesContext) (*BackupActivitiesContext, error) {
	logger := util.GetLogger(ctx)
	if !backupActivitiesContext.BackupWorkflowInit.Backup.Attributes.UseExistingSnapshot {
		backupActivitiesContext.SnapshotName = backupActivitiesContext.BackupWorkflowInit.Backup.Name
	} else {
		// If UseExistingSnapshot is true, we use the existing snapshot name from the backup attributes
		if backupActivitiesContext.BackupWorkflowInit.Backup.Attributes.SnapshotName == "" {
			return nil, errors.New("snapshot name is empty in backup attributes")
		}
		backupActivitiesContext.SnapshotName = backupActivitiesContext.BackupWorkflowInit.Backup.Attributes.SnapshotName
	}
	snapshot := &datamodel.Snapshot{
		Name:               backupActivitiesContext.SnapshotName,
		Description:        BackupComment,
		VolumeID:           backupActivitiesContext.BackupWorkflowInit.Volume.ID,
		AccountID:          backupActivitiesContext.BackupWorkflowInit.Volume.AccountID,
		Volume:             backupActivitiesContext.BackupWorkflowInit.Volume,
		Account:            backupActivitiesContext.BackupWorkflowInit.Volume.Account,
		IsAppConsistent:    false,
		Type:               SnapshotTypeBackup,
		SnapshotAttributes: &datamodel.SnapshotAttributes{},
	}
	var dbSnapshot *datamodel.Snapshot
	var err error
	// If UseExistingSnapshot is true, we do not create a new snapshot in the database
	if backupActivitiesContext.BackupWorkflowInit.Backup.Attributes.UseExistingSnapshot {
		dbSnapshot, err = b.SE.GetSnapshotByNameAndVolumeId(ctx, snapshot.Name, snapshot.AccountID, snapshot.VolumeID)
		if err != nil {
			if errors.IsNotFoundErr(err) {
				return backupActivitiesContext, vsaerrors.NewVCPError(vsaerrors.ErrResourceNotFound, errors.NewNotFoundErr("snapshot", &snapshot.Name))
			}
			logger.Errorf("Failed to get snapshot from database. Error: %v", err)
			return backupActivitiesContext, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
		}
	} else {
		dbSnapshot, err = b.SE.CreatingSnapshot(ctx, snapshot)
		if err != nil {
			logger.Errorf("Failed to create snapshot in database. Error: %v", err)
			return backupActivitiesContext, err
		}
	}
	backupActivitiesContext.DbSnapshot = dbSnapshot
	return backupActivitiesContext, nil
}

// UpdateSnapshotActivity updates snapshot in database
func (b *BackupActivity) UpdateSnapshotActivity(ctx context.Context, backupActivitiesContext *BackupActivitiesContext) (*BackupActivitiesContext, error) {
	logger := util.GetLogger(ctx)
	if backupActivitiesContext.DbSnapshot == nil {
		return nil, errors.New("database snapshot is nil")
	}
	if !backupActivitiesContext.BackupWorkflowInit.Backup.Attributes.UseExistingSnapshot {
		// Update the snapshot in the database
		if backupActivitiesContext.SnapshotResponse != nil {
			backupActivitiesContext.DbSnapshot.State = models.LifeCycleStateREADY
			backupActivitiesContext.DbSnapshot.StateDetails = models.LifeCycleStateAvailableDetails
			backupActivitiesContext.DbSnapshot.SnapshotAttributes.SizeInBytes = backupActivitiesContext.SnapshotResponse.SizeInBytes
			backupActivitiesContext.DbSnapshot.SnapshotAttributes.ExternalUUID = backupActivitiesContext.SnapshotResponse.ExternalUUID
			backupActivitiesContext.DbSnapshot.SnapshotAttributes.LogicalSizeUsedInBytes = backupActivitiesContext.SnapshotResponse.LogicalSizeInBytes
		} else {
			now := time.Now()
			backupActivitiesContext.DbSnapshot.DeletedAt = &gorm.DeletedAt{Time: now, Valid: true}
			backupActivitiesContext.DbSnapshot.State = models.LifeCycleStateError
			backupActivitiesContext.DbSnapshot.StateDetails = models.LifeCycleStateCreationErrorDetails
		}
		_, err := b.SE.UpdateSnapshot(ctx, backupActivitiesContext.DbSnapshot)
		if err != nil {
			logger.Errorf("Failed to update snapshot details in database. Error: %v", err)
			return nil, err
		}
	}
	return backupActivitiesContext, nil
}

// CreateSnapshotActivity creates snapshot in Ontap
func (b *BackupActivity) CreateSnapshotActivity(ctx context.Context, backupActivitiesContext *BackupActivitiesContext) (*BackupActivitiesContext, error) {
	logger := util.GetLogger(ctx)
	var snapshotResponse *vsa.SnapshotProviderResponse
	var err error
	if backupActivitiesContext.BackupWorkflowInit.Backup.Attributes.UseExistingSnapshot {
		// If UseExistingSnapshot is true, we do not create a new snapshot in Ontap
		if backupActivitiesContext.SnapshotName == "" {
			return nil, errors.New("snapshot name is empty in backup attributes")
		}
		logger.Infof("Using existing snapshot: %s", backupActivitiesContext.SnapshotName)
		dbSnapshot, err := b.SE.GetSnapshotByNameAndVolumeId(ctx, backupActivitiesContext.BackupWorkflowInit.Backup.Attributes.SnapshotName, backupActivitiesContext.BackupWorkflowInit.Volume.AccountID, backupActivitiesContext.BackupWorkflowInit.Volume.ID)
		if err != nil {
			logger.Errorf("Failed to get snapshot from database. Error: %v", err)
			return nil, err
		}
		snapshotResponse, err = b.SnapshotGet(ctx, backupActivitiesContext.Node, dbSnapshot.SnapshotAttributes.ExternalUUID, backupActivitiesContext.BackupWorkflowInit.Volume.VolumeAttributes.ExternalUUID)
		if err != nil {
			logger.Errorf("Failed to get snapshot from Ontap. Error: %v", err)
			return nil, err
		}
	} else {
		snapshotResponse, err = b.SnapshotCreate(ctx, backupActivitiesContext.Node, backupActivitiesContext.BackupWorkflowInit.Volume.VolumeAttributes.ExternalUUID, backupActivitiesContext.SnapshotName, BackupComment)
		if err != nil {
			logger.Errorf("Failed to create snapshot in Ontap. Error: %v", err)
			return nil, err
		}
	}

	// Update the backupActivitiesContext with snapshot response
	backupActivitiesContext.SnapshotResponse = snapshotResponse
	if backupActivitiesContext.BackupWorkflowInit != nil && backupActivitiesContext.BackupWorkflowInit.Backup != nil && backupActivitiesContext.BackupWorkflowInit.Backup.Attributes != nil {
		backupActivitiesContext.BackupWorkflowInit.Backup.Attributes.SnapshotName = backupActivitiesContext.SnapshotName
		if backupActivitiesContext.SnapshotResponse != nil {
			backupActivitiesContext.BackupWorkflowInit.Backup.Attributes.SnapshotID = backupActivitiesContext.SnapshotResponse.ExternalUUID
		}
	}
	return backupActivitiesContext, nil
}

// TransferSnapshotActivity initiates snapshot transfer
func (b *BackupActivity) TransferSnapshotActivity(ctx context.Context, backupActivitiesContext *BackupActivitiesContext) (*BackupActivitiesContext, error) {
	err := b.SnapmirrorTransfer(ctx, backupActivitiesContext.Node, backupActivitiesContext.SnapmirrorRelationship.UUID, backupActivitiesContext.SnapshotName)
	if err != nil {
		return nil, err
	}
	return backupActivitiesContext, nil
}

// CheckTransferStatusActivity checks transfer status
func (b *BackupActivity) CheckTransferStatusActivity(ctx context.Context, backupActivitiesContext *BackupActivitiesContext) (*BackupActivitiesContext, error) {
	status, err := b.GetSnapmirrorTransferStatus(ctx, backupActivitiesContext.Node, backupActivitiesContext.SnapmirrorRelationship.UUID, backupActivitiesContext.SnapshotName)
	if err != nil {
		return nil, err
	}
	if status == SmStatusSuccess {
		backupActivitiesContext.BackupWorkflowInit.Backup.Attributes.SnapshotCreationTime = time.Now().String()
	}
	backupActivitiesContext.TransferStatus = status
	return backupActivitiesContext, nil
}

func (a BackupActivity) GetSnapshotFromObjectStore(ctx context.Context, node *models.Node, objectStoreUUID, EndpointUUID, snapshotUUID string) (*vsa.SmObjectStoreEndpointSnapshot, error) {
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return provider.SnapmirrorObjectStoreSnapshotGet(objectStoreUUID, EndpointUUID, snapshotUUID)
}

func (a BackupActivity) GetObjectStoreEndpointInfo(ctx context.Context, node *models.Node, objectStoreUUID, EndpointUUID string) (*vsa.SmObjectStoreEndpointt, error) {
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return provider.ObjectStoreEndpointInfoGet(objectStoreUUID, EndpointUUID)
}

// GetObjectStoreEndpointActivity gets object store endpoint info
func (b *BackupActivity) GetObjectStoreEndpointActivity(ctx context.Context, backupActivitiesContext *BackupActivitiesContext) (*BackupActivitiesContext, error) {
	objStoreEndpointInfo, err := b.GetObjectStoreEndpointInfo(ctx, backupActivitiesContext.Node, backupActivitiesContext.ObjStore.UUID, backupActivitiesContext.BackupWorkflowInit.Backup.Attributes.EndpointUUID)
	if err != nil {
		return nil, err
	}
	backupActivitiesContext.BackupWorkflowInit.Backup.LatestLogicalBackupSize = *objStoreEndpointInfo.LogicalSize
	return backupActivitiesContext, nil
}

// GetObjectStoreSnapshotActivity gets snapshot from object store
func (b *BackupActivity) GetObjectStoreSnapshotActivity(ctx context.Context, backupActivitiesContext *BackupActivitiesContext) (*BackupActivitiesContext, error) {
	objStoreSnapshot, err := b.GetSnapshotFromObjectStore(ctx, backupActivitiesContext.Node, backupActivitiesContext.ObjStore.UUID, backupActivitiesContext.BackupWorkflowInit.Backup.Attributes.EndpointUUID, backupActivitiesContext.BackupWorkflowInit.Backup.Attributes.SnapshotID)
	if err != nil {
		return nil, err
	}
	backupActivitiesContext.ObjStoreSnapshot = objStoreSnapshot
	if objStoreSnapshot.LogicalSize != nil {
		backupActivitiesContext.BackupWorkflowInit.Backup.SizeInBytes = *objStoreSnapshot.LogicalSize
	} else {
		backupActivitiesContext.BackupWorkflowInit.Backup.SizeInBytes = 0
	}
	return backupActivitiesContext, nil
}

// UpdateBackupSizeActivity updates backup size fields in both backup and volume tables
func (b *BackupActivity) UpdateBackupSizeActivity(ctx context.Context, backupActivitiesContext *BackupActivitiesContext) (*BackupActivitiesContext, error) {
	logger := util.GetLogger(ctx)
	backup := backupActivitiesContext.BackupWorkflowInit.Backup
	volumeUUID := backup.VolumeUUID

	_, err := b.SE.FinishBackup(ctx, backup)
	if err != nil {
		logger.Errorf("Failed to update backup %s with size information: %v", backup.UUID, err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Set LatestLogicalBackupSize to 0 for all previous backups of the same volume in a single query
	// This ensures that only the latest backup has the correct size
	// Update only if the latest logical backup size is not zero for the current backup
	if backupActivitiesContext.BackupWorkflowInit.Backup.LatestLogicalBackupSize != 0 {
		err = b.SE.UpdateBackupLatestLogicalBackupSizeByVolume(ctx, volumeUUID, backup.UUID)
		if err != nil {
			logger.Errorf("Failed to reset LatestLogicalBackupSize for previous backups of volume %s: %v", volumeUUID, err)
			return nil, vsaerrors.WrapAsTemporalApplicationError(err)
		}
	}

	// Update the volume's LatestLogicalBackupSize field
	updates := make(map[string]interface{})
	backupActivitiesContext.BackupWorkflowInit.Volume.DataProtection.BackupChainBytes = &backupActivitiesContext.BackupWorkflowInit.Backup.LatestLogicalBackupSize
	updates["data_protection"] = backupActivitiesContext.BackupWorkflowInit.Volume.DataProtection
	// Update the volume's LatestLogicalBackupSize field
	err = b.SE.UpdateVolumeFields(ctx, volumeUUID, updates)

	if err != nil {
		logger.Errorf("Failed to update volume %s with latest logical backup size: %v", volumeUUID, err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	logger.Debugf("Successfully updated backup size fields for backup %s and volume %s", backup.UUID, volumeUUID)
	return backupActivitiesContext, nil
}

func (a *BackupActivity) UpdateVolumeLatestLogicalBackupSize(ctx context.Context, volume *datamodel.Volume, logicalSize int64) error {
	logger := util.GetLogger(ctx)
	// Update volume's latest logical backup size
	volumeUpdates := make(map[string]interface{})
	// LatestLogicalBackupSize(backup datamodel) is equivalent to BackupChainBytes(volume datamodel) and chainStorageBytes
	volume.DataProtection.BackupChainBytes = &logicalSize
	volumeUpdates["data_protection"] = volume.DataProtection
	err := a.SE.UpdateVolumeFields(ctx, volume.UUID, volumeUpdates)
	if err != nil {
		logger.Errorf("Failed to update volume %s with latest logical backup size: %v", volume.Name, err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	logger.Infof("Successfully updated logical size %d for volume %s",
		logicalSize, volume.Name)
	return nil
}

// UpdateConstituentCountForBackup updates constituent count for large volume backups
func (b *BackupActivity) UpdateConstituentCountForBackup(ctx context.Context, backupActivitiesContext *BackupActivitiesContext) (*BackupActivitiesContext, error) {
	if backupActivitiesContext.BackupWorkflowInit.Volume.LargeVolumeAttributes == nil || !backupActivitiesContext.BackupWorkflowInit.Volume.LargeVolumeAttributes.LargeCapacity {
		// No need to update constituent count for non-large volumes
		return backupActivitiesContext, nil
	}

	logger := util.GetLogger(ctx)
	volume := backupActivitiesContext.BackupWorkflowInit.Volume
	if backupActivitiesContext.ScheduledBackupParams != nil && len(backupActivitiesContext.ScheduledBackupParams.Backups) > 0 {
		for _, bkp := range backupActivitiesContext.ScheduledBackupParams.Backups {
			_, err := b.SE.UpdateBackupConstituentCountFromVolume(ctx, bkp, volume)
			if err != nil {
				logger.Errorf("Failed to update Constituent count for a backup in scheduled backups")
				return nil, vsaerrors.WrapAsTemporalApplicationError(err)
			}
		}
	} else {
		backup := backupActivitiesContext.BackupWorkflowInit.Backup

		_, err := b.SE.UpdateBackupConstituentCountFromVolume(ctx, backup, volume)
		if err != nil {
			logger.Errorf("Failed to update Constituent count for a backup")
			return nil, vsaerrors.WrapAsTemporalApplicationError(err)
		}
	}
	return backupActivitiesContext, nil
}

// FinishBackupActivity finishes the backup
func (b *BackupActivity) FinishBackupActivity(ctx context.Context, backupActivitiesContext *BackupActivitiesContext) (*BackupActivitiesContext, error) {
	err := b.FinishBackup(ctx, backupActivitiesContext.BackupWorkflowInit.Backup)
	if err != nil {
		return nil, err
	}
	return backupActivitiesContext, nil
}

func (a BackupActivity) GetOrCreateObjectStore(ctx context.Context, node *models.Node, name, containerName string) (*commonparams.CloudTarget, error) {
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	// Handle both return values from CloudTargetGet
	objectStore, err := provider.CloudTargetGet(&name)
	// Check if the error is nil, which means the object store already exists
	if err == nil {
		// If no error, return the existing object store
		return &commonparams.CloudTarget{Name: *objectStore.Name, UUID: *objectStore.UUID}, nil
	}
	objectStore, err = provider.CloudTargetCreate(name, containerName)
	if err == nil {
		// If no error, return the existing object store
		return &commonparams.CloudTarget{Name: *objectStore.Name, UUID: *objectStore.UUID}, nil
	}

	return nil, errors.New("failed to get or create object store")
}

func (a BackupActivity) SnapmirrorGetOrCreate(ctx context.Context, node *models.Node, params *commonparams.SnapmirrorRelationshipParams) (*commonparams.SnapmirrorRelationship, error) {
	logger := util.GetLogger(ctx)
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	snapmirror, err := provider.SnapmirrorRelationshipGet(params.DestinationPath, params.SourcePath)
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
	// Remove this once we start using cache to store token
	smcLicense, err := GetSmcLicenseFromCloud(ctx)
	if err != nil {
		logger.Errorf("Failed to get SMC license from cloud: %v", err)
		return nil, errors.New("failed to get SMC license from cloud")
	}
	token, err := GenerateTokenForNode(ctx, node, &smcLicense)
	if err != nil {
		logger.Errorf("Failed to generate SMC token for node %s: %v", node.Name, err)
		return nil, errors.New("failed to generate SMC token for node")
	}
	if token == nil || *token == "" {
		logger.Error("SMC token is empty or nil")
		return nil, errors.New("SMC token is empty or nil")
	}
	snapmirror, err = provider.SnapmirrorRelationshipCreate(params, token)
	if snapmirror != nil {
		resp := commonparams.SnapmirrorRelationship{UUID: snapmirror.UUID.String()}
		if snapmirror.Destination != nil && snapmirror.Destination.UUID != nil {
			resp.DestinationUUID = nillable.ToPointer(snapmirror.Destination.UUID.String())
		}
		return &resp, nil
	}
	return nil, err
}

func (a BackupActivity) GetObjectStore(ctx context.Context, node *models.Node, name string) (*commonparams.CloudTarget, error) {
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	// Handle both return values from CloudTargetGet
	objectStore, err := provider.CloudTargetGet(&name)
	if err != nil {
		// If there is an error, it means the object store does not exist
		return nil, errors.New("object store does not exist")
	}
	return &commonparams.CloudTarget{Name: *objectStore.Name, UUID: *objectStore.UUID}, nil
}

func (a BackupActivity) GetSnapmirror(ctx context.Context, node *models.Node, sourcePath, destinationPath string) (*commonparams.SnapmirrorRelationship, error) {
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	snapmirror, err := provider.SnapmirrorRelationshipGet(destinationPath, sourcePath)
	if err != nil {
		return nil, errors.New("failed to get snapmirror relationship: " + err.Error())
	}

	resp := commonparams.SnapmirrorRelationship{UUID: snapmirror.UUID.String()}
	if snapmirror.Destination != nil && snapmirror.Destination.UUID != nil {
		resp.DestinationUUID = nillable.ToPointer(snapmirror.Destination.UUID.String())
	}
	return &resp, nil
}

func (a BackupActivity) SnapshotCreate(ctx context.Context, node *models.Node, volumeUUID, name, comment string) (*vsa.SnapshotProviderResponse, error) {
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return provider.CreateSnapshot(vsa.CreateSnapshotParams{
		VolumeUUID: volumeUUID,
		Name:       name,
		Comment:    comment,
	})
}

func (a BackupActivity) SnapmirrorTransfer(ctx context.Context, node *models.Node, snapmirrorUUID, snapshotName string) error {
	logger := util.GetLogger(ctx)
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	// Remove this once we start using cache to store token
	smcLicense, err := GetSmcLicenseFromCloud(ctx)
	if err != nil {
		logger.Errorf("Failed to get SMC license from cloud: %v", err)
		return errors.New("failed to get SMC license from cloud")
	}
	token, err := GenerateTokenForNode(ctx, node, &smcLicense)
	if err != nil {
		logger.Errorf("Failed to generate SMC token for node %s: %v", node.Name, err)
		return errors.New("failed to generate SMC token for node")
	}
	if token == nil || *token == "" {
		logger.Error("SMC token is empty or nil")
		return errors.New("SMC token is empty or nil")
	}
	return provider.SnapmirrorRelationshipTransferCreate(snapmirrorUUID, snapshotName, token)
}

func (a BackupActivity) GetSnapmirrorTransferStatus(ctx context.Context, node *models.Node, snapmirrorUUID, snapshotName string) (string, error) {
	logger := util.GetLogger(ctx)
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return SmStatusFailed, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	rsp, err := provider.SnapmirrorRelationshipTransferGet(snapmirrorUUID, snapshotName)
	if err != nil {
		logger.Errorf("Snapmirror transfer failed with error: %v", err)
		if rsp != nil && rsp.State != nil {
			logger.Errorf("Snapmirror transfer failed with status: %s", *rsp.State)
		}
		return SmStatusFailed, err
	}
	if rsp == nil {
		return SmStatusSuccess, nil
	}
	if rsp.State != nil {
		if *rsp.State == SmStatusFailed {
			return SmStatusFailed, errors.New("Snapmirror transfer failed with status: " + SmStatusFailed)
		}
		if *rsp.State == SmStatusSuccess {
			return SmStatusSuccess, nil
		}
		if *rsp.State == SmStatusTransferring {
			return SmStatusTransferring, nil
		}
	}
	return SmStatusFailed, errors.New("Snapmirror transfer failed with status: " + *rsp.State)
}

func (a BackupActivity) DeleteBackupSnapshot(ctx context.Context, node *models.Node, snapshotUUID, volumeUUID string) error {
	if snapshotUUID == "" || volumeUUID == "" {
		return vsaerrors.WrapAsNonRetryableTemporalApplicationError(fmt.Errorf("invalid input: snapshotUUID and volumeUUID cannot be empty"))
	}
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return provider.DeleteSnapshot(snapshotUUID, volumeUUID)
}

func (a BackupActivity) SnapshotGet(ctx context.Context, node *models.Node, snapshotUUID, volumeUUID string) (*vsa.SnapshotProviderResponse, error) {
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return provider.GetSnapshot(snapshotUUID, volumeUUID)
}

func (a BackupActivity) IsVolumeDeleted(ctx context.Context, volumeUUID string) (bool, error) {
	se := a.SE
	_, err := se.GetVolume(ctx, volumeUUID)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			// If the volume is not found, it means it has been deleted
			return true, nil
		}
		return false, err
	}
	return false, nil
}

func (a BackupActivity) GetVolume(ctx context.Context, volumeUUID string) (*datamodel.Volume, error) {
	se := a.SE
	volume, err := se.GetVolume(ctx, volumeUUID)
	if err != nil {
		return nil, err
	}
	return volume, nil
}

func (a BackupActivity) GetAccountByName(ctx context.Context, accountName string) (*datamodel.Account, error) {
	se := a.SE
	account, err := se.GetAccount(ctx, accountName)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return account, nil
}

func (a BackupActivity) GetBackupVault(ctx context.Context, backupVaultUUID string) (*datamodel.BackupVault, error) {
	se := a.SE
	return se.GetBackupVault(ctx, backupVaultUUID)
}

func (a BackupActivity) GetBackupCountByVolumeUUID(ctx context.Context, volumeUUID string) (int64, error) {
	se := a.SE
	return se.BackupCountByVolumeID(ctx, volumeUUID)
}

// DeleteSnapshotFromObjectStore Enhanced DeleteSnapshotFromObjectStore with idempotency
func (a BackupActivity) DeleteSnapshotFromObjectStore(ctx context.Context, node *models.Node, objectStoreUUID, EndpointUUID, snapshotUUID string) (*vsa.OntapAsyncResponse, error) {
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	response, err := provider.SnapmirrorObjectStoreSnapshotDelete(objectStoreUUID, EndpointUUID, snapshotUUID)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	return response, nil
}

// Enhanced DeleteSnapmirror with idempotency
func (a BackupActivity) DeleteSnapmirror(ctx context.Context, node *models.Node, snapmirrorUUID string) (*vsa.OntapAsyncResponse, error) {
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	response, err := provider.SnapmirrorRelationshipDelete(snapmirrorUUID)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	return response, nil
}

// DeleteCloudEndpoint Enhanced DeleteCloudEndpoint with idempotency
func (a BackupActivity) DeleteCloudEndpoint(ctx context.Context, node *models.Node, objectStoreUUID string, EndpointUUID string) (*vsa.OntapAsyncResponse, error) {
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	response, err := provider.SnapmirrorObjectStoreEndpointDelete(objectStoreUUID, EndpointUUID)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	return response, nil
}

// Enhanced DeleteSnapshotForBackup with idempotency
func (a BackupActivity) DeleteSnapshotForBackup(ctx context.Context, node *models.Node, snapshotUUID, volumeUUID string, useExistingSnapshot bool) error {
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	logger := util.GetLogger(ctx)
	if useExistingSnapshot {
		// If using an existing snapshot, do not delete it
		logger.Warnf("Skipping deletion of snapshot with external uuid %s", snapshotUUID)
		return nil
	}

	logger.Infof("Deleting snapshot with external uuid %s", snapshotUUID)

	err = provider.DeleteSnapshot(snapshotUUID, volumeUUID)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	return nil
}

func (a BackupActivity) DeleteBackupSnapshotFromDB(ctx context.Context, backup *datamodel.Backup) error {
	logger := util.GetLogger(ctx)
	se := a.SE

	if backup.Attributes == nil {
		// If attributes are nil, do nothing
		return nil
	}

	if backup.Attributes.UseExistingSnapshot {
		// If use_existing_snapshot is True, do nothing
		return nil
	}

	volume, err := se.GetVolume(ctx, backup.VolumeUUID)
	if err != nil {
		logger.Errorf("Failed to get volume for backup %s: %v", backup.UUID, err)
		return fmt.Errorf("failed to get volume for backup %s: %v", backup.UUID, err)
	}

	if volume == nil {
		logger.Errorf("Volume for backup %s is nil", backup.UUID)
		return fmt.Errorf("volume not found for backup UUID: %s", backup.UUID)
	}

	// Get the snapshot by name and volume ID
	snapshot, err := se.GetSnapshotByNameAndVolumeId(ctx, backup.Attributes.SnapshotName, volume.AccountID, volume.ID)
	if err != nil {
		logger.Errorf("Failed to get snapshot by name %s for volume %s: %v", backup.Attributes.SnapshotName, backup.VolumeUUID, err)
		return err
	}

	// Check if the snapshot is already marked as deleted
	if snapshot.DeletedAt != nil && snapshot.DeletedAt.Valid {
		logger.Infof("Snapshot %s is already marked as deleted, skipping deletion for backup %s", snapshot.UUID, backup.UUID)
		return nil
	}

	_, err = se.DeleteSnapshot(ctx, snapshot.UUID)
	if err != nil {
		logger.Errorf("Failed to delete snapshot %s from database: %v", snapshot.UUID, err)
		originalErr := err
		markErr := a.markSnapshotAsError(ctx, snapshot, fmt.Sprintf("Failed to delete from database: %v", err))
		if markErr != nil {
			logger.Errorf("Failed to mark snapshot %s as error: %v", snapshot.Name, markErr)
		}
		return originalErr
	}

	logger.Infof("Successfully deleted snapshot %s from database for backup %s", snapshot.UUID, backup.UUID)
	return nil
}

func (j *BackupActivity) IsBackupShared(ctx context.Context, backup *datamodel.Backup) (bool, error) {
	se := j.SE
	return se.IsBackupShared(ctx, backup)
}

func (a *BackupActivity) UpdateBackup(ctx context.Context, backup *datamodel.Backup) error {
	se := a.SE
	_, err := se.UpdateBackup(ctx, backup)
	if err != nil {
		return err
	}
	return nil
}

func getObjStoreName(backupVault *datamodel.BackupVault, vol *datamodel.Volume) (string, error) {
	bucketDetails, err := getBucketDetails(backupVault, vol)
	if err != nil {
		return "", err
	}
	return bucketDetails.BucketName, nil
}

func getBucketDetails(backupVault *datamodel.BackupVault, vol *datamodel.Volume) (*datamodel.BucketDetails, error) {
	for _, bucketDetail := range backupVault.BucketDetails {
		if bucketDetail.VendorSubnetID == vol.VolumeAttributes.VendorSubnetID && bucketDetail.BucketName != "" {
			return bucketDetail, nil
		}
	}
	return nil, vsaerrors.ExtractCustomError(fmt.Errorf("no matching bucket details found for volume %s in backup vault %s", vol.Name, backupVault.Name))
}

func getSmSourcePath(volume *datamodel.Volume) string {
	return fmt.Sprintf("%s:%s", volume.Svm.Name, volume.Name)
}

func getSmDestinationPath(backupVault *datamodel.BackupVault, volume *datamodel.Volume) (string, error) {
	objStoreName, err := getObjStoreName(backupVault, volume)
	if err != nil {
		return "", fmt.Errorf("failed to get object store name: %w", err)
	}
	return fmt.Sprintf("%s:/objstore/%s", objStoreName, volume.UUID), nil
}

func GetSmSourcePath(volume *datamodel.Volume) string {
	return fmt.Sprintf("%s:%s", volume.Svm.Name, volume.Name)
}

func GetObjStoreName(backupVault *datamodel.BackupVault, vol *datamodel.Volume) (string, error) {
	bucketDetails, err := GetBucketDetails(backupVault, vol)
	if err != nil {
		return "", err
	}
	return bucketDetails.BucketName, nil
}

func GetSmDestinationPath(backupVault *datamodel.BackupVault, volume *datamodel.Volume) (string, error) {
	objStoreName, err := GetObjStoreName(backupVault, volume)
	if err != nil {
		return "", fmt.Errorf("failed to get object store name: %w", err)
	}
	return fmt.Sprintf("%s:/objstore/%s", objStoreName, volume.UUID), nil
}

func GetBucketDetails(backupVault *datamodel.BackupVault, vol *datamodel.Volume) (*datamodel.BucketDetails, error) {
	if vol.VolumeAttributes == nil {
		return nil, vsaerrors.ExtractCustomError(fmt.Errorf("volume %s has no volume attributes", vol.Name))
	}

	for _, bucketDetail := range backupVault.BucketDetails {
		if bucketDetail.VendorSubnetID == vol.VolumeAttributes.VendorSubnetID && bucketDetail.BucketName != "" {
			return bucketDetail, nil
		}
	}
	return nil, vsaerrors.ExtractCustomError(fmt.Errorf("no matching bucket details found for volume %s in backup vault %s", vol.Name, backupVault.Name))
}

func GetObjStoreNameFromBackup(backupVault *datamodel.BackupVault, backup *datamodel.Backup) (string, error) {
	bucketDetails, err := GetBucketDetailsFromBackup(backupVault, backup)
	if err != nil {
		return "", err
	}
	return bucketDetails.BucketName, nil
}

func GetBucketDetailsFromBackup(backupVault *datamodel.BackupVault, backup *datamodel.Backup) (*datamodel.BucketDetails, error) {
	if backup.Attributes == nil {
		return nil, vsaerrors.ExtractCustomError(fmt.Errorf("backup %s has no attributes", backup.Name))
	}

	for _, bucketDetail := range backupVault.BucketDetails {
		if bucketDetail.BucketName != "" && bucketDetail.BucketName == backup.Attributes.BucketName {
			return bucketDetail, nil
		}
	}
	return nil, vsaerrors.ExtractCustomError(fmt.Errorf("no matching bucket details found for backup %s", backup.Name))
}

func GetSmSourcePathForRestore(backupVault *datamodel.BackupVault, backup *datamodel.Backup) (string, error) {
	objStoreName, err := GetObjStoreNameFromBackup(backupVault, backup)
	if err != nil {
		return "", fmt.Errorf("failed to get object store name: %w", err)
	}
	return fmt.Sprintf("%s:/objstore/%s", objStoreName, backup.Attributes.SnapshotID), nil
}

// Activity methods for workflow execution

func (a BackupActivity) GetObjStoreNameActivity(ctx context.Context, backupVault *datamodel.BackupVault, volume *datamodel.Volume) (string, error) {
	return GetObjStoreName(backupVault, volume)
}

func (a BackupActivity) GetBucketDetailsActivity(ctx context.Context, backupVault *datamodel.BackupVault, volume *datamodel.Volume) (*datamodel.BucketDetails, error) {
	return GetBucketDetails(backupVault, volume)
}

func (a BackupActivity) GetObjStoreNameFromBackupActivity(ctx context.Context, backupVault *datamodel.BackupVault, backup *datamodel.Backup) (string, error) {
	return GetObjStoreNameFromBackup(backupVault, backup)
}

func (a BackupActivity) GetBucketDetailsFromBackupActivity(ctx context.Context, backupVault *datamodel.BackupVault, backup *datamodel.Backup) (*datamodel.BucketDetails, error) {
	return GetBucketDetailsFromBackup(backupVault, backup)
}

func (a BackupActivity) GetSmSourcePathActivity(ctx context.Context, volume *datamodel.Volume) (string, error) {
	return GetSmSourcePath(volume), nil
}

func (a BackupActivity) GetSmDestinationPathActivity(ctx context.Context, backupVault *datamodel.BackupVault, volume *datamodel.Volume) (string, error) {
	return GetSmDestinationPath(backupVault, volume)
}

func (a BackupActivity) GetSmSourcePathForRestoreActivity(ctx context.Context, backupVault *datamodel.BackupVault, backup *datamodel.Backup) (string, error) {
	return GetSmSourcePathForRestore(backupVault, backup)
}

// CleanupOldAdhocBackupSnapshotsActivity cleans up older adhoc-backup snapshots for a volume, keeping only the latest one
func (a BackupActivity) CleanupOldAdhocBackupSnapshotsActivity(ctx context.Context, volume *datamodel.Volume, node *models.Node) error {
	logger := util.GetLogger(ctx)

	// Get all adhoc-backup snapshots for this volume, ordered by creation time (newest first)
	snapshots, err := a.SE.GetSnapshotsByTypeAndVolumeID(ctx, SnapshotTypeBackup, volume.ID)
	if err != nil {
		logger.Errorf("Failed to get adhoc-backup snapshots for volume %s: %v", volume.Name, err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// If we have more than 1 snapshot, delete the older ones (skip the first one which is the latest)
	if len(snapshots) > 1 {
		logger.Infof("Found %d adhoc-backup snapshots for volume %s, cleaning up %d older snapshots",
			len(snapshots), volume.Name, len(snapshots)-1)

		// Process older snapshots (skip the first one which is the latest)
		for i := 1; i < len(snapshots); i++ {
			snapshot := snapshots[i]
			logger.Infof("Deleting older adhoc-backup snapshot %s for volume %s", snapshot.Name, volume.Name)

			// Try to delete the snapshot from ONTAP first
			if snapshot.SnapshotAttributes != nil && snapshot.SnapshotAttributes.ExternalUUID != "" && volume.VolumeAttributes != nil && volume.VolumeAttributes.ExternalUUID != "" {
				err := a.DeleteBackupSnapshot(ctx, node, snapshot.SnapshotAttributes.ExternalUUID, volume.VolumeAttributes.ExternalUUID)
				if err != nil {
					logger.Errorf("Failed to delete snapshot %s from ONTAP: %v", snapshot.Name, err)
					// Mark snapshot as error state instead of failing the entire operation
					err = a.markSnapshotAsError(ctx, snapshot, fmt.Sprintf("Failed to delete from ONTAP: %v", err))
					if err != nil {
						logger.Errorf("Failed to mark snapshot %s as error: %v", snapshot.Name, err)
					}
					continue
				}
			}

			// Delete the snapshot from database
			_, err := a.SE.DeleteSnapshot(ctx, snapshot.UUID)
			if err != nil {
				logger.Errorf("Failed to delete snapshot %s from database: %v", snapshot.Name, err)
				// Mark snapshot as error state instead of failing the entire operation
				err = a.markSnapshotAsError(ctx, snapshot, fmt.Sprintf("Failed to delete from database: %v", err))
				if err != nil {
					logger.Errorf("Failed to mark snapshot %s as error: %v", snapshot.Name, err)
				}
				continue
			}

			logger.Infof("Successfully deleted older adhoc-backup snapshot %s for volume %s", snapshot.Name, volume.Name)
		}
	} else {
		logger.Infof("No cleanup needed for volume %s - found %d adhoc-backup snapshots", volume.Name, len(snapshots))
	}

	return nil
}

// markSnapshotAsError marks a snapshot as error state
func (a BackupActivity) markSnapshotAsError(ctx context.Context, snapshot *datamodel.Snapshot, errorMessage string) error {
	snapshot.State = models.LifeCycleStateError
	snapshot.StateDetails = errorMessage
	_, err := a.SE.UpdateSnapshot(ctx, snapshot)
	return err
}

// HydrateSnapshotToCCFEActivity hydrates the created snapshot to CCFE
func (a BackupActivity) HydrateSnapshotToCCFEActivity(ctx context.Context, snapshot *datamodel.Snapshot, volumeName, location, projectId string) error {
	logger := util.GetLogger(ctx)

	if !hydrationEnabled {
		logger.Info("Hydration is disabled, skipping snapshot hydration to CCFE")
		return nil
	}

	if snapshot == nil {
		logger.Warn("No database snapshot found, skipping hydration")
		return nil
	}

	// Generate callback token
	token, err := auth.GenerateCallbackToken(ctx)
	if err != nil {
		logger.Errorf("Failed to generate callback token for snapshot hydration: %v", err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Convert snapshot to GCP hydrate snapshot object
	gcpSnapshot := ConvertSnapshotToGCPHydrateSnapshot(*snapshot)

	// Create request
	request := models.Request{Snapshot: &gcpSnapshot}
	requests := []models.Request{request}

	// Hydrate to CCFE using the existing batch hydration function
	err = commonparams.BatchHydrateCreatedSnapshots(ctx, logger, requests, volumeName, location, projectId, token)
	if err != nil {
		logger.Errorf("Failed to hydrate snapshot to CCFE: %v", err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	logger.Infof("Successfully hydrated snapshot %s to CCFE", snapshot.Name)
	return nil
}

// ConvertSnapshotToGCPHydrateSnapshot converts a datamodel.Snapshot to a GCP-compatible snapshot object
func ConvertSnapshotToGCPHydrateSnapshot(snapshot datamodel.Snapshot) models.HydrateSnapshot {
	gcpSnapshot := models.HydrateSnapshot{
		ResourceId:   utils.RenameSnapshotName(snapshot.Name),
		SnapshotId:   snapshot.UUID,
		State:        commonparams.MapStateToGcpState(snapshot.State),
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

// HydrateSnapshotDeletionToCCFEActivity hydrates the deleted snapshot to CCFE
func (a BackupActivity) HydrateSnapshotDeletionToCCFEActivity(ctx context.Context, snapshot *datamodel.Snapshot, volumeName, location, projectId string) error {
	logger := util.GetLogger(ctx)

	if !hydrationEnabled {
		logger.Info("Hydration is disabled, skipping snapshot deletion hydration to CCFE")
		return nil
	}

	if snapshot == nil {
		logger.Warn("No database snapshot found, skipping deletion hydration")
		return nil
	}

	// Generate callback token
	token, err := auth.GenerateCallbackToken(ctx)
	if err != nil {
		logger.Errorf("Failed to generate callback token for snapshot deletion hydration: %v", err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Convert snapshot to GCP hydrate snapshot object for deletion
	gcpSnapshot := ConvertSnapshotToGCPHydrateSnapshot(*snapshot)

	// Create request for deletion
	request := models.Request{Snapshot: &gcpSnapshot}
	requests := []models.Request{request}

	// Hydrate deletion to CCFE using the existing batch hydration function
	err = commonparams.BatchHydrateDeletedSnapshots(ctx, logger, requests, volumeName, location, projectId, token)
	if err != nil {
		logger.Errorf("Failed to hydrate snapshot deletion to CCFE: %v", err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	logger.Infof("Successfully hydrated snapshot deletion %s to CCFE", snapshot.Name)
	return nil
}

// IsLatestBackupAnyStateActivity checks if a backup is the latest for its volume regardless of state
func (b *BackupActivity) IsLatestBackupAnyStateActivity(ctx context.Context, backupUUID, volumeUUID string) (bool, error) {
	logger := util.GetLogger(ctx)

	isLatest, err := b.SE.IsLatestBackupAnyState(ctx, backupUUID, volumeUUID)
	if err != nil {
		logger.Errorf("Failed to check if backup is latest: %v", err)
		return false, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	return isLatest, nil
}

// PollTransferStatusWithHistoryCheckActivity polls transfer status with event history monitoring and ContinueAsNew capability
// This activity combines CheckTransferStatusActivity with event history limit checking
// and triggers ContinueAsNew when history limits are reached
func (b *BackupActivity) PollTransferStatusWithHistoryCheckActivity(ctx context.Context, input *PollTransferStatusInput, currentTime time.Time) (*PollTransferStatusOutput, error) {
	logger := util.GetLogger(ctx)

	// Check transfer status using existing GetSnapmirrorTransferStatus logic
	// This works for both backup and restore workflows
	status, err := b.GetSnapmirrorTransferStatus(ctx, input.Node, input.SnapmirrorRelationship.UUID, input.SnapshotName)
	if err != nil {
		return nil, err
	}
	logger.Info("Polled snapmirror transfer status", "snapshotName", input.SnapshotName, "status", status)

	// Update the context with the new status
	input.BackupActivitiesContext.TransferStatus = status

	// Check if we need to trigger ContinueAsNew based on event history
	shouldContinueAsNew := false
	continueAsNewReason := ""

	// Check if event history limit is reached
	if input.EventHistoryCount >= EventHistorySafetyThreshold {
		shouldContinueAsNew = true
		continueAsNewReason = "Event history limit reached"
	}

	// Check if transfer is complete
	transferComplete := false
	switch status {
	case SmStatusSuccess:
		if input.BackupActivitiesContext.ScheduledBackupParams != nil {
			for _, bkp := range input.BackupActivitiesContext.ScheduledBackupParams.Backups {
				bkp.Attributes.SnapshotCreationTime = currentTime.String()
			}
		} else {
			input.BackupActivitiesContext.BackupWorkflowInit.Backup.Attributes.SnapshotCreationTime = currentTime.String()
		}
		transferComplete = true
		logger.Info("Transfer completed successfully", "snapshotName", input.SnapshotName)
	case SmStatusFailed:
		return nil, vsaerrors.WrapAsNonRetryableTemporalApplicationError(fmt.Errorf("snapmirror transfer failed for snapshot %s with status: %s", input.SnapshotName, status))
	}

	return &PollTransferStatusOutput{
		BackupActivitiesContext: input.BackupActivitiesContext,
		TransferComplete:        transferComplete,
		ShouldContinueAsNew:     shouldContinueAsNew,
		ContinueAsNewReason:     continueAsNewReason,
		NextWaitTime:            input.NextWaitTime,
	}, nil
}

// CreateBackupMetadataIfFirstBackupActivity creates a BackupMetadata entry if this is the first backup for the volume
func (b *BackupActivity) CreateBackupMetadataIfFirstBackupActivity(ctx context.Context, volume *datamodel.Volume) error {
	logger := util.GetLogger(ctx)

	// Get all backups for this volume
	backups, err := b.SE.GetBackupsByVolumeUUID(ctx, volume.UUID)
	if err != nil {
		logger.Errorf("Failed to get backups for volume %s: %v", volume.UUID, err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Check if this is the first backup (count should be 1 since we just created one)
	if len(backups) == 1 {
		logger.Infof("This is the first backup for volume %s, creating BackupMetadata entry", volume.UUID)

		// Extract labels from volume
		var labels *datamodel.JSONB
		if volume.VolumeAttributes != nil && volume.VolumeAttributes.Labels != nil {
			labels = volume.VolumeAttributes.Labels
		} else {
			// If no labels exist, create empty JSONB
			labels = &datamodel.JSONB{}
		}

		// Create BackupMetadata entry with volume labels
		backupMetadata := &datamodel.BackupMetadata{
			VolumeUUID: volume.UUID,
			Labels:     labels,
		}

		_, err := b.SE.CreateBackupMetadata(ctx, backupMetadata)
		if err != nil {
			logger.Errorf("Failed to create BackupMetadata for volume %s: %v", volume.UUID, err)
			return vsaerrors.WrapAsTemporalApplicationError(err)
		}

		logger.Infof("Successfully created BackupMetadata entry for volume %s with labels", volume.UUID)
	} else {
		logger.Infof("Volume %s already has %d backups, skipping BackupMetadata creation", volume.UUID, len(backups))
	}

	return nil
}

// DeleteBackupMetadataIfLastBackupActivity deletes a BackupMetadata entry if this is the last backup for the volume
func (b *BackupActivity) DeleteBackupMetadataIfLastBackupActivity(ctx context.Context, volumeUUID string) error {
	logger := util.GetLogger(ctx)

	// Get all backups for this volume
	backups, err := b.SE.GetBackupsByVolumeUUID(ctx, volumeUUID)
	if err != nil {
		logger.Errorf("Failed to get backups for volume %s: %v", volumeUUID, err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Check if this is the last backup (count should be 0 since we just deleted one)
	if len(backups) == 0 {
		logger.Infof("This was the last backup for volume %s, deleting BackupMetadata entry", volumeUUID)

		// Delete BackupMetadata entry
		err := b.SE.DeleteBackupMetadata(ctx, volumeUUID)
		if err != nil {
			logger.Errorf("Failed to delete BackupMetadata for volume %s: %v", volumeUUID, err)
			return vsaerrors.WrapAsTemporalApplicationError(err)
		}

		logger.Infof("Successfully deleted BackupMetadata entry for volume %s", volumeUUID)
	} else {
		logger.Infof("Volume %s still has %d backups, keeping BackupMetadata entry", volumeUUID, len(backups))
	}

	return nil
}

// UpdateBackupMetadataIfExistsActivity updates BackupMetadata labels if an entry exists for the volume
func (b *BackupActivity) UpdateBackupMetadataIfExistsActivity(ctx context.Context, volume *datamodel.Volume) error {
	logger := util.GetLogger(ctx)

	// Check if BackupMetadata entry exists for this volume
	backupMetadata, err := b.SE.GetBackupMetadataByVolumeUUID(ctx, volume.UUID)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			// No BackupMetadata entry exists yet - this is expected if no backups have been created
			logger.Infof("No BackupMetadata entry found for volume %s - will be created when first backup is made", volume.UUID)
			return nil
		}
		logger.Errorf("Failed to get BackupMetadata for volume %s: %v", volume.UUID, err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Update the labels in the existing BackupMetadata entry
	if volume.VolumeAttributes != nil && volume.VolumeAttributes.Labels != nil {
		backupMetadata.Labels = volume.VolumeAttributes.Labels
	} else {
		// If no labels exist, set to empty JSONB
		backupMetadata.Labels = &datamodel.JSONB{}
	}
	_, err = b.SE.UpdateBackupMetadata(ctx, backupMetadata)
	if err != nil {
		logger.Errorf("Failed to update BackupMetadata for volume %s: %v", volume.UUID, err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	logger.Infof("Successfully updated BackupMetadata labels for volume %s", volume.UUID)
	return nil
}

// DeleteRemoteBackupFromVCPActivity deletes the Backup from the remote region using Google Proxy Client
func (a *BackupActivity) DeleteRemoteBackupFromVCPActivity(ctx context.Context, backupUUID, backupVaultUUID, projectNumber, region string) error {
	logger := util.GetLogger(ctx)
	basePath, jwtToken, err := commonparams.GetRemoteRegionConfig(region, projectNumber)
	if err != nil {
		logger.Error("Failed to get remote region configuration", "region", region, "error", err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	googleProxyClient := googleproxyclient.GetGProxyClient(basePath, jwtToken, logger)
	correlationID := utils.GetCoRelationIDFromContext(ctx)

	params := googleproxyclient.V1betaInternalDeleteBackupUnderBackupVaultParams{
		ProjectNumber:  projectNumber,
		LocationId:     region,
		BackupVaultId:  backupVaultUUID,
		BackupId:       backupUUID,
		XCorrelationID: googleproxyclient.NewOptString(correlationID),
	}

	_, err = googleProxyClient.Invoker.V1betaInternalDeleteBackupUnderBackupVault(ctx, params)
	if err != nil {
		logger.Errorf("Failed to delete remote Backup: %v, region=%s, backupVaultID=%s, backupID=%s", err, region, backupVaultUUID, backupUUID)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	logger.Infof("Successfully deleted remote Backup, backupID=%s, region=%s", backupUUID, region)
	return nil
}

func (a *BackupActivity) UpdateBackupRestoreCount(ctx context.Context, backupVaultUUID, backupUUID, accountName, operation string) error {
	se := a.SE
	logger := util.GetLogger(ctx)

	// Fetching latest backup so we have the most recent restore count
	backup, err := se.GetBackup(ctx, backupVaultUUID, backupUUID, accountName)
	if err != nil {
		logger.Errorf("Failed to get backup %s: %v", backupUUID, err)
	}

	if operation == BackupRestoreCountIncrement {
		backup.Attributes.RestoreVolumeCount++
	} else {
		backup.Attributes.RestoreVolumeCount--
	}
	updates := map[string]interface{}{
		"attributes": backup.Attributes,
	}
	err = se.UpdateBackupFields(ctx, backupUUID, updates)
	if err != nil {
		logger.Errorf("Failed to update backup %s: %v", backupUUID, err)
		return err
	}
	logger.Infof("Successfully updated backup restore count to %d for backup %s", backup.Attributes.RestoreVolumeCount, backupUUID)
	return nil
}

// CreateRemoteBackupFromVCPActivity creates the Backup in the remote region using Google Proxy Client
// This is done after UpdateBackupSizeActivity to ensure ExternalUUID is set
func (a *BackupActivity) CreateRemoteBackupFromVCPActivity(ctx context.Context, backupActivitiesContext *BackupActivitiesContext) error {
	// Check if this is a cross-region backup
	if backupActivitiesContext.BackupWorkflowInit.BackupVault.BackupVaultType != "CROSS_REGION" ||
		backupActivitiesContext.BackupWorkflowInit.BackupVault.BackupRegionName == nil {
		// Not a cross-region backup, skip
		return nil
	}

	logger := util.GetLogger(ctx)
	backup := backupActivitiesContext.BackupWorkflowInit.Backup
	backupVault := backupActivitiesContext.BackupWorkflowInit.BackupVault
	projectNumber := backupActivitiesContext.BackupWorkflowInit.Volume.Account.Name
	region := *backupActivitiesContext.BackupWorkflowInit.BackupVault.BackupRegionName

	basePath, jwtToken, err := commonparams.GetRemoteRegionConfig(region, projectNumber)
	if err != nil {
		logger.Error("Failed to get remote region configuration", "region", region, "error", err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	googleProxyClient := googleproxyclient.GetGProxyClient(basePath, jwtToken, logger)
	correlationID := utils.GetCoRelationIDFromContext(ctx)

	// Convert backup to InternalBackupCreate format for the request
	// Internal API requires volume and snapshot information as they are not available in remote region DB
	// backupUUID is the ExternalUUID for cross-region backups - use backup.UUID (internal UUID from source region)
	backupCreate := googleproxyclient.InternalBackupCreateV1beta{
		ResourceId: backup.Name,
		BackupUUID: backup.UUID, // This becomes ExternalUUID in the remote region
		VolumeId:   backup.VolumeUUID,
		Description: googleproxyclient.OptString{
			Value: backup.Description,
			Set:   backup.Description != "",
		},
	}

	// Include volume information (required for cross-region)
	if backup.Attributes != nil {
		backupCreate.VolumeName = backup.Attributes.VolumeName
		if len(backup.Attributes.Protocols) > 0 {
			protocols := make([]googleproxyclient.InternalBackupCreateV1betaProtocolsItem, len(backup.Attributes.Protocols))
			for i, p := range backup.Attributes.Protocols {
				protocols[i] = googleproxyclient.InternalBackupCreateV1betaProtocolsItem(p)
			}
			backupCreate.Protocols = protocols
		}
		// Include useExistingSnapshot flag
		backupCreate.UseExistingSnapshot = googleproxyclient.NewOptBool(backup.Attributes.UseExistingSnapshot)
		// Include snapshot information if available (always include all details)
		if backup.Attributes.SnapshotID != "" {
			backupCreate.SnapshotId = googleproxyclient.NewOptString(backup.Attributes.SnapshotID)
		}
		if backup.Attributes.SnapshotName != "" {
			backupCreate.SnapshotName = googleproxyclient.NewOptString(backup.Attributes.SnapshotName)
		}
		// Include backup attributes for cross-region operations
		if backup.Attributes.BucketName != "" {
			backupCreate.BucketName = googleproxyclient.NewOptString(backup.Attributes.BucketName)
		}
		if backup.Attributes.EndpointUUID != "" {
			backupCreate.EndpointUuid = googleproxyclient.NewOptString(backup.Attributes.EndpointUUID)
		}
		backupCreate.IsRegionalHa = googleproxyclient.NewOptBool(backup.Attributes.IsRegionalHA)
		if backup.Attributes.CompletionTime != "" {
			// Parse the completion time string to time.Time for OptDateTime
			if completionTime, err := time.Parse(time.RFC3339, backup.Attributes.CompletionTime); err == nil {
				backupCreate.CompletionTime = googleproxyclient.NewOptDateTime(completionTime)
			}
		}
		if backup.Attributes.BackupPolicyName != "" {
			backupCreate.BackupPolicyName = googleproxyclient.NewOptString(backup.Attributes.BackupPolicyName)
		}
		if backup.Attributes.OntapVolumeStyle != "" {
			backupCreate.OntapVolumeStyle = googleproxyclient.NewOptString(backup.Attributes.OntapVolumeStyle)
		}
		if backup.Attributes.SourceVolumeZone != "" {
			backupCreate.SourceVolumeZone = googleproxyclient.NewOptString(backup.Attributes.SourceVolumeZone)
		}
		if backup.Attributes.ServiceAccountName != "" {
			backupCreate.ServiceAccountName = googleproxyclient.NewOptString(backup.Attributes.ServiceAccountName)
		}
		if backup.Attributes.SnapshotCreationTime != "" {
			// Parse the snapshot creation time string to time.Time for OptDateTime
			if snapshotCreationTime, err := time.Parse(time.RFC3339, backup.Attributes.SnapshotCreationTime); err == nil {
				backupCreate.SnapshotCreationTime = googleproxyclient.NewOptDateTime(snapshotCreationTime)
			}
		}
		if backup.Attributes.ConstituentCountOfBackup > 0 {
			backupCreate.ConstituentCountOfBackup = googleproxyclient.NewOptInt32(backup.Attributes.ConstituentCountOfBackup)
		}
	}

	params := googleproxyclient.V1betaInternalCreateBackupParams{
		ProjectNumber:  projectNumber,
		LocationId:     region,
		BackupVaultId:  backupVault.UUID,
		XCorrelationID: googleproxyclient.NewOptString(correlationID),
	}

	res, err := googleProxyClient.Invoker.V1betaInternalCreateBackup(ctx, &backupCreate, params)
	if err != nil {
		logger.Errorf("Failed to create remote Backup: %v, region=%s, backupVaultID=%s", err, region, backupVault.ExternalUUID)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Check if the response indicates success
	if res == nil {
		logger.Error("Unexpected nil response from remote Backup creation", "backupName", backup.Name)
		return vsaerrors.WrapAsTemporalApplicationError(errors.NewNotFoundErr("remote backup", &backup.Name))
	}

	logger.Infof("Successfully created remote Backup, backupName=%s, region=%s", backup.Name, region)
	return nil
}

// UpdateRemoteBackupFromVCPActivity updates the Backup in the remote region using Google Proxy Client
func (a *BackupActivity) UpdateRemoteBackupFromVCPActivity(ctx context.Context, backup *datamodel.Backup) error {
	logger := util.GetLogger(ctx)
	// Check if this is a cross-region backup
	if backup.BackupVault == nil || backup.BackupVault.BackupVaultType != "CROSS_REGION" || backup.BackupVault.BackupRegionName == nil {
		// Not a cross-region backup or missing required fields, skip
		logger.Infof("Skipping remote backup update for non-cross-region backup, backupID=%s", backup.UUID)
		return nil
	}
	backupVault, err := a.GetBackupVault(ctx, backup.BackupVault.UUID)
	if err != nil {
		logger.Errorf("Failed to get backup vault: %v, backupID=%s", err, backup.UUID)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	region := *backupVault.BackupRegionName

	// Get account name from backup vault
	var projectNumber string
	if backupVault.Account != nil {
		projectNumber = backupVault.Account.Name
	} else if backupVault.AccountVendorID != "" {
		// If account is not loaded, use AccountVendorID (project number)
		projectNumber = backupVault.AccountVendorID
	} else {
		logger.Warnf("BackupVault account not loaded and AccountVendorID not available, cannot update remote backup", "backupID", backup.UUID)
		return nil
	}

	basePath, jwtToken, err := commonparams.GetRemoteRegionConfig(region, projectNumber)
	if err != nil {
		logger.Error("Failed to get remote region configuration", "region", region, "error", err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	googleProxyClient := googleproxyclient.GetGProxyClient(basePath, jwtToken, logger)
	correlationID := utils.GetCoRelationIDFromContext(ctx)

	backupUpdate := googleproxyclient.BackupUpdateV1beta{
		Description: backup.Description,
	}

	params := googleproxyclient.V1betaInternalUpdateBackupParams{
		ProjectNumber:  projectNumber,
		LocationId:     region,
		BackupVaultId:  backupVault.UUID,
		BackupId:       backup.UUID,
		XCorrelationID: googleproxyclient.NewOptString(correlationID),
	}

	res, err := googleProxyClient.Invoker.V1betaInternalUpdateBackup(ctx, &backupUpdate, params)
	if err != nil {
		logger.Errorf("Failed to update remote Backup: %v, region=%s, backupVaultID=%s, backupID=%s", err, region, backupVault.ExternalUUID, backup.ExternalUUID)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Check if the response indicates success
	if res == nil {
		logger.Error("Unexpected nil response from remote Backup update", "backupID", backup.ExternalUUID)
		return vsaerrors.WrapAsTemporalApplicationError(errors.NewNotFoundErr("remote backup", &backup.ExternalUUID))
	}

	logger.Infof("Successfully updated remote Backup, backupID=%s, region=%s", backup.ExternalUUID, region)
	return nil
}
