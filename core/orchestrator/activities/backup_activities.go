package activities

import (
	"context"
	"fmt"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"gorm.io/gorm"
)

const (
	BackupComment        = "VCP-Backup"
	SmStatusTransferring = "transferring"
	SmStatusSuccess      = "success"
	SmStatusFailed       = "failed"
)

type BackupActivity struct {
	SE database.Storage
}

type BackupWorkflowInput struct {
	Backup      *datamodel.Backup
	BackupVault *datamodel.BackupVault
	Volume      *datamodel.Volume
}
type BackupActivitiesContext struct {
	// Initial inputs
	BackupWorkflowInit *BackupWorkflowInput

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
	backup.StateDetails = models.LifeCycleStateCreationErrorDetails
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
		Type:               SnapshotTypeBackupAdhoc,
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
		if rsp != nil && rsp.State != nil {
			logger.Errorf("Snapmirror transfer failed with backupActivitiesContext: %s, error: %v", *rsp.State, err)
		}
		return SmStatusFailed, err
	}
	if rsp == nil {
		return SmStatusSuccess, nil
	}
	if rsp.State != nil {
		if *rsp.State == SmStatusFailed {
			return SmStatusFailed, errors.New("Snapmirror transfer failed with backupActivitiesContext: " + SmStatusFailed)
		}
		if *rsp.State == SmStatusSuccess {
			return SmStatusSuccess, err
		}
		if *rsp.State == SmStatusTransferring {
			return SmStatusTransferring, nil
		}
	}
	return SmStatusFailed, errors.New("Snapmirror transfer failed with backupActivitiesContext: " + *rsp.State)
}

func (a BackupActivity) DeleteBackupSnapshot(ctx context.Context, node *models.Node, snapshotUUID, volumeUUID string) error {
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
func (a BackupActivity) DeleteSnapshotForBackup(ctx context.Context, node *models.Node, snapshotUUID, volumeUUID string) error {
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	err = provider.DeleteSnapshot(snapshotUUID, volumeUUID)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

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
