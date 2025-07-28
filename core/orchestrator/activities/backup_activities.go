package activities

import (
	"context"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	SmStatusTransferring = "transferring"
	SmStatusSuccess      = "success"
	SmStatusFailed       = "failed"
)

type BackupActivity struct {
	SE database.Storage
}

func (a BackupActivity) CreateBackup(ctx context.Context, backup *datamodel.Backup) (*datamodel.Backup, error) {
	se := a.SE
	return se.CreateBackup(ctx, backup)
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
	backup.State = models.LifeCycleStateAvailable
	backup.StateDetails = models.LifeCycleStateAvailableDetails
	_, err := se.UpdateBackupState(ctx, backup)
	return err
}

func (a BackupActivity) GetOrCreateObjectStore(ctx context.Context, node *models.Node, name, containerName string) (*commonparams.CloudTarget, error) {
	provider, err := GetProviderByNode(ctx, node)
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
		return &commonparams.CloudTarget{Name: *objectStore.Name}, nil
	}

	return nil, errors.New("failed to get or create object store")
}

func (a BackupActivity) SnapmirrorGetorCreate(ctx context.Context, node *models.Node, params *commonparams.SnapmirrorRelationshipParams) (*commonparams.SnapmirrorRelationship, error) {
	logger := util.GetLogger(ctx)
	provider, err := GetProviderByNode(ctx, node)
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
	provider, err := GetProviderByNode(ctx, node)
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
	provider, err := GetProviderByNode(ctx, node)
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
	provider, err := GetProviderByNode(ctx, node)
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
	provider, err := GetProviderByNode(ctx, node)
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
	provider, err := GetProviderByNode(ctx, node)
	if err != nil {
		return SmStatusFailed, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	rsp, err := provider.SnapmirrorRelationshipTransferGet(snapmirrorUUID, snapshotName)
	if err != nil {
		if rsp != nil && rsp.State != nil {
			logger.Errorf("Snapmirror transfer failed with state: %s, error: %v", *rsp.State, err)
		}
		return SmStatusFailed, err
	}
	if rsp == nil {
		return SmStatusSuccess, nil
	}
	if rsp.State != nil {
		if *rsp.State == SmStatusFailed {
			return SmStatusFailed, errors.New("Snapmirror transfer failed with state: " + SmStatusFailed)
		}
		if *rsp.State == SmStatusSuccess {
			return SmStatusSuccess, err
		}
		if *rsp.State == SmStatusTransferring {
			return SmStatusTransferring, nil
		}
	}
	return SmStatusFailed, errors.New("Snapmirror transfer failed with state: " + *rsp.State)
}

func (a BackupActivity) DeleteBackupSnapshot(ctx context.Context, node *models.Node, snapshotUUID, volumeUUID string) error {
	provider, err := GetProviderByNode(ctx, node)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return provider.DeleteSnapshot(snapshotUUID, volumeUUID)
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

func (a BackupActivity) GetBackupVault(ctx context.Context, backupVaultUUID string) (*datamodel.BackupVault, error) {
	se := a.SE
	return se.GetBackupVault(ctx, backupVaultUUID)
}

func (a BackupActivity) GetBackupCountByVolumeUUID(ctx context.Context, volumeUUID string) (int64, error) {
	se := a.SE
	return se.BackupCountByVolumeID(ctx, volumeUUID)
}

func (a BackupActivity) DeleteSnapshotFromObjectStore(ctx context.Context, node *models.Node, objectStoreUUID, EndpointUUID, snapshotUUID string) (*vsa.OntapAsyncResponse, error) {
	provider, err := GetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return provider.SnapmirrorObjectStoreSnapshotDelete(objectStoreUUID, EndpointUUID, snapshotUUID)
}

func (a BackupActivity) DeleteSnapmirror(ctx context.Context, node *models.Node, snapmirrorUUID string) (*vsa.OntapAsyncResponse, error) {
	provider, err := GetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return provider.SnapmirrorRelationshipDelete(snapmirrorUUID)
}

func (a BackupActivity) DeleteCloudEndpoint(ctx context.Context, node *models.Node, objectStoreUUID string, EndpointUUID string) (*vsa.OntapAsyncResponse, error) {
	provider, err := GetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return provider.SnapmirrorObjectStoreEndpointDelete(objectStoreUUID, EndpointUUID)
}

func (a BackupActivity) DeleteSnapshotForBackup(ctx context.Context, node *models.Node, snapshotUUID, volumeUUID string) error {
	provider, err := GetProviderByNode(ctx, node)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return provider.DeleteSnapshot(snapshotUUID, volumeUUID)
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

// func (a BackupActivity) CreateHmacKeys(ctx context.Context, params *commonparams.HmacKeyCreateParams, gcpService hyperscaler.GoogleServices) (hmacKeys *commonparams.HmacKeys, err error) {
//	err = gcpService.InitializeClients()
//	if err != nil || !gcpService.IsAdminClientInitialized() {
//		gcpService.GetLogger().Debug("Initialisation of service failed")
//		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPClientInitializationError, errors.New("initialisation of Google GCP service failed"))
//	}
//
//	accessKey, secretKey, err := gcpService.CreateHmacKey(params.ProjectNumber, params.ServiceAccount)
//	if err != nil {
//		return nil, err
//	}
//	if accessKey == nil || secretKey == nil {
//		return nil, errors.New("accessKey or secretKey is nil")
//	}
//	return &commonparams.HmacKeys{
//		AccessKey: *accessKey,
//		SecretKey: *secretKey,
//	}, nil
// }
//
// func (a BackupActivity) DeleteHmacKeys(ctx context.Context, projectNumber, accessKey, ServiceAccount string, gcpService hyperscaler.GoogleServices) error {
//	err := gcpService.InitializeClients()
//	if err != nil || !gcpService.IsAdminClientInitialized() {
//		gcpService.GetLogger().Debug("Initialisation of service failed")
//		return vsaerrors.NewVCPError(vsaerrors.ErrGCPClientInitializationError, errors.New("initialisation of Google GCP service failed"))
//	}
//
//	err = gcpService.InitializeClients()
//	if err != nil || !gcpService.IsAdminClientInitialized() {
//		gcpService.GetLogger().Debug("Initialisation of service failed")
//		return vsaerrors.NewVCPError(vsaerrors.ErrGCPClientInitializationError, errors.New("initialisation of Google GCP service failed"))
//	}
//
//	return gcpService.DeleteHmacKey(projectNumber, accessKey, ServiceAccount)
// }

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
		return nil, fmt.Errorf("volume %s has no volume attributes", vol.Name)
	}

	for _, bucketDetail := range backupVault.BucketDetails {
		if bucketDetail.VendorSubnetID == vol.VolumeAttributes.VendorSubnetID && bucketDetail.BucketName != "" {
			return bucketDetail, nil
		}
	}
	return nil, fmt.Errorf("no matching bucket details found for volume %s in backup vault %s", vol.Name, backupVault.Name)
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
		return nil, fmt.Errorf("backup %s has no attributes", backup.Name)
	}

	for _, bucketDetail := range backupVault.BucketDetails {
		if bucketDetail.BucketName != "" && bucketDetail.BucketName == backup.Attributes.BucketName {
			return bucketDetail, nil
		}
	}
	return nil, fmt.Errorf("no matching bucket details found for backup %s", backup.Name)
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
