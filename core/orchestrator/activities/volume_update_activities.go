package activities

import (
	"context"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var prepareFieldsForUpdate = getUpdatedFieldsFromParams

type VolumeUpdateActivity struct {
	SE database.Storage
}

// UpdateVolumeInONTAP updates the volume in ONTAP
func (a *VolumeUpdateActivity) UpdateVolumeInONTAP(ctx context.Context, volume *datamodel.Volume, params *common.UpdateVolumeParams, node *models.Node) error {
	logger := util.GetLogger(ctx)
	provider := GetProviderByNode(ctx, node)
	snapshotPolicyName := SnapshotPolicyNone
	if volume.SnapshotPolicy != nil && volume.SnapshotPolicy.Name != "" {
		snapshotPolicyName = volume.SnapshotPolicy.Name
	}

	err := provider.UpdateVolume(vsa.UpdateVolumeParams{
		// Set the necessary parameters for updating the volume
		UUID:         volume.VolumeAttributes.ExternalUUID,
		Size:         params.QuotaInBytes,
		SnapshotName: snapshotPolicyName,
	})
	if err != nil {
		logger.Errorf("Failed to update volume %s in ontap: %v", volume.Name, err)
		return err
	}

	logger.Debugf("Volume %s updated successfully in ontap", volume.Name)
	return nil
}

// GetVolumeFromONTAP retrieves the volume from ONTAP
func (a *VolumeUpdateActivity) GetVolumeFromONTAP(ctx context.Context, volume *datamodel.Volume, node *models.Node) (*vsa.VolumeResponse, error) {
	logger := util.GetLogger(ctx)
	provider := GetProviderByNode(ctx, node)

	volumeRes, err := provider.GetVolume(vsa.GetVolumeParams{
		UUID:       volume.VolumeAttributes.ExternalUUID,
		VolumeName: volume.Name,
		SvmName:    volume.Svm.Name,
	})

	if err != nil {
		logger.Errorf("Failed to get volume %s from ONTAP: %v", volume.Name, err)
		return nil, err
	}
	return volumeRes, err
}

// UpdateLun updates the LUN associated with the volume in the VSA cluster
func (a *VolumeUpdateActivity) UpdateLun(ctx context.Context, volume *datamodel.Volume, quotaInBytes int64, node *models.Node) error {
	logger := util.GetLogger(ctx)
	provider := GetProviderByNode(ctx, node)
	lunName := utils.GetLunName(volume.Name)
	err := provider.LunUpdate(vsa.LunUpdateParams{
		// Set the necessary parameters for updating the volume
		UUID:       volume.VolumeAttributes.BlockProperties.LunUUID,
		LunName:    lunName,
		VolumeName: volume.Name,
		SvmName:    volume.Svm.Name,
		Size:       quotaInBytes,
	})
	if err != nil {
		if errors.IsConflictErr(err) {
			logger.Debugf("Lun %s size is same as existing size", volume.Name)
			return nil
		}
		logger.Errorf("Failed to update lun %s in vsa cluster: %v", volume.Name, err)
		return err
	}

	logger.Debugf("Lun %s updated successfully in vsa cluster", volume.Name)
	return nil
}

// UpdateVolumeInDB updates the volume in the database with the new parameters
func (a *VolumeUpdateActivity) UpdateVolumeInDB(ctx context.Context, volume *datamodel.Volume, params *common.UpdateVolumeParams) error {
	logger := util.GetLogger(ctx)

	updatedFields := prepareFieldsForUpdate(volume, params)
	// Update the volume in the database
	err := a.SE.UpdateVolumeFields(ctx, volume.UUID, updatedFields)
	if err != nil {
		logger.Errorf("Failed to update volume %s in the database: %v", volume.Name, err)
		return err
	}

	logger.Debugf("Volume %s updated successfully in the database", volume.Name)
	return nil
}

// getUpdatedFieldsFromParams prepares the fields to be updated in the database
func getUpdatedFieldsFromParams(volume *datamodel.Volume, params *common.UpdateVolumeParams) map[string]interface{} {
	updates := make(map[string]interface{})
	if params.Description != "" {
		updates["description"] = params.Description
	}

	if params.QuotaInBytes != 0 {
		updates["size_in_bytes"] = params.QuotaInBytes
	}

	if params.Labels != nil {
		jsonbLabels := make(datamodel.JSONB)
		for k, v := range params.Labels {
			jsonbLabels[k] = v
		}
		if volume.VolumeAttributes == nil {
			volume.VolumeAttributes = &datamodel.VolumeAttributes{}
		}
		volume.VolumeAttributes.Labels = &jsonbLabels
		updates["volume_attributes"] = volume.VolumeAttributes
	}

	if params.SnapshotPolicy != nil {
		updates["snapshot_policy"] = volume.SnapshotPolicy
	}

	if params.DataProtection != nil {
		if volume.DataProtection == nil {
			volume.DataProtection = &datamodel.DataProtection{}
		}
		volume.DataProtection.BackupVaultID = params.DataProtection.BackupVaultID
		updates["data_protection"] = volume.DataProtection
	}

	updates["state"] = models.LifeCycleStateREADY
	updates["state_details"] = models.LifeCycleStateAvailableDetails

	return updates
}

// UpdateSnapshotPolicyInOntap updates the snapshot policy for the given volume in ONTAP.
func (a *VolumeUpdateActivity) UpdateSnapshotPolicyInOntap(ctx context.Context, node *models.Node, currentPolicy, updatingPolicy *datamodel.SnapshotPolicy) error {
	logger := util.GetLogger(ctx)
	provider := GetProviderByNode(ctx, node)

	err := provider.UpdateSnapshotPolicy(ctx, &vsa.UpdateSnapshotPolicyParams{
		CurrentSnapshotPolicy: &vsa.SnapshotPolicy{
			Name:      currentPolicy.Name,
			IsEnabled: currentPolicy.IsEnabled,
			Schedules: ConvertToVSASnapshotPolicySchedules(currentPolicy.Schedules),
		},
		UpdatingSnapshotPolicy: &vsa.SnapshotPolicy{
			Name:      currentPolicy.Name,
			IsEnabled: updatingPolicy.IsEnabled,
			Schedules: ConvertToVSASnapshotPolicySchedules(updatingPolicy.Schedules),
		},
	})
	if err != nil {
		logger.Errorf("Failed to update snapshot policy %s in ONTAP: %v", updatingPolicy.Name, err)
		return err
	}

	logger.Debugf("Snapshot policy %s updated successfully in ONTAP", updatingPolicy.Name)
	return nil
}

// FindTenancyDetails retrieves the tenancy information for the given consumer VPC and customer project number
func (a *VolumeUpdateActivity) FindTenancyDetails(ctx context.Context, consumerVPC string, customerProjectNumber string, tenantProjectRegion *string) (*common.TenancyInfo, error) {
	gcpService, err := GetGCPService(ctx)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return FindTenancy(gcpService, consumerVPC, customerProjectNumber, tenantProjectRegion)
}

// CheckBackupVaultExistInVCP checks if a backup vault exists in the VCP
func (a *VolumeUpdateActivity) CheckBackupVaultExistInVCP(ctx context.Context, volume *datamodel.Volume, region string) error {
	return CheckBackupVaultExistsInVCP(ctx, a.SE, volume, region)
}

// CreateBucketForBackupVault creates a bucket in the specified region for the given resource name and tenancy details
func (a *VolumeUpdateActivity) CreateBucketForBackupVault(ctx context.Context, resourceName *common.ResourceNames, tenancyDetails *common.TenancyInfo, region string) (*common.BucketDetails, error) {
	return CreateBucket(ctx, resourceName, tenancyDetails, region)
}

// GenerateResourceNamesForBackupVault generates resource names for the volume based on the tenancy details and GCP region
func (a *VolumeUpdateActivity) GenerateResourceNamesForBackupVault(ctx context.Context, volume *datamodel.Volume, tenancyDetails *common.TenancyInfo, gcpRegion string) (*common.ResourceNames, error) {
	return GenerateResourceNames(ctx, volume, tenancyDetails, gcpRegion)
}

// CheckBucketResourceName checks if the volume has a bucket resource name and returns the bucket details
func (a *VolumeUpdateActivity) CheckBucketResourceName(ctx context.Context, volume *datamodel.Volume) (*common.BucketDetails, error) {
	return CheckForBucketResourceName(ctx, a.SE, volume)
}

// UpdateBucketDetailsOfBackupVault updates the backup vault with the bucket details
func (a *VolumeUpdateActivity) UpdateBucketDetailsOfBackupVault(ctx context.Context, volume *datamodel.Volume, bucketDetails *common.BucketDetails) error {
	return UpdateBackupVaultWithBucketDetails(a.SE, ctx, volume, bucketDetails)
}
