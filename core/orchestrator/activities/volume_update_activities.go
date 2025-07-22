package activities

import (
	"context"

	ontapModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	prepareFieldsForUpdate        = getUpdatedFieldsFromParams
	HostGroupsUpdateDiffForVolume = _hostGroupsUpdateDiffForVolume
	getHostGroup                  = _getHostGroup
)

type VolumeUpdateActivity struct {
	SE database.Storage
}

// UpdateVolumeInONTAP updates the volume in ONTAP
func (a *VolumeUpdateActivity) UpdateVolumeInONTAP(ctx context.Context, volume *datamodel.Volume, params *common.UpdateVolumeParams, node *models.Node) error {
	logger := util.GetLogger(ctx)
	provider, err := GetProviderByNode(ctx, node)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	snapshotPolicyName := SnapshotPolicyNone
	if volume.SnapshotPolicy != nil && volume.SnapshotPolicy.Name != "" {
		snapshotPolicyName = volume.SnapshotPolicy.Name
	}
	updateVolumeParams := &vsa.UpdateVolumeParams{
		UUID:               volume.VolumeAttributes.ExternalUUID,
		Size:               params.QuotaInBytes,
		SnapshotPolicyName: snapshotPolicyName,
		SnapReserve:        params.SnapReserve,
	}
	if params.AutoTieringPolicy != nil {
		updateVolumeParams.TieringPolicy = &vsa.TieringPolicy{}
		if params.AutoTieringPolicy.AutoTieringEnabled {
			updateVolumeParams.TieringPolicy.CoolAccessTieringPolicy = nillable.GetString(&params.AutoTieringPolicy.TieringPolicy, ontapModels.VolumeInlineTieringPolicyAuto)
			updateVolumeParams.TieringPolicy.CoolAccessRetrievalPolicy = nillable.GetString(&params.AutoTieringPolicy.RetrievalPolicy, ontapModels.VolumeCloudRetrievalPolicyDefault)
			updateVolumeParams.TieringPolicy.CoolnessPeriod = int64(params.AutoTieringPolicy.CoolingThresholdDays)
		} else {
			updateVolumeParams.TieringPolicy.CoolAccessTieringPolicy = ontapModels.VolumeInlineTieringPolicyNone
		}
	}
	err = updateVolume(ctx, provider, *updateVolumeParams)
	if err != nil {
		logger.Errorf("Failed to update volume %s in ontap: %v", volume.Name, err)
		return err
	}
	logger.Debugf("Volume %s updated successfully in ontap", volume.Name)
	return nil
}

func updateVolume(ctx context.Context, provider vsa.Provider, params vsa.UpdateVolumeParams) error {
	err := provider.UpdateVolume(params)
	if err != nil {
		util.GetLogger(ctx).Errorf("Failed to update volume %s in ontap: %v", params.UUID, err)
		return err
	}
	util.GetLogger(ctx).Debugf("Volume %s updated successfully in ontap", params.UUID)
	return nil
}

// GetVolumeFromONTAP retrieves the volume from ONTAP
func (a *VolumeUpdateActivity) GetVolumeFromONTAP(ctx context.Context, volume *datamodel.Volume, node *models.Node) (*vsa.VolumeResponse, error) {
	logger := util.GetLogger(ctx)
	provider, err := GetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

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
	provider, err := GetProviderByNode(ctx, node)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	lunName := utils.GetLunName(volume.Name)
	err = provider.LunUpdate(vsa.LunUpdateParams{
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

func (a *VolumeUpdateActivity) EnsureHostGroupsExistsAndMapDisk(ctx context.Context, volume *datamodel.Volume, iGroups []string, node *models.Node) error {
	logger := util.GetLogger(ctx)
	provider, err := GetProviderByNode(ctx, node)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	hgs, err := a.SE.GetMultipleHostGroups(ctx, iGroups, volume.AccountID)
	if err != nil {
		return err
	}
	if len(hgs) == 0 {
		logger.Debugf("No host groups to map for volume %s", volume.Name)
		return nil
	}

	hgNames := make([]string, 0)
	for _, hg := range hgs {
		hgNames = append(hgNames, hg.Name)
	}

	for _, hostGroup := range hgs {
		// Check if the hostGroup already exists
		exists, _, err := provider.IgroupExists(hostGroup.Name, &volume.Svm.Name)
		if err != nil {
			return err
		}
		if !exists {
			// Create the hostGroup if it doesn't exist
			if _, err = provider.IgroupCreate(vsa.IgroupCreateParams{
				IgroupName: hostGroup.Name,
				SvmName:    volume.Svm.Name,
				OsType:     hostGroup.OSType,
				Initiator:  hostGroup.Hosts.Hosts,
			}); err != nil {
				logger.Errorf("Failed to create igroup %s: %v", hostGroup.Name, err)
				return err
			}
		}
	}

	err = provider.LunMapCreate(vsa.LunMapCreateParams{
		LunName:    "/vol/" + volume.Name + "/" + utils.GetLunName(volume.Name),
		SvmName:    volume.Svm.Name,
		IGroupName: hgNames,
	})
	if err != nil {
		return err
	}

	return nil
}

// UnmapHostGroupFromDisk deletes the Disk HostGroup map
func (a *VolumeUpdateActivity) UnmapHostGroupFromDisk(ctx context.Context, volume *datamodel.Volume, iGroupUUIDs []string, node *models.Node) error {
	logger := util.GetLogger(ctx)
	provider, err := GetProviderByNode(ctx, node)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	for _, iGroupUUID := range iGroupUUIDs {
		hgMapsToDelete, err := a.SE.GetHostGroup(ctx, iGroupUUID, volume.AccountID)
		if err != nil {
			return err
		}

		// Fetch iGroups uuid to delete the map
		iGroupOntap, err := provider.IgroupGet(&hgMapsToDelete.Name, &volume.Svm.Name)
		if err != nil {
			return err
		}
		err = provider.LunMapDelete(vsa.LunMapDeleteParams{
			LunUUID:    volume.VolumeAttributes.BlockProperties.LunUUID,
			IGroupUUID: *iGroupOntap.UUID,
		})
		if err != nil {
			logger.Errorf("Failed to delete lun map for igroup %s: %v", iGroupUUID, err)
			return err
		}
	}
	return nil
}

func _hostGroupsUpdateDiffForVolume(existingIGroups []string, newIGroups []string) ([]string, []string) {
	toCreate := make([]string, 0)
	toDelete := make([]string, 0)
	for _, newIGroup := range newIGroups {
		if !utils.ContainsString(existingIGroups, newIGroup) {
			toCreate = append(toCreate, newIGroup)
		}
	}

	for _, existingIGroup := range existingIGroups {
		if !utils.ContainsString(newIGroups, existingIGroup) {
			toDelete = append(toDelete, existingIGroup)
		}
	}
	return toCreate, toDelete
}

// UpdateVolumeInDB updates the volume in the database with the new parameters
func (a *VolumeUpdateActivity) UpdateVolumeInDB(ctx context.Context, volume *datamodel.Volume, params *common.UpdateVolumeParams) error {
	logger := util.GetLogger(ctx)

	updatedFields, err := prepareFieldsForUpdate(ctx, a.SE, volume, params)
	if err != nil {
		logger.Errorf("Failed to update volume %s in the database: %v", volume.Name, err)
		return err
	}
	// Update the volume in the database
	err = a.SE.UpdateVolumeFields(ctx, volume.UUID, updatedFields)
	if err != nil {
		logger.Errorf("Failed to update volume %s in the database: %v", volume.Name, err)
		return err
	}

	logger.Debugf("Volume %s updated successfully in the database", volume.Name)
	return nil
}

// getUpdatedFieldsFromParams prepares the fields to be updated in the database
func getUpdatedFieldsFromParams(ctx context.Context, se database.Storage, volume *datamodel.Volume, params *common.UpdateVolumeParams) (map[string]interface{}, error) {
	updates := make(map[string]interface{})
	if params.Description != "" {
		updates["description"] = params.Description
	}

	if params.QuotaInBytes != 0 {
		updates["size_in_bytes"] = params.QuotaInBytes
	}

	if volume.VolumeAttributes == nil {
		volume.VolumeAttributes = &datamodel.VolumeAttributes{}
	}

	if params.Labels != nil {
		volume.VolumeAttributes.Labels = params.Labels
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

	if params.BlockProperties != nil {
		if volume.VolumeAttributes.BlockProperties == nil {
			volume.VolumeAttributes.BlockProperties = &datamodel.BlockProperties{}
		}
		hgDetails := make([]datamodel.HostGroupDetail, 0)
		for _, uuid := range params.BlockProperties.HostGroupUUIDs {
			hg, err := getHostGroup(se, ctx, uuid, volume.AccountID)
			if err != nil {
				return nil, err
			}
			hgDetail := datamodel.HostGroupDetail{
				HostGroupUUID: uuid,
				HostQNs:       hg.Hosts.Hosts,
			}
			hgDetails = append(hgDetails, hgDetail)
		}
		volume.VolumeAttributes.BlockProperties.HostGroupDetails = hgDetails
	}

	if params.AutoTieringPolicy != nil &&
		(params.AutoTieringPolicy.AutoTieringEnabled != volume.AutoTieringEnabled ||
			(params.AutoTieringPolicy.AutoTieringEnabled && volume.AutoTieringPolicy != nil && params.AutoTieringPolicy.CoolingThresholdDays != volume.AutoTieringPolicy.CoolingThresholdDays)) {
		updates["auto_tiering_enabled"] = params.AutoTieringPolicy.AutoTieringEnabled
		autoTieringPolicy := &datamodel.AutoTieringPolicy{
			TieringPolicy: params.AutoTieringPolicy.TieringPolicy,
		}
		if params.AutoTieringPolicy.TieringPolicy != ontapModels.VolumeInlineTieringPolicyNone {
			autoTieringPolicy.CoolingThresholdDays = params.AutoTieringPolicy.CoolingThresholdDays
			autoTieringPolicy.RetrievalPolicy = params.AutoTieringPolicy.RetrievalPolicy
		}
		updates["auto_tiering_policy"] = autoTieringPolicy
	}

	updates["volume_attributes"] = volume.VolumeAttributes
	if params.SnapReserve != nil {
		if volume.VolumeAttributes == nil {
			volume.VolumeAttributes = &datamodel.VolumeAttributes{}
		}
		volume.VolumeAttributes.SnapReserve = *params.SnapReserve
		updates["volume_attributes"] = volume.VolumeAttributes
	}

	updates["state"] = models.LifeCycleStateREADY
	updates["state_details"] = models.LifeCycleStateAvailableDetails

	return updates, nil
}

func _getHostGroup(se database.Storage, ctx context.Context, uuid string, accountId int64) (*datamodel.HostGroup, error) {
	hg, err := se.GetHostGroup(ctx, uuid, accountId)
	if err != nil {
		return nil, err
	}
	return hg, nil
}

// UpdateSnapshotPolicyInOntap updates the snapshot policy for the given volume in ONTAP.
func (a *VolumeUpdateActivity) UpdateSnapshotPolicyInOntap(ctx context.Context, node *models.Node, currentPolicy, updatingPolicy *datamodel.SnapshotPolicy) error {
	logger := util.GetLogger(ctx)
	provider, err := GetProviderByNode(ctx, node)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	err = provider.UpdateSnapshotPolicy(ctx, &vsa.UpdateSnapshotPolicyParams{
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

// RefreshVolumeFieldsInDB updates the used bytes of a volume in the database
func (a *VolumeUpdateActivity) RefreshVolumeFieldsInDB(ctx context.Context, volumeUUID string, volResponse *vsa.VolumeResponse) error {
	se := a.SE

	// add more fields to update if needed
	err := se.UpdateVolumeFields(ctx, volumeUUID, map[string]interface{}{
		"used_bytes": uint64(volResponse.UsedBytes),
	})
	if err != nil {
		util.GetLogger(ctx).Errorf("Failed to update volume fields in DB: %v", err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	return nil
}
