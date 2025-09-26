package activities

import (
	"context"

	ontapModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/scheduler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
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
	SE        database.Storage
	Scheduler *scheduler.TemporalScheduler
}

// UpdateVolumeInONTAP updates the volume in ONTAP
func (a *VolumeUpdateActivity) UpdateVolumeInONTAP(ctx context.Context, volume *datamodel.Volume, params *common.UpdateVolumeParams, node *models.Node) error {
	logger := util.GetLogger(ctx)
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	updateVolumeParams := &vsa.UpdateVolumeParams{
		UUID:        volume.VolumeAttributes.ExternalUUID,
		Size:        params.QuotaInBytes,
		SnapReserve: params.SnapReserve,
	}

	// Set snapshot policy only if the volume is not a data protection volume.
	if !volume.VolumeAttributes.IsDataProtection {
		if params.SnapshotPolicy != nil && params.SnapshotPolicy.Name != "" {
			updateVolumeParams.SnapshotPolicyName = params.SnapshotPolicy.Name
		}
	}

	if params.SnapshotDirectoryAccess != nil {
		updateVolumeParams.SnapshotDirectoryAccess = params.SnapshotDirectoryAccess
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
		return vsaerrors.WrapAsTemporalApplicationError(err)
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
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	isRestore := false
	if volume.VolumeAttributes != nil && volume.VolumeAttributes.RestoredBackupPath != "" {
		isRestore = true
	}
	volumeRes, err := provider.GetVolume(vsa.GetVolumeParams{
		UUID:              volume.VolumeAttributes.ExternalUUID,
		VolumeName:        volume.Name,
		SvmName:           volume.Svm.Name,
		IsRestore:         isRestore,
		SnapshotDirectory: volume.VolumeAttributes.SnapshotDirectory,
	})

	if err != nil {
		logger.Errorf("Failed to get volume %s from ONTAP: %v", volume.Name, err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return volumeRes, nil
}

// UpdateLun updates the LUN associated with the volume in the VSA cluster
func (a *VolumeUpdateActivity) UpdateLun(ctx context.Context, volume *datamodel.Volume, volResponse *vsa.VolumeResponse, node *models.Node) (*vsa.LunResponse, error) {
	logger := util.GetLogger(ctx)
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	var lunUUID string
	var lunName string
	if volume.VolumeAttributes != nil && volume.VolumeAttributes.BlockDevices != nil && len(*volume.VolumeAttributes.BlockDevices) > 0 {
		lunUUID = (*volume.VolumeAttributes.BlockDevices)[0].LunUUID
		lunName = (*volume.VolumeAttributes.BlockDevices)[0].Name
	} else if volume.VolumeAttributes != nil && volume.VolumeAttributes.BlockProperties != nil {
		lunUUID = volume.VolumeAttributes.BlockProperties.LunUUID
		lunName = volume.VolumeAttributes.BlockProperties.LunName
	}
	lun, err := LunGet(ctx, lunName, volume.Name, volume.Svm.Name, provider)
	if err != nil {
		logger.Debug("lun not found !")
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	// Determine the size to update based on snap reserve logic
	sizeToUpdate := volResponse.AFSSize - volResponse.MetadataSize
	lunUpdateParams := vsa.LunUpdateParams{
		UUID:       lunUUID,
		LunName:    lunName,
		VolumeName: volume.Name,
		SvmName:    volume.Svm.Name,
	}
	if sizeToUpdate < lun.Size {
		lunUpdateParams.Size = lun.Size
	} else {
		lunUpdateParams.Size = sizeToUpdate
	}
	// Update the LUN with the calculated size
	err = provider.LunUpdate(lunUpdateParams)
	if err != nil {
		if errors.IsConflictErr(err) {
			logger.Debugf("Lun %s size is same as existing size", volume.Name)
			return nil, nil
		}
		logger.Errorf("Failed to update lun %s in vsa cluster: %v", volume.Name, err)
		return nil, vsaerrors.WrapAsNonRetryableTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrLunUpdate, err))
	}
	logger.Debugf("Lun %s updated successfully in vsa cluster", volume.Name)
	return LunGet(ctx, lunName, volume.Name, volume.Svm.Name, provider)
}

func (a *VolumeUpdateActivity) EnsureHostGroupsExistsAndMapDisk(ctx context.Context, volume *datamodel.Volume, iGroups []string, node *models.Node) error {
	logger := util.GetLogger(ctx)
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	hgs, err := a.SE.GetMultipleHostGroups(ctx, iGroups, volume.AccountID)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
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
	lunName := utils.GetLunName(volume.Name)
	if volume.VolumeAttributes != nil && volume.VolumeAttributes.BlockDevices != nil && len(*volume.VolumeAttributes.BlockDevices) > 0 {
		lunName = (*volume.VolumeAttributes.BlockDevices)[0].Name
	}

	err = provider.LunMapCreate(vsa.LunMapCreateParams{
		LunName:    "/vol/" + volume.Name + "/" + lunName,
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
	se := a.SE
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	var lunUUID string
	if volume.VolumeAttributes != nil && volume.VolumeAttributes.BlockDevices != nil && len(*volume.VolumeAttributes.BlockDevices) > 0 {
		lunUUID = (*volume.VolumeAttributes.BlockDevices)[0].LunUUID
	} else if volume.VolumeAttributes != nil && volume.VolumeAttributes.BlockProperties != nil {
		lunUUID = volume.VolumeAttributes.BlockProperties.LunUUID
	}
	for _, iGroupUUID := range iGroupUUIDs {
		hgMapsToDelete, err := a.SE.GetHostGroup(ctx, iGroupUUID, volume.AccountID)
		if err != nil {
			return err
		}

		// Fetch iGroups uuid to delete the map
		exists, iGroupOntap, err := provider.IgroupExists(hgMapsToDelete.Name, &volume.Svm.Name)
		if err != nil {
			return err
		}
		if !exists {
			logger.Debugf("IGroup %s not found in vsa cluster, skipping unmapping lun map and igroup delete", iGroupUUID)
			continue
		}
		err = provider.LunMapDelete(vsa.LunMapDeleteParams{
			LunUUID:    lunUUID,
			IGroupUUID: *iGroupOntap.UUID,
		})
		if err != nil {
			logger.Errorf("Failed to delete lun map for igroup %s: %v", iGroupUUID, err)
			return err
		}

		// Fetch all volumes for the HG and delete the IGroup which doesn't have any volume in current pool
		hgAttachedVolumes, err := se.GetAllVolumesForHG(ctx, iGroupUUID, volume.AccountID)
		if err != nil {
			return err
		}

		// We have to check if there is any other volume using this HG in the same pool
		deleteIGroup := true
		for _, volumeWithHG := range hgAttachedVolumes {
			if volume.PoolID == volumeWithHG.PoolID && volume.UUID != volumeWithHG.UUID {
				deleteIGroup = false
				break
			}
		}

		if deleteIGroup {
			err = provider.IgroupDelete(*iGroupOntap.UUID)
			if err != nil {
				logger.Errorf("Failed to delete igroup %s: %v", iGroupUUID, err)
				return err
			}
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
		logger.Errorf("Failed to prepareFieldsForUpdate for the volume %s in the database: %v", volume.UUID, err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	// Update the volume in the database
	err = a.SE.UpdateVolumeFields(ctx, volume.UUID, updatedFields)
	if err != nil {
		logger.Errorf("Failed to update volume %s in the database: %v", volume.UUID, err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	logger.Debugf("Volume %s updated successfully in the database", volume.UUID)
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
		if params.DataProtection.BackupVaultID != nil {
			volume.DataProtection.BackupVaultID = *params.DataProtection.BackupVaultID
		}
		if params.DataProtection.BackupPolicyId != nil {
			volume.DataProtection.BackupPolicyID = *params.DataProtection.BackupPolicyId
		}
		volume.DataProtection.ScheduledBackupEnabled = params.DataProtection.ScheduledBackupEnabled
		updates["data_protection"] = volume.DataProtection
	}

	// Check BlockDevices first, then fallback to BlockProperties
	if len(params.BlockDevices) > 0 {
		// Use BlockDevices as primary source
		blockDevices := make([]datamodel.BlockDevice, 0, len(params.BlockDevices))
		for _, blockDeviceReq := range params.BlockDevices {
			blockDevice := datamodel.BlockDevice{
				Name:       blockDeviceReq.Name,
				OSType:     blockDeviceReq.OSType,
				Size:       blockDeviceReq.SizeInBytes,
				Identifier: blockDeviceReq.LunSerialNumber,
				LunUUID:    blockDeviceReq.LunUUID,
			}
			if len(blockDeviceReq.HostGroups) > 0 {
				for _, uuid := range blockDeviceReq.HostGroups {
					hg, err := getHostGroup(se, ctx, uuid, volume.AccountID)
					if err != nil {
						return nil, err
					}
					hgDetail := datamodel.HostGroupDetail{
						HostGroupUUID: uuid,
						HostQNs:       hg.Hosts.Hosts,
					}
					blockDevice.HostGroupDetails = append(blockDevice.HostGroupDetails, hgDetail)
				}
			}
			blockDevices = append(blockDevices, blockDevice)
		}
		volume.VolumeAttributes.BlockDevices = &blockDevices
	} else if params.BlockProperties != nil {
		// Fallback: Use BlockProperties if BlockDevices are not provided
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
			(params.AutoTieringPolicy.AutoTieringEnabled && volume.AutoTieringPolicy != nil &&
				(params.AutoTieringPolicy.CoolingThresholdDays != volume.AutoTieringPolicy.CoolingThresholdDays ||
					params.AutoTieringPolicy.HotTierBypassModeEnabled != volume.AutoTieringPolicy.HotTierBypassModeEnabled))) {
		updates["auto_tiering_enabled"] = params.AutoTieringPolicy.AutoTieringEnabled

		autoTieringPolicy := &datamodel.AutoTieringPolicy{
			TieringPolicy:            params.AutoTieringPolicy.TieringPolicy,
			HotTierBypassModeEnabled: params.AutoTieringPolicy.HotTierBypassModeEnabled,
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
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
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
	gcpService, err := hyperscaler.GetGCPService(ctx)
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

// VerifyIfBackupPolicyExistsInVCP checks if a backup policy exists in the VCP
func (a *VolumeUpdateActivity) VerifyIfBackupPolicyExistsInVCP(ctx context.Context, backupPolicyUUID string, accountId int64) (bool, error) {
	return CheckIfBackupPolicyExistsInVCP(ctx, a.SE, backupPolicyUUID, accountId)
}

// FetchAndCreateBackupPolicyFromSDE creates a backup policy in the VCP
func (a *VolumeUpdateActivity) FetchAndCreateBackupPolicyFromSDE(ctx context.Context, volume *datamodel.Volume, region string) (*datamodel.BackupPolicy, error) {
	return CreateBackupPolicyFetchedFromSDE(ctx, a.SE, volume, region)
}

// CreateScheduleForBackupPolicy creates a backup policy schedule in the VCP
func (a *VolumeUpdateActivity) CreateScheduleForBackupPolicy(ctx context.Context, backupPolicy *datamodel.BackupPolicy, customSchedule string) error {
	return CreateBackupPolicySchedule(ctx, a.Scheduler, backupPolicy, customSchedule)
}

// UpdateJunctionPathInONTAP updates the junction path for a volume in ONTAP
func (a *VolumeUpdateActivity) UpdateJunctionPathInONTAP(ctx context.Context, volume *datamodel.Volume, junctionPath string, node *models.Node) error {
	logger := util.GetLogger(ctx)
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Get current junction path from volume attributes
	var currentJunctionPath string
	if volume.VolumeAttributes != nil && volume.VolumeAttributes.FileProperties != nil {
		currentJunctionPath = volume.VolumeAttributes.FileProperties.JunctionPath
	}

	// Only proceed if junction path is different
	if currentJunctionPath != junctionPath {
		// If volume is currently mounted (has existing junction path), unmount it first
		logger.Debugf("Unmounting volume %s from junction path %s", volume.Name, currentJunctionPath)
		_, err := provider.UnmountVolume(volume.VolumeAttributes.ExternalUUID)
		if err != nil {
			logger.Errorf("Failed to unmount volume %s: %v", volume.Name, err)
			return vsaerrors.WrapAsTemporalApplicationError(err)
		}
		logger.Debugf("Volume %s unmounted successfully", volume.Name)
	}

	// Now mount the volume to the new junction path
	logger.Debugf("Mounting volume %s to new junction path %s", volume.Name,
		junctionPath)
	_, err = provider.MountVolume(vsa.MountVolumeParams{
		UUID:         volume.VolumeAttributes.ExternalUUID,
		JunctionPath: junctionPath,
	})
	if err != nil {
		logger.Errorf("Failed to mount volume %s to junction path %s: %v", volume.Name, junctionPath, err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	logger.Debugf("Junction path updated successfully for volume %s in ONTAP", volume.Name)
	return nil
}

// UpdateExportPolicyRulesInONTAP updates the export policy rules for a volume in ONTAP
func (a *VolumeUpdateActivity) UpdateExportPolicyRulesInONTAP(ctx context.Context, volume *datamodel.Volume, exportPolicy *models.ExportPolicy, node *models.Node) error {
	logger := util.GetLogger(ctx)
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Convert models.ExportPolicy to vsa.ExportPolicy
	vsaExportPolicy := &vsa.ExportPolicy{
		ExportPolicyName: exportPolicy.ExportPolicyName,
		ExportRules:      make([]*vsa.ExportRule, 0, len(exportPolicy.ExportRules)),
	}

	for _, rule := range exportPolicy.ExportRules {
		vsaRule := &vsa.ExportRule{
			AllowedClients:      rule.AllowedClients,
			AnonymousUser:       rule.AnonymousUser,
			Index:               rule.Index,
			ChownMode:           rule.ChownMode,
			AccessType:          rule.AccessType,
			CIFS:                rule.CIFS,
			NFSv3:               rule.NFSv3,
			NFSv4:               rule.NFSv4,
			S3:                  rule.S3,
			UnixReadOnly:        rule.UnixReadOnly,
			UnixReadWrite:       rule.UnixReadWrite,
			Kerberos5ReadOnly:   rule.Kerberos5ReadOnly,
			Kerberos5ReadWrite:  rule.Kerberos5ReadWrite,
			Kerberos5iReadOnly:  rule.Kerberos5iReadOnly,
			Kerberos5iReadWrite: rule.Kerberos5iReadWrite,
			Kerberos5pReadOnly:  rule.Kerberos5pReadOnly,
			Kerberos5pReadWrite: rule.Kerberos5pReadWrite,
			Superuser:           rule.Superuser,
		}
		vsaExportPolicy.ExportRules = append(vsaExportPolicy.ExportRules, vsaRule)
	}

	err = provider.UpdateExportPolicyRules(vsa.UpdateExportPolicyRulesParams{
		VolumeName:   volume.Name,
		SvmName:      volume.Svm.Name,
		ExportPolicy: vsaExportPolicy,
	})
	if err != nil {
		logger.Errorf("Failed to update export policy for volume %s in ONTAP: %v", volume.Name, err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	logger.Debugf("Export policy updated successfully for volume %s in ONTAP", volume.Name)
	return nil
}
