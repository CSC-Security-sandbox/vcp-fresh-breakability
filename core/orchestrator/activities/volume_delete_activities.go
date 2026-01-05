package activities

import (
	"context"
	"fmt"
	"strings"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	utilErrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
)

type VolumeDeleteActivity struct {
	SE database.Storage
}

func (va VolumeDeleteActivity) DeleteVolumeInONTAP(ctx context.Context, volumeExternalUUID, volumeName string, node *models.Node) error {
	logger := util.GetLogger(ctx)
	activity.RecordHeartbeat(ctx, "Starting DeleteVolumeInONTAP activity")
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	err = provider.DeleteVolume(volumeExternalUUID, volumeName)
	if err != nil {
		if strings.Contains(err.Error(), "volume is in use") {
			return vsaerrors.WrapAsNonRetryableTemporalApplicationError(err)
		}
		if strings.Contains(strings.ToLower(err.Error()), vsaerrors.OntapUnreachableError) {
			logger.Errorf("DeleteVolumeInONTAP - Unable to reach node %s Error: %v", node.Name, err)
			return temporal.NewNonRetryableApplicationError("Unable to delete volume: Node not reachable", vsaerrors.DeleteVolumeInONTAPError, utilErrors.New("unable to reach node"))
		}
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	logger.Debugf("Volume %s deleted successfully from the vsa cluster", volumeName)
	activity.RecordHeartbeat(ctx, "Finished DeleteVolumeInONTAP activity")
	return nil
}

func (va VolumeDeleteActivity) DeleteVolume(ctx context.Context, volume *datamodel.Volume) error {
	logger := util.GetLogger(ctx)
	se := va.SE
	activity.RecordHeartbeat(ctx, "Starting DeleteVolume activity")

	_, err := se.DeleteVolume(ctx, volume.UUID)
	if err != nil {
		if utilErrors.IsNotFoundErr(err) {
			return nil
		}
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	logger.Debugf("Volume:%s marked deleted successfully in the db", volume.Name)

	activity.RecordHeartbeat(ctx, "Finished DeleteVolume activity")
	return nil
}

// SmbTeardownContext carries the information required to teardown SMB resources when the last volume is deleted.
type SmbTeardownContext struct {
	ShouldDelete    bool
	ActiveDirectory *datamodel.ActiveDirectory
	SvmExternalUUID string
	FQDN            string
	VolumeUUID      string
	PoolID          int64
}

type cifsServerProvider interface {
	DeleteCIFSServer(externalSVMUUID, adUsername, adPassword string) error
	CreateRESTClient() (ontapRest.RESTClient, error)
}

// DeleteSnapshotPolicyInONTAP deletes the snapshot policy associated with a volume in ONTAP.
func (va VolumeDeleteActivity) DeleteSnapshotPolicyInONTAP(ctx context.Context, SnapshotPolicyName string, node *models.Node) error {
	if node != nil && SnapshotPolicyName != "" {
		activity.RecordHeartbeat(ctx, "Initializing snapshot policy deletion")
		logger := util.GetLogger(ctx)
		provider, err := hyperscaler.GetProviderByNode(ctx, node)
		if err != nil {
			return vsaerrors.WrapAsTemporalApplicationError(err)
		}
		activity.RecordHeartbeat(ctx, "Deleting snapshot policy in ONTAP")
		op := func() error {
			return provider.DeleteSnapshotPolicy(SnapshotPolicyName)
		}
		err = vsa.RetryOnErrors(op, []string{"Policy is in use by at least one volume"})
		if err != nil {
			logger.Errorf("failed to delete snapshot policy: %v", err)
			return vsaerrors.WrapAsTemporalApplicationError(err)
		}
		activity.RecordHeartbeat(ctx, "Snapshot policy deleted successfully")
	}
	return nil
}

func (va VolumeDeleteActivity) DeleteSnapmirrorInONTAP(ctx context.Context, volume *datamodel.Volume, node *models.Node) (*vsa.OntapAsyncResponse, error) {
	logger := util.GetLogger(ctx)
	if node != nil && volume.UUID != "" {
		provider, err := hyperscaler.GetProviderByNode(ctx, node)
		if err != nil {
			return nil, vsaerrors.WrapAsTemporalApplicationError(err)
		}

		se := va.SE
		if volume.DataProtection == nil || volume.DataProtection.BackupVaultID == "" {
			logger.Infof("Volume %s has no data protection configured; skipping snapmirror deletion as expected", volume.UUID)
			return nil, nil
		}

		dbBackupVault, err := se.GetBackupVault(ctx, volume.DataProtection.BackupVaultID)
		if err != nil {
			logger.Errorf("Failed to get backup vault for volume %s: %v", volume.UUID, err)
			return nil, vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("failed to get backup vault for volume %s: %w", volume.UUID, err))
		}

		smDestinationPath, err := GetSmDestinationPath(dbBackupVault, volume)
		if err != nil {
			logger.Errorf("Failed to get snapmirror destination path for volume %s: %v", volume.UUID, err)
			return nil, vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("failed to get snapmirror destination path for volume %s: %w", volume.UUID, err))
		}

		smSourcePath := fmt.Sprintf("%s:%s", volume.Svm.Name, volume.Name)

		snapmirror, err := provider.SnapmirrorRelationshipGet(smDestinationPath, smSourcePath)
		if err != nil {
			if utilErrors.IsNotFoundErr(err) {
				logger.Debugf("No snapmirror relationship found for volume %s (paths: %s -> %s), skipping deletion", volume.UUID, smSourcePath, smDestinationPath)
				return nil, nil
			}
			logger.Errorf("Failed to get snapmirror relationship for volume %s: %v", volume.UUID, err)
			return nil, vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("failed to get snapmirror relationship for volume %s: %w", volume.UUID, err))
		}

		logger.Debugf("Deleting snapmirror relationship %s for volume %s", snapmirror.UUID.String(), volume.UUID)

		response, err := provider.SnapmirrorRelationshipDelete(snapmirror.UUID.String())
		if err != nil {
			logger.Errorf("Failed to delete snapmirror relationship %s for volume %s: %v", snapmirror.UUID.String(), volume.UUID, err)
			return nil, vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("failed to delete snapmirror relationship %s for volume %s: %w", snapmirror.UUID.String(), volume.UUID, err))
		}

		return response, nil
	}
	return nil, nil
}

func (va VolumeDeleteActivity) DeleteVolumeAssociatedSnapshots(ctx context.Context, volumeID int64) error {
	activity.RecordHeartbeat(ctx, "Initializing volume associated snapshots deletion")
	logger := util.GetLogger(ctx)
	se := va.SE
	activity.RecordHeartbeat(ctx, "Retrieving snapshots for volume")
	snapshots, err := se.GetSnapshotsByVolumeID(ctx, volumeID)
	if err != nil {
		if utilErrors.IsNotFoundErr(err) {
			logger.Debugf("no snapshots found for volumeID: %d", volumeID)
			return nil
		}
		logger.Errorf("failed to get snapshot by volumeID: %v", err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	activity.RecordHeartbeat(ctx, "Deleting volume associated snapshots")
	for _, snapshot := range snapshots {
		_, err = se.DeleteSnapshot(ctx, snapshot.UUID)
		if err != nil {
			logger.Warnf("failed to mark snapshot %s as deleted because of error: %v", snapshot.Name, err)
		}
	}
	activity.RecordHeartbeat(ctx, "Volume associated snapshots deleted successfully")
	return nil
}

func (va VolumeDeleteActivity) DetermineIfVolumeIsLastFilesVolume(ctx context.Context, volume *datamodel.Volume, node *models.Node) (bool, error) {
	logger := util.GetLogger(ctx)

	if volume == nil || node == nil {
		return false, vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("volume/node is nil"))
	}

	if volume.VolumeAttributes == nil {
		return false, vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("volume attributes is nil for volume: %s", volume.UUID))
	}

	if !utils.IsNasProtocols(volume.VolumeAttributes.Protocols) {
		logger.Infof("Volume %s is not files volume", volume.UUID)
		return false, nil
	}

	se := va.SE
	volumes, err := se.GetVolumesByPoolID(ctx, volume.PoolID)
	if err != nil {
		logger.Errorf("failed to get volumes for pool %d: %v", volume.PoolID, err)
		return false, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	for _, currentVolume := range volumes {
		if currentVolume == nil {
			continue
		}
		if currentVolume.UUID == volume.UUID {
			continue
		}
		if currentVolume.VolumeAttributes == nil {
			continue
		}
		if utils.IsNasProtocols(currentVolume.VolumeAttributes.Protocols) {
			logger.Infof("Found NFS/SMB volume %s in pool %d", currentVolume.UUID, volume.PoolID)
			return false, nil
		}
	}

	return true, nil
}

func (va VolumeDeleteActivity) DeleteLDAPConfiguration(ctx context.Context, volume *datamodel.Volume, node *models.Node) error {
	logger := util.GetLogger(ctx)

	if volume == nil || node == nil {
		return vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("volume/node is nil"))
	}

	if volume.VolumeAttributes == nil {
		return vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("volume attributes is nil for volume: %s", volume.UUID))
	}

	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	if volume.Svm == nil || volume.Svm.SvmDetails == nil {
		return vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("volume SVM details is nil for volume: %s", volume.UUID))
	}

	err = provider.DeleteLdap(volume.Svm.SvmDetails.ExternalUUID)
	if err != nil {
		if utilErrors.IsNotFoundErr(err) {
			logger.Warnf("Ldap client configuration not found for svm %s, skipping deletion", volume.Svm.SvmDetails.ExternalUUID)
			return nil
		}
		logger.Errorf("failed to delete LDAP config for volume %s: %v", volume.UUID, err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	logger.Infof("Ldap client configuration deleted successfully for svm %s", volume.Svm.SvmDetails.ExternalUUID)
	return nil
}

func (va VolumeDeleteActivity) DetermineSmbTeardownContext(ctx context.Context, volume *datamodel.Volume, node *models.Node) (*SmbTeardownContext, error) {
	logger := util.GetLogger(ctx)
	activity.RecordHeartbeat(ctx, "Starting DetermineSmbTeardownContext activity")
	teardown := &SmbTeardownContext{}

	if volume == nil || node == nil {
		return teardown, nil
	}

	if volume.VolumeAttributes == nil {
		return teardown, nil
	}

	teardown.VolumeUUID = volume.UUID
	teardown.PoolID = volume.PoolID

	if !utils.IsNasProtocols(volume.VolumeAttributes.Protocols) {
		logger.Debugf("Volume %s is not files volume, skipping SMB teardown context", volume.UUID)
		return teardown, nil
	}

	se := va.SE
	volumes, err := se.GetVolumesByPoolID(ctx, volume.PoolID)
	if err != nil {
		logger.Errorf("failed to get volumes for pool %d: %v", volume.PoolID, err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	for _, other := range volumes {
		if other == nil {
			continue
		}
		if other.UUID == volume.UUID {
			continue
		}
		if other.DeletedAt != nil && other.DeletedAt.Valid {
			continue
		}
		if other.State == models.LifeCycleStateDeleted {
			continue
		}
		if other.VolumeAttributes == nil {
			continue
		}
		if utils.IsSMBProtocols(other.VolumeAttributes.Protocols) {
			logger.Debugf("Found SMB volume %s in pool %d, skipping SMB teardown", other.UUID, volume.PoolID)
			return teardown, nil
		}
		if utils.IsNasProtocols(other.VolumeAttributes.Protocols) && other.Pool.PoolAttributes.LdapEnabled {
			logger.Infof("Found LDAP enabled NFS volume %s in pool %d, skipping SMB teardown", other.UUID, volume.PoolID)
			return teardown, nil
		}
	}

	ad, err := se.GetActiveDirectoryForPoolByPoolID(ctx, volume.PoolID)
	if err != nil {
		logger.Errorf("failed to get Active Directory for pool %d: %v", volume.PoolID, err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	if ad == nil {
		logger.Debugf("No Active Directory associated with pool %d, skipping SMB teardown", volume.PoolID)
		return teardown, nil
	}
	if ad.CredentialPath == "" {
		err := fmt.Errorf("active directory credential path is empty")
		logger.Error("Active Directory credential path is empty", "poolID", volume.PoolID, "adUUID", ad.UUID)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	teardown.ActiveDirectory = ad

	svmExternalUUID := ""
	if volume.Svm != nil && volume.Svm.SvmDetails != nil {
		svmExternalUUID = volume.Svm.SvmDetails.ExternalUUID
	}
	if svmExternalUUID == "" {
		dbSvm, dbErr := se.GetSvmForPoolID(ctx, volume.PoolID)
		if dbErr != nil {
			logger.Errorf("failed to fetch SVM for pool %d: %v", volume.PoolID, dbErr)
			return nil, vsaerrors.WrapAsTemporalApplicationError(dbErr)
		}
		if dbSvm != nil && dbSvm.SvmDetails != nil {
			svmExternalUUID = dbSvm.SvmDetails.ExternalUUID
		}
	}
	if svmExternalUUID == "" {
		logger.Warnf("SVM external UUID not found for volume %s, skipping SMB teardown", volume.UUID)
		return teardown, nil
	}
	teardown.SvmExternalUUID = svmExternalUUID

	provider, err := getCifsServerProvider(ctx, node)
	if err != nil {
		return nil, err
	}

	restClient, err := provider.CreateRESTClient()
	if err != nil {
		logger.Errorf("failed to create REST client for SVM %s: %v", svmExternalUUID, err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	dns, err := restClient.NameServices().DNSGet(&ontapRest.DNSGetParams{SvmUUID: svmExternalUUID})
	if err != nil {
		if utilErrors.IsNotFoundErr(err) {
			logger.Debugf("DNS configuration not found for SVM %s", svmExternalUUID)
		} else {
			logger.Errorf("failed to fetch DNS configuration for SVM %s: %v", svmExternalUUID, err)
			return nil, vsaerrors.WrapAsTemporalApplicationError(err)
		}
	} else if dns != nil && dns.DynamicDNS != nil && dns.DynamicDNS.Fqdn != nil {
		teardown.FQDN = *dns.DynamicDNS.Fqdn
	}

	teardown.ShouldDelete = true
	logger.Debugf("SMB teardown context determined for volume %s in pool %d", volume.UUID, volume.PoolID)
	activity.RecordHeartbeat(ctx, "Finished DetermineSmbTeardownContext activity")
	return teardown, nil
}

func (va VolumeDeleteActivity) DeleteCifsServerIfUnused(ctx context.Context, teardownCtx *SmbTeardownContext, node *models.Node) error {
	logger := util.GetLogger(ctx)
	activity.RecordHeartbeat(ctx, "Starting DeleteCifsServerIfUnused activity")

	if teardownCtx == nil || node == nil {
		return nil
	}

	if !teardownCtx.ShouldDelete {
		logger.Info("Skipping CIFS server delete; SMB teardown not required", "volumeUUID", teardownCtx.VolumeUUID)
		return nil
	}

	ad := teardownCtx.ActiveDirectory
	if ad == nil {
		err := fmt.Errorf("active directory not provided in SMB teardown context")
		logger.Error("Active Directory missing in teardown context", "svmUUID", teardownCtx.SvmExternalUUID)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	if ad.CredentialPath == "" {
		err := fmt.Errorf("active directory credential path is empty")
		logger.Error("Active Directory credential path is empty", "poolID", teardownCtx.PoolID, "adUUID", ad.UUID)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	password, err := hyperscaler.GetPasswordFromCacheOrSecretManager(ctx, ad.CredentialPath)
	if err != nil {
		logger.Error("Failed to fetch Active Directory password", "adUUID", ad.UUID, "error", err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	provider, err := getCifsServerProvider(ctx, node)
	if err != nil {
		return err
	}

	if teardownCtx.SvmExternalUUID == "" {
		logger.Warn("SVM external UUID missing in teardown context", "volumeUUID", teardownCtx.VolumeUUID)
		return nil
	}

	if err := provider.DeleteCIFSServer(teardownCtx.SvmExternalUUID, ad.Username, password); err != nil {
		if utilErrors.IsNotFoundErr(err) {
			logger.Debugf("CIFS server already deleted for SVM %s", teardownCtx.SvmExternalUUID)
			return nil
		}
		logger.Errorf("failed to delete CIFS server for SVM %s: %v", teardownCtx.SvmExternalUUID, err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	activity.RecordHeartbeat(ctx, "Finished DeleteCifsServerIfUnused activity")
	logger.Infof("Deleted CIFS server for SVM %s", teardownCtx.SvmExternalUUID)
	return nil
}

func (va VolumeDeleteActivity) DeleteDnsRecordIfUnused(ctx context.Context, teardownCtx *SmbTeardownContext, node *models.Node) error {
	logger := util.GetLogger(ctx)
	activity.RecordHeartbeat(ctx, "Starting DeleteDnsRecordIfUnused activity")

	if teardownCtx == nil || node == nil {
		return nil
	}

	if !teardownCtx.ShouldDelete {
		logger.Debug("Skipping DNS record delete; SMB teardown not required", "volumeUUID", teardownCtx.VolumeUUID)
		return nil
	}

	if teardownCtx.SvmExternalUUID == "" {
		logger.Warn("SVM external UUID missing in teardown context", "volumeUUID", teardownCtx.VolumeUUID)
		return nil
	}

	provider, err := getCifsServerProvider(ctx, node)
	if err != nil {
		return err
	}

	restClient, err := provider.CreateRESTClient()
	if err != nil {
		logger.Errorf("failed to create REST client for SVM %s: %v", teardownCtx.SvmExternalUUID, err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	modifyParams := &ontapRest.DNSModifyParams{
		SvmUUID: teardownCtx.SvmExternalUUID,
		DDNSModifyParams: ontapRest.DDNSModifyParams{
			Enabled:   nillable.ToPointer(false),
			UseSecure: nillable.ToPointer(false),
		},
	}
	if teardownCtx.FQDN != "" {
		modifyParams.DDNSModifyParams.Fqdn = &teardownCtx.FQDN
	}

	if err := restClient.NameServices().DNSModify(modifyParams); err != nil {
		if utilErrors.IsNotFoundErr(err) {
			logger.Debugf("DNS record already deleted for SVM %s", teardownCtx.SvmExternalUUID)
			return nil
		}
		logger.Errorf("failed to delete DNS record for SVM %s: %v", teardownCtx.SvmExternalUUID, err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	activity.RecordHeartbeat(ctx, "Finished DeleteDnsRecordIfUnused activity")
	logger.Infof("Deleted DNS record for SVM %s", teardownCtx.SvmExternalUUID)
	return nil
}

func getCifsServerProvider(ctx context.Context, node *models.Node) (cifsServerProvider, error) {
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	cifsProvider, ok := provider.(cifsServerProvider)
	if !ok {
		err := fmt.Errorf("provider does not support CIFS operations")
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	return cifsProvider, nil
}

func (vda VolumeDeleteActivity) DeleteIgroupsFromBlockProperties(ctx context.Context, volume *datamodel.Volume, node *models.Node) error {
	logger := util.GetLogger(ctx)
	se := vda.SE
	activity.RecordHeartbeat(ctx, "Starting DeleteIgroupsFromBlockProperties activity")
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	for _, hostgroup := range volume.VolumeAttributes.BlockProperties.HostGroupDetails {
		volumesWithHG, err := se.GetAllVolumesForHG(ctx, hostgroup.HostGroupUUID, volume.AccountID)
		if err != nil {
			return vsaerrors.WrapAsTemporalApplicationError(err)
		}

		// We have to check if there is any other volume using this HG
		deleteHG := true
		for _, volumeWithHG := range volumesWithHG {
			if volume.PoolID == volumeWithHG.PoolID && volume.UUID != volumeWithHG.UUID {
				deleteHG = false
				break
			}
		}

		if !deleteHG {
			logger.Debugf("Hostgroup %s has attached volume, not deleting", hostgroup.HostGroupUUID)
			continue
		}

		hostgroupDB, err := se.GetHostGroup(ctx, hostgroup.HostGroupUUID, volume.AccountID)
		if err != nil {
			return vsaerrors.WrapAsTemporalApplicationError(err)
		}

		activity.RecordHeartbeat(ctx, "Fetching Igroup details")
		igroup, err := provider.IgroupGet(&hostgroupDB.Name, nil)
		if err != nil {
			if utilErrors.IsNotFoundErr(err) {
				logger.Debugf("IGroups %s is already deleted, skipping", hostgroup.HostGroupUUID)
				continue
			}
			return vsaerrors.WrapAsTemporalApplicationError(err)
		}
		if igroup != nil && igroup.UUID != nil {
			err = provider.IgroupDelete(*igroup.UUID)
			if err != nil {
				return vsaerrors.WrapAsTemporalApplicationError(err)
			}
		} else {
			logger.Debugf("Igroup %s not found for volume %s", hostgroup.HostGroupUUID, volume.UUID)
		}

		logger.Debug("Igroup deleted successfully", "name", hostgroup.HostGroupUUID)
	}
	activity.RecordHeartbeat(ctx, "Finished DeleteIgroupsFromBlockProperties activity")
	return nil
}

func (vda VolumeDeleteActivity) DeleteIgroups(ctx context.Context, volume *datamodel.Volume, node *models.Node) error {
	logger := util.GetLogger(ctx)
	activity.RecordHeartbeat(ctx, "Starting DeleteIgroups activity")
	se := vda.SE
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	for _, blockDevice := range *volume.VolumeAttributes.BlockDevices {
		for _, hostgroup := range blockDevice.HostGroupDetails {
			volumesWithHG, err := se.GetAllVolumesForHG(ctx, hostgroup.HostGroupUUID, volume.AccountID)
			if err != nil {
				return vsaerrors.WrapAsTemporalApplicationError(err)
			}

			// We have to check if there is any other volume using this HG
			deleteHG := true
			for _, volumeWithHG := range volumesWithHG {
				if volume.PoolID == volumeWithHG.PoolID && volume.UUID != volumeWithHG.UUID {
					deleteHG = false
					break
				}
			}

			if !deleteHG {
				logger.Debugf("Hostgroup %s has attached volume, not deleting", hostgroup.HostGroupUUID)
				continue
			}

			hostgroupDB, err := se.GetHostGroup(ctx, hostgroup.HostGroupUUID, volume.AccountID)
			if err != nil {
				return vsaerrors.WrapAsTemporalApplicationError(err)
			}

			activity.RecordHeartbeat(ctx, "Fetching Igroup details")
			igroup, err := provider.IgroupGet(&hostgroupDB.Name, nil)
			if err != nil {
				if utilErrors.IsNotFoundErr(err) {
					logger.Debugf("IGroups %s is already deleted, skipping", hostgroup.HostGroupUUID)
					continue
				}
				return vsaerrors.WrapAsTemporalApplicationError(err)
			}
			if igroup != nil && igroup.UUID != nil {
				err = provider.IgroupDelete(*igroup.UUID)
				if err != nil {
					return vsaerrors.WrapAsTemporalApplicationError(err)
				}
			} else {
				logger.Debugf("Igroup %s not found for volume %s", hostgroup.HostGroupUUID, volume.UUID)
			}

			logger.Debug("Igroup deleted successfully", "name", hostgroup.HostGroupUUID)
		}
	}
	activity.RecordHeartbeat(ctx, "Finished DeleteIgroups activity")
	return nil
}

func (vda VolumeDeleteActivity) DeleteExportPolicy(ctx context.Context, volume *datamodel.Volume, node *models.Node) error {
	logger := util.GetLogger(ctx)
	activity.RecordHeartbeat(ctx, "Starting DeleteExportPolicy activity")
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	if volume.VolumeAttributes.FileProperties.ExportPolicy.ExportPolicyName == "" {
		logger.Warnf("Volume %s has no export policy, skipping deletion", volume.Name)
		return nil
	}
	exportPolicyName := volume.VolumeAttributes.FileProperties.ExportPolicy.ExportPolicyName
	vsaExportPolicy := &vsa.ExportPolicy{
		ExportPolicyName: exportPolicyName,
		SvmName:          volume.Svm.Name,
	}
	err = provider.DeleteExportPolicy(vsaExportPolicy)
	if err != nil {
		if utilErrors.IsNotFoundErr(err) {
			logger.Warnf("Export policy %s not found for volume %s, skipping deletion", exportPolicyName, volume.Name)
			return nil
		}
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	activity.RecordHeartbeat(ctx, "Finished DeleteExportPolicy activity")
	logger.Infof("Export policy %s deleted successfully for volume %s", exportPolicyName, volume.Name)
	return nil
}
