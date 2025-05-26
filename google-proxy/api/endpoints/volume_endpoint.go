package api

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/go-faster/jx"
	"github.com/google/uuid"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/volumes"
	cvpmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	createCVPClient               = cvp.CreateClient
	convertVolumeV1betaToCVPModel = _convertVolumeV1betaCVPToModel
	getMultipleVolumesFromCVP     = _getMultipleVolumesFromCVP
)

func (h Handler) V1betaDescribeVolume(ctx context.Context, params gcpgenserver.V1betaDescribeVolumeParams) (gcpgenserver.V1betaDescribeVolumeRes, error) {
	logger := util.GetLogger(ctx)
	volume, err := h.Orchestrator.GetVolume(ctx, params.VolumeId)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			return &gcpgenserver.V1betaDescribeVolumeNotFound{
				Code:    404,
				Message: "Volume not found",
			}, nil
		}
		logger.Error("Failed to describe volume", "error", err.Error())
		return &gcpgenserver.V1betaDescribeVolumeInternalServerError{Code: 500, Message: "Internal server error"}, err
	}
	return convertModelToVCPVolume(volume), nil
}

func (h Handler) V1betaCreateVolume(ctx context.Context, req *gcpgenserver.VolumeCreateV1beta, params gcpgenserver.V1betaCreateVolumeParams) (gcpgenserver.V1betaCreateVolumeRes, error) {
	logger := util.GetLogger(ctx)
	region, _, parsingErr := utils.ParseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaCreateVolumeBadRequest{
			Code:    parsingErr.Code,
			Message: parsingErr.Message,
		}, nil
	}

	param, err := prepareCreateVolumeParams(req, params, region)
	if err != nil {
		if errors.IsUserInputValidationErr(err) || errors.IsNotFoundErr(err) {
			return &gcpgenserver.V1betaCreateVolumeBadRequest{
				Code:    400,
				Message: err.Error(),
			}, nil
		}

		logger.Error("Failed to create volume", "error", err.Error())
		return &gcpgenserver.V1betaCreateVolumeInternalServerError{Code: 500, Message: err.Error()}, err
	}

	volume, jobUUID, err := h.Orchestrator.CreateVolume(ctx, param)
	if err != nil {
		if errors.IsUserInputValidationErr(err) || errors.IsNotFoundErr(err) {
			return &gcpgenserver.V1betaCreateVolumeBadRequest{
				Code:    400,
				Message: err.Error(),
			}, nil
		}

		logger.Error("Failed to create volume", "error", err.Error())
		return &gcpgenserver.V1betaCreateVolumeInternalServerError{Code: 500, Message: err.Error()}, err
	}

	resp, err := encodeVolumeV1(convertModelToVCPVolume(volume))
	if err != nil {
		return nil, err
	}

	operationID := "/v1beta/projects/" + params.ProjectNumber + "/locations/" + params.LocationId + "/operations/" + jobUUID
	if volume.LifeCycleState == models.LifeCycleStateCreatingDetails {
		return &gcpgenserver.OperationV1beta{
			Name:     gcpgenserver.NewOptString(operationID),
			Response: resp,
			Done:     gcpgenserver.NewOptBool(false),
		}, nil
	}
	return &gcpgenserver.OperationV1beta{
		Name:     gcpgenserver.NewOptString(operationID),
		Response: resp,
		Done:     gcpgenserver.NewOptBool(true),
	}, nil
}

func prepareCreateVolumeParams(req *gcpgenserver.VolumeCreateV1beta, params gcpgenserver.V1betaCreateVolumeParams, region string) (*common.CreateVolumeParams, error) {
	vendorId := fmt.Sprintf("/projects/%v/locations/%v/volumes/%s", params.ProjectNumber, params.LocationId, req.Volume.ResourceId)

	param := &common.CreateVolumeParams{
		AccountName:   params.ProjectNumber,
		Region:        region,
		Name:          req.Volume.ResourceId,
		VendorID:      vendorId,
		CreationToken: req.Volume.CreationToken.Value,
		PoolID:        req.Volume.PoolId.Value,
		QuotaInBytes:  uint64(req.Volume.QuotaInBytes.Value),
		Protocols:     make([]string, 0),
	}
	if req.Volume.Description.IsSet() {
		param.Description, _ = req.Volume.Description.Get()
	}
	if req.Volume.Network.IsSet() {
		param.Network, _ = req.Volume.Network.Get()
	}

	for _, protocol := range req.Volume.GetProtocols() {
		protocolStr, err := protocol.MarshalText()
		if err != nil {
			return nil, err
		}
		if protocol != gcpgenserver.ProtocolsV1betaISCSI {
			return nil, errors.NewUserInputValidationErr("only ISCSI protocol is supported")
		}
		param.Protocols = append(param.Protocols, string(protocolStr))
	}

	if req.Volume.BlockProperties.IsSet() {
		reqBlockProperties, _ := req.Volume.BlockProperties.Get()
		if reqBlockProperties.OsType.IsSet() {
			osType := reqBlockProperties.GetOsType()
			param.BlockProperties = &models.BlockProperties{
				OSType:         string(osType.Value),
				HostGroupUUIDs: reqBlockProperties.GetHostGroupIds(),
			}
		}
	}
	return param, nil
}

func (h Handler) V1betaUpdateVolume(ctx context.Context, req *gcpgenserver.VolumeUpdateV1beta, params gcpgenserver.V1betaUpdateVolumeParams) (gcpgenserver.V1betaUpdateVolumeRes, error) {
	panic("implement me")
}

func (h Handler) V1betaDeleteVolume(ctx context.Context, req gcpgenserver.OptV1betaDeleteVolumeReq, params gcpgenserver.V1betaDeleteVolumeParams) (gcpgenserver.V1betaDeleteVolumeRes, error) {
	logger := util.GetLogger(ctx)

	volume, err := h.Orchestrator.GetVolume(ctx, params.VolumeId)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			operationID := "/v1beta/projects/" + params.ProjectNumber + "/locations/" + params.LocationId + "/operations/" + uuid.UUID{}.String()
			return &gcpgenserver.OperationV1beta{
				Name: gcpgenserver.NewOptString(operationID),
				Done: gcpgenserver.NewOptBool(true),
			}, nil
		}
		logger.Error("Failed to delete volume", "error", err.Error())
		return &gcpgenserver.V1betaDeleteVolumeInternalServerError{
			Code:    500,
			Message: "Internal server error",
		}, err
	}

	if volume != nil && volume.LifeCycleState == models.LifeCycleStateDeleting {
		msg := "Error deleting volume - Volume is already transitioning between states"
		return &gcpgenserver.V1betaDeleteVolumeConflict{
			Code:    409,
			Message: msg,
		}, err
	}

	volume, jobUUID, err := h.Orchestrator.DeleteVolume(ctx, params.VolumeId)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			operationID := "/v1beta/projects/" + params.ProjectNumber + "/locations/" + params.LocationId + "/operations/" + uuid.UUID{}.String()
			return &gcpgenserver.OperationV1beta{
				Name: gcpgenserver.NewOptString(operationID),
				Done: gcpgenserver.NewOptBool(true),
			}, nil
		}
		logger.Error("Failed to delete volume", "error", err.Error())
		return &gcpgenserver.V1betaDeleteVolumeInternalServerError{
			Code:    500,
			Message: "Internal server error",
		}, err
	}

	resp, err := encodeVolumeV1(convertModelToVCPVolume(volume))
	if err != nil {
		return nil, err
	}

	operationID := "/v1beta/projects/" + params.ProjectNumber + "/locations/" + params.LocationId + "/operations/" + jobUUID
	if volume.LifeCycleState == models.LifeCycleStateDeleting {
		return &gcpgenserver.OperationV1beta{
			Name:     gcpgenserver.NewOptString(operationID),
			Response: resp,
			Done:     gcpgenserver.NewOptBool(false),
		}, nil
	}
	return &gcpgenserver.OperationV1beta{
		Name:     gcpgenserver.NewOptString(operationID),
		Response: resp,
		Done:     gcpgenserver.NewOptBool(true),
	}, nil
}

// encodeVolumeV1 encodes a PoolV1 struct to JSON.
func encodeVolumeV1(volumeV1beta *gcpgenserver.VolumeV1beta) (jx.Raw, error) {
	data, err := json.Marshal(volumeV1beta)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func convertModelToVCPVolume(volume *models.Volume) *gcpgenserver.VolumeV1beta {
	res := &gcpgenserver.VolumeV1beta{
		VolumeId:           gcpgenserver.NewOptString(volume.UUID),
		ResourceId:         volume.DisplayName,
		Created:            gcpgenserver.NewOptDateTime(volume.CreatedAt),
		VolumeStateDetails: gcpgenserver.NewOptString(volume.LifeCycleStateDetails),
		VolumeState:        gcpgenserver.NewOptVolumeV1betaVolumeState(gcpgenserver.VolumeV1betaVolumeState(strings.ToUpper(volume.LifeCycleState))),
		Network:            gcpgenserver.NewOptString(volume.VendorSubnetID),
		Description:        gcpgenserver.NewOptNilString(volume.Description),
		PoolId:             gcpgenserver.NewNilString(volume.PoolID),
		CreationToken:      gcpgenserver.NewOptString(volume.CreationToken),
		QuotaInBytes:       gcpgenserver.NewOptFloat64(float64(volume.QuotaInBytes)),
		PoolResourceId:     gcpgenserver.NewOptNilString(volume.PoolName),
		StorageClass:       gcpgenserver.NewOptStorageClassV1beta(gcpgenserver.StorageClassV1betaSOFTWARE),
	}
	if volume.DeletedAt != nil {
		res.Deleted = gcpgenserver.OptNilDateTime{Value: *volume.DeletedAt}
	}

	res.Protocols = make([]gcpgenserver.ProtocolsV1beta, 0)
	for _, value := range volume.ProtocolTypes {
		var protocolsV1beta gcpgenserver.ProtocolsV1beta
		err := protocolsV1beta.UnmarshalText([]byte(value))
		if err != nil {
			return nil
		}
		res.Protocols = append(res.Protocols, protocolsV1beta)
	}

	if volume.BlockProperties != nil {
		res.BlockProperties = gcpgenserver.NewOptBlockPropertiesV1beta(
			gcpgenserver.BlockPropertiesV1beta{
				OsType:       gcpgenserver.NewOptBlockPropertiesV1betaOsType(gcpgenserver.BlockPropertiesV1betaOsType(volume.BlockProperties.OSType)),
				HostGroupIds: volume.BlockProperties.HostGroupUUIDs,
			})

		if volume.LifeCycleState == string(gcpgenserver.VolumeV1betaVolumeStateREADY) {
			res.MountPoints = make([]gcpgenserver.MountPointV1beta, 0)
			res.MountPoints = append(res.MountPoints, gcpgenserver.MountPointV1beta{
				IpAddress:    gcpgenserver.NewOptString(volume.IPAddress),
				Protocol:     gcpgenserver.NewOptProtocolsV1beta(gcpgenserver.ProtocolsV1betaISCSI),
				Instructions: getMountInstructions(volume.BlockProperties.OSType, volume.IPAddress, volume.DisplayName),
			})
		}
	}

	return res
}

func getMountInstructions(osType string, ipAddress string, volName string) gcpgenserver.OptString {
	instructions := ""
	switch osType {
	case "LINUX":
		instructions = fmt.Sprintf(`Mount instructions for iSCSI target on Linux
1. Install the ISCSI initiator on your host
On Red Hat Enterprise Linux or SUSE Linux:
$ sudo yum install y iscsi-initiator-utils
On Ubuntu or Debian instances:
$ sudo apt-get install open-iscsi
2. Discover the ISCSi target
Use the target IP address and port (default 3260).
$ sudo iscsind -m discovery -t sendtargets -p %s:3260
3. Log in to the ISCSI target
Use the target initiator with the provided IQN.
$ sudo iscsiadm -m node -T <<target-iqn>> -p %s:3260 -l
4. Identify the LUN on your host
After logging in, rescan for new devices:
$ rescan-scsi-bus.sh
Check for the new device (e.g.. /dev/sdb):
$ lsblk
5. Format and mount the LUN (if needed).
If the LUN doesn't have a filesystem, create one (e.g, ext4):
$ sudo mkfs.ext4 /dev/sdb
Create a mount point and mount the device:
$ sudo mkdir /mnt/%s
$ sudo mount /dev/sdb /mnt/%s
To mount automatically on reboot, add to /etc/stab:
$ /dev/sdb /mnt/%s ext4 defaults 0 0`, ipAddress, ipAddress, "lun_"+volName, "lun_"+volName, "lun_"+volName)
		return gcpgenserver.NewOptString(instructions)
	case "WINDOWS":
		instructions = `Mount instructions for iSCS target on Windows
1. Enable the ISCSI initiator on your Windows host
• Open the Start menu and search for ISCSI Initiator.
• If prompted to start the service, click Yes to enable the Microsoft ISCSI Initiator Service.
2. Discover the iSCSi target
• In the iSCSI Initiator window, go to the Discovery tab.
• Click Discover Portal, enter the target IP address from the Target details section (default port 3260), and click OK.
3. Connect to the iSCSI target
• Use the target IQN from the Target details section in the list of discovered
targets.
• Select the target and click Connect.
• In the Connect dialog, check Enable muiti-path (if using multipathing) and click OK.
4. Initialize and format the LUN
• Open Disk Management (right-click Start > Disk Management).
• The new disk (LUN ID 0) should appear as an uninitialized disk
• Right-click the disk, select initialize Disk, and choose GPT or MBR(GPT recommended).
• Right-click the unallocated space, select New Simple Volume, and follow the wizard to format the disk (e.g. with NTFS) and assign a drive letter (e.g.. D:).`
		return gcpgenserver.NewOptString(instructions)

	case "ESXI":
		instructions = `Mount instructions for iSCSI target on on VMware ESXi
1. Enable the ISCSI initiator on your ESXi host.
• Log in to the Sphere Client and select your ESXi host.
• Navigate to Configure > Storage Adapters.
• Select the ISCSI Software Adapter (e.g., vmhbaXX) and click Properties.
• Under General, click Enable to activate the iSCSI initiator.
2. Add the target IP address for discovery
• In the ISCSI Software Adapter properties, go to the Dynamic Discovery tab.
• Click Add and enter the target IP address from the Target details section.
• Leave the port as 3260 (default) and click OK.
3. Rescan the iSCSI adapter to discover the target
• In the Storage Adapters view, select the iSCSI Software Adapter and click Rescan.
• The target IQN from the Target details section should appear under Targets.
3. Verify the LUN is visible and create a datestore.
• Go to Configure > Storage Devices to confirm the LUN (ID 0) is listed.
• Navigate to Datastores and click New Datastore.
• Select VMFS, name the datastore (e.g., iscsi-oras-u02), and choose the ISCSI LUN (LUN ID 0).
• Follow the wizard to format the LUN with VMFS (e.g., VMFS 6) and
complete the setup.`
	}
	return gcpgenserver.NewOptString(instructions)
}

func (h Handler) V1betaGetMultipleVolumes(ctx context.Context, req *gcpgenserver.VolumeIdListV1beta, params gcpgenserver.V1betaGetMultipleVolumesParams) (gcpgenserver.V1betaGetMultipleVolumesRes, error) {
	logger := util.GetLogger(ctx)
	volumesModelVCP, err := h.Orchestrator.GetMultipleVolumes(ctx, req.VolumeUuids, params.ProjectNumber)
	if err != nil {
		logger.Error("Failed to fetch volume", "error", err.Error())
		return &gcpgenserver.V1betaGetMultipleVolumesInternalServerError{Code: 500, Message: "Internal server error"}, err
	}

	volumesVCP := make([]gcpgenserver.VolumeV1beta, 0)
	for _, vol := range volumesModelVCP {
		response := convertModelToVCPVolume(vol)
		volumesVCP = append(volumesVCP, *response)
	}

	return getMultipleVolumesFromCVP(ctx, req, params, volumesVCP)
}

func _getMultipleVolumesFromCVP(ctx context.Context, req *gcpgenserver.VolumeIdListV1beta, params gcpgenserver.V1betaGetMultipleVolumesParams, vcpVolumes []gcpgenserver.VolumeV1beta) (gcpgenserver.V1betaGetMultipleVolumesRes, error) {
	logger := util.GetLogger(ctx)
	jwtToken := utils.GetJWTTokenFromContext(ctx)
	cvpClient := createCVPClient(logger, jwtToken)

	getMultipleVolumesParams := &volumes.V1betaGetMultipleVolumesParams{
		LocationID:    params.LocationId,
		ProjectNumber: params.ProjectNumber,
		Body: &cvpmodels.VolumeIDListV1beta{
			VolumeUUIDs: req.GetVolumeUuids(),
		},
	}
	if params.XCorrelationID.IsSet() {
		getMultipleVolumesParams.XCorrelationID = &params.XCorrelationID.Value
	}

	res, err := cvpClient.Volumes.V1betaGetMultipleVolumes(getMultipleVolumesParams)
	if err != nil {
		switch e := err.(type) {
		case *volumes.V1betaGetMultipleVolumesBadRequest:
			return &gcpgenserver.V1betaGetMultipleVolumesBadRequest{
				Code:    e.Payload.Code,
				Message: e.Payload.Message,
			}, nil
		case *volumes.V1betaGetMultipleVolumesUnprocessableEntity:
			return &gcpgenserver.V1betaGetMultipleVolumesUnprocessableEntity{
				Code:    e.Payload.Code,
				Message: e.Payload.Message,
			}, nil
		case *volumes.V1betaGetMultipleVolumesUnauthorized:
			return &gcpgenserver.V1betaGetMultipleVolumesUnauthorized{
				Code:    e.Payload.Code,
				Message: e.Payload.Message,
			}, nil
		case *volumes.V1betaGetMultipleVolumesForbidden:
			return &gcpgenserver.V1betaGetMultipleVolumesForbidden{
				Code:    e.Payload.Code,
				Message: e.Payload.Message,
			}, nil
		case *volumes.V1betaGetMultipleVolumesNotFound:
			return &gcpgenserver.V1betaGetMultipleVolumesNotFound{
				Code:    e.Payload.Code,
				Message: e.Payload.Message,
			}, nil
		case *volumes.V1betaGetMultipleVolumesTooManyRequests:
			return &gcpgenserver.V1betaGetMultipleVolumesTooManyRequests{
				Code:    e.Payload.Code,
				Message: e.Payload.Message,
			}, nil
		case *volumes.V1betaGetMultipleVolumesDefault:
			return &gcpgenserver.V1betaGetMultipleVolumesInternalServerError{
				Code:    e.Payload.Code,
				Message: e.Payload.Message,
			}, nil
		}
	}
	if res == nil || res.Payload == nil {
		return &gcpgenserver.V1betaGetMultipleVolumesInternalServerError{
			Code:    500,
			Message: "unknown error during get multiple volumes operation",
		}, nil
	}

	volumesList := make([]gcpgenserver.VolumeV1beta, 0)
	for _, vol := range res.GetPayload().Volumes {
		response := convertVolumeV1betaToCVPModel(vol)
		volumesList = append(volumesList, response)
	}

	if vcpVolumes != nil {
		volumesList = append(volumesList, vcpVolumes...)
	}

	return &gcpgenserver.V1betaGetMultipleVolumesOK{
		Volumes: volumesList,
	}, nil
}

func _convertVolumeV1betaCVPToModel(in *cvpmodels.VolumeV1beta) gcpgenserver.VolumeV1beta {
	var deletedAt time.Time
	if in.Deleted != nil {
		deletedAt = time.Time(*in.Deleted)
	}
	volume := gcpgenserver.VolumeV1beta{
		ResourceId:              *in.ResourceID,
		VolumeId:                gcpgenserver.NewOptString(in.VolumeID),
		Created:                 gcpgenserver.NewOptDateTime(time.Time(in.Created)),
		Deleted:                 gcpgenserver.NewOptNilDateTime(deletedAt),
		VolumeState:             gcpgenserver.NewOptVolumeV1betaVolumeState(gcpgenserver.VolumeV1betaVolumeState(in.VolumeState)),
		VolumeStateDetails:      gcpgenserver.NewOptString(in.VolumeStateDetails),
		SecurityStyle:           gcpgenserver.NewOptVolumeV1betaSecurityStyle(gcpgenserver.VolumeV1betaSecurityStyle(in.SecurityStyle)),
		ServiceLevel:            gcpgenserver.NewOptVolumeV1betaServiceLevel(gcpgenserver.VolumeV1betaServiceLevel(in.ServiceLevel)),
		EncryptionType:          gcpgenserver.NewOptVolumeV1betaEncryptionType(gcpgenserver.VolumeV1betaEncryptionType(in.EncryptionType)),
		Network:                 gcpgenserver.NewOptString(in.Network),
		Zone:                    gcpgenserver.NewOptString(in.Zone),
		UsedBytes:               utils.SafeFloat64(in.UsedBytes),
		IsOnPremMigration:       utils.SafeBool(in.IsOnPremMigration),
		Description:             utils.SafeString(in.Description),
		KerberosEnabled:         utils.SafeBool(in.KerberosEnabled),
		LdapEnabled:             utils.SafeBool(in.LdapEnabled),
		UnixPermissions:         utils.SafeString(in.UnixPermissions),
		SecondaryZone:           utils.SafeString(in.SecondaryZone),
		MultipleEndpoints:       utils.SafeBool(in.MultipleEndpoints),
		LargeCapacity:           utils.SafeBool(in.LargeCapacity),
		QuotaInBytes:            utils.SafeOptFloat64(in.QuotaInBytes),
		SnapReserve:             utils.SafeOptFloat64(in.SnapReserve),
		PoolResourceId:          utils.SafeString(in.PoolResourceID),
		ActiveDirectoryConfigId: utils.SafeString(in.ActiveDirectoryConfigID),
		SnapshotDirectory:       utils.GetOptBool(in.SnapshotDirectory),
		KmsConfigId:             utils.SafeString(in.KmsConfigID),
		KmsConfigResourceId:     utils.SafeString(in.KmsConfigResourceID),
		ColdTierSizeGib:         utils.SafeFloat64(in.ColdTierSizeGib),
		InReplication:           utils.GetOptBool(in.InReplication),
		IsDataProtection:        utils.GetOptBool(in.IsDataProtection),
		CreationToken:           utils.GetOptString(in.CreationToken),
	}

	if in.ExportPolicy != nil {
		exportPolicyV1beta := gcpgenserver.ExportPolicyV1beta{}
		if in.ExportPolicy.Rules != nil {
			exportPolicyV1beta.Rules = make([]gcpgenserver.SimpleExportPolicyRuleV1beta, 0)
			for _, rule := range in.ExportPolicy.Rules {
				exportRule := gcpgenserver.SimpleExportPolicyRuleV1beta{
					Kerberos5ReadOnly:   utils.SafeBool(rule.Kerberos5ReadOnly),
					Kerberos5ReadWrite:  utils.SafeBool(rule.Kerberos5ReadWrite),
					Kerberos5iReadOnly:  utils.SafeBool(rule.Kerberos5iReadOnly),
					Kerberos5iReadWrite: utils.SafeBool(rule.Kerberos5iReadWrite),
					Kerberos5pReadOnly:  utils.SafeBool(rule.Kerberos5pReadOnly),
					Kerberos5pReadWrite: utils.SafeBool(rule.Kerberos5pReadWrite),
					Nfsv3:               utils.SafeBool(rule.Nfsv3),
					Nfsv4:               utils.SafeBool(rule.Nfsv4),
				}
				if rule.AccessType != nil {
					exportRule.AccessType = gcpgenserver.SimpleExportPolicyRuleV1betaAccessType(*rule.AccessType)
				}

				if rule.AllowedClients != nil {
					exportRule.AllowedClients = *rule.AllowedClients
				}

				if rule.HasRootAccess != nil {
					exportRule.HasRootAccess = gcpgenserver.NewOptNilSimpleExportPolicyRuleV1betaHasRootAccess(gcpgenserver.SimpleExportPolicyRuleV1betaHasRootAccess(*rule.HasRootAccess))
				}

				exportPolicyV1beta.Rules = append(exportPolicyV1beta.Rules, exportRule)
			}
		}
		volume.ExportPolicy = gcpgenserver.NewOptExportPolicyV1beta(exportPolicyV1beta)
	} else {
		volume.ExportPolicy = gcpgenserver.NewOptExportPolicyV1beta(gcpgenserver.ExportPolicyV1beta{})
	}

	volume.RestrictedActions = make(gcpgenserver.RestrictedActionsV1beta, len(in.RestrictedActions))
	for _, val := range in.RestrictedActions {
		volume.RestrictedActions = append(volume.RestrictedActions, gcpgenserver.RestrictedActionsV1betaItem(val))
	}

	if in.BackupConfig != nil {
		backupConfigV1beta := gcpgenserver.BackupConfigV1beta{
			BackupPolicyId:   utils.SafeString(in.BackupConfig.BackupPolicyID),
			BackupVaultId:    utils.SafeString(in.BackupConfig.BackupVaultID),
			BackupChainBytes: utils.SafeInt64(in.BackupConfig.BackupChainBytes),
		}

		volume.BackupConfig = gcpgenserver.NewOptBackupConfigV1beta(backupConfigV1beta)
	}

	if in.Labels != nil {
		labels := gcpgenserver.VolumeV1betaLabels{}
		for key, value := range in.Labels {
			labels[key] = value
		}
		volume.Labels = gcpgenserver.NewOptVolumeV1betaLabels(labels)
	}

	if in.Protocols != nil {
		for _, protocol := range in.Protocols {
			var protocolV1beta gcpgenserver.ProtocolsV1beta
			err := protocolV1beta.UnmarshalText([]byte(protocol))
			if err != nil {
				return volume
			}
			volume.Protocols = append(volume.Protocols, protocolV1beta)
		}
	}

	if in.StorageClass != nil {
		volume.StorageClass = gcpgenserver.NewOptStorageClassV1beta(gcpgenserver.StorageClassV1beta(*in.StorageClass))
	}

	if in.TieringPolicy != nil {
		tieringPolicyV1beta := gcpgenserver.TieringPolicyV1beta{
			TierAction: gcpgenserver.NewOptNilTieringPolicyV1betaTierAction(gcpgenserver.TieringPolicyV1betaTierAction(*in.TieringPolicy.TierAction)),
		}
		volume.TieringPolicy = gcpgenserver.NewOptTieringPolicyV1beta(tieringPolicyV1beta)
	}

	var snapshotPolicy *gcpgenserver.SnapshotPolicyV1beta
	if in.SnapshotPolicy != nil {
		if in.SnapshotPolicy.Enabled != nil && *in.SnapshotPolicy.Enabled {
			var hourlySchedule *gcpgenserver.HourlyScheduleV1beta
			if in.SnapshotPolicy.HourlySchedule != nil {
				hourlySchedule = &gcpgenserver.HourlyScheduleV1beta{
					Minute:          utils.SafeOptFloat64(in.SnapshotPolicy.HourlySchedule.Minute),
					SnapshotsToKeep: utils.SafeOptFloat64(in.SnapshotPolicy.HourlySchedule.SnapshotsToKeep),
				}
			}

			var dailySchedule *gcpgenserver.DailyScheduleV1beta
			if in.SnapshotPolicy.DailySchedule != nil {
				dailySchedule = &gcpgenserver.DailyScheduleV1beta{
					Hour:            utils.SafeOptFloat64(in.SnapshotPolicy.DailySchedule.Hour),
					Minute:          utils.SafeOptFloat64(in.SnapshotPolicy.DailySchedule.Minute),
					SnapshotsToKeep: utils.SafeOptFloat64(in.SnapshotPolicy.DailySchedule.SnapshotsToKeep),
				}
			}

			var weeklySchedule *gcpgenserver.WeeklyScheduleV1beta
			if in.SnapshotPolicy.WeeklySchedule != nil {
				weeklySchedule = &gcpgenserver.WeeklyScheduleV1beta{
					Day:             gcpgenserver.NewOptString(in.SnapshotPolicy.WeeklySchedule.Day),
					Hour:            utils.SafeOptFloat64(in.SnapshotPolicy.WeeklySchedule.Hour),
					Minute:          utils.SafeOptFloat64(in.SnapshotPolicy.WeeklySchedule.Minute),
					SnapshotsToKeep: utils.SafeOptFloat64(in.SnapshotPolicy.WeeklySchedule.SnapshotsToKeep),
				}
			}

			var monthlySchedule *gcpgenserver.MonthlyScheduleV1beta
			if in.SnapshotPolicy.MonthlySchedule != nil {
				monthlySchedule = &gcpgenserver.MonthlyScheduleV1beta{
					DaysOfMonth:     gcpgenserver.NewOptString(in.SnapshotPolicy.MonthlySchedule.DaysOfMonth),
					Hour:            utils.SafeOptFloat64(in.SnapshotPolicy.MonthlySchedule.Hour),
					Minute:          utils.SafeOptFloat64(in.SnapshotPolicy.MonthlySchedule.Minute),
					SnapshotsToKeep: utils.SafeOptFloat64(in.SnapshotPolicy.MonthlySchedule.SnapshotsToKeep),
				}
			}

			snapshotPolicy = &gcpgenserver.SnapshotPolicyV1beta{
				DailySchedule:   gcpgenserver.NewOptDailyScheduleV1beta(*dailySchedule),
				WeeklySchedule:  gcpgenserver.NewOptWeeklyScheduleV1beta(*weeklySchedule),
				MonthlySchedule: gcpgenserver.NewOptMonthlyScheduleV1beta(*monthlySchedule),
				HourlySchedule:  gcpgenserver.NewOptHourlyScheduleV1beta(*hourlySchedule),
				Enabled:         utils.SafeBool(in.SnapshotPolicy.Enabled),
			}
		}
		volume.SnapshotPolicy = gcpgenserver.NewOptSnapshotPolicyV1beta(*snapshotPolicy)
	}

	volume.SmbSettings = make(gcpgenserver.SMBSettingsV1beta, 0)
	for _, val := range in.SmbSettings {
		volume.SmbSettings = append(volume.SmbSettings, gcpgenserver.SMBSettingsV1betaItem(val))
	}

	volume.MountPoints = make([]gcpgenserver.MountPointV1beta, len(in.MountPoints))
	for i, mp := range in.MountPoints {
		volume.MountPoints[i] = gcpgenserver.MountPointV1beta{
			Export:       gcpgenserver.NewOptString(mp.Export),
			ExportFull:   gcpgenserver.NewOptString(mp.ExportFull),
			IpAddress:    gcpgenserver.NewOptString(mp.IPAddress),
			Instructions: gcpgenserver.NewOptString(mp.Instructions),
			Protocol:     gcpgenserver.NewOptProtocolsV1beta(gcpgenserver.ProtocolsV1beta(mp.Protocol)),
		}
	}

	if in.PoolID != nil {
		volume.PoolId = gcpgenserver.NewNilString(*in.PoolID)
	}

	return volume
}
