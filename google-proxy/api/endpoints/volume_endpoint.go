package api

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"strings"

	"github.com/go-faster/jx"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"golang.org/x/exp/slog"
)

func (h Handler) V1betaDescribeVolume(ctx context.Context, params gcpgenserver.V1betaDescribeVolumeParams) (gcpgenserver.V1betaDescribeVolumeRes, error) {
	logger := utils.GetLoggerFromContext(ctx)
	volume, err := h.Orchestrator.GetVolume(ctx, params.VolumeId)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			return &gcpgenserver.V1betaDescribeVolumeNotFound{
				Code:    404,
				Message: "Volume not found",
			}, nil
		}
		logger.Error("Failed to describe volume", slog.String("error", err.Error()))
		return &gcpgenserver.V1betaDescribeVolumeInternalServerError{Code: 500, Message: "Internal server error"}, err
	}
	return convertToVolumeV1Beta(volume), nil
}

func (h Handler) V1betaCreateVolume(ctx context.Context, req *gcpgenserver.VolumeCreateV1beta, params gcpgenserver.V1betaCreateVolumeParams) (gcpgenserver.V1betaCreateVolumeRes, error) {
	logger := utils.GetLoggerFromContext(ctx)
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

		logger.Error("Failed to create volume", slog.String("error", err.Error()))
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

		logger.Error("Failed to create volume", slog.String("error", err.Error()))
		return &gcpgenserver.V1betaCreateVolumeInternalServerError{Code: 500, Message: err.Error()}, err
	}

	resp, err := encodeVolumeV1(convertToVolumeV1Beta(volume))
	if err != nil {
		return nil, err
	}

	operationID := "/v1beta/projects/" + params.ProjectNumber + "/locations/" + params.LocationId + "/operations/" + jobUUID
	if volume.LifeCycleState == models.LifeCycleStateCreating {
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
		CreationToken: req.Volume.CreationToken,
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
	logger := utils.GetLoggerFromContext(ctx)

	volume, err := h.Orchestrator.GetVolume(ctx, params.VolumeId)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			operationID := "/v1beta/projects/" + params.ProjectNumber + "/locations/" + params.LocationId + "/operations/" + uuid.UUID{}.String()
			return &gcpgenserver.OperationV1beta{
				Name: gcpgenserver.NewOptString(operationID),
				Done: gcpgenserver.NewOptBool(true),
			}, nil
		}
		logger.Error("Failed to delete volume", slog.String("error", err.Error()))
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
		logger.Error("Failed to delete volume", slog.String("error", err.Error()))
		return &gcpgenserver.V1betaDeleteVolumeInternalServerError{
			Code:    500,
			Message: "Internal server error",
		}, err
	}

	resp, err := encodeVolumeV1(convertToVolumeV1Beta(volume))
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

func convertToVolumeV1Beta(volume *models.Volume) *gcpgenserver.VolumeV1beta {
	res := &gcpgenserver.VolumeV1beta{
		VolumeId:           gcpgenserver.NewOptString(volume.UUID),
		ResourceId:         volume.DisplayName,
		Created:            gcpgenserver.NewOptDateTime(volume.CreatedAt),
		VolumeStateDetails: gcpgenserver.NewOptString(volume.LifeCycleStateDetails),
		VolumeState:        gcpgenserver.NewOptVolumeV1betaVolumeState(gcpgenserver.VolumeV1betaVolumeState(strings.ToUpper(volume.LifeCycleState))),
		Network:            gcpgenserver.NewOptString(volume.VendorSubnetID),
		Description:        gcpgenserver.NewOptNilString(volume.Description),
		PoolId:             gcpgenserver.NewNilString(volume.PoolID),
		CreationToken:      volume.CreationToken,
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
				OsType:       gcpgenserver.NewOptBlockVolumeOSTypeV1beta(gcpgenserver.BlockVolumeOSTypeV1beta(volume.BlockProperties.OSType)),
				HostGroupIds: volume.BlockProperties.HostGroupUUIDs,
			})

		res.MountPoints = make([]gcpgenserver.MountPointV1beta, 0)

		res.MountPoints = append(res.MountPoints, gcpgenserver.MountPointV1beta{
			IpAddress:    gcpgenserver.NewOptString(volume.IPAddress),
			Protocol:     gcpgenserver.NewOptProtocolsV1beta(gcpgenserver.ProtocolsV1betaISCSI),
			Instructions: getMountInstructions(volume.BlockProperties.OSType, volume.IPAddress),
		})
	}

	return res
}

func getMountInstructions(osType string, ipAddress string) gcpgenserver.OptString {
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
$ sudo mkdir /mnt/icsi-oras-u02
$ sudo mount /dev/sdb /mnt/icsci-oras-u02
To mount automatically on reboot, add to /etc/stab:
$ /dev/sdb /ant/iscs1-oras-u02 ext4 defaults 0 0`, ipAddress, ipAddress)
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

func (h Handler) V1betaGetMultipleVolumes(ctx context.Context, req *gcpgenserver.VolumeIDListV1beta, params gcpgenserver.V1betaGetMultipleVolumesParams) (gcpgenserver.V1betaGetMultipleVolumesRes, error) {
	return nil, nil
}
