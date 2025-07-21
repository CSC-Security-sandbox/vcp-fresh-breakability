package api

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/go-faster/jx"
	"github.com/google/uuid"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/volumes"
	cvpmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	ontapmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/helper"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	createCVPClient               = cvp.CreateClient
	convertVolumeV1betaToCVPModel = _convertVolumeV1betaCVPToModel
	getMultipleVolumesFromCVP     = _getMultipleVolumesFromCVP
	prepareUpdateVolumeParams     = _prepareUpdateVolumeParams
	prepareCreateVolumeParams     = _prepareCreateVolumeParams
	autoTieringEnabled            = env.GetBool("AUTO_TIERING_ENABLED", false)
)

const (
	volumeTypeSecondary          = "SECONDARY"
	SnapshotScheduleLabelHourly  = "hourly"
	SnapshotScheduleLabelDaily   = "daily"
	SnapshotScheduleLabelWeekly  = "weekly"
	SnapshotScheduleLabelMonthly = "monthly"
	MaxBackupPathComponents      = 8

	daysOfMonthError = `daysOfMonth must include unique values in the range 1-31 (inclusive).`
	daysOfWeekError  = `day in weeklySchedule must include 1-7 (inclusive) unique weekdays, that are comma separated.`
)

func (h Handler) V1betaDescribeVolume(ctx context.Context, params gcpgenserver.V1betaDescribeVolumeParams) (gcpgenserver.V1betaDescribeVolumeRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
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

func (h Handler) V1betaGetVolumeCount(ctx context.Context, params gcpgenserver.V1betaGetVolumeCountParams) (gcpgenserver.V1betaGetVolumeCountRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	count, err := h.Orchestrator.GetVolumeCount(ctx, params.ProjectNumber)
	if err != nil {
		logger.Error("Error while getting volume count", "error", err.Error())
		return &gcpgenserver.V1betaGetVolumeCountInternalServerError{Code: 500, Message: "Internal server error"}, err
	}
	return &gcpgenserver.V1betaGetVolumeCountOK{VolumeCount: int(count)}, nil
}

func (h Handler) V1betaListVolumes(ctx context.Context, params gcpgenserver.V1betaListVolumesParams) (gcpgenserver.V1betaListVolumesRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	volumes, err := h.Orchestrator.ListVolumes(ctx, params.ProjectNumber)
	if err != nil {
		logger.Error("Failed to list volumes", "error", err.Error())
		return &gcpgenserver.V1betaListVolumesInternalServerError{Code: 500, Message: "Internal server error"}, err
	}
	resp := &gcpgenserver.V1betaListVolumesOK{
		Volumes: convertModelsToVCPVolumes(volumes),
	}
	return resp, nil
}

func convertModelsToVCPVolumes(volumes []*models.Volume) []gcpgenserver.VolumeV1beta {
	var volumesList []gcpgenserver.VolumeV1beta
	for _, volume := range volumes {
		p := convertModelToVCPVolume(volume)
		volumesList = append(volumesList, *p)
	}
	return volumesList
}

func (h Handler) V1betaCreateVolume(ctx context.Context, req *gcpgenserver.VolumeCreateV1beta, params gcpgenserver.V1betaCreateVolumeParams) (gcpgenserver.V1betaCreateVolumeRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
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

func _prepareCreateVolumeParams(req *gcpgenserver.VolumeCreateV1beta, params gcpgenserver.V1betaCreateVolumeParams, region string) (*common.CreateVolumeParams, error) {
	vendorId := fmt.Sprintf("/projects/%v/locations/%v/volumes/%s", params.ProjectNumber, params.LocationId, req.Volume.ResourceId)

	var backupPath string
	if req.BackupPath.IsSet() {
		if !backupEnabled {
			return nil, errors.NewUserInputValidationErr("Backup feature is currently not enabled.")
		}
		backupPath = req.BackupPath.Value
		components := strings.Split(backupPath, "/")
		// Ensure there are enough components to avoid out of range errors
		if len(components) < MaxBackupPathComponents {
			return nil, errors.NewUserInputValidationErr("Backup path is not in correct format")
		}
	}
	var backupID string
	if req.BackupId.IsSet() {
		backupID = req.BackupId.Value
	}
	param := &common.CreateVolumeParams{
		AccountName:   params.ProjectNumber,
		Region:        region,
		Name:          req.Volume.ResourceId,
		VendorID:      vendorId,
		CreationToken: req.Volume.CreationToken.Value,
		PoolID:        req.Volume.PoolId.Value,
		QuotaInBytes:  uint64(req.Volume.QuotaInBytes.Value),
		BackupID:      backupID,
		BackupPath:    backupPath,
	}

	if req.VolumeType.IsSet() {
		if req.VolumeType.Value == volumeTypeSecondary {
			param.IsDataProtection = true
		}
	}

	if req.Volume.Description.IsSet() {
		param.Description, _ = req.Volume.Description.Get()
	}

	if req.Volume.Network.IsSet() {
		param.Network, _ = req.Volume.Network.Get()
	}

	if req.Volume.Labels.IsSet() {
		jsonbLabels, err := validateLabels(req.Volume.Labels.Value)
		if err != nil {
			return nil, errors.NewUserInputValidationErr(err.Error())
		}
		param.Labels = jsonbLabels
	}

	for _, protocol := range req.Volume.GetProtocols() {
		protocolStr, err := protocol.MarshalText()
		if err != nil {
			return nil, err
		}
		if !utils.FileProtocolSupported && string(protocolStr) != string(gcpgenserver.ProtocolsV1betaISCSI) {
			return nil, errors.NewUserInputValidationErr("only ISCSI protocol is supported")
		}
		param.Protocols = append(param.Protocols, string(protocolStr))
	}

	if req.Volume.TieringPolicy.IsSet() {
		if !autoTieringEnabled {
			return nil, errors.NewUserInputValidationErr("Auto-Tiering feature is currently not enabled.")
		}
		param.AutoTieringPolicy = &common.AutoTieringPolicy{}
		switch req.Volume.TieringPolicy.Value.TierAction.Value {
		case gcpgenserver.TieringPolicyV1betaTierActionENABLED:
			param.AutoTieringPolicy.AutoTieringEnabled = true
			param.AutoTieringPolicy.TieringPolicy = ontapmodels.VolumeInlineTieringPolicyAuto
			param.AutoTieringPolicy.RetrievalPolicy = ontapmodels.VolumeCloudRetrievalPolicyDefault
			param.AutoTieringPolicy.CoolingThresholdDays = req.Volume.TieringPolicy.Value.CoolingThresholdDays.Value
		case gcpgenserver.TieringPolicyV1betaTierActionPAUSED:
			param.AutoTieringPolicy.AutoTieringEnabled = false
			param.AutoTieringPolicy.TieringPolicy = ontapmodels.VolumeInlineTieringPolicyNone
		}
	}

	param.FileProperties = &models.FileProperties{
		ExportPolicyName: req.Volume.CreationToken.Value,
	}
	if req.Volume.ExportPolicy.IsSet() {
		var exportRules []*models.ExportRule
		for index, rule := range req.Volume.ExportPolicy.Value.GetRules() {
			accessType, err := rule.AccessType.MarshalText()
			if err != nil {
				continue
			}
			exportRules = append(exportRules, &models.ExportRule{
				AllowedClients: rule.GetAllowedClients(),
				AccessType:     string(accessType),
				NFSv3:          rule.Nfsv3.Value,
				NFSv4:          rule.Nfsv4.Value,
				Index:          index + 1, // adding 1 as 0 index is not supported by ontap
			})
		}
		param.FileProperties.ExportRules = exportRules
	}

	if req.Volume.BlockProperties.IsSet() {
		reqBlockProperties, _ := req.Volume.BlockProperties.Get()
		if reqBlockProperties.OsType.IsSet() {
			osType := reqBlockProperties.GetOsType()
			param.BlockProperties = &common.BlockPropertiesRequest{
				OSType:         string(osType.Value),
				HostGroupUUIDs: DeduplicateSlice(reqBlockProperties.GetHostGroupIds()),
			}
		}
	}
	if req.Volume.BackupConfig.IsSet() {
		if !backupEnabled {
			return nil, errors.NewUserInputValidationErr("Backup feature is currently not enabled.")
		}
		param.DataProtection = &models.DataProtection{}
		reqBackupConfig, _ := req.Volume.BackupConfig.Get()
		if reqBackupConfig.BackupVaultId.IsSet() {
			param.DataProtection.BackupVaultID = reqBackupConfig.BackupVaultId.Value
		}
		if reqBackupConfig.BackupPolicyId.IsSet() {
			param.DataProtection.BackupPolicyId = reqBackupConfig.BackupPolicyId.Value
		}
		if reqBackupConfig.ScheduledBackupEnabled.IsSet() {
			param.DataProtection.ScheduledBackupEnabled = &reqBackupConfig.ScheduledBackupEnabled.Value
		}
	}

	if req.Volume.SnapshotPolicy.IsSet() {
		snapShotPolicy, err := convertFromSnapshotPolicyV2(&req.Volume.SnapshotPolicy.Value)
		if err != nil {
			return nil, err
		}
		if snapShotPolicy != nil {
			if snapShotPolicy.Schedules == nil || (snapShotPolicy.Schedules != nil && len(snapShotPolicy.Schedules) == 0) {
				err = errors.NewUserInputValidationErr("SnapshotPolicy parameter must have at least one schedule populated")
				return nil, err
			}
		}
		param.SnapshotPolicy = snapShotPolicy
	}

	if req.SnapshotId.IsSet() {
		param.SnapshotID = req.SnapshotId.Value
	}

	if req.Volume.SnapReserve.IsSet() {
		snapReserve, ok := req.Volume.SnapReserve.Get()
		if !ok {
			return nil, errors.NewUserInputValidationErr("SnapReserve must be a valid number")
		}
		if snapReserve < 0 {
			return nil, errors.NewUserInputValidationErr("SnapReserve cannot be negative")
		}
		if snapReserve > 90 { // ONTAP allows a maximum of 90% for snapshot reserve during creation
			return nil, errors.NewUserInputValidationErr("Maximum allowed snapshot-reserve-percentage value during create is 90.Use volume update to set it to a higher value after the volume has been created.")
		}
		param.SnapReserve = int64(snapReserve)
	}

	return param, nil
}

func (h Handler) V1betaUpdateVolume(ctx context.Context, req *gcpgenserver.VolumeUpdateV1beta, params gcpgenserver.V1betaUpdateVolumeParams) (gcpgenserver.V1betaUpdateVolumeRes, error) {
	logger := util.GetLogger(ctx)
	region, _, parsingErr := utils.ParseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaUpdateVolumeBadRequest{
			Code:    parsingErr.Code,
			Message: parsingErr.Message,
		}, nil
	}

	param, err := prepareUpdateVolumeParams(req, params, region)
	if err != nil {
		if errors.IsUserInputValidationErr(err) || errors.IsNotFoundErr(err) {
			return &gcpgenserver.V1betaUpdateVolumeBadRequest{
				Code:    400,
				Message: err.Error(),
			}, nil
		}

		logger.Error("Failed to update volume", "error", err.Error())
		return &gcpgenserver.V1betaUpdateVolumeInternalServerError{Code: 500, Message: err.Error()}, err
	}

	volume, jobUUID, err := h.Orchestrator.UpdateVolume(ctx, param)
	if err != nil {
		if errors.IsUserInputValidationErr(err) || errors.IsNotFoundErr(err) {
			return &gcpgenserver.V1betaUpdateVolumeBadRequest{
				Code:    400,
				Message: err.Error(),
			}, nil
		}

		logger.Error("Failed to update volume", "error", err.Error())
		return &gcpgenserver.V1betaUpdateVolumeInternalServerError{Code: 500, Message: err.Error()}, err
	}

	resp, err := encodeVolumeV1(convertModelToVCPVolume(volume))
	if err != nil {
		return nil, err
	}

	operationID := "/v1beta/projects/" + params.ProjectNumber + "/locations/" + params.LocationId + "/operations/" + jobUUID
	if volume.LifeCycleState == models.LifeCycleStateUpdating {
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

func _prepareUpdateVolumeParams(req *gcpgenserver.VolumeUpdateV1beta, params gcpgenserver.V1betaUpdateVolumeParams, region string) (*common.UpdateVolumeParams, error) {
	param := &common.UpdateVolumeParams{
		AccountName: params.ProjectNumber,
		Region:      region,
		PoolID:      req.PoolId.Value,
		VolumeId:    params.VolumeId,
	}

	if req.Description.IsSet() {
		param.Description, _ = req.Description.Get()
	}

	if req.QuotaInBytes.IsSet() {
		quota, _ := req.QuotaInBytes.Get()
		if err := validateVolumeQuotaSize(quota); err != nil {
			return nil, err
		}
		param.QuotaInBytes = int64(quota)
	}

	for _, protocol := range req.GetProtocols() {
		protocolStr, err := protocol.MarshalText()
		if err != nil {
			return nil, err
		}
		if !utils.FileProtocolSupported && string(protocolStr) != string(gcpgenserver.ProtocolsV1betaISCSI) {
			return nil, errors.NewUserInputValidationErr("only ISCSI protocol is supported")
		}
		param.Protocols = append(param.Protocols, string(protocolStr))
	}

	if req.BlockProperties.IsSet() {
		reqBlockProperties, _ := req.BlockProperties.Get()
		param.BlockProperties = &common.BlockPropertiesRequest{
			HostGroupUUIDs: DeduplicateSlice(reqBlockProperties.HostGroupIds),
		}
		if reqBlockProperties.OsType.IsSet() {
			osType := reqBlockProperties.GetOsType().Value
			param.BlockProperties.OSType = string(osType)
		}
	}

	if req.TieringPolicy.IsSet() {
		if !autoTieringEnabled {
			return nil, errors.NewUserInputValidationErr("Auto-Tiering feature is currently not enabled.")
		}
		param.AutoTieringPolicy = &common.AutoTieringPolicy{}
		switch req.TieringPolicy.Value.TierAction.Value {
		case gcpgenserver.TieringPolicyV1betaTierActionENABLED:
			param.AutoTieringPolicy.AutoTieringEnabled = true
			param.AutoTieringPolicy.TieringPolicy = ontapmodels.VolumeInlineTieringPolicyAuto
			param.AutoTieringPolicy.RetrievalPolicy = ontapmodels.VolumeCloudRetrievalPolicyDefault
			param.AutoTieringPolicy.CoolingThresholdDays = req.TieringPolicy.Value.CoolingThresholdDays.Value
		case gcpgenserver.TieringPolicyV1betaTierActionPAUSED:
			param.AutoTieringPolicy.AutoTieringEnabled = false
			param.AutoTieringPolicy.TieringPolicy = ontapmodels.VolumeInlineTieringPolicyNone
		}
	}

	if req.BackupConfig.IsSet() {
		if !backupEnabled {
			return nil, errors.NewUserInputValidationErr("Backup feature is currently not enabled.")
		}
		param.DataProtection = &models.DataProtection{}
		reqBackupConfig, _ := req.BackupConfig.Get()
		if reqBackupConfig.BackupVaultId.IsSet() {
			param.DataProtection.BackupVaultID = reqBackupConfig.BackupVaultId.Value
		}
		if reqBackupConfig.BackupPolicyId.IsSet() {
			param.DataProtection.BackupPolicyId = reqBackupConfig.BackupPolicyId.Value
		}
		if reqBackupConfig.ScheduledBackupEnabled.IsSet() {
			param.DataProtection.ScheduledBackupEnabled = &reqBackupConfig.ScheduledBackupEnabled.Value
		}
	}

	if req.Labels.IsSet() {
		jsonbLabels, err := validateLabels(req.Labels.Value)
		if err != nil {
			return nil, errors.NewUserInputValidationErr(err.Error())
		}
		param.Labels = jsonbLabels
	}

	if req.SnapshotPolicy.IsSet() {
		snapShotPolicy, err := convertFromSnapshotPolicyV2(&req.SnapshotPolicy.Value)
		if err != nil {
			return nil, err
		}
		param.SnapshotPolicy = snapShotPolicy
	}

	if req.SnapReserve.IsSet() {
		snapReserve := int64(req.SnapReserve.Value)
		if snapReserve > 100 {
			return nil, errors.NewUserInputValidationErr("SnapReserve cannot be greater than 100")
		}
		param.SnapReserve = nillable.ToPointer(snapReserve)
	} else {
		param.SnapReserve = nil
	}

	return param, nil
}

func validateVolumeQuotaSize(quota float64) error {
	minQuotaVal := float64(utils.MinQuotaInBytesVolumeForVolume)
	maxQuotaVal := float64(utils.MaxQuotaInBytesVolumeForVolume)
	if quota < minQuotaVal || quota > maxQuotaVal {
		return errors.NewUserInputValidationErr(fmt.Sprintf("volume size must be between %d GiB and %d GiB.",
			utils.ConvertBytesToGib(minQuotaVal), utils.ConvertBytesToGib(maxQuotaVal)))
	}
	return nil
}

func (h Handler) V1betaDeleteVolume(ctx context.Context, req gcpgenserver.OptV1betaDeleteVolumeReq, params gcpgenserver.V1betaDeleteVolumeParams) (gcpgenserver.V1betaDeleteVolumeRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
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
		ServiceLevel:       gcpgenserver.NewOptVolumeV1betaServiceLevel(gcpgenserver.VolumeV1betaServiceLevelFLEX),
		IsDataProtection:   gcpgenserver.NewOptBool(volume.IsDataProtection),
		EncryptionType:     gcpgenserver.NewOptVolumeV1betaEncryptionType(gcpgenserver.VolumeV1betaEncryptionType(volume.EncryptionType)),
		SnapshotDirectory:  gcpgenserver.NewOptBool(false),
		SnapReserve:        gcpgenserver.NewOptFloat64(float64(volume.SnapReserve)),
		Zone:               gcpgenserver.NewOptString(volume.Zone),
		UsedBytes:          gcpgenserver.NewOptNilFloat64(float64(volume.UsedBytes)), // default value for now
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

	if volume.Labels != nil {
		labels := gcpgenserver.VolumeV1betaLabels{}
		for key, value := range volume.Labels {
			labels[key] = value
		}
		res.Labels = gcpgenserver.OptVolumeV1betaLabels{Value: labels}
	}

	if volume.FileProperties != nil {
		rules := make([]gcpgenserver.SimpleExportPolicyRuleV1beta, 0)
		for _, rule := range volume.FileProperties.ExportRules {
			rules = append(rules, gcpgenserver.SimpleExportPolicyRuleV1beta{
				AllowedClients: rule.AllowedClients,
				AccessType:     gcpgenserver.SimpleExportPolicyRuleV1betaAccessType(rule.AccessType),
				Nfsv3:          gcpgenserver.OptNilBool{Value: rule.NFSv3},
				Nfsv4:          gcpgenserver.OptNilBool{Value: rule.NFSv4},
			})
		}
		res.ExportPolicy = gcpgenserver.NewOptExportPolicyV1beta(
			gcpgenserver.ExportPolicyV1beta{
				Rules: rules,
			})
		if volume.LifeCycleState == string(gcpgenserver.VolumeV1betaVolumeStateREADY) {
			res.MountPoints = make([]gcpgenserver.MountPointV1beta, 0)
			res.MountPoints = append(res.MountPoints, gcpgenserver.MountPointV1beta{
				IpAddress:    gcpgenserver.NewOptString(volume.IPAddress),
				Protocol:     gcpgenserver.NewOptProtocolsV1beta(gcpgenserver.ProtocolsV1betaNFSV3),
				Instructions: getFilesMountInstructions(volume.IPAddress, volume.FileProperties.JunctionPath, "/"+volume.DisplayName),
			})
		}
	}

	if volume.BlockProperties != nil {
		blockPropertiesV1beta := gcpgenserver.BlockPropertiesV1beta{
			OsType:          gcpgenserver.NewOptBlockPropertiesV1betaOsType(gcpgenserver.BlockPropertiesV1betaOsType(volume.BlockProperties.OSType)),
			LunSerialNumber: gcpgenserver.NewOptString(volume.BlockProperties.LunSerialNumber),
		}
		for _, hostGroup := range volume.BlockProperties.HostGroupDetail {
			blockPropertiesV1beta.HostGroupDetails = append(blockPropertiesV1beta.HostGroupDetails, gcpgenserver.HostGroupDetail{
				Hosts:       hostGroup.Hosts,
				HostGroupId: gcpgenserver.NewOptString(hostGroup.HostGroupID),
			})
			blockPropertiesV1beta.HostGroupIds = append(blockPropertiesV1beta.HostGroupIds, hostGroup.HostGroupID)
		}
		res.BlockProperties = gcpgenserver.NewOptBlockPropertiesV1beta(blockPropertiesV1beta)
		// Only show mount points if volume is ready and has valid LUN name
		if volume.LifeCycleState == string(gcpgenserver.VolumeV1betaVolumeStateREADY) && volume.BlockProperties.LunName != "" {
			res.MountPoints = make([]gcpgenserver.MountPointV1beta, 0)
			res.MountPoints = append(res.MountPoints, gcpgenserver.MountPointV1beta{
				IpAddress:    gcpgenserver.NewOptString(volume.IPAddress),
				Protocol:     gcpgenserver.NewOptProtocolsV1beta(gcpgenserver.ProtocolsV1betaISCSI),
				Instructions: getMountInstructions(volume.BlockProperties.OSType, volume.IPAddress, volume.BlockProperties.LunName),
			})
		}
	}
	var backupConfigId, backupVaultId string
	var backupChainBytes int64
	var scheduledBackupEnabled bool
	if volume.DataProtection != nil {
		if volume.DataProtection.BackupPolicyId != "" {
			backupConfigId = volume.DataProtection.BackupPolicyId
		}
		if volume.DataProtection.BackupVaultID != "" {
			backupVaultId = volume.DataProtection.BackupVaultID
		}
		if volume.DataProtection.BackupChainBytes != nil {
			backupChainBytes = *volume.DataProtection.BackupChainBytes
		}
		if volume.DataProtection.ScheduledBackupEnabled != nil {
			scheduledBackupEnabled = *volume.DataProtection.ScheduledBackupEnabled
		}
	}
	res.BackupConfig = gcpgenserver.NewOptBackupConfigV1beta(
		gcpgenserver.BackupConfigV1beta{
			BackupVaultId:          gcpgenserver.NewOptNilString(backupVaultId),
			BackupPolicyId:         gcpgenserver.NewOptNilString(backupConfigId),
			BackupChainBytes:       gcpgenserver.NewOptNilInt64(backupChainBytes),
			ScheduledBackupEnabled: gcpgenserver.NewOptNilBool(scheduledBackupEnabled),
		},
	)

	if volume.SnapshotPolicy != nil {
		res.SnapshotPolicy = gcpgenserver.NewOptSnapshotPolicyV1beta(*convertToSnapshotPolicyV2(volume.SnapshotPolicy))
	}

	if volume.AutoTieringPolicy != nil && volume.AutoTieringPolicy.AutoTieringEnabled {
		res.TieringPolicy = gcpgenserver.NewOptTieringPolicyV1beta(
			gcpgenserver.TieringPolicyV1beta{
				TierAction:           gcpgenserver.NewOptNilTieringPolicyV1betaTierAction(gcpgenserver.TieringPolicyV1betaTierActionENABLED),
				CoolingThresholdDays: gcpgenserver.NewOptNilInt32(volume.AutoTieringPolicy.CoolingThresholdDays),
			})
	}

	return res
}

func getFilesMountInstructions(ipAddress string, junctionPath string, fileDir string) gcpgenserver.OptString {
	instructions := fmt.Sprintf(`Mount Instructions for NFSv3
Setting up your instance
1. Open an SSH client and connect to your instance.
2. Install the nfs client on your instance.
On Red Hat Enterprise Linux or SuSE Linux instance:
$sudo yum install -y nfs-utils
On an Ubuntu or Debian instance:
$sudo apt-get install nfs-common
Mounting your volume for NFSv3
1. Create a new directory on your instance, such as %s:
$sudo mkdir %s
2. Mount your volume using the example command below:
$sudo mount -t nfs -o rw,hard,rsize=65536,wsize=65536,vers=3,tcp %s:%s %s
3. Repeat the above two steps for future mount targets.
Note. Please use mount options appropriate for your specific workloads when known.`, fileDir, fileDir, ipAddress, junctionPath, fileDir)
	return gcpgenserver.NewOptString(instructions)
}

func getMountInstructions(osType string, ipAddress string, lunName string) gcpgenserver.OptString {
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
$ sudo iscsiadm -m discovery -t sendtargets -p %s:3260
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
$ /dev/sdb /mnt/%s ext4 defaults 0 0`, ipAddress, ipAddress, lunName, lunName, lunName)
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

	// Validate the location first
	_, _, parsingErr := parseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaGetMultipleVolumesBadRequest{
			Code:    parsingErr.Code,
			Message: parsingErr.Message,
		}, nil
	}

	if req.VolumeUuids == nil {
		return &gcpgenserver.V1betaGetMultipleVolumesBadRequest{
			Code:    400,
			Message: "VolumeUuids are required",
		}, nil
	}

	if len(req.VolumeUuids) > 1000 {
		return &gcpgenserver.V1betaGetMultipleVolumesBadRequest{
			Code:    400,
			Message: "VolumeUuids in body should have at most 1000 items",
		}, nil
	}

	volumesModelVCP, err := h.Orchestrator.GetMultipleVolumes(ctx, req.VolumeUuids, params.ProjectNumber)
	if err != nil {
		logger.Error("Failed to fetch volume", "error", err.Error())
		return &gcpgenserver.V1betaGetMultipleVolumesInternalServerError{Code: 500, Message: "Internal server error"}, err
	}

	volumesVCP := make([]gcpgenserver.VolumeV1beta, 0, len(req.VolumeUuids))
	foundVolumeUUIDs := make(map[string]struct{}, len(volumesModelVCP))
	for _, vol := range volumesModelVCP {
		response := convertModelToVCPVolume(vol)
		volumesVCP = append(volumesVCP, *response)
		foundVolumeUUIDs[vol.UUID] = struct{}{}
	}

	// If all volumes are found in VCP, just return them.
	if len(req.VolumeUuids) == len(volumesVCP) {
		return &gcpgenserver.V1betaGetMultipleVolumesOK{
			Volumes: volumesVCP,
		}, nil
	}

	if cvp.CVP_HOST == "" {
		logger.Info("CVP_HOST environment variable is not set, skipping CVP call", "foundVolumes", len(volumesVCP), "requestedVolumes", len(req.VolumeUuids))
		return &gcpgenserver.V1betaGetMultipleVolumesOK{
			Volumes: volumesVCP,
		}, nil
	}

	// Figure out which volumes are missing and need to be fetched from CVP
	missingVolumeUUIDs := helper.FindMissingUUIDs(req.VolumeUuids, foundVolumeUUIDs)

	// If no volumes are missing (e.g. due to duplicates in request), we don't need to call CVP
	if len(missingVolumeUUIDs) == 0 {
		return &gcpgenserver.V1betaGetMultipleVolumesOK{
			Volumes: volumesVCP,
		}, nil
	}

	// The original request object `req` contains all UUIDs. We need a new one with only the missing UUIDs.
	cvpReq := &gcpgenserver.VolumeIdListV1beta{
		VolumeUuids: missingVolumeUUIDs,
	}

	return getMultipleVolumesFromCVP(ctx, cvpReq, params, volumesVCP)
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
		StorageClass:            gcpgenserver.NewOptStorageClassV1beta(gcpgenserver.StorageClassV1beta(in.StorageClass)),
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

	if in.CacheParameters != nil {
		flexCachePrePopulateV1beta := gcpgenserver.FlexCachePrePopulateV1beta{}
		cacheConfigV1beta := gcpgenserver.FlexCacheConfigV1beta{}

		if in.CacheParameters.CacheConfig != nil {
			cacheConfigV1beta = gcpgenserver.FlexCacheConfigV1beta{
				WritebackEnabled:        gcpgenserver.NewOptNilBool(*in.CacheParameters.CacheConfig.WritebackEnabled),
				AtimeScrubEnabled:       gcpgenserver.NewOptNilBool(*in.CacheParameters.CacheConfig.AtimeScrubEnabled),
				AtimeScrubMinutes:       gcpgenserver.NewOptNilInt16(*in.CacheParameters.CacheConfig.AtimeScrubMinutes),
				CifsChangeNotifyEnabled: gcpgenserver.NewOptNilBool(*in.CacheParameters.CacheConfig.CifsChangeNotifyEnabled),
			}
		}

		if in.CacheParameters.CacheConfig.PrePopulate != nil {
			flexCachePrePopulateV1beta = gcpgenserver.FlexCachePrePopulateV1beta{
				PathList:        gcpgenserver.NewOptNilStringArray(in.CacheParameters.CacheConfig.PrePopulate.PathList),
				ExcludePathList: gcpgenserver.NewOptNilStringArray(in.CacheParameters.CacheConfig.PrePopulate.ExcludePathList),
				Recursion:       gcpgenserver.NewOptNilBool(*in.CacheParameters.CacheConfig.PrePopulate.Recursion),
			}
		}

		cacheConfigV1beta.PrePopulate = gcpgenserver.NewOptFlexCachePrePopulateV1beta(flexCachePrePopulateV1beta)

		cacheParams := gcpgenserver.FlexCacheV1beta{
			PeerVolumeName:       gcpgenserver.NewOptString(in.CacheParameters.PeerVolumeName),
			PeerClusterName:      gcpgenserver.NewOptString(in.CacheParameters.PeerClusterName),
			PeerSvmName:          gcpgenserver.NewOptString(in.CacheParameters.PeerSvmName),
			PeerIpAddresses:      in.CacheParameters.PeerIPAddresses,
			EnableGlobalFileLock: gcpgenserver.NewOptNilBool(*in.CacheParameters.EnableGlobalFileLock),
			CacheConfig:          gcpgenserver.NewOptFlexCacheConfigV1beta(cacheConfigV1beta),
			CacheState:           gcpgenserver.NewOptFlexCacheV1betaCacheState(gcpgenserver.FlexCacheV1betaCacheState(in.CacheParameters.CacheState)),
			Command:              gcpgenserver.NewOptString(in.CacheParameters.Command),
			CommandExpiryTime:    gcpgenserver.NewOptNilDateTime(time.Time(*in.CacheParameters.CommandExpiryTime)),
			Passphrase:           gcpgenserver.NewOptNilString(*in.CacheParameters.Passphrase),
		}

		volume.CacheParameters = gcpgenserver.NewOptFlexCacheV1beta(cacheParams)
	}

	return volume
}

func convertFromSnapshotPolicyV2(snapshotPolicy *gcpgenserver.SnapshotPolicyV1beta) (*models.SnapshotPolicy, error) {
	if snapshotPolicy == nil {
		return nil, nil
	}
	snapshotPolicySchedule := make([]*models.SnapshotPolicySchedule, 0)

	monthlySchedule := snapshotPolicy.MonthlySchedule
	if monthlySchedule.IsSet() {
		count := int64(0)
		monthly := monthlySchedule.Value
		if monthly.SnapshotsToKeep.IsSet() {
			count = int64(monthly.SnapshotsToKeep.Value)
		}
		daysOfMonth := []int{}
		if monthly.DaysOfMonth.IsSet() {
			days := strings.Split(monthly.DaysOfMonth.Value, ",")
			for _, day := range days {
				dayOfMonth, err := strconv.Atoi(strings.TrimSpace(day))
				if err == nil {
					daysOfMonth = append(daysOfMonth, dayOfMonth)
				}
			}
		} else {
			daysOfMonth = append(daysOfMonth, 1)
		}
		hours := []int{0}
		if monthly.Hour.IsSet() {
			hours[0] = int(monthly.Hour.Value)
		}
		minutes := []int{0}
		if monthly.Minute.IsSet() {
			minutes[0] = int(monthly.Minute.Value)
		}

		snapshotPolicySchedule = append(snapshotPolicySchedule, &models.SnapshotPolicySchedule{
			Count:           count,
			SnapmirrorLabel: SnapshotScheduleLabelMonthly,
			Schedule: &models.Schedule{
				DaysOfMonth: daysOfMonth,
				Hours:       hours,
				Minutes:     minutes,
			},
		})
	}

	weeklySchedule := snapshotPolicy.WeeklySchedule
	if weeklySchedule.IsSet() {
		count := int64(0)
		var err error
		var daysOfWeek []int
		weekly := weeklySchedule.Value
		if weekly.SnapshotsToKeep.IsSet() {
			count = int64(weekly.SnapshotsToKeep.Value)
		}
		if weekly.Day.IsSet() && len(weekly.Day.Value) > 0 {
			daysOfWeek, err = convertDaysOfWeekToIntArray(weekly.Day.Value)
			if err != nil {
				return nil, err
			}
		}
		hours := []int{0}
		if weekly.Hour.IsSet() {
			hours[0] = int(weekly.Hour.Value)
		}

		minutes := []int{0}
		if weekly.Minute.IsSet() {
			minutes[0] = int(weekly.Minute.Value)
		}

		snapshotPolicySchedule = append(snapshotPolicySchedule, &models.SnapshotPolicySchedule{
			Count:           count,
			SnapmirrorLabel: SnapshotScheduleLabelWeekly,
			Schedule: &models.Schedule{
				DaysOfWeek: daysOfWeek,
				Hours:      hours,
				Minutes:    minutes,
			},
		})
	}

	dailySchedule := snapshotPolicy.DailySchedule
	if dailySchedule.IsSet() {
		count := int64(0)
		daily := dailySchedule.Value
		if daily.SnapshotsToKeep.IsSet() {
			count = int64(daily.SnapshotsToKeep.Value)
		}
		hours := []int{0}
		if daily.Hour.IsSet() {
			hours[0] = int(daily.Hour.Value)
		}
		minutes := []int{0}
		if daily.Minute.IsSet() {
			minutes[0] = int(daily.Minute.Value)
		}

		snapshotPolicySchedule = append(snapshotPolicySchedule, &models.SnapshotPolicySchedule{
			Count:           count,
			SnapmirrorLabel: SnapshotScheduleLabelDaily,
			Schedule: &models.Schedule{
				Hours:   hours,
				Minutes: minutes,
			},
		})
	}

	hourlySchedule := snapshotPolicy.HourlySchedule
	if hourlySchedule.IsSet() {
		count := int64(0)
		hourly := hourlySchedule.Value
		if hourly.SnapshotsToKeep.IsSet() {
			count = int64(hourly.SnapshotsToKeep.Value)
		}
		minutes := []int{0}
		if hourly.Minute.IsSet() {
			minutes[0] = int(hourly.Minute.Value)
		}

		snapshotPolicySchedule = append(snapshotPolicySchedule, &models.SnapshotPolicySchedule{
			Count:           count,
			SnapmirrorLabel: SnapshotScheduleLabelHourly,
			Schedule: &models.Schedule{
				Minutes: minutes,
			},
		})
	}

	return &models.SnapshotPolicy{
		IsEnabled: snapshotPolicy.Enabled.IsSet() && snapshotPolicy.Enabled.Value,
		Schedules: snapshotPolicySchedule,
	}, nil
}

func convertDaysOfWeekToIntArray(days string) ([]int, error) {
	// Return Sunday by default
	if days == "" {
		return []int{int(time.Sunday)}, nil
	}

	splitDays := strings.Split(days, ",")
	weekdays := []time.Weekday{time.Sunday, time.Monday, time.Tuesday, time.Wednesday, time.Thursday, time.Friday, time.Saturday}
	var result []int

	for _, day := range splitDays {
		day := strings.TrimSpace(day)
		hasFoundDay := false
		for _, weekday := range weekdays {
			// Allow for Monday,Tuesday and also MON,TUE
			if strings.HasPrefix(strings.ToLower(weekday.String()), strings.ToLower(day)) {
				weekDayValue := int(weekday)
				if slices.Contains(result, weekDayValue) {
					return nil, errors.NewUserInputValidationErr(daysOfWeekError)
				}
				result = append(result, weekDayValue)
				hasFoundDay = true
				break
			}
		}
		if !hasFoundDay {
			return nil, errors.NewUserInputValidationErr(daysOfWeekError)
		}
	}

	return result, nil
}

func convertToSnapshotPolicyV2(pol *models.SnapshotPolicy) *gcpgenserver.SnapshotPolicyV1beta {
	if pol == nil {
		return nil
	}

	var monthly gcpgenserver.MonthlyScheduleV1beta
	var weekly gcpgenserver.WeeklyScheduleV1beta
	var daily gcpgenserver.DailyScheduleV1beta
	var hourly gcpgenserver.HourlyScheduleV1beta

	for _, sc := range pol.Schedules {
		schedule := *sc
		count := float64(schedule.Count)

		var minute float64
		if len(schedule.Schedule.Minutes) > 0 {
			minute = float64(schedule.Schedule.Minutes[0])
		}

		if len(schedule.Schedule.DaysOfMonth) > 0 {
			var days []string
			for _, day := range schedule.Schedule.DaysOfMonth {
				days = append(days, strconv.Itoa(day))
			}
			daysOfMonth := strings.Join(days, ",")
			hour := float64(0)
			if len(schedule.Schedule.Hours) > 0 {
				hour = float64(schedule.Schedule.Hours[0])
			}
			monthly = gcpgenserver.MonthlyScheduleV1beta{
				SnapshotsToKeep: gcpgenserver.NewOptFloat64(count),
				DaysOfMonth:     gcpgenserver.NewOptString(daysOfMonth),
				Hour:            gcpgenserver.NewOptFloat64(hour),
				Minute:          gcpgenserver.NewOptFloat64(minute),
			}
		} else if len(schedule.Schedule.DaysOfWeek) > 0 {
			days := convertDaysOfWeekFromIntArray(schedule.Schedule.DaysOfWeek)
			hour := float64(0)
			if len(schedule.Schedule.Hours) > 0 {
				hour = float64(schedule.Schedule.Hours[0])
			}
			weekly = gcpgenserver.WeeklyScheduleV1beta{
				SnapshotsToKeep: gcpgenserver.NewOptFloat64(count),
				Day:             gcpgenserver.NewOptString(days),
				Hour:            gcpgenserver.NewOptFloat64(hour),
				Minute:          gcpgenserver.NewOptFloat64(minute),
			}
		} else if len(schedule.Schedule.Hours) > 0 {
			hour := float64(schedule.Schedule.Hours[0])
			daily = gcpgenserver.DailyScheduleV1beta{
				SnapshotsToKeep: gcpgenserver.NewOptFloat64(count),
				Hour:            gcpgenserver.NewOptFloat64(hour),
				Minute:          gcpgenserver.NewOptFloat64(minute),
			}
		} else {
			hourly = gcpgenserver.HourlyScheduleV1beta{
				SnapshotsToKeep: gcpgenserver.NewOptFloat64(count),
				Minute:          gcpgenserver.NewOptFloat64(minute),
			}
		}
	}

	return &gcpgenserver.SnapshotPolicyV1beta{
		Enabled:         gcpgenserver.NewOptNilBool(pol.IsEnabled),
		MonthlySchedule: gcpgenserver.NewOptMonthlyScheduleV1beta(monthly),
		WeeklySchedule:  gcpgenserver.NewOptWeeklyScheduleV1beta(weekly),
		DailySchedule:   gcpgenserver.NewOptDailyScheduleV1beta(daily),
		HourlySchedule:  gcpgenserver.NewOptHourlyScheduleV1beta(hourly),
	}
}

func convertDaysOfWeekFromIntArray(days []int) string {
	var resultDays []string
	for _, day := range days {
		if day >= 0 && day <= 6 {
			resultDays = append(resultDays, time.Weekday(day).String())
		}
	}

	// Return Sunday by default
	if len(resultDays) < 1 {
		resultDays = append(resultDays, time.Sunday.String())
	}

	return strings.Join(resultDays, ",")
}
