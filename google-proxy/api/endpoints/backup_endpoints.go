package api

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-faster/jx"
	"github.com/google/uuid"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/backups"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	coremodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/helper"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

const (

	// BackupTypeMANUAL captures enum value "MANUAL"
	BackupTypeMANUAL string = "MANUAL"
	// BackupTypeSCHEDULED captures enum value "SCHEDULED"
	BackupTypeSCHEDULED string = "SCHEDULED"
)

func (h Handler) V1betaGetMultipleBackups(ctx context.Context, req *gcpgenserver.BackupUuidListV1beta, params gcpgenserver.V1betaGetMultipleBackupsParams) (gcpgenserver.V1betaGetMultipleBackupsRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	listBackups, err := h.Orchestrator.GetBackupsUnderBackupVault(ctx, params.BackupVaultId, params.ProjectNumber, req.BackupUuids)
	if err != nil && errors.IsUserInputValidationErr(err) {
		return &gcpgenserver.V1betaGetMultipleBackupsBadRequest{
			Code:    400,
			Message: err.Error(),
		}, nil
	}
	// If err is not nil and it is not a NotFound error, return an internal server error
	// For NotFoundErr we will return the list from CVP
	if err != nil && !errors.IsNotFoundErr(err) {
		return &gcpgenserver.V1betaGetMultipleBackupsInternalServerError{
			Code:    500,
			Message: err.Error(),
		}, nil
	}
	// Need to fetch the UUIDs not found in VCP to CVP
	uuids := fetchBackupUUIDWhichAreNotPartOfListBackups(listBackups, req.BackupUuids)
	operationResponse := gcpgenserver.V1betaGetMultipleBackupsOK{
		Backups: []gcpgenserver.BackupV1beta{},
	}
	if len(uuids) != 0 {
		jwtToken := utils.GetJWTTokenFromContext(ctx)
		cvpClient := createClient(logger, jwtToken)

		var backupUUIDs []string
		backupUUIDs = append(backupUUIDs, uuids...)

		body := &models.BackupUUIDListV1beta{
			BackupUUIDs: backupUUIDs,
		}
		getMultipleBackupParams := &backups.V1betaGetMultipleBackupsParams{
			BackupVaultID:  params.BackupVaultId,
			Body:           body,
			LocationID:     params.LocationId,
			ProjectNumber:  params.ProjectNumber,
			XCorrelationID: &params.XCorrelationID.Value,
		}

		resp, err := cvpClient.Backups.V1betaGetMultipleBackups(getMultipleBackupParams)
		if err != nil {
			switch e := err.(type) {
			case *backups.V1betaGetMultipleBackupsBadRequest:
				msg := nillable.GetString(&e.Payload.Message, "")
				code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
				return &gcpgenserver.V1betaGetMultipleBackupsBadRequest{
					Code:    code,
					Message: msg,
				}, nil
			case *backups.V1betaGetMultipleBackupsUnauthorized:
				msg := nillable.GetString(&e.Payload.Message, "")
				code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
				return &gcpgenserver.V1betaGetMultipleBackupsUnauthorized{
					Code:    code,
					Message: msg,
				}, nil
			case *backups.V1betaGetMultipleBackupsForbidden:
				msg := nillable.GetString(&e.Payload.Message, "")
				code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
				return &gcpgenserver.V1betaGetMultipleBackupsForbidden{
					Code:    code,
					Message: msg,
				}, nil
			case *backups.V1betaGetMultipleBackupsNotFound:
				msg := nillable.GetString(&e.Payload.Message, "")
				code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
				return &gcpgenserver.V1betaGetMultipleBackupsNotFound{
					Code:    code,
					Message: msg,
				}, nil
			case *backups.V1betaGetMultipleBackupsInternalServerError:
				msg := nillable.GetString(&e.Payload.Message, "")
				code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
				return &gcpgenserver.V1betaGetMultipleBackupsInternalServerError{
					Code:    code,
					Message: msg,
				}, nil
			default:
				return &gcpgenserver.V1betaGetMultipleBackupsInternalServerError{
					Code:    500,
					Message: err.Error(),
				}, nil
			}
		}
		if resp != nil && resp.Payload != nil {
			for _, bp := range resp.Payload.Backups {
				operationResponse.Backups = append(operationResponse.Backups, convertToBackupsV1beta(bp))
			}
		}
	}
	for _, bp := range listBackups {
		operationResponse.Backups = append(operationResponse.Backups, convertBackupDataModelToBackupsV1beta(bp))
	}
	return &operationResponse, nil
}

// V1betaCreateBackup creates a backup for a given volume.
func (h Handler) V1betaCreateBackup(ctx context.Context, req *gcpgenserver.BackupCreateV1beta, params gcpgenserver.V1betaCreateBackupParams) (gcpgenserver.V1betaCreateBackupRes, error) {
	logger := util.GetLogger(ctx)
	_, _, parsingErr := utils.ParseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaCreateBackupBadRequest{
			Code:    parsingErr.Code,
			Message: parsingErr.Message,
		}, nil
	}
	vol, err := h.Orchestrator.GetVolume(ctx, req.VolumeId)

	// Check if volume exist in the VSA if not call CVP API to create the backup
	if (err != nil && errors.IsNotFoundErr(err)) || vol == nil {
		jwtToken := utils.GetJWTTokenFromContext(ctx)
		cvpClient := createClient(logger, jwtToken)

		cvpParams := &backups.V1betaCreateBackupParams{
			BackupVaultID:  params.BackupVaultId,
			LocationID:     params.LocationId,
			ProjectNumber:  params.ProjectNumber,
			XCorrelationID: &params.XCorrelationID.Value,
			Body: &models.BackupCreateV1beta{
				ResourceNameV1beta: models.ResourceNameV1beta{
					ResourceID: &req.ResourceId,
				},
				DescriptionV1beta: models.DescriptionV1beta{
					Description: &req.Description.Value,
				},
				VolumeIDV1beta: models.VolumeIDV1beta{
					VolumeID: &req.VolumeId,
				},
				SnapshotIDV1beta: models.SnapshotIDV1beta{
					SnapshotID: req.SnapshotId.Value,
				},
			},
		}
		cvpBackupCreated, cvpBackupAccepted, err := cvpClient.Backups.V1betaCreateBackup(cvpParams)
		if err != nil {
			switch e := err.(type) {
			case *backups.V1betaCreateBackupBadRequest:
				msg := nillable.GetString(&e.Payload.Message, "")
				code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
				return &gcpgenserver.V1betaCreateBackupBadRequest{
					Code:    code,
					Message: msg,
				}, nil
			case *backups.V1betaCreateBackupUnauthorized:
				msg := nillable.GetString(&e.Payload.Message, "")
				code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
				return &gcpgenserver.V1betaCreateBackupUnauthorized{
					Code:    code,
					Message: msg,
				}, nil
			case *backups.V1betaCreateBackupForbidden:
				msg := nillable.GetString(&e.Payload.Message, "")
				code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
				return &gcpgenserver.V1betaCreateBackupForbidden{
					Code:    code,
					Message: msg,
				}, nil
			case *backups.V1betaCreateBackupConflict:
				msg := nillable.GetString(&e.Payload.Message, "")
				code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
				return &gcpgenserver.V1betaCreateBackupConflict{
					Code:    code,
					Message: msg,
				}, nil
			case *backups.V1betaCreateBackupUnprocessableEntity:
				msg := nillable.GetString(&e.Payload.Message, "")
				code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
				return &gcpgenserver.V1betaCreateBackupUnprocessableEntity{
					Code:    code,
					Message: msg,
				}, nil
			case *backups.V1betaCreateBackupTooManyRequests:
				msg := nillable.GetString(&e.Payload.Message, "")
				code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
				return &gcpgenserver.V1betaCreateBackupTooManyRequests{
					Code:    code,
					Message: msg,
				}, nil
			case *backups.V1betaCreateBackupInternalServerError:
				msg := nillable.GetString(&e.Payload.Message, "")
				code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
				return &gcpgenserver.V1betaCreateBackupInternalServerError{
					Code:    code,
					Message: msg,
				}, nil
			default:
				code := float64(500)
				msg := err.Error()
				return &gcpgenserver.V1betaCreateBackupInternalServerError{
					Code:    code,
					Message: msg,
				}, nil
			}
		}

		if cvpBackupCreated != nil {
			pl := cvpBackupCreated.Payload
			outBackup := convertToBackupsV1beta(pl)
			operationID := "/v1beta/projects/" + params.ProjectNumber + "/locations/" + params.LocationId + "/operations/" + uuid.UUID{}.String()

			done := true
			resp := &models.OperationV1beta{
				Name:     operationID,
				Done:     &done,
				Response: outBackup,
			}
			return convertToOperationV1beta(resp), nil
		}
		if cvpBackupAccepted != nil {
			pl := cvpBackupAccepted.Payload
			done := false
			resp := &models.OperationV1beta{
				Name:     pl.Name,
				Done:     &done,
				Response: pl.Response,
			}
			return convertToOperationV1beta(resp), nil
		}

		msg := "Unexpected function flow"
		return &gcpgenserver.V1betaCreateBackupInternalServerError{
			Code:    float64(500),
			Message: msg,
		}, nil
	}
	if err != nil {
		if errors.IsUserInputValidationErr(err) {
			return &gcpgenserver.V1betaCreateBackupBadRequest{
				Code:    400,
				Message: err.Error(),
			}, nil
		}

		logger.Error("Failed to get volume", "error", err.Error())
		return &gcpgenserver.V1betaCreateBackupInternalServerError{Code: 500, Message: err.Error()}, err
	}
	// If the request belongs to VSA, we will create the backup using the orchestrator
	vsaParams := createBackupParams(req, params)

	backup, jobID, err := h.Orchestrator.CreateBackup(ctx, vsaParams)
	if err != nil {
		if errors.IsUserInputValidationErr(err) || errors.IsNotFoundErr(err) {
			return &gcpgenserver.V1betaCreateBackupBadRequest{
				Code:    400,
				Message: err.Error(),
			}, nil
		}

		logger.Error("Failed to create backup", "error", err.Error())
		return &gcpgenserver.V1betaCreateBackupInternalServerError{Code: 500, Message: err.Error()}, err
	}
	resp, err := encodeBackupV1(convertBackupModelToBackupsV1beta(backup))
	if err != nil {
		logger.Error("Failed to marshal backup", err.Error())
		return &gcpgenserver.V1betaCreateBackupInternalServerError{}, err
	}

	operationID := fmt.Sprintf("/v1beta/projects/%s/locations/%s/operations/%s", params.ProjectNumber, params.LocationId, jobID)
	if backup.LifeCycleState == coremodels.LifeCycleStateCreatingDetails {
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

func (h Handler) V1betaDeleteBackupUnderBackupVault(ctx context.Context, params gcpgenserver.V1betaDeleteBackupUnderBackupVaultParams) (gcpgenserver.V1betaDeleteBackupUnderBackupVaultRes, error) {
	msg := "Unimplemented function flow"
	return &gcpgenserver.V1betaDeleteBackupUnderBackupVaultInternalServerError{
		Code:    float64(500),
		Message: msg,
	}, nil
}

func (h Handler) V1betaUpdateBackupUnderBackupVault(ctx context.Context, req *models.BackupUpdateV1beta, params gcpgenserver.V1betaUpdateBackupParams) (gcpgenserver.V1betaUpdateBackupRes, error) {
	msg := "Unimplemented function flow"
	return &gcpgenserver.V1betaUpdateBackupInternalServerError{
		Code:    float64(500),
		Message: msg,
	}, nil
}

func convertToBackupsV1beta(backup *models.BackupV1beta) gcpgenserver.BackupV1beta {
	return gcpgenserver.BackupV1beta{
		ResourceId:               utils.GetOptString(&backup.ResourceID),
		VolumeId:                 utils.GetOptString(&backup.VolumeID),
		State:                    gcpgenserver.NewOptBackupV1betaState(gcpgenserver.BackupV1betaState(backup.State)),
		Created:                  utils.GetOptDateTime(&backup.Created),
		EnforcedRetentionEndTime: utils.GetOptDateTime(backup.EnforcedRetentionEndTime),
		BackupId:                 utils.GetOptString(&backup.BackupID),
		VolumeUsageBytes:         utils.GetOptInt64(backup.VolumeUsageBytes),
		SourceVolume:             utils.GetOptString(&backup.SourceVolume),
		BackupVaultId:            utils.GetOptString(backup.BackupVaultID),
		Description:              utils.GetOptString(backup.Description),
		SourceSnapshot:           utils.GetOptString(backup.SourceSnapshot),
		BackupType:               gcpgenserver.NewOptBackupV1betaBackupType(gcpgenserver.BackupV1betaBackupType(backup.BackupType)),
		BackupChainBytes:         utils.GetOptInt64(backup.BackupChainBytes),
		SatisfiesPzs:             utils.GetOptBool(backup.SatisfiesPzs),
		SatisfiesPzi:             utils.GetOptBool(backup.SatisfiesPzi),
		VolumeRegion:             utils.GetOptString(backup.VolumeRegion),
		BackupRegion:             utils.GetOptString(backup.BackupRegion),
		AssetLocationMetadata: func() gcpgenserver.OptAssetLocationMetadataV2 {
			if backup.AssetLocationMetadata != nil {
				var assets []gcpgenserver.ChildAssetV2
				for _, asset := range backup.AssetLocationMetadata.ChildAssets {
					assets = append(assets, gcpgenserver.ChildAssetV2{
						AssetType:  utils.GetOptString(&asset.AssetType),
						AssetNames: asset.AssetNames,
					})
				}
				return gcpgenserver.NewOptAssetLocationMetadataV2(gcpgenserver.AssetLocationMetadataV2{
					ChildAssets: assets,
				})
			}
			return gcpgenserver.OptAssetLocationMetadataV2{}
		}(),
	}
}

func convertBackupModelToBackupsV1beta(backup *coremodels.Backup) *gcpgenserver.BackupV1beta {
	return &gcpgenserver.BackupV1beta{
		ResourceId:            utils.GetOptString(&backup.Name),
		VolumeId:              utils.GetOptString(&backup.VolumeID),
		State:                 gcpgenserver.NewOptBackupV1betaState(gcpgenserver.BackupV1betaState(backup.LifeCycleState)),
		BackupId:              utils.GetOptString(&backup.BackupID),
		SourceVolume:          utils.GetOptString(&backup.VolumeName),
		BackupVaultId:         utils.GetOptString(&backup.BackupVaultID),
		Description:           utils.GetOptString(backup.Description),
		SourceSnapshot:        utils.GetOptString(&backup.SnapshotName),
		BackupType:            gcpgenserver.NewOptBackupV1betaBackupType(gcpgenserver.BackupV1betaBackupType(backup.Type)),
		AssetLocationMetadata: gcpgenserver.OptAssetLocationMetadataV2{},
	}
}

func convertBackupDataModelToBackupsV1beta(backup *datamodel.Backup) gcpgenserver.BackupV1beta {
	var state gcpgenserver.BackupV1betaState
	// Need to convert states as DB models and API models have different states
	if backup.State == coremodels.LifeCycleStateAvailable {
		state = gcpgenserver.BackupV1betaStateREADY
	}
	if backup.State == coremodels.LifeCycleStateError {
		state = gcpgenserver.BackupV1betaStateERROR
	}
	return gcpgenserver.BackupV1beta{
		ResourceId: gcpgenserver.OptString{
			Value: backup.Name,
			Set:   true,
		},
		VolumeId: gcpgenserver.OptString{
			Value: backup.VolumeUUID,
			Set:   true,
		},
		State: gcpgenserver.OptBackupV1betaState{
			Value: state,
			Set:   true,
		},
		Created: gcpgenserver.OptDateTime{
			Value: backup.CreatedAt,
			Set:   true,
		},
		EnforcedRetentionEndTime: gcpgenserver.OptDateTime{
			Value: backup.Attributes.EnforcedRetentionDuration,
		},
		BackupId: gcpgenserver.OptString{
			Value: backup.UUID,
			Set:   true,
		},
		VolumeUsageBytes: gcpgenserver.OptInt64{
			Value: backup.SizeInBytes,
			Set:   true,
		},
		BackupVaultId: gcpgenserver.OptString{
			Value: backup.BackupVault.UUID,
			Set:   true,
		},
		Description: gcpgenserver.OptString{
			Value: backup.Description,
			Set:   true,
		},
		BackupType: gcpgenserver.OptBackupV1betaBackupType{
			Value: gcpgenserver.BackupV1betaBackupType(backup.Type),
			Set:   true,
		},
		SourceSnapshot: gcpgenserver.OptString{
			Value: backup.Attributes.SnapshotName,
		},
		SourceVolume: gcpgenserver.OptString{
			Value: backup.Attributes.VolumeName,
			Set:   true,
		},
		BackupRegion: gcpgenserver.OptString{
			Value: *backup.BackupVault.SourceRegionName,
			Set:   true,
		},
		VolumeRegion: gcpgenserver.OptString{
			Value: *backup.BackupVault.SourceRegionName,
			Set:   true,
		},
		// These values are not supported as of now
		SatisfiesPzs: gcpgenserver.OptBool{
			Value: false,
		},
		SatisfiesPzi: gcpgenserver.OptBool{
			Value: false,
		},
		BackupChainBytes: gcpgenserver.OptInt64{
			Value: 0,
		},
		AssetLocationMetadata: gcpgenserver.OptAssetLocationMetadataV2{
			Set: false,
		},
	}
}

func createBackupParams(req *gcpgenserver.BackupCreateV1beta, params gcpgenserver.V1betaCreateBackupParams) *common.CreateBackupParams {
	backupParams := common.CreateBackupParams{
		AccountName:   params.ProjectNumber,
		BackupVaultID: params.BackupVaultId,
		VolumeUUID:    req.VolumeId,
		BackupName:    req.ResourceId,
		BackupType:    BackupTypeMANUAL, // Default to MANUAL, later can be changed based on the request
		LocationID:    params.LocationId,
	}
	if req.Description.IsSet() {
		backupParams.Description = req.Description.Value
	}
	if req.SnapshotId.IsSet() {
		backupParams.SnapshotID = req.SnapshotId.Value
	}
	if params.XCorrelationID.IsSet() {
		backupParams.XCorrelationID = params.XCorrelationID.Value
	}

	return &backupParams
}

// encodeBackupV1 encodes a backupV1 struct to JSON.
func encodeBackupV1(backupV1beta *gcpgenserver.BackupV1beta) (jx.Raw, error) {
	data, err := json.Marshal(backupV1beta)
	if err != nil {
		return nil, err
	}
	return data, nil
}
func fetchBackupUUIDWhichAreNotPartOfListBackups(listBackups []*datamodel.Backup, backupUUIDs []string) []string {
	// Create a map to store UUIDs from listBackups for quick lookup
	backupUUIDMap := make(map[string]bool)
	for _, backup := range listBackups {
		backupUUIDMap[backup.UUID] = true
	}
	// Filter UUIDs from backupUUIDs that are not in listBackups
	var uuids []string
	for _, backupUUID := range backupUUIDs {
		if !backupUUIDMap[backupUUID] {
			uuids = append(uuids, backupUUID)
		}
	}

	return uuids
}
