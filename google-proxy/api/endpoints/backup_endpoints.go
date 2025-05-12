package api

import (
	"context"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/backups"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func (h Handler) V1betaGetMultipleBackups(ctx context.Context, req *gcpgenserver.BackupUuidListV1beta, params gcpgenserver.V1betaGetMultipleBackupsParams) (gcpgenserver.V1betaGetMultipleBackupsRes, error) {
	logger := utils.GetLoggerFromContext(ctx)
	jwtToken := utils.GetJWTTokenFromContext(ctx)
	cvpClient := createClient(logger, jwtToken)

	var backupUUIDs []string
	backupUUIDs = append(backupUUIDs, req.BackupUuids...)

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
		}
	}
	operationResponse := gcpgenserver.V1betaGetMultipleBackupsOK{
		Backups: []gcpgenserver.BackupV1beta{},
	}
	for _, bp := range resp.Payload.Backups {
		operationResponse.Backups = append(operationResponse.Backups, convertToBackupsV1beta(bp))
	}
	return &operationResponse, nil
}

func convertToBackupsV1beta(bv *models.BackupV1beta) gcpgenserver.BackupV1beta {
	return gcpgenserver.BackupV1beta{
		ResourceId:               utils.GetOptString(&bv.ResourceID),
		VolumeId:                 utils.GetOptString(&bv.VolumeID),
		State:                    gcpgenserver.NewOptBackupV1betaState(gcpgenserver.BackupV1betaState(bv.State)),
		Created:                  utils.GetOptDateTime(&bv.Created),
		EnforcedRetentionEndTime: utils.GetOptDateTime(bv.EnforcedRetentionEndTime),
		BackupId:                 utils.GetOptString(&bv.BackupID),
		VolumeUsageBytes:         utils.GetOptInt64(bv.VolumeUsageBytes),
		SourceVolume:             utils.GetOptString(&bv.SourceVolume),
		BackupVaultId:            utils.GetOptString(bv.BackupVaultID),
		Description:              utils.GetOptString(bv.Description),
		SourceSnapshot:           utils.GetOptString(bv.SourceSnapshot),
		BackupType:               gcpgenserver.NewOptBackupV1betaBackupType(gcpgenserver.BackupV1betaBackupType(bv.BackupType)),
		BackupChainBytes:         utils.GetOptInt64(bv.BackupChainBytes),
		SatisfiesPzs:             utils.GetOptBool(bv.SatisfiesPzs),
		SatisfiesPzi:             utils.GetOptBool(bv.SatisfiesPzi),
		VolumeRegion:             utils.GetOptString(bv.VolumeRegion),
		BackupRegion:             utils.GetOptString(bv.BackupRegion),
		AssetLocationMetadata: func() gcpgenserver.OptAssetLocationMetadataV2 {
			if bv.AssetLocationMetadata != nil {
				var assets []gcpgenserver.ChildAssetV2
				for _, asset := range bv.AssetLocationMetadata.ChildAssets {
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
