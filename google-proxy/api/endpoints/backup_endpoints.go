package api

import (
	"context"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/backups"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func (h Handler) V1betaGetMultipleBackups(ctx context.Context, req *gcpgenserver.BackupUUIDListV1beta, params gcpgenserver.V1betaGetMultipleBackupsParams) (gcpgenserver.V1betaGetMultipleBackupsRes, error) {
	logger := utils.GetLoggerFromContext(ctx)
	jwtToken := utils.GetJWTTokenFromContext(ctx)
	cvpClient := createClient(logger, jwtToken)

	var backupUUIDs []string
	backupUUIDs = append(backupUUIDs, req.BackupUUIDs...)

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
	var enforcedTime strfmt.DateTime
	if bv.EnforcedRetentionEndTime != nil {
		enforcedTime = *bv.EnforcedRetentionEndTime
	}
	var snapshotName string
	if bv.SourceSnapshot != nil {
		snapshotName = *bv.SourceSnapshot
	}
	var satisfiesPzs bool
	if bv.SatisfiesPzs != nil {
		satisfiesPzs = *bv.SatisfiesPzs
	}
	var satisfiesPzi bool
	if bv.SatisfiesPzi != nil {
		satisfiesPzi = *bv.SatisfiesPzi
	}

	var assetLocationMetadata *gcpgenserver.AssetLocationMetadataV2
	if bv.AssetLocationMetadata != nil {
		var assets []gcpgenserver.ChildAssetV2
		inChildAssets := bv.AssetLocationMetadata.ChildAssets
		for _, asset := range inChildAssets {
			var cvpAsset gcpgenserver.ChildAssetV2
			cvpAsset.AssetType = gcpgenserver.NewOptString(asset.AssetType)
			cvpAsset.AssetNames = asset.AssetNames
			assets = append(assets, cvpAsset)
		}
		assetLocationMetadata = &gcpgenserver.AssetLocationMetadataV2{
			ChildAssets: assets,
		}
	}

	return gcpgenserver.BackupV1beta{
		ResourceId:               gcpgenserver.NewOptString(bv.ResourceID),
		VolumeId:                 gcpgenserver.NewOptString(bv.VolumeID),
		State:                    gcpgenserver.NewOptBackupV1betaState(gcpgenserver.BackupV1betaState(bv.State)),
		Created:                  gcpgenserver.NewOptDateTime(time.Time(bv.Created)),
		EnforcedRetentionEndTime: gcpgenserver.NewOptDateTime(time.Time(enforcedTime)),
		BackupId:                 gcpgenserver.NewOptString(bv.BackupID),
		VolumeUsageBytes:         gcpgenserver.NewOptInt64(*bv.VolumeUsageBytes),
		SourceVolume:             gcpgenserver.NewOptString(bv.SourceVolume),
		BackupVaultId:            gcpgenserver.NewOptString(*bv.BackupVaultID),
		Description:              gcpgenserver.NewOptString(*bv.Description),
		SourceSnapshot:           gcpgenserver.NewOptString(snapshotName),
		BackupType:               gcpgenserver.NewOptBackupV1betaBackupType(gcpgenserver.BackupV1betaBackupType(bv.BackupType)),
		BackupChainBytes:         gcpgenserver.NewOptInt64(*bv.BackupChainBytes),
		SatisfiesPzs:             gcpgenserver.NewOptBool(satisfiesPzs),
		SatisfiesPzi:             gcpgenserver.NewOptBool(satisfiesPzi),
		VolumeRegion:             gcpgenserver.NewOptString(*bv.VolumeRegion),
		BackupRegion:             gcpgenserver.NewOptString(*bv.BackupRegion),
		AssetLocationMetadata: func() gcpgenserver.OptAssetLocationMetadataV2 {
			if assetLocationMetadata != nil {
				return gcpgenserver.NewOptAssetLocationMetadataV2(*assetLocationMetadata)
			}
			return gcpgenserver.OptAssetLocationMetadataV2{}
		}(),
	}
}
