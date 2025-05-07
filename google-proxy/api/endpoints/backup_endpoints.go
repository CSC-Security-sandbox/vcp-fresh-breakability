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
	return gcpgenserver.BackupV1beta{
		ResourceId:               gcpgenserver.OptString{Value: bv.ResourceID},
		VolumeId:                 gcpgenserver.OptString{Value: bv.VolumeID},
		State:                    gcpgenserver.OptBackupV1betaState{Value: gcpgenserver.BackupV1betaState(bv.State)},
		Created:                  gcpgenserver.OptDateTime{Value: time.Time(bv.Created)},
		EnforcedRetentionEndTime: gcpgenserver.OptDateTime{Value: time.Time(enforcedTime)},
		BackupId:                 gcpgenserver.OptString{Value: bv.BackupID},
		VolumeUsageBytes:         gcpgenserver.OptInt64{Value: *bv.VolumeUsageBytes},
		SourceVolume:             gcpgenserver.OptString{Value: bv.SourceVolume},
		BackupVaultId:            gcpgenserver.OptString{Value: *bv.BackupVaultID},
		Description:              gcpgenserver.OptString{Value: *bv.Description},
		SourceSnapshot:           gcpgenserver.OptString{Value: *bv.SourceSnapshot},
		BackupType:               gcpgenserver.OptBackupV1betaBackupType{Value: gcpgenserver.BackupV1betaBackupType(bv.BackupType)},
		BackupChainBytes:         gcpgenserver.OptInt64{Value: *bv.BackupChainBytes},
		SatisfiesPzs:             gcpgenserver.OptBool{Value: *bv.SatisfiesPzs},
		SatisfiesPzi:             gcpgenserver.OptBool{Value: *bv.SatisfiesPzi},
		VolumeRegion:             gcpgenserver.OptString{Value: *bv.VolumeRegion},
		BackupRegion:             gcpgenserver.OptString{Value: *bv.BackupRegion},
		AssetLocationMetadata:    gcpgenserver.OptAssetLocationMetadataV2{Value: gcpgenserver.AssetLocationMetadataV2{}},
	}
}
