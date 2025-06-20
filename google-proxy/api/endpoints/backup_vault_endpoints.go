package api

import (
	"context"
	"encoding/json"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/backup_vault"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/helper"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	utilsConvertJsonToModel = utils.ConvertJsonToModel
	cvpCreateClient         = cvp.CreateClient
)

const ErrBackupVaultNotFound = "backup vault not found"

func (h Handler) V1betaCreateBackupVault(ctx context.Context, req *gcpgenserver.BackupVaultCreateV1beta, reqPayloadparams gcpgenserver.V1betaCreateBackupVaultParams) (gcpgenserver.V1betaCreateBackupVaultRes, error) {
	logger := util.GetLogger(ctx)
	_, _, parsingErr := parseAndValidateRegionAndZone(reqPayloadparams.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaCreateBackupVaultBadRequest{
			Code:    parsingErr.Code,
			Message: parsingErr.Message,
		}, nil
	}
	var resourceID string
	if req.ResourceId.IsSet() {
		resourceID = req.ResourceId.Value
	} else {
		resourceID = "" // Or handle the unset case appropriately
	}
	req.ResourceId.Value = resourceID
	var description string
	if req.Description.IsSet() {
		description = req.Description.Value
	}
	req.Description.Value = description

	var backupRegion *string
	if req.BackupRegion.IsSet() {
		backupRegion = &req.BackupRegion.Value
	}

	// Check if the BackupVault already exists
	existingBackupVault, err := h.Orchestrator.GetBackupVaultByNameAndOwnerID(ctx, req.ResourceId.Value, reqPayloadparams.ProjectNumber)
	if err == nil && existingBackupVault != nil {
		logger.Infof("backupVault with name: %s already exists ", req.ResourceId)
		bvJSON, err := json.Marshal(existingBackupVault)
		if err != nil {
			logger.Error("Failed to marshal backup vault", "error", err)
			return &gcpgenserver.V1betaCreateBackupVaultInternalServerError{
				Code:    500,
				Message: "Failed to marshal Backup vault",
			}, err
		}

		return &gcpgenserver.OperationV1beta{
			Name:     gcpgenserver.OptString{Value: "operation-id"},
			Done:     gcpgenserver.NewOptBool(true),
			Response: bvJSON,
		}, nil
	} else if err != nil && err.Error() != ErrBackupVaultNotFound {
		logger.Error("Failed to check existing backupVault", "error", err)
		return &gcpgenserver.V1betaCreateBackupVaultInternalServerError{
			Code:    500,
			Message: "Failed to check existing Backup vault",
		}, err
	}

	// not exists in VCP, Call SDE for Creating
	GetSignedJwtToken := utils.GetJWTTokenFromContext(ctx)
	cvpClient := cvpCreateClient(logger, GetSignedJwtToken)
	xCorrelationID := utils.GetCoRelationIDFromContext(ctx)

	body := &models.BackupVaultCreateV1beta{
		BackupRegion: backupRegion,
		Description:  &req.Description.Value,
		ResourceID:   req.ResourceId.Value,
	}
	brPolicy := convertBackupRetentionPolicyToCvpModelForCreate(req.BackupRetentionPolicy)
	if brPolicy != nil {
		body.BackupRetentionPolicy = brPolicy
	}
	vault, err := cvpClient.BackupVault.V1betaCreateBackupVault(&backup_vault.V1betaCreateBackupVaultParams{
		LocationID:     reqPayloadparams.LocationId,
		ProjectNumber:  reqPayloadparams.ProjectNumber,
		XCorrelationID: &xCorrelationID,
		Body:           body,
	})
	if err != nil {
		switch e := err.(type) {
		case *backup_vault.V1betaCreateBackupVaultUnprocessableEntity:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaCreateBackupVaultUnprocessableEntity{
				Code:    code,
				Message: msg,
			}, nil
		case *backup_vault.V1betaCreateBackupVaultConflict:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaCreateBackupVaultConflict{
				Code:    code,
				Message: msg,
			}, nil
		case *backup_vault.V1betaCreateBackupVaultBadRequest:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaCreateBackupVaultBadRequest{
				Code:    code,
				Message: msg,
			}, nil
		case *backup_vault.V1betaCreateBackupVaultUnauthorized:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaCreateBackupVaultUnauthorized{
				Code:    code,
				Message: msg,
			}, nil

		case *backup_vault.V1betaCreateBackupVaultForbidden:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaCreateBackupVaultForbidden{
				Code:    code,
				Message: msg,
			}, nil

		case *backup_vault.V1betaCreateBackupVaultTooManyRequests:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaCreateBackupVaultTooManyRequests{
				Code:    code,
				Message: msg,
			}, nil
		case *backup_vault.V1betaCreateBackupVaultDefault:
			return &gcpgenserver.V1betaCreateBackupVaultInternalServerError{
				Code:    500,
				Message: err.Error(),
			}, nil
		default:
			return &gcpgenserver.V1betaCreateBackupVaultInternalServerError{
				Code:    500,
				Message: err.Error(),
			}, nil
		}
	}

	responseBytes, err := json.MarshalIndent(vault.Payload.Response, "", "  ")
	if err != nil {
		logger.Error("Failed to marshal response from SDE BackupVault creation", "error", err)
		return &gcpgenserver.V1betaCreateBackupVaultInternalServerError{
			Code:    500,
			Message: "Failed to marshal response from SDE BackupVault creation",
		}, err
	}
	data := models.BackupVaultV1beta{}
	err = utilsConvertJsonToModel(responseBytes, &data)
	if err != nil {
		return &gcpgenserver.V1betaCreateBackupVaultInternalServerError{
			Code:    500,
			Message: "Failed to convert response from SDE BackupVault creation to model",
		}, err
	}

	bvJSON, err := json.Marshal(data)
	if err != nil {
		logger.Error("Failed to marshal backup vault", err.Error())
		return &gcpgenserver.V1betaCreateBackupVaultInternalServerError{
			Code:    500,
			Message: "Failed to marshal Backup vault",
		}, err
	}
	return &gcpgenserver.OperationV1beta{
		Name:     gcpgenserver.OptString{Value: vault.Payload.Name},
		Done:     gcpgenserver.NewOptBool(true),
		Response: bvJSON,
	}, nil
}

func convertBackupRetentionPolicyToCvpModelForCreate(brPolicy gcpgenserver.OptBackupRetentionPolicyV1beta) *models.BackupRetentionPolicyV1beta {
	if brPolicy.IsSet() {
		brPolicyValue := brPolicy.Value
		brModel := &models.BackupRetentionPolicyV1beta{}
		if brPolicyValue.BackupMinimumEnforcedRetentionDays.IsSet() {
			retentionDays := int64(brPolicyValue.BackupMinimumEnforcedRetentionDays.Value)
			brModel.BackupMinimumEnforcedRetentionDays = &retentionDays
		}
		if brPolicy.Value.DailyBackupImmutable.IsSet() {
			brModel.DailyBackupImmutable = brPolicyValue.DailyBackupImmutable.Value
		}
		if brPolicy.Value.ManualBackupImmutable.IsSet() {
			brModel.ManualBackupImmutable = brPolicyValue.ManualBackupImmutable.Value
		}
		if brPolicy.Value.MonthlyBackupImmutable.IsSet() {
			brModel.MonthlyBackupImmutable = brPolicyValue.MonthlyBackupImmutable.Value
		}
		if brPolicy.Value.WeeklyBackupImmutable.IsSet() {
			brModel.WeeklyBackupImmutable = brPolicyValue.WeeklyBackupImmutable.Value
		}
		return brModel
	}
	return nil
}

func safeBoolPointer(opt gcpgenserver.OptBackupRetentionPolicyV1beta, getter func() bool) *bool {
	if opt.IsSet() {
		val := getter()
		return &val
	}
	return nil
}

func (h Handler) V1betaDeleteBackupVault(ctx context.Context, params gcpgenserver.V1betaDeleteBackupVaultParams) (r gcpgenserver.V1betaDeleteBackupVaultRes, _ error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)

	deleteParams := &backup_vault.V1betaDeleteBackupVaultParams{
		LocationID:     params.LocationId,
		ProjectNumber:  params.ProjectNumber,
		XCorrelationID: &params.XCorrelationID.Value,
		BackupVaultID:  params.BackupVaultId,
	}
	jwtToken := utils.GetJWTTokenFromContext(ctx)
	cvpClient := createClient(logger, jwtToken)
	deleted, _, err := cvpClient.BackupVault.V1betaDeleteBackupVault(deleteParams)
	if err != nil {
		switch e := err.(type) {
		case *backup_vault.V1betaDeleteBackupVaultUnprocessableEntity:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaDeleteBackupVaultUnprocessableEntity{
				Code:    code,
				Message: msg,
			}, nil
		case *backup_vault.V1betaDeleteBackupVaultConflict:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaDeleteBackupVaultConflict{
				Code:    code,
				Message: msg,
			}, nil
		case *backup_vault.V1betaDeleteBackupVaultBadRequest:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaDeleteBackupVaultBadRequest{
				Code:    code,
				Message: msg,
			}, nil
		case *backup_vault.V1betaDeleteBackupVaultUnauthorized:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaDeleteBackupVaultUnauthorized{
				Code:    code,
				Message: msg,
			}, nil
		case *backup_vault.V1betaDeleteBackupVaultNotFound:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaDeleteBackupVaultNotFound{
				Code:    code,
				Message: msg,
			}, nil

		case *backup_vault.V1betaDeleteBackupVaultForbidden:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaDeleteBackupVaultForbidden{
				Code:    code,
				Message: msg,
			}, nil

		case *backup_vault.V1betaDeleteBackupVaultTooManyRequests:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaDeleteBackupVaultTooManyRequests{
				Code:    code,
				Message: msg,
			}, nil
		case *backup_vault.V1betaDeleteBackupVaultDefault:
			return &gcpgenserver.V1betaDeleteBackupVaultInternalServerError{
				Code:    500,
				Message: err.Error(),
			}, nil
		}
	}
	deletedOperationResponse := convertOperationToOperationV1Beta(deleted.Payload)
	return deletedOperationResponse, nil
}

func (h Handler) V1betaDescribeBackupVault(ctx context.Context, params gcpgenserver.V1betaDescribeBackupVaultParams) (r gcpgenserver.V1betaDescribeBackupVaultRes, _ error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	describeParams := &backup_vault.V1betaDescribeBackupVaultParams{
		LocationID:     params.LocationId,
		ProjectNumber:  params.ProjectNumber,
		XCorrelationID: &params.XCorrelationID.Value,
		BackupVaultID:  params.BackupVaultId,
	}
	jwtToken := utils.GetJWTTokenFromContext(ctx)
	cvpClient := createClient(logger, jwtToken)
	cvpResponse, err := cvpClient.BackupVault.V1betaDescribeBackupVault(describeParams)
	if err != nil {
		switch e := err.(type) {
		case *backup_vault.V1betaDescribeBackupVaultUnprocessableEntity:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaDescribeBackupVaultUnprocessableEntity{
				Code:    code,
				Message: msg,
			}, nil
		case *backup_vault.V1betaDescribeBackupVaultNotFound:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaDescribeBackupVaultNotFound{
				Code:    code,
				Message: msg,
			}, nil
		case *backup_vault.V1betaDescribeBackupVaultBadRequest:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaDescribeBackupVaultBadRequest{
				Code:    code,
				Message: msg,
			}, nil
		case *backup_vault.V1betaDescribeBackupVaultUnauthorized:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaDescribeBackupVaultUnauthorized{
				Code:    code,
				Message: msg,
			}, nil

		case *backup_vault.V1betaDescribeBackupVaultForbidden:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaDescribeBackupVaultForbidden{
				Code:    code,
				Message: msg,
			}, nil

		case *backup_vault.V1betaDescribeBackupVaultTooManyRequests:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaDescribeBackupVaultTooManyRequests{
				Code:    code,
				Message: msg,
			}, nil
		case *backup_vault.V1betaDescribeBackupVaultDefault:
			return &gcpgenserver.V1betaDescribeBackupVaultInternalServerError{
				Code:    500,
				Message: err.Error(),
			}, nil
		}
	}
	response := convertBackupVaultV1Beta(cvpResponse.Payload)
	return &response, nil
}

func (h Handler) V1betaGetMultipleBackupVaults(ctx context.Context, req *gcpgenserver.BackupVaultUuidListV1beta, params gcpgenserver.V1betaGetMultipleBackupVaultsParams) (r gcpgenserver.V1betaGetMultipleBackupVaultsRes, _ error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	body := &models.BackupVaultUUIDListV1beta{
		BackupVaultUUIDs: req.BackupVaultUuids,
	}
	listParams := &backup_vault.V1betaGetMultipleBackupVaultsParams{
		LocationID:     params.LocationId,
		ProjectNumber:  params.ProjectNumber,
		XCorrelationID: &params.XCorrelationID.Value,
		Body:           body,
	}
	jwtToken := utils.GetJWTTokenFromContext(ctx)
	cvpClient := createClient(logger, jwtToken)
	cvpResponse, err := cvpClient.BackupVault.V1betaGetMultipleBackupVaults(listParams)
	if err != nil {
		switch e := err.(type) {
		case *backup_vault.V1betaGetMultipleBackupVaultsBadRequest:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaGetMultipleBackupVaultsBadRequest{
				Code:    code,
				Message: msg,
			}, nil
		case *backup_vault.V1betaGetMultipleBackupVaultsUnauthorized:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaGetMultipleBackupVaultsUnauthorized{
				Code:    code,
				Message: msg,
			}, nil

		case *backup_vault.V1betaGetMultipleBackupVaultsForbidden:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaGetMultipleBackupVaultsForbidden{
				Code:    code,
				Message: msg,
			}, nil
		case *backup_vault.V1betaGetMultipleBackupVaultsNotFound:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaGetMultipleBackupVaultsNotFound{
				Code:    code,
				Message: msg,
			}, nil
		case *backup_vault.V1betaGetMultipleBackupVaultsDefault:
			return &gcpgenserver.V1betaGetMultipleBackupVaultsInternalServerError{
				Code:    500,
				Message: err.Error(),
			}, nil
		}
	}
	if cvpResponse == nil || cvpResponse.Payload == nil {
		return &gcpgenserver.V1betaGetMultipleBackupVaultsInternalServerError{
			Code:    500,
			Message: "unknown error during the get multiple backup vaults",
		}, nil
	}
	bvResponse := gcpgenserver.V1betaGetMultipleBackupVaultsOK{
		BackupVaults: []gcpgenserver.BackupVaultV1beta{},
	}
	for _, bv := range cvpResponse.Payload.BackupVaults {
		bvResponse.BackupVaults = append(bvResponse.BackupVaults, convertBackupVaultV1Beta(bv))
	}
	return &bvResponse, nil
}

func (h Handler) V1betaListBackupVaults(ctx context.Context, params gcpgenserver.V1betaListBackupVaultsParams) (r gcpgenserver.V1betaListBackupVaultsRes, _ error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	listParams := &backup_vault.V1betaListBackupVaultsParams{
		LocationID:     params.LocationId,
		ProjectNumber:  params.ProjectNumber,
		XCorrelationID: &params.XCorrelationID.Value,
	}
	jwtToken := utils.GetJWTTokenFromContext(ctx)
	cvpClient := createClient(logger, jwtToken)
	cvpResponse, err := cvpClient.BackupVault.V1betaListBackupVaults(listParams)
	if err != nil {
		return nil, err
	}
	// Converting CVP model to gcpgenserver.BackupVaultV1beta
	bvResponse := gcpgenserver.V1betaListBackupVaultsOK{
		BackupVaults: []gcpgenserver.BackupVaultV1beta{},
	}
	for _, bv := range cvpResponse.Payload.BackupVaults {
		bvResponse.BackupVaults = append(bvResponse.BackupVaults, convertBackupVaultV1Beta(bv))
	}
	return &bvResponse, nil
}

func (h Handler) V1betaUpdateBackupVault(ctx context.Context, req *gcpgenserver.BackupVaultUpdateV1beta, params gcpgenserver.V1betaUpdateBackupVaultParams) (r gcpgenserver.V1betaUpdateBackupVaultRes, _ error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	brPolicy := convertBackupRetentionPolicyToCvpModelForUpdate(req.BackupRetentionPolicy)
	body := &models.BackupVaultUpdateV1beta{
		BackupRetentionPolicy: brPolicy,
		Description:           &req.Description.Value,
	}
	updateParams := &backup_vault.V1betaUpdateBackupVaultParams{
		LocationID:     params.LocationId,
		ProjectNumber:  params.ProjectNumber,
		XCorrelationID: &params.XCorrelationID.Value,
		BackupVaultID:  params.BackupVaultId,
		Body:           body,
	}
	jwtToken := utils.GetJWTTokenFromContext(ctx)
	cvpClient := createClient(logger, jwtToken)
	updated, err := cvpClient.BackupVault.V1betaUpdateBackupVault(updateParams)
	if err != nil {
		switch e := err.(type) {
		case *backup_vault.V1betaUpdateBackupVaultUnprocessableEntity:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaUpdateBackupVaultUnprocessableEntity{
				Code:    code,
				Message: msg,
			}, nil
		case *backup_vault.V1betaUpdateBackupVaultConflict:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaUpdateBackupVaultConflict{
				Code:    code,
				Message: msg,
			}, nil
		case *backup_vault.V1betaUpdateBackupVaultBadRequest:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaUpdateBackupVaultBadRequest{
				Code:    code,
				Message: msg,
			}, nil
		case *backup_vault.V1betaUpdateBackupVaultUnauthorized:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaUpdateBackupVaultUnauthorized{
				Code:    code,
				Message: msg,
			}, nil
		case *backup_vault.V1betaUpdateBackupVaultForbidden:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaUpdateBackupVaultForbidden{
				Code:    code,
				Message: msg,
			}, nil

		case *backup_vault.V1betaUpdateBackupVaultTooManyRequests:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaUpdateBackupVaultTooManyRequests{
				Code:    code,
				Message: msg,
			}, nil
		case *backup_vault.V1betaUpdateBackupVaultDefault:
			return &gcpgenserver.V1betaUpdateBackupVaultInternalServerError{
				Code:    500,
				Message: err.Error(),
			}, nil
		}
	}
	if updated == nil || updated.Payload == nil {
		return &gcpgenserver.V1betaUpdateBackupVaultInternalServerError{
			Code:    500,
			Message: "unknown error during the update backup vault",
		}, nil
	}
	response := convertOperationToOperationV1Beta(updated.Payload)
	return response, nil
}

func convertBackupRetentionPolicyToCvpModelForUpdate(brPolicy gcpgenserver.OptBackupRetentionPolicyUpdateV1beta) *models.BackupRetentionPolicyUpdateV1beta {
	if brPolicy.IsSet() {
		brPolicyValue := brPolicy.Value
		brModel := &models.BackupRetentionPolicyUpdateV1beta{}
		if brPolicyValue.BackupMinimumEnforcedRetentionDays.IsSet() {
			retentionDays := int64(brPolicyValue.BackupMinimumEnforcedRetentionDays.Value)
			brModel.BackupMinimumEnforcedRetentionDays = &retentionDays
		}
		if brPolicy.Value.DailyBackupImmutable.IsSet() {
			brModel.DailyBackupImmutable = &brPolicyValue.DailyBackupImmutable.Value
		}
		if brPolicy.Value.ManualBackupImmutable.IsSet() {
			brModel.ManualBackupImmutable = &brPolicyValue.ManualBackupImmutable.Value
		}
		if brPolicy.Value.MonthlyBackupImmutable.IsSet() {
			brModel.MonthlyBackupImmutable = &brPolicyValue.MonthlyBackupImmutable.Value
		}
		if brPolicy.Value.WeeklyBackupImmutable.IsSet() {
			brModel.WeeklyBackupImmutable = &brPolicyValue.WeeklyBackupImmutable.Value
		}
		return brModel
	}
	return nil
}

func convertBackupVaultV1Beta(bv *models.BackupVaultV1beta) gcpgenserver.BackupVaultV1beta {
	backupRetentionPolicy := gcpgenserver.BackupRetentionPolicyV1beta{}
	if bv.BackupRetentionPolicy != nil {
		backupRetentionPolicy = gcpgenserver.BackupRetentionPolicyV1beta{
			BackupMinimumEnforcedRetentionDays: gcpgenserver.NewOptInt(int(*bv.BackupRetentionPolicy.BackupMinimumEnforcedRetentionDays)),
			DailyBackupImmutable:               gcpgenserver.NewOptBool(bv.BackupRetentionPolicy.DailyBackupImmutable),
			ManualBackupImmutable:              gcpgenserver.NewOptBool(bv.BackupRetentionPolicy.ManualBackupImmutable),
			MonthlyBackupImmutable:             gcpgenserver.NewOptBool(bv.BackupRetentionPolicy.MonthlyBackupImmutable),
			WeeklyBackupImmutable:              gcpgenserver.NewOptBool(bv.BackupRetentionPolicy.WeeklyBackupImmutable),
		}
	}
	convertedBackupVault := gcpgenserver.BackupVaultV1beta{
		BackupVaultId:         gcpgenserver.NewOptString(bv.BackupVaultID),
		State:                 gcpgenserver.NewOptBackupVaultV1betaState(gcpgenserver.BackupVaultV1betaState(bv.State)),
		StateDetails:          gcpgenserver.NewOptString(bv.StateDetails),
		CreatedAt:             gcpgenserver.NewOptDateTime(time.Time(bv.CreatedAt)),
		BackupRetentionPolicy: gcpgenserver.NewOptBackupRetentionPolicyV1beta(backupRetentionPolicy),
	}
	if bv.BackupRegion != nil {
		convertedBackupVault.BackupRegion = gcpgenserver.NewOptString(*bv.BackupRegion)
	}
	if bv.DestinationBackupVault != nil {
		convertedBackupVault.DestinationBackupVault = gcpgenserver.NewOptString(*bv.DestinationBackupVault)
	}
	if bv.SourceBackupVault != nil {
		convertedBackupVault.SourceBackupVault = gcpgenserver.NewOptString(*bv.SourceBackupVault)
	}
	if bv.SourceRegion != nil {
		convertedBackupVault.SourceRegion = gcpgenserver.NewOptString(*bv.SourceRegion)
	}
	if bv.Description != nil {
		convertedBackupVault.Description = gcpgenserver.NewOptString(*bv.Description)
	}
	if bv.ResourceID != nil {
		convertedBackupVault.ResourceId = *bv.ResourceID
	}
	if bv.BackupVaultType != nil {
		convertedBackupVault.BackupVaultType = gcpgenserver.NewOptBackupVaultV1betaBackupVaultType(gcpgenserver.BackupVaultV1betaBackupVaultType(*bv.BackupVaultType))
	}

	return convertedBackupVault
}
