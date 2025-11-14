package api

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-faster/jx"
	"github.com/go-openapi/strfmt"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/backup_vault"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	coremodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/helper"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	utilsConvertJsonToModel = utils.ConvertJsonToModel
	cvpCreateClient         = cvp.CreateClient
	updateBackupVaultInSDE  = _updateBackupVaultInSDE
	jsonMarshal             = json.Marshal
	deleteBackupVaultInSDE  = _deleteBackupVaultInSDE
)

// _validateRetentionParameters validates retention policy updates against current backup vault settings
func _validateRetentionParameters(currentBackupVault *coremodels.BackupVaultV1beta, newRetentionParams *commonparams.BackupRetentionPolicyParams) error {
	// Constants
	maxImmutablePeriodWhenDailyImmutability := int64(commonparams.ImmutablePeriodInDaysMaxDailyEnabled)
	maxImmutablePeriod := int64(commonparams.ImmutablePeriodInDaysMax)

	// Get current immutable attributes
	currentAttrs := &currentBackupVault.BackupRetentionPolicy

	// Validate retention duration - no decrease allowed
	if newRetentionParams.BackupMinimumEnforcedRetentionDuration != nil {
		newRetentionDays := *newRetentionParams.BackupMinimumEnforcedRetentionDuration

		if currentAttrs.BackupMinimumEnforcedRetentionDuration != nil {
			currentRetentionDays := *currentAttrs.BackupMinimumEnforcedRetentionDuration
			if currentRetentionDays > 0 && newRetentionDays < currentRetentionDays {
				return fmt.Errorf("cannot decrease backup minimum enforced retention duration from %d to %d days",
					currentRetentionDays, newRetentionDays)
			}
		}
	}

	// Validate immutability cannot be disabled once enabled
	if newRetentionParams.IsDailyBackupImmutable != nil {
		if currentAttrs.IsDailyBackupImmutable && !*newRetentionParams.IsDailyBackupImmutable {
			return fmt.Errorf("cannot disable daily backup immutability once enabled")
		}
	}

	if newRetentionParams.IsWeeklyBackupImmutable != nil {
		if currentAttrs.IsWeeklyBackupImmutable && !*newRetentionParams.IsWeeklyBackupImmutable {
			return fmt.Errorf("cannot disable weekly backup immutability once enabled")
		}
	}

	if newRetentionParams.IsMonthlyBackupImmutable != nil {
		if currentAttrs.IsMonthlyBackupImmutable && !*newRetentionParams.IsMonthlyBackupImmutable {
			return fmt.Errorf("cannot disable monthly backup immutability once enabled")
		}
	}

	if newRetentionParams.IsAdhocBackupImmutable != nil {
		if currentAttrs.IsAdhocBackupImmutable && !*newRetentionParams.IsAdhocBackupImmutable {
			return fmt.Errorf("cannot disable manual/adhoc backup immutability once enabled")
		}
	}

	// Determine final immutability states (merge current + new)
	finalDailyImmutable := determineImmutableState(currentAttrs.IsDailyBackupImmutable, newRetentionParams.IsDailyBackupImmutable)
	finalWeeklyImmutable := determineImmutableState(currentAttrs.IsWeeklyBackupImmutable, newRetentionParams.IsWeeklyBackupImmutable)
	finalMonthlyImmutable := determineImmutableState(currentAttrs.IsMonthlyBackupImmutable, newRetentionParams.IsMonthlyBackupImmutable)
	finalAdhocImmutable := determineImmutableState(currentAttrs.IsAdhocBackupImmutable, newRetentionParams.IsAdhocBackupImmutable)

	// Ensure at least one backup type remains immutable
	if !finalDailyImmutable && !finalWeeklyImmutable && !finalMonthlyImmutable && !finalAdhocImmutable {
		return fmt.Errorf("at least one backup type must remain immutable")
	}

	// Validate retention duration range based on daily and non daily immutability
	finalRetentionDays := getFinalRetentionDays(currentAttrs, newRetentionParams)
	if finalRetentionDays > 0 {
		if finalDailyImmutable {
			if finalRetentionDays > maxImmutablePeriodWhenDailyImmutability {
				return fmt.Errorf("retention period in days should be ≤ %d when daily backups are immutable", maxImmutablePeriodWhenDailyImmutability)
			}
		} else {
			if finalRetentionDays > maxImmutablePeriod {
				return fmt.Errorf("retention period in days should be ≤ %d when daily backups are not immutable", maxImmutablePeriod)
			}
		}
	}

	return nil // validation passed
}

// determineImmutableState determines the final immutable state by preferring new state over current
func determineImmutableState(currentState bool, newState *bool) bool {
	if newState != nil {
		return *newState
	}
	return currentState
}

// getFinalRetentionDays gets the final retention days by preferring new value over current
func getFinalRetentionDays(currentAttrs *coremodels.BackupRetentionPolicyparams, newParams *commonparams.BackupRetentionPolicyParams) int64 {
	if newParams.BackupMinimumEnforcedRetentionDuration != nil {
		return *newParams.BackupMinimumEnforcedRetentionDuration
	}

	if currentAttrs.BackupMinimumEnforcedRetentionDuration != nil {
		return *currentAttrs.BackupMinimumEnforcedRetentionDuration
	}

	return 0
}

// extractRetentionPolicyParams extracts retention policy parameters from the request
func extractRetentionPolicyParams(req *gcpgenserver.BackupVaultUpdateV1beta) *commonparams.BackupRetentionPolicyParams {
	var backupMinimumEnforcedRetentionDuration *int64
	var dailyBackupImmutable, weeklyBackupImmutable, monthlyBackupImmutable, adhocBackupImmutable *bool

	if req.BackupRetentionPolicy.IsSet() && req.BackupRetentionPolicy.Value.BackupMinimumEnforcedRetentionDays.IsSet() {
		val := int64(req.BackupRetentionPolicy.Value.BackupMinimumEnforcedRetentionDays.Value)
		backupMinimumEnforcedRetentionDuration = &val
	}
	if req.BackupRetentionPolicy.IsSet() && req.BackupRetentionPolicy.Value.DailyBackupImmutable.IsSet() {
		val := req.BackupRetentionPolicy.Value.DailyBackupImmutable.Value
		dailyBackupImmutable = &val
	}
	if req.BackupRetentionPolicy.IsSet() && req.BackupRetentionPolicy.Value.WeeklyBackupImmutable.IsSet() {
		val := req.BackupRetentionPolicy.Value.WeeklyBackupImmutable.Value
		weeklyBackupImmutable = &val
	}
	if req.BackupRetentionPolicy.IsSet() && req.BackupRetentionPolicy.Value.MonthlyBackupImmutable.IsSet() {
		val := req.BackupRetentionPolicy.Value.MonthlyBackupImmutable.Value
		monthlyBackupImmutable = &val
	}
	if req.BackupRetentionPolicy.IsSet() && req.BackupRetentionPolicy.Value.ManualBackupImmutable.IsSet() {
		val := req.BackupRetentionPolicy.Value.ManualBackupImmutable.Value
		adhocBackupImmutable = &val
	}

	return &commonparams.BackupRetentionPolicyParams{
		BackupMinimumEnforcedRetentionDuration: backupMinimumEnforcedRetentionDuration,
		IsDailyBackupImmutable:                 dailyBackupImmutable,
		IsWeeklyBackupImmutable:                weeklyBackupImmutable,
		IsMonthlyBackupImmutable:               monthlyBackupImmutable,
		IsAdhocBackupImmutable:                 adhocBackupImmutable}
}

func (h Handler) V1betaCreateBackupVault(ctx context.Context, req *gcpgenserver.BackupVaultCreateV1beta, reqPayloadparams gcpgenserver.V1betaCreateBackupVaultParams) (gcpgenserver.V1betaCreateBackupVaultRes, error) {
	if !backupEnabled {
		return &gcpgenserver.V1betaCreateBackupVaultBadRequest{
			Code:    400,
			Message: "Backup feature is currently not enabled.",
		}, nil
	}
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, reqPayloadparams.ProjectNumber, reqPayloadparams.LocationId, nil)
	_, _, parsingErr := parseAndValidateRegionAndZone(reqPayloadparams.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaCreateBackupVaultBadRequest{
			Code:    parsingErr.Code,
			Message: parsingErr.Message,
		}, nil
	}

	// Validate immutability settings if backup retention policy is provided
	if req.BackupRetentionPolicy.IsSet() {
		policy := req.BackupRetentionPolicy.Value

		// Extract retention days and daily backup immutable flag
		var retentionDays int = 0
		var isDailyBackupImmutable bool = false

		if policy.BackupMinimumEnforcedRetentionDays.IsSet() {
			retentionDays = policy.BackupMinimumEnforcedRetentionDays.Value
		}

		if policy.DailyBackupImmutable.IsSet() {
			isDailyBackupImmutable = policy.DailyBackupImmutable.Value
		}

		// Validate retention period based on daily backup immutability
		if retentionDays > 0 {
			if isDailyBackupImmutable {
				if retentionDays > commonparams.ImmutablePeriodInDaysMaxDailyEnabled {
					return &gcpgenserver.V1betaCreateBackupVaultBadRequest{
						Code:    400,
						Message: fmt.Sprintf("Retention period in days should be ≤ %d when considering daily backups for retention.", commonparams.ImmutablePeriodInDaysMaxDailyEnabled),
					}, nil
				}
			} else {
				if retentionDays > commonparams.ImmutablePeriodInDaysMax {
					return &gcpgenserver.V1betaCreateBackupVaultBadRequest{
						Code:    400,
						Message: fmt.Sprintf("Retention period in days should be ≤ %d when considering no daily backups for retention.", commonparams.ImmutablePeriodInDaysMax),
					}, nil
				}
			}
		}
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
		convertedBackupVault := convertCoreToCvpBackupVault(existingBackupVault)
		bvJSON, err := json.Marshal(convertedBackupVault)
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
	} else if err != nil && !errors.IsNotFoundErr(err) {
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

	bvJSON, err := jsonMarshal(data)
	if err != nil {
		logger.Error("Failed to marshal backup vault", err.Error())
		return &gcpgenserver.V1betaCreateBackupVaultInternalServerError{
			Code:    500,
			Message: "Failed to marshal Backup vault",
		}, err
	}
	return &gcpgenserver.OperationV1beta{
		Name:     gcpgenserver.NewOptString(vault.Payload.Name),
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

func _deleteBackupVaultInSDE(ctx context.Context, params gcpgenserver.V1betaDeleteBackupVaultParams, logger log.Logger) (r gcpgenserver.V1betaDeleteBackupVaultRes, _ error) {
	deleteParams := &backup_vault.V1betaDeleteBackupVaultParams{
		LocationID:     params.LocationId,
		ProjectNumber:  params.ProjectNumber,
		XCorrelationID: &params.XCorrelationID.Value,
		BackupVaultID:  params.BackupVaultId,
	}
	jwtToken := utils.GetJWTTokenFromContext(ctx)
	cvpClient := cvpCreateClient(logger, jwtToken)
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
	return convertOperationToOperationV1Beta(deleted.Payload), nil
}

func (h Handler) V1betaDeleteBackupVault(ctx context.Context, params gcpgenserver.V1betaDeleteBackupVaultParams) (r gcpgenserver.V1betaDeleteBackupVaultRes, _ error) {
	if !backupEnabled {
		return &gcpgenserver.V1betaDeleteBackupVaultBadRequest{
			Code:    400,
			Message: "Backup feature is currently not enabled.",
		}, nil
	}
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	_, _, parsingErr := parseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaDeleteBackupVaultBadRequest{
			Code:    parsingErr.Code,
			Message: parsingErr.Message,
		}, nil
	}

	_, err := h.Orchestrator.GetBackupVaultByUUID(ctx, params.BackupVaultId, params.ProjectNumber)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			// SDE delete will handle both in-region/cross-region BV cases as handled through pubsub
			sdeBvResponse, err := deleteBackupVaultInSDE(ctx, params, logger)
			if err != nil {
				return nil, err
			}
			return sdeBvResponse, nil
		}
		return &gcpgenserver.V1betaDeleteBackupVaultInternalServerError{
			Code:    500,
			Message: err.Error(),
		}, nil
	}

	param := &commonparams.BackupVaultParams{
		BackupVaultID: params.BackupVaultId,
		AccountName:   params.ProjectNumber,
		OwnerID:       params.ProjectNumber,
		Region:        params.LocationId,
	}
	_, operationID, err := h.Orchestrator.DeleteBackupVault(ctx, param)
	if err != nil {
		if errors.IsUserInputValidationErr(err) {
			logger.Error("Failed to update backupVault", err.Error())
			return &gcpgenserver.V1betaDeleteBackupVaultBadRequest{
				Code:    400,
				Message: err.Error(),
			}, nil
		}
		logger.Error("Failed to delete backupVault", err.Error())
		return &gcpgenserver.V1betaDeleteBackupVaultInternalServerError{
			Code:    500,
			Message: err.Error(),
		}, nil
	}
	if operationID != "" {
		return &gcpgenserver.OperationV1beta{
			Name: gcpgenserver.NewOptString(fmt.Sprintf("/v1beta/projects/%s/locations/%s/operations/%s", params.ProjectNumber, params.LocationId, operationID)),
			Done: gcpgenserver.NewOptBool(false),
		}, nil
	}
	return &gcpgenserver.OperationV1beta{}, nil
}

func (h Handler) V1betaDescribeBackupVault(ctx context.Context, params gcpgenserver.V1betaDescribeBackupVaultParams) (r gcpgenserver.V1betaDescribeBackupVaultRes, _ error) {
	if !backupEnabled {
		return &gcpgenserver.V1betaDescribeBackupVaultBadRequest{
			Code:    400,
			Message: "Backup feature is currently not enabled.",
		}, nil
	}
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
	vcpBackupVaultDetails, err := h.Orchestrator.GetBackupVaultByUUID(ctx, params.BackupVaultId, params.ProjectNumber)
	if err != nil && !errors.IsNotFoundErr(err) {
		return nil, err
	}
	if vcpBackupVaultDetails != nil {
		cvpResponse.Payload.State = vcpBackupVaultDetails.LifeCycleState
		cvpResponse.Payload.StateDetails = vcpBackupVaultDetails.LifeCycleStateDetails
	}
	response := convertBackupVaultV1Beta(cvpResponse.Payload)
	return &response, nil
}

func (h Handler) V1betaGetMultipleBackupVaults(ctx context.Context, req *gcpgenserver.BackupVaultUuidListV1beta, params gcpgenserver.V1betaGetMultipleBackupVaultsParams) (r gcpgenserver.V1betaGetMultipleBackupVaultsRes, _ error) {
	if !backupEnabled {
		return &gcpgenserver.V1betaGetMultipleBackupVaultsBadRequest{
			Code:    400,
			Message: "Backup feature is currently not enabled.",
		}, nil
	}
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
	vaultsDataModel, err := h.Orchestrator.GetMultipleBackupVaults(ctx, req.BackupVaultUuids)
	if err != nil {
		return &gcpgenserver.V1betaGetMultipleBackupVaultsInternalServerError{
			Code:    500,
			Message: err.Error(),
		}, nil
	}
	res := updateBackupVaultStateDetails(vaultsDataModel, cvpResponse.Payload.BackupVaults)
	for _, bv := range res {
		bvResponse.BackupVaults = append(bvResponse.BackupVaults, convertBackupVaultV1Beta(bv))
	}
	return &bvResponse, nil
}

func (h Handler) V1betaListBackupVaults(ctx context.Context, params gcpgenserver.V1betaListBackupVaultsParams) (r gcpgenserver.V1betaListBackupVaultsRes, _ error) {
	if !backupEnabled {
		return &gcpgenserver.V1betaListBackupVaultsBadRequest{
			Code:    400,
			Message: "Backup feature is currently not enabled.",
		}, nil
	}
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

	bvs, err := h.Orchestrator.ListBackupVaults(ctx, params.ProjectNumber)
	if err != nil {
		logger.Error("Failed to list backup vaults", "error", err)
		return &gcpgenserver.V1betaListBackupVaultsInternalServerError{
			Code:    500,
			Message: "failed to list backup vaults",
		}, nil
	}
	res := updateBackupVaultStateDetails(bvs, cvpResponse.Payload.BackupVaults)
	for _, bv := range res {
		bvResponse.BackupVaults = append(bvResponse.BackupVaults, convertBackupVaultV1Beta(bv))
	}
	return &bvResponse, nil
}

func updateBackupVaultStateDetails(bvs []*coremodels.BackupVaultV1beta, cvpBvs []*models.BackupVaultV1beta) []*models.BackupVaultV1beta {
	// Create a map for quick lookup of cvpBvs by ResourceID
	cvpBvMap := make(map[string]*models.BackupVaultV1beta)
	for _, cvpBv := range cvpBvs {
		if cvpBv.ResourceID != nil {
			cvpBvMap[*cvpBv.ResourceID] = cvpBv
		}
	}

	// Update cvpBvs using the map
	for _, bv := range bvs {
		if cvpBv, exists := cvpBvMap[bv.Name]; exists {
			cvpBv.State = bv.LifeCycleState
			cvpBv.StateDetails = bv.LifeCycleStateDetails
		}
	}

	// Return the updated slice
	return cvpBvs
}

func _updateBackupVaultInSDE(ctx context.Context, req *gcpgenserver.BackupVaultUpdateV1beta, params gcpgenserver.V1betaUpdateBackupVaultParams, description string) (r gcpgenserver.V1betaUpdateBackupVaultRes, _ error) {
	logger := util.GetLogger(ctx)
	GetSignedJwtToken := utils.GetJWTTokenFromContext(ctx)
	cvpClient := createClient(logger, GetSignedJwtToken)
	xCorrelationID := utils.GetCoRelationIDFromContext(ctx)
	body := &models.BackupVaultUpdateV1beta{
		Description: &description,
	}
	brPolicy := convertBackupRetentionPolicyToCvpModelForUpdate(req.BackupRetentionPolicy)
	if brPolicy != nil {
		body.BackupRetentionPolicy = brPolicy
	}

	vault, err := cvpClient.BackupVault.V1betaUpdateBackupVault(&backup_vault.V1betaUpdateBackupVaultParams{
		LocationID:     params.LocationId,
		ProjectNumber:  params.ProjectNumber,
		XCorrelationID: &xCorrelationID,
		BackupVaultID:  params.BackupVaultId,
		Body:           body,
	})

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
		default:
			return &gcpgenserver.V1betaUpdateBackupVaultInternalServerError{
				Code:    500,
				Message: err.Error(),
			}, nil
		}
	}
	return convertOperationToOperationV1Beta(vault.Payload), nil
}

func _validateBackupPoliciesForBackupVault(ctx context.Context, backupVault *coremodels.BackupVaultV1beta, newRetentionParams *commonparams.BackupRetentionPolicyParams, o orchestrator.OrchestratorFactory) error {
	return _validateBackupPoliciesForBackupVaultWithRetry(ctx, backupVault, newRetentionParams, o, commonparams.MaxRetries, commonparams.RetryDelay)
}

func _validateBackupPoliciesForBackupVaultWithRetry(ctx context.Context, backupVault *coremodels.BackupVaultV1beta, newRetentionParams *commonparams.BackupRetentionPolicyParams, o orchestrator.OrchestratorFactory, maxRetries int, retryInterval time.Duration) error {
	logger := util.GetLogger(ctx)

	for attempt := 1; attempt <= maxRetries; attempt++ {
		err := _performBackupPolicyValidation(ctx, backupVault, newRetentionParams, o)
		if err == nil {
			return nil // Success
		}

		// Check if this is a retryable error (backup policy in updating state)
		if isBackupPolicyRetryableError(err) {
			if attempt < maxRetries {
				logger.Warn("Backup policy validation failed due to concurrent update, retrying",
					"attempt", attempt,
					"maxRetries", maxRetries,
					"retryAfter", retryInterval,
					"error", err)
				commonparams.SleepFn(retryInterval)
				continue
			} else {
				logger.Error("Backup policy validation failed after all retry attempts",
					"attempt", attempt,
					"maxRetries", maxRetries,
					"error", err)
				return err
			}
		}

		// Non-retryable error, return immediately
		return err
	}

	return fmt.Errorf("backup policy validation failed after %d attempts", maxRetries)
}

func isBackupPolicyRetryableError(err error) bool {
	if err == nil {
		return false
	}
	// Check if the error is related to backup policy being in updating state
	var customError *vsaerrors.CustomError
	if vsaerrors.As(err, &customError) {
		if customError.TrackingID == vsaerrors.ErrImmutableValidationWithUpdatingBackupPolicy {
			return true
		}
	}
	return false
}

func _performBackupPolicyValidation(ctx context.Context, backupVault *coremodels.BackupVaultV1beta, newRetentionParams *commonparams.BackupRetentionPolicyParams, o orchestrator.OrchestratorFactory) error {
	logger := util.GetLogger(ctx)
	// Get all backup policy UUIDs associated with this backup vault
	backupPolicyUUIDs, err := o.GetBackupPolicyUUIDsFromBackupVaultUUID(ctx, backupVault.BackupVaultID, backupVault.AccountName)
	if err != nil {
		return fmt.Errorf("failed to get backup policy UUIDs from backup vault: %w", err)
	}

	// If no backup policies are associated, no validation needed
	if len(backupPolicyUUIDs) == 0 {
		logger.Info("No backup policies associated with backup vault", "backupVaultID", backupVault.BackupVaultID)
		return nil
	}

	// Fetch all backup policies for the given UUIDs (only non-deleted ones)
	_, backupPolicies, err := o.ListBackupPoliciesAndVolumeCount(ctx, backupVault.AccountName, backupPolicyUUIDs)
	if err != nil {
		return fmt.Errorf("failed to list backup policies for validation: %v", err)
	}

	logger.Info("Found backup policies for validation", "count", len(backupPolicies), "backupVaultID", backupVault.BackupVaultID)

	// Validate each backup policy
	for _, backupPolicy := range backupPolicies {
		if backupPolicy.State != coremodels.LifeCycleStateDeleted {
			// Check if backup policy is in updating state
			if backupPolicy.State == coremodels.LifeCycleStateUpdating {
				return vsaerrors.NewVCPError(vsaerrors.ErrImmutableValidationWithUpdatingBackupPolicy, fmt.Errorf("Cannot update backup vault: backup policy '%s' is currently being updated. Please wait for the policy update to complete.", backupPolicy.BackupPolicyUUID))
			}
			// Validate the backup policy against new immutability settings
			if err := validateBackupPolicyAgainstNewSettings(backupVault, backupPolicy, newRetentionParams); err != nil {
				return fmt.Errorf("backup policy '%s' validation failed: %w", backupPolicy.BackupPolicyUUID, err)
			}
		}
	}

	return nil
}

func validateBackupPolicyAgainstNewSettings(backupVault *coremodels.BackupVaultV1beta, backupPolicy *coremodels.BackupPolicy, newRetentionParams *commonparams.BackupRetentionPolicyParams) error {
	// Get current immutable attributes from BackupRetentionPolicy
	currentAttrs := &backupVault.BackupRetentionPolicy

	// Determine final retention days
	var finalRetentionDays int64 = 0
	if newRetentionParams.BackupMinimumEnforcedRetentionDuration != nil {
		finalRetentionDays = *newRetentionParams.BackupMinimumEnforcedRetentionDuration
	} else if currentAttrs.BackupMinimumEnforcedRetentionDuration != nil {
		finalRetentionDays = *currentAttrs.BackupMinimumEnforcedRetentionDuration
	}

	if finalRetentionDays <= 0 {
		return nil // No immutable period specified
	}

	// Determine final immutability states
	finalDailyImmutable := determineImmutableState(currentAttrs.IsDailyBackupImmutable, newRetentionParams.IsDailyBackupImmutable)
	finalWeeklyImmutable := determineImmutableState(currentAttrs.IsWeeklyBackupImmutable, newRetentionParams.IsWeeklyBackupImmutable)
	finalMonthlyImmutable := determineImmutableState(currentAttrs.IsMonthlyBackupImmutable, newRetentionParams.IsMonthlyBackupImmutable)

	backupPolicyParams := &commonparams.BackupPolicyParams{
		DailyBackupsToKeep:   backupPolicy.DailyBackupLimit,
		WeeklyBackupsToKeep:  backupPolicy.WeeklyBackupLimit,
		MonthlyBackupsToKeep: backupPolicy.MonthlyBackupLimit,
	}
	retentionPolicyParams := &commonparams.BackupRetentionPolicyParams{
		BackupMinimumEnforcedRetentionDuration: &finalRetentionDays,
		IsDailyBackupImmutable:                 &finalDailyImmutable,
		IsWeeklyBackupImmutable:                &finalWeeklyImmutable,
		IsMonthlyBackupImmutable:               &finalMonthlyImmutable,
	}

	err := commonparams.ValidateBackupPolicyRetentionLimits(backupPolicyParams, retentionPolicyParams)
	if err != nil {
		return fmt.Errorf("backup policy '%s' has retention limits that conflict with the new backup vault settings", backupPolicy.BackupPolicyUUID)
	}

	return nil
}

func (h Handler) V1betaUpdateBackupVault(ctx context.Context, req *gcpgenserver.BackupVaultUpdateV1beta, params gcpgenserver.V1betaUpdateBackupVaultParams) (r gcpgenserver.V1betaUpdateBackupVaultRes, _ error) {
	if !backupEnabled {
		return &gcpgenserver.V1betaUpdateBackupVaultBadRequest{
			Code:    400,
			Message: "Backup feature is currently not enabled.",
		}, nil
	}
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	_, _, parsingErr := parseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaUpdateBackupVaultBadRequest{
			Code:    parsingErr.Code,
			Message: parsingErr.Message,
		}, nil
	}
	var description string
	if req.Description.IsSet() {
		description = req.Description.Value
	}
	backupVault, err := h.Orchestrator.GetBackupVaultByUUID(ctx, params.BackupVaultId, params.ProjectNumber)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			sdeBvResponse, err := updateBackupVaultInSDE(ctx, req, params, description)
			if err != nil {
				return nil, err
			}
			return sdeBvResponse, nil
		}
		return &gcpgenserver.V1betaUpdateBackupVaultInternalServerError{
			Code:    500,
			Message: err.Error(),
		}, nil
	}

	// Extract retention policy parameters from request
	retentionParams := extractRetentionPolicyParams(req)
	if retentionParams == nil {
		retentionParams = &commonparams.BackupRetentionPolicyParams{}
	}
	if req.BackupRetentionPolicy.IsSet() {
		if !utils.IsImmutableBackupEnabled() {
			// Check if backup retention policy is being updated and vault is attached to volumes
			isAttached, err := h.Orchestrator.IsBackupVaultAttachedToVolume(ctx, params.BackupVaultId)
			if err != nil {
				logger.Errorf("Failed to check if backup vault %s is attached to vsa volume: %v", params.BackupVaultId, err)
				return &gcpgenserver.V1betaUpdateBackupVaultInternalServerError{
					Code:    500,
					Message: "Failed to check backup vault attachment status",
				}, nil
			}
			if isAttached {
				return &gcpgenserver.V1betaUpdateBackupVaultBadRequest{
					Code:    400,
					Message: utils.ImmutableBackupVaultErrMsg,
				}, nil
			}
		} else {
			// Validate retention parameters against current backup vault
			if validationErr := _validateRetentionParameters(backupVault, retentionParams); validationErr != nil {
				return &gcpgenserver.V1betaUpdateBackupVaultBadRequest{
					Code:    400,
					Message: validationErr.Error(),
				}, nil
			}
			// Validate all associated backup policies
			logger.Info("Validating backup policies associated with backup vault", "backupVaultID", params.BackupVaultId)
			if policyValidationErr := _validateBackupPoliciesForBackupVault(ctx, backupVault, retentionParams, h.Orchestrator); policyValidationErr != nil {
				logger.Error("Backup policy validation failed", "error", policyValidationErr)
				return &gcpgenserver.V1betaUpdateBackupVaultBadRequest{
					Code:    400,
					Message: policyValidationErr.Error(),
				}, nil
			}
		}
	}

	param := &commonparams.BackupVaultParams{
		BackupVaultID:         params.BackupVaultId,
		Description:           &description,
		OwnerID:               params.ProjectNumber,
		Region:                params.LocationId,
		BackupRetentionPolicy: *retentionParams,
		Name:                  backupVault.Name,
		AccountName:           params.ProjectNumber,
	}
	updated, operationID, err := h.Orchestrator.UpdateBackupVault(ctx, param)
	if err != nil {
		if errors.IsUserInputValidationErr(err) {
			logger.Error("Failed to update backupVault", err.Error())
			return &gcpgenserver.V1betaUpdateBackupVaultBadRequest{
				Code:    400,
				Message: err.Error(),
			}, nil
		}
		logger.Error("Failed to update backupVault", err.Error())
		return &gcpgenserver.V1betaUpdateBackupVaultInternalServerError{
			Code:    500,
			Message: err.Error(),
		}, nil
	}
	resp := convertCoreModelsToBackupVaultV1beta(updated)
	bvJSON, err := encodeBackupVaultConfigV1(resp)
	if err != nil {
		logger.Error("Failed to marshal backup vault", err.Error())
		return &gcpgenserver.V1betaUpdateBackupVaultInternalServerError{
			Code:    500,
			Message: "Failed to marshal Backup vault",
		}, nil
	}
	if operationID != "" {
		return &gcpgenserver.OperationV1beta{
			Name:     gcpgenserver.NewOptString(fmt.Sprintf("/v1beta/projects/%s/locations/%s/operations/%s", params.ProjectNumber, params.LocationId, operationID)),
			Response: bvJSON,
			Done:     gcpgenserver.NewOptBool(false),
		}, nil
	}
	return &gcpgenserver.OperationV1beta{}, nil
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
		if bv.BackupRetentionPolicy.BackupMinimumEnforcedRetentionDays != nil {
			backupRetentionPolicy.BackupMinimumEnforcedRetentionDays = gcpgenserver.NewOptInt(int(*bv.BackupRetentionPolicy.BackupMinimumEnforcedRetentionDays))
		}
		if bv.BackupRetentionPolicy.DailyBackupImmutable {
			backupRetentionPolicy.DailyBackupImmutable = gcpgenserver.NewOptBool(true)
		}
		if bv.BackupRetentionPolicy.ManualBackupImmutable {
			backupRetentionPolicy.ManualBackupImmutable = gcpgenserver.NewOptBool(true)
		}
		if bv.BackupRetentionPolicy.MonthlyBackupImmutable {
			backupRetentionPolicy.MonthlyBackupImmutable = gcpgenserver.NewOptBool(true)
		}
		if bv.BackupRetentionPolicy.WeeklyBackupImmutable {
			backupRetentionPolicy.WeeklyBackupImmutable = gcpgenserver.NewOptBool(true)
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

func convertCoreToCvpBackupVault(coreBV *coremodels.BackupVaultV1beta) *models.BackupVaultV1beta {
	var backupRetentionPolicy *models.BackupRetentionPolicyV1beta
	if coreBV.BackupRetentionPolicy.BackupMinimumEnforcedRetentionDuration != nil ||
		coreBV.BackupRetentionPolicy.IsDailyBackupImmutable ||
		coreBV.BackupRetentionPolicy.IsWeeklyBackupImmutable ||
		coreBV.BackupRetentionPolicy.IsMonthlyBackupImmutable ||
		coreBV.BackupRetentionPolicy.IsAdhocBackupImmutable {
		backupRetentionPolicy = &models.BackupRetentionPolicyV1beta{
			BackupMinimumEnforcedRetentionDays: coreBV.BackupRetentionPolicy.BackupMinimumEnforcedRetentionDuration,
			DailyBackupImmutable:               coreBV.BackupRetentionPolicy.IsDailyBackupImmutable,
			WeeklyBackupImmutable:              coreBV.BackupRetentionPolicy.IsWeeklyBackupImmutable,
			MonthlyBackupImmutable:             coreBV.BackupRetentionPolicy.IsMonthlyBackupImmutable,
			ManualBackupImmutable:              coreBV.BackupRetentionPolicy.IsAdhocBackupImmutable,
		}
	}

	// Create the CVP backup vault model
	cvpBV := &models.BackupVaultV1beta{
		BackupVaultID:          coreBV.BackupVaultID,
		ResourceID:             &coreBV.Name,
		Description:            coreBV.Description,
		BackupRegion:           coreBV.BackupRegion,
		SourceRegion:           coreBV.SourceRegion,
		DestinationBackupVault: coreBV.DestinationBackupVault,
		SourceBackupVault:      coreBV.SourceBackupVault,
		BackupVaultType:        coreBV.BackupVaultType,
		State:                  coreBV.LifeCycleState,
		StateDetails:           coreBV.LifeCycleStateDetails,
		CreatedAt:              strfmt.DateTime(coreBV.CreatedAt),
		DeletedAt:              (*strfmt.DateTime)(coreBV.DeletedAt),
		BackupRetentionPolicy:  backupRetentionPolicy,
	}
	return cvpBV
}

func convertCoreModelsToBackupVaultV1beta(beta *coremodels.BackupVaultV1beta) *gcpgenserver.BackupVaultV1beta {
	var description, sourceBackupVault, destinationBackupVault, sourceRegion, backupRegion, backupVaultType string
	var backupMinimumEnforcedRetentionDuration int
	if beta.Description != nil {
		description = *beta.Description
	}
	if beta.BackupVaultType != nil {
		backupVaultType = *beta.BackupVaultType
	}
	if beta.SourceBackupVault != nil {
		sourceBackupVault = *beta.SourceBackupVault
	}
	if beta.DestinationBackupVault != nil {
		destinationBackupVault = *beta.DestinationBackupVault
	}
	if beta.SourceRegion != nil {
		sourceRegion = *beta.SourceRegion
	}
	if beta.BackupRegion != nil {
		backupRegion = *beta.BackupRegion
	}
	if beta.BackupRetentionPolicy.BackupMinimumEnforcedRetentionDuration != nil {
		backupMinimumEnforcedRetentionDuration = int(*beta.BackupRetentionPolicy.BackupMinimumEnforcedRetentionDuration)
	}
	return &gcpgenserver.BackupVaultV1beta{
		BackupVaultId:          gcpgenserver.NewOptString(beta.BackupVaultID),
		State:                  gcpgenserver.NewOptBackupVaultV1betaState(gcpgenserver.BackupVaultV1betaState(beta.LifeCycleState)),
		StateDetails:           gcpgenserver.NewOptString(beta.LifeCycleStateDetails),
		CreatedAt:              gcpgenserver.NewOptDateTime(time.Time(beta.CreatedAt)),
		Description:            gcpgenserver.NewOptString(description),
		ResourceId:             beta.Name,
		DestinationBackupVault: gcpgenserver.NewOptString(destinationBackupVault),
		SourceBackupVault:      gcpgenserver.NewOptString(sourceBackupVault),
		SourceRegion:           gcpgenserver.NewOptString(sourceRegion),
		BackupRegion:           gcpgenserver.NewOptString(backupRegion),
		BackupVaultType:        gcpgenserver.NewOptBackupVaultV1betaBackupVaultType(gcpgenserver.BackupVaultV1betaBackupVaultType(backupVaultType)),
		BackupRetentionPolicy: gcpgenserver.NewOptBackupRetentionPolicyV1beta(gcpgenserver.BackupRetentionPolicyV1beta{
			BackupMinimumEnforcedRetentionDays: gcpgenserver.NewOptInt(backupMinimumEnforcedRetentionDuration),
			DailyBackupImmutable:               gcpgenserver.NewOptBool(beta.BackupRetentionPolicy.IsDailyBackupImmutable),
			ManualBackupImmutable:              gcpgenserver.NewOptBool(beta.BackupRetentionPolicy.IsAdhocBackupImmutable),
			MonthlyBackupImmutable:             gcpgenserver.NewOptBool(beta.BackupRetentionPolicy.IsMonthlyBackupImmutable),
			WeeklyBackupImmutable:              gcpgenserver.NewOptBool(beta.BackupRetentionPolicy.IsWeeklyBackupImmutable),
		}),
	}
}

func encodeBackupVaultConfigV1(BackupVault *gcpgenserver.BackupVaultV1beta) (jx.Raw, error) {
	data, err := json.Marshal(BackupVault)
	if err != nil {
		return nil, err
	}
	return data, nil
}
