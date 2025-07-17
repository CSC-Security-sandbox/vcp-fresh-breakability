package api

import (
	"context"
	"encoding/json"
	goErrors "errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/go-faster/jx"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/kms_configurations"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	coremodel "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/helper"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	roleName                  = "cmekNetAppVolumesRole"
	parseKmsConfigResponse    = _parseKmsConfigResponse
	encodeEncryptVolumeV1beta = _encodeEncryptVolumeV1beta
)

const (
	uriFormat    = "^projects\\/[^\\/]+\\/locations\\/[^\\/]+\\/keyRings\\/[^\\/]+\\/cryptoKeys.+$"
	regionGlobal = "global"
)

func (h Handler) V1betaCheckKmsConfig(ctx context.Context, params gcpgenserver.V1betaCheckKmsConfigParams) (gcpgenserver.V1betaCheckKmsConfigRes, error) {
	_, _, parsingErr := parseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaCheckKmsConfigBadRequest{
			Code:    parsingErr.Code,
			Message: parsingErr.Message,
		}, nil
	}

	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	jwtToken := utils.GetJWTTokenFromContext(ctx)
	cvpClient := createClient(logger, jwtToken)

	getKmsConfigParams := &common.GetKmsConfigParams{
		AccountName:   params.ProjectNumber,
		UUID:          params.KmsConfigId,
		LocationID:    params.LocationId,
		ProjectNumber: params.ProjectNumber,
	}
	// Get the KMS configuration from the vsa DB if not found then try getting this from the SDE
	kmsConfigUUID := params.KmsConfigId
	kmsConfig, err := h.Orchestrator.GetKmsConfig(ctx, getKmsConfigParams)
	if err != nil {
		var notFoundErr *errors.NotFoundErr
		if !goErrors.As(err, &notFoundErr) {
			return &gcpgenserver.V1betaCheckKmsConfigInternalServerError{
				Code:    http.StatusInternalServerError,
				Message: err.Error(),
			}, nil
		}
	} else if kmsConfig != nil {
		// If the KMS configuration is found in the vsa DB, use SDE UUID
		kmsConfigUUID = kmsConfig.KmsAttributes.SdeKmsConfigUUID
	}

	checkKmsConfigParams := &kms_configurations.V1betaCheckKmsConfigParams{
		KmsConfigID:    kmsConfigUUID,
		LocationID:     params.LocationId,
		ProjectNumber:  params.ProjectNumber,
		XCorrelationID: &params.XCorrelationID.Value,
	}
	res, err := cvpClient.KmsConfigurations.V1betaCheckKmsConfig(checkKmsConfigParams)
	if err != nil {
		return categorizeCvpClientErrorsForCheckKmsConfigs(err)
	}
	if res == nil || res.Payload == nil {
		return &gcpgenserver.V1betaCheckKmsConfigInternalServerError{
			Code:    http.StatusInternalServerError,
			Message: "unknown error during the check kms config",
		}, nil
	}

	checkKmsConfigResponse := convertToKmsConfigCheckV1beta(res)
	if kmsConfig != nil {
		checkParams := &coremodel.KmsConfigCheck{
			KmsConfig:   kmsConfig,
			Email:       kmsConfig.ServiceAccount.ServiceAccountEmail,
			IsHealthy:   checkKmsConfigResponse.KmsConfigHealthCheck.Value.IsHealthy,
			HealthError: checkKmsConfigResponse.KmsConfigHealthCheck.Value.HealthError.Value,
			ProxyType:   coremodel.ProxyTypeCvp,
		}

		// Access the KMS crypto key to ensure it is accessible using impersonation
		err = h.Orchestrator.AccessCryptoKeyWithImpersonation(ctx, kmsConfig)
		if err != nil {
			return &gcpgenserver.V1betaCheckKmsConfigInternalServerError{
				Code:    http.StatusInternalServerError,
				Message: err.Error(),
			}, nil
		}

		// Update the KMS config health in the vsa DB
		_, err = h.Orchestrator.CheckAndUpdateKmsConfigHealth(ctx, checkParams)
		if err != nil {
			return &gcpgenserver.V1betaCheckKmsConfigInternalServerError{
				Code:    http.StatusInternalServerError,
				Message: "Failed to update KMS config health",
			}, nil
		}
	}
	return checkKmsConfigResponse, nil
}

func categorizeCvpClientErrorsForCheckKmsConfigs(cvpErr error) (gcpgenserver.V1betaCheckKmsConfigRes, error) {
	getMsg := func(msg *string) string {
		return nillable.GetString(msg, "")
	}
	getCode := func(floatVal *float64) float64 {
		return nillable.GetFloat64(floatVal, 0)
	}
	switch e := cvpErr.(type) {
	case *kms_configurations.V1betaCheckKmsConfigConflict:
		return &gcpgenserver.V1betaCheckKmsConfigConflict{
			Code:    getCode(&e.Payload.Code),
			Message: getMsg(&e.Payload.Message),
		}, nil
	case *kms_configurations.V1betaCheckKmsConfigBadRequest:
		return &gcpgenserver.V1betaCheckKmsConfigBadRequest{
			Code:    getCode(&e.Payload.Code),
			Message: getMsg(&e.Payload.Message),
		}, nil
	case *kms_configurations.V1betaCheckKmsConfigUnprocessableEntity:
		return &gcpgenserver.V1betaCheckKmsConfigUnprocessableEntity{
			Code:    getCode(&e.Payload.Code),
			Message: getMsg(&e.Payload.Message),
		}, nil
	case *kms_configurations.V1betaCheckKmsConfigUnauthorized:
		return &gcpgenserver.V1betaCheckKmsConfigUnauthorized{
			Code:    getCode(&e.Payload.Code),
			Message: getMsg(&e.Payload.Message),
		}, nil
	case *kms_configurations.V1betaCheckKmsConfigForbidden:
		return &gcpgenserver.V1betaCheckKmsConfigForbidden{
			Code:    getCode(&e.Payload.Code),
			Message: getMsg(&e.Payload.Message),
		}, nil
	case *kms_configurations.V1betaCheckKmsConfigNotFound:
		return &gcpgenserver.V1betaCheckKmsConfigNotFound{
			Code:    getCode(&e.Payload.Code),
			Message: getMsg(&e.Payload.Message),
		}, nil
	case *kms_configurations.V1betaCheckKmsConfigTooManyRequests:
		return &gcpgenserver.V1betaCheckKmsConfigTooManyRequests{
			Code:    getCode(&e.Payload.Code),
			Message: getMsg(&e.Payload.Message),
		}, nil
	case *kms_configurations.V1betaCheckKmsConfigDefault:
		return &gcpgenserver.V1betaCheckKmsConfigInternalServerError{
			Code:    getCode(&e.Payload.Code),
			Message: getMsg(&e.Payload.Message),
		}, nil
	default:
		return &gcpgenserver.V1betaCheckKmsConfigInternalServerError{
			Code:    http.StatusInternalServerError,
			Message: "unknown error during the check kms config",
		}, nil
	}
}

func categorizeCvpClientErrorsForCreateKmsConfigs(cvpErr error) (gcpgenserver.V1betaCreateKmsConfigurationRes, error) {
	getMsg := func(msg *string) string {
		return nillable.GetString(msg, "")
	}
	getCode := func(floatVal *float64) float64 {
		return nillable.GetFloat64(floatVal, 0)
	}
	switch e := cvpErr.(type) {
	case *kms_configurations.V1betaCreateKmsConfigurationUnprocessableEntity:
		return &gcpgenserver.V1betaCreateKmsConfigurationUnprocessableEntity{
			Code:    getCode(&e.Payload.Code),
			Message: getMsg(&e.Payload.Message),
		}, nil
	case *kms_configurations.V1betaCreateKmsConfigurationConflict:
		return &gcpgenserver.V1betaCreateKmsConfigurationConflict{
			Code:    getCode(&e.Payload.Code),
			Message: getMsg(&e.Payload.Message),
		}, nil
	case *kms_configurations.V1betaCreateKmsConfigurationBadRequest:
		return &gcpgenserver.V1betaCreateKmsConfigurationBadRequest{
			Code:    getCode(&e.Payload.Code),
			Message: getMsg(&e.Payload.Message),
		}, nil
	case *kms_configurations.V1betaCreateKmsConfigurationUnauthorized:
		return &gcpgenserver.V1betaCreateKmsConfigurationUnauthorized{
			Code:    getCode(&e.Payload.Code),
			Message: getMsg(&e.Payload.Message),
		}, nil
	case *kms_configurations.V1betaCreateKmsConfigurationForbidden:
		return &gcpgenserver.V1betaCreateKmsConfigurationForbidden{
			Code:    getCode(&e.Payload.Code),
			Message: getMsg(&e.Payload.Message),
		}, nil
	case *kms_configurations.V1betaCreateKmsConfigurationTooManyRequests:
		return &gcpgenserver.V1betaCreateKmsConfigurationTooManyRequests{
			Code:    getCode(&e.Payload.Code),
			Message: getMsg(&e.Payload.Message),
		}, nil
	case *kms_configurations.V1betaCreateKmsConfigurationDefault:
		return &gcpgenserver.V1betaCreateKmsConfigurationInternalServerError{
			Code:    getCode(&e.Payload.Code),
			Message: getMsg(&e.Payload.Message),
		}, nil
	default:
		return &gcpgenserver.V1betaCreateKmsConfigurationInternalServerError{
			Code:    http.StatusInternalServerError,
			Message: "unknown error during the create kms config",
		}, nil
	}
}

func (h Handler) V1betaCreateKmsConfiguration(ctx context.Context, req *gcpgenserver.KmsConfigV1beta, params gcpgenserver.V1betaCreateKmsConfigurationParams) (gcpgenserver.V1betaCreateKmsConfigurationRes, error) {
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	region, _, parsingErr := parseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaCreateKmsConfigurationBadRequest{
			Code:    parsingErr.Code,
			Message: parsingErr.Message,
		}, nil
	}
	if region == regionGlobal {
		return &gcpgenserver.V1betaCreateKmsConfigurationBadRequest{
			Code:    400,
			Message: "KMS configuration not supported for global region",
		}, nil
	}
	_, err := utils.ParseKeyFullPathResource(req.KeyFullPath)
	if err != nil {
		return &gcpgenserver.V1betaCreateKmsConfigurationBadRequest{
			Code:    400,
			Message: "Invalid KeyFullPath format",
		}, nil
	}

	getKmsConfigParams := &common.GetKmsConfigParams{
		KeyFullPath: req.KeyFullPath,
	}
	kmsConfig, err := h.Orchestrator.GetKmsConfigByKeyFullPath(ctx, getKmsConfigParams)
	if err != nil {
		var notFoundErr *errors.NotFoundErr
		switch {
		case goErrors.As(err, &notFoundErr):
			logger := util.GetLogger(ctx)
			jwtToken := utils.GetJWTTokenFromContext(ctx)
			cvpClient := createClient(logger, jwtToken)

			var body = &models.KmsConfigV1beta{
				ResourceID:  &req.ResourceId.Value,
				KeyFullPath: &req.KeyFullPath,
				Description: &req.Description.Value,
			}

			cvpCreateKmsConfigParams := &kms_configurations.V1betaCreateKmsConfigurationParams{
				LocationID:     params.LocationId,
				ProjectNumber:  params.ProjectNumber,
				XCorrelationID: &params.XCorrelationID.Value,
				Body:           body,
			}

			cvpResponse, err := cvpClient.KmsConfigurations.V1betaCreateKmsConfiguration(cvpCreateKmsConfigParams)
			if err != nil {
				return categorizeCvpClientErrorsForCreateKmsConfigs(err)
			}

			parsedCvpResponse, err := parseKmsConfigResponse(cvpResponse.Payload.Response)
			if err != nil {
				return &gcpgenserver.V1betaCreateKmsConfigurationInternalServerError{
					Code:    http.StatusInternalServerError,
					Message: "Failed to parse KMS configuration response",
				}, nil
			}

			createKmsConfigParams := &common.CreateKmsConfigParams{
				KeyFullPath:    req.KeyFullPath,
				ResourceID:     req.ResourceId.Value,
				AccountName:    params.ProjectNumber,
				LocationID:     params.LocationId,
				ProjectNumber:  params.ProjectNumber,
				OperationUri:   cvpResponse.Payload.Name,
				OperationDone:  *cvpResponse.Payload.Done,
				UUID:           parsedCvpResponse.UUID, // UUID of the SDE kms config
				XCorrelationID: params.XCorrelationID.Value,
				Description:    req.Description.Value,
			}

			// create kms config in vsa DB and start the workflow to poll the SDE operation
			kmsConfig, operationID, err := h.Orchestrator.CreateKmsConfig(ctx, createKmsConfigParams)
			if err != nil {
				var conflictErr *errors.ConflictErr
				switch {
				case goErrors.As(err, &conflictErr):
					return &gcpgenserver.V1betaCreateKmsConfigurationConflict{
						Message: conflictErr.Error(),
						Code:    http.StatusConflict,
					}, nil
				default:
					// Handle any other error types
					return &gcpgenserver.V1betaCreateKmsConfigurationInternalServerError{
						Message: "An unexpected error occurred",
						Code:    http.StatusInternalServerError,
					}, nil
				}
			}
			resp, err := encodeKmsConfigV1(convertModelToKmsConfigV1Beta(kmsConfig))
			if err != nil {
				return nil, err
			}
			if operationID != "" {
				return &gcpgenserver.OperationV1beta{
					Name:     gcpgenserver.NewOptString(fmt.Sprintf("/v1beta/projects/%s/locations/%s/operations/%s", params.ProjectNumber, params.LocationId, operationID)),
					Response: resp,
					Done:     gcpgenserver.NewOptBool(false),
				}, nil
			}
			return &gcpgenserver.V1betaCreateKmsConfigurationInternalServerError{}, nil
		default:
			return &gcpgenserver.V1betaCreateKmsConfigurationInternalServerError{
				Code:    500,
				Message: err.Error(),
			}, nil
		}
	}
	done := true
	switch kmsConfig.State {
	case coremodel.LifeCycleStateError:
		return &gcpgenserver.V1betaCreateKmsConfigurationConflict{
			Message: "Kms config is in error state, please delete the config and try again",
			Code:    http.StatusConflict,
		}, nil
	case coremodel.LifeCycleStateCreating:
		done = false
	}

	resp, err := encodeKmsConfigV1(convertModelToKmsConfigV1Beta(kmsConfig))
	if err != nil {
		return nil, err
	}
	return &gcpgenserver.OperationV1beta{
		Response: resp,
		Done:     gcpgenserver.NewOptBool(done),
	}, nil
}

func (h Handler) V1betaDescribeKmsConfiguration(ctx context.Context, params gcpgenserver.V1betaDescribeKmsConfigurationParams) (gcpgenserver.V1betaDescribeKmsConfigurationRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	jwtToken := utils.GetJWTTokenFromContext(ctx)
	cvpClient := createClient(logger, jwtToken)
	_, _, parsingErr := parseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaDescribeKmsConfigurationBadRequest{
			Code:    parsingErr.Code,
			Message: parsingErr.Message,
		}, nil
	}
	getKmsConfigParams := &common.GetKmsConfigParams{
		AccountName:   params.ProjectNumber,
		UUID:          params.KmsConfigId,
		LocationID:    params.LocationId,
		ProjectNumber: params.ProjectNumber,
	}
	// Get the KMS configuration from the vsa DB if not found then try getting this from the SDE
	kmsConfig, err := h.Orchestrator.GetKmsConfig(ctx, getKmsConfigParams)
	if err != nil {
		var notFoundErr *errors.NotFoundErr
		switch {
		case goErrors.As(err, &notFoundErr):
			describeKmsConfigParams := &kms_configurations.V1betaDescribeKmsConfigurationParams{
				KmsConfigID:    params.KmsConfigId,
				LocationID:     params.LocationId,
				ProjectNumber:  params.ProjectNumber,
				XCorrelationID: &params.XCorrelationID.Value,
			}
			res, err := cvpClient.KmsConfigurations.V1betaDescribeKmsConfiguration(describeKmsConfigParams)
			if err != nil {
				getMsg := func(msg *string) string {
					return nillable.GetString(msg, "")
				}
				getCode := func(code *float64) float64 {
					return nillable.GetFloat64(code, 0)
				}
				switch e := err.(type) {
				case *kms_configurations.V1betaDescribeKmsConfigurationNotFound:
					msg := getMsg(&e.Payload.Message)
					code := getCode(&e.Payload.Code)
					return &gcpgenserver.V1betaDescribeKmsConfigurationNotFound{
						Code:    code,
						Message: msg,
					}, nil
				case *kms_configurations.V1betaDescribeKmsConfigurationUnprocessableEntity:
					msg := getMsg(&e.Payload.Message)
					code := getCode(&e.Payload.Code)
					return &gcpgenserver.V1betaDescribeKmsConfigurationUnprocessableEntity{
						Code:    code,
						Message: msg,
					}, nil
				case *kms_configurations.V1betaDescribeKmsConfigurationConflict:
					msg := getMsg(&e.Payload.Message)
					code := getCode(&e.Payload.Code)
					return &gcpgenserver.V1betaDescribeKmsConfigurationConflict{
						Code:    code,
						Message: msg,
					}, nil
				case *kms_configurations.V1betaDescribeKmsConfigurationBadRequest:
					msg := getMsg(&e.Payload.Message)
					code := getCode(&e.Payload.Code)
					return &gcpgenserver.V1betaDescribeKmsConfigurationBadRequest{
						Code:    code,
						Message: msg,
					}, nil
				case *kms_configurations.V1betaDescribeKmsConfigurationForbidden:
					msg := getMsg(&e.Payload.Message)
					code := getCode(&e.Payload.Code)
					return &gcpgenserver.V1betaDescribeKmsConfigurationForbidden{
						Code:    code,
						Message: msg,
					}, nil
				case *kms_configurations.V1betaDescribeKmsConfigurationUnauthorized:
					msg := getMsg(&e.Payload.Message)
					code := getCode(&e.Payload.Code)
					return &gcpgenserver.V1betaDescribeKmsConfigurationUnauthorized{
						Code:    code,
						Message: msg,
					}, nil
				case *kms_configurations.V1betaDescribeKmsConfigurationTooManyRequests:
					msg := getMsg(&e.Payload.Message)
					code := getCode(&e.Payload.Code)
					return &gcpgenserver.V1betaDescribeKmsConfigurationTooManyRequests{
						Code:    code,
						Message: msg,
					}, nil
				case *kms_configurations.V1betaDescribeKmsConfigurationDefault:
					msg := getMsg(&e.Payload.Message)
					code := getCode(&e.Payload.Code)
					return &gcpgenserver.V1betaDescribeKmsConfigurationInternalServerError{
						Code:    code,
						Message: msg,
					}, nil
				}
			}
			if res == nil || res.Payload == nil {
				return &gcpgenserver.V1betaDescribeKmsConfigurationInternalServerError{
					Code:    http.StatusInternalServerError,
					Message: "unknown error during the describe kms configuration",
				}, nil
			}
			return convertToKmsConfigV1beta(res.Payload), nil
		default:
			// Handle any other error types
			return &gcpgenserver.V1betaDescribeKmsConfigurationInternalServerError{
				Message: "An unexpected error occurred",
				Code:    http.StatusInternalServerError,
			}, nil
		}
	}
	return convertModelToKmsConfigV1Beta(kmsConfig), nil
}

func (h Handler) V1betaListKmsConfigurations(ctx context.Context, params gcpgenserver.V1betaListKmsConfigurationsParams) (gcpgenserver.V1betaListKmsConfigurationsRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	jwtToken := utils.GetJWTTokenFromContext(ctx)
	cvpClient := createClient(logger, jwtToken)
	listKmsConfigParams := &kms_configurations.V1betaListKmsConfigurationsParams{
		LocationID:     params.LocationId,
		ProjectNumber:  params.ProjectNumber,
		XCorrelationID: &params.XCorrelationID.Value,
	}
	res, err := cvpClient.KmsConfigurations.V1betaListKmsConfigurations(listKmsConfigParams)
	if err != nil {
		switch e := err.(type) {
		case *kms_configurations.V1betaListKmsConfigurationsConflict:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaListKmsConfigurationsConflict{
				Code:    code,
				Message: msg,
			}, nil
		case *kms_configurations.V1betaListKmsConfigurationsBadRequest:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaListKmsConfigurationsBadRequest{
				Code:    code,
				Message: msg,
			}, nil
		case *kms_configurations.V1betaListKmsConfigurationsNotFound:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaListKmsConfigurationsNotFound{
				Code:    code,
				Message: msg,
			}, nil
		case *kms_configurations.V1betaListKmsConfigurationsForbidden:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaListKmsConfigurationsForbidden{
				Code:    code,
				Message: msg,
			}, nil
		case *kms_configurations.V1betaListKmsConfigurationsUnauthorized:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaListKmsConfigurationsUnauthorized{
				Code:    code,
				Message: msg,
			}, nil
		case *kms_configurations.V1betaListKmsConfigurationsTooManyRequests:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaListKmsConfigurationsTooManyRequests{
				Code:    code,
				Message: msg,
			}, nil
		case *kms_configurations.V1betaListKmsConfigurationsDefault:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaListKmsConfigurationsInternalServerError{
				Code:    code,
				Message: msg,
			}, nil
		}
	}
	if res == nil || res.Payload == nil {
		return &gcpgenserver.V1betaListKmsConfigurationsInternalServerError{
			Code:    http.StatusInternalServerError,
			Message: "unknown error during the list kms configurations",
		}, nil
	}
	operationResponse := gcpgenserver.V1betaListKmsConfigurationsOK{
		KmsMinusConfigurations: []gcpgenserver.KmsConfigV1beta{},
	}
	for _, kmsConfig := range res.Payload {
		operationResponse.KmsMinusConfigurations = append(operationResponse.KmsMinusConfigurations, *convertToKmsConfigV1beta(kmsConfig))
	}
	return &operationResponse, nil
}

func (h Handler) V1betaUpdateKmsConfiguration(ctx context.Context, req *gcpgenserver.KmsConfigUpdateV1beta, params gcpgenserver.V1betaUpdateKmsConfigurationParams) (gcpgenserver.V1betaUpdateKmsConfigurationRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	region, _, parsingErr := parseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaUpdateKmsConfigurationBadRequest{
			Code:    parsingErr.Code,
			Message: parsingErr.Message,
		}, nil
	}

	param := &common.UpdateKmsConfigParams{
		KmsConfigID:    params.KmsConfigId,
		AccountName:    params.ProjectNumber,
		Name:           req.ResourceId.Value,
		XCorrelationID: params.XCorrelationID.Value,
		Description:    &req.Description.Value,
		Region:         region,
	}

	var URISplits []string
	if !nillable.IsNilOrEmpty(&req.KeyFullPath.Value) {
		// Sample KeyFull Path Value : projects/projectID/locations/us/keyRings/keyRing/cryptoKeys/keyName
		if regex := regexp.MustCompile(uriFormat).MatchString(req.KeyFullPath.Value); !regex {
			return &gcpgenserver.V1betaUpdateKmsConfigurationBadRequest{
				Code:    400,
				Message: "KeyFullPath is not as expected sample : 'projects/projectID/locations/us-east1/keyRings/keyRing/cryptoKeys/keyName'",
			}, nil
		}

		URISplits = strings.Split(req.KeyFullPath.Value, "/")
		param.KeyName = URISplits[7]
		param.KeyRing = URISplits[5]
		param.KeyProjectID = URISplits[1]
		param.KeyRingLocation = URISplits[3]
		param.KeyUri = req.KeyFullPath.Value
	}

	kmsConfig, jobUUID, err := h.Orchestrator.UpdateKmsConfig(ctx, param)
	if err != nil {
		if errors.IsUserInputValidationErr(err) || errors.IsNotFoundErr(err) {
			return &gcpgenserver.V1betaUpdateKmsConfigurationBadRequest{
				Code:    400,
				Message: err.Error(),
			}, nil
		}

		logger.Error("Failed to update kms configuration", err.Error())
		return &gcpgenserver.V1betaUpdateKmsConfigurationInternalServerError{Code: http.StatusInternalServerError, Message: err.Error()}, err
	}

	var resp jx.Raw
	if kmsConfig != nil {
		resp, err = encodeKmsConfigV1(convertVcpKmsConfigToKmsConfigV1beta(kmsConfig))
		if err != nil {
			return nil, err
		}
	}

	operationID := "/v1beta/projects/" + params.ProjectNumber + "/locations/" + params.LocationId + "/operations/" + jobUUID
	if kmsConfig != nil && kmsConfig.State == coremodel.LifeCycleStateUpdating {
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

func (h Handler) V1betaGetMultipleKmsConfigs(ctx context.Context, req *gcpgenserver.KmsConfigIdListV1beta, params gcpgenserver.V1betaGetMultipleKmsConfigsParams) (gcpgenserver.V1betaGetMultipleKmsConfigsRes, error) {
	logger := util.GetLogger(ctx)

	kmsConfigUUIDList := req.KmsConfigIds
	kmsConfigVSAList, vsaErr := h.Orchestrator.GetMultipleKMSConfigs(ctx, kmsConfigUUIDList)

	if vsaErr != nil {
		logger.Error("Get Multiple KMS Configurations API call failed with error", "Error", vsaErr.Error())
		return &gcpgenserver.V1betaGetMultipleKmsConfigsInternalServerError{
			Code:    http.StatusInternalServerError,
			Message: "Unknown error encountered during Get Multiple KMS configurations operation",
		}, nil
	}
	operationResponse := gcpgenserver.V1betaGetMultipleKmsConfigsOK{
		KmsConfigurations: []gcpgenserver.KmsConfigV1beta{},
	}
	for _, kmsConfigVSA := range kmsConfigVSAList {
		operationResponse.KmsConfigurations = append(operationResponse.KmsConfigurations, *convertOrchestratorModelToKmsConfigV1beta(kmsConfigVSA))
	}

	if len(kmsConfigVSAList) != len(kmsConfigUUIDList) {
		kmsConfigUUIDMissingList := missingKmsConfigIdsInVcp(kmsConfigUUIDList, kmsConfigVSAList)

		// Proceed to call CVP client for those UUIDs which are missing
		jwtToken := utils.GetJWTTokenFromContext(ctx)

		body := &models.KmsConfigIDListV1beta{
			KmsConfigIDs: kmsConfigUUIDMissingList,
		}
		getMultipleKmsConfigsParams := &kms_configurations.V1betaGetMultipleKmsConfigsParams{
			LocationID:     params.LocationId,
			ProjectNumber:  params.ProjectNumber,
			XCorrelationID: &params.XCorrelationID.Value,
			Body:           body,
		}

		cvpClient := createClient(logger, jwtToken)
		cvpResponse, cvpErr := cvpClient.KmsConfigurations.V1betaGetMultipleKmsConfigs(getMultipleKmsConfigsParams)
		if cvpErr != nil {
			gcpgenserverResponse := categorizeCvpClientErrorsForGetMultipleKmsConfigs(cvpErr, logger)
			return gcpgenserverResponse, nil
		}
		if cvpResponse == nil {
			return &gcpgenserver.V1betaGetMultipleKmsConfigsInternalServerError{
				Code:    http.StatusInternalServerError,
				Message: "Unknown error encountered during Get Multiple KMS configurations operation",
			}, nil
		}
		if cvpResponse.Payload != nil {
			// Missing UUID KMSConfigs fetched from SDE are not being stored in VCP for now
			for _, kmsConfig := range cvpResponse.Payload.KmsConfigurations {
				operationResponse.KmsConfigurations = append(operationResponse.KmsConfigurations, *convertToKmsConfigV1beta(kmsConfig))
			}
		}
	}
	// Returning empty list for zero matches is acceptable for GetMultiple API
	return &operationResponse, nil
}

func (h Handler) V1betaEncryptVolumes(ctx context.Context, params gcpgenserver.V1betaEncryptVolumesParams) (gcpgenserver.V1betaEncryptVolumesRes, error) {
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	_, _, parsingErr := parseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaEncryptVolumesBadRequest{
			Code:    parsingErr.Code,
			Message: parsingErr.Message,
		}, nil
	}

	getMultipleKmsParams := gcpgenserver.V1betaGetMultipleKmsConfigsParams{
		ProjectNumber:  params.ProjectNumber,
		LocationId:     params.LocationId,
		XCorrelationID: params.XCorrelationID,
	}
	kmsConfigIdArray := []string{params.KmsConfigId}
	getMultipleKmsBody := gcpgenserver.KmsConfigIdListV1beta{
		KmsConfigIds: kmsConfigIdArray,
	}

	getMultipleKmsResponse, err := h.V1betaGetMultipleKmsConfigs(ctx, &getMultipleKmsBody, getMultipleKmsParams)
	if err != nil {
		return nil, err
	}
	if _, ok := getMultipleKmsResponse.(*gcpgenserver.V1betaGetMultipleKmsConfigsOK); !ok {
		return &gcpgenserver.V1betaEncryptVolumesInternalServerError{
			Code:    500,
			Message: fmt.Sprintf("Unknown error encountered during GetMultipleKmsConfigs with CMEK policy UUID %s", params.KmsConfigId),
		}, nil
	}
	if len(getMultipleKmsResponse.(*gcpgenserver.V1betaGetMultipleKmsConfigsOK).KmsConfigurations) == 0 {
		return &gcpgenserver.V1betaEncryptVolumesBadRequest{
			Code:    404,
			Message: fmt.Sprintf("CMEK policy with UUID %s not found", params.KmsConfigId),
		}, nil
	}

	migrateKmsConfigParams := &common.MigrateKmsConfigParams{
		UUID:          params.KmsConfigId,
		LocationID:    params.LocationId,
		ProjectNumber: params.ProjectNumber,
		AccountName:   params.ProjectNumber,
		State:         string(getMultipleKmsResponse.(*gcpgenserver.V1betaGetMultipleKmsConfigsOK).KmsConfigurations[0].KmsState.Value),
	}

	operationID, err := h.Orchestrator.MigrateKmsConfig(ctx, migrateKmsConfigParams)
	if err != nil {
		if errors.IsBadRequestErr(err) {
			return &gcpgenserver.V1betaEncryptVolumesBadRequest{
				Code:    400,
				Message: err.Error(),
			}, nil
		}
		return &gcpgenserver.V1betaEncryptVolumesInternalServerError{
			Code:    500,
			Message: err.Error(),
		}, nil
	}
	if operationID == "" {
		return &gcpgenserver.V1betaEncryptVolumesInternalServerError{
			Code:    500,
			Message: "Job ID not returned by VCP for CMEK policy migration",
		}, nil
	}

	operationV1Beta, err := convertEncryptVolumesToOperationV1Beta(params, operationID)
	if err != nil {
		return nil, err
	}
	return operationV1Beta, nil
}

func convertEncryptVolumesToOperationV1Beta(params gcpgenserver.V1betaEncryptVolumesParams, operationID string) (*gcpgenserver.OperationV1beta, error) {
	encryptStatus := models.EncryptVolumeStatusV1beta{
		UUID:   params.KmsConfigId,
		Status: coremodel.LifeCycleStateUpdating,
	}
	encryptVolume := models.EncryptVolumeV1beta{
		EncryptionStatus: &encryptStatus,
	}

	jsonRaw, err := encodeEncryptVolumeV1beta(&encryptVolume)
	if err != nil {
		return nil, err
	}

	operationResponse := gcpgenserver.OperationV1beta{
		Name:     gcpgenserver.NewOptString(fmt.Sprintf("/v1beta/projects/%s/locations/%s/operations/%s", params.ProjectNumber, params.LocationId, operationID)),
		Response: jsonRaw,
		Done:     gcpgenserver.NewOptBool(false),
	}
	return &operationResponse, nil
}

func (h Handler) V1betaDeleteKmsConfiguration(ctx context.Context, params gcpgenserver.V1betaDeleteKmsConfigurationParams) (gcpgenserver.V1betaDeleteKmsConfigurationRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	region, _, parsingErr := parseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaDeleteKmsConfigurationBadRequest{
			Code:    parsingErr.Code,
			Message: parsingErr.Message,
		}, nil
	}

	param := &common.DeleteKmsConfigParams{
		AccountName:    params.ProjectNumber,
		Region:         region,
		XCorrelationID: params.XCorrelationID.Value,
		KmsConfigID:    params.KmsConfigId,
	}

	kmsConfig, jobUUID, err := h.Orchestrator.DeleteKmsConfig(ctx, param)
	if err != nil {
		if errors.IsUserInputValidationErr(err) {
			return &gcpgenserver.V1betaDeleteKmsConfigurationBadRequest{
				Code:    http.StatusBadRequest,
				Message: err.Error(),
			}, nil
		} else if errors.IsNotFoundErr(err) {
			return &gcpgenserver.V1betaDeleteKmsConfigurationNotFound{
				Code:    http.StatusNotFound,
				Message: err.Error(),
			}, nil
		}

		logger.Error("Failed to delete kms configuration", err.Error())
		return &gcpgenserver.V1betaDeleteKmsConfigurationInternalServerError{Code: 500, Message: err.Error()}, nil
	}

	var resp jx.Raw
	if kmsConfig != nil {
		resp, err = encodeKmsConfigV1(convertVcpKmsConfigToKmsConfigV1beta(kmsConfig))
		if err != nil {
			return nil, err
		}
	}

	operationID := "/v1beta/projects/" + params.ProjectNumber + "/locations/" + params.LocationId + "/operations/" + jobUUID
	if kmsConfig != nil && kmsConfig.State == coremodel.LifeCycleStateUpdating {
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

func convertToKmsConfigCheckV1beta(res *kms_configurations.V1betaCheckKmsConfigOK) *gcpgenserver.KmsConfigCheckV1beta {
	kmsConfigHealthCheckV1beta := gcpgenserver.KmsConfigHealthCheckV1beta{
		IsHealthy:    *res.Payload.KmsConfigHealthCheck.IsHealthy,
		HealthError:  gcpgenserver.NewOptString(res.Payload.KmsConfigHealthCheck.HealthError),
		Instructions: gcpgenserver.NewOptString(res.Payload.KmsConfigHealthCheck.Instructions),
	}
	return &gcpgenserver.KmsConfigCheckV1beta{
		ServiceAccount:       gcpgenserver.NewOptString(res.Payload.ServiceAccount),
		KmsConfigHealthCheck: gcpgenserver.NewOptKmsConfigHealthCheckV1beta(kmsConfigHealthCheckV1beta),
	}
}

func convertToOperationV1beta(res *models.OperationV1beta) *gcpgenserver.OperationV1beta {
	return &gcpgenserver.OperationV1beta{
		Name: gcpgenserver.NewOptString(res.Name),
		Done: gcpgenserver.NewOptBool(*res.Done),
	}
}

func convertToKmsConfigV1beta(res *models.KmsConfigV1beta) *gcpgenserver.KmsConfigV1beta {
	state := gcpgenserver.KmsConfigV1betaKmsState(res.KmsState)
	kmsConfigV1beta := &gcpgenserver.KmsConfigV1beta{
		UUID:            gcpgenserver.NewOptString(res.UUID),
		KmsState:        gcpgenserver.NewOptKmsConfigV1betaKmsState(state),
		KmsStateDetails: gcpgenserver.NewOptString(res.KmsStateDetails),
		Description:     gcpgenserver.NewOptString(*res.Description),
		CreatedTime:     gcpgenserver.NewOptDateTime(time.Time(res.CreatedTime)),
	}
	if res.KeyFullPath != nil {
		kmsConfigV1beta.KeyFullPath = *res.KeyFullPath
	}
	if res.DeletedTime != nil {
		kmsConfigV1beta.DeletedTime = gcpgenserver.NewOptDateTime(time.Time(*res.DeletedTime))
	}
	if res.Instructions != "" {
		kmsConfigV1beta.Instructions = gcpgenserver.NewOptString(res.Instructions)
	}
	if res.ServiceAccountEmail != "" {
		kmsConfigV1beta.ServiceAccountEmail = gcpgenserver.NewOptString(res.ServiceAccountEmail)
	}
	if res.UpdatedTime != nil {
		kmsConfigV1beta.UpdatedTime = gcpgenserver.NewOptDateTime(time.Time(*res.UpdatedTime))
	}
	if res.ResourceID != nil {
		kmsConfigV1beta.ResourceId = gcpgenserver.NewOptString(*res.ResourceID)
	}
	return kmsConfigV1beta
}

func convertOrchestratorModelToKmsConfigV1beta(kmsConfig *coremodel.KmsConfig) *gcpgenserver.KmsConfigV1beta {
	state := gcpgenserver.KmsConfigV1betaKmsState(kmsConfig.State)
	keyFullPath := fmt.Sprintf("projects/%s/locations/%s/keyRings/%s/cryptoKeys/%s",
		kmsConfig.KeyProjectID, kmsConfig.KeyRingLocation, kmsConfig.KeyRing, kmsConfig.KeyName)
	instructions := getKmsInstructions(kmsConfig)

	res := &gcpgenserver.KmsConfigV1beta{
		UUID:            gcpgenserver.NewOptString(kmsConfig.UUID),
		KeyFullPath:     keyFullPath,
		KmsState:        gcpgenserver.NewOptKmsConfigV1betaKmsState(state),
		KmsStateDetails: gcpgenserver.NewOptString(kmsConfig.StateDetails),
		Description:     gcpgenserver.NewOptString(kmsConfig.Description),
		CreatedTime:     gcpgenserver.NewOptDateTime(kmsConfig.CreatedAt),
		UpdatedTime:     gcpgenserver.NewOptDateTime(kmsConfig.UpdatedAt),
		Instructions:    gcpgenserver.NewOptString(instructions),
		ResourceId:      gcpgenserver.NewOptString(kmsConfig.ResourceID),
	}
	if kmsConfig.DeletedAt != nil {
		res.DeletedTime = gcpgenserver.OptDateTime{Value: *kmsConfig.DeletedAt}
	}
	if kmsConfig.KmsAttributes != nil {
		res.ServiceAccountEmail = gcpgenserver.NewOptString(kmsConfig.KmsAttributes.SdeServiceAccountEmail)
	}
	return res
}

func convertVcpKmsConfigToKmsConfigV1beta(res *coremodel.KmsConfig) *gcpgenserver.KmsConfigV1beta {
	state := gcpgenserver.KmsConfigV1betaKmsState(res.State)
	kmsConfigV1beta := &gcpgenserver.KmsConfigV1beta{
		UUID:            gcpgenserver.NewOptString(res.UUID),
		KmsState:        gcpgenserver.NewOptKmsConfigV1betaKmsState(state),
		KmsStateDetails: gcpgenserver.NewOptString(res.StateDetails),
		Description:     gcpgenserver.NewOptString(nillable.GetString(&res.Description, "")),
		CreatedTime:     gcpgenserver.NewOptDateTime(time.Time(res.CreatedAt)),
		UpdatedTime:     gcpgenserver.NewOptDateTime(time.Time(res.UpdatedAt)),
	}

	KeyFullPath := fmt.Sprintf("projects/%s/locations/%s/keyRings/%s/cryptoKeys/%s",
		res.KeyProjectID, res.KeyRingLocation, res.KeyRing, res.KeyName)
	if KeyFullPath == "" {
		kmsConfigV1beta.KeyFullPath = KeyFullPath
	}
	if res.DeletedAt != nil {
		kmsConfigV1beta.DeletedTime = gcpgenserver.NewOptDateTime(time.Time(*res.DeletedAt))
	}
	if res.Name != "" {
		kmsConfigV1beta.ResourceId = gcpgenserver.NewOptString(res.Name)
	}
	return kmsConfigV1beta
}

func getKmsInstructions(kmsConfig *coremodel.KmsConfig) (instructions string) {
	if kmsConfig.KmsAttributes == nil || kmsConfig.KmsAttributes.SdeServiceAccountEmail == "" {
		return ""
	}

	keyProjectID := kmsConfig.KeyProjectID
	if keyProjectID == "" {
		keyProjectID = kmsConfig.CustomerProjectID
	}

	return fmt.Sprintf(`Please copy and paste the commands listed below into Google Cloud Shell in the project that contains the key ring. The commands create a KMS role and assign it to the CVS service account so that it can access the key.
## CREATE KMS role ## gcloud iam roles create %[1]s --project=%[2]s --title='%[1]s' --description='custom cmek cvs role' --permissions=cloudkms.cryptoKeyVersions.get,cloudkms.cryptoKeyVersions.list,cloudkms.cryptoKeyVersions.useToDecrypt,cloudkms.cryptoKeyVersions.useToEncrypt,cloudkms.cryptoKeys.get,cloudkms.keyRings.get,cloudkms.locations.get,cloudkms.locations.list,resourcemanager.projects.get --stage=GA
 ## ASSIGN role and give KEY ACCESS to CVS service account ## gcloud kms keys add-iam-policy-binding %[3]s --project=%[2]s --keyring %[4]s --location %[5]s --member serviceAccount:%[6]s --role projects/%[2]s/roles/%[1]s`, roleName, keyProjectID, kmsConfig.KeyName, kmsConfig.KeyRing, kmsConfig.KeyRingLocation, kmsConfig.KmsAttributes.SdeServiceAccountEmail)
}

func missingKmsConfigIdsInVcp(kmsConfigUUIDList []string, kmsConfigVSAList []*coremodel.KmsConfig) []string {
	var kmsConfigUUIDMissingList []string

	if kmsConfigVSAList != nil {
		// Create map from id of kmsConfigStruct Array
		kmsConfigUUIDMap := make(map[string]string)
		for _, kmsConfig := range kmsConfigVSAList {
			kmsConfigUUIDMap[kmsConfig.UUID] = ""
		}
		// Iterate through UUID List, find missing ones, and append to Missing list
		for _, uuid := range kmsConfigUUIDList {
			if _, exists := kmsConfigUUIDMap[uuid]; !exists {
				kmsConfigUUIDMissingList = append(kmsConfigUUIDMissingList, uuid)
			}
		}
	} else {
		kmsConfigUUIDMissingList = kmsConfigUUIDList
	}
	return kmsConfigUUIDMissingList
}

func categorizeCvpClientErrorsForGetMultipleKmsConfigs(cvpErr error, logger log.Logger) gcpgenserver.V1betaGetMultipleKmsConfigsRes {
	getMsg := func(msg *string) string {
		return nillable.GetString(msg, "")
	}
	getCode := func(floatVal *float64) float64 {
		return nillable.GetFloat64(floatVal, 0)
	}
	switch e := cvpErr.(type) {
	case *kms_configurations.V1betaGetMultipleKmsConfigsBadRequest:
		return &gcpgenserver.V1betaGetMultipleKmsConfigsBadRequest{
			Code:    getCode(&e.Payload.Code),
			Message: getMsg(&e.Payload.Message),
		}
	case *kms_configurations.V1betaGetMultipleKmsConfigsUnauthorized:
		return &gcpgenserver.V1betaGetMultipleKmsConfigsUnauthorized{
			Code:    getCode(&e.Payload.Code),
			Message: getMsg(&e.Payload.Message),
		}
	case *kms_configurations.V1betaGetMultipleKmsConfigsForbidden:
		return &gcpgenserver.V1betaGetMultipleKmsConfigsForbidden{
			Code:    getCode(&e.Payload.Code),
			Message: getMsg(&e.Payload.Message),
		}
	case *kms_configurations.V1betaGetMultipleKmsConfigsNotFound:
		return &gcpgenserver.V1betaGetMultipleKmsConfigsNotFound{
			Code:    getCode(&e.Payload.Code),
			Message: getMsg(&e.Payload.Message),
		}
	case *kms_configurations.V1betaGetMultipleKmsConfigsTooManyRequests:
		return &gcpgenserver.V1betaGetMultipleKmsConfigsTooManyRequests{
			Code:    getCode(&e.Payload.Code),
			Message: getMsg(&e.Payload.Message),
		}
	case *kms_configurations.V1betaGetMultipleKmsConfigsDefault:
		return &gcpgenserver.V1betaGetMultipleKmsConfigsInternalServerError{
			Code:    getCode(&e.Payload.Code),
			Message: getMsg(&e.Payload.Message),
		}
	default:
		return &gcpgenserver.V1betaGetMultipleKmsConfigsInternalServerError{
			Code:    http.StatusInternalServerError,
			Message: "Unknown error encountered during Get Multiple KMS configurations operation",
		}
	}
}

// encodeKmsConfigV1 encodes a KmsConfigV1 struct to JSON.
func encodeKmsConfigV1(kmsConfigV1beta *gcpgenserver.KmsConfigV1beta) (jx.Raw, error) {
	data, err := json.Marshal(kmsConfigV1beta)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func _encodeEncryptVolumeV1beta(encryptVolumeV1beta *models.EncryptVolumeV1beta) (jx.Raw, error) {
	data, err := json.Marshal(encryptVolumeV1beta)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// convertModelToKmsConfigV1Beta converts a vsaModel.KmsConfig to gcpgenserver.KmsConfigV1beta
func convertModelToKmsConfigV1Beta(kmsConfig *coremodel.KmsConfig) *gcpgenserver.KmsConfigV1beta {
	model := &gcpgenserver.KmsConfigV1beta{
		UUID:            gcpgenserver.NewOptString(kmsConfig.UUID),
		KmsState:        gcpgenserver.NewOptKmsConfigV1betaKmsState(gcpgenserver.KmsConfigV1betaKmsState(kmsConfig.State)),
		KmsStateDetails: gcpgenserver.NewOptString(kmsConfig.StateDetails),
		KeyFullPath: utils.ParsedKeyFullPathResource{ProjectID: kmsConfig.KeyProjectID,
			KeyRing: kmsConfig.KeyRing, Location: kmsConfig.KeyRingLocation, CryptoKey: kmsConfig.KeyName}.String(),
		ResourceId:  gcpgenserver.NewOptString(kmsConfig.ResourceID),
		CreatedTime: gcpgenserver.NewOptDateTime(kmsConfig.CreatedAt),
		UpdatedTime: gcpgenserver.NewOptDateTime(kmsConfig.UpdatedAt),
	}
	if kmsConfig.KmsAttributes.SdeServiceAccountEmail != "" {
		model.ServiceAccountEmail = gcpgenserver.NewOptString(kmsConfig.KmsAttributes.SdeServiceAccountEmail)
	}
	if kmsConfig.Description != "" {
		model.Description = gcpgenserver.NewOptString(kmsConfig.Description)
	}
	instruction := getKmsInstructions(kmsConfig)
	if instruction != "" {
		model.Instructions = gcpgenserver.NewOptString(instruction)
	}
	return model
}

func _parseKmsConfigResponse(payloadResponse interface{}) (*models.KmsConfigV1beta, error) {
	var cvpResponse models.KmsConfigV1beta

	// Marshal the response field back to JSON
	responseJSON, err := json.Marshal(payloadResponse)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal payload response: %w", err)
	}

	// Unmarshal the JSON back into the KmsConfigV1beta struct
	err = json.Unmarshal(responseJSON, &cvpResponse)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON to KmsConfigV1beta: %w", err)
	}

	return &cvpResponse, nil
}
