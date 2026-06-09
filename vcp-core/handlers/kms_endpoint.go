package api

import (
	"context"
	"net/http"
	"strings"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/vcp-core/servergen"
)

var (
	kmsRotationEnabled = env.GetBool("GCP_KMS_KEY_ROTATION_ENABLED", false)
)

// V1RotateGcpKmsConfig implements the KMS config service account key rotation endpoint
func (h *Handler) V1RotateGcpKmsConfig(ctx context.Context, req *oasgenserver.GcpKmsKeyRotateV1, params oasgenserver.V1RotateGcpKmsConfigParams) (oasgenserver.V1RotateGcpKmsConfigRes, error) {
	if !kmsRotationEnabled {
		return &oasgenserver.V1RotateGcpKmsConfigForbidden{
			Message: "KMS rotation feature is currently disabled",
			Code:    http.StatusForbidden,
		}, nil
	}

	accountName := req.OwnerID

	// Extract correlation ID from context
	correlationID := utils.GetCoRelationIDFromContext(ctx)

	// Create rotation parameters
	rotateParams := &common.RotateKmsConfigParams{
		KmsConfigID:    params.UUID,
		AccountName:    accountName,
		XCorrelationID: correlationID,
	}

	// Execute the rotation
	kmsConfig, job, err := h.Orchestrator.RotateKmsConfig(ctx, rotateParams)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return &oasgenserver.V1RotateGcpKmsConfigNotFound{
				Message: "KMS config not found",
				Code:    http.StatusNotFound,
			}, nil
		}
		if errors.IsBadRequestErr(err) {
			return &oasgenserver.V1RotateGcpKmsConfigBadRequest{
				Message: err.Error(),
				Code:    http.StatusBadRequest,
			}, nil
		}
		if strings.Contains(err.Error(), "conflict") || errors.IsConflictErr(err) {
			return &oasgenserver.V1RotateGcpKmsConfigConflict{
				Message: err.Error(),
				Code:    http.StatusConflict,
			}, nil
		}
		return &oasgenserver.V1RotateGcpKmsConfigInternalServerError{
			Message: err.Error(),
			Code:    http.StatusInternalServerError,
		}, nil
	}

	// Convert KMS config model to API response
	response := convertKmsConfigToApiResponse(kmsConfig, job)
	return response, nil
}

// convertKmsConfigToApiResponse converts a KMS config model to the API response format
func convertKmsConfigToApiResponse(kmsConfig *models.KmsConfig, job *models.Job) *oasgenserver.GcpKmsConfigV1 {
	response := &oasgenserver.GcpKmsConfigV1{}
	if kmsConfig != nil {
		// Map KMS config fields
		if kmsConfig.UUID != "" {
			newState, stateDetails := common.ConvertKmsConfigStateV1beta(kmsConfig.State, kmsConfig.StateDetails)
			response.UUID = oasgenserver.NewOptString(kmsConfig.UUID)
			response.ResourceId = oasgenserver.NewOptString(kmsConfig.ResourceID)
			response.KeyRing = oasgenserver.NewOptString(kmsConfig.KeyRing)
			response.KeyName = oasgenserver.NewOptString(kmsConfig.KeyName)
			response.KeyProjectID = oasgenserver.NewOptString(kmsConfig.KeyProjectID)
			response.KeyRingLocation = oasgenserver.NewOptString(kmsConfig.KeyRingLocation)
			response.CreatedAt = oasgenserver.NewOptDateTime(kmsConfig.CreatedAt)
			response.UpdatedAt = oasgenserver.NewOptDateTime(kmsConfig.UpdatedAt)
			response.State = oasgenserver.NewOptGcpKmsConfigV1State(oasgenserver.GcpKmsConfigV1State(newState))
			response.StateDetails = oasgenserver.NewOptString(stateDetails)
		}
		if kmsConfig.Description != "" {
			response.Description = oasgenserver.NewOptString(kmsConfig.Description)
		}

		// Map service account email from KMS attributes or service account
		if kmsConfig.KmsAttributes != nil && kmsConfig.KmsAttributes.SdeServiceAccountEmail != "" {
			response.ServiceAccountEmail = oasgenserver.NewOptString(kmsConfig.KmsAttributes.SdeServiceAccountEmail)
		}
	}

	// Add job information if job is provided
	if job != nil {
		jobV1 := oasgenserver.JobV1{
			JobId:   oasgenserver.NewOptString(job.UUID),
			Created: oasgenserver.NewOptDateTime(job.CreatedAt),
		}

		// Map job state
		if job.State != "" {
			switch job.State {
			case datamodel.JobsStateNEW:
				jobV1.State = oasgenserver.NewOptJobV1State(oasgenserver.JobV1StateOngoing)
			case datamodel.JobsStatePROCESSING:
				jobV1.State = oasgenserver.NewOptJobV1State(oasgenserver.JobV1StateOngoing)
			case datamodel.JobsStateDONE:
				jobV1.State = oasgenserver.NewOptJobV1State(oasgenserver.JobV1StateDone)
			case datamodel.JobsStateERROR:
				jobV1.State = oasgenserver.NewOptJobV1State(oasgenserver.JobV1StateError)
			default:
				jobV1.State = oasgenserver.NewOptJobV1State(oasgenserver.JobV1StateOngoing)
			}
		}
		response.Jobs = []oasgenserver.JobV1{jobV1}
	}
	return response
}
