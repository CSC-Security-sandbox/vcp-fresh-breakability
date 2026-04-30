package api

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/google/uuid"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	ociserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/oci-proxy/api/oci-servergen"
	utilserrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/workflowquery"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// validateRequiredOCIDs checks that none of the provided name/value pairs are empty.
// Returns the error message for the first empty value, or "" if all are present.
func validateRequiredOCIDs(fields ...struct{ name, value string }) string {
	for _, f := range fields {
		if f.value == "" {
			return f.name + " is required"
		}
	}
	return ""
}

// svmErrorResponse builds the SvmOperationErrorResponse body shared by all SVM
// error returns. status defaults to workflowquery.WorkflowStatusFailed.
func svmErrorResponse(svmOCID, errorMessage string) ociserver.SvmOperationErrorResponse {
	return ociserver.SvmOperationErrorResponse{
		Status:       string(workflowquery.WorkflowStatusFailed),
		SvmOCID:      svmOCID,
		ErrorMessage: errorMessage,
	}
}

// CreateSvmByPool implements generated createSvmByPool operation.
func (h *Handler) CreateSvmByPool(ctx context.Context, req *ociserver.CreateSvmRequest, params ociserver.CreateSvmByPoolParams) (ociserver.CreateSvmByPoolRes, error) {
	logger := util.GetLogger(ctx)

	opcRequestID, err := opcRequestIDFromContext(ctx)
	if err != nil {
		logger.Error("missing opc-request-id in context", "error", err)
		return &ociserver.CreateSvmByPoolBadRequest{
			OpcRequestID: uuid.NewString(),
			Response:     svmErrorResponse(req.SvmOCID, invalidOPCRequestID),
		}, nil
	}

	name := strings.TrimSpace(req.Name)
	if name == "" {
		return &ociserver.CreateSvmByPoolBadRequest{
			OpcRequestID: opcRequestID,
			Response:     svmErrorResponse(req.SvmOCID, "Name is required"),
		}, nil
	}
	poolOCID := strings.TrimSpace(params.PoolOCID)
	svmOCID := strings.TrimSpace(req.SvmOCID)
	tenancyOCID := strings.TrimSpace(params.TenancyOcid)

	if msg := validateRequiredOCIDs(
		struct{ name, value string }{"poolOCID path parameter", poolOCID},
		struct{ name, value string }{"svmOCID", svmOCID},
		struct{ name, value string }{"Tenancy-Ocid", tenancyOCID},
	); msg != "" {
		return &ociserver.CreateSvmByPoolBadRequest{
			OpcRequestID: opcRequestID,
			Response:     svmErrorResponse(svmOCID, msg),
		}, nil
	}

	svmAdminPasswordVersion, err := strconv.ParseInt(req.SvmAdminPassword.Version, 10, 64)
	if err != nil {
		return &ociserver.CreateSvmByPoolBadRequest{
			OpcRequestID: opcRequestID,
			Response:     svmErrorResponse(svmOCID, "svmAdminPassword.version must be a valid integer"),
		}, nil
	}
	if svmAdminPasswordVersion < 1 {
		return &ociserver.CreateSvmByPoolBadRequest{
			OpcRequestID: opcRequestID,
			Response:     svmErrorResponse(svmOCID, "svmAdminPassword.version must be greater than or equal to 1"),
		}, nil
	}

	createSvmParams := &commonparams.CreateSvmParams{
		AccountName:           tenancyOCID,
		PoolOCID:              poolOCID,
		SvmExternalIdentifier: svmOCID,
		Name:                  name,
		Ips:                   req.Ips,
		SvmAdminPassword: &commonparams.OciAdminPassword{
			Ocid:    req.SvmAdminPassword.Ocid,
			Version: svmAdminPasswordVersion,
		},
	}

	workflowID, err := h.Orchestrator.CreateSvm(ctx, createSvmParams)
	if err != nil {
		if utilserrors.IsUserInputValidationErr(err) || utilserrors.IsBadRequestErr(err) {
			return &ociserver.CreateSvmByPoolBadRequest{
				OpcRequestID: opcRequestID,
				Response:     svmErrorResponse(svmOCID, err.Error()),
			}, nil
		}
		if utilserrors.IsNotFoundErr(err) {
			return &ociserver.CreateSvmByPoolNotFound{
				OpcRequestID: opcRequestID,
				Response:     svmErrorResponse(svmOCID, err.Error()),
			}, nil
		}
		if utilserrors.IsConflictErr(err) {
			msg := err.Error()
			if unwrapped := errors.Unwrap(err); unwrapped != nil {
				msg = unwrapped.Error()
			}
			return &ociserver.CreateSvmByPoolConflict{
				OpcRequestID: opcRequestID,
				Response:     svmErrorResponse(svmOCID, msg),
			}, nil
		}
		logger.Error("CreateSvm orchestrator error", "error", err)
		return &ociserver.CreateSvmByPoolInternalServerError{
			OpcRequestID: opcRequestID,
			Response:     svmErrorResponse(svmOCID, "Internal server error"),
		}, nil
	}

	return &ociserver.CreateSvmAcceptedResponseHeaders{
		OpcRequestID: opcRequestID,
		Response: ociserver.CreateSvmAcceptedResponse{
			Status:     string(workflowquery.WorkflowStatusInProgress),
			WorkflowId: workflowID,
			SvmOCID:    svmOCID,
		},
	}, nil
}

// DeleteSvm implements generated deleteSvm operation.
func (h *Handler) DeleteSvm(ctx context.Context, req ociserver.OptDeleteSvmReq, params ociserver.DeleteSvmParams) (ociserver.DeleteSvmRes, error) {
	logger := util.GetLogger(ctx)

	opcRequestID, err := opcRequestIDFromContext(ctx)
	if err != nil {
		logger.Error("missing opc-request-id in context", "error", err)
		return &ociserver.DeleteSvmBadRequest{
			OpcRequestID: uuid.NewString(),
			Response:     svmErrorResponse(params.SvmOCID, invalidOPCRequestID),
		}, nil
	}

	svmOCID := strings.TrimSpace(params.SvmOCID)
	poolOCID := strings.TrimSpace(params.PoolOCID)
	tenancyOCID := strings.TrimSpace(params.TenancyOcid)

	if msg := validateRequiredOCIDs(
		struct{ name, value string }{"svmOCID", svmOCID},
		struct{ name, value string }{"poolOCID", poolOCID},
		struct{ name, value string }{"Tenancy-Ocid", tenancyOCID},
	); msg != "" {
		return &ociserver.DeleteSvmBadRequest{
			OpcRequestID: opcRequestID,
			Response:     svmErrorResponse(svmOCID, msg),
		}, nil
	}

	force := false
	if req.IsSet() {
		force = req.Value.Force.Or(false)
	}

	workflowID, err := h.Orchestrator.DeleteSvm(ctx, &commonparams.DeleteSvmParams{
		PoolOCID:    poolOCID,
		AccountName: tenancyOCID,
		SvmID:       svmOCID,
		Force:       force,
	})
	if err != nil {
		if utilserrors.IsUserInputValidationErr(err) || utilserrors.IsBadRequestErr(err) {
			return &ociserver.DeleteSvmBadRequest{
				OpcRequestID: opcRequestID,
				Response:     svmErrorResponse(svmOCID, err.Error()),
			}, nil
		}
		if utilserrors.IsNotFoundErr(err) {
			logger.Info("SVM not found for delete", "svmOCID", svmOCID)
			return &ociserver.DeleteSvmNotFound{
				OpcRequestID: opcRequestID,
				Response:     svmErrorResponse(svmOCID, err.Error()),
			}, nil
		}
		if utilserrors.IsConflictErr(err) {
			msg := err.Error()
			if unwrapped := errors.Unwrap(err); unwrapped != nil {
				msg = unwrapped.Error()
			}
			return &ociserver.DeleteSvmConflict{
				OpcRequestID: opcRequestID,
				Response:     svmErrorResponse(svmOCID, msg),
			}, nil
		}
		logger.Error("DeleteSvm orchestrator error", "svmOCID", svmOCID, "error", err)
		return &ociserver.DeleteSvmInternalServerError{
			OpcRequestID: opcRequestID,
			Response:     svmErrorResponse(svmOCID, "Internal server error"),
		}, nil
	}

	return &ociserver.DeleteSvmAcceptedResponseHeaders{
		OpcRequestID: opcRequestID,
		Response: ociserver.DeleteSvmAcceptedResponse{
			Status:     string(workflowquery.WorkflowStatusInProgress),
			WorkflowId: workflowID,
			SvmOCID:    svmOCID,
		},
	}, nil
}
