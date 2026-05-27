package api

import (
	"context"

	"github.com/google/uuid"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	ociserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/oci-proxy/api/oci-servergen"
	utilserrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/workflowquery"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

func (h *Handler) UpgradePool(ctx context.Context, req *ociserver.UpgradePoolRequest, params ociserver.UpgradePoolParams) (ociserver.UpgradePoolRes, error) {
	logger := util.GetLogger(ctx)
	poolOCID := params.PoolOCID

	opcRequestID, err := opcRequestIDFromContext(ctx)
	if err != nil {
		return createUpgradePoolErrorResponse(uuid.NewString(), poolOCID, invalidOPCRequestID, 400), nil
	}

	if errMsg := validateUpgradePoolRequest(req); errMsg != "" {
		return createUpgradePoolErrorResponse(opcRequestID, poolOCID, errMsg, 400), nil
	}

	forceUpgrade := false
	if req.ForceUpgrade.IsSet() {
		forceUpgrade = req.ForceUpgrade.Value
	}

	skipUpdateRBAC := false
	if req.SkipUpdateRBAC.IsSet() {
		skipUpdateRBAC = req.SkipUpdateRBAC.Value
	}

	upgradeParams := &commonparams.UpgradeClusterParams{
		PoolOCID:           poolOCID,
		AccountName:        params.TenancyOcid,
		TargetOntapVersion: req.TargetOntapVersion,
		VSAImagePath:       req.VsaImagePath,
		ForceUpgrade:       forceUpgrade,
		SkipUpdateRBAC:     skipUpdateRBAC,
	}

	_, workflowID, err := h.Orchestrator.UpgradeCluster(ctx, upgradeParams)
	if err != nil {
		logger.Error("Failed to upgrade pool", "poolOCID", poolOCID, "error", err.Error())
		return mapUpgradeError(err, opcRequestID, poolOCID), nil
	}

	return &ociserver.UpgradePoolAcceptedResponseHeaders{
		OpcRequestID: opcRequestID,
		Response: ociserver.UpgradePoolAcceptedResponse{
			Status:     string(workflowquery.WorkflowStatusInProgress),
			WorkflowId: workflowID,
			PoolOCID:   poolOCID,
		},
	}, nil
}

func validateUpgradePoolRequest(req *ociserver.UpgradePoolRequest) string {
	switch {
	case req.TargetOntapVersion == "":
		return "targetOntapVersion is required"
	case req.VsaImagePath == "":
		return "vsaImagePath is required"
	default:
		return ""
	}
}

func mapUpgradeError(err error, opcRequestID, poolOCID string) ociserver.UpgradePoolRes {
	switch {
	case utilserrors.IsNotFoundErr(err):
		return createUpgradePoolErrorResponse(opcRequestID, poolOCID, err.Error(), 404)
	case utilserrors.IsBadRequestErr(err):
		return createUpgradePoolErrorResponse(opcRequestID, poolOCID, err.Error(), 400)
	case utilserrors.IsConflictErr(err):
		return createUpgradePoolErrorResponse(opcRequestID, poolOCID, err.Error(), 409)
	default:
		return createUpgradePoolErrorResponse(opcRequestID, poolOCID, "Internal server error", 500)
	}
}

func createUpgradePoolErrorResponse(opcRequestID, poolOCID, errorMessage string, statusCode int) ociserver.UpgradePoolRes {
	errResponse := ociserver.PoolOperationErrorResponse{
		Status:       string(workflowquery.WorkflowStatusFailed),
		PoolOCID:     poolOCID,
		ErrorMessage: errorMessage,
	}

	switch statusCode {
	case 404:
		return &ociserver.UpgradePoolNotFound{
			OpcRequestID: opcRequestID,
			Response:     errResponse,
		}
	case 400:
		return &ociserver.UpgradePoolBadRequest{
			OpcRequestID: opcRequestID,
			Response:     errResponse,
		}
	case 409:
		return &ociserver.UpgradePoolConflict{
			OpcRequestID: opcRequestID,
			Response:     errResponse,
		}
	default:
		return &ociserver.UpgradePoolInternalServerError{
			OpcRequestID: opcRequestID,
			Response:     errResponse,
		}
	}
}
