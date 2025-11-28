package api

import (
	"context"
	"net/http"

	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/core-api/core-servergen"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

// V1CreateExpertModeVolume implements the expert mode volume creation endpoint
func (h Handler) V1CreateExpertModeVolume(ctx context.Context, req *oasgenserver.ExpertModeVolumeV1, params oasgenserver.V1CreateExpertModeVolumeParams) (oasgenserver.V1CreateExpertModeVolumeRes, error) {
	// Create orchestrator parameters
	createParams := &commonparams.CreateExpertModeVolumeParams{
		PoolUUID:    req.PoolUUID,
		Action:      string(req.Action),
		VolumeName:  req.VolumeName,
		SizeInBytes: int64(req.SizeInBytes),
		Style:       string(req.Style),
		SvmUuid:     req.SvmUuid.Or(""),
		SvmName:     req.SvmName.Or(""),
		AccountName: req.ProjectNumber,
	}

	// Execute the expert mode volume creation
	err := h.Orchestrator.CreateExpertModeVolume(ctx, createParams)
	if err != nil {
		if customerrors.IsBadRequestErr(err) {
			return &oasgenserver.V1CreateExpertModeVolumeBadRequest{
				Message: err.Error(),
				Code:    http.StatusBadRequest,
			}, nil
		}
		return &oasgenserver.V1CreateExpertModeVolumeInternalServerError{
			Message: err.Error(),
			Code:    http.StatusInternalServerError,
		}, nil
	}

	return &oasgenserver.V1CreateExpertModeVolumeOK{}, nil
}
