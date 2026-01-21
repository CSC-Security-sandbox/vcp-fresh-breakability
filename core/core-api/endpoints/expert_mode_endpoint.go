package api

import (
	"context"
	"fmt"
	"net/http"

	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/core-api/core-servergen"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

// V1ExpertModeVolume implements the expert mode volume creation endpoint
func (h Handler) V1ExpertModeVolume(ctx context.Context, req *oasgenserver.ExpertModeVolumeV1, params oasgenserver.V1ExpertModeVolumeParams) (oasgenserver.V1ExpertModeVolumeRes, error) {
	// Create orchestrator parameters
	orchestratorParams := &commonparams.ExpertModeVolumeParams{
		PoolUUID:    req.PoolUUID,
		Action:      string(req.Action),
		VolumeName:  req.VolumeName,
		VolumeUUID:  req.VolumeUUID.Or(""),
		SizeInBytes: int64(req.SizeInBytes),
		Style:       string(req.Style),
		SvmUuid:     req.SvmUuid.Or(""),
		SvmName:     req.SvmName.Or(""),
		AccountName: req.ProjectNumber,
	}

	var err error
	switch req.Action {
	case oasgenserver.ExpertModeVolumeV1ActionDelete:
		// Execute the expert mode volume deletion
		err = h.Orchestrator.DeleteExpertModeVolume(ctx, orchestratorParams)
	case oasgenserver.ExpertModeVolumeV1ActionCreate:
		// Execute the expert mode volume creation
		err = h.Orchestrator.CreateExpertModeVolume(ctx, orchestratorParams)
	case oasgenserver.ExpertModeVolumeV1ActionUpdate:
		// Execute the expert mode volume update
		err = h.Orchestrator.UpdateExpertModeVolume(ctx, orchestratorParams)
	}

	if err != nil {
		if customerrors.IsBadRequestErr(err) {
			return &oasgenserver.V1ExpertModeVolumeBadRequest{
				Message: err.Error(),
				Code:    http.StatusBadRequest,
			}, nil
		}
		return &oasgenserver.V1ExpertModeVolumeInternalServerError{
			Message: err.Error(),
			Code:    http.StatusInternalServerError,
		}, nil
	}

	return &oasgenserver.V1ExpertModeVolumeOK{}, nil
}

// V1RefreshRbacForExpertModePools implements the RBAC refresh endpoint
func (h Handler) V1RefreshRbacForExpertModePools(ctx context.Context, params oasgenserver.V1RefreshRbacForExpertModePoolsParams) (oasgenserver.V1RefreshRbacForExpertModePoolsRes, error) {
	// Trigger the RBAC update workflow
	jobID, err := h.Orchestrator.UpdateRbacForPools(ctx)
	if err != nil {
		if customerrors.IsBadRequestErr(err) {
			return &oasgenserver.V1RefreshRbacForExpertModePoolsBadRequest{
				Message: err.Error(),
				Code:    http.StatusBadRequest,
			}, nil
		}
		return &oasgenserver.V1RefreshRbacForExpertModePoolsInternalServerError{
			Message: err.Error(),
			Code:    http.StatusInternalServerError,
		}, nil
	}

	// Return 202 Accepted with Operation object
	return &oasgenserver.OperationV1{
		Done: oasgenserver.NewOptBool(false),
		Name: oasgenserver.NewOptString(fmt.Sprintf("/v1/expertMode/rbac/refresh/operations/%s", jobID)),
	}, nil
}
