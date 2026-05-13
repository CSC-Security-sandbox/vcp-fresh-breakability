package api

import (
	"context"
	"fmt"
	"net/http"

	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/vcp-core/servergen"
)

// V1ExpertModeVolume implements the expert mode volume creation endpoint
func (h Handler) V1ExpertModeVolume(ctx context.Context, req *oasgenserver.ExpertModeVolumeV1, params oasgenserver.V1ExpertModeVolumeParams) (oasgenserver.V1ExpertModeVolumeRes, error) {
	// Create orchestrator parameters
	orchestratorParams := &commonparams.ExpertModeVolumeParams{
		PoolUUID:    req.PoolUUID,
		Action:      string(req.Action),
		VolumeName:  req.VolumeName,
		VolumeUUID:  req.VolumeUUID.Or(""),
		SizeInBytes: int64(req.SizeInBytes.Or(0)),
		Style:       string(req.Style),
		SvmUuid:     req.SvmUuid.Or(""),
		SvmName:     req.SvmName.Or(""),
		AccountName: req.ProjectNumber,
	}
	if cloneReq, ok := req.Clone.Get(); ok {
		orchestratorParams.Clone = &commonparams.ExpertModeVolumeCloneParams{
			IsFlexclone: cloneReq.IsFlexclone.Or(false),
		}
		if pv, pvOk := cloneReq.ParentVolume.Get(); pvOk {
			orchestratorParams.Clone.ParentVolume = &commonparams.ExpertModeVolumeCloneParent{
				UUID: pv.UUID.Or(""),
				Name: pv.Name.Or(""),
			}
		}
		if ps, psOk := cloneReq.ParentSnapshot.Get(); psOk {
			orchestratorParams.Clone.ParentSnapshot = &commonparams.ExpertModeVolumeCloneParent{
				UUID: ps.UUID.Or(""),
				Name: ps.Name.Or(""),
			}
		}
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

// V1ExpertModeVolumeRename implements the expert mode volume rename endpoint.
func (h Handler) V1ExpertModeVolumeRename(ctx context.Context, req *oasgenserver.ExpertModeVolumeRenameV1, params oasgenserver.V1ExpertModeVolumeRenameParams) (oasgenserver.V1ExpertModeVolumeRenameRes, error) {
	orchestratorParams := &commonparams.ExpertModeVolumeRenameParams{
		VolumeName:  params.Name,
		NewName:     req.Name,
		PoolUUID:    req.PoolUUID,
		SvmName:     req.SvmName,
		AccountName: req.ProjectNumber,
	}

	err := h.Orchestrator.RenameExpertModeVolume(ctx, orchestratorParams)
	if err != nil {
		if customerrors.IsBadRequestErr(err) {
			return &oasgenserver.V1ExpertModeVolumeRenameBadRequest{
				Message: err.Error(),
				Code:    http.StatusBadRequest,
			}, nil
		}
		return &oasgenserver.V1ExpertModeVolumeRenameInternalServerError{
			Message: err.Error(),
			Code:    http.StatusInternalServerError,
		}, nil
	}

	return &oasgenserver.V1ExpertModeVolumeRenameOK{}, nil
}

// V1ExpertModeVolumeFlexCloneSplit starts a long-running FlexClone split for an expert-mode ONTAP volume.
func (h Handler) V1ExpertModeVolumeFlexCloneSplit(ctx context.Context, req *oasgenserver.ExpertModeVolumeFlexCloneSplitV1, params oasgenserver.V1ExpertModeVolumeFlexCloneSplitParams) (oasgenserver.V1ExpertModeVolumeFlexCloneSplitRes, error) {
	orchParams := &commonparams.ExpertModeFlexCloneSplitParams{
		VolumeUUID:  req.VolumeUUID.Or(""),
		VolumeName:  req.VolumeName.Or(""),
		PoolUUID:    req.PoolUUID,
		AccountName: req.ProjectNumber,
	}
	err := h.Orchestrator.StartExpertModeFlexCloneSplit(ctx, orchParams)
	if err != nil {
		if customerrors.IsBadRequestErr(err) {
			return &oasgenserver.V1ExpertModeVolumeFlexCloneSplitBadRequest{
				Message: err.Error(),
				Code:    http.StatusBadRequest,
			}, nil
		}
		return &oasgenserver.V1ExpertModeVolumeFlexCloneSplitInternalServerError{
			Message: err.Error(),
			Code:    http.StatusInternalServerError,
		}, nil
	}

	return &oasgenserver.V1ExpertModeVolumeFlexCloneSplitAccepted{}, nil
}

// V1RefreshRbacForExpertModePoolById implements the RBAC refresh endpoint for a single pool by UUID
func (h Handler) V1RefreshRbacForExpertModePoolById(ctx context.Context, params oasgenserver.V1RefreshRbacForExpertModePoolByIdParams) (oasgenserver.V1RefreshRbacForExpertModePoolByIdRes, error) {
	jobID, err := h.Orchestrator.UpdateRbacForPoolById(ctx, params.PoolId)
	if err != nil {
		if customerrors.IsBadRequestErr(err) {
			return &oasgenserver.V1RefreshRbacForExpertModePoolByIdBadRequest{
				Message: err.Error(),
				Code:    http.StatusBadRequest,
			}, nil
		}
		if customerrors.IsNotFoundErr(err) {
			return &oasgenserver.V1RefreshRbacForExpertModePoolByIdNotFound{
				Message: err.Error(),
				Code:    http.StatusNotFound,
			}, nil
		}
		return &oasgenserver.V1RefreshRbacForExpertModePoolByIdInternalServerError{
			Message: err.Error(),
			Code:    http.StatusInternalServerError,
		}, nil
	}

	return &oasgenserver.OperationV1{
		Done: oasgenserver.NewOptBool(false),
		Name: oasgenserver.NewOptString(fmt.Sprintf("/v1/expertMode/rbac/refresh/pool/%s/operations/%s", params.PoolId, jobID)),
	}, nil
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
