package api

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/go-faster/jx"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/helper"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	enableVpgEndpoints = env.GetBool("ENABLE_VPG_ENDPOINTS", false)
)

func (h Handler) V1betaCreateVolumePerformanceGroup(ctx context.Context, req *gcpgenserver.VolumePerformanceGroupCreateV1beta, params gcpgenserver.V1betaCreateVolumePerformanceGroupParams) (gcpgenserver.V1betaCreateVolumePerformanceGroupRes, error) {
	if !enableMqos || !enableVpgEndpoints {
		return &gcpgenserver.V1betaCreateVolumePerformanceGroupNotImplemented{
			Code:    http.StatusNotImplemented,
			Message: "Volume performance group creation is not enabled",
		}, nil
	}
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)

	createParams := &common.CreateVolumePerformanceGroupParams{
		AccountName:     params.ProjectNumber,
		PoolID:          params.PoolId,
		Name:            req.ResourceId,
		ThroughputMibps: req.ThroughputMibps,
		Iops:            req.Iops,
		IsShared:        req.IsShared,
	}
	volumePerformanceGroup, err := h.Orchestrator.CreateVolumePerformanceGroup(ctx, createParams)
	if err != nil {
		logger.Error("Failed to create volume performance group", "error", err.Error())
		if errors.IsUserInputValidationErr(err) || errors.IsBadRequestErr(err) || errors.IsNotFoundErr(err) {
			return &gcpgenserver.V1betaCreateVolumePerformanceGroupBadRequest{
				Code:    http.StatusBadRequest,
				Message: err.Error(),
			}, nil
		}
		if errors.IsConflictErr(err) {
			return &gcpgenserver.V1betaCreateVolumePerformanceGroupConflict{
				Code:    http.StatusConflict,
				Message: err.Error(),
			}, nil
		}
		return &gcpgenserver.V1betaCreateVolumePerformanceGroupInternalServerError{
			Code:    http.StatusInternalServerError,
			Message: err.Error(),
		}, err
	}
	vcpVolumePerformanceGroup := convertModelToVCPVolumePerformanceGroup(volumePerformanceGroup, params.PoolId)
	return vcpVolumePerformanceGroup, nil
}

func (h Handler) V1betaListVolumePerformanceGroups(ctx context.Context, params gcpgenserver.V1betaListVolumePerformanceGroupsParams) (gcpgenserver.V1betaListVolumePerformanceGroupsRes, error) {
	if !enableMqos || !enableVpgEndpoints {
		return &gcpgenserver.V1betaListVolumePerformanceGroupsNotImplemented{
			Code:    http.StatusNotImplemented,
			Message: "Listing volume performance groups is not enabled",
		}, nil
	}
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	listParams := &common.ListVolumePerformanceGroupsParams{
		AccountName: params.ProjectNumber,
		PoolID:      params.PoolId,
	}
	vpgs, err := h.Orchestrator.ListVolumePerformanceGroups(ctx, listParams)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			return &gcpgenserver.V1betaListVolumePerformanceGroupsNotFound{
				Code:    http.StatusNotFound,
				Message: "Pool not found",
			}, nil
		}
		if errors.IsUserInputValidationErr(err) {
			return &gcpgenserver.V1betaListVolumePerformanceGroupsBadRequest{
				Code:    http.StatusBadRequest,
				Message: err.Error(),
			}, nil
		}
		logger.Error("Failed to list volume performance groups", "error", err.Error())
		return &gcpgenserver.V1betaListVolumePerformanceGroupsInternalServerError{
			Code:    http.StatusInternalServerError,
			Message: "Internal server error",
		}, err
	}

	// Convert to response model
	vpgResponses := make([]gcpgenserver.VolumePerformanceGroupV1beta, 0, len(vpgs))
	for _, vpg := range vpgs {
		vpgResponse := convertModelToVCPVolumePerformanceGroup(vpg, params.PoolId)
		vpgResponses = append(vpgResponses, *vpgResponse)
	}

	return &gcpgenserver.V1betaListVolumePerformanceGroupsOK{
		VolumePerformanceGroups: vpgResponses,
	}, nil
}

func (h Handler) V1betaDescribeVolumePerformanceGroup(ctx context.Context, params gcpgenserver.V1betaDescribeVolumePerformanceGroupParams) (gcpgenserver.V1betaDescribeVolumePerformanceGroupRes, error) {
	if !enableMqos || !enableVpgEndpoints {
		return &gcpgenserver.V1betaDescribeVolumePerformanceGroupNotImplemented{
			Code:    http.StatusNotImplemented,
			Message: "Describing volume performance group is not enabled",
		}, nil
	}
	logger := util.GetLogger(ctx)
	getParams := &common.GetVolumePerformanceGroupParams{
		AccountName:              params.ProjectNumber,
		PoolID:                   params.PoolId,
		VolumePerformanceGroupID: params.VolumePerformanceGroupId,
	}
	vpg, err := h.Orchestrator.GetVolumePerformanceGroup(ctx, getParams)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			return &gcpgenserver.V1betaDescribeVolumePerformanceGroupNotFound{
				Code:    http.StatusNotFound,
				Message: "Volume performance group not found",
			}, nil
		}
		if errors.IsUserInputValidationErr(err) {
			return &gcpgenserver.V1betaDescribeVolumePerformanceGroupBadRequest{
				Code:    http.StatusBadRequest,
				Message: err.Error(),
			}, nil
		}
		logger.Error("Failed to describe volume performance group", "error", err.Error())
		return &gcpgenserver.V1betaDescribeVolumePerformanceGroupInternalServerError{
			Code:    http.StatusInternalServerError,
			Message: "Internal server error",
		}, err
	}

	// Convert to response model
	response := convertModelToVCPVolumePerformanceGroup(vpg, params.PoolId)
	return response, nil
}

func (h Handler) V1betaUpdateVolumePerformanceGroup(ctx context.Context, req *gcpgenserver.VolumePerformanceGroupUpdateV1beta, params gcpgenserver.V1betaUpdateVolumePerformanceGroupParams) (gcpgenserver.V1betaUpdateVolumePerformanceGroupRes, error) {
	if !enableMqos || !enableVpgEndpoints {
		return &gcpgenserver.V1betaUpdateVolumePerformanceGroupNotImplemented{
			Code:    http.StatusNotImplemented,
			Message: "Updating volume performance group is not enabled",
		}, nil
	}
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)

	name := ""
	if req.ResourceId.IsSet() {
		name = req.ResourceId.Value
	}
	var throughputMibps, iops *int64
	if req.ThroughputMibps.IsSet() {
		throughputMibps = nillable.ToPointer(req.ThroughputMibps.Value)
	}
	if req.Iops.IsSet() {
		iops = nillable.ToPointer(req.Iops.Value)
	}
	updateParams := &common.UpdateVolumePerformanceGroupParams{
		AccountName:              params.ProjectNumber,
		PoolID:                   params.PoolId,
		VolumePerformanceGroupID: params.VolumePerformanceGroupId,
		Name:                     name,
		ThroughputMibps:          throughputMibps,
		Iops:                     iops,
	}
	vpg, jobUUID, err := h.Orchestrator.UpdateVolumePerformanceGroup(ctx, updateParams)
	if err != nil {
		if errors.IsUserInputValidationErr(err) || errors.IsNotFoundErr(err) {
			return &gcpgenserver.V1betaUpdateVolumePerformanceGroupBadRequest{
				Code:    http.StatusBadRequest,
				Message: err.Error(),
			}, nil
		}
		if errors.IsConflictErr(err) {
			return &gcpgenserver.V1betaUpdateVolumePerformanceGroupConflict{
				Code:    http.StatusConflict,
				Message: err.Error(),
			}, nil
		}
		logger.Error("Failed to update volume performance group", "error", err.Error())
		return &gcpgenserver.V1betaUpdateVolumePerformanceGroupInternalServerError{
			Code:    http.StatusInternalServerError,
			Message: "Internal server error",
		}, err
	}

	vcpVPG := convertModelToVCPVolumePerformanceGroup(vpg, params.PoolId)
	data, err := json.Marshal(vcpVPG)
	if err != nil {
		return &gcpgenserver.V1betaUpdateVolumePerformanceGroupInternalServerError{
			Code:    http.StatusInternalServerError,
			Message: "Failed to encode response",
		}, err
	}
	resp := jx.Raw(data)
	operationID := "/v1beta/projects/" + params.ProjectNumber + "/locations/" + params.LocationId + "/operations/" + jobUUID
	return &gcpgenserver.OperationV1beta{
		Name:     gcpgenserver.NewOptString(operationID),
		Response: resp,
		Done:     gcpgenserver.NewOptBool(false),
	}, nil
}

func (h Handler) V1betaDeleteVolumePerformanceGroup(ctx context.Context, params gcpgenserver.V1betaDeleteVolumePerformanceGroupParams) (gcpgenserver.V1betaDeleteVolumePerformanceGroupRes, error) {
	if !enableMqos || !enableVpgEndpoints {
		return &gcpgenserver.V1betaDeleteVolumePerformanceGroupNotImplemented{
			Code:    http.StatusNotImplemented,
			Message: "Deleting volume performance group is not enabled",
		}, nil
	}
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)

	deleteParams := &common.DeleteVolumePerformanceGroupParams{
		AccountName:              params.ProjectNumber,
		PoolID:                   params.PoolId,
		VolumePerformanceGroupID: params.VolumePerformanceGroupId,
	}
	vpg, err := h.Orchestrator.DeleteVolumePerformanceGroup(ctx, deleteParams)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			return &gcpgenserver.V1betaDeleteVolumePerformanceGroupNotFound{
				Code:    http.StatusNotFound,
				Message: "Volume performance group not found",
			}, nil
		}
		if errors.IsConflictErr(err) {
			return &gcpgenserver.V1betaDeleteVolumePerformanceGroupConflict{
				Code:    http.StatusConflict,
				Message: err.Error(),
			}, nil
		}
		if errors.IsUserInputValidationErr(err) {
			return &gcpgenserver.V1betaDeleteVolumePerformanceGroupBadRequest{
				Code:    http.StatusBadRequest,
				Message: err.Error(),
			}, nil
		}
		logger.Error("Failed to delete volume performance group", "error", err.Error())
		return &gcpgenserver.V1betaDeleteVolumePerformanceGroupInternalServerError{
			Code:    http.StatusInternalServerError,
			Message: "Internal server error",
		}, err
	}
	// Return operation with deleted VPG in response (like volume delete).
	vcpVPG := convertModelToVCPVolumePerformanceGroup(vpg, params.PoolId)
	data, err := json.Marshal(vcpVPG)
	if err != nil {
		return &gcpgenserver.V1betaDeleteVolumePerformanceGroupInternalServerError{
			Code:    http.StatusInternalServerError,
			Message: "Failed to encode response",
		}, err
	}
	operationID := "/v1beta/projects/" + params.ProjectNumber + "/locations/" + params.LocationId + "/operations/vpg-delete-" + vpg.UUID
	return &gcpgenserver.OperationV1beta{
		Name:     gcpgenserver.NewOptString(operationID),
		Response: jx.Raw(data),
		Done:     gcpgenserver.NewOptBool(true),
	}, nil
}

func convertModelToVCPVolumePerformanceGroup(vpg *models.VolumePerformanceGroup, poolId string) *gcpgenserver.VolumePerformanceGroupV1beta {
	if vpg == nil {
		return nil
	}

	return &gcpgenserver.VolumePerformanceGroupV1beta{
		ResourceId:               vpg.Name,
		PoolId:                   poolId,
		VolumePerformanceGroupId: vpg.UUID,
		ThroughputMibps:          vpg.ThroughputMibps,
		Iops:                     vpg.Iops,
		IsShared:                 vpg.IsShared,
	}
}
