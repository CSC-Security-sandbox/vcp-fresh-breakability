package api

import (
	"context"
	"net/http"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/helper"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	enableMqos         = env.GetBool("ENABLE_MQOS", false)
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
		if errors.IsBadRequestErr(err) || errors.IsNotFoundErr(err) {
			return &gcpgenserver.V1betaCreateVolumePerformanceGroupBadRequest{
				Code:    http.StatusBadRequest,
				Message: err.Error(),
			}, nil
		} else if errors.IsConflictErr(err) {
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
	listParams := &common.ListVolumePerformanceGroupsParams{}
	_, err := h.Orchestrator.ListVolumePerformanceGroups(ctx, listParams)
	if err != nil {
		return &gcpgenserver.V1betaListVolumePerformanceGroupsInternalServerError{
			Code:    http.StatusInternalServerError,
			Message: "Internal server error",
		}, err
	}
	return &gcpgenserver.V1betaListVolumePerformanceGroupsNotImplemented{
		Code:    http.StatusNotImplemented,
		Message: "Listing volume performance groups is not implemented",
	}, nil
}

func (h Handler) V1betaDescribeVolumePerformanceGroup(ctx context.Context, params gcpgenserver.V1betaDescribeVolumePerformanceGroupParams) (gcpgenserver.V1betaDescribeVolumePerformanceGroupRes, error) {
	if !enableMqos || !enableVpgEndpoints {
		return &gcpgenserver.V1betaDescribeVolumePerformanceGroupNotImplemented{
			Code:    http.StatusNotImplemented,
			Message: "Describing volume performance group is not enabled",
		}, nil
	}
	getParams := &common.GetVolumePerformanceGroupParams{}
	_, err := h.Orchestrator.GetVolumePerformanceGroup(ctx, getParams)
	if err != nil {
		return &gcpgenserver.V1betaDescribeVolumePerformanceGroupInternalServerError{
			Code:    http.StatusInternalServerError,
			Message: "Internal server error",
		}, err
	}
	return &gcpgenserver.V1betaDescribeVolumePerformanceGroupNotImplemented{
		Code:    http.StatusNotImplemented,
		Message: "Describing volume performance group is not implemented",
	}, nil
}

func (h Handler) V1betaUpdateVolumePerformanceGroup(ctx context.Context, req *gcpgenserver.VolumePerformanceGroupUpdateV1beta, params gcpgenserver.V1betaUpdateVolumePerformanceGroupParams) (gcpgenserver.V1betaUpdateVolumePerformanceGroupRes, error) {
	if !enableMqos || !enableVpgEndpoints {
		return &gcpgenserver.V1betaUpdateVolumePerformanceGroupNotImplemented{
			Code:    http.StatusNotImplemented,
			Message: "Updating volume performance group is not enabled",
		}, nil
	}
	updateParams := &common.UpdateVolumePerformanceGroupParams{}
	_, err := h.Orchestrator.UpdateVolumePerformanceGroup(ctx, updateParams)
	if err != nil {
		return &gcpgenserver.V1betaUpdateVolumePerformanceGroupInternalServerError{
			Code:    http.StatusInternalServerError,
			Message: "Internal server error",
		}, err
	}
	return &gcpgenserver.V1betaUpdateVolumePerformanceGroupNotImplemented{
		Code:    http.StatusNotImplemented,
		Message: "Updating volume performance group is not implemented",
	}, nil
}

func (h Handler) V1betaDeleteVolumePerformanceGroup(ctx context.Context, params gcpgenserver.V1betaDeleteVolumePerformanceGroupParams) (gcpgenserver.V1betaDeleteVolumePerformanceGroupRes, error) {
	if !enableMqos || !enableVpgEndpoints {
		return &gcpgenserver.V1betaDeleteVolumePerformanceGroupNotImplemented{
			Code:    http.StatusNotImplemented,
			Message: "Deleting volume performance group is not enabled",
		}, nil
	}
	deleteParams := &common.DeleteVolumePerformanceGroupParams{}
	err := h.Orchestrator.DeleteVolumePerformanceGroup(ctx, deleteParams)
	if err != nil {
		return &gcpgenserver.V1betaDeleteVolumePerformanceGroupInternalServerError{
			Code:    http.StatusInternalServerError,
			Message: "Internal server error",
		}, err
	}
	return &gcpgenserver.V1betaDeleteVolumePerformanceGroupNotImplemented{
		Code:    http.StatusNotImplemented,
		Message: "Deleting volume performance group is not implemented",
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
