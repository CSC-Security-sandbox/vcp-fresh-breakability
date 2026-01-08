package api

import (
	"context"
	"net/http"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
)

func (h Handler) V1betaCreateVolumePerformanceGroup(ctx context.Context, req *gcpgenserver.VolumePerformanceGroupCreateV1beta, params gcpgenserver.V1betaCreateVolumePerformanceGroupParams) (gcpgenserver.V1betaCreateVolumePerformanceGroupRes, error) {
	createParams := &common.CreateVolumePerformanceGroupParams{}
	_, err := h.Orchestrator.CreateVolumePerformanceGroup(ctx, createParams)
	if err != nil {
		return &gcpgenserver.V1betaCreateVolumePerformanceGroupInternalServerError{
			Code:    http.StatusInternalServerError,
			Message: "Internal server error",
		}, err
	}
	return &gcpgenserver.V1betaCreateVolumePerformanceGroupNotImplemented{
		Code:    http.StatusNotImplemented,
		Message: "Volume performance group creation is not implemented",
	}, nil
}

func (h Handler) V1betaListVolumePerformanceGroups(ctx context.Context, params gcpgenserver.V1betaListVolumePerformanceGroupsParams) (gcpgenserver.V1betaListVolumePerformanceGroupsRes, error) {
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
