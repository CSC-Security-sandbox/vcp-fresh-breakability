package api

import (
	"context"
	"encoding/json"
	"github.com/go-faster/jx"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"golang.org/x/exp/slog"
)

func (h Handler) V1betaDescribeHostGroup(ctx context.Context, params gcpgenserver.V1betaDescribeHostGroupParams) (gcpgenserver.V1betaDescribeHostGroupRes, error) {
	logger := utils.GetLoggerFromContext(ctx)
	hostGroup, err := h.Orchestrator.GetHostGroup(ctx, params.HostGroupId, params.ProjectNumber)
	if err != nil {
		if customerrors.IsNotFoundErr(err) {
			return &gcpgenserver.V1betaDescribeHostGroupNotFound{
				Code:    404,
				Message: "host group not found",
			}, nil
		}
		logger.Error("Failed to describe hostgroup", slog.String("error", err.Error()))
		return &gcpgenserver.V1betaDescribeHostGroupInternalServerError{}, err
	}
	return convertToHostGroupV1Beta(hostGroup), nil
}

func (h Handler) V1betaCreateHostGroup(ctx context.Context, req *gcpgenserver.HostGroupV1beta, params gcpgenserver.V1betaCreateHostGroupParams) (gcpgenserver.V1betaCreateHostGroupRes, error) {
	logger := utils.GetLoggerFromContext(ctx)

	createParams := &orchestrator.CreateHostGroupParams{
		Name:      req.ResourceId,
		Hosts:     req.Hosts,
		AccountID: params.ProjectNumber,
	}

	if req.Type.IsSet() {
		createParams.HostGroupType, _ = req.Description.Get()
	} else {
		unspecifiedType, _ := gcpgenserver.HostGroupV1betaTypeUNSPECIFIED.MarshalText()
		createParams.HostGroupType = string(unspecifiedType)
	}
	if req.Description.IsSet() {
		createParams.Description, _ = req.Description.Get()
	}

	osType, _ := req.OsType.MarshalText()
	createParams.OSType = string(osType)

	hostGroups, err := h.Orchestrator.CreateHostGroup(ctx, createParams)
	if err != nil {
		if customerrors.IsConflictErr(err) {
			return &gcpgenserver.V1betaCreateHostGroupConflict{
				Code:    409,
				Message: err.Error(),
			}, nil
		}
		logger.Error("Failed to create hostgroup", slog.String("error", err.Error()))
		return &gcpgenserver.V1betaCreateHostGroupInternalServerError{
			Code:    500,
			Message: "Internal server error",
		}, err
	}

	resp, err := encodeHostGroupV1(convertToHostGroupV1Beta(hostGroups))
	if err != nil {
		return nil, err
	}

	return &gcpgenserver.V1betaCreateHostGroupOK{
		Response: resp,
		Done:     gcpgenserver.NewOptBool(true),
	}, nil
}

// encodeHostGroupV1 encodes a HostGroupV1beta struct to JSON.
func encodeHostGroupV1(hostGroup *gcpgenserver.HostGroupV1beta) (jx.Raw, error) {
	data, err := json.Marshal(hostGroup)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func convertToHostGroupV1Beta(hostGroup *models.HostGroup) *gcpgenserver.HostGroupV1beta {
	hostGroupV1Beta := &gcpgenserver.HostGroupV1beta{
		HostGroupId:  gcpgenserver.NewOptString(hostGroup.UUID),
		Created:      gcpgenserver.NewOptDateTime(hostGroup.CreatedAt),
		Updated:      gcpgenserver.NewOptDateTime(hostGroup.UpdatedAt),
		ResourceId:   hostGroup.Name,
		State:        gcpgenserver.NewOptHostGroupV1betaState(gcpgenserver.HostGroupV1betaState(hostGroup.State)),
		StateDetails: gcpgenserver.NewOptString(hostGroup.StateDetails),
		Description:  gcpgenserver.NewOptString(hostGroup.Description),
		Type:         gcpgenserver.NewOptHostGroupV1betaType(gcpgenserver.HostGroupV1betaType(hostGroup.HostGroupType)),
		Hosts:        hostGroup.Hosts,
		OsType:       gcpgenserver.HostGroupV1betaOsType(hostGroup.OSType),
	}

	if hostGroup.DeletedAt != nil {
		hostGroupV1Beta.Deleted = gcpgenserver.NewOptNilDateTime(*hostGroup.DeletedAt)
	}

	return hostGroupV1Beta
}
