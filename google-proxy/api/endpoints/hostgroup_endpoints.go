package api

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-faster/jx"
	"github.com/google/uuid"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/helper"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	operationPathFormat = "/v1beta/projects/%s/locations/%s/operations/%s"
	maxHostsPerHG       = env.GetInt("MAX_HOSTS_PER_HOSTGROUP", 64)
)

func (h Handler) V1betaDescribeHostGroup(ctx context.Context, params gcpgenserver.V1betaDescribeHostGroupParams) (gcpgenserver.V1betaDescribeHostGroupRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	hostGroup, err := h.Orchestrator.GetHostGroup(ctx, params.HostGroupId, params.ProjectNumber)
	if err != nil {
		if customerrors.IsNotFoundErr(err) {
			return &gcpgenserver.V1betaDescribeHostGroupNotFound{
				Code:    404,
				Message: "host group not found",
			}, nil
		}
		logger.Error("Failed to describe hostgroup", "error", err.Error())
		return &gcpgenserver.V1betaDescribeHostGroupInternalServerError{Code: 500,
			Message: "Internal server error"}, err
	}
	return convertToHostGroupV1Beta(hostGroup), nil
}

func (h Handler) V1betaCreateHostGroup(ctx context.Context, req *gcpgenserver.HostGroupV1beta, params gcpgenserver.V1betaCreateHostGroupParams) (gcpgenserver.V1betaCreateHostGroupRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	// Validate the location
	_, _, parsingErr := parseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaCreateHostGroupBadRequest{
			Code:    parsingErr.Code,
			Message: parsingErr.Message,
		}, nil
	}

	createParams := &common.CreateHostGroupParams{
		Name:        req.ResourceId,
		Hosts:       DeduplicateSlice(req.Hosts),
		AccountName: params.ProjectNumber,
	}

	if len(createParams.Hosts) > maxHostsPerHG {
		return &gcpgenserver.V1betaCreateHostGroupBadRequest{
			Code:    400,
			Message: fmt.Sprintf("Host group cannot have more than %d hosts", maxHostsPerHG),
		}, nil
	}

	if req.Type.IsSet() {
		createParams.HostGroupType = string(req.Type.Value)
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
		logger.Error("Failed to create hostgroup", "error", err.Error())
		return &gcpgenserver.V1betaCreateHostGroupInternalServerError{
			Code:    500,
			Message: "Internal server error",
		}, err
	}

	resp, err := encodeHostGroupV1(convertToHostGroupV1Beta(hostGroups))
	if err != nil {
		return nil, err
	}
	operationID := fmt.Sprintf(operationPathFormat, params.ProjectNumber, params.LocationId, uuid.UUID{}.String())
	return &gcpgenserver.V1betaCreateHostGroupOK{
		Response: resp,
		Done:     gcpgenserver.NewOptBool(true),
		Name:     gcpgenserver.NewOptString(operationID),
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

func (h Handler) V1betaDeleteHostGroup(ctx context.Context, params gcpgenserver.V1betaDeleteHostGroupParams) (gcpgenserver.V1betaDeleteHostGroupRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)

	// Validate the location
	_, _, parsingErr := parseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaDeleteHostGroupBadRequest{
			Code:    parsingErr.Code,
			Message: parsingErr.Message,
		}, nil
	}
	_, err := h.Orchestrator.GetHostGroup(ctx, params.HostGroupId, params.ProjectNumber)
	if err != nil {
		if customerrors.IsNotFoundErr(err) {
			operationID := fmt.Sprintf(operationPathFormat, params.ProjectNumber, params.LocationId, uuid.UUID{}.String())
			return &gcpgenserver.OperationV1beta{
				Name: gcpgenserver.NewOptString(operationID),
				Done: gcpgenserver.NewOptBool(true),
			}, nil
		}
		logger.Error("Failed to delete hostgroup", "error", err.Error())
		return &gcpgenserver.V1betaDeleteHostGroupInternalServerError{
			Code:    500,
			Message: "Internal server error",
		}, err
	}

	_, err = h.Orchestrator.DeleteHostGroup(ctx, params.ProjectNumber, params.HostGroupId)
	if err != nil {
		if customerrors.IsUserInputValidationErr(err) {
			return &gcpgenserver.V1betaDeleteHostGroupBadRequest{
				Code:    400,
				Message: err.Error(),
			}, nil
		}
		logger.Error("Failed to delete hostgroup", "error", err.Error())
		return &gcpgenserver.V1betaDeleteHostGroupInternalServerError{
			Code:    500,
			Message: "Internal server error",
		}, err
	}

	operationID := fmt.Sprintf(operationPathFormat, params.ProjectNumber, params.LocationId, uuid.UUID{}.String())
	return &gcpgenserver.OperationV1beta{
		Name: gcpgenserver.NewOptString(operationID),
		Done: gcpgenserver.NewOptBool(true),
	}, nil
}

func (h Handler) V1betaGetMultipleHostGroups(ctx context.Context, req *gcpgenserver.HostGroupIdListV1beta, params gcpgenserver.V1betaGetMultipleHostGroupsParams) (gcpgenserver.V1betaGetMultipleHostGroupsRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	hg, err := h.Orchestrator.GetMultipleHostGroups(ctx, params.ProjectNumber, req.HostGroupUuids)
	if err != nil {
		logger.Error("Failed to get multiple hostgroup", "error", err.Error())
		return &gcpgenserver.V1betaGetMultipleHostGroupsInternalServerError{
			Code:    500,
			Message: "Internal server error",
		}, err
	}

	resp := &gcpgenserver.V1betaGetMultipleHostGroupsOK{}
	for _, hostGroup := range hg {
		resp.HostGroups = append(resp.HostGroups, *convertToHostGroupV1Beta(hostGroup))
	}
	return resp, nil
}

func (h Handler) V1betaUpdateHostGroup(ctx context.Context, req *gcpgenserver.HostGroupUpdateV1beta, params gcpgenserver.V1betaUpdateHostGroupParams) (r gcpgenserver.V1betaUpdateHostGroupRes, _ error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)

	// Validate the location
	_, _, parsingErr := parseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaUpdateHostGroupBadRequest{
			Code:    parsingErr.Code,
			Message: parsingErr.Message,
		}, nil
	}

	_, err := h.Orchestrator.GetHostGroup(ctx, params.HostGroupId, params.ProjectNumber)
	if err != nil {
		if customerrors.IsNotFoundErr(err) {
			return &gcpgenserver.V1betaUpdateHostGroupNotFound{
				Code:    404,
				Message: "host group not found",
			}, nil
		}
		logger.Error("Failed to update hostgroup", "error", err.Error())
		return &gcpgenserver.V1betaUpdateHostGroupInternalServerError{
			Code:    500,
			Message: "Internal server error",
		}, nil
	}

	updateParams := &common.UpdateHostGroupParams{
		Hosts:         DeduplicateSlice(req.Hosts),
		AccountName:   params.ProjectNumber,
		HostGroupUUID: params.HostGroupId,
	}

	if len(updateParams.Hosts) > maxHostsPerHG {
		return &gcpgenserver.V1betaUpdateHostGroupBadRequest{
			Code:    400,
			Message: fmt.Sprintf("Host group cannot have more than %d hosts", maxHostsPerHG),
		}, nil
	}

	if req.Description.IsSet() {
		updateParams.Description = &req.Description.Value
	}

	updateHG, jobId, err := h.Orchestrator.UpdateHostGroup(ctx, updateParams)
	if err != nil {
		if customerrors.IsUserInputValidationErr(err) {
			return &gcpgenserver.V1betaUpdateHostGroupBadRequest{
				Code:    400,
				Message: err.Error(),
			}, nil
		}
		logger.Error("Failed to update hostgroup", "error", err.Error())
		return &gcpgenserver.V1betaUpdateHostGroupInternalServerError{
			Code:    500,
			Message: "Internal server error",
		}, nil
	}

	resp, err := encodeHostGroupV1(convertToHostGroupV1Beta(updateHG))
	if err != nil {
		return nil, err
	}

	return &gcpgenserver.OperationV1beta{
		Name:     gcpgenserver.NewOptString(hyperscaler.PrepareOperationID(params.ProjectNumber, params.LocationId, jobId)),
		Response: resp,
		Done:     gcpgenserver.NewOptBool(false),
	}, nil
}
