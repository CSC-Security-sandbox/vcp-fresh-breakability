package api

import (
	"context"
	"fmt"

	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/vcp-core/servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// V1OnboardExternalCluster implements POST /v1/externalClusters/onboard.
func (h Handler) V1OnboardExternalCluster(ctx context.Context, req *oasgenserver.ExternalClusterOnboardRequestV1, _ oasgenserver.V1OnboardExternalClusterParams) (oasgenserver.V1OnboardExternalClusterRes, error) {
	logger := util.GetLogger(ctx)

	if req == nil {
		return &oasgenserver.V1OnboardExternalClusterBadRequest{
			Code:    400,
			Message: "request body is required",
		}, nil
	}

	params := &commonparams.OnboardExternalClustersParams{
		LocationID: req.LocationId,
		Hosts:      make([]commonparams.ExternalClusterParams, 0, len(req.Hosts)),
	}
	for _, host := range req.Hosts {
		hostParams := commonparams.ExternalClusterParams{
			HostName:     host.HostName,
			Username:     host.AdminCredentials.Username,
			Password:     host.AdminCredentials.Password,
			ManagementIP: host.ManagementIp,
		}
		if host.Description.IsSet() {
			hostParams.Description = host.Description.Value
		}
		if host.Label.IsSet() {
			hostParams.Label = host.Label.Value
		}
		if host.Protocol.IsSet() {
			hostParams.Protocol = string(host.Protocol.Value)
		}
		if host.Port.IsSet() {
			hostParams.Port = int(host.Port.Value)
		}
		params.Hosts = append(params.Hosts, hostParams)
	}

	created, err := h.Orchestrator.OnboardExternalClusters(ctx, params)
	if err != nil {
		if errors.IsNotImplementedYetErr(err) {
			return &oasgenserver.V1OnboardExternalClusterInternalServerError{
				Code:    500,
				Message: "External cluster onboarding is not implemented yet",
			}, nil
		}
		if customErr := asVCPError(err); customErr != nil && customErr.IsError(vsaerrors.ErrResourceStateConflictError) {
			return &oasgenserver.V1OnboardExternalClusterConflict{
				Code:    409,
				Message: customErr.GetDetailMessage(),
			}, nil
		}
		if errors.IsConflictErr(err) {
			return &oasgenserver.V1OnboardExternalClusterConflict{
				Code:    409,
				Message: err.Error(),
			}, nil
		}
		if errors.IsUserInputValidationErr(err) || errors.IsBadRequestErr(err) {
			return &oasgenserver.V1OnboardExternalClusterBadRequest{
				Code:    400,
				Message: err.Error(),
			}, nil
		}
		if errors.IsNotFoundErr(err) {
			return &oasgenserver.V1OnboardExternalClusterNotFound{
				Code:    404,
				Message: err.Error(),
			}, nil
		}
		logger.Errorf("Failed to onboard external cluster hosts: %v", err)
		return &oasgenserver.V1OnboardExternalClusterInternalServerError{
			Code:    500,
			Message: fmt.Sprintf("Failed to onboard external cluster hosts: %v", err),
		}, nil
	}

	result := make(oasgenserver.V1OnboardExternalClusterCreatedApplicationJSON, 0, len(created))
	for _, row := range created {
		result = append(result, convertExternalClusterToV1(row))
	}
	return &result, nil
}

// V1UpdateExternalCluster implements PUT /v1/externalClusters/{externalClusterId}.
func (h Handler) V1UpdateExternalCluster(ctx context.Context, req *oasgenserver.ExternalClusterHostUpdateV1, params oasgenserver.V1UpdateExternalClusterParams) (oasgenserver.V1UpdateExternalClusterRes, error) {
	logger := util.GetLogger(ctx)

	if req == nil {
		return &oasgenserver.V1UpdateExternalClusterBadRequest{
			Code:    400,
			Message: "request body is required",
		}, nil
	}

	updateParams := &commonparams.UpdateExternalClusterParams{
		ExternalClusterID: params.ExternalClusterId,
		Description:       optionalStringFromOptNil(req.Description),
		Label:             optionalStringFromOpt(req.Label),
		ManagementIP:      optionalStringFromOptNil(req.ManagementIp),
		Protocol:          optionalProtocolFromOpt(req.Protocol),
		Port:              optionalIntFromOpt(req.Port),
	}
	if req.AdminCredentials.IsSet() {
		creds := req.AdminCredentials.Value
		updateParams.Username = &creds.Username
		updateParams.Password = &creds.Password
	}

	if !updateParams.HasUpdates() {
		return &oasgenserver.V1UpdateExternalClusterBadRequest{
			Code:    400,
			Message: "at least one field must be provided to update",
		}, nil
	}

	updated, err := h.Orchestrator.UpdateExternalCluster(ctx, updateParams)
	if err != nil {
		if errors.IsNotImplementedYetErr(err) {
			return &oasgenserver.V1UpdateExternalClusterInternalServerError{
				Code:    500,
				Message: "External cluster host update is not implemented yet",
			}, nil
		}
		if customErr := asVCPError(err); customErr != nil && customErr.IsError(vsaerrors.ErrDatabaseDataNotFoundError) {
			return &oasgenserver.V1UpdateExternalClusterNotFound{
				Code:    404,
				Message: "External cluster host not found",
			}, nil
		}
		if errors.IsUserInputValidationErr(err) || errors.IsBadRequestErr(err) {
			return &oasgenserver.V1UpdateExternalClusterBadRequest{
				Code:    400,
				Message: err.Error(),
			}, nil
		}
		logger.Errorf("Failed to update external cluster host: %v", err)
		return &oasgenserver.V1UpdateExternalClusterInternalServerError{
			Code:    500,
			Message: fmt.Sprintf("Failed to update external cluster host: %v", err),
		}, nil
	}

	result := convertExternalClusterToV1(updated)
	return &result, nil
}

// V1GetExternalCluster implements GET /v1/externalClusters/{externalClusterId}.
func (h Handler) V1GetExternalCluster(ctx context.Context, params oasgenserver.V1GetExternalClusterParams) (oasgenserver.V1GetExternalClusterRes, error) {
	host, err := h.Orchestrator.GetExternalCluster(ctx, params.ExternalClusterId)
	if err != nil {
		if customErr := asVCPError(err); customErr != nil {
			if customErr.IsError(vsaerrors.ErrDatabaseDataNotFoundError) {
				return &oasgenserver.V1GetExternalClusterNotFound{
					Message: "External cluster host not found",
					Code:    404,
				}, nil
			}
		}
		if errors.IsNotImplementedYetErr(err) {
			return &oasgenserver.V1GetExternalClusterInternalServerError{
				Code:    500,
				Message: "External cluster host get is not implemented yet",
			}, nil
		}
		return &oasgenserver.V1GetExternalClusterInternalServerError{
			Message: fmt.Sprintf("Failed to get external cluster host: %v", err),
			Code:    500,
		}, nil
	}

	result := convertExternalClusterToV1(host)
	return &result, nil
}

// V1DeleteExternalCluster implements DELETE /v1/externalClusters/{externalClusterId}.
func (h Handler) V1DeleteExternalCluster(ctx context.Context, params oasgenserver.V1DeleteExternalClusterParams) (oasgenserver.V1DeleteExternalClusterRes, error) {
	deleted, err := h.Orchestrator.DeleteExternalCluster(ctx, params.ExternalClusterId)
	if err != nil {
		if customErr := asVCPError(err); customErr != nil {
			if customErr.IsError(vsaerrors.ErrDatabaseDataNotFoundError) {
				return &oasgenserver.V1DeleteExternalClusterNotFound{
					Message: "External cluster host not found",
					Code:    404,
				}, nil
			}
		}
		if errors.IsNotImplementedYetErr(err) {
			return &oasgenserver.V1DeleteExternalClusterInternalServerError{
				Code:    500,
				Message: "External cluster host delete is not implemented yet",
			}, nil
		}
		return &oasgenserver.V1DeleteExternalClusterInternalServerError{
			Message: fmt.Sprintf("Failed to delete external cluster host: %v", err),
			Code:    500,
		}, nil
	}

	result := convertExternalClusterToV1(deleted)
	return &result, nil
}

func optionalStringFromOptNil(o oasgenserver.OptNilString) *string {
	if !o.IsSet() {
		return nil
	}
	v := ""
	if !o.Null {
		v = o.Value
	}
	return &v
}

func optionalStringFromOpt(o oasgenserver.OptString) *string {
	if !o.IsSet() {
		return nil
	}
	v := o.Value
	return &v
}

func optionalIntFromOpt(o oasgenserver.OptInt32) *int {
	if !o.IsSet() {
		return nil
	}
	v := int(o.Value)
	return &v
}

func optionalProtocolFromOpt(o oasgenserver.OptExternalClusterHostUpdateV1Protocol) *string {
	if !o.IsSet() {
		return nil
	}
	v := string(o.Value)
	return &v
}

func convertExternalClusterToV1(row *datamodel.Cluster) oasgenserver.ExternalClusterHostResourceV1 {
	if row == nil {
		return oasgenserver.ExternalClusterHostResourceV1{}
	}
	v := oasgenserver.ExternalClusterHostResourceV1{
		ExternalClusterId: oasgenserver.NewOptString(row.UUID),
		CreatedAt:         oasgenserver.NewOptDateTime(row.CreatedAt),
		UpdatedAt:         oasgenserver.NewOptDateTime(row.UpdatedAt),
		LocationId:        oasgenserver.NewOptString(row.LocationID),
		HostName:          oasgenserver.NewOptString(row.HostName),
		AdminUsername:     oasgenserver.NewOptString(row.AdminUsername),
	}
	if row.Description != "" {
		v.Description = oasgenserver.NewOptString(row.Description)
	}
	v.Label = oasgenserver.NewOptString(row.Label)
	if row.Protocol != "" {
		v.Protocol = oasgenserver.NewOptExternalClusterHostResourceV1Protocol(
			oasgenserver.ExternalClusterHostResourceV1Protocol(row.Protocol),
		)
	}
	if row.Port > 0 {
		v.Port = oasgenserver.NewOptInt32(int32(row.Port))
	}
	if row.ClusterAttributes != nil {
		if row.ClusterAttributes.OntapVersion != "" {
			v.OntapVersion = oasgenserver.NewOptString(row.ClusterAttributes.OntapVersion)
		}
		if row.ClusterAttributes.ManagementIP != "" {
			v.ManagementIp = oasgenserver.NewOptString(row.ClusterAttributes.ManagementIP)
		}
	}
	if row.DeletedAt != nil && row.DeletedAt.Valid {
		v.DeletedAt = oasgenserver.NewOptNilDateTime(row.DeletedAt.Time)
	}
	if row.LifecycleState != "" {
		v.LifeCycleState = oasgenserver.NewOptExternalClusterHostResourceV1LifeCycleState(
			oasgenserver.ExternalClusterHostResourceV1LifeCycleState(row.LifecycleState),
		)
	}
	return v
}
