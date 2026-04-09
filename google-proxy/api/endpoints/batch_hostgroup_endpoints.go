package api

import (
	"context"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/helper"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var maxBatchHostGroupUUIDs = env.GetInt("MAX_BATCH_HOSTGROUP_UUIDS", 500)

func (h Handler) V1betaBatchListHostGroups(ctx context.Context, req *gcpgenserver.BatchHostGroupUUIDListV1beta, params gcpgenserver.V1betaBatchListHostGroupsParams) (gcpgenserver.V1betaBatchListHostGroupsRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, "", params.LocationId, nil)

	_, _, parsingErr := parseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaBatchListHostGroupsBadRequest{
			Code:    parsingErr.Code,
			Message: parsingErr.Message,
		}, nil
	}

	if len(req.HostGroupUuids) == 0 {
		return &gcpgenserver.V1betaBatchListHostGroupsBadRequest{
			Code:    400,
			Message: "hostGroupUuids must not be empty",
		}, nil
	}

	if len(req.HostGroupUuids) > maxBatchHostGroupUUIDs {
		return &gcpgenserver.V1betaBatchListHostGroupsBadRequest{
			Code:    400,
			Message: fmt.Sprintf("hostGroupUuids must not exceed %d entries", maxBatchHostGroupUUIDs),
		}, nil
	}

	hostGroups, err := h.Orchestrator.GetHostGroupsByUUIDs(ctx, req.HostGroupUuids)
	if err != nil {
		logger.Error("Failed to batch list host groups", "error", err.Error())
		return &gcpgenserver.V1betaBatchListHostGroupsInternalServerError{
			Code:    500,
			Message: "Internal server error",
		}, nil
	}

	resp := &gcpgenserver.V1betaBatchListHostGroupsOK{}
	for _, hg := range hostGroups {
		resp.HostGroups = append(resp.HostGroups, convertToBatchHostGroupV1Beta(hg))
	}
	return resp, nil
}

func convertToBatchHostGroupV1Beta(hg *models.HostGroup) gcpgenserver.BatchHostGroupV1beta {
	result := gcpgenserver.BatchHostGroupV1beta{
		HostGroupId: gcpgenserver.NewOptString(hg.UUID),
		Created:     gcpgenserver.NewOptNilDateTime(hg.CreatedAt),
		State:       gcpgenserver.NewOptNilBatchHostGroupV1betaState(batchHostGroupState(hg.State)),
		Type:        gcpgenserver.NewOptNilBatchHostGroupV1betaType(batchHostGroupType(hg.HostGroupType)),
		Hosts:       gcpgenserver.NewOptNilStringArray(hg.Hosts),
		OsType:      gcpgenserver.NewOptNilBatchHostGroupV1betaOsType(batchHostGroupOsType(hg.OSType)),
	}

	if hg.Name != "" {
		result.Name = gcpgenserver.NewOptNilString(hg.Name)
		result.ResourceId = gcpgenserver.NewOptNilString(hg.Name)
	} else {
		result.Name.SetToNull()
		result.ResourceId.SetToNull()
	}

	if hg.Description != "" {
		result.Description = gcpgenserver.NewOptNilString(hg.Description)
	} else {
		result.Description.SetToNull()
	}

	return result
}

func batchHostGroupState(s string) gcpgenserver.BatchHostGroupV1betaState {
	if s == "" {
		return gcpgenserver.BatchHostGroupV1betaStateSTATEUNSPECIFIED
	}
	return gcpgenserver.BatchHostGroupV1betaState(s)
}

func batchHostGroupType(t string) gcpgenserver.BatchHostGroupV1betaType {
	if t == "" {
		return gcpgenserver.BatchHostGroupV1betaTypeUNSPECIFIED
	}
	return gcpgenserver.BatchHostGroupV1betaType(t)
}

func batchHostGroupOsType(os string) gcpgenserver.BatchHostGroupV1betaOsType {
	if os == "" {
		return gcpgenserver.BatchHostGroupV1betaOsTypeOSTYPEUNSPECIFIED
	}
	return gcpgenserver.BatchHostGroupV1betaOsType(os)
}
