package api

import (
	"context"
	"fmt"
	"strings"

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

	fieldSet := buildHostGroupFieldSet(params.Fields)

	resp := &gcpgenserver.V1betaBatchListHostGroupsOK{}
	for _, hg := range hostGroups {
		if hg == nil {
			continue
		}
		resp.HostGroups = append(resp.HostGroups, convertToBatchHostGroupV1Beta(hg, fieldSet))
	}
	return resp, nil
}

func buildHostGroupFieldSet(fields []gcpgenserver.V1betaBatchListHostGroupsFieldsItem) map[string]bool {
	if len(fields) == 0 {
		return nil
	}
	set := make(map[string]bool, len(fields))
	for _, f := range fields {
		set[string(f)] = true
	}
	return set
}

// convertToBatchHostGroupV1Beta converts a VCP host group to BatchHostGroupV1beta.
// When fieldSet is nil (no fields requested), only hostGroupId is returned.
// When fieldSet is provided, only requested fields are populated; missing values are null.
func convertToBatchHostGroupV1Beta(hg *models.HostGroup, fieldSet map[string]bool) gcpgenserver.BatchHostGroupV1beta {
	if hg == nil {
		return gcpgenserver.BatchHostGroupV1beta{}
	}
	result := gcpgenserver.BatchHostGroupV1beta{
		HostGroupId: gcpgenserver.NewOptString(jsonSafeString(hg.UUID)),
	}

	if fieldSet == nil {
		return result
	}

	// Omit zero CreatedAt so ensureRequestedHostGroupFieldsPresent can emit JSON null for "created".
	if !hg.CreatedAt.IsZero() {
		result.Created = gcpgenserver.NewOptNilDateTime(hg.CreatedAt)
	}
	result.State = gcpgenserver.NewOptNilBatchHostGroupV1betaState(batchHostGroupState(hg.State))
	result.Type = gcpgenserver.NewOptNilBatchHostGroupV1betaType(batchHostGroupType(hg.HostGroupType))
	if len(hg.Hosts) == 0 {
		result.Hosts.SetToNull()
	} else {
		safeHosts := make([]string, len(hg.Hosts))
		for i, h := range hg.Hosts {
			safeHosts[i] = jsonSafeString(h)
		}
		result.Hosts = gcpgenserver.NewOptNilStringArray(safeHosts)
	}
	result.OsType = gcpgenserver.NewOptNilBatchHostGroupV1betaOsType(batchHostGroupOsType(hg.OSType))

	if hg.Name != "" {
		name := jsonSafeString(hg.Name)
		result.Name = gcpgenserver.NewOptNilString(name)
		result.ResourceId = gcpgenserver.NewOptNilString(name)
	} else {
		result.Name.SetToNull()
		result.ResourceId.SetToNull()
	}

	if hg.Description != "" {
		result.Description = gcpgenserver.NewOptNilString(jsonSafeString(hg.Description))
	} else {
		result.Description.SetToNull()
	}

	applyBatchHostGroupFieldSelection(&result, fieldSet)

	return result
}

func applyBatchHostGroupFieldSelection(bp *gcpgenserver.BatchHostGroupV1beta, fieldSet map[string]bool) {
	if fieldSet == nil {
		bp.Name.Reset()
		bp.ResourceId.Reset()
		bp.Description.Reset()
		bp.Created.Reset()
		bp.State.Reset()
		bp.Type.Reset()
		bp.Hosts.Reset()
		bp.OsType.Reset()
		return
	}

	if !fieldSet["name"] {
		bp.Name.Reset()
	}
	if !fieldSet["resourceId"] {
		bp.ResourceId.Reset()
	}
	if !fieldSet["description"] {
		bp.Description.Reset()
	}
	if !fieldSet["created"] {
		bp.Created.Reset()
	}
	if !fieldSet["state"] {
		bp.State.Reset()
	}
	if !fieldSet["type"] {
		bp.Type.Reset()
	}
	if !fieldSet["hosts"] {
		bp.Hosts.Reset()
	}
	if !fieldSet["osType"] {
		bp.OsType.Reset()
	}

	ensureRequestedHostGroupFieldsPresent(bp, fieldSet)
}

// ensureRequestedHostGroupFieldsPresent sets any requested field that is still absent to null.
// This guarantees all requested fields appear in the JSON response.
func ensureRequestedHostGroupFieldsPresent(bp *gcpgenserver.BatchHostGroupV1beta, fieldSet map[string]bool) {
	if fieldSet == nil {
		return
	}
	if fieldSet["name"] && !bp.Name.Set {
		bp.Name.SetToNull()
	}
	if fieldSet["resourceId"] && !bp.ResourceId.Set {
		bp.ResourceId.SetToNull()
	}
	if fieldSet["description"] && !bp.Description.Set {
		bp.Description.SetToNull()
	}
	if fieldSet["created"] && !bp.Created.Set {
		bp.Created.SetToNull()
	}
	if fieldSet["state"] && !bp.State.Set {
		bp.State = gcpgenserver.NewOptNilBatchHostGroupV1betaState(
			gcpgenserver.BatchHostGroupV1betaStateSTATEUNSPECIFIED)
	}
	if fieldSet["type"] && !bp.Type.Set {
		bp.Type = gcpgenserver.NewOptNilBatchHostGroupV1betaType(
			gcpgenserver.BatchHostGroupV1betaTypeUNSPECIFIED)
	}
	if fieldSet["hosts"] && !bp.Hosts.Set {
		bp.Hosts.SetToNull()
	}
	if fieldSet["osType"] && !bp.OsType.Set {
		bp.OsType = gcpgenserver.NewOptNilBatchHostGroupV1betaOsType(
			gcpgenserver.BatchHostGroupV1betaOsTypeOSTYPEUNSPECIFIED)
	}
}

// batchHostGroupState maps VCP lifecycle strings to API enum values. Unknown values must not be
// returned as arbitrary BatchHostGroupV1betaState strings — ogen's MarshalText rejects them and
// the encoder fails with an empty HTTP response (curl error 52).
func batchHostGroupState(s string) gcpgenserver.BatchHostGroupV1betaState {
	if s == "" {
		return gcpgenserver.BatchHostGroupV1betaStateSTATEUNSPECIFIED
	}
	v := gcpgenserver.BatchHostGroupV1betaState(s)
	switch v {
	case gcpgenserver.BatchHostGroupV1betaStateSTATEUNSPECIFIED,
		gcpgenserver.BatchHostGroupV1betaStateCREATING,
		gcpgenserver.BatchHostGroupV1betaStateREADY,
		gcpgenserver.BatchHostGroupV1betaStateUPDATING,
		gcpgenserver.BatchHostGroupV1betaStateDELETING,
		gcpgenserver.BatchHostGroupV1betaStateERROR,
		gcpgenserver.BatchHostGroupV1betaStateDISABLED:
		return v
	}
	switch s {
	case models.LifeCycleStateAvailable, models.LifeCycleStateInUse, models.LifeCycleStateCreated:
		return gcpgenserver.BatchHostGroupV1betaStateREADY
	case models.LifeCycleStateDeleted, models.LifeCycleStateDeleting:
		return gcpgenserver.BatchHostGroupV1betaStateDELETING
	case models.LifeCycleStateDisabled, models.LifeCycleStateDisabling:
		return gcpgenserver.BatchHostGroupV1betaStateDISABLED
	case models.LifeCycleStateUpdating, models.LifeCycleStateEnabling:
		return gcpgenserver.BatchHostGroupV1betaStateUPDATING
	case models.LifeCycleStateCreating, models.LifeCycleStatePreparing:
		return gcpgenserver.BatchHostGroupV1betaStateCREATING
	case models.LifeCycleStateError:
		return gcpgenserver.BatchHostGroupV1betaStateERROR
	default:
		return gcpgenserver.BatchHostGroupV1betaStateSTATEUNSPECIFIED
	}
}

func batchHostGroupType(t string) gcpgenserver.BatchHostGroupV1betaType {
	if t == "" {
		return gcpgenserver.BatchHostGroupV1betaTypeUNSPECIFIED
	}
	v := gcpgenserver.BatchHostGroupV1betaType(t)
	switch v {
	case gcpgenserver.BatchHostGroupV1betaTypeUNSPECIFIED,
		gcpgenserver.BatchHostGroupV1betaTypeISCSIINITIATOR:
		return v
	default:
		return gcpgenserver.BatchHostGroupV1betaTypeUNSPECIFIED
	}
}

// jsonSafeString replaces invalid UTF-8 so jx JSON encoding cannot panic (which after a 200
// WriteHeader yields an empty client response / curl 52).
func jsonSafeString(s string) string {
	return strings.ToValidUTF8(s, "\uFFFD")
}

func batchHostGroupOsType(os string) gcpgenserver.BatchHostGroupV1betaOsType {
	if os == "" {
		return gcpgenserver.BatchHostGroupV1betaOsTypeOSTYPEUNSPECIFIED
	}
	v := gcpgenserver.BatchHostGroupV1betaOsType(os)
	switch v {
	case gcpgenserver.BatchHostGroupV1betaOsTypeOSTYPEUNSPECIFIED,
		gcpgenserver.BatchHostGroupV1betaOsTypeLINUX,
		gcpgenserver.BatchHostGroupV1betaOsTypeWINDOWS,
		gcpgenserver.BatchHostGroupV1betaOsTypeESXI:
		return v
	default:
		return gcpgenserver.BatchHostGroupV1betaOsTypeOSTYPEUNSPECIFIED
	}
}
