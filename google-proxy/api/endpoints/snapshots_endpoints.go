package api

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-faster/jx"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/snapshots"
	cvpmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	coremodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/helper"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	getMultipleSnapshotsFromCVP = _getMultipleSnapshotsFromCVP
)

func (h Handler) V1betaGetMultipleSnapshots(ctx context.Context, req *gcpgenserver.SnapshotIdListV1beta, params gcpgenserver.V1betaGetMultipleSnapshotsParams) (gcpgenserver.V1betaGetMultipleSnapshotsRes, error) {
	logger := util.GetLogger(ctx)
	_, _, parsingErr := parseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaGetMultipleSnapshotsBadRequest{
			Code:    parsingErr.Code,
			Message: parsingErr.Message,
		}, nil
	}

	if req.SnapshotUuids == nil {
		return &gcpgenserver.V1betaGetMultipleSnapshotsBadRequest{
			Code:    400,
			Message: "SnapshotUUIDs cannot be empty",
		}, nil
	}

	snapshotModelVCP, err := h.Orchestrator.GetMultipleSnapshots(ctx, params.VolumeId, params.ProjectNumber, req.SnapshotUuids)
	if err != nil {
		logger.Error("Failed to fetch snapshots", "error", err.Error())
		return &gcpgenserver.V1betaGetMultipleSnapshotsInternalServerError{Code: 500, Message: "Internal server error"}, err
	}

	snapshotsVCP := make([]gcpgenserver.SnapshotV1beta, 0)
	if len(snapshotModelVCP) > 0 { // If snapshots are found in VCP, return them else return from CVP
		for _, snapshot := range snapshotModelVCP {
			response := convertModelToVCPSnapshot(snapshot)
			snapshotsVCP = append(snapshotsVCP, *response)
		}
	}
	return getMultipleSnapshotsFromCVP(ctx, req, params, snapshotsVCP)
}

func _getMultipleSnapshotsFromCVP(ctx context.Context, req *gcpgenserver.SnapshotIdListV1beta, params gcpgenserver.V1betaGetMultipleSnapshotsParams, vcpSnapshots []gcpgenserver.SnapshotV1beta) (gcpgenserver.V1betaGetMultipleSnapshotsRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	reqPrams := &snapshots.V1betaGetMultipleSnapshotsParams{
		LocationID:     params.LocationId,
		ProjectNumber:  params.ProjectNumber,
		VolumeID:       params.VolumeId,
		XCorrelationID: &params.XCorrelationID.Value,
		Body: &cvpmodels.SnapshotIDListV1beta{
			SnapshotUUIDs: req.SnapshotUuids,
		},
	}
	jwtToken := utils.GetJWTTokenFromContext(ctx)
	cvpClient := createClient(logger, jwtToken)
	resp, err := cvpClient.Snapshots.V1betaGetMultipleSnapshots(reqPrams)
	if err != nil {
		logger.Errorf("Received error from CVP client for the V1betaGetMultipleSnapshots call: %v", err)
		switch e := err.(type) {
		case *snapshots.V1betaGetMultipleSnapshotsNotFound:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := nillable.GetFloat64(&e.Payload.Code, 0)
			return &gcpgenserver.V1betaGetMultipleSnapshotsNotFound{
				Code:    code,
				Message: msg,
			}, nil
		case *snapshots.V1betaGetMultipleSnapshotsBadRequest:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := nillable.GetFloat64(&e.Payload.Code, 0)
			return &gcpgenserver.V1betaGetMultipleSnapshotsBadRequest{
				Code:    code,
				Message: msg,
			}, nil
		case *snapshots.V1betaGetMultipleSnapshotsUnauthorized:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := nillable.GetFloat64(&e.Payload.Code, 0)
			return &gcpgenserver.V1betaGetMultipleSnapshotsUnauthorized{
				Code:    code,
				Message: msg,
			}, nil

		case *snapshots.V1betaGetMultipleSnapshotsForbidden:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := nillable.GetFloat64(&e.Payload.Code, 0)
			return &gcpgenserver.V1betaGetMultipleSnapshotsForbidden{
				Code:    code,
				Message: msg,
			}, nil

		case *snapshots.V1betaGetMultipleSnapshotsTooManyRequests:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := nillable.GetFloat64(&e.Payload.Code, 0)
			return &gcpgenserver.V1betaGetMultipleSnapshotsTooManyRequests{
				Code:    code,
				Message: msg,
			}, nil
		case *snapshots.V1betaGetMultipleSnapshotsDefault:
			return &gcpgenserver.V1betaGetMultipleSnapshotsInternalServerError{
				Code:    500,
				Message: err.Error(),
			}, nil
		}
	}

	// Converting CVP model to gcpgenserver.SnapshotV1beta
	snapResponse := gcpgenserver.V1betaGetMultipleSnapshotsOK{
		Snapshots: []gcpgenserver.SnapshotV1beta{},
	}

	if resp != nil && resp.Payload != nil && len(resp.Payload.Snapshots) != 0 {
		for _, snap := range resp.Payload.Snapshots {
			snapResponse.Snapshots = append(snapResponse.Snapshots, convertToSnapshotsV1Beta(snap))
		}
	}

	// Append VCP snapshots if any
	if len(vcpSnapshots) > 0 {
		snapResponse.Snapshots = append(snapResponse.Snapshots, vcpSnapshots...)
	}
	return &snapResponse, nil
}

func (h Handler) V1betaCreateSnapshot(ctx context.Context, req *gcpgenserver.VolumeSnapshotCreateV1beta, params gcpgenserver.V1betaCreateSnapshotParams) (gcpgenserver.V1betaCreateSnapshotRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	volumeId := params.VolumeId
	region, zone, parsingErr := parseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaCreateSnapshotBadRequest{
			Code:    400,
			Message: parsingErr.GetMessage(),
		}, nil
	}

	param := &common.CreateSnapshotParams{
		SnapshotBaseParams: common.SnapshotBaseParams{
			AccountName: params.ProjectNumber,
			VolumeID:    volumeId,
		},
		Name: req.ResourceId,
	}
	if req.Description.IsSet() {
		param.Description = req.GetDescription().Value
	} else {
		param.Description = ""
	}

	if req.IsAppConsistent.IsSet() {
		param.IsAppConsistent = req.IsAppConsistent.Value
	} else {
		param.IsAppConsistent = false
	}

	snapshot, jobUUID, err := h.Orchestrator.CreateSnapshot(ctx, param)

	if err != nil {
		if errors.IsUserInputValidationErr(err) || errors.IsNotFoundErr(err) {
			return &gcpgenserver.V1betaCreateSnapshotBadRequest{
				Code:    400,
				Message: err.Error(),
			}, nil
		} else if errors.IsConflictErr(err) {
			return &gcpgenserver.V1betaCreateSnapshotConflict{
				Code:    409,
				Message: err.Error(),
			}, nil
		}

		logger.Errorf("Failed to create snapshot: %v", err)
		return &gcpgenserver.V1betaCreateSnapshotInternalServerError{Code: 500, Message: err.Error()}, err
	}

	vcpSnapshot := convertModelToVCPSnapshot(snapshot)
	if zone != "" {
		vcpSnapshot.Zone = gcpgenserver.NewOptString(zone)
	} else {
		vcpSnapshot.Zone = gcpgenserver.NewOptString(region)
	}

	resp, err := encodeSnapshotV1(vcpSnapshot)
	if err != nil {
		logger.Errorf("Failed to encode snapshot response: %v", err)
		return &gcpgenserver.V1betaCreateSnapshotInternalServerError{Code: 500, Message: err.Error()}, err
	}

	operationID := "/v1beta/projects/" + params.ProjectNumber + "/locations/" + params.LocationId + "/operations/" + jobUUID
	if snapshot.LifeCycleState == coremodels.LifeCycleStateCreating {
		return &gcpgenserver.OperationV1beta{
			Name:     gcpgenserver.NewOptString(operationID),
			Response: resp,
			Done:     gcpgenserver.NewOptBool(false),
		}, nil
	}
	return &gcpgenserver.OperationV1beta{
		Name:     gcpgenserver.NewOptString(operationID),
		Response: resp,
		Done:     gcpgenserver.NewOptBool(true),
	}, nil
}

func (h Handler) V1betaUpdateSnapshot(ctx context.Context, req *gcpgenserver.VolumeSnapshotUpdateV1beta, params gcpgenserver.V1betaUpdateSnapshotParams) (gcpgenserver.V1betaUpdateSnapshotRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	if params.SnapshotId == "" {
		logger.Error("Snapshot ID is required for UpdateSnapshot")
		return &gcpgenserver.V1betaUpdateSnapshotBadRequest{
			Code:    400,
			Message: "Snapshot ID is required",
		}, nil
	}

	updateParams := &common.UpdateSnapshotParams{
		SnapshotBaseParams: common.SnapshotBaseParams{
			AccountName: params.ProjectNumber,
			VolumeID:    params.VolumeId,
		},
		SnapshotUUID: params.SnapshotId,
		Name:         req.GetResourceId(),
	}
	if req.Description.IsSet() {
		updateParams.Description = req.GetDescription().Value
	}

	snapshot, jobUUID, err := h.Orchestrator.UpdateSnapshot(ctx, updateParams)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			return &gcpgenserver.V1betaUpdateSnapshotNotFound{
				Code:    404,
				Message: "Snapshot not found",
			}, nil
		} else if errors.IsUserInputValidationErr(err) {
			return &gcpgenserver.V1betaUpdateSnapshotBadRequest{
				Code:    400,
				Message: err.Error(),
			}, nil
		} else if errors.IsConflictErr(err) {
			return &gcpgenserver.V1betaUpdateSnapshotConflict{
				Code:    409,
				Message: err.Error(),
			}, nil
		}
		logger.Errorf("Failed to update snapshot %s with error: %v", params.SnapshotId, err.Error())
		return &gcpgenserver.V1betaUpdateSnapshotInternalServerError{Code: 500, Message: err.Error()}, err
	}

	vcpSnapshot := convertModelToVCPSnapshot(snapshot)
	resp, err := encodeSnapshotV1(vcpSnapshot)
	if err != nil {
		logger.Errorf("Failed to encode snapshot response: %v", err)
		return &gcpgenserver.V1betaUpdateSnapshotInternalServerError{Code: 500, Message: err.Error()}, err
	}

	operationID := "/v1beta/projects/" + params.ProjectNumber + "/locations/" + params.LocationId + "/operations/" + jobUUID
	if snapshot.LifeCycleState == coremodels.LifeCycleStateUpdating {
		return &gcpgenserver.OperationV1beta{
			Name:     gcpgenserver.NewOptString(operationID),
			Response: resp,
			Done:     gcpgenserver.NewOptBool(false),
		}, nil
	}
	return &gcpgenserver.OperationV1beta{
		Name:     gcpgenserver.NewOptString(operationID),
		Response: resp,
		Done:     gcpgenserver.NewOptBool(true),
	}, nil
}

func (h Handler) V1betaDescribeSnapshot(ctx context.Context, params gcpgenserver.V1betaDescribeSnapshotParams) (gcpgenserver.V1betaDescribeSnapshotRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	describeParams := &common.GetSnapshotParams{
		SnapshotBaseParams: common.SnapshotBaseParams{
			AccountName: params.ProjectNumber,
			VolumeID:    params.VolumeId,
		},
		SnapshotUUID: params.SnapshotId,
	}
	snapshot, err := h.Orchestrator.GetSnapshot(ctx, describeParams)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			return &gcpgenserver.V1betaDescribeSnapshotNotFound{
				Code:    404,
				Message: err.Error(),
			}, nil
		} else if errors.IsUserInputValidationErr(err) {
			return &gcpgenserver.V1betaDescribeSnapshotBadRequest{
				Code:    400,
				Message: err.Error(),
			}, nil
		}
		logger.Errorf("Failed to get snapshot %s with error: %v", params.SnapshotId, err.Error())
		return &gcpgenserver.V1betaDescribeSnapshotInternalServerError{Code: 500, Message: "Internal server error"}, err
	}
	return convertModelToVCPSnapshot(snapshot), nil
}

// V1betaDeleteSnapshot handles the request to delete a snapshot.
func (h Handler) V1betaDeleteSnapshot(ctx context.Context, params gcpgenserver.V1betaDeleteSnapshotParams) (gcpgenserver.V1betaDeleteSnapshotRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	volumeId := params.VolumeId
	_, _, parsingErr := parseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaDeleteSnapshotBadRequest{
			Code:    400,
			Message: parsingErr.GetMessage(),
		}, nil
	}

	deleteSnapshotParams := &common.DeleteSnapshotParams{
		SnapshotBaseParams: common.SnapshotBaseParams{
			VolumeID:    volumeId,
			AccountName: params.ProjectNumber,
		},
		SnapshotID: params.SnapshotId,
	}

	// Delete the snapshot
	deleted, operationID, err := h.Orchestrator.DeleteSnapshot(ctx, deleteSnapshotParams)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			logger.Info("Snapshot not found", "uuid", params.SnapshotId)
			return &gcpgenserver.V1betaDeleteSnapshotBadRequest{
				Code:    404,
				Message: "Snapshot not found",
			}, nil
		} else if errors.IsUserInputValidationErr(err) {
			return &gcpgenserver.V1betaDeleteSnapshotBadRequest{
				Code:    400,
				Message: err.Error(),
			}, nil
		} else if errors.IsConflictErr(err) {
			return &gcpgenserver.V1betaDeleteSnapshotConflict{
				Code:    409,
				Message: err.Error(),
			}, nil
		}
		logger.Error("Failed to delete snapshot", "error", err.Error())
		return &gcpgenserver.V1betaDeleteSnapshotInternalServerError{
			Code:    500,
			Message: "Internal server error",
		}, err
	}
	resp, err := encodeSnapshotV1(convertModelToVCPSnapshot(deleted))
	if err != nil {
		return nil, err
	}
	if deleted.LifeCycleState == coremodels.LifeCycleStateDeleting {
		return &gcpgenserver.OperationV1beta{
			Name:     gcpgenserver.NewOptString(fmt.Sprintf("/v1beta/projects/%s/locations/%s/operations/%s", params.ProjectNumber, params.LocationId, operationID)),
			Response: resp,
			Done:     gcpgenserver.NewOptBool(false),
		}, nil
	}

	logger.Infof("Snapshot deleted successfully - SnapshotID: %s", params.SnapshotId)
	return &gcpgenserver.OperationV1beta{
		Name:     gcpgenserver.NewOptString(fmt.Sprintf("/v1beta/projects/%s/locations/%s/operations/%s", params.ProjectNumber, params.LocationId, operationID)),
		Response: resp,
		Done:     gcpgenserver.NewOptBool(true),
	}, nil
}

func (h Handler) V1betaListSnapshot(ctx context.Context, params gcpgenserver.V1betaListSnapshotParams) (gcpgenserver.V1betaListSnapshotRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	listParams := &common.ListSnapshotsParams{
		SnapshotBaseParams: common.SnapshotBaseParams{
			AccountName: params.ProjectNumber,
			VolumeID:    params.VolumeId,
		},
	}
	snapshotList, err := h.Orchestrator.ListSnapshots(ctx, listParams)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			return &gcpgenserver.V1betaListSnapshotNotFound{
				Code:    404,
				Message: err.Error(),
			}, nil
		}
		logger.Errorf("Failed to list snapshots for volume %s with error: %v", params.VolumeId, err.Error())
		return &gcpgenserver.V1betaListSnapshotInternalServerError{Code: 500, Message: "Internal server error"}, err
	}
	return &gcpgenserver.V1betaListSnapshotOK{
		Snapshots: convertToVCPSnapshotsV1Beta(snapshotList),
	}, nil
}

func convertToVCPSnapshotsV1Beta(snapshots []*coremodels.Snapshot) []gcpgenserver.SnapshotV1beta {
	snapshotsV1Beta := make([]gcpgenserver.SnapshotV1beta, len(snapshots))
	for i, snapshot := range snapshots {
		snapshotsV1Beta[i] = *convertModelToVCPSnapshot(snapshot)
	}
	return snapshotsV1Beta
}

func convertToSnapshotsV1Beta(snap *cvpmodels.SnapshotV1beta) gcpgenserver.SnapshotV1beta {
	snapshot := gcpgenserver.SnapshotV1beta{
		ResourceId:           nillable.GetString(snap.Description, ""),
		SnapshotId:           gcpgenserver.NewOptString(snap.SnapshotID),
		VolumeId:             gcpgenserver.NewOptString(snap.VolumeID),
		VolumeResourceId:     gcpgenserver.NewOptString(snap.VolumeResourceID),
		Created:              gcpgenserver.NewOptDateTime(time.Time(snap.Created)),
		UsedBytes:            gcpgenserver.NewOptFloat64(nillable.GetFloat64(snap.UsedBytes, 0.0)),
		SnapshotState:        gcpgenserver.NewOptSnapshotV1betaSnapshotState(gcpgenserver.SnapshotV1betaSnapshotState(snap.SnapshotState)),
		SnapshotStateDetails: gcpgenserver.NewOptString(snap.SnapshotStateDetails),
		Zone:                 gcpgenserver.NewOptString(snap.Zone),
		Description:          gcpgenserver.NewOptString(nillable.GetString(snap.Description, "")),
	}
	if snap.StorageClass == "" {
		snapshot.StorageClass = gcpgenserver.NewOptStorageClassV1beta(gcpgenserver.StorageClassV1beta(snap.StorageClass))
	} else {
		snapshot.StorageClass = gcpgenserver.NewOptStorageClassV1beta(gcpgenserver.StorageClassV1betaHARDWARE)
	}
	return snapshot
}

// encodeVolumeV1 encodes a PoolV1 struct to JSON.
func encodeSnapshotV1(snapShotV1beta *gcpgenserver.SnapshotV1beta) (jx.Raw, error) {
	data, err := json.Marshal(snapShotV1beta)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func convertModelToVCPSnapshot(snapshot *coremodels.Snapshot) *gcpgenserver.SnapshotV1beta {
	if snapshot == nil {
		return nil
	}
	return &gcpgenserver.SnapshotV1beta{
		ResourceId:           snapshot.Name,
		SnapshotId:           gcpgenserver.NewOptString(snapshot.UUID),
		VolumeId:             gcpgenserver.NewOptString(snapshot.VolumeUUID),
		VolumeResourceId:     gcpgenserver.NewOptString(snapshot.VolumeName),
		Created:              gcpgenserver.NewOptDateTime(snapshot.CreatedAt),
		SnapshotState:        gcpgenserver.NewOptSnapshotV1betaSnapshotState(gcpgenserver.SnapshotV1betaSnapshotState(snapshot.LifeCycleState)),
		SnapshotStateDetails: gcpgenserver.NewOptString(snapshot.LifeCycleStateDetails),
		Description:          gcpgenserver.NewOptString(snapshot.Description),
	}
}
