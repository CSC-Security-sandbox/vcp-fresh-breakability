package api

import (
	"context"
	"encoding/json"
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

func (h Handler) V1betaGetMultipleSnapshots(ctx context.Context, req *gcpgenserver.SnapshotIdListV1beta, params gcpgenserver.V1betaGetMultipleSnapshotsParams) (gcpgenserver.V1betaGetMultipleSnapshotsRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId)
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

	if resp == nil || resp.Payload == nil {
		logger.Errorf("Received nil response CVP client for the V1betaGetMultipleSnapshots call: %v", err)
		return &gcpgenserver.V1betaGetMultipleSnapshotsInternalServerError{
			Code:    500,
			Message: "unknown error during the get multiple snapshots",
		}, nil
	}

	// Converting CVP model to gcpgenserver.SnapshotV1beta
	snapResponse := gcpgenserver.V1betaGetMultipleSnapshotsOK{
		Snapshots: []gcpgenserver.SnapshotV1beta{},
	}
	for _, snap := range resp.Payload.Snapshots {
		snapResponse.Snapshots = append(snapResponse.Snapshots, convertToSnapshotsV1Beta(snap))
	}
	return &snapResponse, nil
}

func (h Handler) V1betaCreateSnapshot(ctx context.Context, req *gcpgenserver.VolumeSnapshotCreateV1beta, params gcpgenserver.V1betaCreateSnapshotParams) (gcpgenserver.V1betaCreateSnapshotRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId)
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

func (h Handler) V1betaDescribeSnapshot(ctx context.Context, params gcpgenserver.V1betaDescribeSnapshotParams) (gcpgenserver.V1betaDescribeSnapshotRes, error) {
	logger := util.GetLogger(ctx)
	if params.SnapshotId == "" {
		logger.Error("Snapshot ID is required for DescribeSnapshot")
		return &gcpgenserver.V1betaDescribeSnapshotBadRequest{
			Code:    400,
			Message: "Snapshot ID is required",
		}, nil
	}
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
