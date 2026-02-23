package api

import (
	"context"
	"encoding/json"
	"time"

	"github.com/go-faster/jx"
	"github.com/google/uuid"
	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/core-api/core-servergen"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	coremodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
)

// snapshotResponse represents the snapshot data in the Operation response
type snapshotResponse struct {
	ResourceId           string    `json:"resourceId"`
	SnapshotId           string    `json:"snapshotId,omitempty"`
	VolumeId             string    `json:"volumeId,omitempty"`
	VolumeResourceId     string    `json:"volumeResourceId,omitempty"`
	Created              time.Time `json:"created,omitempty"`
	SnapshotState        string    `json:"snapshotState,omitempty"`
	SnapshotStateDetails string    `json:"snapshotStateDetails,omitempty"`
	Description          string    `json:"description,omitempty"`
	UsedBytes            float64   `json:"usedBytes,omitempty"`
	StorageClass         string    `json:"storageClass,omitempty"`
	Zone                 string    `json:"zone,omitempty"`
}

// V1CreateSnapshot implements the snapshot creation endpoint
func (h Handler) V1CreateSnapshot(ctx context.Context, req *oasgenserver.VolumeSnapshotCreateV1, params oasgenserver.V1CreateSnapshotParams) (oasgenserver.V1CreateSnapshotRes, error) {
	logger := util.GetLogger(ctx)
	volumeId := params.VolumeId

	region, zone, parsingErr := parseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &oasgenserver.V1CreateSnapshotBadRequest{
			Code:    float64(parsingErr.Code),
			Message: parsingErr.Message,
		}, nil
	}

	param := &commonparams.CreateSnapshotParams{
		SnapshotBaseParams: commonparams.SnapshotBaseParams{
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
		if errors.IsUserInputValidationErr(err) || errors.IsNotFoundErr(err) || errors.IsBadRequestErr(err) {
			return &oasgenserver.V1CreateSnapshotBadRequest{
				Code:    400,
				Message: err.Error(),
			}, nil
		} else if errors.IsConflictErr(err) {
			return &oasgenserver.V1CreateSnapshotConflict{
				Code:    409,
				Message: err.Error(),
			}, nil
		}

		var customErr *vsaerrors.CustomError
		if vsaerrors.As(err, &customErr) {
			if hasCode, httpCode := customErr.GetHttpCode(); hasCode {
				message := customErr.GetMessage()
				if httpCode == 400 {
					return &oasgenserver.V1CreateSnapshotBadRequest{
						Code:    400,
						Message: message,
					}, nil
				} else if httpCode == 409 {
					return &oasgenserver.V1CreateSnapshotConflict{
						Code:    409,
						Message: message,
					}, nil
				}
			}
		}

		logger.Errorf("Failed to create snapshot: %v", err)
		return &oasgenserver.V1CreateSnapshotInternalServerError{Code: 500, Message: err.Error()}, nil
	}

	snapResp := convertModelToSnapshotResponse(snapshot)
	if zone != "" {
		snapResp.Zone = zone
	} else {
		snapResp.Zone = region
	}

	resp, err := encodeSnapshotResponse(snapResp)
	if err != nil {
		logger.Errorf("Failed to encode snapshot response: %v", err)
		return &oasgenserver.V1CreateSnapshotInternalServerError{Code: 500, Message: err.Error()}, err
	}

	operationUUID := jobUUID
	if operationUUID == "" {
		operationUUID = uuid.UUID{}.String()
	}
	operationID := "/v1beta/projects/" + params.ProjectNumber + "/locations/" + params.LocationId + "/operations/" + operationUUID
	if snapshot.LifeCycleState == coremodels.LifeCycleStateCreating {
		return &oasgenserver.OperationV1{
			Name:     oasgenserver.NewOptString(operationID),
			Response: resp,
			Done:     oasgenserver.NewOptBool(false),
		}, nil
	}
	return &oasgenserver.OperationV1{
		Name:     oasgenserver.NewOptString(operationID),
		Response: resp,
		Done:     oasgenserver.NewOptBool(true),
	}, nil
}

// convertModelToSnapshotResponse converts core model snapshot to snapshot response format
func convertModelToSnapshotResponse(snapshot *coremodels.Snapshot) *snapshotResponse {
	if snapshot == nil {
		return nil
	}
	return &snapshotResponse{
		ResourceId:           snapshot.Name,
		VolumeId:             snapshot.VolumeUUID,
		VolumeResourceId:     snapshot.VolumeName,
		Created:              snapshot.CreatedAt,
		SnapshotId:           snapshot.UUID,
		UsedBytes:            float64(snapshot.SizeInBytes),
		StorageClass:         string(oasgenserver.StorageClassV1SOFTWARE),
		SnapshotState:        snapshot.LifeCycleState,
		SnapshotStateDetails: snapshot.LifeCycleStateDetails,
		Description:          snapshot.Description,
	}
}

// encodeSnapshotResponse encodes a snapshotResponse struct to JSON (jx.Raw)
func encodeSnapshotResponse(snapResp *snapshotResponse) (jx.Raw, error) {
	data, err := json.Marshal(snapResp)
	if err != nil {
		return nil, err
	}
	return data, nil
}
