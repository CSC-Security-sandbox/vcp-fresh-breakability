package api

import (
	"context"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/snapshots"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func (h Handler) V1betaGetMultipleSnapshots(ctx context.Context, req *gcpgenserver.SnapshotIDListV1beta, params gcpgenserver.V1betaGetMultipleSnapshotsParams) (gcpgenserver.V1betaGetMultipleSnapshotsRes, error) {
	logger := utils.GetLoggerFromContext(ctx)
	reqPrams := &snapshots.V1betaGetMultipleSnapshotsParams{
		LocationID:     params.LocationId,
		ProjectNumber:  params.ProjectNumber,
		VolumeID:       params.VolumeResourceId,
		XCorrelationID: &params.XCorrelationID.Value,
		Body: &models.SnapshotIDListV1beta{
			SnapshotUUIDs: req.SnapshotUUIDs,
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

func convertToSnapshotsV1Beta(snap *models.SnapshotV1beta) gcpgenserver.SnapshotV1beta {
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
	if snap.StorageClass != nil {
		snapshot.StorageClass = gcpgenserver.NewOptStorageClassV1beta(gcpgenserver.StorageClassV1beta(*snap.StorageClass))
	} else {
		snapshot.StorageClass = gcpgenserver.NewOptStorageClassV1beta(gcpgenserver.StorageClassV1betaHARDWARE)
	}
	return snapshot
}
