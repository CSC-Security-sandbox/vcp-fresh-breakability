package api

import (
	"context"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/helper"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

func (h Handler) V1betaInternalAcceptClusterPeer(ctx context.Context, req *gcpgenserver.ClusterPeerV1, params gcpgenserver.V1betaInternalAcceptClusterPeerParams) (gcpgenserver.V1betaInternalAcceptClusterPeerRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	_, _, parsingErr := parseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaInternalAcceptClusterPeerBadRequest{
			Code:    parsingErr.Code,
			Message: parsingErr.Message,
		}, nil
	}
	param := &common.ClusterPeerParams{
		PeerAddresses: req.PeerAddresses,
		PeerName:      req.PeerClusterName,
		ExpiryTime:    &req.ExpiryTime.Value,
		Passphrase:    &req.Passphrase,
		AccountName:   params.ProjectNumber,
	}
	created, job, err := h.Orchestrator.AcceptClusterPeer(ctx, param, req.PoolUUID)
	if err != nil {
		logger.Error("Failed to Accept ClusterPeer", "error", err.Error())
		return &gcpgenserver.V1betaInternalAcceptClusterPeerInternalServerError{}, err
	}
	return convertToClusterPeerV1(created, job, req.PoolUUID), nil
}

func convertToClusterPeerV1(clusterPeer *common.ClusterPeerParams, job *datamodel.Job, poolUUID string) *gcpgenserver.ClusterPeerV1 {
	var clusterPeerV1 = gcpgenserver.ClusterPeerV1{
		PeerAddresses:   clusterPeer.PeerAddresses,
		PeerClusterName: clusterPeer.PeerName,
		PoolUUID:        poolUUID,
		Jobs: []gcpgenserver.JobV1beta{
			{
				JobId:    gcpgenserver.NewOptString(job.UUID),
				Created:  gcpgenserver.NewOptDateTime(time.Time(job.CreatedAt)),
				WorkerId: gcpgenserver.NewOptString(job.WorkflowID),
			},
		},
	}
	return &clusterPeerV1
}
