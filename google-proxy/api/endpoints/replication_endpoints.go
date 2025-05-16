package api

import (
	"context"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/replications"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

func (h Handler) V1betaCreateReplication(ctx context.Context, req *gcpgenserver.ReplicationCreateV1beta, params gcpgenserver.V1betaCreateReplicationParams) (gcpgenserver.V1betaCreateReplicationRes, error) {
	// TODO implement me
	return &gcpgenserver.OperationV1beta{}, nil
}

func (h Handler) V1betaGetMultipleReplications(ctx context.Context, req *gcpgenserver.ReplicationURIListV1beta, params gcpgenserver.V1betaGetMultipleReplicationsParams) (gcpgenserver.V1betaGetMultipleReplicationsRes, error) {
	logger := util.GetLogger(ctx)
	body := &models.ReplicationURIListV1beta{
		ReplicationUris: req.ReplicationUris,
	}
	reqParams := &replications.V1betaGetMultipleReplicationsParams{
		LocationID:       params.LocationId,
		ProjectNumber:    params.ProjectNumber,
		VolumeResourceID: params.VolumeResourceId,
		XCorrelationID:   &params.XCorrelationID.Value,
		Body:             body,
	}
	jwtToken := utils.GetJWTTokenFromContext(ctx)
	cvpClient := createClient(logger, jwtToken)
	resp, err := cvpClient.Replications.V1betaGetMultipleReplications(reqParams)
	if err != nil {
		switch e := err.(type) {
		case *replications.V1betaGetMultipleReplicationsBadRequest:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaGetMultipleReplicationsBadRequest{
				Code:    code,
				Message: msg,
			}, nil
		case *replications.V1betaGetMultipleReplicationsUnauthorized:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaGetMultipleReplicationsUnauthorized{
				Code:    code,
				Message: msg,
			}, nil
		case *replications.V1betaGetMultipleReplicationsForbidden:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaGetMultipleReplicationsForbidden{
				Code:    code,
				Message: msg,
			}, nil
		case *replications.V1betaGetMultipleReplicationsNotFound:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaGetMultipleReplicationsNotFound{
				Code:    code,
				Message: msg,
			}, nil
		case *replications.V1betaGetMultipleReplicationsTooManyRequests:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaGetMultipleReplicationsTooManyRequests{
				Code:    code,
				Message: msg,
			}, nil
		case *replications.V1betaGetMultipleReplicationsDefault:
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			msg := nillable.GetString(&e.Payload.Message, "")
			return &gcpgenserver.V1betaGetMultipleReplicationsInternalServerError{
				Code:    code,
				Message: msg,
			}, nil
		}
	}
	if resp == nil || resp.Payload == nil {
		return &gcpgenserver.V1betaGetMultipleReplicationsInternalServerError{
			Code:    500,
			Message: "unknown error during the get multiple replications",
		}, nil
	}

	replicationResp := gcpgenserver.V1betaGetMultipleReplicationsOK{
		Replications: []gcpgenserver.ReplicationV1beta{},
	}

	for _, rep := range resp.Payload.Replications {
		replicationResp.Replications = append(replicationResp.Replications, convertToReplicationV1Beta(rep))
	}

	return &replicationResp, nil
}

func convertToReplicationV1Beta(replication *models.ReplicationV1beta) gcpgenserver.ReplicationV1beta {
	replicationResp := gcpgenserver.ReplicationV1beta{
		ReplicationId:       gcpgenserver.NewOptString(replication.ReplicationID),
		ResourceId:          gcpgenserver.NewOptString(replication.ResourceID),
		Created:             gcpgenserver.NewOptDateTime(time.Time(replication.Created)),
		State:               gcpgenserver.NewOptReplicationV1betaState(gcpgenserver.ReplicationV1betaState(replication.State)),
		StateDetails:        gcpgenserver.NewOptString(replication.StateDetails),
		Labels:              gcpgenserver.NewOptReplicationV1betaLabels(replication.Labels),
		MirrorState:         gcpgenserver.NewOptReplicationV1betaMirrorState(gcpgenserver.ReplicationV1betaMirrorState(replication.MirrorState)),
		ReplicationSchedule: gcpgenserver.NewOptReplicationV1betaReplicationSchedule(gcpgenserver.ReplicationV1betaReplicationSchedule(replication.ReplicationSchedule)),
		Role:                gcpgenserver.NewOptReplicationV1betaRole(gcpgenserver.ReplicationV1betaRole(replication.Role)),
		StateDetailsCode:    gcpgenserver.NewOptInt32(replication.StateDetailsCode),
	}
	if replication.ClusterLocation != nil {
		replicationResp.ClusterLocation = gcpgenserver.NewOptString(*replication.ClusterLocation)
	}
	if replication.Description != nil {
		replicationResp.Description = gcpgenserver.NewOptString(*replication.Description)
	}
	if replication.Destination != nil {
		replicationResp.Destination = gcpgenserver.NewOptReplicationVolumeInformationV1beta(gcpgenserver.ReplicationVolumeInformationV1beta{
			VolumeName: gcpgenserver.NewOptString(replication.Destination.VolumeName),
			VolumeId:   gcpgenserver.NewOptString(replication.Destination.VolumeID),
		})
	}
	if replication.Healthy != nil {
		replicationResp.Healthy = gcpgenserver.NewOptBool(*replication.Healthy)
	}
	if replication.Source != nil {
		replicationResp.Source = gcpgenserver.NewOptReplicationVolumeInformationV1beta(gcpgenserver.ReplicationVolumeInformationV1beta{
			VolumeName: gcpgenserver.NewOptString(replication.Source.VolumeName),
			VolumeId:   gcpgenserver.NewOptString(replication.Source.VolumeID),
		})
	}
	if replication.TransferStats != nil {
		replicationResp.TransferStats = gcpgenserver.NewOptTransferStatsV1beta(gcpgenserver.TransferStatsV1beta{
			TotalTransferBytes:    gcpgenserver.NewOptFloat64(replication.TransferStats.TotalTransferBytes),
			TotalTransferTimeSecs: gcpgenserver.NewOptFloat64(replication.TransferStats.TotalTransferTimeSecs),
			LastTransferSize:      gcpgenserver.NewOptFloat64(replication.TransferStats.LastTransferSize),
			LastTransferError:     gcpgenserver.NewOptString(replication.TransferStats.LastTransferError),
			LastTransferDuration:  gcpgenserver.NewOptFloat64(replication.TransferStats.LastTransferDuration),
			TotalProgress:         gcpgenserver.NewOptFloat64(replication.TransferStats.TotalProgress),
			LagTime:               gcpgenserver.NewOptFloat64(replication.TransferStats.LagTime),
		})
	}
	if replication.HybridPeeringDetails != nil {
		replicationResp.HybridPeeringDetails = gcpgenserver.NewOptHybridPeeringV1beta(gcpgenserver.HybridPeeringV1beta{
			SubnetIp:          gcpgenserver.NewOptString(replication.HybridPeeringDetails.SubnetIP),
			Command:           gcpgenserver.NewOptString(replication.HybridPeeringDetails.Command),
			Passphrase:        gcpgenserver.NewOptString(*replication.HybridPeeringDetails.Passphrase),
			PeerVolumeName:    gcpgenserver.NewOptString(*replication.HybridPeeringDetails.PeerVolumeName),
			PeerClusterName:   gcpgenserver.NewOptString(*replication.HybridPeeringDetails.PeerClusterName),
			PeerSvmName:       gcpgenserver.NewOptString(*replication.HybridPeeringDetails.PeerSvmName),
			CommandExpiryTime: gcpgenserver.NewOptDateTime(time.Time(*replication.HybridPeeringDetails.CommandExpiryTime)),
		})
	}
	if replication.HybridReplicationUserCommands != nil {
		replicationResp.HybridReplicationUserCommands = gcpgenserver.NewOptHybridReplicationUserCommandsV1beta(gcpgenserver.HybridReplicationUserCommandsV1beta{
			Commands: replication.HybridReplicationUserCommands.Commands,
		})
	}
	if replication.HybridReplicationType != nil {
		replicationResp.HybridReplicationType = gcpgenserver.NewOptReplicationV1betaHybridReplicationType(gcpgenserver.ReplicationV1betaHybridReplicationType(*replication.HybridReplicationType))
	}

	return replicationResp
}
