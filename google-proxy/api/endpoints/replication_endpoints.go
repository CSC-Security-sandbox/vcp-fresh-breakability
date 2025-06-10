package api

import (
	"context"
	"encoding/json"
	"time"

	"github.com/go-faster/jx"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/replications"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	models2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/helper"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	convertModelToVCPVolumeReplication = _convertModelToVCPVolumeReplication
)

func (h Handler) V1betaCreateReplication(ctx context.Context, req *gcpgenserver.ReplicationCreateV1beta, params gcpgenserver.V1betaCreateReplicationParams) (gcpgenserver.V1betaCreateReplicationRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId)
	region, _, parsingErr := parseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaCreateReplicationBadRequest{
			Code:    parsingErr.Code,
			Message: parsingErr.Message,
		}, nil
	}

	replicationParams := prepareCreateVolumeReplicationParams(req, params, region)

	volumeRep, jobUUID, err := h.Orchestrator.CreateVolumeReplication(ctx, replicationParams)
	if err != nil {
		if errors.IsUserInputValidationErr(err) || errors.IsNotFoundErr(err) {
			return &gcpgenserver.V1betaCreateReplicationBadRequest{
				Code:    400,
				Message: err.Error(),
			}, nil
		}

		logger.Error("Failed to create volume", "error", err.Error())
		return &gcpgenserver.V1betaCreateReplicationInternalServerError{Code: 500, Message: err.Error()}, err
	}

	resp, err := encodeVolumeReplicationV1(convertModelToVCPVolumeReplication(volumeRep))
	if err != nil {
		return nil, err
	}

	operationID := "/v1beta/projects/" + params.ProjectNumber + "/locations/" + params.LocationId + "/operations/" + jobUUID
	if volumeRep.State == models2.LifeCycleStateCreating {
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

// encodeVolumeReplicationV1 encodes a Replication struct to JSON.
func encodeVolumeReplicationV1(replicationV1beta *gcpgenserver.ReplicationV1beta) (jx.Raw, error) {
	data, err := json.Marshal(replicationV1beta)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (h Handler) V1betaGetReplicationCount(ctx context.Context, params gcpgenserver.V1betaGetReplicationCountParams) (gcpgenserver.V1betaGetReplicationCountRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId)
	count, err := h.Orchestrator.GetReplicationCount(ctx, params.ProjectNumber)
	if err != nil {
		logger.Error("Error while getting replication count", "error", err.Error())
		return nil, err
	}
	return &gcpgenserver.V1betaGetReplicationCountOK{ReplicationCount: int(count)}, nil
}

func (h Handler) V1betaGetMultipleReplications(ctx context.Context, req *gcpgenserver.ReplicationURIListV1beta, params gcpgenserver.V1betaGetMultipleReplicationsParams) (gcpgenserver.V1betaGetMultipleReplicationsRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId)
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

func prepareCreateVolumeReplicationParams(req *gcpgenserver.ReplicationCreateV1beta, params gcpgenserver.V1betaCreateReplicationParams, region string) *common.CreateVolumeReplicationParams {
	replication := common.CreateVolumeReplicationParams{
		AccountName:      params.ProjectNumber,
		Region:           region,
		Name:             req.ResourceId,
		SourceVolumeName: params.VolumeResourceId,
		CorrelationId:    params.XCorrelationID.Value,
	}

	replication.Body = req
	if req.Description.IsSet() {
		replication.Description, _ = req.Description.Get()
	}

	return &replication
}

func _convertModelToVCPVolumeReplication(volumeReplication *models2.VolumeReplication) *gcpgenserver.ReplicationV1beta {
	return &gcpgenserver.ReplicationV1beta{
		ReplicationId:       gcpgenserver.NewOptString(volumeReplication.UUID),
		ResourceId:          gcpgenserver.NewOptString(volumeReplication.Name),
		Description:         gcpgenserver.NewOptString(volumeReplication.Description),
		State:               gcpgenserver.NewOptReplicationV1betaState(gcpgenserver.ReplicationV1betaState(volumeReplication.State)),
		StateDetails:        gcpgenserver.NewOptString(volumeReplication.StateDetails),
		Role:                gcpgenserver.NewOptReplicationV1betaRole(convertToRole(volumeReplication.ReplicationAttributes.EndpointType)),
		ReplicationSchedule: gcpgenserver.NewOptReplicationV1betaReplicationSchedule(gcpgenserver.ReplicationV1betaReplicationSchedule(volumeReplication.ReplicationAttributes.ReplicationSchedule)),
		Created:             gcpgenserver.NewOptDateTime(time.Time(volumeReplication.CreatedAt)),
	}
}

func convertToRole(endpointType string) gcpgenserver.ReplicationV1betaRole {
	switch endpointType {
	case "src":
		return gcpgenserver.ReplicationV1betaRoleSOURCE
	case "dst":
		return gcpgenserver.ReplicationV1betaRoleDESTINATION
	default:
		return gcpgenserver.ReplicationV1betaRoleREPLICATIONROLEUNSPECIFIED
	}
}
