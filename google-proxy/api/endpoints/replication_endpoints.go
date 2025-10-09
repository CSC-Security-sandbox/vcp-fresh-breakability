package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-faster/jx"
	"github.com/google/uuid"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/replications"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	models2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/helper"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	convertModelToVCPVolumeReplication                     = _convertModelToVCPVolumeReplication
	validateReplicationURIList                             = _validateReplicationURIList
	convertVcpReplicationModelToVCPVolumeReplicationV1beta = _convertVcpReplicationModelToVCPVolumeReplicationV1beta
	crrEnabled                                             = env.GetBool("CRR_ENABLED", true)
)

func (h Handler) V1betaCreateReplication(ctx context.Context, req *gcpgenserver.ReplicationCreateV1beta, params gcpgenserver.V1betaCreateReplicationParams) (gcpgenserver.V1betaCreateReplicationRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	if !crrEnabled {
		return &gcpgenserver.V1betaCreateReplicationBadRequest{
			Code:    400,
			Message: "CRR is not enabled",
		}, nil
	}
	region, zone, parsingErr := parseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaCreateReplicationBadRequest{
			Code:    parsingErr.Code,
			Message: parsingErr.Message,
		}, nil
	}

	replicationParams := prepareCreateVolumeReplicationParams(req, params, region, zone)

	volumeRep, jobUUID, err := h.Orchestrator.CreateVolumeReplication(ctx, replicationParams)
	if err != nil {
		if errors.IsUserInputValidationErr(err) || errors.IsNotFoundErr(err) {
			return &gcpgenserver.V1betaCreateReplicationBadRequest{
				Code:    400,
				Message: err.Error(),
			}, nil
		}

		vsaerror, ok := err.(*vsaerrors.CustomError)
		if ok {
			if *vsaerror.HttpCode == 400 || *vsaerror.HttpCode == 404 || *vsaerror.HttpCode == 409 {
				return &gcpgenserver.V1betaCreateReplicationBadRequest{
					Code:    400,
					Message: vsaerror.Message,
				}, nil
			}
		}

		logger.Error("Failed to create volume replication", "error", err.Error())
		return &gcpgenserver.V1betaCreateReplicationInternalServerError{Code: 500, Message: err.Error()}, nil
	}

	resp, err := encodeVolumeReplicationV1(convertModelToVCPVolumeReplication(volumeRep))
	if err != nil {
		return &gcpgenserver.V1betaCreateReplicationInternalServerError{Code: 500, Message: err.Error()}, nil
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
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	count, err := h.Orchestrator.GetReplicationCount(ctx, params.ProjectNumber)
	if err != nil {
		logger.Error("Error while getting replication count", "error", err.Error())
		return nil, err
	}
	return &gcpgenserver.V1betaGetReplicationCountOK{ReplicationCount: int(count)}, nil
}

func (h Handler) V1betaGetMultipleReplications(ctx context.Context, req *gcpgenserver.ReplicationURIListV1beta, params gcpgenserver.V1betaGetMultipleReplicationsParams) (gcpgenserver.V1betaGetMultipleReplicationsRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)

	// Check if replication exists in VCP before making a CVP API call
	replicationURIs := req.GetReplicationUris()
	if len(replicationURIs) == 0 {
		logger.Error("No replication URIs provided")
		return &gcpgenserver.V1betaGetMultipleReplicationsBadRequest{
			Code:    400,
			Message: "Replication URIs cannot be empty",
		}, nil
	}

	getReplicationParams := common.GetMultipleReplicationsParams{
		ReplicationURIs:  req.GetReplicationUris(),
		AccountName:      params.ProjectNumber,
		LocationId:       params.LocationId,
		XCorrelationID:   params.XCorrelationID.Value,
		VolumeResourceId: params.VolumeResourceId,
	}

	err := validateReplicationURIList(getReplicationParams)
	if err != nil {
		logger.Errorf("Error validating replication URIs: %v", err)
		return &gcpgenserver.V1betaGetMultipleReplicationsBadRequest{
			Code:    400,
			Message: err.Error(),
		}, nil
	}

	vcpReplications, err := h.Orchestrator.GetMultipleReplications(ctx, getReplicationParams)
	if err != nil {
		logger.Errorf("Error getting multiple replications: %v", err)
		return &gcpgenserver.V1betaGetMultipleReplicationsInternalServerError{
			Code:    500,
			Message: "Error retrieving replications from VCP",
		}, nil
	}

	if len(vcpReplications) == len(replicationURIs) {
		logger.Infof("Returning %d replications found in VCP", len(vcpReplications))
		reps := make([]gcpgenserver.ReplicationV1beta, len(vcpReplications))
		copy(reps, vcpReplications)
		return &gcpgenserver.V1betaGetMultipleReplicationsOK{Replications: reps}, nil
	}

	// If not all replications are found in VCP, proceed with CVP API call
	body := &models.ReplicationURIListV1beta{
		ReplicationUris: replicationURIs,
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
	// append the replications found in VCP
	if len(vcpReplications) > 0 {
		replicationResp.Replications = append(replicationResp.Replications, vcpReplications...)
	}

	return &replicationResp, nil
}

func _validateReplicationURIList(param common.GetMultipleReplicationsParams) error {
	for _, uri := range param.ReplicationURIs {
		err := utils.ValidateCcfeReplicationUri(uri)
		if err != nil {
			return err
		}

		// projects/netapp-prod-prs-14/locations/northAmerica-northeast1/volumes/vol-1/replications/replication-1
		uriProjectsId := strings.Split(uri, "/")[1]
		if uriProjectsId != param.AccountName {
			return fmt.Errorf("replicationURIs projectNumber in body does not match projectNumber in parameter")
		}

		uriResourceId := strings.Split(uri, "/")[5]
		if uriResourceId != param.VolumeResourceId && param.VolumeResourceId != "-" {
			return fmt.Errorf("replicationURIs volumeId in body does not match volumeResourceId in parameter")
		}

		uriLocationid := strings.Split(uri, "/")[3]
		if uriLocationid != param.LocationId {
			return fmt.Errorf("replicationURIs locationId in body does not match locationId in parameter")
		}
	}
	return nil
}

func convertToReplicationV1Beta(replication *models.ReplicationV1beta) gcpgenserver.ReplicationV1beta {
	replicationResp := gcpgenserver.ReplicationV1beta{
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
	if replication.ReplicationID != "" {
		replicationResp.ReplicationId = gcpgenserver.NewOptString(replication.ReplicationID)
	}
	if replication.ClusterLocation != nil {
		replicationResp.ClusterLocation = gcpgenserver.NewOptString(*replication.ClusterLocation)
	}
	if replication.Description != nil {
		replicationResp.Description = gcpgenserver.NewOptString(*replication.Description)
	}
	if replication.Destination != nil {
		conv := convertVolumeInfoToReplicationVolumeInformationV1beta(replication.Destination)
		if conv != nil {
			replicationResp.Destination = gcpgenserver.NewOptReplicationVolumeInformationV1beta(*conv)
		}
	}
	if replication.Healthy != nil {
		replicationResp.Healthy = gcpgenserver.NewOptBool(*replication.Healthy)
	}
	if replication.Source != nil {
		conv := convertVolumeInfoToReplicationVolumeInformationV1beta(replication.Source)
		if conv != nil {
			replicationResp.Source = gcpgenserver.NewOptReplicationVolumeInformationV1beta(*conv)
		}
	}
	var lastTransferTime time.Time
	if replication.TransferStats != nil && replication.TransferStats.LastTransferEndTime != nil {
		lastTransferTime = time.Time(*replication.TransferStats.LastTransferEndTime)
	}
	if replication.TransferStats != nil {
		replicationResp.TransferStats = gcpgenserver.NewOptTransferStatsV1beta(gcpgenserver.TransferStatsV1beta{
			TotalTransferBytes:    gcpgenserver.NewOptFloat64(replication.TransferStats.TotalTransferBytes),
			TotalTransferTimeSecs: gcpgenserver.NewOptFloat64(replication.TransferStats.TotalTransferTimeSecs),
			LastTransferSize:      gcpgenserver.NewOptFloat64(replication.TransferStats.LastTransferSize),
			LastTransferError:     gcpgenserver.NewOptString(replication.TransferStats.LastTransferError),
			LastTransferEndTime:   gcpgenserver.NewOptDateTime(lastTransferTime),
			LastTransferDuration:  gcpgenserver.NewOptFloat64(replication.TransferStats.LastTransferDuration),
			TotalProgress:         gcpgenserver.NewOptFloat64(replication.TransferStats.TotalProgress),
			LagTime:               gcpgenserver.NewOptFloat64(replication.TransferStats.LagTime),
		})
	}
	if replication.HybridPeeringDetails != nil {
		replicationResp.HybridPeeringDetails = gcpgenserver.NewOptHybridPeeringV1beta(gcpgenserver.HybridPeeringV1beta{
			SubnetIp:        gcpgenserver.NewOptString(replication.HybridPeeringDetails.SubnetIP),
			Command:         gcpgenserver.NewOptString(replication.HybridPeeringDetails.Command),
			Passphrase:      gcpgenserver.NewOptString(nillable.GetString(replication.HybridPeeringDetails.Passphrase, "")),
			PeerVolumeName:  gcpgenserver.NewOptString(nillable.GetString(replication.HybridPeeringDetails.PeerVolumeName, "")),
			PeerClusterName: gcpgenserver.NewOptString(nillable.GetString(replication.HybridPeeringDetails.PeerClusterName, "")),
			PeerSvmName:     gcpgenserver.NewOptString(nillable.GetString(replication.HybridPeeringDetails.PeerSvmName, "")),
		})
		if replication.HybridPeeringDetails.CommandExpiryTime != nil {
			replicationResp.HybridPeeringDetails.Value.CommandExpiryTime = gcpgenserver.NewOptDateTime(time.Time(*replication.HybridPeeringDetails.CommandExpiryTime))
		}
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

func convertVolumeInfoToReplicationVolumeInformationV1beta(in *models.ReplicationVolumeInformationV1beta) *gcpgenserver.ReplicationVolumeInformationV1beta {
	if in == nil {
		return nil
	}
	emptyUUID := uuid.UUID{}
	if nillable.IsNilOrEmpty(&in.VolumeName) || nillable.IsNilOrEmpty(&in.VolumeID) || in.VolumeID == emptyUUID.String() {
		return nil
	}
	return &gcpgenserver.ReplicationVolumeInformationV1beta{
		VolumeName: gcpgenserver.NewOptString(in.VolumeName),
		VolumeId:   gcpgenserver.NewOptString(in.VolumeID),
	}
}

func prepareCreateVolumeReplicationParams(req *gcpgenserver.ReplicationCreateV1beta, params gcpgenserver.V1betaCreateReplicationParams, region, zone string) *common.CreateVolumeReplicationParams {
	replication := common.CreateVolumeReplicationParams{
		AccountName:      params.ProjectNumber,
		Region:           region,
		LocationId:       region,
		Name:             req.ResourceId,
		SourceVolumeName: params.VolumeResourceId,
		CorrelationId:    params.XCorrelationID.Value,
	}

	if zone != "" {
		replication.LocationId = zone
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

func (h Handler) V1betaResumeReplication(ctx context.Context, params gcpgenserver.V1betaResumeReplicationParams) (gcpgenserver.V1betaResumeReplicationRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	if !crrEnabled {
		return &gcpgenserver.V1betaResumeReplicationBadRequest{
			Code:    400,
			Message: "CRR is not enabled",
		}, nil
	}
	region, zone, parsingErr := parseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaResumeReplicationBadRequest{
			Code:    parsingErr.Code,
			Message: parsingErr.Message,
		}, nil
	}

	resumeReplicationParams := &common.ResumeReplicationParams{
		AccountName:           params.ProjectNumber,
		Region:                region,
		Zone:                  zone,
		CorrelationId:         params.XCorrelationID.Value,
		VolumeResourceId:      params.VolumeResourceId,
		ReplicationResourceId: params.ReplicationResourceId,
		Force:                 false,
	}

	volumeRep, jobUUID, err := h.Orchestrator.ResumeReplication(ctx, resumeReplicationParams)
	if err != nil {
		if errors.IsUserInputValidationErr(err) || errors.IsNotFoundErr(err) {
			return &gcpgenserver.V1betaResumeReplicationBadRequest{
				Code:    400,
				Message: err.Error(),
			}, nil
		}

		vsaerror, ok := err.(*vsaerrors.CustomError)
		if ok {
			if *vsaerror.HttpCode == 400 || *vsaerror.HttpCode == 404 || *vsaerror.HttpCode == 409 {
				return &gcpgenserver.V1betaResumeReplicationBadRequest{
					Code:    400,
					Message: vsaerror.Message,
				}, nil
			}
		}

		logger.Error("Failed to resume replication", "error", err.Error())
		return &gcpgenserver.V1betaResumeReplicationInternalServerError{
			Code:    500,
			Message: err.Error(),
		}, nil
	}

	resp, err := encodeVolumeReplicationV1(convertVcpReplicationModelToVCPVolumeReplicationV1beta(volumeRep))
	if err != nil {
		return nil, err
	}

	operationID := "/v1beta/projects/" + params.ProjectNumber + "/locations/" + params.LocationId + "/operations/" + jobUUID
	if volumeRep.State == models2.LifeCycleStateUpdating {
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

func (h Handler) V1betaUpdateReplication(ctx context.Context, req *gcpgenserver.ReplicationUpdateV1beta, params gcpgenserver.V1betaUpdateReplicationParams) (gcpgenserver.V1betaUpdateReplicationRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	if !crrEnabled {
		return &gcpgenserver.V1betaUpdateReplicationBadRequest{
			Code:    400,
			Message: "CRR is not enabled",
		}, nil
	}
	region, zone, parsingErr := parseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaUpdateReplicationBadRequest{
			Code:    parsingErr.Code,
			Message: parsingErr.Message,
		}, nil
	}

	updateReplicationParams := &common.UpdateReplicationParams{
		AccountName:           params.ProjectNumber,
		Region:                region,
		Zone:                  zone,
		CorrelationId:         params.XCorrelationID.Value,
		VolumeResourceId:      params.VolumeResourceId,
		ReplicationResourceId: params.ReplicationResourceId,
	}

	if req.Description.IsSet() {
		updateReplicationParams.Description = nillable.ToPointer(req.Description.Value)
	}
	if req.ReplicationSchedule.IsSet() {
		updateReplicationParams.ReplicationSchedule = nillable.ToPointer(string(req.ReplicationSchedule.Value))
	}

	volumeRep, jobUUID, err := h.Orchestrator.UpdateReplication(ctx, updateReplicationParams)
	if err != nil {
		if errors.IsUserInputValidationErr(err) {
			return &gcpgenserver.V1betaUpdateReplicationBadRequest{
				Code:    http.StatusBadRequest,
				Message: err.Error(),
			}, nil
		} else if errors.IsNotFoundErr(err) {
			return &gcpgenserver.V1betaUpdateReplicationNotFound{
				Code:    http.StatusNotFound,
				Message: err.Error(),
			}, nil
		}

		vsaerror, ok := err.(*vsaerrors.CustomError)
		if ok {
			if *vsaerror.HttpCode == 400 || *vsaerror.HttpCode == 404 || *vsaerror.HttpCode == 409 {
				return &gcpgenserver.V1betaUpdateReplicationBadRequest{
					Code:    400,
					Message: vsaerror.Message,
				}, nil
			}
		}

		logger.Error("Failed to update replication", "error", err.Error())
		return &gcpgenserver.V1betaUpdateReplicationInternalServerError{
			Code:    500,
			Message: err.Error(),
		}, nil
	}

	resp, err := encodeVolumeReplicationV1(convertVcpReplicationModelToVCPVolumeReplicationV1beta(volumeRep))
	if err != nil {
		return nil, err
	}

	operationID := "/v1beta/projects/" + params.ProjectNumber + "/locations/" + params.LocationId + "/operations/" + jobUUID
	if volumeRep.State == models2.LifeCycleStateUpdating {
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

func (h Handler) V1betaDeleteReplication(ctx context.Context, req *gcpgenserver.ReplicationDeleteV1beta, params gcpgenserver.V1betaDeleteReplicationParams) (gcpgenserver.V1betaDeleteReplicationRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	if !crrEnabled {
		return &gcpgenserver.V1betaDeleteReplicationBadRequest{
			Code:    400,
			Message: "CRR is not enabled",
		}, nil
	}
	region, zone, parsingErr := parseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaDeleteReplicationBadRequest{
			Code:    parsingErr.Code,
			Message: parsingErr.Message,
		}, nil
	}
	isCleanUp := false
	if req.CleanupResourcesJobId.IsSet() {
		if req.CleanupResourcesJobId.Value != "" {
			isCleanUp = true
		}
	}

	deleteReplicationParams := &common.DeleteReplicationParams{
		AccountName:           params.ProjectNumber,
		Region:                region,
		CorrelationId:         params.XCorrelationID.Value,
		VolumeResourceId:      params.VolumeResourceId,
		ReplicationResourceId: params.ReplicationResourceId,
		Zone:                  zone,
	}

	volumeRep, jobUUID, err := h.Orchestrator.DeleteReplication(ctx, deleteReplicationParams, isCleanUp)
	if err != nil {
		if errors.IsUserInputValidationErr(err) || errors.IsNotFoundErr(err) {
			return &gcpgenserver.V1betaDeleteReplicationBadRequest{
				Code:    400,
				Message: err.Error(),
			}, nil
		}

		vsaerror, ok := err.(*vsaerrors.CustomError)
		if ok {
			if *vsaerror.HttpCode == 400 || *vsaerror.HttpCode == 404 || *vsaerror.HttpCode == 409 {
				return &gcpgenserver.V1betaDeleteReplicationBadRequest{
					Code:    400,
					Message: vsaerror.Message,
				}, nil
			}
		}

		logger.Error("Failed to delete replication", "error", err.Error())
		return &gcpgenserver.V1betaDeleteReplicationInternalServerError{
			Code:    500,
			Message: err.Error(),
		}, nil
	}
	operationID := "/v1beta/projects/" + params.ProjectNumber + "/locations/" + params.LocationId + "/operations/" + jobUUID

	var resp jx.Raw
	if !isCleanUp {
		resp, err = encodeVolumeReplicationV1(convertVcpReplicationModelToVCPVolumeReplicationV1beta(volumeRep))
		if err != nil {
			return nil, err
		}
	}
	if volumeRep.State == models2.LifeCycleStateDeleting {
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

func _convertVcpReplicationModelToVCPVolumeReplicationV1beta(volumeReplication *models2.VolumeReplication) *gcpgenserver.ReplicationV1beta {
	if volumeReplication == nil {
		return &gcpgenserver.ReplicationV1beta{}
	}

	var lastTransferEndTime time.Time
	var progressLastUpdated time.Time
	if volumeReplication.LastTransferEndTime != nil {
		lastTransferEndTime = *volumeReplication.LastTransferEndTime
	}
	if volumeReplication.ProgressLastUpdated != nil {
		progressLastUpdated = *volumeReplication.ProgressLastUpdated
	}

	return &gcpgenserver.ReplicationV1beta{
		ReplicationId: gcpgenserver.NewOptString(volumeReplication.UUID),
		ResourceId:    gcpgenserver.NewOptString(volumeReplication.Name),
		Description:   gcpgenserver.NewOptString(volumeReplication.Description),
		Source: gcpgenserver.NewOptReplicationVolumeInformationV1beta(gcpgenserver.ReplicationVolumeInformationV1beta{
			VolumeName: gcpgenserver.NewOptString(volumeReplication.ReplicationAttributes.SourceVolumeName),
			VolumeId:   gcpgenserver.NewOptString(volumeReplication.ReplicationAttributes.SourceVolumeUUID),
		}),
		Destination: gcpgenserver.NewOptReplicationVolumeInformationV1beta(gcpgenserver.ReplicationVolumeInformationV1beta{
			VolumeName: gcpgenserver.NewOptString(volumeReplication.ReplicationAttributes.DestinationVolumeName),
			VolumeId:   gcpgenserver.NewOptString(volumeReplication.ReplicationAttributes.DestinationVolumeUUID),
		}),
		State:               gcpgenserver.NewOptReplicationV1betaState(gcpgenserver.ReplicationV1betaState(volumeReplication.State)),
		StateDetails:        gcpgenserver.NewOptString(volumeReplication.StateDetails),
		Role:                gcpgenserver.NewOptReplicationV1betaRole(convertToRole(volumeReplication.ReplicationAttributes.EndpointType)),
		ReplicationSchedule: gcpgenserver.NewOptReplicationV1betaReplicationSchedule(gcpgenserver.ReplicationV1betaReplicationSchedule(volumeReplication.ReplicationAttributes.ReplicationSchedule)),
		MirrorState:         gcpgenserver.NewOptReplicationV1betaMirrorState(gcpgenserver.ReplicationV1betaMirrorState(*volumeReplication.MirrorState)),
		TransferStats: gcpgenserver.NewOptTransferStatsV1beta(gcpgenserver.TransferStatsV1beta{
			TotalTransferBytes:    gcpgenserver.NewOptFloat64(float64(volumeReplication.TotalTransferBytes)),
			TotalTransferTimeSecs: gcpgenserver.NewOptFloat64(float64(volumeReplication.TotalTransferTimeSecs)),
			LastTransferSize:      gcpgenserver.NewOptFloat64(float64(volumeReplication.LastTransferSize)),
			LastTransferError:     gcpgenserver.NewOptString(volumeReplication.LastTransferError),
			LastTransferDuration:  gcpgenserver.NewOptFloat64(float64(volumeReplication.LastTransferDuration)),
			TotalProgress:         gcpgenserver.NewOptFloat64(float64(volumeReplication.TotalProgress)),
			LagTime:               gcpgenserver.NewOptFloat64(float64(volumeReplication.LagTime)),
			LastTransferEndTime:   gcpgenserver.NewOptDateTime(lastTransferEndTime),
			ProgressLastUpdated:   gcpgenserver.NewOptDateTime(progressLastUpdated),
		}),
		Created: gcpgenserver.NewOptDateTime(volumeReplication.CreatedAt),
	}
}

func (h Handler) V1betaStopReplication(ctx context.Context, req *gcpgenserver.ReplicationStopV1beta, params gcpgenserver.V1betaStopReplicationParams) (gcpgenserver.V1betaStopReplicationRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	if !crrEnabled {
		return &gcpgenserver.V1betaStopReplicationBadRequest{
			Code:    400,
			Message: "CRR is not enabled",
		}, nil
	}
	region, zone, parsingErr := parseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaStopReplicationBadRequest{
			Code:    parsingErr.Code,
			Message: parsingErr.Message,
		}, nil
	}

	stopReplicationParams := &common.StopReplicationParams{
		AccountName:           params.ProjectNumber,
		Region:                region,
		Zone:                  zone,
		CorrelationId:         params.XCorrelationID.Value,
		VolumeResourceId:      params.VolumeResourceId,
		ReplicationResourceId: params.ReplicationResourceId,
		ForceStop:             req.GetForce().Value,
	}

	volumeRep, jobUUID, err := h.Orchestrator.StopReplication(ctx, stopReplicationParams)
	if err != nil {
		if errors.IsUserInputValidationErr(err) || errors.IsNotFoundErr(err) {
			return &gcpgenserver.V1betaStopReplicationBadRequest{
				Code:    400,
				Message: err.Error(),
			}, nil
		}

		vsaerror, ok := err.(*vsaerrors.CustomError)
		if ok {
			if *vsaerror.HttpCode == 400 || *vsaerror.HttpCode == 404 || *vsaerror.HttpCode == 409 {
				return &gcpgenserver.V1betaStopReplicationBadRequest{
					Code:    400,
					Message: vsaerror.Message,
				}, nil
			}
		}

		logger.Error("Failed to stop replication", "error", err.Error())
		return &gcpgenserver.V1betaStopReplicationInternalServerError{
			Code:    500,
			Message: err.Error(),
		}, nil
	}

	resp, err := encodeVolumeReplicationV1(convertVcpReplicationModelToVCPVolumeReplicationV1beta(volumeRep))
	if err != nil {
		return nil, err
	}

	operationID := "/v1beta/projects/" + params.ProjectNumber + "/locations/" + params.LocationId + "/operations/" + jobUUID
	if volumeRep.State == models2.LifeCycleStateUpdating {
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

func (h Handler) V1betaSyncReplication(ctx context.Context, params gcpgenserver.V1betaSyncReplicationParams) (gcpgenserver.V1betaSyncReplicationRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	if !crrEnabled {
		return &gcpgenserver.V1betaSyncReplicationBadRequest{
			Code:    400,
			Message: "CRR is not enabled",
		}, nil
	}
	region, zone, parsingErr := parseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaSyncReplicationBadRequest{
			Code:    parsingErr.Code,
			Message: parsingErr.Message,
		}, nil
	}

	syncReplicationParams := &common.ResumeReplicationParams{
		AccountName:           params.ProjectNumber,
		Region:                region,
		Zone:                  zone,
		CorrelationId:         params.XCorrelationID.Value,
		VolumeResourceId:      params.VolumeResourceId,
		ReplicationResourceId: params.ReplicationResourceId,
		Force:                 true,
	}

	volumeRep, jobUUID, err := h.Orchestrator.SyncReplication(ctx, syncReplicationParams)
	if err != nil {
		if errors.IsUserInputValidationErr(err) || errors.IsNotFoundErr(err) {
			return &gcpgenserver.V1betaSyncReplicationBadRequest{
				Code:    400,
				Message: err.Error(),
			}, nil
		}

		vsaerror, ok := err.(*vsaerrors.CustomError)
		if ok {
			if *vsaerror.HttpCode == 400 || *vsaerror.HttpCode == 404 || *vsaerror.HttpCode == 409 {
				return &gcpgenserver.V1betaSyncReplicationBadRequest{
					Code:    400,
					Message: vsaerror.Message,
				}, nil
			}
		}

		logger.Error("Failed to sync replication", "error", err.Error())
		return &gcpgenserver.V1betaSyncReplicationInternalServerError{
			Code:    500,
			Message: err.Error(),
		}, nil
	}
	resp, err := encodeVolumeReplicationV1(convertVcpReplicationModelToVCPVolumeReplicationV1beta(volumeRep))
	if err != nil {
		return nil, err
	}

	operationID := "/v1beta/projects/" + params.ProjectNumber + "/locations/" + params.LocationId + "/operations/" + jobUUID
	if volumeRep.State == models2.LifeCycleStateUpdating {
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

func (h Handler) V1betaReverseAndResumeReplication(ctx context.Context, params gcpgenserver.V1betaReverseAndResumeReplicationParams) (gcpgenserver.V1betaReverseAndResumeReplicationRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)

	if !crrEnabled {
		return &gcpgenserver.V1betaReverseAndResumeReplicationBadRequest{
			Code:    400,
			Message: "CRR is not enabled",
		}, nil
	}

	region, zone, parsingErr := parseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaReverseAndResumeReplicationBadRequest{
			Code:    parsingErr.Code,
			Message: parsingErr.Message,
		}, nil
	}

	reverseAndResumeParams := &common.ReverseAndResumeReplicationParams{
		AccountName:           params.ProjectNumber,
		Region:                region,
		Zone:                  zone,
		CorrelationId:         params.XCorrelationID.Value,
		VolumeResourceId:      params.VolumeResourceId,
		ReplicationResourceId: params.ReplicationResourceId,
	}

	volumeRep, jobUUID, err := h.Orchestrator.ReverseAndResumeReplication(ctx, reverseAndResumeParams)
	if err != nil {
		if errors.IsUserInputValidationErr(err) {
			return &gcpgenserver.V1betaReverseAndResumeReplicationBadRequest{
				Code:    400,
				Message: err.Error(),
			}, nil
		}

		vsaerror, ok := err.(*vsaerrors.CustomError)
		if ok {
			if *vsaerror.HttpCode == 400 || *vsaerror.HttpCode == 404 || *vsaerror.HttpCode == 409 {
				return &gcpgenserver.V1betaReverseAndResumeReplicationBadRequest{
					Code:    400,
					Message: vsaerror.Message,
				}, nil
			}
		}

		logger.Error("Failed to reverse and resume replication", "error", err)
		return &gcpgenserver.V1betaReverseAndResumeReplicationInternalServerError{
			Code:    500,
			Message: err.Error(),
		}, nil
	}

	operationID := fmt.Sprintf("/v1beta/projects/%s/locations/%s/operations/%s", params.ProjectNumber, params.LocationId, *jobUUID)

	resp, err := encodeVolumeReplicationV1(convertVcpReplicationModelToVCPVolumeReplicationV1beta(volumeRep))
	if err != nil {
		logger.Error("Failed to encode volume replication", "error", err)
		return &gcpgenserver.V1betaReverseAndResumeReplicationInternalServerError{
			Code:    500,
			Message: "Internal server error while encoding replication response",
		}, nil
	}

	if volumeRep.State == models2.LifeCycleStateUpdating {
		return &gcpgenserver.OperationV1beta{
			Name:     gcpgenserver.NewOptString(operationID),
			Response: resp,
			Done:     gcpgenserver.NewOptBool(false),
		}, nil
	}
	return &gcpgenserver.OperationV1beta{
		Name: gcpgenserver.NewOptString(operationID),
		Done: gcpgenserver.NewOptBool(true),
	}, nil
}
