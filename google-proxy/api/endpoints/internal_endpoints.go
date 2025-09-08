package api

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/helper"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	jsonUnmarshal = json.Unmarshal
)

func (h Handler) V1betaInternalDescribePool(ctx context.Context, params gcpgenserver.V1betaInternalDescribePoolParams) (gcpgenserver.V1betaInternalDescribePoolRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	queryDepth := 1
	pool, err := h.Orchestrator.GetPoolByName(ctx, params.PoolName, params.ProjectNumber, queryDepth)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			logger.Info("Pool not found", "name", params.PoolName)
			return &gcpgenserver.V1betaInternalDescribePoolNotFound{
				Code:    404,
				Message: "Pool not found",
			}, nil
		}
		logger.Error("Failed to describe pool", "error", err.Error())
		return &gcpgenserver.V1betaInternalDescribePoolInternalServerError{
			Code:    500,
			Message: "Internal server error while describing pool",
		}, err
	}

	return convertToPoolInternalV1Beta(pool), nil
}

func (h Handler) V1betaGetMultipleReplicationsInternal(ctx context.Context, req *gcpgenserver.ReplicationIDListV1beta, params gcpgenserver.V1betaGetMultipleReplicationsInternalParams) (gcpgenserver.V1betaGetMultipleReplicationsInternalRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	replications, err := h.Orchestrator.GetMultipleReplicationsInternal(ctx, params.ProjectNumber, req.GetReplicationUUIDs())
	if err != nil {
		if errors.IsNotFoundErr(err) {
			logger.Info("No replications found for the provided UUIDs", "replicationUUIDs", req.GetReplicationUUIDs())
			return &gcpgenserver.V1betaGetMultipleReplicationsInternalNotFound{
				Code:    404,
				Message: "No replications found for the provided UUIDs",
			}, nil
		}
		logger.Error("Failed to get multiple replications", "error", err.Error())
		return &gcpgenserver.V1betaGetMultipleReplicationsInternalInternalServerError{
			Code:    500,
			Message: "Internal server error while getting multiple replications",
		}, err
	}

	return &gcpgenserver.V1betaGetMultipleReplicationsInternalOK{
		Replications: convertToVolumeReplicationsInternalV1Beta(replications),
	}, nil
}

func (h Handler) V1betaInternalStopVolumeReplication(ctx context.Context, request *gcpgenserver.V1betaInternalStopVolumeReplicationReq, params gcpgenserver.V1betaInternalStopVolumeReplicationParams) (gcpgenserver.V1betaInternalStopVolumeReplicationRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	forceStop := false
	if request.GetForce().Value {
		forceStop = request.GetForce().Value
	}

	replication, job, err := h.Orchestrator.StopReplicationInternal(ctx, params.VolumeReplicationId, params.ProjectNumber, forceStop)
	if err != nil {
		logger.Error("Failed to stop replication", "error", err.Error())
		if errors.IsNotFoundErr(err) {
			return &gcpgenserver.V1betaInternalStopVolumeReplicationNotFound{
				Code:    404,
				Message: "Volume replication not found",
			}, nil
		} else if errors.IsUserInputValidationErr(err) {
			return &gcpgenserver.V1betaInternalStopVolumeReplicationBadRequest{
				Code:    400,
				Message: "Invalid request parameters",
			}, nil
		}
		return &gcpgenserver.V1betaInternalStopVolumeReplicationInternalServerError{
			Code:    500,
			Message: "Internal server error while resuming replication",
		}, nil
	}

	return convertToInternalV1betaVolumeReplication(replication, job), nil
}

func (h Handler) V1betaInternalCreateVolumeReplication(ctx context.Context, req *gcpgenserver.VolumeReplicationCreateInternalV1beta, params gcpgenserver.V1betaInternalCreateVolumeReplicationParams) (gcpgenserver.V1betaInternalCreateVolumeReplicationRes, error) {
	logger := util.GetLogger(ctx)
	if req.EndpointType != "dst" {
		code := float64(400)
		msg := "Incorrect endpoint type"
		return &gcpgenserver.V1betaInternalCreateVolumeReplicationBadRequest{
			Code:    code,
			Message: msg,
		}, nil
	}

	param := prepareCreateVolumeReplicationInternalParams(req, params)
	volumeReplication, job, err := h.Orchestrator.CreateVolumeReplicationInternal(ctx, param)
	if err != nil {
		logger.Error("Failed to create volume replication", "error", err.Error())
		return nil, err
	}
	ans := convertToInternalV1betaVolumeReplication(volumeReplication, job)
	return ans, nil
}

func (h Handler) V1betaInternalReleaseVolumeReplication(ctx context.Context, params gcpgenserver.V1betaInternalReleaseVolumeReplicationParams) (gcpgenserver.V1betaInternalReleaseVolumeReplicationRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)

	_, job, err := h.Orchestrator.ReleaseVolumeReplication(ctx, params.VolumeReplicationId)
	if err != nil {
		logger.Error("Failed to release volume replication", "error", err.Error())
		if errors.IsNotFoundErr(err) {
			return &gcpgenserver.V1betaInternalReleaseVolumeReplicationBadRequest{
				Code:    404,
				Message: "Volume replication not found",
			}, nil
		}
		return &gcpgenserver.V1betaInternalReleaseVolumeReplicationInternalServerError{
			Code:    500,
			Message: err.Error(),
		}, nil
	}
	return &gcpgenserver.OperationV1beta{
		Name:     gcpgenserver.NewOptString(fmt.Sprintf("/v1beta/projects/%s/locations/%s/operations/%s", params.ProjectNumber, params.LocationId, job.UUID)),
		Response: nil,
		Done:     gcpgenserver.NewOptBool(true),
	}, nil
}

func (h Handler) V1betaInternalDeleteVolumeReplication(ctx context.Context, params gcpgenserver.V1betaInternalDeleteVolumeReplicationParams) (gcpgenserver.V1betaInternalDeleteVolumeReplicationRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	volumeReplication, job, err := h.Orchestrator.DeleteReplicationInternal(ctx, params.VolumeReplicationId)
	if err != nil {
		logger.Error("Failed to delete replication", "error", err.Error())
		if errors.IsNotFoundErr(err) {
			return &gcpgenserver.V1betaInternalDeleteVolumeReplicationBadRequest{
				Code:    404,
				Message: "Volume replication not found",
			}, nil
		}
		return &gcpgenserver.V1betaInternalDeleteVolumeReplicationInternalServerError{
			Code:    500,
			Message: err.Error(),
		}, nil
	}
	ans := convertToInternalV1betaVolumeReplication(volumeReplication, job)
	return ans, nil
}

// V1betaInternalDeleteVolumeSnapmirrorSnapshot handles the request to delete a ssnapshots with prefix snapmirror. from a spicific volume.
func (h Handler) V1betaInternalDeleteVolumeSnapmirrorSnapshot(ctx context.Context, params gcpgenserver.V1betaInternalDeleteVolumeSnapmirrorSnapshotParams) (gcpgenserver.V1betaInternalDeleteVolumeSnapmirrorSnapshotRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	volumeId := params.VolumeId
	region, _, parsingErr := parseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaInternalDeleteVolumeSnapmirrorSnapshotBadRequest{
			Code:    400,
			Message: parsingErr.GetMessage(),
		}, nil
	}

	deleteSnapshotParams := &commonparams.SnapshotsInternalDeleteParams{
		SnapshotBaseParams: commonparams.SnapshotBaseParams{
			VolumeID:    volumeId,
			AccountName: params.ProjectNumber,
		},
		Location: region,
	}

	// Delete the snapmirror snapshots
	operationID, err := h.Orchestrator.DeleteSnapmirrorSnapshots(ctx, deleteSnapshotParams)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			return &gcpgenserver.V1betaInternalDeleteVolumeSnapmirrorSnapshotBadRequest{
				Code:    404,
				Message: "Snapshot not found",
			}, nil
		} else if errors.IsUserInputValidationErr(err) {
			return &gcpgenserver.V1betaInternalDeleteVolumeSnapmirrorSnapshotBadRequest{
				Code:    400,
				Message: err.Error(),
			}, nil
		} else if errors.IsConflictErr(err) {
			return &gcpgenserver.V1betaInternalDeleteVolumeSnapmirrorSnapshotConflict{
				Code:    409,
				Message: err.Error(),
			}, nil
		}
		logger.Error("Failed to delete snapshot", "error", err.Error())
		return &gcpgenserver.V1betaInternalDeleteVolumeSnapmirrorSnapshotInternalServerError{
			Code:    500,
			Message: "Internal server error",
		}, err
	}

	logger.Infof("Snapshots deleted in progress for volume %s in region %s", volumeId, region)
	return &gcpgenserver.OperationV1beta{
		Name:     gcpgenserver.NewOptString(fmt.Sprintf("/v1beta/projects/%s/locations/%s/operations/%s", params.ProjectNumber, params.LocationId, operationID)),
		Response: nil,
		Done:     gcpgenserver.NewOptBool(true),
	}, nil
}

func (h Handler) V1betaInternalUpdateVolumeReplication(ctx context.Context, req *gcpgenserver.VolumeReplicationUpdateInternalV1beta, params gcpgenserver.V1betaInternalUpdateVolumeReplicationParams) (gcpgenserver.V1betaInternalUpdateVolumeReplicationRes, error) {
	logger := util.GetLogger(ctx)

	param := prepareUpdateVolumeReplicationInternalParams(req, params)
	volumeReplication, job, err := h.Orchestrator.UpdateVolumeReplicationInternal(ctx, param)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			logger.Info("No replications found to update", "replicationUUID", params.VolumeReplicationId)
			return &gcpgenserver.V1betaInternalUpdateVolumeReplicationBadRequest{
				Code:    404,
				Message: "No replications found for the provided UUIDs",
			}, nil
		}
		logger.Error("Failed to update volume replication", "error", err.Error())
		return &gcpgenserver.V1betaInternalUpdateVolumeReplicationInternalServerError{
			Code:    500,
			Message: err.Error(),
		}, nil
	}
	res := convertToInternalV1betaVolumeReplication(volumeReplication, job)
	return res, nil
}

func (h Handler) V1betaInternalUpdateVolumeReplicationAttributes(ctx context.Context, req *gcpgenserver.VolumeReplicationInternalV1beta, params gcpgenserver.V1betaInternalUpdateVolumeReplicationAttributesParams) (gcpgenserver.V1betaInternalUpdateVolumeReplicationAttributesRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)

	// Convert the request to internal parameters for updating volume replication attributes
	updateParams := models.UpdateVolumeReplicationAttributesParams{
		ProjectNumber:              params.ProjectNumber,
		LocationId:                params.LocationId,
		VolumeReplicationId:       params.VolumeReplicationId,
		VolumeReplicationInternal: req,
	}

	// Call the orchestrator to update volume replication attributes
	job, err := h.Orchestrator.UpdateVolumeReplicationAttributes(ctx, updateParams)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			return &gcpgenserver.V1betaInternalUpdateVolumeReplicationAttributesBadRequest{
				Code:    400,
				Message: err.Error(),
			}, nil
		}
		logger.Error("Failed to update volume replication attributes", "error", err.Error())
		return &gcpgenserver.V1betaInternalUpdateVolumeReplicationAttributesInternalServerError{
			Code:    500,
			Message: "Internal server error",
		}, err
	}

	// Return operation response instead of 204 No Content
	return &gcpgenserver.OperationV1beta{
		Name:     gcpgenserver.NewOptString(fmt.Sprintf("/v1beta/projects/%s/locations/%s/operations/%s", params.ProjectNumber, params.LocationId, job.UUID)),
		Response: nil,
		Done:     gcpgenserver.NewOptBool(true),
	}, nil
}

func convertToInternalV1betaVolumeReplication(volumeReplication *models.VolumeReplication, job *datamodel.Job) *gcpgenserver.VolumeReplicationInternalV1beta {
	return &gcpgenserver.VolumeReplicationInternalV1beta{
		VolumeReplicationUuid: gcpgenserver.NewOptString(volumeReplication.UUID),
		EndpointType:          gcpgenserver.VolumeReplicationInternalV1betaEndpointType(volumeReplication.ReplicationAttributes.EndpointType),
		RemoteRegion:          volumeReplication.ReplicationAttributes.SourceRegion,
		SourceHostName:        volumeReplication.ReplicationAttributes.SourceHostName,
		SourceServerName:      volumeReplication.ReplicationAttributes.SourceSvmName,
		SourceVolumeName:      volumeReplication.ReplicationAttributes.SourceVolumeName,
		SourceVolumeUuid:      gcpgenserver.NewOptString(volumeReplication.ReplicationAttributes.SourceVolumeUUID),
		SourcePoolUuid:        gcpgenserver.NewOptString(volumeReplication.ReplicationAttributes.SourcePoolUUID),
		DestinationHostName:   volumeReplication.ReplicationAttributes.DestinationHostName,
		DestinationServerName: volumeReplication.ReplicationAttributes.DestinationSvmName,
		DestinationVolumeName: volumeReplication.ReplicationAttributes.DestinationVolumeName,
		DestinationVolumeUuid: gcpgenserver.NewOptString(volumeReplication.ReplicationAttributes.DestinationVolumeUUID),
		DestinationPoolUuid:   gcpgenserver.NewOptString(volumeReplication.ReplicationAttributes.DestinationPoolUUID),
		ReplicationType: gcpgenserver.OptVolumeReplicationInternalV1betaReplicationType{
			Value: gcpgenserver.VolumeReplicationInternalV1betaReplicationType(volumeReplication.ReplicationAttributes.ReplicationType),
			Set:   true,
		},
		Jobs: []gcpgenserver.JobV1beta{
			{
				JobId:    gcpgenserver.NewOptString(job.UUID),
				Created:  gcpgenserver.NewOptDateTime(job.CreatedAt),
				WorkerId: gcpgenserver.NewOptString(job.WorkflowID),
			},
		},
	}
}

func prepareCreateVolumeReplicationInternalParams(req *gcpgenserver.VolumeReplicationCreateInternalV1beta, params gcpgenserver.V1betaInternalCreateVolumeReplicationParams) *commonparams.CreateVolumeReplicationInternalParams {
	volRepParams := &models.VolumeReplication{
		Name:        req.Name.Value,
		Description: req.Description.Value,
		Uri:         req.CcfeURI.Value,
		RemoteUri:   req.CcfeRemoteURI.Value,
		Account: &models.Account{
			Name: params.ProjectNumber,
		},
		ReplicationAttributes: &models.ReplicationDetails{
			EndpointType:          string(req.EndpointType),
			ReplicationSchedule:   string(req.ReplicationSchedule.Value),
			ReplicationType:       string(req.ReplicationType.Value),
			SourceRegion:          req.RemoteRegion,
			SourceHostName:        req.SourceHostName,
			SourceReplicationUUID: req.VolumeReplicationUuid.Value,
			SourceSvmName:         req.SourceServerName,
			SourceVolumeName:      req.SourceVolumeName,
			SourceVolumeUUID:      req.SourceVolumeUuid.Value,
			SourcePoolUUID:        req.SourcePoolUuid.Value,
			DestinationVolumeUUID: req.DestinationVolumeUuid.Value,
			DestinationRegion:     params.LocationId,
			DestinationHostName:   req.DestinationHostName,
			DestinationSvmName:    req.DestinationServerName,
			DestinationVolumeName: req.DestinationVolumeName,
			DestinationPoolUUID:   req.DestinationPoolUuid.Value,
			ReplicationPolicy:     string(req.ReplicationPolicy.Value),
		},
	}

	param := &commonparams.CreateVolumeReplicationInternalParams{
		ReverseResync:     req.ReverseResume.Value,
		VolumeReplication: volRepParams,
	}

	return param
}

func prepareUpdateVolumeReplicationInternalParams(req *gcpgenserver.VolumeReplicationUpdateInternalV1beta, params gcpgenserver.V1betaInternalUpdateVolumeReplicationParams) *commonparams.UpdateVolumeReplicationInternalParams {
	param := &commonparams.UpdateVolumeReplicationInternalParams{
		VolumeReplicationUuid: params.VolumeReplicationId,
		AccountName:           params.ProjectNumber,
		XCorrelationID:        params.XCorrelationID.Value,
		LocationId:            params.LocationId,
	}
	if req.Description.IsSet() {
		param.Description = &req.Description.Value
	}
	if req.ReplicationSchedule.IsSet() {
		schedule := string(req.ReplicationSchedule.Value)
		param.ReplicationSchedule = &schedule
	}
	return param
}

func (h Handler) V1betaInternalGetReplicationJobs(ctx context.Context, params gcpgenserver.V1betaInternalGetReplicationJobsParams) (gcpgenserver.V1betaInternalGetReplicationJobsRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	poolUUID := ""
	if params.PoolUUID.Set {
		poolUUID = params.PoolUUID.Value
	}

	jobs, err := h.Orchestrator.GetReplicationJobs(ctx, params.ProjectNumber, poolUUID)
	if err != nil {
		logger.Error("Failed to get replication jobs", "error", err.Error())
		return &gcpgenserver.V1betaInternalGetReplicationJobsInternalServerError{
			Code:    500,
			Message: "Internal server error while getting replication jobs",
		}, err
	}
	jobsList := make([]gcpgenserver.InternalJobV1beta, 0, len(jobs))
	for _, job := range jobs {
		jobsList = append(jobsList, convertJobToInternalJobV1Beta(job))
	}
	return &gcpgenserver.V1betaInternalGetReplicationJobsOK{
		Jobs: jobsList,
	}, nil
}

func (h Handler) V1betaInternalMountVolumeReplication(ctx context.Context, params gcpgenserver.V1betaInternalMountVolumeReplicationParams) (gcpgenserver.V1betaInternalMountVolumeReplicationRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	job, err := h.Orchestrator.PerformMountCheck(ctx, params.VolumeReplicationId, params.ProjectNumber)
	if err != nil {
		logger.Error("Failed to PerformMountCheck", "error", err.Error())
		return &gcpgenserver.V1betaInternalMountVolumeReplicationInternalServerError{Code: 500, Message: err.Error()}, nil
	}
	internalJob := convertJobToInternalJobV1Beta(job)
	return &internalJob, nil
}

func convertJobToInternalJobV1Beta(job *models.Job) gcpgenserver.InternalJobV1beta {
	return gcpgenserver.InternalJobV1beta{
		JobUuid:       gcpgenserver.NewOptString(job.UUID),
		CorrelationId: gcpgenserver.NewOptString(job.CorrelationID),
		State:         gcpgenserver.NewOptString(string(job.State)),
		StateDetails:  gcpgenserver.NewOptString(job.StateDetails),
		JobType:       gcpgenserver.NewOptString(string(job.Type)),
		CreatedAt:     gcpgenserver.NewOptDateTime(job.CreatedAt),
		UpdatedAt:     gcpgenserver.NewOptDateTime(job.UpdatedAt),
		ScheduledAt:   gcpgenserver.NewOptDateTime(job.ScheduledAt),
		ResourceName:  gcpgenserver.NewOptString(job.ResourceName),
	}
}

func (h Handler) V1betaInternalResumeVolumeReplication(ctx context.Context, params gcpgenserver.V1betaInternalResumeVolumeReplicationParams) (gcpgenserver.V1betaInternalResumeVolumeReplicationRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	forceResume := false
	if params.ForceResume.Set {
		forceResume = params.ForceResume.Value
	}
	volumeReplication, job, err := h.Orchestrator.ResumeReplicationInternal(ctx, params.VolumeReplicationId, params.ProjectNumber, forceResume)
	if err != nil {
		logger.Error("Failed to resume replication", "error", err.Error())
		if errors.IsNotFoundErr(err) {
			return &gcpgenserver.V1betaInternalResumeVolumeReplicationNotFound{
				Code:    404,
				Message: "Volume replication not found",
			}, nil
		} else if errors.IsUserInputValidationErr(err) {
			return &gcpgenserver.V1betaInternalResumeVolumeReplicationBadRequest{
				Code:    400,
				Message: "Invalid request parameters",
			}, nil
		}
		return &gcpgenserver.V1betaInternalResumeVolumeReplicationInternalServerError{
			Code:    500,
			Message: "Internal server error while resuming replication",
		}, nil
	}

	return convertToInternalV1betaVolumeReplication(volumeReplication, job), nil
}

func (h Handler) V1betaInternalReverseVolumeReplication(ctx context.Context, params gcpgenserver.V1betaInternalReverseVolumeReplicationParams) (gcpgenserver.V1betaInternalReverseVolumeReplicationRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)

	volumeReplication, job, err := h.Orchestrator.ReverseReplicationInternal(ctx, params.VolumeReplicationId, params.ProjectNumber)
	if err != nil {
		logger.Error("Failed to reverse replication", "error", err.Error())
		if errors.IsNotFoundErr(err) {
			return &gcpgenserver.V1betaInternalReverseVolumeReplicationNotFound{
				Code:    404,
				Message: "Volume replication not found",
			}, nil
		} else if errors.IsUserInputValidationErr(err) {
			return &gcpgenserver.V1betaInternalReverseVolumeReplicationBadRequest{
				Code:    400,
				Message: "Invalid request parameters",
			}, nil
		}
		return &gcpgenserver.V1betaInternalReverseVolumeReplicationInternalServerError{
			Code:    500,
			Message: "Internal server error while reversing replication",
		}, nil
	}

	return convertToInternalV1betaVolumeReplication(volumeReplication, job), nil
}

func (h Handler) V1betaInternalDescribeVolume(ctx context.Context, params gcpgenserver.V1betaInternalDescribeVolumeParams) (gcpgenserver.V1betaInternalDescribeVolumeRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	volume, err := h.Orchestrator.GetVolume(ctx, params.VolumeId, true)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			return &gcpgenserver.V1betaInternalDescribeVolumeNotFound{
				Code:    404,
				Message: "Volume not found",
			}, nil
		}
		logger.Error("Failed to describe volume", "error", err.Error())
		return &gcpgenserver.V1betaInternalDescribeVolumeInternalServerError{Code: 500, Message: "Internal server error"}, err
	}

	volumeRes := convertModelToVCPVolume(volume)
	resp, err := jsonMarshal(volumeRes)
	if err != nil {
		return &gcpgenserver.V1betaInternalDescribeVolumeInternalServerError{Code: 500, Message: err.Error()}, nil
	}
	res := gcpgenserver.InternalVolumeV1beta{}
	err = jsonUnmarshal(resp, &res)
	if err != nil {
		return &gcpgenserver.V1betaInternalDescribeVolumeInternalServerError{Code: 500, Message: err.Error()}, nil
	}
	res.SvmName = gcpgenserver.NewOptNilString(volume.SvmName)
	return &res, nil
}

func (h Handler) V1betaInternalUpdateVolume(ctx context.Context, req *gcpgenserver.VolumeUpdateV1beta, params gcpgenserver.V1betaInternalUpdateVolumeParams) (gcpgenserver.V1betaInternalUpdateVolumeRes, error) {
	logger := util.GetLogger(ctx)
	region, _, parsingErr := parseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaInternalUpdateVolumeBadRequest{
			Code:    parsingErr.Code,
			Message: parsingErr.Message,
		}, nil
	}
	volumeUpdateParams := gcpgenserver.V1betaUpdateVolumeParams{
		ProjectNumber:  params.ProjectNumber,
		LocationId:     params.LocationId,
		VolumeId:       params.VolumeId,
		XCorrelationID: params.XCorrelationID,
	}
	param, err := prepareUpdateVolumeParams(req, volumeUpdateParams, region)
	if err != nil {
		if errors.IsUserInputValidationErr(err) || errors.IsNotFoundErr(err) {
			return &gcpgenserver.V1betaInternalUpdateVolumeBadRequest{
				Code:    400,
				Message: err.Error(),
			}, nil
		}

		logger.Error("Failed to update volume", "error", err.Error())
		return &gcpgenserver.V1betaInternalUpdateVolumeInternalServerError{Code: 500, Message: err.Error()}, err
	}

	volume, jobUUID, err := h.Orchestrator.UpdateVolume(ctx, param)
	if err != nil {
		if errors.IsUserInputValidationErr(err) || errors.IsNotFoundErr(err) {
			return &gcpgenserver.V1betaInternalUpdateVolumeBadRequest{
				Code:    400,
				Message: err.Error(),
			}, nil
		}

		logger.Error("Failed to update volume", "error", err.Error())
		return &gcpgenserver.V1betaInternalUpdateVolumeInternalServerError{Code: 500, Message: err.Error()}, err
	}

	resp, err := encodeVolumeV1(convertModelToVCPVolume(volume))
	if err != nil {
		return nil, err
	}

	operationID := "/v1beta/projects/" + params.ProjectNumber + "/locations/" + params.LocationId + "/operations/" + jobUUID
	if volume.LifeCycleState == models.LifeCycleStateUpdating {
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
