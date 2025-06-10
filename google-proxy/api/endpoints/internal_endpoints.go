package api

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/helper"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

func (h Handler) V1betaInternalDescribePool(ctx context.Context, params gcpgenserver.V1betaInternalDescribePoolParams) (gcpgenserver.V1betaInternalDescribePoolRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId)
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

func convertToPoolInternalV1Beta(pool *models.Pool) *gcpgenserver.PoolInternalV1beta {
	poolResp := &gcpgenserver.PoolInternalV1beta{
		Network:                  pool.VendorSubNetID,
		PoolId:                   gcpgenserver.NewOptString(pool.UUID),
		ResourceId:               pool.Name,
		ServiceLevel:             gcpgenserver.PoolInternalV1betaServiceLevel(pool.ServiceLevel),
		QosType:                  gcpgenserver.NewOptNilString(pool.QosType),
		SizeInBytes:              float64(pool.SizeInBytes),
		AllocatedBytes:           gcpgenserver.NewOptNilFloat64(pool.AllocatedBytes),
		TotalThroughputMibps:     gcpgenserver.NewOptNilFloat64(pool.TotalThroughputMibps),
		AvailableThroughputMibps: gcpgenserver.NewOptNilFloat64(pool.TotalThroughputMibps - pool.UtilizedThroughputMibps),
		NumberOfVolumes:          gcpgenserver.NewOptNilInt32(int32(pool.NumberOfVolumes)),
		StoragePoolState: gcpgenserver.OptPoolInternalV1betaStoragePoolState{
			Value: gcpgenserver.PoolInternalV1betaStoragePoolState(pool.State),
		},
		StoragePoolStateDetails: gcpgenserver.NewOptString(pool.StateDetails),
		CreatedAt:               gcpgenserver.NewOptDateTime(pool.CreatedAt),
		UpdatedAt:               gcpgenserver.NewOptDateTime(pool.UpdatedAt),
		StateDetails:            gcpgenserver.NewOptString(pool.StateDetails),
		Description:             gcpgenserver.NewOptNilString(pool.Description),
		Zone:                    gcpgenserver.NewOptString(pool.Zone),
		AllowAutoTiering:        gcpgenserver.NewOptNilBool(pool.AllowAutoTiering),
	}
	if pool.PoolAttributes != nil {
		poolResp.SecondaryZone = gcpgenserver.NewOptString(pool.PoolAttributes.SecondaryZone)
	}
	if pool.CustomPerformanceParams != nil {
		poolResp.CustomPerformanceEnabled = gcpgenserver.NewOptBool(pool.CustomPerformanceParams.Enabled)
		poolResp.TotalIops = gcpgenserver.NewOptNilFloat64(float64(pool.CustomPerformanceParams.Iops))
	}
	if pool.ClusterAttributes != nil {
		poolResp.InterclusterLifs = pool.ClusterAttributes.InterClusterLifs
		poolResp.ClusterName = gcpgenserver.NewOptString(pool.ClusterAttributes.ExternalName)
	}
	return poolResp
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

func (h Handler) V1betaInternalGetReplicationJobs(ctx context.Context, params gcpgenserver.V1betaInternalGetReplicationJobsParams) (gcpgenserver.V1betaInternalGetReplicationJobsRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId)
	jobs, err := h.Orchestrator.GetReplicationJobs(ctx, params.ProjectNumber, params.PoolId)
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
	}
}
