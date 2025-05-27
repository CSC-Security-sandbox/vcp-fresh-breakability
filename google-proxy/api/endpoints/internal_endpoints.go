package api

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

func (h Handler) V1betaInternalDescribePool(ctx context.Context, params gcpgenserver.V1betaInternalDescribePoolParams) (gcpgenserver.V1betaInternalDescribePoolRes, error) {
	logger := util.GetLogger(ctx)
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
