package api

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"time"

	"github.com/go-faster/jx"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"golang.org/x/exp/slog"
)

func (h Handler) V1betaDescribePool(ctx context.Context, params gcpgenserver.V1betaDescribePoolParams) (gcpgenserver.V1betaDescribePoolRes, error) {
	logger := ctx.Value(middleware.ContextSLoggerKey).(log.Logger)
	pool, err := h.Orchestrator.GetPool(ctx, params.PoolId)
	if err != nil {
		logger.Error("Failed to describe pool", slog.String("error", err.Error()))
		return &gcpgenserver.V1betaDescribePoolInternalServerError{}, err
	}
	return convertToPoolV1Beta(pool), nil
}

func (h Handler) V1betaCreatePool(ctx context.Context, req *gcpgenserver.PoolV1beta, params gcpgenserver.V1betaCreatePoolParams) (gcpgenserver.V1betaCreatePoolRes, error) {
	logger := ctx.Value(middleware.ContextSLoggerKey).(log.Logger)
	if !req.UnifiedPool.Value {
		logger.Error("UnifiedPool is not set to true")
		return &gcpgenserver.V1betaCreatePoolBadRequest{
			Code:    400,
			Message: "UnifiedPool must be set to true",
		}, nil
	}
	region, _, parsingErr := utils.ParseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaCreatePoolBadRequest{
			Code:    parsingErr.Code,
			Message: parsingErr.Message,
		}, nil
	}

	vendorId := fmt.Sprintf("/projects/%v/locations/%v/pools/%s", params.ProjectNumber, params.LocationId, req.ResourceId)
	// Check if the pool already exists
	existingPool, err := h.Orchestrator.GetPoolByVendorID(ctx, vendorId)
	if err == nil {
		logger.Info("Pool already exists", slog.String("vendorId", vendorId))
		resp, err := encodePoolV1(convertToPoolV1Beta(existingPool))
		if err != nil {
			return nil, err
		}
		return &gcpgenserver.OperationV1beta{
			Name:     gcpgenserver.OptString{Value: "operation-id"},
			Response: resp,
		}, nil
	} else if err.Error() != "pool not found" {
		logger.Error("Failed to check existing pool", slog.String("error", err.Error()))
		return &gcpgenserver.V1betaCreatePoolInternalServerError{}, err
	}

	param := &common.CreatePoolParams{AccountName: params.ProjectNumber, Region: region, CurrentZone: req.Zone.Value, Name: req.ResourceId, VendorID: vendorId, VendorSubNetID: req.Network, ServiceLevel: string(req.ServiceLevel), SizeInBytes: uint64(req.SizeInBytes)}
	created, operationID, err := h.Orchestrator.CreatePool(ctx, param)
	if err != nil {
		logger.Error("Failed to create pool", slog.String("error", err.Error()))
		return &gcpgenserver.V1betaCreatePoolInternalServerError{}, err
	}
	resp, err := encodePoolV1(convertToPoolV1Beta(created))
	if err != nil {
		return nil, err
	}
	if operationID != "" {
		return &gcpgenserver.OperationV1beta{
			Name:     gcpgenserver.NewOptString(fmt.Sprintf("/v1beta/projects/%s/locations/%s/operations/%s", params.ProjectNumber, params.LocationId, operationID)),
			Response: resp,
			Done:     gcpgenserver.NewOptBool(false),
		}, nil
	}
	return &gcpgenserver.OperationV1beta{}, nil
}

func (h Handler) V1betaDeletePool(ctx context.Context, params gcpgenserver.V1betaDeletePoolParams) (gcpgenserver.V1betaDeletePoolRes, error) {
	// TODO implement me
	panic("implement me")
}

func (h Handler) V1betaGetMultiplePools(ctx context.Context, req *gcpgenserver.PoolIDListV1beta, params gcpgenserver.V1betaGetMultiplePoolsParams) (gcpgenserver.V1betaGetMultiplePoolsRes, error) {
	// TODO implement me
	panic("implement me")
}

func (h Handler) V1betaListPools(ctx context.Context, params gcpgenserver.V1betaListPoolsParams) (gcpgenserver.V1betaListPoolsRes, error) {
	// TODO implement me
	panic("implement me")
}

func (h Handler) V1betaUpdatePool(ctx context.Context, req *gcpgenserver.PoolUpdateV1beta, params gcpgenserver.V1betaUpdatePoolParams) (gcpgenserver.V1betaUpdatePoolRes, error) {
	// TODO implement me
	panic("implement me")
}

func convertToPoolV1Beta(pool *models.Pool) *gcpgenserver.PoolV1beta {
	var deletedAt time.Time
	if pool.DeletedAt != nil {
		deletedAt = *pool.DeletedAt
	}

	var throughputValue float64
	customPerformanceEnabled := false
	var iops int64 = 0
	if (pool.CustomPerformanceParams != nil) && (pool.CustomPerformanceParams.Enabled) {
		customPerformanceEnabled = pool.CustomPerformanceParams.Enabled
		throughputValue = pool.CustomPerformanceParams.Throughput
		iops = pool.CustomPerformanceParams.Iops
	} else {
		throughputValue = pool.TotalThroughputMibps
	}

	return &gcpgenserver.PoolV1beta{
		PoolId:                   gcpgenserver.OptString{Value: pool.UUID},
		CreatedAt:                gcpgenserver.OptDateTime{Value: pool.CreatedAt},
		UpdatedAt:                gcpgenserver.OptDateTime{Value: pool.UpdatedAt},
		DeletedAt:                gcpgenserver.NewOptNilDateTime(deletedAt),
		ResourceId:               pool.Name,
		Network:                  pool.VendorSubNetID,
		AllocatedBytes:           gcpgenserver.NewOptNilFloat64(pool.AllocatedBytes),
		SizeInBytes:              float64(pool.SizeInBytes),
		TotalThroughputMibps:     gcpgenserver.NewOptNilFloat64(throughputValue),
		StateDetails:             gcpgenserver.OptString{Value: pool.StateDetails},
		ServiceLevel:             gcpgenserver.PoolV1betaServiceLevel(pool.ServiceLevel),
		TotalIops:                gcpgenserver.NewOptNilFloat64(float64(iops)),
		CustomPerformanceEnabled: gcpgenserver.NewOptBool(customPerformanceEnabled),
	}
}

// encodePoolV1 encodes a PoolV1 struct to JSON.
func encodePoolV1(pool *gcpgenserver.PoolV1beta) (jx.Raw, error) {
	data, err := json.Marshal(pool)
	if err != nil {
		return nil, err
	}
	return data, nil
}
