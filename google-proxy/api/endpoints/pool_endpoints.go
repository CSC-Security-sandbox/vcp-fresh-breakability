package api

import (
	"context"
	"golang.org/x/exp/slog"

	coreapiModel "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/core-api/core-servergen"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
)

func (h Handler) V1betaDescribePool(ctx context.Context, params gcpgenserver.V1betaDescribePoolParams) (gcpgenserver.V1betaDescribePoolRes, error) {

	//logger := ctx.Value(common.ContextSLoggerKey).(*slog.Logger)
	logger := &slog.Logger{}
	poolHandler := h.CoreHandler

	param := coreapiModel.V1GetPoolParams{
		ProjectNumber: params.ProjectNumber,
		LocationId:    params.LocationId,
		PoolId:        params.PoolId,
	}
	_, err := poolHandler.V1betaDescribePool(ctx, param)
	if err != nil {
		logger.Error("Failed to describe pool", slog.String("error", err.Error()))
		return &gcpgenserver.V1betaDescribePoolInternalServerError{}, err
	}
	//return &gcpgenserver.PoolV1beta{
	//	Description: gcpgenserver.OptNilString{Value: "Pool description"},
	//	PoolId:      gcpgenserver.OptString{Value: params.PoolId},
	//}, nil
	return nil, nil
}

func (h Handler) V1betaCreatePool(ctx context.Context, req *gcpgenserver.PoolV1beta, params gcpgenserver.V1betaCreatePoolParams) (gcpgenserver.V1betaCreatePoolRes, error) {
	//TODO implement me
	panic("implement me")
}

func (h Handler) V1betaDeletePool(ctx context.Context, params gcpgenserver.V1betaDeletePoolParams) (gcpgenserver.V1betaDeletePoolRes, error) {
	//TODO implement me
	panic("implement me")
}

func (h Handler) V1betaGetMultiplePools(ctx context.Context, req *gcpgenserver.PoolIDListV1beta, params gcpgenserver.V1betaGetMultiplePoolsParams) (gcpgenserver.V1betaGetMultiplePoolsRes, error) {
	//TODO implement me
	panic("implement me")
}

func (h Handler) V1betaListPools(ctx context.Context, params gcpgenserver.V1betaListPoolsParams) (gcpgenserver.V1betaListPoolsRes, error) {
	//TODO implement me
	panic("implement me")
}

func (h Handler) V1betaUpdatePool(ctx context.Context, req *gcpgenserver.PoolUpdateV1beta, params gcpgenserver.V1betaUpdatePoolParams) (gcpgenserver.V1betaUpdatePoolRes, error) {
	//TODO implement me
	panic("implement me")
}
