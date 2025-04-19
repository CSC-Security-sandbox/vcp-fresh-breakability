package api

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
)

func (h Handler) V1betaDescribePool(ctx context.Context, params gcpgenserver.V1betaDescribePoolParams) (gcpgenserver.V1betaDescribePoolRes, error) {
	return orchestrator.ListPool(ctx, params, h.Orchestrator)
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
