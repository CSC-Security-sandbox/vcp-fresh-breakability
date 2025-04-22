package api

import (
	"context"
	"fmt"

	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/core-api/core-servergen"
)

func (h Handler) V1betaDescribePool(ctx context.Context, params oasgenserver.V1GetPoolParams) (oasgenserver.V1GetPoolRes, error) {
	fmt.Println("V1betaDescribePool")
	fmt.Println(params.PoolId, ' ', params.LocationId, ' ', params.ProjectNumber)

	return &oasgenserver.V1GetPoolNotFound{
		Message: "Something went wrong",
		Code:    404,
	}, nil
}
