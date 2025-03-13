package api

import (
	"context"
	"fmt"

	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/core-api/api/servergen"
)

func (h Handler) V1betaDescribePool(ctx context.Context, params oasgenserver.V1betaDescribePoolParams) (oasgenserver.V1betaDescribePoolRes, error) {
	fmt.Println("V1betaDescribePool")
	fmt.Println(params.PoolId, ' ', params.LocationId, ' ', params.ProjectNumber)

	return &oasgenserver.V1betaDescribePoolNotFound{
		Message: "Something went wrong",
		Code:    404,
	}, nil
}
