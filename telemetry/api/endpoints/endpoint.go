package api

import (
	"context"
	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/api/telemetry-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/performance"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/usage"
)

type Handler struct {
	oasgenserver.UnimplementedHandler
}

func (h Handler) V1Performance(ctx context.Context) (r oasgenserver.V1PerformanceRes, _ error) {
	go performance.ProcessPerformanceMetrics()
	return &oasgenserver.V1PerformanceAccepted{}, nil
}

func (h Handler) V1Usage(ctx context.Context) (r oasgenserver.V1UsageRes, _ error) {
	go usage.ProcessUsageMetrics()
	return &oasgenserver.V1UsageAccepted{}, nil
}
