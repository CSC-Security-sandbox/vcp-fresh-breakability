package common

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/utils"
)

type VCPProcessor interface {
	ProcessPerformanceMetrics(ctx context.Context) error
	ProcessUsageMetrics(ctx context.Context) error
	CollectMetrics(ctx context.Context, projectId string) error
	ProcessBizOps(ctx context.Context, params *utils.BizOpsReportParams) error
}
