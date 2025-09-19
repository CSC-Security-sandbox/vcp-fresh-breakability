package common

import "context"

type VCPProcessor interface {
	ProcessPerformanceMetrics(ctx context.Context) error
	ProcessUsageMetrics(ctx context.Context) error
	CollectMetrics(ctx context.Context, projectId string) error
}
