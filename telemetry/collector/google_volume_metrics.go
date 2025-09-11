package collector

import (
	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"context"
	"errors"
	"fmt"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"sync"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var CollectVolumeMetrics = collectVolumeMetrics

func (g *GoogleTenantProjectProvider) GetTenantProjects(ctx context.Context, logger log.Logger) ([]string, error) {
	return GetTenantProject(ctx, logger, g.vcpDatastore)
}

func (g *GoogleVolumeMetricsProvider) GetVolumeMetrics(ctx context.Context, logger log.Logger) ([]datamodel.HydratedMetrics, error) {
	var results []datamodel.HydratedMetrics
	var mu sync.Mutex
	var wg sync.WaitGroup

	projects, err := g.tenantProjectProvider.GetTenantProjects(ctx, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to get tenant projects: %v", err)
	}

	errChan := make(chan error, len(projects))

	for _, projectID := range projects {
		wg.Add(1)
		go func(pid string) {
			defer wg.Done()

			projectResults, err := g.collectProjectMetrics(ctx, logger, pid)
			if err != nil {
				errChan <- fmt.Errorf("failed to collect metrics for project %s: %v", pid, err)
				return
			}

			mu.Lock()
			results = append(results, projectResults...)
			mu.Unlock()
		}(projectID)
	}

	wg.Wait()
	close(errChan)

	if len(errChan) > 0 {
		return nil, <-errChan
	}

	logger.Infof("Found %d Metrics ", len(results))
	return results, nil
}

func (g *GoogleVolumeMetricsProvider) collectProjectMetrics(ctx context.Context, logger log.Logger, projectID string) ([]datamodel.HydratedMetrics, error) {
	var projectResults []datamodel.HydratedMetrics
	projectName := fmt.Sprintf("projects/%s", projectID)
	telemetryConfig := common.LoadConfig()
	for _, metric := range g.metrics {
		filter := fmt.Sprintf(`metric.type="%s/%s"`, metric.ResourceType, metric.Metric)
		req := &monitoringpb.ListTimeSeriesRequest{
			Name:   projectName,
			Filter: filter,
			Interval: &monitoringpb.TimeInterval{
				StartTime: timestamppb.New(g.startTime),
				EndTime:   timestamppb.New(g.endTime),
			},
			View:     monitoringpb.ListTimeSeriesRequest_FULL,
			PageSize: telemetryConfig.PageSize,
		}

		it := g.client.ListTimeSeries(ctx, req)
		for {
			resp, err := it.Next()
			if errors.Is(err, iterator.Done) {
				break
			}
			if err != nil {
				if status.Code(err) == codes.NotFound {
					break
				}
				logger.Errorf("Error retrieving time series data for metric %s in project %s: %v", metric, projectID, err)
				break
			}
			if len(resp.Points) == 0 {
				logger.Warnf("No data points found for metric %s in project %s for region %s", metric, projectID, resp.Resource.Labels["location"])
				continue
			}
			measuredType, exists := metadata.NewMeasuredType(metric.Metric)
			if !exists {
				logger.Warnf("Unknown measured type for metric %s", metric)
				continue
			}
			resourceType := metadata.CombinedKeyResourceTypeMeasuredTypeMap[metric.Metric].ResourceType
			metrics := setupHydratedMetrics(measuredType, resourceType, projectID, resp)
			projectResults = append(projectResults, metrics)
		}
	}
	return projectResults, nil
}

func GetTenantProject(ctx context.Context, logger log.Logger, vcpDatastore database.Storage) ([]string, error) {
	projects, err := vcpDatastore.ListTpProjects(ctx)
	if err != nil {
		logger.Error("Failed to list SnHostsProjects", "error", err.Error())
		return nil, fmt.Errorf("failed to list SnHostsProjects: %v", err)
	}
	if len(projects) == 0 {
		return nil, fmt.Errorf("no projects found from DB")
	}

	return projects, nil
}

func collectVolumeMetrics(ctx context.Context, logger log.Logger, provider VolumeMetricsProvider) ([]datamodel.HydratedMetrics, error) {
	return provider.GetVolumeMetrics(ctx, logger)
}

func setupHydratedMetrics(measuredType metadata.MeasuredType, resourceType metadata.ResourceType, projectID string, resp *monitoringpb.TimeSeries) datamodel.HydratedMetrics {
	return datamodel.HydratedMetrics{
		MetricTimestamp: resp.Points[0].Interval.EndTime.AsTime(),
		MeasuredType:    measuredType,
		ConsumerID:      resp.Metric.Labels["project"],
		ResourceType:    resourceType,
		ResourceName:    resp.Metric.Labels["volume"],
		Location:        resp.Metric.Labels["datacenter"],
		Quantity:        extractValue(resp.Points[0].Value),
	}
}

func extractValue(Value *monitoringpb.TypedValue) float64 {
	var quantity float64
	switch v := Value.Value.(type) {
	case *monitoringpb.TypedValue_DoubleValue:
		quantity = v.DoubleValue
	case *monitoringpb.TypedValue_Int64Value:
		quantity = float64(v.Int64Value)
	case *monitoringpb.TypedValue_BoolValue:
		if v.BoolValue {
			quantity = 1.0
		} else {
			quantity = 0.0
		}
	default:
		quantity = 0
	}
	return quantity
}
