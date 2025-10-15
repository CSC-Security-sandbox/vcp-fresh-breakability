package processor

import (
	"context"
	"time"

	metricdb "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/metrics"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/aggregator"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/bizops"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/collector"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/entity"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/performance"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

type MetricsProcessor struct {
	common.VCPProcessor
	vcpDatastore         database.Storage
	telemetryDatastore   metricdb.Storage
	sink                 performance.Sink
	googleMetricProvider collector.VolumeMetricsProvider
	billingProvider      *aggregator.BillingProvider
	bizopsProvider       bizops.BizOpsProvider
}

func NewMetricsProcessor(
	vcpDatastore database.Storage, telemetryDatastore metricdb.Storage,
	sink performance.Sink, metricsProvider collector.VolumeMetricsProvider,
	billingProvider *aggregator.BillingProvider, bizopsProvider bizops.BizOpsProvider,
) MetricsProcessor {
	return MetricsProcessor{vcpDatastore: vcpDatastore, telemetryDatastore: telemetryDatastore, sink: sink, googleMetricProvider: metricsProvider, billingProvider: billingProvider, bizopsProvider: bizopsProvider}
}

func (mp *MetricsProcessor) ProcessPerformanceMetrics(ctx context.Context) error {
	logger := util.GetLogger(ctx)
	telemetryConfig := common.LoadConfig()
	logger.Infof("Process %s!\n", "Performance Metrics")
	currentTimestamp := time.Now().Truncate(time.Minute)
	// Run metrics collection and processing in a goroutine
	go func() {
		// Create new context with correlation ID for async operation
		asyncCtx := context.WithValue(context.Background(), middleware.CorrelationContextKey, ctx.Value(middleware.CorrelationContextKey))
		asyncLogger := util.GetLogger(asyncCtx)

		// Collect and process all metrics
		allHydratedMetricsDataModel, allHydratedMetrics, err := mp.collectAndProcessMetrics(asyncCtx, telemetryConfig, currentTimestamp)
		if err != nil {
			asyncLogger.Errorf("Failed to collect and process metrics: %v", err)
			return
		}

		// Store metrics in database
		if mp.telemetryDatastore != nil {
			if err := mp.telemetryDatastore.CreateHydratedMetricsBatch(asyncCtx, allHydratedMetricsDataModel, int(telemetryConfig.PushBatchSize)); err != nil {
				asyncLogger.Errorf("Failed to insert hydrated metrics batch: %v", err)
				return
			}
		} else {
			asyncLogger.Error("TelemetryDatastore is nil, cannot store metrics")
		}

		// Deliver metrics to sink
		asyncLogger.Infof("Starting to deliver %d metrics to sink", len(allHydratedMetrics))
		if mp.sink != nil {
			mp.sink.DeliverMetrics(asyncCtx, allHydratedMetrics)
		} else {
			asyncLogger.Error("Sink is nil, cannot deliver metrics")
		}
	}()

	// Process raw metrics asynchronously if enabled
	if telemetryConfig.EnableVolumeMetrics {
		go func(ctx context.Context) {
			metricClient := mp.googleMetricProvider.GetClient()
			if metricClient == nil {
				logger.Error("Metric client is nil")
				return
			}

			asyncCtx := context.WithValue(context.Background(), middleware.CorrelationContextKey, ctx.Value(middleware.CorrelationContextKey))
			mp.processRawMetrics(asyncCtx, currentTimestamp)
		}(ctx)
	}
	return nil
}

// collectAndProcessMetrics collects metrics from all sources and aggregates them
func (mp *MetricsProcessor) collectAndProcessMetrics(ctx context.Context, telemetryConfig *common.TelemetryConfig, timestamp time.Time) ([]datamodel.HydratedMetrics, []entity.HydratedMetric, error) {
	logger := util.GetLogger(ctx)

	// hydrated metrics data model for database storage
	var allHydratedMetricsDataModel []datamodel.HydratedMetrics

	// hydrated metrics for sink delivery
	var allHydratedMetrics []entity.HydratedMetric

	// Collect pool metrics
	poolMetricsResult, err := collector.GetPoolMetrics(ctx, mp.vcpDatastore, telemetryConfig, timestamp)
	if err != nil {
		logger.Error("Failed to get pool metrics", "error", err.Error())
		return nil, nil, err
	}

	logger.Infof("Pool metrics collected %d", len(poolMetricsResult.HydratedMetrics))

	// Collect backup metrics if enabled
	var backupMetricsResult *collector.BackupMetricsResult
	if telemetryConfig.EnableBackupMetrics || telemetryConfig.EnableBackupBillingMetrics {
		backupMetricsResult, err = collector.GetBackupMetrics(ctx, mp.vcpDatastore, telemetryConfig, timestamp)
		if err != nil {
			logger.Error("Failed to get backup metrics", "error", err.Error())
			return nil, nil, err
		}
	}

	// Collect volume metrics
	volumeMetricsResult, err := collector.GetVolumeMetrics(ctx, mp.vcpDatastore, telemetryConfig, poolMetricsResult.PoolMetadataMap, timestamp)
	if err != nil {
		logger.Error("Failed to get volume metrics", "error", err.Error())
		return nil, nil, err
	}

	// Aggregate backup metrics for sink delivery
	if telemetryConfig.EnableBackupMetrics {
		allHydratedMetrics = append(allHydratedMetrics, backupMetricsResult.HydratedMetrics...)
	}

	// Aggregate backup and volume metrics for database storage
	if telemetryConfig.EnableBackupBillingMetrics {
		allHydratedMetricsDataModel = append(allHydratedMetricsDataModel, backupMetricsResult.HydratedMetricsDataModel...)
		allHydratedMetricsDataModel = append(allHydratedMetricsDataModel, volumeMetricsResult.HydratedMetricsDataModel...)
	}

	// Aggregate pool metrics for database storage
	allHydratedMetricsDataModel = append(allHydratedMetricsDataModel, poolMetricsResult.HydratedMetricsDataModel...)

	// Aggregate pool and volume metrics for sink delivery
	allHydratedMetrics = append(allHydratedMetrics, poolMetricsResult.HydratedMetrics...)
	allHydratedMetrics = append(allHydratedMetrics, volumeMetricsResult.VolumeAllocatedThroughputHydratedMetrics...)

	return allHydratedMetricsDataModel, allHydratedMetrics, nil
}

func (mp *MetricsProcessor) processRawMetrics(ctx context.Context, timestamp time.Time) {
	logger := util.GetLogger(ctx)
	logger.Infof("Processing Raw Metrics")
	mp.googleMetricProvider.RefreshTimeWindow()
	err := collector.CollectVolumeMetrics(ctx, logger, mp.googleMetricProvider, timestamp)
	if err != nil {
		logger.Errorf("CollectRawMetrics failed: %v", err)
		return
	}
}

func (mp *MetricsProcessor) ProcessUsageMetrics(ctx context.Context) error {
	logger := util.GetLogger(ctx)
	logger.Infof("Process %s!\n", "Usage Metrics")
	// Shift the aggregation cycle 15 mins prior from the aggregation trigger, to avoid 1 missed sample intermittently.
	// If aggregation is triggered at 1:45 -> It needs to aggregate data from 12:45 to 1:45. But as the current timestamp in also ~1:45.
	// There is a chance that the collection cycle is still running and record for 1:45 is still not hydrated. So aggregation will miss that record and will result into under billing.
	// With this approach, aggregator will aggregate data from 12:30 to 1:30 (This will make sure that 1:30 sample is always available as collection for 1:30 is already finished by 1:45).
	aggregationEndTime := time.Now().Add(-15 * time.Minute)
	err := mp.billingProvider.ProcessBillingMetrics(ctx, aggregationEndTime)
	if err != nil {
		logger.Error("Failed to aggregate hydrated metrics", "error", err.Error())
		return err
	}
	return nil
}

func (mp *MetricsProcessor) CollectMetrics(ctx context.Context, projectId string, timestamp time.Time) error {
	telemetryConfig := common.LoadConfig()
	logger := util.GetLogger(ctx)
	results, err := mp.googleMetricProvider.CollectProjectMetrics(ctx, logger, projectId, timestamp)
	if err != nil {
		logger.Error("Failed to get project metrics", "error", err.Error())
		return err
	}
	if err := mp.telemetryDatastore.CreateHydratedMetricsBatch(ctx, results, int(telemetryConfig.PushBatchSize)); err != nil {
		logger.Errorf("Failed to insert hydrated metrics batch: %v", err)
		return err
	}
	logger.Debugf("Successfully collected %d metrics %v", len(results), projectId)
	return nil
}

func (mp *MetricsProcessor) ProcessBizOps(ctx context.Context, params *utils.BizOpsReportParams) error {
	logger := util.GetLogger(ctx)
	err := mp.bizopsProvider.ProcessBizOps(ctx, logger, params)
	if err != nil {
		logger.Error("Failed to process BizOps metrics", "error", err.Error())
		return err
	}
	logger.Infof("Successfully processed BizOps Report for start time: '%v' with timezone:'%s' ", params.StartDate, params.TimeZone)
	return nil
}
