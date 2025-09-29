package processor

import (
	"context"
	"fmt"
	"time"

	metricdb "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/metrics"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/aggregator"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/collector"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/entity"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/performance"
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
}

func NewMetricsProcessor(vcpDatastore database.Storage, telemetryDatastore metricdb.Storage, sink performance.Sink, metricsProvider collector.VolumeMetricsProvider, billingProvider *aggregator.BillingProvider) MetricsProcessor {
	return MetricsProcessor{vcpDatastore: vcpDatastore, telemetryDatastore: telemetryDatastore, sink: sink, googleMetricProvider: metricsProvider, billingProvider: billingProvider}
}

func (mp *MetricsProcessor) ProcessPerformanceMetrics(ctx context.Context) error {
	logger := util.GetLogger(ctx)
	telemetryConfig := common.LoadConfig()
	logger.Infof("Process %s!\n", "Performance Metrics")

	// hydrated metrics data model for database storage
	var allHydratedMetricsDataModel []datamodel.HydratedMetrics

	// hydrated metrics for sink delivery
	var allHydratedMetrics []entity.HydratedMetric

	poolMetricsResult, err := collector.GetPoolMetrics(ctx, mp.vcpDatastore, telemetryConfig)
	if err != nil {
		logger.Error("Failed to get pool metrics", "error", err.Error())
		return err
	}

	var backupMetricsResult *collector.BackupMetricsResult
	if telemetryConfig.EnableBackupMetrics || telemetryConfig.EnableBackupBillingMetrics {
		backupMetricsResult, err = collector.GetBackupMetrics(ctx, mp.vcpDatastore, telemetryConfig)
		if err != nil {
			logger.Error("Failed to get backup metrics", "error", err.Error())
			return err
		}
	}
	volumeMetricsResult, err := collector.GetVolumeMetrics(ctx, mp.vcpDatastore, telemetryConfig, poolMetricsResult.PoolMetadataMap)
	if err != nil {
		logger.Error("Failed to get volume metrics", "error", err.Error())
		return err
	}
	if telemetryConfig.EnableBackupMetrics {
		allHydratedMetrics = append(allHydratedMetrics, backupMetricsResult.HydratedMetrics...)
	}
	if telemetryConfig.EnableBackupBillingMetrics {
		allHydratedMetricsDataModel = append(allHydratedMetricsDataModel, backupMetricsResult.HydratedMetricsDataModel...)
		allHydratedMetricsDataModel = append(allHydratedMetricsDataModel, volumeMetricsResult.HydratedMetricsDataModel...)
	}

	allHydratedMetricsDataModel = append(allHydratedMetricsDataModel, poolMetricsResult.HydratedMetricsDataModel...)

	if err := mp.telemetryDatastore.CreateHydratedMetricsBatch(ctx, allHydratedMetricsDataModel, int(telemetryConfig.PushBatchSize)); err != nil {
		logger.Errorf("Failed to insert hydrated metrics batch: %v", err)
		return err
	}

	allHydratedMetrics = append(allHydratedMetrics, poolMetricsResult.HydratedMetrics...)
	allHydratedMetrics = append(allHydratedMetrics, volumeMetricsResult.VolumeAllocatedThroughputHydratedMetrics...)

	mp.sink.DeliverMetrics(ctx, allHydratedMetrics)
	if telemetryConfig.EnableVolumeMetrics {
		metricClient := mp.googleMetricProvider.GetClient()
		if metricClient == nil {
			logger.Error("Metric client is nil")
			return fmt.Errorf("metric client is nil")
		}

		go func(ctx context.Context) {
			asyncCtx := context.WithValue(context.Background(), middleware.CorrelationContextKey, ctx.Value(middleware.CorrelationContextKey))
			mp.processRawMetrics(asyncCtx)
		}(ctx)
	}
	return nil
}

func (mp *MetricsProcessor) processRawMetrics(ctx context.Context) {
	logger := util.GetLogger(ctx)
	logger.Infof("Processing Raw Metrics")
	mp.googleMetricProvider.RefreshTimeWindow()
	err := collector.CollectVolumeMetrics(ctx, logger, mp.googleMetricProvider)
	if err != nil {
		logger.Errorf("CollectRawMetrics failed: %v", err)
		return
	}
}

func (mp *MetricsProcessor) ProcessUsageMetrics(ctx context.Context) error {
	logger := util.GetLogger(ctx)
	logger.Infof("Process %s!\n", "Usage Metrics")

	err := mp.billingProvider.ProcessBillingMetrics(ctx, time.Now())
	if err != nil {
		logger.Error("Failed to aggregate hydrated metrics", "error", err.Error())
		return err
	}
	return nil
}

func (mp *MetricsProcessor) CollectMetrics(ctx context.Context, projectId string) error {
	telemetryConfig := common.LoadConfig()
	logger := util.GetLogger(ctx)
	results, err := mp.googleMetricProvider.CollectProjectMetrics(ctx, logger, projectId)
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
