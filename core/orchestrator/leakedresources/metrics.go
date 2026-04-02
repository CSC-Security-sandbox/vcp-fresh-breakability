package leakedresources

import (
	"context"
	"sync"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/model"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	leakedResourcesMeterName = "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources"
	leakedResourcesCountName = "vcp.leaked_resources.count"
	monitoringRunsName       = "vcp.leaked_resources.monitoring_runs"
)

type leakedCountKey struct {
	resourceType string
	reason       string
	projectID    string
	region       string
}

type leakedCountObservation struct {
	count int64
	attrs []attribute.KeyValue
}

var (
	leakedMetricsInitOnce sync.Once
	leakedMetricsMu       sync.RWMutex
	leakedCountsByKey     = map[leakedCountKey]int64{}

	monitoringRunsCounter metric.Int64Counter
	metricsInitLogger     = log.NewLogger()
)

type MetricsReporter struct{}

type MultiReporter struct {
	reporters []Reporter
}

func NewMetricsReporter() *MetricsReporter {
	initLeakedResourceMetrics()
	return &MetricsReporter{}
}

func NewMultiReporter(reporters ...Reporter) *MultiReporter {
	filtered := make([]Reporter, 0, len(reporters))
	for _, r := range reporters {
		if r != nil {
			filtered = append(filtered, r)
		}
	}
	return &MultiReporter{reporters: filtered}
}

func (r *MultiReporter) Report(ctx context.Context, records []model.LeakRecord) error {
	for _, reporter := range r.reporters {
		if err := reporter.Report(ctx, records); err != nil {
			return err
		}
	}
	return nil
}

func (r *MetricsReporter) Report(ctx context.Context, records []model.LeakRecord) error {
	logger := metricsInitLogger
	if ctx != nil {
		logger = util.GetLogger(ctx)
	}
	initLeakedResourceMetrics()
	updateLeakedCounts(records)
	series := currentLeakCountObservations()
	logger.Infof("Leaked resources metrics updated (records=%d, series=%d)", len(records), len(series))
	return nil
}

func recordMonitoringRun(ctx context.Context, status string) {
	logger := metricsInitLogger
	if ctx != nil {
		logger = util.GetLogger(ctx)
	}
	initLeakedResourceMetrics()
	if status == "" {
		status = "error"
	}
	if monitoringRunsCounter != nil {
		recordCtx := ctx
		if recordCtx == nil || recordCtx.Err() != nil {
			recordCtx = context.Background()
		}
		monitoringRunsCounter.Add(recordCtx, 1, metric.WithAttributes(attribute.String("status", status)))
		logger.Infof("Leaked resources monitoring run metric recorded (status=%s)", status)
		return
	}
	logger.Warnf("Leaked resources monitoring run metric not recorded: counter is not initialized (status=%s)", status)
}

func initLeakedResourceMetrics() {
	leakedMetricsInitOnce.Do(func() {
		meter := otel.Meter(leakedResourcesMeterName)
		countGauge, err := meter.Int64ObservableGauge(
			leakedResourcesCountName,
			metric.WithDescription("Number of leaked resources at the last report run, by resource_type and reason."),
			metric.WithUnit("{resource}"),
		)
		if err != nil {
			metricsInitLogger.Error("Failed to create leaked resources count gauge", "error", err)
		} else {
			_, callbackErr := meter.RegisterCallback(func(ctx context.Context, observer metric.Observer) error {
				_ = ctx
				for _, observation := range currentLeakCountObservations() {
					observer.ObserveInt64(countGauge, observation.count, metric.WithAttributes(observation.attrs...))
				}
				return nil
			}, countGauge)
			if callbackErr != nil {
				metricsInitLogger.Error("Failed to register leaked resources count callback", "error", callbackErr)
			}
		}

		monitoringRunsCounter, err = meter.Int64Counter(
			monitoringRunsName,
			metric.WithDescription("Total number of leaked resources monitoring runs."),
			metric.WithUnit("{run}"),
		)
		if err != nil {
			metricsInitLogger.Error("Failed to create leaked resources monitoring runs counter", "error", err)
		}
	})
}

func updateLeakedCounts(records []model.LeakRecord) {
	next := make(map[leakedCountKey]int64)
	for _, record := range records {
		key := leakedCountKey{
			resourceType: string(record.ResourceType),
			reason:       record.Reason,
			projectID:    record.ProjectID,
			region:       record.Region,
		}
		next[key]++
	}

	leakedMetricsMu.Lock()
	leakedCountsByKey = next
	leakedMetricsMu.Unlock()
}

func currentLeakCountObservations() []leakedCountObservation {
	leakedMetricsMu.RLock()
	defer leakedMetricsMu.RUnlock()

	observations := make([]leakedCountObservation, 0, len(leakedCountsByKey))
	for key, count := range leakedCountsByKey {
		attrs := []attribute.KeyValue{
			attribute.String("resource_type", key.resourceType),
			attribute.String("reason", key.reason),
		}
		if key.projectID != "" {
			attrs = append(attrs, attribute.String("project_id", key.projectID))
		}
		if key.region != "" {
			attrs = append(attrs, attribute.String("region", key.region))
		}
		observations = append(observations, leakedCountObservation{
			count: count,
			attrs: attrs,
		})
	}
	return observations
}
