package metrics

import (
	"context"
	"log"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var (
	initOnce sync.Once

	meter              metric.Meter
	taskRunCounter     metric.Int64Counter
	taskErrorCounter   metric.Int64Counter
	taskLastRunGauge   metric.Int64ObservableGauge
	taskLastRunMu      sync.RWMutex
	taskLastRunSeconds = map[string]int64{}
)

// RegisterBackgroundTaskSchedulerMetrics configures the OpenTelemetry counters used by the scheduler.
func RegisterBackgroundTaskSchedulerMetrics() {
	initOnce.Do(func() {
		meter = otel.Meter("github.com/vcp-vsa-control-Plane/vsa-control-plane/worker/metrics")

		var err error

		taskRunCounter, err = meter.Int64Counter(
			"vcp.background.task.runs",
			metric.WithDescription("Total number of background task executions"),
		)
		if err != nil {
			log.Printf("Failed to create background task run counter: %v", err)
		}

		taskErrorCounter, err = meter.Int64Counter(
			"vcp.background.task.errors",
			metric.WithDescription("Total number of background task errors partitioned by task and reason"),
		)
		if err != nil {
			log.Printf("Failed to create background task error counter: %v", err)
		}

		taskLastRunGauge, err = meter.Int64ObservableGauge(
			"vcp.background.task.last_run_timestamp",
			metric.WithDescription("Unix timestamp of the most recent background task execution"),
		)
		if err != nil {
			log.Printf("Failed to create background task last run gauge: %v", err)
			return
		}

		_, err = meter.RegisterCallback(func(ctx context.Context, observer metric.Observer) error {
			taskLastRunMu.RLock()
			defer taskLastRunMu.RUnlock()

			for task, ts := range taskLastRunSeconds {
				observer.ObserveInt64(
					taskLastRunGauge,
					ts,
					metric.WithAttributes(attribute.String("task", task)),
				)
			}
			return nil
		}, taskLastRunGauge)
		if err != nil {
			log.Printf("Failed to register background task last run callback: %v", err)
		}
	})
}

// IncBackgroundTaskRun increments the run counter for the specified task and records the last run timestamp.
func IncBackgroundTaskRun(task string) {
	if taskRunCounter == nil {
		RegisterBackgroundTaskSchedulerMetrics()
	}

	taskRunCounter.Add(context.Background(), 1,
		metric.WithAttributes(attribute.String("task", task)),
	)

	recordBackgroundTaskRun(task)
}

// IncBackgroundTaskError increments the error counter for the specified task and reason.
func IncBackgroundTaskError(task, reason string) {
	if taskErrorCounter == nil {
		RegisterBackgroundTaskSchedulerMetrics()
	}

	taskErrorCounter.Add(context.Background(), 1,
		metric.WithAttributes(
			attribute.String("task", task),
			attribute.String("reason", reason),
		),
	)
}

func recordBackgroundTaskRun(task string) {
	taskLastRunMu.Lock()
	defer taskLastRunMu.Unlock()

	taskLastRunSeconds[task] = time.Now().Unix()
}
