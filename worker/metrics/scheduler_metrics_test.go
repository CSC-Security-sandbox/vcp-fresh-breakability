package metrics

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric/noop"
)

func resetMetricsState(t *testing.T) {
	t.Helper()

	initOnce = sync.Once{}
	taskRunCounter = nil
	taskErrorCounter = nil
	meter = nil

	otel.SetMeterProvider(noop.NewMeterProvider())
}

func TestRegisterBackgroundTaskSchedulerMetrics(t *testing.T) {
	resetMetricsState(t)

	RegisterBackgroundTaskSchedulerMetrics()

	require.NotNil(t, taskRunCounter)
	require.NotNil(t, taskErrorCounter)
}

func TestIncBackgroundTaskRunAutoRegisters(t *testing.T) {
	resetMetricsState(t)

	assert.NotPanics(t, func() {
		IncBackgroundTaskRun("test-task")
	})

	require.NotNil(t, taskRunCounter)
}

func TestIncBackgroundTaskErrorAutoRegisters(t *testing.T) {
	resetMetricsState(t)

	assert.NotPanics(t, func() {
		IncBackgroundTaskError("test-task", "failure")
	})

	require.NotNil(t, taskErrorCounter)
}
