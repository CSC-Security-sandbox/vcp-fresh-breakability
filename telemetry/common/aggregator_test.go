package common

import (
	"context"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	datamodel2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/datamodel"
)

// Helper function to convert HydratedMetrics to DataPoint for testing
func hydratedMetricsToDataPoints(metrics []datamodel2.HydratedMetrics) []DataPoint {
	var dataPoints []DataPoint
	for _, metric := range metrics {
		dataPoints = append(dataPoints, DataPoint{
			Timestamp: metric.MetricTimestamp,
			Quantity:  metric.Quantity,
		})
	}
	// Sort data points by timestamp as required by aggregation functions
	sort.Slice(dataPoints, func(i, j int) bool {
		return dataPoints[i].Timestamp.Before(dataPoints[j].Timestamp)
	})
	return dataPoints
}

func TestIntegral(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name     string
		metrics  []datamodel2.HydratedMetrics
		expected float64
	}{
		{
			name:     "empty metrics",
			metrics:  []datamodel2.HydratedMetrics{},
			expected: 0,
		},
		{
			name: "single metric",
			metrics: []datamodel2.HydratedMetrics{
				{
					MetricTimestamp: now,
					Quantity:        100,
				},
			},
			expected: 0,
		},
		{
			name: "two metrics",
			metrics: []datamodel2.HydratedMetrics{
				{
					MetricTimestamp: now.Add(-1 * time.Hour),
					Quantity:        100,
				},
				{
					MetricTimestamp: now,
					Quantity:        200,
				},
			},
			expected: 200, // 200 * 1 hour = 200 (uses second value * duration)
		},
		{
			name: "three metrics unsorted",
			metrics: []datamodel2.HydratedMetrics{
				{
					MetricTimestamp: now,
					Quantity:        300,
				},
				{
					MetricTimestamp: now.Add(-2 * time.Hour),
					Quantity:        100,
				},
				{
					MetricTimestamp: now.Add(-1 * time.Hour),
					Quantity:        200,
				},
			},
			expected: 500, // After sorting: 200 * 1 hour + 300 * 1 hour = 500 (uses each subsequent value * duration)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Integral(hydratedMetricsToDataPoints(tt.metrics))
			assert.InDelta(t, tt.expected, got, 0.001, "Integral calculation did not match expected value")
		})
	}
}

func TestCounter(t *testing.T) {
	now := time.Now()
	logger := util.GetLogger(context.Background())
	tests := []struct {
		name     string
		metrics  []datamodel2.HydratedMetrics
		expected float64
	}{
		{
			name:     "empty metrics",
			metrics:  []datamodel2.HydratedMetrics{},
			expected: 0,
		},
		{
			name: "single metric",
			metrics: []datamodel2.HydratedMetrics{
				{
					MetricTimestamp: now,
					Quantity:        100,
				},
			},
			expected: 0,
		},
		{
			name: "two metrics",
			metrics: []datamodel2.HydratedMetrics{
				{
					MetricTimestamp: now.Add(-1 * time.Hour),
					Quantity:        100,
				},
				{
					MetricTimestamp: now,
					Quantity:        250,
				},
			},
			expected: 150, // 250 - 100 = 150
		},
		{
			name: "three metrics unsorted",
			metrics: []datamodel2.HydratedMetrics{
				{
					MetricTimestamp: now,
					Quantity:        300,
				},
				{
					MetricTimestamp: now.Add(-2 * time.Hour),
					Quantity:        50,
				},
				{
					MetricTimestamp: now.Add(-1 * time.Hour),
					Quantity:        150,
				},
			},
			expected: 250, // 300 - 50 = 250
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CounterDelta(hydratedMetricsToDataPoints(tt.metrics), logger)
			assert.InDelta(t, tt.expected, got, 0.001, "Counter calculation did not match expected value")
		})
	}
}

func TestSum(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name     string
		metrics  []datamodel2.HydratedMetrics
		expected float64
	}{
		{
			name:     "empty metrics",
			metrics:  []datamodel2.HydratedMetrics{},
			expected: 0,
		},
		{
			name: "single metric",
			metrics: []datamodel2.HydratedMetrics{
				{
					MetricTimestamp: now,
					Quantity:        100,
				},
			},
			expected: 100,
		},
		{
			name: "multiple metrics",
			metrics: []datamodel2.HydratedMetrics{
				{
					MetricTimestamp: now.Add(-2 * time.Hour),
					Quantity:        50,
				},
				{
					MetricTimestamp: now.Add(-1 * time.Hour),
					Quantity:        150,
				},
				{
					MetricTimestamp: now,
					Quantity:        300,
				},
			},
			expected: 500, // 50 + 150 + 300 = 500
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Sum(hydratedMetricsToDataPoints(tt.metrics))
			assert.InDelta(t, tt.expected, got, 0.001, "Sum calculation did not match expected value")
		})
	}
}

func TestFirst(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name     string
		metrics  []datamodel2.HydratedMetrics
		expected float64
	}{
		{
			name:     "empty metrics",
			metrics:  []datamodel2.HydratedMetrics{},
			expected: 0,
		},
		{
			name: "single metric",
			metrics: []datamodel2.HydratedMetrics{
				{
					MetricTimestamp: now,
					Quantity:        100,
				},
			},
			expected: 100,
		},
		{
			name: "multiple metrics sorted",
			metrics: []datamodel2.HydratedMetrics{
				{
					MetricTimestamp: now.Add(-2 * time.Hour),
					Quantity:        50,
				},
				{
					MetricTimestamp: now.Add(-1 * time.Hour),
					Quantity:        150,
				},
				{
					MetricTimestamp: now,
					Quantity:        300,
				},
			},
			expected: 50, // First (earliest) value is 50
		},
		{
			name: "multiple metrics unsorted",
			metrics: []datamodel2.HydratedMetrics{
				{
					MetricTimestamp: now,
					Quantity:        300,
				},
				{
					MetricTimestamp: now.Add(-2 * time.Hour),
					Quantity:        50,
				},
				{
					MetricTimestamp: now.Add(-1 * time.Hour),
					Quantity:        150,
				},
			},
			expected: 50, // First (earliest) value is 50
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := First(hydratedMetricsToDataPoints(tt.metrics))
			assert.InDelta(t, tt.expected, got, 0.001, "First calculation did not match expected value")
		})
	}
}

func TestCounterDelta_CounterReset(t *testing.T) {
	logger := util.GetLogger(context.Background())
	now := time.Now()
	tests := []struct {
		name     string
		metrics  []datamodel2.HydratedMetrics
		expected float64
	}{
		{
			name: "counter reset - quantity drops below 25% threshold",
			metrics: []datamodel2.HydratedMetrics{
				{
					MetricTimestamp: now.Add(-2 * time.Hour),
					Quantity:        1000,
				},
				{
					MetricTimestamp: now.Add(-1 * time.Hour),
					Quantity:        200, // This should trigger counter reset (200 < 1000 * 0.25)
				},
				{
					MetricTimestamp: now,
					Quantity:        300,
				},
			},
			expected: 300, // Only counts from reset point: 200 (reset value) + (300-200) = 300
		},
		{
			name: "anomalous dip - quantity drops but above 25% threshold",
			metrics: []datamodel2.HydratedMetrics{
				{
					MetricTimestamp: now.Add(-2 * time.Hour),
					Quantity:        1000,
				},
				{
					MetricTimestamp: now.Add(-1 * time.Hour),
					Quantity:        500, // This is above 25% threshold (1000 * 0.25 = 250), so skip this point
				},
				{
					MetricTimestamp: now,
					Quantity:        1200,
				},
			},
			expected: 200, // Only counts 1200 - 1000 = 200, skips the anomalous 500 value
		},
		{
			name: "negative quantity difference but not counter reset",
			metrics: []datamodel2.HydratedMetrics{
				{
					MetricTimestamp: now.Add(-1 * time.Hour),
					Quantity:        1000,
				},
				{
					MetricTimestamp: now,
					Quantity:        800, // 800 > 1000 * 0.25 = 250, so this is anomalous dip, skip it
				},
			},
			expected: 0, // No valid delta computed since anomalous point was skipped
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CounterDelta(hydratedMetricsToDataPoints(tt.metrics), logger)
			assert.InDelta(t, tt.expected, got, 0.001, "CounterDelta with reset scenario did not match expected value")
		})
	}
}
