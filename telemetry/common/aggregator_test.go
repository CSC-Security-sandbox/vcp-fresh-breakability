package common

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	datamodel2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/datamodel"
)

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
			got := Integral(tt.metrics)
			assert.InDelta(t, tt.expected, got, 0.001, "Integral calculation did not match expected value")
		})
	}
}

func TestCounter(t *testing.T) {
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
			got := CounterDelta(tt.metrics)
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
			got := Sum(tt.metrics)
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
			got := First(tt.metrics)
			assert.InDelta(t, tt.expected, got, 0.001, "First calculation did not match expected value")
		})
	}
}
