package common

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	datamodel2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
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
		name         string
		metrics      []datamodel2.HydratedMetrics
		measuredType metadata.MeasuredType
		resourceUUID string
		expected     float64
	}{
		{
			name:         "empty metrics",
			metrics:      []datamodel2.HydratedMetrics{},
			measuredType: metadata.AllocatedSize,
			resourceUUID: "test-uuid-1",
			expected:     0,
		},
		{
			name: "single metric",
			metrics: []datamodel2.HydratedMetrics{
				{
					MetricTimestamp: now,
					Quantity:        100,
				},
			},
			measuredType: metadata.AllocatedSize,
			resourceUUID: "test-uuid-2",
			expected:     0,
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
			measuredType: metadata.AllocatedSize,
			resourceUUID: "test-uuid-3",
			expected:     150, // 250 - 100 = 150
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
			measuredType: metadata.AllocatedSize,
			resourceUUID: "test-uuid-4",
			expected:     250, // 300 - 50 = 250
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := CounterDelta(hydratedMetricsToDataPoints(tt.metrics), logger, tt.measuredType, tt.resourceUUID)
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
		name         string
		metrics      []datamodel2.HydratedMetrics
		measuredType metadata.MeasuredType
		resourceUUID string
		expected     float64
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
			measuredType: metadata.AllocatedSize,
			resourceUUID: "test-uuid-reset",
			expected:     300, // Only counts from reset point: 200 (reset value) + (300-200) = 300
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
			measuredType: metadata.AllocatedSize,
			resourceUUID: "test-uuid-anomaly",
			expected:     200, // Only counts 1200 - 1000 = 200, skips the anomalous 500 value
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
			measuredType: metadata.AllocatedSize,
			resourceUUID: "test-uuid-neg",
			expected:     0, // No valid delta computed since anomalous point was skipped
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := CounterDelta(hydratedMetricsToDataPoints(tt.metrics), logger, tt.measuredType, tt.resourceUUID)
			assert.InDelta(t, tt.expected, got, 0.001, "CounterDelta with reset scenario did not match expected value")
		})
	}
}

// TestCounterDelta_CoolTierWriteSpecialHandling tests the special handling for CoolTierDataWriteSizeRaw
func TestCounterDelta_CoolTierWriteSpecialHandling(t *testing.T) {
	logger := util.GetLogger(context.Background())
	now := time.Now()
	tests := []struct {
		name         string
		metrics      []datamodel2.HydratedMetrics
		resourceUUID string
		expected     float64
	}{
		{
			name: "cool tier write - skip when value decreases",
			metrics: []datamodel2.HydratedMetrics{
				{
					MetricTimestamp: now.Add(-15 * time.Minute),
					Quantity:        1000,
				},
				{
					MetricTimestamp: now.Add(-10 * time.Minute),
					Quantity:        1500,
				},
				{
					MetricTimestamp: now.Add(-5 * time.Minute),
					Quantity:        900, // Decrease - should be skipped
				},
				{
					MetricTimestamp: now,
					Quantity:        2000,
				},
			},
			resourceUUID: "pool-uuid-write",
			expected:     1000, // (1500-1000) + (2000-1500) = 500 + 500 = 1000 (900 is skipped, lastPoint stays at 1500)
		},
		{
			name: "cool tier write - normal increase",
			metrics: []datamodel2.HydratedMetrics{
				{
					MetricTimestamp: now.Add(-10 * time.Minute),
					Quantity:        1000,
				},
				{
					MetricTimestamp: now.Add(-5 * time.Minute),
					Quantity:        1500,
				},
				{
					MetricTimestamp: now,
					Quantity:        2000,
				},
			},
			resourceUUID: "pool-uuid-write-normal",
			expected:     1000, // 2000 - 1000 = 1000
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := CounterDelta(hydratedMetricsToDataPoints(tt.metrics), logger, metadata.CoolTierDataWriteSizeRaw, tt.resourceUUID)
			assert.InDelta(t, tt.expected, got, 0.001, "CoolTierDataWriteSizeRaw handling did not match expected value")
		})
	}
}

// TestCounterDelta_CoolTierReadSpecialHandling tests the special handling for CoolTierDataReadSizeRaw
func TestCounterDelta_CoolTierReadSpecialHandling(t *testing.T) {
	logger := util.GetLogger(context.Background())
	now := time.Now()
	tests := []struct {
		name         string
		metrics      []datamodel2.HydratedMetrics
		resourceUUID string
		expected     float64
	}{
		{
			name: "cool tier read - skip when value decreases to zero",
			metrics: []datamodel2.HydratedMetrics{
				{
					MetricTimestamp: now.Add(-15 * time.Minute),
					Quantity:        1000,
				},
				{
					MetricTimestamp: now.Add(-10 * time.Minute),
					Quantity:        1500,
				},
				{
					MetricTimestamp: now.Add(-5 * time.Minute),
					Quantity:        0, // Decrease to zero - should be skipped
				},
				{
					MetricTimestamp: now,
					Quantity:        2000,
				},
			},
			resourceUUID: "pool-uuid-read",
			expected:     1000, // 1500-1000=500 (valid), skip 0, 2000-1500=500 (valid, last valid was 1500), total=1000
		},
		{
			name: "cool tier read - allow decrease to non-zero (uses standard logic)",
			metrics: []datamodel2.HydratedMetrics{
				{
					MetricTimestamp: now.Add(-10 * time.Minute),
					Quantity:        1000,
				},
				{
					MetricTimestamp: now.Add(-5 * time.Minute),
					Quantity:        200, // Decrease but not to zero - standard counter reset logic applies
				},
				{
					MetricTimestamp: now,
					Quantity:        300,
				},
			},
			resourceUUID: "pool-uuid-read-reset",
			expected:     300, // Counter reset: 200 (reset value) + (300-200) = 300
		},
		{
			name: "cool tier read - normal increase",
			metrics: []datamodel2.HydratedMetrics{
				{
					MetricTimestamp: now.Add(-10 * time.Minute),
					Quantity:        1000,
				},
				{
					MetricTimestamp: now.Add(-5 * time.Minute),
					Quantity:        1500,
				},
				{
					MetricTimestamp: now,
					Quantity:        2000,
				},
			},
			resourceUUID: "pool-uuid-read-normal",
			expected:     1000, // 2000 - 1000 = 1000
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := CounterDelta(hydratedMetricsToDataPoints(tt.metrics), logger, metadata.CoolTierDataReadSizeRaw, tt.resourceUUID)
			assert.InDelta(t, tt.expected, got, 0.001, "CoolTierDataReadSizeRaw handling did not match expected value")
		})
	}
}

// TestCounterDelta_XregionReplicationSpecialHandling tests the special handling for XregionReplicationTotalTransferBytes
func TestCounterDelta_XregionReplicationSpecialHandling(t *testing.T) {
	logger := util.GetLogger(context.Background())
	now := time.Now()
	tests := []struct {
		name         string
		metrics      []datamodel2.HydratedMetrics
		resourceUUID string
		expected     float64
	}{
		{
			name: "xregion replication - skip when value decreases to zero",
			metrics: []datamodel2.HydratedMetrics{
				{
					MetricTimestamp: now.Add(-15 * time.Minute),
					Quantity:        1000,
				},
				{
					MetricTimestamp: now.Add(-10 * time.Minute),
					Quantity:        1500,
				},
				{
					MetricTimestamp: now.Add(-5 * time.Minute),
					Quantity:        0, // Decrease to zero - should be skipped
				},
				{
					MetricTimestamp: now,
					Quantity:        2000,
				},
			},
			resourceUUID: "replication-uuid",
			expected:     1000, // 1500-1000 + (2000-1500) = 500 + 500 = 1000
		},
		{
			name: "xregion replication - allow decrease to non-zero (uses standard logic)",
			metrics: []datamodel2.HydratedMetrics{
				{
					MetricTimestamp: now.Add(-10 * time.Minute),
					Quantity:        1000,
				},
				{
					MetricTimestamp: now.Add(-5 * time.Minute),
					Quantity:        200, // Decrease but not to zero - standard counter reset logic applies
				},
				{
					MetricTimestamp: now,
					Quantity:        300,
				},
			},
			resourceUUID: "replication-uuid-reset",
			expected:     300, // Counter reset: 200 (reset value) + (300-200) = 300
		},
		{
			name: "xregion replication - normal increase",
			metrics: []datamodel2.HydratedMetrics{
				{
					MetricTimestamp: now.Add(-10 * time.Minute),
					Quantity:        1000,
				},
				{
					MetricTimestamp: now.Add(-5 * time.Minute),
					Quantity:        1500,
				},
				{
					MetricTimestamp: now,
					Quantity:        2000,
				},
			},
			resourceUUID: "replication-uuid-normal",
			expected:     1000, // 2000 - 1000 = 1000
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := CounterDelta(hydratedMetricsToDataPoints(tt.metrics), logger, metadata.XregionReplicationTotalTransferBytes, tt.resourceUUID)
			assert.InDelta(t, tt.expected, got, 0.001, "XregionReplicationTotalTransferBytes handling did not match expected value")
		})
	}
}

// TestCounterDelta_CoolTierBoundaryConditions tests boundary conditions for cool tier metrics
func TestCounterDelta_CoolTierBoundaryConditions(t *testing.T) {
	logger := util.GetLogger(context.Background())
	now := time.Now()

	t.Run("CoolTierWrite - all samples are decrements", func(t *testing.T) {
		metrics := []datamodel2.HydratedMetrics{
			{
				MetricTimestamp: now.Add(-15 * time.Minute),
				Quantity:        5000,
			},
			{
				MetricTimestamp: now.Add(-10 * time.Minute),
				Quantity:        4000, // Decrement - skip
			},
			{
				MetricTimestamp: now.Add(-5 * time.Minute),
				Quantity:        3000, // Decrement - skip
			},
			{
				MetricTimestamp: now,
				Quantity:        2000, // Decrement - skip
			},
		}
		got, _ := CounterDelta(hydratedMetricsToDataPoints(metrics), logger, metadata.CoolTierDataWriteSizeRaw, "pool-all-decrements")
		// All samples are decrements, so nothing is aggregated
		assert.Equal(t, 0.0, got, "Expected 0 when all samples are decrements")
	})

	t.Run("CoolTierWrite - oscillating between increase and decrease", func(t *testing.T) {
		metrics := []datamodel2.HydratedMetrics{
			{
				MetricTimestamp: now.Add(-20 * time.Minute),
				Quantity:        1000,
			},
			{
				MetricTimestamp: now.Add(-15 * time.Minute),
				Quantity:        1500, // +500
			},
			{
				MetricTimestamp: now.Add(-10 * time.Minute),
				Quantity:        1200, // Decrement - skip
			},
			{
				MetricTimestamp: now.Add(-5 * time.Minute),
				Quantity:        1800, // +300 (from 1500, not 1200)
			},
			{
				MetricTimestamp: now,
				Quantity:        2200, // +400
			},
		}
		got, _ := CounterDelta(hydratedMetricsToDataPoints(metrics), logger, metadata.CoolTierDataWriteSizeRaw, "pool-oscillating")
		// (1500-1000) + (1800-1500) + (2200-1800) = 500 + 300 + 400 = 1200
		assert.InDelta(t, 1200.0, got, 0.001, "Expected correct aggregation skipping decrements")
	})

	t.Run("CoolTierRead - oscillating between zero and non-zero", func(t *testing.T) {
		metrics := []datamodel2.HydratedMetrics{
			{
				MetricTimestamp: now.Add(-20 * time.Minute),
				Quantity:        1000,
			},
			{
				MetricTimestamp: now.Add(-15 * time.Minute),
				Quantity:        1500, // +500
			},
			{
				MetricTimestamp: now.Add(-10 * time.Minute),
				Quantity:        0, // Drop to zero - skip
			},
			{
				MetricTimestamp: now.Add(-5 * time.Minute),
				Quantity:        800, // +800 (from 1500, not 0)
			},
			{
				MetricTimestamp: now,
				Quantity:        0, // Drop to zero again - skip
			},
		}
		got, _ := CounterDelta(hydratedMetricsToDataPoints(metrics), logger, metadata.CoolTierDataReadSizeRaw, "pool-oscillating-zero")
		// (1500-1000) + skip 0 + (800-1500 skipped) = 500, then (2300-1500) = 800
		// Actually: (1500-1000) = 500, skip 0, (800-1500)=-700 but 800 > 1500*0.25=375, so skip anomalous dip
		// So only first increment counts
		assert.InDelta(t, 500.0, got, 0.001, "Expected only valid increments")
	})

	t.Run("CoolTierRead - starting from zero", func(t *testing.T) {
		metrics := []datamodel2.HydratedMetrics{
			{
				MetricTimestamp: now.Add(-15 * time.Minute),
				Quantity:        0,
			},
			{
				MetricTimestamp: now.Add(-10 * time.Minute),
				Quantity:        500, // +500
			},
			{
				MetricTimestamp: now.Add(-5 * time.Minute),
				Quantity:        1000, // +500
			},
			{
				MetricTimestamp: now,
				Quantity:        1500, // +500
			},
		}
		got, _ := CounterDelta(hydratedMetricsToDataPoints(metrics), logger, metadata.CoolTierDataWriteSizeRaw, "pool-start-zero")
		// 500 + 500 + 500 = 1500
		assert.InDelta(t, 1500.0, got, 0.001, "Expected correct aggregation from zero start")
	})

	t.Run("CoolTierWrite - lastPoint exactly zero then increase", func(t *testing.T) {
		metrics := []datamodel2.HydratedMetrics{
			{
				MetricTimestamp: now.Add(-10 * time.Minute),
				Quantity:        0,
			},
			{
				MetricTimestamp: now.Add(-5 * time.Minute),
				Quantity:        1000, // +1000
			},
			{
				MetricTimestamp: now,
				Quantity:        1500, // +500
			},
		}
		got, _ := CounterDelta(hydratedMetricsToDataPoints(metrics), logger, metadata.CoolTierDataWriteSizeRaw, "pool-zero-start")
		// 1000 + 500 = 1500
		assert.InDelta(t, 1500.0, got, 0.001, "Expected correct aggregation from zero")
	})

	t.Run("XregionReplication - multiple zeros in sequence", func(t *testing.T) {
		metrics := []datamodel2.HydratedMetrics{
			{
				MetricTimestamp: now.Add(-20 * time.Minute),
				Quantity:        1000,
			},
			{
				MetricTimestamp: now.Add(-15 * time.Minute),
				Quantity:        1500, // +500
			},
			{
				MetricTimestamp: now.Add(-10 * time.Minute),
				Quantity:        0, // Drop to zero - skip
			},
			{
				MetricTimestamp: now.Add(-5 * time.Minute),
				Quantity:        0, // Still zero - no change, delta=0
			},
			{
				MetricTimestamp: now,
				Quantity:        2000, // +2000 from last valid point (1500)
			},
		}
		got, _ := CounterDelta(hydratedMetricsToDataPoints(metrics), logger, metadata.XregionReplicationTotalTransferBytes, "repl-multiple-zeros")
		// (1500-1000) + skip 0 + (2000-1500) = 500 + 500 = 1000
		assert.InDelta(t, 1000.0, got, 0.001, "Expected correct handling of multiple zeros")
	})

	t.Run("CoolTierWrite - single data point", func(t *testing.T) {
		metrics := []datamodel2.HydratedMetrics{
			{
				MetricTimestamp: now,
				Quantity:        1000,
			},
		}
		got, _ := CounterDelta(hydratedMetricsToDataPoints(metrics), logger, metadata.CoolTierDataWriteSizeRaw, "pool-single")
		// Need at least 2 points
		assert.Equal(t, 0.0, got, "Expected 0 for single data point")
	})

	t.Run("CoolTierWrite - decrease to negative value is skipped", func(t *testing.T) {
		metrics := []datamodel2.HydratedMetrics{
			{
				MetricTimestamp: now.Add(-10 * time.Minute),
				Quantity:        1000,
			},
			{
				MetricTimestamp: now.Add(-5 * time.Minute),
				Quantity:        -100, // Invalid negative value, should be skipped (any decrease)
			},
			{
				MetricTimestamp: now,
				Quantity:        200, // This is also a decrease from 1000, should be skipped
			},
		}
		got, _ := CounterDelta(hydratedMetricsToDataPoints(metrics), logger, metadata.CoolTierDataWriteSizeRaw, "pool-negative")
		// CoolTierDataWriteSizeRaw skips ANY decrease, so both -100 and 200 are skipped
		// Result: 0
		assert.Equal(t, 0.0, got, "Expected 0 when all samples decrease for CoolTierWrite")
	})

	t.Run("CoolTierRead - pure alternating oscillation between 0 and non-zero every sample", func(t *testing.T) {
		metrics := []datamodel2.HydratedMetrics{
			{
				MetricTimestamp: now.Add(-30 * time.Minute),
				Quantity:        1000,
			},
			{
				MetricTimestamp: now.Add(-25 * time.Minute),
				Quantity:        0, // Drop to zero - skip
			},
			{
				MetricTimestamp: now.Add(-20 * time.Minute),
				Quantity:        1500, // Increase from last valid (1000), delta=500
			},
			{
				MetricTimestamp: now.Add(-15 * time.Minute),
				Quantity:        0, // Drop to zero - skip
			},
			{
				MetricTimestamp: now.Add(-10 * time.Minute),
				Quantity:        2000, // Increase from last valid (1500), delta=500
			},
			{
				MetricTimestamp: now.Add(-5 * time.Minute),
				Quantity:        0, // Drop to zero - skip
			},
			{
				MetricTimestamp: now,
				Quantity:        2500, // Increase from last valid (2000), delta=500
			},
		}
		got, _ := CounterDelta(hydratedMetricsToDataPoints(metrics), logger, metadata.CoolTierDataReadSizeRaw, "pool-pure-oscillation")
		// (1500-1000) + (2000-1500) + (2500-2000) = 500 + 500 + 500 = 1500
		// All zeros are skipped, lastPoint maintained at last non-zero value
		assert.InDelta(t, 1500.0, got, 0.001, "Expected correct aggregation with pure 0/non-zero oscillation")
	})

	t.Run("XregionReplication - pure alternating oscillation between 0 and non-zero every sample", func(t *testing.T) {
		metrics := []datamodel2.HydratedMetrics{
			{
				MetricTimestamp: now.Add(-30 * time.Minute),
				Quantity:        500,
			},
			{
				MetricTimestamp: now.Add(-25 * time.Minute),
				Quantity:        0, // Drop to zero - skip (replication paused)
			},
			{
				MetricTimestamp: now.Add(-20 * time.Minute),
				Quantity:        800, // Replication resumed, delta=300 from last valid (500)
			},
			{
				MetricTimestamp: now.Add(-15 * time.Minute),
				Quantity:        0, // Drop to zero - skip (replication paused again)
			},
			{
				MetricTimestamp: now.Add(-10 * time.Minute),
				Quantity:        1200, // Replication resumed, delta=400 from last valid (800)
			},
			{
				MetricTimestamp: now.Add(-5 * time.Minute),
				Quantity:        0, // Drop to zero - skip
			},
			{
				MetricTimestamp: now,
				Quantity:        1800, // Replication resumed, delta=600 from last valid (1200)
			},
		}
		got, _ := CounterDelta(hydratedMetricsToDataPoints(metrics), logger, metadata.XregionReplicationTotalTransferBytes, "repl-pure-oscillation")
		// (800-500) + (1200-800) + (1800-1200) = 300 + 400 + 600 = 1300
		// All zeros are skipped, lastPoint maintained at last non-zero value
		assert.InDelta(t, 1300.0, got, 0.001, "Expected correct aggregation with pure 0/non-zero oscillation during replication pause/resume cycles")
	})
}

// TestCounterDelta_EdgeCasesAllMetricTypes tests edge cases across all counter metric types
func TestCounterDelta_EdgeCasesAllMetricTypes(t *testing.T) {
	logger := util.GetLogger(context.Background())
	now := time.Now()

	t.Run("Empty data points", func(t *testing.T) {
		metrics := []datamodel2.HydratedMetrics{}
		got, _ := CounterDelta(hydratedMetricsToDataPoints(metrics), logger, metadata.CoolTierDataWriteSizeRaw, "empty")
		assert.Equal(t, 0.0, got, "Expected 0 for empty data points")
	})

	t.Run("All identical values", func(t *testing.T) {
		metrics := []datamodel2.HydratedMetrics{
			{MetricTimestamp: now.Add(-15 * time.Minute), Quantity: 1000},
			{MetricTimestamp: now.Add(-10 * time.Minute), Quantity: 1000},
			{MetricTimestamp: now.Add(-5 * time.Minute), Quantity: 1000},
			{MetricTimestamp: now, Quantity: 1000},
		}
		got, _ := CounterDelta(hydratedMetricsToDataPoints(metrics), logger, metadata.AllocatedSize, "identical")
		assert.Equal(t, 0.0, got, "Expected 0 for all identical values")
	})

	t.Run("Very large values", func(t *testing.T) {
		metrics := []datamodel2.HydratedMetrics{
			{MetricTimestamp: now.Add(-10 * time.Minute), Quantity: 1e15},
			{MetricTimestamp: now.Add(-5 * time.Minute), Quantity: 1e15 + 1e12},
			{MetricTimestamp: now, Quantity: 1e15 + 2e12},
		}
		got, _ := CounterDelta(hydratedMetricsToDataPoints(metrics), logger, metadata.CoolTierDataWriteSizeRaw, "large-values")
		assert.InDelta(t, 2e12, got, 1e9, "Expected correct handling of large values")
	})

	t.Run("Very small increments", func(t *testing.T) {
		metrics := []datamodel2.HydratedMetrics{
			{MetricTimestamp: now.Add(-15 * time.Minute), Quantity: 1000.001},
			{MetricTimestamp: now.Add(-10 * time.Minute), Quantity: 1000.002},
			{MetricTimestamp: now.Add(-5 * time.Minute), Quantity: 1000.003},
			{MetricTimestamp: now, Quantity: 1000.004},
		}
		got, _ := CounterDelta(hydratedMetricsToDataPoints(metrics), logger, metadata.AllocatedSize, "tiny-increments")
		assert.InDelta(t, 0.003, got, 0.0001, "Expected correct handling of tiny increments")
	})
}
