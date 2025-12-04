//go:build unit

package entity

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
)

func TestByTimestamp_Len(t *testing.T) {
	tests := []struct {
		name     string
		metrics  ByTimestamp
		expected int
	}{
		{
			name:     "Empty slice",
			metrics:  ByTimestamp{},
			expected: 0,
		},
		{
			name: "Single metric",
			metrics: ByTimestamp{
				{Timestamp: UnixNano(1000)},
			},
			expected: 1,
		},
		{
			name: "Multiple metrics",
			metrics: ByTimestamp{
				{Timestamp: UnixNano(1000)},
				{Timestamp: UnixNano(2000)},
				{Timestamp: UnixNano(3000)},
			},
			expected: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.metrics.Len(); got != tt.expected {
				t.Errorf("ByTimestamp.Len() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestByTimestamp_Swap(t *testing.T) {
	metrics := ByTimestamp{
		{Timestamp: UnixNano(1000), Quantity: 10},
		{Timestamp: UnixNano(2000), Quantity: 20},
	}

	// Test swapping
	metrics.Swap(0, 1)

	if metrics[0].Timestamp != UnixNano(2000) || metrics[0].Quantity != 20 {
		t.Errorf("Expected first element to be {Timestamp: 2000, Quantity: 20}, got %+v", metrics[0])
	}
	if metrics[1].Timestamp != UnixNano(1000) || metrics[1].Quantity != 10 {
		t.Errorf("Expected second element to be {Timestamp: 1000, Quantity: 10}, got %+v", metrics[1])
	}
}

func TestByTimestamp_Less(t *testing.T) {
	tests := []struct {
		name     string
		metrics  ByTimestamp
		i, j     int
		expected bool
	}{
		{
			name: "First timestamp is less than second",
			metrics: ByTimestamp{
				{Timestamp: UnixNano(1000)},
				{Timestamp: UnixNano(2000)},
			},
			i:        0,
			j:        1,
			expected: true,
		},
		{
			name: "First timestamp is greater than second",
			metrics: ByTimestamp{
				{Timestamp: UnixNano(2000)},
				{Timestamp: UnixNano(1000)},
			},
			i:        0,
			j:        1,
			expected: false,
		},
		{
			name: "Both timestamps are equal",
			metrics: ByTimestamp{
				{Timestamp: UnixNano(1000)},
				{Timestamp: UnixNano(1000)},
			},
			i:        0,
			j:        1,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.metrics.Less(tt.i, tt.j); got != tt.expected {
				t.Errorf("ByTimestamp.Less(%d, %d) = %v, want %v", tt.i, tt.j, got, tt.expected)
			}
		})
	}
}

func TestByTimestamp_Sort(t *testing.T) {
	// Test the complete sorting functionality
	metrics := ByTimestamp{
		{
			Timestamp:    UnixNano(3000),
			Quantity:     30,
			MeasuredType: metadata.AllocatedSize,
		},
		{
			Timestamp:    UnixNano(1000),
			Quantity:     10,
			MeasuredType: metadata.AllocatedSize,
		},
		{
			Timestamp:    UnixNano(2000),
			Quantity:     20,
			MeasuredType: metadata.AllocatedSize,
		},
	}

	// Sort the metrics
	sort.Sort(metrics)

	// Verify they are sorted by timestamp
	expectedTimestamps := []UnixNano{UnixNano(1000), UnixNano(2000), UnixNano(3000)}
	expectedQuantities := []float64{10, 20, 30}

	if len(metrics) != 3 {
		t.Fatalf("Expected 3 metrics after sort, got %d", len(metrics))
	}

	for i, metric := range metrics {
		if metric.Timestamp != expectedTimestamps[i] {
			t.Errorf("Expected timestamp %d at index %d, got %d", expectedTimestamps[i], i, metric.Timestamp)
		}
		if metric.Quantity != expectedQuantities[i] {
			t.Errorf("Expected quantity %f at index %d, got %f", expectedQuantities[i], i, metric.Quantity)
		}
	}
}

// TestByTimestamp_MissingLines covers specific missing lines identified by diff-cover
func TestByTimestamp_MissingLines(t *testing.T) {
	t.Run("Len_Function", func(t *testing.T) {
		// Line 21: Len() function
		metrics := ByTimestamp{
			{Timestamp: 100, Quantity: 1.0},
			{Timestamp: 200, Quantity: 2.0},
			{Timestamp: 300, Quantity: 3.0},
		}

		length := metrics.Len()
		assert.Equal(t, 3, length, "Len should return correct length")
	})

	t.Run("Swap_Function", func(t *testing.T) {
		// Line 26: Swap() function
		metrics := ByTimestamp{
			{Timestamp: 100, Quantity: 1.0},
			{Timestamp: 200, Quantity: 2.0},
			{Timestamp: 300, Quantity: 3.0},
		}

		// Swap first and last elements
		metrics.Swap(0, 2)

		assert.Equal(t, UnixNano(300), metrics[0].Timestamp, "First element should be swapped")
		assert.Equal(t, UnixNano(100), metrics[2].Timestamp, "Last element should be swapped")
		assert.Equal(t, float64(3.0), metrics[0].Quantity, "Quantity should be swapped too")
		assert.Equal(t, float64(1.0), metrics[2].Quantity, "Quantity should be swapped too")
	})

	t.Run("Less_Function", func(t *testing.T) {
		// Line 31: Less() function
		metrics := ByTimestamp{
			{Timestamp: 300, Quantity: 3.0},
			{Timestamp: 100, Quantity: 1.0},
			{Timestamp: 200, Quantity: 2.0},
		}

		// Test various comparisons
		assert.False(t, metrics.Less(0, 1), "300 should not be less than 100")
		assert.True(t, metrics.Less(1, 0), "100 should be less than 300")
		assert.True(t, metrics.Less(1, 2), "100 should be less than 200")
		assert.False(t, metrics.Less(2, 1), "200 should not be less than 100")

		// Test with equal timestamps
		metrics[2].Timestamp = 100 // Make two timestamps equal
		assert.False(t, metrics.Less(1, 2), "Equal timestamps should return false")
		assert.False(t, metrics.Less(2, 1), "Equal timestamps should return false")
	})

	t.Run("Sort_Integration_Test", func(t *testing.T) {
		// Integration test to ensure all three functions work together for sorting
		metrics := ByTimestamp{
			{Timestamp: 300, Quantity: 3.0, MeasuredType: metadata.AllocatedSize},
			{Timestamp: 100, Quantity: 1.0, MeasuredType: metadata.AllocatedSize},
			{Timestamp: 200, Quantity: 2.0, MeasuredType: metadata.AllocatedSize},
			{Timestamp: 50, Quantity: 0.5, MeasuredType: metadata.AllocatedSize},
		}

		// Use the sort package to test our implementation
		sorted := make(ByTimestamp, len(metrics))
		copy(sorted, metrics)

		// Manually sort using our interface methods to test coverage
		for i := 0; i < sorted.Len(); i++ {
			for j := i + 1; j < sorted.Len(); j++ {
				if !sorted.Less(i, j) && sorted[i].Timestamp != sorted[j].Timestamp {
					sorted.Swap(i, j)
				}
			}
		}

		// Verify sorting worked
		assert.Equal(t, UnixNano(50), sorted[0].Timestamp, "First should be smallest timestamp")
		assert.Equal(t, UnixNano(100), sorted[1].Timestamp, "Second should be second smallest")
		assert.Equal(t, UnixNano(200), sorted[2].Timestamp, "Third should be third smallest")
		assert.Equal(t, UnixNano(300), sorted[3].Timestamp, "Last should be largest timestamp")
	})
}

func TestByTimestamp_EdgeCases(t *testing.T) {
	// Test cases to cover specific missing lines 21, 26, 31 in HydratedMetric.go

	t.Run("Len with various slice sizes", func(t *testing.T) {
		// Test Len method with different scenarios

		// Empty slice
		emptyMetrics := ByTimestamp{}
		if got := emptyMetrics.Len(); got != 0 {
			t.Errorf("Empty slice Len() = %d, want 0", got)
		}

		// Large slice
		largeMetrics := make(ByTimestamp, 1000)
		if got := largeMetrics.Len(); got != 1000 {
			t.Errorf("Large slice Len() = %d, want 1000", got)
		}
	})

	t.Run("Swap with edge indices", func(t *testing.T) {
		// Test Swap method with boundary conditions
		metrics := ByTimestamp{
			{Timestamp: UnixNano(1000), Quantity: 10},
			{Timestamp: UnixNano(2000), Quantity: 20},
			{Timestamp: UnixNano(3000), Quantity: 30},
		}

		// Swap first and last
		metrics.Swap(0, 2)

		if metrics[0].Timestamp != UnixNano(3000) {
			t.Errorf("After swap(0,2), first element timestamp = %d, want 3000", metrics[0].Timestamp)
		}
		if metrics[2].Timestamp != UnixNano(1000) {
			t.Errorf("After swap(0,2), last element timestamp = %d, want 1000", metrics[2].Timestamp)
		}

		// Swap same index (should do nothing)
		origQuantity := metrics[1].Quantity
		origTimestamp := metrics[1].Timestamp
		metrics.Swap(1, 1)
		if metrics[1].Quantity != origQuantity || metrics[1].Timestamp != origTimestamp {
			t.Error("Swapping same index should not change the element")
		}
	})

	t.Run("Less with edge timestamp values", func(t *testing.T) {
		// Test Less method with boundary timestamp values
		metrics := ByTimestamp{
			{Timestamp: UnixNano(0)},                   // Min value
			{Timestamp: UnixNano(9223372036854775807)}, // Max int64 value
		}

		// Test min < max
		if !metrics.Less(0, 1) {
			t.Error("Expected min timestamp to be less than max timestamp")
		}

		// Test max > min
		if metrics.Less(1, 0) {
			t.Error("Expected max timestamp to not be less than min timestamp")
		}
	})

	t.Run("Less with identical timestamps", func(t *testing.T) {
		// Test Less with exactly equal timestamps
		sameTime := UnixNano(12345)
		metrics := ByTimestamp{
			{Timestamp: sameTime, Quantity: 100},
			{Timestamp: sameTime, Quantity: 200}, // Same timestamp, different quantity
		}

		// Equal timestamps should return false for Less
		if metrics.Less(0, 1) {
			t.Error("Expected false when timestamps are equal")
		}
		if metrics.Less(1, 0) {
			t.Error("Expected false when timestamps are equal (reverse)")
		}
	})
}

func TestByTimestamp_IntegrationCases(t *testing.T) {
	// Additional integration tests to ensure full coverage

	t.Run("Sort with duplicate timestamps", func(t *testing.T) {
		// Test sorting behavior with duplicate timestamps
		metrics := ByTimestamp{
			{Timestamp: UnixNano(3000), Quantity: 30, MeasuredType: metadata.AllocatedSize},
			{Timestamp: UnixNano(1000), Quantity: 10, MeasuredType: metadata.AllocatedSize},
			{Timestamp: UnixNano(2000), Quantity: 20, MeasuredType: metadata.AllocatedSize},
			{Timestamp: UnixNano(2000), Quantity: 25, MeasuredType: metadata.AllocatedSize}, // Duplicate timestamp
		}

		sort.Sort(metrics)

		// Verify sorted order
		if metrics[0].Timestamp != UnixNano(1000) {
			t.Errorf("First element timestamp = %d, want 1000", metrics[0].Timestamp)
		}
		if metrics[1].Timestamp != UnixNano(2000) {
			t.Errorf("Second element timestamp = %d, want 2000", metrics[1].Timestamp)
		}
		if metrics[2].Timestamp != UnixNano(2000) {
			t.Errorf("Third element timestamp = %d, want 2000", metrics[2].Timestamp)
		}
		if metrics[3].Timestamp != UnixNano(3000) {
			t.Errorf("Fourth element timestamp = %d, want 3000", metrics[3].Timestamp)
		}
	})

	t.Run("Large dataset sorting", func(t *testing.T) {
		// Test with larger dataset to ensure all paths are covered
		var metrics ByTimestamp

		// Create reverse-ordered metrics
		for i := 100; i > 0; i-- {
			metrics = append(metrics, HydratedMetric{
				Timestamp:    UnixNano(int64(i * 1000)),
				Quantity:     float64(i),
				MeasuredType: metadata.AllocatedSize,
			})
		}

		// Sort them
		sort.Sort(metrics)

		// Verify they are in ascending order
		for i := 1; i < len(metrics); i++ {
			if metrics[i-1].Timestamp >= metrics[i].Timestamp {
				t.Errorf("Metrics not sorted correctly at index %d: %d >= %d",
					i, metrics[i-1].Timestamp, metrics[i].Timestamp)
			}
		}

		// Verify length is preserved
		if len(metrics) != 100 {
			t.Errorf("Expected 100 metrics after sort, got %d", len(metrics))
		}
	})
}
