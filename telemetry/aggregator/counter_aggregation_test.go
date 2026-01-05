package aggregator

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/metrics"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/common"
	datamodel2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

// TestCalculateCounterDeltaWithAggregatedHistory tests the new method for calculating counter delta with cached history
func TestCalculateCounterDeltaWithAggregatedHistory(t *testing.T) {
	processor := &BillingProvider{}

	ctx := context.Background()
	logger := log.NewLogger()
	now := time.Now()
	aggregationStartTime := now

	resourceKey := ResourceKey{
		ResourceName: "test-volume",
		ConsumerID:   "customer-123",
		ResourceType: metadata.Volume,
	}
	resourceUUID := "test-volume-uuid"

	t.Run("No counter cache - fallback to standard delta", func(t *testing.T) {
		dataPoints := []common.DataPoint{
			{Timestamp: aggregationStartTime.Add(10 * time.Minute), Quantity: 100},
			{Timestamp: aggregationStartTime.Add(20 * time.Minute), Quantity: 200},
			{Timestamp: aggregationStartTime.Add(30 * time.Minute), Quantity: 150}, // Counter reset (anomalous dip, will be skipped)
		}

		// Empty cache
		counterCache := make(map[CounterAggregationCacheResourceKey]*float64)

		result := processor.calculateCounterDeltaWithAggregatedHistory(ctx, resourceKey, dataPoints, metadata.AllocatedSize, aggregationStartTime, counterCache, resourceUUID, logger)

		// Should calculate delta without previous value: (200-100) = 100, the dip to 150 is skipped as anomalous
		expectedDelta := float64(100)
		assert.Equal(t, expectedDelta, result)
	})

	t.Run("With cached counter value - enhanced delta calculation", func(t *testing.T) {
		dataPoints := []common.DataPoint{
			{Timestamp: aggregationStartTime.Add(10 * time.Minute), Quantity: 120},
			{Timestamp: aggregationStartTime.Add(20 * time.Minute), Quantity: 170},
		}

		// Cache with previous counter value using CounterAggregationCacheResourceKey as key
		lastCounterValue := float64(100)
		cacheKey := CounterAggregationCacheResourceKey{
			ResourceUUID: resourceUUID,
			MeasuredType: metadata.AllocatedSize,
		}
		counterCache := map[CounterAggregationCacheResourceKey]*float64{
			cacheKey: &lastCounterValue,
		}

		result := processor.calculateCounterDeltaWithAggregatedHistory(ctx, resourceKey, dataPoints, metadata.AllocatedSize, aggregationStartTime, counterCache, resourceUUID, logger)

		// Should calculate delta with previous value: (120-100) + (170-120) = 20 + 50 = 70
		expectedDelta := float64(70)
		assert.Equal(t, expectedDelta, result)
	})

	t.Run("Empty data points", func(t *testing.T) {
		dataPoints := []common.DataPoint{}
		counterCache := make(map[CounterAggregationCacheResourceKey]*float64)

		result := processor.calculateCounterDeltaWithAggregatedHistory(ctx, resourceKey, dataPoints, metadata.AllocatedSize, aggregationStartTime, counterCache, resourceUUID, logger)

		assert.Equal(t, float64(0), result)
	})
}

// TestPreloadCounterValues tests the bulk counter value fetching
func TestPreloadCounterValues(t *testing.T) {
	mockDB := database.NewMockStorage(t)
	processor := &BillingProvider{
		metricsDB: mockDB,
		config: &common.TelemetryConfig{
			PoolVolumeLabelPageSize: 5000,
		},
	}

	ctx := context.Background()
	logger := log.NewLogger()
	now := time.Now()
	aggregationStartTime := now
	aggregationEndTime := now.Add(1 * time.Hour)

	t.Run("Successfully preload counter values", func(t *testing.T) {
		// Mock aggregated usage records
		counterValue1 := float64(100)
		counterValue2 := float64(200)

		usageRecords := []datamodel2.AggregatedUsage{
			{
				ResourceUUID:     "volume-1",
				ResourceName:     &[]string{"volume-1"}[0],
				VendorCustomerID: &[]string{"customer-123"}[0],
				ResourceType:     metadata.Volume,
				MeasuredType:     metadata.AllocatedSize,
				AggregationType:  "CounterAggregation",
				LastCounterValue: &counterValue1,
				AggregationEnd:   now.Add(-1 * time.Hour),
			},
			{
				ResourceUUID:     "volume-2",
				ResourceName:     &[]string{"volume-2"}[0],
				VendorCustomerID: &[]string{"customer-456"}[0],
				ResourceType:     metadata.Volume,
				MeasuredType:     metadata.LogicalSize,
				AggregationType:  "CounterAggregation",
				LastCounterValue: &counterValue2,
				AggregationEnd:   now.Add(-2 * time.Hour),
			},
		}

		mockDB.On("GetLatestAggregatedUsageForAllResources", mock.Anything, "CounterAggregation", mock.Anything, mock.Anything).Return(usageRecords, nil).Once()
		mockDB.On("GetLatestAggregatedUsageForAllResources", mock.Anything, "CounterAggregation", mock.Anything, mock.Anything).Return([]datamodel2.AggregatedUsage{}, nil).Maybe()

		result, err := processor.preloadCounterValues(ctx, aggregationStartTime, aggregationEndTime, logger)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, 2, len(result))

		// Check first record - use CounterAggregationCacheResourceKey as key
		cacheKey1 := CounterAggregationCacheResourceKey{
			ResourceUUID: "volume-1",
			MeasuredType: metadata.AllocatedSize,
		}
		assert.Contains(t, result, cacheKey1)
		assert.Equal(t, counterValue1, *result[cacheKey1])

		// Check second record - use CounterAggregationCacheResourceKey as key
		cacheKey2 := CounterAggregationCacheResourceKey{
			ResourceUUID: "volume-2",
			MeasuredType: metadata.LogicalSize,
		}
		assert.Contains(t, result, cacheKey2)
		assert.Equal(t, counterValue2, *result[cacheKey2])

		mockDB.AssertExpectations(t)
	})

	t.Run("Database error", func(t *testing.T) {
		mockDB2 := database.NewMockStorage(t)
		processor2 := &BillingProvider{
			metricsDB: mockDB2,
			config: &common.TelemetryConfig{
				PoolVolumeLabelPageSize: 5000,
			},
		}

		mockDB2.On("GetLatestAggregatedUsageForAllResources", mock.Anything, "CounterAggregation", mock.Anything, mock.Anything).Return([]datamodel2.AggregatedUsage{}, assert.AnError)

		result, err := processor2.preloadCounterValues(ctx, aggregationStartTime, aggregationEndTime, logger)

		assert.Error(t, err)
		assert.Nil(t, result)
		mockDB2.AssertExpectations(t)
	})
}
