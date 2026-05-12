package aggregator

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
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

		res := processor.calculateCounterDeltaWithAggregatedHistory(ctx, resourceKey, dataPoints, metadata.AllocatedSize, aggregationStartTime, counterCache, resourceUUID, logger, false)

		// Should calculate delta without previous value: (200-100) = 100, the dip to 150 is skipped as anomalous
		expectedDelta := float64(100)
		assert.Equal(t, expectedDelta, res.billed)
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

		res := processor.calculateCounterDeltaWithAggregatedHistory(ctx, resourceKey, dataPoints, metadata.AllocatedSize, aggregationStartTime, counterCache, resourceUUID, logger, false)

		// Should calculate delta with previous value: (120-100) + (170-120) = 20 + 50 = 70
		expectedDelta := float64(70)
		assert.Equal(t, expectedDelta, res.billed)
	})

	t.Run("Empty data points", func(t *testing.T) {
		dataPoints := []common.DataPoint{}
		counterCache := make(map[CounterAggregationCacheResourceKey]*float64)

		res := processor.calculateCounterDeltaWithAggregatedHistory(ctx, resourceKey, dataPoints, metadata.AllocatedSize, aggregationStartTime, counterCache, resourceUUID, logger, false)

		assert.Equal(t, float64(0), res.billed)
	})

	t.Run("CBS backup transfer cache miss prepends zero baseline", func(t *testing.T) {
		dataPoints := []common.DataPoint{
			{Timestamp: aggregationStartTime.Add(10 * time.Minute), Quantity: 272043},
			{Timestamp: aggregationStartTime.Add(15 * time.Minute), Quantity: 387575},
			{Timestamp: aggregationStartTime.Add(20 * time.Minute), Quantity: 387575},
		}

		counterCache := make(map[CounterAggregationCacheResourceKey]*float64)

		backupResourceKey := ResourceKey{
			ResourceName: "test-backup",
			ConsumerID:   "customer-123",
			ResourceType: metadata.Backup,
		}
		res := processor.calculateCounterDeltaWithAggregatedHistory(ctx, backupResourceKey, dataPoints, metadata.CbsCrossRegionVolumeBackupTransferBytes, aggregationStartTime, counterCache, resourceUUID, logger, false)

		// Zero baseline prepended: [0, 272043, 387575, 387575]
		// Delta = 272043 + 115532 + 0 = 387575
		assert.InDelta(t, 387575.0, res.billed, 0.001)
		require.NotNil(t, res.lastCounter)
		assert.Equal(t, 387575.0, *res.lastCounter)
	})

	t.Run("CBS backup transfer cache hit uses cached value not zero", func(t *testing.T) {
		dataPoints := []common.DataPoint{
			{Timestamp: aggregationStartTime.Add(10 * time.Minute), Quantity: 503159},
			{Timestamp: aggregationStartTime.Add(20 * time.Minute), Quantity: 618707},
		}

		lastCounterValue := float64(387575)
		cacheKey := CounterAggregationCacheResourceKey{
			ResourceUUID: resourceUUID,
			MeasuredType: metadata.CbsCrossRegionVolumeBackupTransferBytes,
		}
		counterCache := map[CounterAggregationCacheResourceKey]*float64{
			cacheKey: &lastCounterValue,
		}

		backupResourceKey := ResourceKey{
			ResourceName: "test-backup",
			ConsumerID:   "customer-123",
			ResourceType: metadata.Backup,
		}
		res := processor.calculateCounterDeltaWithAggregatedHistory(ctx, backupResourceKey, dataPoints, metadata.CbsCrossRegionVolumeBackupTransferBytes, aggregationStartTime, counterCache, resourceUUID, logger, false)

		// Cached 387575 prepended: [387575, 503159, 618707]
		// Delta = 115584 + 115548 = 231132
		assert.InDelta(t, 231132.0, res.billed, 0.001)
		require.NotNil(t, res.lastCounter)
		assert.Equal(t, 618707.0, *res.lastCounter)
	})

	t.Run("CBS backup transfer cache miss single data point", func(t *testing.T) {
		dataPoints := []common.DataPoint{
			{Timestamp: aggregationStartTime.Add(10 * time.Minute), Quantity: 272043},
		}

		counterCache := make(map[CounterAggregationCacheResourceKey]*float64)

		backupResourceKey := ResourceKey{
			ResourceName: "test-backup",
			ConsumerID:   "customer-123",
			ResourceType: metadata.Backup,
		}
		res := processor.calculateCounterDeltaWithAggregatedHistory(ctx, backupResourceKey, dataPoints, metadata.CbsCrossRegionVolumeBackupTransferBytes, aggregationStartTime, counterCache, resourceUUID, logger, false)

		// Zero baseline prepended: [0, 272043] → delta = 272043
		assert.InDelta(t, 272043.0, res.billed, 0.001)
		require.NotNil(t, res.lastCounter)
		assert.Equal(t, 272043.0, *res.lastCounter)
	})

	t.Run("Non-CBS metric cache miss does not prepend zero baseline", func(t *testing.T) {
		// Same data as CBS test above but with a non-CBS metric type.
		// Without zero baseline: [272043, 387575] → delta = 115532
		dataPoints := []common.DataPoint{
			{Timestamp: aggregationStartTime.Add(10 * time.Minute), Quantity: 272043},
			{Timestamp: aggregationStartTime.Add(15 * time.Minute), Quantity: 387575},
		}

		counterCache := make(map[CounterAggregationCacheResourceKey]*float64)

		res := processor.calculateCounterDeltaWithAggregatedHistory(ctx, resourceKey, dataPoints, metadata.CoolTierDataReadSizeRaw, aggregationStartTime, counterCache, resourceUUID, logger, false)

		assert.InDelta(t, 115532.0, res.billed, 0.001)
		require.NotNil(t, res.lastCounter)
		assert.Equal(t, 387575.0, *res.lastCounter)
	})

	t.Run("CBS backup transfer cache hit with cross-window reset", func(t *testing.T) {
		// Previous window cached at 618707. New data starts lower due to a new backup.
		dataPoints := []common.DataPoint{
			{Timestamp: aggregationStartTime.Add(10 * time.Minute), Quantity: 150000},
			{Timestamp: aggregationStartTime.Add(15 * time.Minute), Quantity: 300000},
			{Timestamp: aggregationStartTime.Add(20 * time.Minute), Quantity: 450000},
		}

		lastCounterValue := float64(618707)
		cacheKey := CounterAggregationCacheResourceKey{
			ResourceUUID: resourceUUID,
			MeasuredType: metadata.CbsCrossRegionVolumeBackupTransferBytes,
		}
		counterCache := map[CounterAggregationCacheResourceKey]*float64{
			cacheKey: &lastCounterValue,
		}

		backupResourceKey := ResourceKey{
			ResourceName: "test-backup",
			ConsumerID:   "customer-123",
			ResourceType: metadata.Backup,
		}
		res := processor.calculateCounterDeltaWithAggregatedHistory(ctx, backupResourceKey, dataPoints, metadata.CbsCrossRegionVolumeBackupTransferBytes, aggregationStartTime, counterCache, resourceUUID, logger, false)

		// Cached 618707 prepended: [618707, 150000, 300000, 450000]
		// reset(150000) + (300000-150000) + (450000-300000) = 150000 + 150000 + 150000 = 450000
		assert.InDelta(t, 450000.0, res.billed, 0.001)
		require.NotNil(t, res.lastCounter)
		assert.Equal(t, 450000.0, *res.lastCounter)
	})

	t.Run("CBS backup transfer cache miss with all zero data points", func(t *testing.T) {
		dataPoints := []common.DataPoint{
			{Timestamp: aggregationStartTime.Add(10 * time.Minute), Quantity: 0},
			{Timestamp: aggregationStartTime.Add(15 * time.Minute), Quantity: 0},
			{Timestamp: aggregationStartTime.Add(20 * time.Minute), Quantity: 0},
		}

		counterCache := make(map[CounterAggregationCacheResourceKey]*float64)

		backupResourceKey := ResourceKey{
			ResourceName: "test-backup",
			ConsumerID:   "customer-123",
			ResourceType: metadata.Backup,
		}
		res := processor.calculateCounterDeltaWithAggregatedHistory(ctx, backupResourceKey, dataPoints, metadata.CbsCrossRegionVolumeBackupTransferBytes, aggregationStartTime, counterCache, resourceUUID, logger, false)

		// Zero baseline prepended: [0, 0, 0, 0] → all flat, delta = 0
		assert.InDelta(t, 0.0, res.billed, 0.001)
		require.NotNil(t, res.lastCounter)
		assert.Equal(t, 0.0, *res.lastCounter)
	})

	t.Run("CBS backup transfer cache hit with reset to zero in data", func(t *testing.T) {
		dataPoints := []common.DataPoint{
			{Timestamp: aggregationStartTime.Add(10 * time.Minute), Quantity: 500000},
			{Timestamp: aggregationStartTime.Add(15 * time.Minute), Quantity: 0},
			{Timestamp: aggregationStartTime.Add(20 * time.Minute), Quantity: 200000},
		}

		lastCounterValue := float64(387575)
		cacheKey := CounterAggregationCacheResourceKey{
			ResourceUUID: resourceUUID,
			MeasuredType: metadata.CbsCrossRegionVolumeBackupTransferBytes,
		}
		counterCache := map[CounterAggregationCacheResourceKey]*float64{
			cacheKey: &lastCounterValue,
		}

		backupResourceKey := ResourceKey{
			ResourceName: "test-backup",
			ConsumerID:   "customer-123",
			ResourceType: metadata.Backup,
		}
		res := processor.calculateCounterDeltaWithAggregatedHistory(ctx, backupResourceKey, dataPoints, metadata.CbsCrossRegionVolumeBackupTransferBytes, aggregationStartTime, counterCache, resourceUUID, logger, false)

		// Cached 387575 prepended: [387575, 500000, 0, 200000]
		// (500000-387575) + reset(0) + (200000-0) = 112425 + 0 + 200000 = 312425
		assert.InDelta(t, 312425.0, res.billed, 0.001)
		require.NotNil(t, res.lastCounter)
		assert.Equal(t, 200000.0, *res.lastCounter)
	})

	t.Run("CBS backup transfer cache miss empty data points", func(t *testing.T) {
		dataPoints := []common.DataPoint{}
		counterCache := make(map[CounterAggregationCacheResourceKey]*float64)

		backupResourceKey := ResourceKey{
			ResourceName: "test-backup",
			ConsumerID:   "customer-123",
			ResourceType: metadata.Backup,
		}
		res := processor.calculateCounterDeltaWithAggregatedHistory(ctx, backupResourceKey, dataPoints, metadata.CbsCrossRegionVolumeBackupTransferBytes, aggregationStartTime, counterCache, resourceUUID, logger, false)

		// Empty data returns zero, no zero baseline prepended for empty input
		assert.Equal(t, 0.0, res.billed)
		assert.Nil(t, res.lastCounter)
	})

	t.Run("CBS backup transfer cache hit with multiple resets in window", func(t *testing.T) {
		dataPoints := []common.DataPoint{
			{Timestamp: aggregationStartTime.Add(5 * time.Minute), Quantity: 500000},
			{Timestamp: aggregationStartTime.Add(10 * time.Minute), Quantity: 300000}, // reset
			{Timestamp: aggregationStartTime.Add(15 * time.Minute), Quantity: 400000},
			{Timestamp: aggregationStartTime.Add(20 * time.Minute), Quantity: 100000}, // reset
			{Timestamp: aggregationStartTime.Add(25 * time.Minute), Quantity: 250000},
		}

		lastCounterValue := float64(400000)
		cacheKey := CounterAggregationCacheResourceKey{
			ResourceUUID: resourceUUID,
			MeasuredType: metadata.CbsCrossRegionVolumeBackupTransferBytes,
		}
		counterCache := map[CounterAggregationCacheResourceKey]*float64{
			cacheKey: &lastCounterValue,
		}

		backupResourceKey := ResourceKey{
			ResourceName: "test-backup",
			ConsumerID:   "customer-123",
			ResourceType: metadata.Backup,
		}
		res := processor.calculateCounterDeltaWithAggregatedHistory(ctx, backupResourceKey, dataPoints, metadata.CbsCrossRegionVolumeBackupTransferBytes, aggregationStartTime, counterCache, resourceUUID, logger, false)

		// Cached 400000 prepended: [400000, 500000, 300000, 400000, 100000, 250000]
		// (500000-400000) + reset(300000) + (400000-300000) + reset(100000) + (250000-100000)
		// = 100000 + 300000 + 100000 + 100000 + 150000 = 750000
		assert.InDelta(t, 750000.0, res.billed, 0.001)
		require.NotNil(t, res.lastCounter)
		assert.Equal(t, 250000.0, *res.lastCounter)
	})

	t.Run("Replication initialize splits skipped vs billed before first positive", func(t *testing.T) {
		initTT := TransferTypeInitial
		dataPoints := []common.DataPoint{
			{Timestamp: aggregationStartTime.Add(5 * time.Minute), Quantity: 0, TransferType: nil},
			{Timestamp: aggregationStartTime.Add(15 * time.Minute), Quantity: 0, TransferType: nil},
			{Timestamp: aggregationStartTime.Add(25 * time.Minute), Quantity: 100, TransferType: &initTT},
			{Timestamp: aggregationStartTime.Add(35 * time.Minute), Quantity: 160, TransferType: &initTT},
		}
		counterCache := make(map[CounterAggregationCacheResourceKey]*float64)
		res := processor.calculateCounterDeltaWithAggregatedHistory(ctx, resourceKey, dataPoints, metadata.XregionReplicationTotalTransferBytes, aggregationStartTime, counterCache, resourceUUID, logger, true)
		// Prefix [0,0,100]: delta 100; suffix [100,160]: delta 60
		assert.InDelta(t, 100.0, res.skippedPrePositive, 0.001)
		assert.InDelta(t, 60.0, res.billed, 0.001)
		require.NotNil(t, res.skippedSegmentEndCounter)
		assert.InDelta(t, 100.0, *res.skippedSegmentEndCounter, 0.001)
		require.NotNil(t, res.lastCounter)
		assert.InDelta(t, 160.0, *res.lastCounter, 0.001)
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
