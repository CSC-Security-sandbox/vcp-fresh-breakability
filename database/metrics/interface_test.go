package database

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gormWrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils/gorm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
)

func setupTestDataStoreRepository(t *testing.T) *DataStoreRepository {
	db, err := SetupInMemoryDB()
	require.NoError(t, err)
	wrapper := gormWrapper.New(db)
	return NewDataStoreRepository(wrapper)
}

func TestDataStoreRepository_HydratedMetricsCRUD(t *testing.T) {
	repo := setupTestDataStoreRepository(t)
	ctx := context.Background()

	// Create multiple metrics for testing different query conditions
	metric1 := &datamodel.HydratedMetrics{
		MeasuredType: metadata.MeasuredType("test-type"),
		ResourceType: metadata.ResourceType("test-resource"),
		ResourceName: "test-resource-name-1",
	}
	metric2 := &datamodel.HydratedMetrics{
		MeasuredType: metadata.MeasuredType("test-type-2"),
		ResourceType: metadata.ResourceType("test-resource"),
		ResourceName: "test-resource-name-2",
	}

	// Create
	assert.NoError(t, repo.CreateHydratedMetrics(ctx, metric1))
	assert.NoError(t, repo.CreateHydratedMetrics(ctx, metric2))

	// Get by ID
	metrics, err := repo.GetHydratedMetrics(ctx, map[string]interface{}{"id": metric1.ID})
	assert.NoError(t, err)
	assert.NotEmpty(t, metrics)
	assert.Equal(t, "test-resource-name-1", metrics[0].ResourceName)

	// Test complex conditions - Query with conditions array
	complexFilter := map[string]interface{}{
		"conditions": [][]interface{}{
			{"measured_type = ?", "test-type"},
			{"resource_type = ?", "test-resource"},
		},
	}
	metrics, err = repo.GetHydratedMetrics(ctx, complexFilter)
	assert.NoError(t, err)
	assert.NotEmpty(t, metrics)
	assert.Equal(t, 1, len(metrics))
	assert.Equal(t, "test-resource-name-1", metrics[0].ResourceName)

	// Test complex conditions with empty condition array
	emptyCondFilter := map[string]interface{}{
		"conditions": [][]interface{}{{}},
	}
	metrics, err = repo.GetHydratedMetrics(ctx, emptyCondFilter)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(metrics)) // Should return all metrics

	// Update
	updates := map[string]interface{}{"measured_type": "updated-type"}
	assert.NoError(t, repo.UpdateHydratedMetrics(ctx, fmt.Sprintf("%d", metric1.ID), updates))

	// Get after update
	metrics, err = repo.GetHydratedMetrics(ctx, map[string]interface{}{"id": metric1.ID})
	assert.NoError(t, err)
	assert.Equal(t, metadata.MeasuredType("updated-type"), metrics[0].MeasuredType)

	// Delete
	assert.NoError(t, repo.DeleteHydratedMetrics(ctx, fmt.Sprintf("%d", metric1.ID)))
	metrics, err = repo.GetHydratedMetrics(ctx, map[string]interface{}{"id": metric1.ID})
	assert.NoError(t, err)
	assert.Empty(t, metrics)
}

func TestDataStoreRepository_AggregatedUsageCRUD(t *testing.T) {
	repo := setupTestDataStoreRepository(t)
	ctx := context.Background()

	usage := &datamodel.AggregatedUsage{
		ID:               1001,
		ResourceUUID:     "test-resource-uuid",
		AccountID:        "test-account",
		VendorCustomerID: ptrString("vendor-cust-123"),
		AggregationStart: time.Now(),
		AggregationEnd:   time.Now().Add(1 * time.Hour),
		MeasuredType:     metadata.MeasuredType("test-measured"),
		ResourceType:     metadata.ResourceType("test-resource"),
		Quantity:         10.0,
		AggregationType:  "sum",
		IsBillable:       true,
		State:            datamodel.Unsubmitted,
		VolumeStyle:      "block",
		ServiceLevel:     "gold",
		ReplicationType:  "none",
		IsUnified:        false,
	}
	// Create
	assert.NoError(t, repo.CreateAggregatedUsage(ctx, usage))

	// Get
	usages, err := repo.GetAggregatedUsage(ctx, map[string]interface{}{"id": 1001})
	assert.NoError(t, err)
	assert.NotEmpty(t, usages)

	// Update
	updates := map[string]interface{}{"resource_type": "updated-resource"}
	assert.NoError(t, repo.UpdateAggregatedUsage(ctx, 1001, updates))

	// Get after update
	usages, err = repo.GetAggregatedUsage(ctx, map[string]interface{}{"id": 1001})
	assert.NoError(t, err)
	assert.Equal(t, metadata.ResourceType("updated-resource"), usages[0].ResourceType)

	// Delete
	assert.NoError(t, repo.DeleteAggregatedUsage(ctx, 1001))
	usages, err = repo.GetAggregatedUsage(ctx, map[string]interface{}{"id": 1001})
	assert.NoError(t, err)
	assert.Empty(t, usages)
}

func ptrString(s string) *string {
	return &s
}

func TestPtrString(t *testing.T) {
	s := "hello"
	ptr := ptrString(s)
	assert.NotNil(t, ptr)
	assert.Equal(t, "hello", *ptr)
}

// TestCreateAggregatedUsageBatch tests the batch creation functionality
func TestCreateAggregatedUsageBatch(t *testing.T) {
	repo := setupTestDataStoreRepository(t)
	ctx := context.Background()

	now := time.Now()

	// Test with empty slice
	err := repo.CreateAggregatedUsageBatch(ctx, []datamodel.AggregatedUsage{}, 10)
	assert.NoError(t, err)

	// Create multiple usage records for batch testing
	usages := []datamodel.AggregatedUsage{
		{
			ID:               1001,
			ResourceUUID:     "test-resource-uuid-1",
			AccountID:        "test-account-1",
			VendorCustomerID: ptrString("vendor-cust-123"),
			AggregationStart: now,
			AggregationEnd:   now.Add(1 * time.Hour),
			MeasuredType:     metadata.MeasuredType("test-measured-1"),
			ResourceType:     metadata.ResourceType("test-resource-1"),
			Quantity:         10.0,
			AggregationType:  "sum",
			IsBillable:       true,
			State:            datamodel.Unsubmitted,
			VolumeStyle:      "block",
			ServiceLevel:     "gold",
			ReplicationType:  "none",
			IsUnified:        false,
		},
		{
			ID:               1002,
			ResourceUUID:     "test-resource-uuid-2",
			AccountID:        "test-account-2",
			VendorCustomerID: ptrString("vendor-cust-456"),
			AggregationStart: now,
			AggregationEnd:   now.Add(1 * time.Hour),
			MeasuredType:     metadata.MeasuredType("test-measured-2"),
			ResourceType:     metadata.ResourceType("test-resource-2"),
			Quantity:         20.0,
			AggregationType:  "sum",
			IsBillable:       true,
			State:            datamodel.Unsubmitted,
			VolumeStyle:      "file",
			ServiceLevel:     "silver",
			ReplicationType:  "async",
			IsUnified:        true,
		},
		{
			ID:               1003,
			ResourceUUID:     "test-resource-uuid-3",
			AccountID:        "test-account-3",
			VendorCustomerID: ptrString("vendor-cust-789"),
			AggregationStart: now,
			AggregationEnd:   now.Add(1 * time.Hour),
			MeasuredType:     metadata.MeasuredType("test-measured-3"),
			ResourceType:     metadata.ResourceType("test-resource-3"),
			Quantity:         30.0,
			AggregationType:  "avg",
			IsBillable:       false,
			State:            datamodel.Submitted,
			VolumeStyle:      "mixed",
			ServiceLevel:     "bronze",
			ReplicationType:  "sync",
			IsUnified:        false,
		},
	}

	// Create in batch
	err = repo.CreateAggregatedUsageBatch(ctx, usages, 2)
	assert.NoError(t, err)

	// Verify all records were created
	allUsages, err := repo.GetAggregatedUsage(ctx, map[string]interface{}{})
	assert.NoError(t, err)
	assert.Len(t, allUsages, 3)

	// Verify specific records
	usage1, err := repo.GetAggregatedUsage(ctx, map[string]interface{}{"id": 1001})
	assert.NoError(t, err)
	assert.Len(t, usage1, 1)
	assert.Equal(t, "test-resource-uuid-1", usage1[0].ResourceUUID)
	assert.Equal(t, "test-account-1", usage1[0].AccountID)

	usage2, err := repo.GetAggregatedUsage(ctx, map[string]interface{}{"id": 1002})
	assert.NoError(t, err)
	assert.Len(t, usage2, 1)
	assert.Equal(t, "test-resource-uuid-2", usage2[0].ResourceUUID)
	assert.Equal(t, "test-account-2", usage2[0].AccountID)

	usage3, err := repo.GetAggregatedUsage(ctx, map[string]interface{}{"id": 1003})
	assert.NoError(t, err)
	assert.Len(t, usage3, 1)
	assert.Equal(t, "test-resource-uuid-3", usage3[0].ResourceUUID)
	assert.Equal(t, "test-account-3", usage3[0].AccountID)
}

// TestGetHydratedMetrics_OrderAndLimit tests the order and limit functionality
func TestGetHydratedMetrics_OrderAndLimit(t *testing.T) {
	repo := setupTestDataStoreRepository(t)
	ctx := context.Background()

	// Create test metrics with different timestamps
	now := time.Now()
	metric1 := &datamodel.HydratedMetrics{
		MeasuredType:    metadata.MeasuredType("test-type"),
		ResourceType:    metadata.ResourceType("test-resource"),
		ResourceName:    "resource-1",
		MetricTimestamp: now.Add(-2 * time.Hour),
		Quantity:        100.0,
	}
	metric2 := &datamodel.HydratedMetrics{
		MeasuredType:    metadata.MeasuredType("test-type"),
		ResourceType:    metadata.ResourceType("test-resource"),
		ResourceName:    "resource-2",
		MetricTimestamp: now.Add(-1 * time.Hour),
		Quantity:        200.0,
	}
	metric3 := &datamodel.HydratedMetrics{
		MeasuredType:    metadata.MeasuredType("test-type"),
		ResourceType:    metadata.ResourceType("test-resource"),
		ResourceName:    "resource-3",
		MetricTimestamp: now,
		Quantity:        300.0,
	}

	// Create metrics
	assert.NoError(t, repo.CreateHydratedMetrics(ctx, metric1))
	assert.NoError(t, repo.CreateHydratedMetrics(ctx, metric2))
	assert.NoError(t, repo.CreateHydratedMetrics(ctx, metric3))

	// Test with order by timestamp DESC
	filterWithOrder := map[string]interface{}{
		"measured_type": "test-type",
		"order":         "metric_timestamp DESC",
	}
	metrics, err := repo.GetHydratedMetrics(ctx, filterWithOrder)
	assert.NoError(t, err)
	assert.Len(t, metrics, 3)
	// Should be ordered newest first
	assert.Equal(t, "resource-3", metrics[0].ResourceName)
	assert.Equal(t, "resource-2", metrics[1].ResourceName)
	assert.Equal(t, "resource-1", metrics[2].ResourceName)

	// Test with order by resource_name ASC
	filterWithOrderAsc := map[string]interface{}{
		"measured_type": "test-type",
		"order":         "resource_name ASC",
	}
	metrics, err = repo.GetHydratedMetrics(ctx, filterWithOrderAsc)
	assert.NoError(t, err)
	assert.Len(t, metrics, 3)
	// Should be ordered alphabetically
	assert.Equal(t, "resource-1", metrics[0].ResourceName)
	assert.Equal(t, "resource-2", metrics[1].ResourceName)
	assert.Equal(t, "resource-3", metrics[2].ResourceName)

	// Test with limit
	filterWithLimit := map[string]interface{}{
		"measured_type": "test-type",
		"order":         "metric_timestamp DESC",
		"limit":         2,
	}
	metrics, err = repo.GetHydratedMetrics(ctx, filterWithLimit)
	assert.NoError(t, err)
	assert.Len(t, metrics, 2)
	// Should return only the 2 newest metrics
	assert.Equal(t, "resource-3", metrics[0].ResourceName)
	assert.Equal(t, "resource-2", metrics[1].ResourceName)

	// Test with zero limit (should be ignored)
	filterWithZeroLimit := map[string]interface{}{
		"measured_type": "test-type",
		"limit":         0,
	}
	metrics, err = repo.GetHydratedMetrics(ctx, filterWithZeroLimit)
	assert.NoError(t, err)
	assert.Len(t, metrics, 3) // Should return all metrics since limit 0 is ignored

	// Test with negative limit (should be ignored)
	filterWithNegativeLimit := map[string]interface{}{
		"measured_type": "test-type",
		"limit":         -1,
	}
	metrics, err = repo.GetHydratedMetrics(ctx, filterWithNegativeLimit)
	assert.NoError(t, err)
	assert.Len(t, metrics, 3) // Should return all metrics since negative limit is ignored

	// Test with empty order string (should be ignored)
	filterWithEmptyOrder := map[string]interface{}{
		"measured_type": "test-type",
		"order":         "",
	}
	metrics, err = repo.GetHydratedMetrics(ctx, filterWithEmptyOrder)
	assert.NoError(t, err)
	assert.Len(t, metrics, 3) // Should return all metrics

	// Test with complex conditions, order, and limit together
	complexFilter := map[string]interface{}{
		"conditions": [][]interface{}{
			{"measured_type = ?", "test-type"},
			{"quantity >= ?", 150.0},
		},
		"order": "quantity ASC",
		"limit": 1,
	}
	metrics, err = repo.GetHydratedMetrics(ctx, complexFilter)
	assert.NoError(t, err)
	assert.Len(t, metrics, 1)
	// Should return the metric with quantity 200.0 (smallest >= 150)
	assert.Equal(t, "resource-2", metrics[0].ResourceName)
	assert.Equal(t, 200.0, metrics[0].Quantity)
}
