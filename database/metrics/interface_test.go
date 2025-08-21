package database

import (
	"context"
	"fmt"
	"testing"

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
		ID:           1001,
		ResourceType: metadata.ResourceType("test-resource"),
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
