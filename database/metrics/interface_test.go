package database

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gormWrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils/gorm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/datamodel"
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

	metric := &datamodel.HydratedMetrics{
		MeasuredType: "test-type",
		ResourceType: "test-resource",
		ResourceName: "test-resource-1",
	}
	// Create
	assert.NoError(t, repo.CreateHydratedMetrics(ctx, metric))

	// Get
	metrics, err := repo.GetHydratedMetrics(ctx, map[string]interface{}{"resource_name": "test-resource-1"})
	assert.NoError(t, err)
	assert.NotEmpty(t, metrics)

	// Update
	updates := map[string]interface{}{"MeasuredType": "updated-type"}
	assert.NoError(t, repo.UpdateHydratedMetrics(ctx, "test-resource-1", updates))

	// Get after update
	metrics, err = repo.GetHydratedMetrics(ctx, map[string]interface{}{"resource_name": "test-resource-1"})
	assert.NoError(t, err)
	assert.Equal(t, "updated-type", metrics[0].MeasuredType)

	// Delete
	assert.NoError(t, repo.DeleteHydratedMetrics(ctx, "test-resource-1"))
	metrics, err = repo.GetHydratedMetrics(ctx, map[string]interface{}{"resource_name": "test-resource-1"})
	assert.NoError(t, err)
	assert.Empty(t, metrics)
}

func TestDataStoreRepository_AggregatedUsageCRUD(t *testing.T) {
	repo := setupTestDataStoreRepository(t)
	ctx := context.Background()

	usage := &datamodel.AggregatedUsage{
		ID:           1001,
		ResourceType: "test-resource",
	}
	// Create
	assert.NoError(t, repo.CreateAggregatedUsage(ctx, usage))

	// Get
	usages, err := repo.GetAggregatedUsage(ctx, map[string]interface{}{"id": 1001})
	assert.NoError(t, err)
	assert.NotEmpty(t, usages)

	// Update
	updates := map[string]interface{}{"ResourceType": "updated-resource"}
	assert.NoError(t, repo.UpdateAggregatedUsage(ctx, 1001, updates))

	// Get after update
	usages, err = repo.GetAggregatedUsage(ctx, map[string]interface{}{"id": 1001})
	assert.NoError(t, err)
	assert.Equal(t, "updated-resource", usages[0].ResourceType)

	// Delete
	assert.NoError(t, repo.DeleteAggregatedUsage(ctx, 1001))
	usages, err = repo.GetAggregatedUsage(ctx, map[string]interface{}{"id": 1001})
	assert.NoError(t, err)
	assert.Empty(t, usages)
}

func TestDataStoreRepository_BillingGcpUsageCRUD(t *testing.T) {
	repo := setupTestDataStoreRepository(t)
	ctx := context.Background()

	billing := &datamodel.BillingGcpUsage{
		ID:         2001,
		State:      "pending",
		CustomerID: "cust-1",
	}
	// Create
	assert.NoError(t, repo.CreateBillingGcpUsage(ctx, billing))

	// Get
	billings, err := repo.GetBillingGcpUsage(ctx, map[string]interface{}{"id": 2001})
	assert.NoError(t, err)
	assert.NotEmpty(t, billings)

	// Update
	updates := map[string]interface{}{"State": "sent"}
	assert.NoError(t, repo.UpdateBillingGcpUsage(ctx, 2001, updates))

	// Get after update
	billings, err = repo.GetBillingGcpUsage(ctx, map[string]interface{}{"id": 2001})
	assert.NoError(t, err)
	assert.Equal(t, "sent", billings[0].State)

	// Delete
	assert.NoError(t, repo.DeleteBillingGcpUsage(ctx, 2001))
	billings, err = repo.GetBillingGcpUsage(ctx, map[string]interface{}{"id": 2001})
	assert.NoError(t, err)
	assert.Empty(t, billings)
}
