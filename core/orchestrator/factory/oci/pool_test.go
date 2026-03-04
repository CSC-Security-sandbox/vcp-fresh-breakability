package oci

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	workflowenginemock "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine"
)

// TestCreatePool_Integration uses a real in-memory database for integration testing
func TestCreatePool_Integration(t *testing.T) {
	ctx := context.Background()
	mockLogger := log.NewLogger()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)

	store, err := database.SetupStorageForTest(mockLogger)
	assert.NoError(t, err)
	err = database.ClearInMemoryDB(store.DB())
	assert.NoError(t, err)

	mockTemporal := workflowenginemock.NewMockTemporalTestClient(t)
	orch := &OCIOrchestrator{
		storage:  store,
		temporal: mockTemporal,
	}

	params := &commonparams.CreatePoolParams{
		AccountName:    "test-account",
		Name:           "test-pool",
		SizeInBytes:    1024 * 1024 * 1024, // 1 GiB
		VendorID:       "test-vendor-id",
		VendorSubNetID: "test-subnet",
		Region:         "us-central1",
		ServiceLevel:   "FLEX",
		QosType:        "auto",
		CustomPerformanceParams: &commonparams.CustomPerformanceParams{
			ThroughputMibps: 64,
			Iops:            func() *int64 { v := int64(1024); return &v }(),
		},
		PrimaryZone: "us-central1-a",
	}

	// Mock the ExecuteWorkflow call to simulate successful workflow execution
	mockTemporal.EXPECT().
		ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, nil)

	result, jobID, err := orch.CreatePool(ctx, params)

	// Verify successful pool creation
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.NotEmpty(t, jobID)
	assert.Equal(t, params.Name, result.Name)
	assert.Equal(t, params.AccountName, result.AccountName)
}
