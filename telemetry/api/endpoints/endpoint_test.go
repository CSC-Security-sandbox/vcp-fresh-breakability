package api

import (
	context "context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	metricsdb "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/metrics"
	vcpdb "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/api/telemetry-servergen"
	procMock "github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/processor"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func setupTestDB(t *testing.T) (metricsdb.Storage, func()) {
	logger := log.NewLogger()
	store, err := metricsdb.SetupStorageForTest(logger)
	if err != nil {
		t.Fatalf("Failed to setup test database: %v", err)
	}

	cleanup := func() {
		_ = store.Close()
	}

	return store, cleanup
}

func Test_ReturnsAcceptedResponseForPerformanceEndpoint(t *testing.T) {
	vcpStore := &vcpdb.MockStorage{}
	telemetryStore, cleanup := setupTestDB(t)
	defer cleanup()

	mockProc := procMock.NewMockProcessor(t)
	var wg sync.WaitGroup
	wg.Add(1)

	handler := Handler{
		vcpDatastore:       vcpStore,
		telemetryDatastore: telemetryStore,
		metricsProcessor:   procMock.MetricsProcessor{VCPProcessor: mockProc},
	}
	response, err := handler.V1Performance(context.Background())
	assert.NoError(t, err)
	assert.IsType(t, &oasgenserver.V1PerformanceAccepted{}, response)
}

// Test_ReturnsAcceptedResponseForUsageEndpoint is removed since the usage endpoint
// is not implemented yet. When implementing the usage endpoint, add corresponding tests.

func Test_V1Performance_DBError(t *testing.T) {
	vcpStore := &vcpdb.MockStorage{}
	telemetryStore, cleanup := setupTestDB(t)
	defer cleanup()

	// Force DB error by closing the connection
	_ = telemetryStore.Close()

	mockProc := procMock.NewMockProcessor(t)
	var wg sync.WaitGroup
	wg.Add(1)

	handler := Handler{
		vcpDatastore:       vcpStore,
		telemetryDatastore: telemetryStore,
		metricsProcessor:   procMock.MetricsProcessor{VCPProcessor: mockProc},
	}
	response, err := handler.V1Performance(context.Background())

	assert.NoError(t, err, "Handler should not return error even if DB fails")
	assert.IsType(t, &oasgenserver.V1PerformanceAccepted{}, response)
}

func Test_V1Performance_ContextCancel(t *testing.T) {
	vcpStore := &vcpdb.MockStorage{}
	telemetryStore, cleanup := setupTestDB(t)
	defer cleanup()

	mockProc := procMock.NewMockProcessor(t)
	var wg sync.WaitGroup
	wg.Add(1)

	ctx, cancel := context.WithCancel(context.Background())

	handler := Handler{
		vcpDatastore:       vcpStore,
		telemetryDatastore: telemetryStore,
		metricsProcessor:   procMock.MetricsProcessor{VCPProcessor: mockProc},
	}

	// Context cancellation should not affect the response since we use context.WithoutCancel
	cancel()
	response, err := handler.V1Performance(ctx)

	assert.NoError(t, err)
	assert.IsType(t, &oasgenserver.V1PerformanceAccepted{}, response)
}
