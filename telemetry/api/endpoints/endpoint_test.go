package api

import (
	context "context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	metricsdb "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/metrics"
	vcpdb "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/api/telemetry-servergen"
	procMock "github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/processor"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

// MockVCPProcessor is a mock implementation that satisfies the VCPProcessor interface
type MockVCPProcessor struct {
	*procMock.MockProcessor
}

// ProcessUsageMetrics implements the missing interface method
func (m *MockVCPProcessor) ProcessUsageMetrics(ctx context.Context) error {
	return nil
}

func (m *MockVCPProcessor) CollectMetrics(ctx context.Context, projectId string) error {
	return nil
}

func (m *MockVCPProcessor) ProcessBizOps(ctx context.Context, params *utils.BizOpsReportParams) error {
	return nil
}

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
	mockVCPProc := &MockVCPProcessor{MockProcessor: mockProc}
	var wg sync.WaitGroup
	wg.Add(1)

	queue := utils.NewQueue(telemetryStore.SQLDB(), mockVCPProc)
	handler := Handler{
		vcpDatastore:       vcpStore,
		telemetryDatastore: telemetryStore,
		metricsProcessor:   procMock.MetricsProcessor{VCPProcessor: mockVCPProc},
		jobQueue:           queue,
	}
	response, err := handler.V1Performance(context.Background(), oasgenserver.V1PerformanceParams{})
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
	mockVCPProc := &MockVCPProcessor{MockProcessor: mockProc}
	var wg sync.WaitGroup
	wg.Add(1)

	queue := utils.NewQueue(telemetryStore.SQLDB(), mockVCPProc)
	handler := Handler{
		vcpDatastore:       vcpStore,
		telemetryDatastore: telemetryStore,
		metricsProcessor:   procMock.MetricsProcessor{VCPProcessor: mockVCPProc},
		jobQueue:           queue,
	}
	response, err := handler.V1Performance(context.Background(), oasgenserver.V1PerformanceParams{})

	assert.Error(t, err, "Handler should return error if DB fails")
	assert.IsType(t, &oasgenserver.V1PerformanceInternalServerError{}, response)
}

func Test_V1Performance_ContextCancel(t *testing.T) {
	vcpStore := &vcpdb.MockStorage{}
	telemetryStore, cleanup := setupTestDB(t)
	defer cleanup()

	mockProc := procMock.NewMockProcessor(t)
	mockVCPProc := &MockVCPProcessor{MockProcessor: mockProc}
	var wg sync.WaitGroup
	wg.Add(1)

	ctx, cancel := context.WithCancel(context.Background())
	queue := utils.NewQueue(telemetryStore.SQLDB(), mockVCPProc)

	handler := Handler{
		vcpDatastore:       vcpStore,
		telemetryDatastore: telemetryStore,
		metricsProcessor:   procMock.MetricsProcessor{VCPProcessor: mockVCPProc},
		jobQueue:           queue,
	}

	// Context cancellation should not affect the response since we use context.WithoutCancel
	cancel()
	response, err := handler.V1Performance(ctx, oasgenserver.V1PerformanceParams{})

	assert.NoError(t, err)
	assert.IsType(t, &oasgenserver.V1PerformanceAccepted{}, response)
}

func Test_ReturnsAcceptedResponseForUsageEndpoint(t *testing.T) {
	vcpStore := &vcpdb.MockStorage{}
	telemetryStore, cleanup := setupTestDB(t)
	defer cleanup()

	mockProc := procMock.NewMockProcessor(t)
	mockVCPProc := &MockVCPProcessor{MockProcessor: mockProc}
	var wg sync.WaitGroup
	wg.Add(1)

	ctx, cancel := context.WithCancel(context.Background())
	queue := utils.NewQueue(telemetryStore.SQLDB(), mockVCPProc)

	handler := Handler{
		vcpDatastore:       vcpStore,
		telemetryDatastore: telemetryStore,
		metricsProcessor:   procMock.MetricsProcessor{VCPProcessor: mockVCPProc},
		jobQueue:           queue,
	}

	// Context cancellation should not affect the response since we use context.WithoutCancel
	cancel()
	response, err := handler.V1Usage(ctx, oasgenserver.V1UsageParams{})

	assert.NoError(t, err)
	assert.IsType(t, &oasgenserver.V1UsageAccepted{}, response)
}

func Test_V1Usage_DBError(t *testing.T) {
	vcpStore := &vcpdb.MockStorage{}
	telemetryStore, cleanup := setupTestDB(t)
	defer cleanup()

	// Force DB error by closing the connection
	_ = telemetryStore.Close()

	mockProc := procMock.NewMockProcessor(t)
	mockVCPProc := &MockVCPProcessor{MockProcessor: mockProc}
	var wg sync.WaitGroup
	wg.Add(1)

	queue := utils.NewQueue(telemetryStore.SQLDB(), mockVCPProc)
	handler := Handler{
		vcpDatastore:       vcpStore,
		telemetryDatastore: telemetryStore,
		metricsProcessor:   procMock.MetricsProcessor{VCPProcessor: mockVCPProc},
		jobQueue:           queue,
	}
	response, err := handler.V1Usage(context.Background(), oasgenserver.V1UsageParams{})

	assert.Error(t, err, "Handler should return error if DB fails")
	assert.IsType(t, &oasgenserver.V1UsageInternalServerError{}, response)
}

func Test_ReturnsAcceptedResponseForGenerateReportEndpoint(t *testing.T) {
	vcpStore := &vcpdb.MockStorage{}
	telemetryStore, cleanup := setupTestDB(t)
	defer cleanup()

	mockProc := procMock.NewMockProcessor(t)
	mockVCPProc := &MockVCPProcessor{MockProcessor: mockProc}
	var wg sync.WaitGroup
	wg.Add(1)

	ctx, cancel := context.WithCancel(context.Background())

	queue := utils.NewQueue(telemetryStore.SQLDB(), mockVCPProc)
	handler := Handler{
		vcpDatastore:       vcpStore,
		telemetryDatastore: telemetryStore,
		metricsProcessor:   procMock.MetricsProcessor{VCPProcessor: mockVCPProc},
		jobQueue:           queue,
	}

	// Context cancellation should not affect the response since we use context.WithoutCancel
	cancel()
	req := oasgenserver.OptGenerateReportV1beta{}
	oldParseparseReportParams := parseReportParams
	defer func() { parseReportParams = oldParseparseReportParams }()
	parseReportParams = func(bizOpsReportParams *utils.BizOpsReportParams) error {
		return nil
	}
	response, err := handler.V1GenerateReport(ctx, req)

	assert.NoError(t, err)
	assert.IsType(t, &oasgenserver.V1GenerateReportAccepted{}, response)
}

func Test_V1GenerateReportParamsError(t *testing.T) {
	vcpStore := &vcpdb.MockStorage{}
	telemetryStore, cleanup := setupTestDB(t)
	defer cleanup()

	mockProc := procMock.NewMockProcessor(t)
	mockVCPProc := &MockVCPProcessor{MockProcessor: mockProc}
	var wg sync.WaitGroup
	wg.Add(1)

	ctx, cancel := context.WithCancel(context.Background())

	queue := utils.NewQueue(telemetryStore.SQLDB(), mockVCPProc)
	handler := Handler{
		vcpDatastore:       vcpStore,
		telemetryDatastore: telemetryStore,
		metricsProcessor:   procMock.MetricsProcessor{VCPProcessor: mockVCPProc},
		jobQueue:           queue,
	}

	// Context cancellation should not affect the response since we use context.WithoutCancel
	cancel()
	req := oasgenserver.OptGenerateReportV1beta{}
	oldParseparseReportParams := parseReportParams
	defer func() { parseReportParams = oldParseparseReportParams }()
	parseReportParams = func(bizOpsReportParams *utils.BizOpsReportParams) error {
		return fmt.Errorf("test error")
	}
	response, err := handler.V1GenerateReport(ctx, req)

	assert.Error(t, err)
	assert.IsType(t, &oasgenserver.V1GenerateReportInternalServerError{}, response)
}

func Test_V1GenerateReport_DBError(t *testing.T) {
	vcpStore := &vcpdb.MockStorage{}
	telemetryStore, cleanup := setupTestDB(t)
	defer cleanup()

	// Force DB error by closing the connection
	_ = telemetryStore.Close()

	mockProc := procMock.NewMockProcessor(t)
	mockVCPProc := &MockVCPProcessor{MockProcessor: mockProc}
	var wg sync.WaitGroup
	wg.Add(1)

	queue := utils.NewQueue(telemetryStore.SQLDB(), mockVCPProc)
	handler := Handler{
		vcpDatastore:       vcpStore,
		telemetryDatastore: telemetryStore,
		metricsProcessor:   procMock.MetricsProcessor{VCPProcessor: mockVCPProc},
		jobQueue:           queue,
	}
	req := oasgenserver.OptGenerateReportV1beta{}
	response, err := handler.V1GenerateReport(context.Background(), req)

	assert.Error(t, err, "Handler should return error if DB fails")
	assert.IsType(t, &oasgenserver.V1GenerateReportInternalServerError{}, response)
}

func Test_V1GenerateReport_ContextCancel(t *testing.T) {
	vcpStore := &vcpdb.MockStorage{}
	telemetryStore, cleanup := setupTestDB(t)
	defer cleanup()

	mockProc := procMock.NewMockProcessor(t)
	mockVCPProc := &MockVCPProcessor{MockProcessor: mockProc}
	var wg sync.WaitGroup
	wg.Add(1)

	ctx, cancel := context.WithCancel(context.Background())

	queue := utils.NewQueue(telemetryStore.SQLDB(), mockVCPProc)
	handler := Handler{
		vcpDatastore:       vcpStore,
		telemetryDatastore: telemetryStore,
		metricsProcessor:   procMock.MetricsProcessor{VCPProcessor: mockVCPProc},
		jobQueue:           queue,
	}

	// Context cancellation should not affect the response since we use context.WithoutCancel
	cancel()
	req := oasgenserver.OptGenerateReportV1beta{}
	response, err := handler.V1GenerateReport(ctx, req)

	assert.NoError(t, err)
	assert.IsType(t, &oasgenserver.V1GenerateReportAccepted{}, response)
}

// Test to cover missing lines 46-47: correlation ID handling in V1Performance
func Test_V1Performance_WithCorrelationID(t *testing.T) {
	vcpStore := &vcpdb.MockStorage{}
	telemetryStore, cleanup := setupTestDB(t)
	defer cleanup()

	mockProc := procMock.NewMockProcessor(t)
	mockVCPProc := &MockVCPProcessor{MockProcessor: mockProc}
	var wg sync.WaitGroup
	wg.Add(1)

	queue := utils.NewQueue(telemetryStore.SQLDB(), mockVCPProc)
	handler := Handler{
		vcpDatastore:       vcpStore,
		telemetryDatastore: telemetryStore,
		metricsProcessor:   procMock.MetricsProcessor{VCPProcessor: mockVCPProc},
		jobQueue:           queue,
	}

	// Test with correlation ID provided
	params := oasgenserver.V1PerformanceParams{
		XCorrelationID: oasgenserver.NewOptString("test-correlation-id-123"),
	}
	response, err := handler.V1Performance(context.Background(), params)
	assert.NoError(t, err)
	assert.IsType(t, &oasgenserver.V1PerformanceAccepted{}, response)
}

// Test to cover missing lines 84-85: correlation ID handling in V1Usage
func Test_V1Usage_WithCorrelationID(t *testing.T) {
	vcpStore := &vcpdb.MockStorage{}
	telemetryStore, cleanup := setupTestDB(t)
	defer cleanup()

	mockProc := procMock.NewMockProcessor(t)
	mockVCPProc := &MockVCPProcessor{MockProcessor: mockProc}
	var wg sync.WaitGroup
	wg.Add(1)

	queue := utils.NewQueue(telemetryStore.SQLDB(), mockVCPProc)
	handler := Handler{
		vcpDatastore:       vcpStore,
		telemetryDatastore: telemetryStore,
		metricsProcessor:   procMock.MetricsProcessor{VCPProcessor: mockVCPProc},
		jobQueue:           queue,
	}

	// Test with correlation ID provided
	params := oasgenserver.V1UsageParams{
		XCorrelationID: oasgenserver.NewOptString("test-correlation-id-456"),
	}
	response, err := handler.V1Usage(context.Background(), params)
	assert.NoError(t, err)
	assert.IsType(t, &oasgenserver.V1UsageAccepted{}, response)
}
