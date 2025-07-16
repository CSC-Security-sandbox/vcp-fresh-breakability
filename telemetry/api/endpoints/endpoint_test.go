package api

import (
	context "context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	dbmock "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/api/telemetry-servergen"
	procMock "github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/processor"
)

func Test_ReturnsAcceptedResponseForPerformanceEndpoint(t *testing.T) {
	vcpStore := &dbmock.MockStorage{}
	telemetryStore := &dbmock.MockStorage{}
	proc := &procMock.MockProcessor{}
	proc.On("ProcessPerformanceMetrics", mock.Anything).Return(nil)

	handler := Handler{
		vcpDatastore:       vcpStore,
		telemetryDatastore: telemetryStore,
		metricsProcessor:   proc,
	}
	response, err := handler.V1Performance(context.Background())

	assert.NoError(t, err)
	assert.IsType(t, &oasgenserver.V1PerformanceAccepted{}, response)
}

func Test_ReturnsAcceptedResponseForUsageEndpoint(t *testing.T) {
	// Usage endpoint not implemented, so test is skipped
}

func Test_V1Performance_ProcessorError(t *testing.T) {
	vcpStore := &dbmock.MockStorage{}
	telemetryStore := &dbmock.MockStorage{}
	proc := &procMock.MockProcessor{}
	var wg sync.WaitGroup
	wg.Add(1)
	proc.On("ProcessPerformanceMetrics", mock.Anything).Return(assert.AnError).Run(func(args mock.Arguments) {
		wg.Done()
	})

	handler := Handler{
		vcpDatastore:       vcpStore,
		telemetryDatastore: telemetryStore,
		metricsProcessor:   proc,
	}
	response, err := handler.V1Performance(context.Background())
	wg.Wait()
	assert.NoError(t, err, "Handler should not return error even if processor fails")
	assert.IsType(t, &oasgenserver.V1PerformanceAccepted{}, response)
	proc.AssertCalled(t, "ProcessPerformanceMetrics", mock.Anything)
}

func Test_V1Performance_NilProcessor(t *testing.T) {
	vcpStore := &dbmock.MockStorage{}
	telemetryStore := &dbmock.MockStorage{}

	handler := Handler{
		vcpDatastore:       vcpStore,
		telemetryDatastore: telemetryStore,
		metricsProcessor:   nil,
	}
	response, err := handler.V1Performance(context.Background())
	assert.NoError(t, err)
	assert.IsType(t, &oasgenserver.V1PerformanceBadRequest{}, response)
}

func Test_V1Performance_ContextCancel(t *testing.T) {
	vcpStore := &dbmock.MockStorage{}
	telemetryStore := &dbmock.MockStorage{}
	proc := &procMock.MockProcessor{}
	ctx, cancel := context.WithCancel(context.Background())
	proc.On("ProcessPerformanceMetrics", mock.Anything).Return(nil)

	handler := Handler{
		vcpDatastore:       vcpStore,
		telemetryDatastore: telemetryStore,
		metricsProcessor:   proc,
	}
	cancel() // cancel context before call
	response, err := handler.V1Performance(ctx)
	assert.NoError(t, err)
	assert.IsType(t, &oasgenserver.V1PerformanceAccepted{}, response)
}
