package api

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/api/telemetry-servergen"
)

func Test_ReturnsAcceptedResponseForPerformanceEndpoint(t *testing.T) {
	handler := Handler{}
	response, err := handler.V1Performance(context.Background())

	assert.NoError(t, err)
	assert.IsType(t, &oasgenserver.V1PerformanceAccepted{}, response)
}

func Test_ReturnsAcceptedResponseForUsageEndpoint(t *testing.T) {
	handler := Handler{}
	response, err := handler.V1Usage(context.Background())

	assert.NoError(t, err)
	assert.IsType(t, &oasgenserver.V1UsageAccepted{}, response)
}
