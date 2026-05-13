package api

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/factory"
	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/vcp-core/servergen"
)

func TestGetHealth(t *testing.T) {
	t.Run("ReturnsHealthyResponse", func(t *testing.T) {
		// Setup
		mockOrch := factory.NewMockOrchestratorFactory(t)
		handler := NewHandler(mockOrch)

		// Execute
		ctx := context.Background()
		result, err := handler.GetHealth(ctx)

		// Assert
		assert.NoError(t, err)
		assert.NotNil(t, result)

		healthResponse, ok := result.(*oasgenserver.Health)
		assert.True(t, ok, "Expected result to be of type *oasgenserver.Health")
		assert.NotNil(t, healthResponse)
	})

	t.Run("WithCancelledContext", func(t *testing.T) {
		// Setup
		mockOrch := factory.NewMockOrchestratorFactory(t)
		handler := NewHandler(mockOrch)

		// Execute with cancelled context
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately
		result, err := handler.GetHealth(ctx)

		// Assert - GetHealth should still succeed even with cancelled context
		// as it doesn't perform any long-running operations
		assert.NoError(t, err)
		assert.NotNil(t, result)

		healthResponse, ok := result.(*oasgenserver.Health)
		assert.True(t, ok, "Expected result to be of type *oasgenserver.Health")
		assert.NotNil(t, healthResponse)
	})
}
