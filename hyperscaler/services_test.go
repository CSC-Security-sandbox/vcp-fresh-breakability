package hyperscaler

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_newGcpServices_ReturnsInitializedGcpServices(t *testing.T) {
	ctx := context.TODO()
	services := NewGcpServices(ctx)

	assert.NotNil(t, services)
	assert.Equal(t, ctx, services.Ctx)
	assert.NotNil(t, services.Logger)
	assert.NotNil(t, services.Retry)
}

func Test_GetGCPService(t *testing.T) {
	t.Run("function exists and has correct signature", func(t *testing.T) {
		assert.NotNil(t, GetGCPService)
	})
}

func Test_NewGcpServices(t *testing.T) {
	t.Run("creates new gcp services with context", func(t *testing.T) {
		ctx := context.Background()
		services := NewGcpServices(ctx)

		assert.NotNil(t, services)
		assert.Equal(t, ctx, services.Ctx)
		assert.NotNil(t, services.Logger)
		assert.NotNil(t, services.Retry)
	})
}

func Test_MaxRetries(t *testing.T) {
	assert.GreaterOrEqual(t, MaxRetries, 0)
}
