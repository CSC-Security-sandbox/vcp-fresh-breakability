package api

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
)

func TestNewServerState(t *testing.T) {
	t.Run("NewServerState creates instance with shuttingDown=false", func(tt *testing.T) {
		serverState := NewServerState()

		assert.NotNil(tt, serverState)
		assert.False(tt, serverState.IsShuttingDown(), "new ServerState should have shuttingDown=false")
	})
}

func TestServerState_SetShuttingDown(t *testing.T) {
	t.Run("SetShuttingDown sets shuttingDown to true", func(tt *testing.T) {
		serverState := NewServerState()
		assert.False(tt, serverState.IsShuttingDown())

		serverState.SetShuttingDown()

		assert.True(tt, serverState.IsShuttingDown(), "shuttingDown should be true after SetShuttingDown()")
	})

	t.Run("SetShuttingDown is idempotent", func(tt *testing.T) {
		serverState := NewServerState()

		serverState.SetShuttingDown()
		serverState.SetShuttingDown()

		assert.True(tt, serverState.IsShuttingDown(), "shuttingDown should remain true after multiple calls")
	})
}

func TestServerState_IsShuttingDown(t *testing.T) {
	t.Run("IsShuttingDown returns false initially", func(tt *testing.T) {
		serverState := NewServerState()

		assert.False(tt, serverState.IsShuttingDown())
	})

	t.Run("IsShuttingDown returns true after SetShuttingDown", func(tt *testing.T) {
		serverState := NewServerState()
		serverState.SetShuttingDown()

		assert.True(tt, serverState.IsShuttingDown())
	})
}

func TestGetHealth(t *testing.T) {
	t.Run("GetHealth returns healthy status when server is running", func(tt *testing.T) {
		serverState := NewServerState()
		handler := Handler{
			ServerState: serverState,
		}

		response, err := handler.GetHealth(context.Background())

		assert.NoError(tt, err)
		healthResponse, ok := response.(*gcpgenserver.Health)
		assert.True(tt, ok, "response should be of type *gcpgenserver.Health")
		assert.Equal(tt, "healthy", healthResponse.Status.Value)
	})

	t.Run("GetHealth returns 500 error when server is shutting down", func(tt *testing.T) {
		serverState := NewServerState()
		serverState.SetShuttingDown()
		handler := Handler{
			ServerState: serverState,
		}

		response, err := handler.GetHealth(context.Background())

		assert.NoError(tt, err) // The error is returned as a response, not as an error
		errorResponse, ok := response.(*gcpgenserver.GetHealthInternalServerError)
		assert.True(tt, ok, "response should be of type *gcpgenserver.GetHealthInternalServerError")
		assert.Equal(tt, float64(500), errorResponse.Code)
		assert.Equal(tt, "Server is shutting down", errorResponse.Message)
	})

	t.Run("GetHealth returns healthy status when ServerState is nil", func(tt *testing.T) {
		handler := Handler{
			ServerState: nil,
		}

		response, err := handler.GetHealth(context.Background())

		assert.NoError(tt, err)
		healthResponse, ok := response.(*gcpgenserver.Health)
		assert.True(tt, ok, "response should be of type *gcpgenserver.Health")
		assert.Equal(tt, "healthy", healthResponse.Status.Value)
	})
}

func TestGetHealth_ConcurrentAccess(t *testing.T) {
	t.Run("GetHealth is safe for concurrent access", func(tt *testing.T) {
		serverState := NewServerState()
		handler := Handler{
			ServerState: serverState,
		}

		// Run multiple goroutines that read and write concurrently
		done := make(chan bool)

		// Start goroutines that call GetHealth
		for i := 0; i < 10; i++ {
			go func() {
				for j := 0; j < 100; j++ {
					_, _ = handler.GetHealth(context.Background())
				}
				done <- true
			}()
		}

		// Start a goroutine that sets shutting down
		go func() {
			for j := 0; j < 100; j++ {
				serverState.SetShuttingDown()
			}
			done <- true
		}()

		// Start goroutines that check IsShuttingDown
		for i := 0; i < 10; i++ {
			go func() {
				for j := 0; j < 100; j++ {
					_ = serverState.IsShuttingDown()
				}
				done <- true
			}()
		}

		// Wait for all goroutines to complete (10 + 1 + 10 = 21 goroutines)
		for i := 0; i < 21; i++ {
			<-done
		}

		// After all operations, server should be shutting down
		assert.True(tt, serverState.IsShuttingDown())
	})
}
