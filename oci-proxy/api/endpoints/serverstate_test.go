package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
