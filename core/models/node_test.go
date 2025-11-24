package models

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNode_GetCaURIWithFallback(t *testing.T) {
	t.Run("WhenNodeIsNil_ShouldReturnEnvFallback", func(t *testing.T) {
		var n *Node = nil
		result := n.GetCaURIWithFallback()
		// Should return result from env.BuildCaURI("", "", "")
		assert.NotNil(t, result)
	})

	t.Run("WhenCaURIIsEmpty_ShouldReturnEnvFallback", func(t *testing.T) {
		n := &Node{
			CaURI: "",
		}
		result := n.GetCaURIWithFallback()
		// Should return result from env.BuildCaURI("", "", "")
		assert.NotNil(t, result)
	})

	t.Run("WhenCaURIHasValue_ShouldReturnCaURI", func(t *testing.T) {
		n := &Node{
			CaURI: "project-123/pool-456/ca-789",
		}
		result := n.GetCaURIWithFallback()
		assert.Equal(t, "project-123/pool-456/ca-789", result)
	})
}

func TestNode_ParseCaURIWithFallback(t *testing.T) {
	t.Run("WhenNodeIsNil_ShouldReturnEnvFallback", func(t *testing.T) {
		var n *Node = nil
		projectID, poolName, caName := n.ParseCaURIWithFallback()
		// Should return env vars directly
		assert.NotNil(t, projectID)
		assert.NotNil(t, poolName)
		assert.NotNil(t, caName)
	})

	t.Run("WhenCaURIIsEmpty_ShouldReturnEnvFallback", func(t *testing.T) {
		n := &Node{
			CaURI: "",
		}
		projectID, poolName, caName := n.ParseCaURIWithFallback()
		// Should return env vars directly
		assert.NotNil(t, projectID)
		assert.NotNil(t, poolName)
		assert.NotNil(t, caName)
	})

	t.Run("WhenCaURIHasValue_ShouldParseCaURI", func(t *testing.T) {
		n := &Node{
			CaURI: "project-123/pool-456/ca-789",
		}
		projectID, poolName, caName := n.ParseCaURIWithFallback()
		assert.Equal(t, "project-123", projectID)
		assert.Equal(t, "pool-456", poolName)
		assert.Equal(t, "ca-789", caName)
	})
}

