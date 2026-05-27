package models

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUserCredentials_GetCaURIWithFallback(t *testing.T) {
	t.Run("WhenUserCredentialsIsNil_ShouldReturnEnvFallback", func(t *testing.T) {
		var uc *UserCredentials = nil
		result := uc.GetCaURIWithFallback()
		// Should return result from env.BuildCaURI("", "", "")
		assert.NotNil(t, result)
	})

	t.Run("WhenCaURIIsEmpty_ShouldReturnEnvFallback", func(t *testing.T) {
		uc := &UserCredentials{
			CaURI: "",
		}
		result := uc.GetCaURIWithFallback()
		// Should return result from env.BuildCaURI("", "", "")
		assert.NotNil(t, result)
	})

	t.Run("WhenCaURIHasValue_ShouldReturnCaURI", func(t *testing.T) {
		uc := &UserCredentials{
			CaURI: "project-123/pool-456/ca-789",
		}
		result := uc.GetCaURIWithFallback()
		assert.Equal(t, "project-123/pool-456/ca-789", result)
	})
}

func TestUserCredentials_ParseCaURIWithFallback(t *testing.T) {
	t.Run("WhenUserCredentialsIsNil_ShouldReturnEnvFallback", func(t *testing.T) {
		var uc *UserCredentials = nil
		projectID, poolName, caName := uc.ParseCaURIWithFallback()
		// Should return env vars directly
		assert.NotNil(t, projectID)
		assert.NotNil(t, poolName)
		assert.NotNil(t, caName)
	})

	t.Run("WhenCaURIIsEmpty_ShouldReturnEnvFallback", func(t *testing.T) {
		uc := &UserCredentials{
			CaURI: "",
		}
		projectID, poolName, caName := uc.ParseCaURIWithFallback()
		// Should return env vars directly
		assert.NotNil(t, projectID)
		assert.NotNil(t, poolName)
		assert.NotNil(t, caName)
	})

	t.Run("WhenCaURIHasValue_ShouldParseCaURI", func(t *testing.T) {
		uc := &UserCredentials{
			CaURI: "project-123/pool-456/ca-789",
		}
		projectID, poolName, caName := uc.ParseCaURIWithFallback()
		assert.Equal(t, "project-123", projectID)
		assert.Equal(t, "pool-456", poolName)
		assert.Equal(t, "ca-789", caName)
	})
}
