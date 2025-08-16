package errors

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKmsRotationErrorConstants(t *testing.T) {
	// Test that the new error constants are defined correctly
	assert.Equal(t, 8001, ErrKMSRotate)
	assert.Equal(t, 8002, ErrServiceAccountNotFound)

	// Verify they are valid error codes
	assert.Greater(t, ErrKMSRotate, 0)
	assert.Greater(t, ErrServiceAccountNotFound, 0)
}

func TestErrorConstantsUniqueness(t *testing.T) {
	// Test that error constants are unique
	errorCodes := []int{
		ErrDatabaseDataReadError,
		ErrKMSRotate,
		ErrServiceAccountNotFound,
		// Add other error codes as needed
	}

	// Check uniqueness
	seen := make(map[int]bool)
	for _, errorCode := range errorCodes {
		assert.False(t, seen[errorCode], "Duplicate error code found: %d", errorCode)
		seen[errorCode] = true
	}

	// Verify the new error codes are in the list
	assert.Contains(t, seen, ErrKMSRotate)
	assert.Contains(t, seen, ErrServiceAccountNotFound)
}

func TestErrorCodeValues(t *testing.T) {
	// Test that the error codes have the expected values
	assert.Equal(t, 8001, ErrKMSRotate, "ErrKMSRotate should have value 6069")
	assert.Equal(t, 8002, ErrServiceAccountNotFound, "ErrServiceAccountNotFound should have value 6070")
}
