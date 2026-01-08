package handlers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseCLIError(t *testing.T) {
	t.Run("extracts error message from Error: prefix", func(t *testing.T) {
		output := "Error: File not found"

		code, message := ParseCLIError(output)

		assert.Empty(t, code)
		assert.Equal(t, "File not found", message)
	})

	t.Run("extracts error message case insensitive", func(t *testing.T) {
		output := "error: permission denied"

		code, message := ParseCLIError(output)

		assert.Empty(t, code)
		assert.Equal(t, "permission denied", message)
	})

	t.Run("returns full output when no error prefix found", func(t *testing.T) {
		output := "Something went wrong with the operation"

		code, message := ParseCLIError(output)

		assert.Empty(t, code)
		assert.Equal(t, output, message)
	})

	t.Run("handles multiline error output", func(t *testing.T) {
		output := `Command failed
Error: Access denied
Please check permissions`

		code, message := ParseCLIError(output)

		assert.Empty(t, code)
		assert.Equal(t, "Access denied", message)
	})
}

func TestIsCLISuccess(t *testing.T) {
	t.Run("returns true for success messages", func(t *testing.T) {
		testCases := []struct {
			name   string
			output string
		}{
			{"empty output", ""},
			{"simple success", "OK"},
			{"deleted successfully", "Deleted successfully"},
			{"operation completed", "Operation completed"},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				assert.True(t, IsCLISuccess(tc.output))
			})
		}
	})

	t.Run("returns false for error messages", func(t *testing.T) {
		testCases := []struct {
			name   string
			output string
		}{
			{"error keyword", "Error: something went wrong"},
			{"failed keyword", "Operation failed"},
			{"not found", "File not found"},
			{"permission denied", "Permission denied"},
			{"access denied", "Access denied"},
			{"invalid", "Invalid parameter"},
			{"case insensitive error", "ERROR: test"},
			{"case insensitive failed", "FAILED to complete"},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				assert.False(t, IsCLISuccess(tc.output))
			})
		}
	})
}
