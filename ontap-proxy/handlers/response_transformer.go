package handlers

import (
	"strings"
)

// ParseCLIError attempts to extract an error message from CLI output.
// ONTAP CLI errors often contain specific error codes and messages.
func ParseCLIError(cliOutput string) (code string, message string) {
	// Default values
	code = ""
	message = cliOutput

	// Look for common ONTAP error patterns
	// ONTAP often returns errors in formats like:
	// "Error: <error message>"
	// or includes error codes in the output

	if strings.Contains(strings.ToLower(cliOutput), "error") {
		// Extract the error portion
		lines := strings.Split(cliOutput, "\n")
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(strings.ToLower(line), "error:") {
				message = strings.TrimPrefix(line, "Error:")
				message = strings.TrimPrefix(message, "error:")
				message = strings.TrimSpace(message)
				break
			}
		}
	}

	return code, message
}

// IsCLISuccess checks if CLI output indicates a successful operation.
func IsCLISuccess(cliOutput string) bool {
	output := strings.ToLower(cliOutput)

	// Check for common failure indicators
	failureIndicators := []string{
		"error",
		"failed",
		"not found",
		"permission denied",
		"access denied",
		"invalid",
	}

	for _, indicator := range failureIndicators {
		if strings.Contains(output, indicator) {
			return false
		}
	}

	return true
}
