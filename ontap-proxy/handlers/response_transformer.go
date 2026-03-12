package handlers

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/utils"
)

// quoteCLIArg quotes a CLI argument if it contains spaces (e.g. "7 years").
// Used when building ONTAP CLI commands so arguments are safely passed.
func quoteCLIArg(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return `""`
	}
	if strings.Contains(s, " ") || strings.Contains(s, "\t") {
		return fmt.Sprintf("%q", s)
	}
	return s
}

// StripOntapLoginBanner delegates to utils.StripOntapLoginBanner for backward compatibility.
func StripOntapLoginBanner(output string) string {
	return utils.StripOntapLoginBanner(output)
}

// ParseCLIError extracts a user-facing error message from ONTAP CLI output.
// It looks for an "Error: <message>" line; otherwise returns the full output.
// Use the message for HTTP response bodies and for 404 detection (e.g. "not found", "does not exist").
func ParseCLIError(cliOutput string) (message string) {
	message = cliOutput
	if strings.Contains(strings.ToLower(cliOutput), "error") {
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
	return message
}

// OntapCodeToInt parses an ONTAP CLI error code string to an int (e.g. for HTTP status mapping).
// Returns 400 if the code is unparseable or contains non-numeric characters.
func OntapCodeToInt(code string) int {
	n, err := strconv.Atoi(strings.TrimSpace(code))
	if err != nil {
		return 400
	}
	return n
}

// ParseSnaplockAbortError parses the output of `snaplock legal-hold abort` and returns
// a user-facing message. Recognizes ONTAP messages like "SnapLock legal-hold operation is complete"
// and returns a short message; otherwise falls back to ParseCLIError.
func ParseSnaplockAbortError(cliOutput string) string {
	lower := strings.ToLower(strings.TrimSpace(cliOutput))
	if strings.Contains(lower, "operation is complete") {
		return "SnapLock legal-hold operation is complete; abort only applies to in-progress operations"
	}
	if strings.Contains(lower, "not found") {
		return "SnapLock legal-hold operation not found"
	}
	msg := ParseCLIError(cliOutput)
	if msg != "" {
		return msg
	}
	return cliOutput
}

// IsCLISuccess checks if CLI output indicates a successful operation.
func IsCLISuccess(cliOutput string) bool {
	lines := strings.Split(cliOutput, "\n")

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		lower := strings.ToLower(trimmed)

		// Prefix checks: line start indicates error (avoids "Number of Files Failed: 0" etc.)
		if strings.HasPrefix(lower, "error:") ||
			strings.HasPrefix(lower, "failed") ||
			strings.HasPrefix(lower, "invalid") {
			return false
		}
		// Contains checks: common error phrases anywhere in line
		if strings.Contains(lower, "not found") ||
			strings.Contains(lower, "permission denied") ||
			strings.Contains(lower, "access denied") ||
			strings.Contains(lower, "operation failed") ||
			strings.Contains(lower, "command failed") {
			return false
		}
	}

	return true
}
