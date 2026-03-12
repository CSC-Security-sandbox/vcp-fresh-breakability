package handlers

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
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

// ontapFirstLoginBanner is the message ONTAP prints on first CLI login; we strip it from responses.
const ontapFirstLoginBanner = "This is your first recorded login."

// ontapFirstLoginBannerRe matches a line containing only the first-login banner (with optional surrounding whitespace).
var ontapFirstLoginBannerRe = regexp.MustCompile(`(?m)^\s*` + regexp.QuoteMeta(ontapFirstLoginBanner) + `\s*[\r\n]*`)

// StripOntapLoginBanner removes the ONTAP "first recorded login" message from CLI output
// so it is not shown in API responses. Handles any amount of surrounding newlines or whitespace.
func StripOntapLoginBanner(output string) string {
	if output == "" {
		return output
	}
	s := ontapFirstLoginBannerRe.ReplaceAllString(output, "")
	return strings.TrimLeft(s, "\r\n")
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

// IsCLISuccess checks if CLI output indicates a successful operation.
func IsCLISuccess(cliOutput string) bool {
	output := strings.ToLower(cliOutput)

	// Real errors are lines starting with "Error:"; ignore "No error" / "Status Details: No error".
	if hasErrorLine(cliOutput) {
		return false
	}

	// "failed" indicates failure only when it's a clear failure phrase (e.g. "Operation failed"), not when
	// it's the value of Operation Status in a successful list/show (e.g. "Operation Status: Failed").
	if hasFailedLine(cliOutput) {
		return false
	}

	// Other failure indicators (substring match is safe for these)
	failureIndicators := []string{
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

// hasErrorLine returns true if output contains a line that starts with "Error:" (case-insensitive).
func hasErrorLine(output string) bool {
	for _, line := range strings.Split(output, "\n") {
		if strings.HasPrefix(strings.TrimSpace(strings.ToLower(line)), "error:") {
			return true
		}
	}
	return false
}

// hasFailedLine returns true if output contains a line with a clear failure phrase (e.g. "Operation failed",
// "Command failed", "failed to"). Used so that a successful list/show output that includes an operation in
// state "Failed" (e.g. "Operation Status: Failed") is not misclassified as CLI failure.
func hasFailedLine(output string) bool {
	lower := strings.ToLower(output)
	for _, line := range strings.Split(lower, "\n") {
		line = strings.TrimSpace(line)
		if strings.Contains(line, "operation failed") || strings.Contains(line, "command failed") || strings.Contains(line, "failed to") {
			return true
		}
	}
	return false
}
