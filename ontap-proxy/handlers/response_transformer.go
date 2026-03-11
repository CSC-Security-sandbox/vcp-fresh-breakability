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
// ONTAP often uses "Error: <message>"; otherwise the full output is returned.
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
