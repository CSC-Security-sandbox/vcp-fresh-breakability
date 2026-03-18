package workflows

import (
	"regexp"
	"strings"
)

const (
	// transitionVPGSuffix is appended to the sanitized pool name to form the transition VPG resourceId.
	transitionVPGSuffix = "-vpg"
	// maxVPGResourceIDLen is the maximum length for VPG resourceId (API pattern ^[a-z]([a-z0-9-]{0,61}[a-z0-9])?$).
	maxVPGResourceIDLen = 63
)

var (
	// nonVPGResourceIDChars matches characters not allowed in the VPG resourceId body [a-z0-9-].
	nonVPGResourceIDChars = regexp.MustCompile(`[^a-z0-9-]`)
)

func isOnlyDigits(s string) bool {
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return len(s) > 0
}

// TransitionVPGNameFromPoolName returns a valid VPG resourceId for the transition VPG created during
// auto→manual QoS type transition. The result matches the API pattern: ^[a-z]([a-z0-9-]{0,61}[a-z0-9])?$
// (max 63 chars, first character [a-z], last character [a-z0-9]).
// Format: <sanitized_pool_name>-vpg (e.g. "mypool-vpg"). For empty or invalid pool names, returns "p-vpg".
func TransitionVPGNameFromPoolName(poolName string) string {
	suffixLen := len(transitionVPGSuffix)
	maxBaseLen := maxVPGResourceIDLen - suffixLen // 59

	s := strings.ToLower(poolName)
	s = strings.ReplaceAll(s, " ", "-")
	s = strings.ReplaceAll(s, "_", "-")
	s = nonVPGResourceIDChars.ReplaceAllString(s, "")
	for strings.Contains(s, "--") {
		s = strings.ReplaceAll(s, "--", "-")
	}
	s = strings.Trim(s, "-")

	if s == "" {
		return "p-vpg"
	}
	if len(s) > maxBaseLen {
		s = s[:maxBaseLen]
		s = strings.Trim(s, "-")
	}
	if s == "" {
		return "p-vpg"
	}
	// First char must be [a-z]
	if s[0] < 'a' || s[0] > 'z' {
		s = "p-" + s
		body := strings.Trim(s[2:], "-")
		if body == "" || isOnlyDigits(body) {
			return "p-vpg"
		}
		if len(s) > maxBaseLen {
			s = s[:maxBaseLen]
			s = strings.Trim(s, "-")
		}
	}
	// Last char must be [a-z0-9]
	s = strings.TrimRight(s, "-")
	if s == "" {
		return "p-vpg"
	}
	if len(s) > maxBaseLen {
		s = s[:maxBaseLen]
		s = strings.TrimRight(s, "-")
	}
	if s == "" {
		return "p-vpg"
	}

	result := s + transitionVPGSuffix
	if len(result) > maxVPGResourceIDLen {
		result = result[:maxVPGResourceIDLen]
	}
	return result
}
