package utils

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	ontapserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/api/ontap-proxy-servergen"
)

const (
	// OntapAPISegment is the path segment that identifies the start of ONTAP API paths
	OntapAPISegment = "ontap"
)

func ExtractOntapPath(fullPath string) string {
	parts := strings.Split(fullPath, "/")

	ontapApiIndex := -1
	for i, part := range parts {
		if part == OntapAPISegment {
			ontapApiIndex = i
			break
		}
	}

	if ontapApiIndex == -1 {
		return ""
	}

	ontapPath := "/" + strings.Join(parts[ontapApiIndex+1:], "/")
	return ontapPath
}

// WriteErrorResponse writes a JSON error response with code and message to the ResponseWriter.
func WriteErrorResponse(w http.ResponseWriter, code int, message string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	err := json.NewEncoder(w).Encode(&ontapserver.Error{
		Code:    code,
		Message: message,
	})
	if err != nil {
		// Fallback: write message as plain text directly to body
		// Note: Headers and status code are already written, so we can only write the body
		// Content-Type will remain "application/json" but body will be plain text
		_, _ = w.Write([]byte(message))
	}
}

// ParseSizeString parses a size string (e.g. "10g", "100m", "1024", "10.5GB") into bytes.
// Supports units: K/KB, M/MB, G/GB, T/TB, P/PB (case-insensitive, base-1024).
// Decimal numbers are allowed (e.g. "10.5g"). Leading + or - is not allowed; size must be positive.
// If invalid, returns 0.
func ParseSizeString(s string) float64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	if len(s) > 0 && (s[0] == '+' || s[0] == '-') {
		return 0
	}
	var numPart string
	var unitPart string
	for i, r := range s {
		if (r < '0' || r > '9') && r != '.' {
			numPart = s[:i]
			unitPart = strings.TrimSpace(strings.ToUpper(s[i:]))
			break
		}
	}
	if numPart == "" {
		numPart = s
	}
	val, err := strconv.ParseFloat(numPart, 64)
	if err != nil {
		return 0
	}
	var mult float64
	switch unitPart {
	case "":
		mult = 1
	case "K", "KB":
		mult = 1024
	case "M", "MB":
		mult = 1024 * 1024
	case "G", "GB":
		mult = 1024 * 1024 * 1024
	case "T", "TB":
		mult = 1024 * 1024 * 1024 * 1024
	case "P", "PB":
		mult = 1024 * 1024 * 1024 * 1024 * 1024
	default:
		return 0
	}
	result := val * mult
	if result <= 0 {
		return 0
	}
	return result
}
