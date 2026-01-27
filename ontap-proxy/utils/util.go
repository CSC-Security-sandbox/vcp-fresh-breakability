package utils

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/api/ontap-proxy-servergen"
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
