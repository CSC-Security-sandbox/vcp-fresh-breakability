package utils

import (
	"strings"
)

const (
	// OntapAPISegment is the path segment that identifies the start of ONTAP API paths
	OntapAPISegment = "ontap-api"
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
