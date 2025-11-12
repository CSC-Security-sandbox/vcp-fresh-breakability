package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExtractOntapPath(t *testing.T) {
	tests := []struct {
		name     string
		fullPath string
		expected string
	}{
		{
			name:     "Valid ONTAP API path with full project path",
			fullPath: "/v1beta/projects/1234/locations/us-central1/pools/my-pool/ontap/api/storage/qtrees",
			expected: "/api/storage/qtrees",
		},
		{
			name:     "ONTAP API path with query parameters",
			fullPath: "/v1beta/projects/1234/locations/us-central1/pools/my-pool/ontap/api/storage/volumes?fields=name,size",
			expected: "/api/storage/volumes?fields=name,size",
		},
		{
			name:     "Path without ontap segment",
			fullPath: "/v1beta/projects/1234/locations/us-central1/pools/my-pool/invalid-path",
			expected: "",
		},
		{
			name:     "Empty path",
			fullPath: "",
			expected: "",
		},
		{
			name:     "ONTAP API at root level",
			fullPath: "/ontap/api/storage/qtrees",
			expected: "/api/storage/qtrees",
		},
		{
			name:     "ONTAP API at end of path",
			fullPath: "/v1beta/projects/123/locations/us-central1/pools/pool1/ontap",
			expected: "/",
		},
		{
			name:     "Multiple ontap segments - should use first occurrence",
			fullPath: "/v1beta/projects/123/ontap/api1/ontap/api2",
			expected: "/api1/ontap/api2",
		},
		{
			name:     "Path starting with ontap (no leading slash)",
			fullPath: "ontap/api/storage/qtrees",
			expected: "/api/storage/qtrees",
		},
		{
			name:     "ONTAP API path with UUID",
			fullPath: "/v1beta/projects/1234/locations/us-central1/pools/my-pool/ontap/api/storage/volumes/550e8400-e29b-41d4-a716-446655440000",
			expected: "/api/storage/volumes/550e8400-e29b-41d4-a716-446655440000",
		},
		{
			name:     "ONTAP API path with nested paths",
			fullPath: "/v1beta/projects/1234/locations/us-central1/pools/my-pool/ontap/api/storage/volumes/12345/snapshots",
			expected: "/api/storage/volumes/12345/snapshots",
		},
		{
			name:     "Path with only ontap segment",
			fullPath: "/ontap",
			expected: "/",
		},
		{
			name:     "Path with ontap and trailing slash",
			fullPath: "/v1beta/projects/1234/locations/us-central1/pools/my-pool/ontap/",
			expected: "/",
		},
		{
			name:     "Path with private API",
			fullPath: "/v1beta/projects/1234/locations/us-central1/pools/my-pool/ontap/api/private/cli/snapmirror/break",
			expected: "/api/private/cli/snapmirror/break",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExtractOntapPath(tt.fullPath)
			assert.Equal(t, tt.expected, result, "ExtractOntapPath(%q) = %q, want %q", tt.fullPath, result, tt.expected)
		})
	}
}
