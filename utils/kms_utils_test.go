package utils

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
)

func TestParseKeyFullPathResource(t *testing.T) {
	tests := []struct {
		input    string
		expected ParsedKeyFullPathResource
		hasError bool
	}{
		{
			input: "projects/project-id/locations/location/keyRings/key-ring/cryptoKeys/crypto-key",
			expected: ParsedKeyFullPathResource{
				ProjectID: "project-id",
				Location:  "location",
				KeyRing:   "key-ring",
				CryptoKey: "crypto-key",
			},
			hasError: false,
		},
		{
			input:    "invalid/resource/string",
			expected: ParsedKeyFullPathResource{},
			hasError: true,
		},
	}

	for _, test := range tests {
		result, err := ParseKeyFullPathResource(test.input)
		if test.hasError {
			assert.Nil(t, result)
			assert.NotNil(t, err)
		} else {
			assert.NotNil(t, result)
			assert.Nil(t, err)
		}
	}
}

func TestParseServiceAccountEmail(t *testing.T) {
	tests := []struct {
		input    string
		expected *ParsedServiceAccount
		hasError bool
	}{
		{
			input: "n-cmek-auso1-1234@5678.iam.gserviceaccount.com",
			expected: &ParsedServiceAccount{
				Prefix:            "n-cmek-auso1",
				CustomerProjectID: "1234",
				GlobalProjectID:   "5678",
			},
			hasError: false,
		},
		{
			input:    "invalid-email-format",
			expected: nil,
			hasError: true,
		},
	}

	for _, test := range tests {
		result, err := ParseServiceAccountEmail(test.input)
		if test.hasError {
			if err == nil {
				t.Errorf("expected error for input %s, got nil", test.input)
			} else if err.Error() != ErrInvalidServiceAccountEmail {
				t.Errorf("expected error message %s, got %s", ErrInvalidServiceAccountEmail, err.Error())
			}
		} else {
			if err != nil {
				t.Errorf("unexpected error for input %s: %v", test.input, err)
			}
			if !reflect.DeepEqual(result, test.expected) {
				t.Errorf("expected %v, got %v", test.expected, result)
			}
		}
	}
}

func TestDetermineStartToCloseTimeoutBasedOnUsedSize(t *testing.T) {
	// Generated using GitHub copilot
	tests := []struct {
		name            string
		volumes         []*datamodel.Volume
		expectedTimeout int64
	}{
		{
			name: "Low occupied space (<10GB)",
			volumes: []*datamodel.Volume{
				{UsedBytes: 5 * 1024 * 1024 * 1024}, // 5GB
			},
			expectedTimeout: 15,
		},
		{
			name: "Less than 100GB",
			volumes: []*datamodel.Volume{
				{UsedBytes: 50 * 1024 * 1024 * 1024}, // 50GB
			},
			expectedTimeout: 30,
		},
		{
			name: "Less than 500GB",
			volumes: []*datamodel.Volume{
				{UsedBytes: 300 * 1024 * 1024 * 1024}, // 300GB
			},
			expectedTimeout: 150,
		},
		{
			name: "Less than 1000GB",
			volumes: []*datamodel.Volume{
				{UsedBytes: 800 * 1024 * 1024 * 1024}, // 800GB
			},
			expectedTimeout: 300,
		},
		{
			name: "Less than 5000GB",
			volumes: []*datamodel.Volume{
				{UsedBytes: 4000 * 1024 * 1024 * 1024}, // 4000GB
			},
			expectedTimeout: 1500,
		},
		{
			name: "Less than 10000GB",
			volumes: []*datamodel.Volume{
				{UsedBytes: 9000 * 1024 * 1024 * 1024}, // 9000GB
			},
			expectedTimeout: 3000,
		},
		{
			name: "Greater than or equal to 10000GB",
			volumes: []*datamodel.Volume{
				{UsedBytes: 12000 * 1024 * 1024 * 1024}, // 12000GB
			},
			expectedTimeout: 10000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			timeout := DetermineStartToCloseTimeoutBasedOnUsedSize(tt.volumes)
			assert.Equal(t, tt.expectedTimeout, timeout)
		})
	}
}
