package utils

import (
	"encoding/base64"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	errors2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"google.golang.org/api/cloudkms/v1"
	"google.golang.org/api/googleapi"
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

func TestValidateKeyProperties(t *testing.T) {
	keyName := "KeyName"
	keyRing := "KeyRing"
	t.Run("WhenKeyIsNil", func(tt *testing.T) {
		var cryptoKey *cloudkms.CryptoKey = nil

		err := ValidateKeyProperties(cryptoKey, keyName, keyRing)
		assert.Error(tt, err)
		assert.EqualError(tt, err, "Transient error: Key access verification failed - Unable to get Crypto key from Google")
	})
	t.Run("WhenKeyPrimaryIsNil", func(tt *testing.T) {
		cryptoKey := cloudkms.CryptoKey{Primary: nil}
		err := ValidateKeyProperties(&cryptoKey, keyName, keyRing)
		assert.Error(tt, err)
		assert.EqualError(tt, err, "Failed to validate KMS key due to precondition failure: Specified key KeyName in KeyRing algorithm is not supported")
	})
	t.Run("WhenKeyAlgorithmDoesNotMatch", func(tt *testing.T) {
		err := ValidateKeyProperties(&cloudkms.CryptoKey{Primary: &cloudkms.CryptoKeyVersion{Algorithm: "GOOGLE_ASYMMETRIC_ENCRYPTION"}}, keyName, keyRing)
		assert.Error(tt, err)
		assert.EqualError(tt, err, "Failed to validate KMS key due to precondition failure: Specified key KeyName in KeyRing algorithm is not supported")
	})
	t.Run("WhenKeyHasBeenDisabled", func(tt *testing.T) {
		err := ValidateKeyProperties(&cloudkms.CryptoKey{Primary: &cloudkms.CryptoKeyVersion{Algorithm: "GOOGLE_SYMMETRIC_ENCRYPTION", State: "DISABLED"}}, keyName, keyRing)
		assert.Error(tt, err)
		var cerr *errors2.CustomError
		assert.True(tt, errors.As(err, &cerr))
		assert.Equal(tt, errors2.ErrKMSKeyDisabledOrDestroyed, cerr.TrackingID)
		assert.Contains(tt, cerr.Error(), "KMS key is disabled or destroyed")
	})
	t.Run("WhenValidateKeyPropertiesSuccessful", func(tt *testing.T) {
		err := ValidateKeyProperties(&cloudkms.CryptoKey{Primary: &cloudkms.CryptoKeyVersion{Algorithm: "EXTERNAL_SYMMETRIC_ENCRYPTION", State: enabledKeyState}}, keyName, keyRing)
		assert.NoError(tt, err)
	})
}

func TestReturnEncryptRequest(t *testing.T) {
	tests := []struct {
		name      string
		plainText string
		expected  *cloudkms.EncryptRequest
	}{
		{
			name:      "Simple text encryption request",
			plainText: "test",
			expected: &cloudkms.EncryptRequest{
				Plaintext: base64.StdEncoding.EncodeToString([]byte("test")),
			},
		},
		{
			name:      "Empty string encryption request",
			plainText: "",
			expected: &cloudkms.EncryptRequest{
				Plaintext: base64.StdEncoding.EncodeToString([]byte("")),
			},
		},
		{
			name:      "Special characters encryption request",
			plainText: "Hello, World! @#$%^&*()",
			expected: &cloudkms.EncryptRequest{
				Plaintext: base64.StdEncoding.EncodeToString([]byte("Hello, World! @#$%^&*()")),
			},
		},
		{
			name:      "Unicode characters encryption request",
			plainText: "こんにちは世界",
			expected: &cloudkms.EncryptRequest{
				Plaintext: base64.StdEncoding.EncodeToString([]byte("こんにちは世界")),
			},
		},
		{
			name:      "Multiline text encryption request",
			plainText: "Line 1\nLine 2\nLine 3",
			expected: &cloudkms.EncryptRequest{
				Plaintext: base64.StdEncoding.EncodeToString([]byte("Line 1\nLine 2\nLine 3")),
			},
		},
		{
			name:      "JSON-like string encryption request",
			plainText: `{"key": "value", "number": 123}`,
			expected: &cloudkms.EncryptRequest{
				Plaintext: base64.StdEncoding.EncodeToString([]byte(`{"key": "value", "number": 123}`)),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ReturnEncryptRequest(tt.plainText)

			// Verify the result is not nil
			assert.NotNil(t, result)

			// Verify the structure matches expected
			assert.Equal(t, tt.expected.Plaintext, result.Plaintext)

			// Verify that the base64 encoding is correct by decoding it back
			decodedBytes, err := base64.StdEncoding.DecodeString(result.Plaintext)
			assert.NoError(t, err)
			assert.Equal(t, tt.plainText, string(decodedBytes))

			// Verify the entire struct matches
			assert.True(t, reflect.DeepEqual(tt.expected, result))
		})
	}
}

func TestIsKmsKeyUnreachable(t *testing.T) {
	t.Run("MatchesWhenGoogleApiBodyContainsKeyUnreachable", func(t *testing.T) {
		err := &googleapi.Error{
			Code: 412,
			Body: `{"error":{"code":412,"message":"FAILED_PRECONDITION: KMS key ... in Cloud EKM is unreachable. Retry later","status":"FAILED_PRECONDITION","details":[{"@type":"type.googleapis.com/google.rpc.PreconditionFailure","violations":[{"type":"KEY_UNREACHABLE"}]}]}}`,
		}
		msg, ok := IsKmsKeyUnreachable(err)
		assert.True(t, ok)
		assert.Contains(t, msg, "FAILED_PRECONDITION")
		assert.Contains(t, strings.ToLower(msg), "unreachable")
	})

	t.Run("MatchesWhenMessageMentionsCloudEkmUnreachable", func(t *testing.T) {
		msg, ok := IsKmsKeyUnreachable(errors.New("FAILED_PRECONDITION: KMS key in Cloud EKM is unreachable. Retry later"))
		assert.True(t, ok)
		assert.Contains(t, msg, "Cloud EKM")
	})

	t.Run("MatchesWhenBodyContainsEkmUnreachableReason", func(t *testing.T) {
		err := &googleapi.Error{
			Code:    503,
			Message: "Ekm connection unreachable",
			Body: `{
			  "error": {
				"code": 503,
				"message": "Ekm connection unreachable",
				"status": "UNAVAILABLE",
				"details": [
				  {
					"@type": "type.googleapis.com/google.rpc.ErrorInfo",
					"reason": "EKM_UNREACHABLE",
					"domain": "googleapis.com",
					"metadata": {
					  "service": "cloudkms.googleapis.com",
					  "method": "google.cloud.kms.v1.EkmService.GetEkmConnection",
					  "resource": "projects/123/locations/us-east1/ekmConnections/my-ekm"
					}
				  }
				]
			  }
			}`,
			Errors: []googleapi.ErrorItem{
				{
					Reason:  "EKM_UNREACHABLE",
					Message: "Ekm connection unreachable",
				},
			},
		}
		msg, ok := IsKmsKeyUnreachable(err)
		assert.True(t, ok)
		assert.Equal(t, "Ekm connection unreachable", msg)
	})

	t.Run("MatchesWhenGoogleApiMessageOnly", func(t *testing.T) {
		err := &googleapi.Error{
			Code:    http.StatusServiceUnavailable,
			Message: "Cloud EKM endpoint unreachable",
		}
		msg, ok := IsKmsKeyUnreachable(err)
		assert.True(t, ok)
		assert.Equal(t, "Cloud EKM endpoint unreachable", msg)
	})

	t.Run("ReturnsFalseForUnrelatedError", func(t *testing.T) {
		msg, ok := IsKmsKeyUnreachable(errors.New("some other failure"))
		assert.False(t, ok)
		assert.Empty(t, msg)
	})
}

func TestIsKmsPermissionDenied(t *testing.T) {
	t.Run("MatchesWhenGoogleApiCode403", func(t *testing.T) {
		err := &googleapi.Error{
			Code:    403,
			Message: "Permission denied on resource 'projects/test/locations/us-east1/keyRings/test/cryptoKeys/test'",
		}
		msg, ok := IsKmsPermissionDenied(err)
		assert.True(t, ok)
		assert.Contains(t, msg, "Permission denied")
	})

	t.Run("MatchesWhenGoogleApiCode403WithEmptyMessage", func(t *testing.T) {
		err := &googleapi.Error{
			Code:    403,
			Message: "",
		}
		msg, ok := IsKmsPermissionDenied(err)
		assert.True(t, ok)
		assert.Equal(t, "Permission denied accessing KMS key", msg)
	})

	t.Run("MatchesWhenErrorContainsPermissionDenied", func(t *testing.T) {
		err := errors.New("googleapi: Error 403: PERMISSION_DENIED: The caller does not have permission")
		msg, ok := IsKmsPermissionDenied(err)
		assert.True(t, ok)
		assert.Contains(t, msg, "PERMISSION_DENIED")
	})

	t.Run("MatchesWhenErrorContainsPermissionDeniedLowercase", func(t *testing.T) {
		err := errors.New("permission denied: access denied to key")
		msg, ok := IsKmsPermissionDenied(err)
		assert.True(t, ok)
		assert.Contains(t, msg, "permission denied")
	})

	t.Run("ReturnsFalseForNon403Error", func(t *testing.T) {
		err := &googleapi.Error{
			Code:    404,
			Message: "Resource not found",
		}
		msg, ok := IsKmsPermissionDenied(err)
		assert.False(t, ok)
		assert.Empty(t, msg)
	})

	t.Run("ReturnsFalseForUnrelatedError", func(t *testing.T) {
		msg, ok := IsKmsPermissionDenied(errors.New("some other failure"))
		assert.False(t, ok)
		assert.Empty(t, msg)
	})

	t.Run("ReturnsFalseForNilError", func(t *testing.T) {
		msg, ok := IsKmsPermissionDenied(nil)
		assert.False(t, ok)
		assert.Empty(t, msg)
	})
}
