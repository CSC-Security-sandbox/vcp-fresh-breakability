package vlm

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// setTestEnv sets an environment variable and returns a cleanup function
func setTestEnv(t *testing.T, key, value string) func() {
	t.Helper()
	if err := os.Setenv(key, value); err != nil {
		t.Fatalf("Failed to set environment variable %s: %v", key, err)
	}
	return func() {
		if err := os.Unsetenv(key); err != nil {
			t.Errorf("Failed to unset environment variable %s: %v", key, err)
		}
	}
}

func TestGetEncryptionKey(t *testing.T) {
	tests := []struct {
		name        string
		envKey      string
		expectedLen int
		shouldBeNil bool
	}{
		{
			name:        "No encryption key set",
			envKey:      "",
			shouldBeNil: true,
		},
		{
			name:        "Short key is padded",
			envKey:      "short",
			expectedLen: 32,
			shouldBeNil: false,
		},
		{
			name:        "Exact length key",
			envKey:      "12345678901234567890123456789012",
			expectedLen: 32,
			shouldBeNil: false,
		},
		{
			name:        "Long key is truncated",
			envKey:      "1234567890123456789012345678901234567890",
			expectedLen: 32,
			shouldBeNil: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.envKey != "" {
				cleanup := setTestEnv(t, ONTAP_CREDENTIAL_ENCRYPT_KEY, tt.envKey)
				defer cleanup()
			} else if err := os.Unsetenv(ONTAP_CREDENTIAL_ENCRYPT_KEY); err != nil {
				t.Fatalf("Failed to unset environment variable: %v", err)
			}

			key := getEncryptionKey()
			if tt.shouldBeNil {
				assert.Nil(t, key)
			} else {
				assert.NotNil(t, key)
				assert.Equal(t, tt.expectedLen, len(key))
			}
		})
	}
}

func TestOntapCredentialsEncryptionDecryption(t *testing.T) {
	tests := []struct {
		name        string
		credentials OntapCredentials
		encryptKey  string
	}{
		{
			name: "With encryption key",
			credentials: OntapCredentials{
				AdminPassword: "testpass",
				Certificate: OntapCertificate{
					Certificate: "testcert",
					PrivateKey:  "testkey",
				},
			},
			encryptKey: "12345678901234567890123456789012",
		},
		{
			name: "Without encryption key",
			credentials: OntapCredentials{
				AdminPassword: "testpass",
				Certificate: OntapCertificate{
					Certificate: "testcert",
					PrivateKey:  "testkey",
				},
			},
			encryptKey: "",
		},
		{
			name:        "Empty credentials",
			credentials: OntapCredentials{},
			encryptKey:  "12345678901234567890123456789012",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.encryptKey != "" {
				cleanup := setTestEnv(t, ONTAP_CREDENTIAL_ENCRYPT_KEY, tt.encryptKey)
				defer cleanup()
			} else if err := os.Unsetenv(ONTAP_CREDENTIAL_ENCRYPT_KEY); err != nil {
				t.Fatalf("Failed to unset environment variable: %v", err)
			}

			// Test MarshalJSON
			data, err := json.Marshal(tt.credentials)
			assert.NoError(t, err)

			// Test UnmarshalJSON
			var decoded OntapCredentials
			err = json.Unmarshal(data, &decoded)
			assert.NoError(t, err)

			// Verify the roundtrip
			assert.Equal(t, tt.credentials.AdminPassword, decoded.AdminPassword)
			assert.Equal(t, tt.credentials.Certificate.Certificate, decoded.Certificate.Certificate)
			assert.Equal(t, tt.credentials.Certificate.PrivateKey, decoded.Certificate.PrivateKey)
		})
	}
}

func TestEncryptDecrypt(t *testing.T) {
	creds := OntapCredentials{
		AdminPassword: "secretpassword",
		Certificate: OntapCertificate{
			Certificate: "cert-data",
			PrivateKey:  "private-key-data",
		},
	}

	// Test with encryption
	t.Run("With encryption", func(t *testing.T) {
		cleanup := setTestEnv(t, ONTAP_CREDENTIAL_ENCRYPT_KEY, "12345678901234567890123456789012")
		defer cleanup()

		encrypted, err := creds.encrypt()
		assert.NoError(t, err)
		assert.NotEmpty(t, encrypted)

		decrypted, err := decryptOntapCredentials(encrypted)
		assert.NoError(t, err)
		assert.Equal(t, creds.AdminPassword, decrypted.AdminPassword)
		assert.Equal(t, creds.Certificate.Certificate, decrypted.Certificate.Certificate)
		assert.Equal(t, creds.Certificate.PrivateKey, decrypted.Certificate.PrivateKey)
	})

	// Test without encryption
	t.Run("Without encryption", func(t *testing.T) {
		if err := os.Unsetenv(ONTAP_CREDENTIAL_ENCRYPT_KEY); err != nil {
			t.Fatalf("Failed to unset environment variable: %v", err)
		}

		encrypted, err := creds.encrypt()
		assert.NoError(t, err)

		decrypted, err := decryptOntapCredentials(encrypted)
		assert.NoError(t, err)
		assert.Equal(t, creds.AdminPassword, decrypted.AdminPassword)
		assert.Equal(t, creds.Certificate.Certificate, decrypted.Certificate.Certificate)
		assert.Equal(t, creds.Certificate.PrivateKey, decrypted.Certificate.PrivateKey)
	})
}

func TestInvalidEncryptedData(t *testing.T) {
	cleanup := setTestEnv(t, ONTAP_CREDENTIAL_ENCRYPT_KEY, "12345678901234567890123456789012")
	defer cleanup()

	tests := []struct {
		name        string
		invalidData string
	}{
		{
			name:        "Invalid base64",
			invalidData: "not-base64-data",
		},
		{
			name:        "Too short after base64 decode",
			invalidData: "aGVsbG8=", // "hello" in base64
		},
		{
			name:        "Empty string",
			invalidData: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := decryptOntapCredentials(tt.invalidData)
			assert.Error(t, err)
		})
	}
}

func TestEmptyCredentials(t *testing.T) {
	creds := OntapCredentials{}

	// Test marshaling empty credentials
	data, err := json.Marshal(creds)
	assert.NoError(t, err)
	assert.Equal(t, "null", string(data))

	// Test unmarshaling empty credentials
	var decoded OntapCredentials
	err = json.Unmarshal([]byte("null"), &decoded)
	assert.NoError(t, err)
	assert.Empty(t, decoded.AdminPassword)
	assert.Empty(t, decoded.Certificate.Certificate)
	assert.Empty(t, decoded.Certificate.PrivateKey)
}
