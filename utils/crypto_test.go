package utils

import (
	"context"
	"encoding/base64"
	"testing"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestEncryptUsingQstackCryptoReturnsEncryptedStringOnValidInput(t *testing.T) {
	orig := log.Secret("mySecretPassword")
	encrypted, err := encryptUsingQstackCrypto(orig)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if encrypted == nil || *encrypted == "" {
		t.Errorf("expected non-empty encrypted string, got %v", encrypted)
	}
}

func TestEncryptUsingQstackCryptoReturnsDifferentValueThanInput(t *testing.T) {
	orig := log.Secret("anotherSecret")
	encrypted, err := encryptUsingQstackCrypto(orig)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if encrypted != nil && *encrypted == string(orig) {
		t.Errorf("expected encrypted value to differ from input")
	}
}

func TestDecryptUsingQstackCryptoReturnsDecryptedStringOnValidInput(t *testing.T) {
	orig := log.Secret("mySecretPassword")
	encrypted, err := encryptUsingQstackCrypto(orig)
	if err != nil {
		t.Fatalf("unexpected error during encryption: %v", err)
	}
	decrypted, err := decryptUsingQstackCrypto(log.Secret(*encrypted))
	if err != nil {
		t.Fatalf("unexpected error during decryption: %v", err)
	}
	if decrypted == nil || *decrypted != string(orig) {
		t.Errorf("expected decrypted value to match original, got %v", decrypted)
	}
}

func TestDecryptUsingQstackCryptoReturnsErrorOnInvalidInput(t *testing.T) {
	invalid := log.Secret("not-an-encrypted-value")
	decrypted, err := decryptUsingQstackCrypto(invalid)
	if err == nil {
		t.Errorf("expected error for invalid encrypted input, got nil")
	}
	if decrypted != nil {
		t.Errorf("expected nil decrypted value, got %v", decrypted)
	}
}

func TestProcessCredentialsReturnsCredentialsOnValidInput(t *testing.T) {
	// Create a valid JSON credentials, base64 encode, then encrypt
	credsJSON := `{"type":"service_account","project_id":"test","private_key_id":"id","private_key":"key","client_email":"email","client_id":"cid","auth_uri":"uri","token_uri":"uri","auth_provider_x509_cert_url":"url","client_x509_cert_url":"url"}`
	encoded := base64.StdEncoding.EncodeToString([]byte(credsJSON))
	encrypted, err := encryptUsingQstackCrypto(log.Secret(encoded))
	if err != nil {
		t.Fatalf("unexpected error during encryption: %v", err)
	}
	ctx := context.Background()
	creds, err := processCredentials(ctx, *encrypted)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if creds == nil {
		t.Errorf("expected credentials, got nil")
	}
}

func TestProcessCredentialsReturnsErrorOnDecryptFailure(t *testing.T) {
	ctx := context.Background()
	_, err := processCredentials(ctx, "not-an-encrypted-value")
	if err == nil {
		t.Errorf("expected error for invalid encrypted input, got nil")
	}
}

func TestProcessCredentialsReturnsErrorOnBase64DecodeFailure(t *testing.T) {
	// Encrypt a non-base64 string
	encrypted, err := encryptUsingQstackCrypto(log.Secret("not-base64"))
	if err != nil {
		t.Fatalf("unexpected error during encryption: %v", err)
	}
	ctx := context.Background()
	_, err = processCredentials(ctx, *encrypted)
	if err == nil {
		t.Errorf("expected error for invalid base64, got nil")
	}
}

func TestProcessCredentialsReturnsErrorOnInvalidJSON(t *testing.T) {
	// Base64 encode invalid JSON, then encrypt
	encoded := base64.StdEncoding.EncodeToString([]byte("not-json"))
	encrypted, err := encryptUsingQstackCrypto(log.Secret(encoded))
	if err != nil {
		t.Fatalf("unexpected error during encryption: %v", err)
	}
	ctx := context.Background()
	_, err = processCredentials(ctx, *encrypted)
	if err == nil {
		t.Errorf("expected error for invalid JSON, got nil")
	}
}

func TestDecryptAndDecodeCredentialsReturnsDecodedBytesOnValidInput(t *testing.T) {
	encoded := base64.StdEncoding.EncodeToString([]byte("mySecretData"))
	encrypted, err := encryptUsingQstackCrypto(log.Secret(encoded))
	if err != nil {
		t.Fatalf("unexpected error during encryption: %v", err)
	}
	decoded, err := decryptAndDecodeCredentials(*encrypted)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(decoded) != "mySecretData" {
		t.Errorf("expected decoded value to match original, got %s", decoded)
	}
}

func TestDecryptAndDecodeCredentialsReturnsErrorOnDecryptFailure(t *testing.T) {
	_, err := decryptAndDecodeCredentials("not-an-encrypted-value")
	if err == nil {
		t.Errorf("expected error for invalid encrypted input, got nil")
	}
}

func TestDecryptAndDecodeCredentialsReturnsErrorOnBase64DecodeFailure(t *testing.T) {
	encrypted, err := encryptUsingQstackCrypto(log.Secret("not-base64"))
	if err != nil {
		t.Fatalf("unexpected error during encryption: %v", err)
	}
	_, err = decryptAndDecodeCredentials(*encrypted)
	if err == nil {
		t.Errorf("expected error for invalid base64, got nil")
	}
}
