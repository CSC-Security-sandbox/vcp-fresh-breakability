package utils

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"testing"
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
