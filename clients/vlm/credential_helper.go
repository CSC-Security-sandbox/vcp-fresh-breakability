package vlm

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
)

// getEncryptionKey returns the encryption key from environment variable
// Returns nil if OntapCredentialKey is not set (indicating no encryption should be used)
func getEncryptionKey() []byte {
	key := env.GetString(ONTAP_CREDENTIAL_ENCRYPT_KEY, "")
	if key == "" {
		return nil
	}
	// Ensure key is exactly 32 bytes for AES-256
	keyBytes := []byte(key)
	if len(keyBytes) < 32 {
		// Pad with zeros if too short
		paddedKey := make([]byte, 32)
		copy(paddedKey, keyBytes)
		return paddedKey
	}
	// Truncate if too long
	return keyBytes[:32]
}

// ontapCredentialsAlias is used to avoid recursion in MarshalJSON
type ontapCredentialsAlias OntapCredentials

// encrypt encrypts the OntapCredentials using AES-GCM
func (o OntapCredentials) encrypt() (string, error) {
	encryptionKey := getEncryptionKey()
	if encryptionKey == nil {
		// No encryption key set, return unencrypted JSON
		data, err := json.Marshal(ontapCredentialsAlias(o))
		if err != nil {
			return "", err
		}
		return string(data), nil
	}

	// Marshal using alias to avoid recursion
	data, err := json.Marshal(ontapCredentialsAlias(o))
	if err != nil {
		return "", err
	}

	block, err := aes.NewCipher(encryptionKey)
	if err != nil {
		return "", err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}

	ciphertext := gcm.Seal(nonce, nonce, data, nil)
	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

// decrypt decrypts the encrypted OntapCredentials
func decryptOntapCredentials(encryptedData string) (OntapCredentials, error) {
	encryptionKey := getEncryptionKey()
	if encryptionKey == nil {
		// No encryption key set, treat as unencrypted JSON
		var credentials ontapCredentialsAlias
		if err := json.Unmarshal([]byte(encryptedData), &credentials); err != nil {
			return OntapCredentials{}, err
		}
		return OntapCredentials(credentials), nil
	}

	data, err := base64.StdEncoding.DecodeString(encryptedData)
	if err != nil {
		return OntapCredentials{}, err
	}

	block, err := aes.NewCipher(encryptionKey)
	if err != nil {
		return OntapCredentials{}, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return OntapCredentials{}, err
	}

	nonceSize := gcm.NonceSize()
	if len(data) < nonceSize {
		return OntapCredentials{}, errors.New("invalid encrypted data")
	}

	nonce, ciphertext := data[:nonceSize], data[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return OntapCredentials{}, err
	}

	var credentials ontapCredentialsAlias
	if err := json.Unmarshal(plaintext, &credentials); err != nil {
		return OntapCredentials{}, err
	}

	return OntapCredentials(credentials), nil
}

// MarshalJSON implements custom JSON marshaling to encrypt the credentials
func (o OntapCredentials) MarshalJSON() ([]byte, error) {
	// Check if credentials are empty
	if o.AdminPassword == "" && o.Certificate.Certificate == "" {
		return json.Marshal(nil)
	}

	encrypted, err := o.encrypt()
	if err != nil {
		return nil, err
	}
	return json.Marshal(encrypted)
}

// UnmarshalJSON implements custom JSON unmarshaling to decrypt the credentials
func (o *OntapCredentials) UnmarshalJSON(data []byte) error {
	var encryptedData string
	if err := json.Unmarshal(data, &encryptedData); err != nil {
		return err
	}

	if encryptedData == "" {
		*o = OntapCredentials{}
		return nil
	}

	encryptionKey := getEncryptionKey()
	if encryptionKey == nil {
		// No encryption key set, treat as unencrypted JSON
		var credentials ontapCredentialsAlias
		if err := json.Unmarshal([]byte(encryptedData), &credentials); err != nil {
			return err
		}
		*o = OntapCredentials(credentials)
		return nil
	}

	// Try to decrypt as encrypted data
	decrypted, err := decryptOntapCredentials(encryptedData)
	if err != nil {
		return err
	}
	*o = decrypted
	return nil
}
