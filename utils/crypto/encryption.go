// Package crypto implements the database encryption and decryption cipher
package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"strings"

	"golang.org/x/crypto/pbkdf2"
)

const (
	separator = ":"
	nonceSize = 12 // GCM standard nonce size
)

// SecretsHandler handles all cryptographic operations supported by qstackcrypto
type SecretsHandler struct {
	version *Version
}

// NewSecretsHandler returns an instance of SecretsHandler that can handle
// cryptographic operations using the specified version and block size
func NewSecretsHandler(version *Version) *SecretsHandler {
	return &SecretsHandler{
		version: version,
	}
}

// Encrypt encrypts the specified plain text, using the specified pass phrase,
// and returns the cypher text.
func (sh *SecretsHandler) Encrypt(passphrase, plaintext string) (*string, error) {
	salt, err := randomBytes(8)
	if err != nil {
		return nil, err
	}
	nonce, err := randomBytes(nonceSize)
	if err != nil {
		return nil, err
	}

	key := pbkdf2.Key([]byte(passphrase), salt, sh.version.Iterations, sh.version.Bits/8, sha1.New)
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	enctext := gcm.Seal(nil, nonce, []byte(plaintext), nil)

	cyphertext := fmt.Sprintf("%s%s%s%s%s%s%s", sh.version.Name,
		separator, base64.StdEncoding.EncodeToString(salt),
		separator, base64.StdEncoding.EncodeToString(nonce),
		separator, base64.StdEncoding.EncodeToString(enctext))
	return &cyphertext, nil
}

// Decrypt decrypts the specified cypher text, using the specified pass phrase,
// and returns the plain text.
func (sh *SecretsHandler) Decrypt(passphrase, cyphertext string) (*string, error) {
	if sh.version == nil {
		return nil, errors.New("Version not set for SecretsHandler")
	}

	version, salt, nonce, enctext, err := sh.parse(cyphertext)
	if err != nil {
		return nil, err
	}
	if version == nil || salt == nil || nonce == nil || enctext == nil {
		return nil, errors.New("Internal error - one or more nil values returned from parsing")
	}
	if *version != sh.version.Name {
		return nil, fmt.Errorf("SecretsHandler not configured for specified cypher text")
	}

	key := pbkdf2.Key([]byte(passphrase), salt, sh.version.Iterations, sh.version.Bits/8, sha1.New)
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	plaintext, err := gcm.Open(nil, nonce, enctext, nil)
	if err != nil {
		return nil, err
	}

	plainTextStr := string(plaintext)
	return &plainTextStr, nil
}

func (sh *SecretsHandler) parse(cyphertext string) (*string, []byte, []byte, []byte, error) {
	components := strings.Split(cyphertext, separator)
	if len(components) != 4 {
		return nil, nil, nil, nil, errors.New("Could not parse cypher text")
	}

	salt, err := base64.StdEncoding.DecodeString(components[1])
	if err != nil {
		return nil, nil, nil, nil, errors.New("Could not parse cypher text")
	}
	nonce, err := base64.StdEncoding.DecodeString(components[2])
	if err != nil {
		return nil, nil, nil, nil, errors.New("Could not parse cypher text")
	}
	enctext, err := base64.StdEncoding.DecodeString(components[3])
	if err != nil {
		return nil, nil, nil, nil, errors.New("Could not parse cypher text")
	}

	return &components[0], salt, nonce, enctext, nil
}
