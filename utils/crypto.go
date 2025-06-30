package utils

import (
	"context"
	"encoding/base64"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/crypto"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	googleOauth2 "golang.org/x/oauth2/google"
	"google.golang.org/api/cloudkms/v1"
)

const passphrase = "Koi aisa kaise kar sakta hai"

var (
	// EncryptPassword accepts a golang string object and returns it encrypted
	EncryptPassword = encryptUsingQstackCrypto

	// DecryptPassword accepts a golang string object and returns it decrypted
	DecryptPassword = decryptUsingQstackCrypto

	// ProcessCredentials accepts a context and a secret password, decrypts the password,
	ProcessCredentials = processCredentials

	// DecryptAndDecodeCredentials decrypts the password and decodes the Base64 encoded credentials.
	DecryptAndDecodeCredentials = decryptAndDecodeCredentials
)

func encryptUsingQstackCrypto(password log.Secret) (*string, error) {
	sh := crypto.NewSecretsHandler(crypto.V1)
	return sh.Encrypt(passphrase, string(password))
}

func decryptUsingQstackCrypto(password log.Secret) (*string, error) {
	sh := crypto.NewSecretsHandler(crypto.V1)
	return sh.Decrypt(passphrase, string(password))
}

// decryptAndDecodeCredentials decrypts the password and decodes the Base64 encoded credentials.
func decryptAndDecodeCredentials(secretPassword string) ([]byte, error) {
	// Decrypt the password
	decryptKey, err := DecryptPassword(log.Secret(secretPassword))
	if err != nil {
		return nil, err
	}

	// Decode the Base64 encoded credentials
	credentialsDecoded, err := base64.StdEncoding.DecodeString(*decryptKey)
	if err != nil {
		return nil, err
	}

	return credentialsDecoded, nil
}

func processCredentials(ctx context.Context, secretPassword string) (*googleOauth2.Credentials, error) {
	// Decode the base64 encoded credentials
	credentialsDecoded, err := decryptAndDecodeCredentials(secretPassword)
	if err != nil {
		return nil, err
	}

	// Create a context with the necessary credentials
	scopeCreds, err := googleOauth2.CredentialsFromJSON(ctx, credentialsDecoded, cloudkms.CloudPlatformScope)
	if err != nil {
		return nil, err
	}

	return scopeCreds, nil
}
