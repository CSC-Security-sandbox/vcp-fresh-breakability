package utils

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/crypto"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

const passphrase = "Koi aisa kaise kar sakta hai"

// EncryptPassword accepts a golang string object and returns it encrypted
var EncryptPassword = encryptUsingQstackCrypto

// DecryptPassword accepts a golang string object and returns it decrypted
var DecryptPassword = decryptUsingQstackCrypto

func encryptUsingQstackCrypto(password log.Secret) (*string, error) {
	sh := crypto.NewSecretsHandler(crypto.V1)
	return sh.Encrypt(passphrase, string(password))
}

func decryptUsingQstackCrypto(password log.Secret) (*string, error) {
	sh := crypto.NewSecretsHandler(crypto.V1)
	return sh.Decrypt(passphrase, string(password))
}
