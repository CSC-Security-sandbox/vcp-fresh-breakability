package crypto

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	passphrase = "A normal passphrase; if there is one."
	ciphertext = "V1:VYOx7iO3XKk=:BOnDypazW0yjB8/R:xWTfpLxaJ5G0sQ5PrSBniNpWurwvUFqD1e1sQPzVUphtw6m2BKZVaL28mjtUz3dmqskh3WqWP9R8bXauSnJ8Cw=="
	expected   = "This is indeed the text we expect from all this."
)

func TestEncrypt(t *testing.T) {
	sh := NewSecretsHandler(V1)
	encrypted, err := sh.Encrypt(passphrase, expected)
	if err != nil {
		t.Error("Unexpected error when encrypting - " + err.Error())
	}
	if encrypted == nil {
		t.Error("Cypher text not returned when encrypting")
		return
	}
	t.Run("EncryptDoesNotReturnPlaintext", func(tt *testing.T) {
		if *encrypted == expected {
			tt.Fail()
		}
	})
	t.Run("EncryptReturnsCorrectlyFormattedCyphertext", func(tt *testing.T) {
		print(*encrypted)
		components := strings.Split(*encrypted, separator)
		if len(components) != 4 {
			tt.Fail()
		}
	})
	t.Run("EncryptReturnsDecryptableCyphertext", func(tt *testing.T) {
		if decrypted, err := sh.Decrypt(passphrase, *encrypted); err != nil {
			tt.Error("Unexpected error when decrypting - " + err.Error())
		} else if decrypted == nil {
			tt.Error("Plain text not returned when decrypting")
		} else if *decrypted != expected {
			t.Fail()
		}
	})
}

func TestDecrypt(t *testing.T) {
	sh := NewSecretsHandler(V1)
	if decrypted, err := sh.Decrypt(passphrase, ciphertext); err != nil {
		t.Error("Unexpected error when decrypting - " + err.Error())
	} else if decrypted == nil {
		t.Error("Plain text not returned when decrypting, even though no error was returned")
	} else if *decrypted != expected {
		t.Fail()
	}
	t.Run("DecryptDoesNotReturnCyphertext", func(tt *testing.T) {
		sh := NewSecretsHandler(V1)
		invalidCypher := "V1:VYOx7iO3XKk=:BOnDypazW0yjB8/R=="
		decrypted, err := sh.Decrypt(passphrase, invalidCypher)
		assert.NotNil(tt, err)
		assert.Nil(tt, decrypted)
	})
}

func TestParseReturnsAllComponentsOnValidCyphertext(t *testing.T) {
	sh := NewSecretsHandler(V1)
	cyphertext := "V1:c2FsdA==:bm9uY2U=:ZW5jdGV4dA=="
	version, salt, nonce, enctext, err := sh.parse(cyphertext)
	assert.NoError(t, err)
	assert.NotNil(t, version)
	assert.Equal(t, "V1", *version)
	assert.Equal(t, []byte("salt"), salt)
	assert.Equal(t, []byte("nonce"), nonce)
	assert.Equal(t, []byte("enctext"), enctext)
}

func TestParseReturnsErrorOnMalformedCyphertext(t *testing.T) {
	sh := NewSecretsHandler(V1)
	cyphertext := "V1:only:three"
	version, salt, nonce, enctext, err := sh.parse(cyphertext)
	assert.Error(t, err)
	assert.Nil(t, version)
	assert.Nil(t, salt)
	assert.Nil(t, nonce)
	assert.Nil(t, enctext)
}

func TestParseReturnsErrorOnInvalidBase64Salt(t *testing.T) {
	sh := NewSecretsHandler(V1)
	cyphertext := "V1:not-base64:bm9uY2U=:ZW5jdGV4dA=="
	version, salt, nonce, enctext, err := sh.parse(cyphertext)
	assert.Error(t, err)
	assert.Nil(t, version)
	assert.Nil(t, salt)
	assert.Nil(t, nonce)
	assert.Nil(t, enctext)
}

func TestParseReturnsErrorOnInvalidBase64Nonce(t *testing.T) {
	sh := NewSecretsHandler(V1)
	cyphertext := "V1:c2FsdA==:not-base64:ZW5jdGV4dA=="
	version, salt, nonce, enctext, err := sh.parse(cyphertext)
	assert.Error(t, err)
	assert.Nil(t, version)
	assert.Nil(t, salt)
	assert.Nil(t, nonce)
	assert.Nil(t, enctext)
}

func TestParseReturnsErrorOnInvalidBase64Enctext(t *testing.T) {
	sh := NewSecretsHandler(V1)
	cyphertext := "V1:c2FsdA==:bm9uY2U=:not-base64"
	version, salt, nonce, enctext, err := sh.parse(cyphertext)
	assert.Error(t, err)
	assert.Nil(t, version)
	assert.Nil(t, salt)
	assert.Nil(t, nonce)
	assert.Nil(t, enctext)
}
