package auth

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"log"
	"os"
	"testing"

	credentials2 "cloud.google.com/go/iam/credentials/apiv1/credentialspb"
	"github.com/stretchr/testify/assert"
)

func TestSignJwt(t *testing.T) {
	issuer = "test-issuer"
	audience = "https://test.com"
	projectId = "642418027188"
	mockAccessToken = "access-token"
	client := &mockIamCredentialsClient{}
	ctx := context.Background()
	req := &credentials2.SignJwtRequest{}
	privateKeyPath = "test/keys/testkey"
	createDir := "test/keys"
	err := generateKey(createDir)
	assert.NoError(t, err)
	resp, err := client.SignJwt(ctx, req)
	assert.NoError(t, err)
	err = os.RemoveAll("test")
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.NotEmpty(t, resp.SignedJwt)
}

func TestGenerateAccessToken(t *testing.T) {
	client := &mockIamCredentialsClient{}
	ctx := context.Background()
	req := &credentials2.GenerateAccessTokenRequest{}
	mockAccessToken = "test-access-token"
	resp, err := client.GenerateAccessToken(ctx, req)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, "test-access-token", resp.AccessToken)
}

func TestCreateMockIamClient(t *testing.T) {
	ctx := context.Background()
	client, err := _createMockIamClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)
}

func TestSignJwtInvalidPrivateKey(t *testing.T) {
	client := &mockIamCredentialsClient{}
	ctx := context.Background()
	req := &credentials2.SignJwtRequest{}
	createDir := "test/keys"
	err := generateKey(createDir)
	privateKeyPath = "test/keys/testkey-invalid"
	assert.NoError(t, err)
	_, err = client.SignJwt(ctx, req)
	assert.Error(t, err)
	err = os.RemoveAll("test")
	assert.NoError(t, err)
}

// This function generates RSA key and saves in the given path
func generateKey(createDirPath string) error {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		log.Fatalf("Failed to generate private key: %v", err)
		return err
	}

	if _, err := os.Stat(createDirPath); !os.IsNotExist(err) {
		err = os.RemoveAll(createDirPath)
		if err != nil {
			log.Fatalf("Failed to delete existing keys directory: %v", err)
		}
	}

	err = os.MkdirAll(createDirPath, 0700)
	if err != nil {
		log.Fatalf("Failed to create keys directory: %v", err)
	}

	// Save private key in PKCS1 format
	privateKeyFile, err := os.Create(createDirPath + "/testkey")
	if err != nil {
		log.Fatalf("Failed to create private key file: %v", err)
		return err
	}

	defer func() {
		err := privateKeyFile.Close()
		if err != nil {
			log.Printf("Error in response body close: %v", err)
		}
	}()

	privateKeyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	})
	_, err = privateKeyFile.Write(privateKeyPEM)
	if err != nil {
		log.Fatalf("Failed to write private key to file: %v", err)
		return err
	}

	return nil
}
