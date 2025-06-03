package auth

import (
	"context"
	"os"
	"strconv"
	"time"

	credentials2 "cloud.google.com/go/iam/credentials/apiv1/credentialspb"
	"github.com/golang-jwt/jwt/v4"
	"github.com/googleapis/gax-go/v2"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
)

var (
	createMockIamClient       = _createMockIamClient
	privateKeyPath            = env.GetString("MOCK_PRIVATE_KEY_PATH", "")
	tokenExpirationTimeString = env.GetString("JWT_TOKEN_TIME", "300")
	issuer                    = env.GetString("GCP_AUTH_SERVICE_ACCOUNT", "")
	audience                  = env.GetString("GCP_SERVICE_URL", "")
	accessToken               = env.GetString("MOCK_ACCESS_TOKEN", "")
	projectId                 = env.GetString("MOCK_TEST_PROJECT_ID", "")
)

type mockIamCredentialsClient struct{}

func (m *mockIamCredentialsClient) SignJwt(ctx context.Context, req *credentials2.SignJwtRequest, opts ...gax.CallOption) (*credentials2.SignJwtResponse, error) {
	privateKeyBytes, err := os.ReadFile(privateKeyPath)
	if err != nil {
		// errors.NewError("Error reading private key", err)
		return nil, err
	}
	tokenExpirationTime, err := strconv.Atoi(tokenExpirationTimeString)
	if err != nil {
		// errors.NewError("Error converting project number:", err)
		return nil, err
	}

	projectNum, err := strconv.Atoi(projectId)
	if err != nil {
		// errors.NewError("Error converting project number:", err)
		return nil, err
	}

	privateKey, err := jwt.ParseRSAPrivateKeyFromPEM(privateKeyBytes)
	if err != nil {
		// errors.NewError("Error parsing private key:", err)
		return nil, err
	}
	iat := time.Now().Unix()
	exp := iat + int64(tokenExpirationTime)
	claims := jwt.MapClaims{
		"iss": issuer,
		"sub": "test",
		"aud": audience,
		"iat": iat,
		"exp": exp,
		"cvs": map[string]interface{}{
			"id": projectId,
		},
		"google": map[string]interface{}{
			"project_number": projectNum,
		},
		"Issuer": issuer,
	}
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = "mock"

	signedToken, err := token.SignedString(privateKey)
	if err != nil {
		// errors.NewError("Error signing token:", err)
		return nil, err
	}
	return &credentials2.SignJwtResponse{SignedJwt: signedToken}, nil
}

func (m *mockIamCredentialsClient) Close() error {
	return nil
}

func (m *mockIamCredentialsClient) GenerateAccessToken(ctx context.Context, req *credentials2.GenerateAccessTokenRequest, opts ...gax.CallOption) (*credentials2.GenerateAccessTokenResponse, error) {
	return &credentials2.GenerateAccessTokenResponse{AccessToken: accessToken}, nil
}

type mockCredentialsClientWrapperImplementation struct {
	credentialsClient *mockIamCredentialsClient
}

func (c *mockCredentialsClientWrapperImplementation) SignJwt(ctx context.Context, req *credentials2.SignJwtRequest, opts ...gax.CallOption) (*credentials2.SignJwtResponse, error) {
	return c.credentialsClient.SignJwt(ctx, req, opts...)
}

func (c *mockCredentialsClientWrapperImplementation) Close() error {
	return c.credentialsClient.Close()
}

func (c *mockCredentialsClientWrapperImplementation) GenerateAccessToken(ctx context.Context, req *credentials2.GenerateAccessTokenRequest, opts ...gax.CallOption) (*credentials2.GenerateAccessTokenResponse, error) {
	return c.credentialsClient.GenerateAccessToken(ctx, req, opts...)
}

func _createMockIamClient(ctx context.Context) (credentialsClientWrapper, error) {
	if err := validateEnvVars(); err != nil {
		return nil, err
	}
	c := &mockIamCredentialsClient{}
	return &mockCredentialsClientWrapperImplementation{
		credentialsClient: c,
	}, nil
}

func validateEnvVars() error {
	if privateKeyPath == "" {
		return errors.NewVCPError(errors.ErrIamClientNotFoundError, errors.New("MOCK_PRIVATE_KEY_PATH not set in environment variables"))
	}
	if issuer == "" {
		return errors.NewVCPError(errors.ErrIamClientNotFoundError, errors.New("GCP_AUTH_SERVICE_ACCOUNT not set in environment variables"))
	}
	if audience == "" {
		return errors.NewVCPError(errors.ErrIamClientNotFoundError, errors.New("GCP_SERVICE_URL not set in environment variables"))
	}
	if accessToken == "" {
		return errors.NewVCPError(errors.ErrIamClientNotFoundError, errors.New("MOCK_ACCESS_TOKEN not set in environment variables"))
	}
	if projectId == "" {
		return errors.NewVCPError(errors.ErrIamClientNotFoundError, errors.New("MOCK_TEST_PROJECT_ID not set in environment variables"))
	}
	return nil
}
