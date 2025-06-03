package auth

import (
	credentials "cloud.google.com/go/iam/credentials/apiv1"
	credentials2 "cloud.google.com/go/iam/credentials/apiv1/credentialspb"
	"github.com/googleapis/gax-go/v2"
	"golang.org/x/net/context"
)

var (
	createIamClient = _createIamClient
)

type credentialsClientWrapper interface {
	SignJwt(ctx context.Context, req *credentials2.SignJwtRequest, opts ...gax.CallOption) (*credentials2.SignJwtResponse, error)
	Close() error
	GenerateAccessToken(ctx context.Context, req *credentials2.GenerateAccessTokenRequest, opts ...gax.CallOption) (*credentials2.GenerateAccessTokenResponse, error)
}

type credentialsClientWrapperImplementation struct {
	credentialsClient *credentials.IamCredentialsClient
}

// SignJwt is intentionally not part of any unit tests
func (c *credentialsClientWrapperImplementation) SignJwt(ctx context.Context, req *credentials2.SignJwtRequest, opts ...gax.CallOption) (*credentials2.SignJwtResponse, error) {
	return c.credentialsClient.SignJwt(ctx, req, opts...)
}

// Close is intentionally not part of any unit tests
func (c *credentialsClientWrapperImplementation) Close() error {
	return c.credentialsClient.Close()
}

// GenerateAccessToken is intentionally not part of any unit tests
func (c *credentialsClientWrapperImplementation) GenerateAccessToken(ctx context.Context, req *credentials2.GenerateAccessTokenRequest, opts ...gax.CallOption) (*credentials2.GenerateAccessTokenResponse, error) {
	return c.credentialsClient.GenerateAccessToken(ctx, req, opts...)
}

// createIamClient is intentionally not part of any unit tests
func _createIamClient(ctx context.Context) (credentialsClientWrapper, error) {
	c, err := credentials.NewIamCredentialsClient(ctx)
	if err != nil {
		return nil, err
	}
	return &credentialsClientWrapperImplementation{
		credentialsClient: c,
	}, nil
}
