package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	credentials2 "cloud.google.com/go/iam/credentials/apiv1/credentialspb"
	"github.com/googleapis/gax-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func Test_generateCallbackToken(t *testing.T) {
	t.Run("WhenError", func(tt *testing.T) {
		ctx := context.Background()
		expectedError := errors.New("some error")
		GetSignedAccessToken = func() (string, error) {
			return "", errors.New("some error")
		}
		defer func() { GetSignedAccessToken = _getSignedAccessToken }()
		_, err := _generateCallbackToken(ctx)
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
	})
	t.Run("WhenSuccessful", func(tt *testing.T) {
		ctx := context.Background()
		GetSignedAccessToken = func() (string, error) {
			return "mocked-token", nil
		}
		defer func() { GetSignedAccessToken = _getSignedAccessToken }()
		token, err := _generateCallbackToken(ctx)
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
		if token != "mocked-token" {
			t.Errorf("expected token 'mocked-token', got %v", token)
		}
	})
}

func TestGetSignedJwtToken(t *testing.T) {
	t.Run("WhenCreateIamClientReturnsError", func(tt *testing.T) {
		mockLogger := &log.MockLogger{}
		mm := &monkeyMock{}
		mm.Patch()
		defer mm.UnPatch()
		ctx := context.Background()
		projectNumber := "123"
		clientErr := errors.New("SomeError")
		expectedTime := time.Now()

		mm.On("timeNow").Return(expectedTime)
		mm.On("LogGetLogger", ctx).Return(mockLogger, nil)
		mm.On("createIamClient", ctx).Return(nil, clientErr)

		token, err := GetSignedJwtToken(projectNumber)

		assert.Equal(tt, "", token)
		assert.Error(tt, err)

		mm.AssertExpectations(tt)
	})
	t.Run("WhenCreateMockIamClientReturnsError", func(tt *testing.T) {
		projectNumber := "123"
		privateKeyPath = ""
		err := os.Setenv("INTEGRATION_TEST", "true")
		if err != nil {
			tt.Fatalf("Failed to set environment variable: %v", err)
		}
		defer func() {
			err := os.Unsetenv("INTEGRATION_TEST")
			if err != nil {
				tt.Fatalf("Failed to unset environment variable: %v", err)
			}
		}()
		client, _ := createMockIamClient(context.Background())
		token, err := GetSignedJwtToken(projectNumber)
		assert.Equal(tt, nil, client)
		assert.Equal(tt, "", token)
		assert.Error(tt, err)
	})
	t.Run("WhenParsingProjectNumberReturnsError", func(tt *testing.T) {
		mockLogger := &log.MockLogger{}

		mm := &monkeyMock{}

		mm.Patch()
		defer mm.UnPatch()
		ctx := context.Background()
		credentialsClientMock := &credentialsClientWrapperMock{}

		projectNumber := "123"
		parseErr := errors.New("SomeError")
		expectedTime := time.Now()

		mm.On("timeNow").Return(expectedTime)
		mm.On("parseInt", projectNumber, 10, 64).Return(nil, parseErr)
		mm.On("LogGetLogger", ctx).Return(mockLogger)
		mm.On("createIamClient", ctx).Return(credentialsClientMock, nil)
		credentialsClientMock.On("Close").Return(nil)

		mockLogger.On("Error", "Failed to parse projectNumber")

		token, err := GetSignedJwtToken(projectNumber)

		assert.Equal(tt, "", token)
		assert.Error(tt, err)
	})
	t.Run("WhenJsonMarshalReturnsError", func(tt *testing.T) {
		mockLogger := &log.MockLogger{}
		mm := &monkeyMock{}
		mm.Patch()
		defer mm.UnPatch()
		ctx := context.Background()
		credentialsClientMock := &credentialsClientWrapperMock{}

		expectedTime := time.Now()
		projectNumber := "123"
		projectNumberInt := int64(123)
		ttl := 20 * time.Minute
		payload := JwtPayload{
			Subject:    "",
			Issuer:     "",
			Audience:   "",
			Expiration: expectedTime.Add(ttl).Unix(),
			IssuedAt:   expectedTime.Unix(),
			Google: Google{
				ProjectNumber: projectNumberInt,
			},
		}
		jsonMarshalErr := errors.New("SomeError")

		mm.On("timeNow").Return(expectedTime)
		mm.On("parseInt", projectNumber, 10, 64).Return(int64(123), nil)
		mm.On("jsonMarshal", payload).Return(nil, jsonMarshalErr)
		mm.On("LogGetLogger", ctx).Return(mockLogger)
		mm.On("createIamClient", ctx).Return(credentialsClientMock, nil)
		credentialsClientMock.On("Close").Return(nil)

		mockLogger.On("Error", "Failed to marshal jwt payload")

		token, err := GetSignedJwtToken(projectNumber)

		assert.Equal(tt, "", token)
		assert.Error(tt, err)

		mm.AssertExpectations(tt)
	})
	t.Run("WhenSignJwtReturnsError", func(tt *testing.T) {
		mm := &monkeyMock{}
		mockLogger := &log.MockLogger{}

		mm.Patch()
		defer mm.UnPatch()
		credentialsClientMock := &credentialsClientWrapperMock{}
		ctx := context.Background()
		expectedTime := time.Now()
		projectNumberInt := int64(123)
		projectNumber := "123"
		ttl := 20 * time.Minute
		payload := JwtPayload{
			Subject:    "",
			Issuer:     "",
			Audience:   "",
			Expiration: expectedTime.Add(ttl).Unix(),
			IssuedAt:   expectedTime.Unix(),
			Google: Google{
				ProjectNumber: projectNumberInt,
			},
		}

		jsonPayload, _ := json.Marshal(payload)
		reqToken := &credentials2.SignJwtRequest{
			Name:      "projects/-/serviceAccounts/" + "",
			Delegates: []string{"projects/-/serviceAccounts/" + ""},
			Payload:   string(jsonPayload),
		}
		signJtwError := errors.New("SomeError")

		mm.On("timeNow").Return(expectedTime)
		mm.On("parseInt", projectNumber, 10, 64).Return(int64(123), nil)
		mm.On("jsonMarshal", payload).Return(jsonPayload, nil)
		mm.On("LogGetLogger", ctx).Return(mockLogger)
		mm.On("createIamClient", ctx).Return(credentialsClientMock, nil)

		credentialsClientMock.On("SignJwt", ctx, reqToken).Return(nil, signJtwError)
		credentialsClientMock.On("Close").Return(nil)

		token, err := GetSignedJwtToken(projectNumber)

		assert.Equal(tt, "", token)
		assert.Error(tt, err)

		mm.AssertExpectations(tt)
	})
	t.Run("WhenTTLIsConfigured", func(tt *testing.T) {
		mm := &monkeyMock{}
		mockLogger := &log.MockLogger{}
		mm.Patch()
		defer mm.UnPatch()
		credentialsClientMock := &credentialsClientWrapperMock{}
		ctx := context.Background()
		expectedTime := time.Now()
		projectNumber := "123"
		projectNumberInt := int64(123)
		ttlMinutesFromEnv := "15"
		ttl := 15 * time.Minute
		defer func() {
			err := os.Unsetenv("JWT_TTL_MINUTES")
			if err != nil {
				return
			}
		}()
		err := os.Setenv("JWT_TTL_MINUTES", ttlMinutesFromEnv)
		if err != nil {
			return
		}
		payload := JwtPayload{
			Subject:    "",
			Issuer:     "",
			Audience:   "",
			Expiration: expectedTime.Add(ttl).Unix(),
			IssuedAt:   expectedTime.Unix(),
			Google: Google{
				ProjectNumber: projectNumberInt,
			},
		}
		expectedToken := "123"
		signJwtTokenResponse := &credentials2.SignJwtResponse{
			SignedJwt: expectedToken,
		}

		jsonPayload, _ := json.Marshal(payload)
		reqToken := &credentials2.SignJwtRequest{
			Name:      "projects/-/serviceAccounts/" + "",
			Delegates: []string{"projects/-/serviceAccounts/" + ""},
			Payload:   string(jsonPayload),
		}

		mm.On("timeNow").Return(expectedTime)
		mm.On("parseInt", projectNumber, 10, 64).Return(int64(123), nil)
		mm.On("jsonMarshal", payload).Return(jsonPayload, nil)
		mm.On("LogGetLogger", ctx).Return(mockLogger)
		mm.On("createIamClient", ctx).Return(credentialsClientMock, nil)
		credentialsClientMock.On("SignJwt", ctx, reqToken).Return(signJwtTokenResponse, nil)
		credentialsClientMock.On("Close").Return(nil)

		token, err := GetSignedJwtToken(projectNumber)

		assert.Nil(tt, err)
		assert.Equal(tt, expectedToken, token)

		mm.AssertExpectations(tt)
	})
	t.Run("WhenHappyPath", func(tt *testing.T) {
		mm := &monkeyMock{}
		mockLogger := &log.MockLogger{}
		mm.Patch()
		defer mm.UnPatch()
		credentialsClientMock := &credentialsClientWrapperMock{}
		ctx := context.Background()
		expectedTime := time.Now()
		projectNumber := "123"
		projectNumberInt := int64(123)
		ttl := 20 * time.Minute
		payload := JwtPayload{
			Subject:    "",
			Issuer:     "",
			Audience:   "",
			Expiration: expectedTime.Add(ttl).Unix(),
			IssuedAt:   expectedTime.Unix(),
			Google: Google{
				ProjectNumber: projectNumberInt,
			},
		}
		expectedToken := "123"
		signJwtTokenResponse := &credentials2.SignJwtResponse{
			SignedJwt: expectedToken,
		}

		jsonPayload, _ := json.Marshal(payload)
		reqToken := &credentials2.SignJwtRequest{
			Name:      "projects/-/serviceAccounts/" + "",
			Delegates: []string{"projects/-/serviceAccounts/" + ""},
			Payload:   string(jsonPayload),
		}

		mm.On("timeNow").Return(expectedTime)
		mm.On("parseInt", projectNumber, 10, 64).Return(int64(123), nil)
		mm.On("jsonMarshal", payload).Return(jsonPayload, nil)
		mm.On("LogGetLogger", ctx).Return(mockLogger)
		mm.On("createIamClient", ctx).Return(credentialsClientMock, nil)
		credentialsClientMock.On("SignJwt", ctx, reqToken).Return(signJwtTokenResponse, nil)
		credentialsClientMock.On("Close").Return(nil)

		token, err := GetSignedJwtToken(projectNumber)

		assert.Nil(tt, err)
		assert.Equal(tt, expectedToken, token)

		mm.AssertExpectations(tt)
	})
	t.Run("WhenHappyPathButCloseFails", func(tt *testing.T) {
		mm := &monkeyMock{}
		mockLogger := &log.MockLogger{}
		mm.Patch()
		defer mm.UnPatch()
		credentialsClientMock := &credentialsClientWrapperMock{}
		ctx := context.Background()
		expectedTime := time.Now()
		projectNumber := "123"
		projectNumberInt := int64(123)
		ttl := 20 * time.Minute
		payload := JwtPayload{
			Subject:    "",
			Issuer:     "",
			Audience:   "",
			Expiration: expectedTime.Add(ttl).Unix(),
			IssuedAt:   expectedTime.Unix(),
			Google: Google{
				ProjectNumber: projectNumberInt,
			},
		}
		expectedToken := "123"
		signJwtTokenResponse := &credentials2.SignJwtResponse{
			SignedJwt: expectedToken,
		}

		jsonPayload, _ := json.Marshal(payload)
		reqToken := &credentials2.SignJwtRequest{
			Name:      "projects/-/serviceAccounts/" + "",
			Delegates: []string{"projects/-/serviceAccounts/" + ""},
			Payload:   string(jsonPayload),
		}

		expectedError := fmt.Errorf("close error")

		mm.On("timeNow").Return(expectedTime)
		mm.On("parseInt", projectNumber, 10, 64).Return(int64(123), nil)
		mm.On("jsonMarshal", payload).Return(jsonPayload, nil)
		mm.On("LogGetLogger", ctx).Return(mockLogger)
		mm.On("createIamClient", ctx).Return(credentialsClientMock, nil)
		credentialsClientMock.On("SignJwt", ctx, reqToken).Return(signJwtTokenResponse, nil)
		credentialsClientMock.On("Close").Return(expectedError)
		mockLogger.On("Error", "err", expectedError)

		token, err := GetSignedJwtToken(projectNumber)

		assert.Nil(tt, err)
		assert.Equal(tt, expectedToken, token)

		mm.AssertExpectations(tt)
	})
}

func TestGetSignedAccessToken(t *testing.T) {
	t.Run("WhenCreateIamClientReturnsError", func(tt *testing.T) {
		mockLogger := &log.MockLogger{}
		mm := &monkeyMock{}
		mm.Patch()
		defer mm.UnPatch()
		ctx := context.Background()
		clientErr := errors.New("SomeErrorl")
		mm.On("LogGetLogger", ctx).Return(mockLogger, nil)
		mm.On("createIamClient", ctx).Return(nil, clientErr)
		token, _ := GetSignedAccessToken()
		assert.Equal(tt, "", token)
		mm.AssertExpectations(tt)
	})
	t.Run("WhenGenerateAccessTokenError", func(tt *testing.T) {
		mm := &monkeyMock{}
		mockLogger := &log.MockLogger{}
		mm.Patch()
		defer mm.UnPatch()
		credentialsClientMock := &credentialsClientWrapperMock{}
		ctx := context.Background()
		hydrationServiceAccount := env.GetString("GCP_HYDRATE_AUTH_SERVICE_ACCOUNT", "")
		reqToken := &credentials2.GenerateAccessTokenRequest{
			Name:      "projects/-/serviceAccounts/" + hydrationServiceAccount,
			Delegates: []string{"projects/-/serviceAccounts/" + hydrationServiceAccount},
			Scope:     []string{"https://www.googleapis.com/auth/cloud-platform"},
			Lifetime:  nil,
		}

		generateAccessTokenError := errors.New("token error")
		mm.On("LogGetLogger", ctx).Return(mockLogger, nil)
		mm.On("createIamClient", ctx).Return(credentialsClientMock, nil)

		opts := []gax.CallOption(nil)
		credentialsClientMock.On("GenerateAccessToken", ctx, reqToken, opts).Return(nil, generateAccessTokenError)
		credentialsClientMock.On("Close").Return(nil)

		token, err := GetSignedAccessToken()

		assert.Equal(tt, "", token)
		assert.Error(t, err)

		mm.AssertExpectations(tt)
	})
	t.Run("WhenHappyPath", func(tt *testing.T) {
		mm := &monkeyMock{}
		mockLogger := &log.MockLogger{}
		mm.Patch()
		defer mm.UnPatch()
		credentialsClientMock := &credentialsClientWrapperMock{}
		ctx := context.Background()

		hydrationServiceAccount := env.GetString("GCP_HYDRATE_AUTH_SERVICE_ACCOUNT", "")
		reqToken := &credentials2.GenerateAccessTokenRequest{
			Name:      "projects/-/serviceAccounts/" + hydrationServiceAccount,
			Delegates: []string{"projects/-/serviceAccounts/" + hydrationServiceAccount},
			Scope:     []string{"https://www.googleapis.com/auth/cloud-platform"},
			Lifetime:  nil,
		}

		expectedToken := "123"
		accessTokenResponse := &credentials2.GenerateAccessTokenResponse{
			AccessToken: expectedToken,
		}
		mm.On("LogGetLogger", ctx).Return(mockLogger, nil)
		mm.On("createIamClient", ctx).Return(credentialsClientMock, nil)

		opts := []gax.CallOption(nil)
		credentialsClientMock.On("GenerateAccessToken", ctx, reqToken, opts).Return(accessTokenResponse, nil)
		credentialsClientMock.On("Close").Return(nil)

		token, err := GetSignedAccessToken()

		assert.Nil(tt, err)
		assert.Equal(tt, expectedToken, token)

		mm.AssertExpectations(tt)
	})
	t.Run("WhenHappyPathButCloseFails", func(tt *testing.T) {
		mockLogger := &log.MockLogger{}
		mm := &monkeyMock{}
		mm.Patch()
		defer mm.UnPatch()
		credentialsClientMock := &credentialsClientWrapperMock{}
		ctx := context.Background()

		hydrationServiceAccount := env.GetString("GCP_HYDRATE_AUTH_SERVICE_ACCOUNT", "")
		reqToken := &credentials2.GenerateAccessTokenRequest{
			Name:      "projects/-/serviceAccounts/" + hydrationServiceAccount,
			Delegates: []string{"projects/-/serviceAccounts/" + hydrationServiceAccount},
			Scope:     []string{"https://www.googleapis.com/auth/cloud-platform"},
			Lifetime:  nil,
		}

		expectedError := fmt.Errorf("close error")

		expectedToken := "123"
		accessTokenResponse := &credentials2.GenerateAccessTokenResponse{
			AccessToken: expectedToken,
		}
		mm.On("LogGetLogger", ctx).Return(mockLogger, nil)
		mm.On("createIamClient", ctx).Return(credentialsClientMock, nil)
		opts := []gax.CallOption(nil)
		credentialsClientMock.On("GenerateAccessToken", ctx, reqToken, opts).Return(accessTokenResponse, nil)
		credentialsClientMock.On("Close").Return(expectedError)
		mockLogger.On("Error", "auth failed to close credentials client")
		token, err := GetSignedAccessToken()
		assert.Nil(tt, err)
		assert.Equal(tt, expectedToken, token)
		mm.AssertExpectations(tt)
	})
}
