package api

import (
	"context"
	stderrors "errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/core-api/core-servergen"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/factory"
)

func TestV1GetOntapCredentials(t *testing.T) {
	t.Run("WhenSuccessful", func(t *testing.T) {
		// Setup
		mockOrch := factory.NewMockOrchestratorFactory(t)
		handler := NewHandler(mockOrch)

		// Test data
		poolID := "test-pool-uuid"
		accountName := "test-account"
		userName := "test-user"
		poolCredentials := &models.UserCredentials{
			Username:      "test-user",
			SecretID:      "test-secret-id",
			CertificateID: "test-cert-id",
			Password:      "test-password",
			AuthType:      1,
			OntapEndpoints: []models.OntapEndpoint{
				{IP: "10.0.0.1", DNS: "host1.example.com"},
				{IP: "10.0.0.2", DNS: "host2.example.com"},
			},
		}

		params := oasgenserver.V1GetOntapCredentialsParams{
			PoolId:      poolID,
			AccountName: oasgenserver.NewOptString(accountName),
			UserName:    oasgenserver.NewOptString(userName),
		}

		// Set up expectations
		mockOrch.EXPECT().GetExpertModePoolCreds(mock.Anything, poolID, accountName, userName).Return(poolCredentials, nil)

		// Execute
		ctx := context.Background()
		result, err := handler.V1GetOntapCredentials(ctx, params)

		// Assert
		assert.NoError(t, err)
		assert.NotNil(t, result)

		// Check that we got the expected response type
		response, ok := result.(*oasgenserver.OntapCredentialsV1)
		assert.True(t, ok)

		// Verify response fields
		assert.True(t, response.Username.IsSet())
		assert.Equal(t, "test-user", response.Username.Value)
		assert.True(t, response.SecretID.IsSet())
		assert.Equal(t, "test-secret-id", response.SecretID.Value)
		assert.True(t, response.CertificateID.IsSet())
		assert.Equal(t, "test-cert-id", response.CertificateID.Value)
		assert.True(t, response.Password.IsSet())
		assert.Equal(t, "test-password", response.Password.Value)
		assert.True(t, response.AuthType.IsSet())
		assert.Equal(t, 1, response.AuthType.Value)
		assert.Len(t, response.OntapEndpoints, 2)
		assert.Equal(t, "10.0.0.1", response.OntapEndpoints[0].IP)
		assert.Equal(t, "host1.example.com", response.OntapEndpoints[0].DNS)
		assert.Equal(t, "10.0.0.2", response.OntapEndpoints[1].IP)
		assert.Equal(t, "host2.example.com", response.OntapEndpoints[1].DNS)
	})
	t.Run("WhenAccountNameMissing", func(t *testing.T) {
		// Setup
		mockOrch := factory.NewMockOrchestratorFactory(t)
		handler := NewHandler(mockOrch)

		params := oasgenserver.V1GetOntapCredentialsParams{
			PoolId:      "test-pool-uuid",
			AccountName: oasgenserver.OptString{}, // Not set
			UserName:    oasgenserver.NewOptString("test-user"),
		}

		// Execute
		ctx := context.Background()
		result, err := handler.V1GetOntapCredentials(ctx, params)

		// Assert
		assert.NoError(t, err)
		assert.NotNil(t, result)

		// Check that we got the expected response type
		response, ok := result.(*oasgenserver.V1GetOntapCredentialsBadRequest)
		assert.True(t, ok)
		assert.Equal(t, "Account name is required", response.Message)
		assert.Equal(t, float64(400), response.Code)
	})
	t.Run("WhenPoolNotFound", func(t *testing.T) {
		// Setup
		mockOrch := factory.NewMockOrchestratorFactory(t)
		handler := NewHandler(mockOrch)

		// Test data
		poolID := "non-existent-pool-uuid"
		accountName := "test-account"
		userName := "test-user"
		poolNotFoundError := vsaerrors.NewVCPError(vsaerrors.ErrPoolNotFound, stderrors.New("pool not found"))

		params := oasgenserver.V1GetOntapCredentialsParams{
			PoolId:      poolID,
			AccountName: oasgenserver.NewOptString(accountName),
			UserName:    oasgenserver.NewOptString(userName),
		}

		// Set up expectations
		mockOrch.EXPECT().GetExpertModePoolCreds(mock.Anything, poolID, accountName, userName).Return(nil, poolNotFoundError)

		// Execute
		ctx := context.Background()
		result, err := handler.V1GetOntapCredentials(ctx, params)

		// Assert
		assert.NoError(t, err)
		assert.NotNil(t, result)

		// Check that we got the expected response type
		response, ok := result.(*oasgenserver.V1GetOntapCredentialsNotFound)
		assert.True(t, ok)
		assert.Equal(t, "Pool not found", response.Message)
		assert.Equal(t, float64(404), response.Code)
	})
	t.Run("WhenPoolInCreatingState", func(t *testing.T) {
		// Setup
		mockOrch := factory.NewMockOrchestratorFactory(t)
		handler := NewHandler(mockOrch)

		poolID := "test-pool-uuid"
		accountName := "test-account"
		userName := "test-user"
		poolCreatingError := vsaerrors.NewVCPError(vsaerrors.ErrPoolInCreatingState, stderrors.New("pool is in creating state"))

		params := oasgenserver.V1GetOntapCredentialsParams{
			PoolId:      poolID,
			AccountName: oasgenserver.NewOptString(accountName),
			UserName:    oasgenserver.NewOptString(userName),
		}

		mockOrch.EXPECT().GetExpertModePoolCreds(mock.Anything, poolID, accountName, userName).Return(nil, poolCreatingError)

		ctx := context.Background()
		result, err := handler.V1GetOntapCredentials(ctx, params)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		response, ok := result.(*oasgenserver.V1GetOntapCredentialsConflict)
		assert.True(t, ok)
		assert.Equal(t, "Pool is in creating state", response.Message)
		assert.Equal(t, float64(400), response.Code)
	})
	t.Run("WhenPoolInCreatingState_WithEmptyMessage", func(t *testing.T) {
		mockOrch := factory.NewMockOrchestratorFactory(t)
		handler := NewHandler(mockOrch)

		// CustomError with empty Message so handler uses fallback "Pool is in creating state"
		poolCreatingError := &vsaerrors.CustomError{
			TrackingID: vsaerrors.ErrPoolInCreatingState,
			Message:    "",
		}

		params := oasgenserver.V1GetOntapCredentialsParams{
			PoolId:      "test-pool-uuid",
			AccountName: oasgenserver.NewOptString("test-account"),
			UserName:    oasgenserver.NewOptString("test-user"),
		}
		mockOrch.EXPECT().GetExpertModePoolCreds(mock.Anything, "test-pool-uuid", "test-account", "test-user").Return(nil, poolCreatingError)

		result, err := handler.V1GetOntapCredentials(context.Background(), params)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		response, ok := result.(*oasgenserver.V1GetOntapCredentialsConflict)
		assert.True(t, ok)
		assert.Equal(t, "Pool is in creating state", response.Message)
		assert.Equal(t, float64(400), response.Code)
	})
	t.Run("WhenOtherVSAError", func(t *testing.T) {
		// Setup
		mockOrch := factory.NewMockOrchestratorFactory(t)
		handler := NewHandler(mockOrch)

		// Test data
		poolID := "test-pool-uuid"
		accountName := "test-account"
		userName := "test-user"
		otherError := vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, stderrors.New("database connection failed"))

		params := oasgenserver.V1GetOntapCredentialsParams{
			PoolId:      poolID,
			AccountName: oasgenserver.NewOptString(accountName),
			UserName:    oasgenserver.NewOptString(userName),
		}

		// Set up expectations
		mockOrch.EXPECT().GetExpertModePoolCreds(mock.Anything, poolID, accountName, userName).Return(nil, otherError)

		// Execute
		ctx := context.Background()
		result, err := handler.V1GetOntapCredentials(ctx, params)

		// Assert
		assert.NoError(t, err)
		assert.NotNil(t, result)

		// Check that we got the expected response type
		response, ok := result.(*oasgenserver.V1GetOntapCredentialsInternalServerError)
		assert.True(t, ok)
		assert.Contains(t, response.Message, "Failed to get pool credentials")
		assert.Equal(t, float64(500), response.Code)
	})
	t.Run("WhenStandardError", func(t *testing.T) {
		// Setup
		mockOrch := factory.NewMockOrchestratorFactory(t)
		handler := NewHandler(mockOrch)

		// Test data
		poolID := "test-pool-uuid"
		accountName := "test-account"
		userName := "test-user"
		standardError := stderrors.New("standard go error")

		params := oasgenserver.V1GetOntapCredentialsParams{
			PoolId:      poolID,
			AccountName: oasgenserver.NewOptString(accountName),
			UserName:    oasgenserver.NewOptString(userName),
		}

		// Set up expectations
		mockOrch.EXPECT().GetExpertModePoolCreds(mock.Anything, poolID, accountName, userName).Return(nil, standardError)

		// Execute
		ctx := context.Background()
		result, err := handler.V1GetOntapCredentials(ctx, params)

		// Assert
		assert.NoError(t, err)
		assert.NotNil(t, result)

		// Check that we got the expected response type
		response, ok := result.(*oasgenserver.V1GetOntapCredentialsInternalServerError)
		assert.True(t, ok)
		assert.Contains(t, response.Message, "Failed to get pool credentials")
		assert.Contains(t, response.Message, "standard go error")
		assert.Equal(t, float64(500), response.Code)
	})
	t.Run("WhenEmptyCredentials", func(t *testing.T) {
		// Setup
		mockOrch := factory.NewMockOrchestratorFactory(t)
		handler := NewHandler(mockOrch)

		// Test data
		poolID := "test-pool-uuid"
		accountName := "test-account"
		userName := "test-user"
		poolCredentials := &models.UserCredentials{
			Username:       "",
			SecretID:       "",
			CertificateID:  "",
			Password:       "",
			AuthType:       0,
			OntapEndpoints: []models.OntapEndpoint{},
		}

		params := oasgenserver.V1GetOntapCredentialsParams{
			PoolId:      poolID,
			AccountName: oasgenserver.NewOptString(accountName),
			UserName:    oasgenserver.NewOptString(userName),
		}

		// Set up expectations
		mockOrch.EXPECT().GetExpertModePoolCreds(mock.Anything, poolID, accountName, userName).Return(poolCredentials, nil)

		// Execute
		ctx := context.Background()
		result, err := handler.V1GetOntapCredentials(ctx, params)

		// Assert
		assert.NoError(t, err)
		assert.NotNil(t, result)

		// Check that we got the expected response type
		response, ok := result.(*oasgenserver.OntapCredentialsV1)
		assert.True(t, ok)

		// Verify response fields are set but empty
		assert.True(t, response.Username.IsSet())
		assert.Equal(t, "", response.Username.Value)
		assert.True(t, response.SecretID.IsSet())
		assert.Equal(t, "", response.SecretID.Value)
		assert.True(t, response.CertificateID.IsSet())
		assert.Equal(t, "", response.CertificateID.Value)
		assert.True(t, response.Password.IsSet())
		assert.Equal(t, "", response.Password.Value)
		assert.True(t, response.AuthType.IsSet())
		assert.Equal(t, 0, response.AuthType.Value)
	})
}
