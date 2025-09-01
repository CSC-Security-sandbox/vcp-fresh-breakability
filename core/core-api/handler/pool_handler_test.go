package api

import (
	"context"
	stderrors "errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/core-api/core-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator"
)

func TestV1betaGetOntapCredentials(t *testing.T) {
	t.Run("WhenSuccessful", func(t *testing.T) {
		// Setup
		mockOrch := orchestrator.NewMockOrchestratorFactory(t)
		handler := NewHandler(mockOrch)

		// Test data
		poolID := "test-pool-uuid"
		accountName := "test-account"
		userName := "test-user"
		poolCredentials := &datamodel.PoolCredentials{
			SecretID:      "test-secret-id",
			CertificateID: "test-cert-id",
			Password:      "test-password",
			AuthType:      1,
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
		result, err := handler.V1betaGetOntapCredentials(ctx, params)

		// Assert
		assert.NoError(t, err)
		assert.NotNil(t, result)

		// Check that we got the expected response type
		response, ok := result.(*oasgenserver.OntapCredentialsV1)
		assert.True(t, ok)

		// Verify response fields
		assert.True(t, response.SecretID.IsSet())
		assert.Equal(t, "test-secret-id", response.SecretID.Value)
		assert.True(t, response.CertificateID.IsSet())
		assert.Equal(t, "test-cert-id", response.CertificateID.Value)
		assert.True(t, response.Password.IsSet())
		assert.Equal(t, "test-password", response.Password.Value)
		assert.True(t, response.AuthType.IsSet())
		assert.Equal(t, 1, response.AuthType.Value)
	})
	t.Run("WhenAccountNameMissing", func(t *testing.T) {
		// Setup
		mockOrch := orchestrator.NewMockOrchestratorFactory(t)
		handler := NewHandler(mockOrch)

		params := oasgenserver.V1GetOntapCredentialsParams{
			PoolId:      "test-pool-uuid",
			AccountName: oasgenserver.OptString{}, // Not set
			UserName:    oasgenserver.NewOptString("test-user"),
		}

		// Execute
		ctx := context.Background()
		result, err := handler.V1betaGetOntapCredentials(ctx, params)

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
		mockOrch := orchestrator.NewMockOrchestratorFactory(t)
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
		result, err := handler.V1betaGetOntapCredentials(ctx, params)

		// Assert
		assert.NoError(t, err)
		assert.NotNil(t, result)

		// Check that we got the expected response type
		response, ok := result.(*oasgenserver.V1GetOntapCredentialsNotFound)
		assert.True(t, ok)
		assert.Equal(t, "Pool not found", response.Message)
		assert.Equal(t, float64(404), response.Code)
	})
	t.Run("WhenOtherVSAError", func(t *testing.T) {
		// Setup
		mockOrch := orchestrator.NewMockOrchestratorFactory(t)
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
		result, err := handler.V1betaGetOntapCredentials(ctx, params)

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
		mockOrch := orchestrator.NewMockOrchestratorFactory(t)
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
		result, err := handler.V1betaGetOntapCredentials(ctx, params)

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
		mockOrch := orchestrator.NewMockOrchestratorFactory(t)
		handler := NewHandler(mockOrch)

		// Test data
		poolID := "test-pool-uuid"
		accountName := "test-account"
		userName := "test-user"
		poolCredentials := &datamodel.PoolCredentials{
			SecretID:      "",
			CertificateID: "",
			Password:      "",
			AuthType:      0,
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
		result, err := handler.V1betaGetOntapCredentials(ctx, params)

		// Assert
		assert.NoError(t, err)
		assert.NotNil(t, result)

		// Check that we got the expected response type
		response, ok := result.(*oasgenserver.OntapCredentialsV1)
		assert.True(t, ok)

		// Verify response fields are set but empty
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

func TestV1betaDescribePool_Placeholder(t *testing.T) {
	// Setup
	mockOrch := orchestrator.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	params := oasgenserver.V1GetPoolParams{
		PoolId:        "test-pool-uuid",
		ProjectNumber: "test-project",
		LocationId:    "us-central1",
	}

	// Execute
	ctx := context.Background()
	result, err := handler.V1betaDescribePool(ctx, params)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Check that we got the expected response type
	response, ok := result.(*oasgenserver.V1GetPoolNotFound)
	assert.True(t, ok)
	assert.Equal(t, "Something went wrong", response.Message)
	assert.Equal(t, float64(404), response.Code)
}
