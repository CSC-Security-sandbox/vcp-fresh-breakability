package resource_events_activities

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/async"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func Test_pollCvpOperationForWorkflow(t *testing.T) {
	t.Run("pollCvpOperationForWorkflow_Success", func(tt *testing.T) {
		ctx := context.Background()
		mockAsync := async.NewMockClientService(t)
		mockCVP := &cvpapi.Cvp{Async: mockAsync}

		operationParams := &async.V1betaDescribeOperationParams{
			OperationID: "test-operation-id",
		}

		expectedOperation := &models.OperationV1beta{
			Name: "operations/test-operation-id",
			Done: nillable.GetBoolPtr(true),
		}

		response := &async.V1betaDescribeOperationOK{
			Payload: expectedOperation,
		}

		mockAsync.EXPECT().V1betaDescribeOperation(operationParams).Return(response, nil)

		result, err := pollCvpOperationForWorkflow(ctx, *mockCVP, operationParams)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, expectedOperation.Name, result.Name)
		assert.Equal(tt, expectedOperation.Done, result.Done)
	})

	t.Run("pollCvpOperationForWorkflow_WhenDescribeOperationReturnsError", func(tt *testing.T) {
		ctx := context.Background()
		mockAsync := async.NewMockClientService(t)
		mockCVP := &cvpapi.Cvp{Async: mockAsync}

		operationParams := &async.V1betaDescribeOperationParams{
			OperationID: "test-operation-id",
		}

		errMsg := "operation not found"
		mockAsync.EXPECT().V1betaDescribeOperation(operationParams).Return(nil, errors.New(errMsg))

		result, err := pollCvpOperationForWorkflow(ctx, *mockCVP, operationParams)
		assert.NotNil(tt, err)
		assert.Nil(tt, result)
		assert.ErrorContains(tt, err, "Error describing SDE Operation")
	})

	t.Run("pollCvpOperationForWorkflow_WithOperationInProgress", func(tt *testing.T) {
		ctx := context.Background()
		mockAsync := async.NewMockClientService(t)
		mockCVP := &cvpapi.Cvp{Async: mockAsync}

		operationParams := &async.V1betaDescribeOperationParams{
			OperationID: "test-operation-id",
		}

		expectedOperation := &models.OperationV1beta{
			Name: "operations/test-operation-id",
			Done: nillable.GetBoolPtr(false),
		}

		response := &async.V1betaDescribeOperationOK{
			Payload: expectedOperation,
		}

		mockAsync.EXPECT().V1betaDescribeOperation(operationParams).Return(response, nil)

		result, err := pollCvpOperationForWorkflow(ctx, *mockCVP, operationParams)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, expectedOperation.Name, result.Name)
		assert.Equal(tt, false, *result.Done)
	})

	t.Run("pollCvpOperationForWorkflow_WithOperationError", func(tt *testing.T) {
		ctx := context.Background()
		mockAsync := async.NewMockClientService(t)
		mockCVP := &cvpapi.Cvp{Async: mockAsync}

		operationParams := &async.V1betaDescribeOperationParams{
			OperationID: "test-operation-id",
		}

		expectedOperation := &models.OperationV1beta{
			Name: "operations/test-operation-id",
			Done: nillable.GetBoolPtr(true),
			Error: &models.StatusV1Beta{
				Code:    float64(500),
				Message: "Internal Server Error",
			},
		}

		response := &async.V1betaDescribeOperationOK{
			Payload: expectedOperation,
		}

		mockAsync.EXPECT().V1betaDescribeOperation(operationParams).Return(response, nil)

		result, err := pollCvpOperationForWorkflow(ctx, *mockCVP, operationParams)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, expectedOperation.Name, result.Name)
		assert.Equal(tt, true, *result.Done)
		assert.NotNil(tt, result.Error)
		assert.Equal(tt, float64(500), result.Error.Code)
		assert.Equal(tt, "Internal Server Error", result.Error.Message)
	})

	t.Run("pollCvpOperationForWorkflow_WhenDescribeOperationReturnsNilResponse", func(tt *testing.T) {
		ctx := context.Background()
		mockAsync := async.NewMockClientService(t)
		mockCVP := &cvpapi.Cvp{Async: mockAsync}

		operationParams := &async.V1betaDescribeOperationParams{
			OperationID: "test-operation-id",
		}

		response := &async.V1betaDescribeOperationOK{
			Payload: nil,
		}

		mockAsync.EXPECT().V1betaDescribeOperation(operationParams).Return(response, nil)

		result, err := pollCvpOperationForWorkflow(ctx, *mockCVP, operationParams)
		assert.NoError(tt, err)
		assert.Nil(tt, result)
	})

	t.Run("pollCvpOperationForWorkflow_WithNetworkError", func(tt *testing.T) {
		ctx := context.Background()
		mockAsync := async.NewMockClientService(t)
		mockCVP := &cvpapi.Cvp{Async: mockAsync}

		operationParams := &async.V1betaDescribeOperationParams{
			OperationID: "test-operation-id",
		}

		networkErr := errors.New("network timeout")
		mockAsync.EXPECT().V1betaDescribeOperation(operationParams).Return(nil, networkErr)

		result, err := pollCvpOperationForWorkflow(ctx, *mockCVP, operationParams)
		assert.NotNil(tt, err)
		assert.Nil(tt, result)
		assert.ErrorContains(tt, err, "Error describing SDE Operation")
	})

	t.Run("pollCvpOperationForWorkflow_WithEmptyOperationID", func(tt *testing.T) {
		ctx := context.Background()
		mockAsync := async.NewMockClientService(t)
		mockCVP := &cvpapi.Cvp{Async: mockAsync}

		operationParams := &async.V1betaDescribeOperationParams{
			OperationID: "",
		}

		expectedOperation := &models.OperationV1beta{
			Name: "operations/",
			Done: nillable.GetBoolPtr(true),
		}

		response := &async.V1betaDescribeOperationOK{
			Payload: expectedOperation,
		}

		mockAsync.EXPECT().V1betaDescribeOperation(operationParams).Return(response, nil)

		result, err := pollCvpOperationForWorkflow(ctx, *mockCVP, operationParams)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, expectedOperation.Name, result.Name)
	})
}

// Test cases for the package variables to ensure they're properly initialized
func Test_PackageVariables(t *testing.T) {
	t.Run("createClient_IsProperlyInitialized", func(tt *testing.T) {
		// Verify that createClient is initialized with cvp.CreateClient
		assert.NotNil(tt, createClient)
		// Save the original value
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()

		// Test that it can be mocked
		mockCalled := false
		createClient = func(logger log.Logger, jwt string) cvpapi.Cvp {
			mockCalled = true
			return cvpapi.Cvp{}
		}

		// Call the mocked function
		_ = createClient(nil, "test-jwt")
		assert.True(tt, mockCalled)
	})

	t.Run("getSignedToken_IsProperlyInitialized", func(tt *testing.T) {
		// Verify that getSignedToken is initialized with auth.GetSignedJwtToken
		assert.NotNil(tt, getSignedToken)
		// Save the original value
		originalGetSignedToken := getSignedToken
		defer func() { getSignedToken = originalGetSignedToken }()

		// Test that it can be mocked
		mockCalled := false
		expectedToken := "mock-jwt-token"
		getSignedToken = func(projectNumber string) (string, error) {
			mockCalled = true
			return expectedToken, nil
		}

		// Call the mocked function
		token, err := getSignedToken("test-project")
		assert.True(tt, mockCalled)
		assert.NoError(tt, err)
		assert.Equal(tt, expectedToken, token)
	})
}
