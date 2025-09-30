package resource_events_activities

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/async"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/resource_events"
	models2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	errors2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"go.temporal.io/sdk/temporal"
)

func Test_StartProjectEventForSDEActivity(t *testing.T) {
	t.Run("StartProjectEventForSDEActivity_SuccessCreated", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)
		mockClient := resource_events.NewMockClientService(t)
		cvpClient := &cvpapi.Cvp{ResourceEvents: mockClient}
		originalCreateClient := cvp.CreateClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, JWT string) cvpapi.Cvp {
			return *cvpClient
		}

		params := &common.StartProjectEventParams{
			State:          models.StateOff,
			LocationId:     "test-location-id",
			ProjectNumber:  "test-project-number",
			XCorrelationID: "test-correlation-id",
		}
		originalToken := auth.GetSignedJwtToken
		defer func() { getSignedToken = originalToken }()
		getSignedToken = func(projectNumber string) (string, error) {
			return "test-jwt-token", nil
		}

		created := &resource_events.V1betaStartProjectEventCreated{
			Payload: &models2.OperationV1beta{
				Name: "test-operation-name",
				Done: nillable.GetBoolPtr(true),
			},
		}
		mockClient.EXPECT().V1betaStartProjectEvent(mock.Anything).Return(created, nil, nil, nil)

		activity := &StartProjectEventActivity{SE: mockSE}
		result, err := activity.StartProjectEventForSDEActivity(ctx, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "test-operation-name", *result.Name)
		assert.Equal(tt, true, *result.Done)
	})
	t.Run("StartProjectEventForSDEActivity_SuccessAccepted", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)
		mockClient := resource_events.NewMockClientService(t)
		cvpClient := &cvpapi.Cvp{ResourceEvents: mockClient}
		originalCreateClient := cvp.CreateClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, JWT string) cvpapi.Cvp {
			return *cvpClient
		}
		originalToken := auth.GetSignedJwtToken
		defer func() { getSignedToken = originalToken }()
		getSignedToken = func(projectNumber string) (string, error) {
			return "test-jwt-token", nil
		}

		params := &common.StartProjectEventParams{
			State:          models.StateOff,
			LocationId:     "test-location-id",
			ProjectNumber:  "test-project-number",
			XCorrelationID: "test-correlation-id",
		}

		accepted := &resource_events.V1betaStartProjectEventAccepted{
			Payload: &models2.OperationV1beta{
				Name: "test-operation-name",
				Done: nillable.GetBoolPtr(false),
			},
		}
		mockClient.EXPECT().V1betaStartProjectEvent(mock.Anything).Return(nil, accepted, nil, nil)

		activity := &StartProjectEventActivity{SE: mockSE}
		result, err := activity.StartProjectEventForSDEActivity(ctx, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "test-operation-name", *result.Name)
		assert.False(tt, *result.Done)
	})
	t.Run("StartProjectEventForSDEActivity_WhenCVPClientReturnsError", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)
		mockClient := resource_events.NewMockClientService(t)
		cvpClient := &cvpapi.Cvp{ResourceEvents: mockClient}
		originalCreateClient := cvp.CreateClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, JWT string) cvpapi.Cvp {
			return *cvpClient
		}
		originalToken := auth.GetSignedJwtToken
		defer func() { getSignedToken = originalToken }()
		getSignedToken = func(projectNumber string) (string, error) {
			return "test-jwt-token", nil
		}
		params := &common.StartProjectEventParams{
			State:          models.StateOff,
			LocationId:     "test-location-id",
			ProjectNumber:  "test-project-number",
			XCorrelationID: "test-correlation-id",
		}

		errMsg := "Client not available"
		mockClient.EXPECT().V1betaStartProjectEvent(mock.Anything).Return(nil, nil, nil, errors.New(errMsg))

		activity := &StartProjectEventActivity{SE: mockSE}
		_, err := activity.StartProjectEventForSDEActivity(ctx, params)
		assert.NotNil(tt, err)

		var applicationError *temporal.ApplicationError
		assert.True(tt, errors2.As(err, &applicationError))
		assert.True(tt, applicationError.NonRetryable()) // This error is retryable
		assert.Equal(tt, "CustomError", applicationError.Type())
	})
	t.Run("StartProjectEventForSDEActivity_WhenCVPClientReturnsUnexpectedResponse", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)
		mockClient := resource_events.NewMockClientService(t)
		cvpClient := &cvpapi.Cvp{ResourceEvents: mockClient}
		originalCreateClient := cvp.CreateClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, JWT string) cvpapi.Cvp {
			return *cvpClient
		}
		originalToken := auth.GetSignedJwtToken
		defer func() { getSignedToken = originalToken }()
		getSignedToken = func(projectNumber string) (string, error) {
			return "test-jwt-token", nil
		}
		params := &common.StartProjectEventParams{
			State:          models.StateOff,
			LocationId:     "test-location-id",
			ProjectNumber:  "test-project-number",
			XCorrelationID: "test-correlation-id",
		}

		mockClient.EXPECT().V1betaStartProjectEvent(mock.Anything).Return(nil, nil, nil, nil)

		activity := &StartProjectEventActivity{SE: mockSE}
		res, err := activity.StartProjectEventForSDEActivity(ctx, params)
		assert.NoError(tt, err)
		assert.Nil(tt, res)
	})

	// Test all specific error types for StartProjectEventForSDEActivity
	t.Run("StartProjectEventForSDEActivity_BadRequestError", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)
		mockClient := resource_events.NewMockClientService(t)
		cvpClient := &cvpapi.Cvp{ResourceEvents: mockClient}
		originalCreateClient := cvp.CreateClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, JWT string) cvpapi.Cvp { return *cvpClient }
		originalToken := auth.GetSignedJwtToken
		defer func() { getSignedToken = originalToken }()
		getSignedToken = func(projectNumber string) (string, error) { return "test-jwt-token", nil }

		params := &common.StartProjectEventParams{
			State: models.StateOff, LocationId: "test-location-id",
			ProjectNumber: "test-project-number", XCorrelationID: "test-correlation-id",
		}

		mockClient.EXPECT().V1betaStartProjectEvent(mock.Anything).Return(nil, nil, nil, &resource_events.V1betaResourceStateUpdateBadRequest{})
		activity := &StartProjectEventActivity{SE: mockSE}
		_, err := activity.StartProjectEventForSDEActivity(ctx, params)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Bad request error")
	})

	t.Run("StartProjectEventForSDEActivity_UnauthorizedError", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)
		mockClient := resource_events.NewMockClientService(t)
		cvpClient := &cvpapi.Cvp{ResourceEvents: mockClient}
		originalCreateClient := cvp.CreateClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, JWT string) cvpapi.Cvp { return *cvpClient }
		originalToken := auth.GetSignedJwtToken
		defer func() { getSignedToken = originalToken }()
		getSignedToken = func(projectNumber string) (string, error) { return "test-jwt-token", nil }

		params := &common.StartProjectEventParams{
			State: models.StateOff, LocationId: "test-location-id",
			ProjectNumber: "test-project-number", XCorrelationID: "test-correlation-id",
		}

		mockClient.EXPECT().V1betaStartProjectEvent(mock.Anything).Return(nil, nil, nil, &resource_events.V1betaResourceStateUpdateUnauthorized{})
		activity := &StartProjectEventActivity{SE: mockSE}
		_, err := activity.StartProjectEventForSDEActivity(ctx, params)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Unauthorised error")
	})

	t.Run("StartProjectEventForSDEActivity_ForbiddenError", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)
		mockClient := resource_events.NewMockClientService(t)
		cvpClient := &cvpapi.Cvp{ResourceEvents: mockClient}
		originalCreateClient := cvp.CreateClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, JWT string) cvpapi.Cvp { return *cvpClient }
		originalToken := auth.GetSignedJwtToken
		defer func() { getSignedToken = originalToken }()
		getSignedToken = func(projectNumber string) (string, error) { return "test-jwt-token", nil }

		params := &common.StartProjectEventParams{
			State: models.StateOff, LocationId: "test-location-id",
			ProjectNumber: "test-project-number", XCorrelationID: "test-correlation-id",
		}

		mockClient.EXPECT().V1betaStartProjectEvent(mock.Anything).Return(nil, nil, nil, &resource_events.V1betaResourceStateUpdateForbidden{})
		activity := &StartProjectEventActivity{SE: mockSE}
		_, err := activity.StartProjectEventForSDEActivity(ctx, params)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Forbidden error")
	})

	t.Run("StartProjectEventForSDEActivity_NotFoundError", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)
		mockClient := resource_events.NewMockClientService(t)
		cvpClient := &cvpapi.Cvp{ResourceEvents: mockClient}
		originalCreateClient := cvp.CreateClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, JWT string) cvpapi.Cvp { return *cvpClient }
		originalToken := auth.GetSignedJwtToken
		defer func() { getSignedToken = originalToken }()
		getSignedToken = func(projectNumber string) (string, error) { return "test-jwt-token", nil }

		params := &common.StartProjectEventParams{
			State: models.StateOff, LocationId: "test-location-id",
			ProjectNumber: "test-project-number", XCorrelationID: "test-correlation-id",
		}

		mockClient.EXPECT().V1betaStartProjectEvent(mock.Anything).Return(nil, nil, nil, &resource_events.V1betaResourceStateUpdateNotFound{})
		activity := &StartProjectEventActivity{SE: mockSE}
		_, err := activity.StartProjectEventForSDEActivity(ctx, params)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Resource not found")
	})

	t.Run("StartProjectEventForSDEActivity_ConflictError", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)
		mockClient := resource_events.NewMockClientService(t)
		cvpClient := &cvpapi.Cvp{ResourceEvents: mockClient}
		originalCreateClient := cvp.CreateClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, JWT string) cvpapi.Cvp { return *cvpClient }
		originalToken := auth.GetSignedJwtToken
		defer func() { getSignedToken = originalToken }()
		getSignedToken = func(projectNumber string) (string, error) { return "test-jwt-token", nil }

		params := &common.StartProjectEventParams{
			State: models.StateOff, LocationId: "test-location-id",
			ProjectNumber: "test-project-number", XCorrelationID: "test-correlation-id",
		}

		mockClient.EXPECT().V1betaStartProjectEvent(mock.Anything).Return(nil, nil, nil, &resource_events.V1betaResourceStateUpdateConflict{})
		activity := &StartProjectEventActivity{SE: mockSE}
		_, err := activity.StartProjectEventForSDEActivity(ctx, params)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Conflict error")
	})

	t.Run("StartProjectEventForSDEActivity_InternalServerError", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)
		mockClient := resource_events.NewMockClientService(t)
		cvpClient := &cvpapi.Cvp{ResourceEvents: mockClient}
		originalCreateClient := cvp.CreateClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, JWT string) cvpapi.Cvp { return *cvpClient }
		originalToken := auth.GetSignedJwtToken
		defer func() { getSignedToken = originalToken }()
		getSignedToken = func(projectNumber string) (string, error) { return "test-jwt-token", nil }

		params := &common.StartProjectEventParams{
			State: models.StateOff, LocationId: "test-location-id",
			ProjectNumber: "test-project-number", XCorrelationID: "test-correlation-id",
		}

		mockClient.EXPECT().V1betaStartProjectEvent(mock.Anything).Return(nil, nil, nil, &resource_events.V1betaResourceStateUpdateInternalServerError{})
		activity := &StartProjectEventActivity{SE: mockSE}
		_, err := activity.StartProjectEventForSDEActivity(ctx, params)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Internal server error")
	})

	t.Run("StartProjectEventForSDEActivity_NotImplementedError", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)
		mockClient := resource_events.NewMockClientService(t)
		cvpClient := &cvpapi.Cvp{ResourceEvents: mockClient}
		originalCreateClient := cvp.CreateClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, JWT string) cvpapi.Cvp { return *cvpClient }
		originalToken := auth.GetSignedJwtToken
		defer func() { getSignedToken = originalToken }()
		getSignedToken = func(projectNumber string) (string, error) { return "test-jwt-token", nil }

		params := &common.StartProjectEventParams{
			State: models.StateOff, LocationId: "test-location-id",
			ProjectNumber: "test-project-number", XCorrelationID: "test-correlation-id",
		}

		mockClient.EXPECT().V1betaStartProjectEvent(mock.Anything).Return(nil, nil, nil, &resource_events.V1betaResourceStateUpdateNotImplemented{})
		activity := &StartProjectEventActivity{SE: mockSE}
		_, err := activity.StartProjectEventForSDEActivity(ctx, params)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Not implemented yet error")
	})

	t.Run("StartProjectEventForSDEActivity_TooManyRequestsError", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)
		mockClient := resource_events.NewMockClientService(t)
		cvpClient := &cvpapi.Cvp{ResourceEvents: mockClient}
		originalCreateClient := cvp.CreateClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, JWT string) cvpapi.Cvp { return *cvpClient }
		originalToken := auth.GetSignedJwtToken
		defer func() { getSignedToken = originalToken }()
		getSignedToken = func(projectNumber string) (string, error) { return "test-jwt-token", nil }

		params := &common.StartProjectEventParams{
			State: models.StateOff, LocationId: "test-location-id",
			ProjectNumber: "test-project-number", XCorrelationID: "test-correlation-id",
		}

		mockClient.EXPECT().V1betaStartProjectEvent(mock.Anything).Return(nil, nil, nil, &resource_events.V1betaResourceStateUpdateTooManyRequests{})
		activity := &StartProjectEventActivity{SE: mockSE}
		_, err := activity.StartProjectEventForSDEActivity(ctx, params)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Too many requests error")
		
		// This should be retryable error, not non-retryable
		var applicationError *temporal.ApplicationError
		assert.True(tt, errors2.As(err, &applicationError))
		assert.False(tt, applicationError.NonRetryable())
	})

	t.Run("StartProjectEventForSDEActivity_GetSignedTokenError", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)
		originalToken := auth.GetSignedJwtToken
		defer func() { getSignedToken = originalToken }()
		getSignedToken = func(projectNumber string) (string, error) {
			return "", errors.New("token generation failed")
		}

		params := &common.StartProjectEventParams{
			State: models.StateOff, LocationId: "test-location-id",
			ProjectNumber: "test-project-number", XCorrelationID: "test-correlation-id",
		}

		activity := &StartProjectEventActivity{SE: mockSE}
		_, err := activity.StartProjectEventForSDEActivity(ctx, params)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Failed to get signed token")
	})
}

func Test_PollStartProjectEventSDEOperationActivity(t *testing.T) {
	t.Run("PollStartProjectEventSDEOperationActivity_Success", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)
		mockAsync := &async.MockClientService{}
		mockCVP := &cvpapi.Cvp{Async: mockAsync}
		originalCreateClient := cvp.CreateClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp { return *mockCVP }
		originalToken := auth.GetSignedJwtToken
		defer func() { getSignedToken = originalToken }()
		getSignedToken = func(projectNumber string) (string, error) {
			return "test-jwt-token", nil
		}
		params := &common.StartProjectEventParams{
			State:          models.StateOff,
			LocationId:     "test-location-id",
			ProjectNumber:  "test-project-number",
			XCorrelationID: "test-correlation-id",
		}
		result := &common.StartProjectEventResult{
			Done: nillable.GetBoolPtr(false),
			Name: nillable.GetStringPtr("test-operation-name"),
		}

		response := &async.V1betaDescribeOperationOK{
			Payload: &models2.OperationV1beta{
				Name: "test-operation-name",
				Done: nillable.GetBoolPtr(true),
			},
		}
		mockAsync.EXPECT().V1betaDescribeOperation(mock.Anything).Return(response, nil)

		activity := &StartProjectEventActivity{SE: mockSE}
		err := activity.PollStartProjectEventSDEOperationActivity(ctx, params, result)
		assert.NoError(tt, err)
	})
	t.Run("PollStartProjectEventSDEOperationActivity_WhenJobErrorsOut", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)
		mockAsync := &async.MockClientService{}
		mockCVP := &cvpapi.Cvp{Async: mockAsync}
		originalCreateClient := cvp.CreateClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp { return *mockCVP }
		originalToken := auth.GetSignedJwtToken
		defer func() { getSignedToken = originalToken }()
		getSignedToken = func(projectNumber string) (string, error) {
			return "test-jwt-token", nil
		}
		params := &common.StartProjectEventParams{
			State:          models.StateOff,
			LocationId:     "test-location-id",
			ProjectNumber:  "test-project-number",
			XCorrelationID: "test-correlation-id",
		}
		result := &common.StartProjectEventResult{
			Done: nillable.GetBoolPtr(false),
			Name: nillable.GetStringPtr("test-operation-name"),
		}

		response := &async.V1betaDescribeOperationOK{
			Payload: &models2.OperationV1beta{
				Name: "test-operation-name",
				Done: nillable.GetBoolPtr(true),
				Error: &models2.StatusV1Beta{
					Code:    float64(500),
					Message: "Internal Server Error",
				},
			},
		}
		mockAsync.EXPECT().V1betaDescribeOperation(mock.Anything).Return(response, nil)

		activity := &StartProjectEventActivity{SE: mockSE}
		err := activity.PollStartProjectEventSDEOperationActivity(ctx, params, result)
		assert.NotNil(tt, err)
		assert.ErrorContains(tt, err, "Internal server error")

		var applicationError *temporal.ApplicationError
		assert.True(tt, errors2.As(err, &applicationError))
		assert.True(tt, applicationError.NonRetryable())
		assert.Equal(tt, "CustomError", applicationError.Type())
	})
	t.Run("PollStartProjectEventSDEOperationActivity_WhenJobIsNotFinished", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)
		mockAsync := &async.MockClientService{}
		mockCVP := &cvpapi.Cvp{Async: mockAsync}
		originalCreateClient := cvp.CreateClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp { return *mockCVP }
		originalToken := auth.GetSignedJwtToken
		defer func() { getSignedToken = originalToken }()
		getSignedToken = func(projectNumber string) (string, error) {
			return "test-jwt-token", nil
		}
		params := &common.StartProjectEventParams{
			State:          models.StateOff,
			LocationId:     "test-location-id",
			ProjectNumber:  "test-project-number",
			XCorrelationID: "test-correlation-id",
		}
		result := &common.StartProjectEventResult{
			Done: nillable.GetBoolPtr(false),
			Name: nillable.GetStringPtr("test-operation-name"),
		}

		response := &async.V1betaDescribeOperationOK{
			Payload: &models2.OperationV1beta{
				Name: "test-operation-name",
			},
		}
		mockAsync.EXPECT().V1betaDescribeOperation(mock.Anything).Return(response, nil)

		activity := &StartProjectEventActivity{SE: mockSE}
		err := activity.PollStartProjectEventSDEOperationActivity(ctx, params, result)
		assert.NotNil(tt, err)
		assert.ErrorContains(tt, err, "Error SDE job not done")
	})
	t.Run("PollStartProjectEventSDEOperationActivity_WhenCVPClientReturnsError", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)
		mockAsync := &async.MockClientService{}
		mockCVP := &cvpapi.Cvp{Async: mockAsync}
		originalCreateClient := cvp.CreateClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp { return *mockCVP }
		originalToken := auth.GetSignedJwtToken
		defer func() { getSignedToken = originalToken }()
		getSignedToken = func(projectNumber string) (string, error) {
			return "test-jwt-token", nil
		}
		params := &common.StartProjectEventParams{
			State:          models.StateOff,
			LocationId:     "test-location-id",
			ProjectNumber:  "test-project-number",
			XCorrelationID: "test-correlation-id",
		}
		result := &common.StartProjectEventResult{
			Done: nillable.GetBoolPtr(false),
			Name: nillable.GetStringPtr("test-operation-name"),
		}

		mockAsync.EXPECT().V1betaDescribeOperation(mock.Anything).Return(nil, errors.New("Client not available"))

		activity := &StartProjectEventActivity{SE: mockSE}
		err := activity.PollStartProjectEventSDEOperationActivity(ctx, params, result)
		assert.NotNil(tt, err)

		var applicationError *temporal.ApplicationError
		assert.True(tt, errors2.As(err, &applicationError))
		assert.True(tt, applicationError.NonRetryable())
		assert.Equal(tt, "CustomError", applicationError.Type())
	})
	t.Run("PollStartProjectEventSDEOperationActivity_WhenOperationNameIsNil", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)
		mockAsync := &async.MockClientService{}
		mockCVP := &cvpapi.Cvp{Async: mockAsync}
		originalCreateClient := cvp.CreateClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp { return *mockCVP }
		originalToken := auth.GetSignedJwtToken
		defer func() { getSignedToken = originalToken }()
		getSignedToken = func(projectNumber string) (string, error) {
			return "test-jwt-token", nil
		}
		params := &common.StartProjectEventParams{
			State:          models.StateOff,
			LocationId:     "test-location-id",
			ProjectNumber:  "test-project-number",
			XCorrelationID: "test-correlation-id",
		}
		result := &common.StartProjectEventResult{
			Done: nillable.GetBoolPtr(false),
		}

		activity := &StartProjectEventActivity{SE: mockSE}
		err := activity.PollStartProjectEventSDEOperationActivity(ctx, params, result)
		assert.NotNil(tt, err)
		assert.ErrorContains(tt, err, "Invalid Operation Name")

		var applicationError *temporal.ApplicationError
		assert.True(tt, errors2.As(err, &applicationError))
		assert.True(tt, applicationError.NonRetryable())
		assert.Equal(tt, "CustomError", applicationError.Type())
	})
	t.Run("PollStartProjectEventSDEOperationActivity_WhenResultIsDone", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)
		mockAsync := &async.MockClientService{}
		mockCVP := &cvpapi.Cvp{Async: mockAsync}
		originalCreateClient := cvp.CreateClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp { return *mockCVP }
		originalToken := auth.GetSignedJwtToken
		defer func() { getSignedToken = originalToken }()
		getSignedToken = func(projectNumber string) (string, error) {
			return "test-jwt-token", nil
		}
		params := &common.StartProjectEventParams{
			State:          models.StateOff,
			LocationId:     "test-location-id",
			ProjectNumber:  "test-project-number",
			XCorrelationID: "test-correlation-id",
		}
		result := &common.StartProjectEventResult{
			Done: nillable.GetBoolPtr(true),
		}

		activity := &StartProjectEventActivity{SE: mockSE}
		err := activity.PollStartProjectEventSDEOperationActivity(ctx, params, result)
		assert.Nil(tt, err)
	})

	// Test all specific error types for PollStartProjectEventSDEOperationActivity
	t.Run("PollStartProjectEventSDEOperationActivity_BadRequestError", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)
		mockAsync := &async.MockClientService{}
		mockCVP := &cvpapi.Cvp{Async: mockAsync}
		originalCreateClient := cvp.CreateClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp { return *mockCVP }
		originalToken := auth.GetSignedJwtToken
		defer func() { getSignedToken = originalToken }()
		getSignedToken = func(projectNumber string) (string, error) { return "test-jwt-token", nil }

		params := &common.StartProjectEventParams{
			State: models.StateOff, LocationId: "test-location-id",
			ProjectNumber: "test-project-number", XCorrelationID: "test-correlation-id",
		}
		result := &common.StartProjectEventResult{
			Done: nillable.GetBoolPtr(false),
			Name: nillable.GetStringPtr("test-operation-name"),
		}

		response := &async.V1betaDescribeOperationOK{
			Payload: &models2.OperationV1beta{
				Name: "test-operation-name",
				Done: nillable.GetBoolPtr(true),
				Error: &models2.StatusV1Beta{
					Code:    float64(400),
					Message: "Bad Request",
				},
			},
		}
		mockAsync.EXPECT().V1betaDescribeOperation(mock.Anything).Return(response, nil)
		activity := &StartProjectEventActivity{SE: mockSE}
		err := activity.PollStartProjectEventSDEOperationActivity(ctx, params, result)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Bad request error")
	})

	t.Run("PollStartProjectEventSDEOperationActivity_UnauthorizedError", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)
		mockAsync := &async.MockClientService{}
		mockCVP := &cvpapi.Cvp{Async: mockAsync}
		originalCreateClient := cvp.CreateClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp { return *mockCVP }
		originalToken := auth.GetSignedJwtToken
		defer func() { getSignedToken = originalToken }()
		getSignedToken = func(projectNumber string) (string, error) { return "test-jwt-token", nil }

		params := &common.StartProjectEventParams{
			State: models.StateOff, LocationId: "test-location-id",
			ProjectNumber: "test-project-number", XCorrelationID: "test-correlation-id",
		}
		result := &common.StartProjectEventResult{
			Done: nillable.GetBoolPtr(false),
			Name: nillable.GetStringPtr("test-operation-name"),
		}

		response := &async.V1betaDescribeOperationOK{
			Payload: &models2.OperationV1beta{
				Name: "test-operation-name",
				Done: nillable.GetBoolPtr(true),
				Error: &models2.StatusV1Beta{
					Code:    float64(401),
					Message: "Unauthorized",
				},
			},
		}
		mockAsync.EXPECT().V1betaDescribeOperation(mock.Anything).Return(response, nil)
		activity := &StartProjectEventActivity{SE: mockSE}
		err := activity.PollStartProjectEventSDEOperationActivity(ctx, params, result)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Unauthorised error")
	})

	t.Run("PollStartProjectEventSDEOperationActivity_ForbiddenError", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)
		mockAsync := &async.MockClientService{}
		mockCVP := &cvpapi.Cvp{Async: mockAsync}
		originalCreateClient := cvp.CreateClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp { return *mockCVP }
		originalToken := auth.GetSignedJwtToken
		defer func() { getSignedToken = originalToken }()
		getSignedToken = func(projectNumber string) (string, error) { return "test-jwt-token", nil }

		params := &common.StartProjectEventParams{
			State: models.StateOff, LocationId: "test-location-id",
			ProjectNumber: "test-project-number", XCorrelationID: "test-correlation-id",
		}
		result := &common.StartProjectEventResult{
			Done: nillable.GetBoolPtr(false),
			Name: nillable.GetStringPtr("test-operation-name"),
		}

		response := &async.V1betaDescribeOperationOK{
			Payload: &models2.OperationV1beta{
				Name: "test-operation-name",
				Done: nillable.GetBoolPtr(true),
				Error: &models2.StatusV1Beta{
					Code:    float64(403),
					Message: "Forbidden",
				},
			},
		}
		mockAsync.EXPECT().V1betaDescribeOperation(mock.Anything).Return(response, nil)
		activity := &StartProjectEventActivity{SE: mockSE}
		err := activity.PollStartProjectEventSDEOperationActivity(ctx, params, result)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Forbidden error")
	})

	t.Run("PollStartProjectEventSDEOperationActivity_NotFoundError", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)
		mockAsync := &async.MockClientService{}
		mockCVP := &cvpapi.Cvp{Async: mockAsync}
		originalCreateClient := cvp.CreateClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp { return *mockCVP }
		originalToken := auth.GetSignedJwtToken
		defer func() { getSignedToken = originalToken }()
		getSignedToken = func(projectNumber string) (string, error) { return "test-jwt-token", nil }

		params := &common.StartProjectEventParams{
			State: models.StateOff, LocationId: "test-location-id",
			ProjectNumber: "test-project-number", XCorrelationID: "test-correlation-id",
		}
		result := &common.StartProjectEventResult{
			Done: nillable.GetBoolPtr(false),
			Name: nillable.GetStringPtr("test-operation-name"),
		}

		response := &async.V1betaDescribeOperationOK{
			Payload: &models2.OperationV1beta{
				Name: "test-operation-name",
				Done: nillable.GetBoolPtr(true),
				Error: &models2.StatusV1Beta{
					Code:    float64(404),
					Message: "Not Found",
				},
			},
		}
		mockAsync.EXPECT().V1betaDescribeOperation(mock.Anything).Return(response, nil)
		activity := &StartProjectEventActivity{SE: mockSE}
		err := activity.PollStartProjectEventSDEOperationActivity(ctx, params, result)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Resource not found")
	})

	t.Run("PollStartProjectEventSDEOperationActivity_InternalServerError", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)
		mockAsync := &async.MockClientService{}
		mockCVP := &cvpapi.Cvp{Async: mockAsync}
		originalCreateClient := cvp.CreateClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp { return *mockCVP }
		originalToken := auth.GetSignedJwtToken
		defer func() { getSignedToken = originalToken }()
		getSignedToken = func(projectNumber string) (string, error) { return "test-jwt-token", nil }

		params := &common.StartProjectEventParams{
			State: models.StateOff, LocationId: "test-location-id",
			ProjectNumber: "test-project-number", XCorrelationID: "test-correlation-id",
		}
		result := &common.StartProjectEventResult{
			Done: nillable.GetBoolPtr(false),
			Name: nillable.GetStringPtr("test-operation-name"),
		}

		response := &async.V1betaDescribeOperationOK{
			Payload: &models2.OperationV1beta{
				Name: "test-operation-name",
				Done: nillable.GetBoolPtr(true),
				Error: &models2.StatusV1Beta{
					Code:    float64(500),
					Message: "Internal Server Error",
				},
			},
		}
		mockAsync.EXPECT().V1betaDescribeOperation(mock.Anything).Return(response, nil)
		activity := &StartProjectEventActivity{SE: mockSE}
		err := activity.PollStartProjectEventSDEOperationActivity(ctx, params, result)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Internal server error")
	})

	t.Run("PollStartProjectEventSDEOperationActivity_NotImplementedError", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)
		mockAsync := &async.MockClientService{}
		mockCVP := &cvpapi.Cvp{Async: mockAsync}
		originalCreateClient := cvp.CreateClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp { return *mockCVP }
		originalToken := auth.GetSignedJwtToken
		defer func() { getSignedToken = originalToken }()
		getSignedToken = func(projectNumber string) (string, error) { return "test-jwt-token", nil }

		params := &common.StartProjectEventParams{
			State: models.StateOff, LocationId: "test-location-id",
			ProjectNumber: "test-project-number", XCorrelationID: "test-correlation-id",
		}
		result := &common.StartProjectEventResult{
			Done: nillable.GetBoolPtr(false),
			Name: nillable.GetStringPtr("test-operation-name"),
		}

		response := &async.V1betaDescribeOperationOK{
			Payload: &models2.OperationV1beta{
				Name: "test-operation-name",
				Done: nillable.GetBoolPtr(true),
				Error: &models2.StatusV1Beta{
					Code:    float64(501),
					Message: "Not Implemented",
				},
			},
		}
		mockAsync.EXPECT().V1betaDescribeOperation(mock.Anything).Return(response, nil)
		activity := &StartProjectEventActivity{SE: mockSE}
		err := activity.PollStartProjectEventSDEOperationActivity(ctx, params, result)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Client Error during StartProjectEvent")
	})

	t.Run("PollStartProjectEventSDEOperationActivity_TooManyRequestsError", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)
		mockAsync := &async.MockClientService{}
		mockCVP := &cvpapi.Cvp{Async: mockAsync}
		originalCreateClient := cvp.CreateClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp { return *mockCVP }
		originalToken := auth.GetSignedJwtToken
		defer func() { getSignedToken = originalToken }()
		getSignedToken = func(projectNumber string) (string, error) { return "test-jwt-token", nil }

		params := &common.StartProjectEventParams{
			State: models.StateOff, LocationId: "test-location-id",
			ProjectNumber: "test-project-number", XCorrelationID: "test-correlation-id",
		}
		result := &common.StartProjectEventResult{
			Done: nillable.GetBoolPtr(false),
			Name: nillable.GetStringPtr("test-operation-name"),
		}

		response := &async.V1betaDescribeOperationOK{
			Payload: &models2.OperationV1beta{
				Name: "test-operation-name",
				Done: nillable.GetBoolPtr(true),
				Error: &models2.StatusV1Beta{
					Code:    float64(429),
					Message: "Too Many Requests",
				},
			},
		}
		mockAsync.EXPECT().V1betaDescribeOperation(mock.Anything).Return(response, nil)
		activity := &StartProjectEventActivity{SE: mockSE}
		err := activity.PollStartProjectEventSDEOperationActivity(ctx, params, result)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Too many requests error")
		
		// This should be retryable error, not non-retryable
		var applicationError *temporal.ApplicationError
		assert.True(tt, errors2.As(err, &applicationError))
		assert.False(tt, applicationError.NonRetryable())
	})

	t.Run("PollStartProjectEventSDEOperationActivity_GetSignedTokenError", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)
		originalToken := auth.GetSignedJwtToken
		defer func() { getSignedToken = originalToken }()
		getSignedToken = func(projectNumber string) (string, error) {
			return "", errors.New("token generation failed")
		}

		params := &common.StartProjectEventParams{
			State: models.StateOff, LocationId: "test-location-id",
			ProjectNumber: "test-project-number", XCorrelationID: "test-correlation-id",
		}
		result := &common.StartProjectEventResult{
			Done: nillable.GetBoolPtr(false),
			Name: nillable.GetStringPtr("test-operation-name"),
		}

		activity := &StartProjectEventActivity{SE: mockSE}
		err := activity.PollStartProjectEventSDEOperationActivity(ctx, params, result)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Failed to get signed token")
	})
}

func TestListPoolsForAccount(t *testing.T) {
	t.Run("PoolsExists", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &StartProjectEventActivity{SE: mockSE}

		projectNumber := "test-project-number"
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
		}
		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				AccountID: account.ID,
				Name:      "test-pool-name",
			},
		}

		mockSE.On("GetAccount", ctx, projectNumber).Return(account, nil)
		mockSE.On("ListPools", ctx, mock.Anything).Return([]*datamodel.PoolView{pool}, nil)

		result, err := activity.ListPoolsForAccount(ctx, projectNumber, "OFF")
		assert.NotNil(tt, result)
		assert.Nil(tt, err)
	})

	t.Run("AccountNotFound", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &StartProjectEventActivity{SE: mockSE}

		projectNumber := "test-project-number"

		mockSE.On("GetAccount", ctx, projectNumber).Return(nil, errors2.NewVCPError(errors2.ErrDatabaseListPoolsForAccount, errors.NewNotFoundErr("Account", nil)))

		result, err := activity.ListPoolsForAccount(ctx, projectNumber, "OFF")
		assert.Nil(tt, result)
		assert.NotNil(tt, err)
		assert.IsType(tt, &temporal.ApplicationError{}, err)
	})

	t.Run("PoolNotFound", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &StartProjectEventActivity{SE: mockSE}

		projectNumber := "test-project-number"
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
		}

		mockSE.On("GetAccount", ctx, projectNumber).Return(account, nil)
		mockSE.On("ListPools", ctx, mock.Anything).Return(nil, errors2.NewVCPError(errors2.ErrDatabaseListPoolsForAccount, errors.NewNotFoundErr("Pool", nil)))

		result, err := activity.ListPoolsForAccount(ctx, projectNumber, "OFF")

		assert.Nil(tt, result)
		assert.NotNil(tt, err)
		assert.IsType(tt, &temporal.ApplicationError{}, err)
	})

	t.Run("InvalidResourceState", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &StartProjectEventActivity{SE: mockSE}

		projectNumber := "test-project-number"
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
		}

		mockSE.On("GetAccount", ctx, projectNumber).Return(account, nil)

		result, err := activity.ListPoolsForAccount(ctx, projectNumber, "INVALID_STATE")

		assert.Nil(tt, result)
		assert.NotNil(tt, err)
		assert.Contains(tt, err.Error(), "Invalid Operation Name")
		
		var applicationError *temporal.ApplicationError
		assert.True(tt, errors2.As(err, &applicationError))
		assert.True(tt, applicationError.NonRetryable())
	})
}

func TestUpdateAccountStateForHandleResource(t *testing.T) {
	t.Run("AccountUpdateSucceeds", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &StartProjectEventActivity{SE: mockSE}
		state := "ENABLED"

		projectNumber := "test-project-number"
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
		}

		mockSE.On("GetAccount", ctx, projectNumber).Return(account, nil)
		mockSE.On("UpdateAccountStateForHandleResource", ctx, account.UUID, mock.Anything).Return(nil)

		err := activity.UpdateAccountStateForHandleResource(ctx, projectNumber, state)
		assert.Nil(tt, err)
	})

	t.Run("AccountNotFound", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &StartProjectEventActivity{SE: mockSE}

		projectNumber := "test-project-number"
		state := "ENABLED"

		mockSE.On("GetAccount", ctx, projectNumber).Return(nil, errors2.NewVCPError(errors2.ErrDatabaseListPoolsForAccount, errors.NewNotFoundErr("Account", nil)))

		err := activity.UpdateAccountStateForHandleResource(ctx, projectNumber, state)
		assert.NotNil(tt, err)
		assert.IsType(tt, &temporal.ApplicationError{}, err)
	})

	t.Run("UpdateAccountStateError", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &StartProjectEventActivity{SE: mockSE}

		projectNumber := "test-project-number"
		state := "ENABLED"

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
		}

		mockSE.On("GetAccount", ctx, projectNumber).Return(account, nil)
		mockSE.On("UpdateAccountStateForHandleResource", ctx, account.UUID, mock.Anything).Return(errors2.NewVCPError(errors2.ErrDatabaseListPoolsForAccount, errors.NewNotFoundErr("Account", nil)))

		err := activity.UpdateAccountStateForHandleResource(ctx, projectNumber, state)

		assert.NotNil(tt, err)
		assert.IsType(tt, &temporal.ApplicationError{}, err)
	})
}

func TestFilterPoolsForClusterOperations(t *testing.T) {
	t.Run("FilterPoolsForClusterOperations_AllPoolsInValidState", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &StartProjectEventActivity{SE: mockSE}

		// Create pools in READY and ERROR states (valid for cluster operations)
		pools := []*datamodel.PoolView{
			{
				Pool: datamodel.Pool{
					BaseModel: datamodel.BaseModel{ID: 1, UUID: "pool-1"},
					Name:      "pool-1",
					State:     models.LifeCycleStateREADY,
				},
			},
			{
				Pool: datamodel.Pool{
					BaseModel: datamodel.BaseModel{ID: 2, UUID: "pool-2"},
					Name:      "pool-2",
					State:     models.LifeCycleStateError,
				},
			},
		}

		// Mock no volumes and snapshots for all pools
		mockSE.On("GetVolumesByPoolID", ctx, int64(1)).Return([]*datamodel.Volume{}, nil)
		mockSE.On("GetVolumesByPoolID", ctx, int64(2)).Return([]*datamodel.Volume{}, nil)

		result, err := activity.FilterPoolsForClusterOperations(ctx, pools)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Len(tt, result.FilteredPools, 2)
		assert.False(tt, result.VSAError) // No transient states detected
	})

	t.Run("FilterPoolsForClusterOperations_PoolsInTransientState", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &StartProjectEventActivity{SE: mockSE}

		// Create pools with one in transient state
		pools := []*datamodel.PoolView{
			{
				Pool: datamodel.Pool{
					BaseModel: datamodel.BaseModel{ID: 1, UUID: "pool-1"},
					Name:      "pool-1",
					State:     models.LifeCycleStateREADY,
				},
			},
			{
				Pool: datamodel.Pool{
					BaseModel: datamodel.BaseModel{ID: 2, UUID: "pool-2"},
					Name:      "pool-2",
					State:     models.LifeCycleStateCreating, // Transient state
				},
			},
		}

		// Mock no volumes and snapshots for valid pool
		mockSE.On("GetVolumesByPoolID", ctx, int64(1)).Return([]*datamodel.Volume{}, nil)
		// Pool 2 should be filtered out, so GetVolumesByPoolID shouldn't be called for it

		result, err := activity.FilterPoolsForClusterOperations(ctx, pools)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Len(tt, result.FilteredPools, 1) // Only one pool should pass filter
		assert.Equal(tt, "pool-1", result.FilteredPools[0].Name)
		assert.True(tt, result.VSAError) // Transient state detected
	})

	t.Run("FilterPoolsForClusterOperations_VolumesInTransientState", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &StartProjectEventActivity{SE: mockSE}

		pools := []*datamodel.PoolView{
			{
				Pool: datamodel.Pool{
					BaseModel: datamodel.BaseModel{ID: 1, UUID: "pool-1"},
					Name:      "pool-1",
					State:     models.LifeCycleStateREADY,
				},
			},
		}

		// Create volumes with one in transient state
		volumes := []*datamodel.Volume{
			{
				BaseModel: datamodel.BaseModel{ID: 10, UUID: "volume-1"},
				Name:      "volume-1",
				State:     models.LifeCycleStateREADY,
			},
			{
				BaseModel: datamodel.BaseModel{ID: 11, UUID: "volume-2"},
				Name:      "volume-2",
				State:     models.LifeCycleStateUpdating, // Transient state
			},
		}

		mockSE.On("GetVolumesByPoolID", ctx, int64(1)).Return(volumes, nil)
		mockSE.On("GetSnapshotsByVolumeID", ctx, int64(10)).Return([]*datamodel.Snapshot{}, nil)

		result, err := activity.FilterPoolsForClusterOperations(ctx, pools)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Len(tt, result.FilteredPools, 0) // Pool should be filtered out due to transient volume
		assert.True(tt, result.VSAError)        // Transient state detected
	})

	t.Run("FilterPoolsForClusterOperations_SnapshotsInTransientState", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &StartProjectEventActivity{SE: mockSE}

		pools := []*datamodel.PoolView{
			{
				Pool: datamodel.Pool{
					BaseModel: datamodel.BaseModel{ID: 1, UUID: "pool-1"},
					Name:      "pool-1",
					State:     models.LifeCycleStateREADY,
				},
			},
		}

		volumes := []*datamodel.Volume{
			{
				BaseModel: datamodel.BaseModel{ID: 10, UUID: "volume-1"},
				Name:      "volume-1",
				State:     models.LifeCycleStateREADY,
			},
		}

		snapshots := []*datamodel.Snapshot{
			{
				BaseModel: datamodel.BaseModel{ID: 20, UUID: "snapshot-1"},
				Name:      "snapshot-1",
				State:     models.LifeCycleStateREADY,
			},
			{
				BaseModel: datamodel.BaseModel{ID: 21, UUID: "snapshot-2"},
				Name:      "snapshot-2",
				State:     models.LifeCycleStateDeleting, // Transient state
			},
		}

		mockSE.On("GetVolumesByPoolID", ctx, int64(1)).Return(volumes, nil)
		mockSE.On("GetSnapshotsByVolumeID", ctx, int64(10)).Return(snapshots, nil)

		result, err := activity.FilterPoolsForClusterOperations(ctx, pools)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Len(tt, result.FilteredPools, 0) // Pool should be filtered out due to transient snapshot
		assert.True(tt, result.VSAError)        // Transient state detected
	})

	t.Run("FilterPoolsForClusterOperations_MixedPoolStates", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &StartProjectEventActivity{SE: mockSE}

		pools := []*datamodel.PoolView{
			{
				Pool: datamodel.Pool{
					BaseModel: datamodel.BaseModel{ID: 1, UUID: "pool-1"},
					Name:      "pool-1",
					State:     models.LifeCycleStateREADY, // Valid state
				},
			},
			{
				Pool: datamodel.Pool{
					BaseModel: datamodel.BaseModel{ID: 2, UUID: "pool-2"},
					Name:      "pool-2",
					State:     models.LifeCycleStateCreating, // Transient state
				},
			},
			{
				Pool: datamodel.Pool{
					BaseModel: datamodel.BaseModel{ID: 3, UUID: "pool-3"},
					Name:      "pool-3",
					State:     models.LifeCycleStateDisabled, // Invalid for cluster operations
				},
			},
		}

		// Mock no volumes for valid pool
		mockSE.On("GetVolumesByPoolID", ctx, int64(1)).Return([]*datamodel.Volume{}, nil)

		result, err := activity.FilterPoolsForClusterOperations(ctx, pools)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Len(tt, result.FilteredPools, 1) // Only pool-1 should pass
		assert.Equal(tt, "pool-1", result.FilteredPools[0].Name)
		assert.True(tt, result.VSAError) // Transient/invalid states detected
	})

	t.Run("FilterPoolsForClusterOperations_DatabaseError", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &StartProjectEventActivity{SE: mockSE}

		pools := []*datamodel.PoolView{
			{
				Pool: datamodel.Pool{
					BaseModel: datamodel.BaseModel{ID: 1, UUID: "pool-1"},
					Name:      "pool-1",
					State:     models.LifeCycleStateREADY,
				},
			},
		}

		// Mock database error
		mockSE.On("GetVolumesByPoolID", ctx, int64(1)).Return(nil, errors.New("database connection error"))

		result, err := activity.FilterPoolsForClusterOperations(ctx, pools)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "database connection error")
	})

	t.Run("FilterPoolsForClusterOperations_EmptyPoolList", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &StartProjectEventActivity{SE: mockSE}

		var pools []*datamodel.PoolView

		result, err := activity.FilterPoolsForClusterOperations(ctx, pools)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Len(tt, result.FilteredPools, 0)
		assert.False(tt, result.VSAError) // No pools to process, no errors
	})

	t.Run("FilterPoolsForClusterOperations_GetSnapshotsByVolumeIDError", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &StartProjectEventActivity{SE: mockSE}

		pools := []*datamodel.PoolView{
			{
				Pool: datamodel.Pool{
					BaseModel: datamodel.BaseModel{ID: 1, UUID: "pool-1"},
					Name:      "pool-1",
					State:     models.LifeCycleStateREADY,
				},
			},
		}

		volumes := []*datamodel.Volume{
			{
				BaseModel: datamodel.BaseModel{ID: 10, UUID: "volume-1"},
				Name:      "volume-1",
				State:     models.LifeCycleStateREADY,
			},
		}

		mockSE.On("GetVolumesByPoolID", ctx, int64(1)).Return(volumes, nil)
		mockSE.On("GetSnapshotsByVolumeID", ctx, int64(10)).Return(nil, errors.New("database connection error for snapshots"))

		result, err := activity.FilterPoolsForClusterOperations(ctx, pools)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "database connection error for snapshots")
	})
}

func TestTransientStateHelpers(t *testing.T) {
	t.Run("isPoolInTransientState", func(tt *testing.T) {
		// Test transient states
		assert.True(tt, isPoolInTransientState(models.LifeCycleStateCreating))
		assert.True(tt, isPoolInTransientState(models.LifeCycleStateUpdating))
		assert.True(tt, isPoolInTransientState(models.LifeCycleStateDeleting))

		// Test non-transient states
		assert.False(tt, isPoolInTransientState(models.LifeCycleStateREADY))
		assert.False(tt, isPoolInTransientState(models.LifeCycleStateError))
		assert.False(tt, isPoolInTransientState(models.LifeCycleStateDisabled))
	})

	t.Run("isVolumeInTransientState", func(tt *testing.T) {
		// Test transient states
		assert.True(tt, isVolumeInTransientState(models.LifeCycleStateCreating))
		assert.True(tt, isVolumeInTransientState(models.LifeCycleStateUpdating))
		assert.True(tt, isVolumeInTransientState(models.LifeCycleStateDeleting))
		assert.True(tt, isVolumeInTransientState(models.LifeCycleStateRestoring))

		// Test non-transient states
		assert.False(tt, isVolumeInTransientState(models.LifeCycleStateREADY))
		assert.False(tt, isVolumeInTransientState(models.LifeCycleStateError))
		assert.False(tt, isVolumeInTransientState(models.LifeCycleStateDisabled))
	})

	t.Run("isSnapshotInTransientState", func(tt *testing.T) {
		// Test transient states
		assert.True(tt, isSnapshotInTransientState(models.LifeCycleStateCreating))
		assert.True(tt, isSnapshotInTransientState(models.LifeCycleStateDeleting))

		// Test non-transient states
		assert.False(tt, isSnapshotInTransientState(models.LifeCycleStateREADY))
		assert.False(tt, isSnapshotInTransientState(models.LifeCycleStateError))
	})
}

// Test HTTP status constants are correctly defined
func TestHTTPStatusConstants(t *testing.T) {
	// Test that all required HTTP status constants are defined with correct values
	assert.Equal(t, 400, common.HTTPStatusBadRequest, "BadRequest constant should be 400")
	assert.Equal(t, 401, common.HTTPStatusUnauthorized, "Unauthorized constant should be 401")
	assert.Equal(t, 403, common.HTTPStatusForbidden, "Forbidden constant should be 403")
	assert.Equal(t, 404, common.HTTPStatusNotFound, "NotFound constant should be 404")
	assert.Equal(t, 429, common.HTTPStatusTooManyRequests, "TooManyRequests constant should be 429")
	assert.Equal(t, 500, common.HTTPStatusInternalServerError, "InternalServerError constant should be 500")
}

// Test HTTP status code constants usage in start project event activities
func Test_HTTPStatusConstants_StartProjectEvent(t *testing.T) {
	// Verify that constants from common package are used correctly
	testCases := []struct {
		name           string
		statusCode     int32
		expectedConstant int
	}{
		{"BadRequest", 400, common.HTTPStatusBadRequest},
		{"Unauthorized", 401, common.HTTPStatusUnauthorized}, 
		{"Forbidden", 403, common.HTTPStatusForbidden},
		{"NotFound", 404, common.HTTPStatusNotFound},
		{"TooManyRequests", 429, common.HTTPStatusTooManyRequests},
		{"InternalServerError", 500, common.HTTPStatusInternalServerError},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			assert.Equal(tt, int(tc.statusCode), tc.expectedConstant, 
				"Constant value should match HTTP status code")
		})
	}
}
