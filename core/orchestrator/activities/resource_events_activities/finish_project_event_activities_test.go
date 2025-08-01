package resource_events_activities

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/async"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/resource_events"
	models2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	errors2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"go.temporal.io/sdk/temporal"
	"testing"
)

func Test_FinishProjectEventForSDEActivity(t *testing.T) {
	t.Run("FinishProjectEventForSDEActivity_SuccessCreated", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)
		mockClient := resource_events.NewMockClientService(t)
		cvpClient := &cvpapi.Cvp{ResourceEvents: mockClient}
		originalCreateClient := cvp.CreateClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, JWT string) cvpapi.Cvp {
			return *cvpClient
		}

		params := &common.FinishProjectEventParams{
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

		created := &resource_events.V1betaFinishProjectEventCreated{
			Payload: &models2.OperationV1beta{
				Name: "test-operation-name",
				Done: nillable.GetBoolPtr(true),
			},
		}
		mockClient.EXPECT().V1betaFinishProjectEvent(mock.Anything).Return(created, nil, nil, nil)

		activity := &FinishProjectEventActivity{SE: mockSE}
		result, err := activity.FinishProjectEventForSDEActivity(ctx, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "test-operation-name", *result.Name)
		assert.Equal(tt, true, *result.Done)
	})

	t.Run("FinishProjectEventForSDEActivity_SuccessAccepted", func(tt *testing.T) {
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

		params := &common.FinishProjectEventParams{
			State:          models.StateOn,
			LocationId:     "test-location-id",
			ProjectNumber:  "test-project-number",
			XCorrelationID: "test-correlation-id",
		}

		accepted := &resource_events.V1betaFinishProjectEventAccepted{
			Payload: &models2.OperationV1beta{
				Name: "test-operation-name",
				Done: nillable.GetBoolPtr(false),
			},
		}
		mockClient.EXPECT().V1betaFinishProjectEvent(mock.Anything).Return(nil, accepted, nil, nil)

		activity := &FinishProjectEventActivity{SE: mockSE}
		result, err := activity.FinishProjectEventForSDEActivity(ctx, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "test-operation-name", *result.Name)
		assert.False(tt, *result.Done)
	})

	t.Run("FinishProjectEventForSDEActivity_WhenGetSignedTokenFails", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)

		params := &common.FinishProjectEventParams{
			State:          models.StateOff,
			LocationId:     "test-location-id",
			ProjectNumber:  "test-project-number",
			XCorrelationID: "test-correlation-id",
		}
		originalToken := auth.GetSignedJwtToken
		defer func() { getSignedToken = originalToken }()
		getSignedToken = func(projectNumber string) (string, error) {
			return "", errors.New("Failed to get signed token")
		}

		activity := &FinishProjectEventActivity{SE: mockSE}
		_, err := activity.FinishProjectEventForSDEActivity(ctx, params)
		assert.NotNil(tt, err)
		assert.ErrorContains(tt, err, "Failed to get signed token")

		var applicationError *temporal.ApplicationError
		assert.True(tt, errors2.As(err, &applicationError))
		assert.False(tt, applicationError.NonRetryable())
		assert.Equal(tt, "CustomError", applicationError.Type())
	})

	t.Run("FinishProjectEventForSDEActivity_WhenCVPClientReturnsError", func(tt *testing.T) {
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

		params := &common.FinishProjectEventParams{
			State:          models.StateOff,
			LocationId:     "test-location-id",
			ProjectNumber:  "test-project-number",
			XCorrelationID: "test-correlation-id",
		}

		errMsg := "Client not available"
		mockClient.EXPECT().V1betaFinishProjectEvent(mock.Anything).Return(nil, nil, nil, errors.New(errMsg))

		activity := &FinishProjectEventActivity{SE: mockSE}
		_, err := activity.FinishProjectEventForSDEActivity(ctx, params)
		assert.NotNil(tt, err)
		assert.ErrorContains(tt, err, errMsg)
	})

	t.Run("FinishProjectEventForSDEActivity_WhenCVPClientReturnsUnexpectedResponse", func(tt *testing.T) {
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

		params := &common.FinishProjectEventParams{
			State:          models.StateOff,
			LocationId:     "test-location-id",
			ProjectNumber:  "test-project-number",
			XCorrelationID: "test-correlation-id",
		}

		mockClient.EXPECT().V1betaFinishProjectEvent(mock.Anything).Return(nil, nil, nil, nil)

		activity := &FinishProjectEventActivity{SE: mockSE}
		result, err := activity.FinishProjectEventForSDEActivity(ctx, params)
		assert.NotNil(tt, err)
		assert.ErrorContains(tt, err, "Unexpected response from SDE")
		assert.Nil(tt, result)
	})
}

func Test_PollFinishProjectEventSDEOperationActivity(t *testing.T) {
	t.Run("PollFinishProjectEventSDEOperationActivity_Success", func(tt *testing.T) {
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

		params := &common.FinishProjectEventParams{
			State:          models.StateOff,
			LocationId:     "test-location-id",
			ProjectNumber:  "test-project-number",
			XCorrelationID: "test-correlation-id",
		}
		result := &common.FinishProjectEventResult{
			Done: nillable.GetBoolPtr(false),
			Name: nillable.GetStringPtr("operations/test-operation-uuid"),
		}

		response := &async.V1betaDescribeOperationOK{
			Payload: &models2.OperationV1beta{
				Name: "operations/test-operation-uuid",
				Done: nillable.GetBoolPtr(true),
			},
		}
		mockAsync.EXPECT().V1betaDescribeOperation(mock.Anything).Return(response, nil)

		activity := &FinishProjectEventActivity{SE: mockSE}
		err := activity.PollFinishProjectEventSDEOperationActivity(ctx, params, result)
		assert.NoError(tt, err)
	})

	t.Run("PollFinishProjectEventSDEOperationActivity_WhenGetSignedTokenFails", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)

		params := &common.FinishProjectEventParams{
			State:          models.StateOff,
			LocationId:     "test-location-id",
			ProjectNumber:  "test-project-number",
			XCorrelationID: "test-correlation-id",
		}
		result := &common.FinishProjectEventResult{
			Done: nillable.GetBoolPtr(false),
			Name: nillable.GetStringPtr("operations/test-operation-uuid"),
		}

		originalToken := auth.GetSignedJwtToken
		defer func() { getSignedToken = originalToken }()
		getSignedToken = func(projectNumber string) (string, error) {
			return "", errors.New("Failed to get signed token")
		}

		activity := &FinishProjectEventActivity{SE: mockSE}
		err := activity.PollFinishProjectEventSDEOperationActivity(ctx, params, result)
		assert.NotNil(tt, err)
		assert.ErrorContains(tt, err, "Failed to get signed token")

		var applicationError *temporal.ApplicationError
		assert.True(tt, errors2.As(err, &applicationError))
		assert.False(tt, applicationError.NonRetryable())
		assert.Equal(tt, "CustomError", applicationError.Type())
	})

	t.Run("PollFinishProjectEventSDEOperationActivity_WhenJobErrorsOut", func(tt *testing.T) {
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

		params := &common.FinishProjectEventParams{
			State:          models.StateOff,
			LocationId:     "test-location-id",
			ProjectNumber:  "test-project-number",
			XCorrelationID: "test-correlation-id",
		}
		result := &common.FinishProjectEventResult{
			Done: nillable.GetBoolPtr(false),
			Name: nillable.GetStringPtr("operations/test-operation-uuid"),
		}

		response := &async.V1betaDescribeOperationOK{
			Payload: &models2.OperationV1beta{
				Name: "operations/test-operation-uuid",
				Done: nillable.GetBoolPtr(true),
				Error: &models2.StatusV1Beta{
					Code:    float64(500),
					Message: "Internal Server Error",
				},
			},
		}
		mockAsync.EXPECT().V1betaDescribeOperation(mock.Anything).Return(response, nil)

		activity := &FinishProjectEventActivity{SE: mockSE}
		err := activity.PollFinishProjectEventSDEOperationActivity(ctx, params, result)
		var applicationError *temporal.ApplicationError
		assert.NotNil(tt, err)
		assert.True(tt, errors2.As(err, &applicationError))
		assert.True(tt, applicationError.NonRetryable())
		assert.Equal(tt, "CustomError", applicationError.Type())
		assert.ErrorContains(tt, err, "Internal Server Error")
	})

	t.Run("PollFinishProjectEventSDEOperationActivity_WhenJobIsNotFinished", func(tt *testing.T) {
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

		params := &common.FinishProjectEventParams{
			State:          models.StateOff,
			LocationId:     "test-location-id",
			ProjectNumber:  "test-project-number",
			XCorrelationID: "test-correlation-id",
		}
		result := &common.FinishProjectEventResult{
			Done: nillable.GetBoolPtr(false),
			Name: nillable.GetStringPtr("operations/test-operation-uuid"),
		}

		response := &async.V1betaDescribeOperationOK{
			Payload: &models2.OperationV1beta{
				Name: "operations/test-operation-uuid",
				Done: nil,
			},
		}
		mockAsync.EXPECT().V1betaDescribeOperation(mock.Anything).Return(response, nil)

		activity := &FinishProjectEventActivity{SE: mockSE}
		err := activity.PollFinishProjectEventSDEOperationActivity(ctx, params, result)
		assert.NotNil(tt, err)
		assert.ErrorContains(tt, err, "job not finished")

		var applicationError *temporal.ApplicationError
		assert.True(tt, errors2.As(err, &applicationError))
		assert.False(tt, applicationError.NonRetryable())
		assert.Equal(tt, "CustomError", applicationError.Type())
	})

	t.Run("PollFinishProjectEventSDEOperationActivity_WhenCVPClientReturnsError", func(tt *testing.T) {
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

		params := &common.FinishProjectEventParams{
			State:          models.StateOff,
			LocationId:     "test-location-id",
			ProjectNumber:  "test-project-number",
			XCorrelationID: "test-correlation-id",
		}
		result := &common.FinishProjectEventResult{
			Done: nillable.GetBoolPtr(false),
			Name: nillable.GetStringPtr("operations/test-operation-uuid"),
		}

		errMsg := "Client not available"
		mockAsync.EXPECT().V1betaDescribeOperation(mock.Anything).Return(nil, errors.New(errMsg))

		activity := &FinishProjectEventActivity{SE: mockSE}
		err := activity.PollFinishProjectEventSDEOperationActivity(ctx, params, result)
		assert.NotNil(tt, err)
		assert.ErrorContains(tt, err, errMsg)

		var applicationError *temporal.ApplicationError
		assert.True(tt, errors2.As(err, &applicationError))
		assert.False(tt, applicationError.NonRetryable())
		assert.Equal(tt, "CustomError", applicationError.Type())
	})

	t.Run("PollFinishProjectEventSDEOperationActivity_WhenOperationNameIsNil", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)

		params := &common.FinishProjectEventParams{
			State:          models.StateOff,
			LocationId:     "test-location-id",
			ProjectNumber:  "test-project-number",
			XCorrelationID: "test-correlation-id",
		}
		result := &common.FinishProjectEventResult{
			Done: nillable.GetBoolPtr(false),
			Name: nil,
		}

		activity := &FinishProjectEventActivity{SE: mockSE}
		err := activity.PollFinishProjectEventSDEOperationActivity(ctx, params, result)
		assert.NotNil(tt, err)
		assert.ErrorContains(tt, err, "operation name is nil")

		var applicationError *temporal.ApplicationError
		assert.True(tt, errors2.As(err, &applicationError))
		assert.True(tt, applicationError.NonRetryable())
		assert.Equal(tt, "InvalidOperationNameError", applicationError.Type())
	})

	t.Run("PollFinishProjectEventSDEOperationActivity_WhenResultIsDone", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)

		params := &common.FinishProjectEventParams{
			State:          models.StateOff,
			LocationId:     "test-location-id",
			ProjectNumber:  "test-project-number",
			XCorrelationID: "test-correlation-id",
		}
		result := &common.FinishProjectEventResult{
			Done: nillable.GetBoolPtr(true),
			Name: nillable.GetStringPtr("operations/test-operation-uuid"),
		}

		activity := &FinishProjectEventActivity{SE: mockSE}
		err := activity.PollFinishProjectEventSDEOperationActivity(ctx, params, result)
		assert.Nil(tt, err)
	})
}
