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
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"go.temporal.io/sdk/temporal"
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
			LocationId:     "test-location",
			ProjectNumber:  "test-project",
			XCorrelationID: "test-correlation-id",
			State:          "test-state",
		}
		originalToken := auth.GetSignedJwtToken
		defer func() { getSignedToken = originalToken }()
		getSignedToken = func(projectNumber string) (string, error) {
			return "test-token", nil
		}

		created := &resource_events.V1betaFinishProjectEventCreated{
			Payload: &models2.OperationV1beta{
				Done: nillable.GetBoolPtr(true),
				Name: "test-operation-name",
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
			return "test-token", nil
		}

		params := &common.FinishProjectEventParams{
			LocationId:     "test-location",
			ProjectNumber:  "test-project",
			XCorrelationID: "test-correlation-id",
			State:          "test-state",
		}

		accepted := &resource_events.V1betaFinishProjectEventAccepted{
			Payload: &models2.OperationV1beta{
				Done: nillable.GetBoolPtr(false),
				Name: "test-operation-name",
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
			LocationId:     "test-location",
			ProjectNumber:  "test-project",
			XCorrelationID: "test-correlation-id",
			State:          "test-state",
		}
		originalToken := auth.GetSignedJwtToken
		defer func() { getSignedToken = originalToken }()
		getSignedToken = func(projectNumber string) (string, error) {
			return "", errors.New("token error")
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
			return "test-token", nil
		}

		params := &common.FinishProjectEventParams{
			LocationId:     "test-location",
			ProjectNumber:  "test-project",
			XCorrelationID: "test-correlation-id",
			State:          "test-state",
		}

		errMsg := "Client not available"
		mockClient.EXPECT().V1betaFinishProjectEvent(mock.Anything).Return(nil, nil, nil, errors.New(errMsg))

		activity := &FinishProjectEventActivity{SE: mockSE}
		_, err := activity.FinishProjectEventForSDEActivity(ctx, params)
		assert.NotNil(tt, err)
		assert.ErrorContains(tt, err, "Client not available")
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
			return "test-token", nil
		}

		params := &common.FinishProjectEventParams{
			LocationId:     "test-location",
			ProjectNumber:  "test-project",
			XCorrelationID: "test-correlation-id",
			State:          "test-state",
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
			return "test-token", nil
		}

		params := &common.FinishProjectEventParams{
			LocationId:     "test-location",
			ProjectNumber:  "test-project",
			XCorrelationID: "test-correlation-id",
			State:          "test-state",
		}
		result := &common.FinishProjectEventResult{
			Done: nillable.GetBoolPtr(false),
			Name: nillable.GetStringPtr("operations/test-operation-uuid"),
		}

		response := &async.V1betaDescribeOperationOK{
			Payload: &models2.OperationV1beta{
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
			LocationId:     "test-location",
			ProjectNumber:  "test-project",
			XCorrelationID: "test-correlation-id",
			State:          "test-state",
		}
		result := &common.FinishProjectEventResult{
			Done: nillable.GetBoolPtr(false),
			Name: nillable.GetStringPtr("operations/test-operation-uuid"),
		}

		originalToken := auth.GetSignedJwtToken
		defer func() { getSignedToken = originalToken }()
		getSignedToken = func(projectNumber string) (string, error) {
			return "", errors.New("token error")
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
			return "test-token", nil
		}

		params := &common.FinishProjectEventParams{
			LocationId:     "test-location",
			ProjectNumber:  "test-project",
			XCorrelationID: "test-correlation-id",
			State:          "test-state",
		}
		result := &common.FinishProjectEventResult{
			Done: nillable.GetBoolPtr(false),
			Name: nillable.GetStringPtr("operations/test-operation-uuid"),
		}

		response := &async.V1betaDescribeOperationOK{
			Payload: &models2.OperationV1beta{
				Done: nillable.GetBoolPtr(true),
				Error: &models2.StatusV1Beta{
					Message: "job failed",
				},
			},
		}
		mockAsync.EXPECT().V1betaDescribeOperation(mock.Anything).Return(response, nil)

		activity := &FinishProjectEventActivity{SE: mockSE}
		err := activity.PollFinishProjectEventSDEOperationActivity(ctx, params, result)
		assert.NotNil(tt, err)
		assert.ErrorContains(tt, err, "Client Error during FinishProjectEvent")

		var applicationError *temporal.ApplicationError
		assert.True(tt, errors2.As(err, &applicationError))
		assert.True(tt, applicationError.NonRetryable())
		assert.Equal(tt, "CustomError", applicationError.Type())
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
			return "test-token", nil
		}

		params := &common.FinishProjectEventParams{
			LocationId:     "test-location",
			ProjectNumber:  "test-project",
			XCorrelationID: "test-correlation-id",
			State:          "test-state",
		}
		result := &common.FinishProjectEventResult{
			Done: nillable.GetBoolPtr(false),
			Name: nillable.GetStringPtr("operations/test-operation-uuid"),
		}

		response := &async.V1betaDescribeOperationOK{
			Payload: &models2.OperationV1beta{
				Done: nillable.GetBoolPtr(false),
			},
		}
		mockAsync.EXPECT().V1betaDescribeOperation(mock.Anything).Return(response, nil)

		activity := &FinishProjectEventActivity{SE: mockSE}
		err := activity.PollFinishProjectEventSDEOperationActivity(ctx, params, result)
		assert.NotNil(tt, err)
		assert.ErrorContains(tt, err, "Error SDE job not done")

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
			return "test-token", nil
		}

		params := &common.FinishProjectEventParams{
			LocationId:     "test-location",
			ProjectNumber:  "test-project",
			XCorrelationID: "test-correlation-id",
			State:          "test-state",
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
		assert.ErrorContains(tt, err, "Error describing SDE Operation")

		var applicationError *temporal.ApplicationError
		assert.True(tt, errors2.As(err, &applicationError))
		assert.True(tt, applicationError.NonRetryable())
		assert.Equal(tt, "NonRetryableError", applicationError.Type())
	})

	t.Run("PollFinishProjectEventSDEOperationActivity_WhenOperationNameIsNil", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)

		params := &common.FinishProjectEventParams{
			LocationId:     "test-location",
			ProjectNumber:  "test-project",
			XCorrelationID: "test-correlation-id",
			State:          "test-state",
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
			LocationId:     "test-location",
			ProjectNumber:  "test-project",
			XCorrelationID: "test-correlation-id",
			State:          "test-state",
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
func Test_VerifySoftDeletedResourcesForAccount(t *testing.T) {
	t.Run("VerifySoftDeletedResourcesForAccount_AllResourcesFound", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)

		projectNumber := "test-project-123"
		accountID := int64(123)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID: accountID,
			},
			Name: projectNumber,
		}

		volumes := []*datamodel.Volume{}
		pools := []*datamodel.PoolView{}
		svms := []*datamodel.Svm{}

		mockSE.EXPECT().GetSoftDeleteAccount(ctx, projectNumber).Return(account, nil)
		mockSE.EXPECT().ListVolumes(ctx, mock.Anything).Return(volumes, nil)
		mockSE.EXPECT().ListPools(ctx, mock.Anything).Return(pools, nil)
		mockSE.EXPECT().ListSvmsWithAccountId(ctx, accountID).Return(svms, nil)

		activity := &FinishProjectEventActivity{SE: mockSE}
		result, err := activity.VerifySoftDeletedResourcesForAccount(ctx, projectNumber)
		assert.NoError(tt, err)
		assert.True(tt, result)
		mockSE.AssertExpectations(tt)
	})

	t.Run("VerifySoftDeletedResourcesForAccount_GetAccountError", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)

		projectNumber := "test-project-123"
		expectedErr := errors.New("account not found")

		mockSE.EXPECT().GetSoftDeleteAccount(ctx, projectNumber).Return(nil, expectedErr)

		activity := &FinishProjectEventActivity{SE: mockSE}
		result, err := activity.VerifySoftDeletedResourcesForAccount(ctx, projectNumber)
		assert.Error(tt, err)
		assert.Equal(tt, expectedErr, err)
		assert.False(tt, result)
		mockSE.AssertExpectations(tt)
	})

	t.Run("VerifySoftDeletedResourcesForAccount_ListVolumesError", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)

		projectNumber := "test-project-123"
		accountID := int64(123)
		listErr := errors.New("list volumes failed")

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID: accountID,
			},
			Name: projectNumber,
		}

		mockSE.EXPECT().GetSoftDeleteAccount(ctx, projectNumber).Return(account, nil)
		mockSE.EXPECT().ListVolumes(ctx, mock.Anything).Return(nil, listErr)

		activity := &FinishProjectEventActivity{SE: mockSE}
		result, err := activity.VerifySoftDeletedResourcesForAccount(ctx, projectNumber)
		assert.Error(tt, err)
		assert.Equal(tt, listErr, err)
		assert.False(tt, result)
		mockSE.AssertExpectations(tt)
	})

	t.Run("VerifySoftDeletedResourcesForAccount_ListPoolsError", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)

		projectNumber := "test-project-123"
		accountID := int64(123)
		listErr := errors.New("list pools failed")

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID: accountID,
			},
			Name: projectNumber,
		}

		volumes := []*datamodel.Volume{
			{BaseModel: datamodel.BaseModel{ID: 1}},
		}

		mockSE.EXPECT().GetSoftDeleteAccount(ctx, projectNumber).Return(account, nil)
		mockSE.EXPECT().ListVolumes(ctx, mock.Anything).Return(volumes, nil)
		mockSE.EXPECT().ListPools(ctx, mock.Anything).Return(nil, listErr)

		activity := &FinishProjectEventActivity{SE: mockSE}
		result, err := activity.VerifySoftDeletedResourcesForAccount(ctx, projectNumber)
		assert.Error(tt, err)
		assert.Equal(tt, listErr, err)
		assert.False(tt, result)
		mockSE.AssertExpectations(tt)
	})

	t.Run("VerifySoftDeletedResourcesForAccount_ListSvmsError", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)

		projectNumber := "test-project-123"
		accountID := int64(123)
		listErr := errors.New("list svms failed")

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID: accountID,
			},
			Name: projectNumber,
		}

		volumes := []*datamodel.Volume{
			{BaseModel: datamodel.BaseModel{ID: 1}},
		}
		pools := []*datamodel.PoolView{
			{Pool: datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}},
		}

		mockSE.EXPECT().GetSoftDeleteAccount(ctx, projectNumber).Return(account, nil)
		mockSE.EXPECT().ListVolumes(ctx, mock.Anything).Return(volumes, nil)
		mockSE.EXPECT().ListPools(ctx, mock.Anything).Return(pools, nil)
		mockSE.EXPECT().ListSvmsWithAccountId(ctx, accountID).Return(nil, listErr)

		activity := &FinishProjectEventActivity{SE: mockSE}
		result, err := activity.VerifySoftDeletedResourcesForAccount(ctx, projectNumber)
		assert.Error(tt, err)
		assert.Equal(tt, listErr, err)
		assert.False(tt, result)
		mockSE.AssertExpectations(tt)
	})

	t.Run("VerifySoftDeletedResourcesForAccount_NoResourcesFound", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)

		projectNumber := "test-project-123"
		accountID := int64(123)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID: accountID,
			},
			Name: projectNumber,
		}

		volumes := []*datamodel.Volume{}
		pools := []*datamodel.PoolView{}
		svms := []*datamodel.Svm{}

		mockSE.EXPECT().GetSoftDeleteAccount(ctx, projectNumber).Return(account, nil)
		mockSE.EXPECT().ListVolumes(ctx, mock.Anything).Return(volumes, nil)
		mockSE.EXPECT().ListPools(ctx, mock.Anything).Return(pools, nil)
		mockSE.EXPECT().ListSvmsWithAccountId(ctx, accountID).Return(svms, nil)

		activity := &FinishProjectEventActivity{SE: mockSE}
		result, err := activity.VerifySoftDeletedResourcesForAccount(ctx, projectNumber)
		assert.NoError(tt, err)
		assert.True(tt, result)
		mockSE.AssertExpectations(tt)
	})
}

func Test_RollbackAccountStateActivity(t *testing.T) {
	t.Run("RollbackAccountStateActivity_Success", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)

		projectNumber := "test-project-123"
		accountID := int64(123)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID: accountID,
			},
			Name: projectNumber,
		}

		mockSE.EXPECT().GetSoftDeleteAccount(ctx, projectNumber).Return(account, nil)
		mockSE.EXPECT().RollBackDeletedAccount(ctx, accountID).Return(nil)

		activity := &FinishProjectEventActivity{SE: mockSE}
		err := activity.RollbackAccountStateActivity(ctx, projectNumber)
		assert.Nil(tt, err)
		mockSE.AssertExpectations(tt)
	})

	t.Run("RollbackAccountStateActivity_GetSoftDeleteAccountFails", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)

		projectNumber := "test-project-123"
		expectedErr := errors.New("account not found")

		mockSE.EXPECT().GetSoftDeleteAccount(ctx, projectNumber).Return(nil, expectedErr)

		activity := &FinishProjectEventActivity{SE: mockSE}
		err := activity.RollbackAccountStateActivity(ctx, projectNumber)
		assert.NotNil(tt, err)
		assert.Equal(tt, expectedErr, err)
		mockSE.AssertExpectations(tt)
	})

	t.Run("RollbackAccountStateActivity_RollBackDeletedAccountFails", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)

		projectNumber := "test-project-123"
		accountID := int64(123)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID: accountID,
			},
			Name: projectNumber,
		}

		mockSE.EXPECT().GetSoftDeleteAccount(ctx, projectNumber).Return(account, nil)
		mockSE.EXPECT().RollBackDeletedAccount(ctx, accountID).Return(errors.New("rollback failed"))

		activity := &FinishProjectEventActivity{SE: mockSE}
		err := activity.RollbackAccountStateActivity(ctx, projectNumber)
		assert.Nil(tt, err)
		mockSE.AssertExpectations(tt)
	})
}

func Test_DeleteServiceAccountsFromAccountID(t *testing.T) {
	t.Run("Success", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		projectNumber := "test-project"
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 123}, Name: projectNumber}
		serviceAccounts := []*datamodel.ServiceAccount{
			{BaseModel: datamodel.BaseModel{ID: 1}},
			{BaseModel: datamodel.BaseModel{ID: 2}},
		}

		mockSE.EXPECT().GetAccount(ctx, projectNumber).Return(account, nil)
		mockSE.EXPECT().ListKmsServiceAccounts(ctx, mock.Anything).Return(serviceAccounts, nil)
		mockSE.EXPECT().DeleteServiceAccount(ctx, serviceAccounts[0]).Return(nil)
		mockSE.EXPECT().DeleteServiceAccount(ctx, serviceAccounts[1]).Return(nil)

		activity := &FinishProjectEventActivity{SE: mockSE}
		err := activity.DeleteServiceAccountsFromAccountID(ctx, projectNumber)
		assert.NoError(tt, err)
		mockSE.AssertExpectations(tt)
	})

	t.Run("GetAccount_Error", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		projectNumber := "test-project"
		expectedErr := errors.New("account not found")

		mockSE.EXPECT().GetAccount(ctx, projectNumber).Return(nil, expectedErr)

		activity := &FinishProjectEventActivity{SE: mockSE}
		err := activity.DeleteServiceAccountsFromAccountID(ctx, projectNumber)
		assert.Error(tt, err)
		assert.Equal(tt, expectedErr, err)
		mockSE.AssertExpectations(tt)
	})

	t.Run("ListKmsServiceAccounts_Error", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		projectNumber := "test-project"
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 123}, Name: projectNumber}
		expectedErr := errors.New("list service accounts failed")

		mockSE.EXPECT().GetAccount(ctx, projectNumber).Return(account, nil)
		mockSE.EXPECT().ListKmsServiceAccounts(ctx, mock.Anything).Return(nil, expectedErr)

		activity := &FinishProjectEventActivity{SE: mockSE}
		err := activity.DeleteServiceAccountsFromAccountID(ctx, projectNumber)
		assert.Error(tt, err)
		assert.Equal(tt, expectedErr, err)
		mockSE.AssertExpectations(tt)
	})

	t.Run("DeleteServiceAccount_Error", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		projectNumber := "test-project"
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 123}, Name: projectNumber}
		serviceAccounts := []*datamodel.ServiceAccount{
			{BaseModel: datamodel.BaseModel{ID: 1}},
		}
		expectedErr := errors.New("delete service account failed")

		mockSE.EXPECT().GetAccount(ctx, projectNumber).Return(account, nil)
		mockSE.EXPECT().ListKmsServiceAccounts(ctx, mock.Anything).Return(serviceAccounts, nil)
		mockSE.EXPECT().DeleteServiceAccount(ctx, serviceAccounts[0]).Return(expectedErr)

		activity := &FinishProjectEventActivity{SE: mockSE}
		err := activity.DeleteServiceAccountsFromAccountID(ctx, projectNumber)
		assert.Error(tt, err)
		assert.Equal(tt, expectedErr, err)
		mockSE.AssertExpectations(tt)
	})
}
