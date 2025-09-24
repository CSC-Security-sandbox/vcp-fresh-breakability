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
	cvpmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	errors2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	coremodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/scheduler"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"go.temporal.io/sdk/temporal"
)

func Test_HandleResourceEventForSDEActivity(t *testing.T) {
	t.Run("HandleResourceEventCheckForVCPActivity_WhenResourceTypeIsKmsConfig", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		mockScheduler := scheduler.NewMockScheduler(tt)
		activity := &ResourceEventsActivity{SE: mockSE, Scheduler: mockScheduler}

		params := &common.HandleResourceEventParams{
			ResourceType: common.ResourceStateV1ResourceTypeKmsConfig,
			ResourceId:   "test-kms-config-id",
		}

		mockSE.On("GetKmsConfig", ctx, params.ResourceId).Return(&datamodel.KmsConfig{}, nil)

		result, err := activity.HandleResourceEventCheckForVCPActivity(ctx, params)
		assert.True(tt, result)
		assert.Nil(tt, err)
	})
	t.Run("HandleResourceEventCheckForVCPActivity_WhenResourceTypeIsStoragePool", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &ResourceEventsActivity{SE: mockSE}

		params := &common.HandleResourceEventParams{
			ResourceType:  common.ResourceStateV1ResourceTypeStoragePool,
			ResourceId:    "test-storage-pool-id",
			ProjectNumber: "test-project-number",
		}

		account := datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-account-id",
				ID:   1,
			},
		}

		mockSE.On("GetAccount", ctx, params.ProjectNumber).Return(&account, nil)
		mockSE.On("GetPool", ctx, params.ResourceId, account.ID).Return(&datamodel.PoolView{}, nil)

		result, err := activity.HandleResourceEventCheckForVCPActivity(ctx, params)
		assert.True(tt, result)
		assert.Nil(tt, err)
	})
	t.Run("HandleResourceEventCheckForVCPActivity_WhenResourceTypeIsUnsupported", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &ResourceEventsActivity{SE: mockSE}

		params := &common.HandleResourceEventParams{
			ResourceType: "unsupported-resource-type",
			ResourceId:   "test-resource-id",
		}

		result, err := activity.HandleResourceEventCheckForVCPActivity(ctx, params)
		assert.False(tt, result)
		assert.NotNil(tt, err)
		assert.ErrorContains(tt, err, "unsupported resource type")
	})
	t.Run("HandleResourceEventForSDEActivity_SuccessCreated", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)
		mockClient := resource_events.NewMockClientService(t)
		cvpClient := &cvpapi.Cvp{ResourceEvents: mockClient}
		originalCreateClient := cvp.CreateClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, JWT string) cvpapi.Cvp {
			return *cvpClient
		}

		params := &common.HandleResourceEventParams{
			State:          coremodels.StateOff,
			LocationId:     "test-location-id",
			ProjectNumber:  "test-project-number",
			XCorrelationID: "test-correlation-id",
			ResourceType:   common.ResourceStateV1ResourceTypeVolume,
			ResourceId:     "c9a7591c-0070-5d05-9b28-c58dbc227e84",
		}
		originalToken := auth.GetSignedJwtToken
		defer func() { getSignedToken = originalToken }()
		getSignedToken = func(projectNumber string) (string, error) {
			return "test-jwt-token", nil
		}

		created := &resource_events.V1betaResourceStateUpdateCreated{
			Payload: &cvpmodels.OperationV1beta{
				Name: "test-operation-name",
				Done: nillable.GetBoolPtr(true),
			},
		}
		mockClient.EXPECT().V1betaResourceStateUpdate(mock.Anything).Return(created, nil, nil, nil)

		activity := &ResourceEventsActivity{SE: mockSE}
		result, err := activity.HandleResourceEventsForSDEActivity(ctx, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "test-operation-name", *result.Name)
		assert.Equal(tt, true, *result.Done)
	})
	t.Run("HandleResourceEventForSDEActivity_SuccessAccepted", func(tt *testing.T) {
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

		params := &common.HandleResourceEventParams{
			State:          coremodels.StateOff,
			LocationId:     "test-location-id",
			ProjectNumber:  "test-project-number",
			XCorrelationID: "test-correlation-id",
		}

		accepted := &resource_events.V1betaResourceStateUpdateAccepted{
			Payload: &cvpmodels.OperationV1beta{
				Name: "test-operation-name",
				Done: nillable.GetBoolPtr(false),
			},
		}
		mockClient.EXPECT().V1betaResourceStateUpdate(mock.Anything).Return(nil, accepted, nil, nil)

		activity := &ResourceEventsActivity{SE: mockSE}
		result, err := activity.HandleResourceEventsForSDEActivity(ctx, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "test-operation-name", *result.Name)
		assert.False(tt, *result.Done)
	})
	t.Run("HandleResourceEventForSDEActivity_WhenCVPClientReturnsError", func(tt *testing.T) {
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
		params := &common.HandleResourceEventParams{
			State:          coremodels.StateOff,
			LocationId:     "test-location-id",
			ProjectNumber:  "test-project-number",
			XCorrelationID: "test-correlation-id",
		}

		mockClient.EXPECT().V1betaResourceStateUpdate(mock.Anything).Return(nil, nil, nil, errors.New("Client not available"))

		activity := &ResourceEventsActivity{SE: mockSE}
		_, err := activity.HandleResourceEventsForSDEActivity(ctx, params)
		assert.NotNil(tt, err)
		assert.ErrorContains(tt, err, "Client not available")

		var applicationError *temporal.ApplicationError
		assert.True(tt, errors2.As(err, &applicationError))
		assert.True(tt, applicationError.NonRetryable())
		assert.Equal(tt, "NonRetryableError", applicationError.Type())
	})
	t.Run("HandleResourceEventForSDEActivity_WhenCVPClientReturnsUnexpectedResponse", func(tt *testing.T) {
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
		params := &common.HandleResourceEventParams{
			State:          coremodels.StateOff,
			LocationId:     "test-location-id",
			ProjectNumber:  "test-project-number",
			XCorrelationID: "test-correlation-id",
		}

		mockClient.EXPECT().V1betaResourceStateUpdate(mock.Anything).Return(nil, nil, nil, nil)

		activity := &ResourceEventsActivity{SE: mockSE}
		res, err := activity.HandleResourceEventsForSDEActivity(ctx, params)
		assert.NoError(tt, err)
		assert.Nil(tt, res)
	})
	t.Run("HandleResourceEventForSDEActivity_SuccessWithSnapshot", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)
		mockClient := resource_events.NewMockClientService(t)
		cvpClient := &cvpapi.Cvp{ResourceEvents: mockClient}
		originalCreateClient := cvp.CreateClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, JWT string) cvpapi.Cvp {
			return *cvpClient
		}

		params := &common.HandleResourceEventParams{
			State:              coremodels.StateOff,
			LocationId:         "test-location-id",
			ProjectNumber:      "test-project-number",
			XCorrelationID:     "test-correlation-id",
			ResourceType:       common.ResourceStateV1ResourceTypeSnapshot,
			ResourceId:         "test-snapshot-id",
			ParentResourceID:   "test-parent-volume-id",
			ParentResourceType: "Volume",
		}
		originalToken := auth.GetSignedJwtToken
		defer func() { getSignedToken = originalToken }()
		getSignedToken = func(projectNumber string) (string, error) {
			return "test-jwt-token", nil
		}

		created := &resource_events.V1betaResourceStateUpdateCreated{
			Payload: &cvpmodels.OperationV1beta{
				Name: "test-operation-name",
				Done: nillable.GetBoolPtr(true),
			},
		}
		mockClient.EXPECT().V1betaResourceStateUpdate(mock.Anything).Return(created, nil, nil, nil)

		activity := &ResourceEventsActivity{SE: mockSE}
		result, err := activity.HandleResourceEventsForSDEActivity(ctx, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "test-operation-name", *result.Name)
		assert.Equal(tt, true, *result.Done)
	})
}

func Test_PollHandleResourceEventSDEOperationActivity(t *testing.T) {
	t.Run("PollHandleResourceEventSDEOperationActivity_Success", func(tt *testing.T) {
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
		params := &common.HandleResourceEventParams{
			State:          coremodels.StateOff,
			LocationId:     "test-location-id",
			ProjectNumber:  "test-project-number",
			XCorrelationID: "test-correlation-id",
		}
		result := &common.HandleResourceEventResult{
			Done: nillable.GetBoolPtr(false),
			Name: nillable.GetStringPtr("test-operation-name"),
		}

		response := &async.V1betaDescribeOperationOK{
			Payload: &cvpmodels.OperationV1beta{
				Name: "test-operation-name",
				Done: nillable.GetBoolPtr(true),
			},
		}
		mockAsync.EXPECT().V1betaDescribeOperation(mock.Anything).Return(response, nil)

		activity := &ResourceEventsActivity{SE: mockSE}
		err := activity.PollHandleResourceEventSDEOperationActivity(ctx, params, result)
		assert.NoError(tt, err)
	})
	t.Run("PollHandleResourceEventSDEOperationActivity_WhenJobErrorsOut", func(tt *testing.T) {
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
		params := &common.HandleResourceEventParams{
			State:          coremodels.StateOff,
			LocationId:     "test-location-id",
			ProjectNumber:  "test-project-number",
			XCorrelationID: "test-correlation-id",
		}
		result := &common.HandleResourceEventResult{
			Done: nillable.GetBoolPtr(false),
			Name: nillable.GetStringPtr("test-operation-name"),
		}

		response := &async.V1betaDescribeOperationOK{
			Payload: &cvpmodels.OperationV1beta{
				Name: "test-operation-name",
				Done: nillable.GetBoolPtr(true),
				Error: &cvpmodels.StatusV1Beta{
					Code:    float64(500),
					Message: "Internal Server Error",
				},
			},
		}
		mockAsync.EXPECT().V1betaDescribeOperation(mock.Anything).Return(response, nil)

		activity := &ResourceEventsActivity{SE: mockSE}
		err := activity.PollHandleResourceEventSDEOperationActivity(ctx, params, result)
		var applicationError *temporal.ApplicationError
		assert.NotNil(tt, err)
		assert.True(tt, errors2.As(err, &applicationError))
		assert.True(tt, applicationError.NonRetryable())
		assert.Equal(tt, "CustomError", applicationError.Type())
		assert.ErrorContains(tt, err, "Client error during HandleResouceEvent")
	})
	t.Run("PollHandleResourceEventSDEOperationActivity_WhenJobIsNotFinished", func(tt *testing.T) {
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
		params := &common.HandleResourceEventParams{
			State:          coremodels.StateOff,
			LocationId:     "test-location-id",
			ProjectNumber:  "test-project-number",
			XCorrelationID: "test-correlation-id",
		}
		result := &common.HandleResourceEventResult{
			Done: nillable.GetBoolPtr(false),
			Name: nillable.GetStringPtr("test-operation-name"),
		}

		response := &async.V1betaDescribeOperationOK{
			Payload: &cvpmodels.OperationV1beta{
				Name: "test-operation-name",
			},
		}
		mockAsync.EXPECT().V1betaDescribeOperation(mock.Anything).Return(response, nil)

		activity := &ResourceEventsActivity{SE: mockSE}
		err := activity.PollHandleResourceEventSDEOperationActivity(ctx, params, result)
		assert.NotNil(tt, err)
		assert.ErrorContains(tt, err, "Error SDE job not done")
	})
	t.Run("PollHandleResourceEventSDEOperationActivity_WhenCVPClientReturnsError", func(tt *testing.T) {
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
		params := &common.HandleResourceEventParams{
			State:          coremodels.StateOff,
			LocationId:     "test-location-id",
			ProjectNumber:  "test-project-number",
			XCorrelationID: "test-correlation-id",
		}
		result := &common.HandleResourceEventResult{
			Done: nillable.GetBoolPtr(false),
			Name: nillable.GetStringPtr("test-operation-name"),
		}

		mockAsync.EXPECT().V1betaDescribeOperation(mock.Anything).Return(nil, errors.New("Client not available"))

		activity := &ResourceEventsActivity{SE: mockSE}
		err := activity.PollHandleResourceEventSDEOperationActivity(ctx, params, result)
		assert.NotNil(tt, err)
		assert.ErrorContains(tt, err, "Error describing SDE Operation")

		var applicationError *temporal.ApplicationError
		assert.True(tt, errors2.As(err, &applicationError))
		assert.True(tt, applicationError.NonRetryable())
		assert.Equal(tt, "NonRetryableError", applicationError.Type())
	})
	t.Run("PollHandleResourceEventSDEOperationActivity_WhenOperationNameIsNil", func(tt *testing.T) {
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
		params := &common.HandleResourceEventParams{
			State:          coremodels.StateOff,
			LocationId:     "test-location-id",
			ProjectNumber:  "test-project-number",
			XCorrelationID: "test-correlation-id",
		}
		result := &common.HandleResourceEventResult{
			Done: nillable.GetBoolPtr(false),
		}

		activity := &ResourceEventsActivity{SE: mockSE}
		err := activity.PollHandleResourceEventSDEOperationActivity(ctx, params, result)
		assert.NotNil(tt, err)
		assert.ErrorContains(tt, err, "Invalid Operation Name")

		var applicationError *temporal.ApplicationError
		assert.True(tt, errors2.As(err, &applicationError))
		assert.True(tt, applicationError.NonRetryable())
		assert.Equal(tt, "CustomError", applicationError.Type())
	})
	t.Run("PollHandleResourceEventSDEOperationActivity_WhenResultIsDone", func(tt *testing.T) {
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
		params := &common.HandleResourceEventParams{
			State:          coremodels.StateOff,
			LocationId:     "test-location-id",
			ProjectNumber:  "test-project-number",
			XCorrelationID: "test-correlation-id",
		}
		result := &common.HandleResourceEventResult{
			Done: nillable.GetBoolPtr(true),
		}

		activity := &ResourceEventsActivity{SE: mockSE}
		err := activity.PollHandleResourceEventSDEOperationActivity(ctx, params, result)
		assert.Nil(tt, err)
	})
}

func TestHandleResourceEventsOFFForVCPActivity(t *testing.T) {
	t.Run("HandleResourceEventsOFFForVCPActivity_WhenResourceTypeIsKmsConfig", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &ResourceEventsActivity{SE: mockSE}

		params := &common.HandleResourceEventParams{
			ResourceType: common.ResourceStateV1ResourceTypeKmsConfig,
			ResourceId:   "test-resource-id",
			State:        coremodels.StateOff,
		}

		mockSE.On("UpdateKmsConfigStateForHandleResource", ctx, params.ResourceId, coremodels.LifeCycleStateDisabledDetails, coremodels.StateOff).Return(nil, nil)

		result, err := activity.HandleResourceEventsOFFForVCPActivity(ctx, params)
		assert.True(tt, result)
		assert.Nil(tt, err)
	})

	t.Run("HandleResourceEventsOFFForVCPActivity_WhenResourceTypeIsUnsupported", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &ResourceEventsActivity{SE: mockSE}

		params := &common.HandleResourceEventParams{
			ResourceType: "unsupported-resource-type",
			ResourceId:   "test-resource-id",
		}

		result, err := activity.HandleResourceEventsOFFForVCPActivity(ctx, params)
		assert.False(tt, result)
		assert.NotNil(tt, err)
		assert.ErrorContains(tt, err, "unsupported resource type")
	})

	t.Run("HandleResourceEventsOFFForVCPActivity_WhenResourceTypeIsSnapshot", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &ResourceEventsActivity{SE: mockSE}

		params := &common.HandleResourceEventParams{
			ResourceType: common.ResourceStateV1ResourceTypeSnapshot,
			ResourceId:   "test-snapshot-id",
		}

		mockSE.On("UpdateSnapshotForHandleResource", ctx, &datamodel.Snapshot{
			BaseModel:    datamodel.BaseModel{UUID: params.ResourceId},
			State:        coremodels.LifeCycleStateDisabled,
			StateDetails: coremodels.LifeCycleStateDisabledDetails,
		}).Return(nil, nil)

		result, err := activity.HandleResourceEventsOFFForVCPActivity(ctx, params)
		assert.True(tt, result)
		assert.Nil(tt, err)
	})

	t.Run("HandleResourceEventsOFFForVCPActivity_WhenResourceTypeIsStoragePool", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &ResourceEventsActivity{SE: mockSE}

		params := &common.HandleResourceEventParams{
			ResourceType:  common.ResourceStateV1ResourceTypeStoragePool,
			ResourceId:    "test-storage-pool-id",
			ProjectNumber: "test-project-number",
		}

		mockAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID: 1,
			},
		}

		mockSE.On("GetAccount", ctx, "test-project-number").Return(mockAccount, nil)
		mockSE.On("GetPool", ctx, "test-storage-pool-id", int64(1)).Return(&datamodel.PoolView{}, nil)

		result, err := activity.HandleResourceEventsOFFForVCPActivity(ctx, params)
		assert.True(tt, result)
		assert.Nil(tt, err)
	})

	t.Run("HandleResourceEventsOFFForVCPActivity_WhenResourceTypeIsAD", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &ResourceEventsActivity{SE: mockSE}

		params := &common.HandleResourceEventParams{
			ResourceType: common.ResourceStateV1ResourceTypeAD,
			ResourceId:   "test-ad-id",
		}

		result, err := activity.HandleResourceEventsOFFForVCPActivity(ctx, params)
		assert.False(tt, result)
		assert.Nil(tt, err)
	})

	t.Run("HandleResourceEventsOFFForVCPActivity_WhenResourceTypeIsUnsupported", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &ResourceEventsActivity{SE: mockSE}

		params := &common.HandleResourceEventParams{
			ResourceType: "unsupported-resource-type",
			ResourceId:   "test-resource-id",
		}

		result, err := activity.HandleResourceEventsOFFForVCPActivity(ctx, params)
		assert.False(tt, result)
		assert.NotNil(tt, err)
		assert.ErrorContains(tt, err, "unsupported resource type")
	})

	t.Run("HandleResourceEventsOFFForVCPActivity_WhenUpdateFails", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &ResourceEventsActivity{SE: mockSE}

		params := &common.HandleResourceEventParams{
			ResourceType: common.ResourceStateV1ResourceTypeVolume,
			ResourceId:   "test-volume-id",
		}

		mockSE.On("UpdateVolume", ctx, &datamodel.Volume{
			BaseModel:    datamodel.BaseModel{UUID: params.ResourceId},
			State:        coremodels.LifeCycleStateDisabled,
			StateDetails: coremodels.LifeCycleStateDisabledDetails,
		}).Return(errors.New("update failed"))

		result, err := activity.HandleResourceEventsOFFForVCPActivity(ctx, params)
		assert.False(tt, result)
		assert.NotNil(tt, err)
		assert.ErrorContains(tt, err, "update failed")
	})
}

func Test_HandleResourceEventCheckForVCPActivity(t *testing.T) {
	t.Run("HandleResourceEventCheckForVCPActivity_KmsConfigExists", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &ResourceEventsActivity{SE: mockSE}

		params := &common.HandleResourceEventParams{
			ResourceType: common.ResourceStateV1ResourceTypeKmsConfig,
			ResourceId:   "test-kms-config-id",
		}

		mockSE.On("GetKmsConfig", ctx, params.ResourceId).Return(&datamodel.KmsConfig{}, nil)

		result, err := activity.HandleResourceEventCheckForVCPActivity(ctx, params)
		assert.True(tt, result)
		assert.Nil(tt, err)
	})

	t.Run("HandleResourceEventCheckForVCPActivity_StoragePoolExists", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &ResourceEventsActivity{SE: mockSE}

		params := &common.HandleResourceEventParams{
			ResourceType:  common.ResourceStateV1ResourceTypeStoragePool,
			ResourceId:    "test-storage-pool-id",
			ProjectNumber: "test-project-number",
		}
		account := datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-account-id",
				ID:   1,
			},
		}
		pool := datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				UUID: "test-storage-pool-id",
				ID:   1,
			},
		}

		mockSE.On("GetAccount", ctx, params.ProjectNumber).Return(&datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "test-account-id", ID: 1}}, nil)
		mockSE.On("GetPool", ctx, params.ResourceId, account.ID).Return(&datamodel.PoolView{Pool: pool}, nil)

		result, err := activity.HandleResourceEventCheckForVCPActivity(ctx, params)
		assert.True(tt, result)
		assert.Nil(tt, err)
	})

	t.Run("HandleResourceEventCheckForVCPActivity_SnapshotExists", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &ResourceEventsActivity{SE: mockSE}

		params := &common.HandleResourceEventParams{
			ResourceType:     common.ResourceStateV1ResourceTypeSnapshot,
			ResourceId:       "test-snapshot-id",
			ProjectNumber:    "test-project-number",
			ParentResourceID: "test-volume-id",
		}
		account := datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-account-id",
				ID:   1,
			},
		}
		volume := datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "test-volume-id",
				ID:   1,
			},
		}

		mockSE.On("GetAccount", ctx, params.ProjectNumber).Return(&account, nil)
		mockSE.On("GetVolumeWithAccountID", ctx, params.ParentResourceID, account.ID).Return(&volume, nil)
		mockSE.On("GetSnapshotByUUID", ctx, params.ResourceId, account.ID, volume.ID).Return(&datamodel.Snapshot{}, nil)

		result, err := activity.HandleResourceEventCheckForVCPActivity(ctx, params)
		assert.True(tt, result)
		assert.Nil(tt, err)
	})

	t.Run("HandleResourceEventCheckForVCPActivity_VolumeExists", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &ResourceEventsActivity{SE: mockSE}

		params := &common.HandleResourceEventParams{
			ResourceType: common.ResourceStateV1ResourceTypeVolume,
			ResourceId:   "test-volume-id",
		}

		mockSE.On("GetVolume", ctx, params.ResourceId).Return(&datamodel.Volume{}, nil)

		result, err := activity.HandleResourceEventCheckForVCPActivity(ctx, params)
		assert.True(tt, result)
		assert.Nil(tt, err)
	})

	t.Run("HandleResourceEventCheckForVCPActivity_BackupPolicyExists", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &ResourceEventsActivity{SE: mockSE}

		params := &common.HandleResourceEventParams{
			ResourceType:  common.ResourceStateV1ResourceTypeBackupPolicy,
			ResourceId:    "test-backup-policy-id",
			ProjectNumber: "test-project-123",
		}

		mockAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 123},
		}
		mockBackupPolicy := &datamodel.BackupPolicy{
			BaseModel: datamodel.BaseModel{UUID: "test-backup-policy-id"},
		}

		mockSE.On("GetAccount", ctx, params.ProjectNumber).Return(mockAccount, nil)
		mockSE.On("GetBackupPolicyByUUIDAndOwnerID", ctx, params.ResourceId, mockAccount.ID).Return(mockBackupPolicy, nil)

		result, err := activity.HandleResourceEventCheckForVCPActivity(ctx, params)
		assert.True(tt, result)
		assert.Nil(tt, err)
	})

	t.Run("HandleResourceEventCheckForVCPActivity_UnsupportedResourceType", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &ResourceEventsActivity{SE: mockSE}

		params := &common.HandleResourceEventParams{
			ResourceType: "unsupported-resource-type",
			ResourceId:   "test-resource-id",
		}

		result, err := activity.HandleResourceEventCheckForVCPActivity(ctx, params)
		assert.False(tt, result)
		assert.NotNil(tt, err)
		assert.ErrorContains(tt, err, "unsupported resource type")
	})
}

func TestHandleResourceEventsONForVCPActivity(t *testing.T) {
	t.Run("HandleResourceEventsONForVCPActivity_WhenResourceTypeIsVolume", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &ResourceEventsActivity{SE: mockSE}

		params := &common.HandleResourceEventParams{
			ResourceType: common.ResourceStateV1ResourceTypeVolume,
			ResourceId:   "test-resource-id",
		}

		mockSE.On("UpdateVolume", ctx, &datamodel.Volume{
			BaseModel:    datamodel.BaseModel{UUID: params.ResourceId},
			State:        coremodels.LifeCycleStateREADY,
			StateDetails: coremodels.LifeCycleStateAvailableDetails,
		}).Return(nil)

		result, err := activity.HandleResourceEventsONForVCPActivity(ctx, params)
		assert.True(tt, result)
		assert.Nil(tt, err)
	})

	t.Run("HandleResourceEventsONForVCPActivity_WhenResourceTypeIsUnsupported", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &ResourceEventsActivity{SE: mockSE}

		params := &common.HandleResourceEventParams{
			ResourceType: "unsupported-resource-type",
			ResourceId:   "test-resource-id",
		}

		result, err := activity.HandleResourceEventsONForVCPActivity(ctx, params)
		assert.False(tt, result)
		assert.NotNil(tt, err)
		assert.ErrorContains(tt, err, "unsupported resource type")
	})

	t.Run("HandleResourceEventsONForVCPActivity_WhenResourceTypeIsSnapshot", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &ResourceEventsActivity{SE: mockSE}

		params := &common.HandleResourceEventParams{
			ResourceType: common.ResourceStateV1ResourceTypeSnapshot,
			ResourceId:   "test-snapshot-id",
		}

		mockSE.On("UpdateSnapshotForHandleResource", ctx, &datamodel.Snapshot{
			BaseModel:    datamodel.BaseModel{UUID: params.ResourceId},
			State:        coremodels.LifeCycleStateREADY,
			StateDetails: coremodels.LifeCycleStateAvailableDetails,
		}).Return(nil, nil)

		result, err := activity.HandleResourceEventsONForVCPActivity(ctx, params)
		assert.True(tt, result)
		assert.Nil(tt, err)
	})

	t.Run("HandleResourceEventsONForVCPActivity_WhenResourceTypeIsStoragePool", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &ResourceEventsActivity{SE: mockSE}

		params := &common.HandleResourceEventParams{
			ResourceType:  common.ResourceStateV1ResourceTypeStoragePool,
			ResourceId:    "test-storage-pool-id",
			ProjectNumber: "test-project-number",
		}

		mockAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID: 1,
			},
		}

		mockSE.On("GetAccount", ctx, "test-project-number").Return(mockAccount, nil)
		mockSE.On("GetPool", ctx, "test-storage-pool-id", int64(1)).Return(&datamodel.PoolView{}, nil)

		result, err := activity.HandleResourceEventsONForVCPActivity(ctx, params)
		assert.True(tt, result)
		assert.Nil(tt, err)
	})

	t.Run("HandleResourceEventsONForVCPActivity_WhenResourceTypeIsAD", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &ResourceEventsActivity{SE: mockSE}

		params := &common.HandleResourceEventParams{
			ResourceType: common.ResourceStateV1ResourceTypeAD,
			ResourceId:   "test-ad-id",
		}

		result, err := activity.HandleResourceEventsONForVCPActivity(ctx, params)
		assert.False(tt, result)
		assert.Nil(tt, err)
	})

	t.Run("HandleResourceEventsONForVCPActivity_WhenResourceTypeIsUnsupported", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &ResourceEventsActivity{SE: mockSE}

		params := &common.HandleResourceEventParams{
			ResourceType: "unsupported-resource-type",
			ResourceId:   "test-resource-id",
		}

		result, err := activity.HandleResourceEventsONForVCPActivity(ctx, params)
		assert.False(tt, result)
		assert.NotNil(tt, err)
		assert.ErrorContains(tt, err, "unsupported resource type")
	})

	t.Run("HandleResourceEventsONForVCPActivity_WhenUpdateFails", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &ResourceEventsActivity{SE: mockSE}

		params := &common.HandleResourceEventParams{
			ResourceType: common.ResourceStateV1ResourceTypeVolume,
			ResourceId:   "test-volume-id",
		}

		mockSE.On("UpdateVolume", ctx, &datamodel.Volume{
			BaseModel:    datamodel.BaseModel{UUID: params.ResourceId},
			State:        coremodels.LifeCycleStateREADY,
			StateDetails: coremodels.LifeCycleStateAvailableDetails,
		}).Return(errors.New("update failed"))

		result, err := activity.HandleResourceEventsONForVCPActivity(ctx, params)
		assert.False(tt, result)
		assert.NotNil(tt, err)
		assert.ErrorContains(tt, err, "update failed")
	})

	t.Run("HandleResourceEventsONForVCPActivity_WhenResourceTypeIsKmsConfig", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &ResourceEventsActivity{SE: mockSE}

		params := &common.HandleResourceEventParams{
			ResourceType: common.ResourceStateV1ResourceTypeKmsConfig,
			ResourceId:   "test-kms-config-id",
		}

		mockSE.On("UpdateKmsConfigStateForHandleResource", ctx, params.ResourceId, common.ResourceLifeCycleStateEnabledDetails, params.State).Return(nil, nil)

		result, err := activity.HandleResourceEventsONForVCPActivity(ctx, params)
		assert.True(tt, result)
		assert.Nil(tt, err)
	})
}

func TestCheckKmsConfigExistence(t *testing.T) {
	t.Run("KmsConfigExists", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &ResourceEventsActivity{SE: mockSE}

		params := &common.HandleResourceEventParams{
			ResourceId: "test-kms-config-id",
		}

		mockSE.On("GetKmsConfig", ctx, params.ResourceId).Return(&datamodel.KmsConfig{}, nil)

		result, err := activity.checkKmsConfigExistence(ctx, params)
		assert.True(tt, result)
		assert.Nil(tt, err)
	})

	t.Run("KmsConfigNotFound", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &ResourceEventsActivity{SE: mockSE}

		params := &common.HandleResourceEventParams{
			ResourceId: "test-kms-config-id",
		}

		mockSE.On("GetKmsConfig", ctx, params.ResourceId).Return(nil, errors.NewNotFoundErr("KmsConfig", nil))

		result, err := activity.checkKmsConfigExistence(ctx, params)
		assert.False(tt, result)
		assert.NotNil(tt, err)
		assert.ErrorContains(tt, err, "KmsConfig not found")
		assert.IsType(tt, &temporal.ApplicationError{}, err)
	})

	t.Run("KmsConfigUnexpectedError", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &ResourceEventsActivity{SE: mockSE}

		params := &common.HandleResourceEventParams{
			ResourceId: "test-kms-config-id",
		}

		mockSE.On("GetKmsConfig", ctx, params.ResourceId).Return(nil, errors.New("unexpected error"))

		result, err := activity.checkKmsConfigExistence(ctx, params)
		assert.False(tt, result)
		assert.NotNil(tt, err)
		assert.ErrorContains(tt, err, "unexpected error")
	})
}

func TestCheckStoragePoolExistence(t *testing.T) {
	t.Run("StoragePoolExists", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &ResourceEventsActivity{SE: mockSE}

		params := &common.HandleResourceEventParams{
			ResourceId:    "test-storage-pool-id",
			ProjectNumber: "test-project-number",
		}
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "test-project-number"}

		mockSE.On("GetAccount", ctx, params.ProjectNumber).Return(account, nil)
		mockSE.On("GetPool", ctx, params.ResourceId, account.ID).Return(&datamodel.PoolView{}, nil)

		result, err := activity.checkStoragePoolExistence(ctx, params)
		assert.True(tt, result)
		assert.Nil(tt, err)
	})

	t.Run("StoragePoolNotFound", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &ResourceEventsActivity{SE: mockSE}

		params := &common.HandleResourceEventParams{
			ResourceId:    "test-storage-pool-id",
			ProjectNumber: "test-project-number",
		}

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "test-project-number"}

		mockSE.On("GetAccount", ctx, params.ProjectNumber).Return(account, nil)
		mockSE.On("GetPool", ctx, params.ResourceId, account.ID).Return(nil, errors.NewNotFoundErr("Pool", nil))

		result, err := activity.checkStoragePoolExistence(ctx, params)
		assert.False(tt, result)
		assert.NotNil(tt, err)
		assert.ErrorContains(tt, err, "Pool not found")
		assert.IsType(tt, &temporal.ApplicationError{}, err)
	})

	t.Run("GetAccountFails", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &ResourceEventsActivity{SE: mockSE}

		params := &common.HandleResourceEventParams{
			ResourceId:    "test-storage-pool-id",
			ProjectNumber: "test-project-number",
		}

		mockSE.On("GetAccount", ctx, params.ProjectNumber).Return(nil, errors.New("account retrieval failed"))

		result, err := activity.checkStoragePoolExistence(ctx, params)
		assert.False(tt, result)
		assert.NotNil(tt, err)
		assert.ErrorContains(tt, err, "account retrieval failed")
	})

	t.Run("GetPoolUnexpectedError", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &ResourceEventsActivity{SE: mockSE}

		params := &common.HandleResourceEventParams{
			ResourceId:    "test-storage-pool-id",
			ProjectNumber: "test-project-number",
		}

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "test-project-number"}

		mockSE.On("GetAccount", ctx, params.ProjectNumber).Return(account, nil)
		mockSE.On("GetPool", ctx, params.ResourceId, account.ID).Return(nil, errors.New("unexpected error"))

		result, err := activity.checkStoragePoolExistence(ctx, params)
		assert.False(tt, result)
		assert.NotNil(tt, err)
		assert.ErrorContains(tt, err, "unexpected error")
	})
}

// Unit tests for checkSnapshotExistence
func TestCheckSnapshotExistence(t *testing.T) {
	t.Run("SnapshotExists", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &ResourceEventsActivity{SE: mockSE}

		params := &common.HandleResourceEventParams{
			ResourceId:       "test-snapshot-id",
			ParentResourceID: "test-volume-id",
			ProjectNumber:    "test-project-number",
		}

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "test-project-number"}
		volume := &datamodel.Volume{BaseModel: datamodel.BaseModel{ID: 2}}

		mockSE.On("GetAccount", ctx, params.ProjectNumber).Return(account, nil)
		mockSE.On("GetVolumeWithAccountID", ctx, params.ParentResourceID, account.ID).Return(volume, nil)
		mockSE.On("GetSnapshotByUUID", ctx, params.ResourceId, account.ID, volume.ID).Return(&datamodel.Snapshot{}, nil)

		result, err := activity.checkSnapshotExistence(ctx, params)
		assert.True(tt, result)
		assert.Nil(tt, err)
	})

	t.Run("SnapshotNotFound", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &ResourceEventsActivity{SE: mockSE}

		params := &common.HandleResourceEventParams{
			ResourceId:       "test-snapshot-id",
			ParentResourceID: "test-volume-id",
			ProjectNumber:    "test-project-number",
		}

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "test-project-number"}
		volume := &datamodel.Volume{BaseModel: datamodel.BaseModel{ID: 2}}

		mockSE.On("GetAccount", ctx, params.ProjectNumber).Return(account, nil)
		mockSE.On("GetVolumeWithAccountID", ctx, params.ParentResourceID, account.ID).Return(volume, nil)
		mockSE.On("GetSnapshotByUUID", ctx, params.ResourceId, account.ID, volume.ID).Return(nil, errors.NewNotFoundErr("Snapshot", nil))

		result, err := activity.checkSnapshotExistence(ctx, params)
		assert.False(tt, result)
		assert.NotNil(tt, err)
		assert.ErrorContains(tt, err, "Snapshot not found")
		assert.IsType(tt, &temporal.ApplicationError{}, err)
	})

	t.Run("ParentVolumeNotFound", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &ResourceEventsActivity{SE: mockSE}

		params := &common.HandleResourceEventParams{
			ResourceId:       "test-snapshot-id",
			ParentResourceID: "test-volume-id",
			ProjectNumber:    "test-project-number",
		}

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "test-project-number"}

		mockSE.On("GetAccount", ctx, params.ProjectNumber).Return(account, nil)
		mockSE.On("GetVolumeWithAccountID", ctx, params.ParentResourceID, account.ID).Return(nil, errors.NewNotFoundErr("Volume", nil))

		result, err := activity.checkSnapshotExistence(ctx, params)
		assert.False(tt, result)
		assert.NotNil(tt, err)
		assert.ErrorContains(tt, err, "Volume not found")
	})

	t.Run("UnexpectedErrorWhileRetrievingSnapshot", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &ResourceEventsActivity{SE: mockSE}

		params := &common.HandleResourceEventParams{
			ResourceId:       "test-snapshot-id",
			ParentResourceID: "test-volume-id",
			ProjectNumber:    "test-project-number",
		}

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "test-project-number"}
		volume := &datamodel.Volume{BaseModel: datamodel.BaseModel{ID: 2}}

		mockSE.On("GetAccount", ctx, params.ProjectNumber).Return(account, nil)
		mockSE.On("GetVolumeWithAccountID", ctx, params.ParentResourceID, account.ID).Return(volume, nil)
		mockSE.On("GetSnapshotByUUID", ctx, params.ResourceId, account.ID, volume.ID).Return(nil, errors.New("unexpected error"))

		result, err := activity.checkSnapshotExistence(ctx, params)
		assert.False(tt, result)
		assert.NotNil(tt, err)
		assert.ErrorContains(tt, err, "unexpected error")
	})
}

// Unit tests for checkVolumeExistence
func TestCheckVolumeExistence(t *testing.T) {
	t.Run("VolumeExists", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &ResourceEventsActivity{SE: mockSE}

		params := &common.HandleResourceEventParams{
			ResourceId:    "test-volume-id",
			ProjectNumber: "test-project-number",
		}

		mockSE.On("GetVolume", ctx, params.ResourceId).Return(&datamodel.Volume{}, nil)

		result, err := activity.checkVolumeExistence(ctx, params)
		assert.True(tt, result)
		assert.Nil(tt, err)
	})

	t.Run("VolumeNotFound", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &ResourceEventsActivity{SE: mockSE}

		params := &common.HandleResourceEventParams{
			ResourceId:    "test-volume-id",
			ProjectNumber: "test-project-number",
		}

		mockSE.On("GetVolume", ctx, params.ResourceId).Return(nil, errors.NewNotFoundErr("Volume", nil))

		result, err := activity.checkVolumeExistence(ctx, params)
		assert.False(tt, result)
		assert.NotNil(tt, err)
		assert.ErrorContains(tt, err, "Volume not found")
		assert.IsType(tt, &temporal.ApplicationError{}, err)
	})

	t.Run("UnexpectedErrorWhileRetrievingVolume", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &ResourceEventsActivity{SE: mockSE}

		params := &common.HandleResourceEventParams{
			ResourceId:    "test-volume-id",
			ProjectNumber: "test-project-number",
		}

		mockSE.On("GetVolume", ctx, params.ResourceId).Return(nil, errors.New("unexpected error"))

		result, err := activity.checkVolumeExistence(ctx, params)
		assert.False(tt, result)
		assert.NotNil(tt, err)
		assert.ErrorContains(tt, err, "unexpected error")
	})
}

func TestDeleteVolumeForPool(t *testing.T) {
	t.Run("DeleteVolumeForPool_Success", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &ResourceEventsActivity{SE: mockSE}

		params := &common.HandleResourceEventParams{
			ResourceId:    "test-volume-id",
			ProjectNumber: "test-project-number",
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				UUID: "test-pool-id",
				ID:   1,
			},
			AccountID: 1,
			State:     common.ResourceStateEnabled,
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: params.ResourceId,
				ID:   1,
			},
			AccountID: 1,
			PoolID:    pool.ID,
		}

		mockSE.On("DeleteVolumeAndChildResources", ctx, params.ResourceId).Return(&datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: params.ResourceId,
				ID:   1,
			},
			AccountID: 1,
			PoolID:    1,
			State:     common.ResourceStateEnabled,
		}, nil)

		err := activity.DeleteVolumeForPool(ctx, volume)
		assert.Nil(tt, err)
	})

	t.Run("DeleteVolumeForPool_DeleteVolumeAndChildResourcesFails", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &ResourceEventsActivity{SE: mockSE}

		params := &common.HandleResourceEventParams{
			ResourceId:    "test-volume-id",
			ProjectNumber: "test-project-number",
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				UUID: "test-pool-id",
				ID:   1,
			},
			AccountID: 1,
			State:     common.ResourceStateEnabled,
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: params.ResourceId,
				ID:   1,
			},
			AccountID: 1,
			PoolID:    pool.ID,
		}

		mockSE.On("DeleteVolumeAndChildResources", ctx, params.ResourceId).Return(nil, errors.New("deletion failed"))

		err := activity.DeleteVolumeForPool(ctx, volume)
		assert.NotNil(tt, err)
		assert.ErrorContains(tt, err, "deletion failed")
	})
}

func TestDeleteReplicationsForVolume(t *testing.T) {
	t.Run("DeleteReplicationsForVolume_Success", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &ResourceEventsActivity{SE: mockSE}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "test-volume-id",
				ID:   1,
			},
			AccountID: 1,
			PoolID:    1,
			State:     common.ResourceStateEnabled,
		}

		volumeReplication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{
				UUID: "test-replication-id",
				ID:   1,
			},
			VolumeID:  volume.ID,
			AccountID: volume.AccountID,
			State:     common.ResourceStateEnabled,
		}

		filter := dbutils.CreateFilterWithConditions(
			dbutils.NewFilterCondition("account_id", "=", volume.AccountID),
			dbutils.NewFilterCondition("volume_id", "=", volume.ID))

		mockSE.On("ListVolumeReplications", ctx, *filter).Return([]*datamodel.VolumeReplication{volumeReplication}, nil)
		mockSE.On("DeleteVolumeReplication", ctx, volumeReplication).Return(volumeReplication, nil)

		err := activity.DeleteReplicationsForVolume(ctx, volume)
		assert.Nil(tt, err)
	})

	t.Run("DeleteReplicationsForVolume_ListVolumeReplicationsFails", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &ResourceEventsActivity{SE: mockSE}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "test-volume-id",
				ID:   1,
			},
			AccountID: 1,
			PoolID:    1,
			State:     common.ResourceStateEnabled,
		}

		filter := dbutils.CreateFilterWithConditions(
			dbutils.NewFilterCondition("account_id", "=", volume.AccountID),
			dbutils.NewFilterCondition("volume_id", "=", volume.ID))

		mockSE.On("ListVolumeReplications", ctx, *filter).Return(nil, errors.New("listing failed"))

		err := activity.DeleteReplicationsForVolume(ctx, volume)
		assert.NotNil(tt, err)
		assert.ErrorContains(tt, err, "listing failed")
	})

	t.Run("DeleteReplicationsForVolume_DeleteVolumeReplicationFails", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &ResourceEventsActivity{SE: mockSE}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "test-volume-id",
				ID:   1,
			},
			AccountID: 1,
			PoolID:    1,
			State:     common.ResourceStateEnabled,
		}

		volumeReplication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{
				UUID: "test-replication-id",
				ID:   1,
			},
			VolumeID:  volume.ID,
			AccountID: volume.AccountID,
			State:     common.ResourceStateEnabled,
		}

		filter := dbutils.CreateFilterWithConditions(
			dbutils.NewFilterCondition("account_id", "=", volume.AccountID),
			dbutils.NewFilterCondition("volume_id", "=", volume.ID))

		mockSE.On("ListVolumeReplications", ctx, *filter).Return([]*datamodel.VolumeReplication{volumeReplication}, nil)
		mockSE.On("DeleteVolumeReplication", ctx, volumeReplication).Return(nil, errors.New("deletion failed"))

		err := activity.DeleteReplicationsForVolume(ctx, volume)
		assert.NotNil(tt, err)
		assert.ErrorContains(tt, err, "deletion failed")
	})
}

// Unit tests for checkBackupPolicyExistence
func TestCheckBackupPolicyExistence(t *testing.T) {
	t.Run("BackupPolicyExists", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &ResourceEventsActivity{SE: mockSE}

		params := &common.HandleResourceEventParams{
			ResourceId:    "test-backup-policy-id",
			ProjectNumber: "test-project-number",
		}

		mockAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 123},
		}
		mockBackupPolicy := &datamodel.BackupPolicy{
			BaseModel: datamodel.BaseModel{UUID: "test-backup-policy-id"},
		}

		mockSE.On("GetAccount", ctx, params.ProjectNumber).Return(mockAccount, nil)
		mockSE.On("GetBackupPolicyByUUIDAndOwnerID", ctx, params.ResourceId, mockAccount.ID).Return(mockBackupPolicy, nil)

		result, err := activity.checkBackupPolicyExistence(ctx, params)
		assert.True(tt, result)
		assert.Nil(tt, err)
	})

	t.Run("BackupPolicyNotFound", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &ResourceEventsActivity{SE: mockSE}

		params := &common.HandleResourceEventParams{
			ResourceId:    "test-backup-policy-id",
			ProjectNumber: "test-project-number",
		}

		mockAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 123},
		}

		mockSE.On("GetAccount", ctx, params.ProjectNumber).Return(mockAccount, nil)
		mockSE.On("GetBackupPolicyByUUIDAndOwnerID", ctx, params.ResourceId, mockAccount.ID).Return(nil, errors.NewNotFoundErr("BackupPolicy", nil))

		result, err := activity.checkBackupPolicyExistence(ctx, params)
		assert.False(tt, result)
		assert.NotNil(tt, err)
		assert.ErrorContains(tt, err, "BackupPolicy not found")
		assert.IsType(tt, &temporal.ApplicationError{}, err)
	})

	t.Run("GetAccountFails", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &ResourceEventsActivity{SE: mockSE}

		params := &common.HandleResourceEventParams{
			ResourceId:    "test-backup-policy-id",
			ProjectNumber: "test-project-number",
		}

		mockSE.On("GetAccount", ctx, params.ProjectNumber).Return(nil, errors.New("account error"))

		result, err := activity.checkBackupPolicyExistence(ctx, params)
		assert.False(tt, result)
		assert.NotNil(tt, err)
		assert.ErrorContains(tt, err, "account error")
	})

	t.Run("UnexpectedErrorWhileRetrievingBackupPolicy", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		activity := &ResourceEventsActivity{SE: mockSE}

		params := &common.HandleResourceEventParams{
			ResourceId:    "test-backup-policy-id",
			ProjectNumber: "test-project-number",
		}

		mockAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 123},
		}

		mockSE.On("GetAccount", ctx, params.ProjectNumber).Return(mockAccount, nil)
		mockSE.On("GetBackupPolicyByUUIDAndOwnerID", ctx, params.ResourceId, mockAccount.ID).Return(nil, errors.New("unexpected error"))

		result, err := activity.checkBackupPolicyExistence(ctx, params)
		assert.False(tt, result)
		assert.NotNil(tt, err)
		assert.ErrorContains(tt, err, "unexpected error")
	})
}

func Test_HandleBackupPolicyResourceEvent(t *testing.T) {
	t.Run("HandleBackupPolicyResourceEventOFF_PausesSchedule", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		mockScheduler := scheduler.NewMockScheduler(tt)
		activity := &ResourceEventsActivity{SE: mockSE, Scheduler: mockScheduler}

		params := &common.HandleResourceEventParams{
			ResourceType:  common.ResourceStateV1ResourceTypeBackupPolicy,
			ResourceId:    "test-backup-policy-id",
			ProjectNumber: "test-project-number",
		}

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "test-account"}
		backupPolicy := &datamodel.BackupPolicy{
			BaseModel:     datamodel.BaseModel{UUID: "test-backup-policy-id"},
			PolicyEnabled: true,
		}

		mockSE.On("GetAccount", ctx, params.ProjectNumber).Return(account, nil)
		mockSE.On("GetBackupPolicyByUUIDAndOwnerID", ctx, params.ResourceId, account.ID).Return(backupPolicy, nil)

		// Mock the scheduler describe call to return that it's currently not paused
		mockScheduler.On("Describe", ctx, mock.AnythingOfType("scheduler.DescribeScheduleParams")).Return(&scheduler.ScheduleDescription{Paused: false}, nil)
		// Mock the scheduler pause call
		mockScheduler.On("Pause", ctx, mock.AnythingOfType("scheduler.PauseScheduleParams")).Return(&scheduler.ScheduleResponse{}, nil)

		result, err := activity.handleBackupPolicy(ctx, params, coremodels.LifeCycleStateDisabled, coremodels.LifeCycleStateDisabledDetails)
		assert.True(tt, result)
		assert.Nil(tt, err)
		mockScheduler.AssertExpectations(tt)
	})

	t.Run("HandleBackupPolicyResourceEventON_UnpausesScheduleWhenEnabled", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		mockScheduler := scheduler.NewMockScheduler(tt)
		activity := &ResourceEventsActivity{SE: mockSE, Scheduler: mockScheduler}

		params := &common.HandleResourceEventParams{
			ResourceType:  common.ResourceStateV1ResourceTypeBackupPolicy,
			ResourceId:    "test-backup-policy-id",
			ProjectNumber: "test-project-number",
		}

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "test-account"}
		backupPolicy := &datamodel.BackupPolicy{
			BaseModel:     datamodel.BaseModel{UUID: "test-backup-policy-id"},
			PolicyEnabled: true,
		}

		mockSE.On("GetAccount", ctx, params.ProjectNumber).Return(account, nil)
		mockSE.On("GetBackupPolicyByUUIDAndOwnerID", ctx, params.ResourceId, account.ID).Return(backupPolicy, nil)

		// Mock the scheduler describe call to return that it's currently paused
		mockScheduler.On("Describe", ctx, mock.AnythingOfType("scheduler.DescribeScheduleParams")).Return(&scheduler.ScheduleDescription{Paused: true}, nil)
		// Mock the scheduler unpause call
		mockScheduler.On("Unpause", ctx, mock.AnythingOfType("scheduler.UnpauseScheduleParams")).Return(&scheduler.ScheduleResponse{}, nil)

		result, err := activity.handleBackupPolicy(ctx, params, coremodels.LifeCycleStateREADY, coremodels.LifeCycleStateAvailableDetails)
		assert.True(tt, result)
		assert.Nil(tt, err)
		mockScheduler.AssertExpectations(tt)
	})

	t.Run("HandleBackupPolicyResourceEventON_SkipsUnpauseWhenDisabled", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		mockScheduler := scheduler.NewMockScheduler(tt)
		activity := &ResourceEventsActivity{SE: mockSE, Scheduler: mockScheduler}

		params := &common.HandleResourceEventParams{
			ResourceType:  common.ResourceStateV1ResourceTypeBackupPolicy,
			ResourceId:    "test-backup-policy-id",
			ProjectNumber: "test-project-number",
		}

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "test-account"}
		backupPolicy := &datamodel.BackupPolicy{
			BaseModel:     datamodel.BaseModel{UUID: "test-backup-policy-id"},
			PolicyEnabled: false, // Policy is disabled in DB
		}

		mockSE.On("GetAccount", ctx, params.ProjectNumber).Return(account, nil)
		mockSE.On("GetBackupPolicyByUUIDAndOwnerID", ctx, params.ResourceId, account.ID).Return(backupPolicy, nil)

		// Scheduler unpause should NOT be called - no mock setup for Unpause

		result, err := activity.handleBackupPolicy(ctx, params, coremodels.LifeCycleStateREADY, coremodels.LifeCycleStateAvailableDetails)
		assert.True(tt, result)
		assert.Nil(tt, err)
		mockScheduler.AssertExpectations(tt) // Should pass because Unpause was not called
	})

	t.Run("HandleBackupPolicyResourceEventOFF_SkipsPauseWhenDisabled", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		mockScheduler := scheduler.NewMockScheduler(tt)
		activity := &ResourceEventsActivity{SE: mockSE, Scheduler: mockScheduler}

		params := &common.HandleResourceEventParams{
			ResourceType:  common.ResourceStateV1ResourceTypeBackupPolicy,
			ResourceId:    "test-backup-policy-id",
			ProjectNumber: "test-project-number",
		}

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "test-account"}
		backupPolicy := &datamodel.BackupPolicy{
			BaseModel:     datamodel.BaseModel{UUID: "test-backup-policy-id"},
			PolicyEnabled: false, // Policy is disabled in DB
		}

		mockSE.On("GetAccount", ctx, params.ProjectNumber).Return(account, nil)
		mockSE.On("GetBackupPolicyByUUIDAndOwnerID", ctx, params.ResourceId, account.ID).Return(backupPolicy, nil)

		// Scheduler pause should NOT be called - no mock setup for Pause

		result, err := activity.handleBackupPolicy(ctx, params, coremodels.LifeCycleStateDisabled, coremodels.LifeCycleStateDisabledDetails)
		assert.True(tt, result)
		assert.Nil(tt, err)
		mockScheduler.AssertExpectations(tt) // Should pass because Pause was not called
	})

	t.Run("HandleBackupPolicyResourceEventOFF_SkipsPauseWhenAlreadyPaused", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		mockScheduler := scheduler.NewMockScheduler(tt)
		activity := &ResourceEventsActivity{SE: mockSE, Scheduler: mockScheduler}

		params := &common.HandleResourceEventParams{
			ResourceType:  common.ResourceStateV1ResourceTypeBackupPolicy,
			ResourceId:    "test-backup-policy-id",
			ProjectNumber: "test-project-number",
		}

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "test-account"}
		backupPolicy := &datamodel.BackupPolicy{
			BaseModel:     datamodel.BaseModel{UUID: "test-backup-policy-id"},
			PolicyEnabled: true, // Policy is enabled in DB
		}

		mockSE.On("GetAccount", ctx, params.ProjectNumber).Return(account, nil)
		mockSE.On("GetBackupPolicyByUUIDAndOwnerID", ctx, params.ResourceId, account.ID).Return(backupPolicy, nil)

		// Mock the scheduler describe call to return that it's already paused
		mockScheduler.On("Describe", ctx, mock.AnythingOfType("scheduler.DescribeScheduleParams")).Return(&scheduler.ScheduleDescription{Paused: true}, nil)
		// Scheduler pause should NOT be called because it's already paused

		result, err := activity.handleBackupPolicy(ctx, params, coremodels.LifeCycleStateDisabled, coremodels.LifeCycleStateDisabledDetails)
		assert.True(tt, result)
		assert.Nil(tt, err)
		mockScheduler.AssertExpectations(tt)
	})

	t.Run("HandleBackupPolicyResourceEventON_SkipsUnpauseWhenAlreadyActive", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		mockScheduler := scheduler.NewMockScheduler(tt)
		activity := &ResourceEventsActivity{SE: mockSE, Scheduler: mockScheduler}

		params := &common.HandleResourceEventParams{
			ResourceType:  common.ResourceStateV1ResourceTypeBackupPolicy,
			ResourceId:    "test-backup-policy-id",
			ProjectNumber: "test-project-number",
		}

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "test-account"}
		backupPolicy := &datamodel.BackupPolicy{
			BaseModel:     datamodel.BaseModel{UUID: "test-backup-policy-id"},
			PolicyEnabled: true, // Policy is enabled in DB
		}

		mockSE.On("GetAccount", ctx, params.ProjectNumber).Return(account, nil)
		mockSE.On("GetBackupPolicyByUUIDAndOwnerID", ctx, params.ResourceId, account.ID).Return(backupPolicy, nil)

		// Mock the scheduler describe call to return that it's already active (not paused)
		mockScheduler.On("Describe", ctx, mock.AnythingOfType("scheduler.DescribeScheduleParams")).Return(&scheduler.ScheduleDescription{Paused: false}, nil)
		// Scheduler unpause should NOT be called because it's already active

		result, err := activity.handleBackupPolicy(ctx, params, coremodels.LifeCycleStateREADY, coremodels.LifeCycleStateAvailableDetails)
		assert.True(tt, result)
		assert.Nil(tt, err)
		mockScheduler.AssertExpectations(tt)
	})
}

func Test_HandleResourceEventsOFFForVCPActivity_UnsupportedType(t *testing.T) {
	ctx := context.Background()
	mockSE := database.NewMockStorage(t)
	activity := &ResourceEventsActivity{SE: mockSE}

	params := &common.HandleResourceEventParams{
		ResourceType: "UnsupportedType",
	}

	result, err := activity.HandleResourceEventsOFFForVCPActivity(ctx, params)
	assert.False(t, result)
	assert.NotNil(t, err)
	assert.ErrorContains(t, err, "unsupported resource type")
}

func Test_HandleResourceEventsONForVCPActivity_UnsupportedType(t *testing.T) {
	ctx := context.Background()
	mockSE := database.NewMockStorage(t)
	activity := &ResourceEventsActivity{SE: mockSE}

	params := &common.HandleResourceEventParams{
		ResourceType: "UnsupportedType",
	}

	result, err := activity.HandleResourceEventsONForVCPActivity(ctx, params)
	assert.False(t, result)
	assert.NotNil(t, err)
	assert.ErrorContains(t, err, "unsupported resource type")
}

func Test_HandleBackupPolicyErrorCases(t *testing.T) {
	t.Run("GetAccountFails", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		mockScheduler := scheduler.NewMockScheduler(tt)
		activity := &ResourceEventsActivity{SE: mockSE, Scheduler: mockScheduler}

		params := &common.HandleResourceEventParams{
			ResourceId:    "test-backup-policy-id",
			ProjectNumber: "test-project-number",
		}

		mockSE.On("GetAccount", ctx, params.ProjectNumber).Return(nil, errors.New("account error"))

		result, err := activity.handleBackupPolicy(ctx, params, coremodels.LifeCycleStateDisabled, coremodels.LifeCycleStateDisabledDetails)
		assert.False(tt, result)
		assert.NotNil(tt, err)
		assert.ErrorContains(tt, err, "account error")
	})

	t.Run("GetBackupPolicyFails", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		mockScheduler := scheduler.NewMockScheduler(tt)
		activity := &ResourceEventsActivity{SE: mockSE, Scheduler: mockScheduler}

		params := &common.HandleResourceEventParams{
			ResourceId:    "test-backup-policy-id",
			ProjectNumber: "test-project-number",
		}

		mockAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 123},
		}

		mockSE.On("GetAccount", ctx, params.ProjectNumber).Return(mockAccount, nil)
		mockSE.On("GetBackupPolicyByUUIDAndOwnerID", ctx, params.ResourceId, mockAccount.ID).Return(nil, errors.New("backup policy error"))

		result, err := activity.handleBackupPolicy(ctx, params, coremodels.LifeCycleStateDisabled, coremodels.LifeCycleStateDisabledDetails)
		assert.False(tt, result)
		assert.NotNil(tt, err)
		assert.ErrorContains(tt, err, "backup policy error")
	})

	t.Run("GetBackupPolicyNotFound", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		mockScheduler := scheduler.NewMockScheduler(tt)
		activity := &ResourceEventsActivity{SE: mockSE, Scheduler: mockScheduler}

		params := &common.HandleResourceEventParams{
			ResourceId:    "test-backup-policy-id",
			ProjectNumber: "test-project-number",
		}

		mockAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 123},
		}

		mockSE.On("GetAccount", ctx, params.ProjectNumber).Return(mockAccount, nil)
		mockSE.On("GetBackupPolicyByUUIDAndOwnerID", ctx, params.ResourceId, mockAccount.ID).Return(nil, errors.NewNotFoundErr("BackupPolicy", nil))

		result, err := activity.handleBackupPolicy(ctx, params, coremodels.LifeCycleStateDisabled, coremodels.LifeCycleStateDisabledDetails)
		assert.False(tt, result)
		assert.NotNil(tt, err)
		assert.True(tt, errors.IsNotFoundErr(err))
	})

	t.Run("PauseBackupPolicyScheduleFails", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		mockScheduler := scheduler.NewMockScheduler(tt)
		activity := &ResourceEventsActivity{SE: mockSE, Scheduler: mockScheduler}

		params := &common.HandleResourceEventParams{
			ResourceId:    "test-backup-policy-id",
			ProjectNumber: "test-project-number",
		}

		mockAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 123},
		}
		mockBackupPolicy := &datamodel.BackupPolicy{
			BaseModel:     datamodel.BaseModel{UUID: "test-backup-policy-id"},
			PolicyEnabled: true,
		}

		mockSE.On("GetAccount", ctx, params.ProjectNumber).Return(mockAccount, nil)
		mockSE.On("GetBackupPolicyByUUIDAndOwnerID", ctx, params.ResourceId, mockAccount.ID).Return(mockBackupPolicy, nil)

		// Mock scheduler calls for pause operation
		mockScheduler.On("Describe", ctx, mock.Anything).Return(&scheduler.ScheduleDescription{Paused: false}, nil)
		mockScheduler.On("Pause", ctx, mock.Anything).Return(nil, errors.New("pause failed"))

		result, err := activity.handleBackupPolicy(ctx, params, coremodels.LifeCycleStateDisabled, coremodels.LifeCycleStateDisabledDetails)
		assert.False(tt, result)
		assert.NotNil(tt, err)
		assert.ErrorContains(tt, err, "pause failed")
	})

	t.Run("UnpauseBackupPolicyScheduleFails", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		mockScheduler := scheduler.NewMockScheduler(tt)
		activity := &ResourceEventsActivity{SE: mockSE, Scheduler: mockScheduler}

		params := &common.HandleResourceEventParams{
			ResourceId:    "test-backup-policy-id",
			ProjectNumber: "test-project-number",
		}

		mockAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 123},
		}
		mockBackupPolicy := &datamodel.BackupPolicy{
			BaseModel:     datamodel.BaseModel{UUID: "test-backup-policy-id"},
			PolicyEnabled: true,
		}

		mockSE.On("GetAccount", ctx, params.ProjectNumber).Return(mockAccount, nil)
		mockSE.On("GetBackupPolicyByUUIDAndOwnerID", ctx, params.ResourceId, mockAccount.ID).Return(mockBackupPolicy, nil)

		// Mock scheduler calls for unpause operation
		mockScheduler.On("Describe", ctx, mock.Anything).Return(&scheduler.ScheduleDescription{Paused: true}, nil)
		mockScheduler.On("Unpause", ctx, mock.Anything).Return(nil, errors.New("unpause failed"))

		result, err := activity.handleBackupPolicy(ctx, params, coremodels.LifeCycleStateREADY, coremodels.LifeCycleStateAvailableDetails)
		assert.False(tt, result)
		assert.NotNil(tt, err)
		assert.ErrorContains(tt, err, "unpause failed")
	})

	t.Run("UnknownState", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		mockScheduler := scheduler.NewMockScheduler(tt)
		activity := &ResourceEventsActivity{SE: mockSE, Scheduler: mockScheduler}

		params := &common.HandleResourceEventParams{
			ResourceId:    "test-backup-policy-id",
			ProjectNumber: "test-project-number",
		}

		mockAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 123},
		}
		mockBackupPolicy := &datamodel.BackupPolicy{
			BaseModel:     datamodel.BaseModel{UUID: "test-backup-policy-id"},
			PolicyEnabled: true,
		}

		mockSE.On("GetAccount", ctx, params.ProjectNumber).Return(mockAccount, nil)
		mockSE.On("GetBackupPolicyByUUIDAndOwnerID", ctx, params.ResourceId, mockAccount.ID).Return(mockBackupPolicy, nil)

		result, err := activity.handleBackupPolicy(ctx, params, "unknown-state", "unknown-details")
		assert.False(tt, result)
		assert.NotNil(tt, err)
		assert.ErrorContains(tt, err, "unknown state for backup policy resource event")
	})

	t.Run("BackupPolicyDisabledInDatabase_SkipPause", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		mockScheduler := scheduler.NewMockScheduler(tt)
		activity := &ResourceEventsActivity{SE: mockSE, Scheduler: mockScheduler}

		params := &common.HandleResourceEventParams{
			ResourceId:    "test-backup-policy-id",
			ProjectNumber: "test-project-number",
		}

		mockAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 123},
		}
		mockBackupPolicy := &datamodel.BackupPolicy{
			BaseModel:     datamodel.BaseModel{UUID: "test-backup-policy-id"},
			PolicyEnabled: false, // Disabled in database
		}

		mockSE.On("GetAccount", ctx, params.ProjectNumber).Return(mockAccount, nil)
		mockSE.On("GetBackupPolicyByUUIDAndOwnerID", ctx, params.ResourceId, mockAccount.ID).Return(mockBackupPolicy, nil)

		result, err := activity.handleBackupPolicy(ctx, params, coremodels.LifeCycleStateDisabled, coremodels.LifeCycleStateDisabledDetails)
		assert.True(tt, result)
		assert.Nil(tt, err)
	})

	t.Run("BackupPolicyDisabledInDatabase_SkipUnpause", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(tt)
		mockScheduler := scheduler.NewMockScheduler(tt)
		activity := &ResourceEventsActivity{SE: mockSE, Scheduler: mockScheduler}

		params := &common.HandleResourceEventParams{
			ResourceId:    "test-backup-policy-id",
			ProjectNumber: "test-project-number",
		}

		mockAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 123},
		}
		mockBackupPolicy := &datamodel.BackupPolicy{
			BaseModel:     datamodel.BaseModel{UUID: "test-backup-policy-id"},
			PolicyEnabled: false, // Disabled in database
		}

		mockSE.On("GetAccount", ctx, params.ProjectNumber).Return(mockAccount, nil)
		mockSE.On("GetBackupPolicyByUUIDAndOwnerID", ctx, params.ResourceId, mockAccount.ID).Return(mockBackupPolicy, nil)

		result, err := activity.handleBackupPolicy(ctx, params, coremodels.LifeCycleStateREADY, coremodels.LifeCycleStateAvailableDetails)
		assert.True(tt, result)
		assert.Nil(tt, err)
	})
}

func Test_HandleResourceEventsForSDEActivity_TooManyRequests(t *testing.T) {
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

	params := &common.HandleResourceEventParams{
		State:          coremodels.StateOff,
		LocationId:     "test-location-id",
		ProjectNumber:  "test-project-number",
		XCorrelationID: "test-correlation-id",
		ResourceType:   common.ResourceStateV1ResourceTypeVolume,
		ResourceId:     "test-volume-id",
	}

	tooMany := &resource_events.V1betaResourceStateUpdateTooManyRequests{}
	mockClient.EXPECT().V1betaResourceStateUpdate(mock.Anything).Return(nil, nil, nil, tooMany)

	activity := &ResourceEventsActivity{SE: mockSE}
	_, err := activity.HandleResourceEventsForSDEActivity(ctx, params)
	assert.NotNil(t, err)
	var appErr *temporal.ApplicationError
	assert.True(t, errors2.As(err, &appErr))
	// Wrapped as CustomError (tracking id for ErrCVPClientHandleResourceEventError)
	assert.Equal(t, "CustomError", appErr.Type())
}

func Test_PollHandleResourceEventSDEOperationActivity_TooManyRequests(t *testing.T) {
	ctx := context.Background()
	mockSE := database.NewMockStorage(t)
	mockAsync := &async.MockClientService{} // not used because we short-circuit via stubbed PollCvpOperationForWorkflow
	mockCVP := &cvpapi.Cvp{Async: mockAsync}
	originalCreateClient := cvp.CreateClient
	defer func() { createClient = originalCreateClient }()
	createClient = func(logger log.Logger, JWT string) cvpapi.Cvp { return *mockCVP }

	originalToken := auth.GetSignedJwtToken
	defer func() { getSignedToken = originalToken }()
	getSignedToken = func(projectNumber string) (string, error) { return "test-jwt-token", nil }

	originalPoll := PollCvpOperationForWorkflow
	defer func() { PollCvpOperationForWorkflow = originalPoll }()
	PollCvpOperationForWorkflow = func(ctx context.Context, client cvpapi.Cvp, params *async.V1betaDescribeOperationParams) (*cvpmodels.OperationV1beta, error) {
		return nil, &resource_events.V1betaResourceStateUpdateTooManyRequests{}
	}

	params := &common.HandleResourceEventParams{
		State:          coremodels.StateOff,
		LocationId:     "test-location-id",
		ProjectNumber:  "test-project-number",
		XCorrelationID: "test-correlation-id",
	}
	result := &common.HandleResourceEventResult{Done: nillable.GetBoolPtr(false), Name: nillable.GetStringPtr("operations/test-op")}

	activity := &ResourceEventsActivity{SE: mockSE}
	err := activity.PollHandleResourceEventSDEOperationActivity(ctx, params, result)
	assert.NotNil(t, err)
	var appErr *temporal.ApplicationError
	assert.True(t, errors2.As(err, &appErr))
	assert.Equal(t, "CustomError", appErr.Type())
}

func Test_handleKmsConfig_UserInputValidationErr(t *testing.T) {
	ctx := context.Background()
	mockSE := database.NewMockStorage(t)
	activity := &ResourceEventsActivity{SE: mockSE}

	params := &common.HandleResourceEventParams{ResourceId: "kms-id", State: coremodels.StateOff}

	mockSE.On("UpdateKmsConfigStateForHandleResource", ctx, params.ResourceId, coremodels.LifeCycleStateDisabledDetails, coremodels.StateOff).Return(nil, errors.NewUserInputValidationErr("invalid input"))

	ok, err := activity.handleKmsConfig(ctx, params, coremodels.LifeCycleStateDisabledDetails)
	assert.False(t, ok)
	assert.NotNil(t, err)
	var appErr *temporal.ApplicationError
	assert.True(t, errors2.As(err, &appErr))
	assert.True(t, appErr.NonRetryable())
	assert.Equal(t, ErrTypeResourceNotFound, appErr.Type())
}
