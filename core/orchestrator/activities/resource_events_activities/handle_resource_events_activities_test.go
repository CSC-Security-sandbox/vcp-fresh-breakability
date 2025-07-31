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
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	errors2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"go.temporal.io/sdk/temporal"
	"testing"
)

func Test_HandleResourceEventForSDEActivity(t *testing.T) {
	t.Run("HandleResourceEventCheckForVCPActivity_WhenResourceTypeIsKmsConfig", func(tt *testing.T) {
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
			State:          models.StateOff,
			LocationID:     "test-location-id",
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
			Payload: &models2.OperationV1beta{
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
			State:          models.StateOff,
			LocationID:     "test-location-id",
			ProjectNumber:  "test-project-number",
			XCorrelationID: "test-correlation-id",
		}

		accepted := &resource_events.V1betaResourceStateUpdateAccepted{
			Payload: &models2.OperationV1beta{
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
			State:          models.StateOff,
			LocationID:     "test-location-id",
			ProjectNumber:  "test-project-number",
			XCorrelationID: "test-correlation-id",
		}

		errMsg := "Client not available"
		mockClient.EXPECT().V1betaResourceStateUpdate(mock.Anything).Return(nil, nil, nil, errors.New(errMsg))

		activity := &ResourceEventsActivity{SE: mockSE}
		_, err := activity.HandleResourceEventsForSDEActivity(ctx, params)
		assert.NotNil(tt, err)
		assert.ErrorContains(tt, err, errMsg)

		var applicationError *temporal.ApplicationError
		assert.True(tt, errors2.As(err, &applicationError))
		assert.False(tt, applicationError.NonRetryable())
		assert.Equal(tt, "CustomError", applicationError.Type())
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
			State:          models.StateOff,
			LocationID:     "test-location-id",
			ProjectNumber:  "test-project-number",
			XCorrelationID: "test-correlation-id",
		}

		mockClient.EXPECT().V1betaResourceStateUpdate(mock.Anything).Return(nil, nil, nil, nil)

		activity := &ResourceEventsActivity{SE: mockSE}
		res, err := activity.HandleResourceEventsForSDEActivity(ctx, params)
		assert.NoError(tt, err)
		assert.Nil(tt, res)
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
			State:          models.StateOff,
			LocationID:     "test-location-id",
			ProjectNumber:  "test-project-number",
			XCorrelationID: "test-correlation-id",
		}
		result := &common.HandleResourceEventResult{
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
			State:          models.StateOff,
			LocationID:     "test-location-id",
			ProjectNumber:  "test-project-number",
			XCorrelationID: "test-correlation-id",
		}
		result := &common.HandleResourceEventResult{
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

		activity := &ResourceEventsActivity{SE: mockSE}
		err := activity.PollHandleResourceEventSDEOperationActivity(ctx, params, result)
		var applicationError *temporal.ApplicationError
		assert.NotNil(tt, err)
		assert.True(tt, errors2.As(err, &applicationError))
		assert.True(tt, applicationError.NonRetryable())
		assert.Equal(tt, "CustomError", applicationError.Type())
		assert.ErrorContains(tt, err, "Internal Server Error")
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
			State:          models.StateOff,
			LocationID:     "test-location-id",
			ProjectNumber:  "test-project-number",
			XCorrelationID: "test-correlation-id",
		}
		result := &common.HandleResourceEventResult{
			Done: nillable.GetBoolPtr(false),
			Name: nillable.GetStringPtr("test-operation-name"),
		}

		response := &async.V1betaDescribeOperationOK{
			Payload: &models2.OperationV1beta{
				Name: "test-operation-name",
			},
		}
		mockAsync.EXPECT().V1betaDescribeOperation(mock.Anything).Return(response, nil)

		activity := &ResourceEventsActivity{SE: mockSE}
		err := activity.PollHandleResourceEventSDEOperationActivity(ctx, params, result)
		assert.NotNil(tt, err)
		assert.ErrorContains(tt, err, "job not finished")
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
			State:          models.StateOff,
			LocationID:     "test-location-id",
			ProjectNumber:  "test-project-number",
			XCorrelationID: "test-correlation-id",
		}
		result := &common.HandleResourceEventResult{
			Done: nillable.GetBoolPtr(false),
			Name: nillable.GetStringPtr("test-operation-name"),
		}

		mockAsync.EXPECT().V1betaDescribeOperation(mock.Anything).Return(nil, errors.New("Client not available"))
		errMsg := "Client not available"

		activity := &ResourceEventsActivity{SE: mockSE}
		err := activity.PollHandleResourceEventSDEOperationActivity(ctx, params, result)
		assert.NotNil(tt, err)
		assert.ErrorContains(tt, err, errMsg)

		var applicationError *temporal.ApplicationError
		assert.True(tt, errors2.As(err, &applicationError))
		assert.False(tt, applicationError.NonRetryable())
		assert.Equal(tt, "CustomError", applicationError.Type())
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
			State:          models.StateOff,
			LocationID:     "test-location-id",
			ProjectNumber:  "test-project-number",
			XCorrelationID: "test-correlation-id",
		}
		result := &common.HandleResourceEventResult{
			Done: nillable.GetBoolPtr(false),
		}

		activity := &ResourceEventsActivity{SE: mockSE}
		err := activity.PollHandleResourceEventSDEOperationActivity(ctx, params, result)
		assert.NotNil(tt, err)
		assert.ErrorContains(tt, err, "operation name is nil")

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
			State:          models.StateOff,
			LocationID:     "test-location-id",
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
		}

		mockSE.On("UpdateKmsConfigState", ctx, params.ResourceId, common.ResourceStateDisabled, common.ResourceLifeCycleStateDisabledDetails).Return(nil, nil)

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

		mockSE.On("UpdateSnapshot", ctx, &datamodel.Snapshot{
			BaseModel:    datamodel.BaseModel{UUID: params.ResourceId},
			State:        common.ResourceStateDisabled,
			StateDetails: common.ResourceLifeCycleStateDisabledDetails,
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
			ResourceType: common.ResourceStateV1ResourceTypeStoragePool,
			ResourceId:   "test-storage-pool-id",
		}

		mockSE.On("UpdatePoolState", ctx, &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				UUID: params.ResourceId,
			}, State: common.ResourceStateDisabled,
			StateDetails: common.ResourceLifeCycleStateDisabledDetails,
		}, common.ResourceStateDisabled, common.ResourceLifeCycleStateDisabledDetails).Return(nil, nil)

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
			State:        common.ResourceStateDisabled,
			StateDetails: common.ResourceLifeCycleStateDisabledDetails,
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
			State:        common.ResourceStateEnabled,
			StateDetails: common.ResourceLifeCycleStateEnabledDetails,
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

		mockSE.On("UpdateSnapshot", ctx, &datamodel.Snapshot{
			BaseModel:    datamodel.BaseModel{UUID: params.ResourceId},
			State:        common.ResourceStateEnabled,
			StateDetails: common.ResourceLifeCycleStateEnabledDetails,
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
			ResourceType: common.ResourceStateV1ResourceTypeStoragePool,
			ResourceId:   "test-storage-pool-id",
		}

		mockSE.On("UpdatePoolState", ctx, &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				UUID: params.ResourceId,
			}, State: common.ResourceStateEnabled,
			StateDetails: common.ResourceLifeCycleStateEnabledDetails,
		}, common.ResourceStateEnabled, common.ResourceLifeCycleStateEnabledDetails).Return(nil, nil)

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
			State:        common.ResourceStateEnabled,
			StateDetails: common.ResourceLifeCycleStateEnabledDetails,
		}).Return(errors.New("update failed"))

		result, err := activity.HandleResourceEventsONForVCPActivity(ctx, params)
		assert.False(tt, result)
		assert.NotNil(tt, err)
		assert.ErrorContains(tt, err, "update failed")
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
