package api

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/pools"
	cvpmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	slogger "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestV1betaGetMultiplePools(t *testing.T) {
	t.Run("WhenGetMultiplePoolsFailsWithBadRequest", func(tt *testing.T) {
		mockClient := pools.NewMockClientService(tt)
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaGetMultiplePoolsParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.PoolIdListV1beta{
			PoolUuids: []string{"uuid1", "uuid2"},
		}

		errorMessage := "BadRequest error"
		errorCode := float64(400)
		mockError := &pools.V1betaGetMultiplePoolsBadRequest{
			Payload: &cvpmodels.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		mockClient.EXPECT().V1betaGetMultiplePools(mock.Anything).Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{Pools: mockClient}
		originalClient := createClient
		defer func() {
			createClient = originalClient
		}()
		createClient = func(logger slogger.Logger, JWT string) cvpapi.Cvp {
			return *cvpClient
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaGetMultiplePools(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, errorCode, result.(*gcpgenserver.V1betaGetMultiplePoolsBadRequest).Code)
		assert.Equal(tt, errorMessage, result.(*gcpgenserver.V1betaGetMultiplePoolsBadRequest).Message)
	})
	t.Run("WhenGetMultiplePoolsFailsWithUnauthorized", func(tt *testing.T) {
		mockClient := pools.NewMockClientService(tt)
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaGetMultiplePoolsParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.PoolIdListV1beta{
			PoolUuids: []string{"uuid1", "uuid2"},
		}

		errorMessage := "Unauthorized error"
		errorCode := float64(401)
		mockError := &pools.V1betaGetMultiplePoolsUnauthorized{
			Payload: &cvpmodels.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		mockClient.EXPECT().V1betaGetMultiplePools(mock.Anything).Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{Pools: mockClient}
		originalClient := createClient
		defer func() {
			createClient = originalClient
		}()
		createClient = func(logger slogger.Logger, JWT string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaGetMultiplePools(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, errorCode, result.(*gcpgenserver.V1betaGetMultiplePoolsUnauthorized).Code)
		assert.Equal(tt, errorMessage, result.(*gcpgenserver.V1betaGetMultiplePoolsUnauthorized).Message)
	})
	t.Run("WhenGetMultiplePoolsSucceeds", func(tt *testing.T) {
		mockClient := pools.NewMockClientService(tt)
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaGetMultiplePoolsParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.PoolIdListV1beta{
			PoolUuids: []string{"uuid1", "uuid2"},
		}
		resourceId := "resource-id-1"
		network := "network-1"
		sizeInBytes := float64(1024)
		serviceLevel := "service-level"
		mockResponse := &pools.V1betaGetMultiplePoolsOK{
			Payload: &pools.V1betaGetMultiplePoolsOKBody{
				Pools: []*cvpmodels.PoolV1beta{
					{
						PoolID:       "uuid1",
						ResourceID:   &resourceId,
						StorageClass: cvpmodels.StorageClassV1beta("storage-class-1"),
						ServiceLevel: &serviceLevel,
						Network:      &network,
						SizeInBytes:  &sizeInBytes,
					},
				},
			},
		}
		mockClient.EXPECT().V1betaGetMultiplePools(mock.Anything).Return(mockResponse, nil)
		cvpClient := &cvpapi.Cvp{Pools: mockClient}
		originalClient := createClient
		defer func() {
			createClient = originalClient
		}()
		createClient = func(logger slogger.Logger, JWT string) cvpapi.Cvp {
			return *cvpClient
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}

		mockOrchestrator.EXPECT().GetMultiplePools(mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaGetMultiplePools(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		successResult, ok := result.(*gcpgenserver.V1betaGetMultiplePoolsOK)
		assert.True(tt, ok)
		assert.Equal(tt, "uuid1", successResult.Pools[0].PoolId.Value)
	})
}

func TestV1betaCreatePool(t *testing.T) {
	t.Run("WhenUnifiedPoolIsNotSetToTrue", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaCreatePoolParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
		}

		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "", nil
		}

		req := &gcpgenserver.PoolV1beta{
			UnifiedPool: gcpgenserver.NewOptBool(false),
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaCreatePool(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, float64(400), result.(*gcpgenserver.V1betaCreatePoolBadRequest).Code)
		assert.Equal(tt, "UnifiedPool must be set to true", result.(*gcpgenserver.V1betaCreatePoolBadRequest).Message)
	})
	t.Run("WhenRegionAndZoneParsingFails", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaCreatePoolParams{
			LocationId:    "invalid-location-id",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.PoolV1beta{
			UnifiedPool: gcpgenserver.NewOptBool(true),
		}

		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "", "", &gcpgenserver.Error{
				Code:    400,
				Message: "Invalid location ID",
			}
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaCreatePool(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, float64(400), result.(*gcpgenserver.V1betaCreatePoolBadRequest).Code)
		assert.Equal(tt, "Invalid location ID", result.(*gcpgenserver.V1betaCreatePoolBadRequest).Message)
	})
	t.Run("WhenActiveDirectoryResourceIdIsSet", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaCreatePoolParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.PoolV1beta{
			UnifiedPool:               gcpgenserver.NewOptBool(true),
			ActiveDirectoryResourceId: gcpgenserver.NewOptString("some-resource-id"),
		}

		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaCreatePool(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, float64(400), result.(*gcpgenserver.V1betaCreatePoolBadRequest).Code)
		assert.Equal(tt, "Active directory cannot be assigned to a Unified Flex Storage Pool", result.(*gcpgenserver.V1betaCreatePoolBadRequest).Message)
	})
	t.Run("WhenLdapEnabledIsSet", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaCreatePoolParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.PoolV1beta{
			UnifiedPool: gcpgenserver.NewOptBool(true),
			LdapEnabled: gcpgenserver.NewOptNilBool(true),
		}

		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaCreatePool(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, float64(400), result.(*gcpgenserver.V1betaCreatePoolBadRequest).Code)
		assert.Equal(tt, "Ldap can not enabled on a Unified Flex Storage Pool", result.(*gcpgenserver.V1betaCreatePoolBadRequest).Message)
	})
	t.Run("WhenZoneIsEmpty", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaCreatePoolParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.PoolV1beta{
			UnifiedPool:  gcpgenserver.NewOptBool(true),
			ServiceLevel: gcpgenserver.PoolV1betaServiceLevelFLEX,
			SizeInBytes:  1099511627776,
			QosType:      gcpgenserver.NewOptNilString("auto"),
		}

		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "", nil
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaCreatePool(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, float64(400), result.(*gcpgenserver.V1betaCreatePoolBadRequest).Code)
		assert.Equal(tt, "Zone cannot be empty for regional pool.", result.(*gcpgenserver.V1betaCreatePoolBadRequest).Message)
	})
	t.Run("WhenSecondaryZoneIsEmpty", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaCreatePoolParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.PoolV1beta{
			UnifiedPool:  gcpgenserver.NewOptBool(true),
			ServiceLevel: gcpgenserver.PoolV1betaServiceLevelFLEX,
			SizeInBytes:  1099511627776,
			QosType:      gcpgenserver.NewOptNilString("auto"),
			Zone:         gcpgenserver.NewOptString("us-east4-a"),
		}

		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "", nil
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaCreatePool(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, float64(400), result.(*gcpgenserver.V1betaCreatePoolBadRequest).Code)
		assert.Equal(tt, "Secondary Zone cannot be empty for regional pool.", result.(*gcpgenserver.V1betaCreatePoolBadRequest).Message)
	})
	t.Run("WhenZonesDontMatch", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaCreatePoolParams{
			LocationId:    "us-east4-a",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.PoolV1beta{
			UnifiedPool:   gcpgenserver.NewOptBool(true),
			ServiceLevel:  gcpgenserver.PoolV1betaServiceLevelFLEX,
			SizeInBytes:   1099511627776,
			QosType:       gcpgenserver.NewOptNilString("auto"),
			Zone:          gcpgenserver.NewOptString("us-east4-b"),
			SecondaryZone: gcpgenserver.NewOptString("us-east4-b"),
		}

		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4-a", nil
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaCreatePool(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, float64(400), result.(*gcpgenserver.V1betaCreatePoolBadRequest).Code)
		assert.Equal(tt, "Multiple Zone values cannot be passed for Pool Creation", result.(*gcpgenserver.V1betaCreatePoolBadRequest).Message)
	})
	t.Run("WhenPoolAlreadyExists", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaCreatePoolParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
		}

		req := &gcpgenserver.PoolV1beta{
			UnifiedPool:              gcpgenserver.NewOptBool(true),
			ServiceLevel:             gcpgenserver.PoolV1betaServiceLevelFLEX,
			SizeInBytes:              1099511627776,
			QosType:                  gcpgenserver.NewOptNilString("auto"),
			Zone:                     gcpgenserver.NewOptString("us-east4-a"),
			SecondaryZone:            gcpgenserver.NewOptString("us-east4-b"),
			CustomPerformanceEnabled: gcpgenserver.NewOptBool(true),
			TotalThroughputMibps:     gcpgenserver.NewOptNilFloat64(64), // 128 MiBps
		}

		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "", nil
		}

		existingPool := &models.Pool{
			BaseModel: models.BaseModel{
				UUID: "existing-pool-uuid",
			},
			PoolAttributes: &models.PoolAttributes{},
		}

		mockOrchestrator.EXPECT().GetPoolByVendorID(mock.Anything, mock.Anything).Return(existingPool, nil)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaCreatePool(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "operation-id", result.(*gcpgenserver.OperationV1beta).Name.Value)
	})
	t.Run("WhenPoolCreationFailsWithUserInputValidationError", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaCreatePoolParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
		}

		req := &gcpgenserver.PoolV1beta{
			UnifiedPool:              gcpgenserver.NewOptBool(true),
			ServiceLevel:             gcpgenserver.PoolV1betaServiceLevelFLEX,
			SizeInBytes:              1099511627776,
			QosType:                  gcpgenserver.NewOptNilString("auto"),
			Zone:                     gcpgenserver.NewOptString("us-east4-a"),
			SecondaryZone:            gcpgenserver.NewOptString("us-east4-b"),
			CustomPerformanceEnabled: gcpgenserver.NewOptBool(true),
			TotalThroughputMibps:     gcpgenserver.NewOptNilFloat64(64), // 128 MiBps
		}

		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "", nil
		}

		mockOrchestrator.EXPECT().GetPoolByVendorID(mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundErr("not found", nil))
		mockOrchestrator.EXPECT().CreatePool(mock.Anything, mock.Anything).Return(nil, "", errors.NewUserInputValidationErr("Given pool size must be a multiple of 1GiB"))

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaCreatePool(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, float64(400), result.(*gcpgenserver.V1betaCreatePoolBadRequest).Code)
		assert.Equal(tt, "Given pool size must be a multiple of 1GiB", result.(*gcpgenserver.V1betaCreatePoolBadRequest).Message)
	})
	t.Run("WhenPoolCreationSucceeds", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaCreatePoolParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
		}

		req := &gcpgenserver.PoolV1beta{
			UnifiedPool:              gcpgenserver.NewOptBool(true),
			ServiceLevel:             gcpgenserver.PoolV1betaServiceLevelFLEX,
			SizeInBytes:              1099511627776,
			QosType:                  gcpgenserver.NewOptNilString("auto"),
			Zone:                     gcpgenserver.NewOptString("us-east4-a"),
			SecondaryZone:            gcpgenserver.NewOptString("us-east4-b"),
			CustomPerformanceEnabled: gcpgenserver.NewOptBool(true),
			TotalThroughputMibps:     gcpgenserver.NewOptNilFloat64(64), // 128 MiBps
		}

		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "", nil
		}

		mockOrchestrator.EXPECT().GetPoolByVendorID(mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundErr("not found", nil))
		mockOrchestrator.EXPECT().CreatePool(mock.Anything, mock.Anything).Return(&models.Pool{BaseModel: models.BaseModel{UUID: "new-pool-uuid"}, PoolAttributes: &models.PoolAttributes{}}, "operation-id", nil)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		operationID := fmt.Sprintf("/v1beta/projects/%s/locations/%s/operations/%s", params.ProjectNumber, params.LocationId, "operation-id")
		result, err := handler.V1betaCreatePool(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, operationID, result.(*gcpgenserver.OperationV1beta).Name.Value)
	})
}

func TestV1betaDeletePool(t *testing.T) {
	t.Run("WhenLocationValidationFails", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaDeletePoolParams{
			LocationId:    "invalid-location-id",
			ProjectNumber: "project-number",
			PoolId:        "pool-id",
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "", "", &gcpgenserver.Error{
				Code:    400,
				Message: "Invalid location ID",
			}
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaDeletePool(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, float64(400), result.(*gcpgenserver.V1betaDeletePoolBadRequest).Code)
		assert.Equal(tt, "Invalid location ID", result.(*gcpgenserver.V1betaDeletePoolBadRequest).Message)
	})
	t.Run("WhenPoolDoesNotExist", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaDeletePoolParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
			PoolId:        "non-existent-pool-id",
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		mockOrchestrator.EXPECT().GetPool(mock.Anything, params.PoolId, params.ProjectNumber).Return(nil, errors.NewNotFoundErr("not found", nil))

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaDeletePool(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, float64(404), result.(*gcpgenserver.V1betaDeletePoolBadRequest).Code)
		assert.Equal(tt, "Pool not found", result.(*gcpgenserver.V1betaDeletePoolBadRequest).Message)
	})
	t.Run("WhenPoolHasActiveVolumes", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaDeletePoolParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
			PoolId:        "pool-with-volumes",
		}

		existingPool := &models.Pool{
			BaseModel: models.BaseModel{
				UUID: "pool-with-volumes",
			},
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		mockOrchestrator.EXPECT().GetPool(mock.Anything, params.PoolId, params.ProjectNumber).Return(existingPool, nil)
		mockOrchestrator.EXPECT().DeletePool(mock.Anything, mock.Anything).Return(nil, "", errors.NewConflictErr("pool has active volumes"))

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaDeletePool(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, float64(409), result.(*gcpgenserver.V1betaDeletePoolBadRequest).Code)
		assert.Equal(tt, "Pool has active volumes", result.(*gcpgenserver.V1betaDeletePoolBadRequest).Message)
	})
	t.Run("WhenPoolDeletionSucceeds", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaDeletePoolParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
			PoolId:        "deletable-pool-id",
		}

		existingPool := &models.Pool{
			BaseModel: models.BaseModel{
				UUID: "deletable-pool-id",
			},
			PoolAttributes: &models.PoolAttributes{},
		}
		deletedPool := &models.Pool{
			BaseModel: models.BaseModel{
				UUID: "deletable-pool-id",
			},
			PoolAttributes: &models.PoolAttributes{},
			State:          models.LifeCycleStateDeleting,
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		mockOrchestrator.EXPECT().GetPool(mock.Anything, params.PoolId, params.ProjectNumber).Return(existingPool, nil)
		mockOrchestrator.EXPECT().DeletePool(mock.Anything, mock.Anything).Return(deletedPool, "operation-id", nil)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaDeletePool(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "/v1beta/projects/project-number/locations/us-east4/operations/operation-id", result.(*gcpgenserver.OperationV1beta).Name.Value)
	})
}

func TestV1betaDescribePool(t *testing.T) {
	t.Run("WhenLocationValidationFails", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaDescribePoolParams{
			LocationId:    "invalid-location-id",
			ProjectNumber: "project-number",
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "", "", &gcpgenserver.Error{
				Code:    400,
				Message: "Invalid location ID",
			}
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaDescribePool(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, float64(400), result.(*gcpgenserver.V1betaDescribePoolBadRequest).Code)
		assert.Equal(tt, "Invalid location ID", result.(*gcpgenserver.V1betaDescribePoolBadRequest).Message)
	})
	t.Run("WhenPoolDoesNotExist", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaDescribePoolParams{
			PoolId:        "non-existent-pool-id",
			ProjectNumber: "project-number",
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}

		mockOrchestrator.EXPECT().GetPool(mock.Anything, params.PoolId, params.ProjectNumber).Return(nil, errors.NewNotFoundErr("not found", nil))

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaDescribePool(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, float64(404), result.(*gcpgenserver.V1betaDescribePoolBadRequest).Code)
		assert.Equal(tt, "Pool not found", result.(*gcpgenserver.V1betaDescribePoolBadRequest).Message)
	})

	t.Run("WhenDescribePoolFailsWithInternalError", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaDescribePoolParams{
			PoolId:        "pool-id",
			ProjectNumber: "project-number",
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}

		mockOrchestrator.EXPECT().GetPool(mock.Anything, params.PoolId, params.ProjectNumber).Return(nil, fmt.Errorf("internal error"))

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaDescribePool(context.Background(), params)

		assert.Error(tt, err)
		assert.NotNil(tt, result)
		_, ok := result.(*gcpgenserver.V1betaDescribePoolInternalServerError)
		assert.True(tt, ok)
	})

	t.Run("WhenDescribePoolSucceeds", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaDescribePoolParams{
			PoolId:        "existing-pool-id",
			ProjectNumber: "project-number",
		}

		existingPool := &models.Pool{
			BaseModel: models.BaseModel{
				UUID: "existing-pool-id",
			},
			PoolAttributes: &models.PoolAttributes{},
			Name:           "test-pool",
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		mockOrchestrator.EXPECT().GetPool(mock.Anything, params.PoolId, params.ProjectNumber).Return(existingPool, nil)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaDescribePool(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "existing-pool-id", result.(*gcpgenserver.PoolV1beta).PoolId.Value)
	})
}

func TestV1betaListPools(t *testing.T) {
	t.Run("WhenLocationValidationFails", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaListPoolsParams{
			LocationId:    "invalid-location-id",
			ProjectNumber: "project-number",
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "", "", &gcpgenserver.Error{
				Code:    400,
				Message: "Invalid location ID",
			}
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaListPools(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, float64(400), result.(*gcpgenserver.V1betaListPoolsBadRequest).Code)
		assert.Equal(tt, "Invalid location ID", result.(*gcpgenserver.V1betaListPoolsBadRequest).Message)
	})
	t.Run("WhenListPoolsFailsWithInternalError", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaListPoolsParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		mockOrchestrator.EXPECT().ListPools(mock.Anything, params.ProjectNumber).Return(nil, fmt.Errorf("internal error"))

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaListPools(context.Background(), params)

		assert.Error(tt, err)
		assert.NotNil(tt, result)
		_, ok := result.(*gcpgenserver.V1betaListPoolsInternalServerError)
		assert.True(tt, ok)
	})
	t.Run("WhenListPoolsSucceeds", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaListPoolsParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
		}

		pool1 := &models.Pool{
			BaseModel:      models.BaseModel{UUID: "pool-uuid-1"},
			Name:           "pool-1",
			PoolAttributes: &models.PoolAttributes{},
		}
		pool2 := &models.Pool{
			BaseModel: models.BaseModel{UUID: "pool-uuid-2"},
			Name:      "pool-2",
			PoolAttributes: &models.PoolAttributes{
				NumberOfVolumes: 1,
			},
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		mockOrchestrator.EXPECT().ListPools(mock.Anything, params.ProjectNumber).Return([]*models.Pool{pool1, pool2}, nil)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaListPools(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		successResult, ok := result.(*gcpgenserver.V1betaListPoolsOK)
		assert.True(tt, ok)
		assert.Equal(tt, 2, len(successResult.Pools))
		assert.Equal(tt, "pool-uuid-1", successResult.Pools[0].PoolId.Value)
		assert.Equal(tt, "pool-uuid-2", successResult.Pools[1].PoolId.Value)
	})
}

func TestGetEncryptionTypeForPool(t *testing.T) {
	cloudKms := "kms-id"
	servManaged := (*string)(nil)

	// Test with non-nil, non-empty kmsConfigId
	encType := getEncryptionTypeForPool(&cloudKms)
	assert.Equal(t, gcpgenserver.PoolV1betaEncryptionTypeCLOUDKMS, encType.Value)

	// Test with nil kmsConfigId
	encType = getEncryptionTypeForPool(servManaged)
	assert.Equal(t, gcpgenserver.PoolV1betaEncryptionTypeSERVICEMANAGED, encType.Value)

	// Test with empty string pointer
	empty := ""
	encType = getEncryptionTypeForPool(&empty)
	assert.Equal(t, gcpgenserver.PoolV1betaEncryptionTypeSERVICEMANAGED, encType.Value)
}
