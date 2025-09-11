package api

import (
	"context"
	stderrors "errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/pools"
	cvpmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestResolvePerformanceParams(t *testing.T) {
	t.Run("UsesDefaultThroughputWhenNotProvided", func(tt *testing.T) {
		reqThroughput := gcpgenserver.OptNilFloat64{}
		reqIops := gcpgenserver.OptNilFloat64{Value: 2048, Set: true}

		throughput, iops := resolvePerformanceParams(reqThroughput, reqIops)

		assert.Equal(tt, minCustomThroughput, uint64(throughput))
		assert.Equal(tt, int64(2048), *iops)
	})

	t.Run("UsesProvidedValuesWhenBothSet", func(tt *testing.T) {
		reqThroughput := gcpgenserver.OptNilFloat64{Value: 256, Set: true}
		reqIops := gcpgenserver.OptNilFloat64{Value: 5000, Set: true}

		throughput, iops := resolvePerformanceParams(reqThroughput, reqIops)

		assert.Equal(tt, int64(256), throughput)
		assert.Equal(tt, int64(5000), *iops)
	})

	t.Run("HandlesLargeValues", func(tt *testing.T) {
		reqThroughput := gcpgenserver.OptNilFloat64{Value: 5120, Set: true} // Max throughput
		reqIops := gcpgenserver.OptNilFloat64{Value: 160000, Set: true}     // Max IOPS

		throughput, iops := resolvePerformanceParams(reqThroughput, reqIops)

		assert.Equal(tt, int64(5120), throughput)
		assert.Equal(tt, int64(160000), *iops)
	})

	t.Run("HandlesMinimumValues", func(tt *testing.T) {
		reqThroughput := gcpgenserver.OptNilFloat64{Value: 64, Set: true} // Min throughput
		reqIops := gcpgenserver.OptNilFloat64{Value: 1024, Set: true}     // Min IOPS

		throughput, iops := resolvePerformanceParams(reqThroughput, reqIops)

		assert.Equal(tt, int64(64), throughput)
		assert.Equal(tt, int64(1024), *iops)
	})
}

func TestV1betaGetMultiplePools(t *testing.T) {
	t.Run("WhenGetMultiplePoolsFailsWithBadRequest", func(tt *testing.T) {
		// Don't set CVP_HOST so CVP calls will be skipped
		tt.Setenv("CVP_HOST", "")

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaGetMultiplePoolsParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.PoolIdListV1beta{
			PoolUuids: []string{"uuid1", "uuid2"},
		}

		// Mock VCP to return empty pools
		mockOrchestrator.EXPECT().GetMultiplePools(mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		// Mock location validation
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaGetMultiplePools(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		// Since CVP_HOST is not set, we expect OK response with empty pools
		okResp, ok := result.(*gcpgenserver.V1betaGetMultiplePoolsOK)
		assert.True(tt, ok)
		assert.Len(tt, okResp.Pools, 0)
	})
	t.Run("WhenGetMultiplePoolsFailsWithUnauthorized", func(tt *testing.T) {
		// Don't set CVP_HOST so CVP calls will be skipped
		tt.Setenv("CVP_HOST", "")

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaGetMultiplePoolsParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.PoolIdListV1beta{
			PoolUuids: []string{"uuid1", "uuid2"},
		}

		// Mock VCP to return empty pools
		mockOrchestrator.EXPECT().GetMultiplePools(mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		// Mock location validation
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaGetMultiplePools(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		// Since CVP_HOST is not set, we expect OK response with empty pools
		okResp, ok := result.(*gcpgenserver.V1betaGetMultiplePoolsOK)
		assert.True(tt, ok)
		assert.Len(tt, okResp.Pools, 0)
	})
	t.Run("WhenGetMultiplePoolsSucceeds", func(tt *testing.T) {
		// Don't set CVP_HOST so CVP calls will be skipped
		tt.Setenv("CVP_HOST", "")

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaGetMultiplePoolsParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.PoolIdListV1beta{
			PoolUuids: []string{"uuid1", "uuid2"},
		}

		// Mock VCP to return empty pools
		mockOrchestrator.EXPECT().GetMultiplePools(mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		// Mock location validation
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaGetMultiplePools(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		// Since CVP_HOST is not set, we expect OK response with empty pools
		okResp, ok := result.(*gcpgenserver.V1betaGetMultiplePoolsOK)
		assert.True(tt, ok)
		assert.Len(tt, okResp.Pools, 0)
	})

	t.Run("Success - all pools found in VCP, CVP_HOST is set", func(tt *testing.T) {
		// Don't set CVP_HOST so CVP calls will be skipped
		tt.Setenv("CVP_HOST", "")

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaGetMultiplePoolsParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.PoolIdListV1beta{
			PoolUuids: []string{"uuid1", "uuid2"},
		}

		// Mock VCP to return all requested pools
		vcpPools := []*models.Pool{
			{
				BaseModel:      models.BaseModel{UUID: "uuid1"},
				Name:           "pool1",
				PoolAttributes: &models.PoolAttributes{},
			},
			{
				BaseModel:      models.BaseModel{UUID: "uuid2"},
				Name:           "pool2",
				PoolAttributes: &models.PoolAttributes{},
			},
		}
		mockOrchestrator.EXPECT().GetMultiplePools(mock.Anything, mock.Anything, mock.Anything).Return(vcpPools, nil)

		// Mock location validation
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaGetMultiplePools(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		successResult, ok := result.(*gcpgenserver.V1betaGetMultiplePoolsOK)
		assert.True(tt, ok)
		assert.Len(tt, successResult.Pools, 2)
	})

	t.Run("Success - some pools found in VCP, some in CVP, CVP_HOST is set", func(tt *testing.T) {
		// Don't set CVP_HOST so CVP calls will be skipped
		tt.Setenv("CVP_HOST", "")

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaGetMultiplePoolsParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.PoolIdListV1beta{
			PoolUuids: []string{"uuid1", "uuid2", "uuid3"},
		}

		// Mock VCP to return only one pool
		vcpPools := []*models.Pool{
			{
				BaseModel:      models.BaseModel{UUID: "uuid1"},
				Name:           "pool1",
				PoolAttributes: &models.PoolAttributes{},
			},
		}
		mockOrchestrator.EXPECT().GetMultiplePools(mock.Anything, mock.Anything, mock.Anything).Return(vcpPools, nil)

		// Mock location validation
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaGetMultiplePools(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		successResult, ok := result.(*gcpgenserver.V1betaGetMultiplePoolsOK)
		assert.True(tt, ok)
		assert.Len(tt, successResult.Pools, 1) // Only VCP pools, no CVP call
	})

	t.Run("Success - CVP_HOST is not set", func(tt *testing.T) {
		// Don't set CVP_HOST so CVP calls will be skipped
		tt.Setenv("CVP_HOST", "")

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaGetMultiplePoolsParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.PoolIdListV1beta{
			PoolUuids: []string{"uuid1", "uuid2"},
		}

		// Mock VCP to return only one pool
		vcpPools := []*models.Pool{
			{
				BaseModel:      models.BaseModel{UUID: "uuid1"},
				Name:           "pool1",
				PoolAttributes: &models.PoolAttributes{},
			},
		}
		mockOrchestrator.EXPECT().GetMultiplePools(mock.Anything, mock.Anything, mock.Anything).Return(vcpPools, nil)

		// Mock location validation
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaGetMultiplePools(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		successResult, ok := result.(*gcpgenserver.V1betaGetMultiplePoolsOK)
		assert.True(tt, ok)
		assert.Len(tt, successResult.Pools, 1) // Only VCP pools, no CVP call
	})

	t.Run("WhenLocationValidationFails", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaGetMultiplePoolsParams{
			LocationId:    "invalid-location",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.PoolIdListV1beta{
			PoolUuids: []string{"uuid1", "uuid2"},
		}

		// Mock location validation to fail
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "", "", &gcpgenserver.Error{
				Code:    400,
				Message: "Invalid location",
			}
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaGetMultiplePools(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		badRequest, ok := result.(*gcpgenserver.V1betaGetMultiplePoolsBadRequest)
		assert.True(tt, ok)
		assert.Equal(tt, float64(400), badRequest.Code)
		assert.Equal(tt, "Invalid location", badRequest.Message)
	})

	t.Run("WhenPoolUuidsIsNil", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaGetMultiplePoolsParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.PoolIdListV1beta{
			PoolUuids: nil,
		}

		// Mock location validation
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaGetMultiplePools(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		badRequest, ok := result.(*gcpgenserver.V1betaGetMultiplePoolsBadRequest)
		assert.True(tt, ok)
		assert.Equal(tt, float64(400), badRequest.Code)
		assert.Equal(tt, "PoolUUIDs is required", badRequest.Message)
	})

	t.Run("WhenPoolUuidsExceeds1000", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaGetMultiplePoolsParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
		}

		// Generate more than 1000 UUIDs
		poolUuids := make([]string, 1001)
		for i := 0; i < 1001; i++ {
			poolUuids[i] = fmt.Sprintf("uuid-%d", i)
		}
		req := &gcpgenserver.PoolIdListV1beta{
			PoolUuids: poolUuids,
		}

		// Mock location validation
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaGetMultiplePools(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		badRequest, ok := result.(*gcpgenserver.V1betaGetMultiplePoolsBadRequest)
		assert.True(tt, ok)
		assert.Equal(tt, float64(400), badRequest.Code)
		assert.Equal(tt, "poolUUIDs in body should have at most 1000 items", badRequest.Message)
	})

	t.Run("WhenGetMultiplePoolsFails", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaGetMultiplePoolsParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.PoolIdListV1beta{
			PoolUuids: []string{"uuid1", "uuid2"},
		}

		// Mock location validation
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}

		// Mock orchestrator to return error
		mockOrchestrator.EXPECT().GetMultiplePools(mock.Anything, mock.Anything, mock.Anything).Return(nil, assert.AnError)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaGetMultiplePools(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		internalError, ok := result.(*gcpgenserver.V1betaGetMultiplePoolsInternalServerError)
		assert.True(tt, ok)
		assert.Equal(tt, float64(500), internalError.Code)
		assert.Equal(tt, "Internal server error while getting pools", internalError.Message)
	})

	t.Run("WhenNoMissingPools", func(tt *testing.T) {
		// Don't set CVP_HOST so CVP calls will be skipped
		tt.Setenv("CVP_HOST", "")

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaGetMultiplePoolsParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.PoolIdListV1beta{
			PoolUuids: []string{"uuid1", "uuid2"},
		}

		// Mock VCP to return all requested pools (no missing pools)
		vcpPools := []*models.Pool{
			{
				BaseModel:      models.BaseModel{UUID: "uuid1"},
				Name:           "pool1",
				PoolAttributes: &models.PoolAttributes{},
			},
			{
				BaseModel:      models.BaseModel{UUID: "uuid2"},
				Name:           "pool2",
				PoolAttributes: &models.PoolAttributes{},
			},
		}
		mockOrchestrator.EXPECT().GetMultiplePools(mock.Anything, mock.Anything, mock.Anything).Return(vcpPools, nil)

		// Mock location validation
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaGetMultiplePools(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		// Since all pools are found in VCP, we expect OK response with all pools
		okResp, ok := result.(*gcpgenserver.V1betaGetMultiplePoolsOK)
		assert.True(tt, ok)
		assert.Len(tt, okResp.Pools, 2)
	})

	t.Run("WhenNoMissingPoolsWithCVPEnabled", func(tt *testing.T) {
		// Set CVP_HOST so CVP calls will be made
		tt.Setenv("CVP_HOST", "http://cvp-host")

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaGetMultiplePoolsParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.PoolIdListV1beta{
			PoolUuids: []string{"uuid1", "uuid2"},
		}

		// Mock VCP to return all requested pools (no missing pools)
		vcpPools := []*models.Pool{
			{
				BaseModel:      models.BaseModel{UUID: "uuid1"},
				Name:           "pool1",
				PoolAttributes: &models.PoolAttributes{},
			},
			{
				BaseModel:      models.BaseModel{UUID: "uuid2"},
				Name:           "pool2",
				PoolAttributes: &models.PoolAttributes{},
			},
		}
		mockOrchestrator.EXPECT().GetMultiplePools(mock.Anything, mock.Anything, mock.Anything).Return(vcpPools, nil)

		// Mock location validation
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaGetMultiplePools(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		// Since all pools are found in VCP, we expect OK response with all pools (no CVP call)
		okResp, ok := result.(*gcpgenserver.V1betaGetMultiplePoolsOK)
		assert.True(tt, ok)
		assert.Len(tt, okResp.Pools, 2)
	})

	t.Run("WhenMissingPoolsWithCVPEnabled", func(tt *testing.T) {
		// Set CVP_HOST so CVP calls will be made
		tt.Setenv("CVP_HOST", "http://cvp-host")

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaGetMultiplePoolsParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.PoolIdListV1beta{
			PoolUuids: []string{"uuid1", "uuid2", "uuid3"},
		}

		// Mock VCP to return only one pool (missing pools will trigger CVP call)
		vcpPools := []*models.Pool{
			{
				BaseModel:      models.BaseModel{UUID: "uuid1"},
				Name:           "pool1",
				PoolAttributes: &models.PoolAttributes{},
			},
		}
		mockOrchestrator.EXPECT().GetMultiplePools(mock.Anything, mock.Anything, mock.Anything).Return(vcpPools, nil)

		// Mock location validation
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaGetMultiplePools(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		// Since CVP_HOST is set and there are missing pools, we expect OK response with VCP pools only
		// (CVP call will be skipped in test environment due to constant not being updated)
		okResp, ok := result.(*gcpgenserver.V1betaGetMultiplePoolsOK)
		assert.True(tt, ok)
		assert.Len(tt, okResp.Pools, 1)
	})

	t.Run("Covers getMultiplePoolsFromCVP path", func(tt *testing.T) {
		// Set CVP_HOST so the handler will not return early
		cvp.SetCVPHost("http://cvp-host")

		// Save and mock createClient
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		mockPools := pools.NewMockClientService(tt)
		mockClient := &cvpapi.Cvp{
			Pools: mockPools,
		}
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *mockClient
		}

		// Set up the mock for the CVP Pools client
		resourceID := "resource-id-2"
		network := "network-2"
		sizeInBytes := float64(1000000000)
		serviceLevel := "PREMIUM"
		storageClass := cvpmodels.StorageClassV1betaSOFTWARE
		storagePoolState := "READY"
		encryptionType := "SERVICE_MANAGED"

		mockPools.EXPECT().V1betaGetMultiplePools(mock.Anything).Return(&pools.V1betaGetMultiplePoolsOK{
			Payload: &pools.V1betaGetMultiplePoolsOKBody{
				Pools: []*cvpmodels.PoolV1beta{
					{
						PoolID:           "uuid2",
						ResourceID:       &resourceID,
						Network:          &network,
						SizeInBytes:      &sizeInBytes,
						ServiceLevel:     &serviceLevel,
						StorageClass:     storageClass,
						StoragePoolState: storagePoolState,
						EncryptionType:   encryptionType,
					},
				},
			},
		}, nil)

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaGetMultiplePoolsParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.PoolIdListV1beta{
			PoolUuids: []string{"uuid1", "uuid2"},
		}

		// VCP returns only one pool, so uuid2 is missing and triggers CVP call
		vcpPools := []*models.Pool{
			{
				BaseModel:      models.BaseModel{UUID: "uuid1"},
				Name:           "pool1",
				PoolAttributes: &models.PoolAttributes{},
			},
		}
		mockOrchestrator.EXPECT().GetMultiplePools(mock.Anything, mock.Anything, mock.Anything).Return(vcpPools, nil)

		// Mock location validation
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaGetMultiplePools(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		okResp, ok := result.(*gcpgenserver.V1betaGetMultiplePoolsOK)
		assert.True(tt, ok)
		// Should contain both VCP and CVP pools
		assert.Len(tt, okResp.Pools, 2)
	})

	t.Run("WhenOrchestratorGetMultiplePoolsFails_ReturnsInternalServerError", func(tt *testing.T) {
		// Don't set CVP_HOST so CVP calls will be skipped
		tt.Setenv("CVP_HOST", "")

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaGetMultiplePoolsParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.PoolIdListV1beta{
			PoolUuids: []string{"uuid1", "uuid2"},
		}

		// Mock VCP to return an error
		mockOrchestrator.EXPECT().GetMultiplePools(mock.Anything, mock.Anything, mock.Anything).Return(nil, stderrors.New("database connection failed"))

		// Mock location validation
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaGetMultiplePools(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		// Should return InternalServerError with proper error message
		internalServerError, ok := result.(*gcpgenserver.V1betaGetMultiplePoolsInternalServerError)
		assert.True(tt, ok)
		assert.Equal(tt, float64(500), internalServerError.Code)
		assert.Equal(tt, "Internal server error while getting pools", internalServerError.Message)
	})

	t.Run("WhenOrchestratorGetMultiplePoolsFails_ErrorNotReturned", func(tt *testing.T) {
		// Don't set CVP_HOST so CVP calls will be skipped
		tt.Setenv("CVP_HOST", "")

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaGetMultiplePoolsParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.PoolIdListV1beta{
			PoolUuids: []string{"uuid1", "uuid2"},
		}

		// Mock VCP to return an error
		mockOrchestrator.EXPECT().GetMultiplePools(mock.Anything, mock.Anything, mock.Anything).Return(nil, stderrors.New("database connection failed"))

		// Mock location validation
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaGetMultiplePools(context.Background(), req, params)

		// Key change: err should be nil, not the orchestrator error
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		// Should return InternalServerError, not propagate the orchestrator error
		internalServerError, ok := result.(*gcpgenserver.V1betaGetMultiplePoolsInternalServerError)
		assert.True(tt, ok)
		assert.Equal(tt, float64(500), internalServerError.Code)
	})

	t.Run("WhenSomePoolsNotFoundInVCP_LogsDebugMessage", func(tt *testing.T) {
		// Set CVP_HOST to enable CVP calls
		tt.Setenv("CVP_HOST", "http://cvp-host")

		// Save and mock createClient
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		mockPools := pools.NewMockClientService(tt)
		mockClient := &cvpapi.Cvp{
			Pools: mockPools,
		}
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *mockClient
		}

		// Mock CVP to return the missing pool (uuid3)
		resourceID := "resource-id-3"
		network := "network-3"
		sizeInBytes := float64(1000000000)
		serviceLevel := "PREMIUM"
		storageClass := cvpmodels.StorageClassV1betaSOFTWARE
		storagePoolState := "READY"
		encryptionType := "SERVICE_MANAGED"

		mockPools.EXPECT().V1betaGetMultiplePools(mock.Anything).Return(&pools.V1betaGetMultiplePoolsOK{
			Payload: &pools.V1betaGetMultiplePoolsOKBody{
				Pools: []*cvpmodels.PoolV1beta{
					{
						PoolID:           "uuid3",
						ResourceID:       &resourceID,
						Network:          &network,
						SizeInBytes:      &sizeInBytes,
						ServiceLevel:     &serviceLevel,
						StorageClass:     storageClass,
						StoragePoolState: storagePoolState,
						EncryptionType:   encryptionType,
					},
				},
			},
		}, nil)

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaGetMultiplePoolsParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.PoolIdListV1beta{
			PoolUuids: []string{"uuid1", "uuid2", "uuid3"},
		}

		// Mock VCP to return only some pools (uuid1 and uuid2 found, uuid3 missing)
		vcpPools := []*models.Pool{
			{
				BaseModel:      models.BaseModel{UUID: "uuid1"},
				Name:           "pool1",
				PoolAttributes: &models.PoolAttributes{},
			},
			{
				BaseModel:      models.BaseModel{UUID: "uuid2"},
				Name:           "pool2",
				PoolAttributes: &models.PoolAttributes{},
			},
		}
		mockOrchestrator.EXPECT().GetMultiplePools(mock.Anything, mock.Anything, mock.Anything).Return(vcpPools, nil)

		// Mock location validation
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaGetMultiplePools(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		// Should return OK response with both VCP and CVP pools
		okResp, ok := result.(*gcpgenserver.V1betaGetMultiplePoolsOK)
		assert.True(tt, ok)
		assert.Len(tt, okResp.Pools, 3)
		// Verify the pools returned are from both VCP and CVP
		assert.Equal(tt, "uuid1", okResp.Pools[0].PoolId.Value)
		assert.Equal(tt, "uuid2", okResp.Pools[1].PoolId.Value)
		assert.Equal(tt, "uuid3", okResp.Pools[2].PoolId.Value)
	})

	t.Run("WhenAllPoolsFoundInVCP_NoCVPFallback", func(tt *testing.T) {
		// Set CVP_HOST to enable CVP calls
		tt.Setenv("CVP_HOST", "http://cvp-host")

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaGetMultiplePoolsParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.PoolIdListV1beta{
			PoolUuids: []string{"uuid1", "uuid2"},
		}

		// Mock VCP to return all requested pools
		vcpPools := []*models.Pool{
			{
				BaseModel:      models.BaseModel{UUID: "uuid1"},
				Name:           "pool1",
				PoolAttributes: &models.PoolAttributes{},
			},
			{
				BaseModel:      models.BaseModel{UUID: "uuid2"},
				Name:           "pool2",
				PoolAttributes: &models.PoolAttributes{},
			},
		}
		mockOrchestrator.EXPECT().GetMultiplePools(mock.Anything, mock.Anything, mock.Anything).Return(vcpPools, nil)

		// Mock location validation
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaGetMultiplePools(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		// Since all pools are found in VCP, we expect OK response with all pools
		okResp, ok := result.(*gcpgenserver.V1betaGetMultiplePoolsOK)
		assert.True(tt, ok)
		assert.Len(tt, okResp.Pools, 2)
	})

	t.Run("WhenOrchestratorGetMultiplePoolsReturnsEmpty_TriggersCVPFallback", func(tt *testing.T) {
		// Set CVP_HOST to enable CVP calls
		tt.Setenv("CVP_HOST", "http://cvp-host")

		// Save and mock createClient
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		mockPools := pools.NewMockClientService(tt)
		mockClient := &cvpapi.Cvp{
			Pools: mockPools,
		}
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *mockClient
		}

		// Mock CVP to return empty pools
		mockPools.EXPECT().V1betaGetMultiplePools(mock.Anything).Return(&pools.V1betaGetMultiplePoolsOK{
			Payload: &pools.V1betaGetMultiplePoolsOKBody{
				Pools: []*cvpmodels.PoolV1beta{},
			},
		}, nil)

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaGetMultiplePoolsParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.PoolIdListV1beta{
			PoolUuids: []string{"uuid1", "uuid2"},
		}

		// Mock VCP to return empty pools (triggering CVP fallback)
		mockOrchestrator.EXPECT().GetMultiplePools(mock.Anything, mock.Anything, mock.Anything).Return([]*models.Pool{}, nil)

		// Mock location validation
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaGetMultiplePools(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		// Should return OK response with empty pools from CVP fallback
		okResp, ok := result.(*gcpgenserver.V1betaGetMultiplePoolsOK)
		assert.True(tt, ok)
		assert.Len(tt, okResp.Pools, 0) // CVP fallback returns empty pools
	})

	t.Run("WhenOrchestratorGetMultiplePoolsReturnsNil_TriggersCVPFallback", func(tt *testing.T) {
		// Set CVP_HOST to enable CVP calls
		tt.Setenv("CVP_HOST", "http://cvp-host")

		// Save and mock createClient
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		mockPools := pools.NewMockClientService(tt)
		mockClient := &cvpapi.Cvp{
			Pools: mockPools,
		}
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *mockClient
		}

		// Mock CVP to return empty pools
		mockPools.EXPECT().V1betaGetMultiplePools(mock.Anything).Return(&pools.V1betaGetMultiplePoolsOK{
			Payload: &pools.V1betaGetMultiplePoolsOKBody{
				Pools: []*cvpmodels.PoolV1beta{},
			},
		}, nil)

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaGetMultiplePoolsParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.PoolIdListV1beta{
			PoolUuids: []string{"uuid1", "uuid2"},
		}

		// Mock VCP to return nil (triggering CVP fallback)
		mockOrchestrator.EXPECT().GetMultiplePools(mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		// Mock location validation
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaGetMultiplePools(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		// Should return OK response with empty pools from CVP fallback
		okResp, ok := result.(*gcpgenserver.V1betaGetMultiplePoolsOK)
		assert.True(tt, ok)
		assert.Len(tt, okResp.Pools, 0) // CVP fallback returns empty pools
	})
}

func TestV1betaCreatePool(t *testing.T) {
	t.Run("WhenUnifiedIsNotSetToTrue", func(tt *testing.T) {
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
			Unified: gcpgenserver.NewOptBool(false),
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaCreatePool(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, float64(400), result.(*gcpgenserver.V1betaCreatePoolBadRequest).Code)
		assert.Equal(tt, "type must be set to UNIFIED, or unified/unifiedPool must be set to true (for backward compatibility)", result.(*gcpgenserver.V1betaCreatePoolBadRequest).Message)
	})
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
		assert.Equal(tt, "type must be set to UNIFIED, or unified/unifiedPool must be set to true (for backward compatibility)", result.(*gcpgenserver.V1betaCreatePoolBadRequest).Message)
	})
	t.Run("WhenRegionAndZoneParsingFails", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaCreatePoolParams{
			LocationId:    "invalid-location-id",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.PoolV1beta{
			Unified: gcpgenserver.NewOptBool(true),
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
			Unified:                   gcpgenserver.NewOptBool(true),
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
			Unified:     gcpgenserver.NewOptBool(true),
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
	t.Run("WhenRegionalPoolSupportIsNotEnabled", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)

		// Create a request with regional pool parameters
		req := &gcpgenserver.PoolV1beta{
			Unified:       gcpgenserver.NewOptBool(true),
			Zone:          gcpgenserver.NewOptString("us-east2-b"),
			SecondaryZone: gcpgenserver.NewOptString("us-east2-a"),
		}

		params := gcpgenserver.V1betaCreatePoolParams{
			LocationId:    "us-east2",
			ProjectNumber: "project-number",
		}

		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east2", "", nil
		}

		handler := Handler{Orchestrator: mockOrchestrator}

		// Call the function
		result, err := handler.V1betaCreatePool(context.Background(), req, params)

		// Assert the error
		assert.NotNil(tt, result)
		assert.NoError(tt, err)
		assert.Equal(tt, float64(400), result.(*gcpgenserver.V1betaCreatePoolBadRequest).Code)
		assert.Equal(tt, "Regional Pool Support is not enabled", result.(*gcpgenserver.V1betaCreatePoolBadRequest).Message)
	})
	t.Run("WhenRegionalPoolSupportIsEnabled", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		// Mock the environment variable to enable regional pool support
		originalRegionalPoolEnabled := regionalPoolEnabled
		regionalPoolEnabled = true
		defer func() { regionalPoolEnabled = originalRegionalPoolEnabled }()

		// Create a request with regional pool parameters
		req := &gcpgenserver.PoolV1beta{
			Unified:       gcpgenserver.NewOptBool(true),
			Zone:          gcpgenserver.NewOptString("us-east1-a"),
			SecondaryZone: gcpgenserver.NewOptString("us-east1-b"),
		}

		params := gcpgenserver.V1betaCreatePoolParams{
			LocationId:    "us-east1",
			ProjectNumber: "project-number",
		}

		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east1", "", nil
		}

		mockOrchestrator.EXPECT().GetPoolByVendorID(mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundErr("not found", nil))
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
	t.Run("WhenZoneIsEmpty", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		originalRegionalPoolEnabled := regionalPoolEnabled
		regionalPoolEnabled = true
		defer func() { regionalPoolEnabled = originalRegionalPoolEnabled }()

		params := gcpgenserver.V1betaCreatePoolParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.PoolV1beta{
			Unified:      gcpgenserver.NewOptBool(true),
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
		originalRegionalPoolEnabled := regionalPoolEnabled
		regionalPoolEnabled = true
		defer func() { regionalPoolEnabled = originalRegionalPoolEnabled }()
		params := gcpgenserver.V1betaCreatePoolParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.PoolV1beta{
			Unified:      gcpgenserver.NewOptBool(true),
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
			Unified:       gcpgenserver.NewOptBool(true),
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
		assert.Equal(tt, "Multiple Zone values cannot be passed for Zonal Pool Creation", result.(*gcpgenserver.V1betaCreatePoolBadRequest).Message)
	})
	t.Run("WhenPoolAlreadyExists", func(tt *testing.T) {
		originalRegionalPoolEnabled := regionalPoolEnabled
		regionalPoolEnabled = true
		defer func() { regionalPoolEnabled = originalRegionalPoolEnabled }()
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaCreatePoolParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
		}

		req := &gcpgenserver.PoolV1beta{
			Unified:                  gcpgenserver.NewOptBool(true),
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

		mockOrchestrator.EXPECT().GetPoolByVendorID(mock.Anything, mock.Anything, mock.Anything).Return(existingPool, nil)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaCreatePool(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		conflictResp, ok := result.(*gcpgenserver.V1betaCreatePoolConflict)
		assert.True(tt, ok, "Expected V1betaCreatePoolConflict response")
		assert.NotNil(tt, conflictResp)
	})
	t.Run("WhenPoolCreationFailsWithUserInputValidationError", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		originalRegionalPoolEnabled := regionalPoolEnabled
		regionalPoolEnabled = true
		defer func() { regionalPoolEnabled = originalRegionalPoolEnabled }()
		params := gcpgenserver.V1betaCreatePoolParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
		}

		req := &gcpgenserver.PoolV1beta{
			Unified:                  gcpgenserver.NewOptBool(true),
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

		mockOrchestrator.EXPECT().GetPoolByVendorID(mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundErr("not found", nil))
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
		originalRegionalPoolEnabled := regionalPoolEnabled
		regionalPoolEnabled = true
		defer func() { regionalPoolEnabled = originalRegionalPoolEnabled }()
		params := gcpgenserver.V1betaCreatePoolParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
		}

		labels := make(map[string]string)
		labels["test"] = "label"

		req := &gcpgenserver.PoolV1beta{
			Unified:                  gcpgenserver.NewOptBool(true),
			ServiceLevel:             gcpgenserver.PoolV1betaServiceLevelFLEX,
			SizeInBytes:              1099511627776,
			QosType:                  gcpgenserver.NewOptNilString("auto"),
			Zone:                     gcpgenserver.NewOptString("us-east4-a"),
			SecondaryZone:            gcpgenserver.NewOptString("us-east4-b"),
			CustomPerformanceEnabled: gcpgenserver.NewOptBool(true),
			TotalThroughputMibps:     gcpgenserver.NewOptNilFloat64(64), // 128 MiBps
			Labels:                   gcpgenserver.NewOptPoolV1betaLabels(labels),
		}

		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone

		defer func() {
			parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
			getAndSyncKmsConfigForPool = _getAndSyncKmsConfigForPool
		}()

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "", nil
		}

		getAndSyncKmsConfigForPool = func(ctx context.Context, req *gcpgenserver.PoolV1beta, params *common.CreatePoolParams, orchestratorInterface orchestrator.OrchestratorFactory) (*models.KmsConfig, gcpgenserver.V1betaCreatePoolRes) {
			return nil, nil
		}

		mockOrchestrator.EXPECT().GetPoolByVendorID(mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundErr("not found", nil))
		mockOrchestrator.EXPECT().CreatePool(mock.Anything, mock.Anything).Return(&models.Pool{BaseModel: models.BaseModel{UUID: "new-pool-uuid"}, PoolAttributes: &models.PoolAttributes{Labels: labels}}, "operation-id", nil)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		operationID := fmt.Sprintf("/v1beta/projects/%s/locations/%s/operations/%s", params.ProjectNumber, params.LocationId, "operation-id")
		result, err := handler.V1betaCreatePool(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, operationID, result.(*gcpgenserver.OperationV1beta).Name.Value)
	})
	// Test cases for hotTierSizeInBytes assignment logic - keep only the essential integration tests
	t.Run("HotTierSizeInBytes assignment when auto-tiering is enabled", func(tt *testing.T) {
		// Save and restore the original auto-tiering state
		originalAutoTieringEnabled := autoTieringEnabled
		defer func() { autoTieringEnabled = originalAutoTieringEnabled }()
		autoTieringEnabled = true // Enable auto-tiering for this test

		const (
			poolSize    = 2199023255552 // 2 TiB
			hotTierSize = 1099511627776 // 1 TiB
		)

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaCreatePoolParams{
			LocationId:    "us-east4-a",
			ProjectNumber: "project-number",
		}

		req := &gcpgenserver.PoolV1beta{
			ResourceId:               "test-pool",
			Unified:                  gcpgenserver.NewOptBool(true),
			ServiceLevel:             gcpgenserver.PoolV1betaServiceLevelFLEX,
			SizeInBytes:              poolSize,
			QosType:                  gcpgenserver.NewOptNilString("auto"),
			AllowAutoTiering:         gcpgenserver.NewOptNilBool(true),
			HotTierSizeInBytes:       gcpgenserver.NewOptNilFloat64(float64(hotTierSize)),
			EnableHotTierAutoResize:  gcpgenserver.NewOptNilBool(true),
			CustomPerformanceEnabled: gcpgenserver.NewOptBool(true),
			TotalThroughputMibps:     gcpgenserver.NewOptNilFloat64(64),
			Network:                  "test-network",
		}

		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4-a", nil
		}

		// Mock that pool doesn't exist
		mockOrchestrator.EXPECT().GetPoolByVendorID(mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundErr("not found", nil))

		// Capture the CreatePool parameters to verify hotTierSizeInBytes assignment
		var capturedParams *common.CreatePoolParams
		mockOrchestrator.EXPECT().CreatePool(mock.Anything, mock.MatchedBy(func(params *common.CreatePoolParams) bool {
			capturedParams = params
			return true
		})).Return(&models.Pool{BaseModel: models.BaseModel{UUID: "new-pool-uuid"}, PoolAttributes: &models.PoolAttributes{}}, "operation-id", nil)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		result, err := handler.V1betaCreatePool(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)

		// Verify that when auto-tiering is enabled, hotTierSizeInBytes is set to HotTierSizeInBytes.Value
		assert.NotNil(tt, capturedParams, "CreatePool should have been called")
		assert.Equal(tt, uint64(hotTierSize), capturedParams.HotTierSizeInBytes, "HotTierSizeInBytes should be set to the explicit hot tier size when auto-tiering is enabled")
		assert.True(tt, capturedParams.AllowAutoTiering, "AllowAutoTiering should be true")
	})

	t.Run("HotTierSizeInBytes assignment when auto-tiering is disabled", func(tt *testing.T) {
		const poolSize = 2199023255552 // 2 TiB

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaCreatePoolParams{
			LocationId:    "us-east4-a",
			ProjectNumber: "project-number",
		}

		req := &gcpgenserver.PoolV1beta{
			ResourceId:               "test-pool",
			Unified:                  gcpgenserver.NewOptBool(true),
			ServiceLevel:             gcpgenserver.PoolV1betaServiceLevelFLEX,
			SizeInBytes:              poolSize,
			QosType:                  gcpgenserver.NewOptNilString("auto"),
			AllowAutoTiering:         gcpgenserver.NewOptNilBool(false),
			CustomPerformanceEnabled: gcpgenserver.NewOptBool(true),
			TotalThroughputMibps:     gcpgenserver.NewOptNilFloat64(64),
			Network:                  "test-network",
			// HotTierSizeInBytes not set when auto-tiering is disabled
		}

		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4-a", nil
		}

		// Mock that pool doesn't exist
		mockOrchestrator.EXPECT().GetPoolByVendorID(mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundErr("not found", nil))

		// Capture the CreatePool parameters to verify hotTierSizeInBytes assignment
		var capturedParams *common.CreatePoolParams
		mockOrchestrator.EXPECT().CreatePool(mock.Anything, mock.MatchedBy(func(params *common.CreatePoolParams) bool {
			capturedParams = params
			return true
		})).Return(&models.Pool{BaseModel: models.BaseModel{UUID: "new-pool-uuid"}, PoolAttributes: &models.PoolAttributes{}}, "operation-id", nil)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaCreatePool(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)

		// Verify that when auto-tiering is disabled, hotTierSizeInBytes is set to SizeInBytes (pool size)
		assert.NotNil(tt, capturedParams, "CreatePool should have been called")
		assert.Equal(tt, uint64(poolSize), capturedParams.HotTierSizeInBytes, "HotTierSizeInBytes should be set to pool size when auto-tiering is disabled")
	})

	// Test cases for the new Type enum field
	t.Run("WhenTypeIsSetToUnified", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		originalRegionalPoolEnabled := regionalPoolEnabled
		regionalPoolEnabled = true
		defer func() { regionalPoolEnabled = originalRegionalPoolEnabled }()
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
			Type:          gcpgenserver.NewOptPoolV1betaType(gcpgenserver.PoolV1betaTypeUNIFIED),
			ResourceId:    "test-pool",
			ServiceLevel:  gcpgenserver.PoolV1betaServiceLevelFLEX,
			SizeInBytes:   1099511627776,
			Network:       "test-network",
			Zone:          gcpgenserver.NewOptString("us-east4-a"),
			SecondaryZone: gcpgenserver.NewOptString("us-east4-b"),
		}

		// Mock that pool doesn't exist
		mockOrchestrator.EXPECT().GetPoolByVendorID(mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundErr("not found", nil))
		mockOrchestrator.EXPECT().CreatePool(mock.Anything, mock.Anything).Return(&models.Pool{
			BaseModel:      models.BaseModel{UUID: "new-pool-uuid"},
			PoolAttributes: &models.PoolAttributes{},
		}, "operation-id", nil)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaCreatePool(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		// Should pass validation since Type is set to UNIFIED
		assert.IsType(tt, &gcpgenserver.OperationV1beta{}, result)
	})

	t.Run("WhenTypeIsSetToStandard", func(tt *testing.T) {
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
			Type: gcpgenserver.NewOptPoolV1betaType(gcpgenserver.PoolV1betaTypeFILE),
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaCreatePool(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, float64(400), result.(*gcpgenserver.V1betaCreatePoolBadRequest).Code)
		assert.Equal(tt, "type must be set to UNIFIED, or unified/unifiedPool must be set to true (for backward compatibility)", result.(*gcpgenserver.V1betaCreatePoolBadRequest).Message)
	})

	t.Run("WhenTypeIsSetToUnspecified", func(tt *testing.T) {
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
			Type: gcpgenserver.NewOptPoolV1betaType(gcpgenserver.PoolV1betaTypeSTORAGEPOOLTYPEUNSPECIFIED),
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaCreatePool(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, float64(400), result.(*gcpgenserver.V1betaCreatePoolBadRequest).Code)
		assert.Equal(tt, "type field cannot be STORAGE_POOL_TYPE_UNSPECIFIED", result.(*gcpgenserver.V1betaCreatePoolBadRequest).Message)
	})

	t.Run("WhenTypeIsNotSetAndUnifiedFieldsAreNotSet", func(tt *testing.T) {
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
			ResourceId:   "test-pool",
			ServiceLevel: gcpgenserver.PoolV1betaServiceLevelFLEX,
			SizeInBytes:  1099511627776,
			Network:      "test-network",
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaCreatePool(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, float64(400), result.(*gcpgenserver.V1betaCreatePoolBadRequest).Code)
		assert.Equal(tt, "type must be set to UNIFIED, or unified/unifiedPool must be set to true (for backward compatibility)", result.(*gcpgenserver.V1betaCreatePoolBadRequest).Message)
	})

	t.Run("WhenTypeIsSetToUnifiedAndUnifiedFieldIsAlsoSet", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		originalRegionalPoolEnabled := regionalPoolEnabled
		regionalPoolEnabled = true
		defer func() { regionalPoolEnabled = originalRegionalPoolEnabled }()
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
			Type:          gcpgenserver.NewOptPoolV1betaType(gcpgenserver.PoolV1betaTypeUNIFIED),
			Unified:       gcpgenserver.NewOptBool(true), // Both Type and unified are set
			ResourceId:    "test-pool",
			ServiceLevel:  gcpgenserver.PoolV1betaServiceLevelFLEX,
			SizeInBytes:   1099511627776,
			Network:       "test-network",
			Zone:          gcpgenserver.NewOptString("us-east4-a"),
			SecondaryZone: gcpgenserver.NewOptString("us-east4-b"),
		}

		// Mock that pool doesn't exist
		mockOrchestrator.EXPECT().GetPoolByVendorID(mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundErr("not found", nil))
		mockOrchestrator.EXPECT().CreatePool(mock.Anything, mock.Anything).Return(&models.Pool{
			BaseModel:      models.BaseModel{UUID: "new-pool-uuid"},
			PoolAttributes: &models.PoolAttributes{},
		}, "operation-id", nil)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaCreatePool(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		// Should pass validation since Type is set to UNIFIED
		assert.IsType(tt, &gcpgenserver.OperationV1beta{}, result)
	})

	t.Run("WhenTypeIsSetToUnifiedAndUnifiedPoolFieldIsAlsoSet", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		originalRegionalPoolEnabled := regionalPoolEnabled
		regionalPoolEnabled = true
		defer func() { regionalPoolEnabled = originalRegionalPoolEnabled }()
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
			Type:          gcpgenserver.NewOptPoolV1betaType(gcpgenserver.PoolV1betaTypeUNIFIED),
			UnifiedPool:   gcpgenserver.NewOptBool(true), // Both Type and unifiedPool are set
			ResourceId:    "test-pool",
			ServiceLevel:  gcpgenserver.PoolV1betaServiceLevelFLEX,
			SizeInBytes:   1099511627776,
			Network:       "test-network",
			Zone:          gcpgenserver.NewOptString("us-east4-a"),
			SecondaryZone: gcpgenserver.NewOptString("us-east4-b"),
		}

		// Mock that pool doesn't exist
		mockOrchestrator.EXPECT().GetPoolByVendorID(mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundErr("not found", nil))
		mockOrchestrator.EXPECT().CreatePool(mock.Anything, mock.Anything).Return(&models.Pool{
			BaseModel:      models.BaseModel{UUID: "new-pool-uuid"},
			PoolAttributes: &models.PoolAttributes{},
		}, "operation-id", nil)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaCreatePool(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		// Should pass validation since Type is set to UNIFIED
		assert.IsType(tt, &gcpgenserver.OperationV1beta{}, result)
	})

	t.Run("WhenPoolIsInCreatingState", func(tt *testing.T) {
		originalRegionalPoolEnabled := regionalPoolEnabled
		regionalPoolEnabled = true
		defer func() { regionalPoolEnabled = originalRegionalPoolEnabled }()

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaCreatePoolParams{
			LocationId:    "us-east4-a",
			ProjectNumber: "project-number",
		}

		req := &gcpgenserver.PoolV1beta{
			ResourceId:    "test-pool",
			Unified:       gcpgenserver.NewOptBool(true),
			ServiceLevel:  gcpgenserver.PoolV1betaServiceLevelFLEX,
			SizeInBytes:   1099511627776,
			Network:       "test-network",
			Zone:          gcpgenserver.NewOptString("us-east4-a"),
			SecondaryZone: gcpgenserver.NewOptString("us-east4-b"),
		}

		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "", nil
		}

		existingPool := &models.Pool{
			BaseModel:      models.BaseModel{UUID: "creating-pool-uuid"},
			State:          models.LifeCycleStateCreating,
			PoolAttributes: &models.PoolAttributes{},
		}

		job := &models.Job{
			BaseModel: models.BaseModel{UUID: "job-uuid"},
			Type:      models.JobTypeCreatePool,
			JobAttributes: &models.JobAttributes{
				ResourceUUID: "creating-pool-uuid",
			},
		}

		mockOrchestrator.EXPECT().GetPoolByVendorID(mock.Anything, mock.Anything, mock.Anything).Return(existingPool, nil)
		mockOrchestrator.EXPECT().GetJobByResourceUUID(mock.Anything, existingPool.UUID, string(models.JobTypeCreatePool)).Return(job, nil)

		handler := Handler{Orchestrator: mockOrchestrator}
		result, err := handler.V1betaCreatePool(context.Background(), req, params)

		assert.NoError(tt, err)
		operation, ok := result.(*gcpgenserver.OperationV1beta)
		assert.True(tt, ok, "Expected OperationV1beta response")
		assert.False(tt, operation.Done.Value, "Operation should not be marked as done")
		assert.Contains(tt, operation.Name.Value, "/v1beta/projects/project-number/locations/us-east4-a/operations/job-uuid")
	})

	t.Run("WhenPoolIsInCreatingStateAndJobLookupFails", func(tt *testing.T) {
		originalRegionalPoolEnabled := regionalPoolEnabled
		regionalPoolEnabled = true
		defer func() { regionalPoolEnabled = originalRegionalPoolEnabled }()

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaCreatePoolParams{
			LocationId:    "us-east4-a",
			ProjectNumber: "project-number",
		}

		req := &gcpgenserver.PoolV1beta{
			ResourceId:    "test-pool",
			Unified:       gcpgenserver.NewOptBool(true),
			ServiceLevel:  gcpgenserver.PoolV1betaServiceLevelFLEX,
			SizeInBytes:   1099511627776,
			Network:       "test-network",
			Zone:          gcpgenserver.NewOptString("us-east4-a"),
			SecondaryZone: gcpgenserver.NewOptString("us-east4-b"),
		}

		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "", nil
		}

		existingPool := &models.Pool{
			BaseModel:      models.BaseModel{UUID: "creating-pool-uuid"},
			State:          models.LifeCycleStateCreating,
			PoolAttributes: &models.PoolAttributes{},
		}

		mockOrchestrator.EXPECT().GetPoolByVendorID(mock.Anything, mock.Anything, mock.Anything).Return(existingPool, nil)
		mockOrchestrator.EXPECT().GetJobByResourceUUID(mock.Anything, existingPool.UUID, string(models.JobTypeCreatePool)).Return(nil, errors.NewNotFoundErr("job not found", nil))

		handler := Handler{Orchestrator: mockOrchestrator}
		result, err := handler.V1betaCreatePool(context.Background(), req, params)

		assert.NoError(tt, err)
		operation, ok := result.(*gcpgenserver.OperationV1beta)
		assert.True(tt, ok, "Expected OperationV1beta response")
		assert.True(tt, operation.Done.Value, "Operation should be marked as done when job lookup fails")
		assert.Contains(tt, operation.Name.Value, "/v1beta/projects/project-number/locations/us-east4-a/operations/00000000-0000-0000-0000-000000000000")
	})

	t.Run("WhenPoolIsInCreatingStateWithLargeCapacity_UsesCorrectJobType", func(tt *testing.T) {
		originalRegionalPoolEnabled := regionalPoolEnabled
		regionalPoolEnabled = true
		defer func() { regionalPoolEnabled = originalRegionalPoolEnabled }()

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaCreatePoolParams{
			LocationId:    "us-east4-a",
			ProjectNumber: "project-number",
		}

		req := &gcpgenserver.PoolV1beta{
			ResourceId:    "test-large-pool",
			Unified:       gcpgenserver.NewOptBool(true),
			ServiceLevel:  gcpgenserver.PoolV1betaServiceLevelFLEX,
			SizeInBytes:   13194139533312, // 12TiB - large capacity
			Network:       "test-network",
			Zone:          gcpgenserver.NewOptString("us-east4-a"),
			SecondaryZone: gcpgenserver.NewOptString("us-east4-b"),
			LargeCapacity: gcpgenserver.NewOptBool(true), // This is the key - large capacity pool
		}

		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4-a", nil
		}

		existingPool := &models.Pool{
			BaseModel:      models.BaseModel{UUID: "creating-pool-uuid"},
			State:          models.LifeCycleStateCreating,
			PoolAttributes: &models.PoolAttributes{},
		}

		job := &models.Job{
			BaseModel: models.BaseModel{UUID: "job-uuid"},
			Type:      models.JobTypeCreateLargePool, // Should use large pool job type
			JobAttributes: &models.JobAttributes{
				ResourceUUID: "creating-pool-uuid",
			},
		}

		mockOrchestrator.EXPECT().GetPoolByVendorID(mock.Anything, mock.Anything, mock.Anything).Return(existingPool, nil)
		// This is the key assertion - line 225 should use JobTypeCreateLargePool for large capacity pools
		mockOrchestrator.EXPECT().GetJobByResourceUUID(mock.Anything, existingPool.UUID, string(models.JobTypeCreateLargePool)).Return(job, nil)

		handler := Handler{Orchestrator: mockOrchestrator}
		result, err := handler.V1betaCreatePool(context.Background(), req, params)

		assert.NoError(tt, err)
		operation, ok := result.(*gcpgenserver.OperationV1beta)
		assert.True(tt, ok, "Expected OperationV1beta response")
		assert.False(tt, operation.Done.Value, "Operation should not be marked as done")
		assert.Contains(tt, operation.Name.Value, "/v1beta/projects/project-number/locations/us-east4-a/operations/job-uuid")
	})

	t.Run("WhenPoolIsInCreatingStateWithoutLargeCapacity_UsesCorrectJobType", func(tt *testing.T) {
		originalRegionalPoolEnabled := regionalPoolEnabled
		regionalPoolEnabled = true
		defer func() { regionalPoolEnabled = originalRegionalPoolEnabled }()

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaCreatePoolParams{
			LocationId:    "us-east4-a",
			ProjectNumber: "project-number",
		}

		req := &gcpgenserver.PoolV1beta{
			ResourceId:    "test-regular-pool",
			Unified:       gcpgenserver.NewOptBool(true),
			ServiceLevel:  gcpgenserver.PoolV1betaServiceLevelFLEX,
			SizeInBytes:   2199023255552, // 2TiB - regular pool
			Network:       "test-network",
			Zone:          gcpgenserver.NewOptString("us-east4-a"),
			SecondaryZone: gcpgenserver.NewOptString("us-east4-b"),
			LargeCapacity: gcpgenserver.NewOptBool(false), // Regular pool
		}

		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4-a", nil
		}

		existingPool := &models.Pool{
			BaseModel:      models.BaseModel{UUID: "creating-pool-uuid"},
			State:          models.LifeCycleStateCreating,
			PoolAttributes: &models.PoolAttributes{},
		}

		job := &models.Job{
			BaseModel: models.BaseModel{UUID: "job-uuid"},
			Type:      models.JobTypeCreatePool, // Should use regular pool job type
			JobAttributes: &models.JobAttributes{
				ResourceUUID: "creating-pool-uuid",
			},
		}

		mockOrchestrator.EXPECT().GetPoolByVendorID(mock.Anything, mock.Anything, mock.Anything).Return(existingPool, nil)
		// This is the key assertion - line 225 should use JobTypeCreatePool for regular pools
		mockOrchestrator.EXPECT().GetJobByResourceUUID(mock.Anything, existingPool.UUID, string(models.JobTypeCreatePool)).Return(job, nil)

		handler := Handler{Orchestrator: mockOrchestrator}
		result, err := handler.V1betaCreatePool(context.Background(), req, params)

		assert.NoError(tt, err)
		operation, ok := result.(*gcpgenserver.OperationV1beta)
		assert.True(tt, ok, "Expected OperationV1beta response")
		assert.False(tt, operation.Done.Value, "Operation should not be marked as done")
		assert.Contains(tt, operation.Name.Value, "/v1beta/projects/project-number/locations/us-east4-a/operations/job-uuid")
	})

	t.Run("WhenPoolIsInCreatingStateWithLargeCapacityNotSet_UsesDefaultJobType", func(tt *testing.T) {
		originalRegionalPoolEnabled := regionalPoolEnabled
		regionalPoolEnabled = true
		defer func() { regionalPoolEnabled = originalRegionalPoolEnabled }()

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaCreatePoolParams{
			LocationId:    "us-east4-a",
			ProjectNumber: "project-number",
		}

		req := &gcpgenserver.PoolV1beta{
			ResourceId:    "test-regular-pool",
			Unified:       gcpgenserver.NewOptBool(true),
			ServiceLevel:  gcpgenserver.PoolV1betaServiceLevelFLEX,
			SizeInBytes:   2199023255552, // 2TiB - regular pool
			Network:       "test-network",
			Zone:          gcpgenserver.NewOptString("us-east4-a"),
			SecondaryZone: gcpgenserver.NewOptString("us-east4-b"),
			// LargeCapacity not set - should default to false
		}

		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4-a", nil
		}

		existingPool := &models.Pool{
			BaseModel:      models.BaseModel{UUID: "creating-pool-uuid"},
			State:          models.LifeCycleStateCreating,
			PoolAttributes: &models.PoolAttributes{},
		}

		job := &models.Job{
			BaseModel: models.BaseModel{UUID: "job-uuid"},
			Type:      models.JobTypeCreatePool, // Should default to regular pool job type
			JobAttributes: &models.JobAttributes{
				ResourceUUID: "creating-pool-uuid",
			},
		}

		mockOrchestrator.EXPECT().GetPoolByVendorID(mock.Anything, mock.Anything, mock.Anything).Return(existingPool, nil)
		// This verifies that common.GetBoolOrDefault(req.LargeCapacity, false) defaults to false when LargeCapacity is not set
		mockOrchestrator.EXPECT().GetJobByResourceUUID(mock.Anything, existingPool.UUID, string(models.JobTypeCreatePool)).Return(job, nil)

		handler := Handler{Orchestrator: mockOrchestrator}
		result, err := handler.V1betaCreatePool(context.Background(), req, params)

		assert.NoError(tt, err)
		operation, ok := result.(*gcpgenserver.OperationV1beta)
		assert.True(tt, ok, "Expected OperationV1beta response")
		assert.False(tt, operation.Done.Value, "Operation should not be marked as done")
		assert.Contains(tt, operation.Name.Value, "/v1beta/projects/project-number/locations/us-east4-a/operations/job-uuid")
	})

	t.Run("WhenGetKmsConfigForPoolFails", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		originalRegionalPoolEnabled := regionalPoolEnabled
		regionalPoolEnabled = true
		defer func() { regionalPoolEnabled = originalRegionalPoolEnabled }()
		params := gcpgenserver.V1betaCreatePoolParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
		}
		labels := make(map[string]string)
		labels["test"] = "label"
		kmsConfigUUID := "kms-config-uuid"

		req := &gcpgenserver.PoolV1beta{
			Unified:                  gcpgenserver.NewOptBool(true),
			ServiceLevel:             gcpgenserver.PoolV1betaServiceLevelFLEX,
			SizeInBytes:              1099511627776,
			QosType:                  gcpgenserver.NewOptNilString("auto"),
			Zone:                     gcpgenserver.NewOptString("us-east4-a"),
			SecondaryZone:            gcpgenserver.NewOptString("us-east4-b"),
			CustomPerformanceEnabled: gcpgenserver.NewOptBool(true),
			TotalThroughputMibps:     gcpgenserver.NewOptNilFloat64(64), // 128 MiBps
			Labels:                   gcpgenserver.NewOptPoolV1betaLabels(labels),
			KmsConfigId:              gcpgenserver.NewOptNilString(kmsConfigUUID),
		}

		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() {
			parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
			getAndSyncKmsConfigForPool = _getAndSyncKmsConfigForPool
		}()

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "", nil
		}

		getAndSyncKmsConfigForPool = func(ctx context.Context, req *gcpgenserver.PoolV1beta, params *common.CreatePoolParams, orchestratorInterface orchestrator.OrchestratorFactory) (*models.KmsConfig, gcpgenserver.V1betaCreatePoolRes) {
			return nil, &gcpgenserver.V1betaCreatePoolBadRequest{
				Code:    http.StatusBadRequest,
				Message: fmt.Sprintf("KMS Config with ID %s not found", req.KmsConfigId.Value),
			}
		}

		mockOrchestrator.EXPECT().GetPoolByVendorID(mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundErr("not found", nil))

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaCreatePool(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, float64(http.StatusBadRequest), result.(*gcpgenserver.V1betaCreatePoolBadRequest).Code)
	})
}

func TestV1betaUpdatePoolValidationErrors(t *testing.T) {
	validationErrorCases := []struct {
		name    string
		req     *gcpgenserver.PoolUpdateV1beta
		message string
	}{
		{
			name: "Zone is set",
			req: &gcpgenserver.PoolUpdateV1beta{
				Zone: gcpgenserver.NewOptString("us-east4-b"),
			},
			message: "Migrating to a different Zone is currently not supported",
		},
		{
			name: "GlobalAccessAllowed is set to true",
			req: &gcpgenserver.PoolUpdateV1beta{
				GlobalAccessAllowed: gcpgenserver.NewOptNilBool(true),
			},
			message: "Updating Global access is currently not supported",
		},
		{
			name: "GlobalAccessAllowed is set to false",
			req: &gcpgenserver.PoolUpdateV1beta{
				GlobalAccessAllowed: gcpgenserver.NewOptNilBool(false),
			},
			message: "Updating Global access is currently not supported",
		},
		{
			name: "ActiveDirectoryConfigId is set",
			req: &gcpgenserver.PoolUpdateV1beta{
				ActiveDirectoryConfigId: gcpgenserver.NewOptNilString("some-ad-id"),
			},
			message: "Updating Active Directory is currently not supported",
		},
		{
			name: "AllowAutoTiering is set to true",
			req: &gcpgenserver.PoolUpdateV1beta{
				AllowAutoTiering: gcpgenserver.NewOptNilBool(true),
			},
			message: "Updating Auto tiering is currently not supported",
		},
		{
			name: "AllowAutoTiering is set to false",
			req: &gcpgenserver.PoolUpdateV1beta{
				AllowAutoTiering: gcpgenserver.NewOptNilBool(false),
			},
			message: "Updating Auto tiering is currently not supported",
		},
		{
			name: "HotTierSizeInBytes is set",
			req: &gcpgenserver.PoolUpdateV1beta{
				HotTierSizeInBytes: gcpgenserver.NewOptNilFloat64(1024),
			},
			message: "Updating HotTierSize is currently not supported",
		},
		{
			name: "EnableHotTierAutoResize is set to false",
			req: &gcpgenserver.PoolUpdateV1beta{
				EnableHotTierAutoResize: gcpgenserver.NewOptNilBool(false),
			},
			message: "Updating HotTier auto resize is currently not supported",
		},
		{
			name: "EnableHotTierAutoResize is set to true",
			req: &gcpgenserver.PoolUpdateV1beta{
				EnableHotTierAutoResize: gcpgenserver.NewOptNilBool(true),
			},
			message: "Updating HotTier auto resize is currently not supported",
		},
		{
			name: "Shrink pool size",
			req: &gcpgenserver.PoolUpdateV1beta{
				SizeInBytes: gcpgenserver.NewOptNilFloat64(1073741824), // 1 GiB
			},
			message: "Pool size cannot be reduced",
		},
		{
			name: "QosType is set to manual",
			req: &gcpgenserver.PoolUpdateV1beta{
				QosType: gcpgenserver.NewOptNilString("manual"),
			},
			message: "Updating QosType is currently not supported",
		},
		{
			name: "QosType is set to invalid value",
			req: &gcpgenserver.PoolUpdateV1beta{
				QosType: gcpgenserver.NewOptNilString("invalid-qos-type"),
			},
			message: "Updating QosType is currently not supported",
		},
		{
			name: "CustomPerformanceEnabled is set to false",
			req: &gcpgenserver.PoolUpdateV1beta{
				CustomPerformanceEnabled: gcpgenserver.NewOptNilBool(false),
			},
			message: "Updating CustomerPerformance is currently not supported",
		},
		{
			name: "CustomPerformanceEnabled is set to true",
			req: &gcpgenserver.PoolUpdateV1beta{
				CustomPerformanceEnabled: gcpgenserver.NewOptNilBool(true),
			},
			message: "Updating CustomerPerformance is currently not supported",
		},
	}

	for _, tc := range validationErrorCases {
		t.Run(tc.name, func(tt *testing.T) {
			mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
			params := gcpgenserver.V1betaUpdatePoolParams{
				LocationId:    "us-east4",
				ProjectNumber: "project-number",
				PoolId:        "pool-id",
			}

			originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
			defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()

			parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
				return "us-east4", "", nil
			}

			// Set orchestrator to return a pool when DescribePool is called.
			mockOrchestrator.EXPECT().DescribePool(mock.Anything, mock.Anything, mock.Anything).Return(&models.Pool{
				BaseModel: models.BaseModel{
					UUID: "pool-uuid",
				},
				Description: "original description",
				SizeInBytes: 1099511627776, // 1 TiB
				CustomPerformanceParams: &models.CustomPerformanceParams{
					Throughput: 64, // 64 MiBps
					Iops:       1024,
				},
				PoolAttributes: &models.PoolAttributes{
					PrimaryZone: "us-east4-a",
				},
				State: "READY",
			}, nil)

			handler := Handler{
				Orchestrator: mockOrchestrator,
			}
			result, err := handler.V1betaUpdatePool(context.Background(), tc.req, params)

			assert.NoError(tt, err)
			assert.NotNil(tt, result)
			// Check if result is BadRequest error (most common for validation errors)
			if badReq, ok := result.(*gcpgenserver.V1betaUpdatePoolBadRequest); ok {
				assert.Equal(tt, float64(400), badReq.Code)
				assert.Equal(tt, tc.message, badReq.Message)
			} else if conflict, ok := result.(*gcpgenserver.V1betaUpdatePoolConflict); ok {
				// If it's a conflict error, check its message content
				assert.Equal(tt, float64(409), conflict.Code)
				assert.Equal(tt, tc.message, conflict.Message)
			} else {
				tt.Fatalf("Unexpected response type: %T", result)
			}
		})
	}

	t.Run("TestOngoingUpdatePoolOperationScenario", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaUpdatePoolParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
			PoolId:        "pool-id",
		}

		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "", nil
		}

		// Set orchestrator to return a pool when DescribePool is called.
		mockOrchestrator.EXPECT().DescribePool(mock.Anything, mock.Anything, mock.Anything).Return(&models.Pool{
			BaseModel: models.BaseModel{
				UUID: "pool-uuid",
			},
			Description: "original description",
			SizeInBytes: 1099511627776, // 1 TiB
			State:       "UPDATING",
		}, nil)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaUpdatePool(context.Background(), &gcpgenserver.PoolUpdateV1beta{
			SizeInBytes: gcpgenserver.NewOptNilFloat64(2199023255552), // 2 TiB
		}, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		// Check if result is BadRequest error (most common for validation errors)
		if conflict, ok := result.(*gcpgenserver.V1betaUpdatePoolConflict); ok {
			// If it's a conflict error, check its message content
			assert.Equal(tt, float64(409), conflict.Code)
			assert.Equal(tt, "An update operation is already in progress for this pool", conflict.Message)
		} else {
			tt.Fatalf("Unexpected response type: %T", result)
		}
	})
}

func TestV1betaUpdatePool(t *testing.T) {
	// Save original parseAndValidateRegionAndZone function and restore at end of test.
	originalParseAndValidate := parseAndValidateRegionAndZone
	t.Run("WhenRegionAndZoneParsingFails", func(tt *testing.T) {
		// Set the function to return a parsing error.
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "", "", &gcpgenserver.Error{
				Code:    400,
				Message: "Invalid location ID",
			}
		}
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidate }()

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaUpdatePoolParams{
			LocationId:    "invalid-location",
			ProjectNumber: "project-number",
			PoolId:        "pool-id",
		}
		req := &gcpgenserver.PoolUpdateV1beta{
			Description: gcpgenserver.NewOptNilString("description"),
		}
		handler := Handler{Orchestrator: mockOrchestrator}
		result, err := handler.V1betaUpdatePool(context.Background(), req, params)
		assert.NoError(tt, err)
		badReq, ok := result.(*gcpgenserver.V1betaUpdatePoolBadRequest)
		assert.True(tt, ok)
		assert.Equal(tt, float64(400), badReq.Code)
		assert.Equal(tt, "Invalid location ID", badReq.Message)
	})
	t.Run("WhenGetPoolFails", func(tt *testing.T) {
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidate }()

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		// Set orchestrator to return a pool when DescribePool is called.
		mockOrchestrator.EXPECT().DescribePool(mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundErr("pool not found", nil))

		params := gcpgenserver.V1betaUpdatePoolParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
			PoolId:        "pool-id",
		}
		req := &gcpgenserver.PoolUpdateV1beta{
			Description:          gcpgenserver.NewOptNilString("updated description"),
			SizeInBytes:          gcpgenserver.NewOptNilFloat64(1099511627776),
			TotalThroughputMibps: gcpgenserver.NewOptNilFloat64(128),
			TotalIops:            gcpgenserver.NewOptNilFloat64(2048),
		}
		handler := Handler{Orchestrator: mockOrchestrator}
		result, err := handler.V1betaUpdatePool(context.Background(), req, params)
		assert.NoError(tt, err)
		notFoundErr, ok := result.(*gcpgenserver.V1betaUpdatePoolNotFound)
		assert.True(tt, ok)
		assert.Equal(tt, float64(404), notFoundErr.Code)
		assert.Equal(tt, "Pool not found", notFoundErr.Message)
	})
	t.Run("WhenUpdatePoolFails", func(tt *testing.T) {
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidate }()

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		// Set orchestrator to return a pool when DescribePool is called.
		mockOrchestrator.EXPECT().DescribePool(mock.Anything, mock.Anything, mock.Anything).Return(&models.Pool{
			BaseModel: models.BaseModel{
				UUID: "pool-uuid",
			},
			Description: "original description",
			SizeInBytes: 1099511627776, // 1 TiB
			CustomPerformanceParams: &models.CustomPerformanceParams{
				Throughput: 64, // 64 MiBps
				Iops:       1024,
			},
			PoolAttributes: &models.PoolAttributes{
				PrimaryZone: "us-east4-a",
			},
			State: "READY",
		}, nil)
		// Set orchestrator to return an error when UpdatePool is called.
		mockOrchestrator.EXPECT().UpdatePool(mock.Anything, mock.Anything).
			Return(nil, "", fmt.Errorf("update failed"))

		params := gcpgenserver.V1betaUpdatePoolParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
			PoolId:        "pool-id",
		}
		req := &gcpgenserver.PoolUpdateV1beta{
			Description:          gcpgenserver.NewOptNilString("updated description"),
			SizeInBytes:          gcpgenserver.NewOptNilFloat64(1099511627776),
			TotalThroughputMibps: gcpgenserver.NewOptNilFloat64(128),
			TotalIops:            gcpgenserver.NewOptNilFloat64(2048),
		}
		handler := Handler{Orchestrator: mockOrchestrator}
		result, err := handler.V1betaUpdatePool(context.Background(), req, params)
		assert.NoError(tt, err)
		internalErr, ok := result.(*gcpgenserver.V1betaUpdatePoolInternalServerError)
		assert.True(tt, ok)
		assert.Equal(tt, float64(500), internalErr.Code)
		assert.Equal(tt, "update failed", internalErr.Message)
	})
	t.Run("WhenUpdatePoolFailsDueToConflict", func(tt *testing.T) {
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidate }()

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		// Set orchestrator to return a pool when DescribePool is called.
		mockOrchestrator.EXPECT().DescribePool(mock.Anything, mock.Anything, mock.Anything).Return(&models.Pool{
			BaseModel: models.BaseModel{
				UUID: "pool-uuid",
			},
			Description: "original description",
			SizeInBytes: 1099511627776, // 1 TiB
			CustomPerformanceParams: &models.CustomPerformanceParams{
				Throughput: 64, // 64 MiBps
				Iops:       1024,
			},
			PoolAttributes: &models.PoolAttributes{
				PrimaryZone: "us-east4-a",
			},
		}, nil)
		// Set orchestrator to return an error when UpdatePool is called.
		mockOrchestrator.EXPECT().UpdatePool(mock.Anything, mock.Anything).
			Return(nil, "", errors.NewConflictErr("Pool is already transitioning between states"))

		params := gcpgenserver.V1betaUpdatePoolParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
			PoolId:        "pool-id",
		}
		req := &gcpgenserver.PoolUpdateV1beta{
			Description:          gcpgenserver.NewOptNilString("updated description"),
			SizeInBytes:          gcpgenserver.NewOptNilFloat64(1099511627776),
			TotalThroughputMibps: gcpgenserver.NewOptNilFloat64(128),
			TotalIops:            gcpgenserver.NewOptNilFloat64(2048),
		}
		handler := Handler{Orchestrator: mockOrchestrator}
		result, err := handler.V1betaUpdatePool(context.Background(), req, params)
		assert.NoError(tt, err)
		internalErr, ok := result.(*gcpgenserver.V1betaUpdatePoolConflict)
		assert.True(tt, ok)
		assert.Equal(tt, float64(409), internalErr.Code)
		assert.Equal(tt, "Pool is already transitioning between states", internalErr.Message)
	})
	t.Run("WhenPoolUpdateSucceeds", func(tt *testing.T) {
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "", nil
		}
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidate }()

		// Create a dummy pool that represents the updated pool.
		updatedPool := &models.Pool{
			BaseModel: models.BaseModel{
				UUID: "updated-pool-uuid",
			},
			PoolAttributes: &models.PoolAttributes{},
		}

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		// Set orchestrator to return a pool when DescribePool is called.
		mockOrchestrator.EXPECT().DescribePool(mock.Anything, mock.Anything, mock.Anything).Return(&models.Pool{
			BaseModel: models.BaseModel{
				UUID: "pool-uuid",
			},
			Description: "original description",
			SizeInBytes: 1099511627776, // 1 TiB
			CustomPerformanceParams: &models.CustomPerformanceParams{
				Throughput: 64, // 64 MiBps
				Iops:       1024,
			},
			PoolAttributes: &models.PoolAttributes{
				PrimaryZone: "us-east4-a",
			},
		}, nil)
		mockOrchestrator.EXPECT().UpdatePool(mock.Anything, mock.Anything).
			Return(updatedPool, "op-123", nil)

		params := gcpgenserver.V1betaUpdatePoolParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
			PoolId:        "pool-id",
		}
		labels := make(map[string]string)
		labels["test"] = "label"
		req := &gcpgenserver.PoolUpdateV1beta{
			Description:          gcpgenserver.NewOptNilString("updated description"),
			SizeInBytes:          gcpgenserver.NewOptNilFloat64(1099511627776),
			TotalThroughputMibps: gcpgenserver.NewOptNilFloat64(128),
			TotalIops:            gcpgenserver.NewOptNilFloat64(2048),
			Labels:               gcpgenserver.OptPoolUpdateV1betaLabels{Value: labels, Set: true},
		}
		handler := Handler{Orchestrator: mockOrchestrator}

		result, err := handler.V1betaUpdatePool(context.Background(), req, params)
		assert.NoError(tt, err)
		op, ok := result.(*gcpgenserver.OperationV1beta)
		assert.True(tt, ok)
		expectedOpName := fmt.Sprintf("/v1beta/projects/%s/locations/%s/operations/%s", params.ProjectNumber, params.LocationId, "op-123")
		assert.Equal(tt, expectedOpName, op.Name.Value)
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
		mockOrchestrator.EXPECT().DescribePool(mock.Anything, params.PoolId, params.ProjectNumber).Return(nil, errors.NewNotFoundErr("not found", nil))

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaDeletePool(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, float64(404), result.(*gcpgenserver.V1betaDeletePoolNotFound).Code)
		assert.Equal(tt, "Pool not found", result.(*gcpgenserver.V1betaDeletePoolNotFound).Message)
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
		mockOrchestrator.EXPECT().DescribePool(mock.Anything, params.PoolId, params.ProjectNumber).Return(existingPool, nil)
		mockOrchestrator.EXPECT().DeletePool(mock.Anything, mock.Anything).Return(nil, "", errors.NewConflictErr("pool has active volumes"))

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaDeletePool(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, float64(409), result.(*gcpgenserver.V1betaDeletePoolConflict).Code)
		assert.Equal(tt, "Pool has active volumes", result.(*gcpgenserver.V1betaDeletePoolConflict).Message)
	})
	t.Run("WhenPoolIsAlreadyDeleted", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaDeletePoolParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
			PoolId:        "deletable-pool-id",
		}

		createdAt := time.Now()
		existingPool := &models.Pool{
			BaseModel: models.BaseModel{
				UUID:      "deletable-pool-id",
				CreatedAt: createdAt,
				UpdatedAt: createdAt,
				DeletedAt: &createdAt,
			},
			PoolAttributes: &models.PoolAttributes{},
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		mockOrchestrator.EXPECT().DescribePool(mock.Anything, params.PoolId, params.ProjectNumber).Return(existingPool, nil)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaDeletePool(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, true, result.(*gcpgenserver.OperationV1beta).Done.Value)
	})
	t.Run("WhenPoolDeletionRaceCondition", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaDeletePoolParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
			PoolId:        "deletable-pool-id",
		}

		createdAt := time.Now()
		existingPool := &models.Pool{
			BaseModel: models.BaseModel{
				UUID:      "deletable-pool-id",
				CreatedAt: createdAt,
				UpdatedAt: createdAt,
			},
			PoolAttributes: &models.PoolAttributes{},
		}
		deletePoolParams := &common.DeletePoolParams{
			AccountName: params.ProjectNumber,
			PoolID:      existingPool.UUID,
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		mockOrchestrator.EXPECT().DescribePool(mock.Anything, params.PoolId, params.ProjectNumber).Return(existingPool, nil)
		mockOrchestrator.EXPECT().DeletePool(mock.Anything, deletePoolParams).Return(existingPool, "", errors.NewNotFoundErr("pool not found", nil))

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaDeletePool(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, true, result.(*gcpgenserver.OperationV1beta).Done.Value)
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
		mockOrchestrator.EXPECT().DescribePool(mock.Anything, params.PoolId, params.ProjectNumber).Return(existingPool, nil)
		mockOrchestrator.EXPECT().DeletePool(mock.Anything, mock.Anything).Return(deletedPool, "operation-id", nil)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaDeletePool(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "/v1beta/projects/project-number/locations/us-east4/operations/operation-id", result.(*gcpgenserver.OperationV1beta).Name.Value)
	})
	t.Run("WhenPoolIsInCreatingState", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaDeletePoolParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
			PoolId:        "creating-pool-id",
		}

		existingPool := &models.Pool{
			BaseModel: models.BaseModel{
				UUID: "creating-pool-id",
			},
			State:          models.LifeCycleStateCreating,
			PoolAttributes: &models.PoolAttributes{},
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		mockOrchestrator.EXPECT().DescribePool(mock.Anything, params.PoolId, params.ProjectNumber).Return(existingPool, nil)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaDeletePool(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, float64(409), result.(*gcpgenserver.V1betaDeletePoolConflict).Code)
		assert.Equal(tt, "Error deleting pool - Pool is already transitioning between states", result.(*gcpgenserver.V1betaDeletePoolConflict).Message)
	})
	t.Run("WhenPoolIsInUpdatingState", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaDeletePoolParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
			PoolId:        "updating-pool-id",
		}

		existingPool := &models.Pool{
			BaseModel: models.BaseModel{
				UUID: "updating-pool-id",
			},
			State:          models.LifeCycleStateUpdating,
			PoolAttributes: &models.PoolAttributes{},
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		mockOrchestrator.EXPECT().DescribePool(mock.Anything, params.PoolId, params.ProjectNumber).Return(existingPool, nil)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaDeletePool(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, float64(409), result.(*gcpgenserver.V1betaDeletePoolConflict).Code)
		assert.Equal(tt, "Error deleting pool - Pool is already transitioning between states", result.(*gcpgenserver.V1betaDeletePoolConflict).Message)
	})
	t.Run("WhenPoolIsInDeletingStateAndJobExists_ThenReturnTheSameJob", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaDeletePoolParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
			PoolId:        "deleting-pool-id",
		}

		existingPool := &models.Pool{
			BaseModel: models.BaseModel{
				UUID: "deleting-pool-id",
			},
			State:          models.LifeCycleStateDeleting,
			PoolAttributes: &models.PoolAttributes{},
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}

		job := &models.Job{
			BaseModel: models.BaseModel{UUID: "job-uuid"},
			Type:      models.JobTypeDeletePool,
			JobAttributes: &models.JobAttributes{
				ResourceUUID: "deleting-pool-id",
			},
			State: models.JobsStateNEW,
		}
		mockOrchestrator.EXPECT().DescribePool(mock.Anything, params.PoolId, params.ProjectNumber).Return(existingPool, nil)
		mockOrchestrator.EXPECT().GetJobByResourceUUID(mock.Anything, existingPool.UUID, string(models.JobTypeDeletePool)).Return(job, nil)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaDeletePool(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, result.(*gcpgenserver.OperationV1beta).Name.Value, "/v1beta/projects/project-number/locations/us-east4/operations/job-uuid")
		assert.False(tt, result.(*gcpgenserver.OperationV1beta).Done.Value)
	})
	t.Run("WhenPoolIsInDeletingStateAndJobDoesNotExists_ThenReturnStaticJobWithDone", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaDeletePoolParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
			PoolId:        "deleting-pool-id",
		}

		existingPool := &models.Pool{
			BaseModel: models.BaseModel{
				UUID: "deleting-pool-id",
			},
			State:          models.LifeCycleStateDeleting,
			PoolAttributes: &models.PoolAttributes{},
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}

		mockOrchestrator.EXPECT().DescribePool(mock.Anything, params.PoolId, params.ProjectNumber).Return(existingPool, nil)
		mockOrchestrator.EXPECT().GetJobByResourceUUID(mock.Anything, existingPool.UUID, string(models.JobTypeDeletePool)).Return(nil, errors.NewNotFoundErr("not found", nil))

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaDeletePool(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, result.(*gcpgenserver.OperationV1beta).Name.Value, "/v1beta/projects/project-number/locations/us-east4/operations/00000000-0000-0000-0000-000000000000")
		assert.True(tt, result.(*gcpgenserver.OperationV1beta).Done.Value)
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

		mockOrchestrator.EXPECT().DescribePool(mock.Anything, params.PoolId, params.ProjectNumber).Return(nil, errors.NewNotFoundErr("not found", nil))

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaDescribePool(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, float64(404), result.(*gcpgenserver.V1betaDescribePoolNotFound).Code)
		assert.Equal(tt, "Pool not found", result.(*gcpgenserver.V1betaDescribePoolNotFound).Message)
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

		mockOrchestrator.EXPECT().DescribePool(mock.Anything, params.PoolId, params.ProjectNumber).Return(nil, fmt.Errorf("internal error"))

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
			PoolAttributes: &models.PoolAttributes{
				Labels: map[string]string{"test": "label"},
			},
			Name:        "test-pool",
			Description: "This is a test pool",
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		mockOrchestrator.EXPECT().DescribePool(mock.Anything, params.PoolId, params.ProjectNumber).Return(existingPool, nil)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaDescribePool(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "existing-pool-id", result.(*gcpgenserver.PoolV1beta).PoolId.Value)
		assert.Equal(tt, "This is a test pool", result.(*gcpgenserver.PoolV1beta).Description.Value)
		assert.Equal(tt, existingPool.PoolAttributes.Labels["test"], result.(*gcpgenserver.PoolV1beta).Labels.Value["test"])
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
		mockOrchestrator.EXPECT().ListPools(mock.Anything, params.ProjectNumber, false).Return(nil, fmt.Errorf("internal error"))

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
		mockOrchestrator.EXPECT().ListPools(mock.Anything, params.ProjectNumber, false).Return([]*models.Pool{pool1, pool2}, nil)

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
	t.Run("WhenListPoolsSucceedsIncludeDeleted", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaListPoolsParams{
			LocationId:     "us-east4",
			ProjectNumber:  "project-number",
			IncludeDeleted: gcpgenserver.NewOptBool(true),
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
		mockOrchestrator.EXPECT().ListPools(mock.Anything, params.ProjectNumber, true).Return([]*models.Pool{pool1, pool2}, nil)

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

// TestValidateLabels_Valid validates that a proper label set passes without error.
func TestValidateLabels_Valid(t *testing.T) {
	labels := map[string]string{
		"env":     "prod",
		"version": "v1",
	}
	_, err := validateLabels(labels)
	assert.NoError(t, err)
}

// TestValidateLabels_ExceedLabelCount returns an error when label count exceeds 64.
func TestValidateLabels_ExceedLabelCount(t *testing.T) {
	labels := make(map[string]string)
	for i := 0; i < 65; i++ {
		labels[fmt.Sprintf("key%d", i)] = "value"
	}
	_, err := validateLabels(labels)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid label count")
}

// TestValidateLabels_EmptyKey returns an error when a label key is empty.
func TestValidateLabels_EmptyKey(t *testing.T) {
	labels := map[string]string{
		"": "somevalue",
	}
	_, err := validateLabels(labels)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "key is required in label")
}

// TestValidateLabels_KeyExceedsMaxRune returns an error when a key exceeds the max allowed runes.
func TestValidateLabels_KeyExceedsMaxRune(t *testing.T) {
	// Create a key of 64 runes (maxRuneCount is 63)
	key := strings.Repeat("a", maxRuneCount+1)
	labels := map[string]string{
		key: "value",
	}
	_, err := validateLabels(labels)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "label key")
	assert.Contains(t, err.Error(), "length can't exceed")
}

// TestValidateLabels_KeyExceedsMaxByte returns an error when a key exceeds the max allowed bytes.
func TestValidateLabels_KeyExceedsMaxByte(t *testing.T) {
	// Create a key of 129 bytes (maxByteCount is 128).
	key := string(make([]byte, maxByteCount+1))
	labels := map[string]string{
		key: "value",
	}
	_, err := validateLabels(labels)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "label key")
	assert.Contains(t, err.Error(), "length can't exceed")
}

// TestValidateLabels_ValueExceedsMaxRune returns an error when a value exceeds the max allowed runes.
func TestValidateLabels_ValueExceedsMaxRune(t *testing.T) {
	// Create a value of 64 runes (maxRuneCount is 63)
	value := strings.Repeat("b", maxRuneCount+1)
	labels := map[string]string{
		"key": value,
	}
	_, err := validateLabels(labels)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "label value")
	assert.Contains(t, err.Error(), "length can't exceed")
}

// TestValidateLabels_ValueExceedsMaxByte returns an error when a value exceeds the max allowed bytes.
func TestValidateLabels_ValueExceedsMaxByte(t *testing.T) {
	// Create a value of 129 bytes (maxByteCount is 128).
	value := string(make([]byte, maxByteCount+1))
	labels := map[string]string{
		"key": value,
	}
	_, err := validateLabels(labels)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "label value")
	assert.Contains(t, err.Error(), "length can't exceed")
}

func TestGetEnableHotTierAutoResize(t *testing.T) {
	t.Run("WhenConfigIsNil", func(tt *testing.T) {
		result := getEnableHotTierAutoResize(nil)
		assert.False(tt, result)
	})

	t.Run("WhenConfigIsNotNilAndEnableHotTierAutoResizeIsTrue", func(tt *testing.T) {
		config := &models.AutoTieringConfig{
			EnableHotTierAutoResize: true,
		}
		result := getEnableHotTierAutoResize(config)
		assert.True(tt, result)
	})
}
func TestGetHotTierSizeInBytes(t *testing.T) {
	t.Run("WhenConfigIsNil", func(tt *testing.T) {
		result := getHotTierSizeInBytes(nil)
		assert.Equal(tt, float64(0), result)
	})

	t.Run("WhenConfigIsNotNil", func(tt *testing.T) {
		config := &models.AutoTieringConfig{
			HotTierSizeInBytes: 1024,
		}
		result := getHotTierSizeInBytes(config)
		assert.Equal(tt, float64(1024), result)
	})
}

func TestValidateCreatePoolParams_AutoTieringValidation(t *testing.T) {
	const (
		minPoolHotTierSize = 1099511627776 // 1 TiB
		validPoolSize      = 2199023255552 // 2 TiB
		smallPoolSize      = 1073741824    // 1 GiB
		largePoolSize      = 5497558138880 // 5 TiB
	)

	// Save original state and restore after all tests
	originalAutoTieringEnabled := autoTieringEnabled
	defer func() { autoTieringEnabled = originalAutoTieringEnabled }()

	t.Run("Feature Gate Tests", func(tt *testing.T) {
		t.Run("AutoTiering feature disabled - AllowAutoTiering is true", func(ttt *testing.T) {
			autoTieringEnabled = false
			req := &gcpgenserver.PoolV1beta{
				Unified:          gcpgenserver.NewOptBool(true),
				SizeInBytes:      validPoolSize,
				AllowAutoTiering: gcpgenserver.NewOptNilBool(true),
			}
			zone := "us-east4-a"

			err := validateCreatePoolParams(req, zone)
			assert.NotNil(ttt, err, "Expected error when auto-tiering feature is disabled but AllowAutoTiering is true")
			assert.Equal(ttt, float64(400), err.Code)
			assert.Contains(ttt, err.Message, "Auto-Tiering feature is currently not enabled.")
		})

		t.Run("AutoTiering feature disabled - HotTierSizeInBytes is set", func(ttt *testing.T) {
			autoTieringEnabled = false
			req := &gcpgenserver.PoolV1beta{
				Unified:            gcpgenserver.NewOptBool(true),
				SizeInBytes:        validPoolSize,
				AllowAutoTiering:   gcpgenserver.NewOptNilBool(false),
				HotTierSizeInBytes: gcpgenserver.NewOptNilFloat64(float64(minPoolHotTierSize)),
			}
			zone := "us-east4-a"

			err := validateCreatePoolParams(req, zone)
			assert.NotNil(ttt, err, "Expected error when auto-tiering feature is disabled but HotTierSizeInBytes is set")
			assert.Equal(ttt, float64(400), err.Code)
			assert.Contains(ttt, err.Message, "Auto-Tiering feature is currently not enabled.")
		})

		t.Run("AutoTiering feature disabled - no auto-tiering params set (should pass)", func(ttt *testing.T) {
			autoTieringEnabled = false
			req := &gcpgenserver.PoolV1beta{
				Unified:     gcpgenserver.NewOptBool(true),
				SizeInBytes: validPoolSize,
				// Neither AllowAutoTiering nor HotTierSizeInBytes are set
			}
			zone := "us-east4-a"

			err := validateCreatePoolParams(req, zone)
			assert.Nil(ttt, err, "Expected no error when auto-tiering feature is disabled and no auto-tiering params are set")
		})
	})

	// For the rest of the tests, enable auto-tiering feature
	autoTieringEnabled = true

	t.Run("Valid Auto-Tiering Configurations", func(tt *testing.T) {
		t.Run("Valid configuration with all parameters", func(ttt *testing.T) {
			req := &gcpgenserver.PoolV1beta{
				Unified:                 gcpgenserver.NewOptBool(true),
				SizeInBytes:             validPoolSize,
				AllowAutoTiering:        gcpgenserver.NewOptNilBool(true),
				HotTierSizeInBytes:      gcpgenserver.NewOptNilFloat64(float64(minPoolHotTierSize)),
				EnableHotTierAutoResize: gcpgenserver.NewOptNilBool(true),
			}
			zone := "us-east4-a"

			err := validateCreatePoolParams(req, zone)
			assert.Nil(ttt, err, "Expected no error for valid auto-tiering parameters")
		})

		t.Run("HotTierSizeInBytes equal to pool size", func(ttt *testing.T) {
			req := &gcpgenserver.PoolV1beta{
				Unified:            gcpgenserver.NewOptBool(true),
				SizeInBytes:        validPoolSize,
				AllowAutoTiering:   gcpgenserver.NewOptNilBool(true),
				HotTierSizeInBytes: gcpgenserver.NewOptNilFloat64(float64(validPoolSize)), // Equal to pool size
			}
			zone := "us-east4-a"

			err := validateCreatePoolParams(req, zone)
			assert.Nil(ttt, err, "Expected no error when HotTierSizeInBytes equals pool size")
		})

		t.Run("Complex valid scenario - large pool with explicit auto-resize disabled", func(ttt *testing.T) {
			req := &gcpgenserver.PoolV1beta{
				Unified:                 gcpgenserver.NewOptBool(true),
				SizeInBytes:             largePoolSize, // 5 TiB pool
				AllowAutoTiering:        gcpgenserver.NewOptNilBool(true),
				HotTierSizeInBytes:      gcpgenserver.NewOptNilFloat64(float64(validPoolSize)), // 2 TiB hot tier
				EnableHotTierAutoResize: gcpgenserver.NewOptNilBool(false),                     // Explicitly disable auto-resize
			}
			zone := "us-east4-a"

			err := validateCreatePoolParams(req, zone)
			assert.Nil(ttt, err, "Expected no error for complex valid auto-tiering scenario")
		})
	})

	t.Run("Auto-Tiering Disabled Scenarios", func(tt *testing.T) {
		t.Run("AllowAutoTiering disabled but HotTierSizeInBytes is set", func(ttt *testing.T) {
			req := &gcpgenserver.PoolV1beta{
				Unified:            gcpgenserver.NewOptBool(true),
				SizeInBytes:        validPoolSize,
				AllowAutoTiering:   gcpgenserver.NewOptNilBool(false),
				HotTierSizeInBytes: gcpgenserver.NewOptNilFloat64(float64(minPoolHotTierSize)),
			}
			zone := "us-east4-a"

			err := validateCreatePoolParams(req, zone)
			assert.NotNil(ttt, err, "Expected error when HotTierSizeInBytes is set but auto-tiering is disabled")
			assert.Equal(ttt, float64(400), err.Code)
			assert.Contains(ttt, err.Message, "HotTierSizeInBytes and EnableHotTierAutoResize cannot be set without enabling AllowAutoTiering")
		})

		t.Run("AllowAutoTiering disabled but EnableHotTierAutoResize is set", func(ttt *testing.T) {
			req := &gcpgenserver.PoolV1beta{
				Unified:                 gcpgenserver.NewOptBool(true),
				SizeInBytes:             validPoolSize,
				AllowAutoTiering:        gcpgenserver.NewOptNilBool(false),
				EnableHotTierAutoResize: gcpgenserver.NewOptNilBool(true),
			}
			zone := "us-east4-a"

			err := validateCreatePoolParams(req, zone)
			assert.NotNil(ttt, err, "Expected error when EnableHotTierAutoResize is set but auto-tiering is disabled")
			assert.Equal(ttt, float64(400), err.Code)
			assert.Contains(ttt, err.Message, "HotTierSizeInBytes and EnableHotTierAutoResize cannot be set without enabling AllowAutoTiering")
		})

		t.Run("AllowAutoTiering not set (defaults to false) with HotTierSizeInBytes set", func(ttt *testing.T) {
			req := &gcpgenserver.PoolV1beta{
				Unified:     gcpgenserver.NewOptBool(true),
				SizeInBytes: validPoolSize,
				// AllowAutoTiering not set (defaults to false)
				HotTierSizeInBytes: gcpgenserver.NewOptNilFloat64(float64(minPoolHotTierSize)),
			}
			zone := "us-east4-a"

			err := validateCreatePoolParams(req, zone)
			assert.NotNil(ttt, err, "Expected error when HotTierSizeInBytes is set but auto-tiering is not enabled")
			assert.Equal(ttt, float64(400), err.Code)
			assert.Contains(ttt, err.Message, "HotTierSizeInBytes and EnableHotTierAutoResize cannot be set without enabling AllowAutoTiering")
		})
	})
}

func TestValidateCreatePoolParams_SecondaryZoneValidation(t *testing.T) {
	const (
		validPoolSize = 2199023255552 // 2 TiB
	)

	// Save original state and restore after all tests
	originalRegionalPoolEnabled := regionalPoolEnabled
	defer func() { regionalPoolEnabled = originalRegionalPoolEnabled }()

	t.Run("Regional Pool Secondary Zone Validation", func(tt *testing.T) {
		regionalPoolEnabled = true

		t.Run("Secondary Zone cannot be empty for regional pool", func(ttt *testing.T) {
			req := &gcpgenserver.PoolV1beta{
				Type:          gcpgenserver.NewOptPoolV1betaType(gcpgenserver.PoolV1betaTypeUNIFIED),
				SizeInBytes:   validPoolSize,
				Zone:          gcpgenserver.NewOptString("us-east4-a"),
				SecondaryZone: gcpgenserver.NewOptString(""), // Empty secondary zone
			}
			zone := "" // Empty zone indicates regional pool

			err := validateCreatePoolParams(req, zone)
			assert.NotNil(ttt, err, "Expected error when secondary zone is empty for regional pool")
			assert.Equal(ttt, float64(400), err.Code)
			assert.Contains(ttt, err.Message, "Secondary Zone cannot be empty for regional pool.")
		})

		t.Run("Secondary Zone cannot be same as Primary Zone for regional pool", func(ttt *testing.T) {
			req := &gcpgenserver.PoolV1beta{
				Type:          gcpgenserver.NewOptPoolV1betaType(gcpgenserver.PoolV1betaTypeUNIFIED),
				SizeInBytes:   validPoolSize,
				Zone:          gcpgenserver.NewOptString("us-east4-a"),
				SecondaryZone: gcpgenserver.NewOptString("us-east4-a"), // Same as primary zone
			}
			zone := "" // Empty zone indicates regional pool

			err := validateCreatePoolParams(req, zone)
			assert.NotNil(ttt, err, "Expected error when secondary zone is same as primary zone for regional pool")
			assert.Equal(ttt, float64(400), err.Code)
			assert.Contains(ttt, err.Message, "Secondary Zone cannot be same as Primary Zone")
		})

		t.Run("Valid regional pool with different zones", func(ttt *testing.T) {
			req := &gcpgenserver.PoolV1beta{
				Type:          gcpgenserver.NewOptPoolV1betaType(gcpgenserver.PoolV1betaTypeUNIFIED),
				SizeInBytes:   validPoolSize,
				Zone:          gcpgenserver.NewOptString("us-east4-a"),
				SecondaryZone: gcpgenserver.NewOptString("us-east4-b"), // Different from primary zone
			}
			zone := "" // Empty zone indicates regional pool

			err := validateCreatePoolParams(req, zone)
			assert.Nil(ttt, err, "Expected no error for valid regional pool with different zones")
		})
	})

	t.Run("Zonal Pool Secondary Zone Validation", func(tt *testing.T) {
		regionalPoolEnabled = true

		t.Run("Secondary Zone cannot be same as Primary Zone for zonal pool", func(ttt *testing.T) {
			req := &gcpgenserver.PoolV1beta{
				Type:          gcpgenserver.NewOptPoolV1betaType(gcpgenserver.PoolV1betaTypeUNIFIED),
				SizeInBytes:   validPoolSize,
				Zone:          gcpgenserver.NewOptString("us-east4-a"),
				SecondaryZone: gcpgenserver.NewOptString("us-east4-a"), // Same as primary zone
			}
			zone := "us-east4-a" // Non-empty zone indicates zonal pool

			err := validateCreatePoolParams(req, zone)
			assert.NotNil(ttt, err, "Expected error when secondary zone is same as primary zone for zonal pool")
			assert.Equal(ttt, float64(400), err.Code)
			assert.Contains(ttt, err.Message, "Secondary Zone cannot be same as Primary Zone")
		})

		t.Run("Valid zonal pool with different secondary zone", func(ttt *testing.T) {
			req := &gcpgenserver.PoolV1beta{
				Type:          gcpgenserver.NewOptPoolV1betaType(gcpgenserver.PoolV1betaTypeUNIFIED),
				SizeInBytes:   validPoolSize,
				Zone:          gcpgenserver.NewOptString("us-east4-a"),
				SecondaryZone: gcpgenserver.NewOptString("us-east4-b"), // Different from primary zone
			}
			zone := "us-east4-a" // Non-empty zone indicates zonal pool

			err := validateCreatePoolParams(req, zone)
			assert.Nil(ttt, err, "Expected no error for valid zonal pool with different secondary zone")
		})

		t.Run("Valid zonal pool without secondary zone", func(ttt *testing.T) {
			req := &gcpgenserver.PoolV1beta{
				Type:        gcpgenserver.NewOptPoolV1betaType(gcpgenserver.PoolV1betaTypeUNIFIED),
				SizeInBytes: validPoolSize,
				Zone:        gcpgenserver.NewOptString("us-east4-a"),
				// SecondaryZone not set
			}
			zone := "us-east4-a" // Non-empty zone indicates zonal pool

			err := validateCreatePoolParams(req, zone)
			assert.Nil(ttt, err, "Expected no error for valid zonal pool without secondary zone")
		})
	})

	t.Run("Regional Pool Support Disabled", func(tt *testing.T) {
		regionalPoolEnabled = false

		t.Run("Regional pool creation should fail when feature is disabled", func(ttt *testing.T) {
			req := &gcpgenserver.PoolV1beta{
				Type:          gcpgenserver.NewOptPoolV1betaType(gcpgenserver.PoolV1betaTypeUNIFIED),
				SizeInBytes:   validPoolSize,
				Zone:          gcpgenserver.NewOptString("us-east4-a"),
				SecondaryZone: gcpgenserver.NewOptString("us-east4-b"),
			}
			zone := "" // Empty zone indicates regional pool

			err := validateCreatePoolParams(req, zone)
			assert.NotNil(ttt, err, "Expected error when regional pool support is disabled")
			assert.Equal(ttt, float64(400), err.Code)
			assert.Contains(ttt, err.Message, "Regional Pool Support is not enabled")
		})
	})
}

func TestConvertToPoolV1Beta(t *testing.T) {
	t.Run("WhenPoolHasAllFields", func(tt *testing.T) {
		createdAt := time.Now()
		deletedAt := time.Now().Add(1 * time.Hour)

		pool := &models.Pool{
			BaseModel: models.BaseModel{
				UUID:      "test-pool-uuid",
				CreatedAt: createdAt,
				DeletedAt: &deletedAt,
			},
			Description:  "Test pool description",
			VendorID:     "/projects/123/locations/us-east4/pools/test-pool",
			Region:       "us-east4",
			SizeInBytes:  1099511627776, // 1 TiB
			ServiceLevel: "premium",
			State:        models.LifeCycleStateAvailable,
			QosType:      "auto",
			AutoTieringConfig: &models.AutoTieringConfig{
				HotTierSizeInBytes:      549755813888, // 512 GiB
				EnableHotTierAutoResize: true,
			},
			CustomPerformanceParams: &models.CustomPerformanceParams{
				Enabled:    true,
				Throughput: 1024,
				Iops:       2048,
			},
			KmsConfig: &models.KmsConfig{
				BaseModel: models.BaseModel{
					UUID: "test-kms-config-id",
				},
				KeyName:         "test-kms-config",
				KeyProjectID:    "test-kms-project-id",
				KeyRingLocation: "us-east4",
				KeyRing:         "test-kms-keyring",
			},
			PoolAttributes: &models.PoolAttributes{},
		}

		result := convertToPoolV1Beta(pool)

		assert.NotNil(tt, result)
		assert.Equal(tt, "test-pool-uuid", result.PoolId.Value)
		assert.Equal(tt, "Test pool description", result.Description.Value)
		assert.Equal(tt, float64(1099511627776), result.SizeInBytes)
		assert.Equal(tt, "auto", result.QosType.Value)
		assert.True(tt, result.CreatedAt.IsSet())
		assert.True(tt, result.DeletedAt.IsSet())
		assert.Equal(tt, float64(1024), result.TotalThroughputMibps.Value)
		assert.Equal(tt, float64(2048), result.TotalIops.Value)
		assert.Equal(tt, "test-kms-config-id", result.KmsConfigId.Value)
		assert.Equal(tt, "projects/test-kms-project-id/locations/us-east4/keyRings/test-kms-keyring/cryptoKeys/test-kms-config", result.KmsConfigResourceId.Value)
		assert.Equal(tt, gcpgenserver.PoolV1betaTypeUNIFIED, result.Type.Value, "Type should be set to UNIFIED for VSA pools")
		assert.True(tt, result.Unified.Value, "Unified should be true for VSA pools")
		assert.True(tt, result.UnifiedPool.Value, "UnifiedPool should be true for VSA pools")
	})

	t.Run("WhenPoolIsFromCVP", func(tt *testing.T) {
		createdAt := time.Now()
		deletedAt := time.Now().Add(1 * time.Hour)

		pool := &cvpmodels.PoolV1beta{
			PoolID:    "test-pool-uuid",
			CreatedAt: strfmt.DateTime(createdAt),
			DeletedAt: func() *strfmt.DateTime {
				dt := strfmt.DateTime(deletedAt)
				return &dt
			}(),
			ResourceID: func() *string {
				s := "test-pool"
				return &s
			}(),
			Network: func() *string {
				s := "test-network"
				return &s
			}(),
			SizeInBytes: func() *float64 {
				s := 1099511627776.0
				return &s
			}(),
			ServiceLevel: func() *string {
				s := "premium"
				return &s
			}(),
			StoragePoolState:         "available",
			CustomPerformanceEnabled: true,
		}

		result := convertToPoolV1beta(pool)

		assert.NotNil(tt, result)
		assert.Equal(tt, "test-pool-uuid", result.PoolId.Value)
		assert.Equal(tt, float64(1099511627776), result.SizeInBytes)
		assert.Equal(tt, gcpgenserver.PoolV1betaTypeFILE, result.Type.Value, "Type should be set to FILE for CVP pools")
		assert.False(tt, result.Unified.Value, "Unified should be false for CVP pools")
		assert.False(tt, result.UnifiedPool.Value, "UnifiedPool should be false for CVP pools")
	})
}

// TestValidateThroughputAndIopsForUpdate tests the validateThroughputAndIopsForUpdate function
// which is used for pool updates and covers the missing coverage scenarios
func TestValidateThroughputAndIopsForUpdate(t *testing.T) {
	ctx := context.Background()

	// Create a mock existing pool for testing
	existingPool := &models.Pool{
		CustomPerformanceParams: &models.CustomPerformanceParams{
			Throughput: 128,  // 128 MiBps
			Iops:       2048, // 2048 IOPS
		},
	}

	t.Run("IOPSExplicitlyProvided", func(tt *testing.T) {
		t.Run("ValidIOPS", func(ttt *testing.T) {
			throughput := gcpgenserver.OptNilFloat64{Value: 256, Set: true}
			iops := gcpgenserver.OptNilFloat64{Value: 5000, Set: true}

			result, err := validateThroughputAndIopsForUpdate(ctx, throughput, iops, existingPool)
			assert.Nil(ttt, err)
			assert.Equal(ttt, int64(5000), result)
		})

		t.Run("IOPSBelowMinimum", func(ttt *testing.T) {
			throughput := gcpgenserver.OptNilFloat64{Value: 256, Set: true}
			iops := gcpgenserver.OptNilFloat64{Value: 1000, Set: true} // 1000 < 256*16 = 4096

			// Function doesn't validate - it just returns the provided IOPS value
			result, err := validateThroughputAndIopsForUpdate(ctx, throughput, iops, existingPool)
			assert.Nil(ttt, err)
			assert.Equal(ttt, int64(1000), result)
		})

		t.Run("IOPSAboveMaximum", func(ttt *testing.T) {
			throughput := gcpgenserver.OptNilFloat64{Value: 256, Set: true}
			iops := gcpgenserver.OptNilFloat64{Value: 200000, Set: true} // 200000 > 160000

			// Function doesn't validate - it just returns the provided IOPS value
			result, err := validateThroughputAndIopsForUpdate(ctx, throughput, iops, existingPool)
			assert.Nil(ttt, err)
			assert.Equal(ttt, int64(200000), result)
		})
	})

	t.Run("OnlyThroughputProvided", func(ttt *testing.T) {
		t.Run("CurrentIOPSBelowMinimum", func(ttt *testing.T) {
			throughput := gcpgenserver.OptNilFloat64{Value: 256, Set: true}
			iops := gcpgenserver.OptNilFloat64{} // Not set

			// Current IOPS (2048) is below new minimum (256*16 = 4096)
			// Should increase to minimum
			result, err := validateThroughputAndIopsForUpdate(ctx, throughput, iops, existingPool)
			assert.Nil(ttt, err)
			assert.Equal(ttt, int64(4096), result) // Should increase to minimum
		})

		t.Run("CurrentIOPSBelowMinimum", func(ttt *testing.T) {
			// Create pool with IOPS below new minimum
			lowIopsPool := &models.Pool{
				CustomPerformanceParams: &models.CustomPerformanceParams{
					Throughput: 128,
					Iops:       1000, // 1000 < 256*16 = 4096
				},
			}

			throughput := gcpgenserver.OptNilFloat64{Value: 256, Set: true}
			iops := gcpgenserver.OptNilFloat64{} // Not set

			result, err := validateThroughputAndIopsForUpdate(ctx, throughput, iops, lowIopsPool)
			assert.Nil(ttt, err)
			assert.Equal(ttt, int64(4096), result) // Should increase to minimum
		})

		t.Run("CurrentIOPSAboveMinimum", func(ttt *testing.T) {
			// Create pool with IOPS above new minimum
			highIopsPool := &models.Pool{
				CustomPerformanceParams: &models.CustomPerformanceParams{
					Throughput: 128,
					Iops:       10000, // 10000 > 256*16 = 4096
				},
			}

			throughput := gcpgenserver.OptNilFloat64{Value: 256, Set: true}
			iops := gcpgenserver.OptNilFloat64{} // Not set

			result, err := validateThroughputAndIopsForUpdate(ctx, throughput, iops, highIopsPool)
			assert.Nil(ttt, err)
			assert.Equal(ttt, int64(10000), result) // Should keep current IOPS
		})
	})

	t.Run("NeitherProvided", func(ttt *testing.T) {
		throughput := gcpgenserver.OptNilFloat64{} // Not set
		iops := gcpgenserver.OptNilFloat64{}       // Not set

		result, err := validateThroughputAndIopsForUpdate(ctx, throughput, iops, existingPool)
		assert.Nil(ttt, err)
		assert.Equal(ttt, int64(2048), result) // Should use existing IOPS
	})

	t.Run("ThroughputOnlyValidation", func(ttt *testing.T) {
		t.Run("ThroughputWithNoIOPS", func(ttt *testing.T) {
			throughput := gcpgenserver.OptNilFloat64{Value: 512, Set: true}
			iops := gcpgenserver.OptNilFloat64{} // Not set

			// Should calculate minimum IOPS based on throughput
			result, err := validateThroughputAndIopsForUpdate(ctx, throughput, iops, existingPool)
			assert.Nil(ttt, err)
			assert.Equal(ttt, int64(8192), result) // 512 * 16 = 8192
		})

		t.Run("SmallThroughputIncrease", func(ttt *testing.T) {
			throughput := gcpgenserver.OptNilFloat64{Value: 100, Set: true}
			iops := gcpgenserver.OptNilFloat64{} // Not set

			// Minimum IOPS for 100 MiBps is 1600, but current IOPS (2048) is higher
			result, err := validateThroughputAndIopsForUpdate(ctx, throughput, iops, existingPool)
			assert.Nil(ttt, err)
			assert.Equal(ttt, int64(2048), result) // Should keep current IOPS
		})
	})
}

// TestV1betaUpdatePool_ThroughputOnlyUpdate tests the scenario where only throughput is updated
// This covers the missing coverage for line 523 and smart IOPS calculation
func TestV1betaUpdatePool_ThroughputOnlyUpdate(t *testing.T) {
	// Save original parseAndValidateRegionAndZone function and restore at end of test.
	originalParseAndValidate := parseAndValidateRegionAndZone
	defer func() { parseAndValidateRegionAndZone = originalParseAndValidate }()

	t.Run("ThroughputOnlyUpdate", func(tt *testing.T) {
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)

		// Set orchestrator to return a pool when DescribePool is called.
		mockOrchestrator.EXPECT().DescribePool(mock.Anything, mock.Anything, mock.Anything).Return(&models.Pool{
			BaseModel: models.BaseModel{
				UUID: "pool-uuid",
			},
			Description: "original description",
			SizeInBytes: 1099511627776, // 1 TiB
			CustomPerformanceParams: &models.CustomPerformanceParams{
				Throughput: 64,   // 64 MiBps
				Iops:       1024, // 1024 IOPS (below new minimum)
			},
			PoolAttributes: &models.PoolAttributes{
				PrimaryZone: "us-east4-a",
			},
		}, nil)

		// Set orchestrator to return success when UpdatePool is called.
		mockOrchestrator.EXPECT().UpdatePool(mock.Anything, mock.Anything).
			Return(&models.Pool{
				BaseModel: models.BaseModel{
					UUID: "updated-pool-uuid",
				},
				PoolAttributes: &models.PoolAttributes{
					PrimaryZone: "us-east4-a",
				},
				CustomPerformanceParams: &models.CustomPerformanceParams{
					Throughput: 256,
					Iops:       4096,
				},
			}, "op-123", nil)

		params := gcpgenserver.V1betaUpdatePoolParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
			PoolId:        "pool-id",
		}

		// Only update throughput, not IOPS
		req := &gcpgenserver.PoolUpdateV1beta{
			TotalThroughputMibps: gcpgenserver.NewOptNilFloat64(256), // 256 MiBps
			// TotalIops not set - should trigger smart IOPS calculation
		}

		handler := Handler{Orchestrator: mockOrchestrator}
		result, err := handler.V1betaUpdatePool(context.Background(), req, params)

		assert.NoError(tt, err)
		op, ok := result.(*gcpgenserver.OperationV1beta)
		assert.True(tt, ok)
		expectedOpName := fmt.Sprintf("/v1beta/projects/%s/locations/%s/operations/%s", params.ProjectNumber, params.LocationId, "op-123")
		assert.Equal(tt, expectedOpName, op.Name.Value)
	})
}

func TestGetKmsConfigForPool(t *testing.T) {
	t.Run("ReturnsNilWhenKmsConfigIdIsEmpty", func(tt *testing.T) {
		ctx := context.Background()
		req := &gcpgenserver.PoolV1beta{KmsConfigId: gcpgenserver.NewOptNilString("")}
		params := &common.CreatePoolParams{}
		orchestrator := orchestrator.NewMockOrchestratorFactory(tt)

		kmsConfig, errResp := _getAndSyncKmsConfigForPool(ctx, req, params, orchestrator)
		assert.Nil(tt, kmsConfig)
		assert.Nil(tt, errResp)
	})

	t.Run("ReturnsKmsConfigOnSuccess", func(tt *testing.T) {
		ctx := context.Background()
		req := &gcpgenserver.PoolV1beta{KmsConfigId: gcpgenserver.NewOptNilString("kms-uuid")}
		params := &common.CreatePoolParams{}
		expectedConfig := &models.KmsConfig{}
		orchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		orchestrator.On("GetKmsConfig", ctx, mock.Anything).Return(expectedConfig, nil)

		kmsConfig, errResp := _getAndSyncKmsConfigForPool(ctx, req, params, orchestrator)
		assert.Equal(tt, expectedConfig, kmsConfig)
		assert.Nil(tt, errResp)
	})

	t.Run("WhenSyncSuccess", func(tt *testing.T) {
		ctx := context.Background()
		req := &gcpgenserver.PoolV1beta{KmsConfigId: gcpgenserver.NewOptNilString("kms-uuid")}
		params := &common.CreatePoolParams{}
		orchestratorFactory := orchestrator.NewMockOrchestratorFactory(tt)
		desc := "Description"
		kfp := "projects/project/locations/location/keyRings/keyring/cryptoKeys/key"
		resId := "resourceId"
		sdeResp := &cvpmodels.KmsConfigV1beta{
			Description:  &desc,
			Instructions: "Instructions",
			KeyFullPath:  &kfp,
			ResourceID:   &resId,
		}
		modelKmsConfig := &models.KmsConfig{}

		orchestratorFactory.On("GetKmsConfig", ctx, mock.Anything).Return(nil, errors.NewNotFoundErr("kms", nil))
		orchestratorFactory.On("GetSDEKmsConfiguration", ctx, mock.Anything).Return(sdeResp, nil)
		orchestratorFactory.On("CreateAndSyncKmsConfig", ctx, mock.Anything).Return(modelKmsConfig, nil)

		kmsConfig, errResp := _getAndSyncKmsConfigForPool(ctx, req, params, orchestratorFactory)

		assert.NotNil(tt, kmsConfig)
		assert.Nil(tt, errResp)
	})

	t.Run("ReturnsInternalServerErrorOnOtherError", func(tt *testing.T) {
		ctx := context.Background()
		req := &gcpgenserver.PoolV1beta{KmsConfigId: gcpgenserver.NewOptNilString("kms-uuid")}
		params := &common.CreatePoolParams{}
		orchestratorFactory := orchestrator.NewMockOrchestratorFactory(tt)
		orchestratorFactory.On("GetKmsConfig", ctx, mock.Anything).Return(nil, errors.New("unexpected error"))

		kmsConfig, errResp := _getAndSyncKmsConfigForPool(ctx, req, params, orchestratorFactory)
		assert.Nil(tt, kmsConfig)
		internalErr, ok := errResp.(*gcpgenserver.V1betaCreatePoolInternalServerError)
		assert.True(tt, ok)
		assert.Equal(tt, float64(http.StatusInternalServerError), internalErr.Code)
	})

	t.Run("ReturnsInternalServerErrorWhenSDEKmsConfigFails", func(tt *testing.T) {
		ctx := context.Background()
		req := &gcpgenserver.PoolV1beta{KmsConfigId: gcpgenserver.NewOptNilString("kms-uuid")}
		params := &common.CreatePoolParams{}
		orchestratorFactory := orchestrator.NewMockOrchestratorFactory(tt)

		orchestratorFactory.On("GetKmsConfig", ctx, mock.Anything).Return(nil, errors.NewNotFoundErr("kms", nil))
		orchestratorFactory.On("GetSDEKmsConfiguration", ctx, mock.Anything).Return(nil, stderrors.New("sde error"))

		kmsConfig, errResp := _getAndSyncKmsConfigForPool(ctx, req, params, orchestratorFactory)

		assert.Nil(tt, kmsConfig)
		internalErr, ok := errResp.(*gcpgenserver.V1betaCreatePoolInternalServerError)
		assert.True(tt, ok)
		assert.Equal(tt, float64(http.StatusInternalServerError), internalErr.Code)
	})

	t.Run("ReturnsInternalServerErrorWhenCreateAndSyncKmsConfigFails", func(tt *testing.T) {
		ctx := context.Background()
		req := &gcpgenserver.PoolV1beta{KmsConfigId: gcpgenserver.NewOptNilString("kms-uuid")}
		params := &common.CreatePoolParams{}
		orchestratorFactory := orchestrator.NewMockOrchestratorFactory(tt)
		sdeResp := &cvpmodels.KmsConfigV1beta{}

		orchestratorFactory.On("GetKmsConfig", ctx, mock.Anything).Return(nil, errors.NewNotFoundErr("kms", nil))
		orchestratorFactory.On("GetSDEKmsConfiguration", ctx, mock.Anything).Return(sdeResp, nil)
		orchestratorFactory.On("CreateAndSyncKmsConfig", ctx, mock.Anything).Return(nil, stderrors.New("sync error"))

		kmsConfig, errResp := _getAndSyncKmsConfigForPool(ctx, req, params, orchestratorFactory)

		assert.Nil(tt, kmsConfig)
		internalErr, ok := errResp.(*gcpgenserver.V1betaCreatePoolInternalServerError)
		assert.True(tt, ok)
		assert.Equal(tt, float64(http.StatusInternalServerError), internalErr.Code)
	})

	t.Run("ReturnsBadRequestWhenSDEKmsConfigFails", func(tt *testing.T) {
		ctx := context.Background()
		req := &gcpgenserver.PoolV1beta{KmsConfigId: gcpgenserver.NewOptNilString("kms-uuid")}
		params := &common.CreatePoolParams{}
		orchestratorFactory := orchestrator.NewMockOrchestratorFactory(tt)

		orchestratorFactory.On("GetKmsConfig", ctx, mock.Anything).Return(nil, errors.NewNotFoundErr("kms", nil))
		orchestratorFactory.On("GetSDEKmsConfiguration", ctx, mock.Anything).Return(nil, errors.NewNotFoundErr("kms", nil))

		kmsConfig, errResp := _getAndSyncKmsConfigForPool(ctx, req, params, orchestratorFactory)

		assert.Nil(tt, kmsConfig)
		internalErr, ok := errResp.(*gcpgenserver.V1betaCreatePoolBadRequest)
		assert.True(tt, ok)
		assert.Equal(tt, float64(http.StatusBadRequest), internalErr.Code)
	})
}
