package api

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

func TestV1betaCreateVolumePerformanceGroup(t *testing.T) {
	t.Run("WhenVpgEndpointsDisabled", func(tt *testing.T) {
		origEnableMqos := enableMqos
		origEnableVpgEndpoints := enableVpgEndpoints
		defer func() {
			enableMqos = origEnableMqos
			enableVpgEndpoints = origEnableVpgEndpoints
		}()
		enableMqos = true
		enableVpgEndpoints = false

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		ctx := context.Background()
		req := &gcpgenserver.VolumePerformanceGroupCreateV1beta{
			ResourceId:      "test-performance-group",
			ThroughputMibps: 100,
			Iops:            1000,
			IsShared:        true,
		}
		params := gcpgenserver.V1betaCreateVolumePerformanceGroupParams{
			ProjectNumber: "12345",
			LocationId:    "us-central1",
			PoolId:        "pool-id",
		}

		handler := Handler{Orchestrator: mockOrchestrator}
		res, err := handler.V1betaCreateVolumePerformanceGroup(ctx, req, params)

		assert.NoError(t, err)
		assert.NotNil(t, res)
		assert.Equal(t, float64(http.StatusNotImplemented), res.(*gcpgenserver.V1betaCreateVolumePerformanceGroupNotImplemented).Code)
		assert.Equal(t, "Volume performance group creation is not enabled", res.(*gcpgenserver.V1betaCreateVolumePerformanceGroupNotImplemented).Message)
	})
	t.Run("WhenMqosDisabled", func(tt *testing.T) {
		origEnableMqos := enableMqos
		origEnableVpgEndpoints := enableVpgEndpoints
		defer func() {
			enableMqos = origEnableMqos
			enableVpgEndpoints = origEnableVpgEndpoints
		}()
		enableMqos = false
		enableVpgEndpoints = true

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		ctx := context.Background()
		req := &gcpgenserver.VolumePerformanceGroupCreateV1beta{
			ResourceId:      "test-performance-group",
			ThroughputMibps: 100,
			Iops:            1000,
			IsShared:        true,
		}
		params := gcpgenserver.V1betaCreateVolumePerformanceGroupParams{
			ProjectNumber: "12345",
			LocationId:    "us-central1",
			PoolId:        "pool-id",
		}

		handler := Handler{Orchestrator: mockOrchestrator}
		res, err := handler.V1betaCreateVolumePerformanceGroup(ctx, req, params)

		assert.NoError(t, err)
		assert.NotNil(t, res)
		assert.Equal(t, float64(http.StatusNotImplemented), res.(*gcpgenserver.V1betaCreateVolumePerformanceGroupNotImplemented).Code)
		assert.Equal(t, "Volume performance group creation is not enabled", res.(*gcpgenserver.V1betaCreateVolumePerformanceGroupNotImplemented).Message)
	})
	t.Run("WhenConflict", func(tt *testing.T) {
		origEnableMqos := enableMqos
		origEnableVpgEndpoints := enableVpgEndpoints
		defer func() {
			enableMqos = origEnableMqos
			enableVpgEndpoints = origEnableVpgEndpoints
		}()
		enableMqos = true
		enableVpgEndpoints = true

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		ctx := context.Background()
		req := &gcpgenserver.VolumePerformanceGroupCreateV1beta{
			ResourceId:      "test-performance-group",
			ThroughputMibps: 100,
			Iops:            1000,
			IsShared:        true,
		}
		params := gcpgenserver.V1betaCreateVolumePerformanceGroupParams{
			ProjectNumber: "12345",
			LocationId:    "us-central1",
			PoolId:        "pool-id",
		}

		handler := Handler{Orchestrator: mockOrchestrator}
		mockOrchestrator.EXPECT().CreateVolumePerformanceGroup(mock.Anything, mock.Anything).Return(nil, errors.NewConflictErr("volume performance group already exists"))
		res, err := handler.V1betaCreateVolumePerformanceGroup(ctx, req, params)

		assert.NoError(t, err)
		assert.NotNil(t, res)
		assert.Equal(t, float64(http.StatusConflict), res.(*gcpgenserver.V1betaCreateVolumePerformanceGroupConflict).Code)
		assert.Equal(t, "volume performance group already exists", res.(*gcpgenserver.V1betaCreateVolumePerformanceGroupConflict).Message)
	})

	t.Run("WhenBadRequest", func(tt *testing.T) {
		origEnableMqos := enableMqos
		origEnableVpgEndpoints := enableVpgEndpoints
		defer func() {
			enableMqos = origEnableMqos
			enableVpgEndpoints = origEnableVpgEndpoints
		}()
		enableMqos = true
		enableVpgEndpoints = true

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		ctx := context.Background()
		req := &gcpgenserver.VolumePerformanceGroupCreateV1beta{
			ResourceId:      "test-performance-group",
			ThroughputMibps: 0,
			Iops:            1000,
			IsShared:        true,
		}
		params := gcpgenserver.V1betaCreateVolumePerformanceGroupParams{
			ProjectNumber: "12345",
			LocationId:    "us-central1",
			PoolId:        "pool-id",
		}

		handler := Handler{Orchestrator: mockOrchestrator}
		mockOrchestrator.EXPECT().CreateVolumePerformanceGroup(mock.Anything, mock.Anything).Return(nil, errors.NewBadRequestErr("invalid throughput value"))
		res, err := handler.V1betaCreateVolumePerformanceGroup(ctx, req, params)

		assert.NoError(t, err)
		assert.NotNil(t, res)
		assert.Equal(t, float64(http.StatusBadRequest), res.(*gcpgenserver.V1betaCreateVolumePerformanceGroupBadRequest).Code)
		assert.Equal(t, "invalid throughput value", res.(*gcpgenserver.V1betaCreateVolumePerformanceGroupBadRequest).Message)
	})

	t.Run("WhenSuccessful", func(tt *testing.T) {
		origEnableMqos := enableMqos
		origEnableVpgEndpoints := enableVpgEndpoints
		defer func() {
			enableMqos = origEnableMqos
			enableVpgEndpoints = origEnableVpgEndpoints
		}()
		enableMqos = true
		enableVpgEndpoints = true

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		ctx := context.Background()
		req := &gcpgenserver.VolumePerformanceGroupCreateV1beta{
			ResourceId:      "test-performance-group",
			ThroughputMibps: 100,
			Iops:            1000,
			IsShared:        true,
		}
		params := gcpgenserver.V1betaCreateVolumePerformanceGroupParams{
			ProjectNumber: "12345",
			LocationId:    "us-central1",
			PoolId:        "pool-id",
		}

		expectedVPG := &models.VolumePerformanceGroup{
			BaseModel: models.BaseModel{
				UUID: "vpg-uuid-123",
			},
			Name:            "test-performance-group",
			ThroughputMibps: 100,
			Iops:            1000,
			IsShared:        true,
		}

		handler := Handler{Orchestrator: mockOrchestrator}
		mockOrchestrator.EXPECT().CreateVolumePerformanceGroup(mock.Anything, mock.Anything).Return(expectedVPG, nil)
		res, err := handler.V1betaCreateVolumePerformanceGroup(ctx, req, params)

		assert.NoError(t, err)
		assert.NotNil(t, res)
		vpgRes, ok := res.(*gcpgenserver.VolumePerformanceGroupV1beta)
		assert.True(t, ok)
		assert.Equal(t, "test-performance-group", vpgRes.ResourceId)
		assert.Equal(t, "pool-id", vpgRes.PoolId)
		assert.True(t, vpgRes.IsShared)
		assert.Equal(t, int64(100), vpgRes.ThroughputMibps)
		assert.Equal(t, int64(1000), vpgRes.Iops)
		assert.Equal(t, "vpg-uuid-123", vpgRes.VolumePerformanceGroupId)
	})
}

func TestV1betaListVolumePerformanceGroups_NotImplemented(t *testing.T) {
	t.Run("WhenVpgEndpointsDisabled", func(tt *testing.T) {
		origEnableMqos := enableMqos
		origEnableVpgEndpoints := enableVpgEndpoints
		defer func() {
			enableMqos = origEnableMqos
			enableVpgEndpoints = origEnableVpgEndpoints
		}()
		enableMqos = true
		enableVpgEndpoints = false

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		ctx := context.Background()
		params := gcpgenserver.V1betaListVolumePerformanceGroupsParams{
			ProjectNumber: "12345",
			LocationId:    "us-central1",
			PoolId:        "pool-id",
		}

		handler := Handler{Orchestrator: mockOrchestrator}
		res, err := handler.V1betaListVolumePerformanceGroups(ctx, params)

		assert.NoError(t, err)
		assert.NotNil(t, res)
		assert.Equal(t, float64(http.StatusNotImplemented), res.(*gcpgenserver.V1betaListVolumePerformanceGroupsNotImplemented).Code)
		assert.Equal(t, "Listing volume performance groups is not enabled", res.(*gcpgenserver.V1betaListVolumePerformanceGroupsNotImplemented).Message)
	})
	t.Run("WhenMqosDisabled", func(tt *testing.T) {
		origEnableMqos := enableMqos
		origEnableVpgEndpoints := enableVpgEndpoints
		defer func() {
			enableMqos = origEnableMqos
			enableVpgEndpoints = origEnableVpgEndpoints
		}()
		enableMqos = false
		enableVpgEndpoints = true

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		ctx := context.Background()
		params := gcpgenserver.V1betaListVolumePerformanceGroupsParams{
			ProjectNumber: "12345",
			LocationId:    "us-central1",
			PoolId:        "pool-id",
		}

		handler := Handler{Orchestrator: mockOrchestrator}
		res, err := handler.V1betaListVolumePerformanceGroups(ctx, params)

		assert.NoError(t, err)
		assert.NotNil(t, res)
		assert.Equal(t, float64(http.StatusNotImplemented), res.(*gcpgenserver.V1betaListVolumePerformanceGroupsNotImplemented).Code)
		assert.Equal(t, "Listing volume performance groups is not enabled", res.(*gcpgenserver.V1betaListVolumePerformanceGroupsNotImplemented).Message)
	})
	t.Run("WhenOrchestratorError", func(tt *testing.T) {
		origEnableMqos := enableMqos
		origEnableVpgEndpoints := enableVpgEndpoints
		defer func() {
			enableMqos = origEnableMqos
			enableVpgEndpoints = origEnableVpgEndpoints
		}()
		enableMqos = true
		enableVpgEndpoints = true

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		ctx := context.Background()
		params := gcpgenserver.V1betaListVolumePerformanceGroupsParams{
			ProjectNumber: "12345",
			LocationId:    "us-central1",
			PoolId:        "pool-id",
		}

		handler := Handler{Orchestrator: mockOrchestrator}
		mockOrchestrator.EXPECT().ListVolumePerformanceGroups(mock.Anything, mock.Anything).Return(nil, errors.New("listing volume performance groups is not implemented"))
		res, err := handler.V1betaListVolumePerformanceGroups(ctx, params)

		assert.Error(t, err)
		assert.EqualError(t, err, "listing volume performance groups is not implemented")
		assert.NotNil(t, res)
		assert.Equal(t, float64(http.StatusInternalServerError), res.(*gcpgenserver.V1betaListVolumePerformanceGroupsInternalServerError).Code)
		assert.Equal(t, "Internal server error", res.(*gcpgenserver.V1betaListVolumePerformanceGroupsInternalServerError).Message)
	})

	t.Run("WhenNotFound", func(tt *testing.T) {
		origEnableMqos := enableMqos
		origEnableVpgEndpoints := enableVpgEndpoints
		defer func() {
			enableMqos = origEnableMqos
			enableVpgEndpoints = origEnableVpgEndpoints
		}()
		enableMqos = true
		enableVpgEndpoints = true

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		ctx := context.Background()
		params := gcpgenserver.V1betaListVolumePerformanceGroupsParams{
			ProjectNumber: "12345",
			LocationId:    "us-central1",
			PoolId:        "pool-id",
		}

		handler := Handler{Orchestrator: mockOrchestrator}
		mockOrchestrator.EXPECT().ListVolumePerformanceGroups(mock.Anything, mock.Anything).
			Return(nil, errors.NewNotFoundErr("pool", nil))
		res, err := handler.V1betaListVolumePerformanceGroups(ctx, params)

		assert.NoError(t, err)
		assert.NotNil(t, res)
		assert.Equal(t, float64(http.StatusNotFound), res.(*gcpgenserver.V1betaListVolumePerformanceGroupsNotFound).Code)
		assert.Equal(t, "Pool not found", res.(*gcpgenserver.V1betaListVolumePerformanceGroupsNotFound).Message)
	})

	t.Run("WhenBadRequest", func(tt *testing.T) {
		origEnableMqos := enableMqos
		origEnableVpgEndpoints := enableVpgEndpoints
		defer func() {
			enableMqos = origEnableMqos
			enableVpgEndpoints = origEnableVpgEndpoints
		}()
		enableMqos = true
		enableVpgEndpoints = true

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		ctx := context.Background()
		params := gcpgenserver.V1betaListVolumePerformanceGroupsParams{
			ProjectNumber: "12345",
			LocationId:    "us-central1",
			PoolId:        "pool-id",
		}

		handler := Handler{Orchestrator: mockOrchestrator}
		mockOrchestrator.EXPECT().ListVolumePerformanceGroups(mock.Anything, mock.Anything).
			Return(nil, errors.NewUserInputValidationErr("invalid pool"))
		res, err := handler.V1betaListVolumePerformanceGroups(ctx, params)

		assert.NoError(t, err)
		assert.NotNil(t, res)
		assert.Equal(t, float64(http.StatusBadRequest), res.(*gcpgenserver.V1betaListVolumePerformanceGroupsBadRequest).Code)
		assert.Equal(t, "invalid pool", res.(*gcpgenserver.V1betaListVolumePerformanceGroupsBadRequest).Message)
	})

	t.Run("WhenSuccessful", func(tt *testing.T) {
		origEnableMqos := enableMqos
		origEnableVpgEndpoints := enableVpgEndpoints
		defer func() {
			enableMqos = origEnableMqos
			enableVpgEndpoints = origEnableVpgEndpoints
		}()
		enableMqos = true
		enableVpgEndpoints = true

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		ctx := context.Background()
		params := gcpgenserver.V1betaListVolumePerformanceGroupsParams{
			ProjectNumber: "12345",
			LocationId:    "us-central1",
			PoolId:        "pool-id",
		}

		vpgs := []*models.VolumePerformanceGroup{
			{
				BaseModel: models.BaseModel{UUID: "vpg-uuid-1"},
				Name:      "vpg-1",
				Iops:      1000,
				IsShared:  true,
			},
			{
				BaseModel: models.BaseModel{UUID: "vpg-uuid-2"},
				Name:      "vpg-2",
				Iops:      2000,
				IsShared:  false,
			},
		}

		handler := Handler{Orchestrator: mockOrchestrator}
		mockOrchestrator.EXPECT().ListVolumePerformanceGroups(mock.Anything, mock.Anything).
			Return(vpgs, nil)
		res, err := handler.V1betaListVolumePerformanceGroups(ctx, params)

		assert.NoError(t, err)
		assert.NotNil(t, res)
		okRes, ok := res.(*gcpgenserver.V1betaListVolumePerformanceGroupsOK)
		assert.True(t, ok)
		assert.Len(t, okRes.VolumePerformanceGroups, 2)
		assert.Equal(t, "vpg-1", okRes.VolumePerformanceGroups[0].ResourceId)
		assert.Equal(t, "pool-id", okRes.VolumePerformanceGroups[0].PoolId)
		assert.Equal(t, int64(1000), okRes.VolumePerformanceGroups[0].Iops)
		assert.True(t, okRes.VolumePerformanceGroups[0].IsShared)
		assert.Equal(t, "vpg-2", okRes.VolumePerformanceGroups[1].ResourceId)
		assert.Equal(t, int64(2000), okRes.VolumePerformanceGroups[1].Iops)
		assert.False(t, okRes.VolumePerformanceGroups[1].IsShared)
	})
}

func TestV1betaDescribeVolumePerformanceGroup_NotImplemented(t *testing.T) {
	t.Run("WhenVpgEndpointsDisabled", func(tt *testing.T) {
		origEnableMqos := enableMqos
		origEnableVpgEndpoints := enableVpgEndpoints
		defer func() {
			enableMqos = origEnableMqos
			enableVpgEndpoints = origEnableVpgEndpoints
		}()
		enableMqos = true
		enableVpgEndpoints = false

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		ctx := context.Background()
		params := gcpgenserver.V1betaDescribeVolumePerformanceGroupParams{
			ProjectNumber:            "12345",
			LocationId:               "us-central1",
			PoolId:                   "pool-id",
			VolumePerformanceGroupId: "performance-group-id",
		}

		handler := Handler{Orchestrator: mockOrchestrator}
		res, err := handler.V1betaDescribeVolumePerformanceGroup(ctx, params)

		assert.NoError(t, err)
		assert.NotNil(t, res)
		assert.Equal(t, float64(http.StatusNotImplemented), res.(*gcpgenserver.V1betaDescribeVolumePerformanceGroupNotImplemented).Code)
		assert.Equal(t, "Describing volume performance group is not enabled", res.(*gcpgenserver.V1betaDescribeVolumePerformanceGroupNotImplemented).Message)
	})
	t.Run("WhenMqosDisabled", func(tt *testing.T) {
		origEnableMqos := enableMqos
		origEnableVpgEndpoints := enableVpgEndpoints
		defer func() {
			enableMqos = origEnableMqos
			enableVpgEndpoints = origEnableVpgEndpoints
		}()
		enableMqos = false
		enableVpgEndpoints = true

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		ctx := context.Background()
		params := gcpgenserver.V1betaDescribeVolumePerformanceGroupParams{
			ProjectNumber:            "12345",
			LocationId:               "us-central1",
			PoolId:                   "pool-id",
			VolumePerformanceGroupId: "performance-group-id",
		}

		handler := Handler{Orchestrator: mockOrchestrator}
		res, err := handler.V1betaDescribeVolumePerformanceGroup(ctx, params)

		assert.NoError(t, err)
		assert.NotNil(t, res)
		assert.Equal(t, float64(http.StatusNotImplemented), res.(*gcpgenserver.V1betaDescribeVolumePerformanceGroupNotImplemented).Code)
		assert.Equal(t, "Describing volume performance group is not enabled", res.(*gcpgenserver.V1betaDescribeVolumePerformanceGroupNotImplemented).Message)
	})
	t.Run("WhenOrchestratorError", func(tt *testing.T) {
		origEnableMqos := enableMqos
		origEnableVpgEndpoints := enableVpgEndpoints
		defer func() {
			enableMqos = origEnableMqos
			enableVpgEndpoints = origEnableVpgEndpoints
		}()
		enableMqos = true
		enableVpgEndpoints = true

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		ctx := context.Background()
		params := gcpgenserver.V1betaDescribeVolumePerformanceGroupParams{
			ProjectNumber:            "12345",
			LocationId:               "us-central1",
			PoolId:                   "pool-id",
			VolumePerformanceGroupId: "performance-group-id",
		}

		handler := Handler{Orchestrator: mockOrchestrator}
		mockOrchestrator.EXPECT().GetVolumePerformanceGroup(mock.Anything, mock.Anything).Return(nil, errors.New("get volume performance group is not implemented"))
		res, err := handler.V1betaDescribeVolumePerformanceGroup(ctx, params)

		assert.Error(t, err)
		assert.EqualError(t, err, "get volume performance group is not implemented")
		assert.NotNil(t, res)
		assert.Equal(t, float64(http.StatusInternalServerError), res.(*gcpgenserver.V1betaDescribeVolumePerformanceGroupInternalServerError).Code)
		assert.Equal(t, "Internal server error", res.(*gcpgenserver.V1betaDescribeVolumePerformanceGroupInternalServerError).Message)
	})

	t.Run("WhenNotFound", func(tt *testing.T) {
		origEnableMqos := enableMqos
		origEnableVpgEndpoints := enableVpgEndpoints
		defer func() {
			enableMqos = origEnableMqos
			enableVpgEndpoints = origEnableVpgEndpoints
		}()
		enableMqos = true
		enableVpgEndpoints = true

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		ctx := context.Background()
		params := gcpgenserver.V1betaDescribeVolumePerformanceGroupParams{
			ProjectNumber:            "12345",
			LocationId:               "us-central1",
			PoolId:                   "pool-id",
			VolumePerformanceGroupId: "performance-group-id",
		}

		handler := Handler{Orchestrator: mockOrchestrator}
		mockOrchestrator.EXPECT().GetVolumePerformanceGroup(mock.Anything, mock.Anything).
			Return(nil, errors.NewNotFoundErr("vpg", nil))
		res, err := handler.V1betaDescribeVolumePerformanceGroup(ctx, params)

		assert.NoError(t, err)
		assert.NotNil(t, res)
		assert.Equal(t, float64(http.StatusNotFound), res.(*gcpgenserver.V1betaDescribeVolumePerformanceGroupNotFound).Code)
		assert.Equal(t, "Volume performance group not found", res.(*gcpgenserver.V1betaDescribeVolumePerformanceGroupNotFound).Message)
	})

	t.Run("WhenBadRequest", func(tt *testing.T) {
		origEnableMqos := enableMqos
		origEnableVpgEndpoints := enableVpgEndpoints
		defer func() {
			enableMqos = origEnableMqos
			enableVpgEndpoints = origEnableVpgEndpoints
		}()
		enableMqos = true
		enableVpgEndpoints = true

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		ctx := context.Background()
		params := gcpgenserver.V1betaDescribeVolumePerformanceGroupParams{
			ProjectNumber:            "12345",
			LocationId:               "us-central1",
			PoolId:                   "pool-id",
			VolumePerformanceGroupId: "performance-group-id",
		}

		handler := Handler{Orchestrator: mockOrchestrator}
		mockOrchestrator.EXPECT().GetVolumePerformanceGroup(mock.Anything, mock.Anything).
			Return(nil, errors.NewUserInputValidationErr("invalid vpg"))
		res, err := handler.V1betaDescribeVolumePerformanceGroup(ctx, params)

		assert.NoError(t, err)
		assert.NotNil(t, res)
		assert.Equal(t, float64(http.StatusBadRequest), res.(*gcpgenserver.V1betaDescribeVolumePerformanceGroupBadRequest).Code)
		assert.Equal(t, "invalid vpg", res.(*gcpgenserver.V1betaDescribeVolumePerformanceGroupBadRequest).Message)
	})

	t.Run("WhenSuccessful", func(tt *testing.T) {
		origEnableMqos := enableMqos
		origEnableVpgEndpoints := enableVpgEndpoints
		defer func() {
			enableMqos = origEnableMqos
			enableVpgEndpoints = origEnableVpgEndpoints
		}()
		enableMqos = true
		enableVpgEndpoints = true

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		ctx := context.Background()
		params := gcpgenserver.V1betaDescribeVolumePerformanceGroupParams{
			ProjectNumber:            "12345",
			LocationId:               "us-central1",
			PoolId:                   "pool-id",
			VolumePerformanceGroupId: "performance-group-id",
		}

		vpg := &models.VolumePerformanceGroup{
			BaseModel:       models.BaseModel{UUID: "vpg-uuid-123"},
			Name:            "vpg-name",
			ThroughputMibps: 500,
			Iops:            1500,
			IsShared:        true,
		}

		handler := Handler{Orchestrator: mockOrchestrator}
		mockOrchestrator.EXPECT().GetVolumePerformanceGroup(mock.Anything, mock.Anything).
			Return(vpg, nil)
		res, err := handler.V1betaDescribeVolumePerformanceGroup(ctx, params)

		assert.NoError(t, err)
		assert.NotNil(t, res)
		descRes, ok := res.(*gcpgenserver.VolumePerformanceGroupV1beta)
		assert.True(t, ok)
		assert.Equal(t, "vpg-name", descRes.ResourceId)
		assert.Equal(t, "pool-id", descRes.PoolId)
		assert.Equal(t, "vpg-uuid-123", descRes.VolumePerformanceGroupId)
		assert.Equal(t, int64(500), descRes.ThroughputMibps)
		assert.Equal(t, int64(1500), descRes.Iops)
		assert.True(t, descRes.IsShared)
	})
}

func TestV1betaUpdateVolumePerformanceGroup_NotImplemented(t *testing.T) {
	t.Run("WhenVpgEndpointsDisabled", func(tt *testing.T) {
		origEnableMqos := enableMqos
		origEnableVpgEndpoints := enableVpgEndpoints
		defer func() {
			enableMqos = origEnableMqos
			enableVpgEndpoints = origEnableVpgEndpoints
		}()
		enableMqos = true
		enableVpgEndpoints = false

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		ctx := context.Background()
		req := &gcpgenserver.VolumePerformanceGroupUpdateV1beta{
			ResourceId: "test-performance-group",
		}
		params := gcpgenserver.V1betaUpdateVolumePerformanceGroupParams{
			ProjectNumber:            "12345",
			LocationId:               "us-central1",
			PoolId:                   "pool-id",
			VolumePerformanceGroupId: "performance-group-id",
		}

		handler := Handler{Orchestrator: mockOrchestrator}
		res, err := handler.V1betaUpdateVolumePerformanceGroup(ctx, req, params)

		assert.NoError(t, err)
		assert.NotNil(t, res)
		assert.Equal(t, float64(http.StatusNotImplemented), res.(*gcpgenserver.V1betaUpdateVolumePerformanceGroupNotImplemented).Code)
		assert.Equal(t, "Updating volume performance group is not enabled", res.(*gcpgenserver.V1betaUpdateVolumePerformanceGroupNotImplemented).Message)
	})
	t.Run("WhenMqosDisabled", func(tt *testing.T) {
		origEnableMqos := enableMqos
		origEnableVpgEndpoints := enableVpgEndpoints
		defer func() {
			enableMqos = origEnableMqos
			enableVpgEndpoints = origEnableVpgEndpoints
		}()
		enableMqos = false
		enableVpgEndpoints = true

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		ctx := context.Background()
		req := &gcpgenserver.VolumePerformanceGroupUpdateV1beta{
			ResourceId: "test-performance-group",
		}
		params := gcpgenserver.V1betaUpdateVolumePerformanceGroupParams{
			ProjectNumber:            "12345",
			LocationId:               "us-central1",
			PoolId:                   "pool-id",
			VolumePerformanceGroupId: "performance-group-id",
		}

		handler := Handler{Orchestrator: mockOrchestrator}
		res, err := handler.V1betaUpdateVolumePerformanceGroup(ctx, req, params)

		assert.NoError(t, err)
		assert.NotNil(t, res)
		assert.Equal(t, float64(http.StatusNotImplemented), res.(*gcpgenserver.V1betaUpdateVolumePerformanceGroupNotImplemented).Code)
		assert.Equal(t, "Updating volume performance group is not enabled", res.(*gcpgenserver.V1betaUpdateVolumePerformanceGroupNotImplemented).Message)
	})
	t.Run("WhenOrchestratorError", func(tt *testing.T) {
		origEnableMqos := enableMqos
		origEnableVpgEndpoints := enableVpgEndpoints
		defer func() {
			enableMqos = origEnableMqos
			enableVpgEndpoints = origEnableVpgEndpoints
		}()
		enableMqos = true
		enableVpgEndpoints = true

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		ctx := context.Background()
		req := &gcpgenserver.VolumePerformanceGroupUpdateV1beta{
			ResourceId: "test-performance-group",
		}
		params := gcpgenserver.V1betaUpdateVolumePerformanceGroupParams{
			ProjectNumber:            "12345",
			LocationId:               "us-central1",
			PoolId:                   "pool-id",
			VolumePerformanceGroupId: "performance-group-id",
		}

		handler := Handler{Orchestrator: mockOrchestrator}
		mockOrchestrator.EXPECT().UpdateVolumePerformanceGroup(mock.Anything, mock.Anything).Return(nil, errors.New("updating volume performance group is not implemented"))
		res, err := handler.V1betaUpdateVolumePerformanceGroup(ctx, req, params)

		assert.Error(t, err)
		assert.EqualError(t, err, "updating volume performance group is not implemented")
		assert.NotNil(t, res)
		assert.Equal(t, float64(http.StatusInternalServerError), res.(*gcpgenserver.V1betaUpdateVolumePerformanceGroupInternalServerError).Code)
		assert.Equal(t, "Internal server error", res.(*gcpgenserver.V1betaUpdateVolumePerformanceGroupInternalServerError).Message)
	})
}

func TestV1betaDeleteVolumePerformanceGroup_NotImplemented(t *testing.T) {
	t.Run("WhenVpgEndpointsDisabled", func(tt *testing.T) {
		origEnableMqos := enableMqos
		origEnableVpgEndpoints := enableVpgEndpoints
		defer func() {
			enableMqos = origEnableMqos
			enableVpgEndpoints = origEnableVpgEndpoints
		}()
		enableMqos = true
		enableVpgEndpoints = false

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		ctx := context.Background()
		params := gcpgenserver.V1betaDeleteVolumePerformanceGroupParams{
			ProjectNumber:            "12345",
			LocationId:               "us-central1",
			PoolId:                   "pool-id",
			VolumePerformanceGroupId: "performance-group-id",
		}

		handler := Handler{Orchestrator: mockOrchestrator}
		res, err := handler.V1betaDeleteVolumePerformanceGroup(ctx, params)

		assert.NoError(t, err)
		assert.NotNil(t, res)
		assert.Equal(t, float64(http.StatusNotImplemented), res.(*gcpgenserver.V1betaDeleteVolumePerformanceGroupNotImplemented).Code)
		assert.Equal(t, "Deleting volume performance group is not enabled", res.(*gcpgenserver.V1betaDeleteVolumePerformanceGroupNotImplemented).Message)
	})
	t.Run("WhenMqosDisabled", func(tt *testing.T) {
		origEnableMqos := enableMqos
		origEnableVpgEndpoints := enableVpgEndpoints
		defer func() {
			enableMqos = origEnableMqos
			enableVpgEndpoints = origEnableVpgEndpoints
		}()
		enableMqos = false
		enableVpgEndpoints = true

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		ctx := context.Background()
		params := gcpgenserver.V1betaDeleteVolumePerformanceGroupParams{
			ProjectNumber:            "12345",
			LocationId:               "us-central1",
			PoolId:                   "pool-id",
			VolumePerformanceGroupId: "performance-group-id",
		}

		handler := Handler{Orchestrator: mockOrchestrator}
		res, err := handler.V1betaDeleteVolumePerformanceGroup(ctx, params)

		assert.NoError(t, err)
		assert.NotNil(t, res)
		assert.Equal(t, float64(http.StatusNotImplemented), res.(*gcpgenserver.V1betaDeleteVolumePerformanceGroupNotImplemented).Code)
		assert.Equal(t, "Deleting volume performance group is not enabled", res.(*gcpgenserver.V1betaDeleteVolumePerformanceGroupNotImplemented).Message)
	})
	t.Run("WhenOrchestratorError", func(tt *testing.T) {
		origEnableMqos := enableMqos
		origEnableVpgEndpoints := enableVpgEndpoints
		defer func() {
			enableMqos = origEnableMqos
			enableVpgEndpoints = origEnableVpgEndpoints
		}()
		enableMqos = true
		enableVpgEndpoints = true

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		ctx := context.Background()
		params := gcpgenserver.V1betaDeleteVolumePerformanceGroupParams{
			ProjectNumber:            "12345",
			LocationId:               "us-central1",
			PoolId:                   "pool-id",
			VolumePerformanceGroupId: "performance-group-id",
		}

		handler := Handler{Orchestrator: mockOrchestrator}
		mockOrchestrator.EXPECT().DeleteVolumePerformanceGroup(mock.Anything, mock.Anything).Return(errors.New("deleting volume performance group is not implemented"))
		res, err := handler.V1betaDeleteVolumePerformanceGroup(ctx, params)

		assert.Error(t, err)
		assert.EqualError(t, err, "deleting volume performance group is not implemented")
		assert.NotNil(t, res)
		assert.Equal(t, float64(http.StatusInternalServerError), res.(*gcpgenserver.V1betaDeleteVolumePerformanceGroupInternalServerError).Code)
		assert.Equal(t, "Internal server error", res.(*gcpgenserver.V1betaDeleteVolumePerformanceGroupInternalServerError).Message)
	})
}

func TestConvertModelToVCPVolumePerformanceGroup(t *testing.T) {
	t.Run("NilModelReturnsNil", func(tt *testing.T) {
		assert.Nil(t, convertModelToVCPVolumePerformanceGroup(nil, "pool-id"))
	})
}
