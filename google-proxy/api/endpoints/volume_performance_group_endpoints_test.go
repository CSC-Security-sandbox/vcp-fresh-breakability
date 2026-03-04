package api

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/factory"
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

		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
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

		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		assert.Equal(tt, float64(http.StatusNotImplemented), res.(*gcpgenserver.V1betaCreateVolumePerformanceGroupNotImplemented).Code)
		assert.Equal(tt, "Volume performance group creation is not enabled", res.(*gcpgenserver.V1betaCreateVolumePerformanceGroupNotImplemented).Message)
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

		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
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

		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		assert.Equal(tt, float64(http.StatusNotImplemented), res.(*gcpgenserver.V1betaCreateVolumePerformanceGroupNotImplemented).Code)
		assert.Equal(tt, "Volume performance group creation is not enabled", res.(*gcpgenserver.V1betaCreateVolumePerformanceGroupNotImplemented).Message)
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

		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
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

		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		assert.Equal(tt, float64(http.StatusConflict), res.(*gcpgenserver.V1betaCreateVolumePerformanceGroupConflict).Code)
		assert.Equal(tt, "volume performance group already exists", res.(*gcpgenserver.V1betaCreateVolumePerformanceGroupConflict).Message)
	})

	t.Run("WhenVpgWithThatNameAlreadyExists", func(tt *testing.T) {
		origEnableMqos := enableMqos
		origEnableVpgEndpoints := enableVpgEndpoints
		defer func() {
			enableMqos = origEnableMqos
			enableVpgEndpoints = origEnableVpgEndpoints
		}()
		enableMqos = true
		enableVpgEndpoints = true

		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		ctx := context.Background()
		req := &gcpgenserver.VolumePerformanceGroupCreateV1beta{
			ResourceId:      "existing-vpg-name",
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
		mockOrchestrator.EXPECT().CreateVolumePerformanceGroup(mock.Anything, mock.Anything).
			Return(nil, errors.NewConflictErr("volume performance group with this name already exists"))
		res, err := handler.V1betaCreateVolumePerformanceGroup(ctx, req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		assert.Equal(tt, float64(http.StatusConflict), res.(*gcpgenserver.V1betaCreateVolumePerformanceGroupConflict).Code)
		assert.Equal(tt, "volume performance group with this name already exists", res.(*gcpgenserver.V1betaCreateVolumePerformanceGroupConflict).Message)
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

		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
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

		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		assert.Equal(tt, float64(http.StatusBadRequest), res.(*gcpgenserver.V1betaCreateVolumePerformanceGroupBadRequest).Code)
		assert.Equal(tt, "invalid throughput value", res.(*gcpgenserver.V1betaCreateVolumePerformanceGroupBadRequest).Message)
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

		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
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

		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		vpgRes, ok := res.(*gcpgenserver.VolumePerformanceGroupV1beta)
		assert.True(tt, ok)
		assert.Equal(tt, "test-performance-group", vpgRes.ResourceId)
		assert.Equal(tt, "pool-id", vpgRes.PoolId)
		assert.True(tt, vpgRes.IsShared)
		assert.Equal(tt, int64(100), vpgRes.ThroughputMibps)
		assert.Equal(tt, int64(1000), vpgRes.Iops)
		assert.Equal(tt, "vpg-uuid-123", vpgRes.VolumePerformanceGroupId)
	})
}

func TestV1betaListVolumePerformanceGroups(t *testing.T) {
	t.Run("WhenVpgEndpointsDisabled", func(tt *testing.T) {
		origEnableMqos := enableMqos
		origEnableVpgEndpoints := enableVpgEndpoints
		defer func() {
			enableMqos = origEnableMqos
			enableVpgEndpoints = origEnableVpgEndpoints
		}()
		enableMqos = true
		enableVpgEndpoints = false

		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		ctx := context.Background()
		params := gcpgenserver.V1betaListVolumePerformanceGroupsParams{
			ProjectNumber: "12345",
			LocationId:    "us-central1",
			PoolId:        "pool-id",
		}

		handler := Handler{Orchestrator: mockOrchestrator}
		res, err := handler.V1betaListVolumePerformanceGroups(ctx, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		assert.Equal(tt, float64(http.StatusNotImplemented), res.(*gcpgenserver.V1betaListVolumePerformanceGroupsNotImplemented).Code)
		assert.Equal(tt, "Listing volume performance groups is not enabled", res.(*gcpgenserver.V1betaListVolumePerformanceGroupsNotImplemented).Message)
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

		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		ctx := context.Background()
		params := gcpgenserver.V1betaListVolumePerformanceGroupsParams{
			ProjectNumber: "12345",
			LocationId:    "us-central1",
			PoolId:        "pool-id",
		}

		handler := Handler{Orchestrator: mockOrchestrator}
		res, err := handler.V1betaListVolumePerformanceGroups(ctx, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		assert.Equal(tt, float64(http.StatusNotImplemented), res.(*gcpgenserver.V1betaListVolumePerformanceGroupsNotImplemented).Code)
		assert.Equal(tt, "Listing volume performance groups is not enabled", res.(*gcpgenserver.V1betaListVolumePerformanceGroupsNotImplemented).Message)
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

		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		ctx := context.Background()
		params := gcpgenserver.V1betaListVolumePerformanceGroupsParams{
			ProjectNumber: "12345",
			LocationId:    "us-central1",
			PoolId:        "pool-id",
		}

		handler := Handler{Orchestrator: mockOrchestrator}
		mockOrchestrator.EXPECT().ListVolumePerformanceGroups(mock.Anything, mock.Anything).Return(nil, errors.New("listing volume performance groups is not implemented"))
		res, err := handler.V1betaListVolumePerformanceGroups(ctx, params)

		assert.Error(tt, err)
		assert.EqualError(tt, err, "listing volume performance groups is not implemented")
		assert.NotNil(tt, res)
		assert.Equal(tt, float64(http.StatusInternalServerError), res.(*gcpgenserver.V1betaListVolumePerformanceGroupsInternalServerError).Code)
		assert.Equal(tt, "Internal server error", res.(*gcpgenserver.V1betaListVolumePerformanceGroupsInternalServerError).Message)
	})

	t.Run("WhenPoolNotFound", func(tt *testing.T) {
		origEnableMqos := enableMqos
		origEnableVpgEndpoints := enableVpgEndpoints
		defer func() {
			enableMqos = origEnableMqos
			enableVpgEndpoints = origEnableVpgEndpoints
		}()
		enableMqos = true
		enableVpgEndpoints = true

		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
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

		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		assert.Equal(tt, float64(http.StatusNotFound), res.(*gcpgenserver.V1betaListVolumePerformanceGroupsNotFound).Code)
		assert.Equal(tt, "Pool not found", res.(*gcpgenserver.V1betaListVolumePerformanceGroupsNotFound).Message)
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

		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
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

		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		assert.Equal(tt, float64(http.StatusBadRequest), res.(*gcpgenserver.V1betaListVolumePerformanceGroupsBadRequest).Code)
		assert.Equal(tt, "invalid pool", res.(*gcpgenserver.V1betaListVolumePerformanceGroupsBadRequest).Message)
	})

	t.Run("WhenNoVPGs", func(tt *testing.T) {
		origEnableMqos := enableMqos
		origEnableVpgEndpoints := enableVpgEndpoints
		defer func() {
			enableMqos = origEnableMqos
			enableVpgEndpoints = origEnableVpgEndpoints
		}()
		enableMqos = true
		enableVpgEndpoints = true

		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		ctx := context.Background()
		params := gcpgenserver.V1betaListVolumePerformanceGroupsParams{
			ProjectNumber: "12345",
			LocationId:    "us-central1",
			PoolId:        "pool-id",
		}

		handler := Handler{Orchestrator: mockOrchestrator}
		mockOrchestrator.EXPECT().ListVolumePerformanceGroups(
			mock.Anything,
			mock.MatchedBy(func(p *common.ListVolumePerformanceGroupsParams) bool {
				return p != nil &&
					p.AccountName == params.ProjectNumber &&
					p.PoolID == params.PoolId
			}),
		).Return([]*models.VolumePerformanceGroup{}, nil)
		res, err := handler.V1betaListVolumePerformanceGroups(ctx, params)

		assert.NoError(t, err)
		assert.NotNil(t, res)
		okRes, ok := res.(*gcpgenserver.V1betaListVolumePerformanceGroupsOK)
		assert.True(t, ok)
		assert.NotNil(t, okRes.VolumePerformanceGroups)
		assert.Len(t, okRes.VolumePerformanceGroups, 0)
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

		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
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

		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		okRes, ok := res.(*gcpgenserver.V1betaListVolumePerformanceGroupsOK)
		assert.True(tt, ok)
		assert.Len(tt, okRes.VolumePerformanceGroups, 2)
		assert.Equal(tt, "vpg-1", okRes.VolumePerformanceGroups[0].ResourceId)
		assert.Equal(tt, "pool-id", okRes.VolumePerformanceGroups[0].PoolId)
		assert.Equal(tt, int64(1000), okRes.VolumePerformanceGroups[0].Iops)
		assert.True(tt, okRes.VolumePerformanceGroups[0].IsShared)
		assert.Equal(tt, "vpg-2", okRes.VolumePerformanceGroups[1].ResourceId)
		assert.Equal(tt, int64(2000), okRes.VolumePerformanceGroups[1].Iops)
		assert.False(tt, okRes.VolumePerformanceGroups[1].IsShared)
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

		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		ctx := context.Background()
		params := gcpgenserver.V1betaDescribeVolumePerformanceGroupParams{
			ProjectNumber:            "12345",
			LocationId:               "us-central1",
			PoolId:                   "pool-id",
			VolumePerformanceGroupId: "performance-group-id",
		}

		handler := Handler{Orchestrator: mockOrchestrator}
		res, err := handler.V1betaDescribeVolumePerformanceGroup(ctx, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		assert.Equal(tt, float64(http.StatusNotImplemented), res.(*gcpgenserver.V1betaDescribeVolumePerformanceGroupNotImplemented).Code)
		assert.Equal(tt, "Describing volume performance group is not enabled", res.(*gcpgenserver.V1betaDescribeVolumePerformanceGroupNotImplemented).Message)
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

		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		ctx := context.Background()
		params := gcpgenserver.V1betaDescribeVolumePerformanceGroupParams{
			ProjectNumber:            "12345",
			LocationId:               "us-central1",
			PoolId:                   "pool-id",
			VolumePerformanceGroupId: "performance-group-id",
		}

		handler := Handler{Orchestrator: mockOrchestrator}
		res, err := handler.V1betaDescribeVolumePerformanceGroup(ctx, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		assert.Equal(tt, float64(http.StatusNotImplemented), res.(*gcpgenserver.V1betaDescribeVolumePerformanceGroupNotImplemented).Code)
		assert.Equal(tt, "Describing volume performance group is not enabled", res.(*gcpgenserver.V1betaDescribeVolumePerformanceGroupNotImplemented).Message)
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

		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
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

		assert.Error(tt, err)
		assert.EqualError(tt, err, "get volume performance group is not implemented")
		assert.NotNil(tt, res)
		assert.Equal(tt, float64(http.StatusInternalServerError), res.(*gcpgenserver.V1betaDescribeVolumePerformanceGroupInternalServerError).Code)
		assert.Equal(tt, "Internal server error", res.(*gcpgenserver.V1betaDescribeVolumePerformanceGroupInternalServerError).Message)
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

		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
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

		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		assert.Equal(tt, float64(http.StatusNotFound), res.(*gcpgenserver.V1betaDescribeVolumePerformanceGroupNotFound).Code)
		assert.Equal(tt, "Volume performance group not found", res.(*gcpgenserver.V1betaDescribeVolumePerformanceGroupNotFound).Message)
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

		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
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

		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		assert.Equal(tt, float64(http.StatusBadRequest), res.(*gcpgenserver.V1betaDescribeVolumePerformanceGroupBadRequest).Code)
		assert.Equal(tt, "invalid vpg", res.(*gcpgenserver.V1betaDescribeVolumePerformanceGroupBadRequest).Message)
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

		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
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

		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		descRes, ok := res.(*gcpgenserver.VolumePerformanceGroupV1beta)
		assert.True(tt, ok)
		assert.Equal(tt, "vpg-name", descRes.ResourceId)
		assert.Equal(tt, "pool-id", descRes.PoolId)
		assert.Equal(tt, "vpg-uuid-123", descRes.VolumePerformanceGroupId)
		assert.Equal(tt, int64(500), descRes.ThroughputMibps)
		assert.Equal(tt, int64(1500), descRes.Iops)
		assert.True(tt, descRes.IsShared)
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

		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		ctx := context.Background()
		req := &gcpgenserver.VolumePerformanceGroupUpdateV1beta{
			ResourceId: gcpgenserver.NewOptString("test-performance-group"),
		}
		params := gcpgenserver.V1betaUpdateVolumePerformanceGroupParams{
			ProjectNumber:            "12345",
			LocationId:               "us-central1",
			PoolId:                   "pool-id",
			VolumePerformanceGroupId: "performance-group-id",
		}

		handler := Handler{Orchestrator: mockOrchestrator}
		res, err := handler.V1betaUpdateVolumePerformanceGroup(ctx, req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		assert.Equal(tt, float64(http.StatusNotImplemented), res.(*gcpgenserver.V1betaUpdateVolumePerformanceGroupNotImplemented).Code)
		assert.Equal(tt, "Updating volume performance group is not enabled", res.(*gcpgenserver.V1betaUpdateVolumePerformanceGroupNotImplemented).Message)
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

		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		ctx := context.Background()
		req := &gcpgenserver.VolumePerformanceGroupUpdateV1beta{
			ResourceId: gcpgenserver.NewOptString("test-performance-group"),
		}
		params := gcpgenserver.V1betaUpdateVolumePerformanceGroupParams{
			ProjectNumber:            "12345",
			LocationId:               "us-central1",
			PoolId:                   "pool-id",
			VolumePerformanceGroupId: "performance-group-id",
		}

		handler := Handler{Orchestrator: mockOrchestrator}
		res, err := handler.V1betaUpdateVolumePerformanceGroup(ctx, req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		assert.Equal(tt, float64(http.StatusNotImplemented), res.(*gcpgenserver.V1betaUpdateVolumePerformanceGroupNotImplemented).Code)
		assert.Equal(tt, "Updating volume performance group is not enabled", res.(*gcpgenserver.V1betaUpdateVolumePerformanceGroupNotImplemented).Message)
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

		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		ctx := context.Background()
		req := &gcpgenserver.VolumePerformanceGroupUpdateV1beta{
			ResourceId: gcpgenserver.NewOptString("test-performance-group"),
		}
		params := gcpgenserver.V1betaUpdateVolumePerformanceGroupParams{
			ProjectNumber:            "12345",
			LocationId:               "us-central1",
			PoolId:                   "pool-id",
			VolumePerformanceGroupId: "performance-group-id",
		}

		handler := Handler{Orchestrator: mockOrchestrator}
		mockOrchestrator.EXPECT().UpdateVolumePerformanceGroup(mock.Anything, mock.Anything).Return(nil, "", errors.New("updating volume performance group is not implemented"))
		res, err := handler.V1betaUpdateVolumePerformanceGroup(ctx, req, params)

		assert.Error(tt, err)
		assert.EqualError(tt, err, "updating volume performance group is not implemented")
		assert.NotNil(tt, res)
		assert.Equal(tt, float64(http.StatusInternalServerError), res.(*gcpgenserver.V1betaUpdateVolumePerformanceGroupInternalServerError).Code)
		assert.Equal(tt, "Internal server error", res.(*gcpgenserver.V1betaUpdateVolumePerformanceGroupInternalServerError).Message)
	})

	t.Run("WhenSuccessful_WithResourceId", func(tt *testing.T) {
		origEnableMqos := enableMqos
		origEnableVpgEndpoints := enableVpgEndpoints
		defer func() {
			enableMqos = origEnableMqos
			enableVpgEndpoints = origEnableVpgEndpoints
		}()
		enableMqos = true
		enableVpgEndpoints = true

		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		ctx := context.Background()
		req := &gcpgenserver.VolumePerformanceGroupUpdateV1beta{
			ResourceId:      gcpgenserver.NewOptString("new-vpg-name"),
			ThroughputMibps: gcpgenserver.NewOptInt64(200),
			Iops:            gcpgenserver.NewOptInt64(600),
		}
		params := gcpgenserver.V1betaUpdateVolumePerformanceGroupParams{
			ProjectNumber:            "12345",
			LocationId:               "us-central1",
			PoolId:                   "pool-id",
			VolumePerformanceGroupId: "vpg-uuid",
		}

		expectedVPG := &models.VolumePerformanceGroup{
			BaseModel:       models.BaseModel{UUID: "vpg-uuid"},
			Name:            "new-vpg-name",
			ThroughputMibps: 200,
			Iops:            600,
			IsShared:        true,
		}

		handler := Handler{Orchestrator: mockOrchestrator}
		mockOrchestrator.EXPECT().UpdateVolumePerformanceGroup(mock.Anything, mock.Anything).Return(expectedVPG, "job-uuid-123", nil)
		res, err := handler.V1betaUpdateVolumePerformanceGroup(ctx, req, params)

		assert.NoError(t, err)
		assert.NotNil(t, res)
		opRes, ok := res.(*gcpgenserver.OperationV1beta)
		assert.True(t, ok)
		assert.True(t, opRes.Name.IsSet())
		assert.Contains(t, opRes.Name.Value, "job-uuid-123")
		assert.False(t, opRes.Done.Value)
	})

	t.Run("WhenSuccessful_WithoutResourceId", func(tt *testing.T) {
		origEnableMqos := enableMqos
		origEnableVpgEndpoints := enableVpgEndpoints
		defer func() {
			enableMqos = origEnableMqos
			enableVpgEndpoints = origEnableVpgEndpoints
		}()
		enableMqos = true
		enableVpgEndpoints = true

		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		ctx := context.Background()
		req := &gcpgenserver.VolumePerformanceGroupUpdateV1beta{
			ThroughputMibps: gcpgenserver.NewOptInt64(150),
		}
		params := gcpgenserver.V1betaUpdateVolumePerformanceGroupParams{
			ProjectNumber:            "12345",
			LocationId:               "us-central1",
			PoolId:                   "pool-id",
			VolumePerformanceGroupId: "vpg-uuid",
		}

		expectedVPG := &models.VolumePerformanceGroup{
			BaseModel:       models.BaseModel{UUID: "vpg-uuid"},
			Name:            "existing-name",
			ThroughputMibps: 150,
			Iops:            500,
			IsShared:        true,
		}

		handler := Handler{Orchestrator: mockOrchestrator}
		mockOrchestrator.EXPECT().UpdateVolumePerformanceGroup(mock.Anything, mock.Anything).Return(expectedVPG, "job-uuid-456", nil)
		res, err := handler.V1betaUpdateVolumePerformanceGroup(ctx, req, params)

		assert.NoError(t, err)
		assert.NotNil(t, res)
		opRes, ok := res.(*gcpgenserver.OperationV1beta)
		assert.True(t, ok)
		assert.True(t, opRes.Name.IsSet())
		assert.Contains(t, opRes.Name.Value, "job-uuid-456")
		assert.False(t, opRes.Done.Value)
	})

	t.Run("WhenNotFound_ReturnsBadRequest", func(tt *testing.T) {
		origEnableMqos := enableMqos
		origEnableVpgEndpoints := enableVpgEndpoints
		defer func() {
			enableMqos = origEnableMqos
			enableVpgEndpoints = origEnableVpgEndpoints
		}()
		enableMqos = true
		enableVpgEndpoints = true

		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		ctx := context.Background()
		req := &gcpgenserver.VolumePerformanceGroupUpdateV1beta{}
		params := gcpgenserver.V1betaUpdateVolumePerformanceGroupParams{
			ProjectNumber:            "12345",
			LocationId:               "us-central1",
			PoolId:                   "pool-id",
			VolumePerformanceGroupId: "vpg-uuid",
		}

		handler := Handler{Orchestrator: mockOrchestrator}
		mockOrchestrator.EXPECT().UpdateVolumePerformanceGroup(mock.Anything, mock.Anything).
			Return(nil, "", errors.NewNotFoundErr("volume performance group", nil))
		res, err := handler.V1betaUpdateVolumePerformanceGroup(ctx, req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		assert.Equal(tt, float64(http.StatusBadRequest), res.(*gcpgenserver.V1betaUpdateVolumePerformanceGroupBadRequest).Code)
	})

	t.Run("WhenBadRequest_ReturnsBadRequest", func(tt *testing.T) {
		origEnableMqos := enableMqos
		origEnableVpgEndpoints := enableVpgEndpoints
		defer func() {
			enableMqos = origEnableMqos
			enableVpgEndpoints = origEnableVpgEndpoints
		}()
		enableMqos = true
		enableVpgEndpoints = true

		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		ctx := context.Background()
		req := &gcpgenserver.VolumePerformanceGroupUpdateV1beta{}
		params := gcpgenserver.V1betaUpdateVolumePerformanceGroupParams{
			ProjectNumber:            "12345",
			LocationId:               "us-central1",
			PoolId:                   "pool-id",
			VolumePerformanceGroupId: "vpg-uuid",
		}

		handler := Handler{Orchestrator: mockOrchestrator}
		mockOrchestrator.EXPECT().UpdateVolumePerformanceGroup(mock.Anything, mock.Anything).
			Return(nil, "", errors.NewUserInputValidationErr("pool must have manual QoS to update volume performance group"))
		res, err := handler.V1betaUpdateVolumePerformanceGroup(ctx, req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		assert.Equal(tt, float64(http.StatusBadRequest), res.(*gcpgenserver.V1betaUpdateVolumePerformanceGroupBadRequest).Code)
		assert.Contains(tt, res.(*gcpgenserver.V1betaUpdateVolumePerformanceGroupBadRequest).Message, "manual QoS")
	})

	t.Run("WhenConflict_ReturnsConflict", func(tt *testing.T) {
		origEnableMqos := enableMqos
		origEnableVpgEndpoints := enableVpgEndpoints
		defer func() {
			enableMqos = origEnableMqos
			enableVpgEndpoints = origEnableVpgEndpoints
		}()
		enableMqos = true
		enableVpgEndpoints = true

		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		ctx := context.Background()
		req := &gcpgenserver.VolumePerformanceGroupUpdateV1beta{
			ResourceId: gcpgenserver.NewOptString("new-name"),
		}
		params := gcpgenserver.V1betaUpdateVolumePerformanceGroupParams{
			ProjectNumber:            "12345",
			LocationId:               "us-central1",
			PoolId:                   "pool-id",
			VolumePerformanceGroupId: "vpg-uuid",
		}

		handler := Handler{Orchestrator: mockOrchestrator}
		mockOrchestrator.EXPECT().UpdateVolumePerformanceGroup(mock.Anything, mock.Anything).
			Return(nil, "", errors.NewConflictErr("volume performance group name already in use"))
		res, err := handler.V1betaUpdateVolumePerformanceGroup(ctx, req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		assert.Equal(tt, float64(http.StatusConflict), res.(*gcpgenserver.V1betaUpdateVolumePerformanceGroupConflict).Code)
		assert.Contains(tt, res.(*gcpgenserver.V1betaUpdateVolumePerformanceGroupConflict).Message, "already in use")
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

		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		ctx := context.Background()
		params := gcpgenserver.V1betaDeleteVolumePerformanceGroupParams{
			ProjectNumber:            "12345",
			LocationId:               "us-central1",
			PoolId:                   "pool-id",
			VolumePerformanceGroupId: "performance-group-id",
		}

		handler := Handler{Orchestrator: mockOrchestrator}
		res, err := handler.V1betaDeleteVolumePerformanceGroup(ctx, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		assert.Equal(tt, float64(http.StatusNotImplemented), res.(*gcpgenserver.V1betaDeleteVolumePerformanceGroupNotImplemented).Code)
		assert.Equal(tt, "Deleting volume performance group is not enabled", res.(*gcpgenserver.V1betaDeleteVolumePerformanceGroupNotImplemented).Message)
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

		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		ctx := context.Background()
		params := gcpgenserver.V1betaDeleteVolumePerformanceGroupParams{
			ProjectNumber:            "12345",
			LocationId:               "us-central1",
			PoolId:                   "pool-id",
			VolumePerformanceGroupId: "performance-group-id",
		}

		handler := Handler{Orchestrator: mockOrchestrator}
		res, err := handler.V1betaDeleteVolumePerformanceGroup(ctx, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		assert.Equal(tt, float64(http.StatusNotImplemented), res.(*gcpgenserver.V1betaDeleteVolumePerformanceGroupNotImplemented).Code)
		assert.Equal(tt, "Deleting volume performance group is not enabled", res.(*gcpgenserver.V1betaDeleteVolumePerformanceGroupNotImplemented).Message)
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

		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		ctx := context.Background()
		params := gcpgenserver.V1betaDeleteVolumePerformanceGroupParams{
			ProjectNumber:            "12345",
			LocationId:               "us-central1",
			PoolId:                   "pool-id",
			VolumePerformanceGroupId: "performance-group-id",
		}

		handler := Handler{Orchestrator: mockOrchestrator}
		mockOrchestrator.EXPECT().DeleteVolumePerformanceGroup(mock.Anything, mock.Anything).Return(nil, errors.New("deleting volume performance group is not implemented"))
		res, err := handler.V1betaDeleteVolumePerformanceGroup(ctx, params)

		assert.Error(tt, err)
		assert.EqualError(tt, err, "deleting volume performance group is not implemented")
		assert.NotNil(tt, res)
		assert.Equal(tt, float64(http.StatusInternalServerError), res.(*gcpgenserver.V1betaDeleteVolumePerformanceGroupInternalServerError).Code)
		assert.Equal(tt, "Internal server error", res.(*gcpgenserver.V1betaDeleteVolumePerformanceGroupInternalServerError).Message)
	})

	t.Run("Success_ReturnsOperationWithDeletedVPG", func(tt *testing.T) {
		origEnableMqos := enableMqos
		origEnableVpgEndpoints := enableVpgEndpoints
		defer func() {
			enableMqos = origEnableMqos
			enableVpgEndpoints = origEnableVpgEndpoints
		}()
		enableMqos = true
		enableVpgEndpoints = true

		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		ctx := context.Background()
		params := gcpgenserver.V1betaDeleteVolumePerformanceGroupParams{
			ProjectNumber:            "12345",
			LocationId:               "us-central1",
			PoolId:                   "pool-id",
			VolumePerformanceGroupId: "vpg-uuid-1",
		}
		deletedVPG := &models.VolumePerformanceGroup{
			BaseModel:       models.BaseModel{UUID: "vpg-uuid-1"},
			Name:            "vpg-name",
			ThroughputMibps: 100,
			Iops:            1000,
			IsShared:        true,
		}

		handler := Handler{Orchestrator: mockOrchestrator}
		mockOrchestrator.EXPECT().DeleteVolumePerformanceGroup(mock.Anything, mock.MatchedBy(func(p *common.DeleteVolumePerformanceGroupParams) bool {
			return p.PoolID == "pool-id" && p.VolumePerformanceGroupID == "vpg-uuid-1" && p.AccountName == "12345"
		})).Return(deletedVPG, nil)

		res, err := handler.V1betaDeleteVolumePerformanceGroup(ctx, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		op, ok := res.(*gcpgenserver.OperationV1beta)
		assert.True(tt, ok)
		assert.True(tt, op.Done.IsSet() && op.Done.Value)
		assert.True(tt, op.Name.IsSet())
		assert.Contains(tt, op.Name.Value, "/operations/vpg-delete-vpg-uuid-1")
	})

	t.Run("NotFound_WhenOrchestratorReturnsNotFound", func(tt *testing.T) {
		origEnableMqos := enableMqos
		origEnableVpgEndpoints := enableVpgEndpoints
		defer func() {
			enableMqos = origEnableMqos
			enableVpgEndpoints = origEnableVpgEndpoints
		}()
		enableMqos = true
		enableVpgEndpoints = true

		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		ctx := context.Background()
		params := gcpgenserver.V1betaDeleteVolumePerformanceGroupParams{
			ProjectNumber:            "12345",
			LocationId:               "us-central1",
			PoolId:                   "pool-id",
			VolumePerformanceGroupId: "vpg-uuid",
		}

		handler := Handler{Orchestrator: mockOrchestrator}
		mockOrchestrator.EXPECT().DeleteVolumePerformanceGroup(mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundErr("volume performance group", nil))

		res, err := handler.V1betaDeleteVolumePerformanceGroup(ctx, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		assert.Equal(tt, float64(http.StatusNotFound), res.(*gcpgenserver.V1betaDeleteVolumePerformanceGroupNotFound).Code)
		assert.Equal(tt, "Volume performance group not found", res.(*gcpgenserver.V1betaDeleteVolumePerformanceGroupNotFound).Message)
	})

	t.Run("Conflict_WhenOrchestratorReturnsConflict", func(tt *testing.T) {
		origEnableMqos := enableMqos
		origEnableVpgEndpoints := enableVpgEndpoints
		defer func() {
			enableMqos = origEnableMqos
			enableVpgEndpoints = origEnableVpgEndpoints
		}()
		enableMqos = true
		enableVpgEndpoints = true

		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		ctx := context.Background()
		params := gcpgenserver.V1betaDeleteVolumePerformanceGroupParams{
			ProjectNumber:            "12345",
			LocationId:               "us-central1",
			PoolId:                   "pool-id",
			VolumePerformanceGroupId: "vpg-uuid",
		}

		handler := Handler{Orchestrator: mockOrchestrator}
		mockOrchestrator.EXPECT().DeleteVolumePerformanceGroup(mock.Anything, mock.Anything).Return(nil, errors.NewConflictErr("attached to volumes"))

		res, err := handler.V1betaDeleteVolumePerformanceGroup(ctx, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		assert.Equal(tt, float64(http.StatusConflict), res.(*gcpgenserver.V1betaDeleteVolumePerformanceGroupConflict).Code)
		assert.Contains(tt, res.(*gcpgenserver.V1betaDeleteVolumePerformanceGroupConflict).Message, "attached to volumes")
	})

	t.Run("BadRequest_WhenOrchestratorReturnsUserInputValidation", func(tt *testing.T) {
		origEnableMqos := enableMqos
		origEnableVpgEndpoints := enableVpgEndpoints
		defer func() {
			enableMqos = origEnableMqos
			enableVpgEndpoints = origEnableVpgEndpoints
		}()
		enableMqos = true
		enableVpgEndpoints = true

		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		ctx := context.Background()
		params := gcpgenserver.V1betaDeleteVolumePerformanceGroupParams{
			ProjectNumber:            "12345",
			LocationId:               "us-central1",
			PoolId:                   "pool-id",
			VolumePerformanceGroupId: "vpg-uuid",
		}

		handler := Handler{Orchestrator: mockOrchestrator}
		mockOrchestrator.EXPECT().DeleteVolumePerformanceGroup(mock.Anything, mock.Anything).Return(nil, errors.NewUserInputValidationErr("does not belong to pool"))

		res, err := handler.V1betaDeleteVolumePerformanceGroup(ctx, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		assert.Equal(tt, float64(http.StatusBadRequest), res.(*gcpgenserver.V1betaDeleteVolumePerformanceGroupBadRequest).Code)
		assert.Contains(tt, res.(*gcpgenserver.V1betaDeleteVolumePerformanceGroupBadRequest).Message, "does not belong to pool")
	})
}

func TestConvertModelToVCPVolumePerformanceGroup(t *testing.T) {
	t.Run("NilModelReturnsNil", func(tt *testing.T) {
		assert.Nil(tt, convertModelToVCPVolumePerformanceGroup(nil, "pool-id"))
	})
}
