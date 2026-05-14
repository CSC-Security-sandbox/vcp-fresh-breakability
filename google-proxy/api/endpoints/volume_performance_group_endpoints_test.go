package api

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

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

	t.Run("WhenSuccessful_IsSharedFalse", func(tt *testing.T) {
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
			ResourceId:      "vpg-not-shared",
			ThroughputMibps: 200,
			Iops:            2000,
			IsShared:        false,
		}
		params := gcpgenserver.V1betaCreateVolumePerformanceGroupParams{
			ProjectNumber: "12345",
			LocationId:    "us-central1",
			PoolId:        "pool-id",
		}

		expectedVPG := &models.VolumePerformanceGroup{
			BaseModel: models.BaseModel{
				UUID: "vpg-uuid-not-shared",
			},
			Name:            "vpg-not-shared",
			ThroughputMibps: 200,
			Iops:            2000,
			IsShared:        false,
		}

		handler := Handler{Orchestrator: mockOrchestrator}
		mockOrchestrator.EXPECT().CreateVolumePerformanceGroup(mock.Anything, mock.MatchedBy(func(p *common.CreateVolumePerformanceGroupParams) bool {
			return p != nil && !p.IsShared && p.Name == "vpg-not-shared"
		})).Return(expectedVPG, nil)
		res, err := handler.V1betaCreateVolumePerformanceGroup(ctx, req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		vpgRes, ok := res.(*gcpgenserver.VolumePerformanceGroupV1beta)
		assert.True(tt, ok)
		assert.Equal(tt, "vpg-not-shared", vpgRes.ResourceId)
		assert.Equal(tt, "pool-id", vpgRes.PoolId)
		assert.False(tt, vpgRes.IsShared, "IsShared should be false in API response")
		assert.Equal(tt, int64(200), vpgRes.ThroughputMibps)
		assert.Equal(tt, int64(2000), vpgRes.Iops)
		assert.Equal(tt, "vpg-uuid-not-shared", vpgRes.VolumePerformanceGroupId)
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

		var vpgBody gcpgenserver.VolumePerformanceGroupV1beta
		assert.NoError(t, json.Unmarshal(opRes.Response, &vpgBody))
		assert.Equal(t, "new-vpg-name", vpgBody.ResourceId)
		assert.Equal(t, "pool-id", vpgBody.PoolId)
		assert.Equal(t, "vpg-uuid", vpgBody.VolumePerformanceGroupId)
		assert.Equal(t, int64(200), vpgBody.ThroughputMibps)
		assert.Equal(t, int64(600), vpgBody.Iops)
		assert.True(t, vpgBody.IsShared)
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

		var vpgBody gcpgenserver.VolumePerformanceGroupV1beta
		assert.NoError(t, json.Unmarshal(opRes.Response, &vpgBody))
		assert.Equal(t, "existing-name", vpgBody.ResourceId)
		assert.Equal(t, "pool-id", vpgBody.PoolId)
		assert.Equal(t, "vpg-uuid", vpgBody.VolumePerformanceGroupId)
		assert.Equal(t, int64(150), vpgBody.ThroughputMibps)
		assert.Equal(t, int64(500), vpgBody.Iops)
		assert.True(t, vpgBody.IsShared)
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
		mockOrchestrator.EXPECT().DeleteVolumePerformanceGroup(mock.Anything, mock.Anything).Return(nil, "", errors.New("deleting volume performance group is not implemented"))
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
		})).Return(deletedVPG, "job-uuid-delete-1", nil)

		res, err := handler.V1betaDeleteVolumePerformanceGroup(ctx, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		op, ok := res.(*gcpgenserver.OperationV1beta)
		assert.True(tt, ok)
		assert.True(tt, op.Name.IsSet())
		assert.Contains(tt, op.Name.Value, "/operations/job-uuid-delete-1")
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
		mockOrchestrator.EXPECT().DeleteVolumePerformanceGroup(mock.Anything, mock.Anything).Return(nil, "", errors.NewNotFoundErr("volume performance group", nil))

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
		mockOrchestrator.EXPECT().DeleteVolumePerformanceGroup(mock.Anything, mock.Anything).Return(nil, "", errors.NewConflictErr("attached to volumes"))

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
		mockOrchestrator.EXPECT().DeleteVolumePerformanceGroup(mock.Anything, mock.Anything).Return(nil, "", errors.NewUserInputValidationErr("does not belong to pool"))

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

	t.Run("AllMetadataFields", func(tt *testing.T) {
		createdAt := time.Date(2026, 5, 12, 10, 0, 0, 0, time.UTC)
		vpg := &models.VolumePerformanceGroup{
			BaseModel: models.BaseModel{
				UUID:      "vpg-uuid-123",
				CreatedAt: createdAt,
			},
			Name:                  "test-vpg",
			ThroughputMibps:       128,
			Iops:                  1000,
			IsShared:              true,
			Description:           "my description",
			LifeCycleState:        "READY",
			LifeCycleStateDetails: "Ready for use",
			Labels:                map[string]string{"env": "dev", "team": "storage"},
		}

		res := convertModelToVCPVolumePerformanceGroup(vpg, "pool-id")

		assert.Equal(tt, "test-vpg", res.ResourceId)
		assert.Equal(tt, "pool-id", res.PoolId)
		assert.Equal(tt, "vpg-uuid-123", res.VolumePerformanceGroupId)
		assert.Equal(tt, int64(128), res.ThroughputMibps)
		assert.Equal(tt, int64(1000), res.Iops)
		assert.True(tt, res.IsShared)
		assert.True(tt, res.Created.IsSet())
		assert.Equal(tt, createdAt, res.Created.Value)
		assert.True(tt, res.Description.IsSet())
		assert.Equal(tt, "my description", res.Description.Value)
		assert.True(tt, res.VolumePerformanceGroupState.IsSet())
		assert.Equal(tt, gcpgenserver.VolumePerformanceGroupV1betaVolumePerformanceGroupStateREADY, res.VolumePerformanceGroupState.Value)
		assert.True(tt, res.VolumePerformanceGroupStateDetails.IsSet())
		assert.Equal(tt, "Ready for use", res.VolumePerformanceGroupStateDetails.Value)
		assert.True(tt, res.Labels.IsSet())
		assert.Equal(tt, "dev", res.Labels.Value["env"])
		assert.Equal(tt, "storage", res.Labels.Value["team"])
	})

	t.Run("UnknownStateFallsBackToStateUnspecified", func(tt *testing.T) {
		vpg := &models.VolumePerformanceGroup{
			BaseModel:      models.BaseModel{UUID: "vpg-uuid"},
			Name:           "test-vpg",
			LifeCycleState: "UNKNOWN",
		}

		res := convertModelToVCPVolumePerformanceGroup(vpg, "pool-id")

		assert.True(tt, res.VolumePerformanceGroupState.IsSet())
		assert.Equal(tt, gcpgenserver.VolumePerformanceGroupV1betaVolumePerformanceGroupStateSTATEUNSPECIFIED, res.VolumePerformanceGroupState.Value)
	})

	t.Run("EmptyOptionalFields", func(tt *testing.T) {
		vpg := &models.VolumePerformanceGroup{
			BaseModel: models.BaseModel{UUID: "vpg-uuid"},
			Name:      "test-vpg",
		}

		res := convertModelToVCPVolumePerformanceGroup(vpg, "pool-id")

		assert.True(tt, res.VolumePerformanceGroupState.IsSet())
		assert.Equal(tt, gcpgenserver.VolumePerformanceGroupV1betaVolumePerformanceGroupStateSTATEUNSPECIFIED, res.VolumePerformanceGroupState.Value)
		assert.False(tt, res.VolumePerformanceGroupStateDetails.IsSet())
		assert.False(tt, res.Labels.IsSet())
	})
}

func TestV1betaCreateVolumePerformanceGroup_WithMetadata(t *testing.T) {
	origEnableMqos := enableMqos
	origEnableVpgEndpoints := enableVpgEndpoints
	defer func() {
		enableMqos = origEnableMqos
		enableVpgEndpoints = origEnableVpgEndpoints
	}()
	enableMqos = true
	enableVpgEndpoints = true

	t.Run("DescriptionAndLabelsPassedToOrchestrator", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		ctx := context.Background()
		req := &gcpgenserver.VolumePerformanceGroupCreateV1beta{
			ResourceId:      "test-vpg",
			ThroughputMibps: 128,
			Iops:            1000,
			IsShared:        true,
			Description:     gcpgenserver.NewOptNilString("my description"),
			Labels: gcpgenserver.NewOptVolumePerformanceGroupCreateV1betaLabels(
				gcpgenserver.VolumePerformanceGroupCreateV1betaLabels{"env": "dev"}),
		}
		params := gcpgenserver.V1betaCreateVolumePerformanceGroupParams{
			ProjectNumber: "12345",
			LocationId:    "us-central1",
			PoolId:        "pool-id",
		}

		expectedVPG := &models.VolumePerformanceGroup{
			BaseModel:      models.BaseModel{UUID: "vpg-uuid"},
			Name:           "test-vpg",
			Description:    "my description",
			Labels:         map[string]string{"env": "dev"},
			LifeCycleState: "READY",
		}

		handler := Handler{Orchestrator: mockOrchestrator}
		mockOrchestrator.EXPECT().CreateVolumePerformanceGroup(mock.Anything, mock.MatchedBy(func(p *common.CreateVolumePerformanceGroupParams) bool {
			return p.Description == "my description" && p.Labels != nil
		})).Return(expectedVPG, nil)
		res, err := handler.V1betaCreateVolumePerformanceGroup(ctx, req, params)

		assert.NoError(tt, err)
		vpgRes, ok := res.(*gcpgenserver.VolumePerformanceGroupV1beta)
		assert.True(tt, ok)
		assert.True(tt, vpgRes.Description.IsSet())
		assert.Equal(tt, "my description", vpgRes.Description.Value)
		assert.True(tt, vpgRes.VolumePerformanceGroupState.IsSet())
	})
}

func TestToVPGState(t *testing.T) {
	tests := []struct {
		input    string
		expected gcpgenserver.VolumePerformanceGroupV1betaVolumePerformanceGroupState
	}{
		{"CREATING", gcpgenserver.VolumePerformanceGroupV1betaVolumePerformanceGroupStateCREATING},
		{"READY", gcpgenserver.VolumePerformanceGroupV1betaVolumePerformanceGroupStateREADY},
		{"UPDATING", gcpgenserver.VolumePerformanceGroupV1betaVolumePerformanceGroupStateUPDATING},
		{"DELETING", gcpgenserver.VolumePerformanceGroupV1betaVolumePerformanceGroupStateDELETING},
		{"DELETED", gcpgenserver.VolumePerformanceGroupV1betaVolumePerformanceGroupStateDELETED},
		{"ERROR", gcpgenserver.VolumePerformanceGroupV1betaVolumePerformanceGroupStateERROR},
		{"UNKNOWN", gcpgenserver.VolumePerformanceGroupV1betaVolumePerformanceGroupStateSTATEUNSPECIFIED},
	}
	for _, tc := range tests {
		t.Run(tc.input, func(tt *testing.T) {
			assert.Equal(tt, tc.expected, toVPGState(tc.input))
		})
	}
}

func TestV1betaUpdateVolumePerformanceGroup_NullDescription(t *testing.T) {
	origEnableMqos := enableMqos
	origEnableVpgEndpoints := enableVpgEndpoints
	defer func() {
		enableMqos = origEnableMqos
		enableVpgEndpoints = origEnableVpgEndpoints
	}()
	enableMqos = true
	enableVpgEndpoints = true

	mockOrchestrator := factory.NewMockOrchestratorFactory(t)
	ctx := context.Background()

	desc := gcpgenserver.OptNilString{}
	desc.SetToNull()

	req := &gcpgenserver.VolumePerformanceGroupUpdateV1beta{
		Description: desc,
	}
	params := gcpgenserver.V1betaUpdateVolumePerformanceGroupParams{
		ProjectNumber:            "12345",
		LocationId:               "us-central1",
		PoolId:                   "pool-id",
		VolumePerformanceGroupId: "vpg-uuid",
	}

	expectedVPG := &models.VolumePerformanceGroup{
		BaseModel: models.BaseModel{UUID: "vpg-uuid"},
		Name:      "test-vpg",
	}

	mockOrchestrator.EXPECT().UpdateVolumePerformanceGroup(mock.Anything, mock.MatchedBy(func(p *common.UpdateVolumePerformanceGroupParams) bool {
		return p.Description != nil && *p.Description == ""
	})).Return(expectedVPG, "job-uuid", nil)

	handler := Handler{Orchestrator: mockOrchestrator}
	res, err := handler.V1betaUpdateVolumePerformanceGroup(ctx, req, params)

	assert.NoError(t, err)
	assert.NotNil(t, res)
	_, ok := res.(*gcpgenserver.OperationV1beta)
	assert.True(t, ok)
}

func TestV1betaUpdateVolumePerformanceGroup_EmptyLabels(t *testing.T) {
	origEnableMqos := enableMqos
	origEnableVpgEndpoints := enableVpgEndpoints
	defer func() {
		enableMqos = origEnableMqos
		enableVpgEndpoints = origEnableVpgEndpoints
	}()
	enableMqos = true
	enableVpgEndpoints = true

	mockOrchestrator := factory.NewMockOrchestratorFactory(t)
	ctx := context.Background()

	req := &gcpgenserver.VolumePerformanceGroupUpdateV1beta{
		Labels: gcpgenserver.NewOptVolumePerformanceGroupUpdateV1betaLabels(
			gcpgenserver.VolumePerformanceGroupUpdateV1betaLabels{}),
	}
	params := gcpgenserver.V1betaUpdateVolumePerformanceGroupParams{
		ProjectNumber:            "12345",
		LocationId:               "us-central1",
		PoolId:                   "pool-id",
		VolumePerformanceGroupId: "vpg-uuid",
	}

	expectedVPG := &models.VolumePerformanceGroup{
		BaseModel: models.BaseModel{UUID: "vpg-uuid"},
		Name:      "test-vpg",
	}

	mockOrchestrator.EXPECT().UpdateVolumePerformanceGroup(mock.Anything, mock.MatchedBy(func(p *common.UpdateVolumePerformanceGroupParams) bool {
		return p.Labels != nil && len(*p.Labels) == 0
	})).Return(expectedVPG, "job-uuid", nil)

	handler := Handler{Orchestrator: mockOrchestrator}
	res, err := handler.V1betaUpdateVolumePerformanceGroup(ctx, req, params)

	assert.NoError(t, err)
	assert.NotNil(t, res)
	_, ok := res.(*gcpgenserver.OperationV1beta)
	assert.True(t, ok)
}

func TestV1betaUpdateVolumePerformanceGroup_WithMetadata(t *testing.T) {
	origEnableMqos := enableMqos
	origEnableVpgEndpoints := enableVpgEndpoints
	defer func() {
		enableMqos = origEnableMqos
		enableVpgEndpoints = origEnableVpgEndpoints
	}()
	enableMqos = true
	enableVpgEndpoints = true

	t.Run("DescriptionAndLabelsPassedToOrchestrator", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		ctx := context.Background()
		req := &gcpgenserver.VolumePerformanceGroupUpdateV1beta{
			Description: gcpgenserver.NewOptNilString("updated description"),
			Labels: gcpgenserver.NewOptVolumePerformanceGroupUpdateV1betaLabels(
				gcpgenserver.VolumePerformanceGroupUpdateV1betaLabels{"env": "staging"}),
		}
		params := gcpgenserver.V1betaUpdateVolumePerformanceGroupParams{
			ProjectNumber:            "12345",
			LocationId:               "us-central1",
			PoolId:                   "pool-id",
			VolumePerformanceGroupId: "vpg-uuid",
		}

		expectedVPG := &models.VolumePerformanceGroup{
			BaseModel:      models.BaseModel{UUID: "vpg-uuid"},
			Name:           "test-vpg",
			Description:    "updated description",
			Labels:         map[string]string{"env": "staging"},
			LifeCycleState: "UPDATING",
		}

		handler := Handler{Orchestrator: mockOrchestrator}
		mockOrchestrator.EXPECT().UpdateVolumePerformanceGroup(mock.Anything, mock.MatchedBy(func(p *common.UpdateVolumePerformanceGroupParams) bool {
			return p.Description != nil && *p.Description == "updated description" && p.Labels != nil
		})).Return(expectedVPG, "job-uuid", nil)
		res, err := handler.V1betaUpdateVolumePerformanceGroup(ctx, req, params)

		assert.NoError(tt, err)
		opRes, ok := res.(*gcpgenserver.OperationV1beta)
		assert.True(tt, ok)

		var vpgBody gcpgenserver.VolumePerformanceGroupV1beta
		assert.NoError(tt, json.Unmarshal(opRes.Response, &vpgBody))
		assert.True(tt, vpgBody.Description.IsSet())
		assert.Equal(tt, "updated description", vpgBody.Description.Value)
		assert.True(tt, vpgBody.VolumePerformanceGroupState.IsSet())
		assert.True(tt, vpgBody.Labels.IsSet())
		assert.Equal(tt, "staging", vpgBody.Labels.Value["env"])
	})
}
