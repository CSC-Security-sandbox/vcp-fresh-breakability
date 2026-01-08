package api

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

func TestV1betaCreateVolumePerformanceGroup_NotImplemented(t *testing.T) {
	mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
	ctx := context.Background()
	req := &gcpgenserver.VolumePerformanceGroupCreateV1beta{
		ResourceId: "test-performance-group",
	}
	params := gcpgenserver.V1betaCreateVolumePerformanceGroupParams{
		ProjectNumber: "12345",
		LocationId:    "us-central1",
		PoolId:        "pool-id",
	}

	handler := Handler{Orchestrator: mockOrchestrator}
	mockOrchestrator.EXPECT().CreateVolumePerformanceGroup(mock.Anything, mock.Anything).Return(nil, errors.New("volume performance group creation is not implemented"))
	res, err := handler.V1betaCreateVolumePerformanceGroup(ctx, req, params)

	assert.Error(t, err)
	assert.Equal(t, err, errors.New("volume performance group creation is not implemented"))
	assert.NotNil(t, res)
	assert.Equal(t, float64(500), res.(*gcpgenserver.V1betaCreateVolumePerformanceGroupInternalServerError).Code)
	assert.Equal(t, "Internal server error", res.(*gcpgenserver.V1betaCreateVolumePerformanceGroupInternalServerError).Message)
}

func TestV1betaListVolumePerformanceGroups_NotImplemented(t *testing.T) {
	mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
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
	assert.Equal(t, err, errors.New("listing volume performance groups is not implemented"))
	assert.NotNil(t, res)
	assert.Equal(t, float64(500), res.(*gcpgenserver.V1betaListVolumePerformanceGroupsInternalServerError).Code)
	assert.Equal(t, "Internal server error", res.(*gcpgenserver.V1betaListVolumePerformanceGroupsInternalServerError).Message)
}

func TestV1betaDescribeVolumePerformanceGroup_NotImplemented(t *testing.T) {
	mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
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
	assert.Equal(t, err, errors.New("get volume performance group is not implemented"))
	assert.NotNil(t, res)
	assert.Equal(t, float64(500), res.(*gcpgenserver.V1betaDescribeVolumePerformanceGroupInternalServerError).Code)
	assert.Equal(t, "Internal server error", res.(*gcpgenserver.V1betaDescribeVolumePerformanceGroupInternalServerError).Message)
}

func TestV1betaUpdateVolumePerformanceGroup_NotImplemented(t *testing.T) {
	mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
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
	assert.Equal(t, err, errors.New("updating volume performance group is not implemented"))
	assert.NotNil(t, res)
	assert.Equal(t, float64(500), res.(*gcpgenserver.V1betaUpdateVolumePerformanceGroupInternalServerError).Code)
	assert.Equal(t, "Internal server error", res.(*gcpgenserver.V1betaUpdateVolumePerformanceGroupInternalServerError).Message)
}

func TestV1betaDeleteVolumePerformanceGroup_NotImplemented(t *testing.T) {
	mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
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
	assert.Equal(t, err, errors.New("deleting volume performance group is not implemented"))
	assert.NotNil(t, res)
	assert.Equal(t, float64(500), res.(*gcpgenserver.V1betaDeleteVolumePerformanceGroupInternalServerError).Code)
	assert.Equal(t, "Internal server error", res.(*gcpgenserver.V1betaDeleteVolumePerformanceGroupInternalServerError).Message)
}
