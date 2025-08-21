package collector

import (
	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"context"
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"google.golang.org/api/iterator"
	"google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/protobuf/types/known/timestamppb"
	"testing"
	"time"
)

func TestGoogleTenantProjectProvider_GetTenantProjects_DelegatesToGetTenantProject(t *testing.T) {
	ctx := context.Background()
	logger := log.NewLogger()
	vcpStore := &database.MockStorage{}
	expectedProjects := []string{"projectX", "projectY"}

	vcpStore.On("ListSnHosts", ctx).Return(expectedProjects, nil)
	provider := &GoogleTenantProjectProvider{vcpDatastore: vcpStore}

	projects, err := provider.GetTenantProjects(ctx, logger)
	assert.NoError(t, err)
	assert.Equal(t, expectedProjects, projects)
	vcpStore.AssertExpectations(t)
}

func TestGetTenantProject_LogsErrorOnFailure(t *testing.T) {
	ctx := context.Background()
	logger := log.NewLogger()
	vcpStore := &database.MockStorage{}
	vcpStore.On("ListSnHosts", ctx).Return([]string{}, fmt.Errorf("db error"))

	projects, err := GetTenantProject(ctx, logger, vcpStore)
	assert.Error(t, err)
	assert.Nil(t, projects)
	vcpStore.AssertExpectations(t)
}
func TestGetTenantProject_ReturnsProjectsSuccessfully(t *testing.T) {
	ctx := context.Background()
	logger := log.NewLogger()
	vcpStore := &database.MockStorage{}
	expectedProjects := []string{"project1", "project2"}

	vcpStore.On("ListSnHosts", ctx).Return(expectedProjects, nil)

	projects, err := GetTenantProject(ctx, logger, vcpStore)
	assert.NoError(t, err)
	assert.Equal(t, expectedProjects, projects)
	vcpStore.AssertExpectations(t)
}

func TestGetTenantProject_ReturnsErrorWhenListSnHostsFails(t *testing.T) {
	ctx := context.Background()
	logger := log.NewLogger()
	vcpStore := &database.MockStorage{}

	vcpStore.On("ListSnHosts", ctx).Return([]string{}, fmt.Errorf("db error"))

	projects, err := GetTenantProject(ctx, logger, vcpStore)
	assert.Error(t, err)
	assert.Nil(t, projects)
	assert.Contains(t, err.Error(), "failed to list SnHostsProjects")
	vcpStore.AssertExpectations(t)
}

func TestGetTenantProject_ReturnsErrorWhenNoProjectsFound(t *testing.T) {
	ctx := context.Background()
	logger := log.NewLogger()
	vcpStore := &database.MockStorage{}

	vcpStore.On("ListSnHosts", ctx).Return([]string{}, nil)

	projects, err := GetTenantProject(ctx, logger, vcpStore)
	assert.Error(t, err)
	assert.Nil(t, projects)
	assert.Equal(t, "no projects found from DB", err.Error())
	vcpStore.AssertExpectations(t)
}

func TestGetTenantProjects_ReturnsProjectsFromUnderlyingFunction(t *testing.T) {
	ctx := context.Background()
	logger := log.NewLogger()
	expectedProjects := []string{"projectA", "projectB"}

	mockTenantProvider := new(MockTenantProjectProvider)
	mockTenantProvider.On("GetTenantProjects", ctx, logger).Return(expectedProjects, nil)

	projects, err := mockTenantProvider.GetTenantProjects(ctx, logger)
	assert.NoError(t, err)
	assert.ElementsMatch(t, expectedProjects, projects)
	mockTenantProvider.AssertExpectations(t)
}

func TestGetTenantProjects_PropagatesErrorFromUnderlyingFunction(t *testing.T) {
	ctx := context.Background()
	logger := log.NewLogger()
	mockTenantProvider := new(MockTenantProjectProvider)
	mockTenantProvider.On("GetTenantProjects", ctx, logger).Return(nil, errors.New("db error"))

	projects, err := mockTenantProvider.GetTenantProjects(ctx, logger)
	assert.Error(t, err)
	assert.Nil(t, projects)
	assert.Contains(t, err.Error(), "db error")
	mockTenantProvider.AssertExpectations(t)
}

func TestGetTenantProjects_ReturnsErrorWhenNoPoolsFound(t *testing.T) {
	ctx := context.Background()
	logger := log.NewLogger()
	mockTenantProvider := new(MockTenantProjectProvider)
	mockTenantProvider.On("GetTenantProjects", ctx, logger).Return(nil, fmt.Errorf("no pools found from DB"))

	projects, err := mockTenantProvider.GetTenantProjects(ctx, logger)
	assert.Error(t, err)
	assert.Nil(t, projects)
	assert.Equal(t, "no pools found from DB", err.Error())
	mockTenantProvider.AssertExpectations(t)
}

func TestCollectVolumeMetrics(t *testing.T) {
	ctx := context.Background()
	logger := log.NewLogger()

	t.Run("returns metrics from provider", func(t *testing.T) {
		mockProvider := new(MockVolumeMetricsProvider)
		mockResp := &monitoringpb.TimeSeries{
			Resource: &monitoredres.MonitoredResource{
				Labels: map[string]string{
					"name":     "test-volume",
					"location": "us-west1",
				},
			},
			Points: []*monitoringpb.Point{
				{
					Interval: &monitoringpb.TimeInterval{
						EndTime: timestamppb.New(time.Now()),
					},
					Value: &monitoringpb.TypedValue{
						Value: &monitoringpb.TypedValue_BoolValue{BoolValue: true},
					},
				},
			},
		}

		mockResp2 := &monitoringpb.TimeSeries{
			Resource: &monitoredres.MonitoredResource{
				Labels: map[string]string{
					"name":     "test-volume-3",
					"location": "us-west1",
				},
			},
			Points: []*monitoringpb.Point{
				{
					Interval: &monitoringpb.TimeInterval{
						EndTime: timestamppb.New(time.Now()),
					},
					Value: &monitoringpb.TypedValue{
						Value: &monitoringpb.TypedValue_StringValue{StringValue: "unsupported"},
					},
				},
			},
		}

		mockResp3 := &monitoringpb.TimeSeries{
			Resource: &monitoredres.MonitoredResource{
				Labels: map[string]string{
					"name":     "test-volume-3",
					"location": "us-west1",
				},
			},
			Points: []*monitoringpb.Point{
				{
					Interval: &monitoringpb.TimeInterval{
						EndTime: timestamppb.New(time.Now()),
					},
					Value: &monitoringpb.TypedValue{
						Value: &monitoringpb.TypedValue_Int64Value{Int64Value: 1024},
					},
				},
			},
		}

		expectedMetric1 := setupHydratedMetrics(metadata.FileSystemReadOps, metadata.Volume, "consumer1", mockResp)
		expectedMetric2 := setupHydratedMetrics(metadata.FileSystemWriteOps, metadata.Volume, "consumer1", mockResp2)
		expectedMetric3 := setupHydratedMetrics(metadata.FileSystemReadOps, metadata.Volume, "consumer1", mockResp3)
		expected := []datamodel.HydratedMetrics{expectedMetric1, expectedMetric2, expectedMetric3}
		mockProvider.On("GetVolumeMetrics", ctx, logger).Return(expected, nil)

		results, err := CollectVolumeMetrics(ctx, logger, mockProvider)
		assert.NoError(t, err)
		assert.Equal(t, expected, results)
		assert.Equal(t, 1.0, results[0].Quantity)
		assert.Equal(t, 0.0, results[1].Quantity)
		assert.Equal(t, 1024.0, results[2].Quantity)
		mockProvider.AssertExpectations(t)
	})

	t.Run("returns error from provider", func(t *testing.T) {
		mockProvider := new(MockVolumeMetricsProvider)
		mockProvider.On("GetVolumeMetrics", ctx, logger).Return([]datamodel.HydratedMetrics(nil), fmt.Errorf("fail"))

		results, err := CollectVolumeMetrics(ctx, logger, mockProvider)
		assert.Error(t, err)
		assert.Nil(t, results)
		mockProvider.AssertExpectations(t)
	})
}

func TestCollectVolumeMetrics_ProviderReturnsEmpty(t *testing.T) {
	ctx := context.Background()
	logger := log.NewLogger()
	mockProvider := new(MockVolumeMetricsProvider)
	mockProvider.On("GetVolumeMetrics", ctx, logger).Return([]datamodel.HydratedMetrics{}, nil)

	results, err := CollectVolumeMetrics(ctx, logger, mockProvider)
	assert.NoError(t, err)
	assert.Empty(t, results)
	mockProvider.AssertExpectations(t)
}

func TestGoogleVolumeMetricsProvider_GetVolumeMetrics_Success(t *testing.T) {
	ctx := context.Background()
	logger := log.NewLogger()

	// Mock tenantProjectProvider
	mockTenantProvider := new(MockTenantProjectProvider)
	mockTenantProvider.On("GetTenantProjects", ctx, logger).Return([]string{"project1"}, nil)

	// Mock iterator
	mockIterator := new(MockTimeSeriesIterator)
	mockResp := &monitoringpb.TimeSeries{
		Resource: &monitoredres.MonitoredResource{
			Labels: map[string]string{
				"name":     "test-volume",
				"location": "us-west1",
			},
		},
		Points: []*monitoringpb.Point{
			{
				Interval: &monitoringpb.TimeInterval{
					EndTime: timestamppb.New(time.Now()),
				},
				Value: &monitoringpb.TypedValue{
					Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: 123.45},
				},
			},
		},
	}
	mockIterator.On("Next").Return(mockResp, nil).Once()
	mockIterator.On("Next").Return(nil, iterator.Done).Once()
	mockIterator.On("Next").Return(nil, iterator.Done)

	// Mock client
	mockClient := new(MockMonitoringClient)
	mockClient.On("ListTimeSeries", ctx, mock.AnythingOfType("*monitoringpb.ListTimeSeriesRequest")).Return(mockIterator)

	provider := &GoogleVolumeMetricsProvider{
		tenantProjectProvider: mockTenantProvider,
		client:                mockClient,
		startTime:             time.Now().Add(-time.Hour),
		endTime:               time.Now(),
		metrics: []common.MetricItem{
			{
				Metric:       "volume_read_ops",
				ResourceType: "custom.googleapis.com",
			},
		},
	}

	results, err := provider.GetVolumeMetrics(ctx, logger)
	assert.NoError(t, err)
	assert.Len(t, results, 1)
	assert.Equal(t, metadata.MeasuredType("FILE_SYSTEM_READ_OPS"), results[0].MeasuredType)
	mockTenantProvider.AssertExpectations(t)
	mockClient.AssertExpectations(t)
	mockIterator.AssertExpectations(t)
}

func TestGoogleVolumeMetricsProvider_GetVolumeMetrics_EmptyPoints(t *testing.T) {
	ctx := context.Background()
	logger := log.NewLogger()

	// Mock tenantProjectProvider
	mockTenantProvider := new(MockTenantProjectProvider)
	mockTenantProvider.On("GetTenantProjects", ctx, logger).Return([]string{"project1"}, nil)

	// Mock iterator with empty points
	mockIterator := new(MockTimeSeriesIterator)
	mockResp := &monitoringpb.TimeSeries{
		Resource: &monitoredres.MonitoredResource{
			Labels: map[string]string{
				"name":     "test-volume",
				"location": "us-west1",
			},
		},
		Points: []*monitoringpb.Point{}, // Empty points array
	}
	mockIterator.On("Next").Return(mockResp, nil).Once()
	mockIterator.On("Next").Return(nil, iterator.Done).Once()
	mockIterator.On("Next").Return(nil, iterator.Done)

	// Mock client
	mockClient := new(MockMonitoringClient)
	mockClient.On("ListTimeSeries", ctx, mock.AnythingOfType("*monitoringpb.ListTimeSeriesRequest")).Return(mockIterator)

	provider := &GoogleVolumeMetricsProvider{
		tenantProjectProvider: mockTenantProvider,
		client:                mockClient,
		startTime:             time.Now().Add(-time.Hour),
		endTime:               time.Now(),
		metrics: []common.MetricItem{
			{
				Metric:       "volume_read_ops",
				ResourceType: "custom.googleapis.com",
			},
		},
	}

	results, err := provider.GetVolumeMetrics(ctx, logger)
	assert.NoError(t, err)
	assert.Empty(t, results) // Should be empty since no valid data points
	mockTenantProvider.AssertExpectations(t)
	mockClient.AssertExpectations(t)
	mockIterator.AssertExpectations(t)
}
