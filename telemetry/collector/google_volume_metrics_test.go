package collector

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"google.golang.org/api/iterator"
	metric "google.golang.org/genproto/googleapis/api/metric"
	"google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// setupTestJobQueue creates a test JobQueue with in-memory database
func setupTestJobQueue(t *testing.T) (*utils.JobQueue, func()) {
	gormDB, err := database.SetupTestDB()
	require.NoError(t, err)

	sqlDB, err := gormDB.DB()
	require.NoError(t, err)

	// Drop existing jobs table if it exists (VCP jobs table has different schema)
	_, err = sqlDB.Exec(`DROP TABLE IF EXISTS jobs`)
	require.NoError(t, err)

	// Create jobs table with JobQueue schema
	_, err = sqlDB.Exec(`
CREATE TABLE jobs (
id INTEGER PRIMARY KEY AUTOINCREMENT,
type_name TEXT NOT NULL,
status TEXT NOT NULL DEFAULT 'new',
queue TEXT NOT NULL,
data TEXT NOT NULL,
error TEXT,
attempt INTEGER DEFAULT 0,
created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
started_at DATETIME,
finished_at DATETIME,
scheduled_at DATETIME
)
`)
	require.NoError(t, err)

	jobQueue := utils.NewQueue(sqlDB, nil)

	cleanup := func() {
		_ = sqlDB.Close()
	}

	return jobQueue, cleanup
}

func TestGoogleTenantProjectProvider_GetTenantProjects_DelegatesToGetTenantProject(t *testing.T) {
	ctx := context.Background()
	logger := log.NewLogger()
	vcpStore := &database.MockStorage{}
	expectedProjects := []string{"projectX", "projectY"}

	vcpStore.On("ListTpProjects", ctx).Return(expectedProjects, nil)
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
	vcpStore.On("ListTpProjects", ctx).Return([]string{}, fmt.Errorf("db error"))

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

	vcpStore.On("ListTpProjects", ctx).Return(expectedProjects, nil)

	projects, err := GetTenantProject(ctx, logger, vcpStore)
	assert.NoError(t, err)
	assert.Equal(t, expectedProjects, projects)
	vcpStore.AssertExpectations(t)
}

func TestGetTenantProject_ReturnsErrorWhenListTpProjectsFails(t *testing.T) {
	ctx := context.Background()
	logger := log.NewLogger()
	vcpStore := &database.MockStorage{}

	vcpStore.On("ListTpProjects", ctx).Return([]string{}, fmt.Errorf("db error"))

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

	vcpStore.On("ListTpProjects", ctx).Return([]string{}, nil)

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
			Metric: &metric.Metric{
				Labels: map[string]string{
					"volume": "test-volume-1",
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
			Metric: &metric.Metric{
				Labels: map[string]string{
					"volume": "test-volume-1",
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
			Metric: &metric.Metric{
				Labels: map[string]string{
					"volume": "test-volume-1",
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

		expectedMetric1 := setupHydratedMetrics(metadata.AllocatedSize, metadata.Volume, "consumer1", mockResp)
		expectedMetric2 := setupHydratedMetrics(metadata.AllocatedSize, metadata.Volume, "consumer1", mockResp2)
		expectedMetric3 := setupHydratedMetrics(metadata.AllocatedSize, metadata.Volume, "consumer1", mockResp3)
		expected := []datamodel.HydratedMetrics{expectedMetric1, expectedMetric2, expectedMetric3}
		mockProvider.On("CollectProjectMetrics", ctx, logger, mock.Anything).Return(expected, nil)

		results, err := mockProvider.CollectProjectMetrics(ctx, logger, "project1")
		assert.NoError(t, err)
		assert.Equal(t, expected, results)
		assert.Equal(t, 1.0, results[0].Quantity)
		assert.Equal(t, 0.0, results[1].Quantity)
		assert.Equal(t, 1024.0, results[2].Quantity)
		mockProvider.AssertExpectations(t)
	})

	t.Run("returns metrics from provider including volume replication", func(t *testing.T) {
		mockProvider := new(MockVolumeMetricsProvider)
		mockResp := &monitoringpb.TimeSeries{
			Resource: &monitoredres.MonitoredResource{
				Labels: map[string]string{
					"name":     "test-volume",
					"location": "us-west1",
				},
			},
			Metric: &metric.Metric{
				Labels: map[string]string{
					"relationship_id": "relationship-1",
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
			Metric: &metric.Metric{
				Labels: map[string]string{
					"volume": "test-volume-1",
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
			Metric: &metric.Metric{
				Labels: map[string]string{
					"volume": "test-volume-1",
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

		expectedMetric1 := setupHydratedMetrics(metadata.AllocatedSize, metadata.VolumeReplicationRelationship, "consumer1", mockResp)
		expectedMetric2 := setupHydratedMetrics(metadata.AllocatedSize, metadata.Volume, "consumer1", mockResp2)
		expectedMetric3 := setupHydratedMetrics(metadata.AllocatedSize, metadata.Volume, "consumer1", mockResp3)
		expected := []datamodel.HydratedMetrics{expectedMetric1, expectedMetric2, expectedMetric3}
		mockProvider.On("CollectProjectMetrics", ctx, logger, mock.Anything).Return(expected, nil)

		results, err := mockProvider.CollectProjectMetrics(ctx, logger, "project1")
		assert.NoError(t, err)
		assert.Equal(t, expected, results)
		assert.Equal(t, 1.0, results[0].Quantity)
		assert.Equal(t, 0.0, results[1].Quantity)
		assert.Equal(t, 1024.0, results[2].Quantity)
		mockProvider.AssertExpectations(t)
	})

	t.Run("returns error from provider", func(t *testing.T) {
		mockProvider := new(MockVolumeMetricsProvider)
		mockProvider.On("CollectProjectMetrics", ctx, logger, mock.Anything).Return([]datamodel.HydratedMetrics(nil), fmt.Errorf("fail"))

		results, err := mockProvider.CollectProjectMetrics(ctx, logger, "project1")
		assert.Error(t, err)
		assert.Nil(t, results)
		mockProvider.AssertExpectations(t)
	})
}

func TestCollectVolumeMetrics_ProviderReturnsEmpty(t *testing.T) {
	ctx := context.Background()
	logger := log.NewLogger()
	mockProvider := new(MockVolumeMetricsProvider)
	mockProvider.On("CollectProjectMetrics", ctx, logger, mock.Anything).Return([]datamodel.HydratedMetrics{}, nil)

	results, err := mockProvider.CollectProjectMetrics(ctx, logger, "project1")
	assert.NoError(t, err)
	assert.Empty(t, results)
	mockProvider.AssertExpectations(t)
}

func TestGoogleVolumeMetricsProvider_GetVolumeMetrics_Success(t *testing.T) {
	ctx := context.Background()
	logger := log.NewLogger()

	// Mock iterator
	mockIterator := new(MockTimeSeriesIterator)
	mockResp := &monitoringpb.TimeSeries{
		Resource: &monitoredres.MonitoredResource{
			Labels: map[string]string{
				"name":     "test-volume",
				"location": "us-west1",
			},
		},
		Metric: &metric.Metric{
			Labels: map[string]string{
				"volume": "test-volume-1",
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
		client:    mockClient,
		startTime: time.Now().Add(-time.Hour),
		endTime:   time.Now(),
		metrics: []common.MetricItem{
			{
				Metric:       "volume_space_logical_used",
				ResourceType: "custom.googleapis.com",
			},
		},
	}

	results, err := provider.CollectProjectMetrics(ctx, logger, "project1")
	assert.NoError(t, err)
	assert.Len(t, results, 1)
	assert.Equal(t, metadata.MeasuredType("LOGICAL_SIZE"), results[0].MeasuredType)
	mockClient.AssertExpectations(t)
	mockIterator.AssertExpectations(t)
}

func TestGoogleVolumeMetricsProvider_GetVolumeMetrics_EmptyPoints(t *testing.T) {
	ctx := context.Background()
	logger := log.NewLogger()

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
		client:    mockClient,
		startTime: time.Now().Add(-time.Hour),
		endTime:   time.Now(),
		metrics: []common.MetricItem{
			{
				Metric:       "volume_read_ops",
				ResourceType: "custom.googleapis.com",
			},
		},
	}

	results, err := provider.CollectProjectMetrics(ctx, logger, "project1")
	assert.NoError(t, err)
	assert.Empty(t, results) // Should be empty since no valid data points
	mockClient.AssertExpectations(t)
	mockIterator.AssertExpectations(t)
}

// Comprehensive tests for GetVolumeMetrics method to achieve 100% coverage
func TestGoogleVolumeMetricsProvider_GetVolumeMetrics_Success_NoProjects(t *testing.T) {
	ctx := context.Background()
	logger := log.NewLogger()

	// Mock tenant project provider that returns empty projects list
	mockTenantProvider := new(MockTenantProjectProvider)
	mockTenantProvider.On("GetTenantProjects", ctx, logger).Return([]string{}, nil)

	// Create test job queue (won't be used since no projects)
	jobQueue, cleanup := setupTestJobQueue(t)
	defer cleanup()

	provider := &GoogleVolumeMetricsProvider{
		tenantProjectProvider: mockTenantProvider,
		jobQueue:              jobQueue,
	}

	err := provider.GetVolumeMetrics(ctx, logger)
	assert.NoError(t, err)

	mockTenantProvider.AssertExpectations(t)
}

func TestGoogleVolumeMetricsProvider_GetVolumeMetrics_Success_SingleProject(t *testing.T) {
	ctx := context.Background()
	logger := log.NewLogger()

	projects := []string{"project1"}

	// Mock tenant project provider
	mockTenantProvider := new(MockTenantProjectProvider)
	mockTenantProvider.On("GetTenantProjects", ctx, logger).Return(projects, nil)

	// Create test job queue
	jobQueue, cleanup := setupTestJobQueue(t)
	defer cleanup()

	provider := &GoogleVolumeMetricsProvider{
		tenantProjectProvider: mockTenantProvider,
		jobQueue:              jobQueue,
	}

	err := provider.GetVolumeMetrics(ctx, logger)
	assert.NoError(t, err)

	mockTenantProvider.AssertExpectations(t)
}

func TestGoogleVolumeMetricsProvider_GetVolumeMetrics_Success_MultipleProjects(t *testing.T) {
	ctx := context.Background()
	logger := log.NewLogger()

	projects := []string{"project1", "project2", "project3"}

	// Mock tenant project provider
	mockTenantProvider := new(MockTenantProjectProvider)
	mockTenantProvider.On("GetTenantProjects", ctx, logger).Return(projects, nil)

	// Create test job queue
	jobQueue, cleanup := setupTestJobQueue(t)
	defer cleanup()

	provider := &GoogleVolumeMetricsProvider{
		tenantProjectProvider: mockTenantProvider,
		jobQueue:              jobQueue,
	}

	err := provider.GetVolumeMetrics(ctx, logger)
	assert.NoError(t, err)

	mockTenantProvider.AssertExpectations(t)
}

func TestGoogleVolumeMetricsProvider_GetVolumeMetrics_ErrorFromGetTenantProjects(t *testing.T) {
	ctx := context.Background()
	logger := log.NewLogger()

	expectedError := errors.New("database connection failed")

	// Mock tenant project provider that returns error
	mockTenantProvider := new(MockTenantProjectProvider)
	mockTenantProvider.On("GetTenantProjects", ctx, logger).Return([]string(nil), expectedError)

	// Create test job queue (won't be used due to early error)
	jobQueue, cleanup := setupTestJobQueue(t)
	defer cleanup()

	provider := &GoogleVolumeMetricsProvider{
		tenantProjectProvider: mockTenantProvider,
		jobQueue:              jobQueue,
	}

	err := provider.GetVolumeMetrics(ctx, logger)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get tenant projects")
	assert.Contains(t, err.Error(), "database connection failed")

	mockTenantProvider.AssertExpectations(t)
}
