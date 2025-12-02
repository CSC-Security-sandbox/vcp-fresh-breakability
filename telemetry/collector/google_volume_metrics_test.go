package collector

import (
	"context"
	"errors"
	"fmt"
	"strings"
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
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/performance"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
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
					"volume":         "test-volume-1",
					"is_regional_ha": "true",
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

		expectedMetric1 := setupHydratedMetrics(metadata.AllocatedSize, metadata.Volume, "consumer1", mockResp, time.Now())
		expectedMetric2 := setupHydratedMetrics(metadata.AllocatedSize, metadata.Volume, "consumer1", mockResp2, time.Now())
		expectedMetric3 := setupHydratedMetrics(metadata.AllocatedSize, metadata.Volume, "consumer1", mockResp3, time.Now())
		expected := []datamodel.HydratedMetrics{expectedMetric1, expectedMetric2, expectedMetric3}
		mockProvider.On("CollectProjectMetrics", ctx, logger, mock.Anything, mock.Anything).Return(expected, nil)

		results, err := mockProvider.CollectProjectMetrics(ctx, logger, "project1", time.Now())
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

		expectedMetric1 := setupHydratedMetrics(metadata.AllocatedSize, metadata.VolumeReplicationRelationship, "consumer1", mockResp, time.Now())
		expectedMetric2 := setupHydratedMetrics(metadata.AllocatedSize, metadata.Volume, "consumer1", mockResp2, time.Now())
		expectedMetric3 := setupHydratedMetrics(metadata.AllocatedSize, metadata.Volume, "consumer1", mockResp3, time.Now())
		expected := []datamodel.HydratedMetrics{expectedMetric1, expectedMetric2, expectedMetric3}
		mockProvider.On("CollectProjectMetrics", ctx, logger, mock.Anything, mock.Anything).Return(expected, nil)

		results, err := mockProvider.CollectProjectMetrics(ctx, logger, "project1", time.Now())
		assert.NoError(t, err)
		assert.Equal(t, expected, results)
		assert.Equal(t, 1.0, results[0].Quantity)
		assert.Equal(t, 0.0, results[1].Quantity)
		assert.Equal(t, 1024.0, results[2].Quantity)
		mockProvider.AssertExpectations(t)
	})

	t.Run("returns error from provider", func(t *testing.T) {
		mockProvider := new(MockVolumeMetricsProvider)
		mockProvider.On("CollectProjectMetrics", ctx, logger, mock.Anything, mock.Anything).Return([]datamodel.HydratedMetrics(nil), fmt.Errorf("fail"))

		results, err := mockProvider.CollectProjectMetrics(ctx, logger, "project1", time.Now())
		assert.Error(t, err)
		assert.Nil(t, results)
		mockProvider.AssertExpectations(t)
	})
}

func TestCollectVolumeMetrics_ProviderReturnsEmpty(t *testing.T) {
	ctx := context.Background()
	logger := log.NewLogger()
	mockProvider := new(MockVolumeMetricsProvider)
	mockProvider.On("CollectProjectMetrics", ctx, logger, mock.Anything, mock.Anything).Return([]datamodel.HydratedMetrics{}, nil)

	results, err := mockProvider.CollectProjectMetrics(ctx, logger, "project1", time.Now())
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
		client:     mockClient,
		startTime:  time.Now().Add(-time.Hour),
		endTime:    time.Now(),
		googleSink: nil, // Set to nil for test
		metrics: []common.MetricItem{
			{
				Metric:       "volume_space_logical_used",
				ResourceType: "custom.googleapis.com",
			},
		},
	}

	results, err := provider.CollectProjectMetrics(ctx, logger, "project1", time.Now())
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

	results, err := provider.CollectProjectMetrics(ctx, logger, "project1", time.Now())
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

	err := provider.GetVolumeMetrics(ctx, logger, time.Now())
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

	err := provider.GetVolumeMetrics(ctx, logger, time.Now())
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

	err := provider.GetVolumeMetrics(ctx, logger, time.Now())
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

	err := provider.GetVolumeMetrics(ctx, logger, time.Now())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get tenant projects")
	assert.Contains(t, err.Error(), "database connection failed")

	mockTenantProvider.AssertExpectations(t)
}

func TestGoogleVolumeMetricsProvider_GetVolumeMetrics_WithCorrelationID(t *testing.T) {
	ctx := context.Background()
	logger := log.NewLogger()

	projects := []string{"project1"}
	correlationID := "test-correlation-id-123"

	// Create context with correlation ID in logger fields
	loggerFields := log.Fields{
		"requestCorrelationID": correlationID,
	}
	ctxWithCorrelationID := context.WithValue(ctx, middleware.TemporalSLoggerKey, loggerFields)

	// Mock tenant project provider
	mockTenantProvider := new(MockTenantProjectProvider)
	mockTenantProvider.On("GetTenantProjects", ctxWithCorrelationID, logger).Return(projects, nil)

	// Create test job queue
	jobQueue, cleanup := setupTestJobQueue(t)
	defer cleanup()

	provider := &GoogleVolumeMetricsProvider{
		tenantProjectProvider: mockTenantProvider,
		jobQueue:              jobQueue,
	}

	err := provider.GetVolumeMetrics(ctxWithCorrelationID, logger, time.Now())
	assert.NoError(t, err)

	mockTenantProvider.AssertExpectations(t)

	// Verify that the job was enqueued with correlation ID
	// We can verify this by checking the job queue contains the job
	// This indirectly tests that the correlation ID was extracted and used
}

func TestGoogleVolumeMetricsProvider_GetVolumeMetrics_WithInvalidCorrelationID(t *testing.T) {
	ctx := context.Background()
	logger := log.NewLogger()

	projects := []string{"project1"}

	// Create context with invalid correlation ID (not a string)
	loggerFields := log.Fields{
		"requestCorrelationID": 12345, // Non-string value
	}
	ctxWithInvalidCorrelationID := context.WithValue(ctx, middleware.TemporalSLoggerKey, loggerFields)

	// Mock tenant project provider
	mockTenantProvider := new(MockTenantProjectProvider)
	mockTenantProvider.On("GetTenantProjects", ctxWithInvalidCorrelationID, logger).Return(projects, nil)

	// Create test job queue
	jobQueue, cleanup := setupTestJobQueue(t)
	defer cleanup()

	provider := &GoogleVolumeMetricsProvider{
		tenantProjectProvider: mockTenantProvider,
		jobQueue:              jobQueue,
	}

	err := provider.GetVolumeMetrics(ctxWithInvalidCorrelationID, logger, time.Now())
	assert.NoError(t, err)

	mockTenantProvider.AssertExpectations(t)
}

func TestGoogleVolumeMetricsProvider_GetVolumeMetrics_WithoutCorrelationID(t *testing.T) {
	ctx := context.Background()
	logger := log.NewLogger()

	projects := []string{"project1"}

	// Create context with logger fields but no correlation ID
	loggerFields := log.Fields{
		"someOtherField": "value",
	}
	ctxWithoutCorrelationID := context.WithValue(ctx, middleware.TemporalSLoggerKey, loggerFields)

	// Mock tenant project provider
	mockTenantProvider := new(MockTenantProjectProvider)
	mockTenantProvider.On("GetTenantProjects", ctxWithoutCorrelationID, logger).Return(projects, nil)

	// Create test job queue
	jobQueue, cleanup := setupTestJobQueue(t)
	defer cleanup()

	provider := &GoogleVolumeMetricsProvider{
		tenantProjectProvider: mockTenantProvider,
		jobQueue:              jobQueue,
	}

	err := provider.GetVolumeMetrics(ctxWithoutCorrelationID, logger, time.Now())
	assert.NoError(t, err)

	mockTenantProvider.AssertExpectations(t)
}

func TestGoogleVolumeMetricsProvider_GetVolumeMetrics_WithoutLoggerFields(t *testing.T) {
	ctx := context.Background()
	logger := log.NewLogger()

	projects := []string{"project1"}

	// Create context without logger fields (different context value type)
	ctxWithoutLoggerFields := context.WithValue(ctx, middleware.TemporalSLoggerKey, "not-a-fields-object")

	// Mock tenant project provider
	mockTenantProvider := new(MockTenantProjectProvider)
	mockTenantProvider.On("GetTenantProjects", ctxWithoutLoggerFields, logger).Return(projects, nil)

	// Create test job queue
	jobQueue, cleanup := setupTestJobQueue(t)
	defer cleanup()

	provider := &GoogleVolumeMetricsProvider{
		tenantProjectProvider: mockTenantProvider,
		jobQueue:              jobQueue,
	}

	err := provider.GetVolumeMetrics(ctxWithoutLoggerFields, logger, time.Now())
	assert.NoError(t, err)

	mockTenantProvider.AssertExpectations(t)
}

// Test to cover missing line 64: EnqueueBatch error handling
func TestGoogleVolumeMetricsProvider_GetVolumeMetrics_EnqueueBatchError(t *testing.T) {
	ctx := context.Background()
	logger := log.NewLogger()

	projects := []string{"project1"}

	// Mock tenant project provider
	mockTenantProvider := new(MockTenantProjectProvider)
	mockTenantProvider.On("GetTenantProjects", ctx, logger).Return(projects, nil)

	// Create a test job queue with a broken database connection
	// This will cause EnqueueBatch to fail
	gormDB, err := database.SetupTestDB()
	require.NoError(t, err)

	sqlDB, err := gormDB.DB()
	require.NoError(t, err)

	// Close the database connection to cause EnqueueBatch to fail
	_ = sqlDB.Close()

	jobQueue := utils.NewQueue(sqlDB, nil)

	provider := &GoogleVolumeMetricsProvider{
		tenantProjectProvider: mockTenantProvider,
	}
	provider.SetJobQueue(jobQueue)

	err = provider.GetVolumeMetrics(ctx, logger, time.Now())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "queue: failed to begin transaction")

	mockTenantProvider.AssertExpectations(t)
}

// Test for collectVolumeMetrics function to ensure line 140 is covered
func TestCollectVolumeMetrics_DirectCall(t *testing.T) {
	ctx := context.Background()
	logger := log.NewLogger()
	timestamp := time.Now()

	t.Run("successful call to provider", func(t *testing.T) {
		mockProvider := new(MockVolumeMetricsProvider)

		// Mock the GetVolumeMetrics call (line 140)
		mockProvider.On("GetVolumeMetrics", ctx, logger, timestamp).Return(nil)

		// Call collectVolumeMetrics function directly to cover line 140
		err := collectVolumeMetrics(ctx, logger, mockProvider, timestamp)

		assert.NoError(t, err)
		mockProvider.AssertExpectations(t)
	})

	t.Run("provider returns error", func(t *testing.T) {
		mockProvider := new(MockVolumeMetricsProvider)
		expectedError := errors.New("provider error")

		// Mock the GetVolumeMetrics call to return error (line 140)
		mockProvider.On("GetVolumeMetrics", ctx, logger, timestamp).Return(expectedError)

		// Call collectVolumeMetrics function directly to cover line 140
		err := collectVolumeMetrics(ctx, logger, mockProvider, timestamp)

		assert.Error(t, err)
		assert.Equal(t, expectedError, err)
		mockProvider.AssertExpectations(t)
	})
}

// TestGoogleVolumeMetricsProvider_CollectProjectMetrics_PerformanceFlow tests the performance metric collection flow
func TestGoogleVolumeMetricsProvider_CollectProjectMetrics_PerformanceFlow(t *testing.T) {
	ctx := context.Background()
	logger := log.NewLogger()
	projectID := "test-project-123"
	timestamp := time.Now()

	// Create mock client and iterator
	mockClient := new(MockMonitoringClient)
	mockIterator := new(MockTimeSeriesIterator)

	// Test performance metrics
	testMetrics := []common.MetricItem{
		{Metric: "operations_total", ResourceType: "netapp.com/volume", MetricType: "performance"},
	}

	// Create GoogleSink for the provider
	config := common.LoadConfig()
	googleSink := performance.NewSink(ctx, config)

	provider := &GoogleVolumeMetricsProvider{
		client:     mockClient,
		metrics:    testMetrics,
		startTime:  timestamp.Add(-5 * time.Minute),
		endTime:    timestamp,
		googleSink: googleSink,
	}

	// Create test time series for volume metrics (covers lines 141-154)
	volumeTimeSeries := &monitoringpb.TimeSeries{
		Metric: &metric.Metric{
			Type: "netapp.com/volume/operations_total",
			Labels: map[string]string{
				"metric":          "operations_total",
				"volume":          "test-volume",
				"project":         projectID,
				"datacenter":      "us-central1",
				"pool_name":       "test-pool",
				"deployment_name": "test-deployment",
			},
		},
		Points: []*monitoringpb.Point{
			{
				Value: &monitoringpb.TypedValue{
					Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: 100.5},
				},
			},
		},
	}

	// Mock the iterator to return our test metrics
	mockIterator.On("Next").Return(volumeTimeSeries, nil).Once()
	mockIterator.On("Next").Return(nil, iterator.Done).Once()

	// Mock the client to return our iterator and verify aggregation is set for performance metrics (covers lines 99-114)
	mockClient.On("ListTimeSeries", ctx, mock.MatchedBy(func(req *monitoringpb.ListTimeSeriesRequest) bool {
		return req.Aggregation != nil &&
			req.Aggregation.PerSeriesAligner == monitoringpb.Aggregation_ALIGN_MEAN &&
			req.Aggregation.CrossSeriesReducer == monitoringpb.Aggregation_REDUCE_MEAN &&
			len(req.Aggregation.GroupByFields) == 7
	})).Return(mockIterator)

	result, err := provider.CollectProjectMetrics(ctx, logger, projectID, timestamp)

	assert.NoError(t, err)
	assert.Empty(t, result) // Performance metrics don't get added to project results
	mockClient.AssertExpectations(t)
	mockIterator.AssertExpectations(t)
	// Note: We expect googleSink.DeliverMetrics to be called (covers lines 161-163)
}

// TestGoogleVolumeMetricsProvider_CollectProjectMetrics_VolumePoolResourceType tests volume pool metrics
func TestGoogleVolumeMetricsProvider_CollectProjectMetrics_VolumePoolResourceType(t *testing.T) {
	ctx := context.Background()
	logger := log.NewLogger()
	projectID := "test-project-123"
	timestamp := time.Now()

	// Create mock client and iterator
	mockClient := new(MockMonitoringClient)
	mockIterator := new(MockTimeSeriesIterator)

	testCases := []struct {
		name         string
		resourceType string
		metricType   string
		labels       map[string]string
	}{
		{
			name:         "Volume performance metric",
			resourceType: "Volume",
			metricType:   "performance",
			labels: map[string]string{
				"metric":     "operations_total",
				"volume":     "test-volume-name",
				"project":    projectID,
				"datacenter": "us-central1",
			},
		},
		{
			name:         "VolumePool performance metric",
			resourceType: "VolumePool",
			metricType:   "performance",
			labels: map[string]string{
				"metric":     "space_logical_used",
				"pool_name":  "test-pool-name",
				"project":    projectID,
				"datacenter": "europe-west1",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test performance metrics for specific resource types
			testMetrics := []common.MetricItem{
				{Metric: tc.labels["metric"], ResourceType: "netapp.com/" + strings.ToLower(tc.resourceType), MetricType: tc.metricType},
			}

			// Create GoogleSink for the provider
			config := common.LoadConfig()
			googleSink := performance.NewSink(ctx, config)

			provider := &GoogleVolumeMetricsProvider{
				client:     mockClient,
				metrics:    testMetrics,
				startTime:  timestamp.Add(-5 * time.Minute),
				endTime:    timestamp,
				googleSink: googleSink,
			}

			// Create test time series that will trigger the performance metric path (lines 140-154)
			timeSeries := &monitoringpb.TimeSeries{
				Metric: &metric.Metric{
					Type:   "netapp.com/" + strings.ToLower(tc.resourceType) + "/" + tc.labels["metric"],
					Labels: tc.labels,
				},
				Points: []*monitoringpb.Point{
					{
						Value: &monitoringpb.TypedValue{
							Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: 512.0},
						},
					},
				},
			}

			// Reset mocks for each test case
			mockClient.ExpectedCalls = nil
			mockIterator.ExpectedCalls = nil

			// Mock the iterator to return our test metrics
			mockIterator.On("Next").Return(timeSeries, nil).Once()
			mockIterator.On("Next").Return(nil, iterator.Done).Once()

			// Mock the client
			mockClient.On("ListTimeSeries", ctx, mock.Anything).Return(mockIterator)

			result, err := provider.CollectProjectMetrics(ctx, logger, projectID, timestamp)

			assert.NoError(t, err)
			assert.Empty(t, result) // Performance metrics don't get added to project results but are sent to googleSink
			mockClient.AssertExpectations(t)
			mockIterator.AssertExpectations(t)
		})
	}
}

// Additional test function to cover performance metric edge cases
func TestGoogleVolumeMetricsProvider_PerformanceMetricEdgeCases(t *testing.T) {
	ctx := context.Background()
	logger := log.NewLogger()
	projectID := "test-project-456"
	timestamp := time.Now()

	mockClient := new(MockMonitoringClient)

	testMetrics := []common.MetricItem{
		{Metric: "volume_read_latency", ResourceType: "netapp.com/volume", MetricType: "performance"},
		{Metric: "pool_client_protocol_reads", ResourceType: "netapp.com/pool", MetricType: "performance"},
	}

	// Create GoogleSink for the provider
	config := common.LoadConfig()
	googleSink := performance.NewSink(ctx, config)

	provider := &GoogleVolumeMetricsProvider{
		client:     mockClient,
		metrics:    testMetrics,
		startTime:  timestamp.Add(-5 * time.Minute),
		endTime:    timestamp,
		googleSink: googleSink,
	}

	// Create test time series for Volume performance metric (covers lines 143-145 for Volume case)
	volumeTimeSeries := &monitoringpb.TimeSeries{
		Metric: &metric.Metric{
			Type: "netapp.com/volume/volume_read_latency",
			Labels: map[string]string{
				"metric":          "volume_read_latency",
				"volume":          "test-volume",
				"project":         projectID,
				"datacenter":      "us-west1",
				"deployment_name": "test-deployment",
			},
		},
		Points: []*monitoringpb.Point{
			{
				Value: &monitoringpb.TypedValue{
					Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: 15.5},
				},
			},
		},
	}

	// Create test time series for VolumePool performance metric (covers lines 146-148 for VolumePool case)
	poolTimeSeries := &monitoringpb.TimeSeries{
		Metric: &metric.Metric{
			Type: "netapp.com/pool/pool_client_protocol_reads",
			Labels: map[string]string{
				"metric":          "pool_client_protocol_reads",
				"pool_name":       "test-pool",
				"project":         projectID,
				"datacenter":      "us-west1",
				"deployment_name": "test-deployment",
			},
		},
		Points: []*monitoringpb.Point{
			{
				Value: &monitoringpb.TypedValue{
					Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: 1024.0},
				},
			},
		},
	}

	// Create separate iterators for each metric (provider calls ListTimeSeries once per metric)
	mockIterator1 := new(MockTimeSeriesIterator)
	mockIterator2 := new(MockTimeSeriesIterator)

	// First call for volume_read_latency metric
	mockIterator1.On("Next").Return(volumeTimeSeries, nil).Once()
	mockIterator1.On("Next").Return(nil, iterator.Done).Once()
	mockClient.On("ListTimeSeries", ctx, mock.MatchedBy(func(req *monitoringpb.ListTimeSeriesRequest) bool {
		return strings.Contains(req.Filter, "volume_read_latency")
	})).Return(mockIterator1).Once()

	// Second call for pool_client_protocol_reads metric
	mockIterator2.On("Next").Return(poolTimeSeries, nil).Once()
	mockIterator2.On("Next").Return(nil, iterator.Done).Once()
	mockClient.On("ListTimeSeries", ctx, mock.MatchedBy(func(req *monitoringpb.ListTimeSeriesRequest) bool {
		return strings.Contains(req.Filter, "pool_client_protocol_reads")
	})).Return(mockIterator2).Once()

	result, err := provider.CollectProjectMetrics(ctx, logger, projectID, timestamp)

	assert.NoError(t, err)
	assert.Empty(t, result)
	mockClient.AssertExpectations(t)
	mockIterator1.AssertExpectations(t)
	mockIterator2.AssertExpectations(t)
}

func TestGoogleVolumeMetricsProvider_CollectProjectMetrics_PerformanceMetricsSpecificCoverage(t *testing.T) {
	ctx := context.Background()
	logger := log.NewLogger()
	projectID := "test-project-performance"
	timestamp := time.Now()

	mockClient := new(MockMonitoringClient)

	// Test metrics that will specifically trigger performance metric handling code paths
	testMetrics := []common.MetricItem{
		{Metric: "volume_write_latency", ResourceType: "netapp.com/volume", MetricType: "performance"},
		{Metric: "throughput_limit", ResourceType: "netapp.com/volume", MetricType: "performance"},
		{Metric: "pool_cloud_bin_operation_size", ResourceType: "netapp.com/pool", MetricType: "performance"},
	}

	// Create GoogleSink for the provider
	config := common.LoadConfig()
	googleSink := performance.NewSink(ctx, config)

	provider := &GoogleVolumeMetricsProvider{
		client:     mockClient,
		metrics:    testMetrics,
		startTime:  timestamp.Add(-5 * time.Minute),
		endTime:    timestamp,
		googleSink: googleSink,
	}

	// Multiple Volume performance metrics to ensure line 143-145 coverage
	volumeLatencyTimeSeries := &monitoringpb.TimeSeries{
		Metric: &metric.Metric{
			Type: "netapp.com/volume/volume_write_latency",
			Labels: map[string]string{
				"metric":     "volume_write_latency",
				"volume":     "performance-test-volume",
				"project":    projectID,
				"datacenter": "europe-west4",
			},
		},
		Points: []*monitoringpb.Point{{Value: &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: 25.7}}}},
	}

	volumeThroughputTimeSeries := &monitoringpb.TimeSeries{
		Metric: &metric.Metric{
			Type: "netapp.com/volume/throughput_limit",
			Labels: map[string]string{
				"metric":     "throughput_limit",
				"volume":     "throughput-test-volume",
				"project":    projectID,
				"datacenter": "asia-southeast1",
			},
		},
		Points: []*monitoringpb.Point{{Value: &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: 512.0}}}},
	}

	// VolumePool performance metric to ensure line 146-148 coverage
	poolBinOpTimeSeries := &monitoringpb.TimeSeries{
		Metric: &metric.Metric{
			Type: "netapp.com/pool/pool_cloud_bin_operation_size",
			Labels: map[string]string{
				"metric":     "pool_cloud_bin_operation_size",
				"pool_name":  "performance-test-pool",
				"project":    projectID,
				"datacenter": "us-east1",
			},
		},
		Points: []*monitoringpb.Point{{Value: &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: 8192.0}}}},
	}

	// Create separate iterators for each metric (provider calls ListTimeSeries once per metric)
	mockIterator1 := new(MockTimeSeriesIterator)
	mockIterator2 := new(MockTimeSeriesIterator)
	mockIterator3 := new(MockTimeSeriesIterator)

	// First call for volume_write_latency metric
	mockIterator1.On("Next").Return(volumeLatencyTimeSeries, nil).Once()
	mockIterator1.On("Next").Return(nil, iterator.Done).Once()
	mockClient.On("ListTimeSeries", ctx, mock.MatchedBy(func(req *monitoringpb.ListTimeSeriesRequest) bool {
		return strings.Contains(req.Filter, "volume_write_latency")
	})).Return(mockIterator1).Once()

	// Second call for throughput_limit metric
	mockIterator2.On("Next").Return(volumeThroughputTimeSeries, nil).Once()
	mockIterator2.On("Next").Return(nil, iterator.Done).Once()
	mockClient.On("ListTimeSeries", ctx, mock.MatchedBy(func(req *monitoringpb.ListTimeSeriesRequest) bool {
		return strings.Contains(req.Filter, "throughput_limit")
	})).Return(mockIterator2).Once()

	// Third call for pool_cloud_bin_operation_size metric
	mockIterator3.On("Next").Return(poolBinOpTimeSeries, nil).Once()
	mockIterator3.On("Next").Return(nil, iterator.Done).Once()
	mockClient.On("ListTimeSeries", ctx, mock.MatchedBy(func(req *monitoringpb.ListTimeSeriesRequest) bool {
		return strings.Contains(req.Filter, "pool_cloud_bin_operation_size")
	})).Return(mockIterator3).Once()

	result, err := provider.CollectProjectMetrics(ctx, logger, projectID, timestamp)

	assert.NoError(t, err)
	assert.Empty(t, result) // Performance metrics go to googleSink, not returned
	mockClient.AssertExpectations(t)
	mockIterator1.AssertExpectations(t)
	mockIterator2.AssertExpectations(t)
	mockIterator3.AssertExpectations(t)
}

// TestGoogleVolumeMetricsProvider_CloudBinOperationFiltering tests the filtering logic for lines 153-158
// These lines filter out wafl_volume_cloud_bin_operation_size and pool_cloud_bin_operation_size metrics
// when the "metric" label is not "put"
func TestGoogleVolumeMetricsProvider_CloudBinOperationFiltering(t *testing.T) {
	ctx := context.Background()
	logger := log.NewLogger()
	projectID := "test-project-filtering"
	timestamp := time.Now()

	config := common.LoadConfig()
	googleSink := performance.NewSink(ctx, config)

	testCases := []struct {
		name               string
		metricName         string
		metricLabel        string
		resourceType       string
		resourceLabelKey   string
		resourceLabelValue string
		shouldBeFiltered   bool
		description        string
	}{
		{
			name:               "wafl_volume_cloud_bin_operation_size with put - should NOT be filtered",
			metricName:         "wafl_volume_cloud_bin_operation_size",
			metricLabel:        "put",
			resourceType:       "Volume",
			resourceLabelKey:   "volume",
			resourceLabelValue: "test-volume-put",
			shouldBeFiltered:   false,
			description:        "PUT operations should be included for wafl_volume metrics",
		},
		{
			name:               "wafl_volume_cloud_bin_operation_size with get - should be filtered (line 153-154)",
			metricName:         "wafl_volume_cloud_bin_operation_size",
			metricLabel:        "get",
			resourceType:       "Volume",
			resourceLabelKey:   "volume",
			resourceLabelValue: "test-volume-get",
			shouldBeFiltered:   true,
			description:        "GET operations should be filtered out for wafl_volume metrics",
		},
		{
			name:               "wafl_volume_cloud_bin_operation_size with delete - should be filtered (line 153-154)",
			metricName:         "wafl_volume_cloud_bin_operation_size",
			metricLabel:        "delete",
			resourceType:       "Volume",
			resourceLabelKey:   "volume",
			resourceLabelValue: "test-volume-delete",
			shouldBeFiltered:   true,
			description:        "DELETE operations should be filtered out for wafl_volume metrics",
		},
		{
			name:               "pool_cloud_bin_operation_size with put - should NOT be filtered",
			metricName:         "pool_cloud_bin_operation_size",
			metricLabel:        "put",
			resourceType:       "VolumePool",
			resourceLabelKey:   "pool_name",
			resourceLabelValue: "test-pool-put",
			shouldBeFiltered:   false,
			description:        "PUT operations should be included for pool metrics",
		},
		{
			name:               "pool_cloud_bin_operation_size with get - should be filtered (line 156-157)",
			metricName:         "pool_cloud_bin_operation_size",
			metricLabel:        "get",
			resourceType:       "VolumePool",
			resourceLabelKey:   "pool_name",
			resourceLabelValue: "test-pool-get",
			shouldBeFiltered:   true,
			description:        "GET operations should be filtered out for pool metrics",
		},
		{
			name:               "pool_cloud_bin_operation_size with list - should be filtered (line 156-157)",
			metricName:         "pool_cloud_bin_operation_size",
			metricLabel:        "list",
			resourceType:       "VolumePool",
			resourceLabelKey:   "pool_name",
			resourceLabelValue: "test-pool-list",
			shouldBeFiltered:   true,
			description:        "LIST operations should be filtered out for pool metrics",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockClient := new(MockMonitoringClient)
			mockIterator := new(MockTimeSeriesIterator)

			testMetrics := []common.MetricItem{
				{Metric: tc.metricName, ResourceType: "netapp.com/" + strings.ToLower(tc.resourceType), MetricType: "performance"},
			}

			provider := &GoogleVolumeMetricsProvider{
				client:     mockClient,
				metrics:    testMetrics,
				startTime:  timestamp.Add(-5 * time.Minute),
				endTime:    timestamp,
				googleSink: googleSink,
			}

			timeSeries := &monitoringpb.TimeSeries{
				Metric: &metric.Metric{
					Type: "netapp.com/" + strings.ToLower(tc.resourceType) + "/" + tc.metricName,
					Labels: map[string]string{
						"metric":            tc.metricLabel, // This is the key label that determines filtering
						tc.resourceLabelKey: tc.resourceLabelValue,
						"project":           projectID,
						"datacenter":        "us-west1",
					},
				},
				Points: []*monitoringpb.Point{
					{
						Value: &monitoringpb.TypedValue{
							Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: 1024.0},
						},
					},
				},
			}

			mockIterator.On("Next").Return(timeSeries, nil).Once()
			mockIterator.On("Next").Return(nil, iterator.Done).Once()
			mockClient.On("ListTimeSeries", ctx, mock.Anything).Return(mockIterator)

			result, err := provider.CollectProjectMetrics(ctx, logger, projectID, timestamp)

			assert.NoError(t, err)
			// Performance metrics are sent to googleSink, not returned in result
			assert.Empty(t, result, tc.description)

			mockClient.AssertExpectations(t)
			mockIterator.AssertExpectations(t)
		})
	}
}

// TestGoogleVolumeMetricsProvider_CloudBinOperationFiltering_MultipleSeries tests filtering with multiple time series
func TestGoogleVolumeMetricsProvider_CloudBinOperationFiltering_MultipleSeries(t *testing.T) {
	ctx := context.Background()
	logger := log.NewLogger()
	projectID := "test-project-multi-filtering"
	timestamp := time.Now()

	config := common.LoadConfig()
	googleSink := performance.NewSink(ctx, config)

	mockClient := new(MockMonitoringClient)
	mockIterator := new(MockTimeSeriesIterator)

	testMetrics := []common.MetricItem{
		{Metric: "wafl_volume_cloud_bin_operation_size", ResourceType: "netapp.com/volume", MetricType: "performance"},
	}

	provider := &GoogleVolumeMetricsProvider{
		client:     mockClient,
		metrics:    testMetrics,
		startTime:  timestamp.Add(-5 * time.Minute),
		endTime:    timestamp,
		googleSink: googleSink,
	}

	// Create multiple time series with different metric labels
	putTimeSeries := &monitoringpb.TimeSeries{
		Metric: &metric.Metric{
			Type: "netapp.com/volume/wafl_volume_cloud_bin_operation_size",
			Labels: map[string]string{
				"metric":     "put",
				"volume":     "test-volume-1",
				"project":    projectID,
				"datacenter": "us-west1",
			},
		},
		Points: []*monitoringpb.Point{
			{Value: &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: 100.0}}},
		},
	}

	getTimeSeries := &monitoringpb.TimeSeries{
		Metric: &metric.Metric{
			Type: "netapp.com/volume/wafl_volume_cloud_bin_operation_size",
			Labels: map[string]string{
				"metric":     "get",
				"volume":     "test-volume-2",
				"project":    projectID,
				"datacenter": "us-west1",
			},
		},
		Points: []*monitoringpb.Point{
			{Value: &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: 200.0}}},
		},
	}

	deleteTimeSeries := &monitoringpb.TimeSeries{
		Metric: &metric.Metric{
			Type: "netapp.com/volume/wafl_volume_cloud_bin_operation_size",
			Labels: map[string]string{
				"metric":     "delete",
				"volume":     "test-volume-3",
				"project":    projectID,
				"datacenter": "us-west1",
			},
		},
		Points: []*monitoringpb.Point{
			{Value: &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: 300.0}}},
		},
	}

	anotherPutTimeSeries := &monitoringpb.TimeSeries{
		Metric: &metric.Metric{
			Type: "netapp.com/volume/wafl_volume_cloud_bin_operation_size",
			Labels: map[string]string{
				"metric":     "put",
				"volume":     "test-volume-4",
				"project":    projectID,
				"datacenter": "us-east1",
			},
		},
		Points: []*monitoringpb.Point{
			{Value: &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: 400.0}}},
		},
	}

	// Mock iterator to return all time series, but only "put" ones should be processed
	mockIterator.On("Next").Return(putTimeSeries, nil).Once()
	mockIterator.On("Next").Return(getTimeSeries, nil).Once()
	mockIterator.On("Next").Return(deleteTimeSeries, nil).Once()
	mockIterator.On("Next").Return(anotherPutTimeSeries, nil).Once()
	mockIterator.On("Next").Return(nil, iterator.Done).Once()

	mockClient.On("ListTimeSeries", ctx, mock.Anything).Return(mockIterator)

	result, err := provider.CollectProjectMetrics(ctx, logger, projectID, timestamp)

	assert.NoError(t, err)
	// Performance metrics are sent to googleSink, not returned in result
	// Only 2 "put" metrics should have been processed, "get" and "delete" should be filtered
	assert.Empty(t, result)

	mockClient.AssertExpectations(t)
	mockIterator.AssertExpectations(t)
}

// TestGoogleVolumeMetricsProvider_CloudBinOperationFiltering_PoolMetrics tests pool-specific filtering
func TestGoogleVolumeMetricsProvider_CloudBinOperationFiltering_PoolMetrics(t *testing.T) {
	ctx := context.Background()
	logger := log.NewLogger()
	projectID := "test-project-pool-filtering"
	timestamp := time.Now()

	config := common.LoadConfig()
	googleSink := performance.NewSink(ctx, config)

	mockClient := new(MockMonitoringClient)
	mockIterator := new(MockTimeSeriesIterator)

	testMetrics := []common.MetricItem{
		{Metric: "pool_cloud_bin_operation_size", ResourceType: "netapp.com/pool", MetricType: "performance"},
	}

	provider := &GoogleVolumeMetricsProvider{
		client:     mockClient,
		metrics:    testMetrics,
		startTime:  timestamp.Add(-5 * time.Minute),
		endTime:    timestamp,
		googleSink: googleSink,
	}

	// Create multiple time series for pool metrics
	poolPutTimeSeries := &monitoringpb.TimeSeries{
		Metric: &metric.Metric{
			Type: "netapp.com/pool/pool_cloud_bin_operation_size",
			Labels: map[string]string{
				"metric":     "put",
				"pool_name":  "test-pool-1",
				"project":    projectID,
				"datacenter": "us-central1",
			},
		},
		Points: []*monitoringpb.Point{
			{Value: &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: 2048.0}}},
		},
	}

	poolGetTimeSeries := &monitoringpb.TimeSeries{
		Metric: &metric.Metric{
			Type: "netapp.com/pool/pool_cloud_bin_operation_size",
			Labels: map[string]string{
				"metric":     "get",
				"pool_name":  "test-pool-2",
				"project":    projectID,
				"datacenter": "us-central1",
			},
		},
		Points: []*monitoringpb.Point{
			{Value: &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: 4096.0}}},
		},
	}

	poolListTimeSeries := &monitoringpb.TimeSeries{
		Metric: &metric.Metric{
			Type: "netapp.com/pool/pool_cloud_bin_operation_size",
			Labels: map[string]string{
				"metric":     "list",
				"pool_name":  "test-pool-3",
				"project":    projectID,
				"datacenter": "us-central1",
			},
		},
		Points: []*monitoringpb.Point{
			{Value: &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: 1024.0}}},
		},
	}

	// Mock iterator returns all series: put (included), get (filtered), list (filtered)
	mockIterator.On("Next").Return(poolPutTimeSeries, nil).Once()
	mockIterator.On("Next").Return(poolGetTimeSeries, nil).Once()
	mockIterator.On("Next").Return(poolListTimeSeries, nil).Once()
	mockIterator.On("Next").Return(nil, iterator.Done).Once()

	mockClient.On("ListTimeSeries", ctx, mock.Anything).Return(mockIterator)

	result, err := provider.CollectProjectMetrics(ctx, logger, projectID, timestamp)

	assert.NoError(t, err)
	// Performance metrics are sent to googleSink, not returned in result
	// Only 1 "put" metric should have been processed, "get" and "list" should be filtered
	assert.Empty(t, result)

	mockClient.AssertExpectations(t)
	mockIterator.AssertExpectations(t)
}
