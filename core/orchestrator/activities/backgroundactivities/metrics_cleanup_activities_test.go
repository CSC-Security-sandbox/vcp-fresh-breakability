package backgroundactivities

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	metricsdb "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/metrics"
)

func TestMetricsCleanupActivity_CleanupHydratedMetricsTableActivity_Success(t *testing.T) {
	// Setup
	mockDB := metricsdb.NewMockStorage(t)
	activity := &MetricsCleanupActivity{MetricsDB: mockDB}
	ctx := context.Background()

	// Expected cutoff time (approximately 1 day ago)
	expectedRowsDeleted := int64(100)

	// Mock the delete operation
	mockDB.On("DeleteHydratedMetricsOlderThan", mock.Anything, mock.MatchedBy(func(cutoff time.Time) bool {
		// Verify cutoff is approximately 1 day ago (within 1 minute tolerance)
		dayAgo := time.Now().AddDate(0, 0, -1)
		diff := cutoff.Sub(dayAgo).Abs()
		return diff < time.Minute
	})).Return(expectedRowsDeleted, nil)

	// Execute
	err := activity.CleanupHydratedMetricsTableActivity(ctx)

	// Assert
	assert.NoError(t, err)
	mockDB.AssertExpectations(t)
}

func TestMetricsCleanupActivity_CleanupHydratedMetricsTableActivity_Error(t *testing.T) {
	// Setup
	mockDB := metricsdb.NewMockStorage(t)
	activity := &MetricsCleanupActivity{MetricsDB: mockDB}
	ctx := context.Background()

	expectedError := errors.New("database error")

	// Mock the delete operation to return error
	mockDB.On("DeleteHydratedMetricsOlderThan", mock.Anything, mock.AnythingOfType("time.Time")).Return(int64(0), expectedError)

	// Execute
	err := activity.CleanupHydratedMetricsTableActivity(ctx)

	// Assert
	assert.Error(t, err)
	assert.Equal(t, expectedError, err)
	mockDB.AssertExpectations(t)
}

func TestMetricsCleanupActivity_CleanupAggregatedUsageTableActivity_Success(t *testing.T) {
	// Setup
	mockDB := metricsdb.NewMockStorage(t)
	activity := &MetricsCleanupActivity{MetricsDB: mockDB}
	ctx := context.Background()

	// Expected cutoff time (approximately 1 week ago)
	expectedRowsDeleted := int64(500)

	// Mock the delete operation
	mockDB.On("DeleteAggregatedUsageOlderThan", mock.Anything, mock.MatchedBy(func(cutoff time.Time) bool {
		// Verify cutoff is approximately 1 week ago (within 1 minute tolerance)
		weekAgo := time.Now().AddDate(0, 0, -7)
		diff := cutoff.Sub(weekAgo).Abs()
		return diff < time.Minute
	})).Return(expectedRowsDeleted, nil)

	// Execute
	err := activity.CleanupAggregatedUsageTableActivity(ctx)

	// Assert
	assert.NoError(t, err)
	mockDB.AssertExpectations(t)
}

func TestMetricsCleanupActivity_CleanupAggregatedUsageTableActivity_Error(t *testing.T) {
	// Setup
	mockDB := metricsdb.NewMockStorage(t)
	activity := &MetricsCleanupActivity{MetricsDB: mockDB}
	ctx := context.Background()

	expectedError := errors.New("database error")

	// Mock the delete operation to return error
	mockDB.On("DeleteAggregatedUsageOlderThan", mock.Anything, mock.AnythingOfType("time.Time")).Return(int64(0), expectedError)

	// Execute
	err := activity.CleanupAggregatedUsageTableActivity(ctx)

	// Assert
	assert.Error(t, err)
	assert.Equal(t, expectedError, err)
	mockDB.AssertExpectations(t)
}
