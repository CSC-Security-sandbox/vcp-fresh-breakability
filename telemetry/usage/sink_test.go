package usage

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	database2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/metrics"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

// createMockDB creates a mock database for testing
func createMockDB() database2.Storage {
	mockDB := &database2.MockStorage{}
	// Set up default behavior for GetAggregatedUsage to return empty slice
	mockDB.On("GetAggregatedUsage", mock.Anything, mock.Anything).Return([]datamodel.AggregatedUsage{}, nil)
	// Set up default behavior for UpdateAggregatedUsage to return no error
	mockDB.On("UpdateAggregatedUsage", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	return mockDB
}

func TestNewSink(t *testing.T) {
	ctx := context.Background()
	config := common.LoadConfig()
	mockDB := createMockDB()

	sink := NewSink(ctx, config, mockDB)

	assert.NotNil(t, sink)
	assert.NotNil(t, sink.metricClient)
	assert.NotNil(t, sink.logger)
}

func TestGoogleUsageSink_filterValidUsage(t *testing.T) {
	ctx := context.Background()
	config := common.LoadConfig()
	mockDB := createMockDB()
	sink := NewSink(ctx, config, mockDB)

	customerID := "test-customer-123"

	t.Run("Valid usage records", func(t *testing.T) {
		aggregatedRecords := []datamodel.AggregatedUsage{
			{
				ID:               1,
				VendorCustomerID: &customerID,
				MeasuredType:     metadata.LogicalSize,
				Quantity:         100.0,
			},
			{
				ID:               2,
				VendorCustomerID: &customerID,
				MeasuredType:     metadata.AllocatedSize,
				Quantity:         200.0,
			},
		}

		validUsage, err := sink.filterValidUsage(aggregatedRecords)

		assert.NoError(t, err)
		assert.Len(t, validUsage, 2)
		assert.Equal(t, int64(1), validUsage[0].ID)
		assert.Equal(t, int64(2), validUsage[1].ID)
	})

	t.Run("Usage record with missing ID", func(t *testing.T) {
		aggregatedRecords := []datamodel.AggregatedUsage{
			{
				ID:               0, // Missing ID
				VendorCustomerID: &customerID,
				MeasuredType:     metadata.LogicalSize,
				Quantity:         100.0,
			},
		}

		validUsage, err := sink.filterValidUsage(aggregatedRecords)

		assert.Error(t, err)
		assert.Nil(t, validUsage)
		assert.Contains(t, err.Error(), "attempted mapping aggregated usage with unset id")
	})

	t.Run("Usage record with missing customer ID", func(t *testing.T) {
		ml := &log.MockLogger{}
		sink.logger = ml

		// Set up mock expectations
		ml.On("Errorf", "Skipping usage: Not mapping usage record due to missing project ID/number.",
			"Record ID", int64(1)).Once()
		ml.On("Errorf", "Found records that are not appropriate for billing. Not mapping them. Number of records: %d", 1).Once()

		aggregatedRecords := []datamodel.AggregatedUsage{
			{
				ID:               1,
				VendorCustomerID: nil, // Missing customer ID
				MeasuredType:     metadata.LogicalSize,
				Quantity:         100.0,
			},
		}

		validUsage, err := sink.filterValidUsage(aggregatedRecords)

		assert.NoError(t, err)
		assert.Len(t, validUsage, 0) // No valid records
		ml.AssertExpectations(t)
	})

	t.Run("Usage record with empty customer ID", func(t *testing.T) {
		ml := &log.MockLogger{}
		sink.logger = ml

		emptyCustomerID := ""
		// Set up mock expectations
		ml.On("Errorf", "Skipping usage: Not mapping usage record due to missing project ID/number.",
			"Record ID", int64(1)).Once()
		ml.On("Errorf", "Found records that are not appropriate for billing. Not mapping them. Number of records: %d", 1).Once()

		aggregatedRecords := []datamodel.AggregatedUsage{
			{
				ID:               1,
				VendorCustomerID: &emptyCustomerID, // Empty customer ID
				MeasuredType:     metadata.LogicalSize,
				Quantity:         100.0,
			},
		}

		validUsage, err := sink.filterValidUsage(aggregatedRecords)

		assert.NoError(t, err)
		assert.Len(t, validUsage, 0) // No valid records
		ml.AssertExpectations(t)
	})
}

func TestGoogleUsageSink_isValid(t *testing.T) {
	ctx := context.Background()
	config := common.LoadConfig()
	mockDB := createMockDB()
	sink := NewSink(ctx, config, mockDB)

	customerID := "test-customer-123"

	t.Run("Valid usage record", func(t *testing.T) {
		usage := datamodel.AggregatedUsage{
			ID:               1,
			VendorCustomerID: &customerID,
			MeasuredType:     metadata.LogicalSize,
			Quantity:         100.0,
		}

		isValid := sink.isValid(usage)
		assert.True(t, isValid)
	})

	t.Run("Invalid usage record - nil customer ID", func(t *testing.T) {
		ml := &log.MockLogger{}
		sink.logger = ml

		ml.On("Errorf", "Skipping usage: Not mapping usage record due to missing project ID/number.",
			"Record ID", int64(1)).Once()

		usage := datamodel.AggregatedUsage{
			ID:               1,
			VendorCustomerID: nil,
			MeasuredType:     metadata.LogicalSize,
			Quantity:         100.0,
		}

		isValid := sink.isValid(usage)
		assert.False(t, isValid)
		ml.AssertExpectations(t)
	})

	t.Run("Invalid usage record - empty customer ID", func(t *testing.T) {
		ml := &log.MockLogger{}
		sink.logger = ml

		emptyCustomerID := ""
		ml.On("Errorf", "Skipping usage: Not mapping usage record due to missing project ID/number.",
			"Record ID", int64(1)).Once()

		usage := datamodel.AggregatedUsage{
			ID:               1,
			VendorCustomerID: &emptyCustomerID,
			MeasuredType:     metadata.LogicalSize,
			Quantity:         100.0,
		}

		isValid := sink.isValid(usage)
		assert.False(t, isValid)
		ml.AssertExpectations(t)
	})
}

func TestGoogleUsageSink_completeRecords(t *testing.T) {
	ctx := context.Background()
	config := common.LoadConfig()
	mockDB := createMockDB()
	sink := NewSink(ctx, config, mockDB)

	customerID := "test-customer-123"

	t.Run("Complete valid records", func(t *testing.T) {
		ml := &log.MockLogger{}
		sink.logger = ml

		ml.On("Debugf", "Google Usage Mapping with ID is ready for billing", "Record ID: ", int64(1)).Once()
		ml.On("Debugf", "Google Usage Mapping with ID is ready for billing", "Record ID: ", int64(2)).Once()

		records := []datamodel.AggregatedUsage{
			{
				ID:               1,
				VendorCustomerID: &customerID,
				MeasuredType:     metadata.LogicalSize,
				Quantity:         100.0,
				ResourceType:     metadata.Volume,
			},
			{
				ID:               2,
				VendorCustomerID: &customerID,
				MeasuredType:     "CBS_VOLUME_BACKUP_SIZE",
				Quantity:         200.0,
				ResourceType:     metadata.Volume,
			},
		}

		googleMetrics := sink.completeRecords(records)

		assert.Len(t, googleMetrics, 2)
		ml.AssertExpectations(t)
	})

	t.Run("Complete records with validation failures", func(t *testing.T) {
		ml := &log.MockLogger{}
		sink.logger = ml

		// This record will fail validation because it has an empty customer ID (not nil to avoid panic)
		emptyCustomerID := ""
		ml.On("Errorf", "Google Usage Mapping with ID %d failed GoogleMetric validation: missing fields %s", int64(1), "customerId").Once()

		records := []datamodel.AggregatedUsage{
			{
				ID:               1,
				VendorCustomerID: &emptyCustomerID, // Empty customer ID will cause validation failure without panic
				MeasuredType:     metadata.LogicalSize,
				Quantity:         100.0,
				ResourceType:     metadata.Volume,
			},
		}

		googleMetrics := sink.completeRecords(records)

		assert.Len(t, googleMetrics, 0) // No valid metrics
		ml.AssertExpectations(t)
	})
}

func TestGoogleUsageSink_processGcpUnifiedMetrics(t *testing.T) {
	ctx := context.Background()
	config := common.LoadConfig()
	mockDB := createMockDB()
	sink := NewSink(ctx, config, mockDB)

	t.Run("Process empty metrics", func(t *testing.T) {
		ml := &log.MockLogger{}
		sink.logger = ml

		ml.On("Info", "No Google usage metrics processed in this run.").Once()

		var googleMetrics []common.GoogleMetric // Empty slice

		sink.processGcpUnifiedMetrics(ctx, googleMetrics)
		ml.AssertExpectations(t)
	})

	// Skip the test that would make real HTTP calls
	t.Run("Process metrics with data - skipped due to real HTTP calls", func(t *testing.T) {
		t.Skip("Skipping test that makes real HTTP calls to Google API")
	})
}

func TestGoogleUsageSink_push(t *testing.T) {
	ctx := context.Background()
	config := common.LoadConfig()
	mockDB := createMockDB()
	sink := NewSink(ctx, config, mockDB)

	t.Run("Push empty metrics", func(t *testing.T) {
		ml := &log.MockLogger{}
		sink.logger = ml

		ml.On("Warn", "Google first party billing metrics not found, hence not reporting anything.").Once()

		var googleMetrics []common.GoogleMetric // Empty slice

		sink.push(ctx, googleMetrics)
		ml.AssertExpectations(t)
	})

	t.Run("Push nil metrics", func(t *testing.T) {
		ml := &log.MockLogger{}
		sink.logger = ml

		ml.On("Warn", "Google first party billing metrics not found, hence not reporting anything.").Once()

		sink.push(ctx, nil)
		ml.AssertExpectations(t)
	})

	// Skip the test that would make real HTTP calls
	t.Run("Push metrics with data - skipped due to real HTTP calls", func(t *testing.T) {
		t.Skip("Skipping test that makes real HTTP calls to Google API")
	})
}

func TestGoogleUsageSink_DeliverMetrics(t *testing.T) {
	ctx := context.Background()
	config := common.LoadConfig()
	mockDB := createMockDB()
	sink := NewSink(ctx, config, mockDB)

	t.Run("Deliver valid metrics", func(t *testing.T) {
		customerID := "test-customer-123"
		aggregatedRecords := []datamodel.AggregatedUsage{
			{
				ID:               1,
				VendorCustomerID: &customerID,
				MeasuredType:     metadata.LogicalSize,
				Quantity:         100.0,
				ResourceType:     metadata.Volume,
				AggregationStart: time.Now(),
				AggregationEnd:   time.Now().Add(time.Hour),
			},
		}

		mappedUsage, err := sink.DeliverMetrics(ctx, aggregatedRecords)

		assert.NoError(t, err)
		assert.Equal(t, 1, mappedUsage)
	})

	t.Run("Deliver metrics with validation errors", func(t *testing.T) {
		aggregatedRecords := []datamodel.AggregatedUsage{
			{
				ID:               0, // Invalid ID
				VendorCustomerID: nillable.ToPointer("test-customer"),
				MeasuredType:     metadata.LogicalSize,
				Quantity:         100.0,
			},
		}

		mappedUsage, err := sink.DeliverMetrics(ctx, aggregatedRecords)

		assert.Error(t, err)
		assert.Equal(t, 0, mappedUsage)
		assert.Contains(t, err.Error(), "attempted mapping aggregated usage with unset id")
	})
}

// TestGoogleUsageSink_DeliverMetrics_WithDroppedRecords tests the dropped records logging
func TestGoogleUsageSink_DeliverMetrics_WithDroppedRecords(t *testing.T) {
	ctx := context.Background()
	config := common.LoadConfig()
	mockDB := createMockDB()
	sink := NewSink(ctx, config, mockDB)

	customerID := "test-customer-123"

	aggregatedRecords := []datamodel.AggregatedUsage{
		{
			ID:               1,
			VendorCustomerID: &customerID,
			Quantity:         100.0,
		},
		{
			ID:               2,
			VendorCustomerID: nil, // This will be dropped as invalid
			Quantity:         200.0,
		},
		{
			ID:               3,
			VendorCustomerID: &customerID,
			Quantity:         300.0,
		},
	}

	// This should trigger the dropped records logging (line 45)
	delivered, err := sink.DeliverMetrics(ctx, aggregatedRecords)

	assert.NoError(t, err)
	assert.Equal(t, 2, delivered, "Should deliver 2 valid records")
}

// TestGoogleUsageSink_processResponse_WithResults tests processResponse with actual results
func TestGoogleUsageSink_processResponse_WithResults(t *testing.T) {
	ctx := context.Background()
	config := common.LoadConfig()
	mockDB := createMockDB()
	sink := NewSink(ctx, config, mockDB)

	customerID := "test-customer-123"
	resourceName := "test-resource"

	// Create test metric
	googleMetric := *common.NewGoogleMetric(&datamodel.AggregatedUsage{
		ID:               1,
		VendorCustomerID: &customerID,
		ResourceName:     &resourceName,
		Quantity:         100.0,
	})

	// Create a channel and send results
	resultChan := make(chan []common.MetricsResult, 1)

	// Create mock results
	results := []common.MetricsResult{
		{
			GoogleMetric:   googleMetric,
			Exception:      nil,
			ReportResponse: nil,
		},
	}

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		resultChan <- results
		close(resultChan)
	}()

	// This should test the processResponse method (lines 132, 139)
	sink.processResponse(ctx, &wg, resultChan)
	wg.Wait()
}

// TestGoogleUsageSink_processMetricsResults_WithGoodResults tests processMetricsResults with good results
func TestGoogleUsageSink_processMetricsResults_WithGoodResults(t *testing.T) {
	ctx := context.Background()
	config := common.LoadConfig()
	mockDB := createMockDB()
	sink := NewSink(ctx, config, mockDB)

	customerID := "test-customer-123"
	resourceName := "test-resource"

	// Create test metric
	googleMetric := *common.NewGoogleMetric(&datamodel.AggregatedUsage{
		ID:               1,
		VendorCustomerID: &customerID,
		ResourceName:     &resourceName,
		Quantity:         100.0,
		State:            datamodel.Unsubmitted,
	})

	// Create good results
	gcpResults := []common.MetricsResult{
		{
			GoogleMetric:   googleMetric,
			Exception:      nil,
			ReportResponse: &common.ReportResponse{}, // Good response
		},
	}

	// This should test the processMetricsResults method (lines 141-187)
	sink.processMetricsResults(ctx, gcpResults)
}

// TestGoogleUsageSink_processMetricsResults_WithErrorResults tests processMetricsResults with error results
func TestGoogleUsageSink_processMetricsResults_WithErrorResults(t *testing.T) {
	ctx := context.Background()
	config := common.LoadConfig()
	mockDB := createMockDB()
	sink := NewSink(ctx, config, mockDB)

	customerID := "test-customer-123"
	resourceName := "test-resource"

	// Create test metric
	googleMetric := *common.NewGoogleMetric(&datamodel.AggregatedUsage{
		ID:               1,
		VendorCustomerID: &customerID,
		ResourceName:     &resourceName,
		Quantity:         100.0,
		State:            datamodel.Error,
	})

	// Create error results
	gcpResults := []common.MetricsResult{
		{
			GoogleMetric:   googleMetric,
			Exception:      errors.New("test error"),
			ReportResponse: nil,
		},
	}

	// This should test the processMetricsResults method with error paths
	sink.processMetricsResults(ctx, gcpResults)
}

// TestGoogleUsageSink_processMetricsResults_WithExceptionResults tests processMetricsResults with exception results
func TestGoogleUsageSink_processMetricsResults_WithExceptionResults(t *testing.T) {
	ctx := context.Background()
	config := common.LoadConfig()
	mockDB := createMockDB()
	sink := NewSink(ctx, config, mockDB)

	customerID := "test-customer-123"
	resourceName := "test-resource"

	// Create test metric
	googleMetric := *common.NewGoogleMetric(&datamodel.AggregatedUsage{
		ID:               1,
		VendorCustomerID: &customerID,
		ResourceName:     &resourceName,
		Quantity:         100.0,
		State:            datamodel.Error,
	})

	// Create exception results with a different type of error
	gcpResults := []common.MetricsResult{
		{
			GoogleMetric:   googleMetric,
			Exception:      errors.New("exception error"),
			ReportResponse: nil,
		},
	}

	// This should test the processMetricsResults method with exception paths
	sink.processMetricsResults(ctx, gcpResults)
}

// TestGoogleUsageSink_processMetricsResults_GetAsUsageBillingMetricError tests error in GetAsUsageBillingMetric
func TestGoogleUsageSink_processMetricsResults_GetAsUsageBillingMetricError(t *testing.T) {
	ctx := context.Background()
	config := common.LoadConfig()
	mockDB := createMockDB()
	sink := NewSink(ctx, config, mockDB)

	// Create test metric with invalid type that will cause GetAsUsageBillingMetric to fail
	googleMetric := *common.NewGoogleMetric("invalid-type")

	// Create results
	gcpResults := []common.MetricsResult{
		{
			GoogleMetric:   googleMetric,
			Exception:      nil,
			ReportResponse: &common.ReportResponse{},
		},
	}

	// This should test the error path in processMetricsResults (line 144)
	sink.processMetricsResults(ctx, gcpResults)
}

// TestGoogleUsageSink_isSuccessful tests the isSuccessful function
func TestGoogleUsageSink_isSuccessful(t *testing.T) {
	tests := []struct {
		name     string
		response *common.ReportResponse
		expected bool
	}{
		{
			name:     "successful response",
			response: &common.ReportResponse{},
			expected: true,
		},
		{
			name:     "nil response",
			response: nil,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// This tests the isSuccessful function (line 193)
			result := isSuccessful(tt.response)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestGoogleUsageSink_processMetricsResults_WithErrorLogging tests error logging in processMetricsResults
func TestGoogleUsageSink_processMetricsResults_WithErrorLogging(t *testing.T) {
	ctx := context.Background()
	config := common.LoadConfig()
	mockDB := createMockDB()
	sink := NewSink(ctx, config, mockDB)

	// Set up mock logger to capture error logging
	ml := &log.MockLogger{}
	sink.logger = ml

	customerID := "test-customer-123"
	resourceName := "test-resource"

	// Create test metric
	googleMetric := *common.NewGoogleMetric(&datamodel.AggregatedUsage{
		ID:               1,
		VendorCustomerID: &customerID,
		ResourceName:     &resourceName,
		Quantity:         100.0,
		State:            datamodel.Error,
	})

	// Create error results to trigger error logging (lines 137-138)
	gcpResults := []common.MetricsResult{
		{
			GoogleMetric:   googleMetric,
			Exception:      errors.New("test error"),
			ReportResponse: nil,
		},
	}

	// Set up mock expectations for error logging
	ml.On("Errorf", "Google Usage Mapping with ID %d failed GoogleMetric validation: missing fields %s", int64(1), mock.AnythingOfType("string")).Maybe()
	ml.On("Errorf", "Google Usage Mapping with ID %d failed GoogleMetric validation: missing fields %s", int64(1), "customerId").Maybe()
	ml.On("Debugf", "Processing the Google Metric Result", "result:", mock.AnythingOfType("common.MetricsResult"), "resultCode: ", mock.AnythingOfType("string")).Once()
	ml.On("Infof", "Updating usage information for billingRecord ID: %d, state: %s", int64(1), mock.AnythingOfType("datamodel.TrackingState")).Once()
	ml.On("Infof", "%d metrics were successfully reported.", 0).Once()
	ml.On("Infof", "%d metrics were not reported.", 1).Once()

	// This should test the error logging paths (lines 137-138)
	sink.processMetricsResults(ctx, gcpResults)

	ml.AssertExpectations(t)
}

// TestGoogleUsageSink_processMetricsResults_WithSuccessfulLogging tests successful logging in processMetricsResults
func TestGoogleUsageSink_processMetricsResults_WithSuccessfulLogging(t *testing.T) {
	ctx := context.Background()
	config := common.LoadConfig()
	mockDB := createMockDB()
	sink := NewSink(ctx, config, mockDB)

	// Set up mock logger to capture success logging
	ml := &log.MockLogger{}
	sink.logger = ml

	customerID := "test-customer-123"
	resourceName := "test-resource"

	// Create test metric
	googleMetric := *common.NewGoogleMetric(&datamodel.AggregatedUsage{
		ID:               1,
		VendorCustomerID: &customerID,
		ResourceName:     &resourceName,
		Quantity:         100.0,
		State:            datamodel.Unsubmitted,
	})

	// Create successful results to trigger success logging (line 200)
	gcpResults := []common.MetricsResult{
		{
			GoogleMetric:   googleMetric,
			Exception:      nil,
			ReportResponse: &common.ReportResponse{}, // Good response
		},
	}

	// Set up mock expectations for success logging
	ml.On("Debugf", "Processing the Google Metric Result", "result:", mock.AnythingOfType("common.MetricsResult"), "resultCode: ", mock.AnythingOfType("string")).Once()
	ml.On("Infof", "Updating usage information for billingRecord ID: %d, state: %s", int64(1), mock.AnythingOfType("datamodel.TrackingState")).Once()
	ml.On("Infof", "%d metrics were successfully reported.", 1).Once()
	ml.On("Infof", "%d metrics were not reported.", 0).Once()

	// This should test the success logging path (line 200)
	sink.processMetricsResults(ctx, gcpResults)

	ml.AssertExpectations(t)
}
