package performance

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/entity"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/servicecontrol/v1"
)

// Helper function to simulate FilterAcceptedMetrics functionality for testing
func filterAcceptedMetricsHelper(sink *GoogleSink, metrics []entity.HydratedMetric) []entity.HydratedMetric {
	var warnings []string
	var validMetrics []entity.HydratedMetric

	for _, m := range metrics {
		if sink.isValidHydratedMetric(m, &warnings) {
			validMetrics = append(validMetrics, m)
		}
	}
	return validMetrics
}

// TestDeliverMetrics_ValidMetrics tests the DeliverMetrics function with valid metrics.
func TestDeliverMetrics_ValidMetrics(t *testing.T) {
	ctx := context.Background()
	config := common.LoadConfig()

	sink := NewSink(ctx, config)
	var hydratedM []entity.HydratedMetric
	rm := metadata.ResourceMetadata{
		ResourceUUID:        nillable.ToPointer(uuid.New().String()),
		ResourceName:        nillable.ToPointer("dummy-resource-" + strconv.Itoa(1)),
		ResourceDisplayName: nillable.ToPointer("Dummy Resource " + strconv.Itoa(1)),
		AccountName:         nillable.ToPointer("netapp-au-se1-autopush-sde-tst"),
		RegionName:          nillable.ToPointer("us-central1"),
		ResourceType:        metadata.VolumePool,
	}

	hydratedM = append(hydratedM, entity.HydratedMetric{
		Metadata:     rm,
		MeasuredType: metadata.PoolAllocatedSize,
		Quantity:     float64(1234),
		Timestamp:    entity.UnixNano(time.Now().UnixNano()),
	})

	count := sink.DeliverMetrics(ctx, hydratedM)
	assert.Equal(t, 1, count)
}

// TestDeliverMetrics_InvalidMetrics tests the DeliverMetrics function with valid metrics.
func TestDeliverMetrics_InvalidMetrics(t *testing.T) {
	ctx := context.Background()
	config := common.LoadConfig()

	sink := NewSink(ctx, config)
	var hydratedM []entity.HydratedMetric
	rm := metadata.ResourceMetadata{
		ResourceUUID:        nillable.ToPointer(uuid.New().String()),
		ResourceName:        nillable.ToPointer("dummy-resource-" + strconv.Itoa(1)),
		ResourceDisplayName: nillable.ToPointer("Dummy Resource " + strconv.Itoa(1)),
		AccountName:         nillable.ToPointer("netapp-au-se1-autopush-sde-tst"),
		RegionName:          nillable.ToPointer("us-central1"),
		ResourceType:        metadata.VolumePool,
	}

	hydratedM = append(hydratedM, entity.HydratedMetric{
		Metadata:     rm,
		MeasuredType: metadata.UnknownMeasuredType,
		Quantity:     float64(1234),
		Timestamp:    entity.UnixNano(time.Now().UnixNano()),
	})

	count := sink.DeliverMetrics(ctx, hydratedM)
	assert.Equal(t, 0, count)
}

// TestFilterAcceptedMetrics_ValidMetrics tests the FilterAcceptedMetrics function with valid metrics.
func TestFilterAcceptedMetrics_ValidMetrics(t *testing.T) {
	ctx := context.Background()
	config := common.LoadConfig()
	var hydratedM []entity.HydratedMetric
	sink := NewSink(ctx, config)

	rm := metadata.ResourceMetadata{
		ResourceUUID:        nillable.ToPointer(uuid.New().String()),
		ResourceName:        nillable.ToPointer("dummy-resource-" + strconv.Itoa(1)),
		ResourceDisplayName: nillable.ToPointer("Dummy Resource " + strconv.Itoa(1)),
		AccountName:         nillable.ToPointer("netapp-au-se1-autopush-sde-tst"),
		RegionName:          nillable.ToPointer("us-central1"),
		ResourceType:        metadata.VolumePool,
	}

	hydratedM = append(hydratedM, entity.HydratedMetric{
		Metadata:     rm,
		MeasuredType: metadata.PoolAllocatedSize,
		Quantity:     float64(1234),
		Timestamp:    entity.UnixNano(time.Now().UnixNano()),
	})

	validMetrics := filterAcceptedMetricsHelper(sink, hydratedM)

	assert.Len(t, validMetrics, 1)
}

// TestFilterAcceptedMetrics_InvalidMetrics tests the FilterAcceptedMetrics function with invalid metrics.
func TestFilterAcceptedMetrics_InvalidMetrics(t *testing.T) {
	ctx := context.Background()
	config := common.LoadConfig()
	var hydratedM []entity.HydratedMetric
	sink := NewSink(ctx, config)

	rm := metadata.ResourceMetadata{
		ResourceUUID:        nillable.ToPointer(uuid.New().String()),
		ResourceName:        nillable.ToPointer("dummy-resource-" + strconv.Itoa(1)),
		ResourceDisplayName: nillable.ToPointer("Dummy Resource " + strconv.Itoa(1)),
		AccountName:         nillable.ToPointer("netapp-au-se1-autopush-sde-tst"),
		RegionName:          nillable.ToPointer("us-central1"),
		ResourceType:        metadata.VolumePool,
	}

	hydratedM = append(hydratedM, entity.HydratedMetric{
		Metadata:     rm,
		MeasuredType: metadata.UnknownMeasuredType,
		Quantity:     float64(1234),
		Timestamp:    entity.UnixNano(time.Now().UnixNano()),
	})

	validMetrics := filterAcceptedMetricsHelper(sink, hydratedM)

	assert.Len(t, validMetrics, 0)
}

// TestFilterAcceptedMetrics_EmptyInput tests FilterAcceptedMetrics with an empty slice.
func TestFilterAcceptedMetrics_EmptyInput(t *testing.T) {
	ctx := context.Background()
	config := common.LoadConfig()
	sink := NewSink(ctx, config)
	var hydratedM []entity.HydratedMetric
	validMetrics := filterAcceptedMetricsHelper(sink, hydratedM)
	assert.Len(t, validMetrics, 0)
}

// TestDeliverMetrics_EmptyInput tests DeliverMetrics with an empty slice.
func TestDeliverMetrics_EmptyInput(t *testing.T) {
	ctx := context.Background()
	config := common.LoadConfig()
	sink := NewSink(ctx, config)
	var hydratedM []entity.HydratedMetric
	count := sink.DeliverMetrics(ctx, hydratedM)
	assert.Equal(t, 0, count)
}

// TestDeliverMetrics_AllInvalid tests DeliverMetrics with all invalid metrics.
func TestDeliverMetrics_AllInvalid(t *testing.T) {
	ctx := context.Background()
	config := common.LoadConfig()
	sink := NewSink(ctx, config)
	var hydratedM []entity.HydratedMetric
	for i := 0; i < 3; i++ {
		rm := metadata.ResourceMetadata{
			ResourceUUID:        nillable.ToPointer(uuid.New().String()),
			ResourceName:        nillable.ToPointer("dummy-resource-" + strconv.Itoa(i)),
			ResourceDisplayName: nillable.ToPointer("Dummy Resource " + strconv.Itoa(i)),
			AccountName:         nillable.ToPointer("netapp-au-se1-autopush-sde-tst"),
			RegionName:          nillable.ToPointer("us-central1"),
			ResourceType:        metadata.VolumePool,
		}
		hydratedM = append(hydratedM, entity.HydratedMetric{
			Metadata:     rm,
			MeasuredType: metadata.UnknownMeasuredType,
			Quantity:     float64(1234),
			Timestamp:    entity.UnixNano(time.Now().UnixNano()),
		})
	}
	count := sink.DeliverMetrics(ctx, hydratedM)
	assert.Equal(t, 0, count)
}

// TestFilterAcceptedMetrics_Mixed tests FilterAcceptedMetrics with a mix of valid and invalid metrics.
func TestFilterAcceptedMetrics_Mixed(t *testing.T) {
	ctx := context.Background()
	config := common.LoadConfig()
	sink := NewSink(ctx, config)
	var hydratedM []entity.HydratedMetric
	for i := 0; i < 2; i++ {
		rm := metadata.ResourceMetadata{
			ResourceUUID:        nillable.ToPointer(uuid.New().String()),
			ResourceName:        nillable.ToPointer("dummy-resource-" + strconv.Itoa(i)),
			ResourceDisplayName: nillable.ToPointer("Dummy Resource " + strconv.Itoa(i)),
			AccountName:         nillable.ToPointer("netapp-au-se1-autopush-sde-tst"),
			RegionName:          nillable.ToPointer("us-central1"),
			ResourceType:        metadata.VolumePool,
		}
		metricType := metadata.PoolAllocatedSize
		if i == 1 {
			metricType = metadata.UnknownMeasuredType
		}
		hydratedM = append(hydratedM, entity.HydratedMetric{
			Metadata:     rm,
			MeasuredType: metricType,
			Quantity:     float64(1234),
			Timestamp:    entity.UnixNano(time.Now().UnixNano()),
		})
	}
	validMetrics := filterAcceptedMetricsHelper(sink, hydratedM)
	assert.Len(t, validMetrics, 1)
}

// TestIsValidHydratedMetric_EmptyMeasuredType tests isValidHydratedMetric with empty MeasuredType.
func TestIsValidHydratedMetric_EmptyMeasuredType(t *testing.T) {
	ctx := context.Background()
	config := common.LoadConfig()
	sink := NewSink(ctx, config)
	var warnings []string
	rm := metadata.ResourceMetadata{
		ResourceUUID:        nillable.ToPointer(uuid.New().String()),
		ResourceName:        nillable.ToPointer("dummy-resource-0"),
		ResourceDisplayName: nillable.ToPointer("Dummy Resource 0"),
		AccountName:         nillable.ToPointer("netapp-au-se1-autopush-sde-tst"),
		RegionName:          nillable.ToPointer("us-central1"),
		ResourceType:        metadata.VolumePool,
	}
	metric := entity.HydratedMetric{
		Metadata:     rm,
		MeasuredType: "",
		Quantity:     float64(1234),
		Timestamp:    entity.UnixNano(time.Now().UnixNano()),
	}
	ok := sink.isValidHydratedMetric(metric, &warnings)
	assert.False(t, ok)
	assert.NotEmpty(t, warnings)
}

// TestIsValidHydratedMetric_ValidType tests isValidHydratedMetric with a valid MeasuredType.
func TestIsValidHydratedMetric_ValidType(t *testing.T) {
	ctx := context.Background()
	config := common.LoadConfig()
	sink := NewSink(ctx, config)
	var warnings []string
	rm := metadata.ResourceMetadata{
		ResourceUUID:        nillable.ToPointer(uuid.New().String()),
		ResourceName:        nillable.ToPointer("dummy-resource-0"),
		ResourceDisplayName: nillable.ToPointer("Dummy Resource 0"),
		AccountName:         nillable.ToPointer("netapp-au-se1-autopush-sde-tst"),
		RegionName:          nillable.ToPointer("us-central1"),
		ResourceType:        metadata.VolumePool,
	}
	metric := entity.HydratedMetric{
		Metadata:     rm,
		MeasuredType: metadata.PoolAllocatedSize,
		Quantity:     float64(1234),
		Timestamp:    entity.UnixNano(time.Now().UnixNano()),
	}
	ok := sink.isValidHydratedMetric(metric, &warnings)
	assert.True(t, ok)
	assert.Empty(t, warnings)
}

// TestFilterAcceptedMetrics_MultipleValid tests FilterAcceptedMetrics with multiple valid metrics.
func TestFilterAcceptedMetrics_MultipleValid(t *testing.T) {
	ctx := context.Background()
	config := common.LoadConfig()
	sink := NewSink(ctx, config)
	var hydratedM []entity.HydratedMetric
	for i := 0; i < 5; i++ {
		rm := metadata.ResourceMetadata{
			ResourceUUID:        nillable.ToPointer(uuid.New().String()),
			ResourceName:        nillable.ToPointer("dummy-resource-" + strconv.Itoa(i)),
			ResourceDisplayName: nillable.ToPointer("Dummy Resource " + strconv.Itoa(i)),
			AccountName:         nillable.ToPointer("netapp-au-se1-autopush-sde-tst"),
			RegionName:          nillable.ToPointer("us-central1"),
			ResourceType:        metadata.VolumePool,
		}
		hydratedM = append(hydratedM, entity.HydratedMetric{
			Metadata:     rm,
			MeasuredType: metadata.PoolAllocatedSize,
			Quantity:     float64(1234 + i),
			Timestamp:    entity.UnixNano(time.Now().UnixNano()),
		})
	}
	validMetrics := filterAcceptedMetricsHelper(sink, hydratedM)
	assert.Len(t, validMetrics, 5)
}

// TestFilterAcceptedMetrics_AllInvalidTypes tests FilterAcceptedMetrics with all invalid MeasuredTypes.
func TestFilterAcceptedMetrics_AllInvalidTypes(t *testing.T) {
	ctx := context.Background()
	config := common.LoadConfig()
	sink := NewSink(ctx, config)
	var hydratedM []entity.HydratedMetric
	for i := 0; i < 3; i++ {
		rm := metadata.ResourceMetadata{
			ResourceUUID:        nillable.ToPointer(uuid.New().String()),
			ResourceName:        nillable.ToPointer("dummy-resource-" + strconv.Itoa(i)),
			ResourceDisplayName: nillable.ToPointer("Dummy Resource " + strconv.Itoa(i)),
			AccountName:         nillable.ToPointer("netapp-au-se1-autopush-sde-tst"),
			RegionName:          nillable.ToPointer("us-central1"),
			ResourceType:        metadata.VolumePool,
		}
		hydratedM = append(hydratedM, entity.HydratedMetric{
			Metadata:     rm,
			MeasuredType: "",
			Quantity:     float64(1234 + i),
			Timestamp:    entity.UnixNano(time.Now().UnixNano()),
		})
	}
	validMetrics := filterAcceptedMetricsHelper(sink, hydratedM)
	assert.Len(t, validMetrics, 0)
}

// TestFilterAcceptedMetrics_MixedTypes tests FilterAcceptedMetrics with a mix of valid, empty, and unknown MeasuredTypes.
func TestFilterAcceptedMetrics_MixedTypes(t *testing.T) {
	ctx := context.Background()
	config := common.LoadConfig()
	sink := NewSink(ctx, config)
	var hydratedM []entity.HydratedMetric
	for i := 0; i < 3; i++ {
		rm := metadata.ResourceMetadata{
			ResourceUUID:        nillable.ToPointer(uuid.New().String()),
			ResourceName:        nillable.ToPointer("dummy-resource-" + strconv.Itoa(i)),
			ResourceDisplayName: nillable.ToPointer("Dummy Resource " + strconv.Itoa(i)),
			AccountName:         nillable.ToPointer("netapp-au-se1-autopush-sde-tst"),
			RegionName:          nillable.ToPointer("us-central1"),
			ResourceType:        metadata.VolumePool,
		}
		var metricType metadata.MeasuredType
		switch i {
		case 0:
			metricType = metadata.PoolAllocatedSize
		case 1:
			metricType = ""
		default:
			metricType = metadata.UnknownMeasuredType
		}
		hydratedM = append(hydratedM, entity.HydratedMetric{
			Metadata:     rm,
			MeasuredType: metricType,
			Quantity:     float64(1234 + i),
			Timestamp:    entity.UnixNano(time.Now().UnixNano()),
		})
	}
	validMetrics := filterAcceptedMetricsHelper(sink, hydratedM)
	assert.Len(t, validMetrics, 1)
}

// TestDeliverMetrics_MultipleValid tests DeliverMetrics with multiple valid metrics.
func TestDeliverMetrics_MultipleValid(t *testing.T) {
	ctx := context.Background()
	config := common.LoadConfig()
	sink := NewSink(ctx, config)
	var hydratedM []entity.HydratedMetric
	for i := 0; i < 4; i++ {
		rm := metadata.ResourceMetadata{
			ResourceUUID:        nillable.ToPointer(uuid.New().String()),
			ResourceName:        nillable.ToPointer("dummy-resource-" + strconv.Itoa(i)),
			ResourceDisplayName: nillable.ToPointer("Dummy Resource " + strconv.Itoa(i)),
			AccountName:         nillable.ToPointer("netapp-au-se1-autopush-sde-tst"),
			RegionName:          nillable.ToPointer("us-central1"),
			ResourceType:        metadata.VolumePool,
		}
		hydratedM = append(hydratedM, entity.HydratedMetric{
			Metadata:     rm,
			MeasuredType: metadata.PoolAllocatedSize,
			Quantity:     float64(1234 + i),
			Timestamp:    entity.UnixNano(time.Now().UnixNano()),
		})
	}
	count := sink.DeliverMetrics(ctx, hydratedM)
	assert.Equal(t, 4, count)
}

func TestGoogleSink_processMetricsResults_LogsNotImplemented(t *testing.T) {
	ml := &log.MockLogger{}
	ml.On("Warn", "processMetricsResults not implemented").Once()
	sink := &GoogleSink{
		logger: ml,
	}
	results := []common.MetricsResult{{}}
	sink.processMetricsResults(results)
	ml.AssertCalled(t, "Warn", "processMetricsResults not implemented")
}

// TestGoogleSink_processAndFilterMetricsResults_ErrorHandling tests various error scenarios
func TestGoogleSink_processAndFilterMetricsResults_ErrorHandling(t *testing.T) {
	ml := &log.MockLogger{}
	sink := &GoogleSink{
		logger: ml,
	}

	// Create test metrics
	customerID := "test-customer-123"
	validGoogleMetric := common.NewGoogleMetric(&datamodel.AggregatedUsage{
		VendorCustomerID: &customerID,
		MeasuredType:     metadata.LogicalSize,
	})

	invalidGoogleMetric := common.NewGoogleMetric("invalid-record")

	t.Run("Success case", func(t *testing.T) {
		ml.On("Infof", "Reported %d metrics.", 1).Once()
		ml.On("Infof", "%d metrics were successfully reported.", 1).Once()

		results := []common.MetricsResult{
			{
				GoogleMetric:   *validGoogleMetric,
				ReportResponse: nil,
				OperationID:    "op1",
				OperationName:  "operation1",
				Exception:      nil,
			},
		}

		sink.processAndFilterMetricsResults(results)
		ml.AssertExpectations(t)
	})

	t.Run("GetCustomerId error", func(t *testing.T) {
		ml.On("Infof", "Reported %d metrics.", 1).Once()
		ml.On("Warnf", "An error occurred while getting the Customer ID for:", "GoogleMetric:",
			*invalidGoogleMetric, "error:", mock.AnythingOfType("*common.InvalidGoogleMetricException")).Once()
		ml.On("Infof", "%d metrics were successfully reported.", 0).Once()

		results := []common.MetricsResult{
			{
				GoogleMetric:   *invalidGoogleMetric,
				ReportResponse: nil,
				OperationID:    "op1",
				OperationName:  "operation1",
				Exception:      nil,
			},
		}

		sink.processAndFilterMetricsResults(results)
		ml.AssertExpectations(t)
	})

	t.Run("Exception with 403 error", func(t *testing.T) {
		ml.On("Infof", "Reported %d metrics.", 1).Once()
		testErr := &googleapi.Error{
			Code:    403,
			Message: "Forbidden",
		}
		ml.On("Warnf", "Result with Exception and status code 403 or 404:", "Result - ",
			*validGoogleMetric, "OperationId:", "op1", "OperationName:", "operation1",
			"Project Id:", customerID, "Exception:", (*common.ReportResponse)(nil)).Once()
		ml.On("Infof", "%d metrics were successfully reported.", 0).Once()

		results := []common.MetricsResult{
			{
				GoogleMetric:   *validGoogleMetric,
				ReportResponse: nil,
				OperationID:    "op1",
				OperationName:  "operation1",
				Exception:      testErr,
			},
		}

		sink.processAndFilterMetricsResults(results)
		ml.AssertExpectations(t)
	})

	t.Run("Exception with 404 error", func(t *testing.T) {
		ml.On("Infof", "Reported %d metrics.", 1).Once()
		testErr := &googleapi.Error{
			Code:    404,
			Message: "Not Found",
		}
		ml.On("Warnf", "Result with Exception and status code 403 or 404:", "Result - ",
			*validGoogleMetric, "OperationId:", "op1", "OperationName:", "operation1",
			"Project Id:", customerID, "Exception:", (*common.ReportResponse)(nil)).Once()
		ml.On("Infof", "%d metrics were successfully reported.", 0).Once()

		results := []common.MetricsResult{
			{
				GoogleMetric:   *validGoogleMetric,
				ReportResponse: nil,
				OperationID:    "op1",
				OperationName:  "operation1",
				Exception:      testErr,
			},
		}

		sink.processAndFilterMetricsResults(results)
		ml.AssertExpectations(t)
	})

	t.Run("Exception with other error", func(t *testing.T) {
		ml.On("Infof", "Reported %d metrics.", 1).Once()
		testErr := &googleapi.Error{
			Code:    500,
			Message: "Internal Server Error",
		}
		ml.On("Errorf", "Result with Exception:", "Result - ", *validGoogleMetric,
			"OperationId:", "op1", "OperationName:", "operation1", "Project Id:",
			customerID, "Exception:", (*common.ReportResponse)(nil)).Once()
		ml.On("Infof", "%d metrics were successfully reported.", 0).Once()

		results := []common.MetricsResult{
			{
				GoogleMetric:   *validGoogleMetric,
				ReportResponse: nil,
				OperationID:    "op1",
				OperationName:  "operation1",
				Exception:      testErr,
			},
		}

		sink.processAndFilterMetricsResults(results)
		ml.AssertExpectations(t)
	})

	t.Run("ReportResponse with errors", func(t *testing.T) {
		ml.On("Infof", "Reported %d metrics.", 1).Once()
		reportResponse := &common.ReportResponse{
			ReportErrors: []*servicecontrol.ReportError{
				{
					OperationId: "op1",
					Status: &servicecontrol.Status{
						Code:    400,
						Message: "Invalid metric",
					},
				},
			},
		}
		ml.On("Error", "Result with Error:", "Result - ", *validGoogleMetric, "OperationId:",
			"op1", "OperationName:", "operation1", "Project Id:", customerID, "Exception:", reportResponse).Once()
		ml.On("Infof", "%d metrics were successfully reported.", 0).Once()

		results := []common.MetricsResult{
			{
				GoogleMetric:   *validGoogleMetric,
				ReportResponse: reportResponse,
				OperationID:    "op1",
				OperationName:  "operation1",
				Exception:      nil,
			},
		}

		sink.processAndFilterMetricsResults(results)
		ml.AssertExpectations(t)
	})
}

// TestGoogleSink_push_EmptyMetrics tests push method with empty metrics list
func TestGoogleSink_push_EmptyMetrics(t *testing.T) {
	ml := &log.MockLogger{}
	ml.On("Warn", "google metrics not found, hence not reporting anything.").Once()

	sink := &GoogleSink{
		logger: ml,
	}

	sink.push([]common.GoogleMetric{})
	ml.AssertExpectations(t)
}
