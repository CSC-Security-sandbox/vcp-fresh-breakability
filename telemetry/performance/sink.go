package performance

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/entity"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/googlePusher"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

const (
	HTTPForbiddenError = "403"
	HTTPNotFoundError  = "404"
)

// Sink is any destination for the metrics
type Sink interface {
	DeliverMetrics(ctx context.Context, metrics []entity.HydratedMetric) int
}

// GoogleSink is responsible for delivering metrics to Google.
type GoogleSink struct {
	metricClient googlePusher.GoogleMetricsClient
	logger       log.Logger
}

func (s *GoogleSink) processMetricsResults(results []common.MetricsResult) {
	s.logger.Warn("processMetricsResults not implemented")
}

func NewSink(ctx context.Context, config *common.TelemetryConfig) *GoogleSink {
	return &GoogleSink{
		metricClient: *googlePusher.NewGoogleMetricsClient(ctx, config.RootUrl, config),
		logger:       util.GetLogger(ctx),
	}
}

func (s *GoogleSink) DeliverMetrics(ctx context.Context, hydratedMetrics []entity.HydratedMetric) (validMetricsCount int) {
	validMetrics := s.FilterAndConvertToGoogleMetrics(hydratedMetrics)
	invalidMetricCount := len(hydratedMetrics) - len(validMetrics)
	if invalidMetricCount > 0 {
		s.logger.Infof("Skipped invalid metrics. Invalid Metric Count %d", invalidMetricCount)
	}

	s.push(ctx, validMetrics)
	validMetricsCount = len(validMetrics)
	return validMetricsCount
}

// FilterAndConvertToGoogleMetrics filters out invalid metrics from the provided list and convert to google metrics.
// Parameters:
// - metrics: The list of hydrated metrics to filter.
// Returns:
// - A list of valid hydrated metrics.
func (s *GoogleSink) FilterAndConvertToGoogleMetrics(metrics []entity.HydratedMetric) []common.GoogleMetric {
	var warnings []string
	var validMetrics []common.GoogleMetric

	for _, m := range metrics {
		metric := m
		googleMetric := common.GoogleMetric{Record: &metric}
		if s.isValidHydratedMetric(m, &warnings) {
			validMetrics = append(validMetrics, googleMetric)
		} else {
			s.logger.Infof("Ignoring invalid metric: %v, Warnings: %s", m, strings.Join(warnings, "; "))
		}
	}
	return validMetrics
}

// isValidHydratedMetric checks if the given metric is valid.
// Parameters:
// - metric: The hydrated metric to check.
// - warnings: A list to record any warnings encountered during validation.
// Returns:
// - A boolean indicating whether the metric is valid.
func (s *GoogleSink) isValidHydratedMetric(metric entity.HydratedMetric, warnings *[]string) bool {
	var reason strings.Builder
	reason.WriteString("Invalid metric: ")
	isValid := true

	if metric.MeasuredType == metadata.UnknownMeasuredType {
		reason.WriteString("Measured type not defined in the MeasuredType enum; ")
		isValid = false
	}
	if metric.MeasuredType == "" {
		reason.WriteString("Measured Type is empty")
		isValid = false
	}
	if !isValid {
		*warnings = append(*warnings, reason.String())
	}
	return isValid
}

// formatMetricForLogging formats a GoogleMetric in a user-friendly way for logging
func formatMetricForLogging(metric common.GoogleMetric) string {
	switch metric.GetType() {
	case common.HydratedMetric:
		hydratedMetric, err := metric.GetAsHydratedMetric()
		if err != nil {
			return fmt.Sprintf("HydratedMetric (error: %v)", err)
		}

		resourceName := "unknown"
		if hydratedMetric.Metadata.ResourceName != nil {
			resourceName = *hydratedMetric.Metadata.ResourceName
		}

		accountName := "unknown"
		if hydratedMetric.Metadata.AccountName != nil {
			accountName = *hydratedMetric.Metadata.AccountName
		}

		regionName := "unknown"
		if hydratedMetric.Metadata.RegionName != nil {
			regionName = *hydratedMetric.Metadata.RegionName
		}

		timestamp := hydratedMetric.Timestamp.ToTime().Format(time.RFC3339)

		return fmt.Sprintf("Resource: %s, Type: %s, MeasuredType: %s, Quantity: %.2f, Account: %s, Region: %s, Timestamp: %s",
			resourceName, hydratedMetric.Metadata.ResourceType, hydratedMetric.MeasuredType, hydratedMetric.Quantity, accountName, regionName, timestamp)

	case common.BillingMetric:
		billingMetric, err := metric.GetAsUsageBillingMetric()
		if err != nil {
			return fmt.Sprintf("BillingMetric (error: %v)", err)
		}

		resourceName := "unknown"
		if billingMetric.ResourceName != nil {
			resourceName = *billingMetric.ResourceName
		}

		customerID := "unknown"
		if billingMetric.VendorCustomerID != nil {
			customerID = *billingMetric.VendorCustomerID
		}

		regionName := "unknown"
		if billingMetric.RegionName != nil {
			regionName = *billingMetric.RegionName
		}

		return fmt.Sprintf("Resource: %s, Type: %s, MeasuredType: %s, Quantity: %.2f, CustomerID: %s, Region: %s, AggregationType: %s",
			resourceName, billingMetric.ResourceType, billingMetric.MeasuredType, billingMetric.Quantity, customerID, regionName, billingMetric.AggregationType)

	default:
		return fmt.Sprintf("Unknown metric type: %d", metric.GetType())
	}
}

// push sends the given lists of first-party and third-party Google metrics.
// Parameters:
// - googleMetricList: The list of Google metrics to send.
func (s *GoogleSink) push(ctx context.Context, googleMetricList []common.GoogleMetric) {
	wg := sync.WaitGroup{}
	resultChan := make(chan []common.MetricsResult, 200)

	if len(googleMetricList) > 0 {
		wg.Add(3)
		go func() {
			defer wg.Done()
			s.logger.Debugf("Reporting First Party Google Metrics, Google Metric First Party list count: %d", len(googleMetricList))
			go s.metricClient.ReportMetrics(ctx, googleMetricList, time.Now().Unix(), time.Now().Unix(), &wg, resultChan)
			go s.processResponse(ctx, &wg, resultChan)
		}()
	} else {
		s.logger.Warn("google metrics not found, hence not reporting anything.")
	}

	wg.Wait()
}

// processResponse checks if the required fields are present in the GoogleMetric.
// Parameters:
// - wg: WaitGroup to wait for the goroutines to finish.
// - resultChan: The channel to receive the results.
func (s *GoogleSink) processResponse(ctx context.Context, wg *sync.WaitGroup, resultChan chan []common.MetricsResult) {
	defer wg.Done()

	// Extract correlation ID from context for logging
	logger := util.GetLogger(ctx)
	correlationID := "unknown"
	if loggerFields, ok := ctx.Value(middleware.TemporalSLoggerKey).(log.Fields); ok {
		if corrIDStr, exists := loggerFields["requestCorrelationID"].(string); exists {
			correlationID = corrIDStr
		}
	}

	logger.Infof("Processing performance metrics results with correlation ID: %s", correlationID)

	for result := range resultChan {
		s.processAndFilterMetricsResults(result)
	}

	logger.Info("Finished processing Google Performance Metrics Results")
}

// processAndFilterMetricsResults - Process and Filter Metrics Result
// Parameters:
// - results: List of Metrics Result
func (s *GoogleSink) processAndFilterMetricsResults(results []common.MetricsResult) {
	var goodResults []common.MetricsResult

	s.logger.Infof("Reported %d metrics.", len(results))

	for _, result := range results {
		id, err := result.GoogleMetric.GetCustomerId()
		if err != nil {
			s.logger.Warnf("Failed to get Customer ID for GoogleMetric: %v, error: %v", result.GoogleMetric, err)
			continue
		}

		if result.Exception != nil {
			resultCode := common.GetCode(&result)
			metricInfo := formatMetricForLogging(result.GoogleMetric)

			switch {
			case strings.Contains(resultCode, HTTPForbiddenError), strings.Contains(resultCode, HTTPNotFoundError):
				s.logger.Debugf("Performance metrics delivery failed with %s error - %s, OperationId: %s, OperationName: %s, ProjectId: %s, Exception: %v",
					resultCode, metricInfo, result.OperationID, result.OperationName, id, getCodeResponse(result.ReportResponse))
			default:
				s.logger.Debugf("Performance metrics delivery failed with exception - %s, OperationId: %s, OperationName: %s, ProjectId: %s, Exception: %v",
					metricInfo, result.OperationID, result.OperationName, id, result.GetException().Error())
			}
		} else if result.ReportResponse != nil && result.ReportResponse.ReportErrors != nil && len(result.ReportResponse.ReportErrors) > 0 {
			metricInfo := formatMetricForLogging(result.GoogleMetric)
			s.logger.Debugf("Performance metrics delivery failed with report errors - %s, OperationId: %s, OperationName: %s, ProjectId: %s, ReportErrors: %v",
				metricInfo, result.OperationID, result.OperationName, id, getCodeResponse(result.ReportResponse))
		} else {
			goodResults = append(goodResults, result)
		}
	}

	s.logger.Infof("%d metrics were successfully reported.", len(goodResults))
}

func getCodeResponse(response *common.ReportResponse) string {
	if response == nil {
		return "unknown"
	}
	if len(response.ReportErrors) == 0 {
		return "200"
	}
	return strconv.FormatInt(response.ReportErrors[0].Status.Code, 10)
}
