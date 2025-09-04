package performance

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/entity"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/googlePusher"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
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

	s.push(validMetrics)
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
			s.logger.Infof("Ignoring invalid metric", "Metric: ", m, "Warnings: ", strings.Join(warnings, "; "))
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

// push sends the given lists of first-party and third-party Google metrics.
// Parameters:
// - googleMetricList: The list of Google metrics to send.
func (s *GoogleSink) push(googleMetricList []common.GoogleMetric) {
	wg := sync.WaitGroup{}
	resultChan := make(chan []common.MetricsResult, 200)

	if len(googleMetricList) > 0 {
		wg.Add(3)
		go func() {
			defer wg.Done()
			s.logger.Debugf("Reporting First Party Google Metrics, Google Metric First Party list count: %d", len(googleMetricList))
			go s.metricClient.ReportMetrics(googleMetricList, time.Now().Unix(), time.Now().Unix(), &wg, resultChan)
			go s.processResponse(&wg, resultChan)
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
func (s *GoogleSink) processResponse(wg *sync.WaitGroup, resultChan chan []common.MetricsResult) {
	defer wg.Done()
	for result := range resultChan {
		s.processAndFilterMetricsResults(result)
	}

	s.logger.Info("Finished processing Google Performance Metrics Results")
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
			s.logger.Warnf("An error occurred while getting the Customer ID for:", "GoogleMetric:",
				result.GoogleMetric, "error:", err)
			continue
		}

		if result.Exception != nil {
			resultCode := common.GetCode(&result)

			switch {
			case strings.Contains(resultCode, HTTPForbiddenError), strings.Contains(resultCode, HTTPNotFoundError):
				s.logger.Warnf("Result with Exception and status code 403 or 404:", "Result - ",
					result.GoogleMetric, "OperationId:", result.OperationID, "OperationName:", result.OperationName,
					"Project Id:", id, "Exception:", result.ReportResponse)
			default:
				s.logger.Errorf("Result with Exception:", "Result - ", result.GoogleMetric,
					"OperationId:", result.OperationID, "OperationName:", result.OperationName, "Project Id:",
					id, "Exception:", result.ReportResponse)
			}
		} else if result.ReportResponse != nil && result.ReportResponse.ReportErrors != nil && len(result.ReportResponse.ReportErrors) > 0 {
			s.logger.Error("Result with Error:", "Result - ", result.GoogleMetric, "OperationId:",
				result.OperationID, "OperationName:", result.OperationName, "Project Id:", id, "Exception:", result.ReportResponse)
		} else {
			goodResults = append(goodResults, result)
		}
	}

	s.logger.Infof("%d metrics were successfully reported.", len(goodResults))
}
